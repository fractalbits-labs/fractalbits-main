mod blob_storage;
mod config;
pub mod handler;
mod object_layout;

use blob_storage::{
    BlobStorage, BlobStorageError, BlobStorageImpl, BssOnlyStorage, HybridStorage, S3ExpressStorage,
};
use bucket_tables::{table::KvClientProvider, Versioned};
use bytes::Bytes;
pub use config::{BlobStorageBackend, BlobStorageConfig, Config, S3CacheConfig};
use futures::stream::{self, StreamExt};
use metrics::histogram;
use moka::future::Cache;
use rpc_client_nss::RpcClientNss;
use rpc_client_rss::{RpcClientRss, RpcErrorRss};

use slotmap_conn_pool::{ConnPool, Poolable};
use std::{
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{
    sync::mpsc::{self, Receiver, Sender},
    task::JoinHandle,
};
use tracing::info;
use uuid::Uuid;

pub type BlobId = uuid::Uuid;

pub struct AppState {
    pub config: Arc<Config>,
    pub cache: Arc<Cache<String, Versioned<String>>>,

    rpc_clients_nss: ConnPool<Arc<RpcClientNss>, SocketAddr>,
    rpc_clients_rss: ConnPool<Arc<RpcClientRss>, SocketAddr>,

    blob_client: Arc<BlobClient>,
    blob_deletion: Sender<(BlobId, usize)>,
}

impl KvClientProvider for AppState {
    type Error = RpcErrorRss;

    async fn checkout_rpc_client_rss(
        &self,
    ) -> Result<
        impl kv_client_traits::KvClient<Error = Self::Error>,
        <RpcClientRss as Poolable>::Error,
    > {
        let start = Instant::now();
        let res = self.rpc_clients_rss.checkout(self.config.rss_addr).await?;
        histogram!("checkout_rpc_client_nanos", "type" => "rss")
            .record(start.elapsed().as_nanos() as f64);
        Ok(res)
    }
}

impl AppState {
    pub async fn new(config: Arc<Config>) -> Self {
        let rpc_clients_rss =
            Self::new_rpc_clients_pool_rss(config.rss_addr, config.rss_conn_num).await;
        let rpc_clients_nss =
            Self::new_rpc_clients_pool_nss(config.nss_addr, config.nss_conn_num).await;

        let (tx, rx) = mpsc::channel(1024 * 1024);
        let blob_client = Arc::new(
            BlobClient::new(&config.blob_storage, rx, config.rpc_timeout())
                .await
                .expect("Failed to initialize blob client"),
        );

        let cache = Arc::new(
            Cache::builder()
                .time_to_idle(Duration::from_secs(300))
                .max_capacity(10_000)
                .build(),
        );
        Self {
            config,
            rpc_clients_nss,
            blob_client,
            blob_deletion: tx,
            rpc_clients_rss,
            cache,
        }
    }

    async fn new_rpc_clients_pool_nss(
        nss_addr: SocketAddr,
        nss_conn_num: u16,
    ) -> ConnPool<Arc<RpcClientNss>, SocketAddr> {
        let rpc_clients_nss = ConnPool::new();

        // Use the Poolable trait's retry logic instead of manual retry
        for i in 0..nss_conn_num as usize {
            info!(
                "Connecting to NSS server at {nss_addr} (connection {}/{})",
                i + 1,
                nss_conn_num
            );
            let client = Arc::new(
                <RpcClientNss as slotmap_conn_pool::Poolable>::new(nss_addr)
                    .await
                    .unwrap(),
            );
            rpc_clients_nss.pooled(nss_addr, client);
        }

        info!("NSS RPC client pool initialized with {nss_conn_num} connections.");
        rpc_clients_nss
    }

    async fn new_rpc_clients_pool_rss(
        rss_addr: SocketAddr,
        rss_conn_num: u16,
    ) -> ConnPool<Arc<RpcClientRss>, SocketAddr> {
        let rpc_clients_rss = ConnPool::new();

        // Use the Poolable trait's retry logic
        for i in 0..rss_conn_num as usize {
            info!(
                "Connecting to RSS server at {rss_addr} (connection {}/{})",
                i + 1,
                rss_conn_num
            );
            let client = Arc::new(
                <RpcClientRss as slotmap_conn_pool::Poolable>::new(rss_addr)
                    .await
                    .unwrap(),
            );
            rpc_clients_rss.pooled(rss_addr, client);
        }

        info!("RSS RPC client pool initialized with {rss_conn_num} connections.");
        rpc_clients_rss
    }

    pub async fn checkout_rpc_client_nss(
        &self,
    ) -> Result<Arc<RpcClientNss>, <RpcClientNss as Poolable>::Error> {
        let start = Instant::now();
        let res = self.rpc_clients_nss.checkout(self.config.nss_addr).await?;
        histogram!("checkout_rpc_client_nanos", "type" => "nss")
            .record(start.elapsed().as_nanos() as f64);
        Ok(res)
    }

    pub fn get_blob_client(&self) -> Arc<BlobClient> {
        self.blob_client.clone()
    }

    pub fn get_blob_deletion(&self) -> Sender<(BlobId, usize)> {
        self.blob_deletion.clone()
    }
}

pub struct BlobClient {
    storage: Arc<BlobStorageImpl>,
    #[allow(dead_code)]
    blob_deletion_task_handle: JoinHandle<()>,
}

impl BlobClient {
    pub async fn new(
        blob_storage_config: &BlobStorageConfig,
        rx: Receiver<(BlobId, usize)>,
        rpc_timeout: Duration,
    ) -> Result<Self, BlobStorageError> {
        let storage = match &blob_storage_config.backend {
            BlobStorageBackend::BssOnly => {
                let bss_config = blob_storage_config.bss.as_ref().ok_or_else(|| {
                    BlobStorageError::Config(
                        "BSS configuration required for BssOnly backend".into(),
                    )
                })?;
                BlobStorageImpl::BssOnly(
                    BssOnlyStorage::new(bss_config.addr, bss_config.conn_num, rpc_timeout).await,
                )
            }
            BlobStorageBackend::Hybrid => {
                let bss_config = blob_storage_config.bss.as_ref().ok_or_else(|| {
                    BlobStorageError::Config("BSS configuration required for Hybrid backend".into())
                })?;
                let s3_cache_config = blob_storage_config.s3_cache.as_ref().ok_or_else(|| {
                    BlobStorageError::Config(
                        "S3 cache configuration required for Hybrid backend".into(),
                    )
                })?;
                BlobStorageImpl::Hybrid(
                    HybridStorage::new(
                        bss_config.addr,
                        bss_config.conn_num,
                        s3_cache_config,
                        rpc_timeout,
                    )
                    .await,
                )
            }
            BlobStorageBackend::S3Express => {
                let s3_express_config =
                    blob_storage_config.s3_express.as_ref().ok_or_else(|| {
                        BlobStorageError::Config(
                            "S3 Express configuration required for S3Express backend".into(),
                        )
                    })?;
                let express_config = blob_storage::S3ExpressConfig {
                    s3_host: s3_express_config.s3_host.clone(),
                    s3_port: s3_express_config.s3_port,
                    s3_region: s3_express_config.s3_region.clone(),
                    s3_bucket: s3_express_config.s3_bucket.clone(),
                    az: s3_express_config.az.clone(),
                    express_session_auth: s3_express_config.express_session_auth,
                };
                BlobStorageImpl::S3Express(S3ExpressStorage::new(&express_config).await?)
            }
        };
        let storage = Arc::new(storage);

        let blob_deletion_task_handle = tokio::spawn({
            let storage = storage.clone();
            async move {
                if let Err(e) = Self::blob_deletion_task(storage, rx).await {
                    tracing::error!("FATAL: blob deletion task error: {e}");
                }
            }
        });

        Ok(Self {
            storage,
            blob_deletion_task_handle,
        })
    }

    async fn blob_deletion_task(
        storage: Arc<BlobStorageImpl>,
        mut input: Receiver<(BlobId, usize)>,
    ) -> Result<(), BlobStorageError> {
        while let Some((blob_id, block_numbers)) = input.recv().await {
            let deleted = stream::iter(0..block_numbers)
                .map(|block_number| {
                    let storage = storage.clone();
                    async move {
                        let res = storage.delete_blob(blob_id, block_number as u32).await;
                        match res {
                            Ok(()) => 1,
                            Err(e) => {
                                tracing::warn!("delete {blob_id}-p{block_number} failed: {e}");
                                0
                            }
                        }
                    }
                })
                .buffer_unordered(10)
                .fold(0, |acc, x| async move { acc + x })
                .await;
            let failed = block_numbers - deleted;
            if failed != 0 {
                tracing::warn!("delete parts of {blob_id}: ok={deleted},err={failed}");
            }
        }
        Ok(())
    }

    pub async fn put_blob(
        &self,
        blob_id: Uuid,
        block_number: u32,
        body: Bytes,
    ) -> Result<(), BlobStorageError> {
        self.storage.put_blob(blob_id, block_number, body).await
    }

    pub async fn get_blob(
        &self,
        blob_id: Uuid,
        block_number: u32,
        body: &mut Bytes,
    ) -> Result<(), BlobStorageError> {
        self.storage.get_blob(blob_id, block_number, body).await
    }

    pub async fn delete_blob(
        &self,
        blob_id: Uuid,
        block_number: u32,
    ) -> Result<(), BlobStorageError> {
        self.storage.delete_blob(blob_id, block_number).await
    }
}
