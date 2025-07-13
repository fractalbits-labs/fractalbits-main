pub mod config;
pub mod handler;
mod object_layout;

use aws_sdk_s3::{
    config::{BehaviorVersion, Credentials, Region},
    Client as S3Client, Config as S3Config,
};
use axum::extract::FromRef;

use bucket_tables::{table::KvClientProvider, Versioned};
use bytes::Bytes;
use config::{ArcConfig, S3CacheConfig};
use futures::stream::{self, StreamExt};
use metrics::histogram;
use moka::future::Cache;
use object_layout::ObjectLayout;
use rpc_client_bss::{RpcClientBss, RpcErrorBss};
use rpc_client_nss::RpcClientNss;

use slotmap_conn_pool::ConnPool;
use std::{
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{
    net::TcpStream,
    sync::mpsc::{self, Receiver, Sender},
    task::JoinHandle,
};
use tracing::info;
use uuid::Uuid;

pub type BlobId = uuid::Uuid;

pub struct AppState {
    pub config: ArcConfig,
    pub cache: Arc<Cache<String, Versioned<String>>>,

    rpc_clients_nss: ConnPool<Arc<RpcClientNss>, SocketAddr>,
    rpc_clients_rss: ConnPool<Arc<rpc_client_rss::RpcClientRss>, SocketAddr>,

    blob_client: Arc<BlobClient>,
    blob_deletion: Sender<(BlobId, usize)>,
}

impl FromRef<Arc<AppState>> for ArcConfig {
    fn from_ref(state: &Arc<AppState>) -> Self {
        Self(state.config.0.clone())
    }
}

impl KvClientProvider for AppState {
    type Error = rpc_client_rss::RpcErrorRss;

    async fn get_client(&self) -> impl kv_client_traits::KvClient<Error = Self::Error> {
        self.checkout_rpc_client_rss().await
    }
}

impl AppState {
    const NSS_CONNECTION_POOL_SIZE: usize = 64;
    const RSS_CONNECTION_POOL_SIZE: usize = 64;

    pub async fn new(config: ArcConfig) -> Self {
        let rpc_clients_nss = Self::new_rpc_clients_pool_nss(config.nss_addr).await;
        let rpc_clients_rss = Self::new_rpc_clients_pool_rss(config.rss_addr).await;

        let (tx, rx) = mpsc::channel(1024 * 1024);
        let blob_client = Arc::new(BlobClient::new(config.bss_addr, &config.s3_cache, rx).await);

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
    ) -> ConnPool<Arc<RpcClientNss>, SocketAddr> {
        let rpc_clients_nss = ConnPool::new();
        for _ in 0..Self::NSS_CONNECTION_POOL_SIZE {
            let stream = TcpStream::connect(nss_addr).await.unwrap();
            let client = Arc::new(RpcClientNss::new(stream).await.unwrap());
            rpc_clients_nss.pooled(nss_addr, client);
        }

        info!(
            "NSS RPC client pool initialized with {} connections.",
            Self::NSS_CONNECTION_POOL_SIZE
        );
        rpc_clients_nss
    }

    async fn new_rpc_clients_pool_rss(
        rss_addr: SocketAddr,
    ) -> ConnPool<Arc<rpc_client_rss::RpcClientRss>, SocketAddr> {
        let rpc_clients_rss = ConnPool::new();
        for _ in 0..Self::RSS_CONNECTION_POOL_SIZE {
            let stream = TcpStream::connect(rss_addr).await.unwrap();
            let client = Arc::new(rpc_client_rss::RpcClientRss::new(stream).await.unwrap());
            rpc_clients_rss.pooled(rss_addr, client);
        }

        info!(
            "RSS RPC client pool initialized with {} connections.",
            Self::RSS_CONNECTION_POOL_SIZE
        );
        rpc_clients_rss
    }

    pub async fn checkout_rpc_client_nss(&self) -> Arc<RpcClientNss> {
        let start = Instant::now();
        let res = self
            .rpc_clients_nss
            .checkout(self.config.nss_addr)
            .await
            .unwrap();
        histogram!("checkout_rpc_client_nanos", "type" => "nss")
            .record(start.elapsed().as_nanos() as f64);
        res
    }

    pub fn get_blob_client(&self) -> Arc<BlobClient> {
        self.blob_client.clone()
    }

    pub async fn checkout_rpc_client_rss(&self) -> Arc<rpc_client_rss::RpcClientRss> {
        let start = Instant::now();
        let res = self
            .rpc_clients_rss
            .checkout(self.config.rss_addr)
            .await
            .unwrap();
        histogram!("checkout_rpc_client_nanos", "type" => "rss")
            .record(start.elapsed().as_nanos() as f64);
        res
    }
}

pub struct BlobClient {
    clients_bss: ConnPool<Arc<RpcClientBss>, SocketAddr>,
    client_s3: S3Client,
    s3_cache_bucket: String,
    #[allow(dead_code)]
    blob_deletion_task_handle: JoinHandle<()>,
    bss_addr: SocketAddr,
}

impl BlobClient {
    const BSS_CONNECTION_POOL_SIZE: usize = 64;

    pub async fn new(
        bss_addr: SocketAddr,
        config: &S3CacheConfig,
        rx: Receiver<(BlobId, usize)>,
    ) -> Self {
        let clients_bss = ConnPool::new();
        for _ in 0..Self::BSS_CONNECTION_POOL_SIZE {
            let stream = TcpStream::connect(bss_addr).await.unwrap();
            let client = Arc::new(RpcClientBss::new(stream).await.unwrap());
            clients_bss.pooled(bss_addr, client);
        }

        info!(
            "BSS RPC client pool initialized with {} connections.",
            Self::BSS_CONNECTION_POOL_SIZE
        );

        let client_s3 = if config.s3_host.ends_with("amazonaws.com") {
            let aws_config = aws_config::defaults(BehaviorVersion::latest())
                .region(Region::new(config.s3_region.clone()))
                .load()
                .await;
            S3Client::new(&aws_config)
        } else {
            let credentials = Credentials::new("minioadmin", "minioadmin", None, None, "s3_cache");
            let s3_config = S3Config::builder()
                .endpoint_url(format!("{}:{}", config.s3_host, config.s3_port))
                .region(Region::new(config.s3_region.clone()))
                .credentials_provider(credentials)
                .behavior_version(BehaviorVersion::latest())
                .build();

            S3Client::from_conf(s3_config)
        };

        let blob_deletion_task_handle = tokio::spawn({
            let clients_bss = clients_bss.clone();
            async move {
                if let Err(e) = Self::blob_deletion_task(clients_bss, rx, bss_addr).await {
                    tracing::error!("FATAL: blob deletion task error: {e}");
                }
            }
        });

        Self {
            clients_bss,
            client_s3,
            s3_cache_bucket: config.s3_bucket.clone(),
            blob_deletion_task_handle,
            bss_addr,
        }
    }

    async fn blob_deletion_task(
        clients_bss: ConnPool<Arc<RpcClientBss>, SocketAddr>,
        mut input: Receiver<(BlobId, usize)>,
        bss_addr: SocketAddr,
    ) -> Result<(), RpcErrorBss> {
        while let Some((blob_id, block_numbers)) = input.recv().await {
            let deleted = stream::iter(0..block_numbers)
                .map(|block_number| {
                    let clients_bss = clients_bss.clone();
                    async move {
                        let rpc_client_bss = clients_bss.checkout(bss_addr).await.unwrap();
                        let res = rpc_client_bss
                            .delete_blob(blob_id, block_number as u32)
                            .await;
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
    ) -> Result<usize, RpcErrorBss> {
        let start = Instant::now();
        let rpc_client_bss = self.clients_bss.checkout(self.bss_addr).await.unwrap();
        histogram!("checkout_rpc_client_nanos", "type" => "bss")
            .record(start.elapsed().as_nanos() as f64);
        if block_number == 0 && body.len() < ObjectLayout::DEFAULT_BLOCK_SIZE as usize {
            let res = rpc_client_bss.put_blob(blob_id, block_number, body).await;
            histogram!("bss_rpc", "op" => "put_blob").record(start.elapsed().as_nanos() as f64);
            return res;
        }

        let s3_key = format!("{blob_id}-{block_number}");
        let (res_s3, res_bss) = tokio::join!(
            self.client_s3
                .put_object()
                .bucket(&self.s3_cache_bucket)
                .key(s3_key)
                .body(body.clone().into())
                .send(),
            rpc_client_bss.put_blob(blob_id, block_number, body)
        );
        histogram!("bss_rpc", "op" => "put_blob_join_with_s3")
            .record(start.elapsed().as_nanos() as f64);
        assert!(res_s3.is_ok());
        res_bss
    }

    pub async fn get_blob(
        &self,
        blob_id: Uuid,
        block_number: u32,
        body: &mut Bytes,
    ) -> Result<usize, RpcErrorBss> {
        let start = Instant::now();
        let rpc_client_bss = self.clients_bss.checkout(self.bss_addr).await.unwrap();
        histogram!("checkout_rpc_client_nanos", "type" => "bss")
            .record(start.elapsed().as_nanos() as f64);
        rpc_client_bss.get_blob(blob_id, block_number, body).await
    }

    pub async fn delete_blob(&self, blob_id: Uuid, block_number: u32) -> Result<(), RpcErrorBss> {
        let start = Instant::now();
        let s3_key = format!("{blob_id}-{block_number}");
        let rpc_client_bss = self.clients_bss.checkout(self.bss_addr).await.unwrap();
        histogram!("checkout_rpc_client_nanos", "type" => "bss")
            .record(start.elapsed().as_nanos() as f64);
        let (res_s3, res_bss) = tokio::join!(
            self.client_s3
                .delete_object()
                .bucket(&self.s3_cache_bucket)
                .key(&s3_key)
                .send(),
            rpc_client_bss.delete_blob(blob_id, block_number)
        );
        if let Err(e) = res_s3 {
            // note this blob may not be uploaded to s3 yet
            tracing::warn!("delete {s3_key} failed: {e}");
        }
        res_bss
    }
}
