mod config;
pub mod handler;
mod object_layout;

use aws_sdk_s3::{
    config::{BehaviorVersion, Credentials, Region},
    Client as S3Client, Config as S3Config,
};

use bucket_tables::{table::KvClientProvider, Versioned};
use bytes::Bytes;
pub use config::{Config, S3CacheConfig};
use futures::stream::{self, StreamExt};
use metrics::histogram;
use moka::future::Cache;
use object_layout::ObjectLayout;
use rpc_client_bss::{RpcClientBss, RpcErrorBss};
use rpc_client_nss::RpcClientNss;
use rpc_client_rss::{RpcClientRss, RpcErrorRss};

use rpc_client_common::{bss_rpc_retry, rpc_retry};
use slotmap_conn_pool::{ConnPool, Poolable};
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
            BlobClient::new(config.bss_addr, config.bss_conn_num, &config.s3_cache, rx).await,
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
        for _ in 0..nss_conn_num as usize {
            let stream = TcpStream::connect(nss_addr).await.unwrap();
            let client = Arc::new(RpcClientNss::new(stream).await.unwrap());
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
        for _ in 0..rss_conn_num as usize {
            let stream = TcpStream::connect(rss_addr).await.unwrap();
            let client = Arc::new(RpcClientRss::new(stream).await.unwrap());
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
}

pub struct BlobClient {
    rpc_clients_bss: ConnPool<Arc<RpcClientBss>, SocketAddr>,
    client_s3: S3Client,
    s3_cache_bucket: String,
    #[allow(dead_code)]
    blob_deletion_task_handle: JoinHandle<()>,
    bss_addr: SocketAddr,
}

impl BlobClient {
    pub async fn new(
        bss_addr: SocketAddr,
        bss_conn_num: u16,
        config: &S3CacheConfig,
        rx: Receiver<(BlobId, usize)>,
    ) -> Self {
        let clients_bss = ConnPool::new();
        for _ in 0..bss_conn_num as usize {
            let stream = TcpStream::connect(bss_addr).await.unwrap();
            let client = Arc::new(RpcClientBss::new(stream).await.unwrap());
            clients_bss.pooled(bss_addr, client);
        }

        info!("BSS RPC client pool initialized with {bss_conn_num} connections.");

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
            rpc_clients_bss: clients_bss,
            client_s3,
            s3_cache_bucket: config.s3_bucket.clone(),
            blob_deletion_task_handle,
            bss_addr,
        }
    }

    pub async fn checkout_rpc_client_bss(
        &self,
    ) -> Result<Arc<RpcClientBss>, <RpcClientBss as Poolable>::Error> {
        let start = Instant::now();
        let res = self.rpc_clients_bss.checkout(self.bss_addr).await?;
        histogram!("checkout_rpc_client_nanos", "type" => "bss")
            .record(start.elapsed().as_nanos() as f64);
        Ok(res)
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
                        let res = rpc_retry!(
                            clients_bss,
                            checkout(bss_addr),
                            delete_blob(blob_id, block_number as u32)
                        )
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
    ) -> Result<(), RpcErrorBss> {
        histogram!("blob_size", "operation" => "put").record(body.len() as f64);
        let start = Instant::now();
        if block_number == 0 && body.len() < ObjectLayout::DEFAULT_BLOCK_SIZE as usize {
            let res = bss_rpc_retry!(self, put_blob(blob_id, block_number, body.clone())).await;
            return res;
        }

        let s3_key = format!("{blob_id}-{block_number}");
        let (res_s3, res_bss) = tokio::join!(
            self.client_s3
                .put_object()
                .bucket(&self.s3_cache_bucket)
                .key(&s3_key)
                .body(body.clone().into())
                .send(),
            bss_rpc_retry!(self, put_blob(blob_id, block_number, body.clone()))
        );
        histogram!("rpc_duration_nanos", "type"  => "bss_s3_join",  "name" => "put_blob_join_with_s3")
            .record(start.elapsed().as_nanos() as f64);
        assert!(res_s3.is_ok());
        res_bss
    }

    pub async fn get_blob(
        &self,
        blob_id: Uuid,
        block_number: u32,
        body: &mut Bytes,
    ) -> Result<(), RpcErrorBss> {
        let res = bss_rpc_retry!(self, get_blob(blob_id, block_number, body)).await;
        if res.is_ok() {
            histogram!("blob_size", "operation" => "get").record(body.len() as f64);
        }
        res
    }

    pub async fn delete_blob(&self, blob_id: Uuid, block_number: u32) -> Result<(), RpcErrorBss> {
        let s3_key = format!("{blob_id}-{block_number}");
        let (res_s3, res_bss) = tokio::join!(
            self.client_s3
                .delete_object()
                .bucket(&self.s3_cache_bucket)
                .key(&s3_key)
                .send(),
            bss_rpc_retry!(self, delete_blob(blob_id, block_number))
        );
        if let Err(e) = res_s3 {
            // note this blob may not be uploaded to s3 yet
            tracing::warn!("delete {s3_key} failed: {e}");
        }
        res_bss
    }
}
