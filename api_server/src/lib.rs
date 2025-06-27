pub mod config;
pub mod handler;
mod object_layout;

use aws_sdk_s3::{
    config::{BehaviorVersion, Credentials, Region},
    Client as S3Client, Config as S3Config,
};
use axum::extract::FromRef;
use bb8::{Pool, PooledConnection};
use bucket_tables::table::Versioned;
use bytes::Bytes;
use config::{ArcConfig, S3CacheConfig};
use futures::stream::{self, StreamExt};
use moka::future::Cache;
use object_layout::ObjectLayout;
use rpc_client_bss::{RpcConnManagerBss, RpcErrorBss};
use rpc_client_nss::RpcConnManagerNss;
use rpc_client_rss::RpcConnManagerRss;
use std::{net::SocketAddr, sync::Arc, time::Duration};
use tokio::{
    sync::mpsc::{self, Receiver, Sender},
    task::JoinHandle,
};
use tracing::info;
use uuid::Uuid;

pub type BlobId = uuid::Uuid;

pub struct AppState {
    pub config: ArcConfig,
    pub cache: Arc<Cache<String, Versioned<String>>>,

    rpc_clients_nss: Pool<RpcConnManagerNss>,
    rpc_clients_rss: Pool<RpcConnManagerRss>,

    blob_client: Arc<BlobClient>,
    blob_deletion: Sender<(BlobId, usize)>,
}

impl FromRef<Arc<AppState>> for ArcConfig {
    fn from_ref(state: &Arc<AppState>) -> Self {
        Self(state.config.0.clone())
    }
}

impl AppState {
    const NSS_CONNECTION_POOL_SIZE: u32 = 32;
    const RSS_CONNECTION_POOL_SIZE: u32 = 32;

    pub async fn new(config: ArcConfig) -> Self {
        let rpc_clients_nss = Self::new_rpc_clients_pool_nss(&config.nss_addr).await;
        let rpc_clients_rss = Self::new_rpc_clients_pool_rss(&config.rss_addr).await;

        let (tx, rx) = mpsc::channel(1024 * 1024);
        let blob_client = Arc::new(BlobClient::new(&config.bss_addr, &config.s3_cache, rx).await);

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

    async fn new_rpc_clients_pool_nss(nss_addr: &str) -> Pool<RpcConnManagerNss> {
        let resolved_addrs: Vec<SocketAddr> = tokio::net::lookup_host(nss_addr)
            .await
            .expect("Failed to resolve NSS RPC server address")
            .collect();

        assert!(!resolved_addrs.is_empty());
        let manager = RpcConnManagerNss::new(resolved_addrs);
        let rpc_clients_nss = Pool::builder()
            .max_size(Self::NSS_CONNECTION_POOL_SIZE)
            .min_idle(Some(2))
            .build(manager)
            .await
            .expect("Failed to build nss rpc clients pool");

        info!(
            "NSS RPC client pool initialized with {} connections.",
            Self::NSS_CONNECTION_POOL_SIZE
        );
        rpc_clients_nss
    }

    async fn new_rpc_clients_pool_rss(rss_addr: &str) -> Pool<RpcConnManagerRss> {
        let resolved_addrs: Vec<SocketAddr> = tokio::net::lookup_host(rss_addr)
            .await
            .expect("Failed to resolve RSS RPC server address")
            .collect();

        assert!(!resolved_addrs.is_empty());
        let manager = RpcConnManagerRss::new(resolved_addrs);
        let rpc_clients_rss = Pool::builder()
            .max_size(Self::RSS_CONNECTION_POOL_SIZE)
            .min_idle(Some(2))
            .build(manager)
            .await
            .expect("Failed to build rss rpc clients pool");

        info!(
            "RSS RPC client pool initialized with {} connections.",
            Self::RSS_CONNECTION_POOL_SIZE
        );
        rpc_clients_rss
    }

    pub async fn get_rpc_client_nss(&self) -> PooledConnection<RpcConnManagerNss> {
        self.rpc_clients_nss.get().await.unwrap()
    }

    pub fn get_blob_client(&self) -> Arc<BlobClient> {
        self.blob_client.clone()
    }

    pub async fn get_rpc_client_rss(&self) -> PooledConnection<RpcConnManagerRss> {
        self.rpc_clients_rss.get().await.unwrap()
    }
}

pub struct BlobClient {
    clients_bss: Arc<Pool<RpcConnManagerBss>>,
    client_s3: S3Client,
    s3_cache_bucket: String,
    #[allow(dead_code)]
    blob_deletion_task_handle: JoinHandle<()>,
}

impl BlobClient {
    const BSS_CONNECTION_POOL_SIZE: u32 = 32;

    pub async fn new(
        bss_addr: &str,
        config: &S3CacheConfig,
        rx: Receiver<(BlobId, usize)>,
    ) -> Self {
        let resolved_addrs: Vec<SocketAddr> = tokio::net::lookup_host(bss_addr)
            .await
            .expect("Failed to resolve BSS RPC server address")
            .collect();

        assert!(!resolved_addrs.is_empty());
        let manager = RpcConnManagerBss::new(resolved_addrs);
        let clients_bss = Arc::new(
            Pool::builder()
                .max_size(Self::BSS_CONNECTION_POOL_SIZE)
                .min_idle(Some(2))
                .build(manager)
                .await
                .expect("Failed to build bss rpc clients pool"),
        );

        info!(
            "BSS RPC client pool initialized with {} connections.",
            Self::BSS_CONNECTION_POOL_SIZE
        );

        let client_s3 = if config.s3_host.ends_with("amazonaws.com") {
            let aws_config = aws_config::load_defaults(BehaviorVersion::latest()).await;
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
                if let Err(e) = Self::blob_deletion_task(clients_bss, rx).await {
                    tracing::error!("FATAL: blob deletion task error: {e}");
                }
            }
        });

        Self {
            clients_bss,
            client_s3,
            s3_cache_bucket: config.s3_bucket.clone(),
            blob_deletion_task_handle,
        }
    }

    async fn blob_deletion_task(
        clients_bss: Arc<Pool<RpcConnManagerBss>>,
        mut input: Receiver<(BlobId, usize)>,
    ) -> Result<(), RpcErrorBss> {
        while let Some((blob_id, block_numbers)) = input.recv().await {
            let deleted = stream::iter(0..block_numbers)
                .map(|block_number| {
                    let clients_bss = clients_bss.clone();
                    async move {
                        let rpc_client_bss = clients_bss.get().await.unwrap();
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
        let rpc_client_bss = self.clients_bss.get().await.unwrap();
        if block_number == 0 && body.len() < ObjectLayout::DEFAULT_BLOCK_SIZE as usize {
            return rpc_client_bss.put_blob(blob_id, block_number, body).await;
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
        assert!(res_s3.is_ok());
        res_bss
    }

    pub async fn get_blob(
        &self,
        blob_id: Uuid,
        block_number: u32,
        body: &mut Bytes,
    ) -> Result<usize, RpcErrorBss> {
        self.clients_bss
            .get()
            .await
            .unwrap()
            .get_blob(blob_id, block_number, body)
            .await
    }

    pub async fn delete_blob(&self, blob_id: Uuid, block_number: u32) -> Result<(), RpcErrorBss> {
        let s3_key = format!("{blob_id}-{block_number}");
        let rpc_client_bss = self.clients_bss.get().await.unwrap();
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
