pub mod config;
pub mod handler;
mod object_layout;

use aws_sdk_s3::{
    config::{BehaviorVersion, Credentials, Region},
    Client as S3Client, Config as S3Config,
};
use axum::extract::FromRef;
use bytes::Bytes;
use config::ArcConfig;
use futures::stream::{self, StreamExt};
use object_layout::ObjectLayout;
use rpc_client_bss::{RpcClientBss, RpcErrorBss};
use rpc_client_nss::RpcClientNss;
use rpc_client_rss::{ArcRpcClientRss, RpcClientRss};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::mpsc::{self, Receiver, Sender};
use uuid::Uuid;

pub type BlobId = uuid::Uuid;

pub struct AppState {
    pub config: ArcConfig,

    pub rpc_clients_nss: Vec<RpcClientNss>,

    pub blob_clients: Vec<Arc<BlobClient>>,
    pub blob_deletion: Sender<(BlobId, usize)>,

    pub rpc_client_rss: ArcRpcClientRss,
}

impl FromRef<Arc<AppState>> for ArcConfig {
    fn from_ref(state: &Arc<AppState>) -> Self {
        Self(state.config.0.clone())
    }
}

impl AppState {
    const MAX_NSS_CONNECTION: usize = 8;
    const MAX_BLOB_IO_CONNECTION: usize = 8;

    pub async fn new(config: ArcConfig) -> Self {
        let mut rpc_clients_nss = Vec::with_capacity(Self::MAX_NSS_CONNECTION);
        for _i in 0..AppState::MAX_NSS_CONNECTION {
            let rpc_client_nss = RpcClientNss::new(&config.nss_addr)
                .await
                .expect("rpc client nss failure");
            rpc_clients_nss.push(rpc_client_nss);
        }

        let mut blob_clients = Vec::with_capacity(Self::MAX_BLOB_IO_CONNECTION);
        for _i in 0..AppState::MAX_BLOB_IO_CONNECTION {
            let blob_client = BlobClient::new(&config.bss_addr).await;
            blob_clients.push(Arc::new(blob_client));
        }

        let (tx, rx) = mpsc::channel(1024 * 1024);
        let bss_addr = config.bss_addr.clone();
        tokio::spawn(async move {
            if let Err(e) = Self::blob_deletion_task(&bss_addr, rx).await {
                tracing::error!("FATAL: blob deletion task error: {e}");
            }
        });

        let rpc_client_rss = ArcRpcClientRss(Arc::new(
            RpcClientRss::new(&config.rss_addr)
                .await
                .expect("rpc client rss failure"),
        ));
        Self {
            config,
            rpc_clients_nss,
            blob_clients,
            blob_deletion: tx,
            rpc_client_rss,
        }
    }

    pub fn get_rpc_client_nss(&self, addr: SocketAddr) -> &RpcClientNss {
        let hash = Self::calculate_hash(&addr) % Self::MAX_NSS_CONNECTION;
        &self.rpc_clients_nss[hash]
    }

    pub fn get_blob_client(&self, addr: SocketAddr) -> Arc<BlobClient> {
        let hash = Self::calculate_hash(&addr) % Self::MAX_BLOB_IO_CONNECTION;
        self.blob_clients[hash].clone()
    }

    pub fn get_rpc_client_rss(&self) -> ArcRpcClientRss {
        ArcRpcClientRss(Arc::clone(&self.rpc_client_rss.0))
    }

    #[inline]
    fn calculate_hash<T: Hash>(t: &T) -> usize {
        let mut s = DefaultHasher::new();
        t.hash(&mut s);
        s.finish() as usize
    }

    async fn blob_deletion_task(
        bss_ip: &str,
        mut input: Receiver<(BlobId, usize)>,
    ) -> Result<(), RpcErrorBss> {
        let rpc_client_bss = &RpcClientBss::new(bss_ip).await?;
        while let Some((blob_id, block_numbers)) = input.recv().await {
            let deleted = stream::iter(0..block_numbers)
                .map(|block_number| async move {
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
}

pub struct BlobClient {
    pub client_bss: RpcClientBss,
    pub client_s3: S3Client,
}

impl BlobClient {
    pub async fn new(bss_url: &str) -> Self {
        let client_bss = RpcClientBss::new(bss_url)
            .await
            .expect("rpc client bss failure");

        let credentials = Credentials::new("minioadmin", "minioadmin", None, None, "minio");
        let s3_config = S3Config::builder()
            .endpoint_url("http://127.0.0.1:9000")
            .region(Region::from_static("us-east-1"))
            .credentials_provider(credentials)
            .behavior_version(BehaviorVersion::v2024_03_28())
            .build();

        let client_s3 = S3Client::from_conf(s3_config);
        Self {
            client_bss,
            client_s3,
        }
    }

    pub async fn put_blob(
        &self,
        blob_id: Uuid,
        block_number: u32,
        body: Bytes,
    ) -> Result<usize, RpcErrorBss> {
        if block_number == 0 && body.len() < ObjectLayout::DEFAULT_BLOCK_SIZE as usize {
            return self.client_bss.put_blob(blob_id, block_number, body).await;
        }

        let s3_key = format!("{blob_id}-{block_number}");
        let (res_s3, res_bss) = tokio::join!(
            self.client_s3
                .put_object()
                .bucket("fractalbits-bucket")
                .key(s3_key)
                .body(body.clone().into())
                .send(),
            self.client_bss.put_blob(blob_id, block_number, body)
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
        self.client_bss.get_blob(blob_id, block_number, body).await
    }

    pub async fn delete_blob(&self, blob_id: Uuid, block_number: u32) -> Result<(), RpcErrorBss> {
        let s3_key = format!("{blob_id}-{block_number}");
        let (res_s3, res_bss) = tokio::join!(
            self.client_s3
                .delete_object()
                .bucket("fractalbits-bucket")
                .key(&s3_key)
                .send(),
            self.client_bss.delete_blob(blob_id, block_number)
        );
        if let Err(e) = res_s3 {
            // note this blob may not be uploaded to s3 yet
            tracing::warn!("delete {s3_key} failed: {e}");
        }
        res_bss
    }
}
