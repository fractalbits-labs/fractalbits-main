use super::{blob_key, BlobStorage, BlobStorageError};
use crate::{config::S3CacheConfig, object_layout::ObjectLayout};
use aws_config::BehaviorVersion;
use aws_sdk_s3::{
    config::{Credentials, Region},
    Client as S3Client, Config as S3Config,
};
use bytes::Bytes;
use metrics::histogram;
use rpc_client_bss::RpcClientBss;
use rpc_client_common::{bss_rpc_retry, rpc_retry};
use slotmap_conn_pool::{ConnPool, Poolable};
use std::{
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};
use tracing::info;
use uuid::Uuid;

pub struct HybridStorage {
    rpc_clients_bss: ConnPool<Arc<RpcClientBss>, SocketAddr>,
    client_s3: S3Client,
    s3_cache_bucket: String,
    bss_addr: SocketAddr,
    rpc_timeout: Duration,
}

impl HybridStorage {
    pub async fn new(
        bss_addr: SocketAddr,
        bss_conn_num: u16,
        s3_cache_config: &S3CacheConfig,
        rpc_timeout: Duration,
    ) -> Self {
        let clients_bss = ConnPool::new();

        for i in 0..bss_conn_num as usize {
            info!(
                "Connecting to BSS server at {bss_addr} (connection {}/{})",
                i + 1,
                bss_conn_num
            );
            let client = Arc::new(
                <RpcClientBss as slotmap_conn_pool::Poolable>::new(bss_addr)
                    .await
                    .unwrap(),
            );
            clients_bss.pooled(bss_addr, client);
        }

        info!("BSS RPC client pool initialized with {bss_conn_num} connections.");

        let client_s3 = if s3_cache_config.s3_host.ends_with("amazonaws.com") {
            let aws_config = aws_config::defaults(BehaviorVersion::latest())
                .region(Region::new(s3_cache_config.s3_region.clone()))
                .load()
                .await;
            S3Client::new(&aws_config)
        } else {
            let credentials = Credentials::new("minioadmin", "minioadmin", None, None, "s3_cache");
            let s3_config = S3Config::builder()
                .endpoint_url(format!(
                    "{}:{}",
                    s3_cache_config.s3_host, s3_cache_config.s3_port
                ))
                .region(Region::new(s3_cache_config.s3_region.clone()))
                .credentials_provider(credentials)
                .behavior_version(BehaviorVersion::latest())
                .build();

            S3Client::from_conf(s3_config)
        };

        Self {
            rpc_clients_bss: clients_bss,
            client_s3,
            s3_cache_bucket: s3_cache_config.s3_bucket.clone(),
            bss_addr,
            rpc_timeout,
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
}

impl BlobStorage for HybridStorage {
    async fn put_blob(
        &self,
        blob_id: Uuid,
        block_number: u32,
        body: Bytes,
    ) -> Result<(), BlobStorageError> {
        histogram!("blob_size", "operation" => "put").record(body.len() as f64);
        let start = Instant::now();

        if block_number == 0 && body.len() < ObjectLayout::DEFAULT_BLOCK_SIZE as usize {
            bss_rpc_retry!(
                self,
                put_blob(blob_id, block_number, body.clone(), Some(self.rpc_timeout))
            )
            .await?;
            return Ok(());
        }

        let s3_key = blob_key(blob_id, block_number);
        let s3_fut = self
            .client_s3
            .put_object()
            .bucket(&self.s3_cache_bucket)
            .key(&s3_key)
            .body(body.clone().into())
            .send();
        let bss_fut = bss_rpc_retry!(
            self,
            put_blob(blob_id, block_number, body.clone(), Some(self.rpc_timeout))
        );
        let (res_s3, res_bss) = tokio::join!(s3_fut, bss_fut);

        histogram!("rpc_duration_nanos", "type"  => "bss_s3_join",  "name" => "put_blob_join_with_s3")
            .record(start.elapsed().as_nanos() as f64);

        assert!(res_s3.is_ok());
        res_bss?;

        Ok(())
    }

    async fn get_blob(
        &self,
        blob_id: Uuid,
        block_number: u32,
        body: &mut Bytes,
    ) -> Result<(), BlobStorageError> {
        bss_rpc_retry!(
            self,
            get_blob(blob_id, block_number, body, Some(self.rpc_timeout))
        )
        .await?;

        histogram!("blob_size", "operation" => "get").record(body.len() as f64);
        Ok(())
    }

    async fn delete_blob(&self, blob_id: Uuid, block_number: u32) -> Result<(), BlobStorageError> {
        let s3_key = blob_key(blob_id, block_number);
        let s3_fut = self
            .client_s3
            .delete_object()
            .bucket(&self.s3_cache_bucket)
            .key(&s3_key)
            .send();
        let bss_fut = bss_rpc_retry!(
            self,
            delete_blob(blob_id, block_number, Some(self.rpc_timeout))
        );
        let (res_s3, res_bss) = tokio::join!(s3_fut, bss_fut);

        if let Err(e) = res_s3 {
            tracing::warn!("delete {s3_key} failed: {e}");
        }

        res_bss?;
        Ok(())
    }
}
