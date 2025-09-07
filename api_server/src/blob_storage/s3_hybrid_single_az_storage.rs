use super::{BlobStorage, BlobStorageError, DataBlobGuid, DataVgProxy, blob_key, create_s3_client};
use crate::{config::S3HybridSingleAzConfig, object_layout::ObjectLayout};
use aws_sdk_s3::Client as S3Client;
use bytes::Bytes;
use metrics::histogram;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tracing::info;
use uuid::Uuid;

pub struct S3HybridSingleAzStorage {
    data_vg_proxy: Arc<DataVgProxy>,
    client_s3: S3Client,
    s3_cache_bucket: String,
}

impl S3HybridSingleAzStorage {
    pub async fn new(
        rss_client: Arc<rpc_client_rss::RpcClientRss>,
        s3_hybrid_config: &S3HybridSingleAzConfig,
        rpc_timeout: Duration,
    ) -> Result<Self, BlobStorageError> {
        info!("Fetching DataVg configuration from RSS...");

        // Fetch DataVg configuration from RSS
        let data_vg_info = rss_client
            .get_data_vg_info(Some(rpc_timeout))
            .await
            .map_err(|e| {
                BlobStorageError::Config(format!("Failed to fetch DataVg config: {}", e))
            })?;

        info!(
            "Initializing DataVgProxy with {} volumes",
            data_vg_info.volumes.len()
        );

        // Initialize DataVgProxy with the fetched configuration
        let data_vg_proxy = Arc::new(DataVgProxy::new(data_vg_info, rpc_timeout).await.map_err(
            |e| BlobStorageError::Config(format!("Failed to initialize DataVgProxy: {}", e)),
        )?);

        let client_s3 = create_s3_client(
            &s3_hybrid_config.s3_host,
            s3_hybrid_config.s3_port,
            &s3_hybrid_config.s3_region,
            false, // force_path_style not needed for hybrid storage
        )
        .await;

        Ok(Self {
            data_vg_proxy,
            client_s3,
            s3_cache_bucket: s3_hybrid_config.s3_bucket.clone(),
        })
    }

    pub fn create_data_blob_guid(&self) -> DataBlobGuid {
        self.data_vg_proxy.create_data_blob_guid()
    }
}

impl BlobStorage for S3HybridSingleAzStorage {
    async fn put_blob(
        &self,
        _tracking_root_blob_name: Option<&str>,
        blob_id: Uuid,
        volume_id: u32,
        block_number: u32,
        body: Bytes,
    ) -> Result<DataBlobGuid, BlobStorageError> {
        histogram!("blob_size", "operation" => "put").record(body.len() as f64);
        let start = Instant::now();

        // Create BlobGuid with provided volume_id
        let blob_guid = DataBlobGuid { blob_id, volume_id };

        if block_number == 0 && body.len() < ObjectLayout::DEFAULT_BLOCK_SIZE as usize {
            // Small blob - only store in BSS via DataVgProxy
            self.data_vg_proxy
                .put_blob(blob_guid, block_number, body)
                .await?;
            return Ok(blob_guid);
        }

        // Large blob - store in both S3 and BSS
        let s3_key = blob_key(blob_id, block_number);
        let s3_fut = self
            .client_s3
            .put_object()
            .bucket(&self.s3_cache_bucket)
            .key(&s3_key)
            .body(body.clone().into())
            .send();

        let bss_fut = self.data_vg_proxy.put_blob(blob_guid, block_number, body);

        let (res_s3, res_bss) = tokio::join!(s3_fut, bss_fut);

        histogram!("rpc_duration_nanos", "type"  => "bss_s3_join",  "name" => "put_blob_join_with_s3")
            .record(start.elapsed().as_nanos() as f64);

        res_s3.map_err(|e| BlobStorageError::S3(e.to_string()))?;
        res_bss?;

        Ok(blob_guid)
    }

    async fn get_blob(
        &self,
        blob_guid: DataBlobGuid,
        block_number: u32,
        body: &mut Bytes,
    ) -> Result<(), BlobStorageError> {
        self.data_vg_proxy
            .get_blob(blob_guid, block_number, body)
            .await?;

        histogram!("blob_size", "operation" => "get").record(body.len() as f64);
        Ok(())
    }

    async fn delete_blob(
        &self,
        _tracking_root_blob_name: Option<&str>,
        blob_guid: DataBlobGuid,
        block_number: u32,
    ) -> Result<(), BlobStorageError> {
        let s3_key = blob_key(blob_guid.blob_id, block_number);
        let s3_fut = self
            .client_s3
            .delete_object()
            .bucket(&self.s3_cache_bucket)
            .key(&s3_key)
            .send();
        let bss_fut =
            self.data_vg_proxy
                .delete_blob(_tracking_root_blob_name, blob_guid, block_number);
        let (res_s3, res_bss) = tokio::join!(s3_fut, bss_fut);

        if let Err(e) = res_s3 {
            tracing::warn!("delete {s3_key} failed: {e}");
        }

        res_bss?;
        Ok(())
    }
}
