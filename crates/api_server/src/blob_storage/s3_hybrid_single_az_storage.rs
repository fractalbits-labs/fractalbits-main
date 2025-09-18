use super::{BlobLocation, BlobStorageError, DataVgProxy, blob_key, create_s3_client};
use crate::{config::S3HybridSingleAzConfig, object_layout::ObjectLayout};
use aws_sdk_s3::Client as S3Client;
use bytes::Bytes;
use data_types::DataBlobGuid;
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
    data_blob_in_s3_bucket: String,
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
            data_blob_in_s3_bucket: s3_hybrid_config.s3_bucket.clone(),
        })
    }

    pub fn create_data_blob_guid(&self) -> DataBlobGuid {
        self.data_vg_proxy.create_data_blob_guid()
    }
}

impl S3HybridSingleAzStorage {
    pub async fn put_blob(
        &self,
        blob_id: Uuid,
        volume_id: u16,
        block_number: u32,
        body: Bytes,
    ) -> Result<(), BlobStorageError> {
        histogram!("blob_size", "operation" => "put").record(body.len() as f64);
        let start = Instant::now();

        // Create BlobGuid with provided volume_id
        let blob_guid = DataBlobGuid { blob_id, volume_id };

        // Determine location based on size (single block and small size)
        let is_small = block_number == 0 && body.len() < ObjectLayout::DEFAULT_BLOCK_SIZE as usize;

        if is_small {
            // Small blob - only store in DataVgProxy
            self.data_vg_proxy
                .put_blob(blob_guid, block_number, body)
                .await?;
        } else {
            // Large blob - only store in S3
            let s3_key = blob_key(blob_id, block_number);
            self.client_s3
                .put_object()
                .bucket(&self.data_blob_in_s3_bucket)
                .key(&s3_key)
                .body(body.into())
                .send()
                .await
                .map_err(|e| BlobStorageError::S3(e.to_string()))?;

            histogram!("rpc_duration_nanos", "type" => "s3", "name" => "put_blob_s3")
                .record(start.elapsed().as_nanos() as f64);
        }

        Ok(())
    }

    pub async fn get_blob(
        &self,
        blob_guid: DataBlobGuid,
        block_number: u32,
        location: BlobLocation,
        body: &mut Bytes,
    ) -> Result<(), BlobStorageError> {
        match location {
            BlobLocation::DataVgProxy => {
                // Small blob - get from DataVgProxy
                self.data_vg_proxy
                    .get_blob(blob_guid, block_number, body)
                    .await?;
            }
            BlobLocation::S3 => {
                // Large blob - get from S3
                let s3_key = blob_key(blob_guid.blob_id, block_number);
                let result = self
                    .client_s3
                    .get_object()
                    .bucket(&self.data_blob_in_s3_bucket)
                    .key(&s3_key)
                    .send()
                    .await
                    .map_err(|e| BlobStorageError::S3(e.to_string()))?;

                let bytes = result
                    .body
                    .collect()
                    .await
                    .map_err(|e| BlobStorageError::S3(e.to_string()))?
                    .into_bytes();

                *body = bytes;
            }
        }

        histogram!("blob_size", "operation" => "get").record(body.len() as f64);
        Ok(())
    }

    pub async fn delete_blob(
        &self,
        blob_guid: DataBlobGuid,
        block_number: u32,
        location: BlobLocation,
    ) -> Result<(), BlobStorageError> {
        match location {
            BlobLocation::DataVgProxy => {
                // Small blob - delete from DataVgProxy
                self.data_vg_proxy
                    .delete_blob(blob_guid, block_number)
                    .await?;
            }
            BlobLocation::S3 => {
                // Large blob - delete from S3
                let s3_key = blob_key(blob_guid.blob_id, block_number);
                self.client_s3
                    .delete_object()
                    .bucket(&self.data_blob_in_s3_bucket)
                    .key(&s3_key)
                    .send()
                    .await
                    .map_err(|e| {
                        tracing::warn!("delete {s3_key} failed: {e}");
                        BlobStorageError::S3(e.to_string())
                    })?;
            }
        }

        Ok(())
    }
}
