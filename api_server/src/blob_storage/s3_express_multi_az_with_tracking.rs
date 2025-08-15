use super::{blob_key, create_s3_client, retry, BlobStorage, BlobStorageError};
use crate::s3_retry;
use aws_sdk_s3::Client as S3Client;
use bytes::Bytes;
use data_blob_tracking::{DataBlobTracker, DataBlobTrackingError};
use metrics::{counter, histogram};
use rpc_client_nss::RpcClientNss;
use rpc_client_rss::RpcClientRss;
use std::sync::Arc;
use std::time::Instant;
use tracing::{error, info, warn};
use uuid::Uuid;

#[derive(Clone)]
pub struct S3ExpressWithTrackingConfig {
    pub s3_region: String,
    pub az: String,
    pub local_az_host: String,
    pub local_az_port: u16,
    pub local_az_bucket: String,
    pub remote_az_bucket: String,
    #[allow(dead_code)] // Will be used for separate remote AZ client
    pub remote_az_host: Option<String>,
    #[allow(dead_code)] // Will be used for separate remote AZ client
    pub remote_az_port: Option<u16>,
    pub express_session_auth: bool,
}

pub struct S3ExpressMultiAzWithTracking {
    client_s3: S3Client,
    local_az_bucket: String,
    remote_az_bucket: String,
    data_blob_tracker: Arc<DataBlobTracker>,
    rss_client: Arc<RpcClientRss>,
    nss_client: Arc<RpcClientNss>,
    /// Key for storing AZ availability status in RSS
    az_status_key: String,
    retry_config: retry::RetryConfig,
}

#[derive(Debug, Clone)]
pub enum AzStatus {
    Normal,   // Both AZs available
    Degraded, // Remote AZ unavailable
    Resync,   // Resyncing from degraded
    Sanitize, // Cleaning up after resync
}

impl From<DataBlobTrackingError> for BlobStorageError {
    fn from(e: DataBlobTrackingError) -> Self {
        BlobStorageError::S3(format!("Data blob tracking error: {e}"))
    }
}

impl S3ExpressMultiAzWithTracking {
    pub async fn new(
        config: &S3ExpressWithTrackingConfig,
        data_blob_tracker: Arc<DataBlobTracker>,
        rss_client: Arc<RpcClientRss>,
        nss_client: Arc<RpcClientNss>,
    ) -> Result<Self, BlobStorageError> {
        info!(
            "Initializing S3 Express One Zone storage with tracking for buckets: {} (local) and {} (remote) in AZ: {}",
            config.local_az_bucket, config.remote_az_bucket, config.az
        );

        let client_s3 = if config.express_session_auth {
            // For real AWS S3 Express with session auth
            create_s3_client(
                &config.local_az_host,
                config.local_az_port,
                &config.s3_region,
                false,
            )
            .await
        } else {
            // For local minio testing - use common client creation with force_path_style=true
            create_s3_client(
                &config.local_az_host,
                config.local_az_port,
                &config.s3_region,
                true, // force_path_style for minio
            )
            .await
        };

        let endpoint_url = format!("{}:{}", config.local_az_host, config.local_az_port);
        info!(
            "S3 Express One Zone client with tracking initialized with endpoint: {} and session auth: {}",
            endpoint_url, config.express_session_auth
        );

        Ok(Self {
            client_s3,
            local_az_bucket: config.local_az_bucket.clone(),
            remote_az_bucket: config.remote_az_bucket.clone(),
            data_blob_tracker,
            rss_client,
            nss_client,
            az_status_key: format!("az_status/{}", config.az),
            retry_config: retry::RetryConfig::default(),
        })
    }

    /// Check current AZ status from RSS
    async fn get_az_status(&self) -> AzStatus {
        match self.rss_client.get(&self.az_status_key, None).await {
            Ok((_version, value)) => match value.as_str() {
                "Normal" => AzStatus::Normal,
                "Degraded" => AzStatus::Degraded,
                "Resync" => AzStatus::Resync,
                "Sanitize" => AzStatus::Sanitize,
                _ => {
                    warn!("Unknown AZ status, defaulting to Normal");
                    AzStatus::Normal
                }
            },
            Err(_) => {
                // Default to Normal if status not found
                AzStatus::Normal
            }
        }
    }

    /// Set AZ status in RSS
    async fn set_az_status(&self, status: AzStatus) -> Result<(), BlobStorageError> {
        let status_str = match status {
            AzStatus::Normal => "Normal",
            AzStatus::Degraded => "Degraded",
            AzStatus::Resync => "Resync",
            AzStatus::Sanitize => "Sanitize",
        };

        self.rss_client
            .put(1, &self.az_status_key, status_str, None)
            .await
            .map_err(|e| BlobStorageError::S3(format!("Failed to set AZ status: {e}")))?;

        info!("AZ status changed to: {:?}", status);
        Ok(())
    }

    /// Check if blob is marked as deleted
    async fn is_blob_deleted(
        &self,
        blob_id: Uuid,
        block_number: u32,
    ) -> Result<bool, BlobStorageError> {
        match self
            .data_blob_tracker
            .get_deleted_data_blob(&self.rss_client, &self.nss_client, blob_id, block_number)
            .await?
        {
            Some(_) => Ok(true),
            None => Ok(false),
        }
    }

    /// Record single-copy blob
    async fn record_single_copy_blob(
        &self,
        blob_id: Uuid,
        block_number: u32,
        metadata: &[u8],
    ) -> Result<(), BlobStorageError> {
        self.data_blob_tracker
            .put_single_copy_data_blob(
                &self.rss_client,
                &self.nss_client,
                blob_id,
                block_number,
                metadata,
            )
            .await?;
        Ok(())
    }

    /// Record deleted blob
    async fn record_deleted_blob(
        &self,
        blob_id: Uuid,
        block_number: u32,
    ) -> Result<(), BlobStorageError> {
        let timestamp = DataBlobTracker::current_timestamp_bytes();
        self.data_blob_tracker
            .put_deleted_data_blob(
                &self.rss_client,
                &self.nss_client,
                blob_id,
                block_number,
                &timestamp,
            )
            .await?;
        Ok(())
    }

    /// Attempt to write to remote AZ bucket and detect if it's available
    async fn try_remote_write(&self, s3_key: &str, body: Bytes) -> bool {
        let result = s3_retry!(
            "try_remote_write",
            "s3_express_multi_az_tracking",
            &self.remote_az_bucket,
            &self.retry_config,
            self.client_s3
                .put_object()
                .bucket(&self.remote_az_bucket)
                .key(s3_key)
                .body(body.clone().into())
                .send()
        );

        match result {
            Ok(_) => true,
            Err(e) => {
                warn!("Remote AZ bucket write failed: {}", e);
                false
            }
        }
    }
}

impl BlobStorage for S3ExpressMultiAzWithTracking {
    async fn put_blob(
        &self,
        blob_id: Uuid,
        block_number: u32,
        body: Bytes,
    ) -> Result<(), BlobStorageError> {
        // Record overall blob size for backward compatibility
        histogram!("blob_size", "operation" => "put", "storage" => "s3_express_multi_az_tracking")
            .record(body.len() as f64);

        let start = Instant::now();
        let local_start = Instant::now();
        let s3_key = blob_key(blob_id, block_number);

        // Check if blob is marked as deleted (should bypass write)
        if self.is_blob_deleted(blob_id, block_number).await? {
            warn!(
                "Bypassing write for deleted blob: {}:{}",
                blob_id, block_number
            );
            return Ok(());
        }

        // Get current AZ status
        let az_status = self.get_az_status().await;

        let result: Result<(), BlobStorageError> = match az_status {
            AzStatus::Normal => {
                // Normal mode: try to write to both buckets
                // Try local write first
                let local_result = s3_retry!(
                    "put_blob",
                    "s3_express_multi_az_tracking",
                    &self.local_az_bucket,
                    &self.retry_config,
                    self.client_s3
                        .put_object()
                        .bucket(&self.local_az_bucket)
                        .key(&s3_key)
                        .body(body.clone().into())
                        .send()
                );

                if local_result.is_err() {
                    error!("Local AZ bucket write failed: {:?}", local_result);
                    return Err(BlobStorageError::S3(
                        "Local bucket write failed".to_string(),
                    ));
                }

                // Try remote write
                let remote_success = self.try_remote_write(&s3_key, body.clone()).await;

                if !remote_success {
                    // Remote failed, switch to degraded mode and record single-copy blob
                    warn!("Remote AZ failed, switching to degraded mode");
                    self.set_az_status(AzStatus::Degraded).await?;

                    // Record this blob as single-copy
                    let metadata = format!("size:{}", body.len()).into_bytes();
                    self.record_single_copy_blob(blob_id, block_number, &metadata)
                        .await?;
                }

                // Record metrics
                histogram!("blob_size", "operation" => "put", "storage" => "s3_express_multi_az_tracking", "bucket_type" => "local_az")
                    .record(body.len() as f64);
                histogram!("rpc_duration_nanos", "type" => "s3_express_multi_az_tracking", "name" => "put_blob", "bucket_type" => "local_az")
                    .record(local_start.elapsed().as_nanos() as f64);
                counter!("s3_express_operations_total", "operation" => "put", "bucket_type" => "local_az", "result" => "success")
                    .increment(1);

                if remote_success {
                    histogram!("blob_size", "operation" => "put", "storage" => "s3_express_multi_az_tracking", "bucket_type" => "remote_az")
                        .record(body.len() as f64);
                    counter!("s3_express_operations_total", "operation" => "put", "bucket_type" => "remote_az", "result" => "success")
                        .increment(1);
                } else {
                    counter!("s3_express_operations_total", "operation" => "put", "bucket_type" => "remote_az", "result" => "failure")
                        .increment(1);
                }

                Ok(())
            }
            AzStatus::Degraded => {
                // Degraded mode: write only to local, record as single-copy
                let local_result = s3_retry!(
                    "put_blob",
                    "s3_express_multi_az_tracking",
                    &self.local_az_bucket,
                    &self.retry_config,
                    self.client_s3
                        .put_object()
                        .bucket(&self.local_az_bucket)
                        .key(&s3_key)
                        .body(body.clone().into())
                        .send()
                );

                if let Err(e) = local_result {
                    error!("Local AZ bucket write failed in degraded mode: {}", e);
                    return Err(BlobStorageError::S3(format!(
                        "Local bucket write failed: {e}"
                    )));
                }

                // Record as single-copy blob
                let metadata = format!("size:{}", body.len()).into_bytes();
                self.record_single_copy_blob(blob_id, block_number, &metadata)
                    .await?;

                // Record metrics
                histogram!("blob_size", "operation" => "put", "storage" => "s3_express_multi_az_tracking", "bucket_type" => "local_az")
                    .record(body.len() as f64);
                histogram!("rpc_duration_nanos", "type" => "s3_express_multi_az_tracking", "name" => "put_blob", "bucket_type" => "local_az")
                    .record(local_start.elapsed().as_nanos() as f64);
                counter!("s3_express_operations_total", "operation" => "put", "bucket_type" => "local_az", "result" => "success")
                    .increment(1);
                counter!("s3_express_single_copy_blobs_total", "operation" => "put").increment(1);

                Ok(())
            }
            AzStatus::Resync | AzStatus::Sanitize => {
                // During resync/sanitize, still write only to local to avoid conflicts
                warn!(
                    "Writing in {} mode, using local-only storage",
                    match az_status {
                        AzStatus::Resync => "resync",
                        AzStatus::Sanitize => "sanitize",
                        _ => "unknown",
                    }
                );

                let local_result = s3_retry!(
                    "put_blob",
                    "s3_express_multi_az_tracking",
                    &self.local_az_bucket,
                    &self.retry_config,
                    self.client_s3
                        .put_object()
                        .bucket(&self.local_az_bucket)
                        .key(&s3_key)
                        .body(body.clone().into())
                        .send()
                );

                if let Err(e) = local_result {
                    error!(
                        "Local AZ bucket write failed in {} mode: {}",
                        match az_status {
                            AzStatus::Resync => "resync",
                            AzStatus::Sanitize => "sanitize",
                            _ => "unknown",
                        },
                        e
                    );
                    return Err(BlobStorageError::S3(format!(
                        "Local bucket write failed: {e}"
                    )));
                }

                // Record as single-copy blob during resync/sanitize
                let metadata = format!("size:{}", body.len()).into_bytes();
                self.record_single_copy_blob(blob_id, block_number, &metadata)
                    .await?;

                Ok(())
            }
        };

        result?;

        // Record overall duration for backward compatibility
        histogram!("rpc_duration_nanos", "type" => "s3_express_multi_az_tracking", "name" => "put_blob")
            .record(start.elapsed().as_nanos() as f64);

        Ok(())
    }

    async fn get_blob(
        &self,
        blob_id: Uuid,
        block_number: u32,
        body: &mut Bytes,
    ) -> Result<(), BlobStorageError> {
        let start = Instant::now();
        let s3_key = blob_key(blob_id, block_number);

        // Always read from local AZ bucket for better performance
        let response_result = s3_retry!(
            "get_blob",
            "s3_express_multi_az_tracking",
            &self.local_az_bucket,
            &self.retry_config,
            self.client_s3
                .get_object()
                .bucket(&self.local_az_bucket)
                .key(&s3_key)
                .send()
        );

        let response = match response_result {
            Ok(resp) => resp,
            Err(e) => {
                // Record failure metrics
                histogram!("rpc_duration_nanos", "type" => "s3_express_multi_az_tracking", "name" => "get_blob")
                    .record(start.elapsed().as_nanos() as f64);
                histogram!("rpc_duration_nanos", "type" => "s3_express_multi_az_tracking", "name" => "get_blob", "bucket_type" => "local_az")
                    .record(start.elapsed().as_nanos() as f64);
                counter!("s3_express_operations_total", "operation" => "get", "bucket_type" => "local_az", "result" => "failure")
                    .increment(1);
                return Err(BlobStorageError::from(e));
            }
        };

        let data_result = response.body.collect().await;
        let data = match data_result {
            Ok(d) => d,
            Err(e) => {
                // Record failure metrics
                histogram!("rpc_duration_nanos", "type" => "s3_express_multi_az_tracking", "name" => "get_blob")
                    .record(start.elapsed().as_nanos() as f64);
                histogram!("rpc_duration_nanos", "type" => "s3_express_multi_az_tracking", "name" => "get_blob", "bucket_type" => "local_az")
                    .record(start.elapsed().as_nanos() as f64);
                counter!("s3_express_operations_total", "operation" => "get", "bucket_type" => "local_az", "result" => "failure")
                    .increment(1);
                return Err(BlobStorageError::S3(e.to_string()));
            }
        };

        *body = data.into_bytes();

        // Record overall metrics for backward compatibility
        histogram!("blob_size", "operation" => "get", "storage" => "s3_express_multi_az_tracking")
            .record(body.len() as f64);
        histogram!("rpc_duration_nanos", "type" => "s3_express_multi_az_tracking", "name" => "get_blob")
            .record(start.elapsed().as_nanos() as f64);

        // Record bucket-specific metrics (always reading from local AZ bucket)
        histogram!("blob_size", "operation" => "get", "storage" => "s3_express_multi_az_tracking", "bucket_type" => "local_az")
            .record(body.len() as f64);
        histogram!("rpc_duration_nanos", "type" => "s3_express_multi_az_tracking", "name" => "get_blob", "bucket_type" => "local_az")
            .record(start.elapsed().as_nanos() as f64);
        counter!("s3_express_operations_total", "operation" => "get", "bucket_type" => "local_az", "result" => "success")
            .increment(1);

        Ok(())
    }

    async fn delete_blob(&self, blob_id: Uuid, block_number: u32) -> Result<(), BlobStorageError> {
        let start = Instant::now();
        let local_start = Instant::now();
        let remote_start = Instant::now();
        let s3_key = blob_key(blob_id, block_number);

        // Record the blob as deleted first
        self.record_deleted_blob(blob_id, block_number).await?;

        // Try to delete from single-copy tracking
        let _ = self
            .data_blob_tracker
            .delete_single_copy_data_blob(&self.rss_client, &self.nss_client, blob_id, block_number)
            .await; // Ignore errors - blob may not be in single-copy tracking

        // Delete from both buckets concurrently (best effort)
        let local_future = s3_retry!(
            "delete_blob",
            "s3_express_multi_az_tracking",
            &self.local_az_bucket,
            &self.retry_config,
            self.client_s3
                .delete_object()
                .bucket(&self.local_az_bucket)
                .key(&s3_key)
                .send()
        );

        let remote_future = s3_retry!(
            "delete_blob",
            "s3_express_multi_az_tracking",
            &self.remote_az_bucket,
            &self.retry_config,
            self.client_s3
                .delete_object()
                .bucket(&self.remote_az_bucket)
                .key(&s3_key)
                .send()
        );

        let (local_result, remote_result) =
            tokio::join!(async { local_future }, async { remote_future });

        // Record bucket-specific metrics
        let local_success = local_result.is_ok();
        let remote_success = remote_result.is_ok();

        // Record duration for each bucket operation
        histogram!("rpc_duration_nanos", "type" => "s3_express_multi_az_tracking", "name" => "delete_blob", "bucket_type" => "local_az")
            .record(local_start.elapsed().as_nanos() as f64);
        histogram!("rpc_duration_nanos", "type" => "s3_express_multi_az_tracking", "name" => "delete_blob", "bucket_type" => "remote_az")
            .record(remote_start.elapsed().as_nanos() as f64);

        // Record success/failure counters
        counter!("s3_express_operations_total", "operation" => "delete", "bucket_type" => "local_az", "result" => if local_success { "success" } else { "failure" })
            .increment(1);
        counter!("s3_express_operations_total", "operation" => "delete", "bucket_type" => "remote_az", "result" => if remote_success { "success" } else { "failure" })
            .increment(1);

        // Log errors but don't fail - deletion is recorded for tracking
        if let Err(e) = &local_result {
            warn!(
                "Failed to delete from local AZ bucket {}: {}",
                self.local_az_bucket, e
            );
        }
        if let Err(e) = &remote_result {
            warn!(
                "Failed to delete from remote AZ bucket {}: {}",
                self.remote_az_bucket, e
            );
        }

        // Record overall duration for backward compatibility
        histogram!("rpc_duration_nanos", "type" => "s3_express_multi_az_tracking", "name" => "delete_blob")
            .record(start.elapsed().as_nanos() as f64);

        Ok(())
    }
}
