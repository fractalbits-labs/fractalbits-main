use super::{
    blob_key, create_s3_client_wrapper, BlobStorage, BlobStorageError, S3ClientWrapper,
    S3RateLimitConfig, S3RetryConfig, SessionPrewarmingConfig, SessionPrewarmingService,
};
use crate::s3_retry;
use aws_sdk_s3::{
    error::SdkError,
    operation::{
        delete_object::{DeleteObjectError, DeleteObjectOutput},
        get_object::{GetObjectError, GetObjectOutput},
    },
};
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
    pub local_az: String,
    pub remote_az: String,
    pub local_az_host: String,
    #[allow(dead_code)] // Will be used for separate remote AZ client
    pub remote_az_host: Option<String>,
    pub local_az_port: u16,
    #[allow(dead_code)] // Will be used for separate remote AZ client
    pub remote_az_port: Option<u16>,
    pub local_az_bucket: String,
    pub remote_az_bucket: String,
    pub rate_limit_config: S3RateLimitConfig,
    pub retry_config: S3RetryConfig,
    pub prewarming_config: SessionPrewarmingConfig,
}

pub struct S3ExpressMultiAzWithTracking {
    client_s3: S3ClientWrapper,
    local_az_bucket: String,
    remote_az_bucket: String,
    data_blob_tracker: Arc<DataBlobTracker>,
    rss_client: Arc<RpcClientRss>,
    nss_client: Arc<RpcClientNss>,
    local_az: String,
    remote_az: String,
    retry_config: S3RetryConfig,
    _prewarming_service: Option<Arc<SessionPrewarmingService>>,
    _prewarming_task: Option<tokio::task::JoinHandle<()>>,
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
            "Initializing S3 Express One Zone storage with tracking for buckets: {} (local) and {} (remote) in AZ: {} (rate_limit_enabled: {}, retry_enabled: {})",
            config.local_az_bucket, config.remote_az_bucket, config.local_az, config.rate_limit_config.enabled, config.retry_config.enabled
        );

        let client_s3 = if config.local_az_host.ends_with("amazonaws.com") {
            // For real AWS S3 Express with session auth
            create_s3_client_wrapper(
                &config.local_az_host,
                config.local_az_port,
                &config.s3_region,
                false,
                &config.rate_limit_config,
            )
            .await
        } else {
            // For local minio testing - use common client creation with force_path_style=true
            create_s3_client_wrapper(
                &config.local_az_host,
                config.local_az_port,
                &config.s3_region,
                true, // force_path_style for minio
                &config.rate_limit_config,
            )
            .await
        };

        let endpoint_url = format!("{}:{}", config.local_az_host, config.local_az_port);
        info!("S3 client with tracking initialized with endpoint: {endpoint_url}");

        // Initialize session pre-warming service for AWS S3 Express
        let (prewarming_service, prewarming_task) = if config
            .local_az_host
            .ends_with("amazonaws.com")
            && config.prewarming_config.enabled
        {
            let service = Arc::new(SessionPrewarmingService::new(
                config.prewarming_config.clone(),
                client_s3.clone(),
                config.local_az_bucket.clone(),
                config.remote_az_bucket.clone(),
            ));

            let task = service.clone().start();
            info!("Session pre-warming service initialized for S3 Express with tracking");
            (Some(service), Some(task))
        } else {
            info!("Session pre-warming disabled for tracking storage (not AWS S3 Express or disabled in config)");
            (None, None)
        };

        Ok(Self {
            client_s3,
            local_az_bucket: config.local_az_bucket.clone(),
            remote_az_bucket: config.remote_az_bucket.clone(),
            data_blob_tracker,
            rss_client,
            nss_client,
            local_az: config.local_az.clone(),
            remote_az: config.remote_az.clone(),
            retry_config: config.retry_config.clone(),
            _prewarming_service: prewarming_service,
            _prewarming_task: prewarming_task,
        })
    }

    // Helper method to perform put operation with retry mode handling
    #[allow(dead_code)]
    async fn put_object_with_retry(
        &self,
        bucket: &str,
        key: &str,
        body: Bytes,
    ) -> Result<
        aws_sdk_s3::operation::put_object::PutObjectOutput,
        aws_sdk_s3::error::SdkError<aws_sdk_s3::operation::put_object::PutObjectError>,
    > {
        match self.retry_config.enabled {
            false => {
                self.client_s3
                    .put_object()
                    .await
                    .bucket(bucket)
                    .key(key)
                    .body(body.into())
                    .send()
                    .await
            }
            true => {
                s3_retry!(
                    "put_blob",
                    "s3_express_multi_az_with_tracking",
                    bucket,
                    &self.retry_config,
                    self.client_s3
                        .put_object()
                        .await
                        .bucket(bucket)
                        .key(key)
                        .body(body.clone().into())
                        .send()
                )
            }
        }
    }

    // Helper method to perform get operation with retry mode handling
    #[allow(dead_code)]
    async fn get_object_with_retry(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<GetObjectOutput, SdkError<GetObjectError>> {
        match self.retry_config.enabled {
            false => {
                self.client_s3
                    .get_object()
                    .await
                    .bucket(bucket)
                    .key(key)
                    .send()
                    .await
            }
            true => {
                s3_retry!(
                    "get_blob",
                    "s3_express_multi_az_with_tracking",
                    bucket,
                    &self.retry_config,
                    self.client_s3
                        .get_object()
                        .await
                        .bucket(bucket)
                        .key(key)
                        .send()
                )
            }
        }
    }

    // Helper method to perform delete operation with retry mode handling
    #[allow(dead_code)]
    async fn delete_object_with_retry(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<DeleteObjectOutput, SdkError<DeleteObjectError>> {
        match self.retry_config.enabled {
            false => {
                self.client_s3
                    .delete_object()
                    .await
                    .bucket(bucket)
                    .key(key)
                    .send()
                    .await
            }
            true => {
                s3_retry!(
                    "delete_blob",
                    "s3_express_multi_az_with_tracking",
                    bucket,
                    &self.retry_config,
                    self.client_s3
                        .delete_object()
                        .await
                        .bucket(bucket)
                        .key(key)
                        .send()
                )
            }
        }
    }

    /// Check current AZ status from RSS service discovery
    async fn get_local_az_status(&self) -> String {
        match self.rss_client.get_az_status(None).await {
            Ok(status_map) => status_map
                .az_status
                .get(&self.local_az)
                .unwrap_or(&"Normal".to_string())
                .clone(),
            Err(e) => {
                warn!("Failed to get AZ status: {}, defaulting to Normal", e);
                "Normal".to_string()
            }
        }
    }

    /// Set AZ status for a specific AZ in RSS service discovery
    async fn set_az_status(&self, az_id: &str, status: &str) -> Result<(), BlobStorageError> {
        self.rss_client
            .set_az_status(az_id, status, None)
            .await
            .map_err(|e| BlobStorageError::S3(format!("Failed to set AZ status: {e}")))?;

        info!("AZ status changed - {}: {}", az_id, status);
        Ok(())
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

        // Get current AZ status
        let az_status = self.get_local_az_status().await;

        let result: Result<(), BlobStorageError> = match az_status.as_str() {
            "Normal" => {
                // Normal mode: write to both buckets concurrently
                let (local_result, remote_result) = tokio::join!(
                    self.put_object_with_retry(&self.local_az_bucket, &s3_key, body.clone()),
                    self.put_object_with_retry(&self.remote_az_bucket, &s3_key, body.clone())
                );

                // Check if local write failed (critical)
                if local_result.is_err() {
                    error!("Local AZ bucket write failed: {:?}", local_result);
                    return Err(BlobStorageError::S3(
                        "Local bucket write failed".to_string(),
                    ));
                }

                // Check remote write result
                let remote_success = remote_result.is_ok();
                if !remote_success {
                    // Remote failed, switch to degraded mode and record single-copy blob
                    warn!("Remote AZ failed, switching to degraded mode");
                    self.set_az_status(&self.remote_az, "Degraded").await?;

                    // Record this blob as single-copy
                    let metadata = self.remote_az.as_bytes();
                    self.record_single_copy_blob(blob_id, block_number, metadata)
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
            "Degraded" => {
                // Degraded mode: write only to local, record as single-copy
                let local_result = s3_retry!(
                    "put_blob",
                    "s3_express_multi_az_tracking",
                    &self.local_az_bucket,
                    &self.retry_config,
                    self.client_s3
                        .put_object()
                        .await
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
                let metadata = self.remote_az.as_bytes();
                self.record_single_copy_blob(blob_id, block_number, metadata)
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
            "Resync" | "Sanitize" => {
                // During resync/sanitize, still write only to local to avoid conflicts
                warn!("Writing in {} mode, using local-only storage", az_status);

                let local_result = s3_retry!(
                    "put_blob",
                    "s3_express_multi_az_tracking",
                    &self.local_az_bucket,
                    &self.retry_config,
                    self.client_s3
                        .put_object()
                        .await
                        .bucket(&self.local_az_bucket)
                        .key(&s3_key)
                        .body(body.clone().into())
                        .send()
                );

                if let Err(e) = local_result {
                    error!("Local AZ bucket write failed in {} mode: {}", az_status, e);
                    return Err(BlobStorageError::S3(format!(
                        "Local bucket write failed: {e}"
                    )));
                }

                // Record as single-copy blob during resync/sanitize
                let metadata = self.remote_az.as_bytes();
                self.record_single_copy_blob(blob_id, block_number, metadata)
                    .await?;

                Ok(())
            }
            _ => {
                warn!(
                    "Unknown AZ status: {}, defaulting to Normal behavior",
                    az_status
                );
                // Default to Normal behavior
                let (local_result, remote_result) = tokio::join!(
                    self.put_object_with_retry(&self.local_az_bucket, &s3_key, body.clone()),
                    self.put_object_with_retry(&self.remote_az_bucket, &s3_key, body.clone())
                );

                if local_result.is_err() {
                    error!("Local AZ bucket write failed: {:?}", local_result);
                    return Err(BlobStorageError::S3(
                        "Local bucket write failed".to_string(),
                    ));
                }

                if !remote_result.is_ok() {
                    warn!("Remote AZ failed, switching to degraded mode");
                    self.set_az_status(&self.remote_az, "Degraded").await?;
                    let metadata = self.remote_az.as_bytes();
                    self.record_single_copy_blob(blob_id, block_number, metadata)
                        .await?;
                }

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
                .await
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
                .await
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
                .await
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
