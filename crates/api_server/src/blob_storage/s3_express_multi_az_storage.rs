use super::{
    BlobStorageError, S3ClientWrapper, S3RetryConfig, blob_key, chunks_to_bytestream,
    create_s3_client_wrapper,
};
use crate::s3_retry;
use bytes::Bytes;
use data_blob_tracking::{DataBlobTracker, DataBlobTrackingError};
use data_types::DataBlobGuid;
use metrics_wrapper::{counter, histogram};
use moka::future::Cache;
use std::sync::Arc;
use std::time::Instant;
use tracing::{error, info, warn};
use uuid::Uuid;

pub struct S3ExpressMultiAzStorage {
    local_client: S3ClientWrapper,
    remote_client: S3ClientWrapper,
    local_az_bucket: String,
    remote_az_bucket: String,
    data_blob_tracker: Arc<DataBlobTracker>,
    local_az: String,
    remote_az: String,
    retry_config: S3RetryConfig,
    az_status_cache: Arc<Cache<String, String>>,
}

impl From<DataBlobTrackingError> for BlobStorageError {
    fn from(e: DataBlobTrackingError) -> Self {
        BlobStorageError::S3(format!("Data blob tracking error: {e}"))
    }
}

impl S3ExpressMultiAzStorage {
    pub async fn new(
        config: &crate::config::S3ExpressMultiAzConfig,
        data_blob_tracker: Arc<DataBlobTracker>,
        az_status_cache: Arc<Cache<String, String>>,
    ) -> Result<Self, BlobStorageError> {
        info!(
            "Initializing S3 Express One Zone storage with tracking for buckets: {} (local) and {} (remote) in AZ: {} (rate_limit_enabled: {}, retry_enabled: {})",
            config.local_az_bucket,
            config.remote_az_bucket,
            config.local_az,
            config.ratelimit.enabled,
            config.retry_config.enabled
        );

        // Create local AZ S3 client
        let local_client = if config.local_az_host.ends_with("amazonaws.com") {
            // For real AWS S3 Express with session auth
            create_s3_client_wrapper(
                &config.local_az_host,
                config.local_az_port,
                &config.s3_region,
                false,
                &config.ratelimit,
            )
            .await
        } else {
            // For local minio testing - use common client creation with force_path_style=true
            create_s3_client_wrapper(
                &config.local_az_host,
                config.local_az_port,
                &config.s3_region,
                true, // force_path_style for minio
                &config.ratelimit,
            )
            .await
        };

        // Create remote AZ S3 client
        let remote_client = if config.remote_az_host.ends_with("amazonaws.com") {
            // For real AWS S3 Express with session auth
            create_s3_client_wrapper(
                &config.remote_az_host,
                config.remote_az_port,
                &config.s3_region,
                false,
                &config.ratelimit,
            )
            .await
        } else {
            // For local minio testing - use common client creation with force_path_style=true
            create_s3_client_wrapper(
                &config.remote_az_host,
                config.remote_az_port,
                &config.s3_region,
                true, // force_path_style for minio
                &config.ratelimit,
            )
            .await
        };

        let local_endpoint = format!("{}:{}", config.local_az_host, config.local_az_port);
        let remote_endpoint = format!("{}:{}", config.remote_az_host, config.remote_az_port);
        info!(
            "S3 clients with tracking initialized - local: {}, remote: {}",
            local_endpoint, remote_endpoint
        );

        Ok(Self {
            local_client,
            remote_client,
            local_az_bucket: config.local_az_bucket.clone(),
            remote_az_bucket: config.remote_az_bucket.clone(),
            data_blob_tracker,
            local_az: config.local_az.clone(),
            remote_az: config.remote_az.clone(),
            retry_config: config.retry_config.clone(),
            az_status_cache,
        })
    }

    /// Check current AZ status from RSS service discovery with caching
    async fn get_az_status(&self, az_id: &str) -> String {
        let cache_key = format!("az_status:{}", az_id);

        // Check cache first
        if let Some(cached) = self.az_status_cache.get(&cache_key).await {
            return cached;
        }

        // Cache miss - fetch from RSS
        match self.data_blob_tracker.get_az_status(None).await {
            Ok(status_map) => {
                let status = status_map
                    .az_status
                    .get(az_id)
                    .unwrap_or(&"Normal".to_string())
                    .clone();

                // Cache the result
                self.az_status_cache.insert(cache_key, status.clone()).await;

                status
            }
            Err(e) => {
                warn!("Failed to get AZ status: {}, defaulting to Normal", e);

                // Cache the default value as well to avoid repeated failures
                self.az_status_cache
                    .insert(cache_key, "Normal".to_string())
                    .await;

                "Normal".to_string()
            }
        }
    }

    /// Set AZ status for a specific AZ in RSS service discovery
    async fn set_az_status(&self, az_id: &str, status: &str) -> Result<(), BlobStorageError> {
        self.data_blob_tracker
            .set_az_status(az_id, status, None)
            .await
            .map_err(|e| BlobStorageError::S3(format!("Failed to set AZ status: {e}")))?;

        // Invalidate cache entry for this AZ
        let cache_key = format!("az_status:{}", az_id);
        self.az_status_cache.invalidate(&cache_key).await;

        info!("AZ status changed - {}: {}", az_id, status);
        Ok(())
    }

    /// Record single-copy blob
    async fn record_single_copy_blob(
        &self,
        tracking_root_blob_name: &str,
        s3_key: &str,
        metadata: &[u8],
    ) -> Result<(), BlobStorageError> {
        self.data_blob_tracker
            .put_single_copy_data_blob(tracking_root_blob_name, s3_key, metadata)
            .await?;
        Ok(())
    }

    /// Record deleted blob
    async fn record_deleted_blob(
        &self,
        tracking_root_blob_name: &str,
        s3_key: &str,
    ) -> Result<(), BlobStorageError> {
        let timestamp = DataBlobTracker::current_timestamp_bytes();
        self.data_blob_tracker
            .put_deleted_data_blob(tracking_root_blob_name, s3_key, &timestamp)
            .await?;
        Ok(())
    }

    pub fn create_data_blob_guid(&self) -> DataBlobGuid {
        DataBlobGuid {
            blob_id: Uuid::now_v7(),
            volume_id: DataBlobGuid::S3_VOLUME,
        }
    }
}

impl S3ExpressMultiAzStorage {
    pub async fn put_blob(
        &self,
        tracking_root_blob_name: Option<&str>,
        blob_id: Uuid,
        block_number: u32,
        body: Bytes,
    ) -> Result<(), BlobStorageError> {
        histogram!("blob_size", "operation" => "put", "storage" => "s3_express_multi_az")
            .record(body.len() as f64);

        let start = Instant::now();
        let s3_key = blob_key(blob_id, block_number);
        let tracking_root = tracking_root_blob_name.expect("No tracking_root_blob_name provided");
        assert!(!tracking_root.is_empty());

        // Use tracking logic when tracking_root_blob_name is provided
        let local_az_status = self.get_az_status(&self.local_az).await;
        let remote_az_status = self.get_az_status(&self.remote_az).await;

        match (local_az_status.as_str(), remote_az_status.as_str()) {
            ("Normal", "Normal") => {
                let local_future = async {
                    let start = Instant::now();
                    let result = s3_retry!(
                        "put_blob",
                        "s3_express_multi_az",
                        &self.local_az_bucket,
                        &self.retry_config,
                        self.local_client
                            .put_object()
                            .await
                            .bucket(&self.local_az_bucket)
                            .key(&s3_key)
                            .body(aws_sdk_s3::primitives::ByteStream::from(body.clone()))
                            .send()
                    );
                    (result, start.elapsed())
                };

                let remote_future = async {
                    let start = Instant::now();
                    let result = s3_retry!(
                        "put_blob",
                        "s3_express_multi_az",
                        &self.remote_az_bucket,
                        &self.retry_config,
                        self.remote_client
                            .put_object()
                            .await
                            .bucket(&self.remote_az_bucket)
                            .key(&s3_key)
                            .body(aws_sdk_s3::primitives::ByteStream::from(body.clone()))
                            .send()
                    );
                    (result, start.elapsed())
                };

                let ((local_result, local_duration), (remote_result, remote_duration)) =
                    tokio::join!(local_future, remote_future);

                // Record bucket-specific metrics
                let _local_success = local_result.is_ok();
                let _remote_success = remote_result.is_ok();

                // Record duration for each bucket operation
                histogram!("rpc_duration_nanos", "type" => "s3_express_multi_az", "name" => "put_blob", "bucket_type" => "local_az")
                    .record(local_duration.as_nanos() as f64);
                histogram!("rpc_duration_nanos", "type" => "s3_express_multi_az", "name" => "put_blob", "bucket_type" => "remote_az")
                    .record(remote_duration.as_nanos() as f64);

                // Record success/failure counters
                counter!("s3_express_operations_total", "operation" => "put", "bucket_type" => "local_az", "result" => if _local_success { "success" } else { "failure" })
                    .increment(1);
                counter!("s3_express_operations_total", "operation" => "put", "bucket_type" => "remote_az", "result" => if _remote_success { "success" } else { "failure" })
                    .increment(1);

                // Record bucket-specific blob sizes
                histogram!("blob_size", "operation" => "put", "storage" => "s3_express_multi_az", "bucket_type" => "local_az")
                    .record(body.len() as f64);
                histogram!("blob_size", "operation" => "put", "storage" => "s3_express_multi_az", "bucket_type" => "remote_az")
                    .record(body.len() as f64);

                local_result?;

                if !_remote_success {
                    warn!("Remote AZ failed, switching to degraded mode");
                    self.set_az_status(&self.remote_az, "Degraded").await?;
                    let metadata = self.remote_az.as_bytes();
                    self.record_single_copy_blob(tracking_root, &s3_key, metadata)
                        .await?;
                }
            }
            ("Normal", "Degraded") | ("Normal", "Resync") | ("Normal", "Sanitize") => {
                let local_start = Instant::now();
                let local_result = s3_retry!(
                    "put_blob",
                    "s3_express_multi_az",
                    &self.local_az_bucket,
                    &self.retry_config,
                    self.local_client
                        .put_object()
                        .await
                        .bucket(&self.local_az_bucket)
                        .key(&s3_key)
                        .body(body.clone().into())
                        .send()
                );
                let local_duration = local_start.elapsed();

                // Record metrics
                histogram!("blob_size", "operation" => "put", "storage" => "s3_express_multi_az", "bucket_type" => "local_az")
                    .record(body.len() as f64);
                histogram!("rpc_duration_nanos", "type" => "s3_express_multi_az", "name" => "put_blob", "bucket_type" => "local_az")
                    .record(local_duration.as_nanos() as f64);

                if let Err(e) = local_result {
                    counter!("s3_express_operations_total", "operation" => "put", "bucket_type" => "local_az", "result" => "failure")
                        .increment(1);
                    error!("Local AZ bucket write failed in degraded mode: {}", e);
                    return Err(BlobStorageError::S3(format!(
                        "Local bucket write failed: {e}"
                    )));
                }

                counter!("s3_express_operations_total", "operation" => "put", "bucket_type" => "local_az", "result" => "success")
                    .increment(1);
                counter!("s3_express_single_copy_blobs_total", "operation" => "put").increment(1);

                let metadata = self.remote_az.as_bytes();
                self.record_single_copy_blob(tracking_root, &s3_key, metadata)
                    .await?;
            }
            _ => {
                warn!(
                    "Unknown AZ status combination: local={}, remote={}, defaulting to Normal behavior",
                    local_az_status, remote_az_status
                );
                let local_start = Instant::now();
                let remote_start = Instant::now();

                let local_result = s3_retry!(
                    "put_blob",
                    "s3_express_multi_az",
                    &self.local_az_bucket,
                    &self.retry_config,
                    self.local_client
                        .put_object()
                        .await
                        .bucket(&self.local_az_bucket)
                        .key(&s3_key)
                        .body(aws_sdk_s3::primitives::ByteStream::from(body.clone()))
                        .send()
                );
                let local_duration = local_start.elapsed();

                let remote_result = s3_retry!(
                    "put_blob",
                    "s3_express_multi_az",
                    &self.remote_az_bucket,
                    &self.retry_config,
                    self.remote_client
                        .put_object()
                        .await
                        .bucket(&self.remote_az_bucket)
                        .key(&s3_key)
                        .body(aws_sdk_s3::primitives::ByteStream::from(body.clone()))
                        .send()
                );
                let remote_duration = remote_start.elapsed();

                // Record metrics for both operations
                let _local_success = local_result.is_ok();
                let _remote_success = remote_result.is_ok();

                histogram!("rpc_duration_nanos", "type" => "s3_express_multi_az", "name" => "put_blob", "bucket_type" => "local_az")
                    .record(local_duration.as_nanos() as f64);
                histogram!("rpc_duration_nanos", "type" => "s3_express_multi_az", "name" => "put_blob", "bucket_type" => "remote_az")
                    .record(remote_duration.as_nanos() as f64);

                counter!("s3_express_operations_total", "operation" => "put", "bucket_type" => "local_az", "result" => if _local_success { "success" } else { "failure" })
                    .increment(1);
                counter!("s3_express_operations_total", "operation" => "put", "bucket_type" => "remote_az", "result" => if _remote_success { "success" } else { "failure" })
                    .increment(1);

                histogram!("blob_size", "operation" => "put", "storage" => "s3_express_multi_az", "bucket_type" => "local_az")
                    .record(body.len() as f64);
                histogram!("blob_size", "operation" => "put", "storage" => "s3_express_multi_az", "bucket_type" => "remote_az")
                    .record(body.len() as f64);

                local_result?;
                let _ = remote_result;
            }
        }

        // Record overall duration for backward compatibility
        histogram!("rpc_duration_nanos", "type" => "s3_express_multi_az", "name" => "put_blob")
            .record(start.elapsed().as_nanos() as f64);

        Ok(())
    }

    pub async fn put_blob_vectored(
        &self,
        tracking_root_blob_name: Option<&str>,
        blob_id: Uuid,
        block_number: u32,
        chunks: Vec<Bytes>,
    ) -> Result<(), BlobStorageError> {
        let total_len: usize = chunks.iter().map(|c| c.len()).sum();
        histogram!("blob_size", "operation" => "put", "storage" => "s3_express_multi_az")
            .record(total_len as f64);

        let start = Instant::now();
        let s3_key = blob_key(blob_id, block_number);
        let tracking_root = tracking_root_blob_name.expect("No tracking_root_blob_name provided");
        assert!(!tracking_root.is_empty());

        let local_az_status = self.get_az_status(&self.local_az).await;
        let remote_az_status = self.get_az_status(&self.remote_az).await;

        match (local_az_status.as_str(), remote_az_status.as_str()) {
            ("Normal", "Normal") => {
                let local_chunks = chunks.clone();
                let remote_chunks = chunks;

                let local_future = async {
                    let start = Instant::now();
                    let result = s3_retry!(
                        "put_blob_vectored",
                        "s3_express_multi_az",
                        &self.local_az_bucket,
                        &self.retry_config,
                        self.local_client
                            .put_object()
                            .await
                            .bucket(&self.local_az_bucket)
                            .key(&s3_key)
                            .body(chunks_to_bytestream(local_chunks.clone()))
                            .send()
                    );
                    (result, start.elapsed())
                };

                let remote_future = async {
                    let start = Instant::now();
                    let result = s3_retry!(
                        "put_blob_vectored",
                        "s3_express_multi_az",
                        &self.remote_az_bucket,
                        &self.retry_config,
                        self.remote_client
                            .put_object()
                            .await
                            .bucket(&self.remote_az_bucket)
                            .key(&s3_key)
                            .body(chunks_to_bytestream(remote_chunks.clone()))
                            .send()
                    );
                    (result, start.elapsed())
                };

                let ((local_result, local_duration), (remote_result, remote_duration)) =
                    tokio::join!(local_future, remote_future);

                let _local_success = local_result.is_ok();
                let _remote_success = remote_result.is_ok();

                histogram!("rpc_duration_nanos", "type" => "s3_express_multi_az", "name" => "put_blob_vectored", "bucket_type" => "local_az")
                    .record(local_duration.as_nanos() as f64);
                histogram!("rpc_duration_nanos", "type" => "s3_express_multi_az", "name" => "put_blob_vectored", "bucket_type" => "remote_az")
                    .record(remote_duration.as_nanos() as f64);

                counter!("s3_express_operations_total", "operation" => "put", "bucket_type" => "local_az", "result" => if _local_success { "success" } else { "failure" })
                    .increment(1);
                counter!("s3_express_operations_total", "operation" => "put", "bucket_type" => "remote_az", "result" => if _remote_success { "success" } else { "failure" })
                    .increment(1);

                histogram!("blob_size", "operation" => "put", "storage" => "s3_express_multi_az", "bucket_type" => "local_az")
                    .record(total_len as f64);
                histogram!("blob_size", "operation" => "put", "storage" => "s3_express_multi_az", "bucket_type" => "remote_az")
                    .record(total_len as f64);

                local_result?;

                if !_remote_success {
                    warn!("Remote AZ failed, switching to degraded mode");
                    self.set_az_status(&self.remote_az, "Degraded").await?;
                    let metadata = self.remote_az.as_bytes();
                    self.record_single_copy_blob(tracking_root, &s3_key, metadata)
                        .await?;
                }
            }
            ("Normal", "Degraded") | ("Normal", "Resync") | ("Normal", "Sanitize") => {
                let local_start = Instant::now();
                let local_result = s3_retry!(
                    "put_blob_vectored",
                    "s3_express_multi_az",
                    &self.local_az_bucket,
                    &self.retry_config,
                    self.local_client
                        .put_object()
                        .await
                        .bucket(&self.local_az_bucket)
                        .key(&s3_key)
                        .body(chunks_to_bytestream(chunks.clone()))
                        .send()
                );

                histogram!("rpc_duration_nanos", "type" => "s3_express_multi_az", "name" => "put_blob_vectored", "bucket_type" => "local_az")
                    .record(local_start.elapsed().as_nanos() as f64);
                counter!("s3_express_operations_total", "operation" => "put", "bucket_type" => "local_az", "result" => if local_result.is_ok() { "success" } else { "failure" })
                    .increment(1);
                histogram!("blob_size", "operation" => "put", "storage" => "s3_express_multi_az", "bucket_type" => "local_az")
                    .record(total_len as f64);

                local_result?;

                let metadata = self.remote_az.as_bytes();
                self.record_single_copy_blob(tracking_root, &s3_key, metadata)
                    .await?;
            }
            _ => {
                error!(
                    "Cannot write blob in current AZ state: local={}, remote={}",
                    local_az_status, remote_az_status
                );
                return Err(BlobStorageError::S3(format!(
                    "Both AZs unavailable: local={}, remote={}",
                    local_az_status, remote_az_status
                )));
            }
        }

        histogram!("rpc_duration_nanos", "type" => "s3_express_multi_az", "name" => "put_blob_vectored")
            .record(start.elapsed().as_nanos() as f64);

        Ok(())
    }

    pub async fn get_blob(
        &self,
        blob_guid: DataBlobGuid,
        block_number: u32,
        body: &mut Bytes,
    ) -> Result<(), BlobStorageError> {
        let start = Instant::now();
        let s3_key = blob_key(blob_guid.blob_id, block_number);
        assert_eq!(DataBlobGuid::S3_VOLUME, blob_guid.volume_id);

        // Always read from local AZ bucket for better performance
        let response_result = s3_retry!(
            "get_blob",
            "s3_express_multi_az",
            &self.local_az_bucket,
            &self.retry_config,
            self.local_client
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
                histogram!("rpc_duration_nanos", "type" => "s3_express_multi_az", "name" => "get_blob")
                    .record(start.elapsed().as_nanos() as f64);
                histogram!("rpc_duration_nanos", "type" => "s3_express_multi_az", "name" => "get_blob", "bucket_type" => "local_az")
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
                histogram!("rpc_duration_nanos", "type" => "s3_express_multi_az", "name" => "get_blob")
                    .record(start.elapsed().as_nanos() as f64);
                histogram!("rpc_duration_nanos", "type" => "s3_express_multi_az", "name" => "get_blob", "bucket_type" => "local_az")
                    .record(start.elapsed().as_nanos() as f64);
                counter!("s3_express_operations_total", "operation" => "get", "bucket_type" => "local_az", "result" => "failure")
                    .increment(1);
                return Err(BlobStorageError::S3(e.to_string()));
            }
        };

        *body = data.into_bytes();

        // Record overall metrics for backward compatibility
        histogram!("blob_size", "operation" => "get", "storage" => "s3_express_multi_az")
            .record(body.len() as f64);
        histogram!("rpc_duration_nanos", "type" => "s3_express_multi_az", "name" => "get_blob")
            .record(start.elapsed().as_nanos() as f64);

        // Record bucket-specific metrics (always reading from local AZ bucket)
        histogram!("blob_size", "operation" => "get", "storage" => "s3_express_multi_az", "bucket_type" => "local_az")
            .record(body.len() as f64);
        histogram!("rpc_duration_nanos", "type" => "s3_express_multi_az", "name" => "get_blob", "bucket_type" => "local_az")
            .record(start.elapsed().as_nanos() as f64);
        counter!("s3_express_operations_total", "operation" => "get", "bucket_type" => "local_az", "result" => "success")
            .increment(1);

        Ok(())
    }

    pub async fn delete_blob(
        &self,
        tracking_root_blob_name: Option<&str>,
        blob_guid: DataBlobGuid,
        block_number: u32,
    ) -> Result<(), BlobStorageError> {
        let s3_key = blob_key(blob_guid.blob_id, block_number);
        let tracking_root = tracking_root_blob_name.expect("No tracking_root_blob_name provided");
        assert!(!tracking_root.is_empty());
        assert_eq!(DataBlobGuid::S3_VOLUME, blob_guid.volume_id);

        // Record deleted blob for tracking
        self.record_deleted_blob(tracking_root, &s3_key).await?;

        // Try to delete from single-copy tracking
        let _ = self
            .data_blob_tracker
            .delete_single_copy_data_blob(tracking_root, &s3_key)
            .await; // Ignore errors - blob may not be in single-copy tracking

        // Delete from both buckets concurrently (best effort)
        let start = Instant::now();
        let local_future = async {
            let start = Instant::now();
            let result = s3_retry!(
                "delete_blob",
                "s3_express_multi_az",
                &self.local_az_bucket,
                &self.retry_config,
                self.local_client
                    .delete_object()
                    .await
                    .bucket(&self.local_az_bucket)
                    .key(&s3_key)
                    .send()
            );
            (result, start.elapsed())
        };

        let remote_future = async {
            let start = Instant::now();
            let result = s3_retry!(
                "delete_blob",
                "s3_express_multi_az",
                &self.remote_az_bucket,
                &self.retry_config,
                self.remote_client
                    .delete_object()
                    .await
                    .bucket(&self.remote_az_bucket)
                    .key(&s3_key)
                    .send()
            );
            (result, start.elapsed())
        };

        let ((local_result, local_duration), (remote_result, remote_duration)) =
            tokio::join!(local_future, remote_future);

        // Record bucket-specific metrics
        let _local_success = local_result.is_ok();
        let _remote_success = remote_result.is_ok();

        // Record duration for each bucket operation
        histogram!("rpc_duration_nanos", "type" => "s3_express_multi_az", "name" => "delete_blob", "bucket_type" => "local_az")
            .record(local_duration.as_nanos() as f64);
        histogram!("rpc_duration_nanos", "type" => "s3_express_multi_az", "name" => "delete_blob", "bucket_type" => "remote_az")
            .record(remote_duration.as_nanos() as f64);

        // Record success/failure counters
        counter!("s3_express_operations_total", "operation" => "delete", "bucket_type" => "local_az", "result" => if _local_success { "success" } else { "failure" })
            .increment(1);
        counter!("s3_express_operations_total", "operation" => "delete", "bucket_type" => "remote_az", "result" => if _remote_success { "success" } else { "failure" })
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
        histogram!("rpc_duration_nanos", "type" => "s3_express_multi_az", "name" => "delete_blob")
            .record(start.elapsed().as_nanos() as f64);

        Ok(())
    }
}
