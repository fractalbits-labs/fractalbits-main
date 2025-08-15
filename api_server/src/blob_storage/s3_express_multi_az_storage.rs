use super::{
    blob_key, create_s3_client_wrapper, retry, BlobStorage, BlobStorageError, RetryMode,
    S3ClientWrapper, S3RateLimitConfig,
};
use crate::s3_retry;
use bytes::Bytes;
use metrics::{counter, histogram};
use std::time::Instant;
use tracing::{info, warn};
use uuid::Uuid;

#[derive(Clone)]
pub struct S3ExpressMultiAzConfig {
    pub local_az_host: String,
    pub local_az_port: u16,
    pub s3_region: String,
    pub local_az_bucket: String,
    pub remote_az_bucket: String,
    pub az: String,
    pub rate_limit_config: S3RateLimitConfig,
    pub retry_mode: RetryMode,
    pub max_attempts: u32,
}

pub struct S3ExpressMultiAzStorage {
    client_s3: S3ClientWrapper,
    local_az_bucket: String,
    remote_az_bucket: String,
    retry_config: retry::RetryConfig,
    retry_mode: RetryMode,
}

impl S3ExpressMultiAzStorage {
    pub async fn new(config: &S3ExpressMultiAzConfig) -> Result<Self, BlobStorageError> {
        info!(
            "Initializing S3 Express One Zone storage for buckets: {} (local) and {} (remote) in AZ: {} (rate_limit_enabled: {}, retry_mode: {:?})",
            config.local_az_bucket, config.remote_az_bucket, config.az, config.rate_limit_config.enabled, config.retry_mode
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
        info!("S3 client initialized with endpoint: {endpoint_url}");

        let retry_config = match config.retry_mode {
            RetryMode::Disabled => retry::RetryConfig::disabled(),
            _ => retry::RetryConfig::rate_limited().with_max_attempts(config.max_attempts),
        };

        Ok(Self {
            client_s3,
            local_az_bucket: config.local_az_bucket.clone(),
            remote_az_bucket: config.remote_az_bucket.clone(),
            retry_config,
            retry_mode: config.retry_mode.clone(),
        })
    }

    // Helper method to perform put operation with retry mode handling
    async fn put_object_with_retry(
        &self,
        bucket: &str,
        key: &str,
        body: Bytes,
    ) -> Result<
        aws_sdk_s3::operation::put_object::PutObjectOutput,
        aws_sdk_s3::error::SdkError<aws_sdk_s3::operation::put_object::PutObjectError>,
    > {
        match self.retry_mode {
            RetryMode::Disabled => {
                self.client_s3
                    .put_object()
                    .await
                    .bucket(bucket)
                    .key(key)
                    .body(body.into())
                    .send()
                    .await
            }
            _ => {
                s3_retry!(
                    "put_blob",
                    "s3_express_multi_az",
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
    async fn get_object_with_retry(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<
        aws_sdk_s3::operation::get_object::GetObjectOutput,
        aws_sdk_s3::error::SdkError<aws_sdk_s3::operation::get_object::GetObjectError>,
    > {
        match self.retry_mode {
            RetryMode::Disabled => {
                self.client_s3
                    .get_object()
                    .await
                    .bucket(bucket)
                    .key(key)
                    .send()
                    .await
            }
            _ => {
                s3_retry!(
                    "get_blob",
                    "s3_express_multi_az",
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
    async fn delete_object_with_retry(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<
        aws_sdk_s3::operation::delete_object::DeleteObjectOutput,
        aws_sdk_s3::error::SdkError<aws_sdk_s3::operation::delete_object::DeleteObjectError>,
    > {
        match self.retry_mode {
            RetryMode::Disabled => {
                self.client_s3
                    .delete_object()
                    .await
                    .bucket(bucket)
                    .key(key)
                    .send()
                    .await
            }
            _ => {
                s3_retry!(
                    "delete_blob",
                    "s3_express_multi_az",
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
}

impl BlobStorage for S3ExpressMultiAzStorage {
    async fn put_blob(
        &self,
        blob_id: Uuid,
        block_number: u32,
        body: Bytes,
    ) -> Result<(), BlobStorageError> {
        // Record overall blob size for backward compatibility
        histogram!("blob_size", "operation" => "put", "storage" => "s3_express_multi_az")
            .record(body.len() as f64);

        // Record bucket-specific blob sizes
        histogram!("blob_size", "operation" => "put", "storage" => "s3_express_multi_az", "bucket_type" => "local_az")
            .record(body.len() as f64);
        histogram!("blob_size", "operation" => "put", "storage" => "s3_express_multi_az", "bucket_type" => "remote_az")
            .record(body.len() as f64);

        let start = Instant::now();
        let local_start = Instant::now();
        let remote_start = Instant::now();

        let s3_key = blob_key(blob_id, block_number);

        // Write to both buckets concurrently
        let (local_result, remote_result) = tokio::join!(
            self.put_object_with_retry(&self.local_az_bucket, &s3_key, body.clone()),
            self.put_object_with_retry(&self.remote_az_bucket, &s3_key, body.clone())
        );

        // Record bucket-specific metrics
        let local_success = local_result.is_ok();
        let remote_success = remote_result.is_ok();

        // Record duration for each bucket operation
        histogram!("rpc_duration_nanos", "type" => "s3_express_multi_az", "name" => "put_blob", "bucket_type" => "local_az")
            .record(local_start.elapsed().as_nanos() as f64);
        histogram!("rpc_duration_nanos", "type" => "s3_express_multi_az", "name" => "put_blob", "bucket_type" => "remote_az")
            .record(remote_start.elapsed().as_nanos() as f64);

        // Record success/failure counters
        counter!("s3_express_operations_total", "operation" => "put", "bucket_type" => "local_az", "result" => if local_success { "success" } else { "failure" })
            .increment(1);
        counter!("s3_express_operations_total", "operation" => "put", "bucket_type" => "remote_az", "result" => if remote_success { "success" } else { "failure" })
            .increment(1);

        // Log errors but don't fail if one bucket operation fails
        if let Err(e) = &local_result {
            warn!(
                "Failed to write to local AZ bucket {}: {}",
                self.local_az_bucket, e
            );
        }
        if let Err(e) = &remote_result {
            warn!(
                "Failed to write to remote AZ bucket {}: {}",
                self.remote_az_bucket, e
            );
        }

        // Return error only if both operations failed
        match (local_result, remote_result) {
            (Err(local_err), Err(remote_err)) => {
                tracing::error!(
                    "Both bucket writes failed. Local: {}, Remote: {}",
                    local_err,
                    remote_err
                );
                Err(BlobStorageError::S3(format!(
                    "Both bucket writes failed: {local_err}"
                )))
            }
            _ => {
                // Record overall duration for backward compatibility
                histogram!("rpc_duration_nanos", "type" => "s3_express_multi_az", "name" => "put_blob")
                    .record(start.elapsed().as_nanos() as f64);
                Ok(())
            }
        }
    }

    async fn get_blob(
        &self,
        blob_id: Uuid,
        block_number: u32,
        body: &mut Bytes,
    ) -> Result<(), BlobStorageError> {
        let start = Instant::now();
        let s3_key = blob_key(blob_id, block_number);

        // Always read from local AZ bucket for better performance, with retry
        let response_result = self
            .get_object_with_retry(&self.local_az_bucket, &s3_key)
            .await;

        let response = match response_result {
            Ok(resp) => resp,
            Err(local_err) => {
                // Try remote bucket as fallback with retry
                warn!(
                    "Failed to read from local AZ bucket {}, trying remote: {}",
                    self.local_az_bucket, local_err
                );

                counter!("s3_express_operations_total", "operation" => "get", "bucket_type" => "local_az", "result" => "failure")
                    .increment(1);

                let remote_result = self
                    .get_object_with_retry(&self.remote_az_bucket, &s3_key)
                    .await;

                match remote_result {
                    Ok(resp) => {
                        counter!("s3_express_operations_total", "operation" => "get", "bucket_type" => "remote_az", "result" => "success")
                            .increment(1);
                        resp
                    }
                    Err(remote_err) => {
                        counter!("s3_express_operations_total", "operation" => "get", "bucket_type" => "remote_az", "result" => "failure")
                            .increment(1);
                        tracing::error!(
                            "Failed to read from both buckets. Local: {}, Remote: {}",
                            local_err,
                            remote_err
                        );
                        return Err(BlobStorageError::S3(format!(
                            "Failed to read from both buckets: {local_err}"
                        )));
                    }
                }
            }
        };

        *body = response.body.collect().await.unwrap().into_bytes();

        // Record metrics
        histogram!("rpc_duration_nanos", "type" => "s3_express_multi_az", "name" => "get_blob")
            .record(start.elapsed().as_nanos() as f64);
        histogram!("blob_size", "operation" => "get", "storage" => "s3_express_multi_az")
            .record(body.len() as f64);
        counter!("s3_express_operations_total", "operation" => "get", "bucket_type" => "local_az", "result" => "success")
            .increment(1);

        Ok(())
    }

    async fn delete_blob(&self, blob_id: Uuid, block_number: u32) -> Result<(), BlobStorageError> {
        let start = Instant::now();
        let s3_key = blob_key(blob_id, block_number);

        // Delete from both buckets concurrently
        let (local_result, remote_result) = tokio::join!(
            self.delete_object_with_retry(&self.local_az_bucket, &s3_key),
            self.delete_object_with_retry(&self.remote_az_bucket, &s3_key)
        );

        // Log errors but don't fail if one bucket operation fails
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

        // Return error only if both operations failed
        match (local_result, remote_result) {
            (Err(local_err), Err(remote_err)) => {
                tracing::error!(
                    "Both bucket deletes failed. Local: {}, Remote: {}",
                    local_err,
                    remote_err
                );
                Err(BlobStorageError::S3(format!(
                    "Both bucket deletes failed: {local_err}"
                )))
            }
            _ => {
                histogram!("rpc_duration_nanos", "type" => "s3_express_multi_az", "name" => "delete_blob")
                    .record(start.elapsed().as_nanos() as f64);
                Ok(())
            }
        }
    }
}
