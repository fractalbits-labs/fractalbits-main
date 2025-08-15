use super::{
    blob_key, create_s3_client_wrapper, retry, BlobStorage, BlobStorageError, RetryMode,
    S3ClientWrapper, S3RateLimitConfig,
};
use crate::s3_retry;
use bytes::Bytes;
use metrics::{counter, histogram};
use std::time::Instant;
use tracing::{error, info};
use uuid::Uuid;

#[derive(Clone)]
pub struct S3ExpressSingleAzConfig {
    pub s3_host: String,
    pub s3_port: u16,
    pub s3_region: String,
    pub s3_bucket: String,
    pub az: String,
    pub force_path_style: bool,
    pub rate_limit_config: S3RateLimitConfig,
    pub retry_mode: RetryMode,
    pub max_attempts: u32,
}

pub struct S3ExpressSingleAzStorage {
    client_s3: S3ClientWrapper,
    s3_bucket: String,
    retry_config: retry::RetryConfig,
    retry_mode: RetryMode,
}

impl S3ExpressSingleAzStorage {
    pub async fn new(config: &S3ExpressSingleAzConfig) -> Result<Self, BlobStorageError> {
        info!(
            "Initializing S3 Express Single AZ storage for bucket: {} in AZ: {} (rate_limit_enabled: {}, retry_mode: {:?})",
            config.s3_bucket, config.az, config.rate_limit_config.enabled, config.retry_mode
        );

        let client_s3 = create_s3_client_wrapper(
            &config.s3_host,
            config.s3_port,
            &config.s3_region,
            config.force_path_style,
            &config.rate_limit_config,
        )
        .await;

        let endpoint_url = format!("{}:{}", config.s3_host, config.s3_port);
        info!(
            "S3 Express Single AZ client initialized with endpoint: {}",
            endpoint_url
        );

        let retry_config = match config.retry_mode {
            RetryMode::Disabled => retry::RetryConfig::disabled(),
            _ => retry::RetryConfig::rate_limited().with_max_attempts(config.max_attempts),
        };

        Ok(Self {
            client_s3,
            s3_bucket: config.s3_bucket.clone(),
            retry_config,
            retry_mode: config.retry_mode.clone(),
        })
    }
}

impl BlobStorage for S3ExpressSingleAzStorage {
    async fn put_blob(
        &self,
        blob_id: Uuid,
        block_number: u32,
        body: Bytes,
    ) -> Result<(), BlobStorageError> {
        histogram!("blob_size", "operation" => "put", "storage" => "s3_express_single_az")
            .record(body.len() as f64);

        let start = Instant::now();
        let s3_key = blob_key(blob_id, block_number);

        let result = match self.retry_mode {
            RetryMode::Disabled => {
                // Direct call without retry
                self.client_s3
                    .put_object()
                    .await
                    .bucket(&self.s3_bucket)
                    .key(&s3_key)
                    .body(body.clone().into())
                    .send()
                    .await
            }
            _ => {
                // Use retry macro for put operation
                s3_retry!(
                    "put_blob",
                    "s3_express_single_az",
                    &self.s3_bucket,
                    &self.retry_config,
                    self.client_s3
                        .put_object()
                        .await
                        .bucket(&self.s3_bucket)
                        .key(&s3_key)
                        .body(body.clone().into())
                        .send()
                )
            }
        };

        match result {
            Ok(_) => {
                histogram!("blob_put_duration_nanos", "storage" => "s3_express_single_az")
                    .record(start.elapsed().as_nanos() as f64);
                counter!("blob_put_success", "storage" => "s3_express_single_az").increment(1);
                Ok(())
            }
            Err(e) => {
                error!(bucket = %self.s3_bucket, %blob_id, %block_number, error = %e, "put blob error");
                counter!("blob_put_error", "storage" => "s3_express_single_az").increment(1);
                Err(e.into())
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

        let result = match self.retry_mode {
            RetryMode::Disabled => {
                // Direct call without retry
                self.client_s3
                    .get_object()
                    .await
                    .bucket(&self.s3_bucket)
                    .key(&s3_key)
                    .send()
                    .await
            }
            _ => {
                // Use retry macro for get operation
                s3_retry!(
                    "get_blob",
                    "s3_express_single_az",
                    &self.s3_bucket,
                    &self.retry_config,
                    self.client_s3
                        .get_object()
                        .await
                        .bucket(&self.s3_bucket)
                        .key(&s3_key)
                        .send()
                )
            }
        };

        match result {
            Ok(response) => {
                *body = response.body.collect().await.unwrap().into_bytes();
                histogram!("blob_get_duration_nanos", "storage" => "s3_express_single_az")
                    .record(start.elapsed().as_nanos() as f64);
                histogram!("blob_size", "operation" => "get", "storage" => "s3_express_single_az")
                    .record(body.len() as f64);
                counter!("blob_get_success", "storage" => "s3_express_single_az").increment(1);
                Ok(())
            }
            Err(e) => {
                error!(bucket = %self.s3_bucket, %blob_id, %block_number, error = %e, "get blob error");
                counter!("blob_get_error", "storage" => "s3_express_single_az").increment(1);
                Err(e.into())
            }
        }
    }

    async fn delete_blob(&self, blob_id: Uuid, block_number: u32) -> Result<(), BlobStorageError> {
        let start = Instant::now();
        let s3_key = blob_key(blob_id, block_number);

        let result = match self.retry_mode {
            RetryMode::Disabled => {
                // Direct call without retry
                self.client_s3
                    .delete_object()
                    .await
                    .bucket(&self.s3_bucket)
                    .key(&s3_key)
                    .send()
                    .await
            }
            _ => {
                // Use retry macro for delete operation
                s3_retry!(
                    "delete_blob",
                    "s3_express_single_az",
                    &self.s3_bucket,
                    &self.retry_config,
                    self.client_s3
                        .delete_object()
                        .await
                        .bucket(&self.s3_bucket)
                        .key(&s3_key)
                        .send()
                )
            }
        };

        match result {
            Ok(_) => {
                histogram!("blob_delete_duration_nanos", "storage" => "s3_express_single_az")
                    .record(start.elapsed().as_nanos() as f64);
                counter!("blob_delete_success", "storage" => "s3_express_single_az").increment(1);
                Ok(())
            }
            Err(e) => {
                error!(bucket = %self.s3_bucket, %blob_id, %block_number, error = %e, "delete blob error");
                counter!("blob_delete_error", "storage" => "s3_express_single_az").increment(1);
                Err(e.into())
            }
        }
    }
}
