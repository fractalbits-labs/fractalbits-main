mod bss_only_single_az_storage;
mod hybrid_single_az_storage;
mod retry;
mod s3_express_multi_az_storage;
mod s3_express_multi_az_with_tracking;
mod s3_express_single_az_storage;

pub use bss_only_single_az_storage::BssOnlySingleAzStorage;
pub use hybrid_single_az_storage::HybridSingleAzStorage;
pub use s3_express_multi_az_storage::{S3ExpressMultiAzConfig, S3ExpressMultiAzStorage};
pub use s3_express_multi_az_with_tracking::{
    S3ExpressMultiAzWithTracking, S3ExpressWithTrackingConfig,
};
pub use s3_express_single_az_storage::{S3ExpressSingleAzConfig, S3ExpressSingleAzStorage};

// Rate limiting types are defined in this module

pub enum BlobStorageImpl {
    BssOnlySingleAz(BssOnlySingleAzStorage),
    HybridSingleAz(HybridSingleAzStorage),
    S3ExpressMultiAz(S3ExpressMultiAzStorage),
    S3ExpressMultiAzWithTracking(S3ExpressMultiAzWithTracking),
    S3ExpressSingleAz(S3ExpressSingleAzStorage),
}

use aws_config::{retry::RetryConfig, BehaviorVersion};
use aws_sdk_s3::{
    config::{Credentials, Region},
    error::SdkError,
    operation::{
        delete_object::DeleteObjectError, get_object::GetObjectError, put_object::PutObjectError,
    },
    Client as S3Client, Config as S3Config,
};
use bytes::Bytes;
use governor::{Quota, RateLimiter};
use std::num::NonZeroU32;
use std::sync::Arc;
use uuid::Uuid;

/// S3 operation retry macro - similar to rpc_retry but for S3 operations
#[macro_export]
macro_rules! s3_retry {
    ($operation_name:expr, $storage_type:expr, $bucket:expr, $retry_config:expr, $operation:expr) => {{
        use $crate::blob_storage::retry;
        retry::retry_s3_operation(
            $operation_name,
            $storage_type,
            $bucket,
            $retry_config,
            || async { $operation.await },
        )
        .await
    }};
}

/// Generate a consistent S3 key format for blob storage
pub fn blob_key(blob_id: Uuid, block_number: u32) -> String {
    format!("{blob_id}-p{block_number}")
}

/// Rate limiting configuration for S3 operations
#[derive(Clone, Debug)]
pub struct S3RateLimitConfig {
    pub enabled: bool,
    pub put_qps: u32,
    pub get_qps: u32,
    pub delete_qps: u32,
}

impl Default for S3RateLimitConfig {
    fn default() -> Self {
        Self {
            enabled: false,   // Default to disabled for local testing
            put_qps: 7000,    // Based on initial testing results
            get_qps: 10000,   // Higher for reads
            delete_qps: 5000, // Lower for deletes
        }
    }
}

/// Retry mode configuration
#[derive(Clone, Debug)]
pub enum RetryMode {
    Standard,
    Adaptive,
    Disabled,
}

impl From<&str> for RetryMode {
    fn from(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "standard" => RetryMode::Standard,
            "adaptive" => RetryMode::Adaptive,
            "disabled" => RetryMode::Disabled,
            _ => unreachable!(),
        }
    }
}

impl From<&crate::config::RetryConfig> for S3RateLimitConfig {
    fn from(_config: &crate::config::RetryConfig) -> Self {
        Self {
            enabled: false, // Rate limiting is separate from retry config
            put_qps: 7000,
            get_qps: 10000,
            delete_qps: 5000,
        }
    }
}

impl From<&crate::config::RatelimitConfig> for S3RateLimitConfig {
    fn from(config: &crate::config::RatelimitConfig) -> Self {
        Self {
            enabled: config.enabled,
            put_qps: config.put_qps,
            get_qps: config.get_qps,
            delete_qps: config.delete_qps,
        }
    }
}

/// S3 client wrapper that can be either rate-limited or direct
#[derive(Clone)]
pub enum S3ClientWrapper {
    RateLimited(RateLimitedS3Client),
    Direct(S3Client),
}

impl S3ClientWrapper {
    /// Rate-limited or direct put_object operation
    pub async fn put_object(
        &self,
    ) -> aws_sdk_s3::operation::put_object::builders::PutObjectFluentBuilder {
        match self {
            S3ClientWrapper::RateLimited(client) => client.put_object().await,
            S3ClientWrapper::Direct(client) => client.put_object(),
        }
    }

    /// Rate-limited or direct get_object operation
    pub async fn get_object(
        &self,
    ) -> aws_sdk_s3::operation::get_object::builders::GetObjectFluentBuilder {
        match self {
            S3ClientWrapper::RateLimited(client) => client.get_object().await,
            S3ClientWrapper::Direct(client) => client.get_object(),
        }
    }

    /// Rate-limited or direct delete_object operation
    pub async fn delete_object(
        &self,
    ) -> aws_sdk_s3::operation::delete_object::builders::DeleteObjectFluentBuilder {
        match self {
            S3ClientWrapper::RateLimited(client) => client.delete_object().await,
            S3ClientWrapper::Direct(client) => client.delete_object(),
        }
    }
}

/// S3 client wrapper with per-operation rate limiting
#[derive(Clone)]
pub struct RateLimitedS3Client {
    client: S3Client,
    put_rate_limiter: Arc<governor::DefaultDirectRateLimiter>,
    get_rate_limiter: Arc<governor::DefaultDirectRateLimiter>,
    delete_rate_limiter: Arc<governor::DefaultDirectRateLimiter>,
}

impl RateLimitedS3Client {
    pub fn new(client: S3Client, config: &S3RateLimitConfig) -> Self {
        let put_quota = Quota::per_second(
            NonZeroU32::new(config.put_qps).unwrap_or(NonZeroU32::new(1).unwrap()),
        );
        let get_quota = Quota::per_second(
            NonZeroU32::new(config.get_qps).unwrap_or(NonZeroU32::new(1).unwrap()),
        );
        let delete_quota = Quota::per_second(
            NonZeroU32::new(config.delete_qps).unwrap_or(NonZeroU32::new(1).unwrap()),
        );

        Self {
            client,
            put_rate_limiter: Arc::new(RateLimiter::direct(put_quota)),
            get_rate_limiter: Arc::new(RateLimiter::direct(get_quota)),
            delete_rate_limiter: Arc::new(RateLimiter::direct(delete_quota)),
        }
    }

    /// Rate-limited put_object operation
    pub async fn put_object(
        &self,
    ) -> aws_sdk_s3::operation::put_object::builders::PutObjectFluentBuilder {
        self.put_rate_limiter.until_ready().await;
        self.client.put_object()
    }

    /// Rate-limited get_object operation
    pub async fn get_object(
        &self,
    ) -> aws_sdk_s3::operation::get_object::builders::GetObjectFluentBuilder {
        self.get_rate_limiter.until_ready().await;
        self.client.get_object()
    }

    /// Rate-limited delete_object operation
    pub async fn delete_object(
        &self,
    ) -> aws_sdk_s3::operation::delete_object::builders::DeleteObjectFluentBuilder {
        self.delete_rate_limiter.until_ready().await;
        self.client.delete_object()
    }
}

/// Create an S3 client wrapper (rate-limited or direct) based on configuration
pub async fn create_s3_client_wrapper(
    s3_host: &str,
    s3_port: u16,
    s3_region: &str,
    force_path_style: bool,
    rate_limit_config: &S3RateLimitConfig,
) -> S3ClientWrapper {
    let s3_client = create_s3_client(s3_host, s3_port, s3_region, force_path_style).await;

    if rate_limit_config.enabled {
        S3ClientWrapper::RateLimited(RateLimitedS3Client::new(s3_client, rate_limit_config))
    } else {
        S3ClientWrapper::Direct(s3_client)
    }
}

/// Create an S3 client configured for either AWS S3 or local minio
pub async fn create_s3_client(
    s3_host: &str,
    s3_port: u16,
    s3_region: &str,
    force_path_style: bool,
) -> S3Client {
    // Disable SDK retries - we'll handle retries ourselves for better visibility
    let retry_config = RetryConfig::standard().with_max_attempts(1); // No retries at SDK level

    if s3_host.ends_with("amazonaws.com") {
        // Real AWS S3
        let aws_config = aws_config::defaults(BehaviorVersion::latest())
            .region(Region::new(s3_region.to_string()))
            .retry_config(retry_config)
            .load()
            .await;
        S3Client::new(&aws_config)
    } else {
        // Local minio or other S3-compatible service
        let credentials = Credentials::new("minioadmin", "minioadmin", None, None, "minio");
        let endpoint_url = format!("{s3_host}:{s3_port}");

        let mut s3_config_builder = S3Config::builder()
            .endpoint_url(&endpoint_url)
            .region(Region::new(s3_region.to_string()))
            .credentials_provider(credentials)
            .retry_config(retry_config)
            .behavior_version(BehaviorVersion::latest())
            .disable_s3_express_session_auth(true); // Disable for minio compatibility

        if force_path_style {
            s3_config_builder = s3_config_builder.force_path_style(true);
        }

        S3Client::from_conf(s3_config_builder.build())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum BlobStorageError {
    #[error("BSS RPC error: {0}")]
    BssRpc(#[from] rpc_client_bss::RpcErrorBss),

    #[error("S3 error: {0}")]
    S3(String),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Internal error: {0}")]
    Internal(String),
}

impl From<SdkError<PutObjectError>> for BlobStorageError {
    fn from(err: SdkError<PutObjectError>) -> Self {
        BlobStorageError::S3(err.to_string())
    }
}

impl From<SdkError<GetObjectError>> for BlobStorageError {
    fn from(err: SdkError<GetObjectError>) -> Self {
        BlobStorageError::S3(err.to_string())
    }
}

impl From<SdkError<DeleteObjectError>> for BlobStorageError {
    fn from(err: SdkError<DeleteObjectError>) -> Self {
        BlobStorageError::S3(err.to_string())
    }
}

pub trait BlobStorage: Send + Sync {
    async fn put_blob(
        &self,
        blob_id: Uuid,
        block_number: u32,
        body: Bytes,
    ) -> Result<(), BlobStorageError>;

    async fn get_blob(
        &self,
        blob_id: Uuid,
        block_number: u32,
        body: &mut Bytes,
    ) -> Result<(), BlobStorageError>;

    async fn delete_blob(&self, blob_id: Uuid, block_number: u32) -> Result<(), BlobStorageError>;
}

impl BlobStorage for BlobStorageImpl {
    async fn put_blob(
        &self,
        blob_id: Uuid,
        block_number: u32,
        body: Bytes,
    ) -> Result<(), BlobStorageError> {
        match self {
            BlobStorageImpl::BssOnlySingleAz(storage) => {
                storage.put_blob(blob_id, block_number, body).await
            }
            BlobStorageImpl::HybridSingleAz(storage) => {
                storage.put_blob(blob_id, block_number, body).await
            }
            BlobStorageImpl::S3ExpressMultiAz(storage) => {
                storage.put_blob(blob_id, block_number, body).await
            }
            BlobStorageImpl::S3ExpressMultiAzWithTracking(storage) => {
                storage.put_blob(blob_id, block_number, body).await
            }
            BlobStorageImpl::S3ExpressSingleAz(storage) => {
                storage.put_blob(blob_id, block_number, body).await
            }
        }
    }

    async fn get_blob(
        &self,
        blob_id: Uuid,
        block_number: u32,
        body: &mut Bytes,
    ) -> Result<(), BlobStorageError> {
        match self {
            BlobStorageImpl::BssOnlySingleAz(storage) => {
                storage.get_blob(blob_id, block_number, body).await
            }
            BlobStorageImpl::HybridSingleAz(storage) => {
                storage.get_blob(blob_id, block_number, body).await
            }
            BlobStorageImpl::S3ExpressMultiAz(storage) => {
                storage.get_blob(blob_id, block_number, body).await
            }
            BlobStorageImpl::S3ExpressMultiAzWithTracking(storage) => {
                storage.get_blob(blob_id, block_number, body).await
            }
            BlobStorageImpl::S3ExpressSingleAz(storage) => {
                storage.get_blob(blob_id, block_number, body).await
            }
        }
    }

    async fn delete_blob(&self, blob_id: Uuid, block_number: u32) -> Result<(), BlobStorageError> {
        match self {
            BlobStorageImpl::BssOnlySingleAz(storage) => {
                storage.delete_blob(blob_id, block_number).await
            }
            BlobStorageImpl::HybridSingleAz(storage) => {
                storage.delete_blob(blob_id, block_number).await
            }
            BlobStorageImpl::S3ExpressMultiAz(storage) => {
                storage.delete_blob(blob_id, block_number).await
            }
            BlobStorageImpl::S3ExpressMultiAzWithTracking(storage) => {
                storage.delete_blob(blob_id, block_number).await
            }
            BlobStorageImpl::S3ExpressSingleAz(storage) => {
                storage.delete_blob(blob_id, block_number).await
            }
        }
    }
}
