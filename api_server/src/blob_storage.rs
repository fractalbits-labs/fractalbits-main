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
