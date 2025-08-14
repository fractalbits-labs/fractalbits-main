mod bss_only_storage;
mod hybrid_storage;
mod s3_express_storage;
mod s3_express_with_tracking;

pub use bss_only_storage::BssOnlyStorage;
pub use hybrid_storage::HybridStorage;
pub use s3_express_storage::{S3ExpressConfig, S3ExpressStorage};
pub use s3_express_with_tracking::{S3ExpressWithTracking, S3ExpressWithTrackingConfig};

pub enum BlobStorageImpl {
    BssOnly(BssOnlyStorage),
    Hybrid(HybridStorage),
    S3Express(S3ExpressStorage),
    S3ExpressWithTracking(S3ExpressWithTracking),
}

use aws_config::BehaviorVersion;
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
    if s3_host.ends_with("amazonaws.com") {
        // Real AWS S3
        let aws_config = aws_config::defaults(BehaviorVersion::latest())
            .region(Region::new(s3_region.to_string()))
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
            .behavior_version(BehaviorVersion::latest());

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
            BlobStorageImpl::BssOnly(storage) => {
                storage.put_blob(blob_id, block_number, body).await
            }
            BlobStorageImpl::Hybrid(storage) => storage.put_blob(blob_id, block_number, body).await,
            BlobStorageImpl::S3Express(storage) => {
                storage.put_blob(blob_id, block_number, body).await
            }
            BlobStorageImpl::S3ExpressWithTracking(storage) => {
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
            BlobStorageImpl::BssOnly(storage) => {
                storage.get_blob(blob_id, block_number, body).await
            }
            BlobStorageImpl::Hybrid(storage) => storage.get_blob(blob_id, block_number, body).await,
            BlobStorageImpl::S3Express(storage) => {
                storage.get_blob(blob_id, block_number, body).await
            }
            BlobStorageImpl::S3ExpressWithTracking(storage) => {
                storage.get_blob(blob_id, block_number, body).await
            }
        }
    }

    async fn delete_blob(&self, blob_id: Uuid, block_number: u32) -> Result<(), BlobStorageError> {
        match self {
            BlobStorageImpl::BssOnly(storage) => storage.delete_blob(blob_id, block_number).await,
            BlobStorageImpl::Hybrid(storage) => storage.delete_blob(blob_id, block_number).await,
            BlobStorageImpl::S3Express(storage) => storage.delete_blob(blob_id, block_number).await,
            BlobStorageImpl::S3ExpressWithTracking(storage) => {
                storage.delete_blob(blob_id, block_number).await
            }
        }
    }
}
