use super::{blob_key, BlobStorage, BlobStorageError};
use aws_config::BehaviorVersion;
use aws_sdk_s3::{config::Region, types::StorageClass, Client as S3Client};
use bytes::Bytes;
use metrics::histogram;
use std::time::Instant;
use tracing::info;
use uuid::Uuid;

#[derive(Clone)]
pub struct S3ExpressConfig {
    pub bucket: String,
    pub region: String,
    pub az: String,
    pub express_session_auth: bool,
}

pub struct S3ExpressStorage {
    client_s3: S3Client,
    bucket: String,
}

impl S3ExpressStorage {
    pub async fn new(config: &S3ExpressConfig) -> Result<Self, BlobStorageError> {
        info!(
            "Initializing S3 Express One Zone storage for bucket: {} in AZ: {}",
            config.bucket, config.az
        );

        let aws_config = aws_config::defaults(BehaviorVersion::latest())
            .region(Region::new(config.region.clone()))
            .load()
            .await;

        let client_s3 = if config.express_session_auth {
            S3Client::new(&aws_config)
        } else {
            // For S3 Express with disabled session auth
            let s3_config = {
                let mut builder = aws_sdk_s3::Config::builder();
                builder.set_behavior_version(Some(BehaviorVersion::latest()));
                builder.set_disable_s3_express_session_auth(Some(true));
                builder.build()
            };
            S3Client::from_conf(s3_config)
        };

        info!(
            "S3 Express One Zone client initialized with session auth: {}",
            config.express_session_auth
        );

        Ok(Self {
            client_s3,
            bucket: config.bucket.clone(),
        })
    }
}

impl BlobStorage for S3ExpressStorage {
    async fn put_blob(
        &self,
        blob_id: Uuid,
        block_number: u32,
        body: Bytes,
    ) -> Result<(), BlobStorageError> {
        histogram!("blob_size", "operation" => "put", "storage" => "s3_express")
            .record(body.len() as f64);
        let start = Instant::now();

        let s3_key = blob_key(blob_id, block_number);

        self.client_s3
            .put_object()
            .bucket(&self.bucket)
            .key(&s3_key)
            .body(body.into())
            .storage_class(StorageClass::ExpressOnezone)
            .send()
            .await?;

        histogram!("rpc_duration_nanos", "type" => "s3_express", "name" => "put_blob")
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

        let response = self
            .client_s3
            .get_object()
            .bucket(&self.bucket)
            .key(&s3_key)
            .send()
            .await?;

        let data = response
            .body
            .collect()
            .await
            .map_err(|e| BlobStorageError::S3(e.to_string()))?;

        *body = data.into_bytes();

        histogram!("blob_size", "operation" => "get", "storage" => "s3_express")
            .record(body.len() as f64);
        histogram!("rpc_duration_nanos", "type" => "s3_express", "name" => "get_blob")
            .record(start.elapsed().as_nanos() as f64);

        Ok(())
    }

    async fn delete_blob(&self, blob_id: Uuid, block_number: u32) -> Result<(), BlobStorageError> {
        let start = Instant::now();
        let s3_key = blob_key(blob_id, block_number);

        self.client_s3
            .delete_object()
            .bucket(&self.bucket)
            .key(&s3_key)
            .send()
            .await?;

        histogram!("rpc_duration_nanos", "type" => "s3_express", "name" => "delete_blob")
            .record(start.elapsed().as_nanos() as f64);

        Ok(())
    }
}
