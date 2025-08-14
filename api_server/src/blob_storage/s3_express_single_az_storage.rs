use super::{blob_key, create_s3_client, BlobStorage, BlobStorageError};
use aws_sdk_s3::{types::StorageClass, Client as S3Client};
use bytes::Bytes;
use metrics::{counter, histogram};
use std::time::Instant;
use tracing::info;
use uuid::Uuid;

#[derive(Clone)]
pub struct S3ExpressSingleAzConfig {
    pub s3_host: String,
    pub s3_port: u16,
    pub s3_region: String,
    pub s3_bucket: String,
    pub az: String,
    pub force_path_style: bool,
}

pub struct S3ExpressSingleAzStorage {
    client_s3: S3Client,
    s3_bucket: String,
}

impl S3ExpressSingleAzStorage {
    pub async fn new(config: &S3ExpressSingleAzConfig) -> Result<Self, BlobStorageError> {
        info!(
            "Initializing S3 Express Single AZ storage for bucket: {} in AZ: {}",
            config.s3_bucket, config.az
        );

        let client_s3 = create_s3_client(
            &config.s3_host,
            config.s3_port,
            &config.s3_region,
            config.force_path_style,
        )
        .await;

        let endpoint_url = format!("{}:{}", config.s3_host, config.s3_port);
        info!(
            "S3 Express Single AZ client initialized with endpoint: {}",
            endpoint_url
        );

        Ok(Self {
            client_s3,
            s3_bucket: config.s3_bucket.clone(),
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

        // Put to S3 Express bucket
        let mut put_request = self
            .client_s3
            .put_object()
            .bucket(&self.s3_bucket)
            .key(&s3_key)
            .body(body.into());

        // Only set storage class for real S3 Express (not for local minio testing)
        if self.s3_bucket.ends_with("--x-s3") {
            put_request = put_request.storage_class(StorageClass::ExpressOnezone);
        }

        let put_result = put_request.send().await;

        match put_result {
            Ok(_) => {
                histogram!("blob_put_duration", "storage" => "s3_express_single_az")
                    .record(start.elapsed().as_secs_f64());
                counter!("blob_put_success", "storage" => "s3_express_single_az").increment(1);
                Ok(())
            }
            Err(e) => {
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

        // Get from S3 Express bucket
        let get_result = self
            .client_s3
            .get_object()
            .bucket(&self.s3_bucket)
            .key(&s3_key)
            .send()
            .await;

        match get_result {
            Ok(response) => {
                *body = response.body.collect().await.unwrap().into_bytes();
                histogram!("blob_get_duration", "storage" => "s3_express_single_az")
                    .record(start.elapsed().as_secs_f64());
                histogram!("blob_size", "operation" => "get", "storage" => "s3_express_single_az")
                    .record(body.len() as f64);
                counter!("blob_get_success", "storage" => "s3_express_single_az").increment(1);
                Ok(())
            }
            Err(e) => {
                counter!("blob_get_error", "storage" => "s3_express_single_az").increment(1);
                Err(e.into())
            }
        }
    }

    async fn delete_blob(&self, blob_id: Uuid, block_number: u32) -> Result<(), BlobStorageError> {
        let start = Instant::now();
        let s3_key = blob_key(blob_id, block_number);

        // Delete from S3 Express bucket
        let delete_result = self
            .client_s3
            .delete_object()
            .bucket(&self.s3_bucket)
            .key(&s3_key)
            .send()
            .await;

        match delete_result {
            Ok(_) => {
                histogram!("blob_delete_duration", "storage" => "s3_express_single_az")
                    .record(start.elapsed().as_secs_f64());
                counter!("blob_delete_success", "storage" => "s3_express_single_az").increment(1);
                Ok(())
            }
            Err(e) => {
                counter!("blob_delete_error", "storage" => "s3_express_single_az").increment(1);
                Err(e.into())
            }
        }
    }
}
