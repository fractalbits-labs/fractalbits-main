use super::{blob_key, create_s3_client, BlobStorage, BlobStorageError};
use aws_config::BehaviorVersion;
use aws_sdk_s3::{config::Region, types::StorageClass, Client as S3Client, Config as S3Config};
use bytes::Bytes;
use metrics::{counter, histogram};
use std::time::Instant;
use tracing::info;
use uuid::Uuid;

#[derive(Clone)]
pub struct S3ExpressConfig {
    pub local_az_host: String,
    pub local_az_port: u16,
    pub s3_region: String,
    pub local_az_bucket: String,
    pub remote_az_bucket: String,
    pub az: String,
    pub express_session_auth: bool,
}

pub struct S3ExpressStorage {
    client_s3: S3Client,
    local_az_bucket: String,
    remote_az_bucket: String,
}

impl S3ExpressStorage {
    pub async fn new(config: &S3ExpressConfig) -> Result<Self, BlobStorageError> {
        info!(
            "Initializing S3 Express One Zone storage for buckets: {} (local) and {} (remote) in AZ: {}",
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
            // For local minio testing - need to disable S3 Express session auth
            let endpoint_url = format!("{}:{}", config.local_az_host, config.local_az_port);
            let s3_config = S3Config::builder()
                .behavior_version(BehaviorVersion::latest())
                .disable_s3_express_session_auth(true)
                .endpoint_url(&endpoint_url)
                .region(Region::new(config.s3_region.clone()))
                .force_path_style(true)
                .credentials_provider(aws_sdk_s3::config::Credentials::new(
                    "minioadmin",
                    "minioadmin",
                    None,
                    None,
                    "minio",
                ))
                .build();

            S3Client::from_conf(s3_config)
        };

        let endpoint_url = format!("{}:{}", config.local_az_host, config.local_az_port);
        info!(
            "S3 Express One Zone client initialized with endpoint: {} and session auth: {}",
            endpoint_url, config.express_session_auth
        );

        Ok(Self {
            client_s3,
            local_az_bucket: config.local_az_bucket.clone(),
            remote_az_bucket: config.remote_az_bucket.clone(),
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
        // Record overall blob size for backward compatibility
        histogram!("blob_size", "operation" => "put", "storage" => "s3_express")
            .record(body.len() as f64);

        // Record bucket-specific blob sizes
        histogram!("blob_size", "operation" => "put", "storage" => "s3_express", "bucket_type" => "local_az")
            .record(body.len() as f64);
        histogram!("blob_size", "operation" => "put", "storage" => "s3_express", "bucket_type" => "remote_az")
            .record(body.len() as f64);

        let start = Instant::now();
        let local_start = Instant::now();
        let remote_start = Instant::now();

        let s3_key = blob_key(blob_id, block_number);

        // Create requests for both buckets
        let mut local_request = self
            .client_s3
            .put_object()
            .bucket(&self.local_az_bucket)
            .key(&s3_key)
            .body(body.clone().into());

        let mut remote_request = self
            .client_s3
            .put_object()
            .bucket(&self.remote_az_bucket)
            .key(&s3_key)
            .body(body.into());

        // Only set storage class for real S3 Express (not for local minio testing)
        if self.local_az_bucket.ends_with("--x-s3") {
            local_request = local_request.storage_class(StorageClass::ExpressOnezone);
        }
        if self.remote_az_bucket.ends_with("--x-s3") {
            remote_request = remote_request.storage_class(StorageClass::ExpressOnezone);
        }

        // Write to both buckets concurrently
        let (local_result, remote_result) =
            tokio::join!(local_request.send(), remote_request.send());

        // Record bucket-specific metrics
        let local_success = local_result.is_ok();
        let remote_success = remote_result.is_ok();

        // Record duration for each bucket operation
        histogram!("rpc_duration_nanos", "type" => "s3_express", "name" => "put_blob", "bucket_type" => "local_az")
            .record(local_start.elapsed().as_nanos() as f64);
        histogram!("rpc_duration_nanos", "type" => "s3_express", "name" => "put_blob", "bucket_type" => "remote_az")
            .record(remote_start.elapsed().as_nanos() as f64);

        // Record success/failure counters
        counter!("s3_express_operations_total", "operation" => "put", "bucket_type" => "local_az", "result" => if local_success { "success" } else { "failure" })
            .increment(1);
        counter!("s3_express_operations_total", "operation" => "put", "bucket_type" => "remote_az", "result" => if remote_success { "success" } else { "failure" })
            .increment(1);

        // Log errors but don't fail if one bucket operation fails
        if let Err(e) = &local_result {
            tracing::warn!(
                "Failed to write to local AZ bucket {}: {}",
                self.local_az_bucket,
                e
            );
        }
        if let Err(e) = &remote_result {
            tracing::warn!(
                "Failed to write to remote AZ bucket {}: {}",
                self.remote_az_bucket,
                e
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
                histogram!("rpc_duration_nanos", "type" => "s3_express", "name" => "put_blob")
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

        // Always read from local AZ bucket for better performance
        let response_result = self
            .client_s3
            .get_object()
            .bucket(&self.local_az_bucket)
            .key(&s3_key)
            .send()
            .await;

        let response = match response_result {
            Ok(resp) => resp,
            Err(e) => {
                // Record failure metrics
                histogram!("rpc_duration_nanos", "type" => "s3_express", "name" => "get_blob")
                    .record(start.elapsed().as_nanos() as f64);
                histogram!("rpc_duration_nanos", "type" => "s3_express", "name" => "get_blob", "bucket_type" => "local_az")
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
                histogram!("rpc_duration_nanos", "type" => "s3_express", "name" => "get_blob")
                    .record(start.elapsed().as_nanos() as f64);
                histogram!("rpc_duration_nanos", "type" => "s3_express", "name" => "get_blob", "bucket_type" => "local_az")
                    .record(start.elapsed().as_nanos() as f64);
                counter!("s3_express_operations_total", "operation" => "get", "bucket_type" => "local_az", "result" => "failure")
                    .increment(1);
                return Err(BlobStorageError::S3(e.to_string()));
            }
        };

        *body = data.into_bytes();

        // Record overall metrics for backward compatibility
        histogram!("blob_size", "operation" => "get", "storage" => "s3_express")
            .record(body.len() as f64);
        histogram!("rpc_duration_nanos", "type" => "s3_express", "name" => "get_blob")
            .record(start.elapsed().as_nanos() as f64);

        // Record bucket-specific metrics (always reading from local AZ bucket)
        histogram!("blob_size", "operation" => "get", "storage" => "s3_express", "bucket_type" => "local_az")
            .record(body.len() as f64);
        histogram!("rpc_duration_nanos", "type" => "s3_express", "name" => "get_blob", "bucket_type" => "local_az")
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

        // Delete from both buckets concurrently
        let (local_result, remote_result) = tokio::join!(
            self.client_s3
                .delete_object()
                .bucket(&self.local_az_bucket)
                .key(&s3_key)
                .send(),
            self.client_s3
                .delete_object()
                .bucket(&self.remote_az_bucket)
                .key(&s3_key)
                .send()
        );

        // Record bucket-specific metrics
        let local_success = local_result.is_ok();
        let remote_success = remote_result.is_ok();

        // Record duration for each bucket operation
        histogram!("rpc_duration_nanos", "type" => "s3_express", "name" => "delete_blob", "bucket_type" => "local_az")
            .record(local_start.elapsed().as_nanos() as f64);
        histogram!("rpc_duration_nanos", "type" => "s3_express", "name" => "delete_blob", "bucket_type" => "remote_az")
            .record(remote_start.elapsed().as_nanos() as f64);

        // Record success/failure counters
        counter!("s3_express_operations_total", "operation" => "delete", "bucket_type" => "local_az", "result" => if local_success { "success" } else { "failure" })
            .increment(1);
        counter!("s3_express_operations_total", "operation" => "delete", "bucket_type" => "remote_az", "result" => if remote_success { "success" } else { "failure" })
            .increment(1);

        // Log errors but don't fail if one bucket operation fails
        if let Err(e) = &local_result {
            tracing::warn!(
                "Failed to delete from local AZ bucket {}: {}",
                self.local_az_bucket,
                e
            );
        }
        if let Err(e) = &remote_result {
            tracing::warn!(
                "Failed to delete from remote AZ bucket {}: {}",
                self.remote_az_bucket,
                e
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
                // Record overall duration for backward compatibility
                histogram!("rpc_duration_nanos", "type" => "s3_express", "name" => "delete_blob")
                    .record(start.elapsed().as_nanos() as f64);
                Ok(())
            }
        }
    }
}
