use crate::{
    blob_storage::{
        BlobStorage, BlobStorageError, BlobStorageImpl, BssOnlySingleAzStorage,
        S3ExpressMultiAzStorage, S3ExpressMultiAzWithTracking, S3ExpressSingleAzStorage,
        S3ExpressWithTrackingConfig, S3HybridSingleAzStorage,
    },
    config::{BlobStorageBackend, BlobStorageConfig},
    BlobId,
};
use bytes::Bytes;
use data_blob_tracking::DataBlobTracker;
use futures::stream::{self, StreamExt};
use moka::future::Cache;
use std::{sync::Arc, time::Duration};
use tokio::{sync::mpsc::Receiver, task::JoinHandle};
use uuid::Uuid;

pub struct BlobClient {
    storage: Arc<BlobStorageImpl>,
    #[allow(dead_code)]
    blob_deletion_task_handle: JoinHandle<()>,
}

impl BlobClient {
    pub async fn new(
        blob_storage_config: &BlobStorageConfig,
        rx: Receiver<(Option<String>, BlobId, usize)>,
        rpc_timeout: Duration,
        data_blob_tracker: Option<Arc<DataBlobTracker>>,
    ) -> Result<(Self, Option<Arc<Cache<String, String>>>), BlobStorageError> {
        let (storage, az_status_cache) =
            Self::create_storage_impl(blob_storage_config, rpc_timeout, data_blob_tracker).await?;

        let client = Self::create_client_with_task(storage, rx);
        Ok((client, az_status_cache))
    }

    async fn create_storage_impl(
        blob_storage_config: &BlobStorageConfig,
        rpc_timeout: Duration,
        data_blob_tracker: Option<Arc<DataBlobTracker>>,
    ) -> Result<(Arc<BlobStorageImpl>, Option<Arc<Cache<String, String>>>), BlobStorageError> {
        let storage = match &blob_storage_config.backend {
            BlobStorageBackend::BssOnlySingleAz => {
                let bss_config = Self::get_bss_config(blob_storage_config, "BssOnly")?;
                BlobStorageImpl::BssOnlySingleAz(
                    BssOnlySingleAzStorage::new(&bss_config.addr, bss_config.conn_num, rpc_timeout)
                        .await,
                )
            }
            BlobStorageBackend::S3HybridSingleAz => {
                let bss_config = Self::get_bss_config(blob_storage_config, "Hybrid")?;
                let s3_hybrid_config = blob_storage_config
                    .s3_hybrid_single_az
                    .as_ref()
                    .ok_or_else(|| {
                        BlobStorageError::Config(
                            "S3 hybrid configuration required for Hybrid backend".into(),
                        )
                    })?;
                BlobStorageImpl::HybridSingleAz(
                    S3HybridSingleAzStorage::new(
                        &bss_config.addr,
                        bss_config.conn_num,
                        s3_hybrid_config,
                        rpc_timeout,
                    )
                    .await,
                )
            }
            BlobStorageBackend::S3ExpressMultiAz => {
                let s3_express_config =
                    Self::get_s3_express_multi_az_config(blob_storage_config, "S3Express")?;
                let express_config = Self::build_s3_express_multi_az_config(s3_express_config);
                BlobStorageImpl::S3ExpressMultiAz(
                    S3ExpressMultiAzStorage::new(&express_config).await?,
                )
            }
            BlobStorageBackend::S3ExpressMultiAzWithTracking => {
                let data_blob_tracker = data_blob_tracker.ok_or_else(|| {
                    BlobStorageError::Config(
                        "DataBlobTracker required for S3ExpressWithTracking backend".into(),
                    )
                })?;

                let s3_express_config = Self::get_s3_express_multi_az_config(
                    blob_storage_config,
                    "S3ExpressWithTracking",
                )?;
                let express_config = Self::build_s3_express_with_tracking_config(s3_express_config);

                let az_status_cache = Arc::new(
                    Cache::builder()
                        .time_to_idle(Duration::from_secs(30))
                        .max_capacity(100)
                        .build(),
                );

                let storage = BlobStorageImpl::S3ExpressMultiAzWithTracking(
                    S3ExpressMultiAzWithTracking::new(
                        &express_config,
                        data_blob_tracker,
                        az_status_cache.clone(),
                    )
                    .await?,
                );

                return Ok((Arc::new(storage), Some(az_status_cache)));
            }
            BlobStorageBackend::S3ExpressSingleAz => {
                let s3_express_single_az_config =
                    Self::get_s3_express_single_az_config(blob_storage_config)?;
                let single_az_config =
                    Self::build_s3_express_single_az_config(s3_express_single_az_config);
                BlobStorageImpl::S3ExpressSingleAz(
                    S3ExpressSingleAzStorage::new(&single_az_config).await?,
                )
            }
        };

        Ok((Arc::new(storage), None))
    }

    fn create_client_with_task(
        storage: Arc<BlobStorageImpl>,
        rx: Receiver<(Option<String>, BlobId, usize)>,
    ) -> Self {
        let blob_deletion_task_handle = tokio::spawn({
            let storage = storage.clone();
            async move {
                if let Err(e) = Self::blob_deletion_task(storage, rx).await {
                    tracing::error!("FATAL: blob deletion task error: {e}");
                }
            }
        });

        Self {
            storage,
            blob_deletion_task_handle,
        }
    }

    fn get_bss_config<'a>(
        blob_storage_config: &'a BlobStorageConfig,
        backend_name: &str,
    ) -> Result<&'a crate::config::BssConfig, BlobStorageError> {
        blob_storage_config.bss.as_ref().ok_or_else(|| {
            BlobStorageError::Config(format!(
                "BSS configuration required for {backend_name} backend"
            ))
        })
    }

    fn get_s3_express_multi_az_config<'a>(
        blob_storage_config: &'a BlobStorageConfig,
        backend_name: &str,
    ) -> Result<&'a crate::config::S3ExpressMultiAzConfig, BlobStorageError> {
        blob_storage_config
            .s3_express_multi_az
            .as_ref()
            .ok_or_else(|| {
                BlobStorageError::Config(format!(
                    "S3 Express configuration required for {backend_name} backend"
                ))
            })
    }

    fn get_s3_express_single_az_config(
        blob_storage_config: &BlobStorageConfig,
    ) -> Result<&crate::config::S3ExpressSingleAzConfig, BlobStorageError> {
        blob_storage_config
            .s3_express_single_az
            .as_ref()
            .ok_or_else(|| {
                BlobStorageError::Config(
                    "S3 Express Single AZ configuration required for S3ExpressSingleAz backend"
                        .into(),
                )
            })
    }

    fn build_s3_express_multi_az_config(
        s3_express_config: &crate::config::S3ExpressMultiAzConfig,
    ) -> crate::blob_storage::S3ExpressMultiAzConfig {
        crate::blob_storage::S3ExpressMultiAzConfig {
            local_az_host: s3_express_config.local_az_host.clone(),
            local_az_port: s3_express_config.local_az_port,
            remote_az_host: s3_express_config.remote_az_host.clone(),
            remote_az_port: s3_express_config.remote_az_port,
            s3_region: s3_express_config.s3_region.clone(),
            local_az_bucket: s3_express_config.local_az_bucket.clone(),
            remote_az_bucket: s3_express_config.remote_az_bucket.clone(),
            az: s3_express_config.local_az.clone(),
            rate_limit_config: crate::blob_storage::S3RateLimitConfig::from(
                &s3_express_config.ratelimit,
            ),
            retry_config: s3_express_config.retry_config.clone(),
        }
    }

    fn build_s3_express_with_tracking_config(
        s3_express_config: &crate::config::S3ExpressMultiAzConfig,
    ) -> S3ExpressWithTrackingConfig {
        S3ExpressWithTrackingConfig {
            local_az_host: s3_express_config.local_az_host.clone(),
            local_az_port: s3_express_config.local_az_port,
            s3_region: s3_express_config.s3_region.clone(),
            local_az_bucket: s3_express_config.local_az_bucket.clone(),
            remote_az_bucket: s3_express_config.remote_az_bucket.clone(),
            remote_az_host: s3_express_config.remote_az_host.clone(),
            remote_az_port: s3_express_config.remote_az_port,
            local_az: s3_express_config.local_az.clone(),
            remote_az: s3_express_config.remote_az.clone(),
            rate_limit_config: crate::blob_storage::S3RateLimitConfig::from(
                &s3_express_config.ratelimit,
            ),
            retry_config: s3_express_config.retry_config.clone(),
        }
    }

    fn build_s3_express_single_az_config(
        s3_express_single_az_config: &crate::config::S3ExpressSingleAzConfig,
    ) -> crate::blob_storage::S3ExpressSingleAzConfig {
        crate::blob_storage::S3ExpressSingleAzConfig {
            s3_host: s3_express_single_az_config.s3_host.clone(),
            s3_port: s3_express_single_az_config.s3_port,
            s3_region: s3_express_single_az_config.s3_region.clone(),
            s3_bucket: s3_express_single_az_config.s3_bucket.clone(),
            az: s3_express_single_az_config.az.clone(),
            force_path_style: s3_express_single_az_config.force_path_style,
            rate_limit_config: crate::blob_storage::S3RateLimitConfig::from(
                &s3_express_single_az_config.ratelimit,
            ),
            retry_config: s3_express_single_az_config.retry_config.clone(),
        }
    }

    async fn blob_deletion_task(
        storage: Arc<BlobStorageImpl>,
        mut input: Receiver<(Option<String>, BlobId, usize)>,
    ) -> Result<(), BlobStorageError> {
        while let Some((tracking_root_blob_name, blob_id, block_numbers)) = input.recv().await {
            let deleted = stream::iter(0..block_numbers)
                .map(|block_number| {
                    let storage = storage.clone();
                    let tracking_root = tracking_root_blob_name.clone();
                    async move {
                        let res = storage
                            .delete_blob(tracking_root.as_deref(), blob_id, block_number as u32)
                            .await;
                        match res {
                            Ok(()) => 1,
                            Err(e) => {
                                tracing::warn!("delete {blob_id}-p{block_number} failed: {e}");
                                0
                            }
                        }
                    }
                })
                .buffer_unordered(10)
                .fold(0, |acc, x| async move { acc + x })
                .await;
            let failed = block_numbers - deleted;
            if failed != 0 {
                tracing::warn!("delete parts of {blob_id}: ok={deleted},err={failed}");
            }
        }
        Ok(())
    }

    pub async fn put_blob(
        &self,
        tracking_root_blob_name: Option<&str>,
        blob_id: Uuid,
        block_number: u32,
        body: Bytes,
    ) -> Result<(), BlobStorageError> {
        self.storage
            .put_blob(tracking_root_blob_name, blob_id, block_number, body)
            .await
    }

    pub async fn get_blob(
        &self,
        blob_id: Uuid,
        block_number: u32,
        body: &mut Bytes,
    ) -> Result<(), BlobStorageError> {
        self.storage.get_blob(blob_id, block_number, body).await
    }

    pub async fn delete_blob(
        &self,
        tracking_root_blob_name: Option<&str>,
        blob_id: Uuid,
        block_number: u32,
    ) -> Result<(), BlobStorageError> {
        self.storage
            .delete_blob(tracking_root_blob_name, blob_id, block_number)
            .await
    }
}
