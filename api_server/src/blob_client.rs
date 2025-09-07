use crate::{
    blob_storage::{
        BlobStorage, BlobStorageError, BlobStorageImpl, DataBlobGuid, S3ExpressMultiAzStorage,
        S3HybridSingleAzStorage,
    },
    config::{BlobStorageBackend, BlobStorageConfig},
};
use bytes::Bytes;
use data_blob_tracking::DataBlobTracker;
use futures::stream::{self, StreamExt};
use moka::future::Cache;
use std::{sync::Arc, time::Duration};
use tokio::{sync::mpsc::Receiver, task::JoinHandle};

pub struct BlobClient {
    storage: Arc<BlobStorageImpl>,
    #[allow(dead_code)]
    blob_deletion_task_handle: JoinHandle<()>,
}

impl BlobClient {
    pub async fn new(
        blob_storage_config: &BlobStorageConfig,
        rx: Receiver<(Option<String>, DataBlobGuid, usize)>,
        rpc_timeout: Duration,
        data_blob_tracker: Option<Arc<DataBlobTracker>>,
        rss_clients: Option<
            Arc<slotmap_conn_pool::ConnPool<Arc<rpc_client_rss::RpcClientRss>, String>>,
        >,
        rss_addr: Option<String>,
    ) -> Result<(Self, Option<Arc<Cache<String, String>>>), BlobStorageError> {
        let (storage, az_status_cache) = Self::create_storage_impl(
            blob_storage_config,
            rpc_timeout,
            data_blob_tracker,
            rss_clients,
            rss_addr,
        )
        .await?;

        let client = Self::create_client_with_task(storage, rx);
        Ok((client, az_status_cache))
    }

    async fn create_storage_impl(
        blob_storage_config: &BlobStorageConfig,
        rpc_timeout: Duration,
        data_blob_tracker: Option<Arc<DataBlobTracker>>,
        rss_clients: Option<
            Arc<slotmap_conn_pool::ConnPool<Arc<rpc_client_rss::RpcClientRss>, String>>,
        >,
        rss_addr: Option<String>,
    ) -> Result<(Arc<BlobStorageImpl>, Option<Arc<Cache<String, String>>>), BlobStorageError> {
        let storage = match &blob_storage_config.backend {
            BlobStorageBackend::S3HybridSingleAz => {
                let s3_hybrid_config = blob_storage_config
                    .s3_hybrid_single_az
                    .as_ref()
                    .ok_or_else(|| {
                        BlobStorageError::Config(
                            "S3 hybrid configuration required for Hybrid backend".into(),
                        )
                    })?;

                let rss_client = rss_clients
                    .ok_or_else(|| {
                        BlobStorageError::Config(
                            "RSS client required for S3 hybrid backend with DataVgProxy".into(),
                        )
                    })?
                    .checkout(rss_addr.ok_or_else(|| {
                        BlobStorageError::Config(
                            "RSS address required for S3 hybrid backend".into(),
                        )
                    })?)
                    .await
                    .map_err(|e| {
                        BlobStorageError::Config(format!("Failed to checkout RSS client: {}", e))
                    })?;

                BlobStorageImpl::HybridSingleAz(
                    S3HybridSingleAzStorage::new(rss_client, s3_hybrid_config, rpc_timeout).await?,
                )
            }
            BlobStorageBackend::S3ExpressMultiAz => {
                let data_blob_tracker = data_blob_tracker.ok_or_else(|| {
                    BlobStorageError::Config(
                        "DataBlobTracker required for S3ExpressWithTracking backend".into(),
                    )
                })?;

                let s3_express_multi_az_config =
                    Self::get_s3_express_multi_az_config(blob_storage_config, "S3ExpressMultiAz")?;

                let az_status_cache = Arc::new(
                    Cache::builder()
                        .time_to_idle(Duration::from_secs(30))
                        .max_capacity(100)
                        .build(),
                );

                let storage = BlobStorageImpl::S3ExpressMultiAz(
                    S3ExpressMultiAzStorage::new(
                        s3_express_multi_az_config,
                        data_blob_tracker,
                        az_status_cache.clone(),
                    )
                    .await?,
                );

                return Ok((Arc::new(storage), Some(az_status_cache)));
            }
        };

        Ok((Arc::new(storage), None))
    }

    fn create_client_with_task(
        storage: Arc<BlobStorageImpl>,
        rx: Receiver<(Option<String>, DataBlobGuid, usize)>,
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

    async fn blob_deletion_task(
        storage: Arc<BlobStorageImpl>,
        mut input: Receiver<(Option<String>, DataBlobGuid, usize)>,
    ) -> Result<(), BlobStorageError> {
        while let Some((tracking_root_blob_name, blob_guid, block_numbers)) = input.recv().await {
            let deleted = stream::iter(0..block_numbers)
                .map(|block_number| {
                    let storage = storage.clone();
                    let tracking_root = tracking_root_blob_name.clone();
                    async move {
                        let res = storage
                            .delete_blob(tracking_root.as_deref(), blob_guid, block_number as u32)
                            .await;
                        match res {
                            Ok(()) => 1,
                            Err(e) => {
                                tracing::warn!("delete {blob_guid}-p{block_number} failed: {e}");
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
                tracing::warn!("delete parts of {blob_guid}: ok={deleted},err={failed}");
            }
        }
        Ok(())
    }

    pub fn create_data_blob_guid(&self) -> DataBlobGuid {
        match &*self.storage {
            BlobStorageImpl::HybridSingleAz(storage) => storage.create_data_blob_guid(),
            BlobStorageImpl::S3ExpressMultiAz(storage) => storage.create_data_blob_guid(),
        }
    }

    pub async fn put_blob(
        &self,
        tracking_root_blob_name: Option<&str>,
        blob_guid: DataBlobGuid,
        block_number: u32,
        body: Bytes,
    ) -> Result<DataBlobGuid, BlobStorageError> {
        self.storage
            .put_blob(
                tracking_root_blob_name,
                blob_guid.blob_id,
                blob_guid.volume_id,
                block_number,
                body,
            )
            .await
    }

    pub async fn get_blob(
        &self,
        blob_guid: DataBlobGuid,
        block_number: u32,
        body: &mut Bytes,
    ) -> Result<(), BlobStorageError> {
        self.storage.get_blob(blob_guid, block_number, body).await
    }

    pub async fn delete_blob(
        &self,
        tracking_root_blob_name: Option<&str>,
        blob_guid: DataBlobGuid,
        block_number: u32,
    ) -> Result<(), BlobStorageError> {
        self.storage
            .delete_blob(tracking_root_blob_name, blob_guid, block_number)
            .await
    }
}
