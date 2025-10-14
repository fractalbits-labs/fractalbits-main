pub mod blob_client;
mod blob_storage;
mod cache_registry;
mod config;
pub mod handler;
mod object_layout;
pub mod runtime;

pub use blob_client::BlobClient;
use blob_client::BlobDeletionRequest;
pub use config::{BlobStorageBackend, BlobStorageConfig, Config, S3HybridSingleAzConfig};
pub use data_blob_tracking::{DataBlobTracker, DataBlobTrackingError};
use data_types::{ApiKey, Bucket, Versioned};
use metrics::counter;
use moka::future::Cache;
use rpc_client_common::{RpcError, checkout_rpc_client, rss_rpc_retry};
use rpc_client_nss::RpcClientNss;
use rpc_client_rss::RpcClientRss;
pub use runtime::per_core::PerCoreContext;

pub use cache_registry::CacheCoordinator;
use std::{
    sync::{Arc, atomic::AtomicBool},
    time::Duration,
};
use tokio::sync::{
    OnceCell, RwLock,
    mpsc::{self, Sender},
};
use tracing::debug;
pub type BlobId = uuid::Uuid;

pub struct AppState {
    pub config: Arc<Config>,
    pub cache: Arc<Cache<String, Versioned<String>>>,
    pub cache_coordinator: Arc<CacheCoordinator<Versioned<String>>>,
    pub az_status_coordinator: Arc<CacheCoordinator<String>>,
    pub az_status_enabled: AtomicBool,

    rpc_client_nss: RwLock<Option<Arc<RpcClientNss>>>,
    rpc_client_rss: RwLock<Option<Arc<RpcClientRss>>>,

    blob_client: OnceCell<Arc<BlobClient>>,
    blob_deletion_tx: Sender<BlobDeletionRequest>,
    pub data_blob_tracker: OnceCell<Arc<DataBlobTracker>>,
}

impl AppState {
    const PER_CORE_CACHE_CAPACITY: u64 = 10_000;

    pub fn new_per_core_sync(
        config: Arc<Config>,
        cache_coordinator: Arc<CacheCoordinator<Versioned<String>>>,
        az_status_coordinator: Arc<CacheCoordinator<String>>,
    ) -> Self {
        debug!("Initializing per-core AppState with lazy RPC client connections");

        let (tx, _rx) = mpsc::channel(1024 * 1024);

        let cache = Arc::new(
            Cache::builder()
                .time_to_idle(Duration::from_secs(300))
                .max_capacity(Self::PER_CORE_CACHE_CAPACITY)
                .build(),
        );
        cache_coordinator.register_cache(cache.clone());

        debug!("Per-core AppState initialized with lazy BlobClient initialization");

        Self {
            config,
            rpc_client_nss: RwLock::new(None),
            rpc_client_rss: RwLock::new(None),
            blob_client: OnceCell::new(),
            blob_deletion_tx: tx,
            cache,
            cache_coordinator,
            az_status_coordinator,
            az_status_enabled: AtomicBool::new(false),
            data_blob_tracker: OnceCell::new(),
        }
    }

    pub async fn checkout_rpc_client_nss(
        &self,
    ) -> Result<Arc<RpcClientNss>, Box<dyn std::error::Error + Send + Sync>> {
        checkout_rpc_client(
            &self.rpc_client_nss,
            &self.config.nss_addr,
            |addr| async move { RpcClientNss::new_from_address(addr).await },
        )
        .await
    }

    pub async fn checkout_with_session_nss(
        &self,
        _session_id: u64,
    ) -> Result<Arc<RpcClientNss>, Box<dyn std::error::Error + Send + Sync>> {
        self.checkout_rpc_client_nss().await
    }

    pub async fn checkout_rpc_client_rss(
        &self,
    ) -> Result<Arc<RpcClientRss>, Box<dyn std::error::Error + Send + Sync>> {
        checkout_rpc_client(
            &self.rpc_client_rss,
            &self.config.rss_addr,
            |addr| async move { RpcClientRss::new_from_address(addr).await },
        )
        .await
    }

    pub async fn checkout_with_session_rss(
        &self,
        _session_id: u64,
    ) -> Result<Arc<RpcClientRss>, Box<dyn std::error::Error + Send + Sync>> {
        self.checkout_rpc_client_rss().await
    }

    pub async fn get_blob_client(&self) -> Result<Arc<BlobClient>, String> {
        self.blob_client
            .get_or_try_init(|| async {
                debug!("Creating per-worker BlobClient on-demand");
                let (_tx, rx) = mpsc::channel::<BlobDeletionRequest>(1024 * 1024);

                let data_vg_info = self
                    .config
                    .data_vg_info
                    .clone()
                    .ok_or_else(|| "DataVgInfo not available".to_string())?;

                let (blob_client, az_status_cache) = BlobClient::new_with_data_vg_info(
                    &self.config.blob_storage,
                    rx,
                    self.config.rpc_timeout(),
                    None, // data_blob_tracker created separately if needed
                    data_vg_info,
                )
                .await
                .map_err(|e| e.to_string())?;

                // Register az_status_cache if present (only for Multi-AZ)
                if let Some(cache) = az_status_cache {
                    self.az_status_coordinator.register_cache(cache);
                }

                Ok(Arc::new(blob_client))
            })
            .await
            .cloned()
    }

    pub fn get_blob_deletion(&self) -> Sender<BlobDeletionRequest> {
        self.blob_deletion_tx.clone()
    }
}

// API Key operations
impl AppState {
    pub async fn get_api_key(&self, key_id: String) -> Result<Versioned<ApiKey>, RpcError> {
        let full_key = format!("api_key:{key_id}");
        if let Some(json) = self.cache.get(&full_key).await {
            counter!("api_key_cache_hit").increment(1);
            tracing::debug!("get cached data with full_key: {full_key}");
            return Ok((
                json.version,
                serde_json::from_slice(json.data.as_bytes()).unwrap(),
            )
                .into());
        } else {
            counter!("api_key_cache_miss").increment(1);
        }

        let (version, data) =
            rss_rpc_retry!(self, get(&full_key, Some(self.config.rpc_timeout()))).await?;
        let json = Versioned::new(version, data);
        self.cache.insert(full_key, json.clone()).await;
        Ok((
            json.version,
            serde_json::from_slice(json.data.as_bytes()).unwrap(),
        )
            .into())
    }

    pub async fn get_test_api_key(&self) -> Result<Versioned<ApiKey>, RpcError> {
        self.get_api_key("test_api_key".into()).await
    }

    pub async fn put_api_key(&self, api_key: &Versioned<ApiKey>) -> Result<(), RpcError> {
        let full_key = format!("api_key:{}", api_key.data.key_id);
        let data: String = serde_json::to_string(&api_key.data).unwrap();
        let versioned_data: Versioned<String> = (api_key.version, data).into();

        rss_rpc_retry!(
            self,
            put(
                versioned_data.version,
                &full_key,
                &versioned_data.data,
                Some(self.config.rpc_timeout())
            )
        )
        .await?;

        tracing::debug!("caching data with full_key: {full_key}");
        self.cache.insert(full_key, versioned_data).await;
        Ok(())
    }

    pub async fn delete_api_key(&self, api_key: &ApiKey) -> Result<(), RpcError> {
        let full_key = format!("api_key:{}", api_key.key_id);
        rss_rpc_retry!(self, delete(&full_key, Some(self.config.rpc_timeout()))).await?;
        self.cache.invalidate(&full_key).await;
        Ok(())
    }

    pub async fn list_api_keys(&self) -> Result<Vec<ApiKey>, RpcError> {
        let prefix = "api_key:".to_string();
        let kvs = rss_rpc_retry!(self, list(&prefix, Some(self.config.rpc_timeout()))).await?;
        Ok(kvs
            .iter()
            .map(|x| serde_json::from_slice(x.as_bytes()).unwrap())
            .collect())
    }
}

// Bucket operations
impl AppState {
    pub async fn get_bucket(&self, bucket_name: String) -> Result<Versioned<Bucket>, RpcError> {
        let full_key = format!("bucket:{bucket_name}");
        if let Some(json) = self.cache.get(&full_key).await {
            counter!("bucket_cache_hit").increment(1);
            tracing::debug!("get cached data with full_key: {full_key}");
            return Ok((
                json.version,
                serde_json::from_slice(json.data.as_bytes()).unwrap(),
            )
                .into());
        } else {
            counter!("bucket_cache_miss").increment(1);
        }

        let (version, data) =
            rss_rpc_retry!(self, get(&full_key, Some(self.config.rpc_timeout()))).await?;
        let json = Versioned::new(version, data);
        self.cache.insert(full_key, json.clone()).await;
        Ok((
            json.version,
            serde_json::from_slice(json.data.as_bytes()).unwrap(),
        )
            .into())
    }

    pub async fn create_bucket(
        &self,
        bucket_name: &str,
        api_key_id: &str,
        is_multi_az: bool,
    ) -> Result<(), RpcError> {
        rss_rpc_retry!(
            self,
            create_bucket(
                bucket_name,
                api_key_id,
                is_multi_az,
                Some(self.config.rpc_timeout())
            )
        )
        .await?;

        // Invalidate API key cache since it now has new bucket permissions
        self.cache
            .invalidate(&format!("api_key:{api_key_id}"))
            .await;
        Ok(())
    }

    pub async fn delete_bucket(&self, bucket_name: &str, api_key_id: &str) -> Result<(), RpcError> {
        let client = self.checkout_rpc_client_rss().await.map_err(|e| {
            RpcError::InternalRequestError(format!("Failed to checkout RSS client: {}", e))
        })?;
        client
            .delete_bucket(bucket_name, api_key_id, Some(self.config.rpc_timeout()))
            .await?;

        // Invalidate both bucket and API key cache
        self.cache
            .invalidate(&format!("bucket:{bucket_name}"))
            .await;
        self.cache
            .invalidate(&format!("api_key:{api_key_id}"))
            .await;
        Ok(())
    }

    pub async fn list_buckets(&self) -> Result<Vec<Bucket>, RpcError> {
        let prefix = "bucket:".to_string();
        let client = self.checkout_rpc_client_rss().await.map_err(|e| {
            RpcError::InternalRequestError(format!("Failed to checkout RSS client: {}", e))
        })?;
        let kvs = client
            .list(&prefix, Some(self.config.rpc_timeout()), 0)
            .await?;
        Ok(kvs
            .iter()
            .map(|x| serde_json::from_slice(x.as_bytes()).unwrap())
            .collect())
    }
}
