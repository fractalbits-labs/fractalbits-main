pub mod api_key_routes;
pub mod blob_client;
mod blob_storage;
pub mod cache_mgmt;
mod cache_registry;
mod config;
pub mod handler;
mod object_layout;

pub use blob_client::BlobClient;
use blob_client::BlobDeletionRequest;
pub use config::{BlobStorageBackend, BlobStorageConfig, Config, S3HybridSingleAzConfig};
pub use data_blob_tracking::{DataBlobTracker, DataBlobTrackingError};
use data_types::{ApiKey, Bucket, Versioned};
use metrics::counter;
use moka::future::Cache;
use rpc_client_common::{RpcError, rss_rpc_retry};
use rpc_client_nss::RpcClientNss;
use rpc_client_rss::RpcClientRss;

pub use cache_registry::CacheCoordinator;
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU32, Ordering},
    },
    time::Duration,
};
use tokio::sync::{
    OnceCell,
    mpsc::{self, Sender},
};
use tracing::debug;
pub type BlobId = uuid::Uuid;

thread_local! {
    static REQUEST_COUNTER: AtomicU32 = const { AtomicU32::new(0) };
}

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

pub struct AppState {
    pub config: Arc<Config>,
    pub cache: Arc<Cache<String, Versioned<String>>>,
    pub cache_coordinator: Arc<CacheCoordinator<Versioned<String>>>,
    pub az_status_coordinator: Arc<CacheCoordinator<String>>,
    pub az_status_enabled: AtomicBool,
    pub worker_id: u16,

    rpc_client_nss: RpcClientNss,
    rpc_client_rss: RpcClientRss,

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
        worker_id: u16,
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

        let rpc_client_nss = RpcClientNss::new_from_address(config.nss_addr.clone());
        let rpc_client_rss = RpcClientRss::new_from_address(config.rss_addr.clone());

        Self {
            config,
            rpc_client_nss,
            rpc_client_rss,
            blob_client: OnceCell::new(),
            blob_deletion_tx: tx,
            cache,
            cache_coordinator,
            az_status_coordinator,
            az_status_enabled: AtomicBool::new(false),
            worker_id,
            data_blob_tracker: OnceCell::new(),
        }
    }

    pub fn generate_trace_id(&self) -> u64 {
        let counter = REQUEST_COUNTER.with(|c| c.fetch_add(1, Ordering::Relaxed));
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        // Format: [8 bits worker_id][32 bits timestamp][24 bits counter]
        ((self.worker_id as u64) << 56)
            | ((timestamp & 0xFFFFFFFF) << 24)
            | ((counter as u64) & 0xFFFFFF)
    }

    pub fn get_nss_rpc_client(&self) -> &RpcClientNss {
        &self.rpc_client_nss
    }

    pub fn get_rss_rpc_client(&self) -> &RpcClientRss {
        &self.rpc_client_rss
    }

    pub async fn get_blob_client(&self) -> Result<Arc<BlobClient>, String> {
        self.blob_client
            .get_or_try_init(|| async {
                debug!("Creating per-worker BlobClient on-demand");
                let (_tx, rx) = mpsc::channel::<BlobDeletionRequest>(1024 * 1024);

                debug!("Fetching DataVgInfo from RSS at {}", self.config.rss_addr);
                let rss_client = self.get_rss_rpc_client();

                let data_vg_info = rss_client
                    .get_data_vg_info(Some(self.config.rpc_timeout()), None)
                    .await
                    .map_err(|e| format!("Failed to fetch DataVgInfo from RSS: {}", e))?;

                debug!(
                    "Successfully fetched DataVgInfo with {} volumes",
                    data_vg_info.volumes.len()
                );

                let (blob_client, az_status_cache) = BlobClient::new_with_data_vg_info(
                    &self.config.blob_storage,
                    rx,
                    self.config.rpc_timeout(),
                    None,
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

        let rss_client = self.get_rss_rpc_client();
        let (version, data) = rss_rpc_retry!(
            rss_client,
            get(&full_key, Some(self.config.rpc_timeout()), None)
        )
        .await?;
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

        let rss_client = self.get_rss_rpc_client();
        rss_rpc_retry!(
            rss_client,
            put(
                versioned_data.version,
                &full_key,
                &versioned_data.data,
                Some(self.config.rpc_timeout()),
                None
            )
        )
        .await?;

        tracing::debug!("caching data with full_key: {full_key}");
        self.cache.insert(full_key, versioned_data).await;
        Ok(())
    }

    pub async fn delete_api_key(&self, api_key: &ApiKey) -> Result<(), RpcError> {
        let full_key = format!("api_key:{}", api_key.key_id);
        let rss_client = self.get_rss_rpc_client();
        rss_rpc_retry!(
            rss_client,
            delete(&full_key, Some(self.config.rpc_timeout()), None)
        )
        .await?;
        self.cache.invalidate(&full_key).await;
        Ok(())
    }

    pub async fn list_api_keys(&self) -> Result<Vec<ApiKey>, RpcError> {
        let prefix = "api_key:".to_string();
        let rss_client = self.get_rss_rpc_client();
        let kvs = rss_rpc_retry!(
            rss_client,
            list(&prefix, Some(self.config.rpc_timeout()), None)
        )
        .await?;
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

        let rss_client = self.get_rss_rpc_client();
        let (version, data) = rss_rpc_retry!(
            rss_client,
            get(&full_key, Some(self.config.rpc_timeout()), None)
        )
        .await?;
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
        let rss_client = self.get_rss_rpc_client();
        rss_rpc_retry!(
            rss_client,
            create_bucket(
                bucket_name,
                api_key_id,
                is_multi_az,
                Some(self.config.rpc_timeout()),
                None
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
        let rss_client = self.get_rss_rpc_client();
        rss_rpc_retry!(
            rss_client,
            delete_bucket(
                bucket_name,
                api_key_id,
                Some(self.config.rpc_timeout()),
                None
            )
        )
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
        let rss_client = self.get_rss_rpc_client();
        let kvs = rss_rpc_retry!(
            rss_client,
            list(&prefix, Some(self.config.rpc_timeout()), None)
        )
        .await?;
        Ok(kvs
            .iter()
            .map(|x| serde_json::from_slice(x.as_bytes()).unwrap())
            .collect())
    }
}
