mod blob_client;
mod blob_storage;
mod config;
pub mod handler;
mod object_layout;
pub mod runtime;
pub mod uring;

use blob_client::{BlobClient, BlobDeletionRequest};
pub use config::{BlobStorageBackend, BlobStorageConfig, Config, S3HybridSingleAzConfig};
pub use data_blob_tracking::{DataBlobTracker, DataBlobTrackingError};
use data_types::{ApiKey, Bucket, Versioned};
use metrics::{counter, histogram};
use moka::future::Cache;
use rpc_client_common::{
    RpcError, rss_rpc_retry,
    transport::{RpcTransport, clear_current_transport, set_current_transport},
};
use rpc_client_nss::RpcClientNss;
use rpc_client_rss::RpcClientRss;
pub use runtime::per_core::PerCoreContext;
use uring::reactor::RpcReactorHandle;
pub use uring::ring::PerCoreRing;

use slotmap_conn_pool::{ConnPool, Poolable};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::mpsc::{self, Sender};
use tracing::info;
pub type BlobId = uuid::Uuid;

pub struct AppState {
    pub config: Arc<Config>,
    pub cache: Arc<Cache<String, Versioned<String>>>,
    pub az_status_cache: Option<Arc<Cache<String, String>>>,

    rpc_clients_nss: ConnPool<Arc<RpcClientNss>, String>,
    rpc_clients_rss: ConnPool<Arc<RpcClientRss>, String>,

    blob_client: Arc<BlobClient>,
    blob_deletion: Sender<BlobDeletionRequest>,
    pub data_blob_tracker: Arc<DataBlobTracker>,
    pub per_core_ring: Arc<PerCoreRing>,
    pub rpc_handle: Arc<RpcReactorHandle>,
}

impl AppState {
    const PER_CORE_RPC_POOL_SIZE: u16 = 1;
    const PER_CORE_CACHE_CAPACITY: u64 = 10_000;

    pub async fn new_per_core(
        config: Arc<Config>,
        per_core_ring: Arc<PerCoreRing>,
        rpc_handle: Arc<RpcReactorHandle>,
    ) -> Self {
        let reactor_transport: Arc<dyn RpcTransport> = Arc::new(
            crate::uring::reactor::ReactorTransport::new(rpc_handle.clone()),
        );
        set_current_transport(Some(reactor_transport));
        let rpc_clients_rss =
            Self::new_rpc_clients_pool_rss(&config.rss_addr, Self::PER_CORE_RPC_POOL_SIZE).await;
        let rpc_clients_nss =
            Self::new_rpc_clients_pool_nss(&config.nss_addr, Self::PER_CORE_RPC_POOL_SIZE).await;

        let (tx, rx) = mpsc::channel(1024 * 1024);
        let data_blob_tracker = Arc::new(DataBlobTracker::with_pools(
            config.rss_addr.clone(),
            rpc_clients_rss.clone(),
            config.nss_addr.clone(),
            rpc_clients_nss.clone(),
        ));

        let cache = Arc::new(
            Cache::builder()
                .time_to_idle(Duration::from_secs(300))
                .max_capacity(Self::PER_CORE_CACHE_CAPACITY)
                .build(),
        );

        let (blob_client, az_status_cache) = BlobClient::new(
            &config.blob_storage,
            rx,
            config.rpc_timeout(),
            Self::PER_CORE_RPC_POOL_SIZE,
            Some(data_blob_tracker.clone()),
            Some(Arc::new(rpc_clients_rss.clone())),
            Some(config.rss_addr.clone()),
        )
        .await
        .expect("Failed to initialize blob client");

        let state = Self {
            config,
            rpc_clients_nss,
            blob_client: Arc::new(blob_client),
            blob_deletion: tx,
            rpc_clients_rss,
            cache,
            az_status_cache,
            data_blob_tracker,
            per_core_ring,
            rpc_handle,
        };

        clear_current_transport();
        state
    }

    async fn new_rpc_clients_pool_nss(
        nss_addr: &str,
        nss_conn_num: u16,
    ) -> ConnPool<Arc<RpcClientNss>, String> {
        let rpc_clients_nss = ConnPool::new();

        // Use the Poolable trait's retry logic instead of manual retry
        for i in 0..nss_conn_num as usize {
            info!(
                "Connecting to NSS server at {nss_addr} (connection {}/{})",
                i + 1,
                nss_conn_num
            );
            let client = Arc::new(
                <RpcClientNss as slotmap_conn_pool::Poolable>::new(nss_addr.to_string())
                    .await
                    .unwrap(),
            );
            rpc_clients_nss.pooled(nss_addr.to_string(), client);
        }

        info!("NSS RPC client pool initialized with {nss_conn_num} connections.");
        rpc_clients_nss
    }

    async fn new_rpc_clients_pool_rss(
        rss_addr: &str,
        rss_conn_num: u16,
    ) -> ConnPool<Arc<RpcClientRss>, String> {
        let rpc_clients_rss = ConnPool::new();

        // Use the Poolable trait's retry logic
        for i in 0..rss_conn_num as usize {
            info!(
                "Connecting to RSS server at {rss_addr} (connection {}/{})",
                i + 1,
                rss_conn_num
            );
            let client = Arc::new(
                <RpcClientRss as slotmap_conn_pool::Poolable>::new(rss_addr.to_string())
                    .await
                    .unwrap(),
            );
            rpc_clients_rss.pooled(rss_addr.to_string(), client);
        }

        info!("RSS RPC client pool initialized with {rss_conn_num} connections.");
        rpc_clients_rss
    }

    pub async fn checkout_rpc_client_nss(
        &self,
    ) -> Result<Arc<RpcClientNss>, <RpcClientNss as Poolable>::Error> {
        self.checkout_nss_internal(None).await
    }

    pub async fn checkout_with_session_nss(
        &self,
        session_id: u64,
    ) -> Result<Arc<RpcClientNss>, <RpcClientNss as Poolable>::Error> {
        self.checkout_nss_internal(Some(session_id)).await
    }

    pub async fn checkout_rpc_client_rss(
        &self,
    ) -> Result<Arc<RpcClientRss>, <RpcClientRss as Poolable>::Error> {
        self.checkout_rss_internal(None).await
    }

    pub async fn checkout_with_session_rss(
        &self,
        session_id: u64,
    ) -> Result<Arc<RpcClientRss>, <RpcClientRss as Poolable>::Error> {
        self.checkout_rss_internal(Some(session_id)).await
    }

    async fn checkout_nss_internal(
        &self,
        session_id: Option<u64>,
    ) -> Result<Arc<RpcClientNss>, <RpcClientNss as Poolable>::Error> {
        let start = Instant::now();
        let res = match session_id {
            Some(id) => {
                self.rpc_clients_nss
                    .checkout_with_session(self.config.nss_addr.clone(), id)
                    .await?
            }
            None => {
                self.rpc_clients_nss
                    .checkout(self.config.nss_addr.clone())
                    .await?
            }
        };
        histogram!("checkout_rpc_client_nanos", "type" => "nss")
            .record(start.elapsed().as_nanos() as f64);
        Ok(res)
    }

    async fn checkout_rss_internal(
        &self,
        session_id: Option<u64>,
    ) -> Result<Arc<RpcClientRss>, <RpcClientRss as Poolable>::Error> {
        let start = Instant::now();
        let res = match session_id {
            Some(id) => {
                self.rpc_clients_rss
                    .checkout_with_session(self.config.rss_addr.clone(), id)
                    .await?
            }
            None => {
                self.rpc_clients_rss
                    .checkout(self.config.rss_addr.clone())
                    .await?
            }
        };
        histogram!("checkout_rpc_client_nanos", "type" => "rss")
            .record(start.elapsed().as_nanos() as f64);
        Ok(res)
    }

    pub fn get_blob_client(&self) -> Arc<BlobClient> {
        self.blob_client.clone()
    }

    pub fn get_blob_deletion(&self) -> Sender<BlobDeletionRequest> {
        self.blob_deletion.clone()
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
