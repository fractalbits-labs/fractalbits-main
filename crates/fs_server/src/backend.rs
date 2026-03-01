#![allow(clippy::await_holding_refcell_ref)]

use crate::rpc::RpcError;
use bytes::Bytes;
use data_types::{Bucket, DataBlobGuid, DataVgInfo, TraceId};
use std::cell::RefCell;
use std::time::{Duration, Instant};

use crate::config::Config;
use crate::error::FuseError;
use crate::rpc_bss::DataVgProxy;
use crate::rpc_nss::NssRpcClient;
use crate::rpc_rss::RssRpcClient;
use data_types::object_layout::{ObjectCoreMetaData, ObjectLayout, ObjectMetaData, ObjectState};

/// Represents a listed entry from NSS
pub struct ListEntry {
    pub key: String,
    pub layout: Option<ObjectLayout>,
}

/// Discovered configuration from RSS (shared across threads).
pub struct BackendConfig {
    pub nss_address: String,
    pub data_vg_info: DataVgInfo,
    pub root_blob_name: String,
    pub config: Config,
}

impl BackendConfig {
    /// Perform one-time initialization: discover NSS address, DataVgInfo, bucket info from RSS.
    /// This runs on a compio runtime and creates temporary RPC connections.
    pub async fn discover(config: &Config) -> Result<Self, String> {
        let trace_id = TraceId::new();

        // 1. Create RSS client
        let rss_client = RssRpcClient::new_from_addresses(config.rss_addrs.clone());

        // 2. Get active NSS address from RSS
        let nss_addr = rss_client
            .get_active_nss_address(Some(config.rss_rpc_timeout()), &trace_id)
            .await
            .map_err(|e| format!("Failed to get NSS address from RSS: {e}"))?;
        tracing::info!("Got NSS address: {nss_addr}");

        // 3. Get DataVgInfo from RSS
        let data_vg_info = rss_client
            .get_data_vg_info(Some(config.rss_rpc_timeout()), &trace_id)
            .await
            .map_err(|e| format!("Failed to get DataVgInfo from RSS: {e}"))?;
        tracing::info!("Got DataVgInfo with {} volumes", data_vg_info.volumes.len());

        // 4. Resolve bucket -> root_blob_name
        let bucket_key = format!("bucket:{}", config.bucket_name);
        let (_version, bucket_json) = rss_client
            .get(&bucket_key, Some(config.rss_rpc_timeout()), &trace_id)
            .await
            .map_err(|e| format!("Failed to get bucket '{}': {e}", config.bucket_name))?;

        let bucket: Bucket = serde_json::from_str(&bucket_json)
            .map_err(|e| format!("Failed to parse bucket JSON: {e}"))?;
        tracing::info!(
            "Resolved bucket '{}' -> root_blob_name '{}'",
            config.bucket_name,
            bucket.root_blob_name
        );

        Ok(Self {
            nss_address: nss_addr,
            data_vg_info,
            root_blob_name: bucket.root_blob_name,
            config: config.clone(),
        })
    }
}

/// Per-thread storage backend using compio-native RPC clients.
/// Created once per compio thread via thread_local.
/// Safety: compio is single-threaded, so RefCell borrows across await are safe.
pub struct StorageBackend {
    rss_client: RssRpcClient,
    nss_client: RefCell<NssRpcClient>,
    nss_address: RefCell<String>,
    data_vg_proxy: DataVgProxy,
    root_blob_name: String,
    config: Config,
}

impl StorageBackend {
    /// Create a per-thread backend from discovered configuration.
    pub fn new(backend_config: &BackendConfig) -> Result<Self, String> {
        let nss_client = NssRpcClient::new_from_address(backend_config.nss_address.clone());
        let rss_client = RssRpcClient::new_from_addresses(backend_config.config.rss_addrs.clone());
        let data_vg_proxy = DataVgProxy::new(
            backend_config.data_vg_info.clone(),
            backend_config.config.rpc_request_timeout(),
        )?;

        Ok(Self {
            rss_client,
            nss_client: RefCell::new(nss_client),
            nss_address: RefCell::new(backend_config.nss_address.clone()),
            data_vg_proxy,
            root_blob_name: backend_config.root_blob_name.clone(),
            config: backend_config.config.clone(),
        })
    }

    /// Try to refresh NSS address from RSS when connection fails.
    pub async fn try_refresh_nss_address(&self, trace_id: &TraceId) -> bool {
        let current_addr = self.nss_address.borrow().clone();

        match self
            .rss_client
            .get_active_nss_address(Some(self.config.rss_rpc_timeout()), trace_id)
            .await
        {
            Ok(new_addr) => {
                if current_addr != new_addr {
                    tracing::info!("NSS address changed: {} -> {}", current_addr, new_addr);
                    let new_client = NssRpcClient::new_from_address(new_addr.clone());
                    *self.nss_address.borrow_mut() = new_addr;
                    *self.nss_client.borrow_mut() = new_client;
                    true
                } else {
                    false
                }
            }
            Err(e) => {
                tracing::warn!("Failed to refresh NSS address: {e}");
                false
            }
        }
    }

    /// Get object metadata from NSS. The key should NOT have the trailing \0
    /// (the NSS client adds it).
    pub async fn get_object(
        &self,
        key: &str,
        trace_id: &TraceId,
    ) -> Result<ObjectLayout, FuseError> {
        let failover_timeout = Duration::from_secs(30);
        let failover_start = Instant::now();
        let mut refresh_attempt = 0u32;
        let resp = loop {
            let result = self
                .nss_client
                .borrow()
                .get_inode(
                    &self.root_blob_name,
                    key,
                    Some(self.config.rpc_request_timeout()),
                    trace_id,
                )
                .await;

            match result {
                Ok(r) => break r,
                Err(e) if !e.retryable() => return Err(e.into()),
                Err(e) => {
                    if failover_start.elapsed() > failover_timeout {
                        tracing::warn!(
                            elapsed_ms = failover_start.elapsed().as_millis(),
                            error = %e,
                            "NSS get_inode failed after failover timeout"
                        );
                        return Err(e.into());
                    }

                    if self.try_refresh_nss_address(trace_id).await {
                        refresh_attempt = 0;
                        continue;
                    }

                    let backoff_ms = std::cmp::min(200 * (1u64 << refresh_attempt.min(3)), 1000);
                    compio_runtime::time::sleep(Duration::from_millis(backoff_ms)).await;
                    refresh_attempt = refresh_attempt.saturating_add(1);
                }
            }
        };

        let object_bytes = match resp.result.unwrap() {
            nss_codec::get_inode_response::Result::Ok(res) => res,
            nss_codec::get_inode_response::Result::ErrNotFound(()) => {
                return Err(FuseError::NotFound);
            }
            nss_codec::get_inode_response::Result::ErrOther(e) => {
                tracing::error!("NSS get_inode error: {e}");
                return Err(FuseError::Internal(e));
            }
        };

        let object = rkyv::from_bytes::<ObjectLayout, rkyv::rancor::Error>(&object_bytes)?;
        Ok(object)
    }

    /// List objects from NSS. Returns (key, Option<ObjectLayout>).
    /// Empty inode data means common prefix (directory).
    pub async fn list_objects(
        &self,
        prefix: &str,
        delimiter: &str,
        start_after: &str,
        max_keys: u32,
        trace_id: &TraceId,
    ) -> Result<Vec<ListEntry>, FuseError> {
        let failover_timeout = Duration::from_secs(30);
        let failover_start = Instant::now();
        let mut refresh_attempt = 0u32;
        let resp = loop {
            let result = self
                .nss_client
                .borrow()
                .list_inodes(
                    &self.root_blob_name,
                    max_keys,
                    prefix,
                    delimiter,
                    start_after,
                    true,
                    Some(self.config.rpc_request_timeout()),
                    trace_id,
                )
                .await;

            match result {
                Ok(r) => break r,
                Err(e) if !e.retryable() => return Err(e.into()),
                Err(e) => {
                    if failover_start.elapsed() > failover_timeout {
                        tracing::warn!(
                            elapsed_ms = failover_start.elapsed().as_millis(),
                            error = %e,
                            "NSS list_inodes failed after failover timeout"
                        );
                        return Err(e.into());
                    }

                    if self.try_refresh_nss_address(trace_id).await {
                        refresh_attempt = 0;
                        continue;
                    }

                    let backoff_ms = std::cmp::min(200 * (1u64 << refresh_attempt.min(3)), 1000);
                    compio_runtime::time::sleep(Duration::from_millis(backoff_ms)).await;
                    refresh_attempt = refresh_attempt.saturating_add(1);
                }
            }
        };

        let inodes = match resp.result.unwrap() {
            nss_codec::list_inodes_response::Result::Ok(res) => res.inodes,
            nss_codec::list_inodes_response::Result::Err(e) => {
                tracing::error!("NSS list_inodes error: {e}");
                return Err(FuseError::Internal(e));
            }
        };

        let mut entries = Vec::with_capacity(inodes.len());
        for inode in inodes {
            if inode.inode.is_empty() {
                entries.push(ListEntry {
                    key: inode.key,
                    layout: None,
                });
            } else {
                match rkyv::from_bytes::<ObjectLayout, rkyv::rancor::Error>(&inode.inode) {
                    Ok(object) => {
                        let key = inode.key.trim_end_matches('\0').to_string();
                        entries.push(ListEntry {
                            key,
                            layout: Some(object),
                        });
                    }
                    Err(e) => {
                        tracing::error!(
                            key = %inode.key,
                            inode_len = inode.inode.len(),
                            error = %e,
                            "list entry: rkyv deserialization failed"
                        );
                        return Err(e.into());
                    }
                }
            }
        }
        Ok(entries)
    }

    /// List MPU parts for a completed multipart upload
    pub async fn list_mpu_parts(
        &self,
        key: &str,
        trace_id: &TraceId,
    ) -> Result<Vec<(String, ObjectLayout)>, FuseError> {
        let mpu_prefix = mpu_get_part_prefix(key, 0);
        let failover_timeout = Duration::from_secs(30);
        let failover_start = Instant::now();
        let mut refresh_attempt = 0u32;
        let resp = loop {
            let result = self
                .nss_client
                .borrow()
                .list_inodes(
                    &self.root_blob_name,
                    10000,
                    &mpu_prefix,
                    "",
                    "",
                    false,
                    Some(self.config.rpc_request_timeout()),
                    trace_id,
                )
                .await;

            match result {
                Ok(r) => break r,
                Err(e) if !e.retryable() => return Err(e.into()),
                Err(e) => {
                    if failover_start.elapsed() > failover_timeout {
                        return Err(e.into());
                    }

                    if self.try_refresh_nss_address(trace_id).await {
                        refresh_attempt = 0;
                        continue;
                    }

                    let backoff_ms = std::cmp::min(200 * (1u64 << refresh_attempt.min(3)), 1000);
                    compio_runtime::time::sleep(Duration::from_millis(backoff_ms)).await;
                    refresh_attempt = refresh_attempt.saturating_add(1);
                }
            }
        };

        let inodes = match resp.result.unwrap() {
            nss_codec::list_inodes_response::Result::Ok(res) => res.inodes,
            nss_codec::list_inodes_response::Result::Err(e) => {
                tracing::error!("NSS list_inodes error for MPU parts: {e}");
                return Err(FuseError::Internal(e));
            }
        };

        let mut parts = Vec::with_capacity(inodes.len());
        for inode in inodes {
            let object = rkyv::from_bytes::<ObjectLayout, rkyv::rancor::Error>(&inode.inode)?;
            let key = inode.key.trim_end_matches('\0').to_string();
            parts.push((key, object));
        }
        Ok(parts)
    }

    /// Read a single block from a data blob via DataVgProxy
    pub async fn read_block(
        &self,
        blob_guid: DataBlobGuid,
        block_number: u32,
        content_len: usize,
        trace_id: &TraceId,
    ) -> Result<Bytes, FuseError> {
        let mut body = Bytes::new();
        self.data_vg_proxy
            .get_blob(blob_guid, block_number, content_len, &mut body, trace_id)
            .await?;
        Ok(body)
    }

    /// Create a new data blob GUID via DataVgProxy.
    pub fn create_blob_guid(&self) -> DataBlobGuid {
        self.data_vg_proxy.create_data_blob_guid()
    }

    /// Write a single block to a data blob via DataVgProxy.
    pub async fn write_block(
        &self,
        blob_guid: DataBlobGuid,
        block_number: u32,
        body: Bytes,
        trace_id: &TraceId,
    ) -> Result<(), FuseError> {
        self.data_vg_proxy
            .put_blob(blob_guid, block_number, body, trace_id)
            .await?;
        Ok(())
    }

    /// Put (create/update) an inode in NSS. Returns the previous object bytes
    /// (empty if this is a new object).
    pub async fn put_inode(
        &self,
        key: &str,
        value: Bytes,
        trace_id: &TraceId,
    ) -> Result<Bytes, FuseError> {
        let failover_timeout = Duration::from_secs(30);
        let failover_start = Instant::now();
        let mut refresh_attempt = 0u32;
        let resp = loop {
            let result = self
                .nss_client
                .borrow()
                .put_inode(
                    &self.root_blob_name,
                    key,
                    value.clone(),
                    Some(self.config.rpc_request_timeout()),
                    trace_id,
                )
                .await;

            match result {
                Ok(r) => break r,
                Err(e) if !e.retryable() => return Err(e.into()),
                Err(e) => {
                    if failover_start.elapsed() > failover_timeout {
                        return Err(e.into());
                    }

                    if self.try_refresh_nss_address(trace_id).await {
                        refresh_attempt = 0;
                        continue;
                    }

                    let backoff_ms = std::cmp::min(200 * (1u64 << refresh_attempt.min(3)), 1000);
                    compio_runtime::time::sleep(Duration::from_millis(backoff_ms)).await;
                    refresh_attempt = refresh_attempt.saturating_add(1);
                }
            }
        };

        match resp.result.unwrap() {
            nss_codec::put_inode_response::Result::Ok(res) => Ok(res),
            nss_codec::put_inode_response::Result::Err(e) => {
                tracing::error!("NSS put_inode error: {e}");
                Err(FuseError::Internal(e))
            }
        }
    }

    /// Delete an inode from NSS. Returns the previous object bytes, or None
    /// if the object was not found / already deleted.
    pub async fn delete_inode(
        &self,
        key: &str,
        trace_id: &TraceId,
    ) -> Result<Option<Bytes>, FuseError> {
        let failover_timeout = Duration::from_secs(30);
        let failover_start = Instant::now();
        let mut refresh_attempt = 0u32;
        let resp = loop {
            let result = self
                .nss_client
                .borrow()
                .delete_inode(
                    &self.root_blob_name,
                    key,
                    Some(self.config.rpc_request_timeout()),
                    trace_id,
                )
                .await;

            match result {
                Ok(r) => break r,
                Err(e) if !e.retryable() => return Err(e.into()),
                Err(e) => {
                    if failover_start.elapsed() > failover_timeout {
                        return Err(e.into());
                    }

                    if self.try_refresh_nss_address(trace_id).await {
                        refresh_attempt = 0;
                        continue;
                    }

                    let backoff_ms = std::cmp::min(200 * (1u64 << refresh_attempt.min(3)), 1000);
                    compio_runtime::time::sleep(Duration::from_millis(backoff_ms)).await;
                    refresh_attempt = refresh_attempt.saturating_add(1);
                }
            }
        };

        match resp.result.unwrap() {
            nss_codec::delete_inode_response::Result::Ok(res) => Ok(Some(res)),
            nss_codec::delete_inode_response::Result::ErrNotFound(()) => Ok(None),
            nss_codec::delete_inode_response::Result::ErrAlreadyDeleted(()) => Ok(None),
            nss_codec::delete_inode_response::Result::ErrOther(e) => {
                tracing::error!("NSS delete_inode error: {e}");
                Err(FuseError::Internal(e))
            }
        }
    }

    /// Rename an object (file) in NSS.
    pub async fn rename_object(
        &self,
        src_key: &str,
        dst_key: &str,
        trace_id: &TraceId,
    ) -> Result<(), FuseError> {
        let failover_timeout = Duration::from_secs(30);
        let failover_start = Instant::now();
        let mut refresh_attempt = 0u32;
        loop {
            let result = self
                .nss_client
                .borrow()
                .rename_object(
                    &self.root_blob_name,
                    src_key,
                    dst_key,
                    Some(self.config.rpc_request_timeout()),
                    trace_id,
                )
                .await;

            match result {
                Ok(()) => return Ok(()),
                Err(RpcError::NotFound) => return Err(FuseError::NotFound),
                Err(RpcError::AlreadyExists) => return Err(FuseError::AlreadyExists),
                Err(e) if !e.retryable() => return Err(e.into()),
                Err(e) => {
                    if failover_start.elapsed() > failover_timeout {
                        return Err(e.into());
                    }

                    if self.try_refresh_nss_address(trace_id).await {
                        refresh_attempt = 0;
                        continue;
                    }

                    let backoff_ms = std::cmp::min(200 * (1u64 << refresh_attempt.min(3)), 1000);
                    compio_runtime::time::sleep(Duration::from_millis(backoff_ms)).await;
                    refresh_attempt = refresh_attempt.saturating_add(1);
                }
            }
        }
    }

    /// Rename a folder (directory prefix) in NSS.
    pub async fn rename_folder(
        &self,
        src_key: &str,
        dst_key: &str,
        trace_id: &TraceId,
    ) -> Result<(), FuseError> {
        let failover_timeout = Duration::from_secs(30);
        let failover_start = Instant::now();
        let mut refresh_attempt = 0u32;
        loop {
            let result = self
                .nss_client
                .borrow()
                .rename_folder(
                    &self.root_blob_name,
                    src_key,
                    dst_key,
                    Some(self.config.rpc_request_timeout()),
                    trace_id,
                )
                .await;

            match result {
                Ok(()) => return Ok(()),
                Err(RpcError::NotFound) => return Err(FuseError::NotFound),
                Err(RpcError::AlreadyExists) => return Err(FuseError::AlreadyExists),
                Err(e) if !e.retryable() => return Err(e.into()),
                Err(e) => {
                    if failover_start.elapsed() > failover_timeout {
                        return Err(e.into());
                    }

                    if self.try_refresh_nss_address(trace_id).await {
                        refresh_attempt = 0;
                        continue;
                    }

                    let backoff_ms = std::cmp::min(200 * (1u64 << refresh_attempt.min(3)), 1000);
                    compio_runtime::time::sleep(Duration::from_millis(backoff_ms)).await;
                    refresh_attempt = refresh_attempt.saturating_add(1);
                }
            }
        }
    }

    /// Delete blob blocks for a given ObjectLayout. Fire-and-forget: logs
    /// warnings on failure but does not return errors.
    pub async fn delete_blob_blocks(&self, layout: &ObjectLayout, trace_id: &TraceId) {
        let blob_guid = match layout.blob_guid() {
            Ok(g) => g,
            Err(_) => return,
        };
        let num_blocks = match layout.num_blocks() {
            Ok(n) => n,
            Err(_) => return,
        };
        for block_number in 0..num_blocks {
            if let Err(e) = self
                .data_vg_proxy
                .delete_blob(blob_guid, block_number as u32, trace_id)
                .await
            {
                tracing::warn!(
                    %blob_guid,
                    block_number,
                    error = %e,
                    "Failed to delete blob block"
                );
            }
        }
    }

    /// Create a directory marker in NSS.
    /// Stores a minimal ObjectLayout with size=0 because NSS rejects empty values.
    pub async fn put_dir_marker(&self, key: &str, trace_id: &TraceId) -> Result<(), FuseError> {
        let layout = ObjectLayout {
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
            version_id: ObjectLayout::gen_version_id(),
            block_size: ObjectLayout::DEFAULT_BLOCK_SIZE,
            state: ObjectState::Normal(ObjectMetaData {
                blob_guid: DataBlobGuid {
                    blob_id: uuid::Uuid::nil(),
                    volume_id: 0,
                },
                core_meta_data: ObjectCoreMetaData {
                    size: 0,
                    etag: String::new(),
                    headers: vec![],
                    checksum: None,
                },
            }),
        };
        let value: Vec<u8> =
            rkyv::api::high::to_bytes_in::<_, rkyv::rancor::Error>(&layout, Vec::new())?;
        self.put_inode(key, Bytes::from(value), trace_id).await?;
        Ok(())
    }

    #[allow(dead_code)]
    pub fn root_blob_name(&self) -> &str {
        &self.root_blob_name
    }

    #[allow(dead_code)]
    pub fn config(&self) -> &Config {
        &self.config
    }
}

/// Generate the MPU part prefix key, matching api_server convention
fn mpu_get_part_prefix(key: &str, part_number: u64) -> String {
    let mut result = key.to_string();
    result.push('#');
    if part_number != 0 {
        let part_str = format!("{:04}", part_number - 1);
        result.push_str(&part_str);
    }
    result
}
