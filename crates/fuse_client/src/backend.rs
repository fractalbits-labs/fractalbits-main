use bytes::Bytes;
use data_types::{Bucket, DataBlobGuid, TraceId};
use rpc_client_common::{nss_rpc_retry, rss_rpc_retry};
use rpc_client_nss::RpcClientNss;
use rpc_client_rss::RpcClientRss;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{RwLock, RwLockReadGuard};
use volume_group_proxy::DataVgProxy;

use crate::config::Config;
use crate::error::FuseError;
use data_types::object_layout::{ObjectCoreMetaData, ObjectLayout, ObjectMetaData, ObjectState};

/// Represents a listed entry from NSS
pub struct ListEntry {
    pub key: String,
    pub layout: Option<ObjectLayout>,
}

#[allow(dead_code)]
pub struct StorageBackend {
    rss_client: RpcClientRss,
    nss_client: Arc<RwLock<RpcClientNss>>,
    nss_address: Arc<RwLock<String>>,
    data_vg_proxy: Arc<DataVgProxy>,
    root_blob_name: String,
    config: Arc<Config>,
}

impl StorageBackend {
    pub async fn new(config: Arc<Config>) -> Result<Self, String> {
        let trace_id = TraceId::new();

        // 1. Create RSS client
        let rss_client = RpcClientRss::new_from_addresses(
            config.rss_addrs.clone(),
            config.rpc_connection_timeout(),
        );

        // 2. Get active NSS address from RSS
        let nss_addr = rss_rpc_retry!(
            rss_client,
            get_active_nss_address(Some(config.rss_rpc_timeout()), &trace_id)
        )
        .await
        .map_err(|e| format!("Failed to get NSS address from RSS: {}", e))?;
        tracing::info!("Got NSS address: {}", nss_addr);

        // 3. Create NSS client
        let nss_client =
            RpcClientNss::new_from_address(nss_addr.clone(), config.rpc_connection_timeout());

        // 4. Get DataVgInfo from RSS
        let data_vg_info = rss_client
            .get_data_vg_info(Some(config.rss_rpc_timeout()), &trace_id)
            .await
            .map_err(|e| format!("Failed to get DataVgInfo from RSS: {}", e))?;
        tracing::info!("Got DataVgInfo with {} volumes", data_vg_info.volumes.len());

        // 5. Create DataVgProxy
        let data_vg_proxy = DataVgProxy::new(
            data_vg_info,
            config.rpc_request_timeout(),
            config.rpc_connection_timeout(),
        )
        .map_err(|e| format!("Failed to create DataVgProxy: {}", e))?;

        // 6. Resolve bucket -> root_blob_name
        let bucket_key = format!("bucket:{}", config.bucket_name);
        let (_version, bucket_json) = rss_rpc_retry!(
            rss_client,
            get(&bucket_key, Some(config.rss_rpc_timeout()), &trace_id)
        )
        .await
        .map_err(|e| format!("Failed to get bucket '{}': {}", config.bucket_name, e))?;

        let bucket: Bucket = serde_json::from_str(&bucket_json)
            .map_err(|e| format!("Failed to parse bucket JSON: {}", e))?;
        tracing::info!(
            "Resolved bucket '{}' -> root_blob_name '{}'",
            config.bucket_name,
            bucket.root_blob_name
        );

        Ok(Self {
            rss_client,
            nss_client: Arc::new(RwLock::new(nss_client)),
            nss_address: Arc::new(RwLock::new(nss_addr)),
            data_vg_proxy: Arc::new(data_vg_proxy),
            root_blob_name: bucket.root_blob_name,
            config,
        })
    }

    pub async fn get_nss_client(&self) -> RwLockReadGuard<'_, RpcClientNss> {
        self.nss_client.read().await
    }

    /// Try to refresh NSS address from RSS when connection fails.
    /// Returns true if address was refreshed and caller should retry.
    pub async fn try_refresh_nss_address(&self, trace_id: &TraceId) -> bool {
        let current_addr = self.nss_address.read().await.clone();

        match rss_rpc_retry!(
            self.rss_client,
            get_active_nss_address(Some(self.config.rss_rpc_timeout()), trace_id)
        )
        .await
        {
            Ok(new_addr) => {
                if current_addr != new_addr {
                    tracing::info!(
                        "NSS address changed during refresh: {} -> {}",
                        current_addr,
                        new_addr
                    );
                    let new_client = RpcClientNss::new_from_address(
                        new_addr.clone(),
                        self.config.rpc_connection_timeout(),
                    );
                    *self.nss_address.write().await = new_addr;
                    *self.nss_client.write().await = new_client;
                    true
                } else {
                    tracing::debug!("NSS address unchanged during refresh: {}", current_addr);
                    false
                }
            }
            Err(e) => {
                tracing::warn!("Failed to refresh NSS address from RSS: {}", e);
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
            let result = {
                let nss_client = self.get_nss_client().await;
                nss_rpc_retry!(
                    nss_client,
                    get_inode(
                        &self.root_blob_name,
                        key,
                        Some(self.config.rpc_request_timeout()),
                        trace_id
                    )
                )
                .await
            };

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
                        tracing::info!(
                            elapsed_ms = failover_start.elapsed().as_millis(),
                            "Refreshed NSS address during get_inode failover"
                        );
                        refresh_attempt = 0;
                        continue;
                    }

                    let backoff_ms = std::cmp::min(200 * (1u64 << refresh_attempt.min(3)), 1000);
                    tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
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
                tracing::error!("NSS get_inode error: {}", e);
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
            let result = {
                let nss_client = self.get_nss_client().await;
                nss_rpc_retry!(
                    nss_client,
                    list_inodes(
                        &self.root_blob_name,
                        max_keys,
                        prefix,
                        delimiter,
                        start_after,
                        true, // skip_mpu_parts
                        Some(self.config.rpc_request_timeout()),
                        trace_id
                    )
                )
                .await
            };

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
                        tracing::info!(
                            elapsed_ms = failover_start.elapsed().as_millis(),
                            "Refreshed NSS address during list_inodes failover"
                        );
                        refresh_attempt = 0;
                        continue;
                    }

                    let backoff_ms = std::cmp::min(200 * (1u64 << refresh_attempt.min(3)), 1000);
                    tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                    refresh_attempt = refresh_attempt.saturating_add(1);
                }
            }
        };

        let inodes = match resp.result.unwrap() {
            nss_codec::list_inodes_response::Result::Ok(res) => res.inodes,
            nss_codec::list_inodes_response::Result::Err(e) => {
                tracing::error!("NSS list_inodes error: {}", e);
                return Err(FuseError::Internal(e));
            }
        };

        let mut entries = Vec::with_capacity(inodes.len());
        for inode in inodes {
            if inode.inode.is_empty() {
                // Common prefix (directory marker) - key ends with "/"
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
            let result = {
                let nss_client = self.get_nss_client().await;
                nss_rpc_retry!(
                    nss_client,
                    list_inodes(
                        &self.root_blob_name,
                        10000,
                        &mpu_prefix,
                        "",
                        "",
                        false, // don't skip parts - we want them
                        Some(self.config.rpc_request_timeout()),
                        trace_id
                    )
                )
                .await
            };

            match result {
                Ok(r) => break r,
                Err(e) if !e.retryable() => return Err(e.into()),
                Err(e) => {
                    if failover_start.elapsed() > failover_timeout {
                        tracing::warn!(
                            elapsed_ms = failover_start.elapsed().as_millis(),
                            error = %e,
                            "NSS list_inodes (mpu parts) failed after failover timeout"
                        );
                        return Err(e.into());
                    }

                    if self.try_refresh_nss_address(trace_id).await {
                        tracing::info!(
                            elapsed_ms = failover_start.elapsed().as_millis(),
                            "Refreshed NSS address during MPU list failover"
                        );
                        refresh_attempt = 0;
                        continue;
                    }

                    let backoff_ms = std::cmp::min(200 * (1u64 << refresh_attempt.min(3)), 1000);
                    tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                    refresh_attempt = refresh_attempt.saturating_add(1);
                }
            }
        };

        let inodes = match resp.result.unwrap() {
            nss_codec::list_inodes_response::Result::Ok(res) => res.inodes,
            nss_codec::list_inodes_response::Result::Err(e) => {
                tracing::error!("NSS list_inodes error for MPU parts: {}", e);
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
    pub fn create_blob_guid(&self) -> data_types::DataBlobGuid {
        self.data_vg_proxy.create_data_blob_guid()
    }

    /// Write a single block to a data blob via DataVgProxy.
    pub async fn write_block(
        &self,
        blob_guid: data_types::DataBlobGuid,
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
            let result = {
                let nss_client = self.get_nss_client().await;
                nss_rpc_retry!(
                    nss_client,
                    put_inode(
                        &self.root_blob_name,
                        key,
                        value.clone(),
                        Some(self.config.rpc_request_timeout()),
                        trace_id
                    )
                )
                .await
            };

            match result {
                Ok(r) => break r,
                Err(e) if !e.retryable() => return Err(e.into()),
                Err(e) => {
                    if failover_start.elapsed() > failover_timeout {
                        tracing::warn!(
                            elapsed_ms = failover_start.elapsed().as_millis(),
                            error = %e,
                            "NSS put_inode failed after failover timeout"
                        );
                        return Err(e.into());
                    }

                    if self.try_refresh_nss_address(trace_id).await {
                        refresh_attempt = 0;
                        continue;
                    }

                    let backoff_ms = std::cmp::min(200 * (1u64 << refresh_attempt.min(3)), 1000);
                    tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                    refresh_attempt = refresh_attempt.saturating_add(1);
                }
            }
        };

        match resp.result.unwrap() {
            nss_codec::put_inode_response::Result::Ok(res) => Ok(res),
            nss_codec::put_inode_response::Result::Err(e) => {
                tracing::error!("NSS put_inode error: {}", e);
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
            let result = {
                let nss_client = self.get_nss_client().await;
                nss_rpc_retry!(
                    nss_client,
                    delete_inode(
                        &self.root_blob_name,
                        key,
                        Some(self.config.rpc_request_timeout()),
                        trace_id
                    )
                )
                .await
            };

            match result {
                Ok(r) => break r,
                Err(e) if !e.retryable() => return Err(e.into()),
                Err(e) => {
                    if failover_start.elapsed() > failover_timeout {
                        tracing::warn!(
                            elapsed_ms = failover_start.elapsed().as_millis(),
                            error = %e,
                            "NSS delete_inode failed after failover timeout"
                        );
                        return Err(e.into());
                    }

                    if self.try_refresh_nss_address(trace_id).await {
                        refresh_attempt = 0;
                        continue;
                    }

                    let backoff_ms = std::cmp::min(200 * (1u64 << refresh_attempt.min(3)), 1000);
                    tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                    refresh_attempt = refresh_attempt.saturating_add(1);
                }
            }
        };

        match resp.result.unwrap() {
            nss_codec::delete_inode_response::Result::Ok(res) => Ok(Some(res)),
            nss_codec::delete_inode_response::Result::ErrNotFound(()) => Ok(None),
            nss_codec::delete_inode_response::Result::ErrAlreadyDeleted(()) => Ok(None),
            nss_codec::delete_inode_response::Result::ErrOther(e) => {
                tracing::error!("NSS delete_inode error: {}", e);
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
            let result = {
                let nss_client = self.get_nss_client().await;
                nss_rpc_retry!(
                    nss_client,
                    rename_object(
                        &self.root_blob_name,
                        src_key,
                        dst_key,
                        Some(self.config.rpc_request_timeout()),
                        trace_id
                    )
                )
                .await
            };

            match result {
                Ok(()) => return Ok(()),
                Err(rpc_client_common::RpcError::NotFound) => {
                    return Err(FuseError::NotFound);
                }
                Err(rpc_client_common::RpcError::AlreadyExists) => {
                    return Err(FuseError::AlreadyExists);
                }
                Err(e) if !e.retryable() => return Err(e.into()),
                Err(e) => {
                    if failover_start.elapsed() > failover_timeout {
                        tracing::warn!(
                            elapsed_ms = failover_start.elapsed().as_millis(),
                            error = %e,
                            "NSS rename_object failed after failover timeout"
                        );
                        return Err(e.into());
                    }

                    if self.try_refresh_nss_address(trace_id).await {
                        refresh_attempt = 0;
                        continue;
                    }

                    let backoff_ms = std::cmp::min(200 * (1u64 << refresh_attempt.min(3)), 1000);
                    tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
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
            let result = {
                let nss_client = self.get_nss_client().await;
                nss_rpc_retry!(
                    nss_client,
                    rename_folder(
                        &self.root_blob_name,
                        src_key,
                        dst_key,
                        Some(self.config.rpc_request_timeout()),
                        trace_id
                    )
                )
                .await
            };

            match result {
                Ok(()) => return Ok(()),
                Err(rpc_client_common::RpcError::NotFound) => {
                    return Err(FuseError::NotFound);
                }
                Err(rpc_client_common::RpcError::AlreadyExists) => {
                    return Err(FuseError::AlreadyExists);
                }
                Err(e) if !e.retryable() => return Err(e.into()),
                Err(e) => {
                    if failover_start.elapsed() > failover_timeout {
                        tracing::warn!(
                            elapsed_ms = failover_start.elapsed().as_millis(),
                            error = %e,
                            "NSS rename_folder failed after failover timeout"
                        );
                        return Err(e.into());
                    }

                    if self.try_refresh_nss_address(trace_id).await {
                        refresh_attempt = 0;
                        continue;
                    }

                    let backoff_ms = std::cmp::min(200 * (1u64 << refresh_attempt.min(3)), 1000);
                    tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
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
