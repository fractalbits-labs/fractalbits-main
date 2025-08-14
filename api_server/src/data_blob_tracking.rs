use bytes::Bytes;
use rpc_client_nss::{RpcClientNss, RpcErrorNss};
use rpc_client_rss::{RpcClientRss, RpcErrorRss};
use std::collections::HashMap;
use thiserror::Error;
use uuid::Uuid;

#[derive(Error, Debug)]
pub enum DataBlobTrackingError {
    #[error("RSS error: {0}")]
    Rss(#[from] RpcErrorRss),
    #[error("NSS error: {0}")]
    Nss(#[from] RpcErrorNss),
    #[error("Blob not found")]
    NotFound,
    #[error("Internal error: {0}")]
    Internal(String),
}

/// Helper struct for managing data blob tracking operations
pub struct DataBlobTracker {
    /// Cache for root blob names to avoid repeated RSS lookups
    root_blob_cache: tokio::sync::RwLock<HashMap<String, String>>,
}

impl DataBlobTracker {
    pub fn new() -> Self {
        Self {
            root_blob_cache: tokio::sync::RwLock::new(HashMap::new()),
        }
    }

    /// Get or create root blob name for data blob tracking trees
    async fn get_or_create_data_blob_tree_root(
        &self,
        rss_client: &RpcClientRss,
        nss_client: &RpcClientNss,
        tree_name: &str,
    ) -> Result<String, DataBlobTrackingError> {
        let key = format!("data_blob_trees/{tree_name}");

        // Check cache first
        {
            let cache = self.root_blob_cache.read().await;
            if let Some(root_blob_name) = cache.get(&key) {
                return Ok(root_blob_name.clone());
            }
        }

        // Try to get from RSS
        match rss_client.get(&key, None).await {
            Ok((_version, value)) => {
                // Update cache
                {
                    let mut cache = self.root_blob_cache.write().await;
                    cache.insert(key, value.clone());
                }
                Ok(value)
            }
            Err(RpcErrorRss::NotFound) => {
                // Create new tree and store root blob name
                let response = nss_client.create_root_inode(tree_name, None).await?;
                let root_blob_name = match response.result {
                    Some(resp_result) => match resp_result {
                        rpc_client_nss::rpc::create_root_inode_response::Result::Ok(name) => name,
                        _ => {
                            return Err(DataBlobTrackingError::Internal(
                                "Failed to create root inode".into(),
                            ))
                        }
                    },
                    None => {
                        return Err(DataBlobTrackingError::Internal(
                            "No result in create root inode response".into(),
                        ))
                    }
                };
                rss_client.put(1, &key, &root_blob_name, None).await?;
                // Update cache
                {
                    let mut cache = self.root_blob_cache.write().await;
                    cache.insert(key, root_blob_name.clone());
                }
                Ok(root_blob_name)
            }
            Err(e) => Err(e.into()),
        }
    }

    /// Record a blob that exists only in local AZ
    pub async fn put_single_copy_data_blob(
        &self,
        rss_client: &RpcClientRss,
        nss_client: &RpcClientNss,
        blob_id: Uuid,
        block_num: u32,
        metadata: &[u8],
    ) -> Result<(), DataBlobTrackingError> {
        let root_blob_name = self
            .get_or_create_data_blob_tree_root(rss_client, nss_client, "single_copy_data_blobs")
            .await?;
        let key = format!("{blob_id}:{block_num}");
        nss_client
            .put_inode(
                &root_blob_name,
                &key,
                Bytes::copy_from_slice(metadata),
                None,
            )
            .await?;
        Ok(())
    }

    /// Check if a blob is single-copy
    pub async fn get_single_copy_data_blob(
        &self,
        rss_client: &RpcClientRss,
        nss_client: &RpcClientNss,
        blob_id: Uuid,
        block_num: u32,
    ) -> Result<Option<Vec<u8>>, DataBlobTrackingError> {
        let root_blob_name = self
            .get_or_create_data_blob_tree_root(rss_client, nss_client, "single_copy_data_blobs")
            .await?;
        let key = format!("{blob_id}:{block_num}");
        match nss_client.get_inode(&root_blob_name, &key, None).await {
            Ok(response) => {
                // Extract bytes from response
                match response.result {
                    Some(rpc_client_nss::rpc::get_inode_response::Result::Ok(bytes)) => {
                        Ok(Some(bytes.to_vec()))
                    }
                    Some(rpc_client_nss::rpc::get_inode_response::Result::ErrNotFound(_)) => {
                        Ok(None)
                    }
                    _ => Err(DataBlobTrackingError::Internal(
                        "Unexpected NSS response".into(),
                    )),
                }
            }
            Err(e) => Err(e.into()),
        }
    }

    /// Remove blob from single-copy tracking
    pub async fn delete_single_copy_data_blob(
        &self,
        rss_client: &RpcClientRss,
        nss_client: &RpcClientNss,
        blob_id: Uuid,
        block_num: u32,
    ) -> Result<(), DataBlobTrackingError> {
        let root_blob_name = self
            .get_or_create_data_blob_tree_root(rss_client, nss_client, "single_copy_data_blobs")
            .await?;
        let key = format!("{blob_id}:{block_num}");
        match nss_client.delete_inode(&root_blob_name, &key, None).await {
            Ok(_) => Ok(()),
            Err(RpcErrorNss::NotFound) => Ok(()), // Already deleted, that's fine
            Err(e) => Err(e.into()),
        }
    }

    /// Record a deleted blob to skip during resync
    pub async fn put_deleted_data_blob(
        &self,
        rss_client: &RpcClientRss,
        nss_client: &RpcClientNss,
        blob_id: Uuid,
        block_num: u32,
        timestamp: &[u8],
    ) -> Result<(), DataBlobTrackingError> {
        let root_blob_name = self
            .get_or_create_data_blob_tree_root(rss_client, nss_client, "deleted_data_blobs")
            .await?;
        let key = format!("{blob_id}:{block_num}");
        nss_client
            .put_inode(
                &root_blob_name,
                &key,
                Bytes::copy_from_slice(timestamp),
                None,
            )
            .await?;
        Ok(())
    }

    /// Check if a blob is marked as deleted
    pub async fn get_deleted_data_blob(
        &self,
        rss_client: &RpcClientRss,
        nss_client: &RpcClientNss,
        blob_id: Uuid,
        block_num: u32,
    ) -> Result<Option<Vec<u8>>, DataBlobTrackingError> {
        let root_blob_name = self
            .get_or_create_data_blob_tree_root(rss_client, nss_client, "deleted_data_blobs")
            .await?;
        let key = format!("{blob_id}:{block_num}");
        match nss_client.get_inode(&root_blob_name, &key, None).await {
            Ok(response) => {
                // Extract bytes from response
                match response.result {
                    Some(rpc_client_nss::rpc::get_inode_response::Result::Ok(bytes)) => {
                        Ok(Some(bytes.to_vec()))
                    }
                    Some(rpc_client_nss::rpc::get_inode_response::Result::ErrNotFound(_)) => {
                        Ok(None)
                    }
                    _ => Err(DataBlobTrackingError::Internal(
                        "Unexpected NSS response".into(),
                    )),
                }
            }
            Err(e) => Err(e.into()),
        }
    }

    /// Remove blob from deleted tracking (used during sanitize)
    pub async fn delete_deleted_data_blob(
        &self,
        rss_client: &RpcClientRss,
        nss_client: &RpcClientNss,
        blob_id: Uuid,
        block_num: u32,
    ) -> Result<(), DataBlobTrackingError> {
        let root_blob_name = self
            .get_or_create_data_blob_tree_root(rss_client, nss_client, "deleted_data_blobs")
            .await?;
        let key = format!("{blob_id}:{block_num}");
        match nss_client.delete_inode(&root_blob_name, &key, None).await {
            Ok(_) => Ok(()),
            Err(RpcErrorNss::NotFound) => Ok(()), // Already deleted, that's fine
            Err(e) => Err(e.into()),
        }
    }

    /// List single-copy data blobs for resync
    pub async fn list_single_copy_data_blobs(
        &self,
        rss_client: &RpcClientRss,
        nss_client: &RpcClientNss,
        prefix: &str,
        start_after: &str,
        max_keys: u32,
    ) -> Result<Vec<(String, Vec<u8>)>, DataBlobTrackingError> {
        let root_blob_name = self
            .get_or_create_data_blob_tree_root(rss_client, nss_client, "single_copy_data_blobs")
            .await?;

        let response = nss_client
            .list_inodes(
                &root_blob_name,
                max_keys,
                prefix,
                "",
                start_after,
                false,
                None,
            )
            .await?;

        // Extract the list from the response
        match response.result {
            Some(rpc_client_nss::rpc::list_inodes_response::Result::Ok(inodes)) => {
                let result = inodes
                    .inodes
                    .into_iter()
                    .map(|inode| (inode.key, inode.inode.to_vec()))
                    .collect();
                Ok(result)
            }
            _ => Err(DataBlobTrackingError::Internal(
                "Unexpected NSS list response".into(),
            )),
        }
    }

    /// List deleted data blobs for sanitize
    pub async fn list_deleted_data_blobs(
        &self,
        rss_client: &RpcClientRss,
        nss_client: &RpcClientNss,
        prefix: &str,
        start_after: &str,
        max_keys: u32,
    ) -> Result<Vec<(String, Vec<u8>)>, DataBlobTrackingError> {
        let root_blob_name = self
            .get_or_create_data_blob_tree_root(rss_client, nss_client, "deleted_data_blobs")
            .await?;

        let response = nss_client
            .list_inodes(
                &root_blob_name,
                max_keys,
                prefix,
                "",
                start_after,
                false,
                None,
            )
            .await?;

        // Extract the list from the response
        match response.result {
            Some(rpc_client_nss::rpc::list_inodes_response::Result::Ok(inodes)) => {
                let result = inodes
                    .inodes
                    .into_iter()
                    .map(|inode| (inode.key, inode.inode.to_vec()))
                    .collect();
                Ok(result)
            }
            _ => Err(DataBlobTrackingError::Internal(
                "Unexpected NSS list response".into(),
            )),
        }
    }

    /// Parse blob key back to blob_id and block_num
    pub fn parse_blob_key(key: &str) -> Result<(Uuid, u32), DataBlobTrackingError> {
        let parts: Vec<&str> = key.split(':').collect();
        if parts.len() != 2 {
            return Err(DataBlobTrackingError::Internal(format!(
                "Invalid blob key format: {key}"
            )));
        }

        let blob_id = Uuid::parse_str(parts[0]).map_err(|e| {
            DataBlobTrackingError::Internal(format!("Invalid UUID in blob key {key}: {e}"))
        })?;

        let block_num = parts[1].parse::<u32>().map_err(|e| {
            DataBlobTrackingError::Internal(format!("Invalid block number in blob key {key}: {e}"))
        })?;

        Ok((blob_id, block_num))
    }

    /// Create blob key from blob_id and block_num
    pub fn create_blob_key(blob_id: Uuid, block_num: u32) -> String {
        format!("{blob_id}:{block_num}")
    }

    /// Get current timestamp as bytes for deleted blob tracking
    pub fn current_timestamp_bytes() -> Vec<u8> {
        use std::time::{SystemTime, UNIX_EPOCH};
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        timestamp.to_le_bytes().to_vec()
    }
}

impl Default for DataBlobTracker {
    fn default() -> Self {
        Self::new()
    }
}
