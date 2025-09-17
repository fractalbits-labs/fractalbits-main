use crate::DataVgError;
use bytes::Bytes;
use data_types::QuorumConfig;
use futures::stream::{FuturesUnordered, StreamExt};
use metrics::histogram;
use rpc_client_bss::RpcClientBss;
use rpc_client_common::RpcError;
use slotmap_conn_pool::{ConnPool, Poolable};
use std::{
    collections::HashMap,
    sync::{Arc, atomic::AtomicU64},
    time::{Duration, Instant},
};
use tokio::time::timeout;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

/// MetadataBlobGuid combines blob_id (UUID) with volume_id for multi-BSS metadata support
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, rkyv::Archive, rkyv::Deserialize, rkyv::Serialize,
)]
pub struct MetadataBlobGuid {
    pub blob_id: Uuid,
    pub volume_id: u16,
}

impl std::fmt::Display for MetadataBlobGuid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:m{}", self.blob_id, self.volume_id)
    }
}

/// MetadataVgInfo is similar to DataVgInfo but for metadata volumes
#[derive(Debug, Clone)]
pub struct MetadataVgInfo {
    pub volumes: Vec<MetadataVolume>,
    pub quorum: Option<QuorumConfig>,
}

#[derive(Debug, Clone)]
pub struct MetadataVolume {
    pub volume_id: u16,
    pub bss_nodes: Vec<MetadataBssNode>,
}

#[derive(Debug, Clone)]
pub struct MetadataBssNode {
    pub node_id: String,
    pub ip: String,
    pub port: u16,
}

pub struct MetadataVgProxy {
    volumes: Vec<MetadataVolume>,
    bss_connection_pools: HashMap<String, ConnPool<Arc<RpcClientBss>, String>>,
    round_robin_counter: AtomicU64,
    quorum_config: QuorumConfig,
    rpc_timeout: Duration,
}

impl MetadataVgProxy {
    pub async fn new(
        metadata_vg_info: MetadataVgInfo,
        rpc_timeout: Duration,
    ) -> Result<Self, DataVgError> {
        info!(
            "Initializing MetadataVgProxy with {} volumes",
            metadata_vg_info.volumes.len()
        );

        let quorum_config = metadata_vg_info.quorum.ok_or_else(|| {
            DataVgError::InitializationError(
                "QuorumConfig is required but not provided in MetadataVgInfo".to_string(),
            )
        })?;

        // Create connection pools for all BSS nodes
        let mut bss_connection_pools = HashMap::new();

        for volume in &metadata_vg_info.volumes {
            for bss_node in &volume.bss_nodes {
                let address = format!("{}:{}", bss_node.ip, bss_node.port);
                if !bss_connection_pools.contains_key(&address) {
                    info!(
                        "Creating connection pool for metadata BSS node: {}",
                        address
                    );
                    let pool = ConnPool::new();

                    // Create connections to this BSS node
                    let bss_conn_num = 4; // Default connection pool size
                    for i in 0..bss_conn_num {
                        debug!(
                            "Creating connection {}/{} to metadata BSS {}",
                            i + 1,
                            bss_conn_num,
                            address
                        );
                        let client = Arc::new(
                            <RpcClientBss as Poolable>::new(address.clone())
                                .await
                                .map_err(|e| {
                                    DataVgError::InitializationError(format!(
                                        "Failed to connect to metadata BSS {}: {}",
                                        address, e
                                    ))
                                })?,
                        );
                        pool.pooled(address.clone(), client);
                    }

                    bss_connection_pools.insert(address.clone(), pool);
                }
            }
        }

        info!(
            "MetadataVgProxy initialized successfully with {} BSS connection pools",
            bss_connection_pools.len()
        );

        Ok(Self {
            volumes: metadata_vg_info.volumes,
            bss_connection_pools,
            round_robin_counter: AtomicU64::new(0),
            quorum_config,
            rpc_timeout,
        })
    }

    pub fn select_volume_for_blob(&self) -> u16 {
        // Use round-robin to select volume
        let counter = self
            .round_robin_counter
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let volume_index = (counter as usize) % self.volumes.len();
        self.volumes[volume_index].volume_id
    }

    // Standalone versions of the methods for use in async closures
    async fn checkout_bss_client_for_async(
        bss_connection_pools: &HashMap<String, ConnPool<Arc<RpcClientBss>, String>>,
        bss_address: &str,
    ) -> Result<Arc<RpcClientBss>, DataVgError> {
        let pool = bss_connection_pools.get(bss_address).ok_or_else(|| {
            DataVgError::InitializationError(format!(
                "No connection pool for metadata BSS {}",
                bss_address
            ))
        })?;

        let start = Instant::now();
        let client = pool.checkout(bss_address.to_string()).await.map_err(|e| {
            DataVgError::InitializationError(format!(
                "Failed to checkout metadata BSS client for {}: {}",
                bss_address, e
            ))
        })?;

        histogram!("checkout_rpc_client_nanos", "type" => "bss_metadatavg")
            .record(start.elapsed().as_nanos() as f64);

        Ok(client)
    }

    #[allow(clippy::too_many_arguments)]
    async fn put_metadata_blob_to_node(
        bss_connection_pools: &HashMap<String, ConnPool<Arc<RpcClientBss>, String>>,
        bss_address: &str,
        blob_id: Uuid,
        block_number: u32,
        volume_id: u16,
        version: u64,
        is_new: bool,
        body: Bytes,
        rpc_timeout: Duration,
    ) -> Result<(), RpcError> {
        let client = Self::checkout_bss_client_for_async(bss_connection_pools, bss_address)
            .await
            .map_err(|e| RpcError::InternalResponseError(e.to_string()))?;

        client
            .put_metadata_blob(
                blob_id,
                block_number,
                volume_id,
                version,
                is_new,
                body,
                Some(rpc_timeout),
            )
            .await
    }

    async fn get_metadata_blob_from_node(
        bss_connection_pools: &HashMap<String, ConnPool<Arc<RpcClientBss>, String>>,
        bss_address: &str,
        blob_id: Uuid,
        block_number: u32,
        volume_id: u16,
        version: u64,
        _rpc_timeout: Duration,
    ) -> Result<(Bytes, u64), RpcError> {
        let client = Self::checkout_bss_client_for_async(bss_connection_pools, bss_address)
            .await
            .map_err(|e| RpcError::InternalResponseError(e.to_string()))?;

        let mut body = Bytes::new();
        let actual_version = client
            .get_metadata_blob(blob_id, block_number, volume_id, version, &mut body)
            .await?;
        Ok((body, actual_version))
    }

    async fn delete_metadata_blob_from_node(
        bss_connection_pools: &HashMap<String, ConnPool<Arc<RpcClientBss>, String>>,
        bss_address: &str,
        blob_id: Uuid,
        block_number: u32,
        volume_id: u16,
        rpc_timeout: Duration,
    ) -> Result<(), RpcError> {
        let client = Self::checkout_bss_client_for_async(bss_connection_pools, bss_address)
            .await
            .map_err(|e| RpcError::InternalResponseError(e.to_string()))?;

        client
            .delete_metadata_blob(blob_id, block_number, volume_id, Some(rpc_timeout))
            .await
    }
}

impl MetadataVgProxy {
    /// Create a new metadata blob GUID with a fresh UUID and selected volume
    pub fn create_metadata_blob_guid(&self) -> MetadataBlobGuid {
        let blob_id = Uuid::now_v7();
        let volume_id = self.select_volume_for_blob();
        MetadataBlobGuid { blob_id, volume_id }
    }

    /// Multi-BSS put_metadata_blob with quorum-based replication
    pub async fn put_metadata_blob(
        &self,
        blob_guid: MetadataBlobGuid,
        version: u64,
        is_new: bool,
        content: Bytes,
    ) -> Result<(), DataVgError> {
        let start = Instant::now();
        histogram!("metadata_blob_size", "operation" => "put").record(content.len() as f64);

        // Use the volume_id from blob_guid to find the volume
        let selected_volume = self
            .volumes
            .iter()
            .find(|v| v.volume_id == blob_guid.volume_id)
            .ok_or_else(|| {
                DataVgError::InitializationError(format!(
                    "Metadata volume {} not found in MetadataVgProxy",
                    blob_guid.volume_id
                ))
            })?;
        debug!(
            "Using metadata volume {} for put_metadata_blob",
            selected_volume.volume_id
        );

        // Spawn individual tasks for each write operation
        let bss_connection_pools = self.bss_connection_pools.clone();
        let rpc_timeout = self.rpc_timeout;
        let write_quorum = self.quorum_config.w as usize;
        let bss_nodes = selected_volume.bss_nodes.clone();

        // Spawn all write tasks immediately
        let mut write_futures = FuturesUnordered::new();
        for bss_node in bss_nodes {
            let address = format!("{}:{}", bss_node.ip, bss_node.port);
            let content_clone = content.clone();
            let bss_pools = bss_connection_pools.clone();

            let write_task = tokio::spawn(async move {
                let result = timeout(
                    rpc_timeout,
                    Self::put_metadata_blob_to_node(
                        &bss_pools,
                        &address,
                        blob_guid.blob_id,
                        0, // block_number always 0 for metadata blobs
                        blob_guid.volume_id,
                        version,
                        is_new,
                        content_clone,
                        rpc_timeout,
                    ),
                )
                .await;
                (address, result)
            });
            write_futures.push(write_task);
        }

        let mut successful_writes = 0;
        let mut errors = Vec::new();

        // Wait only until we achieve write quorum
        while let Some(join_result) = write_futures.next().await {
            let (address, result) = match join_result {
                Ok(task_result) => task_result,
                Err(_join_error) => {
                    warn!("Task join error for metadata blob write operation");
                    continue;
                }
            };

            match result {
                Ok(Ok(())) => {
                    successful_writes += 1;
                    debug!("Successful metadata blob write to BSS node: {}", address);
                }
                Ok(Err(rpc_error)) => {
                    warn!(
                        "RPC error writing metadata blob to BSS node {}: {}",
                        address, rpc_error
                    );
                    errors.push(format!("{}: {}", address, rpc_error));
                }
                Err(_timeout_error) => {
                    warn!("Timeout writing metadata blob to BSS node: {}", address);
                    errors.push(format!("{}: timeout", address));
                }
            }

            // Check if we've achieved write quorum
            if successful_writes >= write_quorum {
                histogram!("metadatavg_put_metadata_blob_nanos", "result" => "success")
                    .record(start.elapsed().as_nanos() as f64);
                info!(
                    "Metadata blob write quorum achieved ({}/{}) for blob {} version {}, remaining operations continue in background",
                    successful_writes,
                    selected_volume.bss_nodes.len(),
                    blob_guid.blob_id,
                    version
                );
                return Ok(());
            }
        }

        // Write quorum not achieved
        histogram!("metadatavg_put_metadata_blob_nanos", "result" => "quorum_failure")
            .record(start.elapsed().as_nanos() as f64);
        error!(
            "Metadata blob write quorum failed ({}/{}). Errors: {:?}",
            successful_writes, self.quorum_config.w, errors
        );
        Err(DataVgError::QuorumFailure(format!(
            "Metadata blob write quorum failed ({}/{}): {}",
            successful_writes,
            self.quorum_config.w,
            errors.join("; ")
        )))
    }

    /// Multi-BSS get_metadata_blob with quorum-based reads
    pub async fn get_metadata_blob(
        &self,
        blob_guid: MetadataBlobGuid,
        version: u64,
    ) -> Result<Bytes, DataVgError> {
        let start = Instant::now();

        let blob_id = blob_guid.blob_id;
        let volume_id = blob_guid.volume_id;

        tracing::debug!(%blob_id, volume_id, available_volumes=?self.volumes.iter().map(|v| v.volume_id).collect::<Vec<_>>(), "get_metadata_blob looking for volume");

        let volume = self
            .volumes
            .iter()
            .find(|v| v.volume_id == volume_id)
            .ok_or_else(|| {
                tracing::error!(%blob_id, volume_id, available_volumes=?self.volumes.iter().map(|v| v.volume_id).collect::<Vec<_>>(), "Metadata volume not found in MetadataVgProxy for get_metadata_blob");
                DataVgError::InitializationError(format!("Metadata volume {} not found", volume_id))
            })?;

        // Always use quorum read for metadata blobs
        debug!(
            "Performing quorum read from {} metadata BSS nodes (R={})",
            volume.bss_nodes.len(),
            self.quorum_config.r
        );

        let bss_connection_pools = self.bss_connection_pools.clone();
        let rpc_timeout = self.rpc_timeout;
        let bss_nodes = volume.bss_nodes.clone();
        let read_quorum = self.quorum_config.r as usize;

        // Spawn all read tasks immediately
        let mut read_futures = FuturesUnordered::new();
        for bss_node in bss_nodes {
            let address = format!("{}:{}", bss_node.ip, bss_node.port);
            let bss_pools = bss_connection_pools.clone();

            let read_task = tokio::spawn(async move {
                let result = timeout(
                    rpc_timeout,
                    Self::get_metadata_blob_from_node(
                        &bss_pools,
                        &address,
                        blob_guid.blob_id,
                        0, // block_number always 0 for metadata blobs
                        volume_id,
                        version,
                        rpc_timeout,
                    ),
                )
                .await;
                (address, result)
            });
            read_futures.push(read_task);
        }

        let mut successful_reads = 0;
        let mut successful_responses: Vec<(Bytes, u64)> = Vec::new();
        let mut errors = Vec::new();

        // Wait for R successful reads as per quorum requirements
        // TODO: quorum writeback
        while let Some(join_result) = read_futures.next().await {
            let (address, result) = match join_result {
                Ok(task_result) => task_result,
                Err(_join_error) => {
                    warn!("Task join error for metadata blob read operation");
                    continue;
                }
            };

            match result {
                Ok(Ok((blob_data, actual_version))) => {
                    successful_reads += 1;
                    successful_responses.push((blob_data, actual_version));
                    debug!(
                        "Successful metadata blob read from BSS node: {} (version={})",
                        address, actual_version
                    );

                    // Check if we've achieved read quorum
                    if successful_reads >= read_quorum {
                        // Choose the response with the highest version (most recent)
                        let best_response = successful_responses
                            .iter()
                            .max_by_key(|(_, version)| version)
                            .unwrap();

                        histogram!("metadatavg_get_metadata_blob_nanos", "result" => "quorum_success")
                            .record(start.elapsed().as_nanos() as f64);
                        info!(
                            "Metadata blob read quorum achieved ({}/{}) for blob {} with version {}",
                            successful_reads,
                            volume.bss_nodes.len(),
                            blob_id,
                            best_response.1
                        );
                        return Ok(best_response.0.clone());
                    }
                }
                Ok(Err(rpc_error)) => {
                    warn!(
                        "RPC error reading metadata blob from BSS node {}: {}",
                        address, rpc_error
                    );
                    errors.push(format!("{}: {}", address, rpc_error));
                }
                Err(_timeout_error) => {
                    warn!("Timeout reading metadata blob from BSS node: {}", address);
                    errors.push(format!("{}: timeout", address));
                }
            }
        }

        // Read quorum not achieved
        histogram!("metadatavg_get_metadata_blob_nanos", "result" => "quorum_failure")
            .record(start.elapsed().as_nanos() as f64);
        error!(
            "Metadata blob read quorum failed ({}/{}). Errors: {:?}",
            successful_reads, self.quorum_config.r, errors
        );
        Err(DataVgError::QuorumFailure(format!(
            "Metadata blob read quorum failed ({}/{}): {}",
            successful_reads,
            self.quorum_config.r,
            errors.join("; ")
        )))
    }

    pub async fn delete_metadata_blob(
        &self,
        blob_guid: MetadataBlobGuid,
    ) -> Result<(), DataVgError> {
        let start = Instant::now();

        let volume_id = blob_guid.volume_id;
        let volume = self
            .volumes
            .iter()
            .find(|v| v.volume_id == volume_id)
            .ok_or_else(|| {
                DataVgError::InitializationError(format!("Metadata volume {} not found", volume_id))
            })?;

        // Send delete to all replicas (best effort) using stream-based approach
        let bss_connection_pools = self.bss_connection_pools.clone();
        let rpc_timeout = self.rpc_timeout;
        let bss_nodes = volume.bss_nodes.clone(); // Clone to avoid lifetime issues
        let node_count = bss_nodes.len();

        let delete_results: Vec<(String, Result<_, _>)> = futures::stream::iter(bss_nodes)
            .map(|bss_node| {
                let address = format!("{}:{}", bss_node.ip, bss_node.port);
                let bss_pools = bss_connection_pools.clone();

                async move {
                    let result = timeout(
                        rpc_timeout,
                        Self::delete_metadata_blob_from_node(
                            &bss_pools,
                            &address,
                            blob_guid.blob_id,
                            0, // block_number always 0 for metadata blobs
                            volume_id,
                            rpc_timeout,
                        ),
                    )
                    .await;
                    (address, result)
                }
            })
            .buffer_unordered(node_count)
            .collect()
            .await;

        let mut successful_deletes = 0;
        let mut errors = Vec::new();

        for (address, result) in delete_results {
            match result {
                Ok(Ok(())) => {
                    successful_deletes += 1;
                    debug!("Successful metadata blob delete from BSS node: {}", address);
                }
                Ok(Err(rpc_error)) => {
                    warn!(
                        "RPC error deleting metadata blob from BSS node {}: {}",
                        address, rpc_error
                    );
                    errors.push(format!("{}: {}", address, rpc_error));
                }
                Err(_timeout_error) => {
                    warn!("Timeout deleting metadata blob from BSS node: {}", address);
                    errors.push(format!("{}: timeout", address));
                }
            }
        }

        histogram!("metadatavg_delete_metadata_blob_nanos")
            .record(start.elapsed().as_nanos() as f64);

        if successful_deletes > 0 {
            if !errors.is_empty() {
                warn!(
                    "Metadata blob delete partially succeeded ({}/{}) with errors: {:?}",
                    successful_deletes,
                    volume.bss_nodes.len(),
                    errors
                );
            } else {
                debug!(
                    "Metadata blob delete succeeded on all {} replicas",
                    successful_deletes
                );
            }
            Ok(())
        } else {
            error!("Metadata blob delete failed on all replicas: {:?}", errors);
            Err(DataVgError::QuorumFailure(format!(
                "Metadata blob delete failed on all replicas: {}",
                errors.join("; ")
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    fn create_test_metadata_vg_info() -> MetadataVgInfo {
        MetadataVgInfo {
            volumes: vec![
                MetadataVolume {
                    volume_id: 0,
                    bss_nodes: vec![
                        MetadataBssNode {
                            node_id: "bss0".to_string(),
                            ip: "127.0.0.1".to_string(),
                            port: 9000,
                        },
                        MetadataBssNode {
                            node_id: "bss1".to_string(),
                            ip: "127.0.0.1".to_string(),
                            port: 9001,
                        },
                        MetadataBssNode {
                            node_id: "bss2".to_string(),
                            ip: "127.0.0.1".to_string(),
                            port: 9002,
                        },
                    ],
                },
                MetadataVolume {
                    volume_id: 1,
                    bss_nodes: vec![
                        MetadataBssNode {
                            node_id: "bss3".to_string(),
                            ip: "127.0.0.1".to_string(),
                            port: 9003,
                        },
                        MetadataBssNode {
                            node_id: "bss4".to_string(),
                            ip: "127.0.0.1".to_string(),
                            port: 9004,
                        },
                        MetadataBssNode {
                            node_id: "bss5".to_string(),
                            ip: "127.0.0.1".to_string(),
                            port: 9005,
                        },
                    ],
                },
            ],
            quorum: Some(QuorumConfig { n: 6, r: 4, w: 4 }),
        }
    }

    #[test]
    fn test_metadata_blob_guid_creation() {
        let blob_id = Uuid::now_v7();
        let volume_id = 42;

        let guid = MetadataBlobGuid { blob_id, volume_id };

        assert_eq!(guid.blob_id, blob_id);
        assert_eq!(guid.volume_id, volume_id);

        // Test Display implementation
        let display_str = format!("{}", guid);
        assert!(display_str.contains(":m42"));
        assert!(display_str.contains(&blob_id.to_string()));
    }

    #[test]
    fn test_metadata_blob_guid_hash_and_eq() {
        use std::collections::HashMap;

        let blob_id1 = Uuid::now_v7();
        let blob_id2 = Uuid::now_v7();

        let guid1 = MetadataBlobGuid {
            blob_id: blob_id1,
            volume_id: 0,
        };
        let guid2 = MetadataBlobGuid {
            blob_id: blob_id1,
            volume_id: 0,
        };
        let guid3 = MetadataBlobGuid {
            blob_id: blob_id2,
            volume_id: 0,
        };
        let guid4 = MetadataBlobGuid {
            blob_id: blob_id1,
            volume_id: 1,
        };

        // Test equality
        assert_eq!(guid1, guid2);
        assert_ne!(guid1, guid3);
        assert_ne!(guid1, guid4);

        // Test hash (via HashMap usage)
        let mut map = HashMap::new();
        map.insert(guid1, "value1");
        map.insert(guid3, "value2");
        map.insert(guid4, "value3");

        assert_eq!(map.get(&guid2), Some(&"value1")); // guid1 == guid2
        assert_eq!(map.get(&guid3), Some(&"value2"));
        assert_eq!(map.get(&guid4), Some(&"value3"));
        assert_eq!(map.len(), 3);
    }

    #[test]
    fn test_metadata_vg_info_structure() {
        let vg_info = create_test_metadata_vg_info();

        assert_eq!(vg_info.volumes.len(), 2);
        assert_eq!(vg_info.volumes[0].volume_id, 0);
        assert_eq!(vg_info.volumes[1].volume_id, 1);

        assert_eq!(vg_info.volumes[0].bss_nodes.len(), 3);
        assert_eq!(vg_info.volumes[1].bss_nodes.len(), 3);

        let quorum = vg_info.quorum.unwrap();
        assert_eq!(quorum.n, 6);
        assert_eq!(quorum.r, 4);
        assert_eq!(quorum.w, 4);

        // Test node addresses
        assert_eq!(vg_info.volumes[0].bss_nodes[0].ip, "127.0.0.1");
        assert_eq!(vg_info.volumes[0].bss_nodes[0].port, 9000);
        assert_eq!(vg_info.volumes[0].bss_nodes[0].node_id, "bss0");
        assert_eq!(vg_info.volumes[1].bss_nodes[2].ip, "127.0.0.1");
        assert_eq!(vg_info.volumes[1].bss_nodes[2].port, 9005);
        assert_eq!(vg_info.volumes[1].bss_nodes[2].node_id, "bss5");
    }

    #[test]
    fn test_round_robin_volume_selection() {
        let volumes = [0u8, 1u8, 2u8];
        let mut counter = 0u64;

        let selections: Vec<u8> = (0..10)
            .map(|_| {
                let selection = volumes[(counter as usize) % volumes.len()];
                counter += 1;
                selection
            })
            .collect();

        assert_eq!(selections, vec![0, 1, 2, 0, 1, 2, 0, 1, 2, 0]);
    }

    #[test]
    fn test_metadata_volume_id_is_u16() {
        let guid = MetadataBlobGuid {
            blob_id: Uuid::now_v7(),
            volume_id: 65535u16, // Max u16 value
        };

        assert_eq!(guid.volume_id, 65535u16);

        let volume = MetadataVolume {
            volume_id: 65535u16,
            bss_nodes: vec![],
        };

        assert_eq!(volume.volume_id, 65535u16);
    }

    #[test]
    fn test_serialization_traits() {
        let guid = MetadataBlobGuid {
            blob_id: Uuid::now_v7(),
            volume_id: 123,
        };

        let _archived = rkyv::to_bytes::<rkyv::rancor::Error>(&guid).unwrap();

        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(guid);
        assert!(set.contains(&guid));
    }

    #[test]
    fn test_quorum_configuration_validation() {
        let valid_quorum = QuorumConfig { n: 6, r: 4, w: 4 };
        let edge_case_quorum = QuorumConfig { n: 3, r: 2, w: 2 };
        let minimal_quorum = QuorumConfig { n: 1, r: 1, w: 1 };

        assert!(valid_quorum.r <= valid_quorum.n);
        assert!(valid_quorum.w <= valid_quorum.n);
        assert!(valid_quorum.r + valid_quorum.w > valid_quorum.n);

        assert!(edge_case_quorum.r <= edge_case_quorum.n);
        assert!(edge_case_quorum.w <= edge_case_quorum.n);
        assert!(edge_case_quorum.r + edge_case_quorum.w > edge_case_quorum.n);

        assert!(minimal_quorum.r <= minimal_quorum.n);
        assert!(minimal_quorum.w <= minimal_quorum.n);
    }
}
