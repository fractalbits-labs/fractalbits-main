use crate::DataVgError;
use bytes::Bytes;
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

/// BlobGuid combines blob_id (UUID) with volume_id for multi-BSS support
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, rkyv::Archive, rkyv::Deserialize, rkyv::Serialize,
)]
pub struct DataBlobGuid {
    pub blob_id: Uuid,
    pub volume_id: u32,
}

impl std::fmt::Display for DataBlobGuid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "vol{}-{}", self.volume_id, self.blob_id)
    }
}

pub struct DataVgProxy {
    volumes: Vec<rss_codec::DataVolume>,
    bss_connection_pools: HashMap<String, ConnPool<Arc<RpcClientBss>, String>>,
    round_robin_counter: AtomicU64,
    quorum_config: rss_codec::QuorumConfig,
    rpc_timeout: Duration,
}

impl DataVgProxy {
    pub async fn new(
        data_vg_info: rss_codec::DataVgInfo,
        rpc_timeout: Duration,
    ) -> Result<Self, DataVgError> {
        info!(
            "Initializing DataVgProxy with {} volumes",
            data_vg_info.volumes.len()
        );

        let quorum_config = data_vg_info.quorum.ok_or_else(|| {
            DataVgError::InitializationError(
                "QuorumConfig is required but not provided in DataVgInfo".to_string(),
            )
        })?;

        // Create connection pools for all BSS nodes
        let mut bss_connection_pools = HashMap::new();

        for volume in &data_vg_info.volumes {
            for bss_node in &volume.bss_nodes {
                if !bss_connection_pools.contains_key(&bss_node.address) {
                    info!(
                        "Creating connection pool for BSS node: {}",
                        bss_node.address
                    );
                    let pool = ConnPool::new();

                    // Create connections to this BSS node
                    let bss_conn_num = 4; // Default connection pool size
                    for i in 0..bss_conn_num {
                        debug!(
                            "Creating connection {}/{} to BSS {}",
                            i + 1,
                            bss_conn_num,
                            bss_node.address
                        );
                        let client = Arc::new(
                            <RpcClientBss as Poolable>::new(bss_node.address.clone())
                                .await
                                .map_err(|e| {
                                    DataVgError::InitializationError(format!(
                                        "Failed to connect to BSS {}: {}",
                                        bss_node.address, e
                                    ))
                                })?,
                        );
                        pool.pooled(bss_node.address.clone(), client);
                    }

                    bss_connection_pools.insert(bss_node.address.clone(), pool);
                }
            }
        }

        info!(
            "DataVgProxy initialized successfully with {} BSS connection pools",
            bss_connection_pools.len()
        );

        Ok(Self {
            volumes: data_vg_info.volumes,
            bss_connection_pools,
            round_robin_counter: AtomicU64::new(0),
            quorum_config,
            rpc_timeout,
        })
    }

    pub fn select_volume_for_blob(&self) -> u32 {
        // Use round-robin to select volume
        let counter = self
            .round_robin_counter
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let volume_index = (counter as usize) % self.volumes.len();
        self.volumes[volume_index].volume_id
    }

    async fn checkout_bss_client(
        &self,
        bss_address: &str,
    ) -> Result<Arc<RpcClientBss>, DataVgError> {
        let pool = self.bss_connection_pools.get(bss_address).ok_or_else(|| {
            DataVgError::InitializationError(format!("No connection pool for BSS {}", bss_address))
        })?;

        let start = Instant::now();
        let client = pool.checkout(bss_address.to_string()).await.map_err(|e| {
            DataVgError::InitializationError(format!(
                "Failed to checkout BSS client for {}: {}",
                bss_address, e
            ))
        })?;

        histogram!("checkout_rpc_client_nanos", "type" => "bss_datavg")
            .record(start.elapsed().as_nanos() as f64);

        Ok(client)
    }

    async fn get_blob_from_node_instance(
        &self,
        bss_address: &str,
        blob_id: Uuid,
        block_number: u32,
        volume_id: u32,
    ) -> Result<Bytes, RpcError> {
        tracing::debug!(%blob_id, %bss_address, block_number, volume_id, "get_blob_from_node_instance calling BSS");

        let client = self
            .checkout_bss_client(bss_address)
            .await
            .map_err(|e| RpcError::InternalResponseError(e.to_string()))?;

        let mut body = Bytes::new();
        let result = client
            .get_data_blob(
                blob_id,
                block_number,
                volume_id as u8,
                &mut body,
                Some(self.rpc_timeout),
            )
            .await;

        tracing::debug!(%blob_id, %bss_address, block_number, volume_id, data_size=body.len(), result=?result, "get_blob_from_node_instance result");

        result?;
        Ok(body)
    }

    // Standalone versions of the methods for use in async closures
    async fn checkout_bss_client_for_async(
        bss_connection_pools: &HashMap<String, ConnPool<Arc<RpcClientBss>, String>>,
        bss_address: &str,
    ) -> Result<Arc<RpcClientBss>, DataVgError> {
        let pool = bss_connection_pools.get(bss_address).ok_or_else(|| {
            DataVgError::InitializationError(format!("No connection pool for BSS {}", bss_address))
        })?;

        let start = Instant::now();
        let client = pool.checkout(bss_address.to_string()).await.map_err(|e| {
            DataVgError::InitializationError(format!(
                "Failed to checkout BSS client for {}: {}",
                bss_address, e
            ))
        })?;

        histogram!("checkout_rpc_client_nanos", "type" => "bss_datavg")
            .record(start.elapsed().as_nanos() as f64);

        Ok(client)
    }

    async fn put_blob_to_node(
        bss_connection_pools: &HashMap<String, ConnPool<Arc<RpcClientBss>, String>>,
        bss_address: &str,
        blob_id: Uuid,
        block_number: u32,
        volume_id: u32,
        body: Bytes,
        rpc_timeout: Duration,
    ) -> Result<(), RpcError> {
        let client = Self::checkout_bss_client_for_async(bss_connection_pools, bss_address)
            .await
            .map_err(|e| RpcError::InternalResponseError(e.to_string()))?;

        client
            .put_data_blob(
                blob_id,
                block_number,
                volume_id as u8,
                body,
                Some(rpc_timeout),
            )
            .await
    }

    async fn get_blob_from_node(
        bss_connection_pools: &HashMap<String, ConnPool<Arc<RpcClientBss>, String>>,
        bss_address: &str,
        blob_id: Uuid,
        block_number: u32,
        volume_id: u32,
        rpc_timeout: Duration,
    ) -> Result<Bytes, RpcError> {
        let client = Self::checkout_bss_client_for_async(bss_connection_pools, bss_address)
            .await
            .map_err(|e| RpcError::InternalResponseError(e.to_string()))?;

        let mut body = Bytes::new();
        client
            .get_data_blob(
                blob_id,
                block_number,
                volume_id as u8,
                &mut body,
                Some(rpc_timeout),
            )
            .await?;
        Ok(body)
    }

    async fn delete_blob_from_node(
        bss_connection_pools: &HashMap<String, ConnPool<Arc<RpcClientBss>, String>>,
        bss_address: &str,
        blob_id: Uuid,
        block_number: u32,
        volume_id: u32,
        rpc_timeout: Duration,
    ) -> Result<(), RpcError> {
        let client = Self::checkout_bss_client_for_async(bss_connection_pools, bss_address)
            .await
            .map_err(|e| RpcError::InternalResponseError(e.to_string()))?;

        client
            .delete_data_blob(blob_id, block_number, volume_id as u8, Some(rpc_timeout))
            .await
    }
}

impl DataVgProxy {
    /// Create a new data blob GUID with a fresh UUID and selected volume
    pub fn create_data_blob_guid(&self) -> DataBlobGuid {
        let blob_id = Uuid::now_v7();
        let volume_id = self.select_volume_for_blob();
        DataBlobGuid { blob_id, volume_id }
    }

    /// Multi-BSS put_blob with quorum-based replication
    pub async fn put_blob(
        &self,
        blob_guid: DataBlobGuid,
        block_number: u32,
        body: Bytes,
    ) -> Result<(), DataVgError> {
        let start = Instant::now();
        histogram!("blob_size", "operation" => "put").record(body.len() as f64);

        // Use the volume_id from blob_guid to find the volume
        let selected_volume = self
            .volumes
            .iter()
            .find(|v| v.volume_id == blob_guid.volume_id)
            .ok_or_else(|| {
                DataVgError::InitializationError(format!(
                    "Volume {} not found in DataVgProxy",
                    blob_guid.volume_id
                ))
            })?;
        debug!("Using volume {} for put_blob", selected_volume.volume_id);

        // Spawn individual tasks for each write operation
        let bss_connection_pools = self.bss_connection_pools.clone();
        let rpc_timeout = self.rpc_timeout;
        let write_quorum = self.quorum_config.w as usize;
        let bss_nodes = selected_volume.bss_nodes.clone();

        // Spawn all write tasks immediately
        let mut write_futures = FuturesUnordered::new();
        for bss_node in bss_nodes {
            let address = bss_node.address.clone();
            let body_clone = body.clone();
            let bss_pools = bss_connection_pools.clone();

            let write_task = tokio::spawn(async move {
                let result = timeout(
                    rpc_timeout,
                    Self::put_blob_to_node(
                        &bss_pools,
                        &address,
                        blob_guid.blob_id,
                        block_number,
                        blob_guid.volume_id,
                        body_clone,
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
                    warn!("Task join error for write operation");
                    continue;
                }
            };

            match result {
                Ok(Ok(())) => {
                    successful_writes += 1;
                    debug!("Successful write to BSS node: {}", address);
                }
                Ok(Err(rpc_error)) => {
                    warn!("RPC error writing to BSS node {}: {}", address, rpc_error);
                    errors.push(format!("{}: {}", address, rpc_error));
                }
                Err(_timeout_error) => {
                    warn!("Timeout writing to BSS node: {}", address);
                    errors.push(format!("{}: timeout", address));
                }
            }

            // Check if we've achieved write quorum
            if successful_writes >= write_quorum {
                histogram!("datavg_put_blob_nanos", "result" => "success")
                    .record(start.elapsed().as_nanos() as f64);
                info!(
                    "Write quorum achieved ({}/{}) for blob {}:{}, remaining operations continue in background",
                    successful_writes,
                    selected_volume.bss_nodes.len(),
                    blob_guid.blob_id,
                    block_number
                );
                return Ok(());
            }
        }

        // Write quorum not achieved
        histogram!("datavg_put_blob_nanos", "result" => "quorum_failure")
            .record(start.elapsed().as_nanos() as f64);
        error!(
            "Write quorum failed ({}/{}). Errors: {:?}",
            successful_writes, self.quorum_config.w, errors
        );
        Err(DataVgError::QuorumFailure(format!(
            "Write quorum failed ({}/{}): {}",
            successful_writes,
            self.quorum_config.w,
            errors.join("; ")
        )))
    }

    /// Multi-BSS get_blob with quorum-based reads
    pub async fn get_blob(
        &self,
        blob_guid: DataBlobGuid,
        block_number: u32,
        body: &mut Bytes,
    ) -> Result<(), DataVgError> {
        let start = Instant::now();

        let blob_id = blob_guid.blob_id;
        let volume_id = blob_guid.volume_id;

        tracing::debug!(%blob_id, volume_id, available_volumes=?self.volumes.iter().map(|v| v.volume_id).collect::<Vec<_>>(), "get_blob looking for volume");

        let volume = self
            .volumes
            .iter()
            .find(|v| v.volume_id == volume_id)
            .ok_or_else(|| {
                tracing::error!(%blob_id, volume_id, available_volumes=?self.volumes.iter().map(|v| v.volume_id).collect::<Vec<_>>(), "Volume not found in DataVgProxy for get_blob");
                DataVgError::InitializationError(format!("Volume {} not found", volume_id))
            })?;

        // Fast path: try reading from a random BSS node first
        if let Some(random_node) = volume.bss_nodes.first() {
            debug!(
                "Attempting fast path read from BSS node: {}",
                random_node.address
            );
            match self
                .get_blob_from_node_instance(&random_node.address, blob_id, block_number, volume_id)
                .await
            {
                Ok(blob_data) => {
                    histogram!("datavg_get_blob_nanos", "result" => "fast_path_success")
                        .record(start.elapsed().as_nanos() as f64);
                    *body = blob_data;
                    return Ok(());
                }
                Err(e) => {
                    warn!(
                        "Fast path read failed from {}: {}, falling back to quorum read",
                        random_node.address, e
                    );
                }
            }
        }

        // Fallback: read from all nodes using spawned tasks (normally would be R nodes for quorum)
        debug!(
            "Performing quorum read from {} nodes",
            volume.bss_nodes.len()
        );

        let bss_connection_pools = self.bss_connection_pools.clone();
        let rpc_timeout = self.rpc_timeout;
        let bss_nodes = volume.bss_nodes.clone();
        let _read_quorum = self.quorum_config.r as usize;

        // Spawn all read tasks immediately
        let mut read_futures = FuturesUnordered::new();
        for bss_node in bss_nodes {
            let address = bss_node.address.clone();
            let bss_pools = bss_connection_pools.clone();

            let read_task = tokio::spawn(async move {
                let result = timeout(
                    rpc_timeout,
                    Self::get_blob_from_node(
                        &bss_pools,
                        &address,
                        blob_guid.blob_id,
                        block_number,
                        volume_id,
                        rpc_timeout,
                    ),
                )
                .await;
                (address, result)
            });
            read_futures.push(read_task);
        }

        let mut successful_reads = 0;
        let mut successful_blob_data = None;

        // Wait until we get a successful read (quorum of 1) or all fail
        // TODO: quorum writeback
        while let Some(join_result) = read_futures.next().await {
            let (address, result) = match join_result {
                Ok(task_result) => task_result,
                Err(_join_error) => {
                    warn!("Task join error for read operation");
                    continue;
                }
            };

            match result {
                Ok(Ok(blob_data)) => {
                    successful_reads += 1;
                    debug!("Successful read from BSS node: {}", address);
                    if successful_blob_data.is_none() {
                        successful_blob_data = Some(blob_data);
                        // For reads, we can return as soon as we get one successful result
                        break;
                    }
                }
                Ok(Err(rpc_error)) => {
                    warn!("RPC error reading from BSS node {}: {}", address, rpc_error);
                }
                Err(_timeout_error) => {
                    warn!("Timeout reading from BSS node: {}", address);
                }
            }
        }

        if let Some(blob_data) = successful_blob_data {
            histogram!("datavg_get_blob_nanos", "result" => "success")
                .record(start.elapsed().as_nanos() as f64);
            info!(
                "Read successful from {}/{} nodes for blob {}:{}",
                successful_reads,
                volume.bss_nodes.len(),
                blob_id,
                block_number
            );
            *body = blob_data;
            return Ok(());
        }

        // All reads failed
        histogram!("datavg_get_blob_nanos", "result" => "all_failed")
            .record(start.elapsed().as_nanos() as f64);
        error!(
            "All read attempts failed for blob {}:{}",
            blob_id, block_number
        );
        Err(DataVgError::QuorumFailure(
            "All read attempts failed".to_string(),
        ))
    }

    pub async fn delete_blob(
        &self,
        _tracking_root_blob_name: Option<&str>,
        blob_guid: DataBlobGuid,
        block_number: u32,
    ) -> Result<(), DataVgError> {
        let start = Instant::now();

        let volume_id = blob_guid.volume_id;
        let volume = self
            .volumes
            .iter()
            .find(|v| v.volume_id == volume_id)
            .ok_or_else(|| {
                DataVgError::InitializationError(format!("Volume {} not found", volume_id))
            })?;

        // Send delete to all replicas (best effort) using stream-based approach
        let bss_connection_pools = self.bss_connection_pools.clone();
        let rpc_timeout = self.rpc_timeout;
        let bss_nodes = volume.bss_nodes.clone(); // Clone to avoid lifetime issues
        let node_count = bss_nodes.len();

        let delete_results: Vec<(String, Result<_, _>)> = futures::stream::iter(bss_nodes)
            .map(|bss_node| {
                let address = bss_node.address.clone();
                let bss_pools = bss_connection_pools.clone();

                async move {
                    let result = timeout(
                        rpc_timeout,
                        Self::delete_blob_from_node(
                            &bss_pools,
                            &address,
                            blob_guid.blob_id,
                            block_number,
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
                    debug!("Successful delete from BSS node: {}", address);
                }
                Ok(Err(rpc_error)) => {
                    warn!(
                        "RPC error deleting from BSS node {}: {}",
                        address, rpc_error
                    );
                    errors.push(format!("{}: {}", address, rpc_error));
                }
                Err(_timeout_error) => {
                    warn!("Timeout deleting from BSS node: {}", address);
                    errors.push(format!("{}: timeout", address));
                }
            }
        }

        histogram!("datavg_delete_blob_nanos").record(start.elapsed().as_nanos() as f64);

        if successful_deletes > 0 {
            if !errors.is_empty() {
                warn!(
                    "Delete partially succeeded ({}/{}) with errors: {:?}",
                    successful_deletes,
                    volume.bss_nodes.len(),
                    errors
                );
            } else {
                debug!("Delete succeeded on all {} replicas", successful_deletes);
            }
            Ok(())
        } else {
            error!("Delete failed on all replicas: {:?}", errors);
            Err(DataVgError::QuorumFailure(format!(
                "Delete failed on all replicas: {}",
                errors.join("; ")
            )))
        }
    }
}
