use crate::DataVgError;
use bytes::Bytes;
use data_types::{DataBlobGuid, DataVgInfo, QuorumConfig};
use futures::stream::{FuturesUnordered, StreamExt};
use metrics::histogram;
use rand::seq::SliceRandom;
use rpc_client_bss::RpcClientBss;
use rpc_client_common::{RpcError, bss_rpc_retry};
use slotmap_conn_pool::{ConnPool, Poolable};
use std::{
    sync::{Arc, atomic::AtomicU64},
    time::{Duration, Instant},
};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

struct InFlightGuard<'a> {
    counter: &'a AtomicU64,
}

impl<'a> InFlightGuard<'a> {
    fn new(counter: &'a AtomicU64) -> Self {
        counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Self { counter }
    }
}

impl<'a> Drop for InFlightGuard<'a> {
    fn drop(&mut self) {
        self.counter
            .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
    }
}

struct BssNodeWithPool {
    #[allow(dead_code)]
    node_id: String,
    address: String,
    pool: ConnPool<Arc<RpcClientBss>, String>,
    in_flight_requests: AtomicU64,
}

struct BssNodePoolWrapper {
    pool: ConnPool<Arc<RpcClientBss>, String>,
    address: String,
}

impl BssNodePoolWrapper {
    async fn checkout_rpc_client_bss(&self) -> Result<Arc<RpcClientBss>, slotmap_conn_pool::Error> {
        let start = Instant::now();
        let client = self.pool.checkout(self.address.clone()).await?;
        histogram!("checkout_rpc_client_nanos", "type" => "bss_datavg")
            .record(start.elapsed().as_nanos() as f64);
        Ok(client)
    }
}

struct VolumeWithPools {
    volume_id: u16,
    bss_nodes: Vec<BssNodeWithPool>,
}

pub struct DataVgProxy {
    volumes: Vec<VolumeWithPools>,
    round_robin_counter: AtomicU64,
    quorum_config: QuorumConfig,
    rpc_timeout: Duration,
}

impl DataVgProxy {
    pub async fn new(data_vg_info: DataVgInfo, rpc_timeout: Duration) -> Result<Self, DataVgError> {
        info!(
            "Initializing DataVgProxy with {} volumes",
            data_vg_info.volumes.len()
        );

        let quorum_config = data_vg_info.quorum.ok_or_else(|| {
            DataVgError::InitializationError(
                "QuorumConfig is required but not provided in DataVgInfo".to_string(),
            )
        })?;

        let mut volumes_with_pools = Vec::new();

        for volume in data_vg_info.volumes {
            let mut bss_nodes_with_pools = Vec::new();

            for bss_node in volume.bss_nodes {
                let address = format!("{}:{}", bss_node.ip, bss_node.port);
                info!(
                    "Creating dedicated connection pool for BSS node {}: {}",
                    bss_node.node_id, address
                );

                let pool = ConnPool::new();
                let bss_conn_num = 4;

                for i in 0..bss_conn_num {
                    debug!(
                        "Creating connection {}/{} to BSS {}",
                        i + 1,
                        bss_conn_num,
                        address
                    );
                    let client = Arc::new(
                        <RpcClientBss as Poolable>::new(address.clone())
                            .await
                            .map_err(|e| {
                                DataVgError::InitializationError(format!(
                                    "Failed to connect to BSS {}: {}",
                                    address, e
                                ))
                            })?,
                    );
                    pool.pooled(address.clone(), client);
                }

                bss_nodes_with_pools.push(BssNodeWithPool {
                    node_id: bss_node.node_id,
                    address,
                    pool,
                    in_flight_requests: AtomicU64::new(0),
                });
            }

            volumes_with_pools.push(VolumeWithPools {
                volume_id: volume.volume_id,
                bss_nodes: bss_nodes_with_pools,
            });
        }

        info!(
            "DataVgProxy initialized successfully with {} volumes",
            volumes_with_pools.len()
        );

        Ok(Self {
            volumes: volumes_with_pools,
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

    async fn get_blob_from_node_instance(
        &self,
        bss_node: &BssNodeWithPool,
        blob_guid: DataBlobGuid,
        block_number: u32,
    ) -> Result<Bytes, RpcError> {
        tracing::debug!(%blob_guid, bss_address=%bss_node.address, block_number, "get_blob_from_node_instance calling BSS");

        let pool_for_retry = BssNodePoolWrapper {
            pool: bss_node.pool.clone(),
            address: bss_node.address.clone(),
        };

        let mut body = Bytes::new();
        bss_rpc_retry!(
            pool_for_retry,
            get_data_blob(blob_guid, block_number, &mut body, Some(self.rpc_timeout))
        )
        .await?;

        tracing::debug!(%blob_guid, bss_address=%bss_node.address, block_number, data_size=body.len(), "get_blob_from_node_instance result");

        Ok(body)
    }

    fn delete_blob_from_node(
        bss_node: &BssNodeWithPool,
        blob_guid: DataBlobGuid,
        block_number: u32,
        rpc_timeout: Duration,
    ) -> impl std::future::Future<Output = (String, Result<(), RpcError>)> + Send + 'static {
        let start_node = Instant::now();
        let address = bss_node.address.clone();
        let pool_for_retry = BssNodePoolWrapper {
            pool: bss_node.pool.clone(),
            address: address.clone(),
        };

        async move {
            let result = bss_rpc_retry!(
                pool_for_retry,
                delete_data_blob(blob_guid, block_number, Some(rpc_timeout))
            )
            .await;

            let result_label = if result.is_ok() { "success" } else { "failure" };
            histogram!("datavg_delete_blob_node_nanos", "bss_node" => address.clone(), "result" => result_label)
                .record(start_node.elapsed().as_nanos() as f64);

            (address, result)
        }
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

        let rpc_timeout = self.rpc_timeout;
        let write_quorum = self.quorum_config.w as usize;

        let mut bss_node_indices: Vec<usize> = (0..selected_volume.bss_nodes.len()).collect();
        bss_node_indices.shuffle(&mut rand::thread_rng());

        let mut write_futures = FuturesUnordered::new();
        for &index in &bss_node_indices {
            let bss_node = &selected_volume.bss_nodes[index];
            let future = Self::put_blob_to_node(
                bss_node,
                blob_guid,
                block_number,
                body.clone(),
                rpc_timeout,
            );
            write_futures.push(future);
        }

        let mut successful_writes = 0;
        let mut errors = Vec::with_capacity(selected_volume.bss_nodes.len());

        // Wait only until we achieve write quorum
        while let Some((address, result)) = write_futures.next().await {
            match result {
                Ok(()) => {
                    successful_writes += 1;
                    debug!("Successful write to BSS node: {}", address);
                }
                Err(rpc_error) => {
                    warn!("RPC error writing to BSS node {}: {}", address, rpc_error);
                    errors.push(format!("{}: {}", address, rpc_error));
                }
            }

            // Check if we've achieved write quorum
            if successful_writes >= write_quorum {
                tokio::spawn(async move {
                    while let Some((addr, res)) = write_futures.next().await {
                        match res {
                            Ok(()) => {
                                debug!("Background write to {} completed", addr);
                            }
                            Err(e) => {
                                warn!("Background write to {} failed: {}", addr, e);
                            }
                        }
                    }
                });

                histogram!("datavg_put_blob_nanos", "result" => "success")
                    .record(start.elapsed().as_nanos() as f64);
                debug!(
                    "Write quorum achieved ({}/{}) for blob {}:{}",
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

    fn put_blob_to_node(
        bss_node: &BssNodeWithPool,
        blob_guid: DataBlobGuid,
        block_number: u32,
        body: Bytes,
        rpc_timeout: Duration,
    ) -> impl std::future::Future<Output = (String, Result<(), RpcError>)> + Send + 'static {
        let start_node = Instant::now();
        let address = bss_node.address.clone();
        let pool_for_retry = BssNodePoolWrapper {
            pool: bss_node.pool.clone(),
            address: address.clone(),
        };

        async move {
            let body_ref = &body;
            let result = bss_rpc_retry!(
                pool_for_retry,
                put_data_blob(blob_guid, block_number, body_ref.clone(), Some(rpc_timeout))
            )
            .await;

            let result_label = if result.is_ok() { "success" } else { "failure" };
            histogram!("datavg_put_blob_node_nanos", "bss_node" => address.clone(), "result" => result_label)
                .record(start_node.elapsed().as_nanos() as f64);

            (address, result)
        }
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

        // Fast path: try reading from the node with least in-flight requests
        if !volume.bss_nodes.is_empty() {
            let selected_node = volume
                .bss_nodes
                .iter()
                .min_by_key(|node| {
                    node.in_flight_requests
                        .load(std::sync::atomic::Ordering::Relaxed)
                })
                .unwrap();
            debug!(
                "Attempting fast path read from BSS node: {} (in_flight: {})",
                selected_node.address,
                selected_node
                    .in_flight_requests
                    .load(std::sync::atomic::Ordering::Relaxed)
            );
            let _guard = InFlightGuard::new(&selected_node.in_flight_requests);
            match self
                .get_blob_from_node_instance(selected_node, blob_guid, block_number)
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
                        selected_node.address, e
                    );
                }
            }
        }

        // Fallback: read from all nodes using spawned tasks (normally would be R nodes for quorum)
        debug!(
            "Performing quorum read from {} nodes",
            volume.bss_nodes.len()
        );

        let rpc_timeout = self.rpc_timeout;
        let _read_quorum = self.quorum_config.r as usize;

        // Spawn all read tasks immediately
        let mut read_futures = FuturesUnordered::new();
        for bss_node in &volume.bss_nodes {
            let address = bss_node.address.clone();
            let pool = bss_node.pool.clone();
            let pool_for_retry = BssNodePoolWrapper {
                pool,
                address: address.clone(),
            };

            let read_task = tokio::spawn(async move {
                let result = async {
                    let mut body = Bytes::new();
                    bss_rpc_retry!(
                        pool_for_retry,
                        get_data_blob(blob_guid, block_number, &mut body, Some(rpc_timeout))
                    )
                    .await?;
                    Ok::<Bytes, RpcError>(body)
                }
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
                Ok(blob_data) => {
                    successful_reads += 1;
                    debug!("Successful read from BSS node: {}", address);
                    if successful_blob_data.is_none() {
                        successful_blob_data = Some(blob_data);
                        // For reads, we can return as soon as we get one successful result
                        break;
                    }
                }
                Err(rpc_error) => {
                    warn!("RPC error reading from BSS node {}: {}", address, rpc_error);
                }
            }
        }

        if let Some(blob_data) = successful_blob_data {
            histogram!("datavg_get_blob_nanos", "result" => "success")
                .record(start.elapsed().as_nanos() as f64);
            debug!(
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
        blob_guid: DataBlobGuid,
        block_number: u32,
    ) -> Result<(), DataVgError> {
        let start = Instant::now();

        let volume = self
            .volumes
            .iter()
            .find(|v| v.volume_id == blob_guid.volume_id)
            .ok_or_else(|| {
                DataVgError::InitializationError(format!(
                    "Volume {} not found",
                    blob_guid.volume_id
                ))
            })?;

        let rpc_timeout = self.rpc_timeout;
        let write_quorum = self.quorum_config.w as usize;

        let mut delete_futures = FuturesUnordered::new();
        for bss_node in &volume.bss_nodes {
            let future =
                Self::delete_blob_from_node(bss_node, blob_guid, block_number, rpc_timeout);
            delete_futures.push(future);
        }

        let mut successful_deletes = 0;
        let mut errors = Vec::with_capacity(volume.bss_nodes.len());

        while let Some((address, result)) = delete_futures.next().await {
            match result {
                Ok(()) => {
                    successful_deletes += 1;
                    debug!("Successful delete from BSS node: {}", address);
                }
                Err(rpc_error) => {
                    warn!(
                        "RPC error deleting from BSS node {}: {}",
                        address, rpc_error
                    );
                    errors.push(format!("{}: {}", address, rpc_error));
                }
            }

            if successful_deletes >= write_quorum {
                if !delete_futures.is_empty() {
                    tokio::spawn(async move {
                        while let Some((addr, res)) = delete_futures.next().await {
                            match res {
                                Ok(()) => {
                                    debug!("Background delete to {} completed", addr);
                                }
                                Err(e) => {
                                    warn!("Background delete to {} failed: {}", addr, e);
                                }
                            }
                        }
                    });
                }

                histogram!("datavg_delete_blob_nanos", "result" => "success")
                    .record(start.elapsed().as_nanos() as f64);
                debug!(
                    "Delete quorum achieved ({}/{}) for blob {}:{}",
                    successful_deletes,
                    volume.bss_nodes.len(),
                    blob_guid.blob_id,
                    block_number
                );
                return Ok(());
            }
        }

        histogram!("datavg_delete_blob_nanos", "result" => "quorum_failure")
            .record(start.elapsed().as_nanos() as f64);
        error!(
            "Delete quorum failed ({}/{}). Errors: {:?}",
            successful_deletes, self.quorum_config.w, errors
        );
        Err(DataVgError::QuorumFailure(format!(
            "Delete quorum failed ({}/{}): {}",
            successful_deletes,
            self.quorum_config.w,
            errors.join("; ")
        )))
    }
}
