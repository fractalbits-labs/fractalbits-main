use crate::DataVgError;
use bytes::Bytes;
use data_types::{DataBlobGuid, DataVgInfo, QuorumConfig, TraceId};
use futures::stream::{FuturesUnordered, StreamExt};
use metrics::histogram;
use rand::seq::SliceRandom;
use rpc_client_bss::RpcClientBss;
use rpc_client_common::RpcError;
use std::{
    sync::{Arc, atomic::AtomicU64},
    time::{Duration, Instant},
};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

struct BssNode {
    address: String,
    client: RpcClientBss,
}

impl BssNode {
    fn new(address: String) -> Self {
        debug!("Creating BSS RPC client for {}", address);
        let client = RpcClientBss::new_from_address(address.clone());
        Self { address, client }
    }

    fn get_client(&self) -> &RpcClientBss {
        &self.client
    }
}

struct VolumeWithNodes {
    volume_id: u16,
    bss_nodes: Vec<Arc<BssNode>>,
}

pub struct DataVgProxy {
    volumes: Vec<VolumeWithNodes>,
    round_robin_counter: AtomicU64,
    quorum_config: QuorumConfig,
    rpc_timeout: Duration,
}

impl DataVgProxy {
    pub fn new(data_vg_info: DataVgInfo, rpc_timeout: Duration) -> Result<Self, DataVgError> {
        info!(
            "Initializing DataVgProxy with {} volumes",
            data_vg_info.volumes.len()
        );

        let quorum_config = data_vg_info.quorum.ok_or_else(|| {
            DataVgError::InitializationError(
                "QuorumConfig is required but not provided in DataVgInfo".to_string(),
            )
        })?;

        let mut volumes_with_nodes = Vec::new();

        for volume in data_vg_info.volumes {
            let mut bss_nodes = Vec::new();

            for bss_node in volume.bss_nodes {
                let address = format!("{}:{}", bss_node.ip, bss_node.port);
                debug!(
                    "Creating BSS node for node {}: {}",
                    bss_node.node_id, address
                );

                bss_nodes.push(Arc::new(BssNode::new(address)));
            }

            volumes_with_nodes.push(VolumeWithNodes {
                volume_id: volume.volume_id,
                bss_nodes,
            });
        }

        debug!(
            "DataVgProxy initialized successfully with {} volumes",
            volumes_with_nodes.len()
        );

        Ok(Self {
            volumes: volumes_with_nodes,
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
        bss_node: &BssNode,
        blob_guid: DataBlobGuid,
        block_number: u32,
        content_len: usize,
        trace_id: &TraceId,
    ) -> Result<Bytes, RpcError> {
        tracing::debug!(%blob_guid, bss_address=%bss_node.address, block_number, content_len, "get_blob_from_node_instance calling BSS");

        let bss_client = bss_node.get_client();

        let mut body = Bytes::new();
        let mut retries = 3;
        let mut backoff = Duration::from_millis(5);
        let mut retry_count = 0u32;

        loop {
            match bss_client
                .get_data_blob(
                    blob_guid,
                    block_number,
                    &mut body,
                    content_len,
                    Some(self.rpc_timeout),
                    trace_id,
                    retry_count,
                )
                .await
            {
                Ok(()) => break,
                Err(e) if e.retryable() && retries > 0 => {
                    retries -= 1;
                    retry_count += 1;
                    tokio::time::sleep(backoff).await;
                    backoff = backoff.saturating_mul(2);
                }
                Err(e) => return Err(e),
            }
        }

        tracing::debug!(%blob_guid, bss_address=%bss_node.address, block_number, data_size=body.len(), "get_blob_from_node_instance result");

        Ok(body)
    }

    async fn delete_blob_from_node(
        bss_node: Arc<BssNode>,
        blob_guid: DataBlobGuid,
        block_number: u32,
        rpc_timeout: Duration,
        trace_id: TraceId,
    ) -> (String, Result<(), RpcError>) {
        let start_node = Instant::now();
        let address = bss_node.address.clone();

        let result = async {
            let bss_client = bss_node.get_client();

            let mut retries = 3;
            let mut backoff = Duration::from_millis(5);
            let mut retry_count = 0u32;

            loop {
                match bss_client
                    .delete_data_blob(
                        blob_guid,
                        block_number,
                        Some(rpc_timeout),
                        &trace_id,
                        retry_count,
                    )
                    .await
                {
                    Ok(()) => return Ok(()),
                    Err(e) if e.retryable() && retries > 0 => {
                        retries -= 1;
                        retry_count += 1;
                        tokio::time::sleep(backoff).await;
                        backoff = backoff.saturating_mul(2);
                    }
                    Err(e) => return Err(e),
                }
            }
        }
        .await;

        let result_label = if result.is_ok() { "success" } else { "failure" };
        histogram!("datavg_delete_blob_node_nanos", "bss_node" => address.clone(), "result" => result_label)
            .record(start_node.elapsed().as_nanos() as f64);

        (address, result)
    }

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
        trace_id: &TraceId,
    ) -> Result<(), DataVgError> {
        let start = Instant::now();
        let trace_id = *trace_id;
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

        // Compute checksum once for all replicas
        let body_checksum = xxhash_rust::xxh3::xxh3_64(&body);

        let mut bss_node_indices: Vec<usize> = (0..selected_volume.bss_nodes.len()).collect();
        bss_node_indices.shuffle(&mut rand::thread_rng());

        let mut write_futures = FuturesUnordered::new();
        for &index in &bss_node_indices {
            let bss_node = selected_volume.bss_nodes[index].clone();
            write_futures.push(Self::put_blob_to_node(
                bss_node,
                blob_guid,
                block_number,
                body.clone(),
                body_checksum,
                rpc_timeout,
                trace_id,
            ));
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
                // Spawn remaining writes as background task for eventual consistency
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

    pub async fn put_blob_vectored(
        &self,
        blob_guid: DataBlobGuid,
        block_number: u32,
        chunks: Vec<Bytes>,
        trace_id: &TraceId,
    ) -> Result<(), DataVgError> {
        let start = Instant::now();
        let trace_id = *trace_id;
        let total_size: usize = chunks.iter().map(|c| c.len()).sum();
        histogram!("blob_size", "operation" => "put").record(total_size as f64);

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
        debug!(
            "Using volume {} for put_blob_vectored",
            selected_volume.volume_id
        );

        let rpc_timeout = self.rpc_timeout;
        let write_quorum = self.quorum_config.w as usize;

        // Compute checksum once for all replicas
        let mut hasher = xxhash_rust::xxh3::Xxh3::new();
        for chunk in &chunks {
            hasher.update(chunk);
        }
        let body_checksum = hasher.digest();

        let mut bss_node_indices: Vec<usize> = (0..selected_volume.bss_nodes.len()).collect();
        bss_node_indices.shuffle(&mut rand::thread_rng());

        let mut write_futures = FuturesUnordered::new();
        for &index in &bss_node_indices {
            let bss_node = selected_volume.bss_nodes[index].clone();
            write_futures.push(Self::put_blob_to_node_vectored(
                bss_node,
                blob_guid,
                block_number,
                chunks.clone(),
                body_checksum,
                rpc_timeout,
                trace_id,
            ));
        }

        let mut successful_writes = 0;
        let mut errors = Vec::with_capacity(selected_volume.bss_nodes.len());

        while let Some((address, result)) = write_futures.next().await {
            match result {
                Ok(()) => {
                    successful_writes += 1;
                    debug!("Successful vectored write to BSS node: {}", address);
                }
                Err(rpc_error) => {
                    warn!("RPC error writing to BSS node {}: {}", address, rpc_error);
                    errors.push(format!("{}: {}", address, rpc_error));
                }
            }

            if successful_writes >= write_quorum {
                tokio::spawn(async move {
                    while let Some((addr, res)) = write_futures.next().await {
                        match res {
                            Ok(()) => {
                                debug!("Background vectored write to {} completed", addr);
                            }
                            Err(e) => {
                                warn!("Background vectored write to {} failed: {}", addr, e);
                            }
                        }
                    }
                });

                histogram!("datavg_put_blob_nanos", "result" => "success")
                    .record(start.elapsed().as_nanos() as f64);
                debug!(
                    "Vectored write quorum achieved ({}/{}) for blob {}:{}",
                    successful_writes,
                    selected_volume.bss_nodes.len(),
                    blob_guid.blob_id,
                    block_number
                );
                return Ok(());
            }
        }

        histogram!("datavg_put_blob_nanos", "result" => "quorum_failure")
            .record(start.elapsed().as_nanos() as f64);
        error!(
            "Failed to achieve write quorum ({}/{}) for blob {}:{}: {}",
            successful_writes,
            self.quorum_config.w,
            blob_guid.blob_id,
            block_number,
            errors.join("; ")
        );
        Err(DataVgError::QuorumFailure(format!(
            "Failed to achieve write quorum ({}/{}): {}",
            successful_writes,
            self.quorum_config.w,
            errors.join("; ")
        )))
    }

    async fn put_blob_to_node(
        bss_node: Arc<BssNode>,
        blob_guid: DataBlobGuid,
        block_number: u32,
        body: Bytes,
        body_checksum: u64,
        rpc_timeout: Duration,
        trace_id: TraceId,
    ) -> (String, Result<(), RpcError>) {
        let start_node = Instant::now();
        let address = bss_node.address.clone();

        let bss_client = bss_node.get_client();
        let result = bss_client
            .put_data_blob(
                blob_guid,
                block_number,
                body,
                body_checksum,
                Some(rpc_timeout),
                &trace_id,
                0,
            )
            .await;

        let result_label = if result.is_ok() { "success" } else { "failure" };
        histogram!("datavg_put_blob_node_nanos", "bss_node" => address.clone(), "result" => result_label)
            .record(start_node.elapsed().as_nanos() as f64);

        (address, result)
    }

    async fn put_blob_to_node_vectored(
        bss_node: Arc<BssNode>,
        blob_guid: DataBlobGuid,
        block_number: u32,
        chunks: Vec<Bytes>,
        body_checksum: u64,
        rpc_timeout: Duration,
        trace_id: TraceId,
    ) -> (String, Result<(), RpcError>) {
        let start_node = Instant::now();
        let address = bss_node.address.clone();

        let bss_client = bss_node.get_client();
        let result = bss_client
            .put_data_blob_vectored(
                blob_guid,
                block_number,
                chunks,
                body_checksum,
                Some(rpc_timeout),
                &trace_id,
                0,
            )
            .await;

        let result_label = if result.is_ok() { "success" } else { "failure" };
        histogram!("datavg_put_blob_node_nanos", "bss_node" => address.clone(), "result" => result_label)
            .record(start_node.elapsed().as_nanos() as f64);

        (address, result)
    }

    /// Multi-BSS get_blob with quorum-based reads
    pub async fn get_blob(
        &self,
        blob_guid: DataBlobGuid,
        block_number: u32,
        content_len: usize,
        body: &mut Bytes,
        trace_id: &TraceId,
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

        // Fast path: try reading from a randomly selected node
        if !volume.bss_nodes.is_empty() {
            let selected_node = volume.bss_nodes.choose(&mut rand::thread_rng()).unwrap();
            debug!(
                "Attempting fast path read from BSS node: {}",
                selected_node.address
            );
            match self
                .get_blob_from_node_instance(
                    selected_node,
                    blob_guid,
                    block_number,
                    content_len,
                    trace_id,
                )
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

        let _read_quorum = self.quorum_config.r as usize;

        // Create read futures for all nodes
        let mut read_futures = FuturesUnordered::new();
        for bss_node in &volume.bss_nodes {
            let fut = Self::get_blob_from_node_instance(
                self,
                bss_node,
                blob_guid,
                block_number,
                content_len,
                trace_id,
            );
            let address = bss_node.address.clone();
            read_futures.push(async move {
                let result = fut.await;
                (address, result)
            });
        }

        let mut successful_reads = 0;
        let mut successful_blob_data = None;

        // Wait until we get a successful read (quorum of 1) or all fail
        // TODO: quorum writeback
        while let Some((address, result)) = read_futures.next().await {
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
        trace_id: &TraceId,
    ) -> Result<(), DataVgError> {
        let start = Instant::now();
        let trace_id = *trace_id;

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
            delete_futures.push(Self::delete_blob_from_node(
                bss_node.clone(),
                blob_guid,
                block_number,
                rpc_timeout,
                trace_id,
            ));
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
                // Spawn remaining deletes as background task for eventual consistency
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
