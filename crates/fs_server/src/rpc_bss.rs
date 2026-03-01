#![allow(clippy::field_reassign_with_default)]

use crate::rpc::{AutoReconnectRpcClient, RpcCodec, RpcError};
use bss_codec::{Command, MessageHeader};
use bytes::Bytes;
use data_types::{DataBlobGuid, DataVgInfo, TraceId, VolumeMode};
use reed_solomon_simd::{decode as rs_decode, encode as rs_encode};
use rpc_codec_common::MessageFrame;
use std::cell::Cell;
use std::rc::Rc;
use std::time::Duration;
use tracing::{debug, error, warn};
use uuid::Uuid;

#[derive(Default, Clone)]
pub struct BssCodec;

impl RpcCodec<MessageHeader> for BssCodec {
    const RPC_TYPE: &'static str = "bss";
}

pub type BssRpcClient = AutoReconnectRpcClient<BssCodec, MessageHeader>;

fn check_response_errno(header: &MessageHeader) -> Result<(), RpcError> {
    match header.errno {
        0 => Ok(()),
        1 => Err(RpcError::InternalResponseError(
            "BSS returned InternalError".to_string(),
        )),
        2 => Err(RpcError::NotFound),
        3 => Err(RpcError::ChecksumMismatch),
        4 => Err(RpcError::Retry),
        code => Err(RpcError::InternalResponseError(format!(
            "Unknown BSS error code: {code}"
        ))),
    }
}

struct VolumeNodes {
    volume_id: u16,
    clients: Vec<Rc<BssRpcClient>>,
    mode: VolumeMode,
}

/// Compio-native DataVgProxy for per-core FUSE use.
/// Each compio thread creates its own DataVgProxy with independent TCP connections.
/// Supports both replicated and erasure-coded (EC) volumes.
pub struct DataVgProxy {
    volumes: Vec<VolumeNodes>,
    round_robin: Cell<u64>,
    #[allow(dead_code)]
    rpc_timeout: Duration,
}

impl DataVgProxy {
    pub fn new(data_vg_info: DataVgInfo, rpc_timeout: Duration) -> Result<Self, String> {
        if data_vg_info.volumes.is_empty() {
            return Err("No volumes configured".to_string());
        }

        let mut volumes = Vec::new();
        for volume in data_vg_info.volumes {
            let clients: Vec<Rc<BssRpcClient>> = volume
                .bss_nodes
                .iter()
                .map(|node| {
                    let addr = format!("{}:{}", node.ip, node.port);
                    debug!("Creating BSS RPC client for {}", addr);
                    Rc::new(BssRpcClient::new_from_address(addr))
                })
                .collect();

            volumes.push(VolumeNodes {
                volume_id: volume.volume_id,
                clients,
                mode: volume.mode,
            });
        }

        Ok(Self {
            volumes,
            round_robin: Cell::new(0),
            rpc_timeout,
        })
    }

    fn find_volume(&self, volume_id: u16) -> Option<&VolumeNodes> {
        self.volumes.iter().find(|v| v.volume_id == volume_id)
    }

    fn select_volume(&self) -> u16 {
        let idx = self.round_robin.get() as usize % self.volumes.len();
        self.round_robin.set(self.round_robin.get().wrapping_add(1));
        self.volumes[idx].volume_id
    }

    pub fn create_data_blob_guid(&self) -> DataBlobGuid {
        let blob_id = Uuid::now_v7();
        let volume_id = self.select_volume();
        DataBlobGuid { blob_id, volume_id }
    }

    /// Compute shard-to-node rotation from blob_id (deterministic, matches tokio impl).
    fn ec_rotation(blob_id: &Uuid, total_shards: u32) -> usize {
        let hash = crc32fast::hash(blob_id.as_bytes());
        (hash % total_shards) as usize
    }

    /// Compute padded length for RS stripe alignment (matches tokio impl).
    fn ec_padded_len(content_len: usize, data_shards: usize) -> usize {
        let stripe_size = data_shards * 2;
        if content_len.is_multiple_of(stripe_size) {
            content_len
        } else {
            content_len + (stripe_size - content_len % stripe_size)
        }
    }

    // ── GET ─────────────────────────────────────────────────────────────

    pub async fn get_blob(
        &self,
        blob_guid: DataBlobGuid,
        block_number: u32,
        content_len: usize,
        body: &mut Bytes,
        trace_id: &TraceId,
    ) -> Result<(), RpcError> {
        let volume = self.find_volume(blob_guid.volume_id).ok_or_else(|| {
            RpcError::InternalRequestError(format!("Volume {} not found", blob_guid.volume_id))
        })?;

        if volume.clients.is_empty() {
            return Err(RpcError::InternalRequestError("No BSS nodes".to_string()));
        }

        match &volume.mode {
            VolumeMode::Replicated { .. } => {
                self.get_blob_replicated(
                    volume,
                    blob_guid,
                    block_number,
                    content_len,
                    body,
                    trace_id,
                )
                .await
            }
            VolumeMode::ErasureCoded {
                data_shards,
                parity_shards,
            } => {
                self.get_blob_ec(
                    volume,
                    blob_guid,
                    block_number,
                    content_len,
                    body,
                    trace_id,
                    *data_shards as usize,
                    *parity_shards as usize,
                )
                .await
            }
        }
    }

    async fn get_blob_replicated(
        &self,
        volume: &VolumeNodes,
        blob_guid: DataBlobGuid,
        block_number: u32,
        content_len: usize,
        body: &mut Bytes,
        trace_id: &TraceId,
    ) -> Result<(), RpcError> {
        // Try each node until one succeeds
        let start_idx = self.round_robin.get() as usize % volume.clients.len();
        self.round_robin.set(self.round_robin.get().wrapping_add(1));
        let mut last_err = None;

        for i in 0..volume.clients.len() {
            let idx = (start_idx + i) % volume.clients.len();
            let client = &volume.clients[idx];

            let mut header = MessageHeader::default();
            header.id = client.gen_request_id();
            header.blob_id = blob_guid.blob_id.into_bytes();
            header.volume_id = blob_guid.volume_id;
            header.block_number = block_number;
            header.command = Command::GetDataBlob;
            header.content_len = content_len as u32;
            header.size = size_of::<MessageHeader>() as u32;
            header.trace_id = trace_id.0;

            let frame = MessageFrame::new(header, Bytes::new());
            match client.send_request(frame).await {
                Ok(resp_frame) => {
                    if let Err(e) = check_response_errno(&resp_frame.header) {
                        last_err = Some(e);
                        continue;
                    }
                    *body = resp_frame.body;
                    return Ok(());
                }
                Err(e) => {
                    warn!("BSS get_blob failed on node {}: {}", idx, e);
                    last_err = Some(e);
                }
            }
        }

        Err(last_err.unwrap_or(RpcError::InternalRequestError(
            "All BSS nodes failed".to_string(),
        )))
    }

    #[allow(clippy::too_many_arguments)]
    async fn get_blob_ec(
        &self,
        volume: &VolumeNodes,
        blob_guid: DataBlobGuid,
        block_number: u32,
        content_len: usize,
        body: &mut Bytes,
        trace_id: &TraceId,
        k: usize,
        m: usize,
    ) -> Result<(), RpcError> {
        if content_len == 0 {
            *body = Bytes::new();
            return Ok(());
        }

        let total = k + m;
        let rotation = Self::ec_rotation(&blob_guid.blob_id, total as u32);
        let padded_len = Self::ec_padded_len(content_len, k);
        let shard_size = padded_len / k;

        // Phase 1: Fetch k data shards in parallel
        let mut data_handles = Vec::with_capacity(k);
        for shard_idx in 0..k {
            let node_idx = (shard_idx + rotation) % total;
            let client = volume.clients[node_idx].clone();
            let trace_id_val = trace_id.0;

            let handle = compio_runtime::spawn(async move {
                let mut header = MessageHeader::default();
                header.id = client.gen_request_id();
                header.blob_id = blob_guid.blob_id.into_bytes();
                header.volume_id = blob_guid.volume_id;
                header.block_number = block_number;
                header.command = Command::GetDataBlob;
                header.content_len = shard_size as u32;
                header.size = size_of::<MessageHeader>() as u32;
                header.trace_id = trace_id_val;

                let frame = MessageFrame::new(header, Bytes::new());
                match client.send_request(frame).await {
                    Ok(resp_frame) => {
                        check_response_errno(&resp_frame.header)?;
                        Ok(resp_frame.body)
                    }
                    Err(e) => Err(e),
                }
            });
            data_handles.push((shard_idx, node_idx, handle));
        }

        let mut shard_results: Vec<Option<Vec<u8>>> = vec![None; total];
        let mut data_received = 0;

        for (shard_idx, node_idx, handle) in data_handles {
            match handle.await {
                Ok(Ok(shard_body)) => {
                    shard_results[shard_idx] = Some(Vec::from(shard_body));
                    data_received += 1;
                }
                Ok(Err(e)) => {
                    warn!(
                        "EC data shard {} fetch failed from node {}: {}",
                        shard_idx, node_idx, e
                    );
                }
                Err(e) => {
                    warn!(
                        "EC data shard {} task panicked from node {}: {:?}",
                        shard_idx, node_idx, e
                    );
                }
            }
        }

        // Fast path: all k data shards received
        if data_received == k {
            let mut result_data = Vec::with_capacity(content_len);
            for shard in shard_results.iter().take(k) {
                result_data.extend_from_slice(shard.as_ref().unwrap());
            }
            result_data.truncate(content_len);
            *body = Bytes::from(result_data);
            return Ok(());
        }

        // Degraded path: need parity shards for RS reconstruction
        let missing_count = k - data_received;
        if missing_count > m {
            return Err(RpcError::InternalRequestError(format!(
                "EC read: {} data shards failed, exceeds parity count {}",
                missing_count, m
            )));
        }

        debug!(
            "EC degraded read: {}/{} data shards received, fetching parity",
            data_received, k
        );

        // Phase 2: Fetch all parity shards
        let mut parity_handles = Vec::with_capacity(m);
        for parity_idx in 0..m {
            let shard_idx = k + parity_idx;
            let node_idx = (shard_idx + rotation) % total;
            let client = volume.clients[node_idx].clone();
            let trace_id_val = trace_id.0;

            let handle = compio_runtime::spawn(async move {
                let mut header = MessageHeader::default();
                header.id = client.gen_request_id();
                header.blob_id = blob_guid.blob_id.into_bytes();
                header.volume_id = blob_guid.volume_id;
                header.block_number = block_number;
                header.command = Command::GetDataBlob;
                header.content_len = shard_size as u32;
                header.size = size_of::<MessageHeader>() as u32;
                header.trace_id = trace_id_val;

                let frame = MessageFrame::new(header, Bytes::new());
                match client.send_request(frame).await {
                    Ok(resp_frame) => {
                        check_response_errno(&resp_frame.header)?;
                        Ok(resp_frame.body)
                    }
                    Err(e) => Err(e),
                }
            });
            parity_handles.push((shard_idx, node_idx, handle));
        }

        for (shard_idx, node_idx, handle) in parity_handles {
            match handle.await {
                Ok(Ok(shard_body)) => {
                    shard_results[shard_idx] = Some(Vec::from(shard_body));
                }
                Ok(Err(e)) => {
                    warn!(
                        "EC parity shard {} fetch failed from node {}: {}",
                        shard_idx, node_idx, e
                    );
                }
                Err(e) => {
                    warn!(
                        "EC parity shard {} task panicked from node {}: {:?}",
                        shard_idx, node_idx, e
                    );
                }
            }
        }

        let total_received = shard_results.iter().filter(|s| s.is_some()).count();
        if total_received < k {
            return Err(RpcError::InternalRequestError(format!(
                "EC read: only {} shards available, need {}",
                total_received, k
            )));
        }

        // RS reconstruction
        let shards_for_rs: Vec<Option<Vec<u8>>> = shard_results
            .into_iter()
            .map(|s| s.filter(|d| d.len() == shard_size))
            .collect();

        let original_shards: Vec<_> = shards_for_rs
            .iter()
            .take(k)
            .enumerate()
            .filter_map(|(index, shard)| shard.as_deref().map(|data| (index, data)))
            .collect();
        let recovery_shards: Vec<_> = shards_for_rs
            .iter()
            .skip(k)
            .enumerate()
            .filter_map(|(index, shard)| shard.as_deref().map(|data| (index, data)))
            .collect();

        let restored_original = rs_decode(k, m, original_shards, recovery_shards).map_err(|e| {
            RpcError::InternalResponseError(format!("RS reconstruct failed: {}", e))
        })?;

        let mut result_data = Vec::with_capacity(k * shard_size);
        for (index, shard) in shards_for_rs.iter().take(k).enumerate() {
            if let Some(shard) = shard {
                result_data.extend_from_slice(shard);
            } else if let Some(restored) = restored_original.get(&index) {
                result_data.extend_from_slice(restored);
            } else {
                return Err(RpcError::InternalResponseError(format!(
                    "RS reconstruct missing shard {}",
                    index
                )));
            }
        }
        result_data.truncate(content_len);
        *body = Bytes::from(result_data);

        debug!(
            "EC degraded read succeeded for blob {}:{}",
            blob_guid.blob_id, block_number
        );
        Ok(())
    }

    // ── PUT ─────────────────────────────────────────────────────────────

    pub async fn put_blob(
        &self,
        blob_guid: DataBlobGuid,
        block_number: u32,
        body: Bytes,
        trace_id: &TraceId,
    ) -> Result<(), RpcError> {
        let volume = self.find_volume(blob_guid.volume_id).ok_or_else(|| {
            RpcError::InternalRequestError(format!("Volume {} not found", blob_guid.volume_id))
        })?;

        match &volume.mode {
            VolumeMode::Replicated { w, .. } => {
                self.put_blob_replicated(
                    volume,
                    blob_guid,
                    block_number,
                    body,
                    trace_id,
                    *w as usize,
                )
                .await
            }
            VolumeMode::ErasureCoded {
                data_shards,
                parity_shards,
            } => {
                self.put_blob_ec(
                    volume,
                    blob_guid,
                    block_number,
                    body,
                    trace_id,
                    *data_shards as usize,
                    *parity_shards as usize,
                )
                .await
            }
        }
    }

    async fn put_blob_replicated(
        &self,
        volume: &VolumeNodes,
        blob_guid: DataBlobGuid,
        block_number: u32,
        body: Bytes,
        trace_id: &TraceId,
        write_quorum: usize,
    ) -> Result<(), RpcError> {
        let body_checksum = xxhash_rust::xxh3::xxh3_64(&body);
        let mut successes = 0;
        let mut last_err = None;

        // Write to all nodes, track quorum
        for client in &volume.clients {
            let mut header = MessageHeader::default();
            header.id = client.gen_request_id();
            header.blob_id = blob_guid.blob_id.into_bytes();
            header.volume_id = blob_guid.volume_id;
            header.block_number = block_number;
            header.command = Command::PutDataBlob;
            header.content_len = body.len() as u32;
            header.size = size_of::<MessageHeader>() as u32 + header.content_len;
            header.trace_id = trace_id.0;
            header.checksum_body = body_checksum;

            let frame = MessageFrame::new(header, body.clone());
            match client.send_request(frame).await {
                Ok(resp_frame) => {
                    if let Err(e) = check_response_errno(&resp_frame.header) {
                        last_err = Some(e);
                        continue;
                    }
                    successes += 1;
                    if successes >= write_quorum {
                        return Ok(());
                    }
                }
                Err(e) => {
                    warn!("BSS put_blob failed on a node: {}", e);
                    last_err = Some(e);
                }
            }
        }

        if successes >= write_quorum {
            return Ok(());
        }

        Err(last_err.unwrap_or(RpcError::InternalRequestError(format!(
            "Write quorum failed ({successes}/{write_quorum})"
        ))))
    }

    #[allow(clippy::too_many_arguments)]
    async fn put_blob_ec(
        &self,
        volume: &VolumeNodes,
        blob_guid: DataBlobGuid,
        block_number: u32,
        body: Bytes,
        trace_id: &TraceId,
        k: usize,
        m: usize,
    ) -> Result<(), RpcError> {
        if body.is_empty() {
            return Ok(());
        }

        let total = k + m;
        let write_quorum = k + 1;

        // Pad body and RS-encode into k+m shards
        let original_len = body.len();
        let padded_len = Self::ec_padded_len(original_len, k);
        let shard_size = padded_len / k;

        let mut padded = body.to_vec();
        padded.resize(padded_len, 0u8);

        let mut data_shards: Vec<Vec<u8>> = Vec::with_capacity(k);
        for i in 0..k {
            data_shards.push(padded[i * shard_size..(i + 1) * shard_size].to_vec());
        }
        let parity_shards = rs_encode(k, m, &data_shards)
            .map_err(|e| RpcError::InternalRequestError(format!("RS encode failed: {}", e)))?;
        let mut all_shards = data_shards;
        all_shards.extend(parity_shards);

        // Compute shard-to-node rotation
        let rotation = Self::ec_rotation(&blob_guid.blob_id, total as u32);

        // Send each shard to its rotated node in parallel
        let mut handles = Vec::with_capacity(total);
        for (shard_idx, shard) in all_shards.into_iter().enumerate() {
            let node_idx = (shard_idx + rotation) % total;
            let client = volume.clients[node_idx].clone();
            let shard_data = Bytes::from(shard);
            let checksum = xxhash_rust::xxh3::xxh3_64(&shard_data);
            let trace_id_val = trace_id.0;

            let handle = compio_runtime::spawn(async move {
                let mut header = MessageHeader::default();
                header.id = client.gen_request_id();
                header.blob_id = blob_guid.blob_id.into_bytes();
                header.volume_id = blob_guid.volume_id;
                header.block_number = block_number;
                header.command = Command::PutDataBlob;
                header.content_len = shard_data.len() as u32;
                header.size = size_of::<MessageHeader>() as u32 + shard_data.len() as u32;
                header.trace_id = trace_id_val;
                header.checksum_body = checksum;

                let frame = MessageFrame::new(header, shard_data);
                match client.send_request(frame).await {
                    Ok(resp_frame) => {
                        check_response_errno(&resp_frame.header)?;
                        Ok(())
                    }
                    Err(e) => Err(e),
                }
            });
            handles.push((node_idx, handle));
        }

        let mut successes = 0;
        let mut last_err = None;

        for (node_idx, handle) in handles {
            match handle.await {
                Ok(Ok(())) => {
                    successes += 1;
                }
                Ok(Err(e)) => {
                    warn!("EC shard write failed to node {}: {}", node_idx, e);
                    last_err = Some(e);
                }
                Err(e) => {
                    error!(
                        "EC shard write task panicked for node {}: {:?}",
                        node_idx, e
                    );
                    last_err = Some(RpcError::InternalRequestError(format!(
                        "task panicked: {:?}",
                        e
                    )));
                }
            }
        }

        if successes >= write_quorum {
            debug!(
                "EC write quorum achieved ({}/{}) for blob {}:{}",
                successes, total, blob_guid.blob_id, block_number
            );
            return Ok(());
        }

        Err(last_err.unwrap_or(RpcError::InternalRequestError(format!(
            "EC write quorum failed ({successes}/{write_quorum})"
        ))))
    }

    // ── DELETE ───────────────────────────────────────────────────────────

    pub async fn delete_blob(
        &self,
        blob_guid: DataBlobGuid,
        block_number: u32,
        trace_id: &TraceId,
    ) -> Result<(), RpcError> {
        let volume = self.find_volume(blob_guid.volume_id).ok_or_else(|| {
            RpcError::InternalRequestError(format!("Volume {} not found", blob_guid.volume_id))
        })?;

        match &volume.mode {
            VolumeMode::Replicated { .. } => {
                self.delete_blob_replicated(volume, blob_guid, block_number, trace_id)
                    .await
            }
            VolumeMode::ErasureCoded { .. } => {
                self.delete_blob_ec(volume, blob_guid, block_number, trace_id)
                    .await
            }
        }
    }

    async fn delete_blob_replicated(
        &self,
        volume: &VolumeNodes,
        blob_guid: DataBlobGuid,
        block_number: u32,
        trace_id: &TraceId,
    ) -> Result<(), RpcError> {
        let mut last_err = None;

        for client in &volume.clients {
            let mut header = MessageHeader::default();
            header.id = client.gen_request_id();
            header.blob_id = blob_guid.blob_id.into_bytes();
            header.volume_id = blob_guid.volume_id;
            header.block_number = block_number;
            header.command = Command::DeleteDataBlob;
            header.size = size_of::<MessageHeader>() as u32;
            header.trace_id = trace_id.0;

            let frame = MessageFrame::new(header, Bytes::new());
            match client.send_request(frame).await {
                Ok(resp_frame) => {
                    if let Err(e) = check_response_errno(&resp_frame.header)
                        && !matches!(e, RpcError::NotFound)
                    {
                        last_err = Some(e);
                    }
                }
                Err(e) => {
                    warn!("BSS delete_blob failed on a node: {}", e);
                    last_err = Some(e);
                }
            }
        }

        // Delete is best-effort; only fail if all nodes failed
        if let Some(e) = last_err {
            warn!("Some BSS delete_blob nodes failed: {}", e);
        }
        Ok(())
    }

    async fn delete_blob_ec(
        &self,
        volume: &VolumeNodes,
        blob_guid: DataBlobGuid,
        block_number: u32,
        trace_id: &TraceId,
    ) -> Result<(), RpcError> {
        // Send delete to all nodes in parallel (best-effort)
        let mut handles = Vec::with_capacity(volume.clients.len());
        for (node_idx, client) in volume.clients.iter().enumerate() {
            let client = client.clone();
            let trace_id_val = trace_id.0;

            let handle = compio_runtime::spawn(async move {
                let mut header = MessageHeader::default();
                header.id = client.gen_request_id();
                header.blob_id = blob_guid.blob_id.into_bytes();
                header.volume_id = blob_guid.volume_id;
                header.block_number = block_number;
                header.command = Command::DeleteDataBlob;
                header.size = size_of::<MessageHeader>() as u32;
                header.trace_id = trace_id_val;

                let frame = MessageFrame::new(header, Bytes::new());
                client.send_request(frame).await
            });
            handles.push((node_idx, handle));
        }

        let mut last_err = None;
        for (node_idx, handle) in handles {
            match handle.await {
                Ok(Ok(resp_frame)) => {
                    if let Err(e) = check_response_errno(&resp_frame.header)
                        && !matches!(e, RpcError::NotFound)
                    {
                        last_err = Some(e);
                    }
                }
                Ok(Err(e)) => {
                    warn!("BSS EC delete_blob failed on node {}: {}", node_idx, e);
                    last_err = Some(e);
                }
                Err(e) => {
                    error!("BSS EC delete task panicked for node {}: {:?}", node_idx, e);
                }
            }
        }

        if let Some(e) = last_err {
            warn!("Some BSS EC delete_blob nodes failed: {}", e);
        }
        Ok(())
    }
}
