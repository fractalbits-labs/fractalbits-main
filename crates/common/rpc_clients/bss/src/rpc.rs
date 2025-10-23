use std::time::Duration;

use crate::client::RpcClient;
use bss_codec::{Command, MessageHeader};
use bytes::Bytes;
use data_types::{DataBlobGuid, MetaBlobGuid};
use rpc_client_common::{InflightRpcGuard, RpcError};
use rpc_codec_common::MessageFrame;
use tracing::error;

impl RpcClient {
    pub async fn put_data_blob(
        &self,
        blob_guid: DataBlobGuid,
        block_number: u32,
        body: Bytes,
        timeout: Option<Duration>,
        trace_id: Option<u64>,
        retry_count: u32,
    ) -> Result<(), RpcError> {
        let _guard = InflightRpcGuard::new("bss", "put_data_blob");
        let mut header = MessageHeader::default();
        let request_id = self.gen_request_id();
        header.id = request_id;
        header.blob_id = blob_guid.blob_id.into_bytes();
        header.volume_id = blob_guid.volume_id;
        header.block_number = block_number;
        header.command = Command::PutDataBlob;
        header.size = (MessageHeader::SIZE + body.len()) as u32;
        header.retry_count = retry_count;

        let msg_frame = MessageFrame::new(header, body);
        self
            .send_request(request_id, msg_frame, timeout, trace_id)
            .await
            .map_err(|e| {
                if !e.retryable() {
                    error!(rpc=%"put_data_blob", %request_id, %blob_guid, %block_number, error=?e, "bss rpc failed");
                }
                e
            })?;
        Ok(())
    }

    pub async fn get_data_blob(
        &self,
        blob_guid: DataBlobGuid,
        block_number: u32,
        body: &mut Bytes,
        timeout: Option<Duration>,
        trace_id: Option<u64>,
        retry_count: u32,
    ) -> Result<(), RpcError> {
        let _guard = InflightRpcGuard::new("bss", "get_data_blob");
        let mut header = MessageHeader::default();
        let request_id = self.gen_request_id();
        header.id = request_id;
        header.blob_id = blob_guid.blob_id.into_bytes();
        header.volume_id = blob_guid.volume_id;
        header.block_number = block_number;
        header.command = Command::GetDataBlob;
        header.size = MessageHeader::SIZE as u32;
        header.retry_count = retry_count;

        let msg_frame = MessageFrame::new(header, Bytes::new());
        let resp_frame = self
            .send_request(header.id, msg_frame, timeout, trace_id)
            .await
            .map_err(|e| {
                if !e.retryable() {
                    error!(rpc=%"get_data_blob", %request_id, %blob_guid, %block_number, error=?e, "bss rpc failed");
                }
                e
            })?;
        *body = resp_frame.body;
        Ok(())
    }

    pub async fn delete_data_blob(
        &self,
        blob_guid: DataBlobGuid,
        block_number: u32,
        timeout: Option<Duration>,
        trace_id: Option<u64>,
        retry_count: u32,
    ) -> Result<(), RpcError> {
        let _guard = InflightRpcGuard::new("bss", "delete_data_blob");
        let mut header = MessageHeader::default();
        let request_id = self.gen_request_id();
        header.id = request_id;
        header.blob_id = blob_guid.blob_id.into_bytes();
        header.volume_id = blob_guid.volume_id;
        header.block_number = block_number;
        header.command = Command::DeleteDataBlob;
        header.size = MessageHeader::SIZE as u32;
        header.retry_count = retry_count;

        let msg_frame = MessageFrame::new(header, Bytes::new());
        self
            .send_request(header.id, msg_frame, timeout, trace_id)
            .await
            .map_err(|e| {
                if !e.retryable() {
                    error!(rpc=%"delete_data_blob", %request_id, %blob_guid, %block_number, error=?e, "bss rpc failed");
                }
                e
            })?;
        Ok(())
    }

    // Metadata blob operations
    #[allow(clippy::too_many_arguments)]
    pub async fn put_metadata_blob(
        &self,
        blob_guid: MetaBlobGuid,
        block_number: u32,
        version: u64,
        is_new: bool,
        body: Bytes,
        timeout: Option<Duration>,
        trace_id: Option<u64>,
        retry_count: u32,
    ) -> Result<(), RpcError> {
        let _guard = InflightRpcGuard::new("bss", "put_metadata_blob");
        let mut header = MessageHeader::default();
        let request_id = self.gen_request_id();
        header.id = request_id;
        header.blob_id = blob_guid.blob_id.into_bytes();
        header.volume_id = blob_guid.volume_id;
        header.block_number = block_number;
        header.version = version;
        header.is_new = if is_new { 1 } else { 0 };
        header.command = Command::PutMetadataBlob;
        header.size = (MessageHeader::SIZE + body.len()) as u32;
        header.retry_count = retry_count;

        let msg_frame = MessageFrame::new(header, body);
        self
            .send_request(request_id, msg_frame, timeout, trace_id)
            .await
            .map_err(|e| {
                if !e.retryable() {
                    error!(rpc=%"put_metadata_blob", %request_id, %blob_guid, %block_number, %version, is_new=%is_new, error=?e, "bss rpc failed");
                }
                e
            })?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn get_metadata_blob(
        &self,
        blob_guid: MetaBlobGuid,
        block_number: u32,
        version: u64,
        body: &mut Bytes,
        timeout: Option<Duration>,
        trace_id: Option<u64>,
        retry_count: u32,
    ) -> Result<u64, RpcError> {
        let _guard = InflightRpcGuard::new("bss", "get_metadata_blob");
        let mut header = MessageHeader::default();
        let request_id = self.gen_request_id();
        header.id = request_id;
        header.blob_id = blob_guid.blob_id.into_bytes();
        header.volume_id = blob_guid.volume_id;
        header.block_number = block_number;
        header.version = version;
        header.command = Command::GetMetadataBlob;
        header.size = MessageHeader::SIZE as u32;
        header.retry_count = retry_count;

        let msg_frame = MessageFrame::new(header, Bytes::new());
        let resp_frame = self
            .send_request(header.id, msg_frame, timeout, trace_id)
            .await
            .map_err(|e| {
                if !e.retryable() {
                    error!(rpc=%"get_metadata_blob", %request_id, %blob_guid, %block_number, %version, error=?e, "bss rpc failed");
                }
                e
            })?;
        *body = resp_frame.body;
        Ok(resp_frame.header.version)
    }

    pub async fn delete_metadata_blob(
        &self,
        blob_guid: MetaBlobGuid,
        block_number: u32,
        timeout: Option<Duration>,
        trace_id: Option<u64>,
        retry_count: u32,
    ) -> Result<(), RpcError> {
        let _guard = InflightRpcGuard::new("bss", "delete_metadata_blob");
        let mut header = MessageHeader::default();
        let request_id = self.gen_request_id();
        header.id = request_id;
        header.blob_id = blob_guid.blob_id.into_bytes();
        header.volume_id = blob_guid.volume_id;
        header.block_number = block_number;
        header.command = Command::DeleteMetadataBlob;
        header.size = MessageHeader::SIZE as u32;
        header.retry_count = retry_count;

        let msg_frame = MessageFrame::new(header, Bytes::new());
        self
            .send_request(header.id, msg_frame, timeout, trace_id)
            .await
            .map_err(|e| {
                if !e.retryable() {
                    error!(rpc=%"delete_metadata_blob", %request_id, %blob_guid, %block_number, error=?e, "bss rpc failed");
                }
                e
            })?;
        Ok(())
    }
}
