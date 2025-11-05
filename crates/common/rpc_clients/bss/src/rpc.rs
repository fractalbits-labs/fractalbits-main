use std::time::Duration;

use crate::client::RpcClient;
use bss_codec::{Command, MessageHeader};
use bytes::Bytes;
use data_types::{DataBlobGuid, MetaBlobGuid};
use rpc_client_common::{InflightRpcGuard, RpcError};
use rpc_codec_common::MessageFrame;
use tracing::error;

/// Check the errno field in the response header and return appropriate error
fn check_response_errno(header: &MessageHeader) -> Result<(), RpcError> {
    // errno codes from core/common/rpc/rpc_error.zig
    match header.errno {
        0 => Ok(()), // OK
        1 => Err(RpcError::InternalResponseError(
            "BSS returned InternalError".to_string(),
        )),
        2 => Err(RpcError::NotFound),
        3 => Err(RpcError::ChecksumMismatch), // Corrupted
        4 => Err(RpcError::Retry),            // SlowDown
        code => Err(RpcError::InternalResponseError(format!(
            "Unknown BSS error code: {}",
            code
        ))),
    }
}

impl RpcClient {
    #[allow(clippy::too_many_arguments)]
    pub async fn put_data_blob(
        &self,
        blob_guid: DataBlobGuid,
        block_number: u32,
        body: Bytes,
        body_checksum: u64,
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
        header.content_len = body.len() as u32;
        header.size = (MessageHeader::SIZE as u32) + header.content_len;
        header.retry_count = retry_count as u8;
        header.checksum_body = body_checksum;

        let msg_frame = MessageFrame::new(header, body);
        let resp_frame = self
            .send_request(request_id, msg_frame, timeout, trace_id, Some(crate::OperationType::PutData))
            .await
            .map_err(|e| {
                if !e.retryable() {
                    error!(rpc=%"put_data_blob", %request_id, %blob_guid, %block_number, error=?e, "bss rpc failed");
                }
                e
            })?;
        check_response_errno(&resp_frame.header)?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn put_data_blob_vectored(
        &self,
        blob_guid: DataBlobGuid,
        block_number: u32,
        chunks: Vec<Bytes>,
        body_checksum: u64,
        timeout: Option<Duration>,
        trace_id: Option<u64>,
        retry_count: u32,
    ) -> Result<(), RpcError> {
        let _guard = InflightRpcGuard::new("bss", "put_data_blob_vectored");
        let mut header = MessageHeader::default();
        let request_id = self.gen_request_id();
        header.id = request_id;
        header.blob_id = blob_guid.blob_id.into_bytes();
        header.volume_id = blob_guid.volume_id;
        header.block_number = block_number;
        header.command = Command::PutDataBlob;
        let total_size: usize = chunks.iter().map(|c| c.len()).sum();
        header.content_len = total_size as u32;
        header.size = (MessageHeader::SIZE as u32) + header.content_len;
        header.retry_count = retry_count as u8;
        header.checksum_body = body_checksum;

        let msg_frame = MessageFrame::new(header, chunks);
        let resp_frame = self
            .send_request_vectored(request_id, msg_frame, timeout, trace_id, Some(crate::OperationType::PutData))
            .await
            .map_err(|e| {
                if !e.retryable() {
                    error!(rpc=%"put_data_blob_vectored", %request_id, %blob_guid, %block_number, error=?e, "bss rpc failed");
                }
                e
            })?;
        check_response_errno(&resp_frame.header)?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn get_data_blob(
        &self,
        blob_guid: DataBlobGuid,
        block_number: u32,
        body: &mut Bytes,
        content_len: usize,
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
        header.retry_count = retry_count as u8;
        header.content_len = content_len as u32;
        header.size = MessageHeader::SIZE as u32;

        let msg_frame = MessageFrame::new(header, Bytes::new());
        let resp_frame = self
            .send_request(header.id, msg_frame, timeout, trace_id, Some(crate::OperationType::GetData))
            .await
            .map_err(|e| {
                if !e.retryable() {
                    error!(rpc=%"get_data_blob", %request_id, %blob_guid, %block_number, error=?e, "bss rpc failed");
                }
                e
            })?;
        check_response_errno(&resp_frame.header)?;
        *body = resp_frame.body;
        assert_eq!(content_len, body.len());
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
        header.retry_count = retry_count as u8;

        let msg_frame = MessageFrame::new(header, Bytes::new());
        let resp_frame = self
            .send_request(header.id, msg_frame, timeout, trace_id, Some(crate::OperationType::DeleteData))
            .await
            .map_err(|e| {
                if !e.retryable() {
                    error!(rpc=%"delete_data_blob", %request_id, %blob_guid, %block_number, error=?e, "bss rpc failed");
                }
                e
            })?;
        check_response_errno(&resp_frame.header)?;
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
        header.content_len = body.len() as u32;
        header.size = (MessageHeader::SIZE as u32) + header.content_len;
        header.retry_count = retry_count as u8;
        header.set_body_checksum(&body);

        let msg_frame = MessageFrame::new(header, body);
        let resp_frame = self
            .send_request(request_id, msg_frame, timeout, trace_id, None)
            .await
            .map_err(|e| {
                if !e.retryable() {
                    error!(rpc=%"put_metadata_blob", %request_id, %blob_guid, %block_number, %version, is_new=%is_new, error=?e, "bss rpc failed");
                }
                e
            })?;
        check_response_errno(&resp_frame.header)?;
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
        header.retry_count = retry_count as u8;

        let msg_frame = MessageFrame::new(header, Bytes::new());
        let resp_frame = self
            .send_request(header.id, msg_frame, timeout, trace_id, None)
            .await
            .map_err(|e| {
                if !e.retryable() {
                    error!(rpc=%"get_metadata_blob", %request_id, %blob_guid, %block_number, %version, error=?e, "bss rpc failed");
                }
                e
            })?;
        check_response_errno(&resp_frame.header)?;
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
        header.retry_count = retry_count as u8;

        let msg_frame = MessageFrame::new(header, Bytes::new());
        let resp_frame = self
            .send_request(header.id, msg_frame, timeout, trace_id, None)
            .await
            .map_err(|e| {
                if !e.retryable() {
                    error!(rpc=%"delete_metadata_blob", %request_id, %blob_guid, %block_number, error=?e, "bss rpc failed");
                }
                e
            })?;
        check_response_errno(&resp_frame.header)?;
        Ok(())
    }
}
