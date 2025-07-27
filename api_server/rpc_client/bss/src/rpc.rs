use std::time::Duration;

use crate::{
    codec::MessageFrame,
    message::{Command, MessageHeader},
    rpc_client::{InflightRpcGuard, Message, RpcClient, RpcError},
};
use bytes::Bytes;
use rpc_client_common::ErrorRetryable;
use tracing::error;
use uuid::Uuid;

impl RpcClient {
    pub async fn put_blob(
        &self,
        blob_id: Uuid,
        block_number: u32,
        body: Bytes,
        timeout: Option<Duration>,
    ) -> Result<(), RpcError> {
        let _guard = InflightRpcGuard::new("bss", "put_blob");
        let mut header = MessageHeader::default();
        let request_id = self.gen_request_id();
        header.id = request_id;
        header.blob_id = blob_id.into_bytes();
        header.block_number = block_number;
        header.command = Command::PutBlob;
        header.size = (MessageHeader::SIZE + body.len()) as u32;

        let msg_frame = MessageFrame::new(header, body);
        self
            .send_request(header.id, Message::Frame(msg_frame), timeout)
            .await
            .map_err(|e| {
                if !e.retryable() {
                    error!(rpc=%"put_blob", %request_id, %blob_id, %block_number, error=?e, "bss rpc failed");
                }
                e
            })?;
        Ok(())
    }

    pub async fn get_blob(
        &self,
        blob_id: Uuid,
        block_number: u32,
        body: &mut Bytes,
        timeout: Option<Duration>,
    ) -> Result<(), RpcError> {
        let _guard = InflightRpcGuard::new("bss", "get_blob");
        let mut header = MessageHeader::default();
        let request_id = self.gen_request_id();
        header.id = request_id;
        header.blob_id = blob_id.into_bytes();
        header.block_number = block_number;
        header.command = Command::GetBlob;
        header.size = MessageHeader::SIZE as u32;

        let msg_frame = MessageFrame::new(header, Bytes::new());
        let resp = self
            .send_request(header.id, Message::Frame(msg_frame), timeout)
            .await
            .map_err(|e| {
                if !e.retryable() {
                    error!(rpc=%"get_blob", %request_id, %blob_id, %block_number, error=?e, "bss rpc failed");
                }
                e
            })?;
        *body = resp.body;
        Ok(())
    }

    pub async fn delete_blob(
        &self,
        blob_id: Uuid,
        block_number: u32,
        timeout: Option<Duration>,
    ) -> Result<(), RpcError> {
        let _guard = InflightRpcGuard::new("bss", "delete_blob");
        let mut header = MessageHeader::default();
        let request_id = self.gen_request_id();
        header.id = request_id;
        header.blob_id = blob_id.into_bytes();
        header.block_number = block_number;
        header.command = Command::DeleteBlob;
        header.size = MessageHeader::SIZE as u32;

        let msg_frame = MessageFrame::new(header, Bytes::new());
        self
            .send_request(header.id, Message::Frame(msg_frame), timeout)
            .await
            .map_err(|e| {
                if !e.retryable() {
                    error!(rpc=%"delete_blob", %request_id, %blob_id, %block_number, error=?e, "bss rpc failed");
                }
                e
            })?;
        Ok(())
    }
}
