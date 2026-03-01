#![allow(clippy::field_reassign_with_default)]

use crate::rpc::{AutoReconnectRpcClient, RpcCodec, RpcError};
use bytes::{Bytes, BytesMut};
use data_types::TraceId;
use nss_codec::*;
use prost::Message as PbMessage;
use rpc_codec_common::MessageFrame;
use std::time::Duration;
use tracing::error;

#[derive(Default, Clone)]
pub struct NssCodec;

impl RpcCodec<MessageHeader> for NssCodec {
    const RPC_TYPE: &'static str = "nss";
}

pub struct NssRpcClient(AutoReconnectRpcClient<NssCodec, MessageHeader>);

impl std::ops::Deref for NssRpcClient {
    type Target = AutoReconnectRpcClient<NssCodec, MessageHeader>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

fn encode_protobuf<M: PbMessage>(msg: M) -> Result<Bytes, RpcError> {
    let mut buf = BytesMut::with_capacity(1024);
    msg.encode(&mut buf)
        .map_err(|e| RpcError::EncodeError(e.to_string()))?;
    Ok(buf.freeze())
}

impl NssRpcClient {
    pub fn new_from_address(address: String) -> Self {
        Self(AutoReconnectRpcClient::new_from_address(address))
    }

    pub async fn put_inode(
        &self,
        root_blob_name: &str,
        key: &str,
        value: Bytes,
        _timeout: Option<Duration>,
        trace_id: &TraceId,
    ) -> Result<PutInodeResponse, RpcError> {
        let mut nss_key = key.to_string();
        nss_key.push('\0');
        let body = PutInodeRequest {
            root_blob_name: root_blob_name.to_string(),
            key: nss_key,
            value,
        };

        let mut header = MessageHeader::default();
        header.id = self.gen_request_id();
        header.command = Command::PutInode;
        header.size = (size_of::<MessageHeader>() + body.encoded_len()) as u32;
        header.set_trace_id(trace_id);

        let body_bytes = encode_protobuf(body)?;
        header.set_body_checksum(&body_bytes);
        let frame = MessageFrame::new(header, body_bytes);
        let resp_frame = self.send_request(frame).await?;
        PbMessage::decode(resp_frame.body).map_err(|e| RpcError::DecodeError(e.to_string()))
    }

    pub async fn get_inode(
        &self,
        root_blob_name: &str,
        key: &str,
        _timeout: Option<Duration>,
        trace_id: &TraceId,
    ) -> Result<GetInodeResponse, RpcError> {
        let mut nss_key = key.to_string();
        nss_key.push('\0');
        let body = GetInodeRequest {
            root_blob_name: root_blob_name.to_string(),
            key: nss_key,
        };

        let mut header = MessageHeader::default();
        header.id = self.gen_request_id();
        header.command = Command::GetInode;
        header.size = (size_of::<MessageHeader>() + body.encoded_len()) as u32;
        header.set_trace_id(trace_id);

        let body_bytes = encode_protobuf(body)?;
        header.set_body_checksum(&body_bytes);
        let frame = MessageFrame::new(header, body_bytes);
        let resp_frame = self.send_request(frame).await?;
        PbMessage::decode(resp_frame.body).map_err(|e| RpcError::DecodeError(e.to_string()))
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn list_inodes(
        &self,
        root_blob_name: &str,
        max_keys: u32,
        prefix: &str,
        delimiter: &str,
        start_after: &str,
        skip_mpu_parts: bool,
        _timeout: Option<Duration>,
        trace_id: &TraceId,
    ) -> Result<ListInodesResponse, RpcError> {
        let mut start_after_owned = start_after.to_string();
        if !start_after_owned.ends_with("/") {
            start_after_owned.push('\0');
        }
        let body = ListInodesRequest {
            root_blob_name: root_blob_name.to_string(),
            max_keys,
            prefix: prefix.to_string(),
            delimiter: delimiter.to_string(),
            start_after: start_after_owned,
            skip_mpu_parts,
        };

        let mut header = MessageHeader::default();
        header.id = self.gen_request_id();
        header.command = Command::ListInodes;
        header.size = (size_of::<MessageHeader>() + body.encoded_len()) as u32;
        header.set_trace_id(trace_id);

        let body_bytes = encode_protobuf(body)?;
        header.set_body_checksum(&body_bytes);
        let frame = MessageFrame::new(header, body_bytes);
        let resp_frame = self.send_request(frame).await?;
        PbMessage::decode(resp_frame.body).map_err(|e| RpcError::DecodeError(e.to_string()))
    }

    pub async fn delete_inode(
        &self,
        root_blob_name: &str,
        key: &str,
        _timeout: Option<Duration>,
        trace_id: &TraceId,
    ) -> Result<DeleteInodeResponse, RpcError> {
        let mut nss_key = key.to_string();
        nss_key.push('\0');
        let body = DeleteInodeRequest {
            root_blob_name: root_blob_name.to_string(),
            key: nss_key,
        };

        let mut header = MessageHeader::default();
        header.id = self.gen_request_id();
        header.command = Command::DeleteInode;
        header.size = (size_of::<MessageHeader>() + body.encoded_len()) as u32;
        header.set_trace_id(trace_id);

        let body_bytes = encode_protobuf(body)?;
        header.set_body_checksum(&body_bytes);
        let frame = MessageFrame::new(header, body_bytes);
        let resp_frame = self.send_request(frame).await?;
        PbMessage::decode(resp_frame.body).map_err(|e| RpcError::DecodeError(e.to_string()))
    }

    pub async fn rename_object(
        &self,
        root_blob_name: &str,
        src_path: &str,
        dst_path: &str,
        _timeout: Option<Duration>,
        trace_id: &TraceId,
    ) -> Result<(), RpcError> {
        let mut nss_src = src_path.to_string();
        nss_src.push('\0');
        let mut nss_dst = dst_path.to_string();
        nss_dst.push('\0');

        let body = RenameRequest {
            root_blob_name: root_blob_name.to_string(),
            src_path: nss_src,
            dst_path: nss_dst,
        };

        let mut header = MessageHeader::default();
        header.id = self.gen_request_id();
        header.command = Command::Rename;
        header.size = (size_of::<MessageHeader>() + body.encoded_len()) as u32;
        header.set_trace_id(trace_id);

        let body_bytes = encode_protobuf(body)?;
        header.set_body_checksum(&body_bytes);
        let frame = MessageFrame::new(header, body_bytes);
        let resp_frame = self.send_request(frame).await?;
        let resp: RenameResponse =
            PbMessage::decode(resp_frame.body).map_err(|e| RpcError::DecodeError(e.to_string()))?;
        match resp.result.unwrap() {
            nss_codec::rename_response::Result::Ok(_) => Ok(()),
            nss_codec::rename_response::Result::ErrSrcNonexisted(_) => Err(RpcError::NotFound),
            nss_codec::rename_response::Result::ErrDstExisted(_) => Err(RpcError::AlreadyExists),
            nss_codec::rename_response::Result::ErrOther(e) => {
                error!(rpc=%"rename_object", "nss rpc error: {e}");
                Err(RpcError::InternalResponseError(e))
            }
        }
    }

    pub async fn rename_folder(
        &self,
        root_blob_name: &str,
        src_path: &str,
        dst_path: &str,
        _timeout: Option<Duration>,
        trace_id: &TraceId,
    ) -> Result<(), RpcError> {
        let body = RenameRequest {
            root_blob_name: root_blob_name.to_string(),
            src_path: src_path.to_string(),
            dst_path: dst_path.to_string(),
        };

        let mut header = MessageHeader::default();
        header.id = self.gen_request_id();
        header.command = Command::Rename;
        header.size = (size_of::<MessageHeader>() + body.encoded_len()) as u32;
        header.set_trace_id(trace_id);

        let body_bytes = encode_protobuf(body)?;
        header.set_body_checksum(&body_bytes);
        let frame = MessageFrame::new(header, body_bytes);
        let resp_frame = self.send_request(frame).await?;
        let resp: RenameResponse =
            PbMessage::decode(resp_frame.body).map_err(|e| RpcError::DecodeError(e.to_string()))?;
        match resp.result.unwrap() {
            nss_codec::rename_response::Result::Ok(_) => Ok(()),
            nss_codec::rename_response::Result::ErrSrcNonexisted(_) => Err(RpcError::NotFound),
            nss_codec::rename_response::Result::ErrDstExisted(_) => Err(RpcError::AlreadyExists),
            nss_codec::rename_response::Result::ErrOther(e) => {
                error!(rpc=%"rename_folder", "nss rpc error: {e}");
                Err(RpcError::InternalResponseError(e))
            }
        }
    }
}
