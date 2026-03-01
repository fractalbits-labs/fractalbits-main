#![allow(clippy::field_reassign_with_default)]

use crate::rpc::{AutoReconnectRpcClient, RpcCodec, RpcError};
use bytes::{Bytes, BytesMut};
use data_types::{DataVgInfo, TraceId};
use prost::Message as PbMessage;
use rpc_codec_common::MessageFrame;
use rss_codec::*;
use std::time::Duration;
use tracing::error;

#[derive(Default, Clone)]
pub struct RssCodec;

impl RpcCodec<MessageHeader> for RssCodec {
    const RPC_TYPE: &'static str = "rss";
}

pub struct RssRpcClient(AutoReconnectRpcClient<RssCodec, MessageHeader>);

impl std::ops::Deref for RssRpcClient {
    type Target = AutoReconnectRpcClient<RssCodec, MessageHeader>;
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

impl RssRpcClient {
    pub fn new_from_addresses(addresses: Vec<String>) -> Self {
        Self(AutoReconnectRpcClient::new_from_addresses(addresses))
    }

    pub async fn get(
        &self,
        key: &str,
        _timeout: Option<Duration>,
        trace_id: &TraceId,
    ) -> Result<(i64, String), RpcError> {
        let body = GetRequest {
            key: key.to_string(),
        };

        let mut header = MessageHeader::default();
        header.id = self.gen_request_id();
        header.command = Command::Get;
        header.size = (size_of::<MessageHeader>() + body.encoded_len()) as u32;
        header.set_trace_id(trace_id);

        let body_bytes = encode_protobuf(body)?;
        header.set_body_checksum(&body_bytes);
        let frame = MessageFrame::new(header, body_bytes);
        let resp_frame = self.send_request(frame).await?;
        let resp: GetResponse =
            PbMessage::decode(resp_frame.body).map_err(|e| RpcError::DecodeError(e.to_string()))?;
        match resp.result.unwrap() {
            rss_codec::get_response::Result::Ok(resp) => Ok((resp.version, resp.value)),
            rss_codec::get_response::Result::ErrNotFound(_) => Err(RpcError::NotFound),
            rss_codec::get_response::Result::ErrOther(e) => {
                error!(rpc=%"get", %key, "rss rpc error: {e}");
                Err(RpcError::InternalResponseError(e))
            }
        }
    }

    pub async fn get_active_nss_address(
        &self,
        _timeout: Option<Duration>,
        trace_id: &TraceId,
    ) -> Result<String, RpcError> {
        let body = GetActiveNssAddressRequest {};

        let mut header = MessageHeader::default();
        header.id = self.gen_request_id();
        header.command = Command::GetActiveNssAddress;
        header.size = (size_of::<MessageHeader>() + body.encoded_len()) as u32;
        header.set_trace_id(trace_id);

        let body_bytes = encode_protobuf(body)?;
        header.set_body_checksum(&body_bytes);
        let frame = MessageFrame::new(header, body_bytes);
        let resp_frame = self.send_request(frame).await?;
        let resp: GetActiveNssAddressResponse =
            PbMessage::decode(resp_frame.body).map_err(|e| RpcError::DecodeError(e.to_string()))?;
        match resp.result.unwrap() {
            rss_codec::get_active_nss_address_response::Result::Address(addr) => Ok(addr),
            rss_codec::get_active_nss_address_response::Result::Error(e) => {
                error!(rpc=%"get_active_nss_address", "rss rpc error: {e}");
                Err(RpcError::InternalResponseError(e))
            }
        }
    }

    pub async fn get_data_vg_info(
        &self,
        _timeout: Option<Duration>,
        trace_id: &TraceId,
    ) -> Result<DataVgInfo, RpcError> {
        let body = GetDataVgInfoRequest {};

        let mut header = MessageHeader::default();
        header.id = self.gen_request_id();
        header.command = Command::GetDataVgInfo;
        header.size = (size_of::<MessageHeader>() + body.encoded_len()) as u32;
        header.set_trace_id(trace_id);

        let body_bytes = encode_protobuf(body)?;
        header.set_body_checksum(&body_bytes);
        let frame = MessageFrame::new(header, body_bytes);
        let resp_frame = self.send_request(frame).await?;
        let resp: GetDataVgInfoResponse =
            PbMessage::decode(resp_frame.body).map_err(|e| RpcError::DecodeError(e.to_string()))?;
        match resp.result.unwrap() {
            rss_codec::get_data_vg_info_response::Result::InfoJson(json) => {
                serde_json::from_str::<DataVgInfo>(&json)
                    .map_err(|e| RpcError::DecodeError(format!("JSON parse error: {e}")))
            }
            rss_codec::get_data_vg_info_response::Result::Error(e) => {
                error!(rpc=%"get_data_vg_info", "rss rpc error: {e}");
                Err(RpcError::InternalResponseError(e))
            }
        }
    }
}
