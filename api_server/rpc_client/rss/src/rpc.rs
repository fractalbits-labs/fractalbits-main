use std::time::Instant;

use crate::{
    message::MessageHeader,
    rpc_client::{InflightRpcGuard, Message, RpcClient, RpcError},
};
use bytes::BytesMut;
use metrics::histogram;
use prost::Message as PbMessage;
use tracing::{error, warn};

include!(concat!(env!("OUT_DIR"), "/rss_ops.rs"));

impl RpcClient {
    pub async fn put(&self, version: i64, key: &str, value: &str) -> Result<(), RpcError> {
        let _guard = InflightRpcGuard::new("rss", "put");
        let start = Instant::now();
        let body = PutRequest {
            version,
            key: key.to_string(),
            value: value.to_string(),
        };

        let mut header = MessageHeader::default();
        let request_id = self.gen_request_id();
        header.id = request_id;
        header.command = Command::Put;
        header.size = (MessageHeader::SIZE + body.encoded_len()) as u32;

        let mut request_bytes = BytesMut::with_capacity(header.size as usize);
        header.encode(&mut request_bytes);
        body.encode(&mut request_bytes)
            .map_err(RpcError::EncodeError)?;

        let resp_bytes = self
            .send_request(header.id, Message::Bytes(request_bytes.freeze()))
            .await
            .map_err(|e| {
                error!(rpc="put", %request_id, %key, error=?e, "rss rpc failed");
                e
            })?
            .body;
        let resp: PutResponse = PbMessage::decode(resp_bytes).map_err(RpcError::DecodeError)?;
        let duration = start.elapsed();
        match resp.result.unwrap() {
            put_response::Result::Ok(()) => {
                histogram!("rss_rpc_nanos", "status" => "Put_Ok")
                    .record(duration.as_nanos() as f64);
                Ok(())
            }
            put_response::Result::ErrOthers(resp) => {
                histogram!("rss_rpc_nanos", "status" => "Put_ErrOthers")
                    .record(duration.as_nanos() as f64);
                error!("rpc put for key {key} failed: {}", &resp);
                Err(RpcError::InternalResponseError(resp))
            }
            put_response::Result::ErrRetry(()) => {
                histogram!("rss_rpc_nanos", "status" => "Put_ErrRetry")
                    .record(duration.as_nanos() as f64);
                warn!("rpc put for key {key} failed, retry needed");
                Err(RpcError::Retry)
            }
        }
    }

    pub async fn put_with_extra(
        &self,
        version: i64,
        key: &str,
        value: &str,
        extra_version: i64,
        extra_key: &str,
        extra_value: &str,
    ) -> Result<(), RpcError> {
        let _guard = InflightRpcGuard::new("rss", "put_with_extra");
        let start = Instant::now();
        let body = PutWithExtraRequest {
            version,
            key: key.to_string(),
            value: value.to_string(),
            extra_version,
            extra_key: extra_key.to_string(),
            extra_value: extra_value.to_string(),
        };

        let mut header = MessageHeader::default();
        let request_id = self.gen_request_id();
        header.id = request_id;
        header.command = Command::PutWithExtra;
        header.size = (MessageHeader::SIZE + body.encoded_len()) as u32;

        let mut request_bytes = BytesMut::with_capacity(header.size as usize);
        header.encode(&mut request_bytes);
        body.encode(&mut request_bytes)
            .map_err(RpcError::EncodeError)?;

        let resp_bytes = self
            .send_request(header.id, Message::Bytes(request_bytes.freeze()))
            .await
            .map_err(|e| {
                error!(rpc="put", %request_id, %key, %extra_key, error=?e, "rss rpc failed");
                e
            })?
            .body;
        let resp: PutWithExtraResponse =
            PbMessage::decode(resp_bytes).map_err(RpcError::DecodeError)?;
        let duration = start.elapsed();
        match resp.result.unwrap() {
            put_with_extra_response::Result::Ok(()) => {
                histogram!("rss_rpc_nanos", "status" => "PutWithExtra_Ok")
                    .record(duration.as_nanos() as f64);
                Ok(())
            }
            put_with_extra_response::Result::ErrOthers(resp) => {
                histogram!("rss_rpc_nanos", "status" => "PutWithExtra_ErrOthers")
                    .record(duration.as_nanos() as f64);
                error!("rpc put for key {key} and {extra_key} failed: {}", &resp);
                Err(RpcError::InternalResponseError(resp))
            }
            put_with_extra_response::Result::ErrRetry(()) => {
                histogram!("rss_rpc_nanos", "status" => "PutWithExtra_ErrRetry")
                    .record(duration.as_nanos() as f64);
                warn!("rpc put for key {key} and {extra_key} failed, retry needed");
                Err(RpcError::Retry)
            }
        }
    }

    pub async fn get(&self, key: &str) -> Result<(i64, String), RpcError> {
        let _guard = InflightRpcGuard::new("rss", "get");
        let start = Instant::now();
        let body = GetRequest { key: key.to_string() };

        let mut header = MessageHeader::default();
        let request_id = self.gen_request_id();
        header.id = request_id;
        header.command = Command::Get;
        header.size = (MessageHeader::SIZE + body.encoded_len()) as u32;

        let mut request_bytes = BytesMut::with_capacity(header.size as usize);
        header.encode(&mut request_bytes);
        body.encode(&mut request_bytes)
            .map_err(RpcError::EncodeError)?;

        let resp_bytes = self
            .send_request(header.id, Message::Bytes(request_bytes.freeze()))
            .await
            .map_err(|e| {
                error!(rpc="get", %request_id, %key, error=?e, "rss rpc failed");
                e
            })?
            .body;
        let resp: GetResponse = PbMessage::decode(resp_bytes).map_err(RpcError::DecodeError)?;
        let duration = start.elapsed();
        match resp.result.unwrap() {
            get_response::Result::Ok(resp) => {
                histogram!("rss_rpc_nanos", "status" => "Get_Ok")
                    .record(duration.as_nanos() as f64);
                Ok((resp.version, resp.value))
            }
            get_response::Result::ErrNotFound(_resp) => {
                histogram!("rss_rpc_nanos", "status" => "Get_ErrNotFound")
                    .record(duration.as_nanos() as f64);
                error!("could not find entry for key: {key}");
                Err(RpcError::NotFound)
            }
            get_response::Result::ErrOthers(resp) => {
                histogram!("rss_rpc_nanos", "status" => "Get_ErrOthers")
                    .record(duration.as_nanos() as f64);
                error!("rpc get for key {key} failed: {}", &resp);
                Err(RpcError::InternalResponseError(resp))
            }
        }
    }

    pub async fn delete(&self, key: &str) -> Result<(), RpcError> {
        let _guard = InflightRpcGuard::new("rss", "delete");
        let start = Instant::now();
        let body = DeleteRequest { key: key.to_string() };

        let mut header = MessageHeader::default();
        let request_id = self.gen_request_id();
        header.id = request_id;
        header.command = Command::Delete;
        header.size = (MessageHeader::SIZE + body.encoded_len()) as u32;

        let mut request_bytes = BytesMut::with_capacity(header.size as usize);
        header.encode(&mut request_bytes);
        body.encode(&mut request_bytes)
            .map_err(RpcError::EncodeError)?;

        let resp_bytes = self
            .send_request(header.id, Message::Bytes(request_bytes.freeze()))
            .await
            .map_err(|e| {
                error!(rpc="delete", %request_id, %key, error=?e, "rss rpc failed");
                e
            })?
            .body;
        let resp: DeleteResponse = PbMessage::decode(resp_bytes).map_err(RpcError::DecodeError)?;
        let duration = start.elapsed();
        match resp.result.unwrap() {
            delete_response::Result::Ok(()) => {
                histogram!("rss_rpc_nanos", "status" => "Delete_Ok")
                    .record(duration.as_nanos() as f64);
                Ok(())
            }
            delete_response::Result::Err(resp) => {
                histogram!("rss_rpc_nanos", "status" => "Delete_Err")
                    .record(duration.as_nanos() as f64);
                error!("rpc delete for key {key} failed: {}", &resp);
                Err(RpcError::InternalResponseError(resp))
            }
        }
    }

    pub async fn delete_with_extra(
        &self,
        key: &str,
        extra_version: i64,
        extra_key: &str,
        extra_value: &str,
    ) -> Result<(), RpcError> {
        let _guard = InflightRpcGuard::new("rss", "delete_with_extra");
        let start = Instant::now();
        let body = DeleteWithExtraRequest {
            key: key.to_string(),
            extra_version,
            extra_key: extra_key.to_string(),
            extra_value: extra_value.to_string(),
        };

        let mut header = MessageHeader::default();
        let request_id = self.gen_request_id();
        header.id = request_id;
        header.command = Command::DeleteWithExtra;
        header.size = (MessageHeader::SIZE + body.encoded_len()) as u32;

        let mut request_bytes = BytesMut::with_capacity(header.size as usize);
        header.encode(&mut request_bytes);
        body.encode(&mut request_bytes)
            .map_err(RpcError::EncodeError)?;

        let resp_bytes = self
            .send_request(header.id, Message::Bytes(request_bytes.freeze()))
            .await
            .map_err(|e| {
                error!(rpc="delete_with_extra", %request_id, %key, %extra_key, error=?e, "rss rpc failed");
                e
            })?
            .body;
        let resp: DeleteWithExtraResponse =
            PbMessage::decode(resp_bytes).map_err(RpcError::DecodeError)?;
        let duration = start.elapsed();
        match resp.result.unwrap() {
            delete_with_extra_response::Result::Ok(()) => {
                histogram!("rss_rpc_nanos", "status" => "DeleteWithExtra_Ok")
                    .record(duration.as_nanos() as f64);
                Ok(())
            }
            delete_with_extra_response::Result::ErrOthers(resp) => {
                histogram!("rss_rpc_nanos", "status" => "DeleteWithExtra_ErrOthers")
                    .record(duration.as_nanos() as f64);
                error!("rpc delete for key {key} and {extra_key} failed: {}", &resp);
                Err(RpcError::InternalResponseError(resp))
            }
            delete_with_extra_response::Result::ErrRetry(()) => {
                histogram!("rss_rpc_nanos", "status" => "DeleteWithExtra_ErrRetry")
                    .record(duration.as_nanos() as f64);
                warn!("rpc delete key {key} and {extra_key} failed, retry needed");
                Err(RpcError::Retry)
            }
        }
    }

    pub async fn list(&self, prefix: &str) -> Result<Vec<String>, RpcError> {
        let _guard = InflightRpcGuard::new("rss", "list");
        let start = Instant::now();
        let body = ListRequest {
            prefix: prefix.to_string(),
        };

        let mut header = MessageHeader::default();
        let request_id = self.gen_request_id();
        header.id = request_id;
        header.command = Command::List;
        header.size = (MessageHeader::SIZE + body.encoded_len()) as u32;

        let mut request_bytes = BytesMut::with_capacity(header.size as usize);
        header.encode(&mut request_bytes);
        body.encode(&mut request_bytes)
            .map_err(RpcError::EncodeError)?;

        let resp_bytes = self
            .send_request(header.id, Message::Bytes(request_bytes.freeze()))
            .await
            .map_err(|e| {
                error!(rpc="list", %request_id, %prefix, error=?e, "rss rpc failed");
                e
            })?
            .body;
        let resp: ListResponse = PbMessage::decode(resp_bytes).map_err(RpcError::DecodeError)?;
        let duration = start.elapsed();
        match resp.result.unwrap() {
            list_response::Result::Ok(resp) => {
                histogram!("rss_rpc_nanos", "status" => "List_Ok")
                    .record(duration.as_nanos() as f64);
                Ok(resp.kvs)
            }
            list_response::Result::Err(resp) => {
                histogram!("rss_rpc_nanos", "status" => "List_Err")
                    .record(duration.as_nanos() as f64);
                error!("rpc list for prefix {prefix} failed: {}", &resp);
                Err(RpcError::InternalResponseError(resp))
            }
        }
    }
}
