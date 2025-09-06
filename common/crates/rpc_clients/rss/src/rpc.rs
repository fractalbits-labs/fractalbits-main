use std::time::{Duration, Instant};

use crate::client::RpcClient;
use bytes::{Bytes, BytesMut};
use metrics::histogram;
use prost::Message as PbMessage;
use rpc_client_common::{ErrorRetryable, InflightRpcGuard, RpcError};
use rpc_codec_common::MessageFrame;
use rss_codec::*;
use tracing::{error, warn};

fn parse_data_vg_info_from_json(json_str: &str) -> Result<DataVgInfo, String> {
    let json_value: serde_json::Value =
        serde_json::from_str(json_str).map_err(|e| format!("Failed to parse JSON: {}", e))?;

    let volumes_array = json_value
        .get("volumes")
        .and_then(|v| v.as_array())
        .ok_or("volumes field not found or not an array")?;

    let mut volumes = Vec::new();
    for volume_json in volumes_array {
        let volume_id = volume_json
            .get("volume_id")
            .and_then(|v| v.as_u64())
            .ok_or("volume_id not found or not a number")? as u32;

        let bss_nodes_array = volume_json
            .get("bss_nodes")
            .and_then(|v| v.as_array())
            .ok_or("bss_nodes field not found or not an array")?;

        let mut bss_nodes = Vec::new();
        for node_json in bss_nodes_array {
            let node_id = node_json
                .get("node_id")
                .and_then(|v| v.as_str())
                .ok_or("node_id not found or not a string")?
                .to_string();

            let address = node_json
                .get("address")
                .and_then(|v| v.as_str())
                .ok_or("address not found or not a string")?
                .to_string();

            bss_nodes.push(BssNode { node_id, address });
        }

        volumes.push(DataVolume {
            volume_id,
            bss_nodes,
        });
    }

    let quorum = if let Some(quorum_json) = json_value.get("quorum") {
        if !quorum_json.is_null() {
            let n = quorum_json
                .get("n")
                .and_then(|v| v.as_u64())
                .ok_or("n not found or not a number")? as u32;

            let r = quorum_json
                .get("r")
                .and_then(|v| v.as_u64())
                .ok_or("r not found or not a number")? as u32;

            let w = quorum_json
                .get("w")
                .and_then(|v| v.as_u64())
                .ok_or("w not found or not a number")? as u32;

            Some(QuorumConfig { n, r, w })
        } else {
            None
        }
    } else {
        None
    };

    Ok(DataVgInfo { volumes, quorum })
}

impl RpcClient {
    pub async fn put(
        &self,
        version: i64,
        key: &str,
        value: &str,
        timeout: Option<Duration>,
    ) -> Result<(), RpcError> {
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

        let mut body_bytes = BytesMut::new();
        body.encode(&mut body_bytes)
            .map_err(|e| RpcError::EncodeError(e.to_string()))?;

        let frame = MessageFrame::new(header, body_bytes.freeze());
        let resp_frame = self
            .send_request(request_id, frame, timeout)
            .await
            .map_err(|e| {
                if !e.retryable() {
                    error!(rpc=%"put", %request_id, %key, error=?e, "rss rpc failed");
                }
                e
            })?;
        let resp: PutResponse =
            PbMessage::decode(resp_frame.body).map_err(|e| RpcError::DecodeError(e.to_string()))?;
        let duration = start.elapsed();
        match resp.result.unwrap() {
            rss_codec::put_response::Result::Ok(()) => {
                histogram!("rss_rpc_nanos", "status" => "Put_Ok")
                    .record(duration.as_nanos() as f64);
                Ok(())
            }
            rss_codec::put_response::Result::ErrOthers(resp) => {
                histogram!("rss_rpc_nanos", "status" => "Put_ErrOthers")
                    .record(duration.as_nanos() as f64);
                error!(rpc=%"put", %key, "rss rpc failed: {resp}");
                Err(RpcError::InternalResponseError(resp))
            }
            rss_codec::put_response::Result::ErrRetry(()) => {
                histogram!("rss_rpc_nanos", "status" => "Put_ErrRetry")
                    .record(duration.as_nanos() as f64);
                warn!(rpc=%"put", %key, "rss rpc failed, retry needed");
                Err(RpcError::Retry)
            }
        }
    }

    pub async fn get(
        &self,
        key: &str,
        timeout: Option<Duration>,
    ) -> Result<(i64, String), RpcError> {
        let _guard = InflightRpcGuard::new("rss", "get");
        let start = Instant::now();
        let body = GetRequest {
            key: key.to_string(),
        };

        let mut header = MessageHeader::default();
        let request_id = self.gen_request_id();
        header.id = request_id;
        header.command = Command::Get;
        header.size = (MessageHeader::SIZE + body.encoded_len()) as u32;

        let mut body_bytes = BytesMut::new();
        body.encode(&mut body_bytes)
            .map_err(|e| RpcError::EncodeError(e.to_string()))?;

        let frame = MessageFrame::new(header, body_bytes.freeze());
        let resp_frame = self
            .send_request(request_id, frame, timeout)
            .await
            .map_err(|e| {
                if !e.retryable() {
                    error!(rpc=%"get", %request_id, %key, error=?e, "rss rpc failed");
                }
                e
            })?;
        let resp: GetResponse =
            PbMessage::decode(resp_frame.body).map_err(|e| RpcError::DecodeError(e.to_string()))?;
        let duration = start.elapsed();
        match resp.result.unwrap() {
            rss_codec::get_response::Result::Ok(resp) => {
                histogram!("rss_rpc_nanos", "status" => "Get_Ok")
                    .record(duration.as_nanos() as f64);
                Ok((resp.version, resp.value))
            }
            rss_codec::get_response::Result::ErrNotFound(_resp) => {
                histogram!("rss_rpc_nanos", "status" => "Get_ErrNotFound")
                    .record(duration.as_nanos() as f64);
                warn!(rpc=%"get", %key, "could not find entry");
                Err(RpcError::NotFound)
            }
            rss_codec::get_response::Result::ErrOthers(resp) => {
                histogram!("rss_rpc_nanos", "status" => "Get_ErrOthers")
                    .record(duration.as_nanos() as f64);
                error!(rpc=%"get", %key, "rss rpc failed: {resp}");
                Err(RpcError::InternalResponseError(resp))
            }
        }
    }

    pub async fn delete(&self, key: &str, timeout: Option<Duration>) -> Result<(), RpcError> {
        let _guard = InflightRpcGuard::new("rss", "delete");
        let start = Instant::now();
        let body = DeleteRequest {
            key: key.to_string(),
        };

        let mut header = MessageHeader::default();
        let request_id = self.gen_request_id();
        header.id = request_id;
        header.command = Command::Delete;
        header.size = (MessageHeader::SIZE + body.encoded_len()) as u32;

        let mut body_bytes = BytesMut::new();
        body.encode(&mut body_bytes)
            .map_err(|e| RpcError::EncodeError(e.to_string()))?;

        let frame = MessageFrame::new(header, body_bytes.freeze());
        let resp_frame = self
            .send_request(request_id, frame, timeout)
            .await
            .map_err(|e| {
                error!(rpc=%"delete", %request_id, %key, error=?e, "rss rpc failed");
                e
            })?;
        let resp: DeleteResponse =
            PbMessage::decode(resp_frame.body).map_err(|e| RpcError::DecodeError(e.to_string()))?;
        let duration = start.elapsed();
        match resp.result.unwrap() {
            rss_codec::delete_response::Result::Ok(()) => {
                histogram!("rss_rpc_nanos", "status" => "Delete_Ok")
                    .record(duration.as_nanos() as f64);
                Ok(())
            }
            rss_codec::delete_response::Result::Err(resp) => {
                histogram!("rss_rpc_nanos", "status" => "Delete_Err")
                    .record(duration.as_nanos() as f64);
                error!(rpc=%"delete", %key, "rss rpc failed: {resp}");
                Err(RpcError::InternalResponseError(resp))
            }
        }
    }

    pub async fn get_nss_role(
        &self,
        instance_id: &str,
        timeout: Option<Duration>,
    ) -> Result<String, RpcError> {
        let _guard = InflightRpcGuard::new("rss", "get_nss_role");
        let start = Instant::now();
        let body = GetNssRoleRequest {
            instance_id: instance_id.to_string(),
        };

        let mut header = MessageHeader::default();
        let request_id = self.gen_request_id();
        header.id = request_id;
        header.command = Command::GetNssRole;
        header.size = (MessageHeader::SIZE + body.encoded_len()) as u32;

        let mut body_bytes = BytesMut::new();
        body.encode(&mut body_bytes)
            .map_err(|e| RpcError::EncodeError(e.to_string()))?;

        let frame = MessageFrame::new(header, body_bytes.freeze());
        let resp_frame = self
            .send_request(request_id, frame, timeout)
            .await
            .map_err(|e| {
                if !e.retryable() {
                    error!(rpc=%"get_nss_role", %request_id, %instance_id, error=?e, "rss rpc failed");
                }
                e
            })?;
        let resp: GetNssRoleResponse =
            PbMessage::decode(resp_frame.body).map_err(|e| RpcError::DecodeError(e.to_string()))?;
        let duration = start.elapsed();
        match resp.result.unwrap() {
            rss_codec::get_nss_role_response::Result::Role(role) => {
                histogram!("rss_rpc_nanos", "status" => "GetNssRole_Ok")
                    .record(duration.as_nanos() as f64);
                Ok(role)
            }
            rss_codec::get_nss_role_response::Result::Error(err) => {
                histogram!("rss_rpc_nanos", "status" => "GetNssRole_Error")
                    .record(duration.as_nanos() as f64);
                error!(rpc=%"get_nss_role", %instance_id, "rss rpc failed: {err}");
                Err(RpcError::InternalResponseError(err))
            }
        }
    }

    pub async fn list(
        &self,
        prefix: &str,
        timeout: Option<Duration>,
    ) -> Result<Vec<String>, RpcError> {
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

        let mut body_bytes = BytesMut::new();
        body.encode(&mut body_bytes)
            .map_err(|e| RpcError::EncodeError(e.to_string()))?;

        let frame = MessageFrame::new(header, body_bytes.freeze());
        let resp_frame = self
            .send_request(request_id, frame, timeout)
            .await
            .map_err(|e| {
                if !e.retryable() {
                    error!(rpc=%"list", %request_id, %prefix, error=?e, "rss rpc failed");
                }
                e
            })?;
        let resp: ListResponse =
            PbMessage::decode(resp_frame.body).map_err(|e| RpcError::DecodeError(e.to_string()))?;
        let duration = start.elapsed();
        match resp.result.unwrap() {
            rss_codec::list_response::Result::Ok(resp) => {
                histogram!("rss_rpc_nanos", "status" => "List_Ok")
                    .record(duration.as_nanos() as f64);
                Ok(resp.kvs)
            }
            rss_codec::list_response::Result::Err(resp) => {
                histogram!("rss_rpc_nanos", "status" => "List_Err")
                    .record(duration.as_nanos() as f64);
                error!(rpc=%"list", %prefix, "rss rpc failed: {resp}");
                Err(RpcError::InternalResponseError(resp))
            }
        }
    }

    //== Health agent commands

    pub async fn send_heartbeat(
        &self,
        instance_id: &str,
        timeout: Option<Duration>,
    ) -> Result<(), RpcError> {
        let _guard = InflightRpcGuard::new("rss", "send_heartbeat");
        let start = Instant::now();
        let body = HeartbeatRequest {
            instance_id: instance_id.to_string(),
        };

        let mut header = MessageHeader::default();
        let request_id = self.gen_request_id();
        header.id = request_id;
        header.command = Command::Heartbeat;
        header.size = (MessageHeader::SIZE + body.encoded_len()) as u32;

        let mut body_bytes = BytesMut::new();
        body.encode(&mut body_bytes)
            .map_err(|e| RpcError::EncodeError(e.to_string()))?;

        let frame = MessageFrame::new(header, body_bytes.freeze());
        let resp_frame = self
            .send_request(request_id, frame, timeout)
            .await
            .map_err(|e| {
                if !e.retryable() {
                    error!(rpc=%"list", %request_id, %instance_id, error=?e, "rss rpc failed");
                }
                e
            })?;
        let resp: HeartbeatResponse =
            PbMessage::decode(resp_frame.body).map_err(|e| RpcError::DecodeError(e.to_string()))?;
        let duration = start.elapsed();
        match resp.result.unwrap() {
            rss_codec::heartbeat_response::Result::Ok(()) => {
                histogram!("rss_rpc_nanos", "status" => "Heartbeat_Ok")
                    .record(duration.as_nanos() as f64);
                Ok(())
            }
            rss_codec::heartbeat_response::Result::Error(resp) => {
                histogram!("rss_rpc_nanos", "status" => "Heartbeat_Error")
                    .record(duration.as_nanos() as f64);
                error!(rpc=%"put", %instance_id, "rss rpc failed: {resp}");
                Err(RpcError::InternalResponseError(resp))
            }
        }
    }

    pub async fn get_az_status(&self, timeout: Option<Duration>) -> Result<AzStatusMap, RpcError> {
        let _guard = InflightRpcGuard::new("rss", "get_az_status");
        let start = Instant::now();

        let mut header = MessageHeader::default();
        let request_id = self.gen_request_id();
        header.id = request_id;
        header.command = Command::GetAzStatus;
        header.size = MessageHeader::SIZE as u32;

        let frame = MessageFrame::new(header, Bytes::new());
        let resp_frame = self
            .send_request(header.id, frame, timeout)
            .await
            .map_err(|e| {
                if !e.retryable() {
                    error!(rpc=%"get_az_status", %request_id, error=?e, "rss rpc failed");
                }
                e
            })?;
        let resp: GetAzStatusResponse =
            PbMessage::decode(resp_frame.body).map_err(|e| RpcError::DecodeError(e.to_string()))?;
        let duration = start.elapsed();
        match resp.result.unwrap() {
            rss_codec::get_az_status_response::Result::StatusMap(status_map) => {
                histogram!("rss_rpc_nanos", "status" => "GetAzStatus_Ok")
                    .record(duration.as_nanos() as f64);
                Ok(status_map)
            }
            rss_codec::get_az_status_response::Result::Error(err) => {
                histogram!("rss_rpc_nanos", "status" => "GetAzStatus_Error")
                    .record(duration.as_nanos() as f64);
                error!(rpc=%"get_az_status", "rss rpc failed: {err}");
                Err(RpcError::InternalResponseError(err))
            }
        }
    }

    pub async fn set_az_status(
        &self,
        az_id: &str,
        status: &str,
        timeout: Option<Duration>,
    ) -> Result<(), RpcError> {
        let _guard = InflightRpcGuard::new("rss", "set_az_status");
        let start = Instant::now();
        let body = SetAzStatusRequest {
            az_id: az_id.to_string(),
            status: status.to_string(),
        };

        let mut header = MessageHeader::default();
        let request_id = self.gen_request_id();
        header.id = request_id;
        header.command = Command::SetAzStatus;
        header.size = (MessageHeader::SIZE + body.encoded_len()) as u32;

        let mut body_bytes = BytesMut::new();
        body.encode(&mut body_bytes)
            .map_err(|e| RpcError::EncodeError(e.to_string()))?;

        let frame = MessageFrame::new(header, body_bytes.freeze());
        let resp_frame = self
            .send_request(request_id, frame, timeout)
            .await
            .map_err(|e| {
                if !e.retryable() {
                    error!(rpc=%"set_az_status", %request_id, %az_id, %status, error=?e, "rss rpc failed");
                }
                e
            })?;
        let resp: SetAzStatusResponse =
            PbMessage::decode(resp_frame.body).map_err(|e| RpcError::DecodeError(e.to_string()))?;
        let duration = start.elapsed();
        match resp.result.unwrap() {
            rss_codec::set_az_status_response::Result::Ok(()) => {
                histogram!("rss_rpc_nanos", "status" => "SetAzStatus_Ok")
                    .record(duration.as_nanos() as f64);
                Ok(())
            }
            rss_codec::set_az_status_response::Result::Error(err) => {
                histogram!("rss_rpc_nanos", "status" => "SetAzStatus_Error")
                    .record(duration.as_nanos() as f64);
                error!(rpc=%"set_az_status", "rss rpc failed: {err}");
                Err(RpcError::InternalResponseError(err))
            }
        }
    }

    pub async fn create_bucket(
        &self,
        bucket_name: &str,
        api_key_id: &str,
        is_multi_az: bool,
        timeout: Option<Duration>,
    ) -> Result<(), RpcError> {
        let _guard = InflightRpcGuard::new("rss", "create_bucket");
        let start = Instant::now();
        let body = CreateBucketRequest {
            bucket_name: bucket_name.to_string(),
            enable_versioning: false,
            api_key_id: api_key_id.to_string(),
            is_multi_az,
        };

        let mut header = MessageHeader::default();
        let request_id = self.gen_request_id();
        header.id = request_id;
        header.command = Command::CreateBucket;
        header.size = (MessageHeader::SIZE + body.encoded_len()) as u32;

        let mut body_bytes = BytesMut::new();
        body.encode(&mut body_bytes)
            .map_err(|e| RpcError::EncodeError(e.to_string()))?;

        let frame = MessageFrame::new(header, body_bytes.freeze());
        let resp_frame = self
            .send_request(request_id, frame, timeout)
            .await
            .map_err(|e| {
                if !e.retryable() {
                    error!(rpc=%"create_bucket", %request_id, %bucket_name, error=?e, "rss rpc failed");
                }
                e
            })?;
        let resp: CreateBucketResponse =
            PbMessage::decode(resp_frame.body).map_err(|e| RpcError::DecodeError(e.to_string()))?;
        let duration = start.elapsed();
        match resp.result.unwrap() {
            rss_codec::create_bucket_response::Result::Ok(()) => {
                histogram!("rss_rpc_nanos", "status" => "CreateBucket_Ok")
                    .record(duration.as_nanos() as f64);
                Ok(())
            }
            rss_codec::create_bucket_response::Result::Error(err) => {
                histogram!("rss_rpc_nanos", "status" => "CreateBucket_Error")
                    .record(duration.as_nanos() as f64);
                error!(rpc=%"create_bucket", %bucket_name, "rss rpc failed: {err}");
                Err(RpcError::InternalResponseError(err))
            }
        }
    }

    pub async fn delete_bucket(
        &self,
        bucket_name: &str,
        api_key_id: &str,
        timeout: Option<Duration>,
    ) -> Result<(), RpcError> {
        let _guard = InflightRpcGuard::new("rss", "delete_bucket");
        let start = Instant::now();
        let body = DeleteBucketRequest {
            bucket_name: bucket_name.to_string(),
            api_key_id: api_key_id.to_string(),
        };

        let mut header = MessageHeader::default();
        let request_id = self.gen_request_id();
        header.id = request_id;
        header.command = Command::DeleteBucket;
        header.size = (MessageHeader::SIZE + body.encoded_len()) as u32;

        let mut body_bytes = BytesMut::new();
        body.encode(&mut body_bytes)
            .map_err(|e| RpcError::EncodeError(e.to_string()))?;

        let frame = MessageFrame::new(header, body_bytes.freeze());
        let resp_frame = self
            .send_request(request_id, frame, timeout)
            .await
            .map_err(|e| {
                if !e.retryable() {
                    error!(rpc=%"delete_bucket", %request_id, %bucket_name, error=?e, "rss rpc failed");
                }
                e
            })?;
        let resp: DeleteBucketResponse =
            PbMessage::decode(resp_frame.body).map_err(|e| RpcError::DecodeError(e.to_string()))?;
        let duration = start.elapsed();
        match resp.result.unwrap() {
            rss_codec::delete_bucket_response::Result::Ok(()) => {
                histogram!("rss_rpc_nanos", "status" => "DeleteBucket_Ok")
                    .record(duration.as_nanos() as f64);
                Ok(())
            }
            rss_codec::delete_bucket_response::Result::Error(err) => {
                histogram!("rss_rpc_nanos", "status" => "DeleteBucket_Error")
                    .record(duration.as_nanos() as f64);
                error!(rpc=%"delete_bucket", %bucket_name, "rss rpc failed: {err}");
                Err(RpcError::InternalResponseError(err))
            }
        }
    }

    pub async fn get_data_vg_info(
        &self,
        timeout: Option<Duration>,
    ) -> Result<DataVgInfo, RpcError> {
        let _guard = InflightRpcGuard::new("rss", "get_data_vg_info");
        let start = Instant::now();
        let body = GetDataVgInfoRequest {};

        let mut header = MessageHeader::default();
        let request_id = self.gen_request_id();
        header.id = request_id;
        header.command = Command::GetDataVgInfo;
        header.size = (MessageHeader::SIZE + body.encoded_len()) as u32;

        let mut body_bytes = BytesMut::new();
        body.encode(&mut body_bytes)
            .map_err(|e| RpcError::EncodeError(e.to_string()))?;

        let frame = MessageFrame::new(header, body_bytes.freeze());
        let resp_frame = self
            .send_request(request_id, frame, timeout)
            .await
            .map_err(|e| {
                if !e.retryable() {
                    error!(rpc=%"get_data_vg_info", %request_id, error=?e, "rss rpc failed");
                }
                e
            })?;
        let resp: GetDataVgInfoResponse =
            PbMessage::decode(resp_frame.body).map_err(|e| RpcError::DecodeError(e.to_string()))?;
        let duration = start.elapsed();
        match resp.result.unwrap() {
            rss_codec::get_data_vg_info_response::Result::InfoJson(info_json) => {
                match parse_data_vg_info_from_json(&info_json) {
                    Ok(info) => {
                        histogram!("rss_rpc_nanos", "status" => "GetDataVgInfo_Ok")
                            .record(duration.as_nanos() as f64);
                        Ok(info)
                    }
                    Err(e) => {
                        histogram!("rss_rpc_nanos", "status" => "GetDataVgInfo_ParseError")
                            .record(duration.as_nanos() as f64);
                        error!(rpc=%"get_data_vg_info", "failed to parse JSON response: {e}");
                        Err(RpcError::DecodeError(format!(
                            "Failed to parse JSON response: {}",
                            e
                        )))
                    }
                }
            }
            rss_codec::get_data_vg_info_response::Result::Error(err) => {
                histogram!("rss_rpc_nanos", "status" => "GetDataVgInfo_Error")
                    .record(duration.as_nanos() as f64);
                error!(rpc=%"get_data_vg_info", "rss rpc failed: {err}");
                Err(RpcError::InternalResponseError(err))
            }
        }
    }
}
