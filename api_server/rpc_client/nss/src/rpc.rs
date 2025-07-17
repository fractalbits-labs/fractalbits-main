use crate::{
    message::MessageHeader,
    rpc_client::{InflightRpcGuard, Message, RpcClient, RpcError},
};
use bytes::{Bytes, BytesMut};
use prost::Message as PbMessage;
use rpc_client_common::ErrorRetryable;
use tracing::error;

include!(concat!(env!("OUT_DIR"), "/nss_ops.rs"));

impl RpcClient {
    pub async fn put_inode(
        &self,
        root_blob_name: &str,
        key: &str,
        value: Bytes,
    ) -> Result<PutInodeResponse, RpcError> {
        let _guard = InflightRpcGuard::new("nss", "put_inode");
        let mut nss_key = key.to_string();
        nss_key.push('\0');
        let body = PutInodeRequest {
            root_blob_name: root_blob_name.to_string(),
            key: nss_key,
            value,
        };

        let mut header = MessageHeader::default();
        let request_id = self.gen_request_id();
        header.id = request_id;
        header.command = Command::PutInode;
        header.size = (MessageHeader::SIZE + body.encoded_len()) as u32;

        let mut request_bytes = BytesMut::with_capacity(header.size as usize);
        header.encode(&mut request_bytes);
        body.encode(&mut request_bytes)
            .map_err(RpcError::EncodeError)?;

        let resp_bytes = self
            .send_request(header.id, Message::Bytes(request_bytes.freeze()))
            .await
            .map_err(|e| {
                if !e.retryable() {
                    error!(rpc=%"put_inode", %request_id, %root_blob_name, %key, error=?e, "nss rpc failed");
                }
                e
            })?
            .body;
        let resp: PutInodeResponse =
            PbMessage::decode(resp_bytes).map_err(RpcError::DecodeError)?;
        Ok(resp)
    }

    pub async fn get_inode(
        &self,
        root_blob_name: &str,
        key: &str,
    ) -> Result<GetInodeResponse, RpcError> {
        let _guard = InflightRpcGuard::new("nss", "get_inode");
        let mut nss_key = key.to_string();
        nss_key.push('\0');
        let body = GetInodeRequest {
            root_blob_name: root_blob_name.to_string(),
            key: nss_key,
        };

        let mut header = MessageHeader::default();
        let request_id = self.gen_request_id();
        header.id = request_id;
        header.command = Command::GetInode;
        header.size = (MessageHeader::SIZE + body.encoded_len()) as u32;

        let mut request_bytes = BytesMut::with_capacity(header.size as usize);
        header.encode(&mut request_bytes);
        body.encode(&mut request_bytes)
            .map_err(RpcError::EncodeError)?;

        let resp_bytes = self
            .send_request(header.id, Message::Bytes(request_bytes.freeze()))
            .await
            .map_err(|e| {
                if !e.retryable() {
                    error!(rpc=%"get_inode", %request_id, %root_blob_name, %key, error=?e, "nss rpc failed");
                }
                e
            })?
            .body;
        let resp: GetInodeResponse =
            PbMessage::decode(resp_bytes).map_err(RpcError::DecodeError)?;
        Ok(resp)
    }

    pub async fn list_inodes(
        &self,
        root_blob_name: &str,
        max_keys: u32,
        prefix: &str,
        delimiter: &str,
        start_after: &str,
        skip_mpu_parts: bool,
    ) -> Result<ListInodesResponse, RpcError> {
        let _guard = InflightRpcGuard::new("nss", "list_inodes");
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
        let request_id = self.gen_request_id();
        header.id = request_id;
        header.command = Command::ListInodes;
        header.size = (MessageHeader::SIZE + body.encoded_len()) as u32;

        let mut request_bytes = BytesMut::with_capacity(header.size as usize);
        header.encode(&mut request_bytes);
        body.encode(&mut request_bytes)
            .map_err(RpcError::EncodeError)?;

        let resp_bytes = self
            .send_request(header.id, Message::Bytes(request_bytes.freeze()))
            .await
            .map_err(|e| {
                if !e.retryable() {
                    error!(rpc=%"list_inodes", %request_id, %root_blob_name, %prefix, error=?e, "nss rpc failed");
                }
                e
            })?
            .body;
        let resp: ListInodesResponse =
            PbMessage::decode(resp_bytes).map_err(RpcError::DecodeError)?;
        Ok(resp)
    }

    pub async fn delete_inode(
        &self,
        root_blob_name: &str,
        key: &str,
    ) -> Result<DeleteInodeResponse, RpcError> {
        let _guard = InflightRpcGuard::new("nss", "delete_inode");
        let mut nss_key = key.to_string();
        nss_key.push('\0');
        let body = DeleteInodeRequest {
            root_blob_name: root_blob_name.to_string(),
            key: nss_key,
        };

        let mut header = MessageHeader::default();
        let request_id = self.gen_request_id();
        header.id = request_id;
        header.command = Command::DeleteInode;
        header.size = (MessageHeader::SIZE + body.encoded_len()) as u32;

        let mut request_bytes = BytesMut::with_capacity(header.size as usize);
        header.encode(&mut request_bytes);
        body.encode(&mut request_bytes)
            .map_err(RpcError::EncodeError)?;

        let resp_bytes = self
            .send_request(header.id, Message::Bytes(request_bytes.freeze()))
            .await
            .map_err(|e| {
                if !e.retryable() {
                    error!(rpc=%"delete_inode", %request_id, %root_blob_name, %key, error=?e, "nss rpc failed");
                }
                e
            })?
            .body;
        let resp: DeleteInodeResponse =
            PbMessage::decode(resp_bytes).map_err(RpcError::DecodeError)?;
        Ok(resp)
    }

    pub async fn create_root_inode(
        &self,
        bucket: &str,
    ) -> Result<CreateRootInodeResponse, RpcError> {
        let _guard = InflightRpcGuard::new("nss", "create_root_inode");
        let body = CreateRootInodeRequest {
            bucket: bucket.to_string(),
        };

        let mut header = MessageHeader::default();
        let request_id = self.gen_request_id();
        header.id = request_id;
        header.command = Command::CreateRootInode;
        header.size = (MessageHeader::SIZE + body.encoded_len()) as u32;

        let mut request_bytes = BytesMut::with_capacity(header.size as usize);
        header.encode(&mut request_bytes);
        body.encode(&mut request_bytes)
            .map_err(RpcError::EncodeError)?;

        let resp_bytes = self
            .send_request(header.id, Message::Bytes(request_bytes.freeze()))
            .await
            .map_err(|e| {
                if !e.retryable() {
                    error!(rpc=%"create_root_inode", %request_id, %bucket, error=?e, "nss rpc failed");
                }
                e
            })?
            .body;
        let resp: CreateRootInodeResponse =
            PbMessage::decode(resp_bytes).map_err(RpcError::DecodeError)?;
        Ok(resp)
    }

    pub async fn delete_root_inode(
        &self,
        root_blob_name: &str,
    ) -> Result<DeleteRootInodeResponse, RpcError> {
        let _guard = InflightRpcGuard::new("nss", "delete_root_inode");
        let body = DeleteRootInodeRequest {
            root_blob_name: root_blob_name.to_string(),
        };

        let mut header = MessageHeader::default();
        let request_id = self.gen_request_id();
        header.id = request_id;
        header.command = Command::DeleteRootInode;
        header.size = (MessageHeader::SIZE + body.encoded_len()) as u32;

        let mut request_bytes = BytesMut::with_capacity(header.size as usize);
        header.encode(&mut request_bytes);
        body.encode(&mut request_bytes)
            .map_err(RpcError::EncodeError)?;

        let resp_bytes = self
            .send_request(header.id, Message::Bytes(request_bytes.freeze()))
            .await
            .map_err(|e| {
                if !e.retryable() {
                    error!(rpc=%"delete_root_inode", %request_id, %root_blob_name, error=?e, "nss rpc failed");
                }
                e
            })?
            .body;
        let resp: DeleteRootInodeResponse =
            PbMessage::decode(resp_bytes).map_err(RpcError::DecodeError)?;
        Ok(resp)
    }
}
