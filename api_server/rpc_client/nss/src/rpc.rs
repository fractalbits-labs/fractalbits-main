use crate::{
    message::MessageHeader,
    rpc_client::{InflightRpcGuard, Message, RpcClient, RpcError},
};
use bytes::{Bytes, BytesMut};
use prost::Message as PbMessage;

include!(concat!(env!("OUT_DIR"), "/nss_ops.rs"));

impl RpcClient {
    pub async fn put_inode(
        &self,
        root_blob_name: String,
        mut key: String,
        value: Bytes,
    ) -> Result<PutInodeResponse, RpcError> {
        let _guard = InflightRpcGuard::new("nss", "put_inode");
        key.push('\0');
        let body = PutInodeRequest {
            root_blob_name,
            key,
            value,
        };

        let mut header = MessageHeader::default();
        header.id = self.gen_request_id();
        header.command = Command::PutInode;
        header.size = (MessageHeader::SIZE + body.encoded_len()) as u32;

        let mut request_bytes = BytesMut::with_capacity(header.size as usize);
        header.encode(&mut request_bytes);
        body.encode(&mut request_bytes)
            .map_err(RpcError::EncodeError)?;

        let resp_bytes = self
            .send_request(header.id, Message::Bytes(request_bytes.freeze()))
            .await?
            .body;
        let resp: PutInodeResponse =
            PbMessage::decode(resp_bytes).map_err(RpcError::DecodeError)?;
        Ok(resp)
    }

    pub async fn get_inode(
        &self,
        root_blob_name: String,
        mut key: String,
    ) -> Result<GetInodeResponse, RpcError> {
        let _guard = InflightRpcGuard::new("nss", "get_inode");
        key.push('\0');
        let body = GetInodeRequest {
            root_blob_name,
            key,
        };

        let mut header = MessageHeader::default();
        header.id = self.gen_request_id();
        header.command = Command::GetInode;
        header.size = (MessageHeader::SIZE + body.encoded_len()) as u32;

        let mut request_bytes = BytesMut::with_capacity(header.size as usize);
        header.encode(&mut request_bytes);
        body.encode(&mut request_bytes)
            .map_err(RpcError::EncodeError)?;

        let resp_bytes = self
            .send_request(header.id, Message::Bytes(request_bytes.freeze()))
            .await?
            .body;
        let resp: GetInodeResponse =
            PbMessage::decode(resp_bytes).map_err(RpcError::DecodeError)?;
        Ok(resp)
    }

    pub async fn list_inodes(
        &self,
        root_blob_name: String,
        max_keys: u32,
        prefix: String,
        delimiter: String,
        mut start_after: String,
        skip_mpu_parts: bool,
    ) -> Result<ListInodesResponse, RpcError> {
        let _guard = InflightRpcGuard::new("nss", "list_inodes");
        if !start_after.ends_with("/") {
            start_after.push('\0');
        }
        let body = ListInodesRequest {
            root_blob_name,
            max_keys,
            prefix,
            delimiter,
            start_after,
            skip_mpu_parts,
        };

        let mut header = MessageHeader::default();
        header.id = self.gen_request_id();
        header.command = Command::ListInodes;
        header.size = (MessageHeader::SIZE + body.encoded_len()) as u32;

        let mut request_bytes = BytesMut::with_capacity(header.size as usize);
        header.encode(&mut request_bytes);
        body.encode(&mut request_bytes)
            .map_err(RpcError::EncodeError)?;

        let resp_bytes = self
            .send_request(header.id, Message::Bytes(request_bytes.freeze()))
            .await?
            .body;
        let resp: ListInodesResponse =
            PbMessage::decode(resp_bytes).map_err(RpcError::DecodeError)?;
        Ok(resp)
    }

    pub async fn delete_inode(
        &self,
        root_blob_name: String,
        mut key: String,
    ) -> Result<DeleteInodeResponse, RpcError> {
        let _guard = InflightRpcGuard::new("nss", "delete_inode");
        key.push('\0');
        let body = DeleteInodeRequest {
            root_blob_name,
            key,
        };

        let mut header = MessageHeader::default();
        header.id = self.gen_request_id();
        header.command = Command::DeleteInode;
        header.size = (MessageHeader::SIZE + body.encoded_len()) as u32;

        let mut request_bytes = BytesMut::with_capacity(header.size as usize);
        header.encode(&mut request_bytes);
        body.encode(&mut request_bytes)
            .map_err(RpcError::EncodeError)?;

        let resp_bytes = self
            .send_request(header.id, Message::Bytes(request_bytes.freeze()))
            .await?
            .body;
        let resp: DeleteInodeResponse =
            PbMessage::decode(resp_bytes).map_err(RpcError::DecodeError)?;
        Ok(resp)
    }

    pub async fn create_root_inode(
        &self,
        bucket: String,
    ) -> Result<CreateRootInodeResponse, RpcError> {
        let _guard = InflightRpcGuard::new("nss", "create_root_inode");
        let body = CreateRootInodeRequest { bucket };

        let mut header = MessageHeader::default();
        header.id = self.gen_request_id();
        header.command = Command::CreateRootInode;
        header.size = (MessageHeader::SIZE + body.encoded_len()) as u32;

        let mut request_bytes = BytesMut::with_capacity(header.size as usize);
        header.encode(&mut request_bytes);
        body.encode(&mut request_bytes)
            .map_err(RpcError::EncodeError)?;

        let resp_bytes = self
            .send_request(header.id, Message::Bytes(request_bytes.freeze()))
            .await?
            .body;
        let resp: CreateRootInodeResponse =
            PbMessage::decode(resp_bytes).map_err(RpcError::DecodeError)?;
        Ok(resp)
    }

    pub async fn delete_root_inode(
        &self,
        root_blob_name: String,
    ) -> Result<DeleteRootInodeResponse, RpcError> {
        let _guard = InflightRpcGuard::new("nss", "delete_root_inode");
        let body = DeleteRootInodeRequest { root_blob_name };

        let mut header = MessageHeader::default();
        header.id = self.gen_request_id();
        header.command = Command::DeleteRootInode;
        header.size = (MessageHeader::SIZE + body.encoded_len()) as u32;

        let mut request_bytes = BytesMut::with_capacity(header.size as usize);
        header.encode(&mut request_bytes);
        body.encode(&mut request_bytes)
            .map_err(RpcError::EncodeError)?;

        let resp_bytes = self
            .send_request(header.id, Message::Bytes(request_bytes.freeze()))
            .await?
            .body;
        let resp: DeleteRootInodeResponse =
            PbMessage::decode(resp_bytes).map_err(RpcError::DecodeError)?;
        Ok(resp)
    }
}
