use crate::{
    message::MessageHeader,
    rpc_client::{Message, RpcClient, RpcError},
};
use bytes::{Bytes, BytesMut};
use prost::Message as PbMessage;

include!(concat!(env!("OUT_DIR"), "/rss_ops.rs"));

impl RpcClient {
    pub async fn put(&self, key: Bytes, value: Bytes) -> Result<Bytes, RpcError> {
        let body = PutRequest { key, value };

        let mut header = MessageHeader::default();
        header.id = self.gen_request_id();
        header.command = Command::Put;
        header.size = (MessageHeader::SIZE + body.encoded_len()) as u32;

        let mut request_bytes = BytesMut::with_capacity(header.size as usize);
        header.encode(&mut request_bytes);
        body.encode(&mut request_bytes)
            .map_err(RpcError::EncodeError)?;

        let resp_bytes = self
            .send_request(header.id, Message::Bytes(request_bytes.freeze()))
            .await?
            .body;
        let resp: PutResponse = PbMessage::decode(resp_bytes).map_err(RpcError::DecodeError)?;
        match resp.result.unwrap() {
            put_response::Result::Ok(resp) => Ok(resp),
            put_response::Result::Err(resp) => Err(RpcError::InternalResponseError(resp)),
        }
    }

    pub async fn get(&self, key: Bytes) -> Result<Bytes, RpcError> {
        let body = GetRequest { key };

        let mut header = MessageHeader::default();
        header.id = self.gen_request_id();
        header.command = Command::Get;
        header.size = (MessageHeader::SIZE + body.encoded_len()) as u32;

        let mut request_bytes = BytesMut::with_capacity(header.size as usize);
        header.encode(&mut request_bytes);
        body.encode(&mut request_bytes)
            .map_err(RpcError::EncodeError)?;

        let resp_bytes = self
            .send_request(header.id, Message::Bytes(request_bytes.freeze()))
            .await?
            .body;
        let resp: GetResponse = PbMessage::decode(resp_bytes).map_err(RpcError::DecodeError)?;
        match resp.result.unwrap() {
            get_response::Result::Ok(resp) => Ok(resp),
            get_response::Result::ErrNotFound(_resp) => Err(RpcError::NotFound),
            get_response::Result::ErrOthers(resp) => Err(RpcError::InternalResponseError(resp)),
        }
    }

    pub async fn delete(&self, key: Bytes) -> Result<Bytes, RpcError> {
        let body = DeleteRequest { key };

        let mut header = MessageHeader::default();
        header.id = self.gen_request_id();
        header.command = Command::Delete;
        header.size = (MessageHeader::SIZE + body.encoded_len()) as u32;

        let mut request_bytes = BytesMut::with_capacity(header.size as usize);
        header.encode(&mut request_bytes);
        body.encode(&mut request_bytes)
            .map_err(RpcError::EncodeError)?;

        let resp_bytes = self
            .send_request(header.id, Message::Bytes(request_bytes.freeze()))
            .await?
            .body;
        let resp: DeleteResponse = PbMessage::decode(resp_bytes).map_err(RpcError::DecodeError)?;
        match resp.result.unwrap() {
            delete_response::Result::Ok(resp) => Ok(resp),
            delete_response::Result::Err(resp) => Err(RpcError::InternalResponseError(resp)),
        }
    }
}
