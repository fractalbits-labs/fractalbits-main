use crate::{
    message::MessageHeader,
    rpc_client::{Message, RpcClient, RpcError},
};
use bytes::BytesMut;
use prost::Message as PbMessage;

include!(concat!(env!("OUT_DIR"), "/nss_ops.rs"));

impl RpcClient {
    pub async fn put_inode(
        &self,
        key: String,
        value: Vec<u8>,
    ) -> Result<PutInodeResponse, RpcError> {
        let body = PutInodeRequest { key, value };

        let mut header = MessageHeader::default();
        header.id = self.gen_request_id();
        header.command = Command::PutInode;
        header.size = (MessageHeader::encode_len() + body.encoded_len()) as u32;

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

    pub async fn get_inode(&self, key: String) -> Result<GetInodeResponse, RpcError> {
        let body = GetInodeRequest { key };

        let mut header = MessageHeader::default();
        header.id = self.gen_request_id();
        header.command = Command::GetInode;
        header.size = (MessageHeader::encode_len() + body.encoded_len()) as u32;

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
}
