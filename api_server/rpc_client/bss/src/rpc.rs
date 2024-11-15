use crate::{
    codec::MessageFrame,
    message::{Command, MessageHeader},
    rpc_client::{Message, RpcClient, RpcError},
};
use bytes::Bytes;
use uuid::Uuid;

impl RpcClient {
    pub async fn put_blob(&self, blob_id: Uuid, body: Bytes) -> Result<usize, RpcError> {
        let mut header = MessageHeader::default();
        header.id = self.gen_request_id();
        header.blob_id = blob_id.into_bytes();
        header.command = Command::PutBlob;
        header.size = (MessageHeader::encode_len() + body.len()) as u64;

        let msg_frame = MessageFrame::new(header, body);
        let resp = self
            .send_request(header.id, Message::Frame(msg_frame))
            .await?;
        Ok(resp.header.result as usize)
    }

    pub async fn get_blob(&self, blob_id: Uuid, body: &mut Bytes) -> Result<usize, RpcError> {
        let mut header = MessageHeader::default();
        header.id = self.gen_request_id();
        header.blob_id = blob_id.into_bytes();
        header.command = Command::GetBlob;
        header.size = MessageHeader::encode_len() as u64;

        let msg_frame = MessageFrame::new(header, Bytes::new());
        let resp = self
            .send_request(header.id, Message::Frame(msg_frame))
            .await?;
        let size = resp.header.result;
        *body = resp.body;
        Ok(size as usize)
    }
}
