use crate::{
    message::MessageHeader,
    rpc_client::{RpcClient, RpcError},
};
use bytes::BytesMut;
use prost::Message;

include!(concat!(env!("OUT_DIR"), "/storage_server_ops.rs"));

pub async fn nss_put_blob(
    rpc_client: &RpcClient,
    key: String,
    content: String,
) -> Result<PutBlobResponse, RpcError> {
    let request_body = PutBlobRequest { key, content };

    let mut request_header = MessageHeader::default();
    request_header.id = rpc_client.gen_request_id();
    request_header.command = Command::PutBlob;
    request_header.size = (MessageHeader::encode_len() + request_body.encoded_len()) as u32;

    let mut request_bytes = BytesMut::with_capacity(request_header.size as usize);
    request_header.encode(&mut request_bytes);
    request_body
        .encode(&mut request_bytes)
        .map_err(RpcError::EncodeError)?;

    let resp_bytes = rpc_client
        .send_request(request_header.id, request_bytes.freeze())
        .await?;
    let resp: PutBlobResponse = Message::decode(resp_bytes).map_err(RpcError::DecodeError)?;
    Ok(resp)
}

pub async fn nss_get_blob(
    rpc_client: &RpcClient,
    key: String,
) -> Result<GetBlobResponse, RpcError> {
    let request_body = GetBlobRequest { key };

    let mut request_header = MessageHeader::default();
    request_header.id = rpc_client.gen_request_id();
    request_header.command = Command::GetBlob;
    request_header.size = (MessageHeader::encode_len() + request_body.encoded_len()) as u32;

    let mut request_bytes = BytesMut::with_capacity(request_header.size as usize);
    request_header.encode(&mut request_bytes);
    request_body
        .encode(&mut request_bytes)
        .map_err(RpcError::EncodeError)?;

    let resp_bytes = rpc_client
        .send_request(request_header.id, request_bytes.freeze())
        .await?;
    let resp: GetBlobResponse = Message::decode(resp_bytes).map_err(RpcError::DecodeError)?;
    Ok(resp)
}
