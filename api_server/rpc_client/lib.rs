use bytes::BytesMut;
use prost::Message;
use rpc_client::{
    message::MessageHeader,
    nss_ops::*,
    rpc_client::{RpcClient, RpcError},
};

pub async fn nss_put_inode(
    rpc_client: &RpcClient,
    key: String,
    value: String,
) -> Result<PutInodeResponse, RpcError> {
    let request_body = PutInodeRequest { key, value };

    let mut request_header = MessageHeader::default();
    request_header.id = rpc_client.gen_request_id();
    request_header.command = Command::PutInode;
    request_header.size = (MessageHeader::encode_len() + request_body.encoded_len()) as u32;

    let mut request_bytes = BytesMut::with_capacity(request_header.size as usize);
    request_header.encode(&mut request_bytes);
    request_body
        .encode(&mut request_bytes)
        .map_err(RpcError::EncodeError)?;

    let resp_bytes = rpc_client
        .send_request(request_header.id, request_bytes.freeze())
        .await?;
    let resp: PutInodeResponse = Message::decode(resp_bytes).map_err(RpcError::DecodeError)?;
    Ok(resp)
}

pub async fn nss_get_inode(
    rpc_client: &RpcClient,
    key: String,
) -> Result<GetInodeResponse, RpcError> {
    let request_body = GetInodeRequest { key };

    let mut request_header = MessageHeader::default();
    request_header.id = rpc_client.gen_request_id();
    request_header.command = Command::GetInode;
    request_header.size = (MessageHeader::encode_len() + request_body.encoded_len()) as u32;

    let mut request_bytes = BytesMut::with_capacity(request_header.size as usize);
    request_header.encode(&mut request_bytes);
    request_body
        .encode(&mut request_bytes)
        .map_err(RpcError::EncodeError)?;

    let resp_bytes = rpc_client
        .send_request(request_header.id, request_bytes.freeze())
        .await?;
    let resp: GetInodeResponse = Message::decode(resp_bytes).map_err(RpcError::DecodeError)?;
    Ok(resp)
}
