pub mod codec;
pub mod message;
pub mod rpc_client;

use crate::{
    message::{Command, MessageHeader},
    rpc_client::{RpcClient, RpcError},
};
use bytes::{Bytes, BytesMut};
use uuid::Uuid;

pub async fn nss_put_blob(
    rpc_client: &RpcClient,
    blob_id: Uuid,
    content: Bytes,
) -> Result<usize, RpcError> {
    let mut request_header = MessageHeader::default();
    request_header.id = rpc_client.gen_request_id();
    request_header.blob_id = blob_id.into_bytes();
    request_header.command = Command::PutBlob;
    request_header.size = (MessageHeader::encode_len() + content.len()) as u64;

    let mut header_bytes = BytesMut::with_capacity(MessageHeader::encode_len());
    request_header.encode(&mut header_bytes);
    let msgs = vec![header_bytes.freeze(), content];

    let resp = rpc_client.send_request(request_header.id, &msgs).await?;
    Ok(resp.header.result as usize)
}

pub async fn nss_get_blob(
    rpc_client: &RpcClient,
    blob_id: Uuid,
    content: &mut Bytes,
) -> Result<usize, RpcError> {
    let mut request_header = MessageHeader::default();
    request_header.id = rpc_client.gen_request_id();
    request_header.blob_id = blob_id.into_bytes();
    request_header.command = Command::GetBlob;
    request_header.size = MessageHeader::encode_len() as u64;

    let mut request_bytes = BytesMut::with_capacity(request_header.size as usize);
    request_header.encode(&mut request_bytes);

    let resp = rpc_client
        .send_request(request_header.id, &vec![request_bytes.freeze()])
        .await?;
    let size = resp.header.result;
    *content = resp.body;
    Ok(size as usize)
}
