use axum::{
    extract::Request,
    http::StatusCode,
    response::{IntoResponse, Result},
};
use http_body_util::BodyExt;
use rpc_client_bss::{message::MessageHeader, RpcClientBss};
use rpc_client_nss::RpcClientNss;
use uuid::Uuid;

pub async fn put_object(
    request: Request,
    key: String,
    rpc_client_nss: &RpcClientNss,
    rpc_client_bss: &RpcClientBss,
) -> Result<()> {
    // Write data at first
    // TODO: async stream
    let content = request.into_body().collect().await.unwrap().to_bytes();
    let content_len = content.len();
    let blob_id = Uuid::now_v7();

    let usize = rpc_client_bss
        .put_blob(blob_id, content)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response())?;
    assert_eq!(content_len + MessageHeader::encode_len(), usize);

    let _resp = rpc_client_nss
        .put_inode(key, blob_id.as_bytes().into())
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response())?;
    Ok(())
}
