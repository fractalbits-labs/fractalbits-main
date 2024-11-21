use std::time::{SystemTime, UNIX_EPOCH};

use crate::object_layout::ObjectLayout;
use axum::{
    extract::Request,
    http::StatusCode,
    response::{IntoResponse, Result},
};
use http_body_util::BodyExt;
use rkyv::{self, api::high::to_bytes_in, rancor::Error};
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

    let size = rpc_client_bss
        .put_blob(blob_id, content)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response())?;
    assert_eq!(content_len + MessageHeader::encode_len(), size);

    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let object_layout = ObjectLayout {
        blob_id,
        timestamp,
        size: size as u64,
    };
    let object_layout_bytes = to_bytes_in::<_, Error>(&object_layout, Vec::new()).unwrap();
    let _resp = rpc_client_nss
        .put_inode(key, object_layout_bytes.into())
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response())?;
    Ok(())
}
