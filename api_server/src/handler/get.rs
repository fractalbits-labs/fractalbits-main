use axum::{
    extract::{Query, Request},
    http::StatusCode,
    response::{self, IntoResponse},
    RequestExt,
};
use bytes::Bytes;
use rpc_client_bss::RpcClientBss;
use rpc_client_nss::{rpc::get_inode_response, RpcClientNss};
use serde::Deserialize;
use uuid::Uuid;

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct GetObjectOptions {
    #[serde(rename(deserialize = "partNumber"))]
    part_number: Option<u64>,
    #[serde(rename(deserialize = "versionId"))]
    version_id: Option<String>,
    response_cache_control: Option<String>,
    response_content_disposition: Option<String>,
    response_content_encoding: Option<String>,
    response_content_language: Option<String>,
    response_content_type: Option<String>,
    response_expires: Option<String>,
}

pub async fn get_object(
    mut request: Request,
    key: String,
    rpc_client_nss: &RpcClientNss,
    rpc_client_bss: &RpcClientBss,
) -> response::Result<Bytes> {
    let Query(_opts): Query<GetObjectOptions> = request.extract_parts().await?;
    let resp = rpc_client_nss
        .get_inode(key)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response())?;

    let blob_id = match resp.result.unwrap() {
        get_inode_response::Result::Ok(res) => Uuid::try_from(res).unwrap(),
        get_inode_response::Result::Err(e) => {
            return Err((StatusCode::INTERNAL_SERVER_ERROR, e)
                .into_response()
                .into())
        }
    };

    let mut content = Bytes::new();
    let _size = rpc_client_bss
        .get_blob(blob_id, &mut content)
        .await
        .unwrap();
    Ok(content)
}
