use axum::{
    extract::Request, http::HeaderValue, http::StatusCode, response, response::IntoResponse,
};
use rpc_client_bss::RpcClientBss;
use rpc_client_nss::RpcClientNss;
use serde::Serialize;
use tokio::sync::mpsc::Sender;

use crate::handler::put::put_object;
use crate::BlobId;

#[allow(dead_code)]
#[derive(Default, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
struct ResponseHeaders {
    x_amz_server_side_encryption: String,
    #[serde(rename = "ETag")]
    etag: String,
    x_amz_checksum_crc32: String,
    x_amz_checksum_crc32c: String,
    x_amz_checksum_sha1: String,
    x_amz_checksum_sha256: String,
    x_amz_server_side_encryption_customer_algorithm: String,
    #[serde(rename = "x-amz-server-side-encryption-customer-key-MD5")]
    x_amz_server_side_encryption_customer_key_md5: String,
    x_amz_server_side_encryption_aws_kms_key_id: String,
    x_amz_server_side_encryption_bucket_key_enabled: String,
    x_amz_request_charged: String,
}

pub async fn upload_part(
    request: Request,
    key: String,
    part_number: u64,
    upload_id: String,
    rpc_client_nss: &RpcClientNss,
    rpc_client_bss: &RpcClientBss,
    blob_deletion: Sender<(BlobId, usize)>,
) -> response::Response {
    if !(1..=10_000).contains(&part_number) {
        return (StatusCode::BAD_REQUEST, "invalid part number").into_response();
    }
    // TODO: check upload_id

    let mut key = super::get_part_prefix(key, part_number);
    key.push('\0');
    put_object(request, key, rpc_client_nss, rpc_client_bss, blob_deletion)
        .await
        .unwrap();

    let mut resp = response::Response::default();
    let etag = format!("{upload_id}{part_number}");
    resp.headers_mut()
        .insert("ETag", HeaderValue::from_str(&etag).unwrap());
    resp
}
