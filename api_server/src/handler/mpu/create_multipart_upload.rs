use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::response::xml::Xml;
use crate::{bucket_tables::bucket_table::Bucket, object_layout::*};
use axum::{
    extract::Request,
    http::StatusCode,
    response::{self, IntoResponse, Response},
};
use rkyv::{self, api::high::to_bytes_in, rancor::Error};
use rpc_client_nss::RpcClientNss;
use serde::Serialize;

#[allow(dead_code)]
#[derive(Default, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
struct ResponseHeaders {
    x_amz_abort_date: String,
    x_amz_abort_rule_id: String,
    x_amz_server_side_encryption: String,
    x_amz_server_side_encryption_customer_algorithm: String,
    #[serde(rename = "x-amz-server-side-encryption-customer-key-MD5")]
    x_amz_server_side_encryption_customer_key_md5: String,
    x_amz_server_side_encryption_aws_kms_key_id: String,
    x_amz_server_side_encryption_context: String,
    x_amz_server_side_encryption_bucket_key_enabled: String,
    x_amz_request_charged: String,
    x_amz_checksum_algorithm: String,
}

#[derive(Default, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct InitiateMultipartUploadResult {
    bucket: String,
    key: String,
    upload_id: String,
}

pub async fn create_multipart_upload(
    _request: Request,
    bucket: Arc<Bucket>,
    mut key: String,
    rpc_client_nss: &RpcClientNss,
) -> response::Result<Response> {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let version_id = gen_version_id();
    let object_layout = ObjectLayout {
        version_id,
        block_size: ObjectLayout::DEFAULT_BLOCK_SIZE,
        timestamp,
        state: ObjectState::Mpu(MpuState::Uploading),
    };
    let object_layout_bytes = to_bytes_in::<_, Error>(&object_layout, Vec::new()).unwrap();
    let _resp = rpc_client_nss
        .put_inode(
            bucket.root_blob_name.clone(),
            key.clone(),
            object_layout_bytes.into(),
        )
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response())?;
    key.pop();
    let init_mpu_res = InitiateMultipartUploadResult {
        bucket: bucket.bucket_name.clone(),
        key,
        upload_id: version_id.simple().to_string(),
    };
    Ok(Xml(init_mpu_res).into_response())
}
