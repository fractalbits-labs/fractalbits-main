use bytes::Bytes;
use rpc_client_common::{nss_rpc_retry, rpc_retry};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::handler::common::s3_error::S3Error;
use crate::handler::{
    common::response::xml::{Xml, XmlnsS3},
    Request,
};
use crate::{object_layout::*, AppState};
use axum::response::Response;
use bucket_tables::bucket_table::Bucket;
use rkyv::{self, api::high::to_bytes_in, rancor::Error};
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
    #[serde(rename = "@xmlns")]
    xmlns: XmlnsS3,
    bucket: String,
    key: String,
    upload_id: String,
}

pub async fn create_multipart_upload_handler(
    app: Arc<AppState>,
    _request: Request,
    bucket: &Bucket,
    key: String,
) -> Result<Response, S3Error> {
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
    let object_layout_bytes: Bytes = to_bytes_in::<_, Error>(&object_layout, Vec::new())?.into();
    let _resp = nss_rpc_retry!(
        app,
        put_inode(
            &bucket.root_blob_name,
            &key,
            object_layout_bytes.clone(),
            Some(app.config.rpc_timeout())
        )
    )
    .await?;
    let init_mpu_res = InitiateMultipartUploadResult {
        xmlns: Default::default(),
        bucket: bucket.bucket_name.clone(),
        key,
        upload_id: version_id.simple().to_string(),
    };
    Xml(init_mpu_res).try_into()
}
