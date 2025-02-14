use std::sync::Arc;

use crate::{bucket_tables::bucket_table::Bucket, response::xml::Xml};
use axum::{
    extract::Request,
    response::{self, IntoResponse, Response},
};
use rpc_client_nss::RpcClientNss;
use serde::{Deserialize, Serialize};

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct GetObjectAttributesOptions {
    #[serde(rename(deserialize = "versionId"))]
    version_id: Option<String>,
    response_cache_control: Option<String>,
    response_content_disposition: Option<String>,
    response_content_encoding: Option<String>,
    response_content_language: Option<String>,
    response_content_type: Option<String>,
    response_expires: Option<String>,
}

#[derive(Default, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct GetObjectAttributesOutput {
    etag: String,
    checksum: CheckSum,
    object_parts: ObjectParts,
    storage_class: String,
    object_size: usize,
}

#[derive(Default, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct CheckSum {
    checksum_crc32: String,
    checksum_crc32c: String,
    checksum_sha1: String,
    checksum_sha256: String,
}

#[derive(Default, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct ObjectParts {
    is_truncated: bool,
    max_parts: usize,
    next_part_number_marker: usize,
    part_number_marker: usize,
    part: Part,
    parts_count: usize,
}

#[derive(Default, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct Part {
    checksum_crc32: String,
    checksum_crc32c: String,
    checksum_sha1: String,
    checksum_sha256: String,
    part_number: usize,
    size: usize,
}

#[allow(dead_code)]
#[derive(Default, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
struct ResponseHeaders {
    #[serde(rename = "Last-Modified")]
    last_modified: Option<String>, // timestamp
    x_amz_delete_marker: Option<String>,
    x_amz_request_charged: Option<String>,
    x_amz_version_id: Option<String>,
}

pub async fn get_object_attributes(
    _request: Request,
    _bucket: Arc<Bucket>,
    _key: String,
    _rpc_client_nss: &RpcClientNss,
) -> response::Result<Response> {
    Ok(Xml(GetObjectAttributesOutput::default()).into_response())
}
