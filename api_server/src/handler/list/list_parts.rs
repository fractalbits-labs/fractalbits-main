use crate::response_xml::Xml;
use axum::{
    extract::{Query, Request},
    response::{self, IntoResponse, Response},
    RequestExt,
};
use rpc_client_nss::RpcClientNss;
use serde::{Deserialize, Serialize};

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct ListPartsOptions {
    max_parts: Option<usize>,
    part_number_marker: Option<usize>,
    upload_id: Option<String>,
}

#[derive(Default, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct ListPartsResult {
    bucket: String,
    key: String,
    upload_id: String,
    part_number_marker: usize,
    next_part_number_marker: usize,
    max_parts: usize,
    is_truncated: bool,
    part: Part,
    initiator: Initiator,
    owner: Owner,
    storage_class: String,
    checksum_algorithm: String,
}

#[derive(Default, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct Part {
    checksum_crc32: String,
    checksum_crc32c: String,
    checksum_sha1: String,
    checksum_sha256: String,
    etag: String,
    last_modified: String, // timestamp
    part_number: usize,
    size: usize,
}

#[derive(Default, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct Initiator {
    display_name: String,
    id: String,
}

#[derive(Default, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct Owner {
    display_name: String,
    id: String,
}

pub async fn list_parts(
    mut request: Request,
    _key: String,
    _rpc_client_nss: &RpcClientNss,
) -> response::Result<Response> {
    let Query(_opts): Query<ListPartsOptions> = request.extract_parts().await?;
    Ok(Xml(ListPartsResult::default()).into_response())
}
