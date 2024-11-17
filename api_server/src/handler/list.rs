use crate::response_xml::Xml;
use axum::{
    extract::{Query, Request},
    http::StatusCode,
    response::{self, IntoResponse, Response},
    RequestExt,
};
use rpc_client_nss::RpcClientNss;
use serde::{Deserialize, Serialize};

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct ListObjectsV2Options {
    list_type: Option<String>,
    continuation_token: Option<String>,
    delimiter: Option<String>,
    encoding_type: Option<String>,
    fetch_owner: Option<bool>,
    max_keys: Option<usize>,
    prefix: Option<String>,
    start_after: Option<String>,
}

#[derive(Default, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct ListBucketResult {
    is_truncated: bool,
    contents: Contents,
    name: String,
    prefix: String,
    delimiter: String,
    max_keys: usize,
    common_prefixes: CommonPrefixes,
    encoding_type: String,
    key_count: usize,
    continuation_token: String,
    next_continuation_token: String,
    start_after: String,
}

#[derive(Default, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct Contents {
    checksum_algorithm: String,
    etag: String,
    key: String,
    last_modified: String, // timestamp
    owner: Owner,
    restore_status: RestoreStatus,
    size: usize,
    storage_class: String,
}

#[derive(Default, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct Owner {
    display_name: String,
    id: String,
}

#[derive(Default, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct RestoreStatus {
    is_restore_in_progress: bool,
    restore_expiry_date: String, // timestamp
}

#[derive(Default, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct CommonPrefixes {
    prefix: String,
}

pub async fn list_objects_v2(
    mut request: Request,
    _rpc_client_nss: &RpcClientNss,
) -> response::Result<Response> {
    let Query(opts): Query<ListObjectsV2Options> = request.extract_parts().await?;
    if opts.list_type != Some("2".into()) {
        return Err((StatusCode::BAD_REQUEST, "list-type wrong").into());
    }
    Ok(Xml(ListBucketResult::default()).into_response())
}
