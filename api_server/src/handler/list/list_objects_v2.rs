use std::sync::Arc;

use crate::handler::common::response::xml::Xml;
use axum::{
    extract::{Query, Request},
    http::StatusCode,
    response::{self, IntoResponse, Response},
    RequestExt,
};
use bucket_tables::bucket_table::Bucket;
use rkyv::{self, rancor::Error};
use rpc_client_nss::{rpc::list_inodes_response, RpcClientNss};
use serde::{Deserialize, Serialize};

use crate::object_layout::ObjectLayout;

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct ListObjectsV2Options {
    list_type: Option<String>,
    continuation_token: Option<String>,
    delimiter: Option<String>,
    encoding_type: Option<String>,
    fetch_owner: Option<bool>,
    max_keys: Option<u32>,
    prefix: Option<String>,
    start_after: Option<String>,
}

#[derive(Default, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct ListBucketResult {
    is_truncated: bool,
    contents: Vec<Contents>,
    name: String,
    prefix: String,
    delimiter: String,
    max_keys: u32,
    common_prefixes: Vec<CommonPrefixes>,
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
    bucket: Arc<Bucket>,
    rpc_client_nss: &RpcClientNss,
) -> response::Result<Response> {
    let Query(opts): Query<ListObjectsV2Options> = request.extract_parts().await?;
    tracing::debug!("list_objects_v2 {opts:?}");

    // Sanity checks
    if opts.list_type != Some("2".into()) {
        return Err((StatusCode::BAD_REQUEST, "list-type wrong").into());
    }
    if let Some(encoding_type) = opts.encoding_type {
        if encoding_type != "url" {
            return Err((StatusCode::BAD_REQUEST, "invalid encoding-type").into());
        }
    }

    let max_keys = opts.max_keys.unwrap_or(1000);
    let prefix = opts.prefix.unwrap_or("/".into());
    let start_after = opts.start_after.unwrap_or_default();
    let resp = rpc_client_nss
        .list_inodes(
            bucket.root_blob_name.clone(),
            max_keys,
            prefix,
            start_after,
            true,
        )
        .await
        .unwrap();

    // Process results
    let inodes = match resp.result.unwrap() {
        list_inodes_response::Result::Ok(res) => res.inodes,
        list_inodes_response::Result::Err(e) => {
            return Err((StatusCode::INTERNAL_SERVER_ERROR, e)
                .into_response()
                .into())
        }
    };

    for inode in inodes {
        let _object = rkyv::from_bytes::<ObjectLayout, Error>(&inode.inode).unwrap();
        // dbg!(&inode.key);
        // dbg!(object.timestamp);
        // dbg!(object.size);
    }

    Ok(Xml(ListBucketResult::default()).into_response())
}
