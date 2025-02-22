use std::sync::Arc;

use crate::handler::common::response::xml::Xml;
use crate::handler::common::time;
use crate::handler::get::get_raw_object;
use crate::object_layout::{MpuState, ObjectState};
use axum::http::StatusCode;
use axum::{
    extract::{Query, Request},
    response::{self, IntoResponse, Response},
    RequestExt,
};
use bucket_tables::bucket_table::Bucket;
use rpc_client_nss::RpcClientNss;
use serde::{Deserialize, Serialize};

use crate::handler::mpu;

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct ListPartsOptions {
    max_parts: Option<u32>,
    part_number_marker: Option<usize>,
    #[serde(rename = "uploadId")]
    upload_id: String,
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
    part: Vec<Part>,
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
    size: u64,
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
    bucket: Arc<Bucket>,
    key: String,
    rpc_client_nss: &RpcClientNss,
) -> response::Result<Response> {
    let Query(opts): Query<ListPartsOptions> = request.extract_parts().await?;
    let max_parts = opts.max_parts.unwrap_or(1000);
    let upload_id = opts.upload_id;
    let object = get_raw_object(rpc_client_nss, bucket.root_blob_name.clone(), key.clone()).await?;
    if object.version_id.simple().to_string() != upload_id {
        return Err((StatusCode::BAD_REQUEST, "upload_id mismatch").into());
    }
    if ObjectState::Mpu(MpuState::Uploading) != object.state {
        return Err((StatusCode::BAD_REQUEST, "key is not in uploading state").into());
    }

    let mpu_prefix = mpu::get_part_prefix(key, 0);
    let mpus = super::list_raw_objects(
        bucket.root_blob_name.clone(),
        rpc_client_nss,
        max_parts,
        mpu_prefix,
        "".into(),
        false,
    )
    .await?;
    let mut res = ListPartsResult {
        upload_id,
        ..Default::default()
    };
    for (_key, mpu) in mpus {
        let last_modified = time::format_timestamp(mpu.timestamp);
        let mut part = Part {
            last_modified,
            ..Default::default()
        };
        if let ObjectState::Normal(obj) = mpu.state {
            part.etag = obj.etag;
            part.size = obj.size;
        }
        res.part.push(part);
    }
    Ok(Xml(res).into_response())
}
