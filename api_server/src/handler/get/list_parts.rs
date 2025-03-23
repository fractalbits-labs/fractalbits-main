use crate::handler::common::{
    get_raw_object, list_raw_objects, mpu_get_part_prefix, response::xml::Xml, s3_error::S3Error,
    time,
};
use crate::object_layout::{MpuState, ObjectState};
use axum::{extract::Query, response::Response, RequestPartsExt};
use bucket_tables::bucket_table::Bucket;
use rpc_client_nss::RpcClientNss;
use serde::{Deserialize, Serialize};

use crate::handler::Request;

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
    #[serde(rename = "ID")]
    id: String,
}

#[derive(Default, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct Owner {
    display_name: String,
    id: String,
}

pub async fn list_parts_handler(
    request: Request,
    bucket: &Bucket,
    key: String,
    rpc_client_nss: &RpcClientNss,
) -> Result<Response, S3Error> {
    let Query(opts): Query<ListPartsOptions> = request.into_parts().0.extract().await?;
    let max_parts = opts.max_parts.unwrap_or(1000);
    let upload_id = opts.upload_id;
    let object = get_raw_object(rpc_client_nss, bucket.root_blob_name.clone(), key.clone()).await?;
    if object.version_id.simple().to_string() != upload_id {
        return Err(S3Error::NoSuchUpload);
    }
    if ObjectState::Mpu(MpuState::Uploading) != object.state {
        return Err(S3Error::InvalidObjectState);
    }

    let mpu_prefix = mpu_get_part_prefix(key, 0);
    let mpus = list_raw_objects(
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
    Xml(res).try_into()
}
