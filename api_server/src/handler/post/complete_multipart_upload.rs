use std::collections::HashSet;

use crate::{
    handler::{
        common::{
            get_raw_object, list_raw_objects, mpu_get_part_prefix, mpu_parse_part_number,
            response::xml::Xml, s3_error::S3Error,
        },
        delete::delete_object,
        Request,
    },
    object_layout::{MpuState, ObjectState},
    BlobId,
};
use axum::response::Response;
use bucket_tables::bucket_table::Bucket;
use bytes::Buf;
use rkyv::{self, api::high::to_bytes_in, rancor::Error};
use rpc_client_nss::{rpc::put_inode_response, RpcClientNss};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::Sender;

#[derive(Default, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct CompleteMultipartUpload {
    part: Vec<Part>,
}

#[derive(Default, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct Part {
    #[serde(default)]
    checksum_crc32: String,
    #[serde(default)]
    checksum_crc32c: String,
    #[serde(default)]
    checksum_sha1: String,
    #[serde(default)]
    checksum_sha256: String,
    #[serde(rename = "ETag")]
    etag: String,
    part_number: u32,
}

#[derive(Default, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct CompleteMultipartUploadResult {
    location: String,
    bucket: String,
    key: String,
    #[serde(rename = "ETag")]
    etag: String,
    checksum_crc32: String,
    checksum_crc32c: String,
    checksum_sha1: String,
    checksum_sha256: String,
}

pub async fn complete_multipart_upload(
    request: Request,
    bucket: &Bucket,
    mut key: String,
    upload_id: String,
    rpc_client_nss: &RpcClientNss,
    blob_deletion: Sender<(BlobId, usize)>,
) -> Result<Response, S3Error> {
    let body = request.into_body().collect().await.unwrap();
    let req_body: CompleteMultipartUpload = quick_xml::de::from_reader(body.reader())?;
    let mut valid_part_numbers: HashSet<u32> =
        req_body.part.iter().map(|part| part.part_number).collect();

    let mut object =
        get_raw_object(rpc_client_nss, bucket.root_blob_name.clone(), key.clone()).await?;
    if object.version_id.simple().to_string() != upload_id {
        return Err(S3Error::NoSuchVersion);
    }
    if ObjectState::Mpu(MpuState::Uploading) != object.state {
        return Err(S3Error::InvalidObjectState);
    }

    let max_parts = 10000;
    let mpu_prefix = mpu_get_part_prefix(key.clone(), 0);
    let objs = list_raw_objects(
        bucket.root_blob_name.clone(),
        rpc_client_nss,
        max_parts,
        mpu_prefix.clone(),
        "".into(),
        false,
    )
    .await?;

    let mut total_size = 0;
    let mut invalid_part_keys = HashSet::new();
    for (mpu_key, mpu_obj) in objs.iter() {
        let part_number = mpu_parse_part_number(mpu_key, &key);
        if !valid_part_numbers.remove(&part_number) {
            invalid_part_keys.insert(mpu_key.clone());
        } else {
            total_size += mpu_obj.size()?;
        }
    }
    if !valid_part_numbers.is_empty() {
        return Err(S3Error::InvalidPart);
    }
    for mpu_key in invalid_part_keys.iter() {
        delete_object(
            bucket,
            mpu_key.clone(),
            rpc_client_nss,
            blob_deletion.clone(),
        )
        .await?;
    }

    object.state = ObjectState::Mpu(MpuState::Completed {
        size: total_size,
        etag: upload_id.clone(),
    });
    let new_object_bytes = to_bytes_in::<_, Error>(&object, Vec::new())?;
    let resp = rpc_client_nss
        .put_inode(
            bucket.root_blob_name.clone(),
            key.clone(),
            new_object_bytes.into(),
        )
        .await?;
    match resp.result.unwrap() {
        put_inode_response::Result::Ok(_) => {}
        put_inode_response::Result::Err(e) => {
            tracing::error!(e);
            return Err(S3Error::InternalError);
        }
    };

    let mut resp = CompleteMultipartUploadResult::default();
    key.pop();
    resp.bucket = bucket.bucket_name.clone();
    resp.key = key;
    resp.etag = upload_id;
    Xml(resp).try_into()
}
