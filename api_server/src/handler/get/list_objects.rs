use crate::handler::{
    common::{response::xml::Xml, s3_error::S3Error, time::format_timestamp},
    Request,
};
use axum::{extract::Query, response::Response, RequestPartsExt};
use bucket_tables::bucket_table::Bucket;
use rkyv::{self, rancor::Error};
use rpc_client_nss::{rpc::list_inodes_response, RpcClientNss};
use serde::{Deserialize, Serialize};

use crate::object_layout::ObjectLayout;

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct ListObjectsOptions {
    delimiter: Option<String>,
    encoding_type: Option<String>,
    marker: Option<String>,
    max_keys: Option<u32>,
    prefix: Option<String>,
}

#[derive(Default, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct ListBucketResult {
    is_truncated: bool,
    marker: String,
    next_marker: String,
    contents: Vec<Contents>,
    name: String,
    prefix: String,
    delimiter: String,
    max_keys: u32,
    common_prefixes: Vec<CommonPrefixes>,
    encoding_type: String,
}

impl ListBucketResult {
    fn contents(self, contents: Vec<Contents>) -> Self {
        Self { contents, ..self }
    }

    fn bucket_name(self, bucket_name: String) -> Self {
        Self {
            name: bucket_name,
            ..self
        }
    }

    fn prefix(self, prefix: String) -> Self {
        Self { prefix, ..self }
    }

    fn max_keys(self, max_keys: u32) -> Self {
        Self { max_keys, ..self }
    }
}

#[derive(Default, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct Contents {
    checksum_algorithm: String,
    checksum_type: String,
    etag: String,
    key: String,
    last_modified: String, // timestamp
    owner: Owner,
    restore_status: Option<RestoreStatus>,
    size: u64,
    storage_class: String,
}

impl Contents {
    fn from_obj_and_key(obj: ObjectLayout, key: String) -> Result<Self, S3Error> {
        Ok(Self {
            key,
            last_modified: format_timestamp(obj.timestamp),
            etag: "bf1d737a4d46a19f3bced6905cc8b902".into(), //obj.etag(),
            size: obj.size()?,
            storage_class: "STANDARD".into(),
            ..Default::default()
        })
    }
}

#[derive(Default, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct Owner {
    display_name: String,
    id: String,
}

#[allow(dead_code)]
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

pub async fn list_objects(
    request: Request,
    bucket: &Bucket,
    rpc_client_nss: &RpcClientNss,
) -> Result<Response, S3Error> {
    let Query(opts): Query<ListObjectsOptions> = request.into_parts().0.extract().await?;
    tracing::debug!("list_objects {opts:?}");

    if let Some(encoding_type) = opts.encoding_type {
        if encoding_type != "url" {
            tracing::warn!(
                "expecting content_type as \"url\" only, got {}",
                encoding_type
            );
            return Err(S3Error::InvalidArgument1);
        }
    }

    let max_keys = opts.max_keys.unwrap_or(1000);
    let prefix = opts.prefix.unwrap_or("/".into());
    let resp = rpc_client_nss
        .list_inodes(
            bucket.root_blob_name.clone(),
            max_keys,
            prefix.clone(),
            "".into(),
            true,
        )
        .await?;

    // Process results
    let inodes = match resp.result.unwrap() {
        list_inodes_response::Result::Ok(res) => res.inodes,
        list_inodes_response::Result::Err(e) => {
            tracing::error!(e);
            return Err(S3Error::InternalError);
        }
    };

    let contents = inodes
        .iter()
        .map(|x| {
            match rkyv::from_bytes::<ObjectLayout, Error>(&x.inode) {
                Err(e) => Err(e.into()),
                Ok(obj) => {
                    let mut key = x.key.clone();
                    key.pop(); // removing nss's trailing '\0'
                    Contents::from_obj_and_key(obj, key)
                }
            }
        })
        .collect::<Result<Vec<Contents>, S3Error>>()?;

    Xml(ListBucketResult::default()
        .contents(contents)
        .bucket_name(bucket.bucket_name.clone())
        .prefix(prefix)
        .max_keys(max_keys))
    .try_into()
}
