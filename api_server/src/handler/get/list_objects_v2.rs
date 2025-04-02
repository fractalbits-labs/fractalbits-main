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
struct QueryOpts {
    list_type: Option<String>,
    continuation_token: Option<String>,
    delimiter: Option<String>,
    encoding_type: Option<String>,
    fetch_owner: Option<bool>,
    max_keys: Option<u32>,
    prefix: Option<String>,
    start_after: Option<String>,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct ListBucketResult {
    is_truncated: bool,
    contents: Vec<Object>,
    name: String,
    prefix: String,
    delimiter: String,
    max_keys: u32,
    common_prefixes: Vec<CommonPrefixes>,
    encoding_type: String,
    key_count: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    continuation_token: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    next_continuation_token: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    start_after: Option<String>,
}

impl Default for ListBucketResult {
    fn default() -> Self {
        Self {
            is_truncated: false,
            contents: Default::default(),
            name: Default::default(),
            prefix: Default::default(),
            delimiter: "/".into(),
            max_keys: 1000,
            common_prefixes: Default::default(),
            encoding_type: "url".into(),
            key_count: Default::default(),
            continuation_token: Default::default(),
            next_continuation_token: Default::default(),
            start_after: Default::default(),
        }
    }
}

impl ListBucketResult {
    fn contents(self, contents: Vec<Object>) -> Self {
        Self {
            key_count: contents.len(),
            contents,
            ..self
        }
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
pub struct Object {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub checksum_algorithm: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub checksum_type: Option<String>,
    #[serde(rename = "ETag")]
    pub etag: String,
    pub key: String,
    pub last_modified: String, // timestamp
    #[serde(skip_serializing_if = "Option::is_none")]
    pub owner: Option<Owner>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub restore_status: Option<RestoreStatus>,
    pub size: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub storage_class: Option<String>,
}

impl Object {
    pub(crate) fn from_layout_and_key(obj: ObjectLayout, key: String) -> Result<Self, S3Error> {
        Ok(Self {
            key,
            last_modified: format_timestamp(obj.timestamp),
            etag: obj.etag()?,
            size: obj.size()?,
            ..Default::default()
        })
    }
}

#[derive(Default, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
pub struct Owner {
    pub display_name: String,
    pub id: String,
}

#[allow(dead_code)]
#[derive(Default, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
pub struct RestoreStatus {
    pub is_restore_in_progress: bool,
    pub restore_expiry_date: String, // timestamp
}

#[derive(Default, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct CommonPrefixes {
    prefix: String,
}

pub async fn list_objects_v2_handler(
    request: Request,
    bucket: &Bucket,
    rpc_client_nss: &RpcClientNss,
) -> Result<Response, S3Error> {
    let Query(opts): Query<QueryOpts> = request.into_parts().0.extract().await?;
    tracing::debug!("list_objects_v2 {opts:?}");

    // Sanity checks
    if opts.list_type != Some("2".into()) {
        tracing::warn!(
            "expecting list_type as \"2\" only, got {:?}",
            opts.list_type
        );
        return Err(S3Error::InvalidArgument1);
    }
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
    let start_after = opts.start_after.unwrap_or_default();
    let resp = rpc_client_nss
        .list_inodes(
            bucket.root_blob_name.clone(),
            max_keys,
            prefix.clone(),
            start_after,
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
                    Object::from_layout_and_key(obj, key)
                }
            }
        })
        .collect::<Result<Vec<Object>, S3Error>>()?;

    Xml(ListBucketResult::default()
        .contents(contents)
        .bucket_name(bucket.bucket_name.clone())
        .prefix(prefix)
        .max_keys(max_keys))
    .try_into()
}
