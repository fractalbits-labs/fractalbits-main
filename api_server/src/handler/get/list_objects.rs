use crate::handler::{
    common::{response::xml::Xml, s3_error::S3Error},
    Request,
};
use axum::{extract::Query, response::Response, RequestPartsExt};
use bucket_tables::bucket_table::Bucket;
use rkyv::{self, rancor::Error};
use rpc_client_nss::{rpc::list_inodes_response, RpcClientNss};
use serde::{Deserialize, Serialize};

use super::Object;
use crate::object_layout::ObjectLayout;

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct QueryOpts {
    delimiter: Option<String>,
    encoding_type: Option<String>,
    marker: Option<String>,
    max_keys: Option<u32>,
    prefix: Option<String>,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct ListBucketResult {
    is_truncated: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    marker: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    next_marker: Option<String>,
    contents: Vec<Object>,
    name: String,
    prefix: String,
    delimiter: String,
    max_keys: u32,
    common_prefixes: Vec<CommonPrefixes>,
    encoding_type: String,
}

impl Default for ListBucketResult {
    fn default() -> Self {
        Self {
            is_truncated: false,
            marker: Default::default(),
            next_marker: Default::default(),
            contents: Default::default(),
            name: Default::default(),
            prefix: Default::default(),
            delimiter: "/".into(),
            max_keys: 1000,
            common_prefixes: Default::default(),
            encoding_type: "url".into(),
        }
    }
}
impl ListBucketResult {
    fn contents(self, contents: Vec<Object>) -> Self {
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
struct CommonPrefixes {
    prefix: String,
}

pub async fn list_objects_handler(
    request: Request,
    bucket: &Bucket,
    rpc_client_nss: &RpcClientNss,
) -> Result<Response, S3Error> {
    let Query(opts): Query<QueryOpts> = request.into_parts().0.extract().await?;
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
