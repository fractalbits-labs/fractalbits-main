use std::sync::Arc;

use super::{list_objects, Object, Prefix};
use crate::{
    handler::{
        common::{
            response::xml::{Xml, XmlnsS3},
            s3_error::S3Error,
        },
        Request,
    },
    AppState,
};
use axum::{extract::Query, response::Response, RequestPartsExt};
use bucket_tables::bucket_table::Bucket;
use serde::{Deserialize, Serialize};

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
    #[serde(rename = "@xmlns")]
    xmlns: XmlnsS3,
    is_truncated: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    marker: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    next_marker: Option<String>,
    contents: Vec<Object>,
    name: String,
    prefix: Option<String>,
    delimiter: String,
    max_keys: u32,
    common_prefixes: Vec<Prefix>,
    encoding_type: String,
}

impl Default for ListBucketResult {
    fn default() -> Self {
        Self {
            xmlns: Default::default(),
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
    fn truncated(self, is_truncated: bool) -> Self {
        Self {
            is_truncated,
            ..self
        }
    }

    fn marker(self, marker: Option<String>) -> Self {
        Self { marker, ..self }
    }

    fn next_marker(self, next_marker: Option<String>) -> Self {
        Self {
            next_marker,
            ..self
        }
    }

    fn contents(self, contents: Vec<Object>) -> Self {
        Self { contents, ..self }
    }

    fn bucket_name(self, bucket_name: String) -> Self {
        Self {
            name: bucket_name,
            ..self
        }
    }

    fn prefix(self, prefix: Option<String>) -> Self {
        Self { prefix, ..self }
    }

    fn max_keys(self, max_keys: u32) -> Self {
        Self { max_keys, ..self }
    }

    fn common_prefixes(self, common_prefixes: Vec<Prefix>) -> Self {
        Self {
            common_prefixes,
            ..self
        }
    }
}

pub async fn list_objects_handler(
    app: Arc<AppState>,
    request: Request,
    bucket: &Bucket,
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
    let prefix = format!("/{}", opts.prefix.clone().unwrap_or_default());
    let delimiter = opts.delimiter.clone().unwrap_or("".into());
    if !delimiter.is_empty() && delimiter != "/" {
        tracing::warn!("Got delimiter: {delimiter}, which is not supported.");
        return Err(S3Error::UnsupportedArgument);
    }
    let start_after = match opts.marker {
        Some(ref marker) => format!("/{}", marker),
        None => "".into(),
    };

    let rpc_client_nss = app.get_rpc_client_nss();
    let (objs, common_prefixes, next_continuation_token) = list_objects(
        bucket,
        rpc_client_nss,
        max_keys,
        prefix.clone(),
        delimiter.clone(),
        start_after,
    )
    .await?;

    Xml(ListBucketResult::default()
        .truncated(next_continuation_token.is_some())
        .marker(opts.marker)
        .next_marker(next_continuation_token)
        .contents(objs)
        .bucket_name(bucket.bucket_name.clone())
        .prefix(opts.prefix)
        .max_keys(max_keys)
        .common_prefixes(common_prefixes))
    .try_into()
}
