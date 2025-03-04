use axum::{
    extract::{Query, Request},
    response::{IntoResponse, Response},
    RequestExt,
};
use bucket_tables::{
    bucket_table::{self, BucketTable},
    table::Table,
};
use rpc_client_rss::ArcRpcClientRss;
use serde::{Deserialize, Serialize};

use crate::handler::common::{response::xml::Xml, s3_error::S3Error, time::format_timestamp};

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct ListBucketsOptions {
    bucket_region: Option<String>,
    continuation_token: Option<String>,
    max_buckets: Option<u32>,
    prefix: Option<String>,
}

#[derive(Default, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct ListAllMyBucketsResult {
    buckets: Buckets,
    owner: Owner,
    continuation_token: String,
    prefix: String,
}

// Needs to create wrapper to create the correct lists, see
// https://docs.rs/quick-xml/latest/quick_xml/de/index.html#element-lists
#[derive(Default, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct Buckets {
    bucket: Vec<Bucket>,
}

impl From<Vec<Bucket>> for ListAllMyBucketsResult {
    fn from(buckets: Vec<Bucket>) -> Self {
        Self {
            buckets: Buckets { bucket: buckets },
            ..Default::default()
        }
    }
}

#[derive(Default, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct Bucket {
    bucket_region: String,
    creation_date: String, // timestamp
    name: String,
}

impl Bucket {
    fn from_table_with_region(bucket: &bucket_table::Bucket, region: &str) -> Self {
        Self {
            bucket_region: region.into(),
            creation_date: format_timestamp(bucket.creation_date),
            name: bucket.bucket_name.clone(),
        }
    }
}

#[derive(Default, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct Owner {
    display_name: String,
    #[serde(rename = "ID")]
    id: String,
}

pub async fn list_buckets(
    mut request: Request,
    rpc_client_rss: ArcRpcClientRss,
    region: &str,
) -> Result<Response, S3Error> {
    let Query(_opts): Query<ListBucketsOptions> = request.extract_parts().await?;
    let mut bucket_table: Table<ArcRpcClientRss, BucketTable> = Table::new(rpc_client_rss.clone());
    let buckets: Vec<Bucket> = bucket_table
        .list()
        .await?
        .iter()
        .map(|bucket| Bucket::from_table_with_region(bucket, region))
        .collect();
    Ok(Xml(ListAllMyBucketsResult::from(buckets)).into_response())
}
