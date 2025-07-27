use std::sync::Arc;

use axum::{extract::Query, response::Response, RequestPartsExt};
use bucket_tables::{
    bucket_table::{self, BucketTable},
    table::Table,
};
use serde::{Deserialize, Serialize};

use crate::{
    handler::{
        common::{
            response::xml::{Xml, XmlnsS3},
            s3_error::S3Error,
            time::format_timestamp,
        },
        Request,
    },
    AppState,
};

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
    #[serde(rename = "@xmlns")]
    xmlns: XmlnsS3,
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

pub async fn list_buckets_handler(
    app: Arc<AppState>,
    request: Request,
) -> Result<Response, S3Error> {
    let Query(_opts): Query<ListBucketsOptions> = request.into_parts().0.extract().await?;
    let bucket_table: Table<Arc<AppState>, BucketTable> =
        Table::new(app.clone(), Some(app.cache.clone()));
    let buckets: Vec<Bucket> = bucket_table
        .list(Some(app.config.rpc_timeout()))
        .await?
        .iter()
        .map(|bucket| Bucket::from_table_with_region(bucket, &app.config.region))
        .collect();
    Xml(ListAllMyBucketsResult::from(buckets)).try_into()
}
