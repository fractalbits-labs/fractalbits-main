use std::sync::Arc;

use crate::bucket_tables::{
    bucket_table::{Bucket, BucketTable},
    table::Table,
};
use axum::{extract::Request, response};
use bytes::Buf;
use http_body_util::BodyExt;
use rpc_client_rss::RpcClientRss;
use serde::{Deserialize, Serialize};

#[derive(Default, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct CreateBucketConfiguration {
    location_constraint: String,
    location: Location,
    bucket: BucketConfig,
}

#[derive(Default, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct Location {
    name: String,
    #[serde(rename = "Type")]
    location_type: String,
}

#[derive(Default, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct BucketConfig {
    data_redundancy: String,
    #[serde(rename = "Type")]
    bucket_type: String,
}

pub async fn create_bucket(bucket_name: String, request: Request) -> response::Result<()> {
    let body = request.into_body().collect().await.unwrap().to_bytes();
    if !body.is_empty() {
        let _req_body_res: CreateBucketConfiguration =
            quick_xml::de::from_reader(body.reader()).unwrap();
    }

    let client_rpc_rss = RpcClientRss::new("127.0.0.1:8888").await.unwrap();
    let bucket_table: Table<BucketTable> = Table::new(Arc::new(client_rpc_rss));
    let bucket = Bucket::new(bucket_name.clone());
    bucket_table.put(&bucket).await;
    Ok(())
}
