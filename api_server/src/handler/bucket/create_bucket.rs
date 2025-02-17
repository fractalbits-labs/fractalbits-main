use std::sync::Arc;

use crate::bucket_tables::{
    bucket_table::{Bucket, BucketTable},
    table::Table,
};
use axum::{
    extract::Request,
    http::StatusCode,
    response::{self, IntoResponse},
};
use bytes::Buf;
use http_body_util::BodyExt;
use rpc_client_nss::{rpc::create_root_inode_response, RpcClientNss};
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

pub async fn create_bucket(
    bucket_name: String,
    request: Request,
    rpc_client_nss: &RpcClientNss,
    rpc_client_rss: Arc<RpcClientRss>,
) -> response::Result<()> {
    let body = request.into_body().collect().await.unwrap().to_bytes();
    if !body.is_empty() {
        let _req_body_res: CreateBucketConfiguration =
            quick_xml::de::from_reader(body.reader()).unwrap();
    }

    let resp = rpc_client_nss
        .create_root_inode(bucket_name.clone())
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response())?;
    let root_blob_name = match resp.result.unwrap() {
        create_root_inode_response::Result::Ok(res) => res,
        create_root_inode_response::Result::Err(e) => {
            return Err((StatusCode::INTERNAL_SERVER_ERROR, e)
                .into_response()
                .into())
        }
    };

    let mut bucket_table: Table<Arc<RpcClientRss>, BucketTable> = Table::new(rpc_client_rss);
    let bucket = Bucket::new(bucket_name.clone(), root_blob_name);
    bucket_table.put(&bucket).await;
    Ok(())
}
