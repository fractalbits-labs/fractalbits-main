use axum::{
    extract::Request,
    response::{IntoResponse, Response},
};
use bucket_tables::{
    api_key_table::{ApiKey, ApiKeyTable},
    bucket_table::{Bucket, BucketTable},
    permission::BucketKeyPerm,
    table::Table,
};
use bytes::Buf;
use http_body_util::BodyExt;
use rpc_client_nss::{rpc::create_root_inode_response, RpcClientNss};
use rpc_client_rss::ArcRpcClientRss;
use serde::{Deserialize, Serialize};

use crate::handler::common::s3_error::S3Error;

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
    api_key: Option<ApiKey>,
    bucket_name: String,
    request: Request,
    rpc_client_nss: &RpcClientNss,
    rpc_client_rss: ArcRpcClientRss,
) -> Result<Response, S3Error> {
    match api_key {
        None => return Err(S3Error::InvalidAccessKeyId),
        Some(ref api_key) => {
            if !api_key.allow_create_bucket {
                return Err(S3Error::AccessDenied);
            }
        }
    }

    let body = request.into_body().collect().await.unwrap().to_bytes();
    if !body.is_empty() {
        let _req_body_res: CreateBucketConfiguration =
            quick_xml::de::from_reader(body.reader()).unwrap();
    }

    let resp = rpc_client_nss
        .create_root_inode(bucket_name.clone())
        .await?;
    let root_blob_name = match resp.result.unwrap() {
        create_root_inode_response::Result::Ok(res) => res,
        create_root_inode_response::Result::Err(e) => {
            tracing::error!(e);
            return Err(S3Error::InternalError);
        }
    };

    let mut api_key = api_key.unwrap();
    let mut bucket_table: Table<ArcRpcClientRss, BucketTable> = Table::new(rpc_client_rss.clone());
    let mut bucket = Bucket::new(bucket_name.clone(), root_blob_name);
    let bucket_key_perm = BucketKeyPerm::ALL_PERMISSIONS;
    bucket
        .authorized_keys
        .insert(api_key.key_id.clone(), bucket_key_perm);
    bucket_table.put(&bucket).await?;

    let mut api_key_table: Table<ArcRpcClientRss, ApiKeyTable> = Table::new(rpc_client_rss);
    api_key
        .authorized_buckets
        .insert(bucket_name, bucket_key_perm);
    api_key_table.put(&api_key).await?;

    Ok(().into_response())
}
