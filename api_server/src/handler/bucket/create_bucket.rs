use axum::{
    http::{header, HeaderValue},
    response::{IntoResponse, Response},
};
use bucket_tables::{
    api_key_table::{ApiKey, ApiKeyTable},
    bucket_table::{Bucket, BucketTable},
    permission::BucketKeyPerm,
    table::{Table, Versioned},
};
use bytes::Buf;
use rpc_client_nss::{rpc::create_root_inode_response, RpcClientNss};
use rpc_client_rss::{ArcRpcClientRss, RpcErrorRss};
use serde::{Deserialize, Serialize};

use crate::handler::{common::s3_error::S3Error, Request};

#[derive(Default, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct CreateBucketConfiguration {
    #[serde(default)]
    location_constraint: String,
    #[serde(default)]
    location: Location,
    #[serde(default)]
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

pub async fn create_bucket_handler(
    api_key: Versioned<ApiKey>,
    bucket_name: String,
    request: Request,
    rpc_client_nss: &RpcClientNss,
    rpc_client_rss: ArcRpcClientRss,
    region: &str,
) -> Result<Response, S3Error> {
    let api_key_id = {
        if api_key.data.authorized_buckets.contains_key(&bucket_name) {
            return Err(S3Error::BucketAlreadyExists);
        }
        if !api_key.data.allow_create_bucket {
            return Err(S3Error::AccessDenied);
        }
        api_key.data.key_id.clone()
    };

    let body = request.into_body().collect().await.unwrap();
    if !body.is_empty() {
        let create_bucket_conf: CreateBucketConfiguration =
            quick_xml::de::from_reader(body.reader())?;
        let location_constraint = create_bucket_conf.location_constraint;
        if !location_constraint.is_empty() && location_constraint != region {
            return Err(S3Error::InvalidLocationConstraint);
        }
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

    let retry_times = 10;
    for i in 0..retry_times {
        let mut bucket_table: Table<ArcRpcClientRss, BucketTable> =
            Table::new(rpc_client_rss.clone());
        if bucket_table.get(bucket_name.clone()).await.is_ok() {
            return Err(S3Error::BucketAlreadyExists);
        }

        let mut bucket =
            Versioned::new(0, Bucket::new(bucket_name.clone(), root_blob_name.clone()));
        let bucket_key_perm = BucketKeyPerm::ALL_PERMISSIONS;
        bucket
            .data
            .authorized_keys
            .insert(api_key_id.clone(), bucket_key_perm);

        let mut api_key_table: Table<ArcRpcClientRss, ApiKeyTable> =
            Table::new(rpc_client_rss.clone());
        let mut api_key = api_key_table.get(api_key_id.clone()).await?;
        api_key
            .data
            .authorized_buckets
            .insert(bucket_name.clone(), bucket_key_perm);

        tracing::debug!(
            "Inserting {} into api_key {} (retry={})",
            bucket_name.clone(),
            api_key_id.clone(),
            i,
        );
        match bucket_table
            .put_with_extra::<ApiKeyTable>(&bucket, &api_key)
            .await
        {
            Err(RpcErrorRss::Retry) => continue,
            Err(e) => return Err(e.into()),
            Ok(()) => {
                return Ok([(
                    header::LOCATION,
                    HeaderValue::from_str(&format!("/{bucket_name}")).unwrap(),
                )]
                .into_response())
            }
        }
    }

    tracing::error!("Inserting {bucket_name} into api_key {api_key_id} failed after retrying {retry_times} times");
    Err(S3Error::InternalError)
}
