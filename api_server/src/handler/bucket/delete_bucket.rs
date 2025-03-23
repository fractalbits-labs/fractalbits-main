use axum::response::{IntoResponse, Response};
use bucket_tables::{
    api_key_table::{ApiKey, ApiKeyTable},
    bucket_table::{Bucket, BucketTable},
    table::{Table, Versioned},
};
// use rand::Rng;
use rpc_client_nss::{rpc::delete_root_inode_response, RpcClientNss};
use rpc_client_rss::{ArcRpcClientRss, RpcErrorRss};

use crate::handler::{common::s3_error::S3Error, Request};

pub async fn delete_bucket_handler(
    api_key: Versioned<ApiKey>,
    bucket: &Bucket,
    _request: Request,
    rpc_client_nss: &RpcClientNss,
    rpc_client_rss: ArcRpcClientRss,
) -> Result<Response, S3Error> {
    let api_key_id = {
        if !api_key
            .data
            .authorized_buckets
            .contains_key(&bucket.bucket_name)
        {
            return Err(S3Error::AccessDenied);
        }
        api_key.data.key_id.clone()
    };

    let resp = rpc_client_nss
        .delete_root_inode(bucket.root_blob_name.clone())
        .await?;
    match resp.result.unwrap() {
        delete_root_inode_response::Result::Ok(res) => res,
        delete_root_inode_response::Result::ErrNotEmpty(_e) => {
            return Err(S3Error::BucketNotEmpty);
        }
        delete_root_inode_response::Result::ErrOthers(e) => {
            tracing::error!(e);
            return Err(S3Error::InternalError);
        }
    };

    let retry_times = 10;
    for i in 0..retry_times {
        let mut bucket_table: Table<ArcRpcClientRss, BucketTable> =
            Table::new(rpc_client_rss.clone());

        let mut api_key_table: Table<ArcRpcClientRss, ApiKeyTable> =
            Table::new(rpc_client_rss.clone());
        let mut api_key = api_key_table.get(api_key_id.clone()).await?;
        api_key.data.authorized_buckets.remove(&bucket.bucket_name);
        tracing::debug!(
            "Deleting {} from api_key {} (retry={})",
            bucket.bucket_name,
            api_key_id.clone(),
            i,
        );

        match bucket_table
            .delete_with_extra::<ApiKeyTable>(bucket, &api_key)
            .await
        {
            Err(RpcErrorRss::Retry) => continue,
            Err(e) => return Err(e.into()),
            Ok(()) => return Ok(().into_response()),
        }
    }

    tracing::error!(
        "Deleting {} from api_key {api_key_id} failed after retrying {retry_times} times",
        bucket.bucket_name
    );
    Err(S3Error::InternalError)
}
