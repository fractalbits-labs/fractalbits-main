use rpc_client_common::{nss_rpc_retry, rpc_retry};
use std::sync::Arc;

use axum::{body::Body, response::Response};
use bucket_tables::{
    api_key_table::{ApiKey, ApiKeyTable},
    bucket_table::{Bucket, BucketTable},
    table::Table,
    Versioned,
};
use rpc_client_nss::rpc::delete_root_inode_response;
use rpc_client_rss::RpcErrorRss;
use tracing::info;

use crate::handler::{common::s3_error::S3Error, Request};
use crate::AppState;

pub async fn delete_bucket_handler(
    app: Arc<AppState>,
    api_key: Versioned<ApiKey>,
    bucket: &Bucket,
    _request: Request,
) -> Result<Response, S3Error> {
    info!("handling delete_bucket request: {}", bucket.bucket_name);
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

    let resp = nss_rpc_retry!(app, delete_root_inode(bucket.root_blob_name.clone())).await?;
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
        let bucket_table: Table<Arc<AppState>, BucketTable> =
            Table::new(app.clone(), Some(app.cache.clone()));
        let api_key_table: Table<Arc<AppState>, ApiKeyTable> =
            Table::new(app.clone(), Some(app.cache.clone()));
        let mut api_key = api_key_table.get(api_key_id.clone(), false).await?;
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
            Ok(()) => return Ok(Response::new(Body::empty())),
        }
    }

    tracing::error!(
        "Deleting {} from api_key {api_key_id} failed after retrying {retry_times} times",
        bucket.bucket_name
    );
    Err(S3Error::InternalError)
}
