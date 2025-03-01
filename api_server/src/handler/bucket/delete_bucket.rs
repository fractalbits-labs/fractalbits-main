use std::sync::Arc;

use axum::extract::Request;
use bucket_tables::{
    api_key_table::{ApiKey, ApiKeyTable},
    bucket_table::{Bucket, BucketTable},
    table::Table,
};
use rpc_client_nss::{rpc::delete_root_inode_response, RpcClientNss};
use rpc_client_rss::ArcRpcClientRss;

use crate::handler::common::s3_error::S3Error;

pub async fn delete_bucket(
    api_key: Option<ApiKey>,
    bucket: Arc<Bucket>,
    _request: Request,
    rpc_client_nss: &RpcClientNss,
    rpc_client_rss: ArcRpcClientRss,
) -> Result<(), S3Error> {
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

    let mut bucket_table: Table<ArcRpcClientRss, BucketTable> = Table::new(rpc_client_rss.clone());
    bucket_table.delete(&bucket).await;

    let mut api_key = api_key.unwrap();
    let mut api_key_table: Table<ArcRpcClientRss, ApiKeyTable> = Table::new(rpc_client_rss);
    api_key.authorized_buckets.remove(&bucket.bucket_name);
    api_key_table.put(&api_key).await;
    Ok(())
}
