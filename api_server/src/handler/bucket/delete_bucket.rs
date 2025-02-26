use std::sync::Arc;

use axum::{
    extract::Request,
    http::StatusCode,
    response::{self, IntoResponse},
};
use bucket_tables::{
    api_key_table::{ApiKey, ApiKeyTable},
    bucket_table::{Bucket, BucketTable},
    table::Table,
};
use rpc_client_nss::{rpc::delete_root_inode_response, RpcClientNss};
use rpc_client_rss::ArcRpcClientRss;

pub async fn delete_bucket(
    api_key: Option<ApiKey>,
    bucket: Arc<Bucket>,
    _request: Request,
    rpc_client_nss: &RpcClientNss,
    rpc_client_rss: ArcRpcClientRss,
) -> response::Result<()> {
    let resp = rpc_client_nss
        .delete_root_inode(bucket.root_blob_name.clone())
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response())?;
    match resp.result.unwrap() {
        delete_root_inode_response::Result::Ok(res) => res,
        delete_root_inode_response::Result::ErrNonEmpty(e) => {
            return Err((StatusCode::BAD_REQUEST, e).into_response().into())
        }
        delete_root_inode_response::Result::ErrOthers(e) => {
            return Err((StatusCode::INTERNAL_SERVER_ERROR, e)
                .into_response()
                .into())
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
