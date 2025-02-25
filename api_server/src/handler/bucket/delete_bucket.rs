use std::sync::Arc;

use axum::{extract::Request, response};
use bucket_tables::{
    api_key_table::ApiKey,
    bucket_table::{Bucket, BucketTable},
    table::Table,
};
use rpc_client_nss::RpcClientNss;
use rpc_client_rss::ArcRpcClientRss;

pub async fn delete_bucket(
    _api_key: Option<ApiKey>,
    bucket: Arc<Bucket>,
    _request: Request,
    _rpc_client_nss: &RpcClientNss,
    rpc_client_rss: ArcRpcClientRss,
) -> response::Result<()> {
    // FIXME: check bucket emptiness and clean up related api_key_table
    let mut bucket_table: Table<ArcRpcClientRss, BucketTable> = Table::new(rpc_client_rss);
    bucket_table.delete(&bucket).await;
    Ok(())
}
