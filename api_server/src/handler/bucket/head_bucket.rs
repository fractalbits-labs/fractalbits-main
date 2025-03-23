use axum::response::{IntoResponse, Response};
use bucket_tables::{api_key_table::ApiKey, table::Versioned};
use rpc_client_rss::ArcRpcClientRss;

use super::resolve_bucket;
use crate::handler::common::s3_error::S3Error;

pub async fn head_bucket_handler(
    api_key: Versioned<ApiKey>,
    bucket_name: String,
    rpc_client_rss: ArcRpcClientRss,
) -> Result<Response, S3Error> {
    match api_key.data.authorized_buckets.get(&bucket_name) {
        None => return Err(S3Error::InvalidAccessKeyId),
        Some(bucket_key_perm) => {
            if !bucket_key_perm.allow_read {
                return Err(S3Error::AccessDenied);
            }
        }
    }

    resolve_bucket(bucket_name, rpc_client_rss).await?;
    Ok(().into_response())
}
