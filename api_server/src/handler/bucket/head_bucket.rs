use std::sync::Arc;

use axum::{body::Body, response::Response};
use bucket_tables::{api_key_table::ApiKey, table::Versioned};

use super::resolve_bucket;
use crate::handler::common::s3_error::S3Error;
use crate::AppState;

pub async fn head_bucket_handler(
    app: Arc<AppState>,
    api_key: Versioned<ApiKey>,
    bucket_name: String,
) -> Result<Response, S3Error> {
    match api_key.data.authorized_buckets.get(&bucket_name) {
        None => return Err(S3Error::InvalidAccessKeyId),
        Some(bucket_key_perm) => {
            if !bucket_key_perm.allow_read {
                return Err(S3Error::AccessDenied);
            }
        }
    }

    let rpc_client_rss = app.get_rpc_client_rss();
    resolve_bucket(bucket_name, rpc_client_rss).await?;
    Ok(Response::new(Body::empty()))
}
