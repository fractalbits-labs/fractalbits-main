use std::sync::Arc;

use axum::{body::Body, response::Response};
use bucket_tables::{api_key_table::ApiKey, Versioned};

use super::resolve_bucket;
use crate::handler::common::s3_error::S3Error;
use crate::AppState;
use tracing::error;

pub async fn head_bucket_handler(
    app: Arc<AppState>,
    api_key: Versioned<ApiKey>,
    bucket_name: String,
) -> Result<Response, S3Error> {
    match api_key.data.authorized_buckets.get(&bucket_name) {
        None => {
            error!(
                "bucket {bucket_name} is not associated with api_key: {}",
                api_key.data.key_id
            );
            return Err(S3Error::InvalidAccessKeyId);
        }
        Some(bucket_key_perm) => {
            if !bucket_key_perm.allow_read {
                error!(
                    "bucket {bucket_name} is not associated with api_key: {}",
                    api_key.data.key_id
                );
                return Err(S3Error::AccessDenied);
            }
        }
    }

    resolve_bucket(&app, bucket_name).await.map_err(|e| {
        error!("head_bucket failed due to bucket resolving: {e}");
        e
    })?;
    Ok(Response::new(Body::empty()))
}
