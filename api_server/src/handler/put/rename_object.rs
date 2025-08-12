use crate::handler::{common::s3_error::S3Error, ObjectRequestContext};
use actix_web::HttpResponse;
use rpc_client_common::nss_rpc_retry;
use serde::Deserialize;
use tracing::{error, info};

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "kebab-case")]
struct QueryOpts {
    src_path: String,
}

pub async fn rename_object_handler(ctx: ObjectRequestContext) -> Result<HttpResponse, S3Error> {
    let bucket = ctx.resolve_bucket().await?;

    // Parse query parameters
    let query_string = ctx.request.query_string();
    let QueryOpts { src_path } = serde_urlencoded::from_str(query_string).unwrap_or_default();
    let dst_path = ctx.key;
    assert!(!src_path.ends_with('/'));
    assert!(!dst_path.ends_with('/'));

    info!(bucket=%bucket.bucket_name, %src_path, %dst_path, "renaming object in bucket");

    nss_rpc_retry!(
        ctx.app,
        rename_object(
            &bucket.root_blob_name,
            &src_path,
            &dst_path,
            Some(ctx.app.config.rpc_timeout())
        )
    )
    .await
    .map_err(|e| {
        error!(bucket=%bucket.bucket_name, %src_path, %dst_path, error=%e, "failed to rename object");
        S3Error::InternalError
    })?;

    Ok(HttpResponse::Ok().finish())
}
