use crate::handler::{common::s3_error::S3Error, ObjectRequestContext};
use actix_web::HttpResponse;
use rpc_client_common::nss_rpc_retry;
use serde::Deserialize;
use tracing::error;

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "kebab-case")]
struct QueryOpts {
    src_path: String,
}

pub async fn rename_folder_handler(ctx: ObjectRequestContext) -> Result<HttpResponse, S3Error> {
    let bucket = ctx.resolve_bucket().await?;

    // Parse query parameters
    let query_string = ctx.request.query_string();
    let QueryOpts { src_path } = serde_urlencoded::from_str(query_string).unwrap_or_default();
    let mut dst_path = ctx.key;
    if !dst_path.ends_with('/') {
        dst_path.push('/');
    }

    tracing::info!(
        "Rename folder from '{}' to '{}' in bucket '{}'",
        src_path,
        dst_path,
        ctx.bucket_name
    );

    nss_rpc_retry!(
        ctx.app,
        rename_folder(
            &bucket.root_blob_name,
            &src_path,
            &dst_path,
            Some(ctx.app.config.rpc_timeout())
        )
    )
    .await
    .map_err(|e| {
        error!(bucket=%bucket.bucket_name, %src_path, %dst_path, error=%e, "failed to rename folder");
        S3Error::InternalError
    })?;

    Ok(HttpResponse::Ok().finish())
}
