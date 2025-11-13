use crate::handler::{ObjectRequestContext, common::s3_error::S3Error};
use axum::response::Response;
use axum::{RequestPartsExt, extract::Query, response::IntoResponse};
use rpc_client_common::nss_rpc_retry;
use serde::Deserialize;
use tracing::{error, info};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct QueryOpts {
    src_path: String,
}

pub async fn rename_object_handler(ctx: ObjectRequestContext) -> Result<Response, S3Error> {
    let bucket = ctx.resolve_bucket().await?;
    let Query(QueryOpts { src_path }): Query<QueryOpts> =
        ctx.request.into_parts().0.extract().await?;
    let dst_path = ctx.key;
    assert!(!src_path.ends_with('/'));
    assert!(!dst_path.ends_with('/'));
    info!(bucket=%bucket.bucket_name, %src_path, %dst_path, "renaming object in bucket");

    let root_blob_name = bucket.root_blob_name.clone();
    let nss_client = ctx.app.get_nss_rpc_client().await?;
    nss_rpc_retry!(
        nss_client,
        rename_object(
            &root_blob_name,
            &src_path,
            &dst_path,
            Some(ctx.app.config.rpc_request_timeout()),
            &ctx.trace_id
        ),
        ctx.app,
        &ctx.trace_id
    )
    .await
    .map_err(|e| {
        error!(bucket=%bucket.bucket_name, %src_path, %dst_path, error=%e, "failed to rename object");
        S3Error::InternalError
    })?;

    Ok(().into_response())
}
