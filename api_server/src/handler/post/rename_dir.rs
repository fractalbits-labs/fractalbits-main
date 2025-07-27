use std::sync::Arc;

use crate::{
    handler::{common::s3_error::S3Error, Request},
    AppState,
};
use axum::response::Response;
use axum::{extract::Query, response::IntoResponse, RequestPartsExt};
use bucket_tables::bucket_table::Bucket;
use rpc_client_common::{nss_rpc_retry, rpc_retry};
use serde::Deserialize;
use tracing::{error, info};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct QueryOpts {
    src_path: String,
    dst_path: String,
}

pub async fn rename_dir_handler(
    app: Arc<AppState>,
    request: Request,
    bucket: &Bucket,
) -> Result<Response, S3Error> {
    let Query(QueryOpts { src_path, dst_path }): Query<QueryOpts> =
        request.into_parts().0.extract().await?;
    info!(bucket=%bucket.bucket_name, %src_path, %dst_path, "renaming directory in bucket");

    let root_blob_name = bucket.root_blob_name.clone();

    nss_rpc_retry!(app, rename_dir(&root_blob_name, &src_path, &dst_path, Some(app.config.rpc_timeout())))
        .await
        .map_err(|e| {
            error!(bucket=%bucket.bucket_name, %src_path, %dst_path, error=%e, "failed to rename directory");
            S3Error::InternalError
        })?;

    Ok(().into_response())
}
