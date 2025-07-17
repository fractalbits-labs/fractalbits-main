use rpc_client_common::{nss_rpc_retry, rpc_retry};
use std::sync::Arc;

use axum::{body::Body, response::Response};
use rkyv::{self, api::high::to_bytes_in, rancor::Error};
use rpc_client_nss::rpc::{get_inode_response, put_inode_response};

use crate::{
    handler::{common::s3_error::S3Error, Request},
    object_layout::{MpuState, ObjectLayout, ObjectState},
    AppState,
};
use bucket_tables::bucket_table::Bucket;

pub async fn abort_multipart_upload_handler(
    app: Arc<AppState>,
    _request: Request,
    bucket: &Bucket,
    key: String,
    _upload_id: String,
) -> Result<Response, S3Error> {
    let resp = nss_rpc_retry!(app, get_inode(bucket.root_blob_name.clone(), key.clone())).await?;

    let object_bytes = match resp.result.unwrap() {
        get_inode_response::Result::Ok(res) => res,
        get_inode_response::Result::ErrNotFound(()) => {
            return Err(S3Error::NoSuchKey);
        }
        get_inode_response::Result::ErrOthers(e) => {
            tracing::error!(e);
            return Err(S3Error::InternalError);
        }
    };

    // TODO: check upload_id and also do more clean ups and checks
    let mut object = rkyv::from_bytes::<ObjectLayout, Error>(&object_bytes)?;
    object.state = ObjectState::Mpu(MpuState::Aborted);
    let new_object_bytes = to_bytes_in::<_, Error>(&object, Vec::new())?;

    let resp = nss_rpc_retry!(
        app,
        put_inode(
            bucket.root_blob_name.clone(),
            key.clone(),
            new_object_bytes.clone().into()
        )
    )
    .await?;
    match resp.result.unwrap() {
        put_inode_response::Result::Ok(_) => {}
        put_inode_response::Result::Err(e) => {
            tracing::error!(e);
            return Err(S3Error::InternalError);
        }
    };

    Ok(Response::new(Body::empty()))
}
