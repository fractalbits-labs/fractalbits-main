use metrics::histogram;
use rpc_client_common::{nss_rpc_retry, rpc_retry};
use std::sync::Arc;

use axum::{
    body::Body,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use rkyv::{self, rancor::Error};
use rpc_client_nss::rpc::delete_inode_response;
use tokio::sync::mpsc::Sender;

use crate::{
    handler::common::{list_raw_objects, mpu_get_part_prefix, s3_error::S3Error},
    object_layout::{MpuState, ObjectLayout, ObjectState},
    AppState, BlobId,
};
use bucket_tables::bucket_table::Bucket;

pub async fn delete_object_handler(
    app: Arc<AppState>,
    bucket: &Bucket,
    key: String,
    blob_deletion: Sender<(BlobId, usize)>,
) -> Result<Response, S3Error> {
    let rpc_timeout = app.config.rpc_timeout();
    let resp = nss_rpc_retry!(app, delete_inode(&bucket.root_blob_name, &key, Some(rpc_timeout))).await?;

    let object_bytes = match resp.result.unwrap() {
        // S3 allow delete non-existing object
        delete_inode_response::Result::ErrNotFound(()) => {
            tracing::debug!("delete non-existing object {}/{key}", bucket.bucket_name);
            return Ok(Response::new(Body::empty()));
        }
        delete_inode_response::Result::ErrAlreadyDeleted(()) => {
            tracing::warn!("object {}/{key} is already deleted", bucket.bucket_name);
            return Ok(Response::new(Body::empty()));
        }
        delete_inode_response::Result::Ok(res) => res,
        delete_inode_response::Result::ErrOthers(e) => {
            tracing::error!(e);
            return Err(S3Error::InternalError);
        }
    };

    let object = rkyv::from_bytes::<ObjectLayout, Error>(&object_bytes)?;
    if let Ok(size) = object.size() {
        histogram!("object_size", "operation" => "delete").record(size as f64);
    }
    match object.state {
        ObjectState::Normal(..) => {
            delete_blob(&object, blob_deletion).await?;
        }
        ObjectState::Mpu(mpu_state) => match mpu_state {
            MpuState::Uploading => {
                tracing::warn!("invalid mpu state: Uploading");
                return Err(S3Error::InvalidObjectState);
            }
            MpuState::Aborted => {
                tracing::warn!("invalid mpu state: Aborted");
                return Err(S3Error::InvalidObjectState);
            }
            MpuState::Completed { .. } => {
                let mpu_prefix = mpu_get_part_prefix(key, 0);
                let mpus = list_raw_objects(
                    &app,
                    &bucket.root_blob_name,
                    10000,
                    &mpu_prefix,
                    "",
                    "",
                    false,
                )
                .await?;
                for (mpu_key, mpu_obj) in mpus.iter() {
                    nss_rpc_retry!(
                        app,
                        delete_inode(&bucket.root_blob_name, &mpu_key, Some(rpc_timeout))
                    )
                    .await?;
                    delete_blob(mpu_obj, blob_deletion.clone()).await?;
                }
            }
        },
    }
    Ok((StatusCode::NO_CONTENT).into_response())
}

async fn delete_blob(
    object: &ObjectLayout,
    blob_deletion: Sender<(BlobId, usize)>,
) -> Result<(), S3Error> {
    let blob_id = object.blob_id()?;
    let num_blocks = object.num_blocks()?;
    if let Err(e) = blob_deletion.send((blob_id, num_blocks)).await {
        tracing::warn!(
            "Failed to send blob {blob_id} num_blocks={num_blocks} for background deletion: {e}"
        );
    }
    Ok(())
}
