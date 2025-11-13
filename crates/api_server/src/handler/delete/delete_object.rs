use metrics_wrapper::histogram;
use nss_codec::delete_inode_response;
use rpc_client_common::nss_rpc_retry;

use axum::{
    body::Body,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use rkyv::{self, rancor::Error};
use tokio::sync::mpsc::Sender;

use crate::{
    blob_client::BlobDeletionRequest,
    handler::{
        ObjectRequestContext,
        common::{list_raw_objects, mpu_get_part_prefix, s3_error::S3Error},
    },
    object_layout::{MpuState, ObjectLayout, ObjectState},
};

pub async fn delete_object_handler(ctx: ObjectRequestContext) -> Result<Response, S3Error> {
    let bucket = ctx.resolve_bucket().await?;
    let blob_deletion = ctx.app.get_blob_deletion();
    let rpc_timeout = ctx.app.config.rpc_request_timeout();
    let nss_client = ctx.app.get_nss_rpc_client().await?;
    let resp = nss_rpc_retry!(
        nss_client,
        delete_inode(
            &bucket.root_blob_name,
            &ctx.key,
            Some(rpc_timeout),
            &ctx.trace_id
        ),
        ctx.app,
        &ctx.trace_id
    )
    .await?;

    let object_bytes = match resp.result.unwrap() {
        // S3 allow delete non-existing object
        delete_inode_response::Result::ErrNotFound(()) => {
            tracing::debug!(
                "delete non-existing object {}/{}",
                bucket.bucket_name,
                ctx.key
            );
            return Ok(Response::new(Body::empty()));
        }
        delete_inode_response::Result::ErrAlreadyDeleted(()) => {
            tracing::warn!(
                "object {}/{} is already deleted",
                bucket.bucket_name,
                ctx.key
            );
            return Ok(Response::new(Body::empty()));
        }
        delete_inode_response::Result::Ok(res) => res,
        delete_inode_response::Result::ErrOther(e) => {
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
            delete_blob(
                bucket.tracking_root_blob_name.clone(),
                &object,
                blob_deletion,
            )
            .await?;
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
                let mpu_prefix = mpu_get_part_prefix(ctx.key.clone(), 0);
                let mpus = list_raw_objects(
                    &ctx.app,
                    &bucket.root_blob_name,
                    10000,
                    &mpu_prefix,
                    "",
                    "",
                    false,
                    &ctx.trace_id,
                )
                .await?;
                for (mpu_key, mpu_obj) in mpus.iter() {
                    nss_rpc_retry!(
                        nss_client,
                        delete_inode(
                            &bucket.root_blob_name,
                            &mpu_key,
                            Some(rpc_timeout),
                            &ctx.trace_id
                        ),
                        ctx.app,
                        &ctx.trace_id
                    )
                    .await?;
                    delete_blob(
                        bucket.tracking_root_blob_name.clone(),
                        mpu_obj,
                        blob_deletion.clone(),
                    )
                    .await?;
                }
            }
        },
    }
    Ok((StatusCode::NO_CONTENT).into_response())
}

async fn delete_blob(
    tracking_root_blob_name: Option<String>,
    object: &ObjectLayout,
    blob_deletion: Sender<BlobDeletionRequest>,
) -> Result<(), S3Error> {
    let blob_guid = object.blob_guid()?;
    let num_blocks = object.num_blocks()?;
    let blob_location = object.get_blob_location()?;

    // Send deletion request for each block
    for block_number in 0..num_blocks {
        let request = BlobDeletionRequest {
            tracking_root_blob_name: tracking_root_blob_name.clone(),
            blob_guid,
            block_number: block_number as u32,
            location: blob_location,
        };

        if let Err(e) = blob_deletion.send(request).await {
            tracing::warn!(
                "Failed to send blob {blob_guid} block={block_number} for background deletion: {e}"
            );
        }
    }

    Ok(())
}
