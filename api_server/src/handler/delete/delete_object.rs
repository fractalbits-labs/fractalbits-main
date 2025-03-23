use axum::response::{IntoResponse, Response};
use rkyv::{self, rancor::Error};
use rpc_client_nss::{rpc::delete_inode_response, RpcClientNss};
use tokio::sync::mpsc::Sender;

use crate::{
    handler::common::{list_raw_objects, mpu_get_part_prefix, s3_error::S3Error},
    object_layout::{MpuState, ObjectLayout, ObjectState},
    BlobId,
};
use bucket_tables::bucket_table::Bucket;

pub async fn delete_object_handler(
    bucket: &Bucket,
    key: String,
    rpc_client_nss: &RpcClientNss,
    blob_deletion: Sender<(BlobId, usize)>,
) -> Result<Response, S3Error> {
    let resp = rpc_client_nss
        .delete_inode(bucket.root_blob_name.clone(), key.clone())
        .await?;

    let object_bytes = match resp.result.unwrap() {
        // S3 allow delete non-existing object
        delete_inode_response::Result::ErrNotFound(()) => {
            tracing::debug!("delete non-existing object {}/{key}", bucket.bucket_name);
            return Ok(().into_response());
        }
        delete_inode_response::Result::ErrAlreadyDeleted(()) => {
            tracing::warn!("object {}/{key} is already deleted", bucket.bucket_name);
            return Ok(().into_response());
        }
        delete_inode_response::Result::Ok(res) => res,
        delete_inode_response::Result::ErrOthers(e) => {
            tracing::error!(e);
            return Err(S3Error::InternalError);
        }
    };

    let object = rkyv::from_bytes::<ObjectLayout, Error>(&object_bytes)?;
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
                    bucket.root_blob_name.clone(),
                    rpc_client_nss,
                    10000,
                    mpu_prefix,
                    "".into(),
                    false,
                )
                .await?;
                for (mpu_key, mpu_obj) in mpus.iter() {
                    rpc_client_nss
                        .delete_inode(bucket.root_blob_name.clone(), mpu_key.clone())
                        .await?;
                    delete_blob(mpu_obj, blob_deletion.clone()).await?;
                }
            }
        },
    }
    Ok(().into_response())
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
