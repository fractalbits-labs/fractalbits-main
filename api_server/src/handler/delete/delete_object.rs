use std::sync::Arc;

use axum::{
    http::StatusCode,
    response::{self, IntoResponse},
};
use rkyv::{self, rancor::Error};
use rpc_client_nss::{rpc::delete_inode_response, RpcClientNss};
use tokio::sync::mpsc::Sender;

use crate::{bucket_tables::bucket_table::Bucket, object_layout::ObjectLayout, BlobId};

pub async fn delete_object(
    bucket: Arc<Bucket>,
    key: String,
    rpc_client_nss: &RpcClientNss,
    blob_deletion: Sender<(BlobId, usize)>,
) -> response::Result<()> {
    let resp = rpc_client_nss
        .delete_inode(bucket.root_blob_name.clone(), key)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response())?;

    let object_bytes = match resp.result.unwrap() {
        delete_inode_response::Result::Ok(res) => res,
        delete_inode_response::Result::Err(e) => {
            return Err((StatusCode::INTERNAL_SERVER_ERROR, e)
                .into_response()
                .into())
        }
    };

    let object = rkyv::from_bytes::<ObjectLayout, Error>(&object_bytes).unwrap();
    let blob_id = object.blob_id();
    let num_blocks = object.num_blocks();
    if let Err(e) = blob_deletion.send((blob_id, num_blocks)).await {
        tracing::warn!(
            "Failed to send blob {blob_id} num_blocks={num_blocks} for background deletion: {e}"
        );
    }
    Ok(())
}
