use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use super::block_data_stream::BlockDataStream;
use crate::{bucket_tables::bucket_table::Bucket, object_layout::*, BlobId};
use axum::{extract::Request, http::StatusCode, response, response::IntoResponse};
use futures::{StreamExt, TryStreamExt};
use rkyv::{self, api::high::to_bytes_in, rancor::Error};
use rpc_client_bss::{message::MessageHeader, RpcClientBss};
use rpc_client_nss::{rpc::put_inode_response, RpcClientNss};
use tokio::sync::mpsc::Sender;
use uuid::Uuid;

pub async fn put_object(
    request: Request,
    bucket: Arc<Bucket>,
    key: String,
    rpc_client_nss: &RpcClientNss,
    rpc_client_bss: &RpcClientBss,
    blob_deletion: Sender<(BlobId, usize)>,
) -> response::Result<()> {
    let blob_id = Uuid::now_v7();
    let body_data_stream = request.into_body().into_data_stream();
    let size = BlockDataStream::new(body_data_stream, ObjectLayout::DEFAULT_BLOCK_SIZE)
        .enumerate()
        .map(|(i, block_data)| async move {
            rpc_client_bss
                .put_blob(blob_id, i as u32, block_data)
                .await
                .map(|x| (x - MessageHeader::SIZE) as u64)
        })
        .buffer_unordered(5)
        .try_fold(0, |acc, x| async move { Ok(acc + x) })
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response())?;

    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let etag = String::new();
    let version_id = gen_version_id();
    let object_layout = ObjectLayout {
        version_id,
        block_size: ObjectLayout::DEFAULT_BLOCK_SIZE,
        timestamp,
        state: ObjectState::Normal(ObjectData {
            size,
            blob_id,
            etag,
        }),
    };
    let object_layout_bytes = to_bytes_in::<_, Error>(&object_layout, Vec::new()).unwrap();
    let resp = rpc_client_nss
        .put_inode(
            bucket.root_blob_name.clone(),
            key,
            object_layout_bytes.into(),
        )
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response())?;

    // Delete old object if it is an overwrite request
    let old_object_bytes = match resp.result.unwrap() {
        put_inode_response::Result::Ok(res) => res,
        put_inode_response::Result::Err(e) => {
            return Err((StatusCode::INTERNAL_SERVER_ERROR, e)
                .into_response()
                .into())
        }
    };
    if !old_object_bytes.is_empty() {
        let old_object = rkyv::from_bytes::<ObjectLayout, Error>(&old_object_bytes).unwrap();
        let blob_id = old_object.blob_id();
        let num_blocks = old_object.num_blocks();
        if let Err(e) = blob_deletion.send((blob_id, num_blocks)).await {
            tracing::warn!(
            "Failed to send blob {blob_id} num_blocks={num_blocks} for background deletion: {e}");
        }
    }

    Ok(())
}
