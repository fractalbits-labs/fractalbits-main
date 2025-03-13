use std::time::{SystemTime, UNIX_EPOCH};

// use super::block_data_stream::BlockDataStream;
use crate::{
    handler::{
        common::{
            s3_error::S3Error,
            signature::checksum::{
                request_checksum_value, request_trailer_checksum_algorithm, ExpectedChecksums,
            },
        },
        Request,
    },
    object_layout::*,
    BlobId,
};
use axum::response::{IntoResponse, Response};
use bucket_tables::bucket_table::Bucket;
// use futures::{StreamExt, TryStreamExt};
// use futures::StreamExt;
use rkyv::{self, api::high::to_bytes_in, rancor::Error};
use rpc_client_bss::{message::MessageHeader, RpcClientBss};
use rpc_client_nss::{rpc::put_inode_response, RpcClientNss};
use tokio::sync::mpsc::Sender;
use uuid::Uuid;

pub async fn put_object(
    request: Request,
    bucket: &Bucket,
    key: String,
    rpc_client_nss: &RpcClientNss,
    rpc_client_bss: &RpcClientBss,
    blob_deletion: Sender<(BlobId, usize)>,
) -> Result<Response, S3Error> {
    let expected_checksums = ExpectedChecksums {
        md5: match request.headers().get("content-md5") {
            Some(x) => Some(x.to_str()?.to_string()),
            None => None,
        },
        sha256: None,
        extra: request_checksum_value(request.headers())?,
    };
    let trailer_checksum_algorithm = request_trailer_checksum_algorithm(request.headers())?;
    // let body_data_stream = request.into_body().into_data_stream();
    // let size = BlockDataStream::new(body_data_stream, ObjectLayout::DEFAULT_BLOCK_SIZE)
    //     .enumerate()
    //     .map(|(i, block_data)| async move {
    //         rpc_client_bss
    //             .put_blob(blob_id, i as u32, block_data)
    //             .await
    //             .map(|x| (x - MessageHeader::SIZE) as u64)
    //     })
    //     .buffer_unordered(5)
    //     .try_fold(0, |acc, x| async move { Ok(acc + x) })
    //     .await
    //     .map_err(|_e| S3Error::InternalError)?;

    let blob_id = Uuid::now_v7();
    let (body_content, checksums) = request.into_body().collect_with_checksums().await.unwrap();
    let checksum = match expected_checksums.extra {
        Some(x) => Some(x),
        None => checksums.extract(trailer_checksum_algorithm),
    };
    let size = rpc_client_bss
        .put_blob(blob_id, 0, body_content)
        .await
        .map(|x| (x - MessageHeader::SIZE) as u64)?;

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
            checksum,
        }),
    };
    let object_layout_bytes = to_bytes_in::<_, Error>(&object_layout, Vec::new())?;
    let resp = rpc_client_nss
        .put_inode(
            bucket.root_blob_name.clone(),
            key,
            object_layout_bytes.into(),
        )
        .await?;

    // Delete old object if it is an overwrite request
    let old_object_bytes = match resp.result.unwrap() {
        put_inode_response::Result::Ok(res) => res,
        put_inode_response::Result::Err(e) => {
            tracing::error!(e);
            return Err(S3Error::InternalError);
        }
    };
    if !old_object_bytes.is_empty() {
        let old_object = rkyv::from_bytes::<ObjectLayout, Error>(&old_object_bytes)?;
        let blob_id = old_object.blob_id()?;
        let num_blocks = old_object.num_blocks()?;
        if let Err(e) = blob_deletion.send((blob_id, num_blocks)).await {
            tracing::warn!(
            "Failed to send blob {blob_id} num_blocks={num_blocks} for background deletion: {e}");
        }
    }

    Ok(().into_response())
}
