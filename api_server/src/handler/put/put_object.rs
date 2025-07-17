use rpc_client_common::{nss_rpc_retry, rpc_retry};
use metrics::histogram;
use std::{
    sync::Arc,
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use crate::{
    handler::{
        common::{
            extract_metadata_headers, gen_etag,
            s3_error::S3Error,
            signature::checksum::{
                request_checksum_value, request_trailer_checksum_algorithm, ExpectedChecksums,
            },
            xheader,
        },
        Request,
    },
    object_layout::*,
    AppState, BlobId,
};
use axum::{
    body::Body,
    http::{header, HeaderValue},
    response::Response,
};
use bucket_tables::bucket_table::Bucket;
use futures::{StreamExt, TryStreamExt};
use rkyv::{self, api::high::to_bytes_in, rancor::Error};
use rpc_client_bss::message::MessageHeader;
use rpc_client_nss::rpc::put_inode_response;
use tokio::sync::mpsc::Sender;
use uuid::Uuid;

use super::block_data_stream::BlockDataStream;

pub async fn put_object_handler(
    app: Arc<AppState>,
    request: Request,
    bucket: &Bucket,
    key: String,
    blob_deletion: Sender<(BlobId, usize)>,
) -> Result<Response, S3Error> {
    let start = Instant::now();
    let headers = extract_metadata_headers(request.headers())?;
    let expected_checksums = ExpectedChecksums {
        md5: match request.headers().get("content-md5") {
            Some(x) => Some(x.to_str()?.to_string()),
            None => None,
        },
        sha256: None,
        extra: request_checksum_value(request.headers())?,
    };

    let trailer_checksum_algorithm = request_trailer_checksum_algorithm(request.headers())?;
    let mut req_body = request.into_body();
    req_body.add_expected_checksums(expected_checksums.clone());
    let (body_stream, checksummer) = req_body.streaming_with_checksums();
    let body_data_stream = Body::from_stream(body_stream).into_data_stream();
    let blob_id = Uuid::now_v7();
    let blob_client = app.get_blob_client();
    let size = BlockDataStream::new(body_data_stream, ObjectLayout::DEFAULT_BLOCK_SIZE)
        .enumerate()
        .map(|(i, block_data)| {
            let blob_client = blob_client.clone();
            async move {
                blob_client
                    .put_blob(blob_id, i as u32, block_data)
                    .await
                    .map(|x| (x - MessageHeader::SIZE) as u64)
            }
        })
        .buffer_unordered(5)
        .try_fold(0, |acc, x| async move { Ok(acc + x) })
        .await
        .map_err(|_e| S3Error::InternalError)?;

    histogram!("put_object_handler", "stage" => "put_blob")
        .record(start.elapsed().as_nanos() as f64);

    let checksum_from_stream = checksummer.await.map_err(|e| {
        tracing::error!("JoinHandle error: {e}");
        S3Error::InternalError
    })??;
    let checksum = match expected_checksums.extra {
        Some(x) => Some(x),
        None => checksum_from_stream.extract(trailer_checksum_algorithm),
    };

    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let etag = gen_etag();
    let version_id = gen_version_id();
    let object_layout = ObjectLayout {
        version_id,
        block_size: ObjectLayout::DEFAULT_BLOCK_SIZE,
        timestamp,
        state: ObjectState::Normal(ObjectMetaData {
            blob_id,
            core_meta_data: ObjectCoreMetaData {
                size,
                etag: etag.clone(),
                headers,
                checksum,
            },
        }),
    };
    let object_layout_bytes = to_bytes_in::<_, Error>(&object_layout, Vec::new())?;
    let resp = {
        let start = Instant::now();
        let res = nss_rpc_retry!(
            app,
            put_inode(
                bucket.root_blob_name.clone(),
                key.clone(),
                object_layout_bytes.clone().into()
            )
        )
        .await;
        histogram!("nss_rpc", "op" => "put_inode").record(start.elapsed().as_nanos() as f64);
        res?
    };

    histogram!("put_object_handler", "stage" => "put_inode")
        .record(start.elapsed().as_nanos() as f64);

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

    histogram!("put_object_handler", "stage" => "delete_old_blob")
        .record(start.elapsed().as_nanos() as f64);

    let mut resp = Response::new(Body::empty());
    resp.headers_mut()
        .insert(header::ETAG, HeaderValue::from_str(&etag)?);
    resp.headers_mut().insert(
        xheader::X_AMZ_OBJECT_SIZE,
        HeaderValue::from_str(&size.to_string())?,
    );

    histogram!("put_object_handler", "stage" => "done").record(start.elapsed().as_nanos() as f64);
    Ok(resp)
}
