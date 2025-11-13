use bytes::Bytes;
use data_types::DataBlobGuid;
use metrics_wrapper::histogram;
use rpc_client_common::nss_rpc_retry;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use crate::{
    handler::{
        ObjectRequestContext,
        common::{
            checksum::request_trailer_checksum_algorithm, extract_metadata_headers,
            s3_error::S3Error, xheader,
        },
    },
    object_layout::*,
};
use axum::{
    body::Body,
    http::{HeaderValue, header},
    response::Response,
};
use futures::{StreamExt, TryStreamExt};
use rkyv::{self, api::high::to_bytes_in, rancor::Error};
use tracing::{Instrument, Span};

use super::block_data_stream::BlockDataStream;

pub async fn put_object_handler(ctx: ObjectRequestContext) -> Result<Response, S3Error> {
    let bucket = ctx.resolve_bucket().await?;
    let tracking_root_blob_name = bucket.tracking_root_blob_name;
    let blob_deletion = ctx.app.get_blob_deletion();
    let start = Instant::now();
    let headers = extract_metadata_headers(ctx.request.headers())?;
    let expected_checksum = ctx.checksum_value;

    let trailer_checksum_algorithm = request_trailer_checksum_algorithm(ctx.request.headers())?;
    let mut req_body = ctx.request.into_body();
    if let Some(checksum) = expected_checksum {
        req_body.set_expected_checksum(checksum);
    } else if let Some(algo) = trailer_checksum_algorithm {
        req_body.set_checksum_algorithm(algo);
    }
    let (body_stream, checksummer) = req_body.streaming_with_checksums();
    let body_data_stream = Body::from_stream(body_stream).into_data_stream();
    let blob_client = ctx
        .app
        .get_blob_client()
        .await
        .map_err(|_| S3Error::InternalError)?;
    let mut blob_guid = blob_client.create_data_blob_guid();
    let size = BlockDataStream::new(body_data_stream, ObjectLayout::DEFAULT_BLOCK_SIZE)
        .enumerate()
        .map(|(i, block_data)| {
            let blob_client = blob_client.clone();
            let tracking_root_blob_name = tracking_root_blob_name.clone();
            async move {
                let data = block_data.map_err(|_e| S3Error::InternalError)?;
                let len = data.len() as u64;
                let put_result = blob_client
                    .put_blob(
                        tracking_root_blob_name.as_deref(),
                        blob_guid,
                        i as u32,
                        data,
                        &ctx.trace_id,
                    )
                    .await;

                match put_result {
                    Ok(_returned_guid) => Ok(len),
                    Err(_e) => Err(S3Error::InternalError),
                }
            }
            .instrument(Span::current())
        })
        .buffer_unordered(5)
        .try_fold(0, |acc, x| async move { Ok(acc + x) })
        .await
        .map_err(|_e| S3Error::InternalError)?;

    histogram!("object_size", "operation" => "put").record(size as f64);
    histogram!("put_object_handler", "stage" => "put_blob")
        .record(start.elapsed().as_nanos() as f64);

    let checksum_from_stream = checksummer.await.map_err(|e| {
        tracing::error!("JoinHandle error: {e}");
        S3Error::InternalError
    })??;
    let checksum = match expected_checksum {
        Some(x) => Some(x),
        None => checksum_from_stream,
    };

    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let etag = blob_guid.blob_id.simple().to_string();
    let version_id = gen_version_id();

    // Only use S3_VOLUME for large objects when using S3-based backends
    let uses_s3_for_large_blobs = matches!(
        ctx.app.config.blob_storage.backend,
        crate::BlobStorageBackend::S3HybridSingleAz | crate::BlobStorageBackend::S3ExpressMultiAz
    );
    if uses_s3_for_large_blobs && size >= ObjectLayout::DEFAULT_BLOCK_SIZE as u64 {
        blob_guid.volume_id = DataBlobGuid::S3_VOLUME;
    }

    let object_layout = ObjectLayout {
        version_id,
        block_size: ObjectLayout::DEFAULT_BLOCK_SIZE,
        timestamp,
        state: ObjectState::Normal(ObjectMetaData {
            blob_guid,
            core_meta_data: ObjectCoreMetaData {
                size,
                etag: etag.clone(),
                headers,
                checksum,
            },
        }),
    };
    let object_layout_bytes: Bytes = to_bytes_in::<_, Error>(&object_layout, Vec::new())?.into();
    let nss_client = ctx.app.get_nss_rpc_client().await?;
    let resp = {
        let start = Instant::now();
        let res = nss_rpc_retry!(
            nss_client,
            put_inode(
                &bucket.root_blob_name,
                &ctx.key,
                object_layout_bytes.clone(),
                Some(ctx.app.config.rpc_request_timeout()),
                &ctx.trace_id
            ),
            ctx.app,
            &ctx.trace_id
        )
        .await;
        histogram!("nss_rpc", "op" => "put_inode").record(start.elapsed().as_nanos() as f64);
        res?
    };

    histogram!("put_object_handler", "stage" => "put_inode")
        .record(start.elapsed().as_nanos() as f64);

    // Delete old object if it is an overwrite request
    // But skip deletion for multipart parts (keys containing '#') to avoid race conditions
    let old_object_bytes = match resp.result.unwrap() {
        nss_codec::put_inode_response::Result::Ok(res) => res,
        nss_codec::put_inode_response::Result::Err(e) => {
            tracing::error!(e);
            return Err(S3Error::InternalError);
        }
    };
    let is_multipart_part = ctx.key.contains('#');
    if !old_object_bytes.is_empty() && !is_multipart_part {
        let old_object = rkyv::from_bytes::<ObjectLayout, Error>(&old_object_bytes)?;
        if let Ok(size) = old_object.size() {
            histogram!("object_size", "operation" => "delete_old_blob").record(size as f64);
        }
        let old_blob_guid = old_object.blob_guid().map_err(|e| {
            tracing::error!("Failed to get blob_guid from old object: {e}");
            S3Error::InternalError
        })?;

        // Only delete old blob if it's different from the new one
        if old_blob_guid != blob_guid {
            let num_blocks = old_object.num_blocks().map_err(|e| {
                tracing::error!("Failed to get num_blocks from old object: {e}");
                S3Error::InternalError
            })?;
            let blob_location = old_object.get_blob_location()?;

            // Send deletion request for each block
            for block_number in 0..num_blocks {
                let request = crate::blob_client::BlobDeletionRequest {
                    tracking_root_blob_name: tracking_root_blob_name.clone(),
                    blob_guid: old_blob_guid,
                    block_number: block_number as u32,
                    location: blob_location,
                };

                if let Err(e) = blob_deletion.send(request).await {
                    tracing::warn!(
                        "Failed to send blob {old_blob_guid} block={block_number} for background deletion: {e}"
                    );
                }
            }
        } else {
            tracing::warn!(
                "Skipping deletion of old blob as it matches new blob GUID: {}",
                blob_guid.blob_id
            );
        }
        histogram!("put_object_handler", "stage" => "send_old_blob_for_deletion")
            .record(start.elapsed().as_nanos() as f64);
    }

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
