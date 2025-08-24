use bytes::Bytes;
use metrics::histogram;
use rpc_client_common::{nss_rpc_retry, rpc_retry};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use crate::{
    handler::{
        common::{
            extract_metadata_headers,
            s3_error::S3Error,
            signature::checksum::{
                request_checksum_value, request_trailer_checksum_algorithm, ExpectedChecksums,
            },
            xheader,
        },
        ObjectRequestContext,
    },
    object_layout::*,
};
use axum::{
    body::Body,
    http::{header, HeaderValue},
    response::Response,
};
use futures::{StreamExt, TryStreamExt};
use rkyv::{self, api::high::to_bytes_in, rancor::Error};
use rpc_client_nss::rpc::put_inode_response;
use uuid::Uuid;

use super::block_data_stream::BlockDataStream;

pub async fn put_object_handler(ctx: ObjectRequestContext) -> Result<Response, S3Error> {
    let bucket = ctx.resolve_bucket().await?;
    let blob_deletion = ctx.app.get_blob_deletion();
    let start = Instant::now();
    let headers = extract_metadata_headers(ctx.request.headers())?;
    let expected_checksums = ExpectedChecksums {
        md5: match ctx.request.headers().get("content-md5") {
            Some(x) => Some(x.to_str()?.to_string()),
            None => None,
        },
        sha256: None,
        extra: request_checksum_value(ctx.request.headers())?,
    };

    let trailer_checksum_algorithm = request_trailer_checksum_algorithm(ctx.request.headers())?;
    let mut req_body = ctx.request.into_body();
    req_body.add_expected_checksums(expected_checksums.clone());
    let (body_stream, checksummer) = req_body.streaming_with_checksums();
    let body_data_stream = Body::from_stream(body_stream).into_data_stream();
    let blob_id = Uuid::now_v7();
    let blob_client = ctx.app.get_blob_client();
    let bucket_name = ctx.bucket_name.clone();
    let size = BlockDataStream::new(body_data_stream, ObjectLayout::DEFAULT_BLOCK_SIZE)
        .enumerate()
        .map(|(i, block_data)| {
            let blob_client = blob_client.clone();
            let bucket_name = bucket_name.clone();
            async move {
                let data = block_data.map_err(|_e| S3Error::InternalError)?;
                let len = data.len() as u64;
                let put_result = blob_client
                    .put_blob(&bucket_name, blob_id, i as u32, data)
                    .await;

                match put_result {
                    Ok(()) => Ok(len),
                    Err(_e) => Err(S3Error::InternalError),
                }
            }
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
    let checksum = match expected_checksums.extra {
        Some(x) => Some(x),
        None => checksum_from_stream.extract(trailer_checksum_algorithm),
    };

    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let etag = blob_id.simple().to_string();
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
    let object_layout_bytes: Bytes = to_bytes_in::<_, Error>(&object_layout, Vec::new())?.into();
    let resp = {
        let start = Instant::now();
        let res = nss_rpc_retry!(
            ctx.app,
            put_inode(
                &bucket.root_blob_name,
                &ctx.key,
                object_layout_bytes.clone(),
                Some(ctx.app.config.rpc_timeout())
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
        if let Ok(size) = old_object.size() {
            histogram!("object_size", "operation" => "delete_old_blob").record(size as f64);
        }
        let blob_id = old_object.blob_id()?;
        let num_blocks = old_object.num_blocks()?;
        if let Err(e) = blob_deletion
            .send((ctx.bucket_name.clone(), blob_id, num_blocks))
            .await
        {
            tracing::warn!(
            "Failed to send blob {blob_id} num_blocks={num_blocks} for background deletion: {e}");
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
