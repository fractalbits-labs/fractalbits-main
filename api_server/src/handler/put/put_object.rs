use metrics::histogram;
use rpc_client_common::nss_rpc_retry;
use std::hash::Hasher;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use actix_web::{http::header, HttpResponse};
use crc32c::Crc32cHasher as Crc32c;
use crc32fast::Hasher as Crc32;
use futures::{StreamExt, TryStreamExt};
use nss_codec::put_inode_response;
use rkyv::{self, api::high::to_bytes_in, rancor::Error};
use sha1::Sha1;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use super::block_data_stream::BlockDataStream;
use super::s3_streaming::S3StreamingPayload;
use crate::{
    handler::{
        common::{
            buffer_payload_with_capacity,
            checksum::{self, ChecksumAlgorithm, ChecksumValue},
            extract_metadata_headers, gen_etag,
            s3_error::S3Error,
        },
        ObjectRequestContext,
    },
    object_layout::*,
};

pub async fn put_object_handler(ctx: ObjectRequestContext) -> Result<HttpResponse, S3Error> {
    // Debug: log all request headers to understand what's being sent
    tracing::debug!("PUT object request headers:");
    for (name, value) in ctx.request.headers().iter() {
        tracing::debug!("  {}: {:?}", name, value);
    }

    // Decide whether to use streaming based on request characteristics
    if should_use_streaming(&ctx.request) {
        tracing::debug!("Using streaming path for PUT object");
        put_object_streaming_internal(ctx).await
    } else {
        tracing::debug!("Using buffered path for PUT object");
        put_object_with_no_trailer(ctx).await
    }
}

// Helper function to calculate checksum for the given body data
fn calculate_checksum_for_body(
    body: &actix_web::web::Bytes,
    expected_checksum: &Option<ChecksumValue>,
) -> Result<Option<ChecksumValue>, S3Error> {
    match expected_checksum {
        Some(ChecksumValue::Crc32(expected_bytes)) => {
            let mut hasher = Crc32::new();
            hasher.write(body);
            let calculated = hasher.finalize().to_be_bytes();

            // Verify against expected if provided
            if calculated != *expected_bytes {
                tracing::error!(
                    "CRC32 checksum mismatch: expected {:?}, calculated {:?}",
                    expected_bytes,
                    calculated
                );
                return Err(S3Error::InvalidDigest);
            }
            Ok(Some(ChecksumValue::Crc32(calculated)))
        }
        Some(ChecksumValue::Crc32c(expected_bytes)) => {
            let mut hasher = Crc32c::new(0);
            hasher.write(body);
            let calculated = (hasher.finish() as u32).to_be_bytes();

            if calculated != *expected_bytes {
                tracing::error!(
                    "CRC32C checksum mismatch: expected {:?}, calculated {:?}",
                    expected_bytes,
                    calculated
                );
                return Err(S3Error::InvalidDigest);
            }
            Ok(Some(ChecksumValue::Crc32c(calculated)))
        }
        Some(ChecksumValue::Crc64Nvme(expected_bytes)) => {
            let mut hasher = crc64fast_nvme::Digest::new();
            hasher.write(body);
            let calculated = hasher.sum64().to_be_bytes();

            if calculated != *expected_bytes {
                tracing::error!(
                    "CRC64NVME checksum mismatch: expected {:?}, calculated {:?}",
                    expected_bytes,
                    calculated
                );
                return Err(S3Error::InvalidDigest);
            }
            Ok(Some(ChecksumValue::Crc64Nvme(calculated)))
        }
        Some(ChecksumValue::Sha1(expected_bytes)) => {
            let mut hasher = Sha1::new();
            hasher.update(body);
            let calculated: [u8; 20] = hasher.finalize().into();

            if calculated != *expected_bytes {
                tracing::error!(
                    "SHA1 checksum mismatch: expected {:?}, calculated {:?}",
                    expected_bytes,
                    calculated
                );
                return Err(S3Error::InvalidDigest);
            }
            Ok(Some(ChecksumValue::Sha1(calculated)))
        }
        Some(ChecksumValue::Sha256(expected_bytes)) => {
            let mut hasher = Sha256::new();
            hasher.update(body);
            let calculated: [u8; 32] = hasher.finalize().into();

            if calculated != *expected_bytes {
                tracing::error!(
                    "SHA256 checksum mismatch: expected {:?}, calculated {:?}",
                    expected_bytes,
                    calculated
                );
                return Err(S3Error::InvalidDigest);
            }
            Ok(Some(ChecksumValue::Sha256(calculated)))
        }
        None => Ok(None),
    }
}

// Helper function to calculate checksum for the given body data using specific algorithm
fn calculate_checksum_for_body_with_algorithm(
    body: &actix_web::web::Bytes,
    algorithm: ChecksumAlgorithm,
) -> Result<Option<ChecksumValue>, S3Error> {
    match algorithm {
        ChecksumAlgorithm::Crc32 => {
            let mut hasher = Crc32::new();
            hasher.write(body);
            let calculated = hasher.finalize().to_be_bytes();
            Ok(Some(ChecksumValue::Crc32(calculated)))
        }
        ChecksumAlgorithm::Crc32c => {
            let mut hasher = Crc32c::new(0);
            hasher.write(body);
            let calculated = (hasher.finish() as u32).to_be_bytes();
            Ok(Some(ChecksumValue::Crc32c(calculated)))
        }
        ChecksumAlgorithm::Crc64Nvme => {
            let mut hasher = crc64fast_nvme::Digest::new();
            hasher.write(body);
            let calculated = hasher.sum64().to_be_bytes();
            Ok(Some(ChecksumValue::Crc64Nvme(calculated)))
        }
        ChecksumAlgorithm::Sha1 => {
            let mut hasher = Sha1::new();
            hasher.update(body);
            let calculated: [u8; 20] = hasher.finalize().into();
            Ok(Some(ChecksumValue::Sha1(calculated)))
        }
        ChecksumAlgorithm::Sha256 => {
            let mut hasher = Sha256::new();
            hasher.update(body);
            let calculated: [u8; 32] = hasher.finalize().into();
            Ok(Some(ChecksumValue::Sha256(calculated)))
        }
    }
}

// Helper function to decide whether to use streaming based on request
fn should_use_streaming(request: &actix_web::HttpRequest) -> bool {
    // Always stream if trailers present (requires streaming to extract them)
    if request.headers().get("x-amz-trailer").is_some() {
        tracing::debug!("Streaming due to x-amz-trailer header");
        return true;
    }

    // Always stream if checksum algorithm is specified (for streaming checksum calculation)
    if request.headers().get("x-amz-checksum-algorithm").is_some() {
        tracing::debug!("Streaming due to x-amz-checksum-algorithm header");
        return true;
    }

    // Always stream for chunked transfer encoding (which AWS SDK uses for streaming checksums)
    if let Some(transfer_encoding) = request.headers().get("transfer-encoding") {
        if let Ok(encoding) = transfer_encoding.to_str() {
            if encoding.to_lowercase().contains("chunked") {
                tracing::debug!("Streaming due to chunked transfer-encoding");
                return true;
            }
        }
    }

    // Get content length for size-based decisions
    let size = if let Some(cl) = request.headers().get("content-length") {
        cl.to_str()
            .unwrap_or("0")
            .parse::<usize>()
            .unwrap_or(usize::MAX)
    } else {
        usize::MAX // Unknown size, assume large
    };
    // Check content-length for larger objects
    if size != usize::MAX {
        // Use streaming for objects >= 1 block size
        let should_stream = size >= ObjectLayout::DEFAULT_BLOCK_SIZE as usize;
        tracing::debug!(
            "Content-Length: {}, DEFAULT_BLOCK_SIZE: {}, streaming: {}",
            size,
            ObjectLayout::DEFAULT_BLOCK_SIZE,
            should_stream
        );
        return should_stream;
    }

    // Stream if size is unknown (no content-length header)
    tracing::debug!("Streaming due to missing content-length");
    true
}

// Internal streaming handler that processes chunks as they arrive
async fn put_object_streaming_internal(ctx: ObjectRequestContext) -> Result<HttpResponse, S3Error> {
    let start = Instant::now();

    tracing::debug!(
        "PutObject streaming handler: {}/{}, starting streaming upload",
        ctx.bucket_name,
        ctx.key,
    );

    // Resolve bucket to get bucket details
    let bucket_obj = ctx.resolve_bucket().await?;
    let tracking_root_blob_name = bucket_obj.tracking_root_blob_name.clone();

    // Extract metadata headers
    let headers = extract_metadata_headers(ctx.request.headers())?;

    // Create S3 streaming payload with checksum calculation
    let payload = ctx.payload;
    let (s3_payload, checksum_future) = S3StreamingPayload::with_checksums(payload, &ctx.request)?;

    // Create blob ID for this upload
    let blob_id = Uuid::now_v7();
    let blob_client = ctx.app.get_blob_client();

    // Convert S3 payload to block stream
    let block_stream = BlockDataStream::new(s3_payload, ObjectLayout::DEFAULT_BLOCK_SIZE);

    // Process blocks as they arrive, uploading them concurrently
    let size = block_stream
        .enumerate()
        .map(|(i, block_result)| {
            let blob_client = blob_client.clone();
            let tracking_root_blob_name = tracking_root_blob_name.clone();

            async move {
                let data = block_result.map_err(|e| {
                    tracing::error!("Stream error: {}", e);
                    S3Error::InternalError
                })?;

                let len = data.len() as u64;
                let put_result = blob_client
                    .put_blob(tracking_root_blob_name.as_deref(), blob_id, i as u32, data)
                    .await;

                match put_result {
                    Ok(()) => Ok(len),
                    Err(e) => {
                        tracing::error!("Failed to store blob block {}: {}", i, e);
                        Err(S3Error::InternalError)
                    }
                }
            }
        })
        .buffer_unordered(5) // Process up to 5 blocks concurrently
        .try_fold(0u64, |acc, len| async move { Ok(acc + len) })
        .await
        .map_err(|_| S3Error::InternalError)?;

    let total_size = size;

    histogram!("object_size", "operation" => "put").record(total_size as f64);
    histogram!("put_object_handler", "stage" => "put_blob")
        .record(start.elapsed().as_nanos() as f64);

    // Await checksum calculation completion
    let calculated_checksum = match checksum_future.await {
        Ok(Ok(checksums)) => {
            tracing::debug!("Streaming checksum verification completed successfully");
            // Extract the appropriate checksum value based on what was calculated
            // For now, we'll try to extract from the first available algorithm
            if checksums.crc32.is_some() {
                Some(ChecksumValue::Crc32(checksums.crc32.unwrap()))
            } else if checksums.crc32c.is_some() {
                Some(ChecksumValue::Crc32c(checksums.crc32c.unwrap()))
            } else if checksums.crc64nvme.is_some() {
                Some(ChecksumValue::Crc64Nvme(checksums.crc64nvme.unwrap()))
            } else if checksums.sha1.is_some() {
                Some(ChecksumValue::Sha1(checksums.sha1.unwrap()))
            } else if checksums.sha256.is_some() {
                Some(ChecksumValue::Sha256(checksums.sha256.unwrap()))
            } else {
                None
            }
        }
        Ok(Err(e)) => {
            tracing::error!("Checksum verification failed: {:?}", e);
            return Err(e);
        }
        Err(e) => {
            tracing::error!("Checksum calculation task failed: {:?}", e);
            return Err(S3Error::InternalError);
        }
    };

    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let etag = gen_etag();
    let version_id = gen_version_id();

    // Create object layout
    let object_layout = ObjectLayout {
        version_id,
        block_size: ObjectLayout::DEFAULT_BLOCK_SIZE,
        timestamp,
        state: ObjectState::Normal(ObjectMetaData {
            blob_id,
            core_meta_data: ObjectCoreMetaData {
                size: total_size,
                etag: etag.clone(),
                headers,
                checksum: calculated_checksum,
            },
        }),
    };

    // Serialize and store object metadata
    let object_layout_bytes: bytes::Bytes = to_bytes_in::<_, Error>(&object_layout, Vec::new())
        .map_err(|e| {
            tracing::error!("Failed to serialize object layout: {e}");
            S3Error::InternalError
        })?
        .into();

    // Store object metadata in NSS
    let resp = nss_rpc_retry!(
        ctx.app,
        put_inode(
            &bucket_obj.root_blob_name,
            &ctx.key,
            object_layout_bytes.clone(),
            Some(ctx.app.config.rpc_timeout())
        )
    )
    .await
    .map_err(|e| {
        tracing::error!("Failed to store object metadata: {e}");
        S3Error::InternalError
    })?;

    // Delete old object if it is an overwrite request
    let old_object_bytes = match resp.result.unwrap() {
        put_inode_response::Result::Ok(res) => res,
        put_inode_response::Result::Err(e) => {
            tracing::error!("put_inode error: {}", e);
            return Err(S3Error::InternalError);
        }
    };

    if !old_object_bytes.is_empty() {
        let old_object =
            rkyv::from_bytes::<ObjectLayout, Error>(&old_object_bytes).map_err(|e| {
                tracing::error!("Failed to deserialize old object layout: {e}");
                S3Error::InternalError
            })?;

        if let Ok(size) = old_object.size() {
            histogram!("object_size", "operation" => "delete_old_blob").record(size as f64);
        }
        let blob_id = old_object.blob_id().map_err(|e| {
            tracing::error!("Failed to get blob_id from old object: {e}");
            S3Error::InternalError
        })?;
        let num_blocks = old_object.num_blocks().map_err(|e| {
            tracing::error!("Failed to get num_blocks from old object: {e}");
            S3Error::InternalError
        })?;

        let blob_deletion = ctx.app.get_blob_deletion();
        if let Err(e) = blob_deletion
            .send((
                bucket_obj.tracking_root_blob_name.clone(),
                blob_id,
                num_blocks,
            ))
            .await
        {
            tracing::warn!(
                "Failed to send blob {blob_id} num_blocks={num_blocks} for background deletion: {e}"
            );
        }
    }

    histogram!("put_object_handler", "stage" => "done").record(start.elapsed().as_nanos() as f64);

    tracing::debug!(
        "Successfully stored object {}/{} with size {} via streaming",
        ctx.bucket_name,
        ctx.key,
        total_size
    );

    Ok(HttpResponse::Ok()
        .insert_header((header::ETAG, etag))
        .insert_header(("X-Amz-Object-Size", total_size.to_string()))
        .finish())
}

// Helper function for buffered upload with pre-resolved bucket
async fn put_object_with_no_trailer(ctx: ObjectRequestContext) -> Result<HttpResponse, S3Error> {
    let bucket = ctx.resolve_bucket().await?;
    let expected_size = ctx
        .request
        .headers()
        .get("content-length")
        .and_then(|h| h.to_str().ok())
        .and_then(|s| s.parse().ok());
    // Buffer the entire payload for small objects with pre-allocation
    let body = buffer_payload_with_capacity(ctx.payload, expected_size).await?;

    tracing::debug!(
        "PutObject actix handler with resolved bucket: {}/{}, body size: {}",
        ctx.bucket_name,
        ctx.key,
        body.len()
    );

    // Extract metadata headers
    let headers = extract_metadata_headers(ctx.request.headers())?;

    // Extract expected checksum from headers
    let expected_checksum = checksum::request_checksum_value(ctx.request.headers())?;

    // Check if there's a trailer checksum algorithm specified
    let trailer_algo = checksum::request_trailer_checksum_algorithm(ctx.request.headers())?;

    // Calculate checksums if expected or if trailer algo is specified
    let calculated_checksum = if expected_checksum.is_some() {
        calculate_checksum_for_body(&body, &expected_checksum)?
    } else if let Some(algo) = trailer_algo {
        calculate_checksum_for_body_with_algorithm(&body, algo)?
    } else {
        None
    };

    // Store data in chunks
    let blob_id = Uuid::now_v7();
    let blob_client = ctx.app.get_blob_client();
    let size = body.len() as u64;
    let block_size = ObjectLayout::DEFAULT_BLOCK_SIZE as usize;

    // If the body is small, store as single block, otherwise chunk it
    let tracking_root_blob_name = bucket.tracking_root_blob_name.as_deref();
    if body.len() <= block_size {
        blob_client
            .put_blob(tracking_root_blob_name, blob_id, 0, body.clone())
            .await
            .map_err(|e| {
                tracing::error!("Failed to store blob: {e}");
                S3Error::InternalError
            })?;
    } else {
        let chunks = body.chunks(block_size);
        let mut futures = Vec::new();

        for (block_num, chunk) in chunks.enumerate() {
            let blob_client = blob_client.clone();
            let chunk_bytes = bytes::Bytes::copy_from_slice(chunk);

            let future = async move {
                blob_client
                    .put_blob(
                        tracking_root_blob_name,
                        blob_id,
                        block_num as u32,
                        chunk_bytes,
                    )
                    .await
                    .map_err(|e| {
                        tracing::error!("Failed to store blob block {}: {e}", block_num);
                        S3Error::InternalError
                    })
            };
            futures.push(future);
        }

        let results: Vec<Result<(), S3Error>> = futures::future::join_all(futures).await;
        for result in results {
            result?;
        }
    }

    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let etag = gen_etag();
    let version_id = gen_version_id();

    // Create object layout with calculated checksum
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
                checksum: calculated_checksum,
            },
        }),
    };

    // Serialize and store object metadata
    let object_layout_bytes: bytes::Bytes = to_bytes_in::<_, Error>(&object_layout, Vec::new())
        .map_err(|e| {
            tracing::error!("Failed to serialize object layout: {e}");
            S3Error::InternalError
        })?
        .into();

    // Store object metadata in NSS using the resolved bucket
    let resp = nss_rpc_retry!(
        ctx.app,
        put_inode(
            &bucket.root_blob_name,
            &ctx.key,
            object_layout_bytes.clone(),
            Some(ctx.app.config.rpc_timeout())
        )
    )
    .await
    .map_err(|e| {
        tracing::error!("Failed to store object metadata: {e}");
        S3Error::InternalError
    })?;

    // Delete old object if it is an overwrite request
    let old_object_bytes = match resp.result.unwrap() {
        put_inode_response::Result::Ok(res) => res,
        put_inode_response::Result::Err(e) => {
            tracing::error!("put_inode error: {}", e);
            return Err(S3Error::InternalError);
        }
    };
    if !old_object_bytes.is_empty() {
        let old_object =
            rkyv::from_bytes::<ObjectLayout, Error>(&old_object_bytes).map_err(|e| {
                tracing::error!("Failed to deserialize old object layout: {e}");
                S3Error::InternalError
            })?;

        if let Ok(size) = old_object.size() {
            histogram!("object_size", "operation" => "delete_old_blob").record(size as f64);
        }
        let blob_id = old_object.blob_id().map_err(|e| {
            tracing::error!("Failed to get blob_id from old object: {e}");
            S3Error::InternalError
        })?;
        let num_blocks = old_object.num_blocks().map_err(|e| {
            tracing::error!("Failed to get num_blocks from old object: {e}");
            S3Error::InternalError
        })?;

        let blob_deletion = ctx.app.get_blob_deletion();
        if let Err(e) = blob_deletion
            .send((bucket.tracking_root_blob_name.clone(), blob_id, num_blocks))
            .await
        {
            tracing::warn!(
                "Failed to send blob {blob_id} num_blocks={num_blocks} for background deletion: {e}"
            );
        }
    }

    tracing::debug!(
        "Successfully stored object {}/{} with size {}",
        ctx.bucket_name,
        ctx.key,
        size
    );

    Ok(HttpResponse::Ok()
        .insert_header((header::ETAG, etag))
        .insert_header(("X-Amz-Object-Size", size.to_string()))
        .finish())
}
