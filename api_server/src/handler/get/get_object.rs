use std::sync::Arc;

use axum::{
    body::{Body, BodyDataStream},
    extract::Query,
    http::{header, HeaderMap, HeaderValue, StatusCode},
    response::Response,
    RequestPartsExt,
};
use bytes::Bytes;
use futures::{stream, StreamExt, TryStreamExt};
use metrics::histogram;
use serde::Deserialize;

use crate::{
    handler::{
        common::{
            get_raw_object, list_raw_objects, mpu_get_part_prefix, object_headers,
            s3_error::S3Error, xheader,
        },
        ObjectRequestContext,
    },
    object_layout::ObjectLayout,
};
use crate::{
    object_layout::{MpuState, ObjectState},
    BlobClient,
};
use crate::{AppState, BlobId};
use bucket_tables::bucket_table::Bucket;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct QueryOpts {
    #[serde(rename(deserialize = "partNumber"))]
    part_number: Option<u32>,
    #[allow(dead_code)]
    #[serde(rename(deserialize = "versionId"))]
    version_id: Option<String>,
    response_cache_control: Option<String>,
    response_content_disposition: Option<String>,
    response_content_encoding: Option<String>,
    response_content_language: Option<String>,
    response_content_type: Option<String>,
    response_expires: Option<String>,
}

#[allow(dead_code)]
#[derive(Debug, Default)]
pub struct HeaderOpts<'a> {
    pub if_match: Option<&'a HeaderValue>,
    pub if_modified_since: Option<&'a HeaderValue>,
    pub if_none_match: Option<&'a HeaderValue>,
    pub if_unmodified_since: Option<&'a HeaderValue>,
    pub range: Option<&'a HeaderValue>,
    pub x_amz_server_side_encryption_customer_algorithm: Option<&'a HeaderValue>,
    pub x_amz_server_side_encryption_customer_key: Option<&'a HeaderValue>,
    pub x_amz_server_side_encryption_customer_key_md5: Option<&'a HeaderValue>,
    pub x_amz_request_payer: Option<&'a HeaderValue>,
    pub x_amz_expected_bucket_owner: Option<&'a HeaderValue>,
    pub x_amz_checksum_mode_enabled: bool,
}

impl<'a> HeaderOpts<'a> {
    pub fn from_headers(headers: &'a HeaderMap) -> Result<Self, S3Error> {
        Ok(Self {
            if_match: headers.get(header::IF_MATCH),
            if_modified_since: headers.get(header::IF_MODIFIED_SINCE),
            if_none_match: headers.get(header::IF_NONE_MATCH),
            if_unmodified_since: headers.get(header::IF_UNMODIFIED_SINCE),
            range: headers.get(header::RANGE),
            x_amz_server_side_encryption_customer_algorithm: headers
                .get(xheader::X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_ALGORITHM),
            x_amz_server_side_encryption_customer_key: headers
                .get(xheader::X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY),
            x_amz_server_side_encryption_customer_key_md5: headers
                .get(xheader::X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY_MD5),
            x_amz_request_payer: headers.get(xheader::X_AMZ_REQUEST_PAYER),
            x_amz_expected_bucket_owner: headers.get(xheader::X_AMZ_EXPECTED_BUCKET_OWNER),
            x_amz_checksum_mode_enabled: headers
                .get(xheader::X_AMZ_CHECKSUM_MODE)
                .map(|x| x == "ENABLED")
                .unwrap_or(false),
        })
    }
}

pub async fn get_object_handler(ctx: ObjectRequestContext) -> Result<Response, S3Error> {
    let bucket = ctx.resolve_bucket().await?;
    let mut parts = ctx.request.into_parts().0;
    let Query(query_opts): Query<QueryOpts> = parts.extract().await?;
    let header_opts = HeaderOpts::from_headers(&parts.headers)?;
    let object = get_raw_object(&ctx.app, &bucket.root_blob_name, &ctx.key).await?;
    let total_size = object.size()?;
    histogram!("object_size", "operation" => "get").record(total_size as f64);
    let range = parse_range_header(header_opts.range, total_size)?;
    let checksum_mode_enabled = header_opts.x_amz_checksum_mode_enabled;
    match (query_opts.part_number, range) {
        (_, None) => {
            let (body, body_size) = get_object_content(
                ctx.app,
                &bucket,
                &ctx.bucket_name,
                &object,
                ctx.key,
                query_opts.part_number,
            )
            .await?;

            let mut resp = Response::new(body);
            object_headers(&mut resp, &object, checksum_mode_enabled)?;
            resp.headers_mut().insert(
                header::CONTENT_LENGTH,
                HeaderValue::from_str(&body_size.to_string())?,
            );

            override_headers(&mut resp, &query_opts)?;
            Ok(resp)
        }

        (None, Some(range)) => {
            let body = get_object_range_content(
                ctx.app,
                &bucket,
                &ctx.bucket_name,
                &object,
                ctx.key,
                &range,
            )
            .await?;

            let mut resp = Response::new(body);
            resp.headers_mut().insert(
                header::CONTENT_LENGTH,
                HeaderValue::from_str(&format!("{}", range.end - range.start))?,
            );
            resp.headers_mut().insert(
                header::CONTENT_RANGE,
                HeaderValue::from_str(&format!(
                    "bytes {}-{}/{}",
                    range.start,
                    range.end - 1,
                    total_size
                ))?,
            );
            *resp.status_mut() = StatusCode::PARTIAL_CONTENT;
            Ok(resp)
        }

        (Some(_), Some(_)) => Err(S3Error::InvalidArgument1),
    }
}

pub fn override_headers(resp: &mut Response, query_opts: &QueryOpts) -> Result<(), S3Error> {
    // override headers, see https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html
    let overrides = [
        (header::CACHE_CONTROL, &query_opts.response_cache_control),
        (
            header::CONTENT_DISPOSITION,
            &query_opts.response_content_disposition,
        ),
        (
            header::CONTENT_ENCODING,
            &query_opts.response_content_encoding,
        ),
        (
            header::CONTENT_LANGUAGE,
            &query_opts.response_content_language,
        ),
        (header::CONTENT_TYPE, &query_opts.response_content_type),
        (header::EXPIRES, &query_opts.response_expires),
    ];

    for (hdr, val_opt) in overrides {
        if let Some(val) = val_opt {
            let val = val.try_into()?;
            resp.headers_mut().insert(hdr, val);
        }
    }

    Ok(())
}

pub async fn get_object_content(
    app: Arc<AppState>,
    bucket: &Bucket,
    bucket_name: &str,
    object: &ObjectLayout,
    key: String,
    part_number: Option<u32>,
) -> Result<(Body, u64), S3Error> {
    let blob_client = app.get_blob_client();
    match object.state {
        ObjectState::Normal(ref _obj_data) => {
            let blob_id = object.blob_id()?;
            let num_blocks = object.num_blocks()?;
            let size = object.size()?;
            let body = get_full_blob_stream(blob_client, bucket_name, blob_id, num_blocks).await?;
            Ok((body, size))
        }
        ObjectState::Mpu(ref mpu_state) => match mpu_state {
            MpuState::Uploading => {
                tracing::warn!("invalid mpu state: Uploading");
                Err(S3Error::InvalidObjectState)
            }
            MpuState::Aborted => {
                tracing::warn!("invalid mpu state: Aborted");
                Err(S3Error::InvalidObjectState)
            }
            MpuState::Completed(core_meta_data) => {
                let mpu_prefix = mpu_get_part_prefix(key, 0);
                let mut mpus = list_raw_objects(
                    &app,
                    &bucket.root_blob_name,
                    10000,
                    &mpu_prefix,
                    "",
                    "",
                    false,
                )
                .await?;
                // Do filtering if there is part_number option
                let (mpus_iter, body_size) = match part_number {
                    None => (mpus.into_iter(), core_meta_data.size),
                    Some(n) => {
                        let mpu_obj = mpus.swap_remove(n as usize - 1);
                        let mpu_size = mpu_obj.1.size()?;
                        (vec![mpu_obj].into_iter(), mpu_size)
                    }
                };
                let bucket_name_clone = bucket_name.to_string();
                let body_stream = futures::stream::iter(mpus_iter)
                    .then(move |(_key, mpu_obj)| {
                        let blob_client = blob_client.clone();
                        let bucket_name = bucket_name_clone.clone();
                        async move {
                            let blob_id = mpu_obj.blob_id().map_err(axum::Error::new)?;
                            let num_blocks = mpu_obj.num_blocks().map_err(axum::Error::new)?;
                            get_full_blob_stream(blob_client, &bucket_name, blob_id, num_blocks)
                                .await
                                .map_err(axum::Error::new)
                        }
                    })
                    .map_ok(|body| body.into_data_stream())
                    .try_flatten();
                Ok((Body::from_stream(body_stream), body_size))
            }
        },
    }
}

async fn get_object_range_content(
    app: Arc<AppState>,
    bucket: &Bucket,
    bucket_name: &str,
    object: &ObjectLayout,
    key: String,
    range: &std::ops::Range<usize>,
) -> Result<Body, S3Error> {
    let blob_client = app.get_blob_client();
    let block_size = object.block_size as usize;
    match object.state {
        ObjectState::Normal(ref _obj_data) => {
            let blob_id = object.blob_id()?;
            let body_stream = get_range_blob_stream(
                blob_client,
                bucket_name,
                blob_id,
                block_size,
                range.start,
                range.end,
            )
            .await;
            Ok(Body::from_stream(body_stream))
        }
        ObjectState::Mpu(ref mpu_state) => match mpu_state {
            MpuState::Uploading => {
                tracing::warn!("invalid mpu state: Uploading");
                Err(S3Error::InvalidObjectState)
            }
            MpuState::Aborted => {
                tracing::warn!("invalid mpu state: Aborted");
                Err(S3Error::InvalidObjectState)
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

                let mut mpu_blobs: Vec<(BlobId, usize, usize)> = Vec::new();
                let mut obj_offset = 0;
                for (_mpu_key, mpu_obj) in mpus {
                    let mpu_size = mpu_obj.size()? as usize;
                    if obj_offset >= range.end {
                        break;
                    }
                    // with intersection
                    if obj_offset < range.end && obj_offset + mpu_size > range.start {
                        let blob_start = range.start.saturating_sub(obj_offset);
                        let blob_end = if range.end > obj_offset + mpu_size {
                            mpu_size - blob_start
                        } else {
                            range.end - obj_offset
                        };
                        mpu_blobs.push((mpu_obj.blob_id()?, blob_start, blob_end));
                    }
                    obj_offset += mpu_size;
                }

                let bucket_name_clone = bucket_name.to_string();
                let body_stream = futures::stream::iter(mpu_blobs.into_iter())
                    .then(move |(blob_id, blob_start, blob_end)| {
                        let blob_client = blob_client.clone();
                        let bucket_name = bucket_name_clone.clone();
                        async move {
                            Ok(get_range_blob_stream(
                                blob_client,
                                &bucket_name,
                                blob_id,
                                block_size,
                                blob_start,
                                blob_end,
                            )
                            .await)
                        }
                    })
                    .try_flatten();
                Ok(Body::from_stream(body_stream))
            }
        },
    }
}

async fn get_full_blob_stream(
    blob_client: Arc<BlobClient>,
    bucket_name: &str,
    blob_id: BlobId,
    num_blocks: usize,
) -> Result<Body, S3Error> {
    if num_blocks == 0 {
        return Ok(Body::empty());
    }

    // Get the first block
    let mut first_block = Bytes::new();
    blob_client
        .get_blob(bucket_name, blob_id, 0, &mut first_block)
        .await
        .map_err(|e| {
            tracing::error!(%blob_id, block_number=0, error=?e, "failed to get blob");
            S3Error::from(e)
        })?;

    if num_blocks == 1 {
        return Ok(Body::from(first_block));
    }

    // Stream for remaining blocks
    let bucket_name = bucket_name.to_string();
    let remaining_stream = stream::iter(1..num_blocks).then(move |i| {
        let blob_client = blob_client.clone();
        let bucket_name = bucket_name.clone();
        async move {
            let mut block = Bytes::new();
            match blob_client
                .get_blob(&bucket_name, blob_id, i as u32, &mut block)
                .await
            {
                Err(e) => {
                    tracing::error!(%blob_id, block_number=i, error=?e, "failed to get blob");
                    Err(S3Error::from(e))
                }
                Ok(_) => Ok(block),
            }
        }
    });

    let full_stream = stream::once(async { Ok(first_block) }).chain(remaining_stream);

    Ok(Body::from_stream(full_stream.map_err(axum::Error::new)))
}

async fn get_range_blob_stream(
    blob_client: Arc<BlobClient>,
    bucket_name: &str,
    blob_id: BlobId,
    block_size: usize,
    start: usize,
    end: usize,
) -> BodyDataStream {
    let start_block_i = start / block_size;
    let blob_offset: usize = block_size * start_block_i;
    let bucket_name = bucket_name.to_string();
    let body_stream = futures::stream::iter(start_block_i..)
        .then(move |i| {
            let blob_client = blob_client.clone();
            let bucket_name = bucket_name.clone();
            async move {
                let mut block = Bytes::new();
                match blob_client
                    .get_blob(&bucket_name, blob_id, i as u32, &mut block)
                    .await
                {
                    Err(e) => {
                        tracing::error!(%blob_id, block_number=i, error=?e, "failed to get blob");
                        Err(axum::Error::new(e))
                    }
                    Ok(_) => Ok(Body::from(block).into_data_stream()),
                }
            }
        })
        .try_flatten()
        .scan(blob_offset, move |chunk_offset, chunk| {
            let r = match chunk {
                Ok(chunk_bytes) => {
                    let chunk_len = chunk_bytes.len();
                    let r = if *chunk_offset >= end {
                        // The current chunk is after the part we want to read.
                        // Returning None here will stop the scan, the rest of the
                        // stream will be ignored
                        None
                    } else if *chunk_offset + chunk_len <= start {
                        // The current chunk is before the part we want to read.
                        // We return a None that will be removed by the filter_map
                        // below.
                        Some(None)
                    } else {
                        // The chunk has an intersection with the requested range
                        let start_in_chunk = start.saturating_sub(*chunk_offset);
                        let end_in_chunk = if *chunk_offset + chunk_len < end {
                            chunk_len
                        } else {
                            end - *chunk_offset
                        };
                        Some(Some(Ok(chunk_bytes.slice(start_in_chunk..end_in_chunk))))
                    };
                    *chunk_offset += chunk_bytes.len();
                    r
                }
                Err(e) => Some(Some(Err(e))),
            };
            futures::future::ready(r)
        })
        .filter_map(futures::future::ready);

    Body::from_stream(body_stream).into_data_stream()
}

fn parse_range_header(
    range_header: Option<&HeaderValue>,
    total_size: u64,
) -> Result<Option<std::ops::Range<usize>>, S3Error> {
    let range = match range_header {
        Some(range) => {
            let range_str = range.to_str()?;
            let mut ranges = http_range::HttpRange::parse(range_str, total_size)?;
            if ranges.len() > 1 {
                // Amazon S3 doesn't support retrieving multiple ranges of data per GET request.
                tracing::debug!("Found more than one ranges: {range_str}");
                return Err(S3Error::InvalidRange);
            } else {
                ranges.pop().map(|http_range| {
                    http_range.start as usize..(http_range.start + http_range.length) as usize
                })
            }
        }
        None => None,
    };
    Ok(range)
}
