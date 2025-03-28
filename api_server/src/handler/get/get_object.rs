use std::sync::Arc;

use axum::{
    body::{Body, BodyDataStream},
    extract::Query,
    http::{header, HeaderMap, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    RequestPartsExt,
};
use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use http_range::HttpRange;
use rpc_client_bss::RpcClientBss;
use rpc_client_nss::RpcClientNss;
use serde::Deserialize;

use crate::BlobId;
use crate::{
    handler::common::signature::checksum::ChecksumValue,
    object_layout::{MpuState, ObjectState},
};
use crate::{
    handler::{
        common::{
            get_raw_object, list_raw_objects, mpu_get_part_prefix,
            s3_error::S3Error,
            signature::checksum::{add_checksum_response_headers, X_AMZ_CHECKSUM_MODE},
        },
        Request,
    },
    object_layout::ObjectLayout,
};
use bucket_tables::bucket_table::Bucket;

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct QueryOpts {
    #[serde(rename(deserialize = "partNumber"))]
    part_number: Option<u32>,
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
struct HeaderOpts<'a> {
    if_match: Option<&'a HeaderValue>,
    if_modified_since: Option<&'a HeaderValue>,
    if_none_match: Option<&'a HeaderValue>,
    if_unmodified_since: Option<&'a HeaderValue>,
    range: Option<&'a HeaderValue>,
    x_amz_server_side_encryption_customer_algorithm: Option<&'a HeaderValue>,
    x_amz_server_side_encryption_customer_key: Option<&'a HeaderValue>,
    x_amz_server_side_encryption_customer_key_md5: Option<&'a HeaderValue>,
    x_amz_request_payer: Option<&'a HeaderValue>,
    x_amz_expected_bucket_owner: Option<&'a HeaderValue>,
    x_amz_checksum_mode_enabled: bool,
}

impl<'a> HeaderOpts<'a> {
    fn from_headers(headers: &'a HeaderMap) -> Result<Self, S3Error> {
        Ok(Self {
            if_match: headers.get(header::IF_MATCH),
            if_modified_since: headers.get(header::IF_MODIFIED_SINCE),
            if_none_match: headers.get(header::IF_NONE_MATCH),
            if_unmodified_since: headers.get(header::IF_UNMODIFIED_SINCE),
            range: headers.get(header::RANGE),
            x_amz_server_side_encryption_customer_algorithm: headers
                .get("x-amz-server-side-encryption-customer-algorithm"),
            x_amz_server_side_encryption_customer_key: headers
                .get("x-amz-server-side-encryption-customer-key"),
            x_amz_server_side_encryption_customer_key_md5: headers
                .get("x-amz-server-side-encryption-customer-key-MD5"),
            x_amz_request_payer: headers.get("x-amz-request-payer"),
            x_amz_expected_bucket_owner: headers.get("x-amz-expected-bucket-owner"),
            x_amz_checksum_mode_enabled: headers
                .get(X_AMZ_CHECKSUM_MODE)
                .map(|x| x == "ENABLED")
                .unwrap_or(false),
        })
    }
}

pub async fn get_object_handler(
    request: Request,
    bucket: &Bucket,
    key: String,
    rpc_client_nss: &RpcClientNss,
    rpc_client_bss: Arc<RpcClientBss>,
) -> Result<Response, S3Error> {
    let mut parts = request.into_parts().0;
    let Query(opts): Query<QueryOpts> = parts.extract().await?;
    let header_opts = HeaderOpts::from_headers(&parts.headers)?;
    let object = get_raw_object(rpc_client_nss, bucket.root_blob_name.clone(), key.clone()).await?;
    let total_size = object.size()?;
    let range = parse_range_header(header_opts.range, total_size)?;
    let checksum_mode_enabled = header_opts.x_amz_checksum_mode_enabled;
    match (opts.part_number, range) {
        (_, None) => {
            let (body, body_size, checksum) = get_object_content(
                bucket,
                &object,
                key,
                opts.part_number,
                rpc_client_nss,
                rpc_client_bss,
            )
            .await?;

            let mut resp = body.into_response();
            resp.headers_mut().insert(
                header::CONTENT_LENGTH,
                HeaderValue::from_str(&body_size.to_string())?,
            );
            if checksum_mode_enabled {
                tracing::debug!("checksum_mode enabled, adding checksum: {:?}", checksum);
                add_checksum_response_headers(&checksum, &mut resp)?;
            }
            Ok(resp)
        }

        (None, Some(range)) => {
            let body_bytes = get_object_range_content(
                bucket,
                &object,
                key,
                range,
                rpc_client_nss,
                rpc_client_bss,
            )
            .await?;

            let mut resp = body_bytes.into_response();
            resp.headers_mut().insert(
                header::CONTENT_LENGTH,
                HeaderValue::from_str(&format!("{}", range.length))?,
            );
            resp.headers_mut().insert(
                header::CONTENT_RANGE,
                HeaderValue::from_str(&format!(
                    "bytes {}-{}/{}",
                    range.start,
                    range.start + range.length - 1,
                    total_size
                ))?,
            );
            *resp.status_mut() = StatusCode::PARTIAL_CONTENT;
            Ok(resp)
        }

        (Some(_), Some(_)) => Err(S3Error::InvalidArgument1),
    }
}

pub async fn get_object_content(
    bucket: &Bucket,
    object: &ObjectLayout,
    key: String,
    part_number: Option<u32>,
    rpc_client_nss: &RpcClientNss,
    rpc_client_bss: Arc<RpcClientBss>,
) -> Result<(Body, u64, Option<ChecksumValue>), S3Error> {
    match object.state {
        ObjectState::Normal(ref obj_data) => {
            let blob_id = object.blob_id()?;
            let num_blocks = object.num_blocks()?;
            let size = object.size()?;
            let body_stream = get_full_blob_stream(rpc_client_bss, blob_id, num_blocks).await;
            Ok((Body::from_stream(body_stream), size, obj_data.checksum))
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
            MpuState::Completed { size, etag: _ } => {
                let mpu_prefix = mpu_get_part_prefix(key.clone(), 0);
                let mut mpus = list_raw_objects(
                    bucket.root_blob_name.clone(),
                    rpc_client_nss,
                    10000,
                    mpu_prefix,
                    "".into(),
                    false,
                )
                .await?;
                // Do filtering if there is part_number option
                let (mpus_iter, body_size) = match part_number {
                    None => (mpus.into_iter(), *size),
                    Some(n) => {
                        let mpu_obj = mpus.swap_remove(n as usize - 1);
                        let mpu_size = mpu_obj.1.size()?;
                        (vec![mpu_obj].into_iter(), mpu_size)
                    }
                };
                let body_stream = futures::stream::iter(mpus_iter)
                    .then(move |(_key, mpu_obj)| {
                        let rpc_client_bss = rpc_client_bss.clone();
                        async move {
                            let blob_id = match mpu_obj.blob_id() {
                                Ok(blob_id) => blob_id,
                                Err(e) => return Err(axum::Error::new(e)),
                            };
                            let num_blocks = match mpu_obj.num_blocks() {
                                Ok(num_blocks) => num_blocks,
                                Err(e) => return Err(axum::Error::new(e)),
                            };
                            Ok(get_full_blob_stream(rpc_client_bss, blob_id, num_blocks).await)
                        }
                    })
                    .try_flatten();
                Ok((Body::from_stream(body_stream), body_size, None))
            }
        },
    }
}

async fn get_object_range_content(
    bucket: &Bucket,
    object: &ObjectLayout,
    key: String,
    range: HttpRange,
    rpc_client_nss: &RpcClientNss,
    rpc_client_bss: Arc<RpcClientBss>,
) -> Result<Bytes, S3Error> {
    // convert to std::ops::Range
    let range = range.start as usize..(range.start + range.length) as usize;
    match object.state {
        ObjectState::Normal(ref obj_data) => {
            let blob_id = object.blob_id()?;
            let num_blocks = object.num_blocks()?;
            let body_stream = get_full_blob_stream(rpc_client_bss, blob_id, num_blocks).await;
            Ok(
                axum::body::to_bytes(Body::from_stream(body_stream), obj_data.size as usize)
                    .await?
                    .slice(range),
            )
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
            MpuState::Completed { size, .. } => {
                let mpu_prefix = mpu_get_part_prefix(key.clone(), 0);
                let mpus = list_raw_objects(
                    bucket.root_blob_name.clone(),
                    rpc_client_nss,
                    10000,
                    mpu_prefix,
                    "".into(),
                    false,
                )
                .await?;
                let body_stream = futures::stream::iter(mpus.into_iter())
                    .then(move |(_key, mpu_obj)| {
                        let rpc_client_bss = rpc_client_bss.clone();
                        async move {
                            let blob_id = match mpu_obj.blob_id() {
                                Ok(blob_id) => blob_id,
                                Err(e) => return Err(axum::Error::new(e)),
                            };
                            let num_blocks = match mpu_obj.num_blocks() {
                                Ok(num_blocks) => num_blocks,
                                Err(e) => return Err(axum::Error::new(e)),
                            };
                            Ok(get_full_blob_stream(rpc_client_bss, blob_id, num_blocks).await)
                        }
                    })
                    .try_flatten();
                Ok(
                    axum::body::to_bytes(Body::from_stream(body_stream), *size as usize)
                        .await?
                        .slice(range),
                )
            }
        },
    }
}

async fn get_full_blob_stream(
    rpc_client_bss: Arc<RpcClientBss>,
    blob_id: BlobId,
    num_blocks: usize,
) -> BodyDataStream {
    let body_stream = futures::stream::iter(0..num_blocks)
        .then(move |i| {
            let rpc_client_bss = rpc_client_bss.clone();
            async move {
                let mut block = Bytes::new();
                match rpc_client_bss.get_blob(blob_id, i as u32, &mut block).await {
                    Err(e) => Err(axum::Error::new(e)),
                    Ok(_) => Ok(Body::from(block).into_data_stream()),
                }
            }
        })
        .try_flatten();

    Body::from_stream(body_stream).into_data_stream()
}

fn parse_range_header(
    range_header: Option<&HeaderValue>,
    total_size: u64,
) -> Result<Option<http_range::HttpRange>, S3Error> {
    let range = match range_header {
        Some(range) => {
            let range_str = range.to_str()?;
            let mut ranges = http_range::HttpRange::parse(range_str, total_size)?;
            if ranges.len() > 1 {
                // Amazon S3 doesn't support retrieving multiple ranges of data per GET request.
                tracing::debug!("Found more than one ranges: {range_str}");
                return Err(S3Error::InvalidRange);
            } else {
                ranges.pop()
            }
        }
        None => None,
    };
    Ok(range)
}
