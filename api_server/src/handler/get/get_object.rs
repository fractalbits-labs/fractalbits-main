use axum::{
    extract::Query,
    http::{header, HeaderMap, HeaderValue},
    response::{IntoResponse, Response},
    RequestPartsExt,
};
use bytes::{Bytes, BytesMut};
use rpc_client_bss::RpcClientBss;
use rpc_client_nss::RpcClientNss;
use serde::Deserialize;

use crate::object_layout::{MpuState, ObjectState};
use crate::BlobId;
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
    rpc_client_bss: &RpcClientBss,
) -> Result<Response, S3Error> {
    let mut parts = request.into_parts().0;
    let Query(opts): Query<QueryOpts> = parts.extract().await?;
    let header_opts = HeaderOpts::from_headers(&parts.headers)?;
    let object = get_raw_object(rpc_client_nss, bucket.root_blob_name.clone(), key.clone()).await?;
    get_object_content(
        bucket,
        &object,
        key,
        header_opts.x_amz_checksum_mode_enabled,
        opts.part_number,
        rpc_client_nss,
        rpc_client_bss,
    )
    .await
}

pub async fn get_object_content(
    bucket: &Bucket,
    object: &ObjectLayout,
    key: String,
    checksum_mode_enabled: bool,
    part_number: Option<u32>,
    rpc_client_nss: &RpcClientNss,
    rpc_client_bss: &RpcClientBss,
) -> Result<Response, S3Error> {
    match object.state {
        ObjectState::Normal(ref obj_data) => {
            let mut blob = BytesMut::new();
            get_full_blob(
                &mut blob,
                rpc_client_bss,
                object.blob_id()?,
                object.num_blocks()?,
            )
            .await?;
            let mut resp = blob.freeze().into_response();
            if checksum_mode_enabled {
                tracing::debug!(
                    "checksum_mode enabled, adding checksum: {:?}",
                    obj_data.checksum
                );
                add_checksum_response_headers(&obj_data.checksum, &mut resp)?;
            }
            Ok(resp)
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
            MpuState::Completed { size: _, etag: _ } => {
                let mut content = BytesMut::new();
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
                // Do filtering if there is part_number option
                let mpus = match part_number {
                    None => &mpus[0..],
                    Some(n) => &mpus[n as usize - 1..n as usize],
                };
                for (_, mpu_obj) in mpus.iter() {
                    get_full_blob(
                        &mut content,
                        rpc_client_bss,
                        mpu_obj.blob_id()?,
                        mpu_obj.num_blocks()?,
                    )
                    .await?;
                }
                Ok(content.into_response())
            }
        },
    }
}

async fn get_full_blob(
    blob: &mut BytesMut,
    rpc_client_bss: &RpcClientBss,
    blob_id: BlobId,
    num_blocks: usize,
) -> Result<(), S3Error> {
    for i in 0..num_blocks {
        let mut block = Bytes::new();
        let _size = rpc_client_bss
            .get_blob(blob_id, i as u32, &mut block)
            .await?;
        blob.extend(block);
    }

    Ok(())
}
