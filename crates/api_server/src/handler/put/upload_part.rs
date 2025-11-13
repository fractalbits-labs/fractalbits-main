use crate::handler::ObjectRequestContext;
use crate::handler::common::{mpu_get_part_prefix, s3_error::S3Error};
use crate::handler::put::put_object_handler;
use axum::response::Response;
use axum::{http::HeaderValue, response};
use serde::Serialize;

#[allow(dead_code)]
#[derive(Default, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
struct ResponseHeaders {
    x_amz_server_side_encryption: String,
    #[serde(rename = "ETag")]
    etag: String,
    x_amz_checksum_crc32: String,
    x_amz_checksum_crc32c: String,
    x_amz_checksum_sha1: String,
    x_amz_checksum_sha256: String,
    x_amz_server_side_encryption_customer_algorithm: String,
    #[serde(rename = "x-amz-server-side-encryption-customer-key-MD5")]
    x_amz_server_side_encryption_customer_key_md5: String,
    x_amz_server_side_encryption_aws_kms_key_id: String,
    x_amz_server_side_encryption_bucket_key_enabled: String,
    x_amz_request_charged: String,
}

pub async fn upload_part_handler(
    ctx: ObjectRequestContext,
    part_number: u64,
    upload_id: String,
) -> Result<Response, S3Error> {
    if !(1..=10_000).contains(&part_number) {
        return Err(S3Error::InvalidPart);
    }
    // TODO: check upload_id

    let key = mpu_get_part_prefix(ctx.key, part_number);
    let new_ctx = ObjectRequestContext::new(
        ctx.app,
        ctx.request,
        ctx.api_key,
        ctx.bucket_name,
        key,
        ctx.checksum_value,
        ctx.trace_id,
    );
    put_object_handler(new_ctx).await?;

    let mut resp = response::Response::default();
    let etag = format!("{upload_id}{part_number}");
    resp.headers_mut()
        .insert("ETag", HeaderValue::from_str(&etag).unwrap());
    Ok(resp)
}
