pub mod authorization;
pub mod checksum;
pub mod request;
pub mod response;
pub mod s3_error;
pub mod signature;
pub mod time;
pub mod xheader;

use crate::{
    AppState,
    object_layout::{HeaderList, ObjectLayout},
};
use actix_web::http::header::{self, HeaderMap};
use futures::StreamExt;
use rand::Rng;
use rkyv::{self, rancor::Error};
use rpc_client_common::nss_rpc_retry;
use s3_error::S3Error;
use std::collections::BTreeMap;

/// Helper function to buffer a streaming payload into Bytes
pub async fn buffer_payload(
    payload: actix_web::dev::Payload,
) -> Result<actix_web::web::Bytes, S3Error> {
    buffer_payload_with_capacity(payload, None).await
}

/// Helper function to buffer a streaming payload into Bytes with optional pre-allocation
pub async fn buffer_payload_with_capacity(
    mut payload: actix_web::dev::Payload,
    expected_size: Option<usize>,
) -> Result<actix_web::web::Bytes, S3Error> {
    // Pre-allocate based on content-length to avoid reallocations
    let mut body = if let Some(size) = expected_size {
        tracing::debug!("Pre-allocating buffer for {} bytes", size);
        actix_web::web::BytesMut::with_capacity(size)
    } else {
        actix_web::web::BytesMut::new()
    };

    while let Some(chunk) = payload.next().await {
        let chunk = chunk.map_err(|e| {
            tracing::error!("Error reading payload: {}", e);
            S3Error::InternalError
        })?;
        body.extend_from_slice(&chunk);
    }

    Ok(body.freeze())
}

pub async fn get_raw_object(
    app: &AppState,
    root_blob_name: &str,
    key: &str,
) -> Result<ObjectLayout, S3Error> {
    let resp = nss_rpc_retry!(
        app,
        get_inode(root_blob_name, key, Some(app.config.rpc_timeout()))
    )
    .await?;

    let object_bytes = match resp.result.unwrap() {
        nss_codec::get_inode_response::Result::Ok(res) => res,
        nss_codec::get_inode_response::Result::ErrNotFound(()) => {
            return Err(S3Error::NoSuchKey);
        }
        nss_codec::get_inode_response::Result::ErrOthers(e) => {
            tracing::error!(e);
            return Err(S3Error::InternalError);
        }
    };

    let object = rkyv::from_bytes::<ObjectLayout, Error>(&object_bytes)?;
    Ok(object)
}

pub async fn list_raw_objects(
    app: &AppState,
    root_blob_name: &str,
    max_parts: u32,
    prefix: &str,
    delimiter: &str,
    start_after: &str,
    skip_mpu_parts: bool,
) -> Result<Vec<(String, ObjectLayout)>, S3Error> {
    let resp = nss_rpc_retry!(
        app,
        list_inodes(
            &root_blob_name,
            max_parts,
            &prefix,
            &delimiter,
            &start_after,
            skip_mpu_parts,
            Some(app.config.rpc_timeout())
        )
    )
    .await?;

    // Process results
    let inodes = match resp.result.unwrap() {
        nss_codec::list_inodes_response::Result::Ok(res) => res.inodes,
        nss_codec::list_inodes_response::Result::Err(e) => {
            tracing::error!(e);
            return Err(S3Error::InternalError);
        }
    };

    let mut res = Vec::with_capacity(inodes.len());
    for inode in inodes {
        let object = rkyv::from_bytes::<ObjectLayout, Error>(&inode.inode)?;
        res.push((inode.key, object));
    }
    Ok(res)
}

pub fn mpu_get_part_prefix(mut key: String, part_number: u64) -> String {
    key.push('#');
    // if part number is 0, we treat it as object key
    if part_number != 0 {
        // part numbers range is [1, 10000], which can be encoded as 4 digits
        // See https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html
        let part_str = format!("{:04}", part_number - 1);
        key.push_str(&part_str);
    }
    key
}

pub fn mpu_parse_part_number(mpu_key: &str) -> Result<u32, S3Error> {
    let part_str = mpu_key
        .split('#')
        .next_back()
        .ok_or(S3Error::InternalError)?;
    Ok(part_str
        .parse::<u32>()
        .map_err(|_| S3Error::InternalError)?
        + 1)
}

pub fn extract_metadata_headers(headers: &HeaderMap) -> Result<HeaderList, S3Error> {
    let mut ret = Vec::new();

    // Preserve standard headers
    let standard_header = [
        header::CONTENT_TYPE,
        header::CACHE_CONTROL,
        header::CONTENT_DISPOSITION,
        header::CONTENT_ENCODING,
        header::CONTENT_LANGUAGE,
        header::EXPIRES,
    ];
    for name in standard_header.iter() {
        if let Some(value) = headers.get(name) {
            ret.push((name.to_string(), value.to_str()?.to_string()));
        }
    }

    // Preserve x-amz-meta- headers
    for (name, value) in headers.iter() {
        if name.as_str().starts_with("x-amz-meta-") {
            ret.push((
                name.as_str().to_ascii_lowercase(),
                std::str::from_utf8(value.as_bytes())?.to_string(),
            ));
        }
        if name == xheader::X_AMZ_WEBSITE_REDIRECT_LOCATION {
            let value = std::str::from_utf8(value.as_bytes())?.to_string();
            if !(value.starts_with("/")
                || value.starts_with("http://")
                || value.starts_with("https://"))
            {
                return Err(S3Error::UnexpectedContent);
            }
            ret.push((xheader::X_AMZ_WEBSITE_REDIRECT_LOCATION.to_string(), value));
        }
    }

    Ok(ret)
}

pub fn object_headers(
    resp: &mut actix_web::HttpResponseBuilder,
    object: &ObjectLayout,
    checksum_mode_enabled: bool,
) -> Result<(), S3Error> {
    let etag = object.etag()?;
    let last_modified = time::format_http_date(object.timestamp);
    resp.insert_header((header::LAST_MODIFIED, last_modified));
    resp.insert_header((header::ETAG, etag));

    // When metadata is retrieved through the REST API, Amazon S3 combines headers that
    // have the same name (ignoring case) into a comma-delimited list.
    // See: https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingMetadata.html
    let mut headers_by_name = BTreeMap::new();
    for (name, value) in object.headers()?.iter() {
        let name_lower = name.to_ascii_lowercase();
        headers_by_name
            .entry(name_lower)
            .or_insert(vec![])
            .push(value.as_str());
    }

    for (name, values) in headers_by_name {
        resp.insert_header((name, values.join(",")));
    }

    if checksum_mode_enabled {
        let checksum = object.checksum()?;
        tracing::debug!("checksum_mode enabled, adding checksum: {:?}", checksum);
        checksum::add_checksum_response_headers(&checksum, resp)?;
    }

    Ok(())
}

// Not using md5 as etag for speed reason
pub fn gen_etag() -> String {
    let mut rng = rand::thread_rng();
    let random: [u8; 16] = rng.r#gen();
    hex::encode(random)
}
