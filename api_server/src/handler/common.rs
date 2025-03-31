pub mod authorization;
pub mod data;
pub mod encoding;
pub mod request;
pub mod response;
pub mod s3_error;
pub mod signature;
pub mod time;
pub mod xheader;

use std::collections::BTreeMap;

use crate::object_layout::{HeaderList, ObjectLayout};
use axum::{
    http::{header, HeaderMap, HeaderName, HeaderValue},
    response::Response,
};
use rkyv::{self, rancor::Error};
use rpc_client_nss::{rpc::get_inode_response, rpc::list_inodes_response, RpcClientNss};
use s3_error::S3Error;
use signature::checksum::add_checksum_response_headers;

pub async fn get_raw_object(
    rpc_client_nss: &RpcClientNss,
    root_blob_name: String,
    key: String,
) -> Result<ObjectLayout, S3Error> {
    let resp = rpc_client_nss.get_inode(root_blob_name, key).await?;

    let object_bytes = match resp.result.unwrap() {
        get_inode_response::Result::Ok(res) => res,
        get_inode_response::Result::ErrNotFound(()) => {
            return Err(S3Error::NoSuchKey);
        }
        get_inode_response::Result::ErrOthers(e) => {
            tracing::error!(e);
            return Err(S3Error::InternalError);
        }
    };

    let object = rkyv::from_bytes::<ObjectLayout, Error>(&object_bytes)?;
    Ok(object)
}

pub async fn list_raw_objects(
    root_blob_name: String,
    rpc_client_nss: &RpcClientNss,
    max_parts: u32,
    prefix: String,
    start_after: String,
    skip_mpu_parts: bool,
) -> Result<Vec<(String, ObjectLayout)>, S3Error> {
    let resp = rpc_client_nss
        .list_inodes(
            root_blob_name,
            max_parts,
            prefix,
            start_after,
            skip_mpu_parts,
        )
        .await?;

    // Process results
    let inodes = match resp.result.unwrap() {
        list_inodes_response::Result::Ok(res) => res.inodes,
        list_inodes_response::Result::Err(e) => {
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
    assert_eq!(Some('\0'), key.pop());
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

pub fn mpu_parse_part_number(mpu_key: &str, key: &str) -> u32 {
    let mut part_str = mpu_key.to_owned().split_off(key.len());
    part_str.pop(); // remove trailing '\0'
    part_str.parse::<u32>().unwrap() + 1
}

pub fn extract_metadata_headers(headers: &HeaderMap<HeaderValue>) -> Result<HeaderList, S3Error> {
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
    resp: &mut Response,
    object: &ObjectLayout,
    checksum_mode_enabled: bool,
) -> Result<(), S3Error> {
    let etag = object.etag()?;
    let last_modified = time::format_http_date(object.timestamp);
    resp.headers_mut().insert(
        header::LAST_MODIFIED,
        HeaderValue::from_str(&last_modified)?,
    );
    resp.headers_mut()
        .insert(header::ETAG, HeaderValue::from_str(&etag)?);

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
        resp.headers_mut().insert(
            HeaderName::try_from(name).unwrap(),
            HeaderValue::from_str(&values.join(","))?,
        );
    }

    if checksum_mode_enabled {
        let checksum = object.checksum()?;
        tracing::debug!("checksum_mode enabled, adding checksum: {:?}", checksum);
        add_checksum_response_headers(&checksum, resp)?;
    }

    Ok(())
}
