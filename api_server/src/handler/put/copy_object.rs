#![allow(unused_imports, dead_code)]
use std::time::{SystemTime, UNIX_EPOCH};

use crate::{
    handler::{
        common::{
            response::xml::Xml,
            s3_error::S3Error,
            signature::checksum::{
                request_checksum_value, request_trailer_checksum_algorithm, ChecksumValue,
                ExpectedChecksums,
            },
        },
        Request,
    },
    object_layout::*,
    BlobId,
};
use axum::{
    body::Body,
    http::{header, HeaderMap, HeaderValue},
    response::{IntoResponse, Response},
};
use base64::{prelude::BASE64_STANDARD, Engine};
use bucket_tables::bucket_table::Bucket;
use futures::{StreamExt, TryStreamExt};
use rand::{rngs::OsRng, RngCore};
use rkyv::{self, api::high::to_bytes_in, rancor::Error};
use rpc_client_bss::{message::MessageHeader, RpcClientBss};
use rpc_client_nss::{rpc::put_inode_response, RpcClientNss};
use serde::{Deserialize, Serialize};

use super::block_data_stream::BlockDataStream;

#[derive(Debug, Default)]
struct HeaderOpts {
    x_amz_acl: Option<String>,
    cache_control: Option<String>,
    x_amz_checksum_algorithm: Option<String>,
    content_disposition: Option<String>,
    content_encoding: Option<String>,
    content_language: Option<String>,
    content_type: Option<String>,
    x_amz_copy_source: Option<String>,
    x_amz_copy_source_if_match: Option<String>,
    x_amz_copy_source_if_modified_since: Option<String>,
    x_amz_copy_source_if_none_match: Option<String>,
    x_amz_copy_source_if_unmodified_since: Option<String>,
    expires: Option<String>,
    x_amz_grant_full_control: Option<String>,
    x_amz_grant_read: Option<String>,
    x_amz_grant_read_acp: Option<String>,
    x_amz_grant_write_acp: Option<String>,
    x_amz_metadata_directive: Option<String>,
    x_amz_tagging_directive: Option<String>,
    x_amz_server_side_encryption: Option<String>,
    x_amz_storage_class: Option<String>,
    x_amz_website_redirect_location: Option<String>,
    x_amz_server_side_encryption_customer_algorithm: Option<String>,
    x_amz_server_side_encryption_customer_key: Option<String>,
    x_amz_server_side_encryption_customer_key_md5: Option<String>,
    x_amz_server_side_encryption_aws_kms_key_id: Option<String>,
    x_amz_server_side_encryption_context: Option<String>,
    x_amz_server_side_encryption_bucket_key_enabled: Option<String>,
    x_amz_copy_source_server_side_encryption_customer_algorithm: Option<String>,
    x_amz_copy_source_server_side_encryption_customer_key: Option<String>,
    x_amz_copy_source_server_side_encryption_customer_key_md5: Option<String>,
    x_amz_request_payer: Option<String>,
    x_amz_tagging: Option<String>,
    x_amz_object_lock_mode: Option<String>,
    x_amz_object_lock_retain_until_date: Option<String>,
    x_amz_object_lock_legal_hold: Option<String>,
    x_amz_expected_bucket_owner: Option<String>,
    x_amz_source_expected_bucket_owner: Option<String>,
}

impl HeaderOpts {
    fn from_headers(headers: &HeaderMap) -> Result<Self, S3Error> {
        Ok(Self {
            x_amz_acl: headers
                .get("x-amz-acl")
                .map(|x| x.to_str())
                .transpose()?
                .map(|x| x.to_owned()),
            cache_control: headers
                .get(header::CACHE_CONTROL)
                .map(|x| x.to_str())
                .transpose()?
                .map(|x| x.to_owned()),
            x_amz_checksum_algorithm: headers
                .get("x-amz-checksum-algorithm")
                .map(|x| x.to_str())
                .transpose()?
                .map(|x| x.to_owned()),
            content_disposition: headers
                .get(header::CONTENT_DISPOSITION)
                .map(|x| x.to_str())
                .transpose()?
                .map(|x| x.to_owned()),
            content_encoding: headers
                .get(header::CONTENT_ENCODING)
                .map(|x| x.to_str())
                .transpose()?
                .map(|x| x.to_owned()),
            content_language: headers
                .get(header::CONTENT_LANGUAGE)
                .map(|x| x.to_str())
                .transpose()?
                .map(|x| x.to_owned()),
            content_type: headers
                .get(header::CONTENT_TYPE)
                .map(|x| x.to_str())
                .transpose()?
                .map(|x| x.to_owned()),
            x_amz_copy_source: headers
                .get("x-amz-copy-source")
                .map(|x| x.to_str())
                .transpose()?
                .map(|x| x.to_owned()),
            x_amz_copy_source_if_match: headers
                .get("x-amz-copy-source-if-match")
                .map(|x| x.to_str())
                .transpose()?
                .map(|x| x.to_owned()),
            x_amz_copy_source_if_modified_since: headers
                .get("x-amz-copy-source-if-modified-since")
                .map(|x| x.to_str())
                .transpose()?
                .map(|x| x.to_owned()),
            x_amz_copy_source_if_none_match: headers
                .get("x-amz-copy-source-if-none-match")
                .map(|x| x.to_str())
                .transpose()?
                .map(|x| x.to_owned()),
            x_amz_copy_source_if_unmodified_since: headers
                .get("x-amz-copy-source-if-unmodified-since")
                .map(|x| x.to_str())
                .transpose()?
                .map(|x| x.to_owned()),
            expires: headers
                .get(header::EXPIRES)
                .map(|x| x.to_str())
                .transpose()?
                .map(|x| x.to_owned()),
            x_amz_grant_full_control: headers
                .get("x-amz-grant-full-control")
                .map(|x| x.to_str())
                .transpose()?
                .map(|x| x.to_owned()),
            x_amz_grant_read: headers
                .get("x-amz-grant-read")
                .map(|x| x.to_str())
                .transpose()?
                .map(|x| x.to_owned()),
            x_amz_grant_read_acp: headers
                .get("x-amz-grant-read-acp")
                .map(|x| x.to_str())
                .transpose()?
                .map(|x| x.to_owned()),
            x_amz_grant_write_acp: headers
                .get("x-amz-grant-write-acp")
                .map(|x| x.to_str())
                .transpose()?
                .map(|x| x.to_owned()),
            x_amz_metadata_directive: headers
                .get("x-amz-metadata-directive")
                .map(|x| x.to_str())
                .transpose()?
                .map(|x| x.to_owned()),
            x_amz_tagging_directive: headers
                .get("x-amz-tagging-directive")
                .map(|x| x.to_str())
                .transpose()?
                .map(|x| x.to_owned()),
            x_amz_server_side_encryption: headers
                .get("x-amz-server-side-encryption")
                .map(|x| x.to_str())
                .transpose()?
                .map(|x| x.to_owned()),
            x_amz_storage_class: headers
                .get("x-amz-storage-class")
                .map(|x| x.to_str())
                .transpose()?
                .map(|x| x.to_owned()),
            x_amz_website_redirect_location: headers
                .get("x-amz-website-redirect-location")
                .map(|x| x.to_str())
                .transpose()?
                .map(|x| x.to_owned()),
            x_amz_server_side_encryption_customer_algorithm: headers
                .get("x-amz-server-side-encryption-customer-algorithm")
                .map(|x| x.to_str())
                .transpose()?
                .map(|x| x.to_owned()),
            x_amz_server_side_encryption_customer_key: headers
                .get("x-amz-server-side-encryption-customer-key")
                .map(|x| x.to_str())
                .transpose()?
                .map(|x| x.to_owned()),
            x_amz_server_side_encryption_customer_key_md5: headers
                .get("x-amz-server-side-encryption-customer-key-md5")
                .map(|x| x.to_str())
                .transpose()?
                .map(|x| x.to_owned()),
            x_amz_server_side_encryption_aws_kms_key_id: headers
                .get("x-amz-server-side-encryption-aws-kms-key-id")
                .map(|x| x.to_str())
                .transpose()?
                .map(|x| x.to_owned()),
            x_amz_server_side_encryption_context: headers
                .get("x-amz-server-side-encryption-context")
                .map(|x| x.to_str())
                .transpose()?
                .map(|x| x.to_owned()),
            x_amz_server_side_encryption_bucket_key_enabled: headers
                .get("x-amz-server-side-encryption-bucket-key-enabled")
                .map(|x| x.to_str())
                .transpose()?
                .map(|x| x.to_owned()),
            x_amz_copy_source_server_side_encryption_customer_algorithm: headers
                .get("x-amz-copy-source-server-side-encryption-customer-algorithm")
                .map(|x| x.to_str())
                .transpose()?
                .map(|x| x.to_owned()),
            x_amz_copy_source_server_side_encryption_customer_key: headers
                .get("x-amz-copy-source-server-side-encryption-customer-key")
                .map(|x| x.to_str())
                .transpose()?
                .map(|x| x.to_owned()),
            x_amz_copy_source_server_side_encryption_customer_key_md5: headers
                .get("x-amz-copy-source-server-side-encryption-customer-key-md5")
                .map(|x| x.to_str())
                .transpose()?
                .map(|x| x.to_owned()),
            x_amz_request_payer: headers
                .get("x-amz-request-payer")
                .map(|x| x.to_str())
                .transpose()?
                .map(|x| x.to_owned()),
            x_amz_tagging: headers
                .get("x-amz-tagging")
                .map(|x| x.to_str())
                .transpose()?
                .map(|x| x.to_owned()),
            x_amz_object_lock_mode: headers
                .get("x-amz-object-lock-mode")
                .map(|x| x.to_str())
                .transpose()?
                .map(|x| x.to_owned()),
            x_amz_object_lock_retain_until_date: headers
                .get("x-amz-object-lock-retain-until-date")
                .map(|x| x.to_str())
                .transpose()?
                .map(|x| x.to_owned()),
            x_amz_object_lock_legal_hold: headers
                .get("x-amz-object-lock-legal-hold")
                .map(|x| x.to_str())
                .transpose()?
                .map(|x| x.to_owned()),
            x_amz_expected_bucket_owner: headers
                .get("x-amz-expected-bucket-owner")
                .map(|x| x.to_str())
                .transpose()?
                .map(|x| x.to_owned()),
            x_amz_source_expected_bucket_owner: headers
                .get("x-amz-source-expected-bucket-owner")
                .map(|x| x.to_str())
                .transpose()?
                .map(|x| x.to_owned()),
        })
    }
}

#[derive(Default, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct CopyObjectResult {
    #[serde(rename = "ETag")]
    etag: String,
    last_modified: String,
    checksum_type: Option<String>,
    #[serde(rename = "ChecksumCRC32", skip_serializing_if = "Option::is_none")]
    checksum_crc32: Option<String>,
    #[serde(rename = "ChecksumCRC32C", skip_serializing_if = "Option::is_none")]
    checksum_crc32c: Option<String>,
    #[serde(rename = "ChecksumCRC64NVME", skip_serializing_if = "Option::is_none")]
    checksum_crc64nvme: Option<String>,
    #[serde(rename = "ChecksumSHA1", skip_serializing_if = "Option::is_none")]
    checksum_sha1: Option<String>,
    #[serde(rename = "ChecksumSHA256", skip_serializing_if = "Option::is_none")]
    checksum_sha256: Option<String>,
}

impl CopyObjectResult {
    fn etag(self, etag: String) -> Self {
        Self { etag, ..self }
    }

    fn last_modified(self, last_modified: String) -> Self {
        Self {
            last_modified,
            ..self
        }
    }

    fn checksum(mut self, checksum: Option<ChecksumValue>) -> Self {
        match checksum {
            Some(ChecksumValue::Crc32(crc32)) => {
                self.checksum_crc32 = Some(BASE64_STANDARD.encode(crc32));
                self.checksum_type = Some("CRC32".to_string());
            }
            Some(ChecksumValue::Crc32c(crc32c)) => {
                self.checksum_crc32c = Some(BASE64_STANDARD.encode(crc32c));
                self.checksum_type = Some("CRC32C".to_string());
            }
            Some(ChecksumValue::Sha1(sha1)) => {
                self.checksum_sha1 = Some(BASE64_STANDARD.encode(sha1));
                self.checksum_type = Some("SHA1".to_string());
            }
            Some(ChecksumValue::Sha256(sha256)) => {
                self.checksum_sha256 = Some(BASE64_STANDARD.encode(sha256));
                self.checksum_type = Some("SHA256".to_string());
            }
            None => (),
        }
        self
    }
}

pub async fn copy_object(
    request: Request,
    _bucket: &Bucket,
    _key: String,
    _rpc_client_nss: &RpcClientNss,
    _rpc_client_bss: &RpcClientBss,
) -> Result<Response, S3Error> {
    let _header_opts = HeaderOpts::from_headers(request.headers())?;
    Xml(CopyObjectResult::default()).try_into()
}
