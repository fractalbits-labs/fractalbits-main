use std::collections::HashSet;

use crate::handler::{
    common::{
        get_raw_object, response::xml::Xml, s3_error::S3Error, signature::checksum::ChecksumValue,
        time,
    },
    Request,
};
use axum::{
    extract::Query,
    http::{header, HeaderMap, HeaderValue},
    response::Response,
    RequestPartsExt,
};
use base64::{prelude::BASE64_STANDARD, Engine};
use bucket_tables::bucket_table::Bucket;
use rpc_client_nss::RpcClientNss;
use serde::{Deserialize, Serialize};

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct QueryOpts {
    version_id: Option<String>,
}

#[allow(dead_code)]
#[derive(Debug, Default)]
struct HeaderOpts {
    x_amz_max_parts: Option<String>,
    x_amz_part_number_marker: Option<String>,
    x_amz_server_side_encryption_customer_algorithm: Option<String>,
    x_amz_server_side_encryption_customer_key: Option<String>,
    x_amz_server_side_encryption_customer_key_md5: Option<String>,
    x_amz_request_payer: Option<String>,
    x_amz_expected_bucket_owner: Option<String>,
    x_amz_object_attributes: HashSet<String>, // required
}

impl HeaderOpts {
    fn from_headers(headers: &HeaderMap) -> Result<Self, S3Error> {
        Ok(Self {
            x_amz_max_parts: headers
                .get("x-amz-max-parts")
                .map(|x| x.to_str())
                .transpose()?
                .map(|x| x.to_owned()),
            x_amz_part_number_marker: headers
                .get("x-amz-part-number-marker")
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
                .get("x-amz-server-side-encryption-customer-key-MD5")
                .map(|x| x.to_str())
                .transpose()?
                .map(|x| x.to_owned()),
            x_amz_request_payer: headers
                .get("x-amz-request-payer")
                .map(|x| x.to_str())
                .transpose()?
                .map(|x| x.to_owned()),
            x_amz_expected_bucket_owner: headers
                .get("x-amz-expected-bucket-owner")
                .map(|x| x.to_str())
                .transpose()?
                .map(|x| x.to_owned()),
            x_amz_object_attributes: headers
                .get("x-amz-object-attributes")
                .ok_or(S3Error::InvalidArgument2)?
                .to_str()?
                .split(',')
                .map(|x| x.to_lowercase())
                .collect(),
        })
    }
}

#[derive(Default, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct GetObjectAttributesOutput {
    #[serde(rename = "ETag", skip_serializing_if = "Option::is_none")]
    etag: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    checksum: Option<Checksum>,
    #[serde(skip_serializing_if = "Option::is_none")]
    object_parts: Option<ObjectParts>,
    #[serde(skip_serializing_if = "Option::is_none")]
    storage_class: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    object_size: Option<usize>,
}

impl GetObjectAttributesOutput {
    fn etag(self, etag: String) -> Self {
        Self {
            etag: Some(etag),
            ..self
        }
    }

    fn object_size(self, object_size: usize) -> Self {
        Self {
            object_size: Some(object_size),
            ..self
        }
    }

    fn checksum(mut self, checksum: Option<ChecksumValue>) -> Self {
        match checksum {
            Some(ChecksumValue::Crc32(crc32)) => {
                let checksum = Checksum {
                    checksum_crc32: Some(BASE64_STANDARD.encode(crc32)),
                    checksum_type: "CRC32".to_string(),
                    ..Default::default()
                };
                self.checksum = Some(checksum);
            }
            Some(ChecksumValue::Crc32c(crc32c)) => {
                let checksum = Checksum {
                    checksum_crc32c: Some(BASE64_STANDARD.encode(crc32c)),
                    checksum_type: "CRC32C".to_string(),
                    ..Default::default()
                };
                self.checksum = Some(checksum);
            }
            Some(ChecksumValue::Sha1(sha1)) => {
                let checksum = Checksum {
                    checksum_sha1: Some(BASE64_STANDARD.encode(sha1)),
                    checksum_type: "SHA1".to_string(),
                    ..Default::default()
                };
                self.checksum = Some(checksum);
            }
            Some(ChecksumValue::Sha256(sha256)) => {
                let checksum = Checksum {
                    checksum_sha256: Some(BASE64_STANDARD.encode(sha256)),
                    checksum_type: "SHA256".to_string(),
                    ..Default::default()
                };
                self.checksum = Some(checksum);
            }
            None => (),
        }
        self
    }
}

#[derive(Default, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct Checksum {
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
    checksum_type: String,
}

#[derive(Default, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct ObjectParts {
    is_truncated: bool,
    max_parts: usize,
    next_part_number_marker: usize,
    part_number_marker: usize,
    part: Part,
    parts_count: usize,
}

#[derive(Default, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct Part {
    checksum_crc32: String,
    checksum_crc32c: String,
    checksum_sha1: String,
    checksum_sha256: String,
    part_number: usize,
    size: usize,
}

pub async fn get_object_attributes(
    request: Request,
    bucket: &Bucket,
    key: String,
    rpc_client_nss: &RpcClientNss,
) -> Result<Response, S3Error> {
    let header_opts = HeaderOpts::from_headers(request.headers())?;
    let Query(_query_opts): Query<QueryOpts> = request.into_parts().0.extract().await?;
    let obj = get_raw_object(rpc_client_nss, bucket.root_blob_name.clone(), key.clone()).await?;
    let last_modified = time::format_http_date(obj.timestamp);

    let mut resp = GetObjectAttributesOutput::default();
    if header_opts.x_amz_object_attributes.contains("etag") {
        resp = resp.etag(obj.etag()?);
    }
    if header_opts.x_amz_object_attributes.contains("checksum") {
        resp = resp.checksum(obj.checksum()?);
    }
    if header_opts.x_amz_object_attributes.contains("objectsize") {
        resp = resp.object_size(obj.size()? as usize);
    }
    // TODO: ObjectParts | StorageClass
    let mut resp: Response = Xml(resp).try_into()?;
    resp.headers_mut().insert(
        header::LAST_MODIFIED,
        HeaderValue::from_str(&last_modified)?,
    );
    Ok(resp)
}
