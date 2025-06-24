use std::hash::Hasher;
use std::{collections::HashSet, sync::Arc};

use crate::{
    handler::{
        common::{
            extract_metadata_headers, gen_etag, get_raw_object, list_raw_objects,
            mpu_get_part_prefix, mpu_parse_part_number,
            response::xml::{Xml, XmlnsS3},
            s3_error::S3Error,
            signature::{
                body::ChecksumAlgorithm,
                checksum::{request_checksum_value, ChecksumValue},
            },
        },
        delete::delete_object_handler,
        Request,
    },
    object_layout::{MpuState, ObjectCoreMetaData, ObjectState},
    AppState, BlobId,
};
use axum::{http::HeaderValue, response::Response};
use base64::{prelude::BASE64_STANDARD, Engine};
use bucket_tables::bucket_table::Bucket;
use bytes::Buf;
use crc32c::Crc32cHasher as Crc32c;
use crc32fast::Hasher as Crc32;
use md5::Digest;
use rkyv::{self, api::high::to_bytes_in, rancor::Error};
use rpc_client_nss::rpc::put_inode_response;
use serde::{Deserialize, Serialize};
use sha1::Sha1;
use sha2::Sha256;
use tokio::sync::mpsc::Sender;

#[allow(dead_code)]
#[derive(Debug, Default)]
pub struct HeaderOpts<'a> {
    x_amz_checksum_crc32: Option<&'a HeaderValue>,
    x_amz_checksum_crc32c: Option<&'a HeaderValue>,
    x_amz_checksum_crc64nvme: Option<&'a HeaderValue>,
    x_amz_checksum_sha1: Option<&'a HeaderValue>,
    x_amz_checksum_sha256: Option<&'a HeaderValue>,
    x_amz_checksum_type: Option<&'a HeaderValue>,
    x_amz_mp_object_size: Option<&'a HeaderValue>,
    x_amz_checksum_mode_enabled: bool,
    x_amz_request_payer: Option<&'a HeaderValue>,
    x_amz_expected_bucket_owner: Option<&'a HeaderValue>,
    if_match: Option<&'a HeaderValue>,
    if_none_match: Option<&'a HeaderValue>,
    x_amz_server_side_encryption_customer_algorithm: Option<&'a HeaderValue>,
    x_amz_server_side_encryption_customer_key: Option<&'a HeaderValue>,
    x_amz_server_side_encryption_customer_key_md5: Option<&'a HeaderValue>,
}

#[derive(Default, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct CompleteMultipartUpload {
    part: Vec<Part>,
}

#[derive(Default, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct Part {
    #[serde(default)]
    checksum_crc32: String,
    #[serde(default)]
    checksum_crc32c: String,
    #[serde(default)]
    checksum_sha1: String,
    #[serde(default)]
    checksum_sha256: String,
    #[serde(rename = "ETag")]
    etag: String,
    part_number: u32,
}

#[derive(Default, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct CompleteMultipartUploadResult {
    #[serde(rename = "@xmlns")]
    xmlns: XmlnsS3,
    location: String,
    bucket: String,
    key: String,
    #[serde(rename = "ETag", skip_serializing_if = "Option::is_none")]
    etag: Option<String>,

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
    #[serde(skip_serializing_if = "Option::is_none")]
    checksum_type: Option<String>,
}

impl CompleteMultipartUploadResult {
    fn bucket(self, bucket: String) -> Self {
        Self { bucket, ..self }
    }

    fn key(self, key: String) -> Self {
        Self { key, ..self }
    }

    fn etag(self, etag: String) -> Self {
        Self {
            etag: Some(etag),
            ..self
        }
    }

    fn checksum(self, checksum: Option<ChecksumValue>) -> Self {
        match checksum {
            Some(ChecksumValue::Crc32(crc32)) => Self {
                checksum_crc32: Some(BASE64_STANDARD.encode(crc32)),
                checksum_type: Some("COMPOSITE".to_string()),
                ..self
            },
            Some(ChecksumValue::Crc32c(crc32c)) => Self {
                checksum_crc32c: Some(BASE64_STANDARD.encode(crc32c)),
                checksum_type: Some("COMPOSITE".to_string()),
                ..self
            },
            Some(ChecksumValue::Sha1(sha1)) => Self {
                checksum_sha1: Some(BASE64_STANDARD.encode(sha1)),
                checksum_type: Some("COMPOSITE".to_string()),
                ..self
            },
            Some(ChecksumValue::Sha256(sha256)) => Self {
                checksum_sha256: Some(BASE64_STANDARD.encode(sha256)),
                checksum_type: Some("COMPOSITE".to_string()),
                ..self
            },
            None => self,
        }
    }
}

#[derive(Default)]
pub(crate) struct MpuChecksummer {
    // We only support composite checksum for speed reason
    pub composite_checksum: Option<MpuChecksummerAlgo>,
}

pub(crate) enum MpuChecksummerAlgo {
    Crc32(Crc32),
    Crc32c(Crc32c),
    Sha1(Sha1),
    Sha256(Sha256),
}

impl MpuChecksummer {
    pub(crate) fn init(algo: Option<ChecksumAlgorithm>) -> Self {
        Self {
            composite_checksum: match algo {
                None => None,
                Some(ChecksumAlgorithm::Crc32) => Some(MpuChecksummerAlgo::Crc32(Crc32::new())),
                Some(ChecksumAlgorithm::Crc32c) => {
                    Some(MpuChecksummerAlgo::Crc32c(Crc32c::default()))
                }
                Some(ChecksumAlgorithm::Sha1) => Some(MpuChecksummerAlgo::Sha1(Sha1::new())),
                Some(ChecksumAlgorithm::Sha256) => Some(MpuChecksummerAlgo::Sha256(Sha256::new())),
            },
        }
    }

    pub(crate) fn update(&mut self, checksum: Option<ChecksumValue>) -> Result<(), S3Error> {
        match (&mut self.composite_checksum, checksum) {
            (None, _) => (),
            (Some(MpuChecksummerAlgo::Crc32(ref mut crc32)), Some(ChecksumValue::Crc32(x))) => {
                crc32.update(&x);
            }
            (Some(MpuChecksummerAlgo::Crc32c(ref mut crc32c)), Some(ChecksumValue::Crc32c(x))) => {
                crc32c.write(&x);
            }
            (Some(MpuChecksummerAlgo::Sha1(ref mut sha1)), Some(ChecksumValue::Sha1(x))) => {
                sha1.update(x);
            }
            (Some(MpuChecksummerAlgo::Sha256(ref mut sha256)), Some(ChecksumValue::Sha256(x))) => {
                sha256.update(x);
            }
            (Some(_), b) => {
                tracing::error!("part checksum was not computed correctly, got: {:?}", b);
                return Err(S3Error::InternalError);
            }
        }
        Ok(())
    }

    pub(crate) fn finalize(self) -> Option<ChecksumValue> {
        match self.composite_checksum {
            None => None,
            Some(MpuChecksummerAlgo::Crc32(crc32)) => {
                Some(ChecksumValue::Crc32(u32::to_be_bytes(crc32.finalize())))
            }
            Some(MpuChecksummerAlgo::Crc32c(crc32c)) => Some(ChecksumValue::Crc32c(
                u32::to_be_bytes(u32::try_from(crc32c.finish()).unwrap()),
            )),
            Some(MpuChecksummerAlgo::Sha1(sha1)) => {
                Some(ChecksumValue::Sha1(sha1.finalize()[..].try_into().unwrap()))
            }
            Some(MpuChecksummerAlgo::Sha256(sha256)) => Some(ChecksumValue::Sha256(
                sha256.finalize()[..].try_into().unwrap(),
            )),
        }
    }
}

pub async fn complete_multipart_upload_handler(
    app: Arc<AppState>,
    request: Request,
    bucket: &Bucket,
    key: String,
    upload_id: String,
    blob_deletion: Sender<(BlobId, usize)>,
) -> Result<Response, S3Error> {
    let headers = extract_metadata_headers(request.headers())?;
    let expected_checksum = request_checksum_value(request.headers())?;
    let body = request.into_body().collect().await.unwrap();
    let req_body: CompleteMultipartUpload = quick_xml::de::from_reader(body.reader())?;
    let mut valid_part_numbers: HashSet<u32> =
        req_body.part.iter().map(|part| part.part_number).collect();

    let rpc_client_nss = app.get_rpc_client_nss();
    let mut object =
        get_raw_object(rpc_client_nss, bucket.root_blob_name.clone(), key.clone()).await?;
    if object.version_id.simple().to_string() != upload_id {
        return Err(S3Error::NoSuchVersion);
    }
    if ObjectState::Mpu(MpuState::Uploading) != object.state {
        return Err(S3Error::InvalidObjectState);
    }

    let max_parts = 10000;
    let mpu_prefix = mpu_get_part_prefix(key.clone(), 0);
    let mpu_objs = list_raw_objects(
        bucket.root_blob_name.clone(),
        rpc_client_nss,
        max_parts,
        mpu_prefix.clone(),
        "".into(),
        "".into(),
        false,
    )
    .await?;

    let mut total_size = 0;
    let mut invalid_part_keys = HashSet::new();
    let mut checksummer = MpuChecksummer::init(expected_checksum.map(|x| x.algorithm()));
    for (mut mpu_key, mpu_obj) in mpu_objs {
        assert_eq!(Some('\0'), mpu_key.pop());
        let part_number = mpu_parse_part_number(&mpu_key)?;
        if !valid_part_numbers.remove(&part_number) {
            invalid_part_keys.insert(mpu_key.clone());
        } else {
            checksummer.update(mpu_obj.checksum()?)?;
            total_size += mpu_obj.size()?;
        }
    }

    let checksum = checksummer.finalize();
    if expected_checksum.is_some() && checksum != expected_checksum {
        return Err(S3Error::InvalidDigest);
    }

    if !valid_part_numbers.is_empty() {
        return Err(S3Error::InvalidPart);
    }
    for mpu_key in invalid_part_keys.iter() {
        delete_object_handler(app.clone(), bucket, mpu_key.clone(), blob_deletion.clone()).await?;
    }

    let etag = gen_etag();
    object.state = ObjectState::Mpu(MpuState::Completed(ObjectCoreMetaData {
        size: total_size,
        etag: etag.clone(),
        headers,
        checksum: expected_checksum,
    }));
    let new_object_bytes = to_bytes_in::<_, Error>(&object, Vec::new())?;
    let resp = rpc_client_nss
        .put_inode(
            bucket.root_blob_name.clone(),
            key.clone(),
            new_object_bytes.into(),
        )
        .await?;
    match resp.result.unwrap() {
        put_inode_response::Result::Ok(_) => {}
        put_inode_response::Result::Err(e) => {
            tracing::error!(e);
            return Err(S3Error::InternalError);
        }
    };

    let resp = CompleteMultipartUploadResult::default()
        .bucket(bucket.bucket_name.clone())
        .key(key)
        .etag(object.etag()?)
        .checksum(object.checksum()?);
    Xml(resp).try_into()
}
