use std::convert::{TryFrom, TryInto};
use std::hash::Hasher;

use base64::prelude::*;
use crc32c::Crc32cHasher as Crc32c;
use crc32fast::Hasher as Crc32;
use md5::{Digest, Md5};
use serde::{Deserialize, Serialize};
use sha1::Sha1;
use sha2::Sha256;

use axum::http::{HeaderMap, HeaderValue};
use axum::response::Response;

use crate::handler::common::s3_error::S3Error;
use crate::handler::common::xheader;

use super::super::data::*;

use super::body::ChecksumAlgorithm;
use super::*;

pub type Crc32Checksum = [u8; 4];
pub type Crc32cChecksum = [u8; 4];
pub type Md5Checksum = [u8; 16];
pub type Sha1Checksum = [u8; 20];
pub type Sha256Checksum = [u8; 32];

#[derive(
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Clone,
    Copy,
    Debug,
    Serialize,
    Deserialize,
)]
pub enum ChecksumValue {
    Crc32(#[serde(with = "serde_bytes")] [u8; 4]),
    Crc32c(#[serde(with = "serde_bytes")] [u8; 4]),
    Sha1(#[serde(with = "serde_bytes")] [u8; 20]),
    Sha256(#[serde(with = "serde_bytes")] [u8; 32]),
}

impl ChecksumValue {
    pub fn algorithm(&self) -> ChecksumAlgorithm {
        match self {
            ChecksumValue::Crc32(_) => ChecksumAlgorithm::Crc32,
            ChecksumValue::Crc32c(_) => ChecksumAlgorithm::Crc32c,
            ChecksumValue::Sha1(_) => ChecksumAlgorithm::Sha1,
            ChecksumValue::Sha256(_) => ChecksumAlgorithm::Sha256,
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct ExpectedChecksums {
    // base64-encoded md5 (content-md5 header)
    pub md5: Option<String>,
    // content_sha256 (as a Hash / FixedBytes32)
    pub sha256: Option<Hash>,
    // extra x-amz-checksum-* header
    pub extra: Option<ChecksumValue>,
}

#[derive(Default)]
pub struct Checksummer {
    pub crc32: Option<Crc32>,
    pub crc32c: Option<Crc32c>,
    pub md5: Option<Md5>,
    pub sha1: Option<Sha1>,
    pub sha256: Option<Sha256>,
}

#[derive(Default)]
pub struct Checksums {
    pub crc32: Option<Crc32Checksum>,
    pub crc32c: Option<Crc32cChecksum>,
    pub md5: Option<Md5Checksum>,
    pub sha1: Option<Sha1Checksum>,
    pub sha256: Option<Sha256Checksum>,
}

impl Checksummer {
    pub fn init(expected: &ExpectedChecksums, add_md5: bool) -> Self {
        let mut ret = Self::default();
        ret.add_expected(expected);
        if add_md5 {
            ret.add_md5();
        }
        ret
    }

    pub fn add_md5(&mut self) {
        self.md5 = Some(Md5::new());
    }

    pub fn add_expected(&mut self, expected: &ExpectedChecksums) {
        if expected.md5.is_some() {
            self.md5 = Some(Md5::new());
        }
        if expected.sha256.is_some() || matches!(&expected.extra, Some(ChecksumValue::Sha256(_))) {
            self.sha256 = Some(Sha256::new());
        }
        if matches!(&expected.extra, Some(ChecksumValue::Crc32(_))) {
            self.crc32 = Some(Crc32::new());
        }
        if matches!(&expected.extra, Some(ChecksumValue::Crc32c(_))) {
            self.crc32c = Some(Crc32c::default());
        }
        if matches!(&expected.extra, Some(ChecksumValue::Sha1(_))) {
            self.sha1 = Some(Sha1::new());
        }
    }

    pub fn add_algo(mut self, algo: Option<ChecksumAlgorithm>) -> Self {
        match algo {
            Some(ChecksumAlgorithm::Crc32) => {
                self.crc32 = Some(Crc32::new());
            }
            Some(ChecksumAlgorithm::Crc32c) => {
                self.crc32c = Some(Crc32c::default());
            }
            Some(ChecksumAlgorithm::Sha1) => {
                self.sha1 = Some(Sha1::new());
            }
            Some(ChecksumAlgorithm::Sha256) => {
                self.sha256 = Some(Sha256::new());
            }
            None => (),
        }
        self
    }

    pub fn update(&mut self, bytes: &[u8]) {
        if let Some(crc32) = &mut self.crc32 {
            crc32.update(bytes);
        }
        if let Some(crc32c) = &mut self.crc32c {
            crc32c.write(bytes);
        }
        if let Some(md5) = &mut self.md5 {
            md5.update(bytes);
        }
        if let Some(sha1) = &mut self.sha1 {
            sha1.update(bytes);
        }
        if let Some(sha256) = &mut self.sha256 {
            sha256.update(bytes);
        }
    }

    pub fn finalize(self) -> Checksums {
        Checksums {
            crc32: self.crc32.map(|x| u32::to_be_bytes(x.finalize())),
            crc32c: self
                .crc32c
                .map(|x| u32::to_be_bytes(u32::try_from(x.finish()).unwrap())),
            md5: self.md5.map(|x| x.finalize()[..].try_into().unwrap()),
            sha1: self.sha1.map(|x| x.finalize()[..].try_into().unwrap()),
            sha256: self.sha256.map(|x| x.finalize()[..].try_into().unwrap()),
        }
    }
}

impl Checksums {
    pub fn verify(&self, expected: &ExpectedChecksums) -> Result<(), Error> {
        if let Some(expected_md5) = &expected.md5 {
            match self.md5 {
                Some(md5) if BASE64_STANDARD.encode(md5) == expected_md5.trim_matches('"') => (),
                _ => {
                    return Err(Error::InvalidDigest(
                        "MD5 checksum verification failed (from content-md5)".into(),
                    ))
                }
            }
        }
        if let Some(expected_sha256) = &expected.sha256 {
            match self.sha256 {
                Some(sha256) if &sha256[..] == expected_sha256.as_slice() => (),
                _ => {
                    return Err(Error::InvalidDigest(
                        "SHA256 checksum verification failed (from x-amz-content-sha256)".into(),
                    ))
                }
            }
        }
        if let Some(extra) = expected.extra {
            let algo = extra.algorithm();
            if self.extract(Some(algo)) != Some(extra) {
                return Err(Error::InvalidDigest(format!(
                    "Failed to validate checksum for algorithm {algo:?}"
                )));
            }
        }
        Ok(())
    }

    pub fn extract(&self, algo: Option<ChecksumAlgorithm>) -> Option<ChecksumValue> {
        match algo {
            None => None,
            Some(ChecksumAlgorithm::Crc32) => Some(ChecksumValue::Crc32(self.crc32.unwrap())),
            Some(ChecksumAlgorithm::Crc32c) => Some(ChecksumValue::Crc32c(self.crc32c.unwrap())),
            Some(ChecksumAlgorithm::Sha1) => Some(ChecksumValue::Sha1(self.sha1.unwrap())),
            Some(ChecksumAlgorithm::Sha256) => Some(ChecksumValue::Sha256(self.sha256.unwrap())),
        }
    }
}

// ----

pub fn parse_checksum_algorithm(algo: &str) -> Result<ChecksumAlgorithm, Error> {
    match algo {
        "CRC32" => Ok(ChecksumAlgorithm::Crc32),
        "CRC32C" => Ok(ChecksumAlgorithm::Crc32c),
        "SHA1" => Ok(ChecksumAlgorithm::Sha1),
        "SHA256" => Ok(ChecksumAlgorithm::Sha256),
        _ => Err(Error::Other("invalid checksum algorithm".into())),
    }
}

/// Extract the value of the x-amz-checksum-algorithm header
pub fn request_checksum_algorithm(
    headers: &HeaderMap<HeaderValue>,
) -> Result<Option<ChecksumAlgorithm>, Error> {
    match headers.get(xheader::X_AMZ_CHECKSUM_ALGORITHM) {
        None => Ok(None),
        Some(x) => parse_checksum_algorithm(x.to_str()?).map(Some),
    }
}

pub fn request_trailer_checksum_algorithm(
    headers: &HeaderMap<HeaderValue>,
) -> Result<Option<ChecksumAlgorithm>, Error> {
    match headers
        .get(xheader::X_AMZ_TRAILER)
        .map(|x| x.to_str())
        .transpose()?
    {
        None => Ok(None),
        Some(x) if x == xheader::X_AMZ_CHECKSUM_CRC32 => Ok(Some(ChecksumAlgorithm::Crc32)),
        Some(x) if x == xheader::X_AMZ_CHECKSUM_CRC32C => Ok(Some(ChecksumAlgorithm::Crc32c)),
        Some(x) if x == xheader::X_AMZ_CHECKSUM_SHA1 => Ok(Some(ChecksumAlgorithm::Sha1)),
        Some(x) if x == xheader::X_AMZ_CHECKSUM_SHA256 => Ok(Some(ChecksumAlgorithm::Sha256)),
        _ => Err(Error::Other("invalid checksum algorithm".into())),
    }
}

/// Extract the value of any of the x-amz-checksum-* headers
pub fn request_checksum_value(
    headers: &HeaderMap<HeaderValue>,
) -> Result<Option<ChecksumValue>, Error> {
    let mut ret = vec![];

    if headers.contains_key(xheader::X_AMZ_CHECKSUM_CRC32) {
        ret.push(extract_checksum_value(headers, ChecksumAlgorithm::Crc32)?);
    }
    if headers.contains_key(xheader::X_AMZ_CHECKSUM_CRC32C) {
        ret.push(extract_checksum_value(headers, ChecksumAlgorithm::Crc32c)?);
    }
    if headers.contains_key(xheader::X_AMZ_CHECKSUM_SHA1) {
        ret.push(extract_checksum_value(headers, ChecksumAlgorithm::Sha1)?);
    }
    if headers.contains_key(xheader::X_AMZ_CHECKSUM_SHA256) {
        ret.push(extract_checksum_value(headers, ChecksumAlgorithm::Sha256)?);
    }

    if ret.len() > 1 {
        return Err(Error::Other(
            "multiple x-amz-checksum-* headers given".into(),
        ));
    }
    Ok(ret.pop())
}

/// Checks for the presence of x-amz-checksum-algorithm
/// if so extract the corresponding x-amz-checksum-* value
pub fn extract_checksum_value(
    headers: &HeaderMap<HeaderValue>,
    algo: ChecksumAlgorithm,
) -> Result<ChecksumValue, Error> {
    match algo {
        ChecksumAlgorithm::Crc32 => {
            let crc32 = headers
                .get(xheader::X_AMZ_CHECKSUM_CRC32)
                .and_then(|x| BASE64_STANDARD.decode(x).ok())
                .and_then(|x| x.try_into().ok())
                .ok_or_else(|| Error::Other("invalid x-amz-checksum-crc32 header".into()))?;
            Ok(ChecksumValue::Crc32(crc32))
        }
        ChecksumAlgorithm::Crc32c => {
            let crc32c = headers
                .get(xheader::X_AMZ_CHECKSUM_CRC32C)
                .and_then(|x| BASE64_STANDARD.decode(x).ok())
                .and_then(|x| x.try_into().ok())
                .ok_or_else(|| Error::Other("invalid x-amz-checksum-crc32c header".into()))?;
            Ok(ChecksumValue::Crc32c(crc32c))
        }
        ChecksumAlgorithm::Sha1 => {
            let sha1 = headers
                .get(xheader::X_AMZ_CHECKSUM_SHA1)
                .and_then(|x| BASE64_STANDARD.decode(x).ok())
                .and_then(|x| x.try_into().ok())
                .ok_or_else(|| Error::Other("invalid x-amz-checksum-sha1 header".into()))?;
            Ok(ChecksumValue::Sha1(sha1))
        }
        ChecksumAlgorithm::Sha256 => {
            let sha256 = headers
                .get(xheader::X_AMZ_CHECKSUM_SHA256)
                .and_then(|x| BASE64_STANDARD.decode(x).ok())
                .and_then(|x| x.try_into().ok())
                .ok_or_else(|| Error::Other("invalid x-amz-checksum-sha256 header".into()))?;
            Ok(ChecksumValue::Sha256(sha256))
        }
    }
}

pub fn add_checksum_response_headers(
    checksum: &Option<ChecksumValue>,
    resp: &mut Response,
) -> Result<(), S3Error> {
    match checksum {
        Some(ChecksumValue::Crc32(crc32)) => {
            resp.headers_mut().insert(
                xheader::X_AMZ_CHECKSUM_CRC32,
                HeaderValue::from_str(&BASE64_STANDARD.encode(crc32))?,
            );
        }
        Some(ChecksumValue::Crc32c(crc32c)) => {
            resp.headers_mut().insert(
                xheader::X_AMZ_CHECKSUM_CRC32C,
                HeaderValue::from_str(&BASE64_STANDARD.encode(crc32c)).unwrap(),
            );
        }
        Some(ChecksumValue::Sha1(sha1)) => {
            resp.headers_mut().insert(
                xheader::X_AMZ_CHECKSUM_SHA1,
                HeaderValue::from_str(&BASE64_STANDARD.encode(sha1)).unwrap(),
            );
        }
        Some(ChecksumValue::Sha256(sha256)) => {
            resp.headers_mut().insert(
                xheader::X_AMZ_CHECKSUM_SHA256,
                HeaderValue::from_str(&BASE64_STANDARD.encode(sha256)).unwrap(),
            );
        }
        None => (),
    }
    Ok(())
}
