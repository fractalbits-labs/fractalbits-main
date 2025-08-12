use std::fmt::Write;

use arrayvec::ArrayString;
use chrono::{DateTime, Utc};
use hmac::{Hmac, Mac};
use sha2::Sha256;

use super::data::Hash;

pub use error::*;

pub mod checksum;
pub mod error;
pub mod payload;

pub const SHORT_DATE: &str = "%Y%m%d";
pub const LONG_DATETIME: &str = "%Y%m%dT%H%M%SZ";

// Signature calculation algorithm
pub const AWS4_HMAC_SHA256: &str = "AWS4-HMAC-SHA256";
type HmacSha256 = Hmac<Sha256>;

// Possible values for x-amz-content-sha256, in addition to the actual sha256
pub const UNSIGNED_PAYLOAD: &str = "UNSIGNED-PAYLOAD";
pub const STREAMING_UNSIGNED_PAYLOAD_TRAILER: &str = "STREAMING-UNSIGNED-PAYLOAD-TRAILER";
pub const STREAMING_AWS4_HMAC_SHA256_PAYLOAD: &str = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD";

// Used in the computation of StringToSign
pub const AWS4_HMAC_SHA256_PAYLOAD: &str = "AWS4-HMAC-SHA256-PAYLOAD";

// ---- enums to describe stuff going on in signature calculation ----

#[derive(Debug)]
pub enum ContentSha256Header {
    UnsignedPayload,
    Sha256Checksum(Hash),
    StreamingPayload { trailer: bool, signed: bool },
}

// ---- top-level functions ----

pub fn signing_hmac(
    datetime: &DateTime<Utc>,
    secret_key: &str,
    region: &str,
) -> Result<HmacSha256, hmac::digest::InvalidLength> {
    let service = "s3";

    let mut initial_key = Vec::with_capacity(4 + secret_key.len());
    initial_key.extend_from_slice(b"AWS4");
    initial_key.extend_from_slice(secret_key.as_bytes());

    let mut date_str = ArrayString::<8>::new();
    write!(&mut date_str, "{}", datetime.format(SHORT_DATE))
        .expect("Formatting a date into an 8-byte ArrayString should not fail");

    let mut mac = HmacSha256::new_from_slice(&initial_key)?;
    mac.update(date_str.as_bytes());
    let key = mac.finalize().into_bytes();

    let mut mac = HmacSha256::new_from_slice(&key)?;
    mac.update(region.as_bytes());
    let key = mac.finalize().into_bytes();

    let mut mac = HmacSha256::new_from_slice(&key)?;
    mac.update(service.as_bytes());
    let key = mac.finalize().into_bytes();

    let mut mac = HmacSha256::new_from_slice(&key)?;
    mac.update(b"aws4_request");
    let signing_key = mac.finalize().into_bytes();

    HmacSha256::new_from_slice(&signing_key)
}

pub fn compute_scope(datetime: &DateTime<Utc>, region: &str, service: &str) -> String {
    format!(
        "{}/{}/{}/aws4_request",
        datetime.format(SHORT_DATE),
        region,
        service
    )
}
