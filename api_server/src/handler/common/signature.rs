use body::ReqBody;
use bucket_tables::{api_key_table::ApiKey, Versioned};
use chrono::{DateTime, Utc};
use hmac::{Hmac, Mac};
use sha2::Sha256;

use crate::AppState;

use super::data::{sha256sum, Hash};
use super::request::extract::Authentication;
use axum::body::Body;
use axum::http::{request::Request, HeaderName};

pub use error::*;

pub mod body;
pub mod checksum;
pub mod error;
pub mod payload;
pub mod streaming;

pub const SHORT_DATE: &str = "%Y%m%d";
pub const LONG_DATETIME: &str = "%Y%m%dT%H%M%SZ";

/// Result of `sha256("")`
pub(crate) const EMPTY_STRING_HEX_DIGEST: &str =
    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

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

pub struct VerifiedRequest {
    pub request: Request<ReqBody>,
    pub api_key: Versioned<ApiKey>,
    pub content_sha256_header: ContentSha256Header,
}

pub async fn verify_request(
    app: &AppState,
    mut req: Request<Body>,
    auth: &Authentication,
) -> Result<VerifiedRequest, Error> {
    let checked_signature = payload::check_payload_signature(app, auth, &mut req).await?;

    let request =
        streaming::parse_streaming_body(req, &checked_signature, &app.config.region, "s3")?;

    let api_key = checked_signature
        .key
        .ok_or_else(|| Error::Other("Fractalbits does not support anonymous access yet".into()))?;

    Ok(VerifiedRequest {
        request,
        api_key,
        content_sha256_header: checked_signature.content_sha256_header,
    })
}

pub fn signing_hmac(
    datetime: &DateTime<Utc>,
    secret_key: &str,
    region: &str,
) -> Result<HmacSha256, hmac::digest::InvalidLength> {
    let service = "s3";
    let secret = String::from("AWS4") + secret_key;
    let mut date_hmac = HmacSha256::new_from_slice(secret.as_bytes())?;
    date_hmac.update(datetime.format(SHORT_DATE).to_string().as_bytes());
    let mut region_hmac = HmacSha256::new_from_slice(&date_hmac.finalize().into_bytes())?;
    region_hmac.update(region.as_bytes());
    let mut service_hmac = HmacSha256::new_from_slice(&region_hmac.finalize().into_bytes())?;
    service_hmac.update(service.as_bytes());
    let mut signing_hmac = HmacSha256::new_from_slice(&service_hmac.finalize().into_bytes())?;
    signing_hmac.update(b"aws4_request");
    let hmac = HmacSha256::new_from_slice(&signing_hmac.finalize().into_bytes())?;
    Ok(hmac)
}

pub fn compute_scope(datetime: &DateTime<Utc>, region: &str, service: &str) -> String {
    format!(
        "{}/{}/{}/aws4_request",
        datetime.format(SHORT_DATE),
        region,
        service
    )
}
