use std::fmt::Write;
use std::sync::Arc;

use arrayvec::ArrayString;
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
use rpc_client_rss::RpcErrorRss;
use sync_wrapper::SyncWrapper;

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
    app: Arc<AppState>,
    mut req: Request<Body>,
    auth: &Authentication,
) -> Result<VerifiedRequest, Error> {
    let checked_signature =
        match payload::check_payload_signature(app.clone(), auth, &mut req).await {
            Ok(cs) => cs,
            Err(e) => {
                return Err(Error::SignatureError(
                    Box::new(e),
                    Box::new(SyncWrapper::new(req)),
                ))
            }
        };

    let request =
        streaming::parse_streaming_body(req, &checked_signature, &app.config.region, "s3")?;

    let api_key = checked_signature
        .key
        .ok_or(Error::RpcErrorRss(RpcErrorRss::NotFound))?;

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
