use std::sync::Arc;

use body::ReqBody;
use data_types::{ApiKey, TraceId, Versioned};

use crate::AppState;
use crate::handler::common::s3_error::S3Error;
use crate::handler::common::signature::payload::CheckedSignature;

use super::request::extract::Authentication;
use axum::body::Body;
use axum::http::request::Request;
use data_types::hash::Hash;
use tracing::{debug, error, warn};

pub mod body;
mod error;
pub use error::SignatureError;
pub mod payload;
pub mod streaming;

// Re-export from common crate
pub use aws_signature::streaming::{
    ChunkSignature, ChunkSignatureContext, parse_chunk_signature, verify_chunk_signature,
};

// Use constants from aws_signature crate
pub use aws_signature::{
    AWS4_HMAC_SHA256, AWS4_HMAC_SHA256_PAYLOAD, EMPTY_PAYLOAD_HASH as EMPTY_STRING_HEX_DIGEST,
    STREAMING_PAYLOAD as STREAMING_AWS4_HMAC_SHA256_PAYLOAD, STREAMING_UNSIGNED_PAYLOAD_TRAILER,
    UNSIGNED_PAYLOAD,
};

#[derive(Debug)]
pub enum ContentSha256Header {
    UnsignedPayload,
    Sha256Checksum(Hash),
    StreamingPayload { trailer: bool, signed: bool },
}

pub async fn check_signature(
    app: Arc<AppState>,
    mut req: Request<Body>,
    auth: Option<&Authentication>,
    trace_id: &TraceId,
) -> Result<(Request<ReqBody>, Versioned<ApiKey>), S3Error> {
    let allow_missing_or_bad_signature = app.config.allow_missing_or_bad_signature;
    debug!(%allow_missing_or_bad_signature, ?auth, "starting signature verification");

    let test_api_key = || async {
        app.get_test_api_key(trace_id)
            .await
            .map_err(|_| S3Error::InvalidAccessKeyId)
    };
    let checked_signature = if let Some(auth) = auth {
        match payload::check_signature_impl(app.clone(), auth, &mut req, trace_id).await {
            Ok(verified) => verified,
            Err(e) => {
                if allow_missing_or_bad_signature {
                    warn!(?auth, error = ?e, "allowed bad signature");
                    CheckedSignature {
                        api_key: test_api_key().await?,
                        content_sha256_header: ContentSha256Header::UnsignedPayload,
                        signature_header: None,
                    }
                } else {
                    error!(?auth, error = ?e, "verifying request failed");
                    return Err(S3Error::from(e));
                }
            }
        }
    } else if allow_missing_or_bad_signature {
        warn!("allowing anonymous access");
        CheckedSignature {
            api_key: test_api_key().await?,
            content_sha256_header: ContentSha256Header::UnsignedPayload,
            signature_header: None,
        }
    } else {
        error!("no valid authentication found");
        return Err(S3Error::InvalidSignature);
    };
    let request =
        streaming::parse_streaming_body(req, &checked_signature, &app.config.region, "s3")?;
    Ok((request, checked_signature.api_key))
}
