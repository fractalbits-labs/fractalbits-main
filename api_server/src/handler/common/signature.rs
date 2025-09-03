mod error;
pub use error::SignatureError;
pub mod payload;

// Re-export from common crate
pub use aws_signature::streaming::{
    parse_chunk_signature, verify_chunk_signature, ChunkSignature, ChunkSignatureContext,
};

use crate::{handler::common::request::extract::Authentication, AppState};
use actix_web::HttpRequest;
use data_types::{hash::Hash, ApiKey, Versioned};
use rpc_client_rss::RpcErrorRss;
use std::sync::Arc;

pub struct VerifiedRequest {
    pub api_key: Versioned<ApiKey>,
    pub content_sha256_header: ContentSha256Header,
}

#[derive(Debug)]
pub enum ContentSha256Header {
    UnsignedPayload,
    Sha256Checksum(Hash),
    StreamingPayload { trailer: bool, signed: bool },
}

pub async fn verify_request(
    app: Arc<AppState>,
    request: &HttpRequest,
    auth: &Authentication,
) -> Result<VerifiedRequest, SignatureError> {
    let checked_signature = payload::check_payload_signature(app.clone(), auth, request).await?;

    let api_key = checked_signature
        .key
        .ok_or(SignatureError::RpcErrorRss(RpcErrorRss::NotFound))?;

    Ok(VerifiedRequest {
        api_key,
        content_sha256_header: checked_signature.content_sha256_header,
    })
}
