use axum::{
    body::Body,
    extract::rejection::QueryRejection,
    http::{Request, header::ToStrError},
};
use rpc_client_common::RpcError;
use sync_wrapper::SyncWrapper;
use thiserror::Error;

/// Errors of this crate
#[derive(Debug, Error)]
pub enum SignatureError {
    /// Authorization Header Malformed
    #[error("Authorization header malformed, unexpected scope: {0}")]
    AuthorizationHeaderMalformed(String),

    /// The request contained an invalid UTF-8 sequence in its path or in other parameters
    #[error("Invalid UTF-8: {0}")]
    InvalidUtf8Str(#[from] std::str::Utf8Error),

    /// The provided digest (checksum) value was invalid
    #[error("Invalid digest: {0}")]
    InvalidDigest(String),

    #[error(transparent)]
    QueryRejection(#[from] QueryRejection),

    #[error(transparent)]
    RpcError(#[from] RpcError),

    #[error(transparent)]
    FromHexError(#[from] hex::FromHexError),

    #[error(transparent)]
    ToStrError(#[from] ToStrError),

    #[error(transparent)]
    AxumError(#[from] axum::Error),

    #[error("Other: {0}")]
    Other(String),

    #[error("Signature error: {0}")]
    SignatureError(Box<SignatureError>, Box<SyncWrapper<Request<Body>>>),
}

impl From<aws_signature::SignatureError> for SignatureError {
    fn from(err: aws_signature::SignatureError) -> Self {
        SignatureError::Other(err.to_string())
    }
}
