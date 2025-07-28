use axum::{
    body::Body,
    extract::rejection::QueryRejection,
    http::{header::ToStrError, Request},
};
use rpc_client_rss::RpcErrorRss;
use sync_wrapper::SyncWrapper;
use thiserror::Error;

/// Errors of this crate
#[derive(Debug, Error)]
pub enum Error {
    // #[error(display = "{}", _0)]
    // /// Error from common error
    // Common(CommonError),
    /// Authorization Header Malformed
    #[error("Authorization header malformed, unexpected scope: {0}")]
    AuthorizationHeaderMalformed(String),

    // Category: bad request
    /// The request contained an invalid UTF-8 sequence in its path or in other parameters
    #[error("Invalid UTF-8: {0}")]
    InvalidUtf8Str(#[from] std::str::Utf8Error),

    /// The provided digest (checksum) value was invalid
    #[error("Invalid digest: {0}")]
    InvalidDigest(String),

    #[error(transparent)]
    QueryRejection(#[from] QueryRejection),

    #[error(transparent)]
    RpcErrorRss(#[from] RpcErrorRss),

    #[error(transparent)]
    FromHexError(#[from] hex::FromHexError),

    #[error(transparent)]
    ToStrError(#[from] ToStrError),

    #[error(transparent)]
    AxumError(#[from] axum::Error),

    #[error("Other: {0}")]
    Other(String),

    #[error("Signature error: {0}")]
    SignatureError(Box<Error>, Box<SyncWrapper<Request<Body>>>),
}

impl From<Box<dyn std::error::Error + Send + Sync>> for Error {
    fn from(err: Box<dyn std::error::Error + Send + Sync>) -> Self {
        Error::Other(err.to_string())
    }
}
