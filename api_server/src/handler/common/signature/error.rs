use axum::{extract::rejection::QueryRejection, http::header::ToStrError};
use rpc_client_rss::RpcErrorRss;
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
}
