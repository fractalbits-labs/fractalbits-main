use actix_web::http::header::ToStrError;
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

    #[error("Query parsing error: {0}")]
    QueryParsingError(String),

    #[error(transparent)]
    RpcErrorRss(#[from] RpcErrorRss),

    #[error(transparent)]
    FromHexError(#[from] hex::FromHexError),

    #[error(transparent)]
    ToStrError(#[from] ToStrError),

    #[error("HTTP processing error: {0}")]
    HttpProcessingError(String),

    // Generic HTTP body error that can come from any body implementation
    #[error("Body error: {0}")]
    BodyError(String),

    #[error("Other: {0}")]
    Other(String),
}

impl From<Box<dyn std::error::Error + Send + Sync>> for Error {
    fn from(err: Box<dyn std::error::Error + Send + Sync>) -> Self {
        Error::Other(err.to_string())
    }
}

impl From<std::convert::Infallible> for Error {
    fn from(_: std::convert::Infallible) -> Self {
        // Infallible can never actually be constructed, so this is unreachable
        unreachable!("Infallible can never be constructed")
    }
}
