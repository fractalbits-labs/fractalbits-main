pub mod sigv4;
pub mod streaming;
pub mod utils;

pub use sigv4::*;
pub use streaming::*;
pub use utils::*;

pub type HmacSha256 = hmac::Hmac<sha2::Sha256>;

pub const AWS4_HMAC_SHA256: &str = "AWS4-HMAC-SHA256";
pub const AWS4_HMAC_SHA256_PAYLOAD: &str = "AWS4-HMAC-SHA256-PAYLOAD";

/// Empty payload SHA256 hash (commonly used in AWS signatures)
pub const EMPTY_PAYLOAD_HASH: &str =
    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

/// Unsigned payload constant
pub const UNSIGNED_PAYLOAD: &str = "UNSIGNED-PAYLOAD";

/// Streaming payload constant
pub const STREAMING_PAYLOAD: &str = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD";

/// Streaming unsigned payload with trailer constant
pub const STREAMING_UNSIGNED_PAYLOAD_TRAILER: &str = "STREAMING-UNSIGNED-PAYLOAD-TRAILER";
