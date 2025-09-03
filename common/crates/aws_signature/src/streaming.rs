use crate::{sigv4::get_signing_key, HmacSha256, AWS4_HMAC_SHA256_PAYLOAD};
use chrono::{DateTime, Utc};
use hmac::Mac;
use sha2::{Digest, Sha256};

/// Context needed for chunk signature verification
#[derive(Debug, Clone)]
pub struct ChunkSignatureContext {
    pub signing_key: Vec<u8>,
    pub datetime: DateTime<Utc>,
    pub scope_string: String,
}

/// Result of parsing a chunk signature from chunk extension
#[derive(Debug, Clone, PartialEq)]
pub struct ChunkSignature {
    pub signature: String,
}

/// Error types for signature operations
#[derive(Debug)]
pub enum SignatureError {
    InvalidFormat(String),
    CryptoError(String),
    ValidationError(String),
}

impl std::fmt::Display for SignatureError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SignatureError::InvalidFormat(msg) => write!(f, "Invalid format: {}", msg),
            SignatureError::CryptoError(msg) => write!(f, "Crypto error: {}", msg),
            SignatureError::ValidationError(msg) => write!(f, "Validation error: {}", msg),
        }
    }
}

impl std::error::Error for SignatureError {}

impl From<hex::FromHexError> for SignatureError {
    fn from(err: hex::FromHexError) -> Self {
        SignatureError::CryptoError(format!("Hex decode error: {}", err))
    }
}

impl From<hmac::digest::InvalidLength> for SignatureError {
    fn from(err: hmac::digest::InvalidLength) -> Self {
        SignatureError::CryptoError(format!("HMAC key length error: {}", err))
    }
}

/// Create a chunk signature context
pub fn create_chunk_signature_context(
    datetime: DateTime<Utc>,
    secret_key: &str,
    region: &str,
) -> Result<ChunkSignatureContext, SignatureError> {
    let signing_key = get_signing_key(datetime, secret_key, region)?;
    let scope_string = format!("{}/{}/s3/aws4_request", datetime.format("%Y%m%d"), region);

    Ok(ChunkSignatureContext {
        signing_key,
        datetime,
        scope_string,
    })
}

/// Parse chunk signature from chunk extension
/// Format: "<hex-size>;chunk-signature=<signature>\r\n"
pub fn parse_chunk_signature(chunk_line: &str) -> Option<ChunkSignature> {
    if let Some((_, extensions)) = chunk_line.split_once(';') {
        for extension in extensions.split(';') {
            if let Some(signature) = extension.strip_prefix("chunk-signature=") {
                return Some(ChunkSignature {
                    signature: signature.trim().to_string(),
                });
            }
        }
    }
    None
}

/// Generate string-to-sign for a chunk
pub fn chunk_string_to_sign(
    datetime: &DateTime<Utc>,
    scope_string: &str,
    previous_signature: &str,
    chunk_data: &[u8],
) -> String {
    let empty_payload_hash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

    let mut hasher = Sha256::default();
    hasher.update(chunk_data);
    let chunk_hash = hex::encode(hasher.finalize());

    [
        AWS4_HMAC_SHA256_PAYLOAD,
        &datetime.format("%Y%m%dT%H%M%SZ").to_string(),
        scope_string,
        previous_signature,
        empty_payload_hash,
        &chunk_hash,
    ]
    .join("\n")
}

/// Verify a chunk signature
pub fn verify_chunk_signature(
    context: &ChunkSignatureContext,
    chunk_signature: &ChunkSignature,
    previous_signature: &str,
    chunk_data: &[u8],
) -> Result<(), SignatureError> {
    let string_to_sign = chunk_string_to_sign(
        &context.datetime,
        &context.scope_string,
        previous_signature,
        chunk_data,
    );

    let mut hmac = HmacSha256::new_from_slice(&context.signing_key)?;
    hmac.update(string_to_sign.as_bytes());

    let expected_signature = hex::decode(&chunk_signature.signature)?;
    if hmac.verify_slice(&expected_signature).is_err() {
        return Err(SignatureError::ValidationError(
            "chunk signature mismatch".into(),
        ));
    }

    Ok(())
}

/// Calculate chunk signature for a given chunk
pub fn calculate_chunk_signature(
    context: &ChunkSignatureContext,
    previous_signature: &str,
    chunk_data: &[u8],
) -> Result<String, SignatureError> {
    let string_to_sign = chunk_string_to_sign(
        &context.datetime,
        &context.scope_string,
        previous_signature,
        chunk_data,
    );

    let mut hmac = HmacSha256::new_from_slice(&context.signing_key)?;
    hmac.update(string_to_sign.as_bytes());
    let signature = hex::encode(hmac.finalize().into_bytes());

    Ok(signature)
}

/// Create a complete streaming body with chunk signatures
pub fn create_streaming_body(
    body: &[u8],
    chunk_size: usize,
    context: &ChunkSignatureContext,
    seed_signature: &str,
) -> Result<Vec<u8>, SignatureError> {
    let mut result = Vec::new();
    let mut current_signature = seed_signature.to_string();

    // Process each chunk
    for chunk in body.chunks(chunk_size) {
        let chunk_signature = calculate_chunk_signature(context, &current_signature, chunk)?;

        // Add chunk header with signature
        let chunk_header = format!("{:x};chunk-signature={}\r\n", chunk.len(), chunk_signature);
        result.extend_from_slice(chunk_header.as_bytes());
        result.extend_from_slice(chunk);
        result.extend_from_slice(b"\r\n");

        current_signature = chunk_signature;
    }

    // Add final zero-length chunk
    let final_chunk_signature = calculate_chunk_signature(context, &current_signature, &[])?;
    let final_header = format!("0;chunk-signature={}\r\n\r\n", final_chunk_signature);
    result.extend_from_slice(final_header.as_bytes());

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    #[test]
    fn test_parse_chunk_signature() {
        let chunk_line = "1a;chunk-signature=adadcc";
        let sig = parse_chunk_signature(chunk_line);
        assert!(sig.is_some());
        assert_eq!(sig.unwrap().signature, "adadcc");
    }

    #[test]
    fn test_parse_chunk_signature_no_signature() {
        let chunk_line = "1a";
        let sig = parse_chunk_signature(chunk_line);
        assert!(sig.is_none());
    }

    #[test]
    fn test_chunk_string_to_sign() {
        let datetime = Utc::now();
        let scope = "20130524/us-east-1/s3/aws4_request";
        let prev_signature = "4f232c4386841ef735655705268965c44a0e4690baa4adea153f7db9fa80a0a9";
        let chunk_data = b"a chunk of data";

        let string_to_sign = chunk_string_to_sign(&datetime, scope, prev_signature, chunk_data);

        let lines: Vec<&str> = string_to_sign.lines().collect();
        assert_eq!(lines.len(), 6);
        assert_eq!(lines[0], AWS4_HMAC_SHA256_PAYLOAD);
        assert_eq!(lines[2], scope);
        assert_eq!(lines[3], prev_signature);
        assert_eq!(
            lines[4],
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    #[test]
    fn test_create_streaming_body() {
        let datetime = Utc::now();
        let context = ChunkSignatureContext {
            signing_key: vec![1; 32], // dummy key
            datetime,
            scope_string: "20130524/us-east-1/s3/aws4_request".to_string(),
        };

        let body = b"hello world";
        let result = create_streaming_body(body, 5, &context, "seed_signature");

        assert!(result.is_ok());
        let streaming_body = result.unwrap();

        // Should contain chunk headers and data
        let body_str = String::from_utf8_lossy(&streaming_body);
        assert!(body_str.contains("chunk-signature="));
        assert!(body_str.contains("hello"));
        assert!(body_str.contains(" worl")); // Second chunk
        assert!(body_str.contains("d")); // Third chunk
        assert!(body_str.contains("0;chunk-signature=")); // Final chunk
    }
}
