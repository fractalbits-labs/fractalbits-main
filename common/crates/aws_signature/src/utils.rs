use sha2::{Digest, Sha256};

/// Calculate SHA256 hash and return as hex string
pub fn sha256_hex(data: &[u8]) -> String {
    hex::encode(Sha256::digest(data))
}

/// Calculate SHA256 hash and return as bytes
pub fn sha256_bytes(data: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hasher.finalize().into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sha256_hex() {
        let data = b"hello world";
        let hash = sha256_hex(data);
        assert_eq!(
            hash,
            "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
        );
    }

    #[test]
    fn test_empty_payload_hash() {
        let hash = sha256_hex(b"");
        assert_eq!(hash, crate::EMPTY_PAYLOAD_HASH);
    }

    #[test]
    fn test_sha256_bytes() {
        let data = b"test";
        let hash_bytes = sha256_bytes(data);
        let hash_hex = hex::encode(hash_bytes);
        assert_eq!(hash_hex, sha256_hex(data));
    }
}
