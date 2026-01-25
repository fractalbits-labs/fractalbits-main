use hmac::{Hmac, Mac};
use rand::RngCore;
use sha2::Sha256;
use subtle::ConstantTimeEq;
use thiserror::Error;

type HmacSha256 = Hmac<Sha256>;

pub const NONCE_SIZE: usize = 8;
pub const AUTH_RESPONSE_SIZE: usize = 8;
pub const SECRET_SIZE: usize = 32;

#[derive(Error, Debug)]
pub enum RpcAuthError {
    #[error("invalid hex string length: expected {expected}, got {got}")]
    InvalidHexLength { expected: usize, got: usize },
    #[error("invalid hex string: {0}")]
    InvalidHex(#[from] hex::FromHexError),
    #[error("authentication failed")]
    AuthFailed,
}

#[derive(Clone)]
pub struct RpcSecret {
    key: [u8; SECRET_SIZE],
}

impl RpcSecret {
    pub fn from_hex(hex_str: &str) -> Result<Self, RpcAuthError> {
        if hex_str.len() != SECRET_SIZE * 2 {
            return Err(RpcAuthError::InvalidHexLength {
                expected: SECRET_SIZE * 2,
                got: hex_str.len(),
            });
        }
        let mut key = [0u8; SECRET_SIZE];
        hex::decode_to_slice(hex_str, &mut key)?;
        Ok(Self { key })
    }

    pub fn generate_nonce(&self) -> [u8; NONCE_SIZE] {
        let mut nonce = [0u8; NONCE_SIZE];
        rand::thread_rng().fill_bytes(&mut nonce);
        nonce
    }

    pub fn compute_auth(&self, nonce: &[u8; NONCE_SIZE]) -> [u8; AUTH_RESPONSE_SIZE] {
        let mut mac = HmacSha256::new_from_slice(&self.key).expect("HMAC can take key of any size");
        mac.update(nonce);
        let result = mac.finalize();
        let mut auth = [0u8; AUTH_RESPONSE_SIZE];
        auth.copy_from_slice(&result.into_bytes()[..AUTH_RESPONSE_SIZE]);
        auth
    }

    pub fn verify_auth(
        &self,
        nonce: &[u8; NONCE_SIZE],
        response: &[u8; AUTH_RESPONSE_SIZE],
    ) -> bool {
        let expected = self.compute_auth(nonce);
        expected.ct_eq(response).into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_SECRET_HEX: &str =
        "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    #[test]
    fn test_from_hex_valid() {
        let secret = RpcSecret::from_hex(TEST_SECRET_HEX);
        assert!(secret.is_ok());
    }

    #[test]
    fn test_from_hex_invalid_length() {
        let result = RpcSecret::from_hex("0123456789abcdef");
        assert!(matches!(result, Err(RpcAuthError::InvalidHexLength { .. })));
    }

    #[test]
    fn test_from_hex_invalid_chars() {
        let invalid_hex = "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz";
        let result = RpcSecret::from_hex(invalid_hex);
        assert!(matches!(result, Err(RpcAuthError::InvalidHex(_))));
    }

    #[test]
    fn test_compute_auth_deterministic() {
        let secret = RpcSecret::from_hex(TEST_SECRET_HEX).unwrap();
        let nonce = [1u8, 2, 3, 4, 5, 6, 7, 8];

        let auth1 = secret.compute_auth(&nonce);
        let auth2 = secret.compute_auth(&nonce);

        assert_eq!(auth1, auth2);
    }

    #[test]
    fn test_verify_auth_success() {
        let secret = RpcSecret::from_hex(TEST_SECRET_HEX).unwrap();
        let nonce = [1u8, 2, 3, 4, 5, 6, 7, 8];

        let auth = secret.compute_auth(&nonce);
        assert!(secret.verify_auth(&nonce, &auth));
    }

    #[test]
    fn test_verify_auth_failure_wrong_response() {
        let secret = RpcSecret::from_hex(TEST_SECRET_HEX).unwrap();
        let nonce = [1u8, 2, 3, 4, 5, 6, 7, 8];

        let wrong_auth = [0u8; AUTH_RESPONSE_SIZE];
        assert!(!secret.verify_auth(&nonce, &wrong_auth));
    }

    #[test]
    fn test_verify_auth_failure_wrong_nonce() {
        let secret = RpcSecret::from_hex(TEST_SECRET_HEX).unwrap();
        let nonce1 = [1u8, 2, 3, 4, 5, 6, 7, 8];
        let nonce2 = [8u8, 7, 6, 5, 4, 3, 2, 1];

        let auth = secret.compute_auth(&nonce1);
        assert!(!secret.verify_auth(&nonce2, &auth));
    }

    #[test]
    fn test_different_secrets_produce_different_auth() {
        let secret1 = RpcSecret::from_hex(TEST_SECRET_HEX).unwrap();
        let secret2 =
            RpcSecret::from_hex("fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210")
                .unwrap();
        let nonce = [1u8, 2, 3, 4, 5, 6, 7, 8];

        let auth1 = secret1.compute_auth(&nonce);
        let auth2 = secret2.compute_auth(&nonce);

        assert_ne!(auth1, auth2);
    }

    #[test]
    fn test_generate_nonce_produces_different_values() {
        let secret = RpcSecret::from_hex(TEST_SECRET_HEX).unwrap();

        let nonce1 = secret.generate_nonce();
        let nonce2 = secret.generate_nonce();

        assert_ne!(nonce1, nonce2);
    }

    #[test]
    fn test_cross_language_compatibility() {
        // Known test vector for cross-language verification
        // Secret: 0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
        // Nonce: [1, 2, 3, 4, 5, 6, 7, 8]
        // Expected auth should match between Rust and Zig implementations
        let secret = RpcSecret::from_hex(TEST_SECRET_HEX).unwrap();
        let nonce = [1u8, 2, 3, 4, 5, 6, 7, 8];
        let auth = secret.compute_auth(&nonce);

        // HMAC-SHA256(key, nonce)[0:8] - first 8 bytes of HMAC output
        // This value was computed and verified against the Zig implementation
        let expected: [u8; 8] = [0x22, 0xb3, 0xf6, 0x1e, 0xcc, 0x3c, 0x8e, 0xfc];
        assert_eq!(
            auth, expected,
            "Auth mismatch - update expected value if algorithm changes"
        );
    }
}
