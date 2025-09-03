use crate::{streaming::SignatureError, HmacSha256, AWS4_HMAC_SHA256};
use chrono::{DateTime, Utc};
use hmac::Mac;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

/// AWS SigV4 signing parameters
#[derive(Debug, Clone)]
pub struct SigningParams {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub region: String,
    pub service: String,
    pub datetime: DateTime<Utc>,
}

/// Create AWS SigV4 signing key
pub fn get_signing_key(
    datetime: DateTime<Utc>,
    secret_key: &str,
    region: &str,
) -> Result<Vec<u8>, SignatureError> {
    let service = "s3";
    let date_str = datetime.format("%Y%m%d").to_string();

    let mut initial_key = Vec::with_capacity(4 + secret_key.len());
    initial_key.extend_from_slice(b"AWS4");
    initial_key.extend_from_slice(secret_key.as_bytes());

    let mut mac = HmacSha256::new_from_slice(&initial_key)?;
    mac.update(date_str.as_bytes());
    let date_key = mac.finalize().into_bytes();

    let mut mac = HmacSha256::new_from_slice(&date_key)?;
    mac.update(region.as_bytes());
    let region_key = mac.finalize().into_bytes();

    let mut mac = HmacSha256::new_from_slice(&region_key)?;
    mac.update(service.as_bytes());
    let service_key = mac.finalize().into_bytes();

    let mut mac = HmacSha256::new_from_slice(&service_key)?;
    mac.update(b"aws4_request");
    let signing_key = mac.finalize().into_bytes();

    Ok(signing_key.to_vec())
}

/// URI encode a string for AWS SigV4
pub fn uri_encode(string: &str, encode_slash: bool) -> String {
    let mut result = String::with_capacity(string.len() * 2);
    for c in string.chars() {
        match c {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '_' | '-' | '~' | '.' => result.push(c),
            '/' if encode_slash => result.push_str("%2F"),
            '/' if !encode_slash => result.push('/'),
            _ => {
                for byte in c.to_string().bytes() {
                    result.push_str(&format!("%{:02X}", byte));
                }
            }
        }
    }
    result
}

/// Create canonical request for AWS SigV4
pub fn create_canonical_request(
    method: &str,
    canonical_uri: &str,
    query_params: &BTreeMap<String, String>,
    canonical_headers: &[String],
    signed_headers: &BTreeSet<String>,
    payload_hash: &str,
) -> String {
    // Canonical query string
    let canonical_query_string = {
        let mut items = Vec::with_capacity(query_params.len());
        for (key, value) in query_params.iter() {
            items.push(format!(
                "{}={}",
                uri_encode(key, true),
                uri_encode(value, true)
            ));
        }
        items.sort();
        items.join("&")
    };

    let canonical_headers_str = canonical_headers.join("\n");
    let signed_headers_str = signed_headers
        .iter()
        .cloned()
        .collect::<Vec<String>>()
        .join(";");

    format!(
        "{}\n{}\n{}\n{}\n\n{}\n{}",
        method,
        canonical_uri,
        canonical_query_string,
        canonical_headers_str,
        signed_headers_str,
        payload_hash
    )
}

/// Create string to sign for AWS SigV4
pub fn string_to_sign(
    datetime: &DateTime<Utc>,
    credential_scope: &str,
    canonical_request: &str,
) -> String {
    let canonical_request_hash = hex::encode(Sha256::digest(canonical_request.as_bytes()));

    format!(
        "{}\n{}\n{}\n{}",
        AWS4_HMAC_SHA256,
        datetime.format("%Y%m%dT%H%M%SZ"),
        credential_scope,
        canonical_request_hash
    )
}

/// Calculate AWS SigV4 signature
pub fn calculate_signature(
    signing_key: &[u8],
    string_to_sign: &str,
) -> Result<String, SignatureError> {
    let mut mac = HmacSha256::new_from_slice(signing_key)?;
    mac.update(string_to_sign.as_bytes());
    let signature = hex::encode(mac.finalize().into_bytes());
    Ok(signature)
}

/// Create complete authorization header
pub fn create_authorization_header(
    access_key_id: &str,
    credential_scope: &str,
    signed_headers: &BTreeSet<String>,
    signature: &str,
) -> String {
    format!(
        "AWS4-HMAC-SHA256 Credential={}/{}, SignedHeaders={}, Signature={}",
        access_key_id,
        credential_scope,
        signed_headers
            .iter()
            .cloned()
            .collect::<Vec<String>>()
            .join(";"),
        signature
    )
}

/// Sign a complete request using AWS SigV4
pub fn sign_request(
    params: &SigningParams,
    method: &str,
    uri: &str,
    query_params: &BTreeMap<String, String>,
    canonical_headers: &[String],
    signed_headers: &BTreeSet<String>,
    payload_hash: &str,
) -> Result<String, SignatureError> {
    let credential_scope = format!(
        "{}/{}/{}/aws4_request",
        params.datetime.format("%Y%m%d"),
        params.region,
        params.service
    );

    let canonical_request = create_canonical_request(
        method,
        uri,
        query_params,
        canonical_headers,
        signed_headers,
        payload_hash,
    );

    let string_to_sign = string_to_sign(&params.datetime, &credential_scope, &canonical_request);

    let signing_key = get_signing_key(params.datetime, &params.secret_access_key, &params.region)?;
    let signature = calculate_signature(&signing_key, &string_to_sign)?;

    let authorization = create_authorization_header(
        &params.access_key_id,
        &credential_scope,
        signed_headers,
        &signature,
    );

    Ok(authorization)
}

/// Verify signature against expected signature
pub fn verify_signature(
    signing_key: &[u8],
    string_to_sign: &str,
    expected_signature: &str,
) -> Result<bool, SignatureError> {
    let signature_bytes = hex::decode(expected_signature)?;
    let mut mac = HmacSha256::new_from_slice(signing_key)?;
    mac.update(string_to_sign.as_bytes());
    Ok(mac.verify_slice(&signature_bytes).is_ok())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    #[test]
    fn test_uri_encode() {
        assert_eq!(uri_encode("hello world", true), "hello%20world");
        assert_eq!(uri_encode("path/to/file", false), "path/to/file");
        assert_eq!(uri_encode("path/to/file", true), "path%2Fto%2Ffile");
    }

    #[test]
    fn test_get_signing_key() {
        let datetime = Utc::now();
        let key = get_signing_key(datetime, "test_secret", "us-east-1");
        assert!(key.is_ok());
        assert_eq!(key.unwrap().len(), 32); // SHA256 output is 32 bytes
    }

    #[test]
    fn test_create_canonical_request() {
        let mut query_params = BTreeMap::new();
        query_params.insert("param1".to_string(), "value1".to_string());

        let mut headers = BTreeMap::new();
        headers.insert("host".to_string(), "example.com".to_string());
        headers.insert("x-amz-date".to_string(), "20220101T120000Z".to_string());

        let signed_headers: BTreeSet<String> =
            ["host".to_string(), "x-amz-date".to_string()].into();

        // Build canonical headers
        let mut canonical_headers = Vec::new();
        for header_name in &signed_headers {
            if let Some(header_value) = headers.get(header_name) {
                canonical_headers.push(format!("{}:{}", header_name, header_value.trim()));
            }
        }

        let canonical = create_canonical_request(
            "GET",
            "/",
            &query_params,
            &canonical_headers,
            &signed_headers,
            "payload_hash",
        );

        assert!(canonical.contains("GET"));
        assert!(canonical.contains("host:example.com"));
        assert!(canonical.contains("param1=value1"));
        assert!(canonical.contains("payload_hash"));
    }

    #[test]
    fn test_create_string_to_sign() {
        let datetime = Utc::now();
        let credential_scope = "20220101/us-east-1/s3/aws4_request";
        let canonical_request = "GET\n/\n\nhost:example.com\n\nhost\npayload_hash";

        let string_to_sign = string_to_sign(&datetime, credential_scope, canonical_request);

        assert!(string_to_sign.starts_with("AWS4-HMAC-SHA256"));
        assert!(string_to_sign.contains(credential_scope));
    }
}
