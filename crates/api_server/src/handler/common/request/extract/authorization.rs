use std::collections::{BTreeSet, HashMap};

use actix_web::{
    HttpRequest,
    http::header::{AUTHORIZATION, ToStrError},
};
use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::handler::common::{
    s3_error::S3Error,
    time::{LONG_DATETIME, SHORT_DATE},
};

const SCOPE_ENDING: &str = "aws4_request";

#[derive(Error, Debug)]
pub enum AuthError {
    #[error(transparent)]
    ToStrError(#[from] ToStrError),
    #[error("invalid format: {0}")]
    Invalid(String),
}

impl From<AuthError> for S3Error {
    fn from(value: AuthError) -> Self {
        tracing::error!("AuthError: {value}");
        S3Error::AuthorizationHeaderMalformed
    }
}

#[derive(Debug)]
pub struct Authentication<'a> {
    pub key_id: &'a str,
    pub scope: Scope<'a>,
    pub signed_headers: BTreeSet<&'a str>,
    pub signature: &'a str,
    pub content_sha256: &'a str,
    pub date: DateTime<Utc>,
    pub scope_string: String,
    pub formatted_date: String,
}

#[derive(Debug)]
pub struct Scope<'a> {
    pub date: &'a str,
    pub region: &'a str,
    pub service: &'a str,
}

/// Extract authentication from request with zero-copy optimization
pub fn extract_authentication(req: &HttpRequest) -> Result<Option<Authentication<'_>>, AuthError> {
    const AWS4_HMAC_SHA256: &str = "AWS4-HMAC-SHA256";

    let authorization = match req.headers().get(AUTHORIZATION) {
        Some(auth) => auth
            .to_str()
            .map_err(|e| AuthError::Invalid(format!("Header error: {e}")))?,
        None => return Ok(None),
    };

    let (auth_kind, rest) = authorization
        .split_once(' ')
        .ok_or(AuthError::Invalid("Authorization field too short".into()))?;

    if auth_kind != AWS4_HMAC_SHA256 {
        return Err(AuthError::Invalid(
            "Unsupported authorization method".into(),
        ));
    }

    let mut auth_params = HashMap::new();
    for auth_part in rest.split(',') {
        let auth_part = auth_part.trim();
        let eq = auth_part
            .find('=')
            .ok_or(AuthError::Invalid("missing =".into()))?;
        let (key, value) = auth_part.split_at(eq);
        auth_params.insert(key, value.trim_start_matches('='));
    }

    let cred = auth_params.get("Credential").ok_or(AuthError::Invalid(
        "Could not find Credential in Authorization field".into(),
    ))?;
    let signed_headers = auth_params
        .get("SignedHeaders")
        .ok_or(AuthError::Invalid(
            "Could not find SignedHeaders in Authorization field".into(),
        ))?
        .split(';')
        .collect();
    let signature = auth_params.get("Signature").ok_or(AuthError::Invalid(
        "Could not find Signature in Authorization field".into(),
    ))?;

    let content_sha256 = req
        .headers()
        .get("x-amz-content-sha256")
        .ok_or(AuthError::Invalid(
            "Missing x-amz-content-sha256 field".into(),
        ))?
        .to_str()
        .map_err(|e| AuthError::Invalid(format!("Header error: {e}")))?;

    let date = req
        .headers()
        .get("x-amz-date")
        .ok_or(AuthError::Invalid("Missing x-amz-date field".into()))?
        .to_str()
        .map_err(|e| AuthError::Invalid(format!("Header error: {e}")))?;
    let date = parse_date(date)?;

    if (Utc::now() - date).num_hours() > 24 {
        return Err(AuthError::Invalid("Date is too old".into()));
    }
    let (key_id, scope) = parse_credential(cred)?;
    if scope.date != format!("{}", date.format(SHORT_DATE)) {
        return Err(AuthError::Invalid("Date mismatch".into()));
    }

    let scope_string =
        aws_signature::sigv4::format_scope_string(&date, scope.region, scope.service);
    let formatted_date = format!("{}", date.format("%Y%m%dT%H%M%SZ"));

    let auth = Authentication {
        key_id,
        scope,
        signed_headers,
        signature,
        content_sha256,
        date,
        scope_string,
        formatted_date,
    };
    Ok(Some(auth))
}

fn parse_date(date: &str) -> Result<DateTime<Utc>, AuthError> {
    let date: NaiveDateTime = NaiveDateTime::parse_from_str(date, LONG_DATETIME)
        .map_err(|_| AuthError::Invalid("Invalid date".into()))?;
    Ok(Utc.from_utc_datetime(&date))
}

fn parse_credential(cred: &str) -> Result<(&str, Scope<'_>), AuthError> {
    let parts: Vec<&str> = cred.split('/').collect();
    if parts.len() != 5 || parts[4] != SCOPE_ENDING {
        return Err(AuthError::Invalid("wrong scope format".into()));
    }

    let scope = Scope {
        date: parts[1],
        region: parts[2],
        service: parts[3],
    };
    Ok((parts[0], scope))
}

/// Zero-copy SigV4 canonical request hasher
/// Streams bytes directly into SHA-256 without intermediate buffers
pub struct CanonicalRequestHasher {
    hasher: Sha256,
}

impl CanonicalRequestHasher {
    pub fn new() -> Self {
        Self {
            hasher: Sha256::new(),
        }
    }

    #[inline]
    fn write_str(&mut self, s: &str) {
        self.hasher.update(s.as_bytes());
    }

    #[inline]
    fn write_byte(&mut self, b: u8) {
        self.hasher.update([b]);
    }

    /// Add HTTP method to canonical request
    pub fn add_method(&mut self, method: &str) {
        self.write_str(method);
        self.write_byte(b'\n');
    }

    /// Add URI path to canonical request
    pub fn add_uri(&mut self, uri: &str) {
        self.write_str(uri);
        self.write_byte(b'\n');
    }

    /// Add query string to canonical request
    pub fn add_query(&mut self, query: &str) {
        self.write_str(query);
        self.write_byte(b'\n');
    }

    /// Add canonical headers in sorted order
    /// Headers should be pre-sorted by the caller
    pub fn add_headers<'a, I>(&mut self, headers: I)
    where
        I: Iterator<Item = (&'a str, &'a str)>,
    {
        for (name, value) in headers {
            self.write_str(name);
            self.write_byte(b':');
            self.write_str(value);
            self.write_byte(b'\n');
        }
        self.write_byte(b'\n');
    }

    /// Add signed headers list
    pub fn add_signed_headers<'a, I>(&mut self, signed_headers: I)
    where
        I: Iterator<Item = &'a str>,
    {
        let mut first = true;
        for header in signed_headers {
            if !first {
                self.write_byte(b';');
            }
            self.write_str(header);
            first = false;
        }
        self.write_byte(b'\n');
    }

    /// Add payload hash
    pub fn add_payload_hash(&mut self, hash: &str) {
        self.write_str(hash);
    }

    /// Finalize and return the hex-encoded hash
    pub fn finalize(self) -> String {
        let result = self.hasher.finalize();
        hex::encode(result)
    }

    /// Finalize and return raw hash bytes
    pub fn finalize_bytes(self) -> [u8; 32] {
        self.hasher.finalize().into()
    }
}

impl Default for CanonicalRequestHasher {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::test::TestRequest;
    use chrono::{Datelike, Timelike};

    #[test]
    fn test_extract_auth_none() {
        let (req, _) = TestRequest::get().uri("/obj1").to_http_parts();
        let auth = extract_authentication(&req).unwrap();
        assert!(auth.is_none());
    }

    #[test]
    fn test_parse_credential() {
        let cred = "AKIAIOSFODNN7EXAMPLE/20230101/us-east-1/s3/aws4_request";
        let (key_id, scope) = parse_credential(cred).unwrap();

        assert_eq!(key_id, "AKIAIOSFODNN7EXAMPLE");
        assert_eq!(scope.date, "20230101");
        assert_eq!(scope.region, "us-east-1");
        assert_eq!(scope.service, "s3");
        // Verify the scope can be formatted correctly
        let expected_scope = format!(
            "{}/{}/{}/{}",
            scope.date, scope.region, scope.service, SCOPE_ENDING
        );
        assert_eq!(expected_scope, "20230101/us-east-1/s3/aws4_request");
    }

    #[test]
    fn test_parse_credential_invalid_format() {
        let cred = "invalid/format";
        assert!(parse_credential(cred).is_err());

        let cred = "key/date/region/service/wrong_ending";
        assert!(parse_credential(cred).is_err());
    }

    #[test]
    fn test_parse_date() {
        let date_str = "20230101T120000Z";
        let parsed = parse_date(date_str).unwrap();

        assert_eq!(parsed.year(), 2023);
        assert_eq!(parsed.month(), 1);
        assert_eq!(parsed.day(), 1);
        assert_eq!(parsed.hour(), 12);
        assert_eq!(parsed.minute(), 0);
        assert_eq!(parsed.second(), 0);
    }

    #[test]
    fn test_parse_date_invalid() {
        let date_str = "invalid_date";
        assert!(parse_date(date_str).is_err());

        let date_str = "2023-01-01T12:00:00Z"; // Wrong format
        assert!(parse_date(date_str).is_err());
    }

    #[test]
    fn test_canonical_request_hasher() {
        let mut hasher = CanonicalRequestHasher::new();
        hasher.add_method("GET");
        hasher.add_uri("/test");
        hasher.add_query("");
        hasher.add_headers(
            [("host", "example.com"), ("x-amz-date", "20230101T120000Z")]
                .iter()
                .copied(),
        );
        hasher.add_signed_headers(["host", "x-amz-date"].iter().copied());
        hasher.add_payload_hash("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");

        let hash = hasher.finalize();
        assert!(!hash.is_empty());
        assert_eq!(hash.len(), 64);
    }
}
