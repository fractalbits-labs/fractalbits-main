use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use axum::{
    extract::FromRequestParts,
    http::{
        header::{AUTHORIZATION, ToStrError},
        request::Parts,
    },
};
use chrono::{DateTime, Duration, NaiveDateTime, TimeZone, Utc};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::{
    AppState,
    handler::common::{
        s3_error::S3Error,
        time::{LONG_DATETIME, SHORT_DATE},
        xheader,
    },
};

const AWS4_HMAC_SHA256: &str = "AWS4-HMAC-SHA256";
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
pub struct Authentication {
    pub key_id: String,
    pub scope: Scope,
    pub signed_headers: BTreeSet<String>,
    pub signature: String,
    pub content_sha256: String,
    pub date: DateTime<Utc>,
}

#[derive(Debug)]
pub struct Scope {
    pub date: String,
    pub region: String,
    pub service: String,
}

impl Scope {
    pub fn to_sign_string(&self) -> String {
        format!(
            "{}/{}/{}/{}",
            self.date, self.region, self.service, SCOPE_ENDING
        )
    }
}

pub struct AuthFromHeaders(pub Option<Authentication>);

impl FromRequestParts<Arc<AppState>> for AuthFromHeaders {
    type Rejection = S3Error;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &Arc<AppState>,
    ) -> Result<Self, Self::Rejection> {
        let config = &state.config;
        match Self::from_request_parts_inner(parts, state) {
            Ok(auth) => Ok(auth),
            Err(e) => {
                if config.allow_missing_or_bad_signature {
                    Ok(Self(None))
                } else {
                    Err(e)
                }
            }
        }
    }
}

impl AuthFromHeaders {
    fn from_request_parts_inner(
        parts: &mut Parts,
        _state: &Arc<AppState>,
    ) -> Result<Self, S3Error> {
        // Note The name of the standard header is unfortunate because it carries authentication
        // information, not authorization.
        // https://docs.aws.amazon.com/AmazonS3/latest/API/RESTAuthentication.html#ConstructingTheAuthenticationHeader
        let authorization = match parts.headers.get(AUTHORIZATION) {
            Some(auth) => auth.to_str()?,
            None => return Err(S3Error::AccessDenied),
        };

        let (auth_kind, rest) = authorization
            .split_once(' ')
            .ok_or(AuthError::Invalid("Authorization field too short".into()))?;

        if auth_kind != AWS4_HMAC_SHA256 {
            return Err(AuthError::Invalid("Unsupported authorization method".into()).into());
        }

        let mut auth_params: HashMap<&str, &str> = HashMap::new();
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
            .map(|s| s.to_string())
            .collect();
        let signature = auth_params
            .get("Signature")
            .ok_or(AuthError::Invalid(
                "Could not find Signature in Authorization field".into(),
            ))?
            .to_string();

        let content_sha256 = parts
            .headers
            .get(xheader::X_AMZ_CONTENT_SHA256)
            .ok_or(AuthError::Invalid(
                "Missing x-amz-content-sha256 field".into(),
            ))?
            .to_str()?;

        let date = parts
            .headers
            .get(xheader::X_AMZ_DATE)
            .ok_or(AuthError::Invalid("Missing x-amz-date field".into()))?
            .to_str()?;
        let date = parse_date(date)?;

        if Utc::now() - date > Duration::hours(24) {
            return Err(AuthError::Invalid("Date is too old".into()).into());
        }
        let (key_id, scope) = parse_credential(cred)?;
        if scope.date != format!("{}", date.format(SHORT_DATE)) {
            return Err(AuthError::Invalid("Date mismatch".into()).into());
        }

        let auth = Authentication {
            key_id,
            scope,
            signed_headers,
            signature,
            content_sha256: content_sha256.to_string(),
            date,
        };
        Ok(Self(Some(auth)))
    }
}
fn parse_date(date: &str) -> Result<DateTime<Utc>, AuthError> {
    let date: NaiveDateTime = NaiveDateTime::parse_from_str(date, LONG_DATETIME)
        .map_err(|_| AuthError::Invalid("Invalid date".into()))?;
    Ok(Utc.from_utc_datetime(&date))
}

fn parse_credential(cred: &str) -> Result<(String, Scope), AuthError> {
    let parts: Vec<&str> = cred.split('/').collect();
    if parts.len() != 5 || parts[4] != SCOPE_ENDING {
        return Err(AuthError::Invalid("wrong scope format".into()));
    }

    let scope = Scope {
        date: parts[1].to_string(),
        region: parts[2].to_string(),
        service: parts[3].to_string(),
    };
    Ok((parts[0].to_string(), scope))
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
    #[inline]
    pub fn add_method(&mut self, method: &str) {
        self.write_str(method);
        self.write_byte(b'\n');
    }

    /// Add URI path to canonical request
    #[inline]
    pub fn add_uri(&mut self, uri: &str) {
        self.write_str(uri);
        self.write_byte(b'\n');
    }

    /// Add query string to canonical request
    #[inline]
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
