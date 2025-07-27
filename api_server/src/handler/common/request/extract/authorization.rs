use std::collections::BTreeSet;
use std::collections::HashMap;
use std::sync::Arc;

use crate::handler::common::{
    s3_error::S3Error,
    time::{LONG_DATETIME, SHORT_DATE},
    xheader,
};
use crate::AppState;
use axum::{
    extract::FromRequestParts,
    http::{
        header::{ToStrError, AUTHORIZATION},
        request::Parts,
    },
};
use chrono::{DateTime, Duration, NaiveDateTime, TimeZone, Utc};
use thiserror::Error;

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
pub struct AuthFromHeaders(pub Option<Authentication>);

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

        let mut auth_params = HashMap::new();
        for auth_part in rest.split(',') {
            let auth_part = auth_part.trim();
            let eq = auth_part
                .find('=')
                .ok_or(AuthError::Invalid("missing ,".into()))?;
            let (key, value) = auth_part.split_at(eq);
            auth_params.insert(key.to_string(), value.trim_start_matches('=').to_string());
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

        let content_sha256 =
            parts
                .headers
                .get(xheader::X_AMZ_CONTENT_SHA256)
                .ok_or(AuthError::Invalid(
                    "Missing x-amz-content-sha256 field".into(),
                ))?;

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
            content_sha256: content_sha256.to_str()?.to_string(),
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
