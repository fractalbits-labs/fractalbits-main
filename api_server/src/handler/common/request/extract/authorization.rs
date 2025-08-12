use std::collections::BTreeSet;

use crate::handler::common::s3_error::S3Error;
use actix_web::dev::Payload;
use actix_web::http::header::ToStrError;
use actix_web::FromRequest;
use actix_web::HttpRequest;
use chrono::{DateTime, TimeZone, Utc};
use futures::future::{ready, Ready};
use thiserror::Error;

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

impl FromRequest for AuthFromHeaders {
    type Error = actix_web::Error;
    type Future = Ready<Result<Self, Self::Error>>;

    fn from_request(req: &HttpRequest, _payload: &mut Payload) -> Self::Future {
        use crate::handler::common::request::extract::{AuthError, Authentication};
        use actix_web::http::header::AUTHORIZATION;
        use chrono::Utc;
        use std::collections::HashMap;

        const AWS4_HMAC_SHA256: &str = "AWS4-HMAC-SHA256";
        const _SCOPE_ENDING: &str = "aws4_request";

        let result = (|| -> Result<Option<Authentication>, AuthError> {
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
                req.headers()
                    .get("x-amz-content-sha256")
                    .ok_or(AuthError::Invalid(
                        "Missing x-amz-content-sha256 field".into(),
                    ))?;

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
            if scope.date != format!("{}", date.format(crate::handler::common::time::SHORT_DATE)) {
                return Err(AuthError::Invalid("Date mismatch".into()));
            }

            let auth = Authentication {
                key_id,
                scope,
                signed_headers,
                signature,
                content_sha256: content_sha256
                    .to_str()
                    .map_err(|e| AuthError::Invalid(format!("Header error: {e}")))?
                    .to_string(),
                date,
            };
            Ok(Some(auth))
        })();

        match result {
            Ok(auth) => ready(Ok(AuthFromHeaders(auth))),
            Err(e) => ready(Err(actix_web::error::ErrorBadRequest(format!(
                "Auth error: {e}"
            )))),
        }
    }
}

fn parse_date(
    date: &str,
) -> Result<chrono::DateTime<chrono::Utc>, crate::handler::common::request::extract::AuthError> {
    use crate::handler::common::request::extract::AuthError;
    use chrono::{NaiveDateTime, Utc};
    let date: NaiveDateTime =
        NaiveDateTime::parse_from_str(date, crate::handler::common::time::LONG_DATETIME)
            .map_err(|_| AuthError::Invalid("Invalid date".into()))?;
    Ok(Utc.from_utc_datetime(&date))
}

fn parse_credential(
    cred: &str,
) -> Result<
    (String, crate::handler::common::request::extract::Scope),
    crate::handler::common::request::extract::AuthError,
> {
    use crate::handler::common::request::extract::{AuthError, Scope};
    const SCOPE_ENDING: &str = "aws4_request";

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

#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::{test, web, App, HttpResponse};
    use chrono::{Datelike, Timelike};

    async fn handler(auth: AuthFromHeaders) -> HttpResponse {
        match auth.0 {
            Some(auth) => HttpResponse::Ok().body(auth.key_id),
            None => HttpResponse::Ok().body("no_auth"),
        }
    }

    #[actix_web::test]
    async fn test_extract_auth_none() {
        let app = test::init_service(App::new().route("/{key:.*}", web::get().to(handler))).await;

        let req = test::TestRequest::get().uri("/obj1").to_request();

        let resp = test::call_service(&app, req).await;
        let body = test::read_body(resp).await;
        let result = String::from_utf8(body.to_vec()).unwrap();
        assert_eq!(result, "no_auth");
    }

    #[test]
    async fn test_parse_credential() {
        let cred = "AKIAIOSFODNN7EXAMPLE/20230101/us-east-1/s3/aws4_request";
        let (key_id, scope) = parse_credential(cred).unwrap();

        assert_eq!(key_id, "AKIAIOSFODNN7EXAMPLE");
        assert_eq!(scope.date, "20230101");
        assert_eq!(scope.region, "us-east-1");
        assert_eq!(scope.service, "s3");
        assert_eq!(scope.to_sign_string(), "20230101/us-east-1/s3/aws4_request");
    }

    #[test]
    async fn test_parse_credential_invalid_format() {
        let cred = "invalid/format";
        assert!(parse_credential(cred).is_err());

        let cred = "key/date/region/service/wrong_ending";
        assert!(parse_credential(cred).is_err());
    }

    #[test]
    async fn test_parse_date() {
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
    async fn test_parse_date_invalid() {
        let date_str = "invalid_date";
        assert!(parse_date(date_str).is_err());

        let date_str = "2023-01-01T12:00:00Z"; // Wrong format
        assert!(parse_date(date_str).is_err());
    }
}
