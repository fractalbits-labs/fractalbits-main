#![allow(dead_code)]
use std::collections::BTreeSet;
use std::collections::HashMap;

use crate::handler::common::time::{LONG_DATETIME, SHORT_DATE};
use axum::{
    extract::{rejection::QueryRejection, FromRequestParts},
    http::{
        header::{HeaderName, AUTHORIZATION},
        request::Parts,
    },
};
use chrono::{DateTime, Duration, NaiveDateTime, TimeZone, Utc};

const AWS4_HMAC_SHA256: &str = "AWS4-HMAC-SHA256";
const SCOPE_ENDING: &str = "aws4_request";
const X_AMZ_CONTENT_SHA256: HeaderName = HeaderName::from_static("x-amz-content-sha256");
const X_AMZ_DATE: HeaderName = HeaderName::from_static("x-amz-date");

pub struct AuthorizationFromReq(pub Option<Authorization>);

#[derive(Debug)]
pub struct Authorization {
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

impl<S> FromRequestParts<S> for AuthorizationFromReq
where
    S: Send + Sync,
{
    type Rejection = QueryRejection;

    // TODO: better error handling
    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        let authorization = match parts.headers.get(AUTHORIZATION) {
            Some(auth) => auth.to_str().unwrap(),
            None => return Ok(Self(None)),
        };

        let (auth_kind, rest) = authorization
            .split_once(' ')
            .expect("Authorization field too short");

        assert_eq!(
            auth_kind, AWS4_HMAC_SHA256,
            "Unsupported authorization method"
        );

        let mut auth_params = HashMap::new();
        for auth_part in rest.split(',') {
            let auth_part = auth_part.trim();
            let eq = auth_part.find('=').unwrap();
            let (key, value) = auth_part.split_at(eq);
            auth_params.insert(key.to_string(), value.trim_start_matches('=').to_string());
        }

        let cred = auth_params
            .get("Credential")
            .expect("Could not find Credential in Authorization field");
        let signed_headers = auth_params
            .get("SignedHeaders")
            .expect("Could not find SignedHeaders in Authorization field")
            .split(';')
            .map(|s| s.to_string())
            .collect();
        let signature = auth_params
            .get("Signature")
            .expect("Could not find Signature in Authorization field")
            .to_string();

        let content_sha256 = parts
            .headers
            .get(X_AMZ_CONTENT_SHA256)
            .expect("Missing x-amz-content-sha256 field");

        let date = parts
            .headers
            .get(X_AMZ_DATE)
            .expect("Missing x-amz-date field")
            .to_str()
            .unwrap();
        let date = parse_date(date);

        assert!(Utc::now() - date <= Duration::hours(24), "Date is too old");
        let (key_id, scope) = parse_credential(cred);
        assert_eq!(scope.date, format!("{}", date.format(SHORT_DATE)));

        let auth = Authorization {
            key_id,
            scope,
            signed_headers,
            signature,
            content_sha256: content_sha256.to_str().unwrap().to_string(),
            date,
        };
        Ok(Self(Some(auth)))
    }
}

fn parse_date(date: &str) -> DateTime<Utc> {
    let date: NaiveDateTime =
        NaiveDateTime::parse_from_str(date, LONG_DATETIME).expect("Invalid date");
    Utc.from_utc_datetime(&date)
}

fn parse_credential(cred: &str) -> (String, Scope) {
    let parts: Vec<&str> = cred.split('/').collect();
    assert_eq!(5, parts.len());
    assert_eq!(SCOPE_ENDING, parts[4]);

    let scope = Scope {
        date: parts[1].to_string(),
        region: parts[2].to_string(),
        service: parts[3].to_string(),
    };
    (parts[0].to_string(), scope)
}

// TODO: unit test with format like
// "authorization": "AWS4-HMAC-SHA256 Credential=test_api_key/20250222/fractalbits-integ-tests/s3/aws4_request,
// SignedHeaders=content-length;content-type;host;x-amz-content-sha256;x-amz-date;x-amz-user-agent,
// Signature=58a81f5f7074d7ff990b87ce25d1fedf0a54fa3011196ab51c8342bc7609db95",
