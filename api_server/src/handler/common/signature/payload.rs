use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use actix_web::http::header::{HeaderMap, AUTHORIZATION, HOST};
use actix_web::HttpRequest;
use chrono::{DateTime, Utc};
use data_types::{ApiKey, Versioned};
use hmac::Mac;
use itertools::Itertools;
use rpc_client_rss::RpcErrorRss;
use sha2::{Digest, Sha256};

use crate::handler::common::{request::extract::Authentication, xheader};
use crate::AppState;

use super::super::data::Hash;
use super::super::encoding::uri_encode;
use super::*;

pub struct CheckedSignature {
    pub key: Option<Versioned<ApiKey>>,
    pub content_sha256_header: ContentSha256Header,
    pub signature_header: Option<String>,
}

pub struct VerifiedRequest {
    pub api_key: Versioned<ApiKey>,
    pub content_sha256_header: ContentSha256Header,
}

pub async fn verify_request(
    app: Arc<AppState>,
    request: &HttpRequest,
    auth: &Authentication,
) -> Result<VerifiedRequest, Error> {
    let checked_signature = check_payload_signature(app.clone(), auth, request).await?;

    let api_key = checked_signature
        .key
        .ok_or(Error::RpcErrorRss(RpcErrorRss::NotFound))?;

    Ok(VerifiedRequest {
        api_key,
        content_sha256_header: checked_signature.content_sha256_header,
    })
}

pub async fn check_payload_signature(
    app: Arc<AppState>,
    auth: &Authentication,
    request: &HttpRequest,
) -> Result<CheckedSignature, Error> {
    let query_string = request.query_string();
    let mut query: BTreeMap<String, String> =
        serde_urlencoded::from_str(query_string).unwrap_or_default();

    if query.contains_key(xheader::X_AMZ_ALGORITHM.as_str()) {
        // Presigned URL style authentication
        check_presigned_signature(app, auth, request, &mut query).await
    } else if request.headers().contains_key(AUTHORIZATION.as_str()) {
        // Standard signature authentication
        check_standard_signature(app, auth, request).await
    } else {
        // Unsigned (anonymous) request
        let content_sha256 = request
            .headers()
            .get(xheader::X_AMZ_CONTENT_SHA256.as_str())
            .map(|x| x.to_str())
            .transpose()
            .map_err(|e| Error::Other(format!("Invalid header: {e}")))?;

        Ok(CheckedSignature {
            key: None,
            content_sha256_header: parse_x_amz_content_sha256(content_sha256)?,
            signature_header: None,
        })
    }
}

fn parse_x_amz_content_sha256(header: Option<&str>) -> Result<ContentSha256Header, Error> {
    let header = match header {
        Some(x) => x,
        None => return Ok(ContentSha256Header::UnsignedPayload),
    };
    if header == UNSIGNED_PAYLOAD {
        Ok(ContentSha256Header::UnsignedPayload)
    } else if let Some(rest) = header.strip_prefix("STREAMING-") {
        let (trailer, algo) = if let Some(rest2) = rest.strip_suffix("-TRAILER") {
            (true, rest2)
        } else {
            (false, rest)
        };
        let signed = match algo {
            AWS4_HMAC_SHA256_PAYLOAD => true,
            UNSIGNED_PAYLOAD => false,
            _ => {
                return Err(Error::Other(
                    "invalid or unsupported x-amz-content-sha256".into(),
                ))
            }
        };
        Ok(ContentSha256Header::StreamingPayload { trailer, signed })
    } else {
        let sha256 = hex::decode(header)
            .ok()
            .and_then(|bytes| Hash::try_from(&bytes))
            .ok_or_else(|| Error::Other("Invalid content sha256 hash".into()))?;
        Ok(ContentSha256Header::Sha256Checksum(sha256))
    }
}

pub async fn check_standard_signature(
    app: Arc<AppState>,
    authentication: &Authentication,
    request: &HttpRequest,
) -> Result<CheckedSignature, Error> {
    let query_params: BTreeMap<String, String> =
        serde_urlencoded::from_str(request.query_string()).unwrap_or_default();

    let canonical_request = canonical_request(
        request.method(),
        request.path(),
        &query_params,
        request.headers(),
        &authentication.signed_headers,
        &authentication.content_sha256,
    )?;

    let string_to_sign = string_to_sign(
        &authentication.date,
        &authentication.scope.to_sign_string(),
        &canonical_request,
    );

    tracing::trace!("canonical request:\n{}", canonical_request);
    tracing::trace!("string to sign:\n{}", string_to_sign);

    let key = verify_v4(app, authentication, string_to_sign.as_bytes()).await?;

    let content_sha256_header = parse_x_amz_content_sha256(Some(&authentication.content_sha256))?;

    Ok(CheckedSignature {
        key,
        content_sha256_header,
        signature_header: Some(authentication.signature.clone()),
    })
}

async fn check_presigned_signature(
    app: Arc<AppState>,
    authentication: &Authentication,
    request: &HttpRequest,
    query: &mut BTreeMap<String, String>,
) -> Result<CheckedSignature, Error> {
    let signed_headers = &authentication.signed_headers;

    // Verify signed headers
    verify_signed_headers(request.headers(), signed_headers)?;

    // Remove X-Amz-Signature from query for canonical request calculation
    query.remove(xheader::X_AMZ_SIGNATURE.as_str());

    let canonical_request = canonical_request(
        request.method(),
        request.path(),
        query,
        request.headers(),
        signed_headers,
        &authentication.content_sha256,
    )?;

    let string_to_sign = string_to_sign(
        &authentication.date,
        &authentication.scope.to_sign_string(),
        &canonical_request,
    );

    tracing::trace!("canonical request (presigned url):\n{}", canonical_request);
    tracing::trace!("string to sign (presigned url):\n{}", string_to_sign);

    let key = verify_v4(app, authentication, string_to_sign.as_bytes()).await?;

    Ok(CheckedSignature {
        key,
        content_sha256_header: ContentSha256Header::UnsignedPayload,
        signature_header: Some(authentication.signature.clone()),
    })
}

fn verify_signed_headers(
    headers: &HeaderMap,
    signed_headers: &BTreeSet<String>,
) -> Result<(), Error> {
    if !signed_headers.contains(HOST.as_str()) {
        return Err(Error::Other("Header `Host` should be signed".into()));
    }
    for (name, _) in headers.iter() {
        if name.as_str().starts_with("x-amz-") && !signed_headers.contains(name.as_str()) {
            return Err(Error::Other(format!("Header `{name}` should be signed")));
        }
    }
    Ok(())
}

pub fn string_to_sign(datetime: &DateTime<Utc>, scope_string: &str, canonical_req: &str) -> String {
    let mut hasher = Sha256::default();
    hasher.update(canonical_req.as_bytes());
    [
        "AWS4-HMAC-SHA256",
        &datetime.format("%Y%m%dT%H%M%SZ").to_string(),
        scope_string,
        &format!("{:x}", hasher.finalize()),
    ]
    .join("\n")
}

pub fn canonical_request(
    method: &actix_web::http::Method,
    canonical_uri: &str,
    query_params: &BTreeMap<String, String>,
    headers: &actix_web::http::header::HeaderMap,
    signed_headers: &BTreeSet<String>,
    content_sha256: &str,
) -> Result<String, Error> {
    // Canonical query string from passed query params
    let canonical_query_string = {
        let mut items = Vec::with_capacity(query_params.len());
        for (key, value) in query_params.iter() {
            items.push(uri_encode(key, true) + "=" + &uri_encode(value, true));
        }
        items.sort();
        items.join("&")
    };

    // Canonical header string calculated from signed headers
    let canonical_header_string = signed_headers
        .iter()
        .map(|name| {
            let value = headers
                .get(name)
                .ok_or_else(|| Error::Other(format!("signed header `{name}` is not present")))?;
            // Handle potentially non-ASCII header values (e.g., x-amz-meta-* headers with unicode)
            let value_str = if let Ok(s) = value.to_str() {
                s.to_string()
            } else {
                // For non-ASCII headers, convert bytes to UTF-8 lossy
                String::from_utf8_lossy(value.as_bytes()).to_string()
            };
            Ok(format!("{}:{}", name.as_str(), value_str.trim()))
        })
        .collect::<Result<Vec<String>, Error>>()?
        .join("\n");
    let signed_headers = signed_headers.iter().join(";");

    let list = [
        method.as_str(),
        canonical_uri,
        &canonical_query_string,
        &canonical_header_string,
        "",
        &signed_headers,
        content_sha256,
    ];

    Ok(list.join("\n"))
}

pub async fn verify_v4(
    app: Arc<AppState>,
    auth: &Authentication,
    payload: &[u8],
) -> Result<Option<Versioned<ApiKey>>, Error> {
    let key = app.get_api_key(auth.key_id.clone()).await?;

    let mut hmac = signing_hmac(&auth.date, &key.data.secret_key, &app.config.region)
        .map_err(|_| Error::Other("Unable to build signing HMAC".into()))?;
    hmac.update(payload);
    let signature = hex::decode(&auth.signature)?;
    if hmac.verify_slice(&signature).is_err() {
        return Err(Error::Other("signature mismatch".into()));
    }

    Ok(Some(key))
}
