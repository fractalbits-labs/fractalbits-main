use aws_signature::sigv4::uri_encode;
use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use axum::RequestExt;
use axum::body::Body;
use axum::extract::Query;
use axum::http::header::{HeaderMap, HeaderValue};
use axum::http::{Method, request::Request};
use data_types::hash::Hash;
use data_types::{ApiKey, TraceId, Versioned};

use crate::handler::common::signature;
use crate::{
    AppState,
    handler::common::{
        request::extract::{Authentication, CanonicalRequestHasher},
        signature::{ContentSha256Header, SignatureError},
    },
};
use aws_signature::{UNSIGNED_PAYLOAD, get_signing_key_cached, verify_signature};

pub struct CheckedSignature {
    pub api_key: Versioned<ApiKey>,
    pub content_sha256_header: ContentSha256Header,
    pub signature_header: Option<String>,
}

pub async fn check_signature_impl(
    app: Arc<AppState>,
    auth: &Authentication,
    request: &mut Request<Body>,
    trace_id: &TraceId,
) -> Result<CheckedSignature, SignatureError> {
    check_header_based_signature(app, auth, request, trace_id).await
}

async fn check_header_based_signature(
    app: Arc<AppState>,
    authentication: &Authentication,
    request: &mut Request<Body>,
    trace_id: &TraceId,
) -> Result<CheckedSignature, SignatureError> {
    let query_params: Query<BTreeMap<String, String>> = request.extract_parts().await?;
    let canonical_hash = hash_canonical_request_streaming(
        request.method(),
        request.uri(),
        &query_params,
        request.headers(),
        &authentication.signed_headers,
        &authentication.content_sha256,
    )?;

    let string_to_sign = build_string_to_sign(
        &authentication.date,
        &authentication.scope.to_sign_string(),
        &canonical_hash,
    );

    tracing::trace!(?authentication, %canonical_hash, %string_to_sign);

    let key = verify_v4(app, authentication, &string_to_sign, trace_id).await?;
    let content_sha256_header = parse_x_amz_content_sha256(&authentication.content_sha256)?;

    Ok(CheckedSignature {
        api_key: key,
        content_sha256_header,
        signature_header: Some(authentication.signature.clone()),
    })
}

fn parse_x_amz_content_sha256(header: &str) -> Result<ContentSha256Header, SignatureError> {
    if header == UNSIGNED_PAYLOAD {
        Ok(ContentSha256Header::UnsignedPayload)
    } else if let Some(rest) = header.strip_prefix("STREAMING-") {
        let (trailer, algo) = if let Some(rest2) = rest.strip_suffix("-TRAILER") {
            (true, rest2)
        } else {
            (false, rest)
        };
        let signed = match algo {
            signature::AWS4_HMAC_SHA256_PAYLOAD => true,
            signature::UNSIGNED_PAYLOAD => false,
            _ => {
                return Err(SignatureError::Other(
                    "invalid or unsupported x-amz-content-sha256".into(),
                ));
            }
        };
        Ok(ContentSha256Header::StreamingPayload { trailer, signed })
    } else {
        let sha256 = hex::decode(header)
            .ok()
            .and_then(|bytes| Hash::try_from(&bytes))
            .ok_or_else(|| SignatureError::Other("Invalid content sha256 hash".into()))?;
        Ok(ContentSha256Header::Sha256Checksum(sha256))
    }
}

/// Stream canonical request directly to hasher
fn hash_canonical_request_streaming(
    method: &Method,
    uri: &axum::http::Uri,
    query_params: &BTreeMap<String, String>,
    headers: &HeaderMap<HeaderValue>,
    signed_headers: &BTreeSet<String>,
    payload_hash: &str,
) -> Result<String, SignatureError> {
    let mut hasher = CanonicalRequestHasher::new();

    // Stream HTTP method
    hasher.add_method(method.as_str());

    // Stream URI
    hasher.add_uri(uri.path());

    // Build and stream canonical query string
    let query_str = build_canonical_query_string(query_params);
    hasher.add_query(&query_str);

    let mut header_pairs: Vec<(&str, Cow<'_, str>)> = Vec::with_capacity(signed_headers.len());

    for header_name in signed_headers {
        let value = if header_name == "host" {
            if let Some(host) = headers.get(header_name.as_str()) {
                let host_str = std::str::from_utf8(host.as_bytes())?;
                let trimmed = host_str.trim();
                if trimmed.len() == host_str.len() {
                    Cow::Borrowed(trimmed)
                } else {
                    Cow::Owned(trimmed.to_string())
                }
            } else if let Some(authority) = uri.authority() {
                Cow::Borrowed(authority.as_str())
            } else {
                return Err(SignatureError::Other(format!(
                    "signed header `{header_name}` is not present"
                )));
            }
        } else if let Some(header_value) = headers.get(header_name.as_str()) {
            let value_str = std::str::from_utf8(header_value.as_bytes())?;
            let trimmed = value_str.trim();
            if trimmed.len() == value_str.len() {
                Cow::Borrowed(trimmed)
            } else {
                Cow::Owned(trimmed.to_string())
            }
        } else {
            return Err(SignatureError::Other(format!(
                "signed header `{header_name}` is not present"
            )));
        };
        header_pairs.push((header_name.as_str(), value));
    }

    hasher.add_headers(header_pairs.iter().map(|(k, v)| (*k, v.as_ref())));

    // Stream signed headers list
    hasher.add_signed_headers(signed_headers.iter().map(|s| s.as_str()));

    // Stream payload hash
    hasher.add_payload_hash(payload_hash);

    // Return hex hash (no canonical request string ever created!)
    Ok(hasher.finalize())
}

fn build_canonical_query_string(query_params: &BTreeMap<String, String>) -> String {
    if query_params.is_empty() {
        return String::new();
    }
    let query_len = query_params
        .iter()
        .fold(0, |acc, (k, v)| acc + k.len() + v.len());
    let mut items = String::with_capacity(query_len * 2);
    for (key, value) in query_params.iter() {
        if !items.is_empty() {
            items.push('&');
        }
        items.push_str(&uri_encode(key, true));
        items.push('=');
        items.push_str(&uri_encode(value, true));
    }
    items
}

fn build_string_to_sign(
    date: &chrono::DateTime<chrono::Utc>,
    credential_scope: &str,
    canonical_hash: &str,
) -> String {
    const AWS4_HMAC_SHA256: &str = "AWS4-HMAC-SHA256";
    let formatted_datetime = format!("{}", date.format("%Y%m%dT%H%M%SZ"));
    let mut result = String::with_capacity(
        AWS4_HMAC_SHA256.len() + 1 + formatted_datetime.len() + 1 + credential_scope.len() + 1 + 64,
    );
    result.push_str(AWS4_HMAC_SHA256);
    result.push('\n');
    result.push_str(&formatted_datetime);
    result.push('\n');
    result.push_str(credential_scope);
    result.push('\n');
    result.push_str(canonical_hash);
    result
}

async fn verify_v4(
    app: Arc<AppState>,
    auth: &Authentication,
    string_to_sign: &str,
    trace_id: &TraceId,
) -> Result<Versioned<ApiKey>, SignatureError> {
    let key = app.get_api_key(auth.key_id.to_string(), trace_id).await?;

    let signing_key =
        get_signing_key_cached(auth.date, &key.data.secret_key, &app.config.region)
            .map_err(|e| SignatureError::Other(format!("Unable to build signing key: {}", e)))?;

    if !verify_signature(&signing_key, string_to_sign, &auth.signature)? {
        return Err(SignatureError::Other("signature mismatch".into()));
    }

    Ok(key)
}
