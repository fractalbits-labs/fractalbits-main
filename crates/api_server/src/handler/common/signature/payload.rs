use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use crate::{
    AppState,
    handler::common::{
        request::extract::{Authentication, CanonicalRequestHasher},
        signature::SignatureError,
    },
};
use actix_web::{HttpRequest, http::header::HOST, web::Query};
use aws_signature::{get_signing_key, verify_signature};
use data_types::{ApiKey, TraceId, Versioned};

pub async fn check_signature_impl(
    app: Arc<AppState>,
    auth: &Authentication<'_>,
    request: &HttpRequest,
    trace_id: &TraceId,
) -> Result<Versioned<ApiKey>, SignatureError> {
    // Support HTTP Authorization header based signature only for now
    check_header_based_signature(app, auth, request, trace_id).await
}

async fn check_header_based_signature(
    app: Arc<AppState>,
    authentication: &Authentication<'_>,
    request: &HttpRequest,
    trace_id: &TraceId,
) -> Result<Versioned<ApiKey>, SignatureError> {
    let query_params = Query::<BTreeMap<String, String>>::from_query(request.query_string())
        .unwrap_or_else(|_| Query(Default::default()))
        .into_inner();

    let canonical_hash = hash_canonical_request_streaming(
        request,
        &query_params,
        &authentication.signed_headers,
        authentication.content_sha256,
    )?;

    let string_to_sign = build_string_to_sign(
        &authentication.formatted_date,
        &authentication.scope_string,
        &canonical_hash,
    );

    tracing::trace!(?authentication, %canonical_hash, %string_to_sign);

    let key = verify_v4(app, authentication, &string_to_sign, trace_id).await?;
    Ok(key)
}

/// Stream canonical request directly to hasher
fn hash_canonical_request_streaming(
    request: &HttpRequest,
    query_params: &BTreeMap<String, String>,
    signed_headers: &BTreeSet<&str>,
    payload_hash: &str,
) -> Result<String, SignatureError> {
    let mut hasher = CanonicalRequestHasher::new();

    // Stream HTTP method
    hasher.add_method(request.method().as_str());

    // Stream URI
    hasher.add_uri(request.path());

    // Build and stream canonical query string
    let query_str = build_canonical_query_string(query_params);
    hasher.add_query(&query_str);

    // Stream canonical headers
    let headers = request.headers();
    let mut header_pairs: Vec<(&str, String)> = Vec::with_capacity(signed_headers.len());

    for header_name in signed_headers {
        let value_str = if *header_name == "host" && !headers.contains_key(HOST) {
            // For HTTP/2, get host from connection info (:authority pseudo-header)
            let connection_info = request.connection_info();
            let host_value = connection_info.host();
            tracing::debug!("Using host from connection info for HTTP/2: {}", host_value);
            host_value.to_string()
        } else if let Some(header_value) = headers.get(*header_name) {
            if let Ok(s) = header_value.to_str() {
                s.trim().to_string()
            } else {
                // For non-ASCII headers, convert bytes to UTF-8 lossy
                String::from_utf8_lossy(header_value.as_bytes())
                    .trim()
                    .to_string()
            }
        } else {
            return Err(SignatureError::Other(format!(
                "signed header `{}` is not present",
                header_name
            )));
        };
        header_pairs.push((header_name, value_str));
    }

    // Headers are already sorted because signed_headers is a BTreeSet
    hasher.add_headers(header_pairs.iter().map(|(k, v)| (*k, v.as_str())));

    // Stream signed headers list
    hasher.add_signed_headers(signed_headers.iter().copied());

    // Stream payload hash
    hasher.add_payload_hash(payload_hash);

    // Return hex hash (no canonical request string ever created!)
    Ok(hasher.finalize())
}

fn build_canonical_query_string(query_params: &BTreeMap<String, String>) -> String {
    if query_params.is_empty() {
        return String::new();
    }
    let mut items = Vec::with_capacity(query_params.len());
    for (key, value) in query_params.iter() {
        let mut item = String::with_capacity(key.len() + value.len() + 10);
        item.push_str(&aws_signature::sigv4::uri_encode(key, true));
        item.push('=');
        item.push_str(&aws_signature::sigv4::uri_encode(value, true));
        items.push(item);
    }
    items.sort();
    items.join("&")
}

fn build_string_to_sign(
    formatted_datetime: &str,
    credential_scope: &str,
    canonical_hash: &str,
) -> String {
    const AWS4_HMAC_SHA256: &str = "AWS4-HMAC-SHA256";
    let mut result = String::with_capacity(
        AWS4_HMAC_SHA256.len() + 1 + formatted_datetime.len() + 1 + credential_scope.len() + 1 + 64,
    );
    result.push_str(AWS4_HMAC_SHA256);
    result.push('\n');
    result.push_str(formatted_datetime);
    result.push('\n');
    result.push_str(credential_scope);
    result.push('\n');
    result.push_str(canonical_hash);
    result
}

async fn verify_v4(
    app: Arc<AppState>,
    auth: &Authentication<'_>,
    string_to_sign: &str,
    trace_id: &TraceId,
) -> Result<Versioned<ApiKey>, SignatureError> {
    let key = app.get_api_key(auth.key_id.to_string(), trace_id).await?;

    let signing_key = get_signing_key(auth.date, &key.data.secret_key, &app.config.region)
        .map_err(|e| SignatureError::Other(format!("Unable to build signing key: {}", e)))?;

    if !verify_signature(&signing_key, string_to_sign, auth.signature)? {
        return Err(SignatureError::Other("signature mismatch".into()));
    }

    Ok(key)
}
