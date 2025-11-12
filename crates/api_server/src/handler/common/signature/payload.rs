use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use crate::{
    AppState,
    handler::common::{request::extract::Authentication, signature::SignatureError},
};
use actix_web::{HttpRequest, http::header::HOST, web::Query};
use aws_signature::{create_canonical_request, get_signing_key, string_to_sign, verify_signature};
use data_types::{ApiKey, TraceId, Versioned};

pub async fn check_signature_impl(
    app: Arc<AppState>,
    auth: &Authentication,
    request: &HttpRequest,
    trace_id: &TraceId,
) -> Result<Versioned<ApiKey>, SignatureError> {
    // Support HTTP Authorization header based signature only for now
    check_header_based_signature(app, auth, request, trace_id).await
}

async fn check_header_based_signature(
    app: Arc<AppState>,
    authentication: &Authentication,
    request: &HttpRequest,
    trace_id: &TraceId,
) -> Result<Versioned<ApiKey>, SignatureError> {
    let query_params = Query::<BTreeMap<String, String>>::from_query(request.query_string())
        .unwrap_or_else(|_| Query(Default::default()))
        .into_inner();

    let canonical_request = canonical_request(
        request,
        &query_params,
        &authentication.signed_headers,
        &authentication.content_sha256,
    )?;

    let string_to_sign = string_to_sign(
        &authentication.date,
        &authentication.scope_string(),
        &canonical_request,
    );

    tracing::trace!(?authentication, %canonical_request, %string_to_sign);

    let key = verify_v4(app, authentication, &string_to_sign, trace_id).await?;
    Ok(key)
}

// Create canonical request using actix-web types
fn canonical_request(
    request: &HttpRequest,
    query_params: &BTreeMap<String, String>,
    signed_headers: &BTreeSet<String>,
    payload_hash: &str,
) -> Result<String, SignatureError> {
    // Build canonical headers from HeaderMap
    let mut canonical_headers = Vec::new();
    let headers = request.headers();

    for header_name in signed_headers {
        let value_str = if header_name == "host" && !headers.contains_key(HOST) {
            // For HTTP/2, get host from connection info (:authority pseudo-header)
            let connection_info = request.connection_info();
            let host_value = connection_info.host();
            tracing::debug!("Using host from connection info for HTTP/2: {}", host_value);
            host_value.to_string()
        } else if let Some(header_value) = headers.get(header_name) {
            if let Ok(s) = header_value.to_str() {
                s.to_string()
            } else {
                // For non-ASCII headers, convert bytes to UTF-8 lossy
                String::from_utf8_lossy(header_value.as_bytes()).to_string()
            }
        } else {
            return Err(SignatureError::Other(format!(
                "signed header `{}` is not present",
                header_name
            )));
        };
        canonical_headers.push(format!("{}:{}", header_name, value_str.trim()));
    }
    Ok(create_canonical_request(
        request.method().as_str(),
        request.path(),
        query_params,
        &canonical_headers,
        signed_headers,
        payload_hash,
    ))
}

async fn verify_v4(
    app: Arc<AppState>,
    auth: &Authentication,
    string_to_sign: &str,
    trace_id: &TraceId,
) -> Result<Versioned<ApiKey>, SignatureError> {
    let key = app.get_api_key(auth.key_id.clone(), trace_id).await?;

    let signing_key = get_signing_key(auth.date, &key.data.secret_key, &app.config.region)
        .map_err(|e| SignatureError::Other(format!("Unable to build signing key: {}", e)))?;

    if !verify_signature(&signing_key, string_to_sign, &auth.signature)? {
        return Err(SignatureError::Other("signature mismatch".into()));
    }

    Ok(key)
}
