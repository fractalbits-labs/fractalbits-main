use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use actix_web::{
    http::header::{HeaderMap, HeaderValue, AUTHORIZATION, HOST},
    web::Query,
    HttpRequest,
};
use data_types::{hash::Hash, ApiKey, Versioned};

use crate::{
    handler::common::{
        request::extract::Authentication,
        signature::{ContentSha256Header, SignatureError},
        xheader,
    },
    AppState,
};
use aws_signature::{
    create_canonical_request, get_signing_key, string_to_sign, verify_signature,
    AWS4_HMAC_SHA256_PAYLOAD, UNSIGNED_PAYLOAD,
};

pub struct CheckedSignature {
    pub key: Option<Versioned<ApiKey>>,
    pub content_sha256_header: ContentSha256Header,
    pub signature_header: Option<String>,
}

pub async fn check_payload_signature(
    app: Arc<AppState>,
    auth: &Authentication,
    request: &HttpRequest,
) -> Result<CheckedSignature, SignatureError> {
    let mut query = Query::<BTreeMap<String, String>>::from_query(request.query_string())
        .unwrap_or_else(|_| Query(Default::default()))
        .into_inner();

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
            .map_err(|e| SignatureError::Other(format!("Invalid header: {e}")))?;

        Ok(CheckedSignature {
            key: None,
            content_sha256_header: parse_x_amz_content_sha256(content_sha256)?,
            signature_header: None,
        })
    }
}

fn parse_x_amz_content_sha256(header: Option<&str>) -> Result<ContentSha256Header, SignatureError> {
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
                return Err(SignatureError::Other(
                    "invalid or unsupported x-amz-content-sha256".into(),
                ))
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

async fn check_standard_signature(
    app: Arc<AppState>,
    authentication: &Authentication,
    request: &HttpRequest,
) -> Result<CheckedSignature, SignatureError> {
    let query_params = Query::<BTreeMap<String, String>>::from_query(request.query_string())
        .unwrap_or_else(|_| Query(Default::default()))
        .into_inner();

    // Create a headers map that includes the host header for HTTP/2 compatibility
    let headers_for_signature = ensure_host_header_present(request)?;

    let canonical_request = canonical_request(
        request.method(),
        request.path(),
        &query_params,
        &headers_for_signature,
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

    let key = verify_v4(app, authentication, &string_to_sign).await?;

    let content_sha256_header = parse_x_amz_content_sha256(Some(&authentication.content_sha256))?;

    Ok(CheckedSignature {
        key,
        content_sha256_header,
        signature_header: Some(authentication.signature.clone()),
    })
}

/// Ensures the host header is present in the headers map, which is required for SigV4 signature verification.
/// For HTTP/2 requests, the :authority pseudo-header replaces the host header, but actix-web doesn't
/// automatically add it to the headers map. This function creates a new headers map with the host header
/// added from the request's connection info when it's missing.
fn ensure_host_header_present(request: &HttpRequest) -> Result<HeaderMap, SignatureError> {
    let mut headers = request.headers().clone();

    // Check if host header is missing (typical for HTTP/2)
    if !headers.contains_key(HOST) {
        // Get the host from connection info (:authority pseudo-header for HTTP/2)
        let connection_info = request.connection_info();
        let host_value = connection_info.host();
        let header_value = HeaderValue::from_str(host_value)
            .map_err(|_| SignatureError::Other("Invalid host value from connection info".into()))?;
        headers.insert(HOST, header_value);
        tracing::debug!("Added missing host header for HTTP/2: {}", host_value);
    }

    Ok(headers)
}

async fn check_presigned_signature(
    app: Arc<AppState>,
    authentication: &Authentication,
    request: &HttpRequest,
    query: &mut BTreeMap<String, String>,
) -> Result<CheckedSignature, SignatureError> {
    let signed_headers = &authentication.signed_headers;

    // Create a headers map that includes the host header
    let headers_for_signature = ensure_host_header_present(request)?;

    // Verify signed headers
    verify_signed_headers(&headers_for_signature, signed_headers)?;

    // Remove X-Amz-Signature from query for canonical request calculation
    query.remove(xheader::X_AMZ_SIGNATURE.as_str());

    let canonical_request = canonical_request(
        request.method(),
        request.path(),
        query,
        &headers_for_signature,
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

    let key = verify_v4(app, authentication, &string_to_sign).await?;

    Ok(CheckedSignature {
        key,
        content_sha256_header: ContentSha256Header::UnsignedPayload,
        signature_header: Some(authentication.signature.clone()),
    })
}

fn verify_signed_headers(
    headers: &HeaderMap,
    signed_headers: &BTreeSet<String>,
) -> Result<(), SignatureError> {
    if !signed_headers.contains(HOST.as_str()) {
        return Err(SignatureError::Other(
            "Header `Host` should be signed".into(),
        ));
    }
    for (name, _) in headers.iter() {
        if name.as_str().starts_with("x-amz-") && !signed_headers.contains(name.as_str()) {
            return Err(SignatureError::Other(format!(
                "Header `{name}` should be signed"
            )));
        }
    }
    Ok(())
}

async fn verify_v4(
    app: Arc<AppState>,
    auth: &Authentication,
    string_to_sign: &str,
) -> Result<Option<Versioned<ApiKey>>, SignatureError> {
    let key = app.get_api_key(auth.key_id.clone()).await?;

    let signing_key = get_signing_key(auth.date, &key.data.secret_key, &app.config.region)
        .map_err(|e| SignatureError::Other(format!("Unable to build signing key: {}", e)))?;

    if !verify_signature(&signing_key, string_to_sign, &auth.signature)? {
        return Err(SignatureError::Other("signature mismatch".into()));
    }

    Ok(Some(key))
}

/// Create canonical request using actix-web types
fn canonical_request(
    method: &actix_web::http::Method,
    canonical_uri: &str,
    query_params: &BTreeMap<String, String>,
    headers: &HeaderMap,
    signed_headers: &BTreeSet<String>,
    payload_hash: &str,
) -> Result<String, SignatureError> {
    // Check that all signed headers are present
    for header_name in signed_headers {
        if !headers.contains_key(header_name) {
            return Err(SignatureError::Other(format!(
                "signed header `{}` is not present",
                header_name
            )));
        }
    }

    // Build canonical headers directly from HeaderMap
    let mut canonical_headers = Vec::new();
    for header_name in signed_headers {
        if let Some(header_value) = headers.get(header_name) {
            let value_str = if let Ok(s) = header_value.to_str() {
                s.to_string()
            } else {
                // For non-ASCII headers, convert bytes to UTF-8 lossy
                String::from_utf8_lossy(header_value.as_bytes()).to_string()
            };
            canonical_headers.push(format!("{}:{}", header_name, value_str.trim()));
        }
    }
    Ok(create_canonical_request(
        method.as_str(),
        canonical_uri,
        query_params,
        &canonical_headers,
        signed_headers,
        payload_hash,
    ))
}
