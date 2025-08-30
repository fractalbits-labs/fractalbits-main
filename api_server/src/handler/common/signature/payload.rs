use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write;
use std::sync::Arc;

use actix_web::{
    http::header::{HeaderMap, HeaderValue, AUTHORIZATION, HOST},
    web::Query,
    HttpRequest,
};
use arrayvec::ArrayString;
use chrono::{DateTime, Utc};
use data_types::{ApiKey, Versioned};
use hmac::{Hmac, Mac};
use itertools::Itertools;
use sha2::{Digest, Sha256};

use crate::{
    handler::common::{
        data::Hash,
        encoding::uri_encode,
        request::extract::Authentication,
        signature::{ContentSha256Header, SignatureError},
        time::SHORT_DATE,
        xheader,
    },
    AppState,
};

type HmacSha256 = Hmac<Sha256>;
// Possible values for x-amz-content-sha256, in addition to the actual sha256
const UNSIGNED_PAYLOAD: &str = "UNSIGNED-PAYLOAD";
const AWS4_HMAC_SHA256_PAYLOAD: &str = "AWS4-HMAC-SHA256-PAYLOAD";

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

    let key = verify_v4(app, authentication, string_to_sign.as_bytes()).await?;

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

fn string_to_sign(datetime: &DateTime<Utc>, scope_string: &str, canonical_req: &str) -> String {
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

fn canonical_request(
    method: &actix_web::http::Method,
    canonical_uri: &str,
    query_params: &BTreeMap<String, String>,
    headers: &actix_web::http::header::HeaderMap,
    signed_headers: &BTreeSet<String>,
    content_sha256: &str,
) -> Result<String, SignatureError> {
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
            let value = headers.get(name).ok_or_else(|| {
                SignatureError::Other(format!("signed header `{name}` is not present"))
            })?;
            // Handle potentially non-ASCII header values (e.g., x-amz-meta-* headers with unicode)
            let value_str = if let Ok(s) = value.to_str() {
                s.to_string()
            } else {
                // For non-ASCII headers, convert bytes to UTF-8 lossy
                String::from_utf8_lossy(value.as_bytes()).to_string()
            };
            Ok(format!("{}:{}", name.as_str(), value_str.trim()))
        })
        .collect::<Result<Vec<String>, SignatureError>>()?
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

async fn verify_v4(
    app: Arc<AppState>,
    auth: &Authentication,
    payload: &[u8],
) -> Result<Option<Versioned<ApiKey>>, SignatureError> {
    let key = app.get_api_key(auth.key_id.clone()).await?;

    let mut hmac = signing_hmac(&auth.date, &key.data.secret_key, &app.config.region)
        .map_err(|_| SignatureError::Other("Unable to build signing HMAC".into()))?;
    hmac.update(payload);
    let signature = hex::decode(&auth.signature)?;
    if hmac.verify_slice(&signature).is_err() {
        return Err(SignatureError::Other("signature mismatch".into()));
    }

    Ok(Some(key))
}

fn signing_hmac(
    datetime: &DateTime<Utc>,
    secret_key: &str,
    region: &str,
) -> Result<HmacSha256, hmac::digest::InvalidLength> {
    let service = "s3";

    let mut initial_key = Vec::with_capacity(4 + secret_key.len());
    initial_key.extend_from_slice(b"AWS4");
    initial_key.extend_from_slice(secret_key.as_bytes());

    let mut date_str = ArrayString::<8>::new();
    write!(&mut date_str, "{}", datetime.format(SHORT_DATE))
        .expect("Formatting a date into an 8-byte ArrayString should not fail");

    let mut mac = HmacSha256::new_from_slice(&initial_key)?;
    mac.update(date_str.as_bytes());
    let key = mac.finalize().into_bytes();

    let mut mac = HmacSha256::new_from_slice(&key)?;
    mac.update(region.as_bytes());
    let key = mac.finalize().into_bytes();

    let mut mac = HmacSha256::new_from_slice(&key)?;
    mac.update(service.as_bytes());
    let key = mac.finalize().into_bytes();

    let mut mac = HmacSha256::new_from_slice(&key)?;
    mac.update(b"aws4_request");
    let signing_key = mac.finalize().into_bytes();

    HmacSha256::new_from_slice(&signing_key)
}
