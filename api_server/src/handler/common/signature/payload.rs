use std::collections::{BTreeMap, BTreeSet};

use axum::body::Body;
use axum::extract::Query;
use axum::http::header::{HeaderMap, HeaderValue, AUTHORIZATION, HOST};
use axum::http::{request::Request, Method};
use axum::RequestExt;
use bucket_tables::{api_key_table::{ApiKey, ApiKeyTable}, table::Table, Versioned};
use chrono::{DateTime, Utc};
use hmac::Mac;
use itertools::Itertools;
use rpc_client_rss::RpcClientRss;
use sha2::{Digest, Sha256};

use crate::handler::common::{request::extract::Authentication, xheader};

use super::super::data::Hash;

use super::*;

use super::super::encoding::uri_encode;

pub struct CheckedSignature {
    pub key: Option<Versioned<ApiKey>>,
    pub content_sha256_header: ContentSha256Header,
    pub signature_header: Option<String>,
}

pub async fn check_payload_signature(
    app: &AppState,
    auth: &Authentication,
    request: &mut Request<Body>,
) -> Result<CheckedSignature, Error> {
    let Query(mut query): Query<BTreeMap<String, String>> = request.extract_parts().await?;

    if query.contains_key(xheader::X_AMZ_ALGORITHM.as_str()) {
        // We check for presigned-URL-style authentication first, because
        // the browser or something else could inject an Authorization header
        // that is totally unrelated to AWS signatures.
        check_presigned_signature(app, auth, request, &mut query).await
    } else if request.headers().contains_key(AUTHORIZATION) {
        check_standard_signature(app, auth, request).await
    } else {
        // Unsigned (anonymous) request
        let content_sha256 = request
            .headers()
            .get(xheader::X_AMZ_CONTENT_SHA256)
            .map(|x| x.to_str())
            .transpose()?;
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
    app: &AppState,
    authentication: &Authentication,
    request: &mut Request<Body>,
) -> Result<CheckedSignature, Error> {
    let query_params: Query<BTreeMap<String, String>> = request.extract_parts().await?;
    let canonical_request = canonical_request(
        request.method(),
        request.uri().path(),
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
    app: &AppState,
    authentication: &Authentication,
    request: &mut Request<Body>,
    query: &mut BTreeMap<String, String>,
) -> Result<CheckedSignature, Error> {
    let signed_headers = &authentication.signed_headers;
    // Verify that all necessary request headers are included in signed_headers
    // For AWSv4 pre-signed URLs, the following must be included:
    // - the Host header (mandatory)
    // - all x-amz-* headers used in the request
    verify_signed_headers(request.headers(), signed_headers)?;

    // The X-Amz-Signature value is passed as a query parameter,
    // but the signature cannot be computed from a string that contains itself.
    // AWS specifies that all query params except X-Amz-Signature are included
    // in the canonical request.
    query.remove(xheader::X_AMZ_SIGNATURE.as_str());
    let canonical_request = canonical_request(
        request.method(),
        request.uri().path(),
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

    // In the page on presigned URLs, AWS specifies that if a signed query
    // parameter and a signed header of the same name have different values,
    // then an InvalidRequest error is raised.
    let headers_mut = request.headers_mut();
    for (name, value) in query.iter() {
        if let Some(existing) = headers_mut.get(name) {
            if signed_headers.contains(name) && existing.as_bytes() != value.as_bytes() {
                return Err(Error::Other(format!(
                    "Conflicting values for `{}` in query parameters and request headers",
                    name
                )));
            }
        }
        if name.starts_with("x-amz-") {
            // Query parameters that start by x-amz- are actually intended to stand in for
            // headers that can't be added at the time the request is made.
            // What we do is just add them to the Request object as regular headers,
            // that will be handled downstream as if they were included like in a normal request.
            // (Here we allow such query parameters to override headers with the same name
            // that are not signed, however there is not much reason that this would happen)
            headers_mut.insert(
                HeaderName::from_lowercase(name.as_bytes()).unwrap(),
                HeaderValue::from_bytes(value.as_bytes()).unwrap(),
            );
        }
    }

    // Presigned URLs always use UNSIGNED-PAYLOAD,
    // so there is no sha256 hash to return.
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
            return Err(Error::Other(format!("Header `{}` should be signed", name)));
        }
    }
    Ok(())
}

pub fn string_to_sign(datetime: &DateTime<Utc>, scope_string: &str, canonical_req: &str) -> String {
    let mut hasher = Sha256::default();
    hasher.update(canonical_req.as_bytes());
    [
        AWS4_HMAC_SHA256,
        &datetime.format(LONG_DATETIME).to_string(),
        scope_string,
        &hex::encode(hasher.finalize().as_slice()),
    ]
    .join("\n")
}

pub fn canonical_request(
    method: &Method,
    canonical_uri: &str,
    query_params: &BTreeMap<String, String>,
    headers: &HeaderMap<HeaderValue>,
    signed_headers: &BTreeSet<String>,
    content_sha256: &str,
) -> Result<String, Error> {
    // Canonical query string from passed HeaderMap
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
            let value = headers.get(name).ok_or(Error::Other(format!(
                "signed header `{}` is not present",
                name
            )))?;
            let value = std::str::from_utf8(value.as_bytes())?;
            Ok(format!("{}:{}", name.as_str(), value.trim()))
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
    app: &AppState,
    auth: &Authentication,
    payload: &[u8],
) -> Result<Option<Versioned<ApiKey>>, Error> {
    let rpc_client_rss = app.get_rpc_client_rss().await;
    let api_key_table: Table<RpcClientRss, ApiKeyTable> =
        Table::new(&rpc_client_rss, Some(app.cache.clone()));
    let key = api_key_table.get(auth.key_id.clone(), true).await?;
    drop(rpc_client_rss);

    let mut hmac = signing_hmac(&auth.date, &key.data.secret_key, &app.config.region)
        .map_err(|_| Error::Other("Unable to build signing HMAC".into()))?;
    hmac.update(payload);
    let signature = hex::decode(&auth.signature)?;
    if hmac.verify_slice(&signature).is_err() {
        return Err(Error::Other("signature mismatch".into()));
    }

    Ok(Some(key))
}
