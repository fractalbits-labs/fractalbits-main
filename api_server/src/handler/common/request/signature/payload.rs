use axum::http::{HeaderMap, HeaderValue, Method};
use chrono::{DateTime, Utc};
use itertools::Itertools;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

use crate::handler::common::encoding::uri_encode;
use crate::handler::common::time::LONG_DATETIME;

use super::SignatureError;

const AWS4_HMAC_SHA256: &str = "AWS4-HMAC-SHA256";

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
) -> Result<String, SignatureError> {
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
            let value = headers.get(name).ok_or(SignatureError::Invalid(format!(
                "signed header `{name}` is not present"
            )))?;
            let value = std::str::from_utf8(value.as_bytes())?;
            Ok(format!("{}:{}", name.as_str(), value.trim()))
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
