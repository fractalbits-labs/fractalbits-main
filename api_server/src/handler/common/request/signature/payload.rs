use axum::http::{HeaderMap, HeaderValue, Method};
use axum::{
    extract::{Query, Request},
    RequestPartsExt,
};
use bucket_tables::table::Table;
use chrono::{DateTime, Utc};
use hmac::Mac;
use itertools::Itertools;
use rpc_client_rss::ArcRpcClientRss;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

use bucket_tables::api_key_table::{ApiKey, ApiKeyTable};

use crate::handler::common::encoding::uri_encode;
use crate::handler::common::request::extract::authorization::Authorization;
use crate::handler::common::time::LONG_DATETIME;

use super::signing_hmac;

const AWS4_HMAC_SHA256: &str = "AWS4-HMAC-SHA256";

pub async fn check_standard_signature(
    auth: &Authorization,
    request: Request,
    rpc_client_rss: ArcRpcClientRss,
    region: &str,
) -> (Request, Option<ApiKey>) {
    let (mut head, body) = request.into_parts();
    let query_params: Query<BTreeMap<String, String>> = head.extract().await.unwrap();
    let request = Request::from_parts(head, body);
    let canonical_request = canonical_request(
        request.method(),
        request.uri().path(),
        &query_params,
        request.headers(),
        &auth.signed_headers,
        &auth.content_sha256,
    );
    let string_to_sign =
        string_to_sign(&auth.date, &auth.scope.to_sign_string(), &canonical_request);

    tracing::trace!("canonical request:\n{}", canonical_request);
    tracing::trace!("string to sign:\n{}", string_to_sign);

    let key = verify_v4(&auth, string_to_sign.as_bytes(), rpc_client_rss, region)
        .await
        .unwrap();

    (request, Some(key))
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
) -> String {
    // Canonical query string from passed HeaderMap
    let canonical_query_string = {
        let mut items = Vec::with_capacity(query_params.len());
        for (key, value) in query_params.iter() {
            items.push(uri_encode(&key, true) + "=" + &uri_encode(&value, true));
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
                .expect(&format!("signed header `{}` is not present", name));
            let value = std::str::from_utf8(value.as_bytes()).unwrap();
            format!("{}:{}", name.as_str(), value.trim())
        })
        .collect::<Vec<String>>()
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
    list.join("\n")
}

pub async fn verify_v4(
    auth: &Authorization,
    payload: &[u8],
    rpc_client_rss: ArcRpcClientRss,
    region: &str,
) -> Option<ApiKey> {
    let mut api_key_table: Table<ArcRpcClientRss, ApiKeyTable> = Table::new(rpc_client_rss);
    let key = api_key_table.get(auth.key_id.clone()).await;

    let mut hmac =
        signing_hmac(&auth.date, &key.secret_key, region).expect("Unable to build signing HMAC");
    hmac.update(payload);
    let signature = hex::decode(&auth.signature).unwrap();
    if hmac.verify_slice(&signature).is_err() {
        panic!("Invalid signature");
    }

    Some(key)
}
