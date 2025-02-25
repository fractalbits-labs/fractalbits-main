use crate::handler::common::time::SHORT_DATE;

use super::extract::authorization::Authorization;
use axum::extract::Request;
use bucket_tables::api_key_table::ApiKey;
use chrono::{DateTime, Utc};
use hmac::{Hmac, Mac};
use rpc_client_rss::ArcRpcClientRss;
use sha2::Sha256;

pub mod payload;

type HmacSha256 = Hmac<Sha256>;

pub async fn verify_request(
    request: Request,
    auth: &Authorization,
    rpc_client_rss: ArcRpcClientRss,
    region: &str,
) -> (Request, Option<ApiKey>) {
    payload::check_standard_signature(auth, request, rpc_client_rss, region).await
}

pub fn signing_hmac(
    datetime: &DateTime<Utc>,
    secret_key: &str,
    region: &str,
) -> Result<HmacSha256, hmac::digest::InvalidLength> {
    let service = "s3";
    let secret = String::from("AWS4") + secret_key;
    let mut date_hmac = HmacSha256::new_from_slice(secret.as_bytes())?;
    date_hmac.update(datetime.format(SHORT_DATE).to_string().as_bytes());
    let mut region_hmac = HmacSha256::new_from_slice(&date_hmac.finalize().into_bytes())?;
    region_hmac.update(region.as_bytes());
    let mut service_hmac = HmacSha256::new_from_slice(&region_hmac.finalize().into_bytes())?;
    service_hmac.update(service.as_bytes());
    let mut signing_hmac = HmacSha256::new_from_slice(&service_hmac.finalize().into_bytes())?;
    signing_hmac.update(b"aws4_request");
    let hmac = HmacSha256::new_from_slice(&signing_hmac.finalize().into_bytes())?;
    Ok(hmac)
}
