use axum::{
    extract::Query,
    http::{header, HeaderValue},
    response::{IntoResponse, Response},
    RequestPartsExt,
};
use bucket_tables::bucket_table::Bucket;
use rpc_client_nss::RpcClientNss;
use serde::Deserialize;

use crate::handler::{
    common::{get_raw_object, object_headers, s3_error::S3Error},
    get::GetObjectHeaderOpts,
    Request,
};

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct QueryOpts {
    #[serde(rename(deserialize = "partNumber"))]
    part_number: Option<u64>,
    #[serde(rename(deserialize = "versionId"))]
    version_id: Option<String>,
    response_cache_control: Option<String>,
    response_content_disposition: Option<String>,
    response_content_encoding: Option<String>,
    response_content_language: Option<String>,
    response_content_type: Option<String>,
    response_expires: Option<String>,
}

pub async fn head_object_handler(
    request: Request,
    bucket: &Bucket,
    key: String,
    rpc_client_nss: &RpcClientNss,
) -> Result<Response, S3Error> {
    let mut parts = request.into_parts().0;
    let Query(_query_opts): Query<QueryOpts> = parts.extract().await?;
    let header_opts = GetObjectHeaderOpts::from_headers(&parts.headers)?;
    let checksum_mode_enabled = header_opts.x_amz_checksum_mode_enabled;
    let obj = get_raw_object(rpc_client_nss, bucket.root_blob_name.clone(), key).await?;

    let mut resp = ().into_response();
    resp.headers_mut().insert(
        header::CONTENT_LENGTH,
        HeaderValue::from_str(&obj.size()?.to_string())?,
    );
    object_headers(&mut resp, &obj, checksum_mode_enabled)?;
    Ok(resp)
}
