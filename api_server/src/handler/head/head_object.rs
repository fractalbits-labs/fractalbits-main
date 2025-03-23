use axum::{
    extract::Query,
    http::{HeaderMap, HeaderValue},
    response::{IntoResponse, Response},
    RequestPartsExt,
};
use bucket_tables::bucket_table::Bucket;
use rpc_client_nss::RpcClientNss;
use serde::Deserialize;

use crate::handler::{
    common::{get_raw_object, s3_error::S3Error, time},
    Request,
};
use crate::object_layout::{MpuState, ObjectState};

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct HeadObjectOptions {
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
    let Query(_opts): Query<HeadObjectOptions> = request.into_parts().0.extract().await?;
    let obj = get_raw_object(rpc_client_nss, bucket.root_blob_name.clone(), key).await?;

    let mut headers = HeaderMap::new();
    let last_modified = time::format_http_date(obj.timestamp);
    headers.insert(
        "Last-Modified",
        HeaderValue::from_str(&last_modified).unwrap(),
    );
    match obj.state {
        ObjectState::Normal(obj_data) => {
            headers.insert("ETag", HeaderValue::from_str(&obj_data.etag).unwrap());
            headers.insert(
                "Content-Length",
                HeaderValue::from_str(&obj_data.size.to_string()).unwrap(),
            );
        }
        ObjectState::Mpu(MpuState::Completed { size, etag }) => {
            headers.insert("ETag", HeaderValue::from_str(&etag).unwrap());
            headers.insert(
                "Content-Length",
                HeaderValue::from_str(&size.to_string()).unwrap(),
            );
        }
        _ => {
            return Err(S3Error::InvalidObjectState);
        }
    }
    Ok(headers.into_response())
}
