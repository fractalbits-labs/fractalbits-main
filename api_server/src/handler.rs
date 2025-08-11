mod bucket;
pub mod common;
mod delete;
mod endpoint;
mod get;
mod head;
mod post;
mod put;

use metrics::{counter, gauge, histogram, Gauge};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::AppState;
use axum::{
    body::Body,
    extract::{ConnectInfo, FromRequestParts, State},
    http,
    response::{IntoResponse, Response},
};
use bucket::BucketEndpoint;
use bucket_tables::api_key_table::ApiKey;
use bucket_tables::bucket_table::Bucket;
use bucket_tables::Versioned;
use common::{
    authorization::Authorization,
    request::extract::*,
    s3_error::S3Error,
    signature::{self, body::ReqBody, verify_request, VerifiedRequest},
};
use delete::DeleteEndpoint;
use endpoint::Endpoint;
use get::GetEndpoint;
use head::HeadEndpoint;
use post::PostEndpoint;
use put::PutEndpoint;
use rpc_client_rss::RpcErrorRss;

pub type Request<T = ReqBody> = http::Request<T>;

macro_rules! extract_or_return {
    ($parts:expr, $app:expr, $extractor:ty) => {
        match <$extractor>::from_request_parts($parts, $app).await {
            Ok(value) => value,
            Err(rejection) => {
                tracing::warn!(
                    "failed to extract parts at {}:{} {:?} {:?}",
                    file!(),
                    line!(),
                    rejection,
                    $parts
                );
                return rejection.into_response();
            }
        }
    };
}

pub async fn any_handler(
    State(app): State<Arc<AppState>>,
    ConnectInfo(client_addr): ConnectInfo<SocketAddr>,
    request: http::Request<Body>,
) -> Response {
    let start = Instant::now();
    let (mut parts, body) = request.into_parts();
    let ApiCommandFromQuery(api_cmd) = extract_or_return!(&mut parts, &app, ApiCommandFromQuery);
    let AuthFromHeaders(auth) = extract_or_return!(&mut parts, &app, AuthFromHeaders);
    let BucketAndKeyName { bucket, key } = extract_or_return!(&mut parts, &app, BucketAndKeyName);
    let api_sig = extract_or_return!(&mut parts, &app, ApiSignature);
    let request = http::Request::from_parts(parts, body);

    tracing::debug!(%bucket, %key, %client_addr);

    let resource = format!("/{bucket}{key}");
    let endpoint =
        match Endpoint::from_extractors(&request, &bucket, &key, api_cmd, api_sig.clone()) {
            Err(e) => {
                let api_cmd = api_cmd.map_or("".into(), |cmd| cmd.to_string());
                tracing::warn!(
                    %api_cmd,
                    %api_sig,
                    %bucket,
                    %key,
                    %client_addr,
                    error=?e,
                    "failed to create endpoint"
                );
                return e.into_response_with_resource(&resource);
            }
            Ok(endpoint) => endpoint,
        };

    let endpoint_name = endpoint.as_str();
    let gauge_guard = InflightRequestGuard::new(endpoint_name);
    let result = tokio::time::timeout(
        Duration::from_secs(app.config.http_request_timeout_seconds),
        any_handler_inner(app, bucket.clone(), key.clone(), auth, request, endpoint),
    )
    .await;
    let duration = start.elapsed();
    drop(gauge_guard);

    let result = match result {
        Ok(result) => result,
        Err(_) => {
            tracing::error!(
                endpoint = %endpoint_name,
                %bucket,
                %key,
                %client_addr,
                "request timed out"
            );
            counter!("request_timeout", "endpoint" => endpoint_name).increment(1);
            return S3Error::InternalError.into_response_with_resource(&resource);
        }
    };

    match result {
        Ok(response) => {
            histogram!("request_duration_nanos", "status" => format!("{endpoint_name}_Ok"))
                .record(duration.as_nanos() as f64);
            response
        }
        Err(e) => {
            histogram!("request_duration_nanos", "status" => format!("{endpoint_name}_Err"))
                .record(duration.as_nanos() as f64);
            tracing::error!(
                endpoint=%endpoint_name,
                %bucket,
                %key,
                %client_addr,
                error=?e,
                "failed to handle request"
            );
            e.into_response_with_resource(&resource)
        }
    }
}

async fn any_handler_inner(
    app: Arc<AppState>,
    bucket_name: String,
    key: String,
    auth: Option<Authentication>,
    request: http::Request<Body>,
    endpoint: Endpoint,
) -> Result<Response, S3Error> {
    let (parts, body) = request.into_parts();
    let request = http::Request::from_parts(parts, body);
    let start = Instant::now();

    let VerifiedRequest {
        request, api_key, ..
    } = if app.config.allow_missing_or_bad_signature {
        if auth.is_none() {
            tracing::warn!("allowing anonymous access, falling back to 'test_api_key'");
            let access_key = "test_api_key";
            let api_key = common::signature::payload::get_api_key(app.clone(), access_key)
                .await
                .map_err(|_| S3Error::InvalidAccessKeyId)?;
            VerifiedRequest {
                request: request.map(ReqBody::from),
                api_key,
                content_sha256_header: signature::ContentSha256Header::UnsignedPayload,
            }
        } else {
            let auth_unwrapped = auth.unwrap();
            match verify_request(app.clone(), request, &auth_unwrapped).await {
                Ok(res) => res,
                Err(signature::error::Error::SignatureError(e, request_wrapper)) => {
                    let request = request_wrapper.into_inner();
                    match *e {
                        signature::error::Error::RpcErrorRss(RpcErrorRss::NotFound) => {
                            return Err(S3Error::InvalidAccessKeyId);
                        }
                        _ => {
                            tracing::warn!(
                                "allowed bad signature for {:?}, falling back to 'test_api_key'",
                                auth_unwrapped
                            );
                            let access_key = "test_api_key";
                            let api_key =
                                common::signature::payload::get_api_key(app.clone(), access_key)
                                    .await
                                    .map_err(|_| S3Error::InvalidAccessKeyId)?;
                            VerifiedRequest {
                                request: request.map(ReqBody::from),
                                api_key,
                                content_sha256_header:
                                    signature::ContentSha256Header::UnsignedPayload,
                            }
                        }
                    }
                }
                Err(e) => {
                    tracing::error!("unexpected error during signature verification: {:?}", e);
                    return Err(S3Error::InternalError);
                }
            }
        }
    } else {
        let auth_unwrapped = auth.ok_or(S3Error::InvalidSignature)?;
        match verify_request(app.clone(), request, &auth_unwrapped).await {
            Ok(res) => res,
            Err(signature::error::Error::SignatureError(e, _)) => match *e {
                signature::error::Error::RpcErrorRss(RpcErrorRss::NotFound) => {
                    return Err(S3Error::InvalidAccessKeyId);
                }
                _ => {
                    return Err(S3Error::InvalidSignature);
                }
            },
            Err(e) => {
                tracing::error!("unexpected error during signature verification: {:?}", e);
                return Err(S3Error::InternalError);
            }
        }
    };
    histogram!("verify_request_duration_nanos", "endpoint" => endpoint.as_str())
        .record(start.elapsed().as_nanos() as f64);

    let allowed = match endpoint.authorization_type() {
        Authorization::Read => api_key.data.allow_read(&bucket_name),
        Authorization::Write => api_key.data.allow_write(&bucket_name),
        Authorization::Owner => api_key.data.allow_owner(&bucket_name),
        Authorization::None => true,
    };
    if !allowed {
        return Err(S3Error::AccessDenied);
    }

    match endpoint {
        Endpoint::Bucket(bucket_endpoint) => {
            bucket_handler(app, request, api_key, bucket_name, bucket_endpoint).await
        }
        Endpoint::Head(head_endpoint) => {
            let bucket = bucket::resolve_bucket(app.clone(), bucket_name).await?;
            head_handler(app, request, &bucket, key, head_endpoint).await
        }
        Endpoint::Get(get_endpoint) => {
            let bucket = bucket::resolve_bucket(app.clone(), bucket_name).await?;
            get_handler(app, request, &bucket, key, get_endpoint).await
        }
        Endpoint::Put(put_endpoint) => {
            let bucket = bucket::resolve_bucket(app.clone(), bucket_name).await?;
            put_handler(app, request, api_key, &bucket, key, put_endpoint).await
        }
        Endpoint::Post(post_endpoint) => {
            let bucket = bucket::resolve_bucket(app.clone(), bucket_name).await?;
            post_handler(app, request, &bucket, key, post_endpoint).await
        }
        Endpoint::Delete(delete_endpoint) => {
            let bucket = bucket::resolve_bucket(app.clone(), bucket_name).await?;
            delete_handler(app, request, &bucket, key, delete_endpoint).await
        }
    }
}

async fn bucket_handler(
    app: Arc<AppState>,
    request: Request,
    api_key: Versioned<ApiKey>,
    bucket_name: String,
    endpoint: BucketEndpoint,
) -> Result<Response, S3Error> {
    match endpoint {
        BucketEndpoint::CreateBucket => {
            bucket::create_bucket_handler(app, api_key, bucket_name, request).await
        }
        BucketEndpoint::DeleteBucket => {
            let bucket = bucket::resolve_bucket(app.clone(), bucket_name).await?;
            bucket::delete_bucket_handler(app, api_key, &bucket, request).await
        }
        BucketEndpoint::HeadBucket => bucket::head_bucket_handler(app, api_key, bucket_name).await,
        BucketEndpoint::ListBuckets => bucket::list_buckets_handler(app, request).await,
    }
}

async fn head_handler(
    app: Arc<AppState>,
    request: Request,
    bucket: &Bucket,
    key: String,
    endpoint: HeadEndpoint,
) -> Result<Response, S3Error> {
    match endpoint {
        HeadEndpoint::HeadObject => head::head_object_handler(app, request, bucket, key).await,
    }
}

async fn get_handler(
    app: Arc<AppState>,
    request: Request,
    bucket: &Bucket,
    key: String,
    endpoint: GetEndpoint,
) -> Result<Response, S3Error> {
    match endpoint {
        GetEndpoint::GetObject => get::get_object_handler(app, request, bucket, key).await,
        GetEndpoint::GetObjectAttributes => {
            get::get_object_attributes_handler(app, request, bucket, key).await
        }
        GetEndpoint::ListMultipartUploads => {
            get::list_multipart_uploads_handler(app, request).await
        }
        GetEndpoint::ListObjects => get::list_objects_handler(app, request, bucket).await,
        GetEndpoint::ListObjectsV2 => get::list_objects_v2_handler(app, request, bucket).await,
        GetEndpoint::ListParts => get::list_parts_handler(app, request, bucket, key).await,
    }
}

async fn put_handler(
    app: Arc<AppState>,
    request: Request,
    api_key: Versioned<ApiKey>,
    bucket: &Bucket,
    key: String,
    endpoint: PutEndpoint,
) -> Result<Response, S3Error> {
    match endpoint {
        PutEndpoint::PutObject => put::put_object_handler(app, request, bucket, key).await,
        PutEndpoint::UploadPart(part_number, upload_id) => {
            put::upload_part_handler(app, request, bucket, key, part_number, upload_id).await
        }
        PutEndpoint::CopyObject => {
            put::copy_object_handler(app, request, api_key, bucket, key).await
        }
        PutEndpoint::RenameFolder => put::rename_folder_handler(app, request, bucket, key).await,
        PutEndpoint::RenameObject => put::rename_object_handler(app, request, bucket, key).await,
    }
}

async fn post_handler(
    app: Arc<AppState>,
    request: Request,
    bucket: &Bucket,
    key: String,
    endpoint: PostEndpoint,
) -> Result<Response, S3Error> {
    match endpoint {
        PostEndpoint::CompleteMultipartUpload(upload_id) => {
            post::complete_multipart_upload_handler(app, request, bucket, key, upload_id).await
        }
        PostEndpoint::CreateMultipartUpload => {
            post::create_multipart_upload_handler(app, request, bucket, key).await
        }
        PostEndpoint::DeleteObjects => post::delete_objects_handler(app, request, bucket).await,
    }
}

async fn delete_handler(
    app: Arc<AppState>,
    request: Request,
    bucket: &Bucket,
    key: String,
    endpoint: DeleteEndpoint,
) -> Result<Response, S3Error> {
    match endpoint {
        DeleteEndpoint::AbortMultipartUpload(upload_id) => {
            delete::abort_multipart_upload_handler(app, request, bucket, key, upload_id).await
        }
        DeleteEndpoint::DeleteObject => delete::delete_object_handler(app, bucket, key).await,
    }
}

struct InflightRequestGuard {
    gauge: Gauge,
}

impl InflightRequestGuard {
    fn new(endpoint_name: &'static str) -> Self {
        let gauge = gauge!("inflight_request", "endpoint" => endpoint_name);
        gauge.increment(1.0);
        Self { gauge }
    }
}

impl Drop for InflightRequestGuard {
    fn drop(&mut self) {
        self.gauge.decrement(1.0);
    }
}
