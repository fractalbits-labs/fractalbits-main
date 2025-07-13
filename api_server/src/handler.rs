mod bucket;
pub mod common;
mod delete;
mod endpoint;
mod get;
mod head;
mod post;
mod put;

use metrics::histogram;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

use crate::{AppState, BlobId};
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
use tokio::sync::mpsc::Sender;

pub type Request<T = ReqBody> = http::Request<T>;

macro_rules! extract_or_return {
    ($parts:expr, $app:expr, $extractor:ty) => {
        match <$extractor>::from_request_parts($parts, $app).await {
            Ok(value) => value,
            Err(rejection) => {
                tracing::warn!("failed to extract parts: {:?}", rejection);
                return rejection.into_response();
            }
        }
    };
}

pub async fn any_handler(
    State(app): State<Arc<AppState>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    request: http::Request<Body>,
) -> Response {
    let start = Instant::now();

    let (mut parts, body) = request.into_parts();
    let ApiCommandFromQuery(api_cmd) = extract_or_return!(&mut parts, &app, ApiCommandFromQuery);
    let auth = extract_or_return!(&mut parts, &app, Authentication);
    let BucketNameAndKey { bucket_name, key } =
        extract_or_return!(&mut parts, &app, BucketNameAndKey);
    let api_sig = extract_or_return!(&mut parts, &app, ApiSignature);
    let request = http::Request::from_parts(parts, body);

    tracing::debug!(%bucket_name, %key, %addr);

    let resource = format!("/{bucket_name}{key}");
    let endpoint =
        match Endpoint::from_extractors(&request, &bucket_name, &key, api_cmd, api_sig.clone()) {
            Err(e) => {
                tracing::error!(
                    %bucket_name,
                    %key,
                    ?api_cmd,
                    ?api_sig,
                    error = ?e,
                    "failed to create endpoint"
                );
                return e.into_response_with_resource(&resource);
            }
            Ok(endpoint) => endpoint,
        };
    let endpoint_name = endpoint.as_str();
    let result = any_handler_inner(
        app,
        bucket_name.clone(),
        key.clone(),
        auth,
        request,
        endpoint,
    )
    .await;

    let duration = start.elapsed();
    let endpoint = endpoint_name;
    match result {
        Ok(response) => {
            histogram!("request_duration_nanos", "status" => format!("{endpoint}_Ok"))
                .record(duration.as_nanos() as f64);
            response
        }
        Err(e) => {
            histogram!("request_duration_nanos", "status" => format!("{endpoint}_Err"))
                .record(duration.as_nanos() as f64);
            tracing::error!(
                %bucket_name,
                %key,
                %endpoint,
                error = ?e,
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
    auth: Authentication,
    request: http::Request<Body>,
    endpoint: Endpoint,
) -> Result<Response, S3Error> {
    let start = Instant::now();
    let VerifiedRequest {
        request, api_key, ..
    } = match verify_request(app.clone(), request, &auth).await {
        Ok(res) => res,
        Err(signature::error::Error::RpcErrorRss(RpcErrorRss::NotFound)) => {
            return Err(S3Error::InvalidAccessKeyId)
        }
        Err(_e) => return Err(S3Error::InvalidSignature),
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

    let blob_deletion = app.blob_deletion.clone();
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
            put_handler(
                app,
                request,
                api_key,
                &bucket,
                key,
                blob_deletion,
                put_endpoint,
            )
            .await
        }
        Endpoint::Post(post_endpoint) => {
            let bucket = bucket::resolve_bucket(app.clone(), bucket_name).await?;
            post_handler(app, request, &bucket, key, blob_deletion, post_endpoint).await
        }
        Endpoint::Delete(delete_endpoint) => {
            let bucket = bucket::resolve_bucket(app.clone(), bucket_name).await?;
            delete_handler(app, request, &bucket, key, blob_deletion, delete_endpoint).await
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
    blob_deletion: Sender<(BlobId, usize)>,
    endpoint: PutEndpoint,
) -> Result<Response, S3Error> {
    match endpoint {
        PutEndpoint::PutObject => {
            put::put_object_handler(app, request, bucket, key, blob_deletion).await
        }
        PutEndpoint::UploadPart(part_number, upload_id) => {
            put::upload_part_handler(
                app,
                request,
                bucket,
                key,
                part_number,
                upload_id,
                blob_deletion,
            )
            .await
        }
        PutEndpoint::CopyObject => {
            put::copy_object_handler(app, request, api_key, bucket, key, blob_deletion).await
        }
    }
}

async fn post_handler(
    app: Arc<AppState>,
    request: Request,
    bucket: &Bucket,
    key: String,
    blob_deletion: Sender<(BlobId, usize)>,
    endpoint: PostEndpoint,
) -> Result<Response, S3Error> {
    match endpoint {
        PostEndpoint::CompleteMultipartUpload(upload_id) => {
            post::complete_multipart_upload_handler(
                app,
                request,
                bucket,
                key,
                upload_id,
                blob_deletion,
            )
            .await
        }
        PostEndpoint::CreateMultipartUpload => {
            post::create_multipart_upload_handler(app, request, bucket, key).await
        }
        PostEndpoint::DeleteObjects => {
            post::delete_objects_handler(app, request, bucket, blob_deletion).await
        }
    }
}

async fn delete_handler(
    app: Arc<AppState>,
    request: Request,
    bucket: &Bucket,
    key: String,
    blob_deletion: Sender<(BlobId, usize)>,
    endpoint: DeleteEndpoint,
) -> Result<Response, S3Error> {
    match endpoint {
        DeleteEndpoint::AbortMultipartUpload(upload_id) => {
            delete::abort_multipart_upload_handler(app, request, bucket, key, upload_id).await
        }
        DeleteEndpoint::DeleteObject => {
            delete::delete_object_handler(app, bucket, key, blob_deletion).await
        }
    }
}
