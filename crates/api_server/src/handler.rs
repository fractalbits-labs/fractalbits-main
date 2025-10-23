mod bucket;
pub mod common;
mod delete;
mod endpoint;
mod get;
mod head;
mod post;
mod put;

use crate::AppState;
use actix_web::{
    HttpRequest, HttpResponse, ResponseError,
    web::{self, Payload},
};
use bucket::BucketEndpoint;
use bumpalo::Bump;
use common::{
    authorization::Authorization, checksum::ChecksumValue, request::extract::*, s3_error::S3Error,
    signature::check_signature,
};
use data_types::{ApiKey, Bucket, Versioned};
use delete::DeleteEndpoint;
use endpoint::Endpoint;
use get::GetEndpoint;
use head::HeadEndpoint;
use metrics::{Gauge, counter, gauge, histogram};
use post::PostEndpoint;
use put::PutEndpoint;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, error, warn};

pub struct BucketRequestContext {
    pub app: Arc<AppState>,
    pub request: HttpRequest,
    pub api_key: Versioned<ApiKey>,
    pub bucket_name: String,
    pub payload: actix_web::dev::Payload,
    pub trace_id: u64,
}

impl BucketRequestContext {
    pub fn new(
        app: Arc<AppState>,
        request: HttpRequest,
        api_key: Versioned<ApiKey>,
        bucket_name: String,
        payload: actix_web::dev::Payload,
        trace_id: u64,
    ) -> Self {
        Self {
            app,
            request,
            api_key,
            bucket_name,
            payload,
            trace_id,
        }
    }

    pub async fn resolve_bucket(&self) -> Result<Bucket, S3Error> {
        bucket::resolve_bucket(self.app.clone(), self.bucket_name.clone()).await
    }
}

pub struct ObjectRequestContext {
    pub app: Arc<AppState>,
    pub request: HttpRequest,
    pub api_key: Option<Versioned<ApiKey>>,
    pub auth: Option<Authentication>,
    pub bucket_name: String,
    pub key: String,
    pub checksum_value: Option<ChecksumValue>,
    pub payload: actix_web::dev::Payload,
    pub trace_id: u64,
}

impl ObjectRequestContext {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        app: Arc<AppState>,
        request: HttpRequest,
        api_key: Option<Versioned<ApiKey>>,
        auth: Option<Authentication>,
        bucket_name: String,
        key: String,
        checksum_value: Option<ChecksumValue>,
        payload: actix_web::dev::Payload,
        trace_id: u64,
    ) -> Self {
        Self {
            app,
            request,
            api_key,
            auth,
            bucket_name,
            key,
            checksum_value,
            payload,
            trace_id,
        }
    }

    pub async fn resolve_bucket(&self) -> Result<Bucket, S3Error> {
        bucket::resolve_bucket(self.app.clone(), self.bucket_name.clone()).await
    }
}

struct RequestBumpGuard {
    trace_id: u64,
}

impl RequestBumpGuard {
    fn new(trace_id: u64, bump: &bumpalo::Bump) -> Self {
        rpc_client_common::register_request_bump(trace_id, bump);
        Self { trace_id }
    }
}

impl Drop for RequestBumpGuard {
    fn drop(&mut self) {
        rpc_client_common::unregister_request_bump(self.trace_id);
    }
}

/// Extracts data from request and returns early with warning log on failure
macro_rules! extract_or_return {
    ($extractor_type:ty, $req:expr) => {{
        use actix_web::FromRequest;
        match <$extractor_type>::from_request($req, &mut actix_web::dev::Payload::None).await {
            Ok(extracted) => extracted,
            Err(rejection) => {
                tracing::warn!(
                    "failed to extract {} at {}:{} {:?} {:?}",
                    stringify!($extractor_type),
                    file!(),
                    line!(),
                    rejection,
                    $req.uri()
                );
                return Ok(S3Error::InternalError.error_response());
            }
        }
    }};
}

pub async fn any_handler(req: HttpRequest, payload: Payload) -> Result<HttpResponse, S3Error> {
    let start = Instant::now();

    // Extract all the required data using the macro
    let ApiCommandFromQuery(api_cmd) = extract_or_return!(ApiCommandFromQuery, &req);
    let AuthFromHeaders(auth) = extract_or_return!(AuthFromHeaders, &req);
    let BucketAndKeyName { bucket, key } = extract_or_return!(BucketAndKeyName, &req);
    let api_sig = extract_or_return!(ApiSignatureExtractor, &req);
    let ChecksumValueFromHeaders(checksum_value) =
        extract_or_return!(ChecksumValueFromHeaders, &req);

    let client_addr = req
        .connection_info()
        .realip_remote_addr()
        .unwrap_or("0.0.0.0:0")
        .to_string();

    debug!(%bucket, %key, %client_addr);

    let resource = format!("/{bucket}{key}");
    let endpoint = match Endpoint::from_extractors(&req, &bucket, &key, api_cmd, api_sig.0.clone())
    {
        Err(e) => {
            let api_cmd = api_cmd.map_or("".into(), |cmd| cmd.to_string());
            warn!(%api_cmd, %api_sig, %bucket, %key, %client_addr, error = ?e, "failed to create endpoint");
            return Ok(e.error_response_with_resource(&resource));
        }
        Ok(endpoint) => endpoint,
    };

    let endpoint_name = endpoint.as_str();
    let gauge_guard = InflightRequestGuard::new(endpoint_name);

    // Get app state and per-core context
    let app_data = req
        .app_data::<web::Data<Arc<AppState>>>()
        .ok_or(S3Error::InternalError)?;
    let app = app_data.get_ref().clone();

    let result = tokio::time::timeout(
        Duration::from_secs(app.config.http_request_timeout_seconds),
        any_handler_inner(
            app,
            bucket.clone(),
            key.clone(),
            auth,
            checksum_value,
            &req,
            payload.into_inner(),
            endpoint,
        ),
    )
    .await;
    let duration = start.elapsed();
    drop(gauge_guard);

    let result = match result {
        Ok(result) => result,
        Err(_) => {
            error!( endpoint = %endpoint_name, %bucket, %key, %client_addr, "request timed out");
            counter!("request_timeout", "endpoint" => endpoint_name).increment(1);
            return Ok(S3Error::InternalError.error_response_with_resource(&resource));
        }
    };

    match result {
        Ok(response) => {
            histogram!("request_duration_nanos", "status" => format!("{endpoint_name}_Ok"))
                .record(duration.as_nanos() as f64);
            Ok(response)
        }
        Err(e) => {
            histogram!("request_duration_nanos", "status" => format!("{endpoint_name}_Err"))
                .record(duration.as_nanos() as f64);
            error!(endpoint = %endpoint_name, %bucket, %key, %client_addr, error = ?e, "failed to handle request");
            Ok(e.error_response_with_resource(&resource))
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn any_handler_inner(
    app: Arc<AppState>,
    bucket: String,
    key: String,
    auth: Option<Authentication>,
    checksum_value: Option<ChecksumValue>,
    request: &HttpRequest,
    payload: actix_web::dev::Payload,
    endpoint: Endpoint,
) -> Result<HttpResponse, S3Error> {
    let bump = Bump::with_capacity(64 * 1024);

    // Generate trace ID and register bump for RPC calls
    let trace_id = app.generate_trace_id();
    let _bump_guard = RequestBumpGuard::new(trace_id, &bump);

    let start = Instant::now();

    let api_key = check_signature(app.clone(), request, auth.as_ref()).await?;
    histogram!("verify_request_duration_nanos", "endpoint" => endpoint.as_str())
        .record(start.elapsed().as_nanos() as f64);

    // Check authorization
    let authorization_type = endpoint.authorization_type();
    let allowed = match authorization_type {
        Authorization::Read => api_key.data.allow_read(&bucket),
        Authorization::Write => api_key.data.allow_write(&bucket),
        Authorization::Owner => api_key.data.allow_owner(&bucket),
        Authorization::None => true,
    };
    debug!(
        "Authorization check: endpoint={:?}, bucket={}, required={:?}, allowed={}",
        endpoint.as_str(),
        bucket,
        authorization_type,
        allowed
    );
    if !allowed {
        return Err(S3Error::AccessDenied);
    }

    match endpoint {
        Endpoint::Bucket(bucket_endpoint) => {
            let bucket_ctx =
                BucketRequestContext::new(app, request.clone(), api_key, bucket, payload, trace_id);
            bucket_handler(bucket_ctx, bucket_endpoint).await
        }
        ref _object_endpoints => {
            let object_ctx = ObjectRequestContext::new(
                app,
                request.clone(),
                Some(api_key),
                auth,
                bucket,
                key,
                checksum_value,
                payload,
                trace_id,
            );
            match endpoint {
                Endpoint::Head(head_endpoint) => head_handler(object_ctx, head_endpoint).await,
                Endpoint::Get(get_endpoint) => get_handler(object_ctx, get_endpoint).await,
                Endpoint::Put(put_endpoint) => put_handler(object_ctx, put_endpoint).await,
                Endpoint::Post(post_endpoint) => post_handler(object_ctx, post_endpoint).await,
                Endpoint::Delete(delete_endpoint) => {
                    delete_handler(object_ctx, delete_endpoint).await
                }
                Endpoint::Bucket(_) => unreachable!(),
            }
        }
    }
}

async fn bucket_handler(
    ctx: BucketRequestContext,
    endpoint: BucketEndpoint,
) -> Result<HttpResponse, S3Error> {
    match endpoint {
        BucketEndpoint::CreateBucket => bucket::create_bucket_handler(ctx).await,
        BucketEndpoint::DeleteBucket => bucket::delete_bucket_handler(ctx).await,
        BucketEndpoint::HeadBucket => bucket::head_bucket_handler(ctx).await,
        BucketEndpoint::ListBuckets => bucket::list_buckets_handler(ctx).await,
    }
}

async fn head_handler(
    ctx: ObjectRequestContext,
    endpoint: HeadEndpoint,
) -> Result<HttpResponse, S3Error> {
    match endpoint {
        HeadEndpoint::HeadObject => head::head_object_handler(ctx).await,
    }
}

async fn get_handler(
    ctx: ObjectRequestContext,
    endpoint: GetEndpoint,
) -> Result<HttpResponse, S3Error> {
    match endpoint {
        GetEndpoint::GetObject => get::get_object_handler(ctx).await,
        GetEndpoint::GetObjectAttributes => get::get_object_attributes_handler(ctx).await,
        GetEndpoint::ListMultipartUploads => get::list_multipart_uploads_handler(ctx).await,
        GetEndpoint::ListObjects => get::list_objects_handler(ctx).await,
        GetEndpoint::ListObjectsV2 => get::list_objects_v2_handler(ctx).await,
        GetEndpoint::ListParts => get::list_parts_handler(ctx).await,
    }
}

async fn put_handler(
    ctx: ObjectRequestContext,
    endpoint: PutEndpoint,
) -> Result<HttpResponse, S3Error> {
    match endpoint {
        PutEndpoint::PutObject => put::put_object_handler(ctx).await,
        PutEndpoint::UploadPart(part_number, upload_id) => {
            put::upload_part_handler(ctx, part_number, upload_id).await
        }
        PutEndpoint::CopyObject => put::copy_object_handler(ctx).await,
        PutEndpoint::RenameFolder => put::rename_folder_handler(ctx).await,
        PutEndpoint::RenameObject => put::rename_object_handler(ctx).await,
    }
}

async fn post_handler(
    ctx: ObjectRequestContext,
    endpoint: PostEndpoint,
) -> Result<HttpResponse, S3Error> {
    match endpoint {
        PostEndpoint::CompleteMultipartUpload(upload_id) => {
            post::complete_multipart_upload_handler(ctx, upload_id).await
        }
        PostEndpoint::CreateMultipartUpload => post::create_multipart_upload_handler(ctx).await,
        PostEndpoint::DeleteObjects => post::delete_objects_handler(ctx).await,
    }
}

async fn delete_handler(
    ctx: ObjectRequestContext,
    endpoint: DeleteEndpoint,
) -> Result<HttpResponse, S3Error> {
    match endpoint {
        DeleteEndpoint::AbortMultipartUpload(upload_id) => {
            delete::abort_multipart_upload_handler(ctx, upload_id).await
        }
        DeleteEndpoint::DeleteObject => delete::delete_object_handler(ctx).await,
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
