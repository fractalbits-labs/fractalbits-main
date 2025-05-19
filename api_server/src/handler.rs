mod bucket;
pub mod common;
mod delete;
mod endpoint;
mod get;
mod head;
mod post;
mod put;

use std::net::SocketAddr;
use std::sync::Arc;

use crate::{AppState, BlobClient, BlobId};
use axum::{
    body::Body,
    extract::{ConnectInfo, State},
    http,
    response::Response,
};
use bucket::BucketEndpoint;
use bucket_tables::api_key_table::ApiKey;
use bucket_tables::bucket_table::Bucket;
use bucket_tables::table::Versioned;
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
use rpc_client_nss::RpcClientNss;
use rpc_client_rss::{ArcRpcClientRss, RpcErrorRss};
use tokio::sync::mpsc::Sender;

pub type Request<T = ReqBody> = http::Request<T>;

pub async fn any_handler(
    State(app): State<Arc<AppState>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    ApiCommandFromQuery(api_cmd): ApiCommandFromQuery,
    auth: Authentication,
    BucketNameAndKey { bucket_name, key }: BucketNameAndKey,
    api_sig: ApiSignature,
    request: http::Request<Body>,
) -> Response {
    tracing::debug!(%bucket_name, %key);

    let resource = format!("/{bucket_name}{key}");
    let endpoint = match Endpoint::from_extractors(&request, &bucket_name, &key, api_cmd, api_sig) {
        Err(e) => return e.into_response_with_resource(&resource),
        Ok(endpoint) => endpoint,
    };
    match any_handler_inner(app, addr, bucket_name, key, auth, request, endpoint).await {
        Err(e) => e.into_response_with_resource(&resource),
        Ok(response) => response,
    }
}

async fn any_handler_inner(
    app: Arc<AppState>,
    addr: SocketAddr,
    bucket_name: String,
    key: String,
    auth: Authentication,
    request: http::Request<Body>,
    endpoint: Endpoint,
) -> Result<Response, S3Error> {
    let rpc_client_rss = app.get_rpc_client_rss();
    let VerifiedRequest {
        request, api_key, ..
    } = match verify_request(request, &auth, rpc_client_rss.clone(), &app.config.region).await {
        Ok(res) => res,
        Err(signature::error::Error::RpcErrorRss(RpcErrorRss::NotFound)) => {
            return Err(S3Error::InvalidAccessKeyId)
        }
        Err(_e) => return Err(S3Error::InvalidSignature),
    };

    let allowed = match endpoint.authorization_type() {
        Authorization::Read => api_key.data.allow_read(&bucket_name),
        Authorization::Write => api_key.data.allow_write(&bucket_name),
        Authorization::Owner => api_key.data.allow_owner(&bucket_name),
        Authorization::None => true,
    };
    if !allowed {
        return Err(S3Error::AccessDenied);
    }

    let rpc_client_nss = app.get_rpc_client_nss(addr);
    let blob_client = app.get_blob_client(addr);
    let blob_deletion = app.blob_deletion.clone();
    match endpoint {
        Endpoint::Bucket(bucket_endpoint) => {
            bucket_handler(
                &app,
                request,
                api_key,
                bucket_name,
                rpc_client_nss,
                rpc_client_rss,
                bucket_endpoint,
            )
            .await
        }
        Endpoint::Head(head_endpoint) => {
            let bucket = bucket::resolve_bucket(bucket_name, rpc_client_rss.clone()).await?;
            head_handler(request, &bucket, key, rpc_client_nss, head_endpoint).await
        }
        Endpoint::Get(get_endpoint) => {
            let bucket = bucket::resolve_bucket(bucket_name, rpc_client_rss.clone()).await?;
            get_handler(
                request,
                &bucket,
                key,
                rpc_client_nss,
                blob_client,
                get_endpoint,
            )
            .await
        }
        Endpoint::Put(put_endpoint) => {
            let bucket = bucket::resolve_bucket(bucket_name, rpc_client_rss.clone()).await?;
            put_handler(
                request,
                api_key,
                &bucket,
                key,
                rpc_client_nss,
                blob_client,
                rpc_client_rss,
                blob_deletion,
                put_endpoint,
            )
            .await
        }
        Endpoint::Post(post_endpoint) => {
            let bucket = bucket::resolve_bucket(bucket_name, rpc_client_rss.clone()).await?;
            post_handler(
                request,
                &bucket,
                key,
                rpc_client_nss,
                blob_deletion,
                post_endpoint,
            )
            .await
        }
        Endpoint::Delete(delete_endpoint) => {
            let bucket = bucket::resolve_bucket(bucket_name, rpc_client_rss.clone()).await?;
            delete_handler(
                request,
                &bucket,
                key,
                rpc_client_nss,
                blob_client,
                blob_deletion,
                delete_endpoint,
            )
            .await
        }
    }
}

async fn bucket_handler(
    app: &Arc<AppState>,
    request: Request,
    api_key: Versioned<ApiKey>,
    bucket_name: String,
    rpc_client_nss: &RpcClientNss,
    rpc_client_rss: ArcRpcClientRss,
    endpoint: BucketEndpoint,
) -> Result<Response, S3Error> {
    match endpoint {
        BucketEndpoint::CreateBucket => {
            bucket::create_bucket_handler(
                api_key,
                bucket_name,
                request,
                rpc_client_nss,
                rpc_client_rss,
                &app.config.region,
            )
            .await
        }
        BucketEndpoint::DeleteBucket => {
            let bucket = bucket::resolve_bucket(bucket_name, rpc_client_rss.clone()).await?;
            bucket::delete_bucket_handler(api_key, &bucket, request, rpc_client_nss, rpc_client_rss)
                .await
        }
        BucketEndpoint::HeadBucket => {
            bucket::head_bucket_handler(api_key, bucket_name, rpc_client_rss).await
        }
        BucketEndpoint::ListBuckets => {
            bucket::list_buckets_handler(request, rpc_client_rss, &app.config.region).await
        }
    }
}

async fn head_handler(
    request: Request,
    bucket: &Bucket,
    key: String,
    rpc_client_nss: &RpcClientNss,
    endpoint: HeadEndpoint,
) -> Result<Response, S3Error> {
    match endpoint {
        HeadEndpoint::HeadObject => {
            head::head_object_handler(request, bucket, key, rpc_client_nss).await
        }
    }
}

async fn get_handler(
    request: Request,
    bucket: &Bucket,
    key: String,
    rpc_client_nss: &RpcClientNss,
    blob_client: Arc<BlobClient>,
    endpoint: GetEndpoint,
) -> Result<Response, S3Error> {
    match endpoint {
        GetEndpoint::GetObject => {
            get::get_object_handler(request, bucket, key, rpc_client_nss, blob_client).await
        }
        GetEndpoint::GetObjectAttributes => {
            get::get_object_attributes_handler(request, bucket, key, rpc_client_nss).await
        }
        GetEndpoint::ListMultipartUploads => {
            get::list_multipart_uploads_handler(request, rpc_client_nss).await
        }
        GetEndpoint::ListObjects => {
            get::list_objects_handler(request, bucket, rpc_client_nss).await
        }
        GetEndpoint::ListObjectsV2 => {
            get::list_objects_v2_handler(request, bucket, rpc_client_nss).await
        }
        GetEndpoint::ListParts => {
            get::list_parts_handler(request, bucket, key, rpc_client_nss).await
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn put_handler(
    request: Request,
    api_key: Versioned<ApiKey>,
    bucket: &Bucket,
    key: String,
    rpc_client_nss: &RpcClientNss,
    blob_client: Arc<BlobClient>,
    rpc_client_rss: ArcRpcClientRss,
    blob_deletion: Sender<(BlobId, usize)>,
    endpoint: PutEndpoint,
) -> Result<Response, S3Error> {
    match endpoint {
        PutEndpoint::PutObject => {
            put::put_object_handler(
                request,
                bucket,
                key,
                rpc_client_nss,
                blob_client,
                blob_deletion,
            )
            .await
        }
        PutEndpoint::UploadPart(part_number, upload_id) => {
            put::upload_part_handler(
                request,
                bucket,
                key,
                part_number,
                upload_id,
                rpc_client_nss,
                blob_client,
                blob_deletion,
            )
            .await
        }
        PutEndpoint::CopyObject => {
            put::copy_object_handler(
                request,
                api_key,
                bucket,
                key,
                rpc_client_nss,
                blob_client,
                rpc_client_rss,
                blob_deletion,
            )
            .await
        }
    }
}

async fn post_handler(
    request: Request,
    bucket: &Bucket,
    key: String,
    rpc_client_nss: &RpcClientNss,
    blob_deletion: Sender<(BlobId, usize)>,
    endpoint: PostEndpoint,
) -> Result<Response, S3Error> {
    match endpoint {
        PostEndpoint::CompleteMultipartUpload(upload_id) => {
            post::complete_multipart_upload_handler(
                request,
                bucket,
                key,
                upload_id,
                rpc_client_nss,
                blob_deletion,
            )
            .await
        }
        PostEndpoint::CreateMultipartUpload => {
            post::create_multipart_upload_handler(request, bucket, key, rpc_client_nss).await
        }
        PostEndpoint::DeleteObjects => {
            post::delete_objects_handler(request, bucket, rpc_client_nss, blob_deletion).await
        }
    }
}

async fn delete_handler(
    request: Request,
    bucket: &Bucket,
    key: String,
    rpc_client_nss: &RpcClientNss,
    blob_client: Arc<BlobClient>,
    blob_deletion: Sender<(BlobId, usize)>,
    endpoint: DeleteEndpoint,
) -> Result<Response, S3Error> {
    match endpoint {
        DeleteEndpoint::AbortMultipartUpload(upload_id) => {
            delete::abort_multipart_upload_handler(
                request,
                bucket,
                key,
                upload_id,
                rpc_client_nss,
                blob_client,
            )
            .await
        }
        DeleteEndpoint::DeleteObject => {
            delete::delete_object_handler(bucket, key, rpc_client_nss, blob_deletion).await
        }
    }
}
