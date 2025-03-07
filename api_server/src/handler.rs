mod bucket;
pub mod common;
mod delete;
mod get;
mod head;
mod list;
mod mpu;
mod put;
mod session;

use std::net::SocketAddr;
use std::sync::Arc;

use crate::{AppState, BlobId};
use axum::http::Method;
use axum::{
    extract::{ConnectInfo, Request, State},
    response::{IntoResponse, Response},
};
use bucket_tables::bucket_table::Bucket;
use common::request::extract::authorization::Authorization;
use common::request::extract::{
    api_command::ApiCommand, api_command::ApiCommandFromQuery, api_signature::ApiSignature,
    authorization::AuthorizationFromReq, bucket_name::BucketNameFromHost, key::KeyFromPath,
};
use common::request::signature::{verify_request, SignatureError};
use common::s3_error::S3Error;
use rpc_client_bss::RpcClientBss;
use rpc_client_nss::RpcClientNss;
use rpc_client_rss::RpcErrorRss;
use tokio::sync::mpsc::Sender;

#[allow(clippy::too_many_arguments)]
pub async fn any_handler(
    State(app): State<Arc<AppState>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    bucket_name_from_host: Result<BucketNameFromHost, S3Error>,
    ApiCommandFromQuery(api_cmd): ApiCommandFromQuery,
    KeyFromPath(key_from_path): KeyFromPath,
    api_sig: ApiSignature,
    AuthorizationFromReq(auth): AuthorizationFromReq,
    request: Request,
) -> Response {
    let (bucket_name, key) = match bucket_name_from_host {
        Err(e) => return e.into_response(),
        // Virtual-hosted-style request
        Ok(BucketNameFromHost(Some(bucket_name))) => (bucket_name, key_from_path),
        // Path-style request
        Ok(BucketNameFromHost(None)) => get_bucket_and_key_from_path(key_from_path),
    };
    tracing::debug!(%bucket_name, %key);

    let resource = format!("/{bucket_name}{key}");
    match any_handler_inner(app, addr, bucket_name, key, api_cmd, api_sig, auth, request).await {
        Err(e) => e.into_response_with_resource(&resource),
        Ok(response) => response,
    }
}

// Get bucket and key for path-style requests
fn get_bucket_and_key_from_path(path: String) -> (String, String) {
    let mut bucket = String::new();
    let mut key = String::from("/");
    let mut bucket_part = true;
    path.chars().skip_while(|c| c == &'/').for_each(|c| {
        if bucket_part && c == '/' {
            bucket_part = false;
            return;
        }
        if bucket_part && c != '\0' {
            bucket.push(c);
        } else {
            key.push(c);
        }
    });
    // Not a real key, removing hacking for nss
    if key == "/\0" {
        key.pop();
    }
    (bucket, key)
}

#[allow(clippy::too_many_arguments)]
async fn any_handler_inner(
    app: Arc<AppState>,
    addr: SocketAddr,
    bucket_name: String,
    key: String,
    api_cmd: Option<ApiCommand>,
    api_sig: ApiSignature,
    auth: Option<Authorization>,
    request: Request,
) -> Result<Response, S3Error> {
    let rpc_client_rss = app.get_rpc_client_rss();
    let (request, api_key) = match auth {
        None => (request, None),
        Some(auth) => {
            match verify_request(
                request,
                &auth,
                rpc_client_rss.clone(),
                &app.config.s3_region,
            )
            .await
            {
                Ok(res) => res,
                Err(SignatureError::RpcErrorRss(RpcErrorRss::NotFound)) => {
                    return Err(S3Error::InvalidAccessKeyId)
                }
                Err(e) => return Err(e.into()),
            }
        }
    };

    // Handle bucket related apis at first
    let rpc_client_nss = app.get_rpc_client_nss(addr);
    if key == "/" {
        match *request.method() {
            Method::HEAD => {
                return bucket::head_bucket(api_key, bucket_name, rpc_client_rss).await;
            }
            Method::PUT => {
                return bucket::create_bucket(
                    api_key,
                    bucket_name,
                    request,
                    rpc_client_nss,
                    rpc_client_rss,
                    &app.config.s3_region,
                )
                .await;
            }
            Method::DELETE => {
                let bucket = bucket::resolve_bucket(bucket_name, rpc_client_rss.clone()).await?;
                return bucket::delete_bucket(
                    api_key,
                    &bucket,
                    request,
                    rpc_client_nss,
                    rpc_client_rss,
                )
                .await;
            }
            Method::GET => {
                // Or it will be list_objects* api, which will be handled in later code
                if bucket_name.is_empty() {
                    return bucket::list_buckets(request, rpc_client_rss, &app.config.s3_region)
                        .await;
                }
            }
            _ => return Err(S3Error::NotImplemented),
        }
    }

    let bucket = bucket::resolve_bucket(bucket_name, rpc_client_rss).await?;
    let rpc_client_bss = app.get_rpc_client_bss(addr);
    match *request.method() {
        Method::HEAD => head_handler(request, &bucket, key, rpc_client_nss).await,
        Method::GET => {
            get_handler(
                request,
                api_cmd,
                api_sig,
                &bucket,
                key,
                rpc_client_nss,
                rpc_client_bss,
            )
            .await
        }
        Method::PUT => {
            put_handler(
                request,
                api_cmd,
                api_sig,
                &bucket,
                key,
                rpc_client_nss,
                rpc_client_bss,
                app.blob_deletion.clone(),
            )
            .await
        }
        Method::POST => {
            post_handler(
                request,
                api_cmd,
                api_sig,
                &bucket,
                key,
                rpc_client_nss,
                app.blob_deletion.clone(),
            )
            .await
        }
        Method::DELETE => {
            delete_handler(
                request,
                api_sig,
                &bucket,
                key,
                rpc_client_nss,
                rpc_client_bss,
                app.blob_deletion.clone(),
            )
            .await
        }
        _ => Err(S3Error::MethodNotAllowed),
    }
}

async fn head_handler(
    request: Request,
    bucket: &Bucket,
    key: String,
    rpc_client_nss: &RpcClientNss,
) -> Result<Response, S3Error> {
    match key.as_str() {
        "/" => Err(S3Error::InvalidArgument1),
        _key => head::head_object(request, bucket, key, rpc_client_nss).await,
    }
}

async fn get_handler(
    request: Request,
    api_cmd: Option<ApiCommand>,
    api_sig: ApiSignature,
    bucket: &Bucket,
    key: String,
    rpc_client_nss: &RpcClientNss,
    rpc_client_bss: &RpcClientBss,
) -> Result<Response, S3Error> {
    match (api_cmd, key.as_str()) {
        (Some(ApiCommand::Attributes), _) => {
            get::get_object_attributes(request, bucket, key, rpc_client_nss).await
        }

        (Some(ApiCommand::Uploads), "/") => {
            list::list_multipart_uploads(request, rpc_client_nss).await
        }
        (Some(ApiCommand::Session), _) => session::create_session(request).await,
        (Some(api_cmd), _) => {
            tracing::warn!("{api_cmd} not implemented");
            Err(S3Error::NotImplemented)
        }
        (None, "/") => {
            if api_sig.list_type.is_some() {
                list::list_objects_v2(request, bucket, rpc_client_nss).await
            } else {
                list::list_objects(request, bucket, rpc_client_nss).await
            }
        }
        (None, _key) if api_sig.upload_id.is_some() => {
            list::list_parts(request, bucket, key, rpc_client_nss).await
        }
        (None, _key) => get::get_object(request, bucket, key, rpc_client_nss, rpc_client_bss).await,
    }
}

#[allow(clippy::too_many_arguments)]
async fn put_handler(
    request: Request,
    api_cmd: Option<ApiCommand>,
    api_sig: ApiSignature,
    bucket: &Bucket,
    key: String,
    rpc_client_nss: &RpcClientNss,
    rpc_client_bss: &RpcClientBss,
    blob_deletion: Sender<(BlobId, usize)>,
) -> Result<Response, S3Error> {
    match (api_cmd, api_sig.part_number, api_sig.upload_id) {
        (Some(_api_cmd), _, _) => Err(S3Error::NotImplemented),
        (None, Some(part_number), Some(upload_id)) if key != "/" => {
            mpu::upload_part(
                request,
                bucket,
                key,
                part_number,
                upload_id,
                rpc_client_nss,
                rpc_client_bss,
                blob_deletion,
            )
            .await
        }
        (None, None, None) if key != "/" => {
            put::put_object(
                request,
                bucket,
                key,
                rpc_client_nss,
                rpc_client_bss,
                blob_deletion,
            )
            .await
        }
        _ => Err(S3Error::NotImplemented),
    }
}

async fn post_handler(
    request: Request,
    api_cmd: Option<ApiCommand>,
    api_sig: ApiSignature,
    bucket: &Bucket,
    key: String,
    rpc_client_nss: &RpcClientNss,
    blob_deletion: Sender<(BlobId, usize)>,
) -> Result<Response, S3Error> {
    match (api_cmd, api_sig.upload_id) {
        (Some(ApiCommand::Delete), None) if key == "/" => {
            delete::delete_objects(request, rpc_client_nss, blob_deletion).await
        }
        (Some(ApiCommand::Uploads), None) if key != "/" => {
            mpu::create_multipart_upload(request, bucket, key, rpc_client_nss).await
        }
        (None, Some(upload_id)) if key != "/" => {
            mpu::complete_multipart_upload(
                request,
                bucket,
                key,
                upload_id,
                rpc_client_nss,
                blob_deletion,
            )
            .await
        }
        (_, _) => Err(S3Error::NotImplemented),
    }
}

#[allow(clippy::too_many_arguments)]
async fn delete_handler(
    request: Request,
    api_sig: ApiSignature,
    bucket: &Bucket,
    key: String,
    rpc_client_nss: &RpcClientNss,
    rpc_client_bss: &RpcClientBss,
    blob_deletion: Sender<(BlobId, usize)>,
) -> Result<Response, S3Error> {
    match api_sig.upload_id {
        Some(upload_id) if key != "/" => {
            mpu::abort_multipart_upload(
                request,
                bucket,
                key,
                upload_id,
                rpc_client_nss,
                rpc_client_bss,
            )
            .await
        }
        None if key != "/" => {
            delete::delete_object(bucket, key, rpc_client_nss, blob_deletion).await
        }
        _ => Err(S3Error::NotImplemented),
    }
}
