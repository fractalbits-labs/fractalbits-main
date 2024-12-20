mod delete;
mod get;
mod head;
mod list;
mod mpu;
mod put;
mod session;
mod time;

use std::net::SocketAddr;
use std::sync::Arc;

use super::extract::{
    api_command::ApiCommandFromQuery, bucket_name::BucketNameFromHost, key::KeyFromPath,
};
use super::AppState;
use crate::extract::api_command::ApiCommand;
use crate::extract::api_signature::ApiSignature;
use crate::BlobId;
use axum::http::status::StatusCode;
use axum::http::Method;
use axum::{
    extract::{ConnectInfo, Request, State},
    response::{IntoResponse, Response},
};
use rpc_client_bss::RpcClientBss;
use rpc_client_nss::RpcClientNss;
use tokio::sync::mpsc::Sender;

pub async fn any_handler(
    State(app): State<Arc<AppState>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    BucketNameFromHost(bucket_name_from_host): BucketNameFromHost,
    ApiCommandFromQuery(api_cmd): ApiCommandFromQuery,
    KeyFromPath(key_from_path): KeyFromPath,
    api_sig: ApiSignature,
    request: Request,
) -> Response {
    let (bucket_name, key) = match bucket_name_from_host {
        // Virtual-hosted-style request
        Some(bucket_name) => (bucket_name, key_from_path),
        // Path-style request
        None => get_bucket_and_key_from_path(key_from_path),
    };
    tracing::debug!(%bucket_name, %key);
    let rpc_client_nss = app.get_rpc_client_nss(addr);
    let rpc_client_bss = app.get_rpc_client_bss(addr);
    match request.method() {
        &Method::HEAD => head_handler(request, key, rpc_client_nss).await,
        &Method::GET => {
            get_handler(
                request,
                api_cmd,
                api_sig,
                key,
                rpc_client_nss,
                rpc_client_bss,
            )
            .await
        }
        &Method::PUT => {
            put_handler(
                request,
                api_cmd,
                api_sig,
                key,
                rpc_client_nss,
                rpc_client_bss,
                app.blob_deletion.clone(),
            )
            .await
        }
        &Method::POST => {
            post_handler(
                request,
                api_cmd,
                api_sig,
                bucket_name,
                key,
                rpc_client_nss,
                app.blob_deletion.clone(),
            )
            .await
        }
        &Method::DELETE => {
            delete_handler(
                request,
                api_sig,
                key,
                rpc_client_nss,
                rpc_client_bss,
                app.blob_deletion.clone(),
            )
            .await
        }
        method => (StatusCode::BAD_REQUEST, format!("TODO: method {method}")).into_response(),
    }
}

// Get bucket and key for path-style requests
fn get_bucket_and_key_from_path(path: String) -> (String, String) {
    let mut bucket = String::new();
    let mut key = String::new();
    let mut bucket_part = true;
    path.chars().skip_while(|c| c == &'/').for_each(|c| {
        if c == '/' {
            bucket_part = false;
        }
        if bucket_part {
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

async fn head_handler(request: Request, key: String, rpc_client_nss: &RpcClientNss) -> Response {
    match key.as_str() {
        "/" => StatusCode::BAD_REQUEST.into_response(),
        _key => head::head_object(request, key, rpc_client_nss)
            .await
            .into_response(),
    }
}

async fn get_handler(
    request: Request,
    api_cmd: Option<ApiCommand>,
    api_sig: ApiSignature,
    key: String,
    rpc_client_nss: &RpcClientNss,
    rpc_client_bss: &RpcClientBss,
) -> Response {
    match (api_cmd, key.as_str()) {
        (Some(ApiCommand::Attributes), _) => {
            get::get_object_attributes(request, key, rpc_client_nss)
                .await
                .into_response()
        }
        (Some(ApiCommand::Uploads), "/") => list::list_multipart_uploads(request, rpc_client_nss)
            .await
            .into_response(),
        (Some(ApiCommand::Session), _) => session::create_session(request).await,
        (Some(api_cmd), _) => (StatusCode::BAD_REQUEST, format!("TODO: {api_cmd}")).into_response(),
        (None, "/") if api_sig.list_type.is_some() => {
            list::list_objects_v2(request, rpc_client_nss)
                .await
                .into_response()
        }
        (None, "/") => (StatusCode::BAD_REQUEST, "Legacy listObjects api").into_response(),
        (None, _key) if api_sig.upload_id.is_some() => {
            list::list_parts(request, key, rpc_client_nss)
                .await
                .into_response()
        }
        (None, _key) => get::get_object(request, key, rpc_client_nss, rpc_client_bss)
            .await
            .into_response(),
    }
}

async fn put_handler(
    request: Request,
    api_cmd: Option<ApiCommand>,
    api_sig: ApiSignature,
    key: String,
    rpc_client_nss: &RpcClientNss,
    rpc_client_bss: &RpcClientBss,
    blob_deletion: Sender<(BlobId, usize)>,
) -> Response {
    match (api_cmd, api_sig.part_number, api_sig.upload_id) {
        (Some(api_cmd), _, _) => {
            (StatusCode::BAD_REQUEST, format!("TODO: {api_cmd}")).into_response()
        }
        (None, Some(part_number), Some(upload_id)) if key != "/" => mpu::upload_part(
            request,
            key,
            part_number,
            upload_id,
            rpc_client_nss,
            rpc_client_bss,
            blob_deletion,
        )
        .await
        .into_response(),
        (None, None, None) if key != "/" => {
            put::put_object(request, key, rpc_client_nss, rpc_client_bss, blob_deletion)
                .await
                .into_response()
        }
        _ => StatusCode::BAD_REQUEST.into_response(),
    }
}

async fn post_handler(
    request: Request,
    api_cmd: Option<ApiCommand>,
    api_sig: ApiSignature,
    bucket: String,
    key: String,
    rpc_client_nss: &RpcClientNss,
    blob_deletion: Sender<(BlobId, usize)>,
) -> Response {
    match (api_cmd, api_sig.upload_id) {
        (Some(ApiCommand::Delete), None) if key == "/" => {
            delete::delete_objects(request, rpc_client_nss, blob_deletion)
                .await
                .into_response()
        }
        (Some(ApiCommand::Uploads), None) if key != "/" => {
            mpu::create_multipart_upload(request, bucket, key, rpc_client_nss)
                .await
                .into_response()
        }
        (None, Some(upload_id)) if key != "/" => mpu::complete_multipart_upload(
            request,
            bucket,
            key,
            upload_id,
            rpc_client_nss,
            blob_deletion,
        )
        .await
        .into_response(),
        (_, _) => StatusCode::BAD_REQUEST.into_response(),
    }
}

async fn delete_handler(
    request: Request,
    api_sig: ApiSignature,
    key: String,
    rpc_client_nss: &RpcClientNss,
    rpc_client_bss: &RpcClientBss,
    blob_deletion: Sender<(BlobId, usize)>,
) -> Response {
    match api_sig.upload_id {
        Some(upload_id) if key != "/" => {
            mpu::abort_multipart_upload(request, key, upload_id, rpc_client_nss, rpc_client_bss)
                .await
                .into_response()
        }
        None if key != "/" => delete::delete_object(key, rpc_client_nss, blob_deletion)
            .await
            .into_response(),
        _ => StatusCode::BAD_REQUEST.into_response(),
    }
}
