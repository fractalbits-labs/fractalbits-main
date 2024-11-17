mod delete;
mod get;
mod list;
mod put;
mod session;

use std::net::SocketAddr;
use std::sync::Arc;

use super::extract::{api_command::ApiCommandFromQuery, bucket_name::BucketName, key::Key};
use super::AppState;
use crate::extract::api_command::ApiCommand;
use crate::extract::api_signature::ApiSignature;
use axum::http::Method;
use axum::{
    extract::{ConnectInfo, Request, State},
    response::{IntoResponse, Response},
};
use rpc_client_bss::RpcClientBss;
use rpc_client_nss::RpcClientNss;

pub async fn any_handler(
    State(app): State<Arc<AppState>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    BucketName(bucket_name): BucketName,
    ApiCommandFromQuery(api_command): ApiCommandFromQuery,
    Key(key): Key,
    api_signature: ApiSignature,
    request: Request,
) -> Response {
    tracing::debug!(%bucket_name);
    let rpc_client_nss = app.get_rpc_client_nss(addr);
    let rpc_client_bss = app.get_rpc_client_bss(addr);
    match request.method() {
        &Method::GET => {
            get_handler(
                request,
                api_command,
                api_signature,
                key,
                rpc_client_nss,
                rpc_client_bss,
            )
            .await
        }
        &Method::PUT => {
            put_handler(
                request,
                api_command,
                api_signature,
                key,
                rpc_client_nss,
                rpc_client_bss,
            )
            .await
        }
        method => panic!("TODO: method {method}"),
    }
}

async fn get_handler(
    request: Request,
    api_command: Option<ApiCommand>,
    api_signature: ApiSignature,
    key: String,
    rpc_client_nss: &RpcClientNss,
    rpc_client_bss: &RpcClientBss,
) -> Response {
    match api_command {
        Some(ApiCommand::Session) => session::create_session(request).await,
        Some(api_command) => panic!("TODO: {api_command}"),
        None => {
            if key.is_empty() {
                if api_signature.list_type.is_some() {
                    list::list_objects_v2(request, rpc_client_nss)
                        .await
                        .into_response()
                } else {
                    "Legacy listObjects api is not supported!".into_response()
                }
            } else {
                get::get_object(request, key, rpc_client_nss, rpc_client_bss)
                    .await
                    .into_response()
            }
        }
    }
}

async fn put_handler(
    request: Request,
    api_command: Option<ApiCommand>,
    _api_signature: ApiSignature,
    key: String,
    rpc_client_nss: &RpcClientNss,
    rpc_client_bss: &RpcClientBss,
) -> Response {
    match api_command {
        Some(api_command) => panic!("TODO: {api_command}"),
        None => put::put_object(request, key, rpc_client_nss, rpc_client_bss)
            .await
            .into_response(),
    }
}
