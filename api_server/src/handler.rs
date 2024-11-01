mod delete;
mod get;
mod list;
mod put;
mod session;

use std::net::SocketAddr;
use std::sync::Arc;

use super::extract::{api_command::Api, bucket_name::BucketName, key::Key};
use super::AppState;
use crate::extract::api_command::ApiCommand;
use axum::http::Method;
use axum::{
    extract::{ConnectInfo, Request, State},
    response::{IntoResponse, Response},
};
use nss_rpc_client::rpc_client::RpcClient;

pub async fn any_handler(
    State(app): State<Arc<AppState>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    BucketName(bucket_name): BucketName,
    Api(api_command): Api,
    Key(key): Key,
    request: Request,
) -> Response {
    tracing::debug!(%bucket_name);
    let rpc_client = app.get_rpc_client(addr);
    match request.method() {
        &Method::GET => get_handler(request, api_command, key, rpc_client).await,
        &Method::PUT => put_handler(request, api_command, key, rpc_client).await,
        method => panic!("TODO: method {method}"),
    }
}

async fn get_handler(
    request: Request,
    api_command: Option<ApiCommand>,
    key: String,
    rpc_client: &RpcClient,
) -> Response {
    match api_command {
        Some(ApiCommand::Session) => session::create_session(request).await,
        Some(api_command) => panic!("TODO: {api_command}"),
        None => get::get_object(request, key, rpc_client)
            .await
            .into_response(),
    }
}

async fn put_handler(
    request: Request,
    api_command: Option<ApiCommand>,
    key: String,
    rpc_client: &RpcClient,
) -> Response {
    match api_command {
        Some(api_command) => panic!("TODO: {api_command}"),
        None => put::put_object(request, key, rpc_client)
            .await
            .into_response(),
    }
}
