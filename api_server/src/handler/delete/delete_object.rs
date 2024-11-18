use axum::{extract::Request, response};
use rpc_client_bss::RpcClientBss;
use rpc_client_nss::RpcClientNss;

pub async fn delete_object(
    _request: Request,
    _key: String,
    _rpc_client_nss: &RpcClientNss,
    _rpc_client_bss: &RpcClientBss,
) -> response::Result<()> {
    Ok(())
}
