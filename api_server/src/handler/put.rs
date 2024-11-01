use axum::{
    extract::Request,
    http::StatusCode,
    response::{IntoResponse, Result},
    RequestExt,
};
use nss_rpc_client::rpc_client::RpcClient;

pub async fn put_object(request: Request, key: String, rpc_client: &RpcClient) -> Result<()> {
    let value: String = request.extract().await?;

    // Write data at first

    let _resp = nss_rpc_client::nss::nss_put_inode(rpc_client, key, value)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response())?;
    // serde_json::to_string_pretty(&resp.result)
    // .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into());
    Ok(())
}
