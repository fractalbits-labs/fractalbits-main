use axum::{extract::Path, http::StatusCode, routing::get, Router};

async fn get_obj(Path(key): Path<String>) -> Result<String, (StatusCode, String)> {
    let resp = api_server::nss_get_inode(key).await;
    match serde_json::to_string_pretty(&resp.result) {
        Ok(resp) => Ok(resp),
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("internal server error: {e}"),
        )),
    }
}

async fn put_obj(Path(key): Path<String>, value: String) -> Result<String, (StatusCode, String)> {
    let resp = api_server::nss_put_inode(key, value).await;
    match serde_json::to_string_pretty(&resp.result) {
        Ok(resp) => Ok(resp),
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("internal server error: {e}"),
        )),
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let app = Router::new().route("/:key", get(get_obj).post(put_obj));

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
