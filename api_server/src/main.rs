use api_server::{nss_get_inode, nss_ops, nss_put_inode};
use axum::{extract::Path, routing::get, Router};
use axum_extra::protobuf::Protobuf;

async fn get_obj(Path(key): Path<String>) -> Protobuf<nss_ops::GetInodeResponse> {
    nss_get_inode(key).await.into()
}

async fn put_obj(Path(key): Path<String>) -> Protobuf<nss_ops::PutInodeResponse> {
    let value: String = key.chars().rev().collect();
    nss_put_inode(key, value).await.into()
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let app = Router::new().route("/:key", get(get_obj).post(put_obj));

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
