use crate::{response_xml::Xml, BlobId};
use axum::{
    extract::Request,
    response::{self, IntoResponse, Response},
};
use rpc_client_nss::RpcClientNss;
use serde::Serialize;
use tokio::sync::mpsc::Sender;

#[derive(Default, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct DeleteResult {
    deleted: Deleted,
    error: Error,
}

#[derive(Default, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct Deleted {
    key: String,
}

#[derive(Default, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct Error {
    key: String,
    code: String,
    message: String,
}

pub async fn delete_objects(
    _request: Request,
    _rpc_client_nss: &RpcClientNss,
    _blob_deletion: Sender<(BlobId, usize)>,
) -> response::Result<Response> {
    Ok(Xml(DeleteResult::default()).into_response())
}
