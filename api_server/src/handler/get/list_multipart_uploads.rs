use crate::handler::{
    common::{response::xml::Xml, s3_error::S3Error},
    Request,
};
use axum::{extract::Query, response::Response, RequestPartsExt};
use rpc_client_nss::RpcClientNss;
use serde::{Deserialize, Serialize};

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct ListMultipartUploadsOptions {
    delimiter: Option<String>,
    encoding_type: Option<String>,
    key_marker: Option<String>,
    max_uploads: Option<u32>,
    prefix: Option<String>,
    upload_id_marker: Option<usize>,
}

#[derive(Default, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct ListMultipartUploadsResult {
    bucket: String,
    key_marker: String,
    upload_id_marker: String,
    next_key_marker: String,
    prefix: String,
    delimiter: String,
    next_upload_id_marker: String,
    max_uploads: usize,
    is_truncated: bool,
    upload: Vec<Upload>,
    common_prefixes: Vec<CommonPrefixes>,
    encoding_type: String,
}

#[derive(Default, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct Upload {
    checksum_algorithm: String,
    initiated: String, // timestamp
    initiator: Initiator,
    key: String,
    owner: Owner,
    storage_class: String,
    upload_id: String,
}

#[derive(Default, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct Initiator {
    display_name: String,
    id: String,
}

#[derive(Default, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct Owner {
    display_name: String,
    id: String,
}

#[derive(Default, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct CommonPrefixes {
    prefix: String,
}

pub async fn list_multipart_uploads_handler(
    request: Request,
    _rpc_client_nss: &RpcClientNss,
) -> Result<Response, S3Error> {
    let Query(_opts): Query<ListMultipartUploadsOptions> = request.into_parts().0.extract().await?;
    Xml(ListMultipartUploadsResult::default()).try_into()
}
