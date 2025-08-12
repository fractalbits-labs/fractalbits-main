use crate::handler::{
    common::{
        response::xml::{Xml, XmlnsS3},
        s3_error::S3Error,
    },
    ObjectRequestContext,
};
use serde::{Deserialize, Serialize};

#[allow(dead_code)]
#[derive(Debug, Deserialize, Default)]
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
    #[serde(rename = "@xmlns")]
    xmlns: XmlnsS3,
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
    ctx: ObjectRequestContext,
) -> Result<actix_web::HttpResponse, S3Error> {
    // Parse query parameters
    let query_string = ctx.request.query_string();
    let _opts: ListMultipartUploadsOptions =
        serde_urlencoded::from_str(query_string).unwrap_or_default();
    Xml(ListMultipartUploadsResult::default()).try_into()
}
