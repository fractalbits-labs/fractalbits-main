use std::sync::Arc;

use crate::handler::delete::delete_object_handler;
use crate::handler::Request;
use crate::BlobId;
use crate::{
    handler::common::{
        response::xml::{Xml, XmlnsS3},
        s3_error::S3Error,
    },
    AppState,
};
use axum::response::Response;
use bucket_tables::bucket_table::Bucket;
use bytes::Buf;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::Sender;

// Xml request

#[derive(Default, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct Delete {
    object: Vec<Object>,
    #[serde(default)]
    quiet: bool,
}

#[derive(Default, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct Object {
    #[serde(default)]
    etag: String,
    key: String,
    #[serde(default)]
    last_modified_time: String, // timestamp
    #[serde(default)]
    size: u64,
    #[serde(default)]
    version_id: String,
}

// Xml response
#[derive(Default, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct DeleteResult {
    #[serde(rename = "@xmlns")]
    xmlns: XmlnsS3,
    deleted: Vec<Deleted>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<Error>,
}

#[derive(Default, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct Deleted {
    #[serde(skip_serializing_if = "Option::is_none")]
    delete_marker: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    delete_marker_version_id: Option<String>,
    key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    version_id: Option<String>,
}

#[derive(Default, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct Error {
    code: String,
    key: String,
    message: String,
    version_id: String,
}

pub async fn delete_objects_handler(
    app: Arc<AppState>,
    request: Request,
    bucket: &Bucket,
    blob_deletion: Sender<(BlobId, usize)>,
) -> Result<Response, S3Error> {
    let body = request.into_body().collect().await?;
    let to_be_deleted: Delete = quick_xml::de::from_reader(body.reader())?;
    let mut delete_result = DeleteResult::default();
    for obj in to_be_deleted.object {
        let key = format!("/{}\0", obj.key);
        match delete_object_handler(app.clone(), bucket, key, blob_deletion.clone()).await {
            Ok(_) => {
                let deleted = Deleted {
                    key: obj.key,
                    ..Default::default()
                };
                delete_result.deleted.push(deleted);
            }
            Err(e) => {
                delete_result.error = Some(Error {
                    code: e.as_ref().to_owned(),
                    key: obj.key,
                    message: e.to_string(),
                    version_id: "".into(),
                });
                break;
            }
        }
    }

    Xml(delete_result).try_into()
}
