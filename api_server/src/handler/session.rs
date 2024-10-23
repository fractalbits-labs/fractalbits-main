use crate::response_xml::Xml;
use axum::{
    extract::Request,
    response::{IntoResponse, Response},
};
use serde::Serialize;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Serialize, PartialEq, Eq)]
struct CreateSessionOutput {
    #[serde(rename = "Credentials")]
    credentials: Credentials,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
struct Credentials {
    #[serde(rename = "AccessKeyId")]
    access_key_id: String,
    #[serde(rename = "Expiration")]
    expiration: u64,
    #[serde(rename = "SecretAccessKey")]
    secret_access_key: String,
    #[serde(rename = "SessionToken")]
    session_token: String,
}

pub async fn create_session(_request: Request) -> Response {
    const TIMEOUT_SECS: u64 = 300;
    let expiration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + TIMEOUT_SECS;
    let output = CreateSessionOutput {
        credentials: Credentials {
            access_key_id: "todo".into(),
            expiration,
            secret_access_key: "todo".into(),
            session_token: "todo".into(),
        },
    };
    Xml(output).into_response()
}
