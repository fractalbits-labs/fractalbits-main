use crate::handler::common::response::xml::Xml;
use axum::{
    extract::Request,
    response::{IntoResponse, Response},
};
use serde::Serialize;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct CreateSessionOutput {
    credentials: Credentials,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct Credentials {
    access_key_id: String,
    expiration: u64,
    secret_access_key: String,
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
