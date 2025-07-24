use std::borrow::Cow;
use std::str::FromStr;

use axum::{
    extract::{FromRequestParts, Query},
    http::request::Parts,
    RequestPartsExt,
};
use strum::EnumString;

use crate::handler::common::s3_error::S3Error;

#[derive(Debug, EnumString, Copy, PartialEq, Clone, strum::Display)]
#[strum(serialize_all = "camelCase")]
pub enum ApiCommand {
    Accelerate,
    Acl,
    Attributes,
    Analytics,
    Cors,
    Delete,
    Encryption,
    IntelligentTiering,
    Inventory,
    LegalHold,
    Lifecycle,
    Location,
    Logging,
    Metrics,
    Notification,
    ObjectLock,
    OwnershipControls,
    Policy,
    PolicyStatus,
    PublicAccessBlock,
    Replication,
    RequestPayment,
    Rename,
    Restore,
    Retention,
    Select,
    Session,
    Tagging,
    Torrent,
    Uploads,
    Versioning,
    Versions,
    Website,
}

pub struct ApiCommandFromQuery(pub Option<ApiCommand>);

impl<S> FromRequestParts<S> for ApiCommandFromQuery
where
    S: Send + Sync,
{
    type Rejection = S3Error;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        let query_params: Query<Vec<(Cow<'_, str>, Cow<'_, str>)>> = parts.extract().await?;
        let api_commands: Vec<ApiCommand> = query_params
            .iter()
            .filter_map(|(k, v)| v.as_ref().is_empty().then_some(k))
            .filter_map(|cmd| ApiCommand::from_str(cmd.as_ref()).ok())
            .collect();
        if api_commands.is_empty() {
            Ok(ApiCommandFromQuery(None))
        } else {
            if api_commands.len() > 1 {
                tracing::warn!(
                    "Multiple api command found: {api_commands:?}, pick up the first one"
                );
            }
            Ok(ApiCommandFromQuery(Some(api_commands[0])))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{body::Body, http::Request, routing::get, Router};
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    fn app() -> Router {
        Router::new().route("/{*key}", get(handler))
    }

    async fn handler(ApiCommandFromQuery(api_command): ApiCommandFromQuery) -> String {
        match api_command {
            Some(api_command) => api_command.to_string(),
            None => "".into(),
        }
    }

    #[tokio::test]
    async fn test_extract_api_command_ok() {
        let api_cmd = "acl";
        assert_eq!(send_request_get_body(api_cmd).await, api_cmd);
    }

    #[tokio::test]
    async fn test_extract_api_command_null() {
        let api_cmd = "";
        assert_eq!(send_request_get_body(api_cmd).await, api_cmd);
    }

    async fn send_request_get_body(api_cmd: &str) -> String {
        let api_cmd = if api_cmd.is_empty() {
            ""
        } else {
            &format!("?{api_cmd}")
        };
        let body = app()
            .oneshot(
                Request::builder()
                    .uri(format!("http://my-bucket.localhost/obj1{api_cmd}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap()
            .into_body();
        let bytes = body.collect().await.unwrap().to_bytes();
        String::from_utf8(bytes.to_vec()).unwrap()
    }
}
