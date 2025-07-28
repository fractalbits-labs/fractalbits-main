use axum::{
    extract::{FromRequestParts, Query},
    http::request::Parts,
    RequestPartsExt,
};
use serde::Deserialize;
use std::fmt;

use crate::handler::common::s3_error::S3Error;

#[allow(dead_code)]
#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "kebab-case")]
pub struct ApiSignature {
    #[serde(rename = "uploadId")]
    pub upload_id: Option<String>,
    #[serde(rename = "partNumber")]
    pub part_number: Option<u64>,
    pub list_type: Option<String>,
    // Note this is actually from header
    pub x_amz_copy_source: Option<String>,
}

impl fmt::Display for ApiSignature {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut parts = Vec::new();
        if let Some(upload_id) = &self.upload_id {
            parts.push(format!("uploadId={upload_id}"));
        }
        if let Some(part_number) = self.part_number {
            parts.push(format!("partNumber={part_number}"));
        }
        if let Some(list_type) = &self.list_type {
            parts.push(format!("list-type={list_type}"));
        }
        if let Some(x_amz_copy_source) = &self.x_amz_copy_source {
            parts.push(format!("x-amz-copy-source={x_amz_copy_source}"));
        }
        write!(f, "{}", parts.join("&"))
    }
}

impl<S> FromRequestParts<S> for ApiSignature
where
    S: Send + Sync,
{
    type Rejection = S3Error;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        let Query(mut api_signature): Query<ApiSignature> = parts.extract().await?;
        if let Some(x_amz_copy_source) = parts.headers.get("x-amz-copy-source") {
            if let Ok(value) = x_amz_copy_source.to_str() {
                api_signature.x_amz_copy_source = Some(value.to_owned());
            }
        }
        Ok(api_signature)
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

    async fn handler(api_signature: ApiSignature) -> String {
        if let Some(upload_id) = api_signature.upload_id {
            upload_id
        } else {
            "".into()
        }
    }

    #[tokio::test]
    async fn test_extract_api_signature_from_query_ok() {
        let api_signature_str = "uploadId=abc123";
        assert_eq!(send_request_get_body(api_signature_str).await, "abc123");
    }

    async fn send_request_get_body(api_signature: &str) -> String {
        let api_signature = if api_signature.is_empty() {
            ""
        } else {
            &format!("?{api_signature}")
        };
        let body = app()
            .oneshot(
                Request::builder()
                    .uri(format!("http://my-bucket.localhost/obj1{api_signature}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap()
            .into_body();
        let bytes = body.collect().await.unwrap().to_bytes();
        String::from_utf8(bytes.to_vec()).unwrap()
    }

    #[test]
    fn test_display_api_signature() {
        let api_signature = ApiSignature {
            upload_id: Some("abc123".to_string()),
            part_number: Some(1),
            list_type: None,
            x_amz_copy_source: None,
        };
        assert_eq!(api_signature.to_string(), "uploadId=abc123&partNumber=1");

        let api_signature = ApiSignature {
            upload_id: None,
            part_number: None,
            list_type: Some("2".to_string()),
            x_amz_copy_source: None,
        };
        assert_eq!(api_signature.to_string(), "list-type=2");

        let api_signature = ApiSignature {
            upload_id: None,
            part_number: None,
            list_type: None,
            x_amz_copy_source: Some("bucket/key".to_string()),
        };
        assert_eq!(api_signature.to_string(), "x-amz-copy-source=bucket/key");

        let api_signature = ApiSignature {
            upload_id: Some("abc123".to_string()),
            part_number: Some(1),
            list_type: Some("2".to_string()),
            x_amz_copy_source: Some("bucket/key".to_string()),
        };
        assert_eq!(
            api_signature.to_string(),
            "uploadId=abc123&partNumber=1&list-type=2&x-amz-copy-source=bucket/key"
        );

        let api_signature = ApiSignature {
            upload_id: None,
            part_number: None,
            list_type: None,
            x_amz_copy_source: None,
        };
        assert_eq!(api_signature.to_string(), "");
    }
}
