use axum::{
    async_trait,
    extract::{rejection::QueryRejection, FromRequestParts, Query},
    http::request::Parts,
    RequestPartsExt,
};
use serde::Deserialize;

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
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

#[async_trait]
impl<S> FromRequestParts<S> for ApiSignature
where
    S: Send + Sync,
{
    type Rejection = QueryRejection;

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
        Router::new().route("/*key", get(handler))
    }

    async fn handler(api_signature: ApiSignature) -> String {
        dbg!(&api_signature);
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
}
