use axum::{
    extract::{FromRef, FromRequestParts},
    http::{request::Parts, uri::Authority},
    RequestPartsExt,
};
use axum_extra::extract::Host;

use crate::{config::ArcConfig, handler::common::s3_error::S3Error};

pub struct BucketNameFromHost(pub Option<String>);

impl<S> FromRequestParts<S> for BucketNameFromHost
where
    ArcConfig: FromRef<S>,
    S: Send + Sync,
{
    type Rejection = S3Error;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let config = ArcConfig::from_ref(state);
        let Host(host) = parts.extract::<Host>().await?;
        let authority: Authority = host.parse::<Authority>()?;
        let bucket_name = authority.host().strip_suffix(&config.root_domain);
        Ok(Self(bucket_name.map(|s| s.to_owned())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use axum::{body::Body, http::Request, routing::get, Router};
    use http_body_util::BodyExt;
    use std::sync::Arc;
    use tower::ServiceExt;

    fn app() -> Router {
        let config = ArcConfig(Arc::new(Config::default()));
        Router::new()
            .route("/{*key}", get(handler))
            .with_state(config)
    }

    async fn handler(BucketNameFromHost(bucket): BucketNameFromHost) -> String {
        bucket.unwrap()
    }

    #[tokio::test]
    async fn test_extract_bucket_name_ok() {
        let bucket_name = "my-bucket";
        assert_eq!(send_request_get_body(bucket_name).await, bucket_name);
    }

    async fn send_request_get_body(bucket_name: &str) -> String {
        let body = app()
            .oneshot(
                Request::builder()
                    .uri(format!("http://{bucket_name}.localhost:3000/obj1?query1"))
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
