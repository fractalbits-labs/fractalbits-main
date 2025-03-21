use axum::{
    extract::{FromRef, FromRequestParts},
    http::{request::Parts, uri::Authority},
    RequestPartsExt,
};
use axum_extra::extract::Host;

use crate::{config::ArcConfig, handler::common::s3_error::S3Error};

pub struct BucketNameAndKey {
    pub bucket_name: String,
    pub key: String,
}

impl BucketNameAndKey {
    async fn buket_name_from_host(
        parts: &mut Parts,
        config: ArcConfig,
    ) -> Result<Option<String>, S3Error> {
        let Host(host) = parts.extract::<Host>().await?;
        let authority: Authority = host.parse::<Authority>()?;
        let bucket_name = authority.host().strip_suffix(&config.root_domain);
        Ok(bucket_name.map(|s| s.to_owned()))
    }

    pub fn get_bucket_and_key_from_path(full_key: &str) -> (String, String) {
        let mut bucket = String::new();
        let mut key = String::from("/");
        let mut bucket_part = true;
        full_key.chars().skip_while(|c| c == &'/').for_each(|c| {
            if bucket_part && c == '/' {
                bucket_part = false;
                return;
            }
            if bucket_part && c != '\0' {
                bucket.push(c);
            } else {
                key.push(c);
            }
        });
        if key != "/" {
            key.push('\0'); // nss requires '\0' for now
        }
        (bucket, key)
    }
}

impl<S> FromRequestParts<S> for BucketNameAndKey
where
    ArcConfig: FromRef<S>,
    S: Send + Sync,
{
    type Rejection = S3Error;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let mut full_key = parts.uri.path().to_owned();
        let config = ArcConfig::from_ref(state);
        let (bucket_name, key) = match Self::buket_name_from_host(parts, config).await? {
            // Virtual-hosted-style request
            Some(bucket_name) => {
                full_key.push('\0'); // nss requires '\0' for now
                (bucket_name, full_key)
            }
            // Path-style request
            None => Self::get_bucket_and_key_from_path(&full_key),
        };
        Ok(Self { bucket_name, key })
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

    async fn handler(BucketNameAndKey { bucket_name, .. }: BucketNameAndKey) -> String {
        bucket_name
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
