use axum::{
    extract::FromRequestParts,
    http::{request::Parts, uri::Authority},
    RequestPartsExt,
};
use axum_extra::extract::Host;
use std::sync::Arc;

use crate::{handler::common::s3_error::S3Error, AppState, Config};

pub struct BucketAndKeyName {
    pub bucket: String,
    pub key: String,
}

impl BucketAndKeyName {
    async fn buket_name_from_host(
        parts: &mut Parts,
        config: &Config,
    ) -> Result<Option<String>, S3Error> {
        let Host(host) = parts.extract::<Host>().await?;
        let authority: Authority = host.parse::<Authority>()?;
        let bucket = authority.host().strip_suffix(&config.root_domain);
        Ok(bucket.map(|s| s.to_owned()))
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
            if bucket_part {
                bucket.push(c);
            } else {
                key.push(c);
            }
        });
        (bucket, key)
    }
}

impl FromRequestParts<Arc<AppState>> for BucketAndKeyName {
    type Rejection = S3Error;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &Arc<AppState>,
    ) -> Result<Self, Self::Rejection> {
        let full_key = parts.uri.path().to_owned();
        let (bucket, key) = match Self::buket_name_from_host(parts, &state.config).await? {
            // Virtual-hosted-style request
            Some(bucket) => (bucket, full_key),
            // Path-style request
            None => Self::get_bucket_and_key_from_path(&full_key),
        };

        // Get the original key from the URL encoded key
        let key = percent_encoding::percent_decode_str(&key)
            .decode_utf8()?
            .into_owned();

        check_key_name(&key)?;

        Ok(Self { bucket, key })
    }
}

// Allow key names as documented:
// https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html
fn check_key_name(n: &str) -> Result<(), S3Error> {
    if n.len() > 1024 {
        return Err(S3Error::KeyTooLongError);
    }

    // Our internal implementation reserved these characters for ending
    if n.ends_with("#") || n.ends_with("\0") {
        return Err(S3Error::KeyUnsupported);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Config;
    use axum::{body::Body, http::Request, Router};
    use http_body_util::BodyExt;
    use std::sync::Arc;
    use tower::ServiceExt;

    async fn handler(
        BucketAndKeyName {
            bucket: bucket_name,
            ..
        }: BucketAndKeyName,
    ) -> String {
        bucket_name
    }

    #[tokio::test]
    async fn test_extract_bucket_name_ok() {
        let bucket_name = "my-bucket";
        assert_eq!(send_request_get_body(bucket_name).await, bucket_name);
    }

    async fn send_request_get_body(bucket_name: &str) -> String {
        let config = Arc::new(Config::default());
        let app_state = Arc::new(AppState::new(config).await);
        let app = Router::new()
            .route("/{*key}", axum::routing::get(handler))
            .with_state(app_state);

        let body = app
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
