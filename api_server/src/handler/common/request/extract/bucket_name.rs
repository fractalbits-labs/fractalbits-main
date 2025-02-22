use axum::{
    extract::FromRequestParts,
    http::{request::Parts, uri::Authority, StatusCode},
    RequestPartsExt,
};
use axum_extra::extract::Host;

// FIXME: put it into configs as part of the state
#[allow(non_upper_case_globals)]
const root_domain: &str = ".localhost";

pub struct BucketNameFromHost(pub Option<String>);

impl<S> FromRequestParts<S> for BucketNameFromHost
where
    S: Send + Sync,
{
    type Rejection = (StatusCode, &'static str);

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        let Host(host) = parts
            .extract::<Host>()
            .await
            .map_err(|_| (StatusCode::NOT_FOUND, "host information not found"))?;
        let authority: Authority = host.parse::<Authority>().unwrap();
        let bucket_name = authority.host().strip_suffix(root_domain);
        Ok(Self(bucket_name.map(|s| s.to_owned())))
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
