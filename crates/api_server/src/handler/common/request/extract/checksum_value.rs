use axum::{
    extract::FromRequestParts,
    http::{HeaderMap, request::Parts},
};
use base64::{Engine, prelude::BASE64_STANDARD};
use std::sync::Arc;

use crate::{
    AppState,
    handler::common::{
        checksum::ChecksumValue, s3_error::S3Error, signature::SignatureError, xheader,
    },
};

pub struct ChecksumValueFromHeaders(pub Option<ChecksumValue>);

impl FromRequestParts<Arc<AppState>> for ChecksumValueFromHeaders {
    type Rejection = S3Error;

    async fn from_request_parts(
        parts: &mut Parts,
        _state: &Arc<AppState>,
    ) -> Result<Self, Self::Rejection> {
        let result = extract_checksum_value_from_headers(&parts.headers);
        match result {
            Ok(checksum) => Ok(ChecksumValueFromHeaders(checksum)),
            Err(e) => Err(e.into()),
        }
    }
}

fn extract_checksum_value_from_headers(
    headers: &HeaderMap,
) -> Result<Option<ChecksumValue>, SignatureError> {
    let mut found_checksum: Option<ChecksumValue> = None;

    // Check CRC32
    if headers.contains_key(xheader::X_AMZ_CHECKSUM_CRC32.as_str()) {
        if found_checksum.is_some() {
            return Err(SignatureError::Other(
                "multiple x-amz-checksum-* headers given".into(),
            ));
        }
        let crc32 = headers
            .get(xheader::X_AMZ_CHECKSUM_CRC32.as_str())
            .and_then(|x| BASE64_STANDARD.decode(x).ok())
            .and_then(|x| x.try_into().ok())
            .ok_or_else(|| SignatureError::Other("invalid x-amz-checksum-crc32 header".into()))?;
        found_checksum = Some(ChecksumValue::Crc32(crc32));
    }

    // Check CRC32C
    if headers.contains_key(xheader::X_AMZ_CHECKSUM_CRC32C.as_str()) {
        if found_checksum.is_some() {
            return Err(SignatureError::Other(
                "multiple x-amz-checksum-* headers given".into(),
            ));
        }
        let crc32c = headers
            .get(xheader::X_AMZ_CHECKSUM_CRC32C.as_str())
            .and_then(|x| BASE64_STANDARD.decode(x).ok())
            .and_then(|x| x.try_into().ok())
            .ok_or_else(|| SignatureError::Other("invalid x-amz-checksum-crc32c header".into()))?;
        found_checksum = Some(ChecksumValue::Crc32c(crc32c));
    }

    // Check CRC64NVME
    if headers.contains_key(xheader::X_AMZ_CHECKSUM_CRC64NVME.as_str()) {
        if found_checksum.is_some() {
            return Err(SignatureError::Other(
                "multiple x-amz-checksum-* headers given".into(),
            ));
        }
        let crc64nvme = headers
            .get(xheader::X_AMZ_CHECKSUM_CRC64NVME.as_str())
            .and_then(|x| BASE64_STANDARD.decode(x).ok())
            .and_then(|x| x.try_into().ok())
            .ok_or_else(|| {
                SignatureError::Other("invalid x-amz-checksum-crc64nvme header".into())
            })?;
        found_checksum = Some(ChecksumValue::Crc64Nvme(crc64nvme));
    }

    // Check SHA1
    if headers.contains_key(xheader::X_AMZ_CHECKSUM_SHA1.as_str()) {
        if found_checksum.is_some() {
            return Err(SignatureError::Other(
                "multiple x-amz-checksum-* headers given".into(),
            ));
        }
        let sha1 = headers
            .get(xheader::X_AMZ_CHECKSUM_SHA1.as_str())
            .and_then(|x| BASE64_STANDARD.decode(x).ok())
            .and_then(|x| x.try_into().ok())
            .ok_or_else(|| SignatureError::Other("invalid x-amz-checksum-sha1 header".into()))?;
        found_checksum = Some(ChecksumValue::Sha1(sha1));
    }

    // Check SHA256
    if headers.contains_key(xheader::X_AMZ_CHECKSUM_SHA256.as_str()) {
        if found_checksum.is_some() {
            return Err(SignatureError::Other(
                "multiple x-amz-checksum-* headers given".into(),
            ));
        }
        let sha256 = headers
            .get(xheader::X_AMZ_CHECKSUM_SHA256.as_str())
            .and_then(|x| BASE64_STANDARD.decode(x).ok())
            .and_then(|x| x.try_into().ok())
            .ok_or_else(|| SignatureError::Other("invalid x-amz-checksum-sha256 header".into()))?;
        found_checksum = Some(ChecksumValue::Sha256(sha256));
    }

    Ok(found_checksum)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Config;
    use axum::{
        Router,
        body::Body,
        http::{Request, StatusCode},
        response::Response,
        routing::get,
    };
    use base64::{Engine, engine::general_purpose::STANDARD as BASE64_STANDARD};
    use std::sync::Arc;
    use tower::ServiceExt;

    async fn handler(checksum: ChecksumValueFromHeaders) -> Response<Body> {
        let body = match checksum.0 {
            Some(ChecksumValue::Crc32(crc32)) => {
                format!("crc32:{}", BASE64_STANDARD.encode(crc32))
            }
            Some(ChecksumValue::Crc32c(crc32c)) => {
                format!("crc32c:{}", BASE64_STANDARD.encode(crc32c))
            }
            Some(ChecksumValue::Sha1(sha1)) => {
                format!("sha1:{}", BASE64_STANDARD.encode(sha1))
            }
            Some(ChecksumValue::Sha256(sha256)) => {
                format!("sha256:{}", BASE64_STANDARD.encode(sha256))
            }
            Some(ChecksumValue::Crc64Nvme(crc64nvme)) => {
                format!("crc64nvme:{}", BASE64_STANDARD.encode(crc64nvme))
            }
            None => "no_checksum".to_string(),
        };
        Response::builder()
            .status(StatusCode::OK)
            .body(Body::from(body))
            .unwrap()
    }

    #[tokio::test]
    async fn test_extract_checksum_value_none() {
        let config = Arc::new(Config::default());
        let app_state = Arc::new(AppState::new_for_test(config));
        let app = Router::new()
            .route("/obj1", get(handler))
            .with_state(app_state);

        let request = Request::builder().uri("/obj1").body(Body::empty()).unwrap();

        let response = app.oneshot(request).await.unwrap();
        let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let result = String::from_utf8(body_bytes.to_vec()).unwrap();
        assert_eq!(result, "no_checksum");
    }

    #[tokio::test]
    async fn test_extract_checksum_value_crc32() {
        let config = Arc::new(Config::default());
        let app_state = Arc::new(AppState::new_for_test(config));
        let app = Router::new()
            .route("/obj1", get(handler))
            .with_state(app_state);

        let crc32_bytes = [0x12, 0x34, 0x56, 0x78];
        let crc32_b64 = BASE64_STANDARD.encode(crc32_bytes);

        let request = Request::builder()
            .uri("/obj1")
            .header("x-amz-checksum-crc32", crc32_b64.as_str())
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let result = String::from_utf8(body_bytes.to_vec()).unwrap();
        assert_eq!(result, format!("crc32:{}", crc32_b64));
    }

    #[tokio::test]
    async fn test_extract_checksum_value_multiple_headers_error() {
        let config = Arc::new(Config::default());
        let app_state = Arc::new(AppState::new_for_test(config));
        let app = Router::new()
            .route("/obj1", get(handler))
            .with_state(app_state);

        let crc32_bytes = [0x12, 0x34, 0x56, 0x78];
        let crc32c_bytes = [0x87, 0x65, 0x43, 0x21];

        let request = Request::builder()
            .uri("/obj1")
            .header("x-amz-checksum-crc32", BASE64_STANDARD.encode(crc32_bytes))
            .header(
                "x-amz-checksum-crc32c",
                BASE64_STANDARD.encode(crc32c_bytes),
            )
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        // Should return an error response for multiple checksum headers
        assert!(!response.status().is_success());
    }
}
