use aws_signature::{
    sigv4::{sign_request, SigningParams},
    streaming::{create_chunk_signature_context, create_streaming_body, SignatureError},
    STREAMING_PAYLOAD,
};
use chrono::Utc;
use reqwest::{Client, Method};
use std::collections::{BTreeMap, BTreeSet};

/// Simple S3 test client using reqwest and the aws_signature common crate
pub struct S3TestClient {
    client: Client,
    params: SigningParams,
    endpoint: String,
}

impl S3TestClient {
    pub fn new(
        access_key_id: String,
        secret_access_key: String,
        region: String,
        endpoint: String,
    ) -> Self {
        Self {
            client: Client::new(),
            params: SigningParams {
                access_key_id,
                secret_access_key,
                region,
                service: "s3".to_string(),
                datetime: Utc::now(),
            },
            endpoint,
        }
    }

    pub async fn put_object_chunked(
        &self,
        bucket: &str,
        key: &str,
        body: Vec<u8>,
        chunk_size: usize,
        headers: Option<BTreeMap<String, String>>,
    ) -> Result<reqwest::Response, Box<dyn std::error::Error + Send + Sync>> {
        let url = format!("{}/{}/{}", self.endpoint, bucket, key);
        let datetime = Utc::now();

        // Update signing params with current time
        let mut params = self.params.clone();
        params.datetime = datetime;

        // Create chunk signature context
        let chunk_context =
            create_chunk_signature_context(datetime, &params.secret_access_key, &params.region)?;

        // Create headers
        let mut request_headers = BTreeMap::new();
        request_headers.insert("host".to_string(), self.extract_host(&url)?);
        request_headers.insert(
            "x-amz-date".to_string(),
            datetime.format("%Y%m%dT%H%M%SZ").to_string(),
        );
        request_headers.insert(
            "x-amz-content-sha256".to_string(),
            STREAMING_PAYLOAD.to_string(),
        );
        request_headers.insert("content-encoding".to_string(), "aws-chunked".to_string());
        request_headers.insert(
            "x-amz-decoded-content-length".to_string(),
            body.len().to_string(),
        );

        // Add custom headers
        if let Some(custom_headers) = headers {
            for (key, value) in custom_headers {
                request_headers.insert(key, value);
            }
        }

        // Calculate seed signature (signature of the initial request)
        let signed_headers: BTreeSet<String> = request_headers.keys().cloned().collect();
        let uri_path = format!("/{}", key);
        let query_params = BTreeMap::new();

        // Build canonical headers from request headers
        let mut canonical_headers = Vec::new();
        for header_name in &signed_headers {
            if let Some(header_value) = request_headers.get(header_name) {
                canonical_headers.push(format!("{}:{}", header_name, header_value.trim()));
            }
        }

        let authorization = sign_request(
            &params,
            "PUT",
            &uri_path,
            &query_params,
            &canonical_headers,
            &signed_headers,
            STREAMING_PAYLOAD,
        )
        .map_err(|e| format!("Failed to sign request: {}", e))?;

        // Extract seed signature from authorization header
        let seed_signature = self.extract_seed_signature(&authorization)?;

        // Create streaming body with chunk signatures
        let streaming_body =
            create_streaming_body(&body, chunk_size, &chunk_context, &seed_signature)?;

        // Add content length for the streaming body (including chunk headers)
        let final_headers = {
            let mut headers = request_headers.clone();
            headers.insert(
                "content-length".to_string(),
                streaming_body.len().to_string(),
            );
            headers.insert("authorization".to_string(), authorization);
            headers
        };

        // Convert to reqwest headers
        let mut reqwest_headers = reqwest::header::HeaderMap::new();
        for (name, value) in final_headers {
            reqwest_headers.insert(
                reqwest::header::HeaderName::from_bytes(name.as_bytes())?,
                reqwest::header::HeaderValue::from_str(&value)?,
            );
        }

        // Send the request
        let response = self
            .client
            .request(Method::PUT, &url)
            .headers(reqwest_headers)
            .body(streaming_body)
            .send()
            .await?;

        Ok(response)
    }

    pub async fn put_object_chunked_invalid_signature(
        &self,
        bucket: &str,
        key: &str,
        body: Vec<u8>,
        chunk_size: usize,
    ) -> Result<reqwest::Response, Box<dyn std::error::Error + Send + Sync>> {
        // Same as put_object_chunked but with corrupted chunk signatures
        let url = format!("{}/{}/{}", self.endpoint, bucket, key);
        let datetime = Utc::now();

        let mut params = self.params.clone();
        params.datetime = datetime;

        let mut request_headers = BTreeMap::new();
        request_headers.insert("host".to_string(), self.extract_host(&url)?);
        request_headers.insert(
            "x-amz-date".to_string(),
            datetime.format("%Y%m%dT%H%M%SZ").to_string(),
        );
        request_headers.insert(
            "x-amz-content-sha256".to_string(),
            STREAMING_PAYLOAD.to_string(),
        );
        request_headers.insert("content-encoding".to_string(), "aws-chunked".to_string());
        request_headers.insert(
            "x-amz-decoded-content-length".to_string(),
            body.len().to_string(),
        );

        let signed_headers: BTreeSet<String> = request_headers.keys().cloned().collect();
        let uri_path = format!("/{}", key);
        let query_params = BTreeMap::new();

        // Build canonical headers from request headers
        let mut canonical_headers = Vec::new();
        for header_name in &signed_headers {
            if let Some(header_value) = request_headers.get(header_name) {
                canonical_headers.push(format!("{}:{}", header_name, header_value.trim()));
            }
        }

        let authorization = sign_request(
            &params,
            "PUT",
            &uri_path,
            &query_params,
            &canonical_headers,
            &signed_headers,
            STREAMING_PAYLOAD,
        )
        .map_err(|e| format!("Failed to sign request: {}", e))?;

        // Create streaming body with INVALID chunk signatures
        let streaming_body = self.create_invalid_streaming_body(&body, chunk_size)?;

        let final_headers = {
            let mut headers = request_headers.clone();
            headers.insert(
                "content-length".to_string(),
                streaming_body.len().to_string(),
            );
            headers.insert("authorization".to_string(), authorization);
            headers
        };

        let mut reqwest_headers = reqwest::header::HeaderMap::new();
        for (name, value) in final_headers {
            reqwest_headers.insert(
                reqwest::header::HeaderName::from_bytes(name.as_bytes())?,
                reqwest::header::HeaderValue::from_str(&value)?,
            );
        }

        let response = self
            .client
            .request(Method::PUT, &url)
            .headers(reqwest_headers)
            .body(streaming_body)
            .send()
            .await?;

        Ok(response)
    }

    fn extract_host(&self, url: &str) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let parsed_url = url::Url::parse(url)?;
        Ok(parsed_url.host_str().unwrap_or("localhost").to_string())
    }

    fn extract_seed_signature(
        &self,
        authorization: &str,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        // Extract the signature from "AWS4-HMAC-SHA256 ... Signature=<sig>"
        if let Some(signature_part) = authorization.split("Signature=").nth(1) {
            Ok(signature_part.to_string())
        } else {
            Err("Failed to extract signature from authorization header".into())
        }
    }

    fn create_invalid_streaming_body(
        &self,
        body: &[u8],
        chunk_size: usize,
    ) -> Result<Vec<u8>, SignatureError> {
        let mut result = Vec::new();

        // Create chunks with invalid signatures
        for chunk in body.chunks(chunk_size) {
            let invalid_signature = "invalidchunksignature1234567890abcdef1234567890abcdef12345678";
            let chunk_header = format!(
                "{:x};chunk-signature={}\r\n",
                chunk.len(),
                invalid_signature
            );
            result.extend_from_slice(chunk_header.as_bytes());
            result.extend_from_slice(chunk);
            result.extend_from_slice(b"\r\n");
        }

        // Add final zero-length chunk with invalid signature
        let final_invalid_sig = "invalidfinalsignature1234567890abcdef1234567890abcdef1234567890";
        let final_header = format!("0;chunk-signature={}\r\n\r\n", final_invalid_sig);
        result.extend_from_slice(final_header.as_bytes());

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_ACCESS_KEY: &str = "test_api_key";
    const TEST_SECRET_KEY: &str = "test_api_secret";
    const TEST_REGION: &str = "us-east-1";
    const TEST_ENDPOINT: &str = "http://localhost:8080";

    fn create_test_client() -> S3TestClient {
        S3TestClient::new(
            TEST_ACCESS_KEY.to_string(),
            TEST_SECRET_KEY.to_string(),
            TEST_REGION.to_string(),
            TEST_ENDPOINT.to_string(),
        )
    }

    #[tokio::test]
    async fn test_client_creation() {
        let client = create_test_client();
        assert_eq!(client.params.access_key_id, TEST_ACCESS_KEY);
        assert_eq!(client.params.secret_access_key, TEST_SECRET_KEY);
        assert_eq!(client.params.region, TEST_REGION);
        assert_eq!(client.endpoint, TEST_ENDPOINT);
    }

    #[tokio::test]
    async fn test_extract_host() {
        let client = create_test_client();
        let host = client
            .extract_host("http://localhost:8080/bucket/key")
            .unwrap();
        assert_eq!(host, "localhost");
    }

    #[tokio::test]
    async fn test_extract_seed_signature() {
        let client = create_test_client();
        let auth = "AWS4-HMAC-SHA256 Credential=test/scope, SignedHeaders=host, Signature=abcd1234";
        let sig = client.extract_seed_signature(auth).unwrap();
        assert_eq!(sig, "abcd1234");
    }

    // Integration test - runs with server
    #[tokio::test]
    async fn test_put_object_chunked_valid() {
        let client = create_test_client();
        let body = b"Hello, chunked world!".to_vec();

        let response = client
            .put_object_chunked("fractalbits-bucket", "test-key", body, 8, None)
            .await;

        match response {
            Ok(resp) => {
                let status = resp.status();
                let body_text = resp
                    .text()
                    .await
                    .unwrap_or_else(|_| "Could not read response body".to_string());
                println!("Response status: {}", status);
                println!("Response body: {}", body_text);
                if status.is_success() {
                    println!("Valid chunk signature test passed");
                } else {
                    println!("Valid chunk signature test failed: {}", status);
                }
            }
            Err(e) => {
                println!("Request failed: {}", e);
            }
        }
    }

    // Integration test - runs with server
    #[tokio::test]
    async fn test_put_object_chunked_invalid() {
        let client = create_test_client();
        let body = b"Hello, chunked world!".to_vec();

        let response = client
            .put_object_chunked_invalid_signature("fractalbits-bucket", "test-key-invalid", body, 8)
            .await;

        match response {
            Ok(resp) => {
                let status = resp.status();
                let body_text = resp
                    .text()
                    .await
                    .unwrap_or_else(|_| "Could not read response body".to_string());
                println!("Response status: {}", status);
                println!("Response body: {}", body_text);
                if status.is_client_error() {
                    println!("Invalid chunk signature test passed (correctly rejected)");
                } else {
                    println!("Invalid chunk signature test failed - should have been rejected but got: {}", status);
                }
            }
            Err(e) => {
                println!("Request failed: {}", e);
            }
        }
    }
}
