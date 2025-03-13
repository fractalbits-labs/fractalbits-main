// convert from aws's s3 rust sdk tests
mod common;

use aws_sdk_s3::{
    operation::get_object::GetObjectOutput, primitives::ByteStream, types::ChecksumAlgorithm,
    types::ChecksumMode,
};
use axum::http::HeaderValue;
use common::Context;

// The test structure is identical for all supported checksum algorithms
async fn test_checksum_on_streaming(
    ctx: &Context,
    bucket: &str,
    key: &str,
    body: &'static [u8],
    expected_decoded_content_length: usize,
    expected_encoded_content_length: usize,
    checksum_algorithm: ChecksumAlgorithm,
) -> GetObjectOutput {
    // ByteStreams created from a file are streaming and have a known size
    let mut file = tempfile::NamedTempFile::new().unwrap();
    use std::io::Write;
    file.write_all(body).unwrap();

    let stream_body = ByteStream::from_path(file.path()).await.unwrap();

    // The response from the fake connection won't return the expected XML but we don't care about
    // that error in this test
    let _ = ctx
        .client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(stream_body)
        .checksum_algorithm(checksum_algorithm)
        .customize()
        .mutate_request(move |request| {
            let x_amz_decoded_content_length = request
                .headers()
                .get("x-amz-decoded-content-length")
                .expect("x-amz-decoded-content-length header exists");
            let content_length = request
                .headers()
                .get("Content-Length")
                .expect("Content-Length header exists");

            // The length of the string "Hello world"
            assert_eq!(
                HeaderValue::from_str(&expected_decoded_content_length.to_string()).unwrap(),
                x_amz_decoded_content_length,
                "decoded content length was wrong"
            );
            // The sum of the length of the original body, chunk markers, and trailers
            assert_eq!(
                HeaderValue::from_str(&expected_encoded_content_length.to_string()).unwrap(),
                content_length,
                "content-length was expected to be {} but was {} instead",
                expected_encoded_content_length,
                content_length
            );
        })
        .send()
        .await
        .unwrap();

    ctx.client
        .get_object()
        .bucket(bucket)
        .key(key)
        .checksum_mode(ChecksumMode::Enabled)
        .send()
        .await
        .unwrap()
}

#[tokio::test]
async fn test_crc32_checksum_on_streaming() {
    let (ctx, bucket, key) = setup("test_crc32_checksum_on_streaming").await;

    let expected_aws_chunked_encoded_body: &'static str =
        "B\r\nHello world\r\n0\r\nx-amz-checksum-crc32:i9aeUg==\r\n\r\n";
    let expected_encoded_content_length = expected_aws_chunked_encoded_body.len();
    let res = test_checksum_on_streaming(
        &ctx,
        &bucket,
        &key,
        b"Hello world",
        11,
        expected_encoded_content_length,
        ChecksumAlgorithm::Crc32,
    )
    .await;
    // Header checksums are base64 encoded
    assert_eq!(res.checksum_crc32(), Some("i9aeUg=="));
    assert_bytes_eq!(res.body, b"Hello world");

    cleanup(&ctx, &bucket, &key).await;
}

// This test isn't a duplicate. It tests CRC32C (note the C) checksum request validation
#[tokio::test]
async fn test_crc32c_checksum_on_streaming() {
    let (ctx, bucket, key) = setup("test_crc32c_checksum_on_streaming").await;

    let expected_aws_chunked_encoded_body =
        "B\r\nHello world\r\n0\r\nx-amz-checksum-crc32c:crUfeA==\r\n\r\n";
    let expected_encoded_content_length = expected_aws_chunked_encoded_body.len();
    let res = test_checksum_on_streaming(
        &ctx,
        &bucket,
        &key,
        b"Hello world",
        11,
        expected_encoded_content_length,
        ChecksumAlgorithm::Crc32C,
    )
    .await;
    // Header checksums are base64 encoded
    assert_eq!(res.checksum_crc32_c(), Some("crUfeA=="));
    assert_bytes_eq!(res.body, b"Hello world");

    cleanup(&ctx, &bucket, &key).await;
}

#[tokio::test]
async fn test_sha1_checksum_on_streaming() {
    let (ctx, bucket, key) = setup("test_sha1_checksum_on_streaming").await;

    let expected_aws_chunked_encoded_body =
        "B\r\nHello world\r\n0\r\nx-amz-checksum-sha1:e1AsOh9IyGCa4hLN+2Od7jlnP14=\r\n\r\n";
    let expected_encoded_content_length = expected_aws_chunked_encoded_body.len();
    let res = test_checksum_on_streaming(
        &ctx,
        &bucket,
        &key,
        b"Hello world",
        11,
        expected_encoded_content_length,
        ChecksumAlgorithm::Sha1,
    )
    .await;
    // Header checksums are base64 encoded
    assert_eq!(res.checksum_sha1(), Some("e1AsOh9IyGCa4hLN+2Od7jlnP14="));
    assert_bytes_eq!(res.body, b"Hello world");

    cleanup(&ctx, &bucket, &key).await;
}

#[tokio::test]
async fn test_sha256_checksum_on_streaming() {
    let (ctx, bucket, key) = setup("test_sha256_checksum_on_streaming").await;

    let expected_aws_chunked_encoded_body = "B\r\nHello world\r\n0\r\nx-amz-checksum-sha256:ZOyIygCyaOW6GjVnihtTFtIS9PNmskdyMlNKiuyjfzw=\r\n\r\n";
    let expected_encoded_content_length = expected_aws_chunked_encoded_body.len();
    let res = test_checksum_on_streaming(
        &ctx,
        &bucket,
        &key,
        b"Hello world",
        11,
        expected_encoded_content_length,
        ChecksumAlgorithm::Sha256,
    )
    .await;
    // Header checksums are base64 encoded
    assert_eq!(
        res.checksum_sha256(),
        Some("ZOyIygCyaOW6GjVnihtTFtIS9PNmskdyMlNKiuyjfzw=")
    );
    assert_bytes_eq!(res.body, b"Hello world");

    cleanup(&ctx, &bucket, &key).await;
}

async fn setup(bucket: &str) -> (Context, String, String) {
    let ctx = common::context();
    let bucket = ctx.create_bucket(bucket).await;
    let key = "test.txt".to_string();
    (ctx, bucket, key)
}

async fn cleanup(ctx: &Context, bucket: &str, key: &str) {
    ctx.client
        .delete_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .unwrap();
    ctx.delete_bucket(bucket).await;
}
