mod common;
use aws_sdk_s3::primitives::ByteStream;

#[tokio::test]
async fn test_basic() {
    let ctx = common::context();
    let bucket_name = "my_bucket";
    ctx.create_bucket(bucket_name).await;

    let key = "hello";
    let value = b"42";
    let data = ByteStream::from_static(value);

    ctx.client
        .put_object()
        .bucket(bucket_name)
        .key(key)
        .body(data)
        .send()
        .await
        .unwrap();

    let res = ctx
        .client
        .get_object()
        .bucket(bucket_name)
        .key(key)
        .send()
        .await
        .unwrap();

    assert_bytes_eq!(res.body, value);
}
