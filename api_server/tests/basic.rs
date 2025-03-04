mod common;
use aws_sdk_s3::primitives::ByteStream;

#[tokio::test]
async fn test_basic_object_apis() {
    let ctx = common::context();
    let bucket_name = "my_bucket1";

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

    ctx.client
        .delete_object()
        .bucket(bucket_name)
        .key(key)
        .send()
        .await
        .unwrap();

    ctx.delete_bucket(bucket_name).await;
}

#[tokio::test]
async fn test_basic_bucket_apis() {
    let ctx = common::context();
    let bucket_name = "my_bucket2";
    ctx.create_bucket(bucket_name).await;
    let buckets = ctx.list_buckets().await.buckets.unwrap();
    // Note we may have concurrent tests running, so just do basic testing here
    assert!(buckets.len() >= 1);
    ctx.delete_bucket(bucket_name).await;
}
