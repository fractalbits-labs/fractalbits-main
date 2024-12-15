mod common;
use aws_sdk_s3::primitives::ByteStream;

#[tokio::test]
async fn test_basic() {
    let s3_client = common::build_client();
    let bucket: String = "mybucket".into();
    let key = "hello";
    let value = b"42";
    let data = ByteStream::from_static(value);

    s3_client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(data)
        .send()
        .await
        .unwrap();

    let res = s3_client
        .get_object()
        .bucket(&bucket)
        .key(key)
        .send()
        .await
        .unwrap();

    assert_bytes_eq!(res.body, value);
}
