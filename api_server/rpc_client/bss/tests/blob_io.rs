use bytes::Bytes;
use fake::Fake;
use rpc_client_bss::*;
use tokio::net::TcpStream;
use tracing_test::traced_test;
use uuid::Uuid;

#[tokio::test]
#[traced_test]
async fn test_basic_blob_io_with_fixed_bytes() {
    let url = "127.0.0.1:9225";
    tracing::debug!(%url);
    // Skip testing if blob storage server is not up
    let stream = match TcpStream::connect(url).await {
        Ok(stream) => stream,
        Err(_) => return,
    };
    let rpc_client = match RpcClientBss::new(stream).await {
        Ok(client) => client,
        Err(_) => return,
    };

    for _ in 0..1 {
        let header_len = message::MessageHeader::SIZE;
        let blob_id = Uuid::now_v7();
        let content: Bytes = vec![0xff; 1024 * 1024 - 256].into();
        let mut readback_content = Bytes::new();
        let content_len = content.len();
        let size = rpc_client
            .put_blob(blob_id.clone(), 0, content.clone())
            .await
            .unwrap();
        assert_eq!(header_len + content_len, size);

        let size = rpc_client
            .get_blob(blob_id, 0, &mut readback_content)
            .await
            .unwrap();
        assert_eq!(header_len + content_len, size);
        assert_eq!(content, readback_content);
    }
}

#[tokio::test]
#[traced_test]
async fn test_basic_blob_io_with_random_bytes() {
    let url = "127.0.0.1:9225";
    tracing::debug!(%url);
    // Skip testing if blob storage server is not up
    let stream = match TcpStream::connect(url).await {
        Ok(stream) => stream,
        Err(_) => return,
    };
    let rpc_client = match RpcClientBss::new(stream).await {
        Ok(client) => client,
        Err(_) => return,
    };

    for _ in 0..1 {
        let header_len = message::MessageHeader::SIZE;
        let blob_id = Uuid::now_v7();
        let content = Bytes::from((4096..1024 * 1024 - 256).fake::<String>());
        let mut readback_content = Bytes::new();
        let content_len = content.len();
        let size = rpc_client
            .put_blob(blob_id.clone(), 0, content.clone())
            .await
            .unwrap();
        assert_eq!(header_len + content_len, size);

        let size = rpc_client
            .get_blob(blob_id, 0, &mut readback_content)
            .await
            .unwrap();
        assert_eq!(header_len + content_len, size);
        assert_eq!(content, readback_content);
    }
}
