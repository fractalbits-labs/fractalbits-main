use rpc_auth::RpcSecret;
use rpc_client_common::AutoReconnectRpcClient;
use std::time::Duration;

pub struct RpcClient {
    inner: AutoReconnectRpcClient<rss_codec::MessageCodec, rss_codec::MessageHeader>,
}

impl RpcClient {
    pub fn new(
        addresses: Vec<String>,
        connection_timeout: Duration,
        rpc_secret: Option<RpcSecret>,
    ) -> Self {
        let inner =
            AutoReconnectRpcClient::new(addresses, connection_timeout, rpc_secret);
        Self { inner }
    }

    pub fn gen_request_id(&self) -> u32 {
        self.inner.gen_request_id()
    }

    pub async fn send_request(
        &self,
        frame: rpc_codec_common::MessageFrame<rss_codec::MessageHeader, bytes::Bytes>,
        timeout: Option<std::time::Duration>,
    ) -> Result<rpc_codec_common::MessageFrame<rss_codec::MessageHeader>, rpc_client_common::RpcError>
    {
        self.inner.send_request(frame, timeout).await
    }
}
