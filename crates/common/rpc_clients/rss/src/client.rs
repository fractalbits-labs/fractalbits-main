use rpc_client_common::AutoReconnectRpcClient;

pub struct RpcClient {
    inner: AutoReconnectRpcClient<rss_codec::MessageCodec, rss_codec::MessageHeader>,
}

impl RpcClient {
    pub fn new_from_address(address: String) -> Self {
        let inner = AutoReconnectRpcClient::new_from_address(address);
        Self { inner }
    }

    pub fn gen_request_id(&self) -> u32 {
        self.inner.gen_request_id()
    }

    pub async fn send_request(
        &self,
        request_id: u32,
        frame: rpc_codec_common::MessageFrame<rss_codec::MessageHeader, bytes::Bytes>,
        timeout: Option<std::time::Duration>,
        trace_id: Option<u64>,
    ) -> Result<rpc_codec_common::MessageFrame<rss_codec::MessageHeader>, rpc_client_common::RpcError>
    {
        self.inner
            .send_request(request_id, frame, timeout, trace_id)
            .await
    }
}
