use rpc_client_common::AutoReconnectRpcClient;
use std::sync::Arc;

#[derive(Clone)]
pub struct RpcClient {
    inner: Arc<AutoReconnectRpcClient<bss_codec::MessageCodec, bss_codec::MessageHeader>>,
}

impl RpcClient {
    pub fn new_from_address(address: String) -> Self {
        let inner = Arc::new(AutoReconnectRpcClient::<
            bss_codec::MessageCodec,
            bss_codec::MessageHeader,
        >::new_from_address(address));
        RpcClient { inner }
    }

    pub fn gen_request_id(&self) -> u32 {
        self.inner.gen_request_id()
    }

    pub async fn send_request<B: AsRef<[u8]>>(
        &self,
        request_id: u32,
        frame: rpc_codec_common::MessageFrame<bss_codec::MessageHeader, B>,
        timeout: Option<std::time::Duration>,
        trace_id: Option<u64>,
    ) -> Result<rpc_codec_common::MessageFrame<bss_codec::MessageHeader>, rpc_client_common::RpcError>
    {
        self.inner
            .send_request(request_id, frame, timeout, trace_id)
            .await
    }
}
