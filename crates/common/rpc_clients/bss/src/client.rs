use rpc_client_common::{Closeable, RpcClient as GenericRpcClient, RpcError};
use rpc_codec_common::MessageFrame;
use slotmap_conn_pool::Poolable;
use std::time::Duration;
use tokio::net::TcpStream;

// Create a wrapper struct to avoid orphan rule issues
#[derive(Clone)]
pub struct RpcClient {
    inner: GenericRpcClient<bss_codec::MessageCodec, bss_codec::MessageHeader>,
}

impl RpcClient {
    pub async fn new(stream: TcpStream) -> Result<Self, RpcError> {
        let inner = GenericRpcClient::new(stream).await?;
        Ok(RpcClient { inner })
    }

    pub fn gen_request_id(&self) -> u32 {
        self.inner.gen_request_id()
    }

    pub async fn send_request(
        &self,
        request_id: u32,
        frame: MessageFrame<bss_codec::MessageHeader>,
        timeout: Option<Duration>,
    ) -> Result<MessageFrame<bss_codec::MessageHeader>, RpcError> {
        self.inner.send_request(request_id, frame, timeout).await
    }
}

impl RpcClient {
    pub async fn new_from_address(
        address: String,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        Self::new_internal(address, None).await
    }

    async fn new_internal(
        address: String,
        session_id: Option<u64>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let inner = GenericRpcClient::<bss_codec::MessageCodec, bss_codec::MessageHeader>::establish_connection(
            address,
            session_id,
        )
        .await?;
        Ok(RpcClient { inner })
    }

    pub fn is_closed(&self) -> bool {
        self.inner.is_closed()
    }

    #[allow(dead_code)]
    fn get_session_state(&self) -> (u64, u32) {
        self.inner.get_session_state()
    }
}

impl Closeable for RpcClient {
    fn is_closed(&self) -> bool {
        self.inner.is_closed()
    }
}

impl Poolable for RpcClient {
    type AddrKey = String;
    type Error = Box<dyn std::error::Error + Send + Sync>;

    async fn new(address: Self::AddrKey) -> Result<Self, Self::Error> {
        Self::new_internal(address, None).await
    }

    async fn new_with_session_id(
        address: Self::AddrKey,
        session_id: u64,
    ) -> Result<Self, Self::Error> {
        Self::new_internal(address, Some(session_id)).await
    }

    async fn new_with_session_and_request_id(
        address: Self::AddrKey,
        session_id: u64,
        next_request_id: u32,
    ) -> Result<Self, Self::Error> {
        let inner = GenericRpcClient::establish_connection_with_session_state(
            address,
            session_id,
            next_request_id,
        )
        .await?;
        Ok(RpcClient { inner })
    }

    fn is_closed(&self) -> bool {
        self.inner.is_closed()
    }

    fn get_session_state(&self) -> (u64, u32) {
        self.inner.get_session_state()
    }
}
