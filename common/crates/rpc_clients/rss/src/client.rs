use rpc_client_common::{RpcClient as GenericRpcClient, RpcError};
use rpc_codec_common::MessageFrame;
use slotmap_conn_pool::Poolable;
use std::time::Duration;
use tokio::net::TcpStream;

// Create a wrapper struct to avoid orphan rule issues
pub struct RpcClient {
    inner: GenericRpcClient<rss_codec::MessageCodec, rss_codec::MessageHeader>,
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
        frame: MessageFrame<rss_codec::MessageHeader>,
        timeout: Option<Duration>,
    ) -> Result<MessageFrame<rss_codec::MessageHeader>, RpcError> {
        self.inner.send_request(request_id, frame, timeout).await
    }
}

impl RpcClient {
    async fn new_internal(
        address: String,
        session_id: Option<u64>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let inner = match session_id {
            Some(id) => {
                <GenericRpcClient<rss_codec::MessageCodec, rss_codec::MessageHeader> as Poolable>::new_with_session_id(
                    address,
                    id,
                )
                .await?
            }
            None => {
                <GenericRpcClient<rss_codec::MessageCodec, rss_codec::MessageHeader> as Poolable>::new(
                    address,
                )
                .await?
            }
        };
        Ok(RpcClient { inner })
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
