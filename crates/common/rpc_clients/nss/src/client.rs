use rpc_client_common::AutoReconnectRpcClient;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

const CONNS_PER_CORE: usize = 3;

pub struct RpcClient {
    connections:
        Vec<Arc<AutoReconnectRpcClient<nss_codec::MessageCodec, nss_codec::MessageHeader>>>,
    next_conn: AtomicUsize,
}

impl RpcClient {
    pub fn new_from_address(address: String) -> Self {
        let mut connections = Vec::with_capacity(CONNS_PER_CORE);

        for _ in 0..CONNS_PER_CORE {
            let inner = AutoReconnectRpcClient::new_from_address(address.clone());
            connections.push(Arc::new(inner));
        }

        Self {
            connections,
            next_conn: AtomicUsize::new(0),
        }
    }

    fn get_connection(
        &self,
    ) -> &Arc<AutoReconnectRpcClient<nss_codec::MessageCodec, nss_codec::MessageHeader>> {
        let idx = self.next_conn.fetch_add(1, Ordering::Relaxed) % self.connections.len();
        &self.connections[idx]
    }

    pub fn gen_request_id(&self) -> u32 {
        self.get_connection().gen_request_id()
    }

    pub async fn send_request(
        &self,
        request_id: u32,
        frame: rpc_codec_common::MessageFrame<nss_codec::MessageHeader, bytes::Bytes>,
        timeout: Option<std::time::Duration>,
        trace_id: Option<u64>,
    ) -> Result<rpc_codec_common::MessageFrame<nss_codec::MessageHeader>, rpc_client_common::RpcError>
    {
        self.get_connection()
            .send_request(request_id, frame, timeout, trace_id)
            .await
    }
}
