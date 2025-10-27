use crate::stats::{BssNodeStats, get_global_registry};
use rpc_client_common::AutoReconnectRpcClient;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

const CONNS_PER_CORE: usize = 8;

pub struct RpcClient {
    connections:
        Vec<Arc<AutoReconnectRpcClient<bss_codec::MessageCodec, bss_codec::MessageHeader>>>,
    next_conn: AtomicUsize,
    stats: Arc<BssNodeStats>,
}

impl RpcClient {
    pub fn new_from_address(address: String) -> Self {
        let mut connections = Vec::with_capacity(CONNS_PER_CORE);

        for _ in 0..CONNS_PER_CORE {
            let inner = AutoReconnectRpcClient::new_from_address(address.clone());
            connections.push(Arc::new(inner));
        }

        let registry = get_global_registry();
        let stats = registry.register_node(address);

        Self {
            connections,
            next_conn: AtomicUsize::new(0),
            stats,
        }
    }

    fn get_connection(
        &self,
    ) -> &Arc<AutoReconnectRpcClient<bss_codec::MessageCodec, bss_codec::MessageHeader>> {
        let idx = self.next_conn.fetch_add(1, Ordering::Relaxed) % self.connections.len();
        &self.connections[idx]
    }

    pub fn gen_request_id(&self) -> u32 {
        self.get_connection().gen_request_id()
    }

    pub async fn send_request(
        &self,
        request_id: u32,
        frame: rpc_codec_common::MessageFrame<bss_codec::MessageHeader, bytes::Bytes>,
        timeout: Option<std::time::Duration>,
        trace_id: Option<u64>,
        operation: Option<crate::stats::OperationType>,
    ) -> Result<rpc_codec_common::MessageFrame<bss_codec::MessageHeader>, rpc_client_common::RpcError>
    {
        if let Some(op) = operation {
            self.stats.increment(op);
        }

        let result = self
            .get_connection()
            .send_request(request_id, frame, timeout, trace_id)
            .await;

        if let Some(op) = operation {
            self.stats.decrement(op);
        }

        result
    }

    pub async fn send_request_vectored(
        &self,
        request_id: u32,
        frame: rpc_codec_common::MessageFrame<bss_codec::MessageHeader, Vec<bytes::Bytes>>,
        timeout: Option<std::time::Duration>,
        trace_id: Option<u64>,
        operation: Option<crate::stats::OperationType>,
    ) -> Result<rpc_codec_common::MessageFrame<bss_codec::MessageHeader>, rpc_client_common::RpcError>
    {
        if let Some(op) = operation {
            self.stats.increment(op);
        }

        let result = self
            .get_connection()
            .send_request_vectored(request_id, frame, timeout, trace_id)
            .await;

        if let Some(op) = operation {
            self.stats.decrement(op);
        }

        result
    }
}
