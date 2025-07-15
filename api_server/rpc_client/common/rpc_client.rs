use bytes::{Bytes, BytesMut};
use metrics::{counter, gauge, histogram, Gauge};
use std::collections::HashMap;
use std::io::{self};
use std::net::SocketAddr;
use std::sync::atomic::AtomicU32;
use std::sync::Arc;
use std::time::Instant;
use thiserror::Error;
use tokio::{
    self,
    io::AsyncWriteExt,
    net::tcp::{OwnedReadHalf, OwnedWriteHalf},
    net::TcpStream,
    sync::{
        mpsc::{self, Receiver, Sender},
        oneshot, RwLock,
    },
    task::JoinHandle,
};
use tokio_stream::StreamExt;
use tokio_util::codec::FramedRead;

use crate::codec::{MessageFrame, MesssageCodec};
use crate::message::MessageHeader;
use slotmap_conn_pool::Poolable;
use tokio_retry::{strategy::ExponentialBackoff, Retry};
use tracing::{debug, error, warn};

#[cfg(feature = "nss")]
const RPC_TYPE: &'static str = "nss";
#[cfg(feature = "bss")]
const RPC_TYPE: &'static str = "bss";
#[cfg(feature = "rss")]
const RPC_TYPE: &'static str = "rss";

const MAX_CONNECTION_RETRIES: usize = 5; // Max attempts to connect to an RPC server

#[derive(Error, Debug)]
pub enum RpcError {
    #[error(transparent)]
    IoError(#[from] io::Error),
    #[error(transparent)]
    OneshotRecvError(oneshot::error::RecvError),
    #[cfg(any(feature = "nss", feature = "rss"))]
    #[error(transparent)]
    EncodeError(prost::EncodeError),
    #[cfg(any(feature = "nss", feature = "rss"))]
    #[error(transparent)]
    DecodeError(prost::DecodeError),
    #[error("Internal request sending error: {0}")]
    InternalRequestError(String),
    #[error("Internal response error: {0}")]
    InternalResponseError(String),
    #[error("Entry not found")]
    NotFound,
    #[error("Send error: {0}")]
    SendError(String),
    #[cfg(feature = "rss")] // for rss txn api
    #[error("Retry")]
    Retry,
}

impl<T> From<mpsc::error::SendError<T>> for RpcError {
    fn from(e: mpsc::error::SendError<T>) -> Self {
        RpcError::SendError(e.to_string())
    }
}

pub enum Message {
    Frame(MessageFrame),
    Bytes(Bytes),
}

pub struct RpcClient {
    requests: Arc<RwLock<HashMap<u32, oneshot::Sender<MessageFrame>>>>,
    sender: Sender<Message>,
    next_id: AtomicU32,
    send_task: JoinHandle<()>,
    recv_task: JoinHandle<()>,
}

impl RpcClient {
    pub async fn new(stream: TcpStream) -> Result<Self, RpcError> {
        stream.set_nodelay(true)?;
        let (receiver, sender) = stream.into_split();

        // Start message receiver task, for rpc responses
        let requests = Arc::new(RwLock::new(HashMap::new()));
        let recv_task = {
            let requests_clone = requests.clone();
            tokio::spawn(async move {
                if let Err(e) = Self::receive_message_task(receiver, requests_clone).await {
                    error!("FATAL: receive message task error: {e}");
                }
            })
        };

        // Start message sender task, to send rpc requests. We are launching a dedicated task here
        // to reduce lock contention on the sender socket itself.
        let (tx, rx) = mpsc::channel(1024 * 1024);
        let send_task = {
            tokio::spawn(async move {
                if let Err(e) = Self::send_message_task(sender, rx).await {
                    error!("FATAL: send message task error: {e}");
                }
            })
        };

        Ok(Self {
            requests,
            sender: tx,
            next_id: AtomicU32::new(1),
            send_task,
            recv_task,
        })
    }

    async fn receive_message_task(
        receiver: OwnedReadHalf,
        requests: Arc<RwLock<HashMap<u32, oneshot::Sender<MessageFrame>>>>,
    ) -> Result<(), RpcError> {
        let decoder = MesssageCodec::default();
        let mut reader = FramedRead::new(receiver, decoder);
        while let Some(frame) = reader.next().await {
            let frame = frame?;
            let request_id = frame.header.id;
            debug!(%request_id, "receiving response:");
            counter!("rpc_response_received", "type" => RPC_TYPE, "name" => "all").increment(1);
            let tx: oneshot::Sender<MessageFrame> =
                match requests.write().await.remove(&frame.header.id) {
                    Some(tx) => tx,
                    None => continue, // we may have received the response already
                };
            gauge!("rpc_request_pending_in_resp_map", "type" => RPC_TYPE).decrement(1.0);
            if tx.send(frame).is_err() {
                warn!(%request_id, "oneshot response send failed");
            }
        }
        warn!("connection closed, receive_message_task quit");
        Ok(())
    }

    async fn send_message_task(
        mut sender: OwnedWriteHalf,
        mut input: Receiver<Message>,
    ) -> Result<(), RpcError> {
        while let Some(message) = input.recv().await {
            gauge!("rpc_request_pending_in_send_queue", "type" => RPC_TYPE).decrement(1.0);
            match message {
                Message::Bytes(mut bytes) => {
                    sender.write_all_buf(&mut bytes).await?;
                }
                Message::Frame(mut frame) => {
                    let mut header_bytes = BytesMut::with_capacity(MessageHeader::SIZE);
                    frame.header.encode(&mut header_bytes);
                    sender.write_all_buf(&mut header_bytes).await?;
                    if !frame.body.is_empty() {
                        sender.write_all_buf(&mut frame.body).await?;
                    }
                }
            }
            sender.flush().await?;
            counter!("rpc_request_sent", "type" => RPC_TYPE, "name" => "all").increment(1);
        }
        Ok(())
    }

    pub async fn send_request(
        &self,
        request_id: u32,
        msg: Message,
    ) -> Result<MessageFrame, RpcError> {
        self.sender.send(msg).await?;
        gauge!("rpc_request_pending_in_send_queue", "type" => RPC_TYPE).increment(1.0);
        debug!(%request_id, "request sent from handler:");

        let (tx, rx) = oneshot::channel();
        self.requests.write().await.insert(request_id, tx);
        gauge!("rpc_request_pending_in_resp_map", "type" => RPC_TYPE).increment(1.0);
        let timeout_val = std::time::Duration::from_secs(4); // TODO: align with s3 handler setting
        let result = tokio::time::timeout(timeout_val, rx).await;
        let result = match result {
            Ok(result) => result,
            Err(_) => {
                warn!(%request_id, "rpc request timeout");
                return Err(RpcError::InternalResponseError("timeout".into()));
            }
        };
        result.map_err(RpcError::OneshotRecvError)
    }

    pub fn gen_request_id(&self) -> u32 {
        self.next_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    pub fn tasks_running(&self) -> bool {
        !self.send_task.is_finished() && !self.recv_task.is_finished()
    }
}

impl Poolable for RpcClient {
    type AddrKey = SocketAddr;
    type Error = Box<dyn std::error::Error + Send + Sync>; // Using Box<dyn Error> for simplicity

    async fn new(addr_key: Self::AddrKey) -> Result<Self, Self::Error> {
        let retry_strategy = ExponentialBackoff::from_millis(100) // Initial delay
            .factor(2) // Doubles the delay each time
            .take(MAX_CONNECTION_RETRIES); // Max retries for a single connection attempt

        let stream = Retry::spawn(retry_strategy, move || async move {
            TcpStream::connect(addr_key).await.map_err(|e| {
                warn!(rpc_type=RPC_TYPE, %addr_key, error=%e, "failed to connect RPC server");
                e
            })
        })
        .await
        .map_err(|e| Box::new(e) as Self::Error)?;

        RpcClient::new(stream)
            .await
            .map_err(|e| Box::new(e) as Self::Error)
    }

    fn is_open(&self) -> bool {
        self.tasks_running()
    }
}

pub struct InflightRpcGuard {
    start: Instant,
    gauge: Gauge,
    rpc_type: &'static str,
    rpc_name: &'static str,
}

impl InflightRpcGuard {
    pub fn new(rpc_type: &'static str, rpc_name: &'static str) -> Self {
        let gauge = gauge!("inflight_rpc", "type" => rpc_type, "name" => rpc_name);
        gauge.increment(1.0);
        counter!("rpc_request_sent", "type" => rpc_type, "name" => rpc_name).increment(1);

        Self {
            start: Instant::now(),
            gauge,
            rpc_type,
            rpc_name,
        }
    }
}

impl Drop for InflightRpcGuard {
    fn drop(&mut self) {
        histogram!("rpc_duration_nanos", "type" => self.rpc_type, "name" => self.rpc_name)
            .record(self.start.elapsed().as_nanos() as f64);
        self.gauge.decrement(1.0);
    }
}
