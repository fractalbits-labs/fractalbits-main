use bytes::{Bytes, BytesMut};
use metrics::{counter, gauge, histogram, Gauge};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::io::{self};
use std::net::SocketAddr;
use std::os::fd::RawFd;
use std::os::unix::io::AsRawFd;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
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
        oneshot,
    },
    task::JoinSet,
};
use tokio_retry::strategy::jitter;
use tokio_stream::StreamExt;
use tokio_util::codec::FramedRead;

use crate::codec::{MessageFrame, MesssageCodec};
use crate::message::MessageHeader;
use rpc_client_common::ErrorRetryable;
use slotmap_conn_pool::Poolable;
use strum::AsRefStr;
use tokio_retry::{strategy::FixedInterval, Retry};
use tracing::{debug, error, warn};

#[cfg(feature = "nss")]
const RPC_TYPE: &str = "nss";
#[cfg(feature = "bss")]
const RPC_TYPE: &str = "bss";
#[cfg(feature = "rss")]
const RPC_TYPE: &str = "rss";

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
    #[error("Entry already exists")]
    AlreadyExists,
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

impl ErrorRetryable for RpcError {
    fn retryable(&self) -> bool {
        matches!(self, RpcError::OneshotRecvError(_))
    }
}

pub enum Message {
    Frame(MessageFrame),
    Bytes(Bytes),
}

pub struct RpcClient {
    requests: Arc<Mutex<HashMap<u32, oneshot::Sender<MessageFrame>>>>,
    sender: Sender<Message>,
    next_id: AtomicU32,
    #[allow(unused)]
    tasks: JoinSet<()>, // Use JoinSet to manage background tasks
    socket_fd: RawFd,
    is_closed: Arc<AtomicBool>,
}

impl Drop for RpcClient {
    fn drop(&mut self) {
        // Just try to make metrics (`rpc_request_pending_in_resp_map`) accurate
        Self::drain_pending_requests(self.socket_fd, &self.requests, DrainFrom::RpcClient);
    }
}

impl RpcClient {
    const MAX_CONNECTION_RETRIES: usize = 100 * 3600; // Max attempts to connect to an RPC server

    pub async fn new(stream: TcpStream) -> Result<Self, RpcError> {
        stream.set_nodelay(true)?;
        let socket_fd = stream.as_raw_fd();
        let (receiver, sender) = stream.into_split();

        let requests = Arc::new(Mutex::new(HashMap::new()));
        let is_closed = Arc::new(AtomicBool::new(false));

        let mut tasks = JoinSet::new();

        // Start message receiver task, for rpc responses
        tasks.spawn({
            let requests_clone = requests.clone();
            let is_closed_clone = is_closed.clone();
            async move {
                if let Err(e) =
                    Self::receive_message_task(socket_fd, receiver, &requests_clone).await
                {
                    warn!(%socket_fd, error=%e, "receive message task quit");
                }
                is_closed_clone.store(true, Ordering::SeqCst);
                Self::drain_pending_requests(socket_fd, &requests_clone, DrainFrom::ReceiveTask);
            }
        });

        // Start message sender task, to send rpc requests. We are launching a dedicated task here
        // to reduce lock contention on the sender socket itself.
        let (tx, rx) = mpsc::channel(1024 * 1024);
        tasks.spawn({
            let is_closed_clone = is_closed.clone();
            let requests_clone = requests.clone();
            async move {
                if let Err(e) = Self::send_message_task(socket_fd, sender, rx).await {
                    warn!(%socket_fd, error=%e, "send message task quit");
                }
                is_closed_clone.store(true, Ordering::SeqCst);
                Self::drain_pending_requests(socket_fd, &requests_clone, DrainFrom::SendTask);
            }
        });

        Ok(Self {
            requests,
            sender: tx,
            next_id: AtomicU32::new(1),
            tasks,
            socket_fd,
            is_closed,
        })
    }

    async fn receive_message_task(
        socket_fd: RawFd,
        receiver: OwnedReadHalf,
        requests: &Arc<Mutex<HashMap<u32, oneshot::Sender<MessageFrame>>>>,
    ) -> Result<(), RpcError> {
        let decoder = MesssageCodec::default();
        let mut reader = FramedRead::new(receiver, decoder);
        while let Some(frame) = reader.next().await {
            let frame = frame?;
            let request_id = frame.header.id;
            debug!(%socket_fd, %request_id, "receiving response:");
            counter!("rpc_response_received", "type" => RPC_TYPE, "name" => "all").increment(1);
            let tx: oneshot::Sender<MessageFrame> = match requests.lock().remove(&frame.header.id) {
                Some(tx) => tx,
                None => {
                    warn!(%socket_fd, request_id=frame.header.id,
                            "received {RPC_TYPE} rpc message with id not in the resp_map");
                    continue;
                }
            };
            gauge!("rpc_request_pending_in_resp_map", "type" => RPC_TYPE).decrement(1.0);
            if tx.send(frame).is_err() {
                warn!(%socket_fd, %request_id, "oneshot response send failed");
            }
        }
        warn!(%socket_fd, "connection closed, receive message task quit");
        Ok(())
    }

    async fn send_message_task(
        socket_fd: RawFd,
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
        warn!(%socket_fd, "connection closed, send_message_task quit");
        Ok(())
    }

    pub async fn send_request(
        &self,
        request_id: u32,
        msg: Message,
    ) -> Result<MessageFrame, RpcError> {
        let (tx, rx) = oneshot::channel();
        assert!(self.requests.lock().insert(request_id, tx).is_none());
        gauge!("rpc_request_pending_in_resp_map", "type" => RPC_TYPE).increment(1.0);

        self.sender.send(msg).await?;
        gauge!("rpc_request_pending_in_send_queue", "type" => RPC_TYPE).increment(1.0);
        debug!(%request_id, "request sent from handler:");

        let timeout_val = std::time::Duration::from_secs(110); // TODO: align with s3 handler setting
        let result = tokio::time::timeout(timeout_val, rx).await;
        let result = match result {
            Ok(result) => result,
            Err(_) => {
                warn!(socket_fd=%self.socket_fd, %request_id, "{RPC_TYPE} rpc request timeout");
                return Err(RpcError::InternalResponseError("timeout".into()));
            }
        };
        result.map_err(RpcError::OneshotRecvError)
    }

    pub fn gen_request_id(&self) -> u32 {
        self.next_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    fn drain_pending_requests(
        socket_fd: RawFd,
        requests: &Arc<Mutex<HashMap<u32, oneshot::Sender<MessageFrame>>>>,
        drain_from: DrainFrom,
    ) {
        let mut requests = requests.lock();
        let pending_count = requests.len();
        if pending_count > 0 {
            warn!(
                %socket_fd,
                "draining {pending_count} pending requests from {} on connection close",
                drain_from.as_ref()
            );
            gauge!("rpc_request_pending_in_resp_map", "type" => RPC_TYPE)
                .decrement(pending_count as f64);
            requests.clear(); // This drops the senders, notifying receivers of an error.
        }
    }
}

#[derive(AsRefStr)]
#[strum(serialize_all = "snake_case")]
enum DrainFrom {
    SendTask,
    ReceiveTask,
    RpcClient,
}

impl Poolable for RpcClient {
    type AddrKey = SocketAddr;
    type Error = Box<dyn std::error::Error + Send + Sync>; // Using Box<dyn Error> for simplicity

    async fn new(addr_key: Self::AddrKey) -> Result<Self, Self::Error> {
        let retry_strategy = FixedInterval::from_millis(10)
            .map(jitter)
            .take(Self::MAX_CONNECTION_RETRIES);

        let stream = Retry::spawn(retry_strategy, || async {
            TcpStream::connect(addr_key).await
        })
        .await
        .map_err(|e| {
            warn!(rpc_type=RPC_TYPE, %addr_key, error=%e, "failed to connect RPC server");
            Box::new(e) as Self::Error
        })?;

        RpcClient::new(stream)
            .await
            .map_err(|e| Box::new(e) as Self::Error)
    }

    fn is_closed(&self) -> bool {
        self.is_closed.load(Ordering::SeqCst)
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
