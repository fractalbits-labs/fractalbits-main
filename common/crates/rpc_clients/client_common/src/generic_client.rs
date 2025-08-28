use bytes::BytesMut;
use metrics::{counter, gauge};
use parking_lot::Mutex;
use rpc_codec_common::{MessageFrame, MessageHeaderTrait};
use slotmap_conn_pool::Poolable;
use socket2::{Socket, TcpKeepalive};
use std::collections::HashMap;
use std::io;
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::os::fd::RawFd;
use std::os::unix::io::AsRawFd;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;
use strum::AsRefStr;
use tokio::io::AsyncWriteExt;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tokio::sync::{
    mpsc::{self, Receiver, Sender},
    oneshot,
};
use tokio::task::JoinSet;
use tokio_retry::{
    strategy::{jitter, FixedInterval},
    Retry,
};
use tokio_stream::StreamExt;
use tokio_util::codec::FramedRead;
use tracing::{debug, warn};

use crate::RpcError;

type RequestMap<Header> = Arc<Mutex<HashMap<u32, oneshot::Sender<MessageFrame<Header>>>>>;

pub trait RpcCodec<Header: MessageHeaderTrait>:
    Default
    + tokio_util::codec::Decoder<Item = MessageFrame<Header>, Error = io::Error>
    + Clone
    + Send
    + Sync
    + 'static
{
    const RPC_TYPE: &'static str;
}

pub struct RpcClient<Codec, Header: MessageHeaderTrait> {
    requests: RequestMap<Header>,
    sender: Sender<MessageFrame<Header>>,
    next_id: AtomicU32,
    #[allow(unused)]
    tasks: JoinSet<()>,
    socket_fd: RawFd,
    is_closed: Arc<AtomicBool>,
    _phantom: PhantomData<Codec>,
}

#[derive(AsRefStr)]
#[strum(serialize_all = "snake_case")]
enum DrainFrom {
    SendTask,
    ReceiveTask,
}

impl<Codec, Header> RpcClient<Codec, Header>
where
    Codec: RpcCodec<Header>,
    Header: MessageHeaderTrait + Clone + Send + Sync + 'static,
{
    async fn resolve_address(addr_str: &str) -> Result<SocketAddr, io::Error> {
        // Try to parse as SocketAddr first (for backward compatibility with IP addresses)
        if let Ok(socket_addr) = addr_str.parse::<SocketAddr>() {
            return Ok(socket_addr);
        }

        // Use tokio's native async DNS resolution
        let mut addrs = tokio::net::lookup_host(addr_str).await?;
        addrs.next().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("No addresses found for '{addr_str}'"),
            )
        })
    }

    pub async fn new(stream: TcpStream) -> Result<Self, RpcError> {
        let rpc_type = Codec::RPC_TYPE;
        let socket_fd = stream.as_raw_fd();
        let (reader, writer) = stream.into_split();
        let requests: Arc<Mutex<HashMap<u32, oneshot::Sender<MessageFrame<Header>>>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let (sender, receiver) = mpsc::channel::<MessageFrame<Header>>(1024);
        let is_closed = Arc::new(AtomicBool::new(false));

        let mut tasks = JoinSet::new();

        // Send task
        {
            let sender_requests = requests.clone();
            let sender_is_closed = is_closed.clone();
            tasks.spawn(async move {
                if let Err(e) = Self::send_task(
                    writer,
                    receiver,
                    &sender_requests,
                    socket_fd,
                    &sender_is_closed,
                    rpc_type,
                )
                .await
                {
                    warn!(%socket_fd, %e, "send task failed");
                    sender_is_closed.store(true, Ordering::SeqCst);
                    Self::drain_requests(&sender_requests, DrainFrom::SendTask);
                }
            });
        }

        // Receive task
        {
            let receiver_requests = requests.clone();
            let receiver_is_closed = is_closed.clone();
            tasks.spawn(async move {
                if let Err(e) =
                    Self::receive_task(reader, &receiver_requests, socket_fd, rpc_type).await
                {
                    warn!(%socket_fd, %e, "receive task failed");
                    receiver_is_closed.store(true, Ordering::SeqCst);
                    Self::drain_requests(&receiver_requests, DrainFrom::ReceiveTask);
                }
            });
        }

        Ok(RpcClient {
            requests,
            sender,
            next_id: AtomicU32::new(1),
            tasks,
            socket_fd,
            is_closed,
            _phantom: PhantomData,
        })
    }

    async fn send_task(
        mut writer: OwnedWriteHalf,
        mut receiver: Receiver<MessageFrame<Header>>,
        requests: &RequestMap<Header>,
        socket_fd: RawFd,
        is_closed: &Arc<AtomicBool>,
        rpc_type: &'static str,
    ) -> Result<(), RpcError> {
        while let Some(frame) = receiver.recv().await {
            let request_id = frame.header.get_id();
            debug!(%socket_fd, %request_id, "sending request:");
            let mut buf = BytesMut::new();
            // Encode header first
            frame.header.encode(&mut buf);
            // Then append body
            buf.extend_from_slice(&frame.body);
            writer.write_all(&buf).await.map_err(RpcError::IoError)?;
            counter!("rpc_request_sent", "type" => rpc_type, "name" => "all").increment(1);
        }
        is_closed.store(true, Ordering::SeqCst);
        Self::drain_requests(requests, DrainFrom::SendTask);
        warn!(%socket_fd, "sender closed, send message task quit");
        Ok(())
    }

    async fn receive_task(
        receiver: OwnedReadHalf,
        requests: &RequestMap<Header>,
        socket_fd: RawFd,
        rpc_type: &'static str,
    ) -> Result<(), RpcError> {
        let decoder = Codec::default();
        let mut reader = FramedRead::new(receiver, decoder);
        while let Some(frame) = reader.next().await {
            let frame = frame?;
            let request_id = frame.header.get_id();
            debug!(%socket_fd, %request_id, "receiving response:");
            counter!("rpc_response_received", "type" => rpc_type, "name" => "all").increment(1);
            let tx: oneshot::Sender<MessageFrame<Header>> =
                match requests.lock().remove(&request_id) {
                    Some(tx) => tx,
                    None => {
                        warn!(%socket_fd, %request_id,
                            "received {rpc_type} rpc message with id not in the resp_map");
                        continue;
                    }
                };
            gauge!("rpc_request_pending_in_resp_map", "type" => rpc_type).decrement(1.0);
            if tx.send(frame).is_err() {
                warn!(%socket_fd, %request_id, "oneshot response send failed");
            }
        }
        warn!(%socket_fd, "connection closed, receive message task quit");
        Ok(())
    }

    fn drain_requests(requests: &RequestMap<Header>, drain_from: DrainFrom) {
        let drained_requests = std::mem::take(&mut *requests.lock());
        for (request_id, _tx) in drained_requests {
            debug!(%request_id, drain_from = %drain_from.as_ref(), "dropping pending request");
        }
    }

    pub async fn send_message(
        &self,
        mut frame: MessageFrame<Header>,
    ) -> Result<MessageFrame<Header>, RpcError> {
        if self.is_closed.load(Ordering::SeqCst) {
            return Err(RpcError::InternalRequestError(
                "Connection is closed".into(),
            ));
        }

        let request_id = self.next_id.fetch_add(1, Ordering::SeqCst);
        frame.header.set_id(request_id);

        let (tx, rx) = oneshot::channel();
        {
            self.requests.lock().insert(request_id, tx);
        }
        gauge!("rpc_request_pending_in_resp_map", "type" => Codec::RPC_TYPE).increment(1.0);

        self.sender
            .send(frame)
            .await
            .map_err(|e| RpcError::InternalRequestError(e.to_string()))?;

        let response = rx
            .await
            .map_err(|e| RpcError::InternalResponseError(e.to_string()))?;

        Ok(response)
    }

    pub fn gen_request_id(&self) -> u32 {
        self.next_id.fetch_add(1, Ordering::SeqCst)
    }

    pub async fn send_request(
        &self,
        request_id: u32,
        frame: MessageFrame<Header>,
        timeout: Option<std::time::Duration>,
    ) -> Result<MessageFrame<Header>, RpcError> {
        if self.is_closed.load(Ordering::SeqCst) {
            return Err(RpcError::InternalRequestError(
                "Connection is closed".into(),
            ));
        }

        let (tx, rx) = oneshot::channel();
        {
            self.requests.lock().insert(request_id, tx);
        }
        gauge!("rpc_request_pending_in_resp_map", "type" => Codec::RPC_TYPE).increment(1.0);

        self.sender
            .send(frame)
            .await
            .map_err(|e| RpcError::InternalRequestError(e.to_string()))?;

        let result = match timeout {
            None => rx.await,
            Some(rpc_timeout) => match tokio::time::timeout(rpc_timeout, rx).await {
                Ok(result) => result,
                Err(_) => {
                    warn!(socket_fd=%self.socket_fd, %request_id, "{} rpc request timeout", Codec::RPC_TYPE);
                    return Err(RpcError::InternalRequestError("timeout".into()));
                }
            },
        };
        result.map_err(|e| RpcError::InternalResponseError(e.to_string()))
    }

    pub fn is_closed(&self) -> bool {
        self.is_closed.load(Ordering::SeqCst)
    }
}

impl<Codec: RpcCodec<Header> + Unpin, Header: MessageHeaderTrait> Poolable
    for RpcClient<Codec, Header>
{
    type AddrKey = String;
    type Error = Box<dyn std::error::Error + Send + Sync>;

    async fn new(addr_key: Self::AddrKey) -> Result<Self, Self::Error> {
        const MAX_CONNECTION_RETRIES: usize = 100 * 3600; // Max attempts to connect to an RPC server

        let start = std::time::Instant::now();
        let retry_strategy = FixedInterval::from_millis(10)
            .map(jitter)
            .take(MAX_CONNECTION_RETRIES);

        let stream = Retry::spawn(retry_strategy, || async {
            // Resolve DNS name to socket address for each connection attempt
            let socket_addr = Self::resolve_address(&addr_key).await?;
            TcpStream::connect(socket_addr).await
        })
        .await
        .map_err(|e| {
            warn!(rpc_type = Codec::RPC_TYPE, addr = %addr_key, error = %e, "failed to connect RPC server");
            Box::new(e) as Self::Error
        })?;

        let connect_duration = start.elapsed();
        if connect_duration > Duration::from_secs(1) {
            warn!(
                rpc_type = Codec::RPC_TYPE,
                addr = %addr_key,
                duration_ms = %connect_duration.as_millis(),
                "Slow connection establishment to RPC server"
            );
        } else if connect_duration > Duration::from_millis(100) {
            debug!(
                rpc_type = Codec::RPC_TYPE,
                addr = %addr_key,
                duration_ms = %connect_duration.as_millis(),
                "Connection established to RPC server"
            );
        }

        // Configure socket
        let std_stream = stream.into_std().map_err(|e| Box::new(e) as Self::Error)?;
        let socket = Socket::from(std_stream);

        // Set 16MB buffers for data-intensive operations
        socket
            .set_recv_buffer_size(16 * 1024 * 1024)
            .map_err(|e| Box::new(e) as Self::Error)?;
        socket
            .set_send_buffer_size(16 * 1024 * 1024)
            .map_err(|e| Box::new(e) as Self::Error)?;

        // Configure aggressive keepalive for internal network
        let keepalive = TcpKeepalive::new()
            .with_time(Duration::from_secs(5))
            .with_interval(Duration::from_secs(2))
            .with_retries(2);
        socket
            .set_tcp_keepalive(&keepalive)
            .map_err(|e| Box::new(e) as Self::Error)?;

        // Set TCP_NODELAY for low latency
        socket
            .set_nodelay(true)
            .map_err(|e| Box::new(e) as Self::Error)?;

        // Convert back to tokio TcpStream
        let std_stream: std::net::TcpStream = socket.into();
        std_stream
            .set_nonblocking(true)
            .map_err(|e| Box::new(e) as Self::Error)?;
        let stream = TcpStream::from_std(std_stream).map_err(|e| Box::new(e) as Self::Error)?;

        Self::new(stream)
            .await
            .map_err(|e| Box::new(e) as Self::Error)
    }

    fn is_closed(&self) -> bool {
        self.is_closed()
    }
}
