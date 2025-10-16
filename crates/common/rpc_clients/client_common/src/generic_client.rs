#[cfg(feature = "io_uring")]
use crate::io_uring;
use bytes::BytesMut;
use metrics::{counter, gauge};
use parking_lot::Mutex;
use rpc_codec_common::{MessageFrame, MessageHeaderTrait};
use socket2::{Socket, TcpKeepalive};
use std::collections::HashMap;
use std::io;
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::os::fd::RawFd;
use std::os::unix::io::AsRawFd;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::Duration;
use strum::AsRefStr;
#[cfg(not(feature = "io_uring"))]
use tokio::io::AsyncWriteExt;
#[cfg(not(feature = "io_uring"))]
use tokio::net::TcpStream;
#[cfg(not(feature = "io_uring"))]
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::{
    mpsc::{self, Receiver, Sender},
    oneshot,
};
use tokio::task::JoinSet;
use tokio_retry::{
    Retry,
    strategy::{FixedInterval, jitter},
};
use tracing::{debug, warn};

#[cfg(not(feature = "io_uring"))]
use tokio_stream::StreamExt;
#[cfg(not(feature = "io_uring"))]
use tokio_util::codec::FramedRead;

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

#[derive(Clone)]
pub struct RpcClient<Codec: RpcCodec<Header>, Header: MessageHeaderTrait> {
    requests: RequestMap<Header>,
    sender: Sender<MessageFrame<Header>>,
    next_id: Arc<AtomicU32>,
    #[allow(unused)]
    tasks: Arc<parking_lot::Mutex<JoinSet<()>>>,
    socket_fd: RawFd,
    is_closed: Arc<AtomicBool>,
    #[cfg(feature = "io_uring")]
    #[allow(dead_code)]
    socket_owner: Arc<Socket>,
    _phantom: PhantomData<Codec>,
}

#[derive(AsRefStr)]
#[strum(serialize_all = "snake_case")]
enum DrainFrom {
    SendTask,
    ReceiveTask,
    RpcClient,
}

impl<Codec: RpcCodec<Header>, Header: MessageHeaderTrait> Drop for RpcClient<Codec, Header> {
    fn drop(&mut self) {
        if Arc::strong_count(&self.tasks) == 1 {
            debug!(rpc_type = Codec::RPC_TYPE, socket_fd = %self.socket_fd, "Last RpcClient clone dropped, aborting tasks");
            if let Some(mut tasks) = self.tasks.try_lock() {
                tasks.abort_all();
            }
            Self::drain_pending_requests(self.socket_fd, &self.requests, DrainFrom::RpcClient);
        }
    }
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

    #[cfg(feature = "io_uring")]
    async fn create_raw_socket_io_uring(addr: SocketAddr) -> Result<(RawFd, Socket), io::Error> {
        use socket2::{Domain, Protocol, Type};

        let domain = Domain::for_address(addr);
        let socket = Socket::new(domain, Type::STREAM, Some(Protocol::TCP))?;

        Self::configure_tcp_socket(&socket)?;

        match socket.connect(&addr.into()) {
            Ok(_) => {}
            Err(e) if e.raw_os_error() == Some(libc::EINPROGRESS) => {
                // EINPROGRESS is expected for non-blocking connect
            }
            Err(e) => return Err(e),
        }

        let fd = socket.as_raw_fd();
        Ok((fd, socket))
    }

    #[cfg(not(feature = "io_uring"))]
    async fn new_internal(stream: TcpStream) -> Result<Self, RpcError> {
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
            let is_closed = is_closed.clone();
            tasks.spawn(async move {
                if let Err(e) = Self::send_task(writer, receiver, socket_fd, rpc_type).await {
                    warn!(%rpc_type, %socket_fd, %e, "send task failed");
                }
                is_closed.store(true, Ordering::SeqCst);
                Self::drain_pending_requests(socket_fd, &sender_requests, DrainFrom::SendTask);
            });
        }

        // Receive task
        {
            let receiver_requests = requests.clone();
            let is_closed = is_closed.clone();
            tasks.spawn(async move {
                if let Err(e) =
                    Self::receive_task(reader, receiver_requests.clone(), socket_fd, rpc_type).await
                {
                    warn!(%rpc_type, %socket_fd, %e, "receive task failed");
                }
                is_closed.store(true, Ordering::SeqCst);
                Self::drain_pending_requests(socket_fd, &receiver_requests, DrainFrom::ReceiveTask);
            });
        }

        debug!(%rpc_type, %socket_fd, "Creating RPC client");

        Ok(RpcClient {
            requests,
            sender,
            next_id: Arc::new(AtomicU32::new(1)),
            tasks: Arc::new(parking_lot::Mutex::new(tasks)),
            socket_fd,
            is_closed,
            _phantom: PhantomData,
        })
    }

    #[cfg(feature = "io_uring")]
    async fn new_internal_io_uring(addr: SocketAddr) -> Result<Self, RpcError> {
        let rpc_type = Codec::RPC_TYPE;
        let (socket_fd, socket) = Self::create_raw_socket_io_uring(addr)
            .await
            .map_err(RpcError::IoError)?;

        let requests: Arc<Mutex<HashMap<u32, oneshot::Sender<MessageFrame<Header>>>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let (sender, receiver) = mpsc::channel::<MessageFrame<Header>>(1024);
        let is_closed = Arc::new(AtomicBool::new(false));

        let mut tasks = JoinSet::new();

        // Send task
        {
            let sender_requests = requests.clone();
            let is_closed = is_closed.clone();
            tasks.spawn(async move {
                if let Err(e) = Self::send_task_io_uring(receiver, socket_fd, rpc_type).await {
                    warn!(%rpc_type, %socket_fd, %e, "send task failed");
                }
                is_closed.store(true, Ordering::SeqCst);
                Self::drain_pending_requests(socket_fd, &sender_requests, DrainFrom::SendTask);
            });
        }

        // Receive task
        {
            let receiver_requests = requests.clone();
            let is_closed = is_closed.clone();
            tasks.spawn(async move {
                if let Err(e) =
                    Self::receive_task_io_uring(socket_fd, &receiver_requests, rpc_type).await
                {
                    warn!(%rpc_type, %socket_fd, %e, "receive task failed");
                }
                is_closed.store(true, Ordering::SeqCst);
                Self::drain_pending_requests(socket_fd, &receiver_requests, DrainFrom::ReceiveTask);
            });
        }

        debug!(%rpc_type, %socket_fd, "Creating RPC client (io_uring)");

        Ok(RpcClient {
            requests,
            sender,
            next_id: Arc::new(AtomicU32::new(1)),
            tasks: Arc::new(parking_lot::Mutex::new(tasks)),
            socket_fd,
            is_closed,
            socket_owner: Arc::new(socket),
            _phantom: PhantomData,
        })
    }

    #[cfg(not(feature = "io_uring"))]
    async fn send_task(
        writer: OwnedWriteHalf,
        receiver: Receiver<MessageFrame<Header>>,
        socket_fd: RawFd,
        rpc_type: &'static str,
    ) -> Result<(), RpcError> {
        Self::send_task_tokio(writer, receiver, socket_fd, rpc_type).await
    }

    #[cfg(not(feature = "io_uring"))]
    async fn send_task_tokio(
        mut writer: OwnedWriteHalf,
        mut receiver: Receiver<MessageFrame<Header>>,
        socket_fd: RawFd,
        rpc_type: &'static str,
    ) -> Result<(), RpcError> {
        let mut header_buf = BytesMut::with_capacity(Header::SIZE);
        while let Some(frame) = receiver.recv().await {
            gauge!("rpc_request_pending_in_send_queue", "type" => rpc_type).decrement(1.0);
            let MessageFrame { header, body } = frame;
            let request_id = frame.header.get_id();
            debug!(%rpc_type, %socket_fd, %request_id, "sending request:");

            header_buf.clear();
            header.encode(&mut header_buf);

            writer
                .write_all(header_buf.as_ref())
                .await
                .map_err(RpcError::IoError)?;

            if !body.is_empty() {
                writer
                    .write_all(body.as_ref())
                    .await
                    .map_err(RpcError::IoError)?;
            }
            writer.flush().await?;
            counter!("rpc_request_sent", "type" => rpc_type, "name" => "all").increment(1);
        }
        warn!(%rpc_type, %socket_fd, "sender closed, send message task quit");
        Ok(())
    }

    #[cfg(feature = "io_uring")]
    async fn send_task_io_uring(
        mut receiver: Receiver<MessageFrame<Header>>,
        socket_fd: RawFd,
        rpc_type: &'static str,
    ) -> Result<(), RpcError> {
        let transport = io_uring::get_current_reactor().expect("io_uring reactor missing");

        let mut header_buf = BytesMut::with_capacity(Header::SIZE);

        while let Some(frame) = receiver.recv().await {
            gauge!("rpc_request_pending_in_send_queue", "type" => rpc_type).decrement(1.0);
            let MessageFrame { header, body } = frame;
            let request_id = header.get_id();
            debug!(%rpc_type, %socket_fd, %request_id, "sending request via io_uring");

            header_buf.clear();
            header_buf.reserve(Header::SIZE);
            header.encode(&mut header_buf);
            let header_len = header_buf.len();
            let header_bytes = header_buf.split_to(header_len).freeze();
            let body_len = body.len();
            let expected_total = header_len + body_len;

            let written = transport
                .send(socket_fd, header_bytes, body)
                .await
                .map_err(RpcError::IoError)?;

            if written != expected_total {
                warn!(
                    %rpc_type,
                    %socket_fd,
                    %request_id,
                    written,
                    expected_total,
                    "io_uring send wrote unexpected byte count"
                );
            }

            counter!("rpc_request_sent", "type" => rpc_type, "name" => "all").increment(1);
        }

        warn!(%rpc_type, %socket_fd, "sender closed, send message task quit");
        Ok(())
    }

    #[cfg(not(feature = "io_uring"))]
    async fn receive_task(
        receiver: OwnedReadHalf,
        requests: RequestMap<Header>,
        socket_fd: RawFd,
        rpc_type: &'static str,
    ) -> Result<(), RpcError> {
        Self::receive_task_tokio(receiver, &requests, socket_fd, rpc_type).await
    }

    #[cfg(feature = "io_uring")]
    async fn receive_task_io_uring(
        socket_fd: RawFd,
        requests: &RequestMap<Header>,
        rpc_type: &'static str,
    ) -> Result<(), RpcError> {
        let transport = io_uring::get_current_reactor().expect("io_uring reactor missing");
        while let Ok(frame) = transport.recv_frame(socket_fd).await {
            Self::handle_incoming_frame(frame, requests, socket_fd, rpc_type);
        }

        warn!(%rpc_type, %socket_fd, "connection closed, receive message task quit");
        Ok(())
    }

    #[cfg(not(feature = "io_uring"))]
    async fn receive_task_tokio(
        receiver: OwnedReadHalf,
        requests: &RequestMap<Header>,
        socket_fd: RawFd,
        rpc_type: &'static str,
    ) -> Result<(), RpcError> {
        let decoder = Codec::default();
        let mut reader = FramedRead::new(receiver, decoder);
        while let Some(frame) = reader.next().await {
            let frame = frame?;
            Self::handle_incoming_frame(frame, requests, socket_fd, rpc_type);
        }
        warn!(%rpc_type, %socket_fd, "connection closed, receive message task quit");
        Ok(())
    }

    fn handle_incoming_frame(
        frame: MessageFrame<Header>,
        requests: &RequestMap<Header>,
        socket_fd: RawFd,
        rpc_type: &'static str,
    ) {
        let request_id = frame.header.get_id();
        debug!(%rpc_type, %socket_fd, %request_id, "receiving response:");
        counter!("rpc_response_received", "type" => rpc_type, "name" => "all").increment(1);
        let tx: oneshot::Sender<MessageFrame<Header>> = match requests.lock().remove(&request_id) {
            Some(tx) => tx,
            None => {
                warn!(%rpc_type, %socket_fd, %request_id,
                    "received rpc message with id not in the resp_map");
                return;
            }
        };
        gauge!("rpc_request_pending_in_resp_map", "type" => rpc_type).decrement(1.0);
        if tx.send(frame).is_err() {
            warn!(%rpc_type, %socket_fd, %request_id, "oneshot response send failed");
        }
    }

    fn drain_pending_requests(
        socket_fd: RawFd,
        requests: &RequestMap<Header>,
        drain_from: DrainFrom,
    ) {
        let mut requests = requests.lock();
        let pending_count = requests.len();
        if pending_count > 0 {
            warn!(
                rpc_type = %Codec::RPC_TYPE,
                %socket_fd,
                "draining {pending_count} pending requests from {} on connection close",
                drain_from.as_ref()
            );
            gauge!("rpc_request_pending_in_resp_map", "type" => Codec::RPC_TYPE)
                .decrement(pending_count as f64);
            requests.clear(); // This drops the senders, notifying receivers of an error.
        }
    }

    async fn send_request_internal(
        &self,
        request_id: u32,
        retry_count: u32,
        mut frame: MessageFrame<Header>,
        timeout: Option<Duration>,
    ) -> Result<MessageFrame<Header>, RpcError> {
        if self.is_closed.load(Ordering::SeqCst) {
            return Err(RpcError::InternalRequestError(
                "Connection is closed".into(),
            ));
        }

        let rpc_type = Codec::RPC_TYPE;
        frame.header.set_id(request_id);
        frame.header.set_retry_count(retry_count);

        let (tx, rx) = oneshot::channel();
        {
            self.requests.lock().insert(request_id, tx);
        }
        gauge!("rpc_request_pending_in_resp_map", "type" => rpc_type).increment(1.0);

        self.sender
            .send(frame)
            .await
            .map_err(|e| RpcError::InternalRequestError(e.to_string()))?;
        gauge!("rpc_request_pending_in_send_queue", "type" => rpc_type).increment(1.0);

        let result = match timeout {
            None => rx.await,
            Some(rpc_timeout) => match tokio::time::timeout(rpc_timeout, rx).await {
                Ok(result) => result,
                Err(_) => {
                    warn!(%rpc_type, socket_fd=%self.socket_fd, %request_id, "rpc request timeout");
                    return Err(RpcError::InternalResponseError("timeout".into()));
                }
            },
        };
        result.map_err(|e| RpcError::InternalResponseError(e.to_string()))
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
        self.send_request_internal(request_id, 0, frame, timeout)
            .await
    }

    pub fn is_closed(&self) -> bool {
        self.is_closed.load(Ordering::SeqCst)
    }
}

macro_rules! establish_connection_with_retry {
    ($addr_key:expr, $connect_fn:expr) => {{
        const MAX_CONNECTION_RETRIES: usize = 100 * 3600;
        let start = std::time::Instant::now();
        let retry_strategy = FixedInterval::from_millis(10)
            .map(jitter)
            .take(MAX_CONNECTION_RETRIES);

        let client = Retry::spawn(retry_strategy, $connect_fn)
            .await
            .map_err(|e| {
                warn!(rpc_type = %Codec::RPC_TYPE, addr = %$addr_key, error = %e, "failed to connect RPC server");
                RpcError::IoError(io::Error::other(e.to_string()))
            })?;

        let duration = start.elapsed();
        if duration > Duration::from_secs(1) {
            warn!(
                rpc_type = %Codec::RPC_TYPE,
                addr = %$addr_key,
                duration_ms = %duration.as_millis(),
                "Slow connection establishment to RPC server"
            );
        } else if duration > Duration::from_millis(100) {
            debug!(
                rpc_type = %Codec::RPC_TYPE,
                addr = %$addr_key,
                duration_ms = %duration.as_millis(),
                "Connection established to RPC server"
            );
        }

        Ok(client)
    }};
}

impl<Codec, Header> RpcClient<Codec, Header>
where
    Codec: RpcCodec<Header>,
    Header: MessageHeaderTrait + Clone + Send + Sync + 'static,
{
    fn configure_tcp_socket(socket: &Socket) -> Result<(), io::Error> {
        socket.set_recv_buffer_size(16 * 1024 * 1024)?;
        socket.set_send_buffer_size(16 * 1024 * 1024)?;

        let keepalive = TcpKeepalive::new()
            .with_time(Duration::from_secs(5))
            .with_interval(Duration::from_secs(2))
            .with_retries(2);
        socket.set_tcp_keepalive(&keepalive)?;

        socket.set_nodelay(true)?;
        socket.set_nonblocking(true)?;

        Ok(())
    }

    #[cfg(not(feature = "io_uring"))]
    pub async fn establish_connection(addr_key: String) -> Result<Self, RpcError>
    where
        Header: Default,
    {
        debug!("Trying to connect to {addr_key}");
        establish_connection_with_retry!(&addr_key, || async {
            let socket_addr = Self::resolve_address(&addr_key).await?;
            let stream = TcpStream::connect(socket_addr).await?;

            let std_stream = stream.into_std().map_err(RpcError::IoError)?;
            let socket = Socket::from(std_stream);
            Self::configure_tcp_socket(&socket).map_err(RpcError::IoError)?;
            let std_stream: std::net::TcpStream = socket.into();
            let configured_stream = TcpStream::from_std(std_stream).map_err(RpcError::IoError)?;

            Self::new_internal(configured_stream).await
        })
    }

    #[cfg(feature = "io_uring")]
    pub async fn establish_connection(addr_key: String) -> Result<Self, RpcError>
    where
        Header: Default,
    {
        debug!("Trying to connect to {addr_key} via io_uring");
        establish_connection_with_retry!(&addr_key, || async {
            let socket_addr = Self::resolve_address(&addr_key).await?;
            Self::new_internal_io_uring(socket_addr).await
        })
    }
}
