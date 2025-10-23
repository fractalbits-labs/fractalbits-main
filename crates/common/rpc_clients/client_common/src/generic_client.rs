use crate::RpcError;
#[cfg(feature = "io_uring")]
use crate::io_uring;
use bytes::BytesMut;
use metrics::{counter, gauge};
use rpc_codec_common::{MessageFrame, MessageHeaderTrait};
use socket2::{Socket, TcpKeepalive};
use std::collections::HashMap;
use std::io;
use std::io::IoSlice;
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::os::fd::RawFd;
use std::os::unix::io::AsRawFd;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use strum::AsRefStr;
use tokio::sync::oneshot;
use tokio::task::AbortHandle;
use tokio_retry::{
    Retry,
    strategy::{FixedInterval, jitter},
};
use tracing::{debug, warn};

use bumpalo::Bump;
use std::cell::RefCell;

thread_local! {
    static REQUEST_BUMPS: RefCell<HashMap<u64, &'static Bump>> =
        RefCell::new(HashMap::new());
}

pub fn register_request_bump(trace_id: u64, bump: &Bump) {
    REQUEST_BUMPS.with(|map| {
        map.borrow_mut().insert(trace_id, unsafe {
            std::mem::transmute::<&Bump, &'static Bump>(bump)
        });
    });
}

pub fn unregister_request_bump(trace_id: u64) {
    REQUEST_BUMPS.with(|map| map.borrow_mut().remove(&trace_id));
}

pub fn get_request_bump(trace_id: u64) -> Option<&'static Bump> {
    REQUEST_BUMPS.with(|map| map.borrow().get(&trace_id).copied())
}

type RequestMap<Header> =
    Arc<parking_lot::Mutex<HashMap<u32, oneshot::Sender<MessageFrame<Header>>>>>;

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

pub struct RpcClient<Codec: RpcCodec<Header>, Header: MessageHeaderTrait> {
    requests: RequestMap<Header>,
    #[cfg(not(feature = "io_uring"))]
    writer: Arc<tokio::sync::Mutex<tokio::net::tcp::OwnedWriteHalf>>,
    #[cfg(feature = "io_uring")]
    writer_fd: RawFd,
    recv_task_handle: AbortHandle,
    socket_fd: RawFd,
    is_closed: Arc<AtomicBool>,
    // For io_uring: we use the raw FD directly, but must keep the Socket alive
    // to prevent it from being dropped and closing the FD. This field ensures
    // proper RAII ownership - the socket is closed when RpcClient is dropped.
    #[cfg(feature = "io_uring")]
    _socket_owner: Socket,
    _phantom: PhantomData<Codec>,
}

#[derive(AsRefStr)]
#[strum(serialize_all = "snake_case")]
enum DrainFrom {
    ReceiveTask,
    RpcClient,
}

impl<Codec: RpcCodec<Header>, Header: MessageHeaderTrait> Drop for RpcClient<Codec, Header> {
    fn drop(&mut self) {
        debug!(rpc_type = Codec::RPC_TYPE, socket_fd = %self.socket_fd, "RpcClient dropped, aborting tasks");
        self.recv_task_handle.abort();
        Self::drain_pending_requests(self.socket_fd, &self.requests, DrainFrom::RpcClient);
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
    async fn new_internal_tokio(stream: tokio::net::TcpStream) -> Result<Self, RpcError> {
        let rpc_type = Codec::RPC_TYPE;
        let socket_fd = stream.as_raw_fd();
        let (reader, writer) = stream.into_split();
        let requests: RequestMap<Header> = Arc::new(parking_lot::Mutex::new(HashMap::new()));
        let is_closed = Arc::new(AtomicBool::new(false));
        let writer = Arc::new(tokio::sync::Mutex::new(writer));

        // Receive task
        let recv_handle = {
            let receiver_requests = requests.clone();
            let is_closed = is_closed.clone();
            tokio::spawn(async move {
                if let Err(e) =
                    Self::receive_task_tokio(reader, &receiver_requests, socket_fd, rpc_type).await
                {
                    warn!(%rpc_type, %socket_fd, %e, "receive task failed");
                }
                is_closed.store(true, Ordering::SeqCst);
                Self::drain_pending_requests(socket_fd, &receiver_requests, DrainFrom::ReceiveTask);
            })
            .abort_handle()
        };

        debug!(%rpc_type, %socket_fd, "Creating RPC client");

        Ok(RpcClient {
            requests,
            writer,
            recv_task_handle: recv_handle,
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

        let requests: RequestMap<Header> = Arc::new(parking_lot::Mutex::new(HashMap::new()));
        let is_closed = Arc::new(AtomicBool::new(false));

        // Receive task
        let recv_handle = {
            let receiver_requests = requests.clone();
            let is_closed = is_closed.clone();
            tokio::spawn(async move {
                if let Err(e) =
                    Self::receive_task_io_uring(socket_fd, &receiver_requests, rpc_type).await
                {
                    warn!(%rpc_type, %socket_fd, %e, "receive task failed");
                }
                is_closed.store(true, Ordering::SeqCst);
                Self::drain_pending_requests(socket_fd, &receiver_requests, DrainFrom::ReceiveTask);
            })
            .abort_handle()
        };

        debug!(%rpc_type, %socket_fd, "Creating RPC client (io_uring)");

        Ok(RpcClient {
            requests,
            writer_fd: socket_fd,
            recv_task_handle: recv_handle,
            socket_fd,
            is_closed,
            _socket_owner: socket,
            _phantom: PhantomData,
        })
    }

    #[cfg(feature = "io_uring")]
    async fn receive_task_io_uring(
        socket_fd: RawFd,
        requests: &RequestMap<Header>,
        rpc_type: &'static str,
    ) -> Result<(), RpcError> {
        let transport = io_uring::get_current_reactor().expect("io_uring reactor missing");
        while let Ok(frame) = transport.recv(socket_fd).await {
            Self::handle_incoming_frame(frame, requests, socket_fd, rpc_type);
        }

        warn!(%rpc_type, %socket_fd, "connection closed, receive message task quit");
        Ok(())
    }

    #[cfg(not(feature = "io_uring"))]
    async fn receive_task_tokio(
        receiver: tokio::net::tcp::OwnedReadHalf,
        requests: &RequestMap<Header>,
        socket_fd: RawFd,
        rpc_type: &'static str,
    ) -> Result<(), RpcError> {
        use tokio_stream::StreamExt;
        let decoder = Codec::default();
        let mut reader = tokio_util::codec::FramedRead::new(receiver, decoder);
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

    #[cfg(not(feature = "io_uring"))]
    async fn send_request_internal<B: AsRef<[u8]>>(
        &self,
        request_id: u32,
        retry_count: u32,
        mut frame: MessageFrame<Header, B>,
        timeout: Option<Duration>,
    ) -> Result<MessageFrame<Header>, RpcError> {
        use tokio::io::AsyncWriteExt;

        if self.is_closed.load(Ordering::SeqCst) {
            return Err(RpcError::InternalRequestError(
                "Connection is closed".into(),
            ));
        }

        let rpc_type = Codec::RPC_TYPE;
        let trace_id = frame.header.get_trace_id();
        frame.header.set_id(request_id);
        frame.header.set_retry_count(retry_count);

        let (tx, rx) = oneshot::channel();
        self.requests.lock().insert(request_id, tx);
        gauge!("rpc_request_pending_in_resp_map", "type" => rpc_type).increment(1.0);

        debug!(%rpc_type, socket_fd=%self.socket_fd, %request_id, %trace_id, "sending request");

        let mut header_bytes = BytesMut::with_capacity(Header::SIZE);
        frame.header.encode(&mut header_bytes);
        let body_buf = frame.body.as_ref();
        let iov = if body_buf.is_empty() {
            vec![IoSlice::new(&header_bytes[..])]
        } else {
            vec![IoSlice::new(&header_bytes[..]), IoSlice::new(body_buf)]
        };

        let mut writer = self.writer.lock().await;
        writer
            .write_vectored(&iov)
            .await
            .map_err(RpcError::IoError)?;

        counter!("rpc_request_sent", "type" => rpc_type, "name" => "all").increment(1);

        drop(writer);

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

    #[cfg(feature = "io_uring")]
    async fn send_request_internal<B: AsRef<[u8]>>(
        &self,
        request_id: u32,
        retry_count: u32,
        mut frame: MessageFrame<Header, B>,
        timeout: Option<Duration>,
    ) -> Result<MessageFrame<Header>, RpcError> {
        if self.is_closed.load(Ordering::SeqCst) {
            return Err(RpcError::InternalRequestError(
                "Connection is closed".into(),
            ));
        }

        let rpc_type = Codec::RPC_TYPE;
        let trace_id = frame.header.get_trace_id();
        frame.header.set_id(request_id);
        frame.header.set_retry_count(retry_count);

        let (tx, rx) = oneshot::channel();
        self.requests.lock().insert(request_id, tx);
        gauge!("rpc_request_pending_in_resp_map", "type" => rpc_type).increment(1.0);

        debug!(%rpc_type, socket_fd=%self.socket_fd, %request_id, %trace_id, "sending request via io_uring");

        let mut header_bytes = BytesMut::with_capacity(Header::SIZE);
        frame.header.encode(&mut header_bytes);
        let header = header_bytes.freeze();

        let body_buf = frame.body.as_ref();
        let iov = if body_buf.is_empty() {
            vec![IoSlice::new(&header)]
        } else {
            vec![IoSlice::new(&header), IoSlice::new(body_buf)]
        };

        let transport = io_uring::get_current_reactor().expect("io_uring reactor missing");
        transport
            .send_vectored(self.writer_fd, &iov, false)
            .await
            .map_err(RpcError::IoError)?;

        counter!("rpc_request_sent", "type" => rpc_type, "name" => "all").increment(1);

        let result = match timeout {
            None => rx.await,
            Some(rpc_timeout) => match tokio::time::timeout(rpc_timeout, rx).await {
                Ok(result) => result,
                Err(_) => {
                    warn!(%rpc_type, socket_fd=%self.socket_fd, %request_id, "rpc request timeout via io_uring");
                    return Err(RpcError::InternalResponseError("timeout".into()));
                }
            },
        };
        result.map_err(|e| RpcError::InternalResponseError(e.to_string()))
    }

    pub async fn send_request<B: AsRef<[u8]>>(
        &self,
        request_id: u32,
        mut frame: MessageFrame<Header, B>,
        timeout: Option<std::time::Duration>,
        trace_id: Option<u64>,
    ) -> Result<MessageFrame<Header>, RpcError> {
        if let Some(trace_id) = trace_id {
            frame.header.set_trace_id(trace_id);
        }
        self.send_request_internal(request_id, 0, frame, timeout)
            .await
    }

    pub fn is_closed(&self) -> bool {
        self.is_closed.load(Ordering::SeqCst)
    }
}

macro_rules! establish_connection_with_retry {
    ($addr_key:expr, $connect_fn:expr) => {{
        const MAX_CONNECTION_RETRIES: usize = 100 * 3600 * 24;
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
    pub async fn establish_connection(addr: String) -> Result<Self, RpcError>
    where
        Header: Default,
    {
        debug!(rpc_type=%Codec::RPC_TYPE, %addr, "Trying to connect to rpc server");
        establish_connection_with_retry!(&addr, || async {
            let socket_addr = Self::resolve_address(&addr).await?;
            let stream = tokio::net::TcpStream::connect(socket_addr).await?;

            let std_stream = stream.into_std().map_err(RpcError::IoError)?;
            let socket = Socket::from(std_stream);
            Self::configure_tcp_socket(&socket).map_err(RpcError::IoError)?;
            let std_stream: std::net::TcpStream = socket.into();
            let configured_stream =
                tokio::net::TcpStream::from_std(std_stream).map_err(RpcError::IoError)?;

            Self::new_internal_tokio(configured_stream).await
        })
    }

    #[cfg(feature = "io_uring")]
    pub async fn establish_connection(addr: String) -> Result<Self, RpcError>
    where
        Header: Default,
    {
        debug!(rpc_type=%Codec::RPC_TYPE, %addr, "Trying to connect to rpc server via io_uring");
        establish_connection_with_retry!(&addr, || async {
            let socket_addr = Self::resolve_address(&addr).await?;
            Self::new_internal_io_uring(socket_addr).await
        })
    }
}
