use bytes::{Bytes, BytesMut};
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
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::Duration;
use strum::AsRefStr;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
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
use tokio_stream::StreamExt;
use tokio_util::codec::FramedRead;
use tracing::{debug, warn};

use crate::{
    RpcError,
    transport::{RpcTransport, current_transport},
};

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

pub struct RpcClient<Codec: RpcCodec<Header>, Header: MessageHeaderTrait> {
    requests: RequestMap<Header>,
    sender: Sender<MessageFrame<Header>>,
    next_id: AtomicU32,
    #[allow(unused)]
    tasks: JoinSet<()>,
    socket_fd: RawFd,
    is_closed: Arc<AtomicBool>,
    client_session_id: u64,
    #[allow(unused)]
    transport: Option<Arc<dyn RpcTransport>>,
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
        debug!(rpc_type = Codec::RPC_TYPE, socket_fd = %self.socket_fd, "RpcClient dropped");
        // Just try to make metrics (`rpc_request_pending_in_resp_map`) accurate
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

    pub async fn new(stream: TcpStream) -> Result<Self, RpcError>
    where
        Header: Default,
    {
        let stream = Self::configure_tcp_socket(stream).map_err(|e| {
            RpcError::IoError(io::Error::other(format!(
                "failed to configure tcp stream: {e}"
            )))
        })?;
        let mut client = Self::new_internal(stream, 0).await?;

        // For new connections, perform handshake if needed based on RPC type
        let rpc_type = Codec::RPC_TYPE;
        if rpc_type == "rss" {
            // RSS doesn't use handshake, just set session_id = 1
            client.update_session_id(1);
        } else {
            // For NSS and BSS, perform handshake as first RPC call
            let session_id = client.perform_handshake().await?;
            client.update_session_id(session_id);
        }

        Ok(client)
    }

    pub async fn new_with_session_id(stream: TcpStream, session_id: u64) -> Result<Self, RpcError>
    where
        Header: Default,
    {
        let client = Self::new_internal(stream, session_id).await?;

        // For reconnection with existing session_id, perform handshake with that session_id
        let rpc_type = Codec::RPC_TYPE;
        if rpc_type != "rss" {
            // For NSS and BSS, perform handshake with existing session_id for routing
            client.perform_handshake_with_session_id(session_id).await?;
        }

        Ok(client)
    }

    /// Create client with existing session state (performing handshake for reconnection)
    pub async fn new_with_session_and_request_id(
        stream: TcpStream,
        session_id: u64,
        next_request_id: u32,
    ) -> Result<Self, RpcError>
    where
        Header: Default,
    {
        let client = Self::new_internal(stream, session_id).await?;
        // Set the next request ID to continue from where we left off
        client.next_id.store(next_request_id, Ordering::SeqCst);

        // For reconnection with existing session state, perform handshake with session_id
        let rpc_type = Codec::RPC_TYPE;
        if rpc_type != "rss" {
            // For NSS and BSS, perform handshake with existing session_id for routing
            client.perform_handshake_with_session_id(session_id).await?;
        }

        Ok(client)
    }

    async fn new_internal(stream: TcpStream, session_id: u64) -> Result<Self, RpcError> {
        let transport = current_transport();
        if let Some(t) = transport.as_ref() {
            tracing::debug!(
                rpc_type = Codec::RPC_TYPE,
                transport = t.name(),
                "rpc client detected alternate transport"
            );
        }
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
            let transport = transport.clone();
            tasks.spawn(async move {
                if let Err(e) =
                    Self::send_task(writer, receiver, socket_fd, rpc_type, transport).await
                {
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
            let transport = transport.clone();
            tasks.spawn(async move {
                if let Err(e) = Self::receive_task(
                    reader,
                    receiver_requests.clone(),
                    socket_fd,
                    rpc_type,
                    transport,
                )
                .await
                {
                    warn!(%rpc_type, %socket_fd, %e, "receive task failed");
                }
                is_closed.store(true, Ordering::SeqCst);
                Self::drain_pending_requests(socket_fd, &receiver_requests, DrainFrom::ReceiveTask);
            });
        }

        debug!(%rpc_type, %socket_fd, %session_id, "Creating RPC client with session ID");

        Ok(RpcClient {
            requests,
            sender,
            next_id: AtomicU32::new(1),
            tasks,
            socket_fd,
            is_closed,
            client_session_id: session_id,
            transport,
            _phantom: PhantomData,
        })
    }

    async fn send_task(
        mut writer: OwnedWriteHalf,
        mut receiver: Receiver<MessageFrame<Header>>,
        socket_fd: RawFd,
        rpc_type: &'static str,
        transport: Option<Arc<dyn RpcTransport>>,
    ) -> Result<(), RpcError> {
        let mut header_buf = BytesMut::with_capacity(Header::SIZE);
        while let Some(frame) = receiver.recv().await {
            gauge!("rpc_request_pending_in_send_queue", "type" => rpc_type).decrement(1.0);
            let MessageFrame { header, body } = frame;
            let request_id = frame.header.get_id();
            debug!(%rpc_type, %socket_fd, %request_id, "sending request:");

            header_buf.clear();
            header.encode(&mut header_buf);

            if let Some(tp) = transport.as_ref() {
                tp.send(
                    socket_fd,
                    Bytes::copy_from_slice(header_buf.as_ref()),
                    body.clone(),
                )
                .await
                .map_err(RpcError::IoError)?;
            } else {
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
            }
            writer.flush().await?;
            counter!("rpc_request_sent", "type" => rpc_type, "name" => "all").increment(1);
        }
        warn!(%rpc_type, %socket_fd, "sender closed, send message task quit");
        Ok(())
    }

    async fn receive_task(
        receiver: OwnedReadHalf,
        requests: RequestMap<Header>,
        socket_fd: RawFd,
        rpc_type: &'static str,

        transport: Option<Arc<dyn RpcTransport>>,
    ) -> Result<(), RpcError> {
        if let Some(tp) = transport {
            drop(receiver);
            Self::receive_task_transport(tp, socket_fd, &requests, rpc_type).await
        } else {
            Self::receive_task_tokio(receiver, &requests, socket_fd, rpc_type).await
        }
    }

    async fn receive_task_transport(
        transport: Arc<dyn RpcTransport>,
        socket_fd: RawFd,
        requests: &RequestMap<Header>,
        rpc_type: &'static str,
    ) -> Result<(), RpcError> {
        const TRANSPORT_CHUNK_SIZE: usize = 64 * 1024;
        let mut decoder = Codec::default();
        let mut buffer = BytesMut::with_capacity(128 * 1024);

        loop {
            debug!(
                rpc_type = %rpc_type,
                %socket_fd,
                "transport recv waiting"
            );
            let chunk = transport
                .recv(socket_fd, TRANSPORT_CHUNK_SIZE)
                .await
                .map_err(RpcError::IoError)?;
            debug!(
                rpc_type = %rpc_type,
                %socket_fd,
                chunk_len = chunk.len(),
                "transport recv completed"
            );

            if chunk.is_empty() {
                if let Some(frame) = decoder
                    .decode_eof(&mut buffer)
                    .map_err(|e| RpcError::DecodeError(e.to_string()))?
                {
                    Self::handle_incoming_frame(frame, requests, socket_fd, rpc_type);
                }
                break;
            }

            buffer.extend_from_slice(&chunk);

            loop {
                match decoder.decode(&mut buffer) {
                    Ok(Some(frame)) => {
                        Self::handle_incoming_frame(frame, requests, socket_fd, rpc_type);
                    }
                    Ok(None) => break,
                    Err(e) => return Err(RpcError::DecodeError(e.to_string())),
                }
            }
        }

        warn!(%rpc_type, %socket_fd, "connection closed, receive message task quit");
        Ok(())
    }

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
        frame.header.set_client_session_id(self.client_session_id);
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

    pub fn get_client_session_id(&self) -> u64 {
        self.client_session_id
    }

    /// Extract current session state for persistence across reconnections
    pub fn get_session_state(&self) -> (u64, u32) {
        (self.client_session_id, self.next_id.load(Ordering::SeqCst))
    }

    fn update_session_id(&mut self, session_id: u64) {
        self.client_session_id = session_id;
    }

    async fn perform_handshake(&self) -> Result<u64, RpcError>
    where
        Header: Default,
    {
        let response = self.perform_handshake_internal(0).await?;

        // Extract assigned session ID from response
        let assigned_session_id = response.header.get_client_session_id();

        if assigned_session_id == 0 {
            return Err(RpcError::InternalResponseError(
                "Server did not assign valid session ID".into(),
            ));
        }

        debug!(rpc_type = %Codec::RPC_TYPE, socket_fd = %self.socket_fd, session_id = %assigned_session_id, "Received session ID from handshake");
        Ok(assigned_session_id)
    }

    async fn perform_handshake_with_session_id(&self, session_id: u64) -> Result<(), RpcError>
    where
        Header: Default,
    {
        let response = self.perform_handshake_internal(session_id).await?;

        // Verify the server acknowledged the session ID
        let response_session_id = response.header.get_client_session_id();
        if response_session_id != session_id {
            return Err(RpcError::InternalResponseError(format!(
                "Server returned different session ID: expected {}, got {}",
                session_id, response_session_id
            )));
        }

        debug!(rpc_type = %Codec::RPC_TYPE, socket_fd = %self.socket_fd, session_id = %session_id, "Handshake completed with existing session ID");
        Ok(())
    }

    async fn perform_handshake_internal(
        &self,
        session_id: u64,
    ) -> Result<MessageFrame<Header>, RpcError>
    where
        Header: Default,
    {
        // Create handshake request with command = 1 (HANDSHAKE)
        let mut handshake_frame = MessageFrame {
            header: Header::default(),
            body: bytes::Bytes::new(),
        };

        // Set handshake command and session_id (0 for new, existing for reconnection)
        handshake_frame.header.set_handshake_command();
        handshake_frame.header.set_client_session_id(session_id);
        // Set size to header size (no body for handshake)
        handshake_frame.header.set_size(Header::SIZE as u32);

        // Send handshake request and wait for response
        let request_id = self.gen_request_id();
        self.send_request_internal(request_id, 0, handshake_frame, None)
            .await
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

impl<Codec, Header> RpcClient<Codec, Header>
where
    Codec: RpcCodec<Header>,
    Header: MessageHeaderTrait + Clone + Send + Sync + 'static,
{
    async fn connect_with_retry(
        addr_key: &str,
    ) -> Result<TcpStream, Box<dyn std::error::Error + Send + Sync>> {
        debug!("Trying to connect to {addr_key}");
        const MAX_CONNECTION_RETRIES: usize = 100 * 3600;

        let start = std::time::Instant::now();
        let retry_strategy = FixedInterval::from_millis(10)
            .map(jitter)
            .take(MAX_CONNECTION_RETRIES);

        let stream = Retry::spawn(retry_strategy, || async {
            let socket_addr = Self::resolve_address(addr_key).await?;
            TcpStream::connect(socket_addr).await
        })
        .await
        .map_err(|e| {
            warn!(rpc_type = %Codec::RPC_TYPE, addr = %addr_key, error = %e, "failed to connect RPC server");
            Box::new(e) as Box<dyn std::error::Error + Send + Sync>
        })?;

        let connect_duration = start.elapsed();
        if connect_duration > Duration::from_secs(1) {
            warn!(
                rpc_type = %Codec::RPC_TYPE,
                addr = %addr_key,
                duration_ms = %connect_duration.as_millis(),
                "Slow connection establishment to RPC server"
            );
        } else if connect_duration > Duration::from_millis(100) {
            debug!(
                rpc_type = %Codec::RPC_TYPE,
                addr = %addr_key,
                duration_ms = %connect_duration.as_millis(),
                "Connection established to RPC server"
            );
        }

        Ok(stream)
    }

    async fn establish_connection(
        addr_key: String,
        session_id: Option<u64>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>>
    where
        Header: Default,
    {
        let stream = Self::connect_with_retry(&addr_key).await?;
        let configured_stream = Self::configure_tcp_socket(stream)?;

        match session_id {
            Some(id) => Self::new_with_session_id(configured_stream, id)
                .await
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>),
            None => Self::new(configured_stream)
                .await
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>),
        }
    }

    fn configure_tcp_socket(
        stream: TcpStream,
    ) -> Result<TcpStream, Box<dyn std::error::Error + Send + Sync>> {
        // Configure socket
        let std_stream = stream
            .into_std()
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
        let socket = Socket::from(std_stream);

        // Set 16MB buffers for data-intensive operations
        socket
            .set_recv_buffer_size(16 * 1024 * 1024)
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
        socket
            .set_send_buffer_size(16 * 1024 * 1024)
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

        // Configure aggressive keepalive for internal network
        let keepalive = TcpKeepalive::new()
            .with_time(Duration::from_secs(5))
            .with_interval(Duration::from_secs(2))
            .with_retries(2);
        socket
            .set_tcp_keepalive(&keepalive)
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

        // Set TCP_NODELAY for low latency
        socket
            .set_nodelay(true)
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

        // Convert back to tokio TcpStream
        let std_stream: std::net::TcpStream = socket.into();
        std_stream
            .set_nonblocking(true)
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
        TcpStream::from_std(std_stream)
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
    }

    /// Establish connection with existing session state (for reconnection)
    pub async fn establish_connection_with_session_state(
        addr_key: String,
        session_id: u64,
        next_request_id: u32,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>>
    where
        Header: Default,
    {
        let stream = Self::connect_with_retry(&addr_key).await?;
        let configured_stream = Self::configure_tcp_socket(stream)?;

        Self::new_with_session_and_request_id(configured_stream, session_id, next_request_id)
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
    }
}

impl<Codec: RpcCodec<Header> + Unpin, Header: MessageHeaderTrait + Default> Poolable
    for RpcClient<Codec, Header>
{
    type AddrKey = String;
    type Error = Box<dyn std::error::Error + Send + Sync>;

    async fn new(addr_key: Self::AddrKey) -> Result<Self, Self::Error> {
        Self::establish_connection(addr_key, None).await
    }

    async fn new_with_session_id(
        addr_key: Self::AddrKey,
        session_id: u64,
    ) -> Result<Self, Self::Error> {
        Self::establish_connection(addr_key, Some(session_id)).await
    }

    async fn new_with_session_and_request_id(
        addr_key: Self::AddrKey,
        session_id: u64,
        next_request_id: u32,
    ) -> Result<Self, Self::Error> {
        Self::establish_connection_with_session_state(addr_key, session_id, next_request_id).await
    }

    fn is_closed(&self) -> bool {
        self.is_closed()
    }

    fn get_session_state(&self) -> (u64, u32) {
        self.get_session_state()
    }
}
