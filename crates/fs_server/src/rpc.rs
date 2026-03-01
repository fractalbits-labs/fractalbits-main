use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::future::Future;
use std::io;
use std::marker::PhantomData;
use std::mem::size_of;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

use bytes::Bytes;
use compio_buf::BufResult;
use compio_io::{AsyncReadExt, AsyncWriteExt};
use compio_net::{OwnedReadHalf, OwnedWriteHalf, SocketOpts, TcpStream};
use rpc_codec_common::{MessageFrame, MessageHeaderTrait};
use tracing::{debug, error, warn};

/// Marker trait for RPC codec types (same semantics as client_common::RpcCodec).
pub trait RpcCodec<Header: MessageHeaderTrait>: Default + Clone + 'static {
    const RPC_TYPE: &'static str;
}

/// Errors from the compio RPC client.
#[derive(Debug)]
pub enum RpcError {
    IoError(io::Error),
    InternalRequestError(String),
    InternalResponseError(String),
    ConnectionClosed,
    ChecksumMismatch,
    SendError(String),
    NotFound,
    AlreadyExists,
    Retry,
    DecodeError(String),
    EncodeError(String),
}

impl std::fmt::Display for RpcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::IoError(e) => write!(f, "I/O error: {}", e),
            Self::InternalRequestError(s) => write!(f, "Internal request error: {}", s),
            Self::InternalResponseError(s) => write!(f, "Internal response error: {}", s),
            Self::ConnectionClosed => write!(f, "Connection closed"),
            Self::ChecksumMismatch => write!(f, "Checksum mismatch"),
            Self::SendError(s) => write!(f, "Send error: {}", s),
            Self::NotFound => write!(f, "Not found"),
            Self::AlreadyExists => write!(f, "Already exists"),
            Self::Retry => write!(f, "Retry"),
            Self::DecodeError(s) => write!(f, "Decode error: {}", s),
            Self::EncodeError(s) => write!(f, "Encode error: {}", s),
        }
    }
}

impl std::error::Error for RpcError {}

impl From<io::Error> for RpcError {
    fn from(e: io::Error) -> Self {
        Self::IoError(e)
    }
}

impl RpcError {
    pub fn retryable(&self) -> bool {
        matches!(
            self,
            Self::InternalRequestError(_)
                | Self::InternalResponseError(_)
                | Self::ConnectionClosed
                | Self::Retry
        )
    }
}

struct PendingSlot<Header: MessageHeaderTrait> {
    result: Option<MessageFrame<Header>>,
    waker: Option<Waker>,
}

struct ClientInner<Header: MessageHeaderTrait> {
    pending: RefCell<HashMap<u32, PendingSlot<Header>>>,
    writer: RefCell<Option<OwnedWriteHalf<TcpStream>>>,
    is_closed: Cell<bool>,
}

/// Single-threaded compio-based RPC client.
///
/// Designed for use within a single compio Runtime thread. This client is
/// !Send and !Sync by design, using Rc/RefCell instead of Arc/Mutex.
///
/// A background receive task reads responses from the socket and delivers
/// them to waiting callers via a shared pending-request map.
pub struct CompioRpcClient<Codec, Header: MessageHeaderTrait> {
    inner: Rc<ClientInner<Header>>,
    _phantom: PhantomData<Codec>,
}

impl<Codec, Header> CompioRpcClient<Codec, Header>
where
    Codec: RpcCodec<Header>,
    Header: MessageHeaderTrait + Default + 'static,
{
    /// Connect to an RPC server with default socket options (16MB buffers, nodelay, keepalive).
    pub async fn connect(addr: &str) -> Result<Self, RpcError> {
        let opts = SocketOpts::new()
            .recv_buffer_size(16 * 1024 * 1024)
            .send_buffer_size(16 * 1024 * 1024)
            .nodelay(true)
            .keepalive(true);

        Self::connect_with_options(addr, &opts).await
    }

    /// Connect to an RPC server with custom socket options.
    pub async fn connect_with_options(addr: &str, opts: &SocketOpts) -> Result<Self, RpcError> {
        let rpc_type = Codec::RPC_TYPE;
        let start = std::time::Instant::now();

        debug!(%rpc_type, %addr, "connecting to RPC server");

        let stream = TcpStream::connect_with_options(addr, opts).await?;

        let duration = start.elapsed();
        if duration > std::time::Duration::from_secs(1) {
            warn!(%rpc_type, %addr, duration_ms = %duration.as_millis(),
                "slow connection establishment to RPC server");
        } else if duration > std::time::Duration::from_millis(100) {
            debug!(%rpc_type, %addr, duration_ms = %duration.as_millis(),
                "connection established to RPC server");
        }

        Self::from_stream(stream)
    }

    fn from_stream(stream: TcpStream) -> Result<Self, RpcError> {
        let (reader, writer) = stream.into_split();

        let inner = Rc::new(ClientInner {
            pending: RefCell::new(HashMap::with_capacity(256)),
            writer: RefCell::new(Some(writer)),
            is_closed: Cell::new(false),
        });

        // Spawn background receive task
        let recv_inner = inner.clone();
        let rpc_type = Codec::RPC_TYPE;
        compio_runtime::spawn(async move {
            if let Err(e) = receive_loop::<Header>(reader, &recv_inner, rpc_type).await {
                warn!(%rpc_type, error = %e, "receive task failed");
            }
            recv_inner.is_closed.set(true);
            drain_pending(&recv_inner.pending);
        })
        .detach();

        Ok(Self {
            inner,
            _phantom: PhantomData,
        })
    }

    /// Send a request and wait for the response.
    pub async fn send_request(
        &self,
        frame: MessageFrame<Header, Bytes>,
    ) -> Result<MessageFrame<Header>, RpcError> {
        self.send_request_vectored(MessageFrame::new(frame.header, vec![frame.body]))
            .await
    }

    /// Send a request with vectored body and wait for the response.
    pub async fn send_request_vectored(
        &self,
        frame: MessageFrame<Header, Vec<Bytes>>,
    ) -> Result<MessageFrame<Header>, RpcError> {
        if self.inner.is_closed.get() {
            return Err(RpcError::ConnectionClosed);
        }

        let rpc_type = Codec::RPC_TYPE;
        let request_id = frame.header.get_id();
        let trace_id = frame.header.get_trace_id();
        debug!(%rpc_type, %request_id, %trace_id, "sending request");

        // Register pending slot before writing (so the receive task can deliver)
        self.inner.pending.borrow_mut().insert(
            request_id,
            PendingSlot {
                result: None,
                waker: None,
            },
        );

        // Write to socket
        if let Err(e) = self.write_frame(frame).await {
            self.inner.pending.borrow_mut().remove(&request_id);
            return Err(e);
        }

        // Wait for response
        ResponseFuture {
            inner: self.inner.clone(),
            request_id,
        }
        .await
    }

    async fn write_frame(
        &self,
        mut frame: MessageFrame<Header, Vec<Bytes>>,
    ) -> Result<(), RpcError> {
        frame.header.set_checksum();
        let header_bytes = frame.header.encode().to_vec();

        // Take writer out of RefCell (borrow released immediately)
        let mut writer = self
            .inner
            .writer
            .borrow_mut()
            .take()
            .ok_or_else(|| RpcError::SendError("writer in use".into()))?;

        // Write header
        let BufResult(res, _) = writer.write_all(header_bytes).await;
        if let Err(e) = res {
            *self.inner.writer.borrow_mut() = Some(writer);
            return Err(RpcError::IoError(e));
        }

        // Write body chunks
        for chunk in frame.body {
            if chunk.is_empty() {
                continue;
            }
            let BufResult(res, _) = writer.write_all(chunk.to_vec()).await;
            if let Err(e) = res {
                *self.inner.writer.borrow_mut() = Some(writer);
                return Err(RpcError::IoError(e));
            }
        }

        // Return writer
        *self.inner.writer.borrow_mut() = Some(writer);
        Ok(())
    }

    pub fn is_closed(&self) -> bool {
        self.inner.is_closed.get()
    }
}

/// Background receive loop: reads header+body frames and delivers to pending callers.
async fn receive_loop<Header: MessageHeaderTrait>(
    mut reader: OwnedReadHalf<TcpStream>,
    inner: &ClientInner<Header>,
    rpc_type: &str,
) -> Result<(), RpcError> {
    let header_size = size_of::<Header>();

    loop {
        // Read fixed-size header
        let header_buf = Vec::with_capacity(header_size);
        let BufResult(res, header_buf) = reader.read_exact(header_buf).await;
        match res {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                debug!(%rpc_type, "connection closed by peer");
                return Ok(());
            }
            Err(e) => return Err(RpcError::IoError(e)),
        }

        // Verify header checksum before decoding (prevents UB from corrupted enum values)
        if !Header::verify_header_checksum_raw(&header_buf) {
            warn!(%rpc_type, "header checksum verification failed");
            return Err(RpcError::ChecksumMismatch);
        }
        let header = Header::decode(&header_buf);

        // Read body
        let body_size = header.get_body_size();
        let body = if body_size > 0 {
            let body_buf = Vec::with_capacity(body_size);
            let BufResult(res, body_buf) = reader.read_exact(body_buf).await;
            res?;
            Bytes::from(body_buf)
        } else {
            Bytes::new()
        };

        // Verify body checksum
        if !header.verify_body_checksum(&body) {
            error!(%rpc_type, request_id = %header.get_id(),
                "response body checksum verification failed, closing connection");
            return Err(RpcError::ChecksumMismatch);
        }

        // Deliver to pending caller
        let request_id = header.get_id();
        let trace_id = header.get_trace_id();
        debug!(%rpc_type, %request_id, %trace_id, "received response");

        let frame = MessageFrame::new(header, body);
        let mut pending = inner.pending.borrow_mut();
        if let Some(slot) = pending.get_mut(&request_id) {
            slot.result = Some(frame);
            if let Some(waker) = slot.waker.take() {
                drop(pending);
                waker.wake();
            }
        } else {
            warn!(%rpc_type, %request_id,
                "received response for unknown request ID");
        }
    }
}

/// Drain all pending requests, waking waiters so they see ConnectionClosed.
fn drain_pending<Header: MessageHeaderTrait>(pending: &RefCell<HashMap<u32, PendingSlot<Header>>>) {
    let mut map = pending.borrow_mut();
    for (_, slot) in map.drain() {
        if let Some(waker) = slot.waker {
            waker.wake();
        }
    }
}

/// Future that resolves when the receive task delivers a response for our request_id.
struct ResponseFuture<Header: MessageHeaderTrait> {
    inner: Rc<ClientInner<Header>>,
    request_id: u32,
}

impl<Header: MessageHeaderTrait> Future for ResponseFuture<Header> {
    type Output = Result<MessageFrame<Header>, RpcError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut pending = self.inner.pending.borrow_mut();

        if let Some(slot) = pending.get_mut(&self.request_id) {
            if let Some(result) = slot.result.take() {
                pending.remove(&self.request_id);
                return Poll::Ready(Ok(result));
            }
            // Not ready yet - register waker for when result arrives
            slot.waker = Some(cx.waker().clone());

            // Check if connection closed while we were waiting
            if self.inner.is_closed.get() {
                pending.remove(&self.request_id);
                return Poll::Ready(Err(RpcError::ConnectionClosed));
            }

            Poll::Pending
        } else {
            // Slot was removed (drained on connection close)
            Poll::Ready(Err(RpcError::ConnectionClosed))
        }
    }
}

impl<Header: MessageHeaderTrait> Drop for ResponseFuture<Header> {
    fn drop(&mut self) {
        // Clean up pending slot if the future is dropped before completion
        self.inner.pending.borrow_mut().remove(&self.request_id);
    }
}

/// Auto-reconnecting RPC client wrapper for use within a single compio Runtime thread.
///
/// Lazily connects on first request. Automatically reconnects on retryable errors.
/// Uses Rc/Cell instead of Arc/Atomic since compio is single-threaded.
pub struct AutoReconnectRpcClient<Codec, Header>
where
    Codec: RpcCodec<Header>,
    Header: MessageHeaderTrait,
{
    inner: RefCell<Option<Rc<CompioRpcClient<Codec, Header>>>>,
    addresses: Vec<String>,
    next_id: Cell<u32>,
    _phantom: PhantomData<(Codec, Header)>,
}

impl<Codec, Header> AutoReconnectRpcClient<Codec, Header>
where
    Codec: RpcCodec<Header>,
    Header: MessageHeaderTrait + Default + 'static,
{
    pub fn new_from_address(address: String) -> Self {
        Self {
            inner: RefCell::new(None),
            addresses: vec![address],
            next_id: Cell::new(1),
            _phantom: PhantomData,
        }
    }

    pub fn new_from_addresses(addresses: Vec<String>) -> Self {
        Self {
            inner: RefCell::new(None),
            addresses,
            next_id: Cell::new(1),
            _phantom: PhantomData,
        }
    }

    async fn ensure_connected(&self) -> Result<(), RpcError> {
        let rpc_type = Codec::RPC_TYPE;

        {
            let inner = self.inner.borrow();
            if let Some(client) = inner.as_ref()
                && !client.is_closed()
            {
                return Ok(());
            }
        }

        // Need to (re)connect - try all addresses
        for address in &self.addresses {
            debug!(%rpc_type, %address, "trying to connect to RPC server");
            match CompioRpcClient::<Codec, Header>::connect(address).await {
                Ok(new_client) => {
                    debug!(%rpc_type, %address, "successfully connected to RPC server");
                    *self.inner.borrow_mut() = Some(Rc::new(new_client));
                    return Ok(());
                }
                Err(e) => {
                    debug!(%rpc_type, %address, error = %e,
                        "failed to connect, trying next address");
                    continue;
                }
            }
        }

        error!(%rpc_type, addresses = ?self.addresses,
            "failed to establish RPC connection to any address");
        Err(RpcError::ConnectionClosed)
    }

    pub fn gen_request_id(&self) -> u32 {
        let id = self.next_id.get();
        self.next_id.set(id.wrapping_add(1));
        id
    }

    pub async fn send_request(
        &self,
        frame: MessageFrame<Header, Bytes>,
    ) -> Result<MessageFrame<Header>, RpcError> {
        self.ensure_connected().await?;
        let client = self.inner.borrow().as_ref().unwrap().clone();
        client.send_request(frame).await
    }

    #[allow(dead_code)]
    pub async fn send_request_vectored(
        &self,
        frame: MessageFrame<Header, Vec<Bytes>>,
    ) -> Result<MessageFrame<Header>, RpcError> {
        self.ensure_connected().await?;
        let client = self.inner.borrow().as_ref().unwrap().clone();
        client.send_request_vectored(frame).await
    }
}
