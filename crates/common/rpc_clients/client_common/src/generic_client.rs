use crate::RpcError;
use bytes::{Bytes, BytesMut};
use metrics::{counter, gauge};
use parking_lot::Mutex;
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
use tokio::sync::{
    mpsc::{self, Receiver, Sender},
    oneshot,
};
use tokio::task::AbortHandle;
use tokio_retry::{
    Retry,
    strategy::{FixedInterval, jitter},
};
use tracing::{debug, error, warn};

type ZcMessageFrame<Header> = MessageFrame<Header, Vec<Bytes>>;
type RequestMap<Header> = Arc<Mutex<HashMap<u32, oneshot::Sender<MessageFrame<Header>>>>>;

pub trait RpcCodec<Header: MessageHeaderTrait>: Default + Clone + Send + Sync + 'static {
    const RPC_TYPE: &'static str;
}

pub struct RpcClient<Codec: RpcCodec<Header>, Header: MessageHeaderTrait> {
    requests: RequestMap<Header>,
    sender: Sender<ZcMessageFrame<Header>>,
    send_task_handle: AbortHandle,
    recv_task_handle: AbortHandle,
    socket_fd: RawFd,
    is_closed: Arc<AtomicBool>,
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
        debug!(rpc_type = Codec::RPC_TYPE, socket_fd = %self.socket_fd, "RpcClient dropped, aborting tasks");
        self.send_task_handle.abort();
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

    async fn new_internal_tokio(stream: tokio::net::TcpStream) -> Result<Self, RpcError> {
        let rpc_type = Codec::RPC_TYPE;
        let socket_fd = stream.as_raw_fd();
        let (reader, writer) = stream.into_split();
        let requests: Arc<Mutex<HashMap<u32, oneshot::Sender<MessageFrame<Header>>>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let (sender, receiver) = mpsc::channel::<ZcMessageFrame<Header>>(1024 * 32);
        let is_closed = Arc::new(AtomicBool::new(false));

        // Send task
        let send_handle = {
            let sender_requests = requests.clone();
            let is_closed = is_closed.clone();
            tokio::spawn(async move {
                if let Err(e) = Self::send_task_tokio(writer, receiver, socket_fd, rpc_type).await {
                    warn!(%rpc_type, %socket_fd, %e, "send task failed");
                }
                is_closed.store(true, Ordering::SeqCst);
                Self::drain_pending_requests(socket_fd, &sender_requests, DrainFrom::SendTask);
            })
            .abort_handle()
        };

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
            sender,
            send_task_handle: send_handle,
            recv_task_handle: recv_handle,
            socket_fd,
            is_closed,
            _phantom: PhantomData,
        })
    }

    async fn send_task_tokio(
        mut writer: tokio::net::tcp::OwnedWriteHalf,
        mut receiver: Receiver<ZcMessageFrame<Header>>,
        socket_fd: RawFd,
        rpc_type: &'static str,
    ) -> Result<(), RpcError> {
        use tokio::io::AsyncWriteExt;

        const MAX_BATCH_SIZE: usize = 32;
        let mut batch = Vec::with_capacity(MAX_BATCH_SIZE);
        let mut encoded_frames: Vec<Vec<Bytes>> = Vec::with_capacity(MAX_BATCH_SIZE);

        loop {
            batch.clear();
            let count = receiver.recv_many(&mut batch, MAX_BATCH_SIZE).await;
            if count == 0 {
                break;
            }

            gauge!("rpc_request_pending_in_send_queue", "type" => rpc_type).decrement(count as f64);
            counter!("rpc_send_batch_size", "type" => rpc_type).increment(count as u64);

            encoded_frames.clear();
            for mut frame in batch.drain(..) {
                let request_id = frame.header.get_id();
                debug!(%rpc_type, %socket_fd, %request_id, "sending request");

                frame.header.set_checksum();
                let mut header_buf = BytesMut::with_capacity(Header::SIZE);
                frame.header.encode(&mut header_buf);

                let mut all_chunks = vec![header_buf.freeze()];
                all_chunks.extend(frame.body);
                encoded_frames.push(all_chunks);
            }

            let total_len: usize = encoded_frames
                .iter()
                .map(|chunks| chunks.iter().map(|chunk| chunk.len()).sum::<usize>())
                .sum();
            let mut total_written = 0;

            while total_written < total_len {
                let mut iov = Vec::with_capacity(encoded_frames.len() * 2);
                let mut current_offset = 0;

                for chunks in &encoded_frames {
                    let frame_len: usize = chunks.iter().map(|chunk| chunk.len()).sum();
                    let frame_start = current_offset;
                    let frame_end = frame_start + frame_len;

                    if total_written < frame_end {
                        let skip_in_frame = total_written.saturating_sub(frame_start);
                        let mut accumulated = 0;

                        for chunk in chunks {
                            let chunk_start = accumulated;
                            let chunk_end = chunk_start + chunk.len();

                            if skip_in_frame < chunk_end {
                                let skip_in_chunk = skip_in_frame.saturating_sub(chunk_start);
                                iov.push(IoSlice::new(&chunk[skip_in_chunk..]));
                            }

                            accumulated = chunk_end;
                        }
                    }

                    current_offset = frame_end;
                }

                let written = writer
                    .write_vectored(&iov)
                    .await
                    .map_err(RpcError::IoError)?;

                if written == 0 {
                    return Err(RpcError::IoError(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "failed to write whole buffer",
                    )));
                }

                total_written += written;
            }

            counter!("rpc_request_sent", "type" => rpc_type, "name" => "all")
                .increment(encoded_frames.len() as u64);
        }

        warn!(%rpc_type, %socket_fd, "sender closed, send message task quit");
        Ok(())
    }

    async fn receive_task_tokio(
        mut receiver: tokio::net::tcp::OwnedReadHalf,
        requests: &RequestMap<Header>,
        socket_fd: RawFd,
        rpc_type: &'static str,
    ) -> Result<(), RpcError> {
        use tokio::io::AsyncReadExt;

        let header_size = Header::SIZE;

        loop {
            // Read fixed-size header into stack buffer
            let mut header_buf = [0u8; 256];
            let header = match receiver.read_exact(&mut header_buf[..header_size]).await {
                Ok(_) => {
                    let header = Header::decode(&header_buf[..header_size]);
                    if !header.verify_checksum() {
                        warn!(%rpc_type, %socket_fd, "header checksum verification failed");
                        return Err(RpcError::ChecksumMismatch);
                    }
                    header
                }
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    warn!(%rpc_type, %socket_fd, "connection closed, receive message task quit");
                    return Ok(());
                }
                Err(e) => return Err(RpcError::IoError(e)),
            };

            // Read body directly into uninitialized buffer to avoid memset overhead
            let body_size = header.get_body_size();
            let body = if body_size > 0 {
                let mut body_buf = Vec::<u8>::with_capacity(body_size);
                // Safety: We create an uninitialized buffer and read data directly into it.
                // This is safe because:
                // 1. The buffer has allocated capacity >= body_size
                // 2. read_exact guarantees it fills the entire buffer or returns an error
                // 3. We only set_len after read_exact succeeds, ensuring all bytes are initialized
                unsafe {
                    let buf_ptr = body_buf.as_mut_ptr();
                    let slice = std::slice::from_raw_parts_mut(buf_ptr, body_size);
                    receiver.read_exact(slice).await?;
                    body_buf.set_len(body_size);
                }
                Bytes::from(body_buf)
            } else {
                bytes::Bytes::new()
            };

            // Verify header checksum before processing the frame
            if !header.verify_checksum() {
                error!(%rpc_type, %socket_fd, request_id = %header.get_id(),
                    "Response header checksum verification failed, dropping response");
                counter!("rpc_response_checksum_failed", "type" => rpc_type).increment(1);
                continue;
            }

            let frame = MessageFrame::new(header, body);
            Self::handle_incoming_frame(frame, requests, socket_fd, rpc_type);
        }
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

    pub async fn send_request(
        &self,
        request_id: u32,
        mut frame: MessageFrame<Header, Bytes>,
        timeout: Option<std::time::Duration>,
        trace_id: Option<u64>,
    ) -> Result<MessageFrame<Header>, RpcError> {
        if let Some(trace_id) = trace_id {
            frame.header.set_trace_id(trace_id);
        }
        let vectored_frame = MessageFrame::new(frame.header, vec![frame.body]);
        self.send_request_vectored_internal(request_id, vectored_frame, timeout)
            .await
    }

    pub async fn send_request_vectored(
        &self,
        request_id: u32,
        mut frame: MessageFrame<Header, Vec<Bytes>>,
        timeout: Option<std::time::Duration>,
        trace_id: Option<u64>,
    ) -> Result<MessageFrame<Header>, RpcError> {
        if let Some(trace_id) = trace_id {
            frame.header.set_trace_id(trace_id);
        }
        self.send_request_vectored_internal(request_id, frame, timeout)
            .await
    }

    async fn send_request_vectored_internal(
        &self,
        request_id: u32,
        mut frame: ZcMessageFrame<Header>,
        timeout: Option<Duration>,
    ) -> Result<MessageFrame<Header>, RpcError> {
        if self.is_closed.load(Ordering::SeqCst) {
            return Err(RpcError::InternalRequestError(
                "Connection is closed".into(),
            ));
        }

        let rpc_type = Codec::RPC_TYPE;
        frame.header.set_id(request_id);
        frame.header.set_retry_count(0);

        let (tx, rx) = oneshot::channel();
        self.requests.lock().insert(request_id, tx);
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

    pub fn is_closed(&self) -> bool {
        self.is_closed.load(Ordering::SeqCst)
    }
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

    pub async fn establish_connection(addr: String) -> Result<Self, RpcError>
    where
        Header: Default,
    {
        const MAX_CONNECTION_RETRIES: usize = 100 * 3600 * 24;
        let start = std::time::Instant::now();
        let retry_strategy = FixedInterval::from_millis(10)
            .map(jitter)
            .take(MAX_CONNECTION_RETRIES);

        debug!(rpc_type=%Codec::RPC_TYPE, %addr, "Trying to connect to rpc server");

        let client = Retry::spawn(retry_strategy, || async {
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
        .await
        .map_err(|e| {
            warn!(rpc_type = %Codec::RPC_TYPE, addr = %addr, error = %e, "failed to connect RPC server");
            RpcError::IoError(io::Error::other(e.to_string()))
        })?;

        let duration = start.elapsed();
        if duration > Duration::from_secs(1) {
            warn!(
                rpc_type = %Codec::RPC_TYPE,
                addr = %addr,
                duration_ms = %duration.as_millis(),
                "Slow connection establishment to RPC server"
            );
        } else if duration > Duration::from_millis(100) {
            debug!(
                rpc_type = %Codec::RPC_TYPE,
                addr = %addr,
                duration_ms = %duration.as_millis(),
                "Connection established to RPC server"
            );
        }

        Ok(client)
    }
}
