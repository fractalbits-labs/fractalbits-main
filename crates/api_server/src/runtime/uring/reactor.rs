use super::ring::PerCoreRing;
use bytes::Bytes;
use core_affinity;
use crossbeam_channel::{Receiver, RecvTimeoutError, Sender, unbounded};
use libc;
use metrics::gauge;
use rpc_client_common::{IoUringTransport, MessageFrame, MessageHeaderTrait};
use std::cell::RefCell;
use std::collections::HashMap;
use std::io;
use std::os::fd::RawFd;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;
use tokio::sync::oneshot;
use tracing::{debug, info, warn};

#[derive(Debug)]
pub enum RpcTask {
    Noop,
    Send(SendTask),
    Recv(RecvTask),
}

#[derive(Debug)]
pub struct SendTask {
    pub fd: RawFd,
    pub header: Bytes,
    pub body: Bytes,
    pub completion: oneshot::Sender<io::Result<usize>>,
}

#[derive(Debug)]
pub struct RecvTask {
    pub fd: RawFd,
    pub len: usize,
    pub completion: oneshot::Sender<io::Result<Bytes>>,
}

#[derive(Debug, Default)]
struct ReactorMetrics {
    queue_depth: usize,
}

impl ReactorMetrics {
    fn update_queue_depth(&mut self, depth: usize, worker_index: usize) {
        self.queue_depth = depth;
        gauge!(
            "rpc_reactor_command_queue",
            "worker_index" => worker_index.to_string()
        )
        .set(depth as f64);
    }
}

pub struct RpcReactorHandle {
    worker_index: usize,
    sender: Sender<RpcCommand>,
    closed: AtomicBool,
    join_handle: Mutex<Option<JoinHandle<()>>>,
    io: Arc<ReactorIo>,
}

impl RpcReactorHandle {
    fn new(worker_index: usize, sender: Sender<RpcCommand>, io: Arc<ReactorIo>) -> Self {
        Self {
            worker_index,
            sender,
            closed: AtomicBool::new(false),
            join_handle: Mutex::new(None),
            io,
        }
    }

    pub fn worker_index(&self) -> usize {
        self.worker_index
    }

    pub fn command_sender(&self) -> Sender<RpcCommand> {
        self.sender.clone()
    }

    pub fn initiate_shutdown(&self) {
        if self.closed.swap(true, Ordering::AcqRel) {
            return;
        }
        if let Err(err) = self.sender.send(RpcCommand::Shutdown) {
            warn!(worker_index = self.worker_index, error = %err, "failed to send shutdown to rpc reactor");
        }
    }

    fn attach_join_handle(&self, join: JoinHandle<()>) {
        *self
            .join_handle
            .lock()
            .expect("reactor join handle poisoned") = Some(join);
    }

    pub fn submit_send(
        &self,
        fd: RawFd,
        header: Bytes,
        body: Bytes,
    ) -> oneshot::Receiver<io::Result<usize>> {
        let (tx, mut rx) = oneshot::channel();
        let header_len = header.len();
        let body_len = body.len();
        let task = RpcTask::Send(SendTask {
            fd,
            header,
            body,
            completion: tx,
        });
        debug!(
            worker_index = self.worker_index,
            fd, header_len, body_len, "enqueue send task"
        );
        if let Err(err) = self.sender.send(RpcCommand::Task(task)) {
            rx.close();
            warn!(
                worker_index = self.worker_index,
                error = %err,
                "failed to enqueue send task"
            );
        }
        rx
    }

    pub fn submit_recv(&self, fd: RawFd, len: usize) -> oneshot::Receiver<io::Result<Bytes>> {
        let (tx, mut rx) = oneshot::channel();
        if len == 0 {
            let _ = tx.send(Ok(Bytes::new()));
            return rx;
        }
        let task = RpcTask::Recv(RecvTask {
            fd,
            len,
            completion: tx,
        });
        debug!(
            worker_index = self.worker_index,
            fd, len, "enqueue recv task"
        );
        if let Err(err) = self.sender.send(RpcCommand::Task(task)) {
            rx.close();
            warn!(
                worker_index = self.worker_index,
                error = %err,
                "failed to enqueue recv task"
            );
        }
        rx
    }
}

impl Drop for RpcReactorHandle {
    fn drop(&mut self) {
        self.initiate_shutdown();
        if let Some(handle) = self
            .join_handle
            .lock()
            .expect("reactor join handle poisoned")
            .take()
        {
            handle.join().unwrap_or_else(|err| {
                warn!(
                    worker_index = self.worker_index,
                    "failed to join rpc reactor thread: {err:?}"
                );
            });
        }
    }
}

#[derive(Debug)]
pub enum RpcCommand {
    Shutdown,
    Task(RpcTask),
}

thread_local! {
    static CURRENT_REACTOR: RefCell<Option<ReactorTransport>> = const { RefCell::new(None) };
}

pub fn set_current_reactor(reactor: ReactorTransport) {
    use futures::FutureExt;
    use rpc_client_common::io_uring_support::{RecvFrameFunction, set_io_uring_recv_frame};

    let reactor_clone = reactor.clone();
    let recv_fn: RecvFrameFunction = Arc::new(move |fd, header_size, body_size| {
        let r = reactor_clone.clone();
        async move {
            if header_size > 0 {
                let header_bytes = r.recv_exact(fd, header_size).await?;
                if header_bytes.is_empty() {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "connection closed during header read",
                    ));
                }
                let body_bytes = if body_size > 0 {
                    r.recv_exact(fd, body_size).await?
                } else {
                    Bytes::new()
                };
                Ok((header_bytes, body_bytes))
            } else if body_size > 0 {
                let body_bytes = r.recv_exact(fd, body_size).await?;
                Ok((Bytes::new(), body_bytes))
            } else {
                Ok((Bytes::new(), Bytes::new()))
            }
        }
        .boxed()
    });

    set_io_uring_recv_frame(Some(recv_fn));

    CURRENT_REACTOR.with(|slot| {
        *slot.borrow_mut() = Some(reactor);
    });
}

pub fn get_current_reactor() -> Option<ReactorTransport> {
    CURRENT_REACTOR.with(|slot| slot.borrow().clone())
}

pub fn spawn_rpc_reactor(worker_index: usize, ring: Arc<PerCoreRing>) -> Arc<RpcReactorHandle> {
    let (tx, rx) = unbounded::<RpcCommand>();
    let io = Arc::new(ReactorIo::new(ring));
    let handle = Arc::new(RpcReactorHandle::new(worker_index, tx, io));
    let thread_handle = Arc::clone(&handle);
    let join = thread::Builder::new()
        .name(format!("rpc-reactor-{worker_index}"))
        .spawn(move || reactor_thread(thread_handle, rx))
        .expect("failed to spawn rpc reactor thread");
    handle.attach_join_handle(join);
    handle
}

fn reactor_thread(handle: Arc<RpcReactorHandle>, rx: Receiver<RpcCommand>) {
    let worker_index = handle.worker_index;

    if let Some(core_ids) = core_affinity::get_core_ids()
        && core_ids.len() > 1
    {
        let core_index = (worker_index % (core_ids.len() - 1)) + 1;
        let core = core_ids[core_index];
        if core_affinity::set_for_current(core) {
            info!(
                worker_index,
                core_id = core.id,
                "rpc reactor thread pinned to core (skipping core 0)"
            );
        } else {
            warn!(
                worker_index,
                core_id = core.id,
                "failed to pin rpc reactor thread to core"
            );
        }
    }

    info!(worker_index, "rpc reactor thread started");

    let mut running = true;
    let mut metrics = ReactorMetrics::default();
    let mut metric_update_counter = 0u64;
    const BATCH_SIZE: usize = 128;

    while running {
        let mut batch_count = 0;

        while let Ok(cmd) = rx.try_recv() {
            if !process_command(&handle, cmd) {
                running = false;
                break;
            }
            batch_count += 1;
            if batch_count >= BATCH_SIZE {
                break;
            }
        }

        if batch_count > 0
            && let Err(err) = handle.io.flush_submissions()
        {
            warn!(worker_index, error = %err, "failed to flush io_uring submissions");
        }

        handle.io.poll_completions(worker_index);

        if !running {
            break;
        }

        metric_update_counter += 1;
        if metric_update_counter.is_multiple_of(1000) {
            metrics.update_queue_depth(rx.len(), worker_index);
        }

        if batch_count == 0 {
            match rx.recv_timeout(Duration::from_micros(100)) {
                Ok(cmd) => {
                    if !process_command(&handle, cmd) {
                        running = false;
                    } else if let Err(err) = handle.io.flush_submissions() {
                        warn!(worker_index, error = %err, "failed to flush io_uring submissions");
                    }
                }
                Err(RecvTimeoutError::Timeout) => {
                    let shutdown_seen = handle.closed.load(Ordering::Acquire);
                    if shutdown_seen && !handle.io.has_pending_operations() {
                        running = false;
                    }
                }
                Err(RecvTimeoutError::Disconnected) => {
                    debug!(worker_index, "rpc reactor command channel closed");
                    running = false;
                }
            }
        }
    }

    handle.closed.store(true, Ordering::Release);
    info!(worker_index, "rpc reactor thread exiting");
}

fn process_command(handle: &Arc<RpcReactorHandle>, cmd: RpcCommand) -> bool {
    match cmd {
        RpcCommand::Shutdown => {
            debug!(
                worker_index = handle.worker_index,
                "rpc reactor received shutdown"
            );
            false
        }
        RpcCommand::Task(task) => {
            match task {
                RpcTask::Noop => {
                    debug!(
                        worker_index = handle.worker_index,
                        "rpc reactor handled noop task"
                    );
                }
                RpcTask::Send(task) => handle_send(handle, task),
                RpcTask::Recv(task) => handle_recv(handle, task),
            }
            true
        }
    }
}

fn handle_send(handle: &Arc<RpcReactorHandle>, task: SendTask) {
    let SendTask {
        fd,
        header,
        body,
        completion,
    } = task;

    if let Err(err) = handle
        .io
        .submit_send(handle.worker_index, fd, header, body, completion)
    {
        warn!(
            worker_index = handle.worker_index,
            fd,
            error = %err,
            "failed to submit send task"
        );
    }
}

fn handle_recv(handle: &Arc<RpcReactorHandle>, task: RecvTask) {
    let RecvTask {
        fd,
        len,
        completion,
    } = task;
    if let Err(err) = handle
        .io
        .submit_recv(handle.worker_index, fd, len, completion)
    {
        warn!(
            worker_index = handle.worker_index,
            fd,
            error = %err,
            "failed to submit recv task"
        );
    }
}

struct ReactorIo {
    ring: Arc<PerCoreRing>,
    next_uring_id: AtomicU64,
    pending_recv: Mutex<HashMap<u64, PendingRecv>>,
    pending_send: Mutex<HashMap<u64, PendingSend>>,
}

impl ReactorIo {
    fn new(ring: Arc<PerCoreRing>) -> Self {
        Self {
            ring,
            next_uring_id: AtomicU64::new(1),
            pending_recv: Mutex::new(HashMap::new()),
            pending_send: Mutex::new(HashMap::new()),
        }
    }

    fn submit_send(
        &self,
        worker_index: usize,
        fd: RawFd,
        header: Bytes,
        body: Bytes,
        completion: oneshot::Sender<io::Result<usize>>,
    ) -> io::Result<()> {
        let user_data = self.next_uring_id.fetch_add(1, Ordering::Relaxed);

        debug!(
            worker_index,
            fd,
            header_len = header.len(),
            body_len = body.len(),
            user_data,
            "submitting vectored send to io_uring"
        );

        let mut iovecs = Box::new([
            libc::iovec {
                iov_base: header.as_ptr() as *mut libc::c_void,
                iov_len: header.len(),
            },
            libc::iovec {
                iov_base: body.as_ptr() as *mut libc::c_void,
                iov_len: body.len(),
            },
        ]);

        let msg = Box::new(libc::msghdr {
            msg_name: std::ptr::null_mut(),
            msg_namelen: 0,
            msg_iov: iovecs.as_mut_ptr(),
            msg_iovlen: 2,
            msg_control: std::ptr::null_mut(),
            msg_controllen: 0,
            msg_flags: 0,
        });

        let expected_total = header.len() + body.len();

        let mut pending = self.pending_send.lock().expect("pending send map poisoned");

        pending.insert(
            user_data,
            PendingSend {
                fd,
                expected_total,
                _header: header,
                _body: body,
                _iovecs: SendableIoVec(iovecs),
                _msg: SendableMsgHdr(msg),
                completion,
            },
        );

        let pending_entry = pending.get(&user_data).unwrap();
        let iovecs_ptr = pending_entry._iovecs.0.as_ptr();
        let entry = io_uring::opcode::Writev::new(io_uring::types::Fd(fd), iovecs_ptr, 2)
            .build()
            .user_data(user_data);

        let submit_result: io::Result<()> = self.ring.with_lock(|ring| {
            unsafe {
                if ring.submission().push(&entry).is_err() {
                    ring.submit()?;
                    ring.submission()
                        .push(&entry)
                        .map_err(|_| io::Error::other("submission queue full after submit"))?;
                }
            }
            Ok(())
        });

        if let Err(err) = submit_result {
            if let Some(send) = pending.remove(&user_data) {
                let _ = send.completion.send(Err(err));
            }
            return Err(io::Error::other("failed to push send to submission queue"));
        }

        Ok(())
    }

    fn submit_recv(
        &self,
        worker_index: usize,
        fd: RawFd,
        len: usize,
        completion: oneshot::Sender<io::Result<Bytes>>,
    ) -> io::Result<()> {
        if len == 0 {
            let _ = completion.send(Ok(Bytes::new()));
            return Ok(());
        }
        let mut buffer = vec![0u8; len];
        let user_data = self.next_uring_id.fetch_add(1, Ordering::Relaxed);
        debug!(
            worker_index,
            fd, len, user_data, "submitting recv to io_uring"
        );
        let entry =
            io_uring::opcode::Recv::new(io_uring::types::Fd(fd), buffer.as_mut_ptr(), len as u32)
                .flags(libc::MSG_NOSIGNAL)
                .build()
                .user_data(user_data);

        let submit_result: io::Result<()> = self.ring.with_lock(|ring| {
            unsafe {
                if ring.submission().push(&entry).is_err() {
                    ring.submit()?;
                    ring.submission()
                        .push(&entry)
                        .map_err(|_| io::Error::other("submission queue full after submit"))?;
                }
            }
            Ok(())
        });

        match submit_result {
            Ok(_) => {
                let mut pending = self.pending_recv.lock().expect("pending recv map poisoned");
                pending.insert(
                    user_data,
                    PendingRecv {
                        fd,
                        buffer,
                        completion,
                    },
                );
                Ok(())
            }
            Err(err) => {
                let _ = completion.send(Err(err));
                Err(io::Error::other("failed to push recv to submission queue"))
            }
        }
    }

    fn poll_completions(&self, worker_index: usize) {
        let mut completions = Vec::new();
        self.ring.with_lock(|ring| {
            let mut cq = ring.completion();
            for cqe in &mut cq {
                if io_uring::cqueue::notif(cqe.flags()) {
                    continue;
                }
                completions.push((cqe.user_data(), cqe.result()));
            }
        });

        if completions.is_empty() {
            return;
        }

        let mut pending_recv = self.pending_recv.lock().expect("pending recv map poisoned");
        let mut pending_send = self.pending_send.lock().expect("pending send map poisoned");

        for (user_data, result) in completions {
            if user_data == 0 {
                debug!(
                    worker_index,
                    user_data, result, "completion without pending entry"
                );
                continue;
            }

            // Check if this is a recv completion
            if let Some(mut recv) = pending_recv.remove(&user_data) {
                if result < 0 {
                    let err = io::Error::from_raw_os_error(-result);
                    debug!(
                        worker_index,
                        fd = recv.fd,
                        user_data,
                        error = %err,
                        "recv completion with error"
                    );
                    let _ = recv.completion.send(Err(err));
                    continue;
                }
                let read = result as usize;
                recv.buffer.truncate(read);
                debug!(
                    worker_index,
                    fd = recv.fd,
                    user_data,
                    read,
                    "recv completion"
                );
                let bytes = Bytes::from(recv.buffer);
                let _ = recv.completion.send(Ok(bytes));
                continue;
            }

            // Check if this is a send completion
            if let Some(send) = pending_send.remove(&user_data) {
                if result < 0 {
                    let err = io::Error::from_raw_os_error(-result);
                    debug!(
                        worker_index,
                        fd = send.fd,
                        user_data,
                        error = %err,
                        "send completion with error"
                    );
                    let _ = send.completion.send(Err(err));
                    continue;
                }

                let written = result as usize;
                if written != send.expected_total {
                    warn!(
                        worker_index,
                        fd = send.fd,
                        user_data,
                        written,
                        expected = send.expected_total,
                        "partial send detected"
                    );
                    let _ = send.completion.send(Err(io::Error::new(
                        io::ErrorKind::WriteZero,
                        format!(
                            "partial send: wrote {} bytes, expected {}",
                            written, send.expected_total
                        ),
                    )));
                    continue;
                }

                debug!(
                    worker_index,
                    fd = send.fd,
                    user_data,
                    written,
                    "send completion"
                );
                let _ = send.completion.send(Ok(written));
                continue;
            }

            if !pending_recv.contains_key(&user_data) {
                warn!(
                    worker_index,
                    user_data, "received completion for unknown operation"
                );
            }
        }
    }

    fn has_pending_operations(&self) -> bool {
        let has_recv = !self
            .pending_recv
            .lock()
            .expect("pending recv map poisoned")
            .is_empty();
        let has_send = !self
            .pending_send
            .lock()
            .expect("pending send map poisoned")
            .is_empty();
        has_recv || has_send
    }

    fn flush_submissions(&self) -> io::Result<usize> {
        self.ring.with_lock(|ring| ring.submit())
    }
}

struct PendingRecv {
    fd: RawFd,
    buffer: Vec<u8>,
    completion: oneshot::Sender<io::Result<Bytes>>,
}

#[allow(dead_code)]
struct SendableIoVec(Box<[libc::iovec; 2]>);
unsafe impl Send for SendableIoVec {}

#[allow(dead_code)]
struct SendableMsgHdr(Box<libc::msghdr>);
unsafe impl Send for SendableMsgHdr {}

struct PendingSend {
    fd: RawFd,
    expected_total: usize,
    _header: Bytes,
    _body: Bytes,
    _iovecs: SendableIoVec,
    _msg: SendableMsgHdr,
    completion: oneshot::Sender<io::Result<usize>>,
}

#[derive(Clone)]
pub struct ReactorTransport {
    handle: Arc<RpcReactorHandle>,
}

impl ReactorTransport {
    pub fn new(handle: Arc<RpcReactorHandle>) -> Self {
        Self { handle }
    }
}

impl IoUringTransport for ReactorTransport {
    async fn send(&self, fd: RawFd, header: Bytes, body: Bytes) -> io::Result<usize> {
        let rx = self.handle.submit_send(fd, header, body);
        match rx.await {
            Ok(result) => result,
            Err(_) => Err(io::Error::new(
                io::ErrorKind::ConnectionAborted,
                "rpc reactor send cancelled",
            )),
        }
    }

    async fn recv_frame<H: MessageHeaderTrait>(&self, fd: RawFd) -> io::Result<MessageFrame<H>> {
        let header_size = H::SIZE;

        let header_bytes = self.recv_exact(fd, header_size).await?;

        if header_bytes.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "connection closed during header read",
            ));
        }

        if header_bytes.len() != header_size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "incomplete header: expected {}, got {}",
                    header_size,
                    header_bytes.len()
                ),
            ));
        }

        let header = H::decode(&header_bytes);
        let body_size = header.get_body_size();

        let body = if body_size == 0 {
            Bytes::new()
        } else {
            let body_bytes = self.recv_exact(fd, body_size).await?;

            if body_bytes.len() != body_size {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "incomplete body: expected {}, got {}",
                        body_size,
                        body_bytes.len()
                    ),
                ));
            }

            body_bytes
        };

        Ok(MessageFrame { header, body })
    }

    fn name(&self) -> &'static str {
        "reactor_io_uring"
    }
}

impl ReactorTransport {
    async fn recv_exact(&self, fd: RawFd, len: usize) -> io::Result<Bytes> {
        if len == 0 {
            return Ok(Bytes::new());
        }

        let mut total = 0usize;
        let mut buffer: Option<Vec<u8>> = None;

        while total < len {
            let remaining = len - total;
            let rx = self.handle.submit_recv(fd, remaining);
            let chunk = match rx.await {
                Ok(result) => result?,
                Err(_) => {
                    return Err(io::Error::new(
                        io::ErrorKind::ConnectionAborted,
                        "rpc reactor recv cancelled",
                    ));
                }
            };

            if chunk.is_empty() {
                if total == 0 {
                    return Ok(Bytes::new());
                }
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "connection closed before completing recv",
                ));
            }

            if total == 0 && chunk.len() == len {
                return Ok(chunk);
            }

            if buffer.is_none() {
                buffer = Some(vec![0u8; len]);
            }

            let buf = buffer.as_mut().expect("recv buffer initialized");
            let end = total + chunk.len();
            if end > len {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "recv received more data than requested",
                ));
            }
            buf[total..end].copy_from_slice(&chunk);
            total = end;
        }

        let buf = buffer.expect("recv buffer must be initialized");
        Ok(Bytes::from(buf))
    }
}
