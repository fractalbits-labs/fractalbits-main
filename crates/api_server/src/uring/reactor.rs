use super::ring::PerCoreRing;
use async_trait::async_trait;
use bytes::Bytes;
use crossbeam_channel::{Receiver, RecvTimeoutError, Sender, unbounded};
use libc;
use metrics::gauge;
use rpc_client_common::transport::RpcTransport;
use std::collections::{HashMap, HashSet};
use std::io;
use std::os::fd::RawFd;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;
use tokio::sync::oneshot;
use tracing::{debug, info, warn};

const RECV_BUFFER_SIZE: usize = 64 * 1024;

#[derive(Debug)]
pub enum RpcTask {
    Noop,
    ZeroCopySend(ZeroCopySendTask),
    Recv(RecvTask),
}

#[derive(Debug)]
pub struct ZeroCopySendTask {
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

    pub fn submit_zero_copy_send(
        &self,
        fd: RawFd,
        header: Bytes,
        body: Bytes,
    ) -> oneshot::Receiver<io::Result<usize>> {
        let (tx, mut rx) = oneshot::channel();
        let header_len = header.len();
        let body_len = body.len();
        let task = RpcTask::ZeroCopySend(ZeroCopySendTask {
            fd,
            header,
            body,
            completion: tx,
        });
        debug!(
            worker_index = self.worker_index,
            fd, header_len, body_len, "enqueue zero-copy send task"
        );
        if let Err(err) = self.sender.send(RpcCommand::Task(task)) {
            rx.close();
            warn!(
                worker_index = self.worker_index,
                error = %err,
                "failed to enqueue zero-copy send task"
            );
        }
        rx
    }

    pub fn submit_recv(&self, fd: RawFd, len: usize) -> oneshot::Receiver<io::Result<Bytes>> {
        let (tx, mut rx) = oneshot::channel();
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
    info!(
        worker_index = handle.worker_index,
        "rpc reactor thread started"
    );

    let mut running = true;
    let mut metrics = ReactorMetrics::default();

    while running {
        while let Ok(cmd) = rx.try_recv() {
            if !process_command(&handle, cmd) {
                running = false;
                break;
            }
        }

        metrics.update_queue_depth(rx.len(), handle.worker_index);

        if !running {
            break;
        }

        handle.io.poll_completions(handle.worker_index);

        if !running {
            break;
        }

        metrics.update_queue_depth(rx.len(), handle.worker_index);

        match rx.recv_timeout(Duration::from_millis(10)) {
            Ok(cmd) => {
                if !process_command(&handle, cmd) {
                    running = false;
                }
            }
            Err(RecvTimeoutError::Timeout) => {
                if handle.io.has_pending_operations() {
                    handle.io.poll_completions(handle.worker_index);
                }
                let shutdown_seen = handle.closed.load(Ordering::Acquire);
                if shutdown_seen && !handle.io.has_pending_operations() {
                    running = false;
                }
            }
            Err(RecvTimeoutError::Disconnected) => {
                debug!(
                    worker_index = handle.worker_index,
                    "rpc reactor command channel closed"
                );
                running = false;
            }
        }
    }

    handle.closed.store(true, Ordering::Release);
    info!(
        worker_index = handle.worker_index,
        "rpc reactor thread exiting"
    );
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
                RpcTask::ZeroCopySend(task) => handle_zero_copy_send(handle, task),
                RpcTask::Recv(task) => handle_recv(handle, task),
            }
            true
        }
    }
}

fn handle_zero_copy_send(handle: &Arc<RpcReactorHandle>, task: ZeroCopySendTask) {
    let ZeroCopySendTask {
        fd,
        header,
        body,
        completion,
    } = task;

    debug!(
        worker_index = handle.worker_index,
        fd,
        header_len = header.len(),
        body_len = body.len(),
        "enqueuing async zero-copy send"
    );

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
    zerocopy_fds: Mutex<HashSet<RawFd>>,
}

impl ReactorIo {
    fn new(ring: Arc<PerCoreRing>) -> Self {
        Self {
            ring,
            next_uring_id: AtomicU64::new(1),
            pending_recv: Mutex::new(HashMap::new()),
            pending_send: Mutex::new(HashMap::new()),
            zerocopy_fds: Mutex::new(HashSet::new()),
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
        self.ensure_zero_copy(fd)?;

        let header_user_data = self.next_uring_id.fetch_add(1, Ordering::Relaxed);

        debug!(
            worker_index,
            fd,
            header_len = header.len(),
            body_len = body.len(),
            header_user_data,
            "submitting send to io_uring"
        );

        // Submit header send
        let header_entry = io_uring::opcode::SendZc::new(
            io_uring::types::Fd(fd),
            header.as_ptr(),
            header.len() as u32,
        )
        .flags(libc::MSG_ZEROCOPY | libc::MSG_NOSIGNAL)
        .build()
        .user_data(header_user_data);

        // Prepare body entry if needed
        let body_user_data = if !body.is_empty() {
            Some(self.next_uring_id.fetch_add(1, Ordering::Relaxed))
        } else {
            None
        };

        let submit_result = self.ring.with_lock(|ring| {
            unsafe {
                ring.submission()
                    .push(&header_entry)
                    .map_err(|_| io::Error::other("submission queue full"))?;
            }

            // If there's a body, submit it too
            if let Some(body_user_data) = body_user_data {
                let body_entry = io_uring::opcode::SendZc::new(
                    io_uring::types::Fd(fd),
                    body.as_ptr(),
                    body.len() as u32,
                )
                .flags(libc::MSG_ZEROCOPY | libc::MSG_NOSIGNAL)
                .build()
                .user_data(body_user_data);

                unsafe {
                    ring.submission()
                        .push(&body_entry)
                        .map_err(|_| io::Error::other("submission queue full"))?;
                }
            }

            ring.submit()
        });

        match submit_result {
            Ok(_) => {
                // Store in pending map after successful submission
                let mut pending = self.pending_send.lock().expect("pending send map poisoned");
                pending.insert(
                    header_user_data,
                    PendingSend {
                        fd,
                        _header: header,
                        body: body_user_data.map(|id| (body, id)),
                        completion,
                        header_result: None,
                        body_result: None,
                    },
                );
                Ok(())
            }
            Err(err) => {
                let send_err = io::Error::new(err.kind(), err.to_string());
                let _ = completion.send(Err(send_err));
                Err(err)
            }
        }
    }

    fn submit_recv(
        &self,
        worker_index: usize,
        fd: RawFd,
        len: usize,
        completion: oneshot::Sender<io::Result<Bytes>>,
    ) -> io::Result<()> {
        let requested = len.max(RECV_BUFFER_SIZE);
        let mut buffer = vec![0u8; requested];
        let user_data = self.next_uring_id.fetch_add(1, Ordering::Relaxed);
        debug!(
            worker_index,
            fd, requested, user_data, "submitting recv to io_uring"
        );
        let entry = io_uring::opcode::Recv::new(
            io_uring::types::Fd(fd),
            buffer.as_mut_ptr(),
            requested as u32,
        )
        .flags(libc::MSG_NOSIGNAL)
        .build()
        .user_data(user_data);

        let submit_result = self.ring.with_lock(|ring| {
            unsafe {
                ring.submission()
                    .push(&entry)
                    .map_err(|_| io::Error::other("submission queue full"))?;
            }
            ring.submit()
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
                let send_err = io::Error::new(err.kind(), err.to_string());
                let _ = completion.send(Err(send_err));
                Err(err)
            }
        }
    }

    fn ensure_zero_copy(&self, fd: RawFd) -> io::Result<()> {
        let mut configured = self.zerocopy_fds.lock().expect("zero-copy fd set poisoned");

        if configured.contains(&fd) {
            return Ok(());
        }

        let enable: libc::c_int = 1;
        let ret = unsafe {
            libc::setsockopt(
                fd,
                libc::SOL_SOCKET,
                libc::SO_ZEROCOPY,
                &enable as *const _ as *const libc::c_void,
                std::mem::size_of_val(&enable) as libc::socklen_t,
            )
        };
        if ret == -1 {
            return Err(io::Error::last_os_error());
        }

        configured.insert(fd);
        Ok(())
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

            // Check if this is a send completion (header)
            if let Some(send) = pending_send.get_mut(&user_data) {
                if result < 0 {
                    let err = io::Error::from_raw_os_error(-result);
                    debug!(
                        worker_index,
                        fd = send.fd,
                        user_data,
                        error = %err,
                        "send header completion with error"
                    );
                    let send = pending_send.remove(&user_data).unwrap();
                    let _ = send.completion.send(Err(err));
                    continue;
                }

                send.header_result = Some(result as usize);
                debug!(
                    worker_index,
                    fd = send.fd,
                    user_data,
                    written = result,
                    "send header completion"
                );

                // Check if we're done (no body or body also completed)
                if send.body.is_none() || send.body_result.is_some() {
                    let send = pending_send.remove(&user_data).unwrap();
                    let header_written = send.header_result.unwrap_or(0);
                    let total = header_written + send.body_result.unwrap_or(0);
                    let _ = send.completion.send(Ok(total));
                }
                continue;
            }

            // Check if this is a send body completion
            let mut header_to_complete = None;
            for (header_user_data, send) in pending_send.iter_mut() {
                match send.body.as_ref() {
                    Some((_, body_user_data)) if *body_user_data == user_data => {
                        if result < 0 {
                            let err = io::Error::from_raw_os_error(-result);
                            debug!(
                                worker_index,
                                fd = send.fd,
                                user_data,
                                error = %err,
                                "send body completion with error"
                            );
                            header_to_complete = Some((*header_user_data, Err(err)));
                        } else {
                            let written = result as usize;
                            send.body_result = Some(written);
                            debug!(
                                worker_index,
                                fd = send.fd,
                                user_data,
                                written = result,
                                "send body completion"
                            );

                            if let Some(header_written) = send.header_result {
                                header_to_complete =
                                    Some((*header_user_data, Ok(header_written + written)));
                            }
                        }
                        break;
                    }
                    _ => continue,
                }
            }

            if let Some((header_user_data, result)) = header_to_complete {
                let send = pending_send.remove(&header_user_data).unwrap();
                let _ = send.completion.send(result);
            } else if !pending_send.contains_key(&user_data)
                && !pending_recv.contains_key(&user_data)
            {
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
}

struct PendingRecv {
    fd: RawFd,
    buffer: Vec<u8>,
    completion: oneshot::Sender<io::Result<Bytes>>,
}

struct PendingSend {
    fd: RawFd,
    _header: Bytes,             // hold header buffer alive until completion
    body: Option<(Bytes, u64)>, // (body_bytes, body_user_data)
    completion: oneshot::Sender<io::Result<usize>>,
    header_result: Option<usize>,
    body_result: Option<usize>,
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

#[async_trait]
impl RpcTransport for ReactorTransport {
    async fn send(&self, fd: RawFd, header: Bytes, body: Bytes) -> io::Result<usize> {
        let rx = self.handle.submit_zero_copy_send(fd, header, body);
        match rx.await {
            Ok(result) => result,
            Err(_) => Err(io::Error::new(
                io::ErrorKind::ConnectionAborted,
                "rpc reactor send cancelled",
            )),
        }
    }

    async fn recv(&self, fd: RawFd, len: usize) -> io::Result<Bytes> {
        let rx = self.handle.submit_recv(fd, len);
        match rx.await {
            Ok(result) => result,
            Err(_) => Err(io::Error::new(
                io::ErrorKind::ConnectionAborted,
                "rpc reactor recv cancelled",
            )),
        }
    }

    fn name(&self) -> &'static str {
        "reactor_io_uring"
    }
}
