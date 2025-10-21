use super::ring::PerCoreRing;
use crate::{MessageFrame, MessageHeaderTrait};
use bytes::Bytes;
use core_affinity::{self, CoreId};
use crossbeam_channel::{Receiver, Sender, unbounded};
use libc;
use metrics::gauge;
use std::cell::RefCell;
use std::collections::HashMap;
use std::io;
use std::os::fd::RawFd;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use tokio::sync::oneshot;
use tracing::{debug, error, info, warn};

#[derive(Debug)]
pub enum RpcTask {
    Noop,
    SendVectored(SendVectoredTask),
    Recv(RecvTask),
}

#[derive(Debug)]
pub struct SendVectoredTask {
    pub fd: RawFd,
    pub buffers: Vec<Bytes>,
    pub zero_copy: bool,
    pub completion: oneshot::Sender<io::Result<usize>>,
}

pub struct RecvTask {
    pub fd: RawFd,
    pub buffer_ptr: *mut u8,
    pub len: usize,
    pub completion: oneshot::Sender<io::Result<usize>>,
}

unsafe impl Send for RecvTask {}

impl std::fmt::Debug for RecvTask {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RecvTask")
            .field("fd", &self.fd)
            .field("buffer_ptr", &self.buffer_ptr)
            .field("len", &self.len)
            .field("completion", &"<oneshot::Sender>")
            .finish()
    }
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
    core_id: CoreId,
    sender: Sender<RpcCommand>,
    closed: AtomicBool,
    join_handle: Mutex<Option<JoinHandle<()>>>,
    io: Arc<ReactorIo>,
}

impl RpcReactorHandle {
    fn new(
        worker_index: usize,
        core_id: CoreId,
        sender: Sender<RpcCommand>,
        io: Arc<ReactorIo>,
    ) -> Self {
        Self {
            worker_index,
            core_id,
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

    pub fn submit_send_vectored(
        &self,
        fd: RawFd,
        buffers: Vec<Bytes>,
        zero_copy: bool,
    ) -> oneshot::Receiver<io::Result<usize>> {
        let (tx, mut rx) = oneshot::channel();
        let total_len: usize = buffers.iter().map(|b| b.len()).sum();
        let task = RpcTask::SendVectored(SendVectoredTask {
            fd,
            buffers,
            zero_copy,
            completion: tx,
        });
        debug!(
            worker_index = self.worker_index,
            fd, total_len, zero_copy, "enqueue vectored send task"
        );
        if let Err(err) = self.sender.send(RpcCommand::Task(task)) {
            rx.close();
            warn!(
                worker_index = self.worker_index,
                error = %err,
                "failed to enqueue vectored send task"
            );
        }
        rx
    }

    pub fn submit_recv(
        &self,
        fd: RawFd,
        buffer_ptr: *mut u8,
        len: usize,
    ) -> oneshot::Receiver<io::Result<usize>> {
        let (tx, mut rx) = oneshot::channel();
        if len == 0 {
            let _ = tx.send(Ok(0));
            return rx;
        }
        let task = RpcTask::Recv(RecvTask {
            fd,
            buffer_ptr,
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
    CURRENT_REACTOR.with(|slot| {
        *slot.borrow_mut() = Some(reactor);
    });
}

pub fn get_current_reactor() -> Option<ReactorTransport> {
    CURRENT_REACTOR.with(|slot| slot.borrow().clone())
}

pub fn spawn_rpc_reactor(
    worker_index: usize,
    core_id: CoreId,
    ring: Arc<PerCoreRing>,
) -> Arc<RpcReactorHandle> {
    let (tx, rx) = unbounded::<RpcCommand>();
    let io = Arc::new(ReactorIo::new(ring));
    let handle = Arc::new(RpcReactorHandle::new(worker_index, core_id, tx, io));
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
    let core_id = handle.core_id;
    if core_affinity::set_for_current(core_id) {
        info!(
            %worker_index,
            core_id = %core_id.id,
            "rpc reactor thread pinned to core"
        );
    } else {
        warn!(
            %worker_index,
            core_id = %core_id.id,
            "failed to pin rpc reactor thread to core"
        );
    }

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

        metric_update_counter += 1;
        if metric_update_counter.is_multiple_of(1000) {
            metrics.update_queue_depth(rx.len(), worker_index);
        }

        if batch_count > 0 {
            if let Err(err) = handle.io.flush_submissions() {
                warn!(worker_index, error = %err, "failed to flush io_uring submissions");
            }
        } else {
            if let Err(err) = handle.io.submit_and_wait_with_timeout(100_000) {
                warn!(worker_index, error = %err, "failed to wait on io_uring");
            }

            let shutdown_seen = handle.closed.load(Ordering::Acquire);
            if shutdown_seen && !handle.io.has_pending_operations() {
                running = false;
            }
        }

        handle.io.poll_completions(worker_index);
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
                RpcTask::SendVectored(task) => handle_send_vectored(handle, task),
                RpcTask::Recv(task) => handle_recv(handle, task),
            }
            true
        }
    }
}

fn handle_send_vectored(handle: &Arc<RpcReactorHandle>, task: SendVectoredTask) {
    let SendVectoredTask {
        fd,
        buffers,
        zero_copy,
        completion,
    } = task;

    if let Err(err) =
        handle
            .io
            .submit_send_vectored(handle.worker_index, fd, buffers, zero_copy, completion)
    {
        warn!(
            worker_index = handle.worker_index,
            fd,
            error = %err,
            "failed to submit vectored send task"
        );
    }
}

fn handle_recv(handle: &Arc<RpcReactorHandle>, task: RecvTask) {
    let RecvTask {
        fd,
        buffer_ptr,
        len,
        completion,
    } = task;
    if let Err(err) = handle
        .io
        .submit_recv(handle.worker_index, fd, buffer_ptr, len, completion)
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
    pending_send_vectored: Mutex<HashMap<u64, PendingSendVectored>>,
    pending_send_vectored_zc: Mutex<HashMap<u64, PendingSendVectoredZc>>,
}

impl ReactorIo {
    fn new(ring: Arc<PerCoreRing>) -> Self {
        Self {
            ring,
            next_uring_id: AtomicU64::new(1),
            pending_recv: Mutex::new(HashMap::new()),
            pending_send_vectored: Mutex::new(HashMap::new()),
            pending_send_vectored_zc: Mutex::new(HashMap::new()),
        }
    }

    fn submit_recv(
        &self,
        worker_index: usize,
        fd: RawFd,
        buffer_ptr: *mut u8,
        len: usize,
        completion: oneshot::Sender<io::Result<usize>>,
    ) -> io::Result<()> {
        debug_assert_ne!(len, 0, "submit_recv called with zero length");
        let user_data = self.next_uring_id.fetch_add(1, Ordering::Relaxed);
        debug!(
            worker_index,
            fd, len, user_data, "submitting recv to io_uring"
        );
        let entry = io_uring::opcode::Recv::new(io_uring::types::Fd(fd), buffer_ptr, len as u32)
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
                pending.insert(user_data, PendingRecv { fd, completion });
                Ok(())
            }
            Err(err) => {
                let _ = completion.send(Err(err));
                Err(io::Error::other("failed to push recv to submission queue"))
            }
        }
    }

    fn submit_send_vectored(
        &self,
        worker_index: usize,
        fd: RawFd,
        buffers: Vec<Bytes>,
        zero_copy: bool,
        completion: oneshot::Sender<io::Result<usize>>,
    ) -> io::Result<()> {
        let user_data = self.next_uring_id.fetch_add(1, Ordering::Relaxed);

        let total_len: usize = buffers.iter().map(|b| b.len()).sum();

        debug!(
            worker_index,
            fd,
            buffer_count = buffers.len(),
            total_len,
            zero_copy,
            user_data,
            "submitting vectored send to io_uring"
        );

        let iovecs: Vec<libc::iovec> = buffers
            .iter()
            .map(|buf| libc::iovec {
                iov_base: buf.as_ptr() as *mut libc::c_void,
                iov_len: buf.len(),
            })
            .collect();

        let iovecs = SendableIoVecSlice(iovecs.into_boxed_slice());

        if !zero_copy {
            let mut pending = self
                .pending_send_vectored
                .lock()
                .expect("pending send vectored map poisoned");

            pending.insert(
                user_data,
                PendingSendVectored {
                    fd,
                    _buffers: buffers,
                    _iovecs: iovecs,
                    completion,
                },
            );

            let pending_entry = pending.get(&user_data).unwrap();
            let iovecs_ptr = pending_entry._iovecs.0.as_ptr();
            let iovecs_len = pending_entry._iovecs.0.len();

            let entry = io_uring::opcode::Writev::new(
                io_uring::types::Fd(fd),
                iovecs_ptr,
                iovecs_len as u32,
            )
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
                return Err(io::Error::other(
                    "failed to push vectored send to submission queue",
                ));
            }
        } else {
            let mut pending = self
                .pending_send_vectored_zc
                .lock()
                .expect("pending send vectored zc map poisoned");

            let mut iovecs_mut = Box::new(
                iovecs
                    .0
                    .iter()
                    .map(|iov| libc::iovec {
                        iov_base: iov.iov_base,
                        iov_len: iov.iov_len,
                    })
                    .collect::<Vec<_>>(),
            );

            let msg = Box::new(libc::msghdr {
                msg_name: std::ptr::null_mut(),
                msg_namelen: 0,
                msg_iov: iovecs_mut.as_mut_ptr(),
                msg_iovlen: iovecs_mut.len(),
                msg_control: std::ptr::null_mut(),
                msg_controllen: 0,
                msg_flags: 0,
            });

            pending.insert(
                user_data,
                PendingSendVectoredZc {
                    fd,
                    expected_total: total_len,
                    _buffers: buffers,
                    _iovecs: SendableIoVecSlice(iovecs_mut.into_boxed_slice()),
                    _msg: SendableMsgHdr(msg),
                    completion,
                    data_completed: false,
                    send_result: None,
                },
            );

            let pending_entry = pending.get(&user_data).unwrap();
            let msg_ptr = pending_entry._msg.0.as_ref() as *const libc::msghdr;
            let entry = io_uring::opcode::SendMsgZc::new(io_uring::types::Fd(fd), msg_ptr)
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
                return Err(io::Error::other(
                    "failed to push zero-copy vectored send to submission queue",
                ));
            }
        }

        Ok(())
    }

    fn poll_completions(&self, worker_index: usize) {
        let mut completions = Vec::new();
        let mut notifications = Vec::new();
        self.ring.with_lock(|ring| {
            let mut cq = ring.completion();
            for cqe in &mut cq {
                if io_uring::cqueue::notif(cqe.flags()) {
                    notifications.push((cqe.user_data(), cqe.result()));
                } else {
                    completions.push((cqe.user_data(), cqe.result()));
                }
            }
        });

        if completions.is_empty() && notifications.is_empty() {
            return;
        }

        let mut pending_recv = self.pending_recv.lock().expect("pending recv map poisoned");
        let mut pending_send_vectored = self
            .pending_send_vectored
            .lock()
            .expect("pending send vectored map poisoned");
        let mut pending_send_vectored_zc = self
            .pending_send_vectored_zc
            .lock()
            .expect("pending send vectored zc map poisoned");

        for (user_data, result) in completions {
            if user_data == 0 {
                debug!(
                    worker_index,
                    user_data, result, "completion without pending entry"
                );
                continue;
            }

            // Check if this is a recv completion
            if let Some(recv) = pending_recv.remove(&user_data) {
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
                let bytes_read = result as usize;
                debug!(
                    worker_index,
                    fd = recv.fd,
                    user_data,
                    bytes_read,
                    "recv completion"
                );
                let _ = recv.completion.send(Ok(bytes_read));
                continue;
            }

            // Check if this is a vectored send completion
            if let Some(send) = pending_send_vectored.remove(&user_data) {
                if result < 0 {
                    let err = io::Error::from_raw_os_error(-result);
                    error!(
                        worker_index,
                        fd = send.fd,
                        user_data,
                        error = %err,
                        "vectored send completion with error"
                    );
                    let _ = send.completion.send(Err(err));
                    continue;
                }
                let bytes_written = result as usize;
                debug!(
                    worker_index,
                    fd = send.fd,
                    user_data,
                    bytes_written,
                    "vectored send completion"
                );
                let _ = send.completion.send(Ok(bytes_written));
                continue;
            }

            // Check if this is a zero-copy vectored send data completion (notification will come later)
            if let Some(send) = pending_send_vectored_zc.get_mut(&user_data) {
                if result < 0 {
                    let err = io::Error::from_raw_os_error(-result);
                    error!(
                        worker_index,
                        fd = send.fd,
                        user_data,
                        error = %err,
                        "zero-copy vectored send data completion with error"
                    );
                    send.send_result = Some(Err(err));
                    send.data_completed = true;
                    continue;
                }

                let written = result as usize;
                if written != send.expected_total {
                    error!(
                        worker_index,
                        fd = send.fd,
                        user_data,
                        written,
                        expected = send.expected_total,
                        "partial zero-copy vectored send detected"
                    );
                    send.send_result = Some(Err(io::Error::new(
                        io::ErrorKind::WriteZero,
                        format!(
                            "partial zero-copy vectored send: wrote {} bytes, expected {}",
                            written, send.expected_total
                        ),
                    )));
                    send.data_completed = true;
                    continue;
                }

                debug!(
                    worker_index,
                    fd = send.fd,
                    user_data,
                    written,
                    "zero-copy vectored send data completion (waiting for notification)"
                );
                send.send_result = Some(Ok(written));
                send.data_completed = true;
                continue;
            }

            if !pending_recv.contains_key(&user_data)
                && !pending_send_vectored.contains_key(&user_data)
                && !pending_send_vectored_zc.contains_key(&user_data)
            {
                warn!(
                    worker_index,
                    user_data, "received completion for unknown operation"
                );
            }
        }

        for (user_data, _result) in notifications {
            if user_data == 0 {
                debug!(
                    worker_index,
                    user_data, "notification without pending entry"
                );
                continue;
            }

            if let Some(send) = pending_send_vectored_zc.remove(&user_data) {
                debug!(
                    worker_index,
                    fd = send.fd,
                    user_data,
                    data_completed = send.data_completed,
                    "zero-copy vectored send notification received, kernel done with buffers"
                );

                if !send.data_completed {
                    warn!(
                        worker_index,
                        fd = send.fd,
                        user_data,
                        "received notification before data completion"
                    );
                }

                if let Some(result) = send.send_result {
                    let _ = send.completion.send(result);
                } else {
                    let _ = send.completion.send(Err(io::Error::other(
                        "notification received but no send result stored",
                    )));
                }
                continue;
            }

            warn!(
                worker_index,
                user_data, "received notification for unknown zero-copy operation"
            );
        }
    }

    fn has_pending_operations(&self) -> bool {
        let has_recv = !self
            .pending_recv
            .lock()
            .expect("pending recv map poisoned")
            .is_empty();
        let has_send_vectored = !self
            .pending_send_vectored
            .lock()
            .expect("pending send vectored map poisoned")
            .is_empty();
        let has_send_vectored_zc = !self
            .pending_send_vectored_zc
            .lock()
            .expect("pending send vectored zc map poisoned")
            .is_empty();
        has_recv || has_send_vectored || has_send_vectored_zc
    }

    fn flush_submissions(&self) -> io::Result<usize> {
        self.ring.with_lock(|ring| ring.submit())
    }

    fn submit_and_wait_with_timeout(&self, timeout_ns: u64) -> io::Result<()> {
        self.ring.with_lock(|ring| {
            let timeout_sec = timeout_ns / 1_000_000_000;
            let timeout_nsec = (timeout_ns % 1_000_000_000) as u32;
            let timeout_spec = io_uring::types::Timespec::new()
                .sec(timeout_sec)
                .nsec(timeout_nsec);

            unsafe {
                let timeout_sqe = io_uring::opcode::Timeout::new(&timeout_spec)
                    .build()
                    .user_data(0);

                if ring.submission().push(&timeout_sqe).is_err() {
                    ring.submit()?;
                    ring.submission()
                        .push(&timeout_sqe)
                        .map_err(|_| io::Error::other("failed to push timeout"))?;
                }
            }

            ring.submit_and_wait(1)?;
            Ok(())
        })
    }
}

struct PendingRecv {
    fd: RawFd,
    completion: oneshot::Sender<io::Result<usize>>,
}

#[allow(dead_code)]
struct SendableIoVec(Box<[libc::iovec; 2]>);
unsafe impl Send for SendableIoVec {}

#[allow(dead_code)]
struct SendableMsgHdr(Box<libc::msghdr>);
unsafe impl Send for SendableMsgHdr {}

#[allow(dead_code)]
struct SendableIoVecSlice(Box<[libc::iovec]>);
unsafe impl Send for SendableIoVecSlice {}

struct PendingSendVectored {
    fd: RawFd,
    _buffers: Vec<Bytes>,
    _iovecs: SendableIoVecSlice,
    completion: oneshot::Sender<io::Result<usize>>,
}

struct PendingSendVectoredZc {
    fd: RawFd,
    expected_total: usize,
    _buffers: Vec<Bytes>,
    _iovecs: SendableIoVecSlice,
    _msg: SendableMsgHdr,
    completion: oneshot::Sender<io::Result<usize>>,
    data_completed: bool,
    send_result: Option<io::Result<usize>>,
}

#[derive(Clone)]
pub struct ReactorTransport {
    handle: Arc<RpcReactorHandle>,
}

impl ReactorTransport {
    pub fn new(handle: Arc<RpcReactorHandle>) -> Self {
        Self { handle }
    }

    pub async fn send_vectored(
        &self,
        fd: RawFd,
        iov: &[io::IoSlice<'_>],
        zero_copy: bool,
    ) -> io::Result<usize> {
        let total_len: usize = iov.iter().map(|s| s.len()).sum();
        let mut total_written = 0;

        while total_written < total_len {
            let mut remaining_buffers = Vec::new();
            let mut current = 0;

            for slice in iov {
                let slice_end = current + slice.len();
                if total_written < slice_end {
                    let offset = total_written.saturating_sub(current);
                    remaining_buffers.push(Bytes::copy_from_slice(&slice[offset..]));
                }
                current = slice_end;
            }

            let rx = self
                .handle
                .submit_send_vectored(fd, remaining_buffers, zero_copy);
            let written = match rx.await {
                Ok(result) => result?,
                Err(_) => {
                    return Err(io::Error::new(
                        io::ErrorKind::ConnectionAborted,
                        "rpc reactor send_vectored cancelled",
                    ));
                }
            };

            if written == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "failed to write whole buffer",
                ));
            }

            total_written += written;
        }

        Ok(total_written)
    }

    pub async fn recv<H: MessageHeaderTrait>(&self, fd: RawFd) -> io::Result<MessageFrame<H>> {
        let header_size = H::SIZE;
        let header_vec = self.recv_exact(fd, header_size).await?;
        if header_vec.len() != header_size {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "connection closed during header read",
            ));
        }

        let header_bytes = Bytes::from(header_vec);
        let header = H::decode(&header_bytes);
        let body_size = header.get_body_size();
        let body = if body_size > 0 {
            let body_vec = self.recv_exact(fd, body_size).await?;

            if body_vec.len() != body_size {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "incomplete body: expected {}, got {}",
                        body_size,
                        body_vec.len()
                    ),
                ));
            }

            Bytes::from(body_vec)
        } else {
            Bytes::new()
        };

        Ok(MessageFrame { header, body })
    }

    async fn recv_exact(&self, fd: RawFd, len: usize) -> io::Result<Vec<u8>> {
        debug_assert_ne!(len, 0, "recv_exact called with zero length");
        let mut buffer: Vec<u8> = Vec::with_capacity(len);

        let mut total = 0usize;

        while total < len {
            let remaining = len - total;
            let buffer_ptr = unsafe { buffer.as_mut_ptr().add(total) };

            let rx = self.handle.submit_recv(fd, buffer_ptr, remaining);
            let bytes_read = match rx.await {
                Ok(result) => result?,
                Err(_) => {
                    return Err(io::Error::new(
                        io::ErrorKind::ConnectionAborted,
                        "rpc reactor recv cancelled",
                    ));
                }
            };

            if bytes_read == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "connection closed before completing recv",
                ));
            }

            total += bytes_read;
            if total > len {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "recv received more data than requested",
                ));
            }
        }

        unsafe {
            buffer.set_len(len);
        }
        Ok(buffer)
    }
}
