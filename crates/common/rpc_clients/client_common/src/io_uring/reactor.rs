use super::ring::PerCoreRing;
use crate::{MessageFrame, MessageHeaderTrait};
use bytes::Bytes;
use core_affinity;
use crossbeam_channel::{Receiver, RecvTimeoutError, Sender, unbounded};
use libc;
use metrics::gauge;
use std::cell::RefCell;
use std::collections::HashMap;
use std::io;
use std::os::fd::RawFd;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;
use tokio::sync::oneshot;
use tracing::{debug, error, info, warn};

#[derive(Debug)]
pub enum RpcTask {
    Noop,
    Send(SendTask),
    SendZc(SendZcTask),
    Recv(RecvTask),
}

#[derive(Debug)]
pub struct SendTask {
    pub fd: RawFd,
    pub header: Bytes,
    pub body: Bytes,
    pub completion: oneshot::Sender<io::Result<()>>,
}

#[derive(Debug)]
pub struct SendZcTask {
    pub fd: RawFd,
    pub header: Bytes,
    pub body: Bytes,
    pub completion: oneshot::Sender<io::Result<()>>,
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
    ) -> oneshot::Receiver<io::Result<()>> {
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

    pub fn submit_send_zc(
        &self,
        fd: RawFd,
        header: Bytes,
        body: Bytes,
    ) -> oneshot::Receiver<io::Result<()>> {
        let (tx, mut rx) = oneshot::channel();
        let header_len = header.len();
        let body_len = body.len();
        let task = RpcTask::SendZc(SendZcTask {
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
                RpcTask::SendZc(task) => handle_send_zc(handle, task),
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

fn handle_send_zc(handle: &Arc<RpcReactorHandle>, task: SendZcTask) {
    let SendZcTask {
        fd,
        header,
        body,
        completion,
    } = task;

    if let Err(err) = handle
        .io
        .submit_send_zc(handle.worker_index, fd, header, body, completion)
    {
        warn!(
            worker_index = handle.worker_index,
            fd,
            error = %err,
            "failed to submit zero-copy send task"
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
    pending_send: Mutex<HashMap<u64, PendingSend>>,
    pending_send_zc: Mutex<HashMap<u64, PendingSendZc>>,
}

impl ReactorIo {
    fn new(ring: Arc<PerCoreRing>) -> Self {
        Self {
            ring,
            next_uring_id: AtomicU64::new(1),
            pending_recv: Mutex::new(HashMap::new()),
            pending_send: Mutex::new(HashMap::new()),
            pending_send_zc: Mutex::new(HashMap::new()),
        }
    }

    fn submit_send(
        &self,
        worker_index: usize,
        fd: RawFd,
        header: Bytes,
        body: Bytes,
        completion: oneshot::Sender<io::Result<()>>,
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

    fn submit_send_zc(
        &self,
        worker_index: usize,
        fd: RawFd,
        header: Bytes,
        body: Bytes,
        completion: oneshot::Sender<io::Result<()>>,
    ) -> io::Result<()> {
        let user_data = self.next_uring_id.fetch_add(1, Ordering::Relaxed);

        debug!(
            worker_index,
            fd,
            header_len = header.len(),
            body_len = body.len(),
            user_data,
            "submitting zero-copy send to io_uring"
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

        let mut pending = self
            .pending_send_zc
            .lock()
            .expect("pending send zc map poisoned");

        pending.insert(
            user_data,
            PendingSendZc {
                fd,
                expected_total,
                _header: header,
                _body: body,
                _iovecs: SendableIoVec(iovecs),
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
                "failed to push zero-copy send to submission queue",
            ));
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
        let mut pending_send = self.pending_send.lock().expect("pending send map poisoned");
        let mut pending_send_zc = self
            .pending_send_zc
            .lock()
            .expect("pending send zc map poisoned");

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

            // Check if this is a send completion
            if let Some(send) = pending_send.remove(&user_data) {
                if result < 0 {
                    let err = io::Error::from_raw_os_error(-result);
                    error!(
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
                    error!(
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
                let _ = send.completion.send(Ok(()));
                continue;
            }

            // Check if this is a zero-copy send data completion (notification will come later)
            if let Some(send) = pending_send_zc.get_mut(&user_data) {
                if result < 0 {
                    let err = io::Error::from_raw_os_error(-result);
                    error!(
                        worker_index,
                        fd = send.fd,
                        user_data,
                        error = %err,
                        "zero-copy send data completion with error"
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
                        "partial zero-copy send detected"
                    );
                    send.send_result = Some(Err(io::Error::new(
                        io::ErrorKind::WriteZero,
                        format!(
                            "partial zero-copy send: wrote {} bytes, expected {}",
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
                    "zero-copy send data completion (waiting for notification)"
                );
                send.send_result = Some(Ok(()));
                send.data_completed = true;
                continue;
            }

            if !pending_recv.contains_key(&user_data)
                && !pending_send.contains_key(&user_data)
                && !pending_send_zc.contains_key(&user_data)
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

            if let Some(send) = pending_send_zc.remove(&user_data) {
                debug!(
                    worker_index,
                    fd = send.fd,
                    user_data,
                    data_completed = send.data_completed,
                    "zero-copy send notification received, kernel done with buffers"
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
                    let _ = send.completion.send(Err(io::Error::new(
                        io::ErrorKind::Other,
                        "notification received but no send result stored",
                    )));
                }
            } else {
                warn!(
                    worker_index,
                    user_data, "received notification for unknown zero-copy send operation"
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
        let has_send_zc = !self
            .pending_send_zc
            .lock()
            .expect("pending send zc map poisoned")
            .is_empty();
        has_recv || has_send || has_send_zc
    }

    fn flush_submissions(&self) -> io::Result<usize> {
        self.ring.with_lock(|ring| ring.submit())
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

struct PendingSend {
    fd: RawFd,
    expected_total: usize,
    _header: Bytes,
    _body: Bytes,
    _iovecs: SendableIoVec,
    _msg: SendableMsgHdr,
    completion: oneshot::Sender<io::Result<()>>,
}

struct PendingSendZc {
    fd: RawFd,
    expected_total: usize,
    _header: Bytes,
    _body: Bytes,
    _iovecs: SendableIoVec,
    _msg: SendableMsgHdr,
    completion: oneshot::Sender<io::Result<()>>,
    data_completed: bool,
    send_result: Option<io::Result<()>>,
}

#[derive(Clone)]
pub struct ReactorTransport {
    handle: Arc<RpcReactorHandle>,
}

impl ReactorTransport {
    pub fn new(handle: Arc<RpcReactorHandle>) -> Self {
        Self { handle }
    }

    pub async fn send(&self, fd: RawFd, header: Bytes, body: Bytes) -> io::Result<()> {
        let rx = self.handle.submit_send(fd, header, body);
        match rx.await {
            Ok(result) => result,
            Err(_) => Err(io::Error::new(
                io::ErrorKind::ConnectionAborted,
                "rpc reactor send cancelled",
            )),
        }
    }

    pub async fn zero_copy_send(&self, fd: RawFd, header: Bytes, body: Bytes) -> io::Result<()> {
        let rx = self.handle.submit_send_zc(fd, header, body);
        match rx.await {
            Ok(result) => result,
            Err(_) => Err(io::Error::new(
                io::ErrorKind::ConnectionAborted,
                "rpc reactor zero-copy send cancelled",
            )),
        }
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
