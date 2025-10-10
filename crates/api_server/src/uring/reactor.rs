use super::{ring::PerCoreRing, rpc::UringRpcSender};
use bytes::Bytes;
use crossbeam_channel::{Receiver, Sender, select, unbounded};
use metrics::gauge;
use rpc_client_common::transport::RpcTransport;
use std::io;
use std::os::fd::RawFd;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;
use tokio::sync::oneshot;
use tracing::{debug, info, warn};

#[derive(Debug)]
pub enum RpcTask {
    Noop,
    ZeroCopySend(ZeroCopySendTask),
}

#[derive(Debug)]
pub struct ZeroCopySendTask {
    pub fd: RawFd,
    pub header: Bytes,
    pub body: Bytes,
    pub completion: oneshot::Sender<io::Result<usize>>,
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

#[derive(Debug)]
pub enum RpcCommand {
    Shutdown,
    Task(RpcTask),
}

pub struct RpcReactorHandle {
    worker_index: usize,
    sender: Sender<RpcCommand>,
    closed: AtomicBool,
    join_handle: Mutex<Option<JoinHandle<()>>>,
    zero_copy: Arc<UringRpcSender>,
}

impl RpcReactorHandle {
    fn new(
        worker_index: usize,
        sender: Sender<RpcCommand>,
        zero_copy: Arc<UringRpcSender>,
    ) -> Self {
        Self {
            worker_index,
            sender,
            closed: AtomicBool::new(false),
            join_handle: Mutex::new(None),
            zero_copy,
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
        let task = RpcTask::ZeroCopySend(ZeroCopySendTask {
            fd,
            header,
            body,
            completion: tx,
        });
        if let Err(err) = self.sender.send(RpcCommand::Task(task)) {
            let _ = rx.close();
            warn!(
                worker_index = self.worker_index,
                error = %err,
                "failed to enqueue zero-copy send task"
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
            if let Err(err) = handle.join() {
                warn!(
                    worker_index = self.worker_index,
                    "failed to join rpc reactor thread: {err:?}"
                );
            }
        }
    }
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

use async_trait::async_trait;

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

    fn name(&self) -> &'static str {
        "reactor_zero_copy"
    }
}

pub fn spawn_rpc_reactor(worker_index: usize, ring: Arc<PerCoreRing>) -> Arc<RpcReactorHandle> {
    let (tx, rx) = unbounded::<RpcCommand>();
    let zero_copy = UringRpcSender::new(ring.clone());
    let handle = Arc::new(RpcReactorHandle::new(worker_index, tx, zero_copy));
    let thread_handle = Arc::clone(&handle);
    let join = thread::Builder::new()
        .name(format!("rpc-reactor-{worker_index}"))
        .spawn(move || reactor_thread(thread_handle, ring, rx))
        .expect("failed to spawn rpc reactor thread");
    handle.attach_join_handle(join);
    handle
}

fn reactor_thread(handle: Arc<RpcReactorHandle>, ring: Arc<PerCoreRing>, rx: Receiver<RpcCommand>) {
    info!(
        worker_index = handle.worker_index,
        "rpc reactor thread started"
    );
    let _ring = ring; // actual IO submission will arrive in later stages.

    let mut running = true;
    let mut shutdown_seen = false;
    let mut metrics = ReactorMetrics::default();

    while running {
        // Drain any already queued commands before we block again.
        while let Ok(cmd) = rx.try_recv() {
            if !process_command(&handle, &_ring, cmd) {
                running = false;
                break;
            }
        }

        metrics.update_queue_depth(rx.len(), handle.worker_index);

        if !running {
            break;
        }

        select! {
            recv(rx) -> msg => match msg {
                Ok(cmd) => {
                    if !process_command(&handle, &_ring, cmd) {
                        running = false;
                    }
                }
                Err(_) => {
                    debug!(worker_index = handle.worker_index, "rpc reactor command channel closed");
                    running = false;
                }
            },
            default(Duration::from_millis(50)) => {
                if shutdown_seen {
                    running = false;
                }
                // Future: poll completion queue here when submissions exist.
            }
        }

        shutdown_seen = handle.closed.load(Ordering::Acquire);
    }

    handle.closed.store(true, Ordering::Release);
    info!(
        worker_index = handle.worker_index,
        "rpc reactor thread exiting"
    );
}

fn process_command(
    handle: &Arc<RpcReactorHandle>,
    ring: &Arc<PerCoreRing>,
    cmd: RpcCommand,
) -> bool {
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
                RpcTask::ZeroCopySend(task) => {
                    handle_zero_copy_send(handle, ring, task);
                }
            }
            true
        }
    }
}

fn handle_zero_copy_send(
    handle: &Arc<RpcReactorHandle>,
    _ring: &Arc<PerCoreRing>,
    task: ZeroCopySendTask,
) {
    let ZeroCopySendTask {
        fd,
        header,
        body,
        completion,
    } = task;

    let mut total_written = 0usize;
    let result = handle
        .zero_copy
        .send_slice(fd, header.as_ref())
        .and_then(|written| {
            total_written += written;
            if body.is_empty() {
                Ok(0)
            } else {
                handle
                    .zero_copy
                    .send_slice(fd, body.as_ref())
                    .map(|written| written as i64)
            }
        })
        .map(|additional| {
            if additional > 0 {
                total_written += additional as usize;
            }
            total_written
        });

    if completion.send(result).is_err() {
        debug!(
            worker_index = handle.worker_index,
            fd, "zero-copy send completion receiver dropped"
        );
    }
}
