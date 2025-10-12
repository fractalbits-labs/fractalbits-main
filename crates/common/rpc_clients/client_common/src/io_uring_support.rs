use futures::future::BoxFuture;
use std::cell::RefCell;
use std::io::{self};
use std::os::fd::RawFd;
use std::sync::Arc;

use bytes::Bytes;

pub type RecvFrameFunction = Arc<
    dyn Fn(RawFd, usize, usize) -> BoxFuture<'static, io::Result<(Bytes, Bytes)>> + Send + Sync,
>;

thread_local! {
    static IO_URING_RECV_FRAME: RefCell<Option<RecvFrameFunction>> = const { RefCell::new(None) };
}

pub fn set_io_uring_recv_frame(func: Option<RecvFrameFunction>) {
    IO_URING_RECV_FRAME.with(|slot| {
        *slot.borrow_mut() = func;
    });
}

pub fn get_io_uring_recv_frame() -> Option<RecvFrameFunction> {
    IO_URING_RECV_FRAME.with(|slot| slot.borrow().as_ref().map(Arc::clone))
}
