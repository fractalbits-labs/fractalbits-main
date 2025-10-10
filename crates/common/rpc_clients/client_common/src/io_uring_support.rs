use futures::future::BoxFuture;
use std::cell::RefCell;
use std::io::{self, IoSlice};
use std::os::fd::RawFd;
use std::sync::Arc;

pub trait ZeroCopySender: Send + Sync + 'static {
    fn send_vectored(&self, fd: RawFd, bufs: &[IoSlice<'_>]) -> io::Result<usize>;
}

thread_local! {
    static ZERO_COPY_SENDER: RefCell<Option<Arc<dyn ZeroCopySender>>> = RefCell::new(None);
}

pub fn set_current_zero_copy_sender(sender: Option<Arc<dyn ZeroCopySender>>) {
    ZERO_COPY_SENDER.with(|slot| {
        *slot.borrow_mut() = sender;
    });
}

pub fn current_zero_copy_sender() -> Option<Arc<dyn ZeroCopySender>> {
    ZERO_COPY_SENDER.with(|slot| slot.borrow().as_ref().map(Arc::clone))
}

pub fn clear_current_zero_copy_sender() {
    set_current_zero_copy_sender(None);
}

pub trait IoUringDriver: Send + Sync + 'static {
    fn recv(&self, fd: RawFd, len: usize) -> BoxFuture<'static, io::Result<Vec<u8>>>;
}

thread_local! {
    static IO_URING_DRIVER: RefCell<Option<Arc<dyn IoUringDriver>>> = RefCell::new(None);
}

pub fn set_current_io_uring_driver(driver: Option<Arc<dyn IoUringDriver>>) {
    IO_URING_DRIVER.with(|slot| {
        *slot.borrow_mut() = driver;
    });
}

pub fn current_io_uring_driver() -> Option<Arc<dyn IoUringDriver>> {
    IO_URING_DRIVER.with(|slot| slot.borrow().as_ref().map(Arc::clone))
}

pub fn clear_current_io_uring_driver() {
    set_current_io_uring_driver(None);
}
