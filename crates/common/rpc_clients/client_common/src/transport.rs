use async_trait::async_trait;
use bytes::Bytes;
use std::cell::RefCell;
use std::io;
use std::os::fd::RawFd;
use std::sync::Arc;

#[async_trait]
pub trait RpcTransport: Send + Sync + 'static {
    async fn send(&self, fd: RawFd, header: Bytes, body: Bytes) -> io::Result<usize>;

    fn name(&self) -> &'static str;
}

thread_local! {
    static CURRENT_TRANSPORT: RefCell<Option<Arc<dyn RpcTransport>>> = RefCell::new(None);
}

pub fn set_current_transport(transport: Option<Arc<dyn RpcTransport>>) {
    CURRENT_TRANSPORT.with(|slot| {
        *slot.borrow_mut() = transport;
    });
}

pub fn current_transport() -> Option<Arc<dyn RpcTransport>> {
    CURRENT_TRANSPORT.with(|slot| slot.borrow().as_ref().map(Arc::clone))
}

pub fn clear_current_transport() {
    set_current_transport(None);
}
