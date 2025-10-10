use crate::runtime::uring::config::UringConfig;
use parking_lot::Mutex;
use std::io;
use std::sync::Arc;

pub type SharedRing = Arc<Mutex<io_uring::IoUring>>;

pub struct PerCoreRing {
    worker_index: usize,
    inner: SharedRing,
    config: UringConfig,
}

impl PerCoreRing {
    pub fn new(worker_index: usize, config: &UringConfig) -> io::Result<Self> {
        config.validate()?;
        let ring = io_uring::IoUring::new(config.queue_depth())?;
        Ok(Self {
            worker_index,
            inner: Arc::new(Mutex::new(ring)),
            config: config.clone(),
        })
    }

    pub fn worker_index(&self) -> usize {
        self.worker_index
    }

    pub fn queue_depth(&self) -> u32 {
        self.config.queue_depth()
    }

    pub fn shared(&self) -> SharedRing {
        self.inner.clone()
    }

    pub fn with_lock<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut io_uring::IoUring) -> R,
    {
        let mut guard = self.inner.lock();
        f(&mut guard)
    }
}
