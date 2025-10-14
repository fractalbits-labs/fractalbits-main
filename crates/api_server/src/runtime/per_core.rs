use core_affinity::{self, CoreId};
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tracing::{debug, warn};

#[derive(Clone)]
pub struct PerCoreBuilder {
    next_worker: Arc<AtomicUsize>,
    worker_limit: usize,
    core_ids: Arc<Vec<CoreId>>,
}

impl Default for PerCoreBuilder {
    fn default() -> Self {
        let core_ids = core_affinity::get_core_ids().unwrap_or_default();
        if core_ids.is_empty() {
            warn!("core affinity metadata unavailable; workers will not be pinned");
        }
        let worker_limit = core_ids.len().max(1);
        Self {
            next_worker: Arc::new(AtomicUsize::new(0)),
            worker_limit,
            core_ids: Arc::new(core_ids),
        }
    }
}

impl PerCoreBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn build_context(&self) -> io::Result<Arc<PerCoreContext>> {
        let raw_index = self.next_worker.fetch_add(1, Ordering::Relaxed);
        let worker_index = raw_index % self.worker_limit;
        Ok(Arc::new(PerCoreContext::new(worker_index)))
    }

    pub fn pin_current_thread(&self, worker_index: usize) {
        if self.core_ids.is_empty() {
            return;
        }
        if self.core_ids.len() <= 1 {
            warn!("insufficient cores for pinning (skipping core 0)");
            return;
        }
        let core_index = (worker_index % (self.core_ids.len() - 1)) + 1;
        let core = self.core_ids[core_index];
        if core_affinity::set_for_current(core) {
            debug!(
                worker_index,
                core_id = core.id,
                "pinned worker to core (skipping core 0)"
            );
        } else {
            warn!(
                worker_index,
                core_id = core.id,
                "failed to pin worker to core"
            );
        }
    }
}

pub struct PerCoreContext {
    worker_index: usize,
    // ring: Arc<PerCoreRing>,
}

impl PerCoreContext {
    fn new(worker_index: usize) -> Self {
        Self {
            worker_index,
            // ring,
        }
    }

    pub fn worker_index(&self) -> usize {
        self.worker_index
    }

    // pub fn ring(&self) -> Arc<PerCoreRing> {
    //     self.ring.clone()
    // }
}
