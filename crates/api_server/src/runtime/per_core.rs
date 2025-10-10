use crate::runtime::uring::{reactor::RpcReactorHandle, ring::PerCoreRing};
use core_affinity::{self, CoreId};
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tracing::{debug, warn};

#[derive(Clone)]
pub struct PerCoreBuilder {
    rings: Arc<Vec<Arc<PerCoreRing>>>,
    reactors: Arc<Vec<Arc<RpcReactorHandle>>>,
    next_worker: Arc<AtomicUsize>,
    worker_limit: usize,
    core_ids: Arc<Vec<CoreId>>,
}

impl PerCoreBuilder {
    pub fn new(
        rings: Arc<Vec<Arc<PerCoreRing>>>,
        reactors: Arc<Vec<Arc<RpcReactorHandle>>>,
    ) -> Self {
        let core_ids = core_affinity::get_core_ids().unwrap_or_default();
        if core_ids.is_empty() {
            warn!("core affinity metadata unavailable; workers will not be pinned");
        }
        if rings.len() != reactors.len() {
            warn!(
                rings = rings.len(),
                reactors = reactors.len(),
                "ring and reactor counts differ; runtime may behave unexpectedly"
            );
        }
        let worker_limit = rings.len().max(1);
        Self {
            rings,
            reactors,
            next_worker: Arc::new(AtomicUsize::new(0)),
            worker_limit,
            core_ids: Arc::new(core_ids),
        }
    }

    pub fn build_context(&self) -> io::Result<Arc<PerCoreContext>> {
        let raw_index = self.next_worker.fetch_add(1, Ordering::Relaxed);
        let worker_index = raw_index % self.worker_limit;
        let ring = self
            .rings
            .get(worker_index)
            .ok_or_else(|| io::Error::other("ring missing for worker"))?
            .clone();
        let reactor = self
            .reactors
            .get(worker_index)
            .ok_or_else(|| io::Error::other("reactor missing for worker"))?
            .clone();
        Ok(Arc::new(PerCoreContext::new(worker_index, ring, reactor)))
    }

    pub fn pin_current_thread(&self, worker_index: usize) {
        if self.core_ids.is_empty() {
            return;
        }
        let core = self.core_ids[worker_index % self.core_ids.len()];
        if core_affinity::set_for_current(core) {
            debug!(worker_index, core_id = core.id, "pinned worker to core");
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
    ring: Arc<PerCoreRing>,
    reactor: Arc<RpcReactorHandle>,
}

impl PerCoreContext {
    fn new(worker_index: usize, ring: Arc<PerCoreRing>, reactor: Arc<RpcReactorHandle>) -> Self {
        Self {
            worker_index,
            ring,
            reactor,
        }
    }

    pub fn worker_index(&self) -> usize {
        self.worker_index
    }

    pub fn ring(&self) -> Arc<PerCoreRing> {
        self.ring.clone()
    }

    pub fn reactor(&self) -> Arc<RpcReactorHandle> {
        self.reactor.clone()
    }
}
