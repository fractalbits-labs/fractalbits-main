use moka::sync::Cache;
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone)]
pub struct DirEntry {
    pub name: String,
    pub ino: u64,
    pub is_dir: bool,
}

pub struct DirCache {
    inner: Cache<String, Arc<Vec<DirEntry>>>,
}

impl DirCache {
    pub fn new(ttl: Duration) -> Self {
        let cache = Cache::builder()
            .max_capacity(10_000)
            .time_to_live(ttl)
            .build();
        Self { inner: cache }
    }

    pub fn get(&self, prefix: &str) -> Option<Arc<Vec<DirEntry>>> {
        self.inner.get(prefix)
    }

    pub fn insert(&self, prefix: String, entries: Arc<Vec<DirEntry>>) {
        self.inner.insert(prefix, entries);
    }

    pub fn invalidate(&self, prefix: &str) {
        self.inner.invalidate(prefix);
    }
}
