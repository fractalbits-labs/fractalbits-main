use std::sync::Arc;

use moka::future::Cache;
use parking_lot::RwLock;

/// Coordinates cache operations across all per-worker cache instances so that
/// management requests can broadcast updates.
pub struct CacheCoordinator<T> {
    caches: RwLock<Vec<Arc<Cache<String, T>>>>,
}

impl<T> Default for CacheCoordinator<T>
where
    T: Clone + Send + Sync + 'static,
{
    fn default() -> Self {
        Self {
            caches: RwLock::new(Vec::new()),
        }
    }
}

impl<T> CacheCoordinator<T>
where
    T: Clone + Send + Sync + 'static,
{
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register_cache(&self, cache: Arc<Cache<String, T>>) {
        let mut caches = self.caches.write();
        if caches.iter().any(|existing| Arc::ptr_eq(existing, &cache)) {
            return;
        }
        caches.push(cache);
    }

    pub async fn insert(&self, key: &str, value: T) {
        let caches = self.snapshot();
        for cache in caches {
            cache.insert(key.to_owned(), value.clone()).await;
        }
    }

    pub async fn invalidate_entry(&self, key: &str) {
        let caches = self.snapshot();
        for cache in caches {
            cache.invalidate(key).await;
        }
    }

    pub fn invalidate_all(&self) {
        let caches = self.snapshot();
        for cache in caches {
            cache.invalidate_all();
        }
    }

    pub fn has_registered_caches(&self) -> bool {
        !self.caches.read().is_empty()
    }

    fn snapshot(&self) -> Vec<Arc<Cache<String, T>>> {
        let guard = self.caches.read();
        guard.iter().cloned().collect()
    }
}
