use bytes::Bytes;
use moka::future::Cache;
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

/// Cache key: (blob_id, volume_id, block_number)
type BlockCacheKey = (Uuid, u16, u32);

pub struct BlockCache {
    inner: Cache<BlockCacheKey, Bytes>,
}

impl BlockCache {
    pub fn new(max_size_mb: u64) -> Self {
        let max_bytes = max_size_mb * 1024 * 1024;
        let cache = Cache::builder()
            .weigher(|_key: &BlockCacheKey, value: &Bytes| -> u32 {
                value.len().try_into().unwrap_or(u32::MAX)
            })
            .max_capacity(max_bytes)
            .build();
        Self { inner: cache }
    }

    pub async fn get(&self, blob_id: Uuid, volume_id: u16, block_number: u32) -> Option<Bytes> {
        self.inner.get(&(blob_id, volume_id, block_number)).await
    }

    pub async fn insert(&self, blob_id: Uuid, volume_id: u16, block_number: u32, data: Bytes) {
        self.inner
            .insert((blob_id, volume_id, block_number), data)
            .await;
    }
}

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

    pub async fn get(&self, prefix: &str) -> Option<Arc<Vec<DirEntry>>> {
        self.inner.get(prefix).await
    }

    pub async fn insert(&self, prefix: String, entries: Arc<Vec<DirEntry>>) {
        self.inner.insert(prefix, entries).await;
    }

    pub async fn invalidate(&self, prefix: &str) {
        self.inner.invalidate(prefix).await;
    }
}
