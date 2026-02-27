use crate::object_layout::ObjectLayout;
use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

pub const ROOT_INODE: u64 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EntryType {
    File,
    Directory,
}

pub struct InodeEntry {
    pub s3_key: String,
    pub entry_type: EntryType,
    pub layout: Option<ObjectLayout>,
    pub cache_expiry: Instant,
    refcount: AtomicU64,
}

impl InodeEntry {
    fn new(s3_key: String, entry_type: EntryType, layout: Option<ObjectLayout>) -> Self {
        Self {
            s3_key,
            entry_type,
            layout,
            cache_expiry: Instant::now(),
            refcount: AtomicU64::new(1),
        }
    }

    pub fn increment_ref(&self) {
        self.refcount.fetch_add(1, Ordering::Relaxed);
    }

    /// Decrements refcount by nlookup. Returns true if entry should be removed.
    pub fn forget(&self, nlookup: u64) -> bool {
        let prev = self.refcount.fetch_sub(nlookup, Ordering::Relaxed);
        prev <= nlookup
    }
}

pub struct InodeTable {
    map: DashMap<u64, InodeEntry>,
    next_ino: AtomicU64,
    // Reverse map: (s3_key, entry_type) -> inode for lookup dedup.
    // EntryType is included to avoid aliasing between files and directories
    // with the same key (e.g., a file at "dir/" vs a directory prefix "dir/").
    key_to_ino: DashMap<(String, EntryType), u64>,
}

impl InodeTable {
    pub fn new() -> Self {
        let table = Self {
            map: DashMap::new(),
            next_ino: AtomicU64::new(2), // 1 is root
            key_to_ino: DashMap::new(),
        };
        // Insert root inode. Root key is "/" matching NSS key convention
        // where all keys are stored with a leading "/".
        table.map.insert(
            ROOT_INODE,
            InodeEntry {
                s3_key: "/".to_string(),
                entry_type: EntryType::Directory,
                layout: None,
                cache_expiry: Instant::now(),
                refcount: AtomicU64::new(u64::MAX), // root never gets forgotten
            },
        );
        table
            .key_to_ino
            .insert(("/".to_string(), EntryType::Directory), ROOT_INODE);
        table
    }

    /// Look up or insert an inode for a given s3_key. Returns (ino, is_new).
    pub fn lookup_or_insert(
        &self,
        s3_key: &str,
        entry_type: EntryType,
        layout: Option<ObjectLayout>,
    ) -> (u64, bool) {
        let dedup_key = (s3_key.to_string(), entry_type);
        // Check if we already have this key
        if let Some(existing_ino) = self.key_to_ino.get(&dedup_key) {
            let ino = *existing_ino;
            if let Some(entry) = self.map.get(&ino) {
                entry.increment_ref();
                // Update layout if provided
                drop(entry);
                if let Some(new_layout) = layout
                    && let Some(mut entry) = self.map.get_mut(&ino)
                {
                    entry.layout = Some(new_layout);
                    entry.cache_expiry = Instant::now();
                }
                return (ino, false);
            }
        }

        let ino = self.next_ino.fetch_add(1, Ordering::Relaxed);
        self.map
            .insert(ino, InodeEntry::new(s3_key.to_string(), entry_type, layout));
        self.key_to_ino.insert(dedup_key, ino);
        (ino, true)
    }

    pub fn get(&self, ino: u64) -> Option<dashmap::mapref::one::Ref<'_, u64, InodeEntry>> {
        self.map.get(&ino)
    }

    pub fn get_mut(&self, ino: u64) -> Option<dashmap::mapref::one::RefMut<'_, u64, InodeEntry>> {
        self.map.get_mut(&ino)
    }

    pub fn get_s3_key(&self, ino: u64) -> Option<String> {
        self.map.get(&ino).map(|e| e.s3_key.clone())
    }

    /// Forget an inode (decrement refcount). Removes entry when refcount reaches 0.
    /// Root inode is never removed.
    pub fn forget(&self, ino: u64, nlookup: u64) {
        if ino == ROOT_INODE {
            return;
        }

        let should_remove = self
            .map
            .get(&ino)
            .map(|entry| entry.forget(nlookup))
            .unwrap_or(false);

        if should_remove && let Some((_, entry)) = self.map.remove(&ino) {
            self.key_to_ino
                .remove(&(entry.s3_key.clone(), entry.entry_type));
        }
    }
}
