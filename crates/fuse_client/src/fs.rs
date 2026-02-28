use bytes::{Bytes, BytesMut};
use dashmap::DashMap;
use data_types::TraceId;
use fuse3::Errno;
use fuse3::raw::reply::{
    DirectoryEntry, DirectoryEntryPlus, FileAttr, ReplyAttr, ReplyCreated, ReplyData,
    ReplyDirectory, ReplyDirectoryPlus, ReplyEntry, ReplyInit, ReplyOpen, ReplyStatFs, ReplyWrite,
};
use fuse3::raw::{Filesystem, Request};
use fuse3::{FileType, SetAttr, Timestamp};
use futures::stream;
use rkyv::api::high::to_bytes_in;
use std::ffi::OsStr;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::backend::StorageBackend;
use crate::cache::{BlockCache, DirCache, DirEntry};
use crate::error::FuseError;
use crate::inode::{EntryType, InodeTable, ROOT_INODE};
use data_types::object_layout::{
    MpuState, ObjectCoreMetaData, ObjectLayout, ObjectMetaData, ObjectState,
};

const TTL: Duration = Duration::from_secs(1);
const DEFAULT_BLOCK_SIZE: u32 = 1024 * 1024 - 256;

struct WriteBuffer {
    data: BytesMut,
    dirty: bool,
}

struct FileHandle {
    ino: u64,
    s3_key: String,
    layout: Option<ObjectLayout>,
    write_buf: Option<WriteBuffer>,
}

pub struct FuseFs {
    backend: Arc<StorageBackend>,
    inodes: Arc<InodeTable>,
    block_cache: BlockCache,
    dir_cache: DirCache,
    file_handles: DashMap<u64, FileHandle>,
    next_fh: AtomicU64,
    read_write: bool,
    // Tracks blob data for unlinked files that still have open handles.
    // Cleanup is deferred until the last handle is released.
    deferred_blob_cleanup: DashMap<u64, Bytes>,
}

impl FuseFs {
    pub fn new(backend: Arc<StorageBackend>, inodes: Arc<InodeTable>, read_write: bool) -> Self {
        let block_cache_size_mb = backend.config().block_cache_size_mb;
        let dir_cache_ttl = backend.config().dir_cache_ttl();
        Self {
            backend,
            inodes,
            block_cache: BlockCache::new(block_cache_size_mb),
            dir_cache: DirCache::new(dir_cache_ttl),
            file_handles: DashMap::new(),
            next_fh: AtomicU64::new(1),
            read_write,
            deferred_blob_cleanup: DashMap::new(),
        }
    }

    fn alloc_fh(&self) -> u64 {
        self.next_fh.fetch_add(1, Ordering::Relaxed)
    }

    fn dir_prefix(&self, ino: u64) -> Option<String> {
        self.inodes.get_s3_key(ino)
    }

    fn check_write_enabled(&self) -> fuse3::Result<()> {
        if !self.read_write {
            return Err(Errno::from(libc::EROFS));
        }
        Ok(())
    }

    fn has_open_handles_for_inode(&self, ino: u64, exclude_fh: Option<u64>) -> bool {
        self.file_handles.iter().any(|entry| {
            entry.value().ino == ino && exclude_fh.is_none_or(|excl| *entry.key() != excl)
        })
    }

    async fn preload_file_content(
        &self,
        s3_key: &str,
        layout: &ObjectLayout,
    ) -> fuse3::Result<BytesMut> {
        let size = layout
            .size()
            .map_err(|e| std::io::Error::from(FuseError::from(e)))?;
        if size == 0 {
            return Ok(BytesMut::new());
        }
        let read_size = size.min(u32::MAX as u64) as u32;
        let data = match &layout.state {
            ObjectState::Normal(_) => self
                .read_normal(layout, 0, read_size)
                .await
                .map_err(std::io::Error::from)?,
            ObjectState::Mpu(MpuState::Completed(_)) => self
                .read_mpu(s3_key, layout, 0, read_size)
                .await
                .map_err(std::io::Error::from)?,
            _ => return Err(Errno::from(libc::EIO)),
        };
        Ok(BytesMut::from(data.as_ref()))
    }

    fn file_perm(&self) -> u16 {
        if self.read_write { 0o644 } else { 0o444 }
    }

    fn dir_perm(&self) -> u16 {
        if self.read_write { 0o755 } else { 0o555 }
    }

    async fn flush_write_buffer(&self, fh_id: u64) -> fuse3::Result<()> {
        let (s3_key, data) = {
            let handle = self
                .file_handles
                .get(&fh_id)
                .ok_or_else(|| Errno::from(libc::EBADF))?;
            let wb = match &handle.write_buf {
                Some(wb) if wb.dirty => wb,
                _ => return Ok(()),
            };
            (handle.s3_key.clone(), Bytes::copy_from_slice(&wb.data))
        };

        let trace_id = TraceId::new();
        let blob_guid = self.backend.create_blob_guid();
        let block_size = DEFAULT_BLOCK_SIZE as usize;

        // Write data blocks
        let num_blocks = if data.is_empty() {
            0
        } else {
            data.len().div_ceil(block_size)
        };
        for block_i in 0..num_blocks {
            let start = block_i * block_size;
            let end = std::cmp::min(start + block_size, data.len());
            let chunk = data.slice(start..end);
            self.backend
                .write_block(blob_guid, block_i as u32, chunk, &trace_id)
                .await
                .map_err(std::io::Error::from)?;
        }

        // Build ObjectLayout
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let layout = ObjectLayout {
            version_id: ObjectLayout::gen_version_id(),
            block_size: DEFAULT_BLOCK_SIZE,
            timestamp,
            state: ObjectState::Normal(ObjectMetaData {
                blob_guid,
                core_meta_data: ObjectCoreMetaData {
                    size: data.len() as u64,
                    etag: blob_guid.blob_id.simple().to_string(),
                    headers: vec![],
                    checksum: None,
                },
            }),
        };

        // Serialize layout
        let layout_bytes: Bytes = to_bytes_in::<_, rkyv::rancor::Error>(&layout, Vec::new())
            .map_err(|e| std::io::Error::other(format!("Failed to serialize object layout: {e}")))?
            .into();

        // Put inode in NSS, get old object bytes
        let old_bytes = self
            .backend
            .put_inode(&s3_key, layout_bytes, &trace_id)
            .await
            .map_err(std::io::Error::from)?;

        // Delete old blob blocks if there was a previous version
        if !old_bytes.is_empty()
            && let Ok(old_layout) =
                rkyv::from_bytes::<ObjectLayout, rkyv::rancor::Error>(&old_bytes)
        {
            self.backend
                .delete_blob_blocks(&old_layout, &trace_id)
                .await;
        }

        // Update file handle with new layout and clear dirty flag
        if let Some(mut handle) = self.file_handles.get_mut(&fh_id) {
            handle.layout = Some(layout.clone());
            if let Some(ref mut wb) = handle.write_buf {
                wb.dirty = false;
            }
        }

        // Update inode table layout
        {
            let handle = self.file_handles.get(&fh_id);
            if let Some(handle) = handle
                && let Some(mut entry) = self.inodes.get_mut(handle.ino)
            {
                entry.layout = Some(layout);
            }
        }

        // Invalidate dir cache for parent prefix
        let parent_prefix = parent_prefix_of(&s3_key);
        self.dir_cache.invalidate(&parent_prefix).await;

        Ok(())
    }

    async fn fetch_dir_entries(
        &self,
        parent: u64,
        prefix: &str,
    ) -> fuse3::Result<Arc<Vec<DirEntry>>> {
        if let Some(cached) = self.dir_cache.get(prefix).await {
            let stale = cached
                .iter()
                .any(|entry| self.inodes.get(entry.ino).is_none());
            if !stale {
                return Ok(cached);
            }
            tracing::debug!(%prefix, "Directory cache contains stale inode(s), rebuilding");
            self.dir_cache.invalidate(prefix).await;
        }

        let trace_id = TraceId::new();
        let mut all_entries = Vec::new();

        // Resolve parent-of-parent inode for ".." entry.
        // For root ("/") or top-level dirs, parent-of-parent is root.
        let dotdot_ino = if parent == ROOT_INODE {
            ROOT_INODE
        } else {
            let trimmed = prefix.trim_end_matches('/');
            match trimmed.rfind('/') {
                Some(pos) => {
                    let parent_key = &prefix[..=pos];
                    if parent_key == "/" {
                        ROOT_INODE
                    } else {
                        let (ino, _) =
                            self.inodes
                                .lookup_or_insert(parent_key, EntryType::Directory, None);
                        ino
                    }
                }
                None => ROOT_INODE,
            }
        };

        all_entries.push(DirEntry {
            name: ".".to_string(),
            ino: parent,
            is_dir: true,
        });
        all_entries.push(DirEntry {
            name: "..".to_string(),
            ino: dotdot_ino,
            is_dir: true,
        });

        let mut start_after = String::new();
        loop {
            let entries = self
                .backend
                .list_objects(prefix, "/", &start_after, 1000, &trace_id)
                .await
                .map_err(std::io::Error::from)?;

            if entries.is_empty() {
                break;
            }

            let last_key = entries.last().map(|e| e.key.clone());

            for entry in entries {
                let raw_key = &entry.key;

                let name = if raw_key.len() >= prefix.len() {
                    &raw_key[prefix.len()..]
                } else {
                    raw_key.as_str()
                };

                if entry.layout.is_none() {
                    // Directory (common prefix)
                    let dir_name = name.trim_end_matches('/');
                    if dir_name.is_empty() {
                        continue;
                    }
                    let dir_key = raw_key.clone();
                    let (ino, _) =
                        self.inodes
                            .lookup_or_insert(&dir_key, EntryType::Directory, None);
                    all_entries.push(DirEntry {
                        name: dir_name.to_string(),
                        ino,
                        is_dir: true,
                    });
                } else {
                    // File - backend already stripped trailing \0 from keys
                    let layout = entry.layout.as_ref().unwrap();
                    if !layout.is_listable() {
                        continue;
                    }
                    if name.is_empty() {
                        continue;
                    }
                    let (ino, _) =
                        self.inodes
                            .lookup_or_insert(raw_key, EntryType::File, entry.layout);
                    all_entries.push(DirEntry {
                        name: name.to_string(),
                        ino,
                        is_dir: false,
                    });
                }
            }

            if let Some(last) = last_key {
                start_after = last;
            } else {
                break;
            }
        }

        let entries = Arc::new(all_entries);
        self.dir_cache
            .insert(prefix.to_string(), entries.clone())
            .await;
        Ok(entries)
    }

    fn make_file_attr(&self, ino: u64, layout: &ObjectLayout) -> Result<FileAttr, FuseError> {
        let size = layout.size()?;
        let ts = Timestamp::new((layout.timestamp / 1000) as i64, 0);
        Ok(FileAttr {
            ino,
            size,
            blocks: size.div_ceil(512),
            atime: ts,
            mtime: ts,
            ctime: ts,
            kind: FileType::RegularFile,
            perm: self.file_perm(),
            nlink: 1,
            uid: 0,
            gid: 0,
            rdev: 0,
            blksize: DEFAULT_BLOCK_SIZE,
        })
    }

    /// Fallback file attr when layout is unavailable (e.g., inode evicted
    /// between fetch_dir_entries and readdirplus iteration). Uses correct
    /// kind=RegularFile to avoid on-wire inconsistency.
    fn make_default_file_attr(&self, ino: u64) -> FileAttr {
        let now = Timestamp::new(0, 0);
        FileAttr {
            ino,
            size: 0,
            blocks: 0,
            atime: now,
            mtime: now,
            ctime: now,
            kind: FileType::RegularFile,
            perm: self.file_perm(),
            nlink: 1,
            uid: 0,
            gid: 0,
            rdev: 0,
            blksize: DEFAULT_BLOCK_SIZE,
        }
    }

    fn make_dir_attr(&self, ino: u64) -> FileAttr {
        let now = Timestamp::new(0, 0);
        FileAttr {
            ino,
            size: 0,
            blocks: 0,
            atime: now,
            mtime: now,
            ctime: now,
            kind: FileType::Directory,
            perm: self.dir_perm(),
            nlink: 2,
            uid: 0,
            gid: 0,
            rdev: 0,
            blksize: DEFAULT_BLOCK_SIZE,
        }
    }

    fn make_new_file_attr(&self, ino: u64, size: u64) -> FileAttr {
        let now_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        let ts = Timestamp::new(now_secs, 0);
        FileAttr {
            ino,
            size,
            blocks: size.div_ceil(512),
            atime: ts,
            mtime: ts,
            ctime: ts,
            kind: FileType::RegularFile,
            perm: self.file_perm(),
            nlink: 1,
            uid: 0,
            gid: 0,
            rdev: 0,
            blksize: DEFAULT_BLOCK_SIZE,
        }
    }

    async fn read_normal(
        &self,
        layout: &ObjectLayout,
        offset: u64,
        size: u32,
    ) -> Result<Bytes, FuseError> {
        let file_size = layout.size()?;
        if size == 0 || offset >= file_size {
            return Ok(Bytes::new());
        }

        let blob_guid = layout.blob_guid()?;
        let block_size = layout.block_size as u64;
        let read_end = std::cmp::min(offset.saturating_add(size as u64), file_size);
        let actual_len = (read_end - offset) as usize;

        let first_block = (offset / block_size) as u32;
        let last_block = ((read_end - 1) / block_size) as u32;

        let trace_id = TraceId::new();
        let mut result = BytesMut::with_capacity(actual_len);

        for block_num in first_block..=last_block {
            let block_start = block_num as u64 * block_size;
            let block_content_len = std::cmp::min(block_size, file_size - block_start) as usize;

            let block_data = if let Some(cached) = self
                .block_cache
                .get(blob_guid.blob_id, blob_guid.volume_id, block_num)
                .await
            {
                cached
            } else {
                let data = self
                    .backend
                    .read_block(blob_guid, block_num, block_content_len, &trace_id)
                    .await?;
                self.block_cache
                    .insert(
                        blob_guid.blob_id,
                        blob_guid.volume_id,
                        block_num,
                        data.clone(),
                    )
                    .await;
                data
            };

            let slice_start = if block_num == first_block {
                (offset - block_start) as usize
            } else {
                0
            };
            let slice_end = if block_num == last_block {
                (read_end - block_start) as usize
            } else {
                block_data.len()
            };

            if slice_start < block_data.len() {
                let end = std::cmp::min(slice_end, block_data.len());
                result.extend_from_slice(&block_data[slice_start..end]);
            }
        }

        Ok(result.freeze())
    }

    async fn read_mpu(
        &self,
        key: &str,
        layout: &ObjectLayout,
        offset: u64,
        size: u32,
    ) -> Result<Bytes, FuseError> {
        let file_size = layout.size()?;
        if size == 0 || offset >= file_size {
            return Ok(Bytes::new());
        }

        let read_end = std::cmp::min(offset.saturating_add(size as u64), file_size);
        let actual_len = (read_end - offset) as usize;
        let trace_id = TraceId::new();

        let parts = self.backend.list_mpu_parts(key, &trace_id).await?;

        let mut result = BytesMut::with_capacity(actual_len);
        let mut obj_offset: u64 = 0;

        for (_part_key, part_obj) in &parts {
            let part_size = part_obj.size()?;
            let part_end = obj_offset + part_size;

            if obj_offset >= read_end {
                break;
            }

            if part_end > offset {
                let blob_guid = part_obj.blob_guid()?;
                let block_size = part_obj.block_size as u64;

                let part_read_start = offset.saturating_sub(obj_offset);
                let part_read_end = if read_end < part_end {
                    read_end - obj_offset
                } else {
                    part_size
                };

                let first_block = (part_read_start / block_size) as u32;
                let last_block = ((part_read_end - 1) / block_size) as u32;

                for block_num in first_block..=last_block {
                    let block_start = block_num as u64 * block_size;
                    let block_content_len =
                        std::cmp::min(block_size, part_size - block_start) as usize;

                    let block_data = if let Some(cached) = self
                        .block_cache
                        .get(blob_guid.blob_id, blob_guid.volume_id, block_num)
                        .await
                    {
                        cached
                    } else {
                        let data = self
                            .backend
                            .read_block(blob_guid, block_num, block_content_len, &trace_id)
                            .await?;
                        self.block_cache
                            .insert(
                                blob_guid.blob_id,
                                blob_guid.volume_id,
                                block_num,
                                data.clone(),
                            )
                            .await;
                        data
                    };

                    let slice_start = if block_num == first_block {
                        (part_read_start - block_start) as usize
                    } else {
                        0
                    };
                    let slice_end = if block_num == last_block {
                        (part_read_end - block_start) as usize
                    } else {
                        block_data.len()
                    };

                    if slice_start < block_data.len() {
                        let end = std::cmp::min(slice_end, block_data.len());
                        result.extend_from_slice(&block_data[slice_start..end]);
                    }
                }
            }

            obj_offset = part_end;
        }

        Ok(result.freeze())
    }
}

/// Extract the parent prefix from an s3_key.
/// e.g. "/foo/bar" -> "/foo/", "/top" -> "/"
fn parent_prefix_of(key: &str) -> String {
    let trimmed = key.trim_end_matches('/');
    match trimmed.rfind('/') {
        Some(pos) => trimmed[..=pos].to_string(),
        None => "/".to_string(),
    }
}

impl Filesystem for FuseFs {
    type DirEntryStream<'a> = stream::Iter<std::vec::IntoIter<Result<DirectoryEntry, Errno>>>;
    type DirEntryPlusStream<'a> =
        stream::Iter<std::vec::IntoIter<Result<fuse3::raw::reply::DirectoryEntryPlus, Errno>>>;

    async fn init(&self, _req: Request) -> fuse3::Result<ReplyInit> {
        tracing::info!("FUSE filesystem mounted");
        Ok(ReplyInit {
            max_write: NonZeroU32::new(1024 * 1024).unwrap(),
        })
    }

    async fn destroy(&self, _req: Request) {
        tracing::info!("FUSE filesystem unmounted");
    }

    async fn lookup(&self, _req: Request, parent: u64, name: &OsStr) -> fuse3::Result<ReplyEntry> {
        let name_str = name.to_str().ok_or_else(|| Errno::from(libc::EINVAL))?;

        let prefix = self
            .dir_prefix(parent)
            .ok_or_else(|| Errno::from(libc::ENOENT))?;

        let full_key = if prefix.is_empty() {
            name_str.to_string()
        } else {
            format!("{}{}", prefix, name_str)
        };

        let trace_id = TraceId::new();

        // Try as file first
        match self.backend.get_object(&full_key, &trace_id).await {
            Ok(layout) => {
                if !layout.is_listable() {
                    return Err(Errno::from(libc::ENOENT));
                }
                let (ino, _) =
                    self.inodes
                        .lookup_or_insert(&full_key, EntryType::File, Some(layout.clone()));
                let attr = self.make_file_attr(ino, &layout)?;
                return Ok(ReplyEntry {
                    ttl: TTL,
                    attr,
                    generation: 0,
                });
            }
            Err(FuseError::NotFound) => {}
            Err(e) => return Err(std::io::Error::from(e).into()),
        }

        // Try as directory
        let dir_key = format!("{}/", full_key);
        let entries = self
            .backend
            .list_objects(&dir_key, "/", "", 1, &trace_id)
            .await;

        match entries {
            Ok(entries) if !entries.is_empty() => {
                let (ino, _) = self
                    .inodes
                    .lookup_or_insert(&dir_key, EntryType::Directory, None);
                let attr = self.make_dir_attr(ino);
                Ok(ReplyEntry {
                    ttl: TTL,
                    attr,
                    generation: 0,
                })
            }
            _ => Err(Errno::from(libc::ENOENT)),
        }
    }

    async fn forget(&self, _req: Request, inode: u64, nlookup: u64) {
        self.inodes.forget(inode, nlookup);
    }

    async fn getattr(
        &self,
        _req: Request,
        inode: u64,
        fh: Option<u64>,
        _flags: u32,
    ) -> fuse3::Result<ReplyAttr> {
        if inode == ROOT_INODE {
            return Ok(ReplyAttr {
                ttl: TTL,
                attr: self.make_dir_attr(ROOT_INODE),
            });
        }

        // If there's an open write handle with a dirty buffer, report its size
        if let Some(fh_id) = fh
            && let Some(handle) = self.file_handles.get(&fh_id)
            && let Some(ref wb) = handle.write_buf
            && wb.dirty
        {
            let attr = self.make_new_file_attr(inode, wb.data.len() as u64);
            return Ok(ReplyAttr { ttl: TTL, attr });
        }

        let entry = self
            .inodes
            .get(inode)
            .ok_or_else(|| Errno::from(libc::ENOENT))?;

        match entry.entry_type {
            EntryType::Directory => {
                let attr = self.make_dir_attr(inode);
                Ok(ReplyAttr { ttl: TTL, attr })
            }
            EntryType::File => {
                if let Some(ref layout) = entry.layout {
                    let attr = self.make_file_attr(inode, layout)?;
                    Ok(ReplyAttr { ttl: TTL, attr })
                } else {
                    let key = entry.s3_key.clone();
                    drop(entry);
                    let trace_id = TraceId::new();
                    let layout = self
                        .backend
                        .get_object(&key, &trace_id)
                        .await
                        .map_err(std::io::Error::from)?;
                    let attr = self.make_file_attr(inode, &layout)?;
                    if let Some(mut entry) = self.inodes.get_mut(inode) {
                        entry.layout = Some(layout);
                    }
                    Ok(ReplyAttr { ttl: TTL, attr })
                }
            }
        }
    }

    async fn setattr(
        &self,
        _req: Request,
        inode: u64,
        fh: Option<u64>,
        set_attr: SetAttr,
    ) -> fuse3::Result<ReplyAttr> {
        // Only handle truncate to zero
        if let Some(0) = set_attr.size {
            let fh_id = fh.ok_or_else(|| Errno::from(libc::ENOSYS))?;
            let mut handle = self
                .file_handles
                .get_mut(&fh_id)
                .ok_or_else(|| Errno::from(libc::EBADF))?;
            if let Some(ref mut wb) = handle.write_buf {
                wb.data.clear();
                wb.dirty = true;
            } else {
                handle.write_buf = Some(WriteBuffer {
                    data: BytesMut::new(),
                    dirty: true,
                });
            }
            let attr = self.make_new_file_attr(inode, 0);
            return Ok(ReplyAttr { ttl: TTL, attr });
        }

        // For other setattr calls, just return current attr
        self.getattr(_req, inode, fh, 0).await
    }

    async fn open(&self, _req: Request, inode: u64, flags: u32) -> fuse3::Result<ReplyOpen> {
        let write_flags = libc::O_WRONLY as u32
            | libc::O_RDWR as u32
            | libc::O_APPEND as u32
            | libc::O_TRUNC as u32;
        let is_write = flags & write_flags != 0;

        if is_write && !self.read_write {
            return Err(Errno::from(libc::EROFS));
        }

        let entry = self
            .inodes
            .get(inode)
            .ok_or_else(|| Errno::from(libc::ENOENT))?;

        if entry.entry_type != EntryType::File {
            return Err(Errno::from(libc::EISDIR));
        }

        let s3_key = entry.s3_key.clone();
        let layout = entry.layout.clone();
        drop(entry);

        // Resolve layout if not cached
        let layout = match layout {
            Some(l) => Some(l),
            None => {
                let trace_id = TraceId::new();
                match self.backend.get_object(&s3_key, &trace_id).await {
                    Ok(l) => Some(l),
                    Err(FuseError::NotFound) if is_write => None,
                    Err(e) => return Err(std::io::Error::from(e).into()),
                }
            }
        };

        let has_trunc = flags & libc::O_TRUNC as u32 != 0;
        let write_buf = if is_write {
            if let Some(ref l) = layout
                && !has_trunc
            {
                // Existing file without truncate: preload content so partial
                // writes don't lose surrounding data
                let data = self.preload_file_content(&s3_key, l).await?;
                Some(WriteBuffer { data, dirty: false })
            } else {
                // O_TRUNC or new file: start empty
                Some(WriteBuffer {
                    data: BytesMut::new(),
                    dirty: false,
                })
            }
        } else {
            None
        };

        let fh = self.alloc_fh();
        self.file_handles.insert(
            fh,
            FileHandle {
                ino: inode,
                s3_key,
                layout,
                write_buf,
            },
        );

        Ok(ReplyOpen { fh, flags: 0 })
    }

    async fn read(
        &self,
        _req: Request,
        _inode: u64,
        fh: u64,
        offset: u64,
        size: u32,
    ) -> fuse3::Result<ReplyData> {
        let handle = self
            .file_handles
            .get(&fh)
            .ok_or_else(|| Errno::from(libc::EBADF))?;

        // If there's a dirty write buffer, read from it
        if let Some(ref wb) = handle.write_buf
            && wb.dirty
        {
            let buf_len = wb.data.len() as u64;
            if offset >= buf_len {
                return Ok(ReplyData { data: Bytes::new() });
            }
            let end = std::cmp::min(offset + size as u64, buf_len) as usize;
            let data = Bytes::copy_from_slice(&wb.data[offset as usize..end]);
            return Ok(ReplyData { data });
        }

        let s3_key = handle.s3_key.clone();
        let layout = match &handle.layout {
            Some(l) => l.clone(),
            None => return Ok(ReplyData { data: Bytes::new() }),
        };
        drop(handle);

        let data = match &layout.state {
            ObjectState::Normal(_) => self.read_normal(&layout, offset, size).await?,
            ObjectState::Mpu(MpuState::Completed(_)) => {
                self.read_mpu(&s3_key, &layout, offset, size).await?
            }
            _ => {
                return Err(Errno::from(libc::EIO));
            }
        };

        Ok(ReplyData { data })
    }

    async fn write(
        &self,
        _req: Request,
        _inode: u64,
        fh: u64,
        offset: u64,
        data: &[u8],
        _write_flags: u32,
        _flags: u32,
    ) -> fuse3::Result<ReplyWrite> {
        let mut handle = self
            .file_handles
            .get_mut(&fh)
            .ok_or_else(|| Errno::from(libc::EBADF))?;

        let wb = handle.write_buf.get_or_insert_with(|| WriteBuffer {
            data: BytesMut::new(),
            dirty: false,
        });

        let needed = offset as usize + data.len();
        if needed > wb.data.len() {
            wb.data.resize(needed, 0);
        }
        wb.data[offset as usize..offset as usize + data.len()].copy_from_slice(data);
        wb.dirty = true;

        Ok(ReplyWrite {
            written: data.len() as u32,
        })
    }

    async fn flush(
        &self,
        _req: Request,
        _inode: u64,
        fh: u64,
        _lock_owner: u64,
    ) -> fuse3::Result<()> {
        self.flush_write_buffer(fh).await
    }

    async fn release(
        &self,
        _req: Request,
        _inode: u64,
        fh: u64,
        _flags: u32,
        _lock_owner: u64,
        _flush: bool,
    ) -> fuse3::Result<()> {
        // Flush any dirty write buffer before releasing
        let has_dirty = self
            .file_handles
            .get(&fh)
            .map(|h| h.write_buf.as_ref().map(|wb| wb.dirty).unwrap_or(false))
            .unwrap_or(false);

        if has_dirty {
            self.flush_write_buffer(fh).await?;
        }

        // Get the inode before removing the handle
        let ino = self.file_handles.get(&fh).map(|h| h.ino);
        self.file_handles.remove(&fh);

        // Handle deferred blob cleanup for unlinked files
        if let Some(ino) = ino
            && let Some((_, old_bytes)) = self.deferred_blob_cleanup.remove(&ino)
        {
            if !self.has_open_handles_for_inode(ino, None) {
                // Last handle closed, clean up blobs now
                let trace_id = TraceId::new();
                if let Ok(old_layout) =
                    rkyv::from_bytes::<ObjectLayout, rkyv::rancor::Error>(&old_bytes)
                {
                    self.backend
                        .delete_blob_blocks(&old_layout, &trace_id)
                        .await;
                }
            } else {
                // Still more handles open, re-insert
                self.deferred_blob_cleanup.insert(ino, old_bytes);
            }
        }

        Ok(())
    }

    async fn create(
        &self,
        _req: Request,
        parent: u64,
        name: &OsStr,
        _mode: u32,
        _flags: u32,
    ) -> fuse3::Result<ReplyCreated> {
        self.check_write_enabled()?;

        let name_str = name.to_str().ok_or_else(|| Errno::from(libc::EINVAL))?;
        let prefix = self
            .dir_prefix(parent)
            .ok_or_else(|| Errno::from(libc::ENOENT))?;
        let key = format!("{}{}", prefix, name_str);

        let (ino, _) = self.inodes.lookup_or_insert(&key, EntryType::File, None);

        let fh = self.alloc_fh();
        self.file_handles.insert(
            fh,
            FileHandle {
                ino,
                s3_key: key,
                layout: None,
                write_buf: Some(WriteBuffer {
                    data: BytesMut::new(),
                    dirty: true,
                }),
            },
        );

        let attr = self.make_new_file_attr(ino, 0);

        // Invalidate dir cache so the new file shows up in listings
        self.dir_cache.invalidate(&prefix).await;

        Ok(ReplyCreated {
            ttl: TTL,
            attr,
            generation: 0,
            fh,
            flags: 0,
        })
    }

    async fn unlink(&self, _req: Request, parent: u64, name: &OsStr) -> fuse3::Result<()> {
        self.check_write_enabled()?;

        let name_str = name.to_str().ok_or_else(|| Errno::from(libc::EINVAL))?;
        let prefix = self
            .dir_prefix(parent)
            .ok_or_else(|| Errno::from(libc::ENOENT))?;
        let key = format!("{}{}", prefix, name_str);

        let trace_id = TraceId::new();

        // Delete the inode from NSS
        let old_bytes = self
            .backend
            .delete_inode(&key, &trace_id)
            .await
            .map_err(std::io::Error::from)?;

        // Return ENOENT if file didn't exist
        let old_bytes = old_bytes.ok_or_else(|| Errno::from(libc::ENOENT))?;

        // Remove name mapping from inode table (read-only lookup, no refcount leak)
        let ino = self.inodes.find_ino_by_key(&key, EntryType::File);
        if let Some(ino) = ino {
            self.inodes.remove_name_mapping(ino);
        }

        // Handle blob cleanup: defer if file has open handles
        if !old_bytes.is_empty() {
            if let Some(ino) = ino
                && self.has_open_handles_for_inode(ino, None)
            {
                // Defer blob cleanup until last handle is released
                self.deferred_blob_cleanup.insert(ino, old_bytes);
            } else if let Ok(old_layout) =
                rkyv::from_bytes::<ObjectLayout, rkyv::rancor::Error>(&old_bytes)
            {
                match &old_layout.state {
                    ObjectState::Normal(_) => {
                        self.backend
                            .delete_blob_blocks(&old_layout, &trace_id)
                            .await;
                    }
                    ObjectState::Mpu(MpuState::Completed(_)) => {
                        if let Ok(parts) = self.backend.list_mpu_parts(&key, &trace_id).await {
                            for (part_key, part_layout) in &parts {
                                self.backend
                                    .delete_blob_blocks(part_layout, &trace_id)
                                    .await;
                                let _ = self.backend.delete_inode(part_key, &trace_id).await;
                            }
                        }
                    }
                    _ => {}
                }
            }
        }

        // Invalidate dir cache for parent
        self.dir_cache.invalidate(&prefix).await;

        Ok(())
    }

    async fn mkdir(
        &self,
        _req: Request,
        parent: u64,
        name: &OsStr,
        _mode: u32,
        _umask: u32,
    ) -> fuse3::Result<ReplyEntry> {
        self.check_write_enabled()?;

        let name_str = name.to_str().ok_or_else(|| Errno::from(libc::EINVAL))?;
        let prefix = self
            .dir_prefix(parent)
            .ok_or_else(|| Errno::from(libc::ENOENT))?;
        let key = format!("{}{}/", prefix, name_str);

        let trace_id = TraceId::new();
        self.backend
            .put_dir_marker(&key, &trace_id)
            .await
            .map_err(std::io::Error::from)?;

        let (ino, _) = self
            .inodes
            .lookup_or_insert(&key, EntryType::Directory, None);

        // Invalidate dir cache for parent
        self.dir_cache.invalidate(&prefix).await;

        let attr = self.make_dir_attr(ino);
        Ok(ReplyEntry {
            ttl: TTL,
            attr,
            generation: 0,
        })
    }

    async fn rmdir(&self, _req: Request, parent: u64, name: &OsStr) -> fuse3::Result<()> {
        self.check_write_enabled()?;

        let name_str = name.to_str().ok_or_else(|| Errno::from(libc::EINVAL))?;
        let prefix = self
            .dir_prefix(parent)
            .ok_or_else(|| Errno::from(libc::ENOENT))?;
        let key = format!("{}{}/", prefix, name_str);

        let trace_id = TraceId::new();

        // List to check existence and emptiness
        let entries = self
            .backend
            .list_objects(&key, "/", "", 2, &trace_id)
            .await
            .map_err(std::io::Error::from)?;

        // If no entries at all, directory doesn't exist
        if entries.is_empty() {
            return Err(Errno::from(libc::ENOENT));
        }

        let has_children = entries.iter().any(|e| e.key != key);
        if has_children {
            return Err(Errno::from(libc::ENOTEMPTY));
        }

        // Delete the directory marker
        self.backend
            .delete_inode(&key, &trace_id)
            .await
            .map_err(std::io::Error::from)?;

        // Remove from inode table (read-only lookup, no refcount leak)
        if let Some(ino) = self.inodes.find_ino_by_key(&key, EntryType::Directory) {
            self.inodes.remove_name_mapping(ino);
        }

        // Invalidate dir cache for parent and self
        self.dir_cache.invalidate(&prefix).await;
        self.dir_cache.invalidate(&key).await;

        Ok(())
    }

    async fn rename(
        &self,
        _req: Request,
        parent: u64,
        name: &OsStr,
        new_parent: u64,
        new_name: &OsStr,
    ) -> fuse3::Result<()> {
        self.check_write_enabled()?;

        let name_str = name.to_str().ok_or_else(|| Errno::from(libc::EINVAL))?;
        let new_name_str = new_name.to_str().ok_or_else(|| Errno::from(libc::EINVAL))?;

        let src_prefix = self
            .dir_prefix(parent)
            .ok_or_else(|| Errno::from(libc::ENOENT))?;
        let dst_prefix = self
            .dir_prefix(new_parent)
            .ok_or_else(|| Errno::from(libc::ENOENT))?;

        let src_key = format!("{}{}", src_prefix, name_str);
        let dst_key = format!("{}{}", dst_prefix, new_name_str);

        let trace_id = TraceId::new();

        // Determine type by probing NSS backend directly (no inode side effects)
        let is_dir = match self.backend.get_object(&src_key, &trace_id).await {
            Ok(_) => false,
            Err(FuseError::NotFound) => true,
            Err(e) => return Err(std::io::Error::from(e).into()),
        };

        if is_dir {
            let src_dir_key = format!("{}/", src_key);
            let dst_dir_key = format!("{}/", dst_key);

            self.backend
                .rename_folder(&src_dir_key, &dst_dir_key, &trace_id)
                .await
                .map_err(std::io::Error::from)?;

            // Update the directory inode's s3_key since the kernel still
            // holds a reference to it after rename.
            if let Some(ino) = self
                .inodes
                .find_ino_by_key(&src_dir_key, EntryType::Directory)
            {
                self.inodes.update_s3_key(ino, &dst_dir_key);
            }

            // Update cached child inodes to reflect the new prefix so the
            // kernel's existing inode references remain valid.
            self.inodes.rename_children(&src_dir_key, &dst_dir_key);

            self.dir_cache.invalidate(&src_prefix).await;
            self.dir_cache.invalidate(&dst_prefix).await;
            self.dir_cache.invalidate(&src_dir_key).await;
        } else {
            self.backend
                .rename_object(&src_key, &dst_key, &trace_id)
                .await
                .map_err(std::io::Error::from)?;

            // Update inode s3_key if cached (read-only lookup, no refcount leak)
            if let Some(ino) = self.inodes.find_ino_by_key(&src_key, EntryType::File) {
                self.inodes.update_s3_key(ino, &dst_key);
            }

            // Update any open file handles to reflect the new key
            for mut fh_entry in self.file_handles.iter_mut() {
                if fh_entry.value().s3_key == src_key {
                    fh_entry.value_mut().s3_key = dst_key.clone();
                }
            }

            self.dir_cache.invalidate(&src_prefix).await;
            self.dir_cache.invalidate(&dst_prefix).await;
        }

        Ok(())
    }

    async fn opendir(&self, _req: Request, inode: u64, _flags: u32) -> fuse3::Result<ReplyOpen> {
        if inode != ROOT_INODE {
            let entry = self
                .inodes
                .get(inode)
                .ok_or_else(|| Errno::from(libc::ENOENT))?;
            if entry.entry_type != EntryType::Directory {
                return Err(Errno::from(libc::ENOTDIR));
            }
        }

        let fh = self.alloc_fh();
        Ok(ReplyOpen { fh, flags: 0 })
    }

    async fn readdir(
        &self,
        _req: Request,
        parent: u64,
        _fh: u64,
        offset: i64,
    ) -> fuse3::Result<ReplyDirectory<Self::DirEntryStream<'_>>> {
        let prefix = self
            .dir_prefix(parent)
            .ok_or_else(|| Errno::from(libc::ENOENT))?;
        let dir_entries = self.fetch_dir_entries(parent, &prefix).await?;

        let offset = offset as usize;
        let fuse_entries: Vec<Result<DirectoryEntry, Errno>> = dir_entries
            .iter()
            .skip(offset)
            .enumerate()
            .map(|(idx, entry)| {
                Ok(DirectoryEntry {
                    inode: entry.ino,
                    kind: if entry.is_dir {
                        FileType::Directory
                    } else {
                        FileType::RegularFile
                    },
                    name: entry.name.clone().into(),
                    offset: (offset + idx + 1) as i64,
                })
            })
            .collect();

        Ok(ReplyDirectory {
            entries: stream::iter(fuse_entries),
        })
    }

    async fn readdirplus(
        &self,
        _req: Request,
        parent: u64,
        _fh: u64,
        offset: u64,
        _lock_owner: u64,
    ) -> fuse3::Result<ReplyDirectoryPlus<Self::DirEntryPlusStream<'_>>> {
        let prefix = self
            .dir_prefix(parent)
            .ok_or_else(|| Errno::from(libc::ENOENT))?;
        let dir_entries = self.fetch_dir_entries(parent, &prefix).await?;

        let offset = offset as usize;
        let fuse_entries: Vec<Result<DirectoryEntryPlus, Errno>> = dir_entries
            .iter()
            .skip(offset)
            .enumerate()
            .map(|(idx, entry)| {
                let attr = if entry.is_dir {
                    self.make_dir_attr(entry.ino)
                } else {
                    self.inodes
                        .get(entry.ino)
                        .and_then(|e| e.layout.as_ref().map(|l| self.make_file_attr(entry.ino, l)))
                        .transpose()?
                        .unwrap_or_else(|| self.make_default_file_attr(entry.ino))
                };
                Ok(DirectoryEntryPlus {
                    inode: entry.ino,
                    generation: 0,
                    kind: if entry.is_dir {
                        FileType::Directory
                    } else {
                        FileType::RegularFile
                    },
                    name: entry.name.clone().into(),
                    offset: (offset + idx + 1) as i64,
                    attr,
                    entry_ttl: TTL,
                    attr_ttl: TTL,
                })
            })
            .collect();

        Ok(ReplyDirectoryPlus {
            entries: stream::iter(fuse_entries),
        })
    }

    async fn releasedir(
        &self,
        _req: Request,
        _inode: u64,
        _fh: u64,
        _flags: u32,
    ) -> fuse3::Result<()> {
        Ok(())
    }

    async fn statfs(&self, _req: Request, _inode: u64) -> fuse3::Result<ReplyStatFs> {
        Ok(ReplyStatFs {
            blocks: 1024 * 1024,
            bfree: if self.read_write { 512 * 1024 } else { 0 },
            bavail: if self.read_write { 512 * 1024 } else { 0 },
            files: 1024 * 1024,
            ffree: if self.read_write { 512 * 1024 } else { 0 },
            bsize: DEFAULT_BLOCK_SIZE,
            namelen: 1024,
            frsize: DEFAULT_BLOCK_SIZE,
        })
    }
}
