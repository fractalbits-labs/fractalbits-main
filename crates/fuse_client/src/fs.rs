use bytes::{Bytes, BytesMut};
use dashmap::DashMap;
use data_types::TraceId;
use fuse3::Errno;
use fuse3::raw::reply::{
    DirectoryEntry, DirectoryEntryPlus, FileAttr, ReplyAttr, ReplyData, ReplyDirectory,
    ReplyDirectoryPlus, ReplyEntry, ReplyInit, ReplyOpen, ReplyStatFs,
};
use fuse3::raw::{Filesystem, Request};
use fuse3::{FileType, Timestamp};
use futures::stream;
use std::ffi::OsStr;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use crate::backend::StorageBackend;
use crate::cache::{BlockCache, DirCache, DirEntry};
use crate::error::FuseError;
use crate::inode::{EntryType, InodeTable, ROOT_INODE};
use crate::object_layout::{MpuState, ObjectLayout, ObjectState};

const TTL: Duration = Duration::from_secs(1);
const DEFAULT_BLOCK_SIZE: u32 = 1024 * 1024 - 256;

struct FileHandle {
    _ino: u64,
    s3_key: String,
    layout: ObjectLayout,
}

pub struct FuseFs {
    backend: Arc<StorageBackend>,
    inodes: Arc<InodeTable>,
    block_cache: BlockCache,
    dir_cache: DirCache,
    file_handles: DashMap<u64, FileHandle>,
    next_fh: AtomicU64,
}

impl FuseFs {
    pub fn new(backend: Arc<StorageBackend>, inodes: Arc<InodeTable>) -> Self {
        let block_cache_size_mb = backend.config().block_cache_size_mb;
        let dir_cache_ttl = backend.config().dir_cache_ttl();
        Self {
            backend,
            inodes,
            block_cache: BlockCache::new(block_cache_size_mb),
            dir_cache: DirCache::new(dir_cache_ttl),
            file_handles: DashMap::new(),
            next_fh: AtomicU64::new(1),
        }
    }

    fn alloc_fh(&self) -> u64 {
        self.next_fh.fetch_add(1, Ordering::Relaxed)
    }

    fn dir_prefix(&self, ino: u64) -> Option<String> {
        self.inodes.get_s3_key(ino)
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

                let name = if raw_key.len() > prefix.len() {
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
            perm: 0o444,
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
            perm: 0o444,
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
            perm: 0o555,
            nlink: 2,
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
        _fh: Option<u64>,
        _flags: u32,
    ) -> fuse3::Result<ReplyAttr> {
        if inode == ROOT_INODE {
            return Ok(ReplyAttr {
                ttl: TTL,
                attr: self.make_dir_attr(ROOT_INODE),
            });
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

    async fn open(&self, _req: Request, inode: u64, flags: u32) -> fuse3::Result<ReplyOpen> {
        let write_flags = libc::O_WRONLY as u32
            | libc::O_RDWR as u32
            | libc::O_APPEND as u32
            | libc::O_TRUNC as u32;
        if flags & write_flags != 0 {
            return Err(Errno::from(libc::EROFS));
        }

        let entry = self
            .inodes
            .get(inode)
            .ok_or_else(|| Errno::from(libc::ENOENT))?;

        if entry.entry_type != EntryType::File {
            return Err(Errno::from(libc::EISDIR));
        }

        // Clone key + layout from inode entry. The key is stored in the file
        // handle so that read() doesn't depend on the inode table (the inode
        // can be forgotten while the file handle is still open).
        let s3_key = entry.s3_key.clone();
        let layout = match entry.layout {
            Some(ref l) => l.clone(),
            None => {
                drop(entry);
                let trace_id = TraceId::new();
                self.backend
                    .get_object(&s3_key, &trace_id)
                    .await
                    .map_err(std::io::Error::from)?
            }
        };

        let fh = self.alloc_fh();
        self.file_handles.insert(
            fh,
            FileHandle {
                _ino: inode,
                s3_key,
                layout,
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

        let s3_key = handle.s3_key.clone();
        let layout = handle.layout.clone();
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

    async fn release(
        &self,
        _req: Request,
        _inode: u64,
        fh: u64,
        _flags: u32,
        _lock_owner: u64,
        _flush: bool,
    ) -> fuse3::Result<()> {
        self.file_handles.remove(&fh);
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
            bfree: 0,
            bavail: 0,
            files: 1024 * 1024,
            ffree: 0,
            bsize: DEFAULT_BLOCK_SIZE,
            namelen: 1024,
            frsize: DEFAULT_BLOCK_SIZE,
        })
    }
}
