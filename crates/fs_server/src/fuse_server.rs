use fractal_fuse::*;
use std::ffi::OsStr;

use crate::error::FsError;
use crate::vfs::{TTL, VfsAttr, VfsCore};

pub struct FuseServer {
    vfs: VfsCore,
}

impl FuseServer {
    pub fn new(vfs: VfsCore) -> Self {
        Self { vfs }
    }
}

fn to_file_attr(va: &VfsAttr) -> FileAttr {
    FileAttr {
        ino: va.ino,
        size: va.size,
        blocks: va.blocks,
        atime: Timestamp::new(va.atime_secs, 0),
        mtime: Timestamp::new(va.mtime_secs, 0),
        ctime: Timestamp::new(va.ctime_secs, 0),
        mode: va.mode,
        nlink: va.nlink,
        uid: va.uid,
        gid: va.gid,
        rdev: va.rdev,
        blksize: va.blksize,
    }
}

fn fs_err(e: FsError) -> Errno {
    e.into()
}

impl Filesystem for FuseServer {
    async fn init(&self, _req: Request) -> fractal_fuse::Result<ReplyInit> {
        self.vfs.vfs_init();
        Ok(ReplyInit {
            max_write: 1024 * 1024,
            ..Default::default()
        })
    }

    async fn destroy(&self) {
        self.vfs.vfs_destroy();
    }

    async fn lookup(
        &self,
        _req: Request,
        parent: u64,
        name: &OsStr,
    ) -> fractal_fuse::Result<ReplyEntry> {
        let name_str = name.to_str().ok_or(libc::EINVAL)?;
        let attr = self
            .vfs
            .vfs_lookup(parent, name_str)
            .await
            .map_err(fs_err)?;
        Ok(ReplyEntry {
            ttl: TTL,
            attr: to_file_attr(&attr),
            generation: 0,
        })
    }

    fn forget(&self, _req: Request, inode: u64, nlookup: u64) {
        self.vfs.vfs_forget(inode, nlookup);
    }

    async fn getattr(
        &self,
        _req: Request,
        inode: u64,
        fh: Option<u64>,
        _flags: u32,
    ) -> fractal_fuse::Result<ReplyAttr> {
        let attr = self.vfs.vfs_getattr(inode, fh).await.map_err(fs_err)?;
        Ok(ReplyAttr {
            ttl: TTL,
            attr: to_file_attr(&attr),
        })
    }

    async fn setattr(
        &self,
        _req: Request,
        inode: u64,
        fh: Option<u64>,
        set_attr: SetAttr,
    ) -> fractal_fuse::Result<ReplyAttr> {
        // Only handle truncate to zero
        if let Some(0) = set_attr.size {
            let fh_id = fh.ok_or(libc::ENOSYS)?;
            let attr = self
                .vfs
                .vfs_setattr_truncate(inode, fh_id)
                .await
                .map_err(fs_err)?;
            return Ok(ReplyAttr {
                ttl: TTL,
                attr: to_file_attr(&attr),
            });
        }

        // For other setattr calls, just return current attr
        let attr = self.vfs.vfs_getattr(inode, fh).await.map_err(fs_err)?;
        Ok(ReplyAttr {
            ttl: TTL,
            attr: to_file_attr(&attr),
        })
    }

    async fn open(&self, _req: Request, inode: u64, flags: u32) -> fractal_fuse::Result<ReplyOpen> {
        let fh = self.vfs.vfs_open(inode, flags).await.map_err(fs_err)?;
        Ok(ReplyOpen { fh, flags: 0 })
    }

    async fn read(
        &self,
        _req: Request,
        _inode: u64,
        fh: u64,
        offset: u64,
        size: u32,
    ) -> fractal_fuse::Result<ReplyData> {
        let data = self.vfs.vfs_read(fh, offset, size).await.map_err(fs_err)?;
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
    ) -> fractal_fuse::Result<ReplyWrite> {
        let written = self.vfs.vfs_write(fh, offset, data).await.map_err(fs_err)?;
        Ok(ReplyWrite { written })
    }

    async fn flush(
        &self,
        _req: Request,
        _inode: u64,
        fh: u64,
        _lock_owner: u64,
    ) -> fractal_fuse::Result<()> {
        self.vfs.vfs_flush(fh).await.map_err(fs_err)
    }

    async fn release(
        &self,
        _req: Request,
        _inode: u64,
        fh: u64,
        _flags: u32,
        _lock_owner: u64,
        _flush: bool,
    ) -> fractal_fuse::Result<()> {
        self.vfs.vfs_release(fh).await.map_err(fs_err)
    }

    async fn create(
        &self,
        _req: Request,
        parent: u64,
        name: &OsStr,
        _mode: u32,
        _flags: u32,
    ) -> fractal_fuse::Result<ReplyCreate> {
        let name_str = name.to_str().ok_or(libc::EINVAL)?;
        let (attr, fh) = self
            .vfs
            .vfs_create(parent, name_str)
            .await
            .map_err(fs_err)?;
        Ok(ReplyCreate {
            ttl: TTL,
            attr: to_file_attr(&attr),
            generation: 0,
            fh,
            flags: 0,
        })
    }

    async fn unlink(&self, _req: Request, parent: u64, name: &OsStr) -> fractal_fuse::Result<()> {
        let name_str = name.to_str().ok_or(libc::EINVAL)?;
        self.vfs.vfs_unlink(parent, name_str).await.map_err(fs_err)
    }

    async fn mkdir(
        &self,
        _req: Request,
        parent: u64,
        name: &OsStr,
        _mode: u32,
        _umask: u32,
    ) -> fractal_fuse::Result<ReplyEntry> {
        let name_str = name.to_str().ok_or(libc::EINVAL)?;
        let attr = self.vfs.vfs_mkdir(parent, name_str).await.map_err(fs_err)?;
        Ok(ReplyEntry {
            ttl: TTL,
            attr: to_file_attr(&attr),
            generation: 0,
        })
    }

    async fn rmdir(&self, _req: Request, parent: u64, name: &OsStr) -> fractal_fuse::Result<()> {
        let name_str = name.to_str().ok_or(libc::EINVAL)?;
        self.vfs.vfs_rmdir(parent, name_str).await.map_err(fs_err)
    }

    async fn rename(
        &self,
        _req: Request,
        parent: u64,
        name: &OsStr,
        new_parent: u64,
        new_name: &OsStr,
        _flags: u32,
    ) -> fractal_fuse::Result<()> {
        let name_str = name.to_str().ok_or(libc::EINVAL)?;
        let new_name_str = new_name.to_str().ok_or(libc::EINVAL)?;
        self.vfs
            .vfs_rename(parent, name_str, new_parent, new_name_str)
            .await
            .map_err(fs_err)
    }

    async fn opendir(
        &self,
        _req: Request,
        inode: u64,
        _flags: u32,
    ) -> fractal_fuse::Result<ReplyOpen> {
        let fh = self.vfs.vfs_opendir(inode).map_err(fs_err)?;
        Ok(ReplyOpen { fh, flags: 0 })
    }

    async fn readdir(
        &self,
        _req: Request,
        parent: u64,
        _fh: u64,
        offset: u64,
        _size: u32,
    ) -> fractal_fuse::Result<Vec<DirectoryEntry>> {
        let entries = self.vfs.vfs_readdir(parent, offset).await.map_err(fs_err)?;
        Ok(entries
            .into_iter()
            .map(|e| DirectoryEntry {
                ino: e.ino,
                kind: if e.is_dir {
                    FileType::Directory
                } else {
                    FileType::RegularFile
                },
                name: e.name.into_bytes(),
                offset: e.offset,
            })
            .collect())
    }

    async fn readdirplus(
        &self,
        _req: Request,
        parent: u64,
        _fh: u64,
        offset: u64,
        _size: u32,
    ) -> fractal_fuse::Result<Vec<DirectoryEntryPlus>> {
        let entries = self
            .vfs
            .vfs_readdirplus(parent, offset)
            .await
            .map_err(fs_err)?;
        Ok(entries
            .into_iter()
            .map(|e| DirectoryEntryPlus {
                ino: e.ino,
                generation: 0,
                kind: if e.is_dir {
                    FileType::Directory
                } else {
                    FileType::RegularFile
                },
                name: e.name.into_bytes(),
                offset: e.offset,
                attr: to_file_attr(&e.attr),
                entry_ttl: TTL,
            })
            .collect())
    }

    async fn releasedir(
        &self,
        _req: Request,
        _inode: u64,
        _fh: u64,
        _flags: u32,
    ) -> fractal_fuse::Result<()> {
        Ok(())
    }

    async fn statfs(&self, _req: Request, _inode: u64) -> fractal_fuse::Result<ReplyStatfs> {
        let s = self.vfs.vfs_statfs();
        Ok(ReplyStatfs {
            blocks: s.blocks,
            bfree: s.bfree,
            bavail: s.bavail,
            files: s.files,
            ffree: s.ffree,
            bsize: s.bsize,
            namelen: s.namelen,
            frsize: s.frsize,
        })
    }
}
