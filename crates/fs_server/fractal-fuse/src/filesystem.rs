use std::ffi::OsStr;

use crate::types::*;

pub type Result<T> = std::result::Result<T, Errno>;

/// Async filesystem trait for FUSE operations.
///
/// All methods default to returning ENOSYS. Implement only the operations
/// your filesystem supports. Futures are `!Send` (compio single-threaded).
/// The trait object itself is `Send + Sync` for sharing via Arc across threads.
#[allow(clippy::too_many_arguments)]
pub trait Filesystem: Send + Sync + 'static {
    /// Initialize the filesystem. Return max_write and other capabilities.
    fn init(&self, req: Request) -> impl std::future::Future<Output = Result<ReplyInit>> {
        let _ = req;
        async { Ok(ReplyInit::default()) }
    }

    /// Clean up filesystem on unmount.
    fn destroy(&self) -> impl std::future::Future<Output = ()> {
        async {}
    }

    fn lookup(
        &self,
        req: Request,
        parent: Inode,
        name: &OsStr,
    ) -> impl std::future::Future<Output = Result<ReplyEntry>> {
        let _ = (req, parent, name);
        async { Err(ENOSYS) }
    }

    fn forget(&self, req: Request, inode: Inode, nlookup: u64) {
        let _ = (req, inode, nlookup);
    }

    fn batch_forget(&self, req: Request, inodes: &[(Inode, u64)]) {
        for &(inode, nlookup) in inodes {
            self.forget(req, inode, nlookup);
        }
    }

    fn getattr(
        &self,
        req: Request,
        inode: Inode,
        fh: Option<u64>,
        flags: u32,
    ) -> impl std::future::Future<Output = Result<ReplyAttr>> {
        let _ = (req, inode, fh, flags);
        async { Err(ENOSYS) }
    }

    fn setattr(
        &self,
        req: Request,
        inode: Inode,
        fh: Option<u64>,
        set_attr: SetAttr,
    ) -> impl std::future::Future<Output = Result<ReplyAttr>> {
        let _ = (req, inode, fh, set_attr);
        async { Err(ENOSYS) }
    }

    fn readlink(
        &self,
        req: Request,
        inode: Inode,
    ) -> impl std::future::Future<Output = Result<ReplyReadlink>> {
        let _ = (req, inode);
        async { Err(ENOSYS) }
    }

    fn symlink(
        &self,
        req: Request,
        parent: Inode,
        name: &OsStr,
        link: &OsStr,
    ) -> impl std::future::Future<Output = Result<ReplyEntry>> {
        let _ = (req, parent, name, link);
        async { Err(ENOSYS) }
    }

    fn mknod(
        &self,
        req: Request,
        parent: Inode,
        name: &OsStr,
        mode: u32,
        rdev: u32,
    ) -> impl std::future::Future<Output = Result<ReplyEntry>> {
        let _ = (req, parent, name, mode, rdev);
        async { Err(ENOSYS) }
    }

    fn mkdir(
        &self,
        req: Request,
        parent: Inode,
        name: &OsStr,
        mode: u32,
        umask: u32,
    ) -> impl std::future::Future<Output = Result<ReplyEntry>> {
        let _ = (req, parent, name, mode, umask);
        async { Err(ENOSYS) }
    }

    fn unlink(
        &self,
        req: Request,
        parent: Inode,
        name: &OsStr,
    ) -> impl std::future::Future<Output = Result<()>> {
        let _ = (req, parent, name);
        async { Err(ENOSYS) }
    }

    fn rmdir(
        &self,
        req: Request,
        parent: Inode,
        name: &OsStr,
    ) -> impl std::future::Future<Output = Result<()>> {
        let _ = (req, parent, name);
        async { Err(ENOSYS) }
    }

    fn rename(
        &self,
        req: Request,
        parent: Inode,
        name: &OsStr,
        new_parent: Inode,
        new_name: &OsStr,
        flags: u32,
    ) -> impl std::future::Future<Output = Result<()>> {
        let _ = (req, parent, name, new_parent, new_name, flags);
        async { Err(ENOSYS) }
    }

    fn link(
        &self,
        req: Request,
        inode: Inode,
        new_parent: Inode,
        new_name: &OsStr,
    ) -> impl std::future::Future<Output = Result<ReplyEntry>> {
        let _ = (req, inode, new_parent, new_name);
        async { Err(ENOSYS) }
    }

    fn open(
        &self,
        req: Request,
        inode: Inode,
        flags: u32,
    ) -> impl std::future::Future<Output = Result<ReplyOpen>> {
        let _ = (req, inode, flags);
        async { Err(ENOSYS) }
    }

    fn read(
        &self,
        req: Request,
        inode: Inode,
        fh: u64,
        offset: u64,
        size: u32,
    ) -> impl std::future::Future<Output = Result<ReplyData>> {
        let _ = (req, inode, fh, offset, size);
        async { Err(ENOSYS) }
    }

    fn write(
        &self,
        req: Request,
        inode: Inode,
        fh: u64,
        offset: u64,
        data: &[u8],
        write_flags: u32,
        flags: u32,
    ) -> impl std::future::Future<Output = Result<ReplyWrite>> {
        let _ = (req, inode, fh, offset, data, write_flags, flags);
        async { Err(ENOSYS) }
    }

    fn flush(
        &self,
        req: Request,
        inode: Inode,
        fh: u64,
        lock_owner: u64,
    ) -> impl std::future::Future<Output = Result<()>> {
        let _ = (req, inode, fh, lock_owner);
        async { Err(ENOSYS) }
    }

    fn release(
        &self,
        req: Request,
        inode: Inode,
        fh: u64,
        flags: u32,
        lock_owner: u64,
        flush: bool,
    ) -> impl std::future::Future<Output = Result<()>> {
        let _ = (req, inode, fh, flags, lock_owner, flush);
        async { Err(ENOSYS) }
    }

    fn fsync(
        &self,
        req: Request,
        inode: Inode,
        fh: u64,
        datasync: bool,
    ) -> impl std::future::Future<Output = Result<()>> {
        let _ = (req, inode, fh, datasync);
        async { Err(ENOSYS) }
    }

    fn opendir(
        &self,
        req: Request,
        inode: Inode,
        flags: u32,
    ) -> impl std::future::Future<Output = Result<ReplyOpen>> {
        let _ = (req, inode, flags);
        async { Err(ENOSYS) }
    }

    fn readdir(
        &self,
        req: Request,
        inode: Inode,
        fh: u64,
        offset: u64,
        size: u32,
    ) -> impl std::future::Future<Output = Result<Vec<DirectoryEntry>>> {
        let _ = (req, inode, fh, offset, size);
        async { Err(ENOSYS) }
    }

    fn readdirplus(
        &self,
        req: Request,
        inode: Inode,
        fh: u64,
        offset: u64,
        size: u32,
    ) -> impl std::future::Future<Output = Result<Vec<DirectoryEntryPlus>>> {
        let _ = (req, inode, fh, offset, size);
        async { Err(ENOSYS) }
    }

    fn releasedir(
        &self,
        req: Request,
        inode: Inode,
        fh: u64,
        flags: u32,
    ) -> impl std::future::Future<Output = Result<()>> {
        let _ = (req, inode, fh, flags);
        async { Err(ENOSYS) }
    }

    fn fsyncdir(
        &self,
        req: Request,
        inode: Inode,
        fh: u64,
        datasync: bool,
    ) -> impl std::future::Future<Output = Result<()>> {
        let _ = (req, inode, fh, datasync);
        async { Err(ENOSYS) }
    }

    fn statfs(
        &self,
        req: Request,
        inode: Inode,
    ) -> impl std::future::Future<Output = Result<ReplyStatfs>> {
        let _ = (req, inode);
        async {
            Ok(ReplyStatfs {
                blocks: 0,
                bfree: 0,
                bavail: 0,
                files: 0,
                ffree: 0,
                bsize: 512,
                namelen: 255,
                frsize: 512,
            })
        }
    }

    fn access(
        &self,
        req: Request,
        inode: Inode,
        mask: u32,
    ) -> impl std::future::Future<Output = Result<()>> {
        let _ = (req, inode, mask);
        async { Err(ENOSYS) }
    }

    fn create(
        &self,
        req: Request,
        parent: Inode,
        name: &OsStr,
        mode: u32,
        flags: u32,
    ) -> impl std::future::Future<Output = Result<ReplyCreate>> {
        let _ = (req, parent, name, mode, flags);
        async { Err(ENOSYS) }
    }

    fn fallocate(
        &self,
        req: Request,
        inode: Inode,
        fh: u64,
        offset: u64,
        length: u64,
        mode: u32,
    ) -> impl std::future::Future<Output = Result<()>> {
        let _ = (req, inode, fh, offset, length, mode);
        async { Err(ENOSYS) }
    }

    fn lseek(
        &self,
        req: Request,
        inode: Inode,
        fh: u64,
        offset: u64,
        whence: u32,
    ) -> impl std::future::Future<Output = Result<u64>> {
        let _ = (req, inode, fh, offset, whence);
        async { Err(ENOSYS) }
    }

    fn copy_file_range(
        &self,
        req: Request,
        inode_in: Inode,
        fh_in: u64,
        off_in: u64,
        inode_out: Inode,
        fh_out: u64,
        off_out: u64,
        length: u64,
        flags: u64,
    ) -> impl std::future::Future<Output = Result<ReplyWrite>> {
        let _ = (
            req, inode_in, fh_in, off_in, inode_out, fh_out, off_out, length, flags,
        );
        async { Err(ENOSYS) }
    }
}
