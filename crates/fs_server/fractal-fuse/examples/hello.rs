//! A minimal read-only FUSE filesystem that exposes a single file "hello"
//! containing "Hello, FUSE over io_uring!\n".
//!
//! Usage:
//!   mkdir -p /tmp/hello_fuse
//!   cargo run --example hello -- /tmp/hello_fuse
//!
//! Then in another terminal:
//!   ls /tmp/hello_fuse/
//!   cat /tmp/hello_fuse/hello
//!
//! Unmount with:
//!   fusermount3 -u /tmp/hello_fuse

use std::ffi::OsStr;
use std::path::PathBuf;
use std::time::Duration;

use fractal_fuse::abi::FUSE_ROOT_ID;
use fractal_fuse::{
    DirectoryEntry, DirectoryEntryPlus, ENOENT, FileAttr, FileType, Filesystem, FsResult,
    MountOptions, ReplyAttr, ReplyData, ReplyEntry, ReplyOpen, ReplyStatfs, Request, Session,
    Timestamp,
};

const HELLO_INO: u64 = 2;
const HELLO_CONTENT: &[u8] = b"Hello, FUSE over io_uring!\n";
const TTL: Duration = Duration::from_secs(60);

fn now_ts() -> Timestamp {
    let d = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    Timestamp::new(d.as_secs(), d.subsec_nanos())
}

fn root_attr() -> FileAttr {
    let ts = now_ts();
    FileAttr {
        ino: FUSE_ROOT_ID,
        size: 0,
        blocks: 0,
        atime: ts,
        mtime: ts,
        ctime: ts,
        mode: FileType::Directory.to_mode() | 0o755,
        nlink: 2,
        uid: unsafe { libc::getuid() },
        gid: unsafe { libc::getgid() },
        rdev: 0,
        blksize: 512,
    }
}

fn hello_attr() -> FileAttr {
    let ts = now_ts();
    FileAttr {
        ino: HELLO_INO,
        size: HELLO_CONTENT.len() as u64,
        blocks: 1,
        atime: ts,
        mtime: ts,
        ctime: ts,
        mode: FileType::RegularFile.to_mode() | 0o444,
        nlink: 1,
        uid: unsafe { libc::getuid() },
        gid: unsafe { libc::getgid() },
        rdev: 0,
        blksize: 512,
    }
}

struct HelloFs;

impl Filesystem for HelloFs {
    async fn lookup(&self, _req: Request, parent: u64, name: &OsStr) -> FsResult<ReplyEntry> {
        if parent == FUSE_ROOT_ID && name == "hello" {
            Ok(ReplyEntry {
                ttl: TTL,
                attr: hello_attr(),
                generation: 0,
            })
        } else {
            Err(ENOENT)
        }
    }

    async fn getattr(
        &self,
        _req: Request,
        inode: u64,
        _fh: Option<u64>,
        _flags: u32,
    ) -> FsResult<ReplyAttr> {
        match inode {
            FUSE_ROOT_ID => Ok(ReplyAttr {
                ttl: TTL,
                attr: root_attr(),
            }),
            HELLO_INO => Ok(ReplyAttr {
                ttl: TTL,
                attr: hello_attr(),
            }),
            _ => Err(ENOENT),
        }
    }

    async fn open(&self, _req: Request, inode: u64, _flags: u32) -> FsResult<ReplyOpen> {
        if inode == HELLO_INO {
            Ok(ReplyOpen {
                fh: 0,
                flags: 0,
                backing_id: 0,
            })
        } else {
            Err(ENOENT)
        }
    }

    async fn read(
        &self,
        _req: Request,
        inode: u64,
        _fh: u64,
        offset: u64,
        size: u32,
    ) -> FsResult<ReplyData> {
        if inode != HELLO_INO {
            return Err(ENOENT);
        }
        let offset = offset as usize;
        if offset >= HELLO_CONTENT.len() {
            return Ok(ReplyData {
                data: bytes::Bytes::new(),
            });
        }
        let end = (offset + size as usize).min(HELLO_CONTENT.len());
        Ok(ReplyData {
            data: bytes::Bytes::from_static(&HELLO_CONTENT[offset..end]),
        })
    }

    async fn readdir(
        &self,
        _req: Request,
        inode: u64,
        _fh: u64,
        offset: u64,
        _size: u32,
    ) -> FsResult<Vec<DirectoryEntry>> {
        if inode != FUSE_ROOT_ID {
            return Err(ENOENT);
        }
        let entries = vec![
            DirectoryEntry {
                ino: FUSE_ROOT_ID,
                offset: 1,
                kind: FileType::Directory,
                name: b".".to_vec(),
            },
            DirectoryEntry {
                ino: FUSE_ROOT_ID,
                offset: 2,
                kind: FileType::Directory,
                name: b"..".to_vec(),
            },
            DirectoryEntry {
                ino: HELLO_INO,
                offset: 3,
                kind: FileType::RegularFile,
                name: b"hello".to_vec(),
            },
        ];
        Ok(entries.into_iter().filter(|e| e.offset > offset).collect())
    }

    async fn readdirplus(
        &self,
        _req: Request,
        inode: u64,
        _fh: u64,
        offset: u64,
        _size: u32,
    ) -> FsResult<Vec<DirectoryEntryPlus>> {
        if inode != FUSE_ROOT_ID {
            return Err(ENOENT);
        }
        let entries = vec![
            DirectoryEntryPlus {
                ino: FUSE_ROOT_ID,
                offset: 1,
                kind: FileType::Directory,
                name: b".".to_vec(),
                entry_ttl: TTL,
                attr: root_attr(),
                generation: 0,
            },
            DirectoryEntryPlus {
                ino: FUSE_ROOT_ID,
                offset: 2,
                kind: FileType::Directory,
                name: b"..".to_vec(),
                entry_ttl: TTL,
                attr: root_attr(),
                generation: 0,
            },
            DirectoryEntryPlus {
                ino: HELLO_INO,
                offset: 3,
                kind: FileType::RegularFile,
                name: b"hello".to_vec(),
                entry_ttl: TTL,
                attr: hello_attr(),
                generation: 0,
            },
        ];
        Ok(entries.into_iter().filter(|e| e.offset > offset).collect())
    }

    async fn opendir(&self, _req: Request, _inode: u64, _flags: u32) -> FsResult<ReplyOpen> {
        Ok(ReplyOpen {
            fh: 0,
            flags: 0,
            backing_id: 0,
        })
    }

    async fn statfs(&self, _req: Request, _inode: u64) -> FsResult<ReplyStatfs> {
        Ok(ReplyStatfs {
            blocks: 0,
            bfree: 0,
            bavail: 0,
            files: 2,
            ffree: 0,
            bsize: 512,
            namelen: 255,
            frsize: 512,
        })
    }

    async fn access(&self, _req: Request, _inode: u64, _mask: u32) -> FsResult<()> {
        Ok(())
    }
}

fn main() -> std::io::Result<()> {
    tracing_subscriber::fmt::init();

    let mount_path: PathBuf = std::env::args_os()
        .nth(1)
        .expect("usage: hello <mountpoint>")
        .into();

    if !mount_path.is_dir() {
        eprintln!("error: {:?} is not a directory", mount_path);
        std::process::exit(1);
    }

    eprintln!("mounting hello filesystem at {:?}", mount_path);
    eprintln!("try: ls {:?}  or  cat {:?}/hello", mount_path, mount_path);
    eprintln!("unmount with: fusermount3 -u {:?}", mount_path);

    let opts = MountOptions::new()
        .fs_name("hellofs")
        .read_only(true)
        .default_permissions(true);

    Session::new(opts).run(HelloFs, &mount_path)
}
