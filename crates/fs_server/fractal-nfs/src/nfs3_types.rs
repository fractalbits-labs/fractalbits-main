/// NFSv3 type definitions per RFC 1813.
use bytes::Bytes;

use crate::xdr::{XdrError, XdrReader, XdrWriter};

// ── Status Codes ──

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum Nfsstat3 {
    Ok = 0,
    Perm = 1,
    Noent = 2,
    Io = 5,
    Nxio = 6,
    Acces = 13,
    Exist = 17,
    Xdev = 18,
    Nodev = 19,
    Notdir = 20,
    Isdir = 21,
    Inval = 22,
    Fbig = 27,
    Nospc = 28,
    Rofs = 30,
    Mlink = 31,
    Nametoolong = 63,
    Notempty = 66,
    Dquot = 69,
    Stale = 70,
    Remote = 71,
    Badhandle = 10001,
    NotSync = 10002,
    BadCookie = 10003,
    NotSupp = 10004,
    TooSmall = 10005,
    ServerFault = 10006,
    Badtype = 10007,
    Jukebox = 10008,
}

impl Nfsstat3 {
    pub fn encode(&self, w: &mut XdrWriter) {
        w.write_u32(*self as u32);
    }
}

// ── File Types ──

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum Ftype3 {
    Reg = 1,
    Dir = 2,
    Blk = 3,
    Chr = 4,
    Lnk = 5,
    Sock = 6,
    Fifo = 7,
}

impl Ftype3 {
    pub fn from_mode(mode: u32) -> Self {
        match mode & libc::S_IFMT {
            x if x == libc::S_IFREG => Ftype3::Reg,
            x if x == libc::S_IFDIR => Ftype3::Dir,
            x if x == libc::S_IFBLK => Ftype3::Blk,
            x if x == libc::S_IFCHR => Ftype3::Chr,
            x if x == libc::S_IFLNK => Ftype3::Lnk,
            x if x == libc::S_IFSOCK => Ftype3::Sock,
            x if x == libc::S_IFIFO => Ftype3::Fifo,
            _ => Ftype3::Reg,
        }
    }
}

// ── File Handle ──

/// NFS file handle: 16 bytes = ino (u64) + fsid (u64).
pub const NFS_FH_SIZE: usize = 16;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NfsFh3 {
    pub data: Bytes,
}

impl NfsFh3 {
    pub fn new(ino: u64, fsid: u64) -> Self {
        let mut buf = [0u8; NFS_FH_SIZE];
        buf[..8].copy_from_slice(&ino.to_be_bytes());
        buf[8..].copy_from_slice(&fsid.to_be_bytes());
        Self {
            data: Bytes::copy_from_slice(&buf),
        }
    }

    pub fn ino(&self) -> u64 {
        if self.data.len() >= 8 {
            u64::from_be_bytes(self.data[..8].try_into().unwrap())
        } else {
            0
        }
    }

    pub fn fsid(&self) -> u64 {
        if self.data.len() >= 16 {
            u64::from_be_bytes(self.data[8..16].try_into().unwrap())
        } else {
            0
        }
    }

    pub fn decode(r: &mut XdrReader<'_>) -> Result<Self, XdrError> {
        let data = r.read_opaque()?;
        Ok(Self {
            data: Bytes::copy_from_slice(data),
        })
    }

    pub fn encode(&self, w: &mut XdrWriter) {
        w.write_opaque(&self.data);
    }
}

// ── Time ──

#[derive(Debug, Clone, Copy, Default)]
pub struct Nfstime3 {
    pub seconds: u32,
    pub nseconds: u32,
}

impl Nfstime3 {
    pub fn new(seconds: u32, nseconds: u32) -> Self {
        Self { seconds, nseconds }
    }

    pub fn from_secs(s: u64) -> Self {
        Self {
            seconds: s as u32,
            nseconds: 0,
        }
    }

    pub fn encode(&self, w: &mut XdrWriter) {
        w.write_u32(self.seconds);
        w.write_u32(self.nseconds);
    }

    pub fn decode(r: &mut XdrReader<'_>) -> Result<Self, XdrError> {
        Ok(Self {
            seconds: r.read_u32()?,
            nseconds: r.read_u32()?,
        })
    }
}

// ── File Attributes ──

#[derive(Debug, Clone)]
pub struct Fattr3 {
    pub ftype: Ftype3,
    pub mode: u32,
    pub nlink: u32,
    pub uid: u32,
    pub gid: u32,
    pub size: u64,
    pub used: u64,
    pub rdev: Specdata3,
    pub fsid: u64,
    pub fileid: u64,
    pub atime: Nfstime3,
    pub mtime: Nfstime3,
    pub ctime: Nfstime3,
}

impl Fattr3 {
    pub fn encode(&self, w: &mut XdrWriter) {
        w.write_u32(self.ftype as u32);
        w.write_u32(self.mode);
        w.write_u32(self.nlink);
        w.write_u32(self.uid);
        w.write_u32(self.gid);
        w.write_u64(self.size);
        w.write_u64(self.used);
        self.rdev.encode(w);
        w.write_u64(self.fsid);
        w.write_u64(self.fileid);
        self.atime.encode(w);
        self.mtime.encode(w);
        self.ctime.encode(w);
    }
}

/// Optional post-op attributes (used in almost every NFS response).
pub fn encode_post_op_attr(w: &mut XdrWriter, attr: Option<&Fattr3>) {
    match attr {
        Some(a) => {
            w.write_bool(true);
            a.encode(w);
        }
        None => {
            w.write_bool(false);
        }
    }
}

/// Encode optional post-op file handle.
pub fn encode_post_op_fh(w: &mut XdrWriter, fh: Option<&NfsFh3>) {
    match fh {
        Some(h) => {
            w.write_bool(true);
            h.encode(w);
        }
        None => {
            w.write_bool(false);
        }
    }
}

// ── Specdata (rdev) ──

#[derive(Debug, Clone, Copy, Default)]
pub struct Specdata3 {
    pub specdata1: u32,
    pub specdata2: u32,
}

impl Specdata3 {
    pub fn encode(&self, w: &mut XdrWriter) {
        w.write_u32(self.specdata1);
        w.write_u32(self.specdata2);
    }
}

// ── Set Attribute Types ──

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum TimeHow {
    DontChange = 0,
    SetToServerTime = 1,
    SetToClientTime = 2,
}

#[derive(Debug, Clone)]
pub struct Sattr3 {
    pub mode: Option<u32>,
    pub uid: Option<u32>,
    pub gid: Option<u32>,
    pub size: Option<u64>,
    pub atime: TimeHow,
    pub atime_val: Nfstime3,
    pub mtime: TimeHow,
    pub mtime_val: Nfstime3,
}

impl Sattr3 {
    pub fn decode(r: &mut XdrReader<'_>) -> Result<Self, XdrError> {
        let mode = if r.read_bool()? {
            Some(r.read_u32()?)
        } else {
            None
        };
        let uid = if r.read_bool()? {
            Some(r.read_u32()?)
        } else {
            None
        };
        let gid = if r.read_bool()? {
            Some(r.read_u32()?)
        } else {
            None
        };
        let size = if r.read_bool()? {
            Some(r.read_u64()?)
        } else {
            None
        };
        let atime_how = r.read_u32()?;
        let atime = match atime_how {
            2 => TimeHow::SetToClientTime,
            1 => TimeHow::SetToServerTime,
            _ => TimeHow::DontChange,
        };
        let atime_val = if atime == TimeHow::SetToClientTime {
            Nfstime3::decode(r)?
        } else {
            Nfstime3::default()
        };
        let mtime_how = r.read_u32()?;
        let mtime = match mtime_how {
            2 => TimeHow::SetToClientTime,
            1 => TimeHow::SetToServerTime,
            _ => TimeHow::DontChange,
        };
        let mtime_val = if mtime == TimeHow::SetToClientTime {
            Nfstime3::decode(r)?
        } else {
            Nfstime3::default()
        };

        Ok(Self {
            mode,
            uid,
            gid,
            size,
            atime,
            atime_val,
            mtime,
            mtime_val,
        })
    }
}

// ── WCC (Weak Cache Consistency) ──

/// Pre-op attributes for WCC.
#[derive(Debug, Clone)]
pub struct WccAttr {
    pub size: u64,
    pub mtime: Nfstime3,
    pub ctime: Nfstime3,
}

impl WccAttr {
    pub fn encode(&self, w: &mut XdrWriter) {
        w.write_u64(self.size);
        self.mtime.encode(w);
        self.ctime.encode(w);
    }
}

/// WCC data: optional pre-op + optional post-op attributes.
pub fn encode_wcc_data(w: &mut XdrWriter, pre: Option<&WccAttr>, post: Option<&Fattr3>) {
    match pre {
        Some(p) => {
            w.write_bool(true);
            p.encode(w);
        }
        None => w.write_bool(false),
    }
    encode_post_op_attr(w, post);
}

// ── NFS Access Bits ──

pub const ACCESS3_READ: u32 = 0x0001;
pub const ACCESS3_LOOKUP: u32 = 0x0002;
pub const ACCESS3_MODIFY: u32 = 0x0004;
pub const ACCESS3_EXTEND: u32 = 0x0008;
pub const ACCESS3_DELETE: u32 = 0x0010;
pub const ACCESS3_EXECUTE: u32 = 0x0020;

// ── FSINFO Constants ──

pub const FSF3_LINK: u32 = 0x0001;
pub const FSF3_SYMLINK: u32 = 0x0002;
pub const FSF3_HOMOGENEOUS: u32 = 0x0008;
pub const FSF3_CANSETTIME: u32 = 0x0010;

// ── Create Mode ──

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum Createmode3 {
    Unchecked = 0,
    Guarded = 1,
    Exclusive = 2,
}

impl Createmode3 {
    pub fn from_u32(v: u32) -> Self {
        match v {
            1 => Createmode3::Guarded,
            2 => Createmode3::Exclusive,
            _ => Createmode3::Unchecked,
        }
    }
}

// ── Stable Write ──

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum StableHow {
    Unstable = 0,
    DataSync = 1,
    FileSync = 2,
}

// ── Directory Entry Types ──

#[derive(Debug, Clone)]
pub struct Entry3 {
    pub fileid: u64,
    pub name: String,
    pub cookie: u64,
}

#[derive(Debug, Clone)]
pub struct Entryplus3 {
    pub fileid: u64,
    pub name: String,
    pub cookie: u64,
    pub attr: Option<Fattr3>,
    pub fh: Option<NfsFh3>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::xdr::{XdrReader, XdrWriter};

    #[test]
    fn nfs_fh_roundtrip() {
        let fh = NfsFh3::new(42, 0xDEAD);
        let mut w = XdrWriter::new();
        fh.encode(&mut w);
        let data = w.into_bytes();
        let mut r = XdrReader::new(&data);
        let fh2 = NfsFh3::decode(&mut r).unwrap();
        assert_eq!(fh2.ino(), 42);
        assert_eq!(fh2.fsid(), 0xDEAD);
    }

    #[test]
    fn fattr3_encode() {
        let attr = Fattr3 {
            ftype: Ftype3::Reg,
            mode: 0o644,
            nlink: 1,
            uid: 0,
            gid: 0,
            size: 1024,
            used: 1024,
            rdev: Specdata3::default(),
            fsid: 1,
            fileid: 100,
            atime: Nfstime3::from_secs(1000),
            mtime: Nfstime3::from_secs(2000),
            ctime: Nfstime3::from_secs(3000),
        };
        let mut w = XdrWriter::new();
        attr.encode(&mut w);
        // fattr3 is 84 bytes:
        // ftype(4) + mode(4) + nlink(4) + uid(4) + gid(4) + size(8) + used(8) +
        // rdev(8) + fsid(8) + fileid(8) + atime(8) + mtime(8) + ctime(8) = 84
        assert_eq!(w.len(), 84);
    }
}
