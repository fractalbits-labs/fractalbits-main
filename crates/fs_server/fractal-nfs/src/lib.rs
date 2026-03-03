pub mod dispatch;
pub mod mount;
pub mod nfs3_types;
pub mod nfs3_wire;
pub mod rpc;
pub mod server;
pub mod xdr;

pub use nfs3_types::*;
pub use server::{NfsServerConfig, run};

/// Trait for implementing an NFSv3 filesystem.
///
/// Each method receives pre-decoded arguments and an XdrWriter for encoding
/// the response body (status + result data). The RPC accepted header is
/// already written before these methods are called.
///
/// All methods are async and !Send (compio single-threaded model).
/// The trait itself is Send + Sync for Arc sharing across threads.
pub trait Nfs3Filesystem: Send + Sync + 'static {
    fn getattr(&self, fh: &NfsFh3, w: &mut xdr::XdrWriter)
    -> impl std::future::Future<Output = ()>;

    fn setattr(
        &self,
        fh: &NfsFh3,
        attrs: &Sattr3,
        w: &mut xdr::XdrWriter,
    ) -> impl std::future::Future<Output = ()> {
        let _ = (fh, attrs);
        async {
            nfs3_wire::encode_setattr_err(w, Nfsstat3::NotSupp);
        }
    }

    fn lookup(
        &self,
        dir_fh: &NfsFh3,
        name: &str,
        w: &mut xdr::XdrWriter,
    ) -> impl std::future::Future<Output = ()>;

    fn access(
        &self,
        fh: &NfsFh3,
        access: u32,
        uid: u32,
        gid: u32,
        w: &mut xdr::XdrWriter,
    ) -> impl std::future::Future<Output = ()>;

    fn read(
        &self,
        fh: &NfsFh3,
        offset: u64,
        count: u32,
        w: &mut xdr::XdrWriter,
    ) -> impl std::future::Future<Output = ()>;

    fn write(
        &self,
        fh: &NfsFh3,
        offset: u64,
        data: &[u8],
        stable: StableHow,
        w: &mut xdr::XdrWriter,
    ) -> impl std::future::Future<Output = ()> {
        let _ = (fh, offset, data, stable);
        async {
            nfs3_wire::encode_write_err(w, Nfsstat3::Rofs);
        }
    }

    fn create(
        &self,
        dir_fh: &NfsFh3,
        name: &str,
        w: &mut xdr::XdrWriter,
    ) -> impl std::future::Future<Output = ()> {
        let _ = (dir_fh, name);
        async {
            nfs3_wire::encode_create_err(w, Nfsstat3::Rofs);
        }
    }

    fn mkdir(
        &self,
        dir_fh: &NfsFh3,
        name: &str,
        w: &mut xdr::XdrWriter,
    ) -> impl std::future::Future<Output = ()> {
        let _ = (dir_fh, name);
        async {
            nfs3_wire::encode_mkdir_err(w, Nfsstat3::Rofs);
        }
    }

    fn remove(
        &self,
        dir_fh: &NfsFh3,
        name: &str,
        w: &mut xdr::XdrWriter,
    ) -> impl std::future::Future<Output = ()> {
        let _ = (dir_fh, name);
        async {
            nfs3_wire::encode_remove_err(w, Nfsstat3::Rofs);
        }
    }

    fn rmdir(
        &self,
        dir_fh: &NfsFh3,
        name: &str,
        w: &mut xdr::XdrWriter,
    ) -> impl std::future::Future<Output = ()> {
        let _ = (dir_fh, name);
        async {
            nfs3_wire::encode_remove_err(w, Nfsstat3::Rofs);
        }
    }

    fn rename(
        &self,
        from_dir: &NfsFh3,
        from_name: &str,
        to_dir: &NfsFh3,
        to_name: &str,
        w: &mut xdr::XdrWriter,
    ) -> impl std::future::Future<Output = ()> {
        let _ = (from_dir, from_name, to_dir, to_name);
        async {
            nfs3_wire::encode_rename_err(w, Nfsstat3::Rofs);
        }
    }

    fn readdir(
        &self,
        dir_fh: &NfsFh3,
        cookie: u64,
        count: u32,
        w: &mut xdr::XdrWriter,
    ) -> impl std::future::Future<Output = ()>;

    fn readdirplus(
        &self,
        dir_fh: &NfsFh3,
        cookie: u64,
        maxcount: u32,
        w: &mut xdr::XdrWriter,
    ) -> impl std::future::Future<Output = ()>;

    fn fsstat(&self, fh: &NfsFh3, w: &mut xdr::XdrWriter) -> impl std::future::Future<Output = ()>;

    fn fsinfo(&self, fh: &NfsFh3, w: &mut xdr::XdrWriter) -> impl std::future::Future<Output = ()>;

    fn pathconf(
        &self,
        fh: &NfsFh3,
        w: &mut xdr::XdrWriter,
    ) -> impl std::future::Future<Output = ()>;

    fn commit(
        &self,
        fh: &NfsFh3,
        offset: u64,
        count: u32,
        w: &mut xdr::XdrWriter,
    ) -> impl std::future::Future<Output = ()> {
        let _ = (fh, offset, count);
        async {
            nfs3_wire::encode_commit_err(w, Nfsstat3::NotSupp);
        }
    }
}
