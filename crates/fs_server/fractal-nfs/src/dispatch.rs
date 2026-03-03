/// RPC dispatch: route incoming RPC calls to Nfs3Filesystem trait methods.
use std::sync::Arc;

use crate::Nfs3Filesystem;
use crate::mount;
use crate::nfs3_types::*;
use crate::nfs3_wire::*;
use crate::rpc::{self, RpcCallHeader};
use crate::xdr::{XdrReader, XdrWriter};

pub const NFS_PROGRAM: u32 = 100003;
pub const NFS_VERSION: u32 = 3;

/// Dispatch a single RPC call. Returns the reply as an XdrWriter.
pub async fn dispatch_rpc<F: Nfs3Filesystem>(
    fs: &Arc<F>,
    header: &RpcCallHeader,
    args_buf: &[u8],
    root_fh: &NfsFh3,
) -> XdrWriter {
    // Check RPC version
    if header.rpc_version != rpc::RPC_VERSION {
        let mut w = XdrWriter::new();
        rpc::write_reply_prog_unavail(&mut w, header.xid);
        return w;
    }

    // Route by program
    match header.program {
        mount::MOUNT_PROGRAM => {
            mount::handle_mount_call(header.xid, header.procedure, args_buf, root_fh)
        }
        NFS_PROGRAM => {
            if header.prog_version != NFS_VERSION {
                let mut w = XdrWriter::new();
                rpc::write_reply_prog_unavail(&mut w, header.xid);
                return w;
            }
            dispatch_nfs3(fs, header, args_buf).await
        }
        _ => {
            let mut w = XdrWriter::new();
            rpc::write_reply_prog_unavail(&mut w, header.xid);
            w
        }
    }
}

async fn dispatch_nfs3<F: Nfs3Filesystem>(
    fs: &Arc<F>,
    header: &RpcCallHeader,
    args_buf: &[u8],
) -> XdrWriter {
    let mut r = XdrReader::new(args_buf);
    let xid = header.xid;
    let uid = header.cred_uid;
    let gid = header.cred_gid;

    match header.procedure {
        NFSPROC3_NULL => {
            let mut w = XdrWriter::new();
            rpc::write_reply_accepted(&mut w, xid);
            w
        }

        NFSPROC3_GETATTR => {
            let args = match GetattrArgs::decode(&mut r) {
                Ok(a) => a,
                Err(_) => return garbage_args(xid),
            };
            let mut w = XdrWriter::new();
            rpc::write_reply_accepted(&mut w, xid);
            fs.getattr(&args.fh, &mut w).await;
            w
        }

        NFSPROC3_SETATTR => {
            let args = match SetattrArgs::decode(&mut r) {
                Ok(a) => a,
                Err(_) => return garbage_args(xid),
            };
            let mut w = XdrWriter::new();
            rpc::write_reply_accepted(&mut w, xid);
            fs.setattr(&args.fh, &args.new_attrs, &mut w).await;
            w
        }

        NFSPROC3_LOOKUP => {
            let args = match LookupArgs::decode(&mut r) {
                Ok(a) => a,
                Err(_) => return garbage_args(xid),
            };
            let mut w = XdrWriter::new();
            rpc::write_reply_accepted(&mut w, xid);
            fs.lookup(&args.dir_fh, &args.name, &mut w).await;
            w
        }

        NFSPROC3_ACCESS => {
            let args = match AccessArgs::decode(&mut r) {
                Ok(a) => a,
                Err(_) => return garbage_args(xid),
            };
            let mut w = XdrWriter::new();
            rpc::write_reply_accepted(&mut w, xid);
            fs.access(&args.fh, args.access, uid, gid, &mut w).await;
            w
        }

        NFSPROC3_READ => {
            let args = match ReadArgs::decode(&mut r) {
                Ok(a) => a,
                Err(_) => return garbage_args(xid),
            };
            let mut w = XdrWriter::with_capacity(args.count as usize + 256);
            rpc::write_reply_accepted(&mut w, xid);
            fs.read(&args.fh, args.offset, args.count, &mut w).await;
            w
        }

        NFSPROC3_WRITE => {
            let args = match WriteArgs::decode(&mut r) {
                Ok(a) => a,
                Err(_) => return garbage_args(xid),
            };
            let mut w = XdrWriter::new();
            rpc::write_reply_accepted(&mut w, xid);
            fs.write(&args.fh, args.offset, &args.data, args.stable, &mut w)
                .await;
            w
        }

        NFSPROC3_CREATE => {
            let args = match CreateArgs::decode(&mut r) {
                Ok(a) => a,
                Err(_) => return garbage_args(xid),
            };
            let mut w = XdrWriter::new();
            rpc::write_reply_accepted(&mut w, xid);
            fs.create(&args.dir_fh, &args.name, &mut w).await;
            w
        }

        NFSPROC3_MKDIR => {
            let args = match MkdirArgs::decode(&mut r) {
                Ok(a) => a,
                Err(_) => return garbage_args(xid),
            };
            let mut w = XdrWriter::new();
            rpc::write_reply_accepted(&mut w, xid);
            fs.mkdir(&args.dir_fh, &args.name, &mut w).await;
            w
        }

        NFSPROC3_REMOVE => {
            let args = match RemoveArgs::decode(&mut r) {
                Ok(a) => a,
                Err(_) => return garbage_args(xid),
            };
            let mut w = XdrWriter::new();
            rpc::write_reply_accepted(&mut w, xid);
            fs.remove(&args.dir_fh, &args.name, &mut w).await;
            w
        }

        NFSPROC3_RMDIR => {
            let args = match RemoveArgs::decode(&mut r) {
                Ok(a) => a,
                Err(_) => return garbage_args(xid),
            };
            let mut w = XdrWriter::new();
            rpc::write_reply_accepted(&mut w, xid);
            fs.rmdir(&args.dir_fh, &args.name, &mut w).await;
            w
        }

        NFSPROC3_RENAME => {
            let args = match RenameArgs::decode(&mut r) {
                Ok(a) => a,
                Err(_) => return garbage_args(xid),
            };
            let mut w = XdrWriter::new();
            rpc::write_reply_accepted(&mut w, xid);
            fs.rename(
                &args.from_dir,
                &args.from_name,
                &args.to_dir,
                &args.to_name,
                &mut w,
            )
            .await;
            w
        }

        NFSPROC3_READDIR => {
            let args = match ReaddirArgs::decode(&mut r) {
                Ok(a) => a,
                Err(_) => return garbage_args(xid),
            };
            let mut w = XdrWriter::new();
            rpc::write_reply_accepted(&mut w, xid);
            fs.readdir(&args.dir_fh, args.cookie, args.count, &mut w)
                .await;
            w
        }

        NFSPROC3_READDIRPLUS => {
            let args = match ReaddirplusArgs::decode(&mut r) {
                Ok(a) => a,
                Err(_) => return garbage_args(xid),
            };
            let mut w = XdrWriter::new();
            rpc::write_reply_accepted(&mut w, xid);
            fs.readdirplus(&args.dir_fh, args.cookie, args.maxcount, &mut w)
                .await;
            w
        }

        NFSPROC3_FSSTAT => {
            let args = match GetattrArgs::decode(&mut r) {
                Ok(a) => a,
                Err(_) => return garbage_args(xid),
            };
            let mut w = XdrWriter::new();
            rpc::write_reply_accepted(&mut w, xid);
            fs.fsstat(&args.fh, &mut w).await;
            w
        }

        NFSPROC3_FSINFO => {
            let args = match GetattrArgs::decode(&mut r) {
                Ok(a) => a,
                Err(_) => return garbage_args(xid),
            };
            let mut w = XdrWriter::new();
            rpc::write_reply_accepted(&mut w, xid);
            fs.fsinfo(&args.fh, &mut w).await;
            w
        }

        NFSPROC3_PATHCONF => {
            let args = match GetattrArgs::decode(&mut r) {
                Ok(a) => a,
                Err(_) => return garbage_args(xid),
            };
            let mut w = XdrWriter::new();
            rpc::write_reply_accepted(&mut w, xid);
            fs.pathconf(&args.fh, &mut w).await;
            w
        }

        NFSPROC3_COMMIT => {
            let args = match CommitArgs::decode(&mut r) {
                Ok(a) => a,
                Err(_) => return garbage_args(xid),
            };
            let mut w = XdrWriter::new();
            rpc::write_reply_accepted(&mut w, xid);
            fs.commit(&args.fh, args.offset, args.count, &mut w).await;
            w
        }

        _ => {
            let mut w = XdrWriter::new();
            rpc::write_reply_proc_unavail(&mut w, xid);
            w
        }
    }
}

fn garbage_args(xid: u32) -> XdrWriter {
    let mut w = XdrWriter::new();
    rpc::write_reply_garbage_args(&mut w, xid);
    w
}
