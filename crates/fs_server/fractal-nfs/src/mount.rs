/// MOUNT protocol (program 100005, version 3) per RFC 1813 appendix I.
/// Only MNT, UMNT, EXPORT, and NULL are needed for basic NFSv3 mounting.
use crate::nfs3_types::NfsFh3;
use crate::rpc;
use crate::xdr::{XdrReader, XdrWriter};

pub const MOUNT_PROGRAM: u32 = 100005;
pub const MOUNT_VERSION: u32 = 3;

pub const MOUNTPROC3_NULL: u32 = 0;
pub const MOUNTPROC3_MNT: u32 = 1;
pub const MOUNTPROC3_UMNT: u32 = 3;
pub const MOUNTPROC3_EXPORT: u32 = 5;

pub const MNT3_OK: u32 = 0;

/// Handle a MOUNT protocol call. Returns the reply body.
pub fn handle_mount_call(xid: u32, procedure: u32, args: &[u8], root_fh: &NfsFh3) -> XdrWriter {
    match procedure {
        MOUNTPROC3_NULL => handle_null(xid),
        MOUNTPROC3_MNT => handle_mnt(xid, args, root_fh),
        MOUNTPROC3_UMNT => handle_umnt(xid),
        MOUNTPROC3_EXPORT => handle_export(xid),
        _ => {
            let mut w = XdrWriter::new();
            rpc::write_reply_proc_unavail(&mut w, xid);
            w
        }
    }
}

fn handle_null(xid: u32) -> XdrWriter {
    let mut w = XdrWriter::new();
    rpc::write_reply_accepted(&mut w, xid);
    w
}

fn handle_mnt(xid: u32, args: &[u8], root_fh: &NfsFh3) -> XdrWriter {
    let mut w = XdrWriter::new();
    rpc::write_reply_accepted(&mut w, xid);

    // Decode the export path (we accept any path)
    let mut r = XdrReader::new(args);
    let _path = r.read_string().unwrap_or("/");
    tracing::debug!(path = _path, "MOUNT MNT request");

    // mountres3: status + fhandle + auth_flavors
    w.write_u32(MNT3_OK);
    root_fh.encode(&mut w);
    // auth flavors: just AUTH_NONE
    w.write_u32(1); // count
    w.write_u32(rpc::AUTH_NONE);

    w
}

fn handle_umnt(xid: u32) -> XdrWriter {
    let mut w = XdrWriter::new();
    rpc::write_reply_accepted(&mut w, xid);
    // UMNT has no return data beyond the accepted header
    w
}

fn handle_export(xid: u32) -> XdrWriter {
    let mut w = XdrWriter::new();
    rpc::write_reply_accepted(&mut w, xid);
    // Export list: single entry "/" with no groups, then end of list
    w.write_bool(true); // value follows
    w.write_string("/");
    w.write_bool(false); // no groups
    w.write_bool(false); // end of export list
    w
}
