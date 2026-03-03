/// NFSv3 per-procedure argument decoding and result encoding per RFC 1813.
use bytes::Bytes;

use crate::nfs3_types::*;
use crate::xdr::{XdrError, XdrReader, XdrWriter};

// ── Procedure Numbers ──

pub const NFSPROC3_NULL: u32 = 0;
pub const NFSPROC3_GETATTR: u32 = 1;
pub const NFSPROC3_SETATTR: u32 = 2;
pub const NFSPROC3_LOOKUP: u32 = 3;
pub const NFSPROC3_ACCESS: u32 = 4;
pub const NFSPROC3_READ: u32 = 6;
pub const NFSPROC3_WRITE: u32 = 7;
pub const NFSPROC3_CREATE: u32 = 8;
pub const NFSPROC3_MKDIR: u32 = 9;
pub const NFSPROC3_REMOVE: u32 = 12;
pub const NFSPROC3_RMDIR: u32 = 13;
pub const NFSPROC3_RENAME: u32 = 14;
pub const NFSPROC3_READDIR: u32 = 16;
pub const NFSPROC3_READDIRPLUS: u32 = 17;
pub const NFSPROC3_FSSTAT: u32 = 18;
pub const NFSPROC3_FSINFO: u32 = 19;
pub const NFSPROC3_PATHCONF: u32 = 20;
pub const NFSPROC3_COMMIT: u32 = 21;

// ── Argument Structures ──

pub struct GetattrArgs {
    pub fh: NfsFh3,
}

impl GetattrArgs {
    pub fn decode(r: &mut XdrReader<'_>) -> Result<Self, XdrError> {
        Ok(Self {
            fh: NfsFh3::decode(r)?,
        })
    }
}

pub struct SetattrArgs {
    pub fh: NfsFh3,
    pub new_attrs: Sattr3,
    // guard is optional; skip for now
}

impl SetattrArgs {
    pub fn decode(r: &mut XdrReader<'_>) -> Result<Self, XdrError> {
        let fh = NfsFh3::decode(r)?;
        let new_attrs = Sattr3::decode(r)?;
        // sattrguard3: bool + optional pre_op_attr
        let has_guard = r.read_bool()?;
        if has_guard {
            let _ctime = Nfstime3::decode(r)?;
        }
        Ok(Self { fh, new_attrs })
    }
}

pub struct LookupArgs {
    pub dir_fh: NfsFh3,
    pub name: String,
}

impl LookupArgs {
    pub fn decode(r: &mut XdrReader<'_>) -> Result<Self, XdrError> {
        let dir_fh = NfsFh3::decode(r)?;
        let name = r.read_string()?.to_string();
        Ok(Self { dir_fh, name })
    }
}

pub struct AccessArgs {
    pub fh: NfsFh3,
    pub access: u32,
}

impl AccessArgs {
    pub fn decode(r: &mut XdrReader<'_>) -> Result<Self, XdrError> {
        let fh = NfsFh3::decode(r)?;
        let access = r.read_u32()?;
        Ok(Self { fh, access })
    }
}

pub struct ReadArgs {
    pub fh: NfsFh3,
    pub offset: u64,
    pub count: u32,
}

impl ReadArgs {
    pub fn decode(r: &mut XdrReader<'_>) -> Result<Self, XdrError> {
        let fh = NfsFh3::decode(r)?;
        let offset = r.read_u64()?;
        let count = r.read_u32()?;
        Ok(Self { fh, offset, count })
    }
}

pub struct WriteArgs {
    pub fh: NfsFh3,
    pub offset: u64,
    pub count: u32,
    pub stable: StableHow,
    pub data: Bytes,
}

impl WriteArgs {
    pub fn decode(r: &mut XdrReader<'_>) -> Result<Self, XdrError> {
        let fh = NfsFh3::decode(r)?;
        let offset = r.read_u64()?;
        let count = r.read_u32()?;
        let stable_val = r.read_u32()?;
        let stable = match stable_val {
            1 => StableHow::DataSync,
            2 => StableHow::FileSync,
            _ => StableHow::Unstable,
        };
        let data = r.read_opaque_bytes()?;
        Ok(Self {
            fh,
            offset,
            count,
            stable,
            data,
        })
    }
}

pub struct CreateArgs {
    pub dir_fh: NfsFh3,
    pub name: String,
    pub mode: Createmode3,
    pub attrs: Option<Sattr3>,
}

impl CreateArgs {
    pub fn decode(r: &mut XdrReader<'_>) -> Result<Self, XdrError> {
        let dir_fh = NfsFh3::decode(r)?;
        let name = r.read_string()?.to_string();
        let mode = Createmode3::from_u32(r.read_u32()?);
        let attrs = if mode != Createmode3::Exclusive {
            Some(Sattr3::decode(r)?)
        } else {
            // exclusive create: skip 8-byte createverf
            r.skip(8)?;
            None
        };
        Ok(Self {
            dir_fh,
            name,
            mode,
            attrs,
        })
    }
}

pub struct MkdirArgs {
    pub dir_fh: NfsFh3,
    pub name: String,
    pub attrs: Sattr3,
}

impl MkdirArgs {
    pub fn decode(r: &mut XdrReader<'_>) -> Result<Self, XdrError> {
        let dir_fh = NfsFh3::decode(r)?;
        let name = r.read_string()?.to_string();
        let attrs = Sattr3::decode(r)?;
        Ok(Self {
            dir_fh,
            name,
            attrs,
        })
    }
}

pub struct RemoveArgs {
    pub dir_fh: NfsFh3,
    pub name: String,
}

impl RemoveArgs {
    pub fn decode(r: &mut XdrReader<'_>) -> Result<Self, XdrError> {
        let dir_fh = NfsFh3::decode(r)?;
        let name = r.read_string()?.to_string();
        Ok(Self { dir_fh, name })
    }
}

pub struct RenameArgs {
    pub from_dir: NfsFh3,
    pub from_name: String,
    pub to_dir: NfsFh3,
    pub to_name: String,
}

impl RenameArgs {
    pub fn decode(r: &mut XdrReader<'_>) -> Result<Self, XdrError> {
        let from_dir = NfsFh3::decode(r)?;
        let from_name = r.read_string()?.to_string();
        let to_dir = NfsFh3::decode(r)?;
        let to_name = r.read_string()?.to_string();
        Ok(Self {
            from_dir,
            from_name,
            to_dir,
            to_name,
        })
    }
}

pub struct ReaddirArgs {
    pub dir_fh: NfsFh3,
    pub cookie: u64,
    pub cookieverf: [u8; 8],
    pub count: u32,
}

impl ReaddirArgs {
    pub fn decode(r: &mut XdrReader<'_>) -> Result<Self, XdrError> {
        let dir_fh = NfsFh3::decode(r)?;
        let cookie = r.read_u64()?;
        let verf_data = r.read_opaque_fixed(8)?;
        let mut cookieverf = [0u8; 8];
        cookieverf.copy_from_slice(verf_data);
        let count = r.read_u32()?;
        Ok(Self {
            dir_fh,
            cookie,
            cookieverf,
            count,
        })
    }
}

pub struct ReaddirplusArgs {
    pub dir_fh: NfsFh3,
    pub cookie: u64,
    pub cookieverf: [u8; 8],
    pub dircount: u32,
    pub maxcount: u32,
}

impl ReaddirplusArgs {
    pub fn decode(r: &mut XdrReader<'_>) -> Result<Self, XdrError> {
        let dir_fh = NfsFh3::decode(r)?;
        let cookie = r.read_u64()?;
        let verf_data = r.read_opaque_fixed(8)?;
        let mut cookieverf = [0u8; 8];
        cookieverf.copy_from_slice(verf_data);
        let dircount = r.read_u32()?;
        let maxcount = r.read_u32()?;
        Ok(Self {
            dir_fh,
            cookie,
            cookieverf,
            dircount,
            maxcount,
        })
    }
}

pub struct CommitArgs {
    pub fh: NfsFh3,
    pub offset: u64,
    pub count: u32,
}

impl CommitArgs {
    pub fn decode(r: &mut XdrReader<'_>) -> Result<Self, XdrError> {
        let fh = NfsFh3::decode(r)?;
        let offset = r.read_u64()?;
        let count = r.read_u32()?;
        Ok(Self { fh, offset, count })
    }
}

// ── Result Encoders ──

pub fn encode_getattr_ok(w: &mut XdrWriter, attr: &Fattr3) {
    Nfsstat3::Ok.encode(w);
    attr.encode(w);
}

pub fn encode_getattr_err(w: &mut XdrWriter, status: Nfsstat3) {
    status.encode(w);
}

pub fn encode_setattr_ok(w: &mut XdrWriter, attr: &Fattr3) {
    Nfsstat3::Ok.encode(w);
    encode_wcc_data(w, None, Some(attr));
}

pub fn encode_setattr_err(w: &mut XdrWriter, status: Nfsstat3) {
    status.encode(w);
    encode_wcc_data(w, None, None);
}

pub fn encode_lookup_ok(w: &mut XdrWriter, fh: &NfsFh3, attr: &Fattr3, dir_attr: Option<&Fattr3>) {
    Nfsstat3::Ok.encode(w);
    fh.encode(w);
    encode_post_op_attr(w, Some(attr));
    encode_post_op_attr(w, dir_attr);
}

pub fn encode_lookup_err(w: &mut XdrWriter, status: Nfsstat3, dir_attr: Option<&Fattr3>) {
    status.encode(w);
    encode_post_op_attr(w, dir_attr);
}

pub fn encode_access_ok(w: &mut XdrWriter, attr: &Fattr3, access: u32) {
    Nfsstat3::Ok.encode(w);
    encode_post_op_attr(w, Some(attr));
    w.write_u32(access);
}

pub fn encode_access_err(w: &mut XdrWriter, status: Nfsstat3) {
    status.encode(w);
    encode_post_op_attr(w, None);
}

pub fn encode_read_ok(w: &mut XdrWriter, attr: &Fattr3, data: &[u8], eof: bool) {
    Nfsstat3::Ok.encode(w);
    encode_post_op_attr(w, Some(attr));
    w.write_u32(data.len() as u32);
    w.write_bool(eof);
    w.write_opaque(data);
}

pub fn encode_read_err(w: &mut XdrWriter, status: Nfsstat3) {
    status.encode(w);
    encode_post_op_attr(w, None);
}

pub fn encode_write_ok(
    w: &mut XdrWriter,
    attr: &Fattr3,
    count: u32,
    committed: StableHow,
    verf: &[u8; 8],
) {
    Nfsstat3::Ok.encode(w);
    encode_wcc_data(w, None, Some(attr));
    w.write_u32(count);
    w.write_u32(committed as u32);
    w.write_opaque_fixed(verf);
}

pub fn encode_write_err(w: &mut XdrWriter, status: Nfsstat3) {
    status.encode(w);
    encode_wcc_data(w, None, None);
}

pub fn encode_create_ok(w: &mut XdrWriter, fh: &NfsFh3, attr: &Fattr3, dir_attr: Option<&Fattr3>) {
    Nfsstat3::Ok.encode(w);
    encode_post_op_fh(w, Some(fh));
    encode_post_op_attr(w, Some(attr));
    encode_wcc_data(w, None, dir_attr);
}

pub fn encode_create_err(w: &mut XdrWriter, status: Nfsstat3) {
    status.encode(w);
    encode_post_op_fh(w, None);
    encode_post_op_attr(w, None);
    encode_wcc_data(w, None, None);
}

pub fn encode_mkdir_ok(w: &mut XdrWriter, fh: &NfsFh3, attr: &Fattr3, dir_attr: Option<&Fattr3>) {
    Nfsstat3::Ok.encode(w);
    encode_post_op_fh(w, Some(fh));
    encode_post_op_attr(w, Some(attr));
    encode_wcc_data(w, None, dir_attr);
}

pub fn encode_mkdir_err(w: &mut XdrWriter, status: Nfsstat3) {
    status.encode(w);
    encode_post_op_fh(w, None);
    encode_post_op_attr(w, None);
    encode_wcc_data(w, None, None);
}

pub fn encode_remove_ok(w: &mut XdrWriter, dir_attr: Option<&Fattr3>) {
    Nfsstat3::Ok.encode(w);
    encode_wcc_data(w, None, dir_attr);
}

pub fn encode_remove_err(w: &mut XdrWriter, status: Nfsstat3) {
    status.encode(w);
    encode_wcc_data(w, None, None);
}

pub fn encode_rename_ok(
    w: &mut XdrWriter,
    from_dir_attr: Option<&Fattr3>,
    to_dir_attr: Option<&Fattr3>,
) {
    Nfsstat3::Ok.encode(w);
    encode_wcc_data(w, None, from_dir_attr);
    encode_wcc_data(w, None, to_dir_attr);
}

pub fn encode_rename_err(w: &mut XdrWriter, status: Nfsstat3) {
    status.encode(w);
    encode_wcc_data(w, None, None);
    encode_wcc_data(w, None, None);
}

pub fn encode_readdir_ok(
    w: &mut XdrWriter,
    dir_attr: Option<&Fattr3>,
    cookieverf: &[u8; 8],
    entries: &[Entry3],
    eof: bool,
) {
    Nfsstat3::Ok.encode(w);
    encode_post_op_attr(w, dir_attr);
    w.write_opaque_fixed(cookieverf);
    for entry in entries {
        w.write_bool(true); // value follows
        w.write_u64(entry.fileid);
        w.write_string(&entry.name);
        w.write_u64(entry.cookie);
    }
    w.write_bool(false); // no more entries
    w.write_bool(eof);
}

pub fn encode_readdir_err(w: &mut XdrWriter, status: Nfsstat3) {
    status.encode(w);
    encode_post_op_attr(w, None);
}

pub fn encode_readdirplus_ok(
    w: &mut XdrWriter,
    dir_attr: Option<&Fattr3>,
    cookieverf: &[u8; 8],
    entries: &[Entryplus3],
    eof: bool,
) {
    Nfsstat3::Ok.encode(w);
    encode_post_op_attr(w, dir_attr);
    w.write_opaque_fixed(cookieverf);
    for entry in entries {
        w.write_bool(true); // value follows
        w.write_u64(entry.fileid);
        w.write_string(&entry.name);
        w.write_u64(entry.cookie);
        encode_post_op_attr(w, entry.attr.as_ref());
        encode_post_op_fh(w, entry.fh.as_ref());
    }
    w.write_bool(false); // no more entries
    w.write_bool(eof);
}

pub fn encode_readdirplus_err(w: &mut XdrWriter, status: Nfsstat3) {
    status.encode(w);
    encode_post_op_attr(w, None);
}

#[allow(clippy::too_many_arguments)]
pub fn encode_fsstat_ok(
    w: &mut XdrWriter,
    attr: &Fattr3,
    tbytes: u64,
    fbytes: u64,
    abytes: u64,
    tfiles: u64,
    ffiles: u64,
    afiles: u64,
    invarsec: u32,
) {
    Nfsstat3::Ok.encode(w);
    encode_post_op_attr(w, Some(attr));
    w.write_u64(tbytes);
    w.write_u64(fbytes);
    w.write_u64(abytes);
    w.write_u64(tfiles);
    w.write_u64(ffiles);
    w.write_u64(afiles);
    w.write_u32(invarsec);
}

pub fn encode_fsstat_err(w: &mut XdrWriter, status: Nfsstat3) {
    status.encode(w);
    encode_post_op_attr(w, None);
}

#[allow(clippy::too_many_arguments)]
pub fn encode_fsinfo_ok(
    w: &mut XdrWriter,
    attr: &Fattr3,
    rtmax: u32,
    rtpref: u32,
    rtmult: u32,
    wtmax: u32,
    wtpref: u32,
    wtmult: u32,
    dtpref: u32,
    maxfilesize: u64,
    properties: u32,
) {
    Nfsstat3::Ok.encode(w);
    encode_post_op_attr(w, Some(attr));
    w.write_u32(rtmax);
    w.write_u32(rtpref);
    w.write_u32(rtmult);
    w.write_u32(wtmax);
    w.write_u32(wtpref);
    w.write_u32(wtmult);
    w.write_u32(dtpref);
    w.write_u64(maxfilesize);
    // time_delta: nfstime3 = {1, 0} meaning 1-second resolution
    w.write_u32(1);
    w.write_u32(0);
    w.write_u32(properties);
}

pub fn encode_fsinfo_err(w: &mut XdrWriter, status: Nfsstat3) {
    status.encode(w);
    encode_post_op_attr(w, None);
}

pub fn encode_pathconf_ok(w: &mut XdrWriter, attr: &Fattr3, linkmax: u32, name_max: u32) {
    Nfsstat3::Ok.encode(w);
    encode_post_op_attr(w, Some(attr));
    w.write_u32(linkmax);
    w.write_u32(name_max);
    w.write_bool(true); // no_trunc
    w.write_bool(false); // chown_restricted
    w.write_bool(true); // case_insensitive: false (case-sensitive)
    w.write_bool(true); // case_preserving
}

pub fn encode_pathconf_err(w: &mut XdrWriter, status: Nfsstat3) {
    status.encode(w);
    encode_post_op_attr(w, None);
}

pub fn encode_commit_ok(w: &mut XdrWriter, attr: &Fattr3, verf: &[u8; 8]) {
    Nfsstat3::Ok.encode(w);
    encode_wcc_data(w, None, Some(attr));
    w.write_opaque_fixed(verf);
}

pub fn encode_commit_err(w: &mut XdrWriter, status: Nfsstat3) {
    status.encode(w);
    encode_wcc_data(w, None, None);
}
