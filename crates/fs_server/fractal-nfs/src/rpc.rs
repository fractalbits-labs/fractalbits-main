/// Sun RPC (ONC RPC) framing and headers per RFC 5531.
/// Handles TCP record marking and RPC call/reply message structure.
use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::xdr::{XdrError, XdrReader, XdrWriter};

// ── TCP Record Marking ──

const LAST_FRAGMENT_BIT: u32 = 0x8000_0000;

/// Read a complete RPC message from TCP record-marked stream.
/// Returns the reassembled message payload.
pub fn read_record_mark(buf: &[u8]) -> Option<(u32, bool)> {
    if buf.len() < 4 {
        return None;
    }
    let header = (&buf[..4]).get_u32();
    let last = header & LAST_FRAGMENT_BIT != 0;
    let len = header & !LAST_FRAGMENT_BIT;
    Some((len, last))
}

/// Write a TCP record mark header for a complete (last-fragment) message.
pub fn write_record_mark(buf: &mut BytesMut, payload_len: u32) {
    buf.put_u32(LAST_FRAGMENT_BIT | payload_len);
}

// ── RPC Message Types ──

pub const RPC_VERSION: u32 = 2;
pub const MSG_TYPE_CALL: u32 = 0;
pub const MSG_TYPE_REPLY: u32 = 1;

pub const REPLY_ACCEPTED: u32 = 0;
pub const REPLY_DENIED: u32 = 1;

pub const ACCEPT_SUCCESS: u32 = 0;
pub const ACCEPT_PROG_UNAVAIL: u32 = 1;
pub const ACCEPT_PROG_MISMATCH: u32 = 2;
pub const ACCEPT_PROC_UNAVAIL: u32 = 3;
pub const ACCEPT_GARBAGE_ARGS: u32 = 4;

pub const AUTH_NONE: u32 = 0;
pub const AUTH_SYS: u32 = 1;

// ── RPC Call Header ──

#[derive(Debug, Clone)]
pub struct RpcCallHeader {
    pub xid: u32,
    pub rpc_version: u32,
    pub program: u32,
    pub prog_version: u32,
    pub procedure: u32,
    pub cred_flavor: u32,
    pub cred_uid: u32,
    pub cred_gid: u32,
}

impl RpcCallHeader {
    pub fn decode(r: &mut XdrReader<'_>) -> Result<Self, XdrError> {
        let xid = r.read_u32()?;
        let msg_type = r.read_u32()?;
        if msg_type != MSG_TYPE_CALL {
            return Err(XdrError::Underflow);
        }
        let rpc_version = r.read_u32()?;
        let program = r.read_u32()?;
        let prog_version = r.read_u32()?;
        let procedure = r.read_u32()?;

        // Credentials
        let cred_flavor = r.read_u32()?;
        let (cred_uid, cred_gid) = if cred_flavor == AUTH_SYS {
            let cred_body = r.read_opaque()?;
            let mut cr = XdrReader::new(cred_body);
            let _stamp = cr.read_u32()?;
            let _machine = cr.read_opaque()?;
            let uid = cr.read_u32()?;
            let gid = cr.read_u32()?;
            // Skip auxiliary gids
            let _ngids = cr.read_u32()?;
            (uid, gid)
        } else {
            let _cred_body = r.read_opaque()?;
            (0, 0)
        };

        // Verifier (skip)
        let _verf_flavor = r.read_u32()?;
        let _verf_body = r.read_opaque()?;

        Ok(Self {
            xid,
            rpc_version,
            program,
            prog_version,
            procedure,
            cred_flavor,
            cred_uid,
            cred_gid,
        })
    }
}

// ── RPC Reply Encoding ──

/// Write a successful RPC reply header.
pub fn write_reply_accepted(w: &mut XdrWriter, xid: u32) {
    w.write_u32(xid);
    w.write_u32(MSG_TYPE_REPLY);
    w.write_u32(REPLY_ACCEPTED);
    // NULL verifier
    w.write_u32(AUTH_NONE);
    w.write_u32(0); // verf body length
    w.write_u32(ACCEPT_SUCCESS);
}

/// Write an RPC "program unavailable" reply.
pub fn write_reply_prog_unavail(w: &mut XdrWriter, xid: u32) {
    w.write_u32(xid);
    w.write_u32(MSG_TYPE_REPLY);
    w.write_u32(REPLY_ACCEPTED);
    w.write_u32(AUTH_NONE);
    w.write_u32(0);
    w.write_u32(ACCEPT_PROG_UNAVAIL);
}

/// Write an RPC "procedure unavailable" reply.
pub fn write_reply_proc_unavail(w: &mut XdrWriter, xid: u32) {
    w.write_u32(xid);
    w.write_u32(MSG_TYPE_REPLY);
    w.write_u32(REPLY_ACCEPTED);
    w.write_u32(AUTH_NONE);
    w.write_u32(0);
    w.write_u32(ACCEPT_PROC_UNAVAIL);
}

/// Write an RPC "garbage arguments" reply.
pub fn write_reply_garbage_args(w: &mut XdrWriter, xid: u32) {
    w.write_u32(xid);
    w.write_u32(MSG_TYPE_REPLY);
    w.write_u32(REPLY_ACCEPTED);
    w.write_u32(AUTH_NONE);
    w.write_u32(0);
    w.write_u32(ACCEPT_GARBAGE_ARGS);
}

/// Assemble a complete TCP record-marked reply from an XdrWriter.
pub fn frame_reply(reply_body: &XdrWriter) -> Bytes {
    let payload = reply_body.as_bytes();
    let mut buf = BytesMut::with_capacity(4 + payload.len());
    write_record_mark(&mut buf, payload.len() as u32);
    buf.extend_from_slice(payload);
    buf.freeze()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_mark_roundtrip() {
        let mut buf = BytesMut::new();
        write_record_mark(&mut buf, 100);
        let (len, last) = read_record_mark(&buf).unwrap();
        assert_eq!(len, 100);
        assert!(last);
    }

    #[test]
    fn reply_accepted_structure() {
        let mut w = XdrWriter::new();
        write_reply_accepted(&mut w, 0x42);
        let data = w.into_bytes();
        let mut r = XdrReader::new(&data);
        assert_eq!(r.read_u32().unwrap(), 0x42); // xid
        assert_eq!(r.read_u32().unwrap(), MSG_TYPE_REPLY);
        assert_eq!(r.read_u32().unwrap(), REPLY_ACCEPTED);
        assert_eq!(r.read_u32().unwrap(), AUTH_NONE); // verf flavor
        assert_eq!(r.read_u32().unwrap(), 0); // verf body
        assert_eq!(r.read_u32().unwrap(), ACCEPT_SUCCESS);
    }
}
