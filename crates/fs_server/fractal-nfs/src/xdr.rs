/// XDR (External Data Representation) encoding/decoding.
/// All values are big-endian, 4-byte aligned per RFC 4506.
use bytes::{Buf, BufMut, Bytes, BytesMut};

pub struct XdrReader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> XdrReader<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    pub fn remaining(&self) -> usize {
        self.buf.len().saturating_sub(self.pos)
    }

    pub fn position(&self) -> usize {
        self.pos
    }

    fn ensure(&self, n: usize) -> Result<(), XdrError> {
        if self.remaining() < n {
            Err(XdrError::Underflow)
        } else {
            Ok(())
        }
    }

    pub fn read_u32(&mut self) -> Result<u32, XdrError> {
        self.ensure(4)?;
        let val = (&self.buf[self.pos..]).get_u32();
        self.pos += 4;
        Ok(val)
    }

    pub fn read_i32(&mut self) -> Result<i32, XdrError> {
        Ok(self.read_u32()? as i32)
    }

    pub fn read_u64(&mut self) -> Result<u64, XdrError> {
        self.ensure(8)?;
        let val = (&self.buf[self.pos..]).get_u64();
        self.pos += 8;
        Ok(val)
    }

    pub fn read_bool(&mut self) -> Result<bool, XdrError> {
        Ok(self.read_u32()? != 0)
    }

    /// Read opaque fixed-length data (padded to 4-byte boundary).
    pub fn read_opaque_fixed(&mut self, len: usize) -> Result<&'a [u8], XdrError> {
        let padded = pad4(len);
        self.ensure(padded)?;
        let data = &self.buf[self.pos..self.pos + len];
        self.pos += padded;
        Ok(data)
    }

    /// Read variable-length opaque data (length-prefixed).
    pub fn read_opaque(&mut self) -> Result<&'a [u8], XdrError> {
        let len = self.read_u32()? as usize;
        self.read_opaque_fixed(len)
    }

    /// Read a variable-length opaque into owned Bytes.
    pub fn read_opaque_bytes(&mut self) -> Result<Bytes, XdrError> {
        Ok(Bytes::copy_from_slice(self.read_opaque()?))
    }

    /// Read a string (same wire format as opaque).
    pub fn read_string(&mut self) -> Result<&'a str, XdrError> {
        let data = self.read_opaque()?;
        std::str::from_utf8(data).map_err(|_| XdrError::InvalidUtf8)
    }

    /// Skip n bytes (respecting alignment).
    pub fn skip(&mut self, n: usize) -> Result<(), XdrError> {
        let padded = pad4(n);
        self.ensure(padded)?;
        self.pos += padded;
        Ok(())
    }

    /// Read remaining bytes as a slice (no alignment padding).
    pub fn read_remaining(&mut self) -> &'a [u8] {
        let data = &self.buf[self.pos..];
        self.pos = self.buf.len();
        data
    }
}

pub struct XdrWriter {
    buf: BytesMut,
}

impl XdrWriter {
    pub fn new() -> Self {
        Self {
            buf: BytesMut::with_capacity(256),
        }
    }

    pub fn with_capacity(cap: usize) -> Self {
        Self {
            buf: BytesMut::with_capacity(cap),
        }
    }

    pub fn len(&self) -> usize {
        self.buf.len()
    }

    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    pub fn write_u32(&mut self, val: u32) {
        self.buf.put_u32(val);
    }

    pub fn write_i32(&mut self, val: i32) {
        self.buf.put_i32(val);
    }

    pub fn write_u64(&mut self, val: u64) {
        self.buf.put_u64(val);
    }

    pub fn write_bool(&mut self, val: bool) {
        self.write_u32(if val { 1 } else { 0 });
    }

    /// Write fixed-length opaque data (padded to 4-byte boundary).
    pub fn write_opaque_fixed(&mut self, data: &[u8]) {
        self.buf.extend_from_slice(data);
        let pad = pad4(data.len()) - data.len();
        if pad > 0 {
            self.buf.extend_from_slice(&[0u8; 3][..pad]);
        }
    }

    /// Write variable-length opaque data (length-prefixed).
    pub fn write_opaque(&mut self, data: &[u8]) {
        self.write_u32(data.len() as u32);
        self.write_opaque_fixed(data);
    }

    /// Write a string (same wire format as opaque).
    pub fn write_string(&mut self, s: &str) {
        self.write_opaque(s.as_bytes());
    }

    /// Write raw bytes (no length prefix, no padding). Use for pre-encoded data.
    pub fn write_raw(&mut self, data: &[u8]) {
        self.buf.extend_from_slice(data);
    }

    pub fn into_bytes(self) -> Bytes {
        self.buf.freeze()
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.buf
    }
}

impl Default for XdrWriter {
    fn default() -> Self {
        Self::new()
    }
}

/// Round up to next 4-byte boundary.
#[inline]
pub fn pad4(n: usize) -> usize {
    (n + 3) & !3
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum XdrError {
    Underflow,
    InvalidUtf8,
}

impl std::fmt::Display for XdrError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            XdrError::Underflow => write!(f, "XDR buffer underflow"),
            XdrError::InvalidUtf8 => write!(f, "XDR invalid UTF-8 string"),
        }
    }
}

impl std::error::Error for XdrError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_u32() {
        let mut w = XdrWriter::new();
        w.write_u32(0x12345678);
        w.write_u32(0);
        w.write_u32(u32::MAX);
        let data = w.into_bytes();
        let mut r = XdrReader::new(&data);
        assert_eq!(r.read_u32().unwrap(), 0x12345678);
        assert_eq!(r.read_u32().unwrap(), 0);
        assert_eq!(r.read_u32().unwrap(), u32::MAX);
        assert_eq!(r.remaining(), 0);
    }

    #[test]
    fn round_trip_u64() {
        let mut w = XdrWriter::new();
        w.write_u64(0x123456789ABCDEF0);
        let data = w.into_bytes();
        let mut r = XdrReader::new(&data);
        assert_eq!(r.read_u64().unwrap(), 0x123456789ABCDEF0);
    }

    #[test]
    fn round_trip_opaque() {
        let mut w = XdrWriter::new();
        w.write_opaque(b"hello");
        w.write_opaque(b"");
        w.write_opaque(b"ab");
        let data = w.into_bytes();
        let mut r = XdrReader::new(&data);
        assert_eq!(r.read_opaque().unwrap(), b"hello");
        assert_eq!(r.read_opaque().unwrap(), b"");
        assert_eq!(r.read_opaque().unwrap(), b"ab");
        assert_eq!(r.remaining(), 0);
    }

    #[test]
    fn round_trip_string() {
        let mut w = XdrWriter::new();
        w.write_string("test");
        let data = w.into_bytes();
        let mut r = XdrReader::new(&data);
        assert_eq!(r.read_string().unwrap(), "test");
    }

    #[test]
    fn padding_alignment() {
        // 5-byte opaque should be padded to 8 bytes (4 len + 8 data+pad = 12 total)
        let mut w = XdrWriter::new();
        w.write_opaque(b"12345");
        assert_eq!(w.len(), 12); // 4 (len) + 5 (data) + 3 (pad) = 12
    }
}
