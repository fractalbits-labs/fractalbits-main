use bumpalo::Bump;
use bumpalo::collections::Vec as BumpVec;
use bytes::{BufMut, Bytes};
use std::ops::Deref;

/// A buffer that implements `BufMut` backed by bumpalo's arena allocator.
///
/// This allows encoding data (e.g., protobuf messages) directly into
/// bump-allocated memory without intermediate allocations.
///
/// # Example
///
/// ```ignore
/// use bumpalo::Bump;
/// use rpc_codec_common::BumpBuf;
///
/// let bump = Bump::new();
/// let mut buf = BumpBuf::with_capacity_in(1024, &bump);
///
/// // Use with protobuf encoding
/// message.encode(&mut buf).unwrap();
///
/// // Access the encoded data
/// let data: &[u8] = buf.as_slice();
/// ```
pub struct BumpBuf<'bump> {
    data: BumpVec<'bump, u8>,
}

impl<'bump> BumpBuf<'bump> {
    /// Creates a new empty `BumpBuf` using the given bump allocator.
    #[inline]
    pub fn new_in(bump: &'bump Bump) -> Self {
        Self {
            data: BumpVec::new_in(bump),
        }
    }

    /// Creates a new `BumpBuf` with the specified capacity using the given bump allocator.
    #[inline]
    pub fn with_capacity_in(capacity: usize, bump: &'bump Bump) -> Self {
        Self {
            data: BumpVec::with_capacity_in(capacity, bump),
        }
    }

    /// Returns the number of bytes in the buffer.
    #[inline]
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Returns true if the buffer is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Returns the capacity of the buffer.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.data.capacity()
    }

    /// Returns a reference to the underlying data as a byte slice.
    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        &self.data
    }

    /// Consumes the `BumpBuf` and returns the underlying `BumpVec`.
    #[inline]
    pub fn into_vec(self) -> BumpVec<'bump, u8> {
        self.data
    }

    /// Converts the `BumpBuf` into `Bytes` by copying the data.
    ///
    /// Note: This creates a copy because `Bytes` requires owned data.
    #[inline]
    pub fn freeze(self) -> Bytes {
        Bytes::copy_from_slice(&self.data)
    }

    /// Clears the buffer, removing all data.
    #[inline]
    pub fn clear(&mut self) {
        self.data.clear();
    }
}

unsafe impl<'bump> BufMut for BumpBuf<'bump> {
    #[inline]
    fn remaining_mut(&self) -> usize {
        usize::MAX - self.data.len()
    }

    #[inline]
    fn chunk_mut(&mut self) -> &mut bytes::buf::UninitSlice {
        if self.data.capacity() == self.data.len() {
            let additional = if self.data.capacity() == 0 {
                64
            } else {
                self.data.capacity()
            };
            self.data.reserve(additional);
        }

        let cap = self.data.capacity();
        let len = self.data.len();

        unsafe {
            let ptr = self.data.as_mut_ptr().add(len);
            let uninit_slice =
                std::slice::from_raw_parts_mut(ptr as *mut std::mem::MaybeUninit<u8>, cap - len);
            bytes::buf::UninitSlice::from_raw_parts_mut(
                uninit_slice.as_mut_ptr() as *mut _,
                uninit_slice.len(),
            )
        }
    }

    #[inline]
    unsafe fn advance_mut(&mut self, cnt: usize) {
        let new_len = self.data.len() + cnt;
        debug_assert!(new_len <= self.data.capacity(), "advance_mut past capacity");
        unsafe {
            self.data.set_len(new_len);
        }
    }

    #[inline]
    fn put_slice(&mut self, src: &[u8]) {
        self.data.extend_from_slice(src);
    }

    #[inline]
    fn put_u8(&mut self, n: u8) {
        self.data.push(n);
    }

    #[inline]
    fn put_i8(&mut self, n: i8) {
        self.data.push(n as u8);
    }
}

impl<'bump> AsRef<[u8]> for BumpBuf<'bump> {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        &self.data
    }
}

impl<'bump> Deref for BumpBuf<'bump> {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &[u8] {
        &self.data
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_bump_buf() {
        let bump = Bump::new();
        let buf = BumpBuf::new_in(&bump);
        assert_eq!(buf.len(), 0);
        assert!(buf.is_empty());
    }

    #[test]
    fn test_with_capacity() {
        let bump = Bump::new();
        let buf = BumpBuf::with_capacity_in(1024, &bump);
        assert_eq!(buf.len(), 0);
        assert!(buf.capacity() >= 1024);
    }

    #[test]
    fn test_put_u8() {
        let bump = Bump::new();
        let mut buf = BumpBuf::new_in(&bump);
        buf.put_u8(42);
        buf.put_u8(255);
        assert_eq!(buf.len(), 2);
        assert_eq!(buf.as_slice(), &[42, 255]);
    }

    #[test]
    fn test_put_slice() {
        let bump = Bump::new();
        let mut buf = BumpBuf::new_in(&bump);
        buf.put_slice(b"hello");
        buf.put_slice(b" world");
        assert_eq!(buf.len(), 11);
        assert_eq!(buf.as_slice(), b"hello world");
    }

    #[test]
    fn test_put_i8() {
        let bump = Bump::new();
        let mut buf = BumpBuf::new_in(&bump);
        buf.put_i8(-1);
        buf.put_i8(127);
        assert_eq!(buf.len(), 2);
        assert_eq!(buf.as_slice(), &[255, 127]);
    }

    #[test]
    fn test_chunk_mut_and_advance() {
        let bump = Bump::new();
        let mut buf = BumpBuf::with_capacity_in(10, &bump);

        unsafe {
            let chunk = buf.chunk_mut();
            assert!(chunk.len() >= 10);

            chunk.as_mut_ptr().write_bytes(0x42, 5);
            buf.advance_mut(5);
        }

        assert_eq!(buf.len(), 5);
        assert_eq!(buf.as_slice(), &[0x42; 5]);
    }

    #[test]
    fn test_capacity_growth() {
        let bump = Bump::new();
        let mut buf = BumpBuf::new_in(&bump);
        let initial_cap = buf.capacity();

        for i in 0..1000 {
            buf.put_u8(i as u8);
        }

        assert_eq!(buf.len(), 1000);
        assert!(buf.capacity() > initial_cap);
    }

    #[test]
    fn test_freeze() {
        let bump = Bump::new();
        let mut buf = BumpBuf::new_in(&bump);
        buf.put_slice(b"test data");

        let bytes = buf.freeze();
        assert_eq!(bytes.as_ref(), b"test data");
    }

    #[test]
    fn test_clear() {
        let bump = Bump::new();
        let mut buf = BumpBuf::new_in(&bump);
        buf.put_slice(b"data");
        assert_eq!(buf.len(), 4);

        buf.clear();
        assert_eq!(buf.len(), 0);
        assert!(buf.is_empty());
    }

    #[test]
    fn test_as_ref() {
        let bump = Bump::new();
        let mut buf = BumpBuf::new_in(&bump);
        buf.put_slice(b"hello");

        let slice: &[u8] = buf.as_ref();
        assert_eq!(slice, b"hello");
    }

    #[test]
    fn test_deref() {
        let bump = Bump::new();
        let mut buf = BumpBuf::new_in(&bump);
        buf.put_slice(b"test");

        assert_eq!(&*buf, b"test");
        assert_eq!(buf[0], b't');
    }

    #[test]
    fn test_bufmut_put_methods() {
        let bump = Bump::new();
        let mut buf = BumpBuf::new_in(&bump);

        buf.put_u16(0x1234);
        buf.put_u32(0x12345678);
        buf.put_u64(0x123456789ABCDEF0);

        assert_eq!(buf.len(), 2 + 4 + 8);
    }

    #[test]
    fn test_remaining_mut() {
        let bump = Bump::new();
        let buf = BumpBuf::new_in(&bump);
        assert_eq!(buf.remaining_mut(), usize::MAX);
    }
}
