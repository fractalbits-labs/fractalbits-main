//! Storage server message format.
//! Note if this file is updated, the corresponding message.zig file also needs to be updated!
use bytemuck::{Pod, Zeroable};
use bytes::{Buf, BufMut, Bytes, BytesMut};

#[repr(C)]
#[derive(Pod, Default, Debug, Clone, Copy, Zeroable)]
pub struct MessageHeader {
    /// A checksum covering only the remainder of this header.
    /// This allows the header to be trusted without having to recv() or read() the associated body.
    /// This checksum is enough to uniquely identify a network message or prepare.
    checksum: u128,

    // TODO(zig): When Zig supports u256 in extern-structs, merge this into `checksum`.
    checksum_padding: u128,

    /// A checksum covering only the associated body after this header.
    checksum_body: u128,

    // TODO(zig): When Zig supports u256 in extern-structs, merge this into `checksum_body`.
    checksum_body_padding: u128,

    /// The cluster number binds intention into the header, so that a nss or api_server can indicate
    /// the cluster it believes it is speaking to, instead of accidentally talking to the wrong
    /// cluster (for example, staging vs production).
    cluster: u128,

    /// The size of the Header structure (always), plus any associated body.
    pub size: u64,

    /// Response result
    pub result: u64,

    /// Errno which can be converted into `std::io::Error`(`from_raw_os_error()`)
    pub errno: i32,

    /// Every request would be sent with a unique id, so the client can get the right response
    pub id: u32,

    /// The protocol command (method) for this message.
    /// i32 size, defined as protobuf enum type
    pub command: Command,

    /// The version of the protocol implementation that originated this message.
    pub protocol: u16,

    /// Bucket Id
    pub bucket_id: [u8; 16],

    /// Blob Id
    pub blob_id: [u8; 16],

    /// Reserved parts for padding
    // Note rust arrays of sizes from 0 to 32 (inclusive) implement the Default trait if the element
    // type allows it. As a stopgap, trait implementations are statically generated up to size 32.
    // See [doc](https://doc.rust-lang.org/std/primitive.array.html) for more details.
    reserved0: [u8; 18],
    reserved1: [u8; 32],
    reserved2: [u8; 32],
    reserved3: [u8; 32],
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[repr(u32)]
pub enum Command {
    Invalid = 0,
    PutBlob = 1,
    GetBlob = 2,
}

impl Default for Command {
    fn default() -> Self {
        Command::Invalid
    }
}

// Safety: Command is defined as protobuf enum type (u32), and 0 as Invalid. There is also no padding
// as verified from the zig side. With header checksum validation, we can also be sure no invalid
// enum value being interpreted.
unsafe impl Pod for Command {}
unsafe impl Zeroable for Command {}

impl MessageHeader {
    const _SIZE_OK: () = assert!(size_of::<Self>() == 256);
    pub fn encode_len() -> usize {
        256
    }

    pub fn encode(&self, dst: &mut BytesMut) {
        let bytes: &[u8] = bytemuck::bytes_of(self);
        dst.put(bytes);
    }

    pub fn decode(src: &Bytes) -> Self {
        let header_bytes = &src.chunk()[0..Self::encode_len()];
        // TODO: verify header checksum
        bytemuck::pod_read_unaligned::<Self>(header_bytes).to_owned()
    }

    pub fn get_size(src: &mut BytesMut) -> usize {
        let offset = std::mem::offset_of!(MessageHeader, size);
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&src[offset..offset + 8]);
        u64::from_le_bytes(bytes) as usize
    }
}
