use bytemuck::{Pod, Zeroable};
use bytes::{BufMut, BytesMut};
use data_types::TraceId;
use xxhash_rust::xxh3::xxh3_64;

use crate::MessageHeaderTrait;

/// XXH3-64 hash of an empty buffer (seed=0)
/// This is the correct checksum value for empty message bodies
pub const EMPTY_BODY_CHECKSUM: u64 = 0x2d06800538d394c2;

/// Generic protobuf-based message header implementation
///
/// This provides a common implementation for protobuf-based RPC protocols.
/// The Command type must be a protobuf enum (i32) that implements Pod and Zeroable.
#[repr(C)]
#[derive(Debug, Default, Clone, Copy)]
pub struct ProtobufMessageHeader<Command>
where
    Command: Pod + Zeroable + Default + Clone + Copy + Send + Sync + 'static,
{
    /// A checksum covering only the remainder of this header.
    /// This allows the header to be trusted without having to recv() or read() the associated body.
    pub checksum: u64,
    /// The current protocol version, note the position should never be changed
    /// so that we can upgrade proto version in the future.
    pub proto_version: u8,
    /// Number of retry attempts for this request (0 = first attempt)
    pub retry_count: u8,
    /// Reserved for future use
    pub _reserved0: u16,
    /// The size of the Header structure, plus any associated body.
    pub size: u32,

    /// A checksum covering only the associated body after this header.
    pub checksum_body: u64,
    /// Every request would be sent with a unique id, so the client can get the right response
    pub id: u32,
    /// The protocol command (method) for this message.
    /// i32 size, defined as protobuf enum type
    pub command: Command,

    /// Trace ID for distributed tracing
    pub trace_id: u64,
    pub _reserved1: u64,
}

// Safety: ProtobufMessageHeader has the same layout requirements as its fields.
// When Command implements Pod (meaning it's valid for any bit pattern), and all other fields
// are primitive types that implement Pod, the whole struct is Pod.
unsafe impl<Command> Pod for ProtobufMessageHeader<Command> where
    Command: Pod + Zeroable + Default + Clone + Copy + Send + Sync + 'static
{
}

// Safety: When Command implements Zeroable (meaning all zeros is a valid value),
// and all other fields are primitive types that implement Zeroable, the whole struct is Zeroable.
unsafe impl<Command> Zeroable for ProtobufMessageHeader<Command> where
    Command: Pod + Zeroable + Default + Clone + Copy + Send + Sync + 'static
{
}

impl<Command> ProtobufMessageHeader<Command>
where
    Command: Pod + Zeroable + Default + Clone + Copy + Send + Sync + 'static,
{
    const _SIZE_OK: () = assert!(size_of::<Self>() == 48);

    pub fn encode(&self, dst: &mut BytesMut) {
        let bytes: &[u8] = bytemuck::bytes_of(self);
        dst.put(bytes);
    }

    pub fn decode(src: &[u8]) -> Self {
        bytemuck::pod_read_unaligned::<Self>(&src[..size_of::<Self>()])
    }

    /// Calculate and set the checksum field for this header.
    /// The checksum covers all header fields after the checksum field itself.
    pub fn set_checksum(&mut self) {
        let checksum_offset = std::mem::offset_of!(Self, checksum);
        let bytes: &[u8] = bytemuck::bytes_of(self);
        let bytes_to_hash = &bytes[checksum_offset + size_of::<u64>()..size_of::<Self>()];
        self.checksum = xxh3_64(bytes_to_hash);
    }

    /// Calculate and set the body checksum field.
    /// The checksum covers the message body after this header.
    pub fn set_body_checksum(&mut self, body: &[u8]) {
        self.checksum_body = if body.is_empty() {
            EMPTY_BODY_CHECKSUM
        } else {
            xxh3_64(body)
        };
    }

    /// Verify that the body checksum field matches the calculated checksum.
    /// Returns true if valid, false otherwise.
    pub fn verify_body_checksum(&self, body: &[u8]) -> bool {
        let calculated = if body.is_empty() {
            EMPTY_BODY_CHECKSUM
        } else {
            xxh3_64(body)
        };
        self.checksum_body == calculated
    }

    /// Calculate and set the body checksum field from multiple chunks.
    /// Uses streaming hash to avoid concatenation.
    pub fn set_body_checksum_vectored(&mut self, chunks: &[impl AsRef<[u8]>]) {
        use xxhash_rust::xxh3::Xxh3;
        let mut hasher = Xxh3::new();
        for chunk in chunks {
            hasher.update(chunk.as_ref());
        }
        self.checksum_body = hasher.digest();
    }

    pub fn set_trace_id(&mut self, trace_id: &TraceId) {
        self.trace_id = trace_id.0;
    }
}

impl<Command> MessageHeaderTrait for ProtobufMessageHeader<Command>
where
    Command: Pod + Zeroable + Default + Clone + Copy + Send + Sync + 'static,
{
    fn encode(&self, dst: &mut BytesMut) {
        self.encode(dst)
    }

    fn decode(src: &[u8]) -> Self {
        bytemuck::pod_read_unaligned::<Self>(&src[..size_of::<Self>()])
    }

    fn get_size(&self) -> usize {
        self.size as usize
    }

    fn get_id(&self) -> u32 {
        self.id
    }

    fn get_body_size(&self) -> usize {
        (self.size as usize).saturating_sub(size_of::<Self>())
    }

    fn get_trace_id(&self) -> TraceId {
        TraceId::from(self.trace_id)
    }

    fn set_checksum(&mut self) {
        self.set_checksum()
    }

    fn verify_body_checksum(&self, body: &[u8]) -> bool {
        self.verify_body_checksum(body)
    }
}
