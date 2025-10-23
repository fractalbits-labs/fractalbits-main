use crate::Command;
use bytemuck::{Pod, Zeroable};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use rpc_codec_common::MessageHeaderTrait;

#[repr(C)]
#[derive(Pod, Debug, Default, Clone, Copy, Zeroable)]
pub struct MessageHeader {
    /// The size of the Header structure (always), plus any associated body.
    pub size: u32,

    /// Every request would be sent with a unique id, so the client can get the right response
    pub id: u32,

    /// The protocol command (method) for this message.
    /// i32 size, defined as protobuf enum type
    pub command: Command,

    /// The message type: Request=0, Response=1, Notify=2
    message_type: u16,

    /// The version of the protocol implementation that originated this message.
    protocol: u16,

    /// Trace ID for distributed tracing and bump allocator lookup
    pub trace_id: u64,

    /// Number of retry attempts for this request (0 = first attempt)
    pub retry_count: u32,

    /// Reserved for future use
    reserved: [u8; 4],
}

// Safety: Command is defined as protobuf enum type (i32), and 0 as Invalid. There is also no padding
// as verified from the zig side. With header checksum validation, we can also be sure no invalid
// enum value being interpreted.
unsafe impl Pod for Command {}
unsafe impl Zeroable for Command {}

impl MessageHeader {
    const _SIZE_OK: () = assert!(size_of::<Self>() == 32);
    pub const SIZE: usize = size_of::<Self>();

    pub fn encode(&self, dst: &mut BytesMut) {
        let bytes: &[u8] = bytemuck::bytes_of(self);
        dst.put(bytes);
    }

    pub fn decode_bytes(src: &Bytes) -> Self {
        let header_bytes = &src.chunk()[0..Self::SIZE];
        // TODO: verify header checksum
        bytemuck::pod_read_unaligned::<Self>(header_bytes)
    }

    pub fn get_size_bytes(src: &mut BytesMut) -> usize {
        let offset = std::mem::offset_of!(MessageHeader, size);
        let mut bytes = [0u8; 4];
        bytes.copy_from_slice(&src[offset..offset + 4]);
        u32::from_le_bytes(bytes) as usize
    }
}

impl MessageHeaderTrait for MessageHeader {
    const SIZE: usize = 32;

    fn encode(&self, dst: &mut BytesMut) {
        self.encode(dst)
    }

    fn decode(src: &[u8]) -> Self {
        // TODO: verify header checksum
        bytemuck::pod_read_unaligned::<Self>(&src[..Self::SIZE])
    }

    fn get_size(src: &[u8]) -> usize {
        let offset = std::mem::offset_of!(MessageHeader, size);
        let mut bytes = [0u8; 4];
        bytes.copy_from_slice(&src[offset..offset + 4]);
        u32::from_le_bytes(bytes) as usize
    }

    fn set_size(&mut self, size: u32) {
        self.size = size;
    }

    fn get_id(&self) -> u32 {
        self.id
    }

    fn set_id(&mut self, id: u32) {
        self.id = id;
    }

    fn get_body_size(&self) -> usize {
        (self.size as usize).saturating_sub(Self::SIZE)
    }

    fn get_retry_count(&self) -> u32 {
        self.retry_count
    }

    fn set_retry_count(&mut self, retry_count: u32) {
        self.retry_count = retry_count;
    }

    fn get_trace_id(&self) -> u64 {
        self.trace_id
    }

    fn set_trace_id(&mut self, trace_id: u64) {
        self.trace_id = trace_id;
    }
}
