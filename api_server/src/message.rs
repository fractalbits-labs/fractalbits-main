use crate::nss_ops::*;
use bytes::{BufMut, Bytes};
use prost::DecodeError;
use prost::EncodeError;

#[derive(bytemuck::NoUninit, Clone, Copy)]
#[repr(C)]
#[derive(Default)]
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

    /// role: request, response, broadcast ?
    /// The size of the Header structure (always), plus any associated body.
    pub size: u32,

    /// Every request would be sent with a unique id, so the client can get the right response
    pub id: u32,

    /// The protocol command (method) for this message.
    /// i32 size, defined as protobuf enum type
    pub command: Command,

    /// The message type: Request=0, Response=1, Stream=2
    message_type: u16,

    /// The version of the protocol implementation that originated this message.
    protocol: u16,

    /// Reserved for future use
    reserved1: u128,
    reserved2: u128,
}

// Safety: Command is defined as protobuf enum type (i32)
unsafe impl bytemuck::NoUninit for Command {}

impl MessageHeader {
    pub fn encode_len() -> usize {
        size_of::<Self>()
    }

    pub fn encode(&self, buf: &mut impl BufMut) -> Result<(), EncodeError>
    where
        Self: Sized,
    {
        let bytes: &[u8] = bytemuck::bytes_of(self);
        buf.put(bytes);
        Ok(())
    }

    pub fn decode(buf: Bytes) -> Result<Self, DecodeError>
    where
        Self: Default,
    {
        let _bytes: &[u8] = buf.slice(0..Self::encode_len()).as_ref();
        // Ok(bytemuck::from_bytes::<Self>(bytes).clone())
        todo!()
    }
}
