use bumpalo::Bump;
use bytes::{Buf, Bytes, BytesMut};
use tokio_util::codec::Decoder;

mod bump_buf;
pub use bump_buf::BumpBuf;

pub trait MessageHeaderTrait: Sized + Clone + Copy + Send + Sync + 'static {
    const SIZE: usize;

    fn encode(&self, dst: &mut BytesMut);
    fn decode(src: &[u8]) -> Self;
    fn get_size(src: &[u8]) -> usize;
    fn set_size(&mut self, size: u32);
    fn get_id(&self) -> u32;
    fn set_id(&mut self, id: u32);
    fn get_body_size(&self) -> usize;
    fn get_retry_count(&self) -> u32;
    fn set_retry_count(&mut self, retry_count: u32);
    fn get_trace_id(&self) -> u64;
    fn set_trace_id(&mut self, trace_id: u64);
}

pub struct MessageFrame<H: MessageHeaderTrait, B = Bytes> {
    pub header: H,
    pub body: B,
}

impl<H: MessageHeaderTrait, B> MessageFrame<H, B> {
    pub fn new(header: H, body: B) -> Self {
        Self { header, body }
    }
}

impl<H: MessageHeaderTrait> MessageFrame<H, Bytes> {
    pub fn from_bytes(header: H, body: Bytes) -> Self {
        Self { header, body }
    }
}

impl<'a, H: MessageHeaderTrait> MessageFrame<H, &'a [u8]> {
    pub fn from_slice(header: H, body: &'a [u8]) -> Self {
        Self { header, body }
    }
}

#[derive(Default, Clone)]
pub struct MessageCodec<H: MessageHeaderTrait> {
    _phantom: std::marker::PhantomData<H>,
}

const MAX: usize = 2 * 1024 * 1024;

impl<H: MessageHeaderTrait> Decoder for MessageCodec<H> {
    type Item = MessageFrame<H>;
    type Error = std::io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let header_size = H::SIZE;
        if src.len() < header_size {
            return Ok(None);
        }

        let size = H::get_size(src.as_ref());
        if size < header_size {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Frame size {size} is smaller than header size {header_size}"),
            ));
        }
        if size > MAX {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Frame of size {size} is too large."),
            ));
        }

        if src.len() < size {
            src.reserve(size - src.len());
            return Ok(None);
        }

        // Use bump allocator for temporary header parsing
        let bump = Bump::with_capacity(header_size);
        let header_data = bump.alloc_slice_copy(&src[..header_size]);
        let header = H::decode(header_data);

        // Extract body as Bytes (zero-copy from BytesMut)
        src.advance(header_size);
        let body_size = size - header_size;
        let body = src.split_to(body_size).freeze();

        Ok(Some(MessageFrame::new(header, body)))
    }
}
