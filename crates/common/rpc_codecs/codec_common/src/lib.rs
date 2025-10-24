use bytes::{Bytes, BytesMut};

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
