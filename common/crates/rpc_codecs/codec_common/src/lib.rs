use bytes::{Bytes, BytesMut};
use tokio_util::codec::Decoder;

pub trait MessageHeaderTrait: Sized + Clone + Copy + Send + Sync + 'static {
    const SIZE: usize;

    fn encode(&self, dst: &mut BytesMut);
    fn decode(src: &Bytes) -> Self;
    fn get_size(src: &BytesMut) -> usize;
    fn set_size(&mut self, size: u32);
    fn get_id(&self) -> u32;
    fn set_id(&mut self, id: u32);
    fn set_client_session_id(&mut self, session_id: u64);
    fn get_client_session_id(&self) -> u64;
    fn set_handshake_command(&mut self);
    fn get_body_size(&self) -> usize;
}

pub struct MessageFrame<H: MessageHeaderTrait> {
    pub header: H,
    pub body: Bytes,
}

impl<H: MessageHeaderTrait> MessageFrame<H> {
    pub fn new(header: H, body: Bytes) -> Self {
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

        let size = H::get_size(src);
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

        let header = H::decode(&src.split_to(header_size).freeze());
        let body = src.split_to(size - header_size).freeze();
        Ok(Some(MessageFrame { header, body }))
    }
}
