use crate::message::MessageHeader;
use bytes::{Buf, Bytes, BytesMut};
use tokio_util::codec::Decoder;

pub struct MessageFrame {
    pub header: MessageHeader,
    pub body: Bytes,
}

impl MessageFrame {
    pub fn new(header: MessageHeader, body: Bytes) -> Self {
        Self { header, body }
    }
}

#[derive(Default)]
pub struct MesssageCodec {}

#[cfg(feature = "nss")]
const MAX: usize = 2 * 1024 * 1024;

#[cfg(feature = "bss")]
const MAX: usize = 5 * 1024 * 1024 * 1024;

impl Decoder for MesssageCodec {
    type Item = MessageFrame;
    type Error = std::io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let header_size = MessageHeader::encode_len();
        if src.len() < header_size {
            return Ok(None);
        }

        let size = MessageHeader::get_size(src);
        if size > MAX {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Frame of size {} is too large.", size),
            ));
        }

        if src.len() < size {
            // The full message has not yet arrived.
            src.reserve(size - src.len());
            return Ok(None);
        }

        let header = MessageHeader::decode(&src.copy_to_bytes(header_size));
        let body = Bytes::copy_from_slice(&src.chunk()[0..header.size as usize - header_size]);
        src.advance(size - header_size);
        Ok(Some(MessageFrame { header, body }))
    }
}
