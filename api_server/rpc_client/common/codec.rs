use crate::message::MessageHeader;
use bytes::{Bytes, BytesMut};
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

#[cfg(any(feature = "nss", feature = "rss"))]
const MAX: usize = 2 * 1024 * 1024;

#[cfg(feature = "bss")]
const MAX: usize = 5 * 1024 * 1024 * 1024;

impl Decoder for MesssageCodec {
    type Item = MessageFrame;
    type Error = std::io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let header_size = MessageHeader::SIZE;
        if src.len() < header_size {
            return Ok(None);
        }

        let size = MessageHeader::get_size(src);
        if size > MAX {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Frame of size {size} is too large."),
            ));
        }

        if src.len() < size {
            // The full message has not yet arrived.
            src.reserve(size - src.len());
            return Ok(None);
        }

        let header = MessageHeader::decode(&src.split_to(header_size).freeze());
        let body = src.split_to(size - header_size).freeze();
        Ok(Some(MessageFrame { header, body }))
    }
}
