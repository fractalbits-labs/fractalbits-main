use crate::message::MessageHeader;
use bytes::{Buf, Bytes, BytesMut};
use tokio_util::codec::Decoder;

pub struct MessageFrame {
    pub header: MessageHeader,
    pub body: Bytes,
}

#[derive(Default)]
pub struct MesssageCodec {}

#[cfg(feature = "nss")]
const MAX: usize = 2 * 1024 * 1024;

#[cfg(feature = "storage_server")]
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

        let header = MessageHeader::decode(src);
        let body = Bytes::copy_from_slice(&src.chunk()[header_size..header.size as usize]);
        src.advance(size);
        Ok(Some(MessageFrame { header, body }))
    }
}
