use axum::body::Body;
use axum::http::header::{CONTENT_ENCODING, HeaderMap, HeaderValue};
use axum::http::request::Request;
use bytes::{BufMut, Bytes};
use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};
use futures::prelude::*;
use futures::task;
use hmac::Mac;
use http_body::Frame;
use pin_project_lite::pin_project;
use std::pin::Pin;
use sync_wrapper::SyncWrapper;

use crate::handler::common::checksum::{
    ChecksumValue, Checksummer, request_trailer_checksum_algorithm,
};
use crate::handler::common::xheader;

use super::body::ReqBody;
use super::payload::CheckedSignature;
use super::*;
use crate::handler::common::time::{LONG_DATETIME, SHORT_DATE};
use aws_signature::{HmacSha256, get_signing_key};
use data_types::hash::{Hash, sha256sum};

pub fn parse_streaming_body(
    mut req: Request<Body>,
    checked_signature: &CheckedSignature,
    region: &str,
    service: &str,
) -> Result<Request<ReqBody>, SignatureError> {
    tracing::debug!(
        "Content signature mode: {:?}",
        checked_signature.content_sha256_header
    );

    match checked_signature.content_sha256_header {
        ContentSha256Header::StreamingPayload { signed, trailer } => {
            // Sanity checks
            if !signed && !trailer {
                return Err(SignatureError::Other(
                    "STREAMING-UNSIGNED-PAYLOAD without trailer is not a valid combination".into(),
                ));
            }

            // Remove the aws-chunked component in the content-encoding: header
            // Note: this header is not properly sent by minio client, so don't fail
            // if it is absent from the request.
            if let Some(content_encoding) = req.headers_mut().remove(CONTENT_ENCODING) {
                if let Some(rest) = content_encoding.as_bytes().strip_prefix(b"aws-chunked,") {
                    req.headers_mut()
                        .insert(CONTENT_ENCODING, HeaderValue::from_bytes(rest).unwrap());
                } else if content_encoding != "aws-chunked" {
                    return Err(SignatureError::Other(
                        "content-encoding does not contain aws-chunked for STREAMING-*-PAYLOAD"
                            .into(),
                    ));
                }
            }

            // If trailer header is announced, add the calculation of the requested checksum
            let mut checksummer: Option<Checksummer> = None;
            let trailer_algorithm = if trailer {
                let algo = request_trailer_checksum_algorithm(req.headers())?
                    .ok_or_else(|| SignatureError::Other("Missing x-amz-trailer header".into()))?;
                checksummer = Some(Checksummer::new(algo));
                Some(algo)
            } else {
                None
            };

            // For signed variants, determine signing parameters
            let sign_params = if signed {
                let signature = checked_signature
                    .signature_header
                    .clone()
                    .ok_or_else(|| SignatureError::Other("No signature provided".into()))?;
                let signature = hex::decode(signature)
                    .ok()
                    .and_then(|bytes| Hash::try_from(&bytes))
                    .ok_or_else(|| SignatureError::Other("Invalid signature".into()))?;

                let secret_key = checked_signature.api_key.data.secret_key.clone();
                let date = req
                    .headers()
                    .get(xheader::X_AMZ_DATE)
                    .ok_or_else(|| SignatureError::Other("Missing X-Amz-Date field".into()))?
                    .to_str()?;
                let date: NaiveDateTime = NaiveDateTime::parse_from_str(date, LONG_DATETIME)
                    .map_err(|_e| SignatureError::Other("Invalid date".into()))?;
                let date: DateTime<Utc> = Utc.from_utc_datetime(&date);

                let scope = format!(
                    "{}/{}/{}/aws4_request",
                    date.format(SHORT_DATE),
                    region,
                    service
                );
                let signing_key = get_signing_key(date, &secret_key, region)
                    .map_err(|_e| SignatureError::Other("Unable to build signing key".into()))?;
                let signing_hmac = HmacSha256::new_from_slice(&signing_key)
                    .map_err(|_e| SignatureError::Other("Unable to build signing HMAC".into()))?;

                Some(SignParams {
                    datetime: date,
                    scope,
                    signing_hmac,
                    previous_signature: signature,
                })
            } else {
                None
            };

            Ok(req.map(move |body| {
                let stream = body.into_data_stream().map_err(SignatureError::from);

                let signed_payload_stream =
                    StreamingPayloadStream::new(stream, sign_params, trailer)
                        .map_err(SignatureError::from);
                ReqBody {
                    stream: SyncWrapper::new(signed_payload_stream.boxed()),
                    checksummer,
                    expected_checksum: None,
                    trailer_algorithm,
                }
            }))
        }
        _ => Ok(req.map(|body| {
            let expected_checksum = match &checked_signature.content_sha256_header {
                ContentSha256Header::Sha256Checksum(sha256) => {
                    let mut bytes = [0u8; 32];
                    bytes.copy_from_slice(sha256.as_slice());
                    Some(ChecksumValue::Sha256(bytes))
                }
                _ => None,
            };
            let checksummer = expected_checksum.map(|c| Checksummer::new(c.algorithm()));

            let stream = http_body_util::BodyStream::new(body).map_err(SignatureError::from);
            ReqBody {
                stream: SyncWrapper::new(stream.boxed()),
                checksummer,
                expected_checksum,
                trailer_algorithm: None,
            }
        })),
    }
}

fn compute_streaming_payload_signature(
    signing_hmac: &HmacSha256,
    date: DateTime<Utc>,
    scope: &str,
    previous_signature: Hash,
    content_sha256: Hash,
) -> Result<Hash, StreamingPayloadError> {
    let string_to_sign = [
        AWS4_HMAC_SHA256_PAYLOAD,
        &date.format(LONG_DATETIME).to_string(),
        scope,
        &hex::encode(previous_signature),
        EMPTY_STRING_HEX_DIGEST,
        &hex::encode(content_sha256),
    ]
    .join("\n");

    let mut hmac = signing_hmac.clone();
    hmac.update(string_to_sign.as_bytes());

    Hash::try_from(&hmac.finalize().into_bytes())
        .ok_or_else(|| StreamingPayloadError::Message("Could not build signature".into()))
}

fn compute_streaming_trailer_signature(
    signing_hmac: &HmacSha256,
    date: DateTime<Utc>,
    scope: &str,
    previous_signature: Hash,
    trailer_sha256: Hash,
) -> Result<Hash, StreamingPayloadError> {
    let string_to_sign = [
        AWS4_HMAC_SHA256_PAYLOAD,
        &date.format(LONG_DATETIME).to_string(),
        scope,
        &hex::encode(previous_signature),
        &hex::encode(trailer_sha256),
    ]
    .join("\n");

    let mut hmac = signing_hmac.clone();
    hmac.update(string_to_sign.as_bytes());

    Hash::try_from(&hmac.finalize().into_bytes())
        .ok_or_else(|| StreamingPayloadError::Message("Could not build signature".into()))
}

mod payload {
    use axum::http::{HeaderName, HeaderValue};

    use data_types::hash::Hash;

    use nom::bytes::streaming::{tag, take_while};
    use nom::character::streaming::hex_digit1;
    use nom::combinator::{map_res, opt};
    use nom::number::streaming::hex_u32;

    macro_rules! try_parse {
        ($expr:expr) => {
            $expr.map_err(|e| e.map(Error::Parser))?
        };
    }

    pub enum Error<I> {
        Parser(nom::error::Error<I>),
        BadSignature,
    }

    impl<I> Error<I> {
        pub fn description(&self) -> &str {
            match *self {
                Error::Parser(ref e) => e.code.description(),
                Error::BadSignature => "Bad signature",
            }
        }
    }

    #[derive(Debug, Clone)]
    pub struct ChunkHeader {
        pub size: usize,
        pub signature: Option<Hash>,
    }

    impl ChunkHeader {
        pub fn parse_signed(input: &[u8]) -> nom::IResult<&[u8], Self, Error<&[u8]>> {
            let (input, size) = try_parse!(hex_u32(input));
            let (input, _) = try_parse!(tag(";")(input));

            let (input, _) = try_parse!(tag("chunk-signature=")(input));
            let (input, data) = try_parse!(map_res(hex_digit1, hex::decode)(input));
            let signature = Hash::try_from(&data).ok_or(nom::Err::Failure(Error::BadSignature))?;

            let (input, _) = try_parse!(tag("\r\n")(input));

            let header = ChunkHeader {
                size: size as usize,
                signature: Some(signature),
            };

            Ok((input, header))
        }

        pub fn parse_unsigned(input: &[u8]) -> nom::IResult<&[u8], Self, Error<&[u8]>> {
            let (input, size) = try_parse!(hex_u32(input));
            let (input, _) = try_parse!(tag("\r\n")(input));

            let header = ChunkHeader {
                size: size as usize,
                signature: None,
            };

            Ok((input, header))
        }
    }

    #[derive(Debug, Clone)]
    pub struct TrailerChunk {
        pub header_name: HeaderName,
        pub header_value: HeaderValue,
        pub signature: Option<Hash>,
    }

    impl TrailerChunk {
        fn parse_content(input: &[u8]) -> nom::IResult<&[u8], Self, Error<&[u8]>> {
            let (input, header_name) = try_parse!(map_res(
                take_while(|c: u8| c.is_ascii_alphanumeric() || c == b'-'),
                HeaderName::from_bytes
            )(input));
            let (input, _) = try_parse!(tag(b":")(input));
            let (input, header_value) = try_parse!(map_res(
                take_while(|c: u8| c.is_ascii_alphanumeric() || b"+/=".contains(&c)),
                HeaderValue::from_bytes
            )(input));

            // Possible '\n' after the header value, depends on clients
            // https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html
            let (input, _) = try_parse!(opt(tag(b"\n"))(input));

            let (input, _) = try_parse!(tag(b"\r\n")(input));

            Ok((
                input,
                TrailerChunk {
                    header_name,
                    header_value,
                    signature: None,
                },
            ))
        }
        pub fn parse_signed(input: &[u8]) -> nom::IResult<&[u8], Self, Error<&[u8]>> {
            let (input, trailer) = Self::parse_content(input)?;

            let (input, _) = try_parse!(tag(b"x-amz-trailer-signature:")(input));
            let (input, data) = try_parse!(map_res(hex_digit1, hex::decode)(input));
            let signature = Hash::try_from(&data).ok_or(nom::Err::Failure(Error::BadSignature))?;
            let (input, _) = try_parse!(tag(b"\r\n")(input));

            Ok((
                input,
                TrailerChunk {
                    signature: Some(signature),
                    ..trailer
                },
            ))
        }
        pub fn parse_unsigned(input: &[u8]) -> nom::IResult<&[u8], Self, Error<&[u8]>> {
            let (input, trailer) = Self::parse_content(input)?;
            let (input, _) = try_parse!(tag(b"\r\n")(input));

            Ok((input, trailer))
        }
    }
}

#[derive(Debug)]
pub enum StreamingPayloadError {
    Stream(SignatureError),
    InvalidSignature,
    Message(String),
}

impl StreamingPayloadError {
    fn message(msg: &str) -> Self {
        StreamingPayloadError::Message(msg.into())
    }
}

impl From<StreamingPayloadError> for SignatureError {
    fn from(err: StreamingPayloadError) -> Self {
        match err {
            StreamingPayloadError::Stream(e) => e,
            StreamingPayloadError::InvalidSignature => {
                SignatureError::Other("Invalid payload signature".into())
            }
            StreamingPayloadError::Message(e) => {
                SignatureError::Other(format!("Chunk format error: {e}"))
            }
        }
    }
}

impl<I> From<payload::Error<I>> for StreamingPayloadError {
    fn from(err: payload::Error<I>) -> Self {
        Self::message(err.description())
    }
}

impl<I> From<nom::error::Error<I>> for StreamingPayloadError {
    fn from(err: nom::error::Error<I>) -> Self {
        Self::message(err.code.description())
    }
}

enum StreamingPayloadChunk {
    Chunk {
        header: payload::ChunkHeader,
        data: Bytes,
    },
    Trailer(payload::TrailerChunk),
}

struct SignParams {
    datetime: DateTime<Utc>,
    scope: String,
    signing_hmac: HmacSha256,
    previous_signature: Hash,
}

pin_project! {
    pub struct StreamingPayloadStream<S>
    where
        S: Stream<Item = Result<Bytes, SignatureError>>,
    {
        #[pin]
        stream: S,
        buf: bytes::BytesMut,
        signing: Option<SignParams>,
        has_trailer: bool,
        done: bool,
    }
}

impl<S> StreamingPayloadStream<S>
where
    S: Stream<Item = Result<Bytes, SignatureError>>,
{
    fn new(stream: S, signing: Option<SignParams>, has_trailer: bool) -> Self {
        Self {
            stream,
            buf: bytes::BytesMut::new(),
            signing,
            has_trailer,
            done: false,
        }
    }

    fn parse_next(
        input: &[u8],
        is_signed: bool,
        has_trailer: bool,
    ) -> nom::IResult<&[u8], StreamingPayloadChunk, StreamingPayloadError> {
        use nom::bytes::streaming::{tag, take};

        macro_rules! try_parse {
            ($expr:expr) => {
                $expr.map_err(nom::Err::convert)?
            };
        }

        let (input, header) = if is_signed {
            try_parse!(payload::ChunkHeader::parse_signed(input))
        } else {
            try_parse!(payload::ChunkHeader::parse_unsigned(input))
        };

        // 0-sized chunk is the last
        if header.size == 0 {
            if has_trailer {
                let (input, trailer) = if is_signed {
                    try_parse!(payload::TrailerChunk::parse_signed(input))
                } else {
                    try_parse!(payload::TrailerChunk::parse_unsigned(input))
                };
                return Ok((input, StreamingPayloadChunk::Trailer(trailer)));
            } else {
                return Ok((
                    input,
                    StreamingPayloadChunk::Chunk {
                        header,
                        data: Bytes::new(),
                    },
                ));
            }
        }

        let (input, data) = try_parse!(take::<_, _, nom::error::Error<_>>(header.size)(input));
        let (input, _) = try_parse!(tag::<_, _, nom::error::Error<_>>("\r\n")(input));

        let data = Bytes::copy_from_slice(data);

        Ok((input, StreamingPayloadChunk::Chunk { header, data }))
    }
}

impl<S> Stream for StreamingPayloadStream<S>
where
    S: Stream<Item = Result<Bytes, SignatureError>> + Unpin,
{
    type Item = Result<Frame<Bytes>, StreamingPayloadError>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut task::Context<'_>,
    ) -> task::Poll<Option<Self::Item>> {
        use bytes::Buf;
        use sha2::{Digest, Sha256};
        use std::task::Poll;

        let mut this = self.project();

        if *this.done {
            return Poll::Ready(None);
        }

        loop {
            let buf_len = this.buf.len();
            let (input, payload) =
                match Self::parse_next(this.buf, this.signing.is_some(), *this.has_trailer) {
                    Ok(res) => res,
                    Err(nom::Err::Incomplete(_)) => {
                        match futures::ready!(this.stream.as_mut().poll_next(cx)) {
                            Some(Ok(bytes)) => {
                                this.buf.put(bytes);
                                continue;
                            }
                            Some(Err(e)) => {
                                return Poll::Ready(Some(Err(StreamingPayloadError::Stream(e))));
                            }
                            None => {
                                if this.buf.is_empty() {
                                    return Poll::Ready(None);
                                }
                                return Poll::Ready(Some(Err(StreamingPayloadError::message(
                                    "Unexpected EOF",
                                ))));
                            }
                        }
                    }
                    Err(nom::Err::Error(e)) | Err(nom::Err::Failure(e)) => {
                        return Poll::Ready(Some(Err(e)));
                    }
                };

            let consumed = buf_len - input.len();

            match payload {
                StreamingPayloadChunk::Chunk { data, header } => {
                    if let Some(signing) = this.signing.as_mut() {
                        let data_sha256sum = sha256sum(&data);

                        let expected_signature = compute_streaming_payload_signature(
                            &signing.signing_hmac,
                            signing.datetime,
                            &signing.scope,
                            signing.previous_signature,
                            data_sha256sum,
                        )?;

                        if header.signature.unwrap() != expected_signature {
                            return Poll::Ready(Some(Err(StreamingPayloadError::InvalidSignature)));
                        }

                        signing.previous_signature = header.signature.unwrap();
                    }

                    this.buf.advance(consumed);

                    // 0-sized chunk is the last
                    if data.is_empty() {
                        // if there was a trailer, it would have been returned by the parser
                        assert!(!*this.has_trailer);
                        *this.done = true;
                        return Poll::Ready(None);
                    }

                    return Poll::Ready(Some(Ok(Frame::data(data))));
                }
                StreamingPayloadChunk::Trailer(trailer) => {
                    tracing::trace!(
                        "In StreamingPayloadStream::poll_next: got trailer {:?}",
                        trailer
                    );

                    if let Some(signing) = this.signing.as_mut() {
                        let mut hasher = Sha256::new();
                        hasher.update(trailer.header_name.as_ref() as &[u8]);
                        hasher.update(b":");
                        hasher.update(trailer.header_value.as_ref() as &[u8]);
                        hasher.update(b"\n");
                        let digest = hasher.finalize();
                        let hash_bytes: [u8; 32] = digest.into();
                        let trailer_sha256sum: Hash = hash_bytes.into();

                        let expected_signature = compute_streaming_trailer_signature(
                            &signing.signing_hmac,
                            signing.datetime,
                            &signing.scope,
                            signing.previous_signature,
                            trailer_sha256sum,
                        )?;

                        if trailer.signature.unwrap() != expected_signature {
                            return Poll::Ready(Some(Err(StreamingPayloadError::InvalidSignature)));
                        }
                    }

                    this.buf.advance(consumed);
                    *this.done = true;

                    let mut trailers_map = HeaderMap::new();
                    trailers_map.insert(trailer.header_name, trailer.header_value);

                    return Poll::Ready(Some(Ok(Frame::trailers(trailers_map))));
                }
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.stream.size_hint()
    }
}

#[cfg(test)]
mod tests {
    use futures::prelude::*;

    use super::{SignParams, StreamingPayloadError, StreamingPayloadStream};

    #[tokio::test]
    async fn test_interrupted_signed_payload_stream() {
        use chrono::{DateTime, Utc};

        use crate::handler::common::time::SHORT_DATE;
        use aws_signature::{HmacSha256, get_signing_key};
        use data_types::hash::Hash;
        use hmac::Mac;

        let datetime = DateTime::parse_from_rfc3339("2021-12-13T13:12:42+01:00") // TODO UNIX 0
            .unwrap()
            .with_timezone(&Utc);
        let secret_key = "test";
        let region = "test";
        let scope = format!("{}/{}/s3/aws4_request", datetime.format(SHORT_DATE), region);
        let signing_key = get_signing_key(datetime, secret_key, region).unwrap();
        let signing_hmac = HmacSha256::new_from_slice(&signing_key).unwrap();

        let data: &[&[u8]] = &[b"1"];
        let body = futures::stream::iter(data.iter().map(|block| Ok(block.to_vec().into())));

        let seed_signature = Hash::default();

        let mut stream = StreamingPayloadStream::new(
            body,
            Some(SignParams {
                signing_hmac,
                datetime,
                scope,
                previous_signature: seed_signature,
            }),
            false,
        );

        assert!(stream.try_next().await.is_err());
        match stream.try_next().await {
            Err(StreamingPayloadError::Message(msg)) if msg == "Unexpected EOF" => {}
            item => panic!("Unexpected result, expected early EOF error, got {item:?}"),
        }
    }
}
