use actix_web::error::PayloadError;
use actix_web::HttpRequest;
use base64::prelude::*;
use bytes::{Buf, Bytes, BytesMut};
use futures::{ready, Stream};
use pin_project_lite::pin_project;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::handler::common::data::Hash;
use crate::handler::common::s3_error::S3Error;
use crate::handler::common::signature::checksum::{
    request_checksum_value, request_trailer_checksum_algorithm, ChecksumAlgorithm, Checksummer,
    Checksums, ExpectedChecksums,
};

/// Streaming checksum receiver type
pub type StreamingChecksumReceiver = JoinHandle<Result<Checksums, S3Error>>;

/// Message types for communication between streaming payload and checksum calculator
#[derive(Debug, Clone)]
pub enum ChecksumMessage {
    /// Regular data chunk to be included in checksum calculation
    Data(Bytes),
    /// Trailer checksums received at the end of the stream
    Trailers(S3Trailers),
}

/// S3-specific trailer information extracted from HTTP trailers
#[derive(Debug, Default, Clone)]
pub struct S3Trailers {
    pub checksum_crc32: Option<String>,
    pub checksum_crc32c: Option<String>,
    pub checksum_sha1: Option<String>,
    pub checksum_sha256: Option<String>,
    pub trailer_signature: Option<String>,
}

/// State machine for parsing S3 streaming payloads
#[derive(Debug, Clone, PartialEq)]
enum S3ChunkState {
    ReadingChunkSize,
    ReadingChunkData { remaining: usize },
    ReadingChunkEnd,
    ReadingTrailers,
    Done,
}

pin_project! {
    /// A wrapper around actix-web's Payload that handles S3-specific streaming features:
    /// - AWS chunk-signature extensions
    /// - HTTP trailers with checksums
    /// - Streaming checksum calculation
    pub struct S3StreamingPayload {
        #[pin]
        inner: actix_web::dev::Payload,
        state: S3ChunkState,
        trailer_buffer: BytesMut,
        checksum_sender: Option<mpsc::UnboundedSender<ChecksumMessage>>,
    }
}

impl S3StreamingPayload {
    /// Create a streaming payload with optimized checksum calculation
    /// Avoids task spawning for simple cases (no trailers, small objects)
    pub fn with_checksums(
        payload: actix_web::dev::Payload,
        request: &HttpRequest,
    ) -> Result<(Self, StreamingChecksumReceiver), S3Error> {
        // For now, always use the full checksum path to ensure correctness
        // TODO: Re-implement optimization after fixing checksum calculation
        Self::with_checksums_full(payload, request)
    }

    /// Full path: complete functionality with task spawning for complex cases
    fn with_checksums_full(
        payload: actix_web::dev::Payload,
        request: &HttpRequest,
    ) -> Result<(Self, StreamingChecksumReceiver), S3Error> {
        tracing::debug!("Using full checksum path (with task spawning)");

        // Extract expected checksums from request headers
        let expected_checksums = Self::extract_expected_checksums(request)?;

        // Extract trailer algorithm if present
        let trailer_algorithm = Self::extract_trailer_algorithm(request)?;

        // Determine if this is a streaming request or regular content-length request
        let is_streaming = Self::is_streaming_request(request);
        tracing::debug!("Request is streaming: {}", is_streaming);

        // Create checksummer
        let mut checksummer = Checksummer::init(&expected_checksums, false);
        if let Some(algo) = trailer_algorithm {
            checksummer = checksummer.add_algo(Some(algo));
        }

        // Create channel for streaming checksum data
        let (checksum_tx, mut checksum_rx) = mpsc::unbounded_channel::<ChecksumMessage>();

        // Spawn checksum calculation task
        let checksum_handle = tokio::spawn(async move {
            let final_expected = expected_checksums;
            let mut trailer_checksums: Option<S3Trailers> = None;

            // Process messages for checksum calculation
            let mut total_bytes = 0;
            while let Some(msg) = checksum_rx.recv().await {
                match msg {
                    ChecksumMessage::Data(data) => {
                        total_bytes += data.len();
                        tracing::trace!(
                            "Updating checksummer with {} bytes: {:?}",
                            data.len(),
                            std::str::from_utf8(&data).unwrap_or("<non-utf8>")
                        );
                        checksummer.update(&data);
                    }
                    ChecksumMessage::Trailers(trailers) => {
                        tracing::trace!("Received trailer checksums: {:?}", trailers);
                        trailer_checksums = Some(trailers);
                    }
                }
            }
            tracing::debug!("Total bytes processed for checksum: {}", total_bytes);

            // Finalize checksums
            let checksums = checksummer.finalize();

            // Debug: log calculated checksums
            if let Some(crc32) = checksums.crc32 {
                tracing::debug!(
                    "Calculated CRC32: {:?} (base64: {})",
                    crc32,
                    base64::prelude::BASE64_STANDARD.encode(crc32)
                );
            }
            if let Some(crc32c) = checksums.crc32c {
                tracing::debug!(
                    "Calculated CRC32C: {:?} (base64: {})",
                    crc32c,
                    base64::prelude::BASE64_STANDARD.encode(crc32c)
                );
            }
            if let Some(sha1) = checksums.sha1 {
                tracing::debug!(
                    "Calculated SHA1: {:?} (base64: {})",
                    sha1,
                    base64::prelude::BASE64_STANDARD.encode(sha1)
                );
            }
            if let Some(sha256) = checksums.sha256 {
                tracing::debug!(
                    "Calculated SHA256: {:?} (base64: {})",
                    sha256,
                    base64::prelude::BASE64_STANDARD.encode(sha256)
                );
            }

            // Verify trailer checksums if present
            if let Some(trailers) = trailer_checksums {
                // Verify CRC32
                if let Some(trailer_crc32) = trailers.checksum_crc32 {
                    if let Some(calculated_crc32) = checksums.crc32 {
                        let calculated_b64 =
                            base64::prelude::BASE64_STANDARD.encode(calculated_crc32);
                        if trailer_crc32 != calculated_b64 {
                            tracing::error!(
                                "CRC32 checksum mismatch: trailer={}, calculated={}",
                                trailer_crc32,
                                calculated_b64
                            );
                            return Err(S3Error::InvalidDigest);
                        }
                    }
                }

                // Verify CRC32C
                if let Some(trailer_crc32c) = trailers.checksum_crc32c {
                    if let Some(calculated_crc32c) = checksums.crc32c {
                        let calculated_b64 =
                            base64::prelude::BASE64_STANDARD.encode(calculated_crc32c);
                        if trailer_crc32c != calculated_b64 {
                            tracing::error!(
                                "CRC32C checksum mismatch: trailer={}, calculated={}",
                                trailer_crc32c,
                                calculated_b64
                            );
                            return Err(S3Error::InvalidDigest);
                        }
                    }
                }

                // Verify SHA1
                if let Some(trailer_sha1) = trailers.checksum_sha1 {
                    if let Some(calculated_sha1) = checksums.sha1 {
                        let calculated_b64 =
                            base64::prelude::BASE64_STANDARD.encode(calculated_sha1);
                        if trailer_sha1 != calculated_b64 {
                            tracing::error!(
                                "SHA1 checksum mismatch: trailer={}, calculated={}",
                                trailer_sha1,
                                calculated_b64
                            );
                            return Err(S3Error::InvalidDigest);
                        }
                    }
                }

                // Verify SHA256
                if let Some(trailer_sha256) = trailers.checksum_sha256 {
                    if let Some(calculated_sha256) = checksums.sha256 {
                        let calculated_b64 =
                            base64::prelude::BASE64_STANDARD.encode(calculated_sha256);
                        if trailer_sha256 != calculated_b64 {
                            tracing::error!(
                                "SHA256 checksum mismatch: trailer={}, calculated={}",
                                trailer_sha256,
                                calculated_b64
                            );
                            return Err(S3Error::InvalidDigest);
                        }
                    }
                }

                tracing::debug!("All trailer checksums verified successfully");
            }

            checksums.verify(&final_expected).map_err(|e| {
                tracing::error!("Checksum verification failed: {:?}", e);
                S3Error::InvalidDigest
            })?;

            Ok(checksums)
        });

        // Choose initial state based on whether this is a streaming request
        let initial_state = if is_streaming {
            S3ChunkState::ReadingChunkSize
        } else {
            // For regular content-length requests, we pass data through directly
            S3ChunkState::ReadingChunkData {
                remaining: usize::MAX,
            }
        };

        let streaming_payload = Self {
            inner: payload,
            state: initial_state,
            trailer_buffer: BytesMut::new(),
            checksum_sender: Some(checksum_tx),
        };

        Ok((streaming_payload, checksum_handle))
    }

    /// Extract expected checksums from request headers
    fn extract_expected_checksums(request: &HttpRequest) -> Result<ExpectedChecksums, S3Error> {
        let extra_checksum = request_checksum_value(request.headers()).map_err(|e| {
            tracing::error!("Failed to extract checksum value: {:?}", e);
            S3Error::InvalidDigest
        })?;

        // Extract MD5 from Content-MD5 header
        let md5 = request
            .headers()
            .get("content-md5")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        // Extract SHA256 from x-amz-content-sha256 header
        let sha256 = request
            .headers()
            .get("x-amz-content-sha256")
            .and_then(|v| v.to_str().ok())
            .filter(|s| *s != "UNSIGNED-PAYLOAD" && *s != "STREAMING-AWS4-HMAC-SHA256-PAYLOAD")
            .and_then(|s| {
                hex::decode(s).ok().and_then(|bytes| {
                    let array: Result<[u8; 32], _> = bytes.try_into();
                    array.ok().map(Hash::from)
                })
            });

        Ok(ExpectedChecksums {
            md5,
            sha256,
            extra: extra_checksum,
        })
    }

    /// Extract trailer algorithm from request headers
    fn extract_trailer_algorithm(
        request: &HttpRequest,
    ) -> Result<Option<ChecksumAlgorithm>, S3Error> {
        request_trailer_checksum_algorithm(request.headers()).map_err(|e| {
            tracing::error!("Failed to extract trailer algorithm: {:?}", e);
            S3Error::InvalidDigest
        })
    }

    /// Determine if this is a streaming request (S3 chunked transfer encoding) or regular content-length
    fn is_streaming_request(request: &HttpRequest) -> bool {
        // S3 streaming requests have these characteristics:
        // 1. x-amz-decoded-content-length header (the actual payload size)
        // 2. x-amz-trailer header (indicating trailers will be sent)
        // 3. content-encoding header might be present
        // 4. The content-length is the size of the chunked encoding, not the payload

        let headers = request.headers();

        // Check for decoded content length - this is the strongest indicator
        if headers.contains_key("x-amz-decoded-content-length") {
            return true;
        }

        // Check for trailer header
        if headers.contains_key("x-amz-trailer") {
            return true;
        }

        // Check for streaming signature in content-sha256
        if let Some(content_sha256) = headers.get("x-amz-content-sha256") {
            if let Ok(value) = content_sha256.to_str() {
                if value == "STREAMING-UNSIGNED-PAYLOAD-TRAILER" || value.starts_with("STREAMING-")
                {
                    return true;
                }
            }
        }

        false
    }
}

impl Stream for S3StreamingPayload {
    type Item = Result<Bytes, PayloadError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.as_mut().project();

        loop {
            match this.state.clone() {
                S3ChunkState::ReadingChunkSize => {
                    match ready!(this.inner.as_mut().poll_next(cx)) {
                        Some(Ok(data)) => {
                            this.trailer_buffer.extend_from_slice(&data);

                            // Look for chunk size line ending with \r\n
                            if let Some(line_end) =
                                this.trailer_buffer.windows(2).position(|w| w == b"\r\n")
                            {
                                let chunk_line = this.trailer_buffer.split_to(line_end + 2);
                                let chunk_line_str =
                                    match std::str::from_utf8(&chunk_line[..line_end]) {
                                        Ok(s) => s,
                                        Err(_) => {
                                            tracing::error!("Invalid UTF-8 in chunk size line");
                                            return Poll::Ready(Some(Err(
                                                PayloadError::EncodingCorrupted,
                                            )));
                                        }
                                    };

                                // Parse chunk size (hex), ignoring chunk extensions
                                let chunk_size_hex =
                                    chunk_line_str.split(';').next().unwrap_or("").trim();
                                match usize::from_str_radix(chunk_size_hex, 16) {
                                    Ok(0) => {
                                        // Zero-sized chunk means end of chunks, start reading trailers
                                        *this.state = S3ChunkState::ReadingTrailers;
                                        continue;
                                    }
                                    Ok(size) => {
                                        *this.state =
                                            S3ChunkState::ReadingChunkData { remaining: size };
                                        continue;
                                    }
                                    Err(e) => {
                                        tracing::error!(
                                            "Failed to parse chunk size '{}': {}",
                                            chunk_size_hex,
                                            e
                                        );
                                        return Poll::Ready(Some(Err(
                                            PayloadError::EncodingCorrupted,
                                        )));
                                    }
                                }
                            }
                            // Not enough data yet, keep reading
                            continue;
                        }
                        Some(Err(e)) => return Poll::Ready(Some(Err(e))),
                        None => {
                            // Stream ended - this could be temporary or permanent
                            // If we have some buffered data, try to process it
                            if !this.trailer_buffer.is_empty() {
                                // Check if we have a complete chunk size line
                                if let Some(line_end) =
                                    this.trailer_buffer.windows(2).position(|w| w == b"\r\n")
                                {
                                    let chunk_line = this.trailer_buffer.split_to(line_end + 2);
                                    let chunk_line_str =
                                        match std::str::from_utf8(&chunk_line[..line_end]) {
                                            Ok(s) => s,
                                            Err(_) => {
                                                tracing::error!("Invalid UTF-8 in chunk size line");
                                                return Poll::Ready(Some(Err(
                                                    PayloadError::EncodingCorrupted,
                                                )));
                                            }
                                        };

                                    let chunk_size_hex =
                                        chunk_line_str.split(';').next().unwrap_or("").trim();
                                    match usize::from_str_radix(chunk_size_hex, 16) {
                                        Ok(0) => {
                                            *this.state = S3ChunkState::ReadingTrailers;
                                            continue;
                                        }
                                        Ok(size) => {
                                            *this.state =
                                                S3ChunkState::ReadingChunkData { remaining: size };
                                            continue;
                                        }
                                        Err(e) => {
                                            tracing::error!(
                                                "Failed to parse final chunk size '{}': {}",
                                                chunk_size_hex,
                                                e
                                            );
                                            return Poll::Ready(Some(Err(
                                                PayloadError::EncodingCorrupted,
                                            )));
                                        }
                                    }
                                } else {
                                    // Try to treat remaining buffer as trailers
                                    *this.state = S3ChunkState::ReadingTrailers;
                                    continue;
                                }
                            } else {
                                // End of stream with no buffered data - this is normal completion
                                if let Some(sender) = this.checksum_sender.take() {
                                    drop(sender);
                                }
                                *this.state = S3ChunkState::Done;
                                return Poll::Ready(None);
                            }
                        }
                    }
                }
                S3ChunkState::ReadingChunkData { remaining } => {
                    // Special case: if remaining is usize::MAX, we're in pass-through mode (non-streaming)
                    if remaining == usize::MAX {
                        // Pass through all data directly without chunk parsing
                        match ready!(this.inner.as_mut().poll_next(cx)) {
                            Some(Ok(data)) => {
                                // Send data to checksum calculator
                                if let Some(ref sender) = this.checksum_sender {
                                    let _ = sender.send(ChecksumMessage::Data(data.clone()));
                                }
                                return Poll::Ready(Some(Ok(data)));
                            }
                            Some(Err(e)) => return Poll::Ready(Some(Err(e))),
                            None => {
                                // End of stream in pass-through mode
                                if let Some(sender) = this.checksum_sender.take() {
                                    drop(sender);
                                }
                                *this.state = S3ChunkState::Done;
                                return Poll::Ready(None);
                            }
                        }
                    }

                    if remaining == 0 {
                        *this.state = S3ChunkState::ReadingChunkEnd;
                        continue;
                    }

                    // Check if we have buffered data
                    if !this.trailer_buffer.is_empty() {
                        let to_take = std::cmp::min(remaining, this.trailer_buffer.len());
                        let chunk_data = this.trailer_buffer.split_to(to_take);

                        // Send data to checksum calculator
                        if let Some(ref sender) = this.checksum_sender {
                            let _ = sender.send(ChecksumMessage::Data(chunk_data.clone().into()));
                        }

                        *this.state = S3ChunkState::ReadingChunkData {
                            remaining: remaining - to_take,
                        };
                        return Poll::Ready(Some(Ok(chunk_data.into())));
                    }

                    // Read more data from the stream
                    match ready!(this.inner.as_mut().poll_next(cx)) {
                        Some(Ok(data)) => {
                            let to_take = std::cmp::min(remaining, data.len());
                            let chunk_data = data.slice(0..to_take);
                            let leftover = data.slice(to_take..);

                            // Buffer any leftover data
                            if !leftover.is_empty() {
                                this.trailer_buffer.extend_from_slice(&leftover);
                            }

                            // Send data to checksum calculator
                            if let Some(ref sender) = this.checksum_sender {
                                let _ = sender.send(ChecksumMessage::Data(chunk_data.clone()));
                            }

                            *this.state = S3ChunkState::ReadingChunkData {
                                remaining: remaining - to_take,
                            };
                            return Poll::Ready(Some(Ok(chunk_data)));
                        }
                        Some(Err(e)) => return Poll::Ready(Some(Err(e))),
                        None => {
                            tracing::error!("Unexpected end of stream while reading chunk data");
                            return Poll::Ready(Some(Err(PayloadError::Incomplete(None))));
                        }
                    }
                }
                S3ChunkState::ReadingChunkEnd => {
                    // Read the \r\n after chunk data
                    if this.trailer_buffer.len() < 2 {
                        match ready!(this.inner.as_mut().poll_next(cx)) {
                            Some(Ok(data)) => {
                                this.trailer_buffer.extend_from_slice(&data);
                            }
                            Some(Err(e)) => return Poll::Ready(Some(Err(e))),
                            None => {
                                tracing::error!("Unexpected end of stream while reading chunk end");
                                return Poll::Ready(Some(Err(PayloadError::Incomplete(None))));
                            }
                        }
                    }

                    if this.trailer_buffer.len() >= 2 {
                        let chunk_end = this.trailer_buffer.split_to(2);
                        if &chunk_end[..] != b"\r\n" {
                            tracing::error!(
                                "Expected \\r\\n after chunk data, got: {:?}",
                                chunk_end
                            );
                            return Poll::Ready(Some(Err(PayloadError::EncodingCorrupted)));
                        }
                        *this.state = S3ChunkState::ReadingChunkSize;
                        continue;
                    }
                }
                S3ChunkState::ReadingTrailers => {
                    // Read until we find \r\n\r\n (end of trailers)
                    match ready!(this.inner.as_mut().poll_next(cx)) {
                        Some(Ok(data)) => {
                            this.trailer_buffer.extend_from_slice(&data);

                            // Look for end of trailers
                            if let Some(end_pos) = this
                                .trailer_buffer
                                .windows(4)
                                .position(|w| w == b"\r\n\r\n")
                            {
                                let trailer_data = this.trailer_buffer.split_to(end_pos);
                                this.trailer_buffer.advance(4); // Skip the \r\n\r\n

                                // Parse trailers for S3 checksums
                                let trailers = parse_s3_trailers(&trailer_data);
                                tracing::debug!("Parsed S3 trailers: {:?}", trailers);

                                // Send trailer checksums through the checksum channel for verification
                                // The checksummer task will handle the verification
                                if let Some(sender) = &this.checksum_sender {
                                    let _ =
                                        sender.send(ChecksumMessage::Trailers(trailers.clone()));
                                }

                                // Close checksum channel
                                if let Some(sender) = this.checksum_sender.take() {
                                    drop(sender);
                                }

                                *this.state = S3ChunkState::Done;
                                return Poll::Ready(None);
                            }
                            // Keep reading for more trailer data
                            continue;
                        }
                        Some(Err(e)) => return Poll::Ready(Some(Err(e))),
                        None => {
                            // End of stream, close checksum channel
                            if let Some(sender) = this.checksum_sender.take() {
                                drop(sender);
                            }
                            *this.state = S3ChunkState::Done;
                            return Poll::Ready(None);
                        }
                    }
                }
                S3ChunkState::Done => return Poll::Ready(None),
            }
        }
    }
}

/// Parse S3 trailers from HTTP trailer section
/// Format: "x-amz-checksum-sha256: base64value\r\n"
fn parse_s3_trailers(trailer_data: &[u8]) -> S3Trailers {
    let mut trailers = S3Trailers::default();

    // Split trailer data into lines
    let trailer_str = match std::str::from_utf8(trailer_data) {
        Ok(s) => s,
        Err(_) => {
            tracing::warn!("Invalid UTF-8 in trailer data");
            return trailers;
        }
    };

    for line in trailer_str.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        // Parse "header: value" format
        if let Some((name, value)) = line.split_once(':') {
            let name = name.trim().to_lowercase();
            let value = value.trim();

            match name.as_str() {
                "x-amz-checksum-crc32" => trailers.checksum_crc32 = Some(value.to_string()),
                "x-amz-checksum-crc32c" => trailers.checksum_crc32c = Some(value.to_string()),
                "x-amz-checksum-sha1" => trailers.checksum_sha1 = Some(value.to_string()),
                "x-amz-checksum-sha256" => trailers.checksum_sha256 = Some(value.to_string()),
                "x-amz-trailer-signature" => trailers.trailer_signature = Some(value.to_string()),
                _ => {
                    // Ignore unknown trailer headers
                    tracing::debug!("Unknown trailer header: {}", name);
                }
            }
        }
    }

    trailers
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_s3_trailers() {
        let trailer_data =
            b"x-amz-checksum-sha256: YWJjZGVmZ2g=\r\nx-amz-trailer-signature: signature123\r\n";
        let trailers = parse_s3_trailers(trailer_data);

        assert_eq!(trailers.checksum_sha256, Some("YWJjZGVmZ2g=".to_string()));
        assert_eq!(trailers.trailer_signature, Some("signature123".to_string()));
        assert!(trailers.checksum_crc32.is_none());
    }
}
