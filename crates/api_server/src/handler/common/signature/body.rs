use bytes::Bytes;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use http_body::Frame;
use http_body_util::StreamBody;
use sync_wrapper::SyncWrapper;
use tokio::sync::mpsc;
use tokio::task;

use super::*;
use crate::handler::common::checksum::{
    ChecksumAlgorithm, ChecksumValue, Checksummer, verify_checksum,
};

pub struct ReqBody {
    pub(crate) stream: SyncWrapper<BoxStream<'static, Result<Frame<Bytes>, SignatureError>>>,
    pub(crate) checksummer: Option<Checksummer>,
    pub(crate) expected_checksum: Option<ChecksumValue>,
    pub(crate) trailer_algorithm: Option<ChecksumAlgorithm>,
}

pub type StreamingChecksumReceiver =
    task::JoinHandle<Result<Option<ChecksumValue>, SignatureError>>;

impl From<axum::body::Body> for ReqBody {
    fn from(body: axum::body::Body) -> Self {
        let stream = http_body_util::BodyStream::new(body).map_err(super::SignatureError::from);
        Self {
            stream: SyncWrapper::new(stream.boxed()),
            checksummer: None,
            expected_checksum: None,
            trailer_algorithm: None,
        }
    }
}

impl ReqBody {
    pub fn set_expected_checksum(&mut self, checksum: ChecksumValue) {
        self.expected_checksum = Some(checksum);
        self.checksummer = Some(Checksummer::new(checksum.algorithm()));
    }

    pub fn set_checksum_algorithm(&mut self, algo: ChecksumAlgorithm) {
        self.checksummer = Some(Checksummer::new(algo));
    }

    // ============ non-streaming =============

    pub async fn collect(self) -> Result<Bytes, SignatureError> {
        self.collect_with_checksums().await.map(|(b, _)| b)
    }

    pub async fn collect_with_checksums(
        self,
    ) -> Result<(Bytes, Option<ChecksumValue>), SignatureError> {
        let stream: BoxStream<_> = self.stream.into_inner();
        let bytes = http_body_util::BodyExt::collect(StreamBody::new(stream))
            .await?
            .to_bytes();

        let calculated_checksum = if let Some(mut checksummer) = self.checksummer {
            checksummer.update(&bytes);
            Some(checksummer.finalize())
        } else {
            None
        };

        verify_checksum(calculated_checksum, self.expected_checksum)?;

        Ok((bytes, calculated_checksum))
    }

    // ============ streaming =============

    pub fn streaming_with_checksums(
        self,
    ) -> (
        BoxStream<'static, Result<Bytes, SignatureError>>,
        StreamingChecksumReceiver,
    ) {
        let Self {
            stream,
            mut checksummer,
            expected_checksum,
            trailer_algorithm: _trailer_algorithm,
        } = self;

        let (frame_tx, mut frame_rx) = mpsc::channel::<Frame<Bytes>>(5);

        let join_checksums = tokio::spawn(async move {
            while let Some(frame) = frame_rx.recv().await {
                match frame.into_data() {
                    Ok(data) => {
                        if let Some(ref mut checksummer) = checksummer {
                            checksummer.update(&data);
                        }
                    }
                    Err(_frame) => {
                        // Handle trailers if needed
                        break;
                    }
                }
            }

            let calculated_checksum = checksummer.map(|c| c.finalize());
            verify_checksum(calculated_checksum, expected_checksum)?;

            Ok(calculated_checksum)
        });

        let stream: BoxStream<_> = stream.into_inner();
        let stream = stream.filter_map(move |x| {
            let frame_tx = frame_tx.clone();
            async move {
                match x {
                    Err(e) => Some(Err(e)),
                    Ok(frame) => {
                        if frame.is_data() {
                            let data = frame.data_ref().unwrap().clone();
                            let _ = frame_tx.send(frame).await;
                            Some(Ok(data))
                        } else {
                            let _ = frame_tx.send(frame).await;
                            None
                        }
                    }
                }
            }
        });

        (stream.boxed(), join_checksums)
    }
}
