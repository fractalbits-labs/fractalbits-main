use bytes::Bytes;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use http_body::Frame;
use http_body_util::StreamBody;
use serde::{Deserialize, Serialize};
use sync_wrapper::SyncWrapper;
use tokio::sync::mpsc;
use tokio::task;

use super::checksum::*;
use super::*;

pub struct ReqBody {
    pub(crate) stream: SyncWrapper<BoxStream<'static, Result<Frame<Bytes>, Error>>>,
    pub(crate) checksummer: Checksummer,
    pub(crate) expected_checksums: ExpectedChecksums,
    pub(crate) trailer_algorithm: Option<ChecksumAlgorithm>,
}

pub type StreamingChecksumReceiver = task::JoinHandle<Result<Checksums, Error>>;

impl ReqBody {
    /// Create a ReqBody from any http-body compatible body type
    pub fn from_http_body<B>(body: B) -> Self
    where
        B: http_body::Body<Data = bytes::Bytes> + Send + 'static,
        B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
        super::Error: From<B::Error>,
    {
        let expected_checksums = ExpectedChecksums::default();
        let checksummer = Checksummer::init(&expected_checksums, false);

        let stream = http_body_util::BodyStream::new(body).map_err(super::Error::from);
        Self {
            stream: SyncWrapper::new(stream.boxed()),
            checksummer,
            expected_checksums,
            trailer_algorithm: None,
        }
    }

    /// Create an empty ReqBody
    pub fn empty() -> Self {
        Self::from_http_body(http_body_util::Empty::<bytes::Bytes>::new())
    }

    pub fn add_expected_checksums(&mut self, more: ExpectedChecksums) {
        if more.md5.is_some() {
            self.expected_checksums.md5 = more.md5;
        }
        if more.sha256.is_some() {
            self.expected_checksums.sha256 = more.sha256;
        }
        if more.extra.is_some() {
            self.expected_checksums.extra = more.extra;
        }
        self.checksummer.add_expected(&self.expected_checksums);
    }

    pub fn add_md5(&mut self) {
        self.checksummer.add_md5();
    }

    // ============ non-streaming =============

    pub async fn collect(self) -> Result<Bytes, Error> {
        self.collect_with_checksums().await.map(|(b, _)| b)
    }

    pub async fn collect_with_checksums(mut self) -> Result<(Bytes, Checksums), Error> {
        let stream: BoxStream<_> = self.stream.into_inner();
        let bytes = http_body_util::BodyExt::collect(StreamBody::new(stream))
            .await?
            .to_bytes();

        self.checksummer.update(&bytes);
        let checksums = self.checksummer.finalize();
        checksums.verify(&self.expected_checksums)?;

        Ok((bytes, checksums))
    }

    // ============ streaming =============

    pub fn streaming_with_checksums(
        self,
    ) -> (
        BoxStream<'static, Result<Bytes, Error>>,
        StreamingChecksumReceiver,
    ) {
        let Self {
            stream,
            mut checksummer,
            mut expected_checksums,
            trailer_algorithm,
        } = self;

        let (frame_tx, mut frame_rx) = mpsc::channel::<Frame<Bytes>>(5);

        let join_checksums = tokio::spawn(async move {
            while let Some(frame) = frame_rx.recv().await {
                match frame.into_data() {
                    Ok(data) => {
                        checksummer = tokio::task::spawn_blocking(move || {
                            checksummer.update(&data);
                            checksummer
                        })
                        .await
                        .unwrap()
                    }
                    Err(frame) => {
                        let trailers = frame.into_trailers().unwrap();
                        let algo = trailer_algorithm.unwrap();
                        // Convert http::HeaderMap to actix_web::http::header::HeaderMap for checksum processing
                        let mut actix_trailers = actix_web::http::header::HeaderMap::new();
                        for (name, value) in trailers.iter() {
                            if let (Ok(actix_name), Ok(actix_value)) = (
                                actix_web::http::header::HeaderName::from_bytes(
                                    name.as_str().as_bytes(),
                                ),
                                actix_web::http::header::HeaderValue::from_bytes(value.as_bytes()),
                            ) {
                                actix_trailers.insert(actix_name, actix_value);
                            }
                        }
                        expected_checksums.extra =
                            Some(extract_checksum_value(&actix_trailers, algo)?);
                        break;
                    }
                }
            }

            if trailer_algorithm.is_some() && expected_checksums.extra.is_none() {
                return Err(Error::Other("trailing checksum was not sent".into()));
            }

            let checksums = checksummer.finalize();
            checksums.verify(&expected_checksums)?;

            Ok(checksums)
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
