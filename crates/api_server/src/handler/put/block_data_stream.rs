use axum::Error;
use bytes::{BufMut, Bytes, BytesMut};
use futures::{Stream, ready};
use pin_project_lite::pin_project;
use std::pin::Pin;
use std::task::{Context, Poll};

pin_project! {
    /// A stream that wraps another stream of `Bytes` and yields them in fixed-size blocks.
    pub struct BlockDataStream<S, E>
    where
        S: Stream<Item = Result<Bytes, E>>,
        E: Into<Error>,
    {
        #[pin]
        stream: S,
        chunks: Vec<Bytes>,
        accumulated_size: usize,
        block_size: usize,
        overflow: Option<Bytes>,
    }
}

impl<S, E> BlockDataStream<S, E>
where
    S: Stream<Item = Result<Bytes, E>>,
    E: Into<Error>,
{
    pub fn new(stream: S, block_size: u32) -> Self {
        assert!(block_size > 0, "Block size must be greater than 0");
        let block_size = block_size as usize;

        Self {
            stream,
            chunks: Vec::new(),
            accumulated_size: 0,
            block_size,
            overflow: None,
        }
    }
}

impl<S, E> Stream for BlockDataStream<S, E>
where
    S: Stream<Item = Result<Bytes, E>>,
    E: Into<Error>,
{
    type Item = Result<Bytes, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.as_mut().project();

        loop {
            if let Some(overflow_data) = this.overflow.take() {
                let remaining = *this.block_size - *this.accumulated_size;
                if overflow_data.len() <= remaining {
                    *this.accumulated_size += overflow_data.len();
                    this.chunks.push(overflow_data);
                } else {
                    this.chunks.push(overflow_data.slice(0..remaining));
                    *this.accumulated_size += remaining;
                    *this.overflow = Some(overflow_data.slice(remaining..));
                }

                if *this.accumulated_size == *this.block_size {
                    let chunks = std::mem::take(this.chunks);
                    *this.accumulated_size = 0;
                    let mut merged = BytesMut::with_capacity(chunks.iter().map(|c| c.len()).sum());
                    for chunk in chunks {
                        merged.put(chunk);
                    }
                    return Poll::Ready(Some(Ok(merged.freeze())));
                }
            }

            if *this.accumulated_size == *this.block_size {
                let chunks = std::mem::take(this.chunks);
                *this.accumulated_size = 0;
                let mut merged = BytesMut::with_capacity(chunks.iter().map(|c| c.len()).sum());
                for chunk in chunks {
                    merged.put(chunk);
                }
                return Poll::Ready(Some(Ok(merged.freeze())));
            }

            match ready!(this.stream.as_mut().poll_next(cx)) {
                Some(Ok(data)) => {
                    if data.is_empty() {
                        continue;
                    }

                    let remaining = *this.block_size - *this.accumulated_size;
                    if data.len() <= remaining {
                        *this.accumulated_size += data.len();
                        this.chunks.push(data);
                    } else {
                        this.chunks.push(data.slice(0..remaining));
                        *this.accumulated_size += remaining;
                        *this.overflow = Some(data.slice(remaining..));
                    }
                }

                Some(Err(e)) => {
                    return Poll::Ready(Some(Err(e.into())));
                }

                None => {
                    if *this.accumulated_size == 0 {
                        return Poll::Ready(None);
                    } else {
                        let chunks = std::mem::take(this.chunks);
                        *this.accumulated_size = 0;
                        let mut merged =
                            BytesMut::with_capacity(chunks.iter().map(|c| c.len()).sum());
                        for chunk in chunks {
                            merged.put(chunk);
                        }
                        return Poll::Ready(Some(Ok(merged.freeze())));
                    }
                }
            }
        }
    }
}
