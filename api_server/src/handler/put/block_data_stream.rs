use axum::Error;
use bytes::{BufMut, Bytes, BytesMut};
use futures::{ready, Stream};
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
        buffer: BytesMut,
        block_size: usize,
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
            buffer: BytesMut::with_capacity(block_size),
            block_size,
        }
    }
}

impl<S, E> Stream for BlockDataStream<S, E>
where
    S: Stream<Item = Result<Bytes, E>>,
    E: Into<Error>,
{
    // The stream now yields a Result to propagate potential errors.
    type Item = Result<Bytes, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.as_mut().project();

        loop {
            // 1. If the buffer already contains a full block, yield it immediately.
            // This is more efficient than polling the underlying stream again if it's not necessary.
            if this.buffer.len() >= *this.block_size {
                let block = this.buffer.split_to(*this.block_size).freeze();
                // Ensure enough capacity for the next block, if needed.
                // BytesMut's reserve is additive and will only allocate if necessary.
                this.buffer.reserve(*this.block_size);
                return Poll::Ready(Some(Ok(block)));
            }

            // 2. Otherwise, poll the underlying stream for more data.
            match ready!(this.stream.as_mut().poll_next(cx)) {
                // A chunk of data arrived.
                Some(Ok(data)) => {
                    // Reserve additional capacity if the incoming chunk is large.
                    // This prevents multiple small reallocations inside `put()`.
                    this.buffer.reserve(data.len());
                    this.buffer.put(data);
                    // The loop will continue, checking if the buffer is now large enough.
                }

                // An error occurred in the underlying stream.
                Some(Err(e)) => {
                    // Propagate the error, converting it to the required type.
                    return Poll::Ready(Some(Err(e.into())));
                }

                // The underlying stream has ended.
                None => {
                    // If the buffer is empty, we're done.
                    if this.buffer.is_empty() {
                        return Poll::Ready(None);
                    }
                    // Otherwise, yield the final, possibly smaller, block of data.
                    else {
                        let last_block = std::mem::take(this.buffer).freeze();
                        return Poll::Ready(Some(Ok(last_block)));
                    }
                }
            }
        }
    }
}
