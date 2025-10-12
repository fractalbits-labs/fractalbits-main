use bytes::Bytes;
use rpc_codec_common::{MessageFrame, MessageHeaderTrait};
use std::io;
use std::os::fd::RawFd;

#[allow(async_fn_in_trait)]
pub trait IoUringTransport: Send + Sync + Clone + 'static {
    async fn send(&self, fd: RawFd, header: Bytes, body: Bytes) -> io::Result<usize>;
    async fn recv_frame<H: MessageHeaderTrait>(&self, fd: RawFd) -> io::Result<MessageFrame<H>>;
    fn name(&self) -> &'static str;
}
