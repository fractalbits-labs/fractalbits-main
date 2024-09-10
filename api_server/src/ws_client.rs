use bytes::Buf;
use bytes::Bytes;
use std::collections::HashMap;
use std::io;
use std::mem::size_of;
use std::sync::atomic::AtomicU32;
use std::sync::Arc;
use thiserror::Error;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::{
    self,
    io::{ReadHalf, WriteHalf},
    net::TcpStream,
    sync::{oneshot, RwLock},
};

use crate::message::MessageHeader;

#[derive(Error, Debug)]
#[error(transparent)]
pub enum WebSocketError {
    #[error("server response with redirect")]
    Redirect,
    #[error("server rejected with status code: {0}")]
    RejectedWithStatusCode(u16),
    HandShakeError(soketto::handshake::Error),
    ConnectionErr(soketto::connection::Error),
    OneshotRecvError(oneshot::error::RecvError),
    EncodeError(prost::EncodeError),
    DecodeError(prost::DecodeError),
    #[error("internal request sending error: {0}")]
    InternalRequestError(String),
    #[error("internal response sending error: {0}")]
    InternalResponseError(String),
}

impl From<io::Error> for WebSocketError {
    fn from(err: io::Error) -> Self {
        WebSocketError::HandShakeError(soketto::handshake::Error::Io(err))
    }
}

pub struct RpcClient {
    requests: Arc<RwLock<HashMap<u32, oneshot::Sender<Bytes>>>>,
    sender: tokio::sync::mpsc::Sender<Bytes>,
    next_id: AtomicU32,
}

impl RpcClient {
    pub async fn new(url: &str) -> Result<Self, WebSocketError> {
        let stream = TcpStream::connect(url).await?;
        stream.set_nodelay(true)?;
        let (receiver, sender) = tokio::io::split(stream);

        // Start message receiver task, for rpc responses
        let requests = Arc::new(RwLock::new(HashMap::new()));
        {
            let requests_clone = requests.clone();
            tokio::spawn(async move {
                if let Err(e) = Self::receive_message_task(receiver, requests_clone).await {
                    tracing::error!("FATAL: receive message task error: {e:?}");
                }
            });
        }

        // Start message sender task, to send rpc requests. We are launching a dedicated task here
        // to reduce lock contention on the sender socket itself.
        let (tx, rx) = tokio::sync::mpsc::channel(1024);
        {
            tokio::spawn(async move {
                if let Err(e) = Self::send_message_task(sender, rx).await {
                    tracing::error!("FATAL: receive message task error: {e:?}");
                }
            });
        }

        Ok(Self {
            requests,
            sender: tx,
            next_id: AtomicU32::new(1),
        })
    }

    async fn receive_message_task(
        mut receiver: ReadHalf<TcpStream>,
        requests: Arc<RwLock<HashMap<u32, oneshot::Sender<Bytes>>>>,
    ) -> Result<(), WebSocketError> {
        loop {
            let mut buffer = [0; 1024];
            let n = receiver.read(&mut buffer).await.unwrap();
            if n == 0 {
                // socket is closed
                break Ok(());
            }
            let mut bytes = Bytes::copy_from_slice(&buffer[0..n]);
            let request_id = RpcClient::extract_request_id_from_header(&mut bytes)?;
            let tx: oneshot::Sender<Bytes> = match requests.write().await.remove(&request_id) {
                Some(tx) => tx,
                None => continue, // we may have received the response already
            };
            let _ = tx.send(bytes);
        }
    }

    async fn send_message_task(
        mut sender: WriteHalf<TcpStream>,
        mut input: tokio::sync::mpsc::Receiver<Bytes>,
    ) -> Result<(), WebSocketError> {
        while let Some(message) = input.recv().await {
            sender.write(message.as_ref()).await.unwrap();
        }
        Ok(())
    }

    pub async fn send_request(&self, id: u32, msg: Bytes) -> Result<Bytes, WebSocketError> {
        self.sender
            .send(msg)
            .await
            .map_err(|e| WebSocketError::InternalRequestError(e.to_string()))?;
        tracing::info!("request sent from handler: request_id={id}");

        let (tx, rx) = oneshot::channel();
        self.requests.write().await.insert(id, tx);

        rx.await.map_err(WebSocketError::OneshotRecvError)
    }

    pub fn gen_request_id(&self) -> u32 {
        let request_id = self.next_id.load(std::sync::atomic::Ordering::SeqCst);
        self.next_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        request_id
    }

    #[allow(unused)]
    fn extract_request_id_from_header(buf: &mut impl Buf) -> Result<u32, WebSocketError> {
        let offset = std::mem::offset_of!(MessageHeader, id);
        buf.advance(offset);
        let id = buf.get_u32_le();
        buf.advance(size_of::<MessageHeader>() - offset - size_of::<u32>());
        Ok(id)
    }

    #[allow(unused)]
    fn extract_request_id_from_body(buf: &mut impl Buf) -> Result<u32, WebSocketError> {
        let (tag, wire_type) =
            prost::encoding::decode_key(buf).map_err(WebSocketError::DecodeError)?;
        assert_eq!(1, tag);
        assert_eq!(prost::encoding::WireType::Varint, wire_type);
        prost::encoding::decode_varint(buf)
            .map(|i| i as u32)
            .map_err(WebSocketError::DecodeError)
    }
}
