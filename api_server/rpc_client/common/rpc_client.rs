#[cfg(feature = "rss")]
use bucket_tables::table::{KvClient, Versioned};
use bytes::{Bytes, BytesMut};
use std::collections::HashMap;
use std::io::{self};
use std::sync::atomic::AtomicU32;
use std::sync::Arc;
use thiserror::Error;
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::{
    self,
    net::tcp::{OwnedReadHalf, OwnedWriteHalf},
    net::TcpStream,
    sync::{oneshot, RwLock},
};
use tokio_stream::StreamExt;
use tokio_util::codec::FramedRead;

use crate::codec::{MessageFrame, MesssageCodec};
use crate::message::MessageHeader;

#[derive(Error, Debug)]
pub enum RpcError {
    #[error(transparent)]
    IoError(#[from] io::Error),
    #[error(transparent)]
    OneshotRecvError(oneshot::error::RecvError),
    #[cfg(any(feature = "nss", feature = "rss"))]
    #[error(transparent)]
    EncodeError(prost::EncodeError),
    #[cfg(any(feature = "nss", feature = "rss"))]
    #[error(transparent)]
    DecodeError(prost::DecodeError),
    #[error("Internal request sending error: {0}")]
    InternalRequestError(String),
    #[error("Internal response error: {0}")]
    InternalResponseError(String),
    #[error("Entry not found")]
    NotFound,
    #[cfg(feature = "rss")] // for etcd txn api
    #[error("Retry")]
    Retry,
}

pub enum Message {
    Frame(MessageFrame),
    Bytes(Bytes),
}

pub struct RpcClient {
    requests: Arc<RwLock<HashMap<u32, oneshot::Sender<MessageFrame>>>>,
    sender: Sender<Message>,
    next_id: AtomicU32,
}

impl RpcClient {
    pub async fn new(stream: TcpStream) -> Result<Self, RpcError> {
        stream.set_nodelay(true)?;
        let (receiver, sender) = stream.into_split();

        // Start message receiver task, for rpc responses
        let requests = Arc::new(RwLock::new(HashMap::new()));
        {
            let requests_clone = requests.clone();
            tokio::spawn(async move {
                if let Err(e) = Self::receive_message_task(receiver, requests_clone).await {
                    tracing::error!("FATAL: receive message task error: {e}");
                }
            });
        }

        // Start message sender task, to send rpc requests. We are launching a dedicated task here
        // to reduce lock contention on the sender socket itself.
        let (tx, rx) = mpsc::channel(1024 * 1024);
        {
            tokio::spawn(async move {
                if let Err(e) = Self::send_message_task(sender, rx).await {
                    tracing::error!("FATAL: receive message task error: {e}");
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
        receiver: OwnedReadHalf,
        requests: Arc<RwLock<HashMap<u32, oneshot::Sender<MessageFrame>>>>,
    ) -> Result<(), RpcError> {
        let decoder = MesssageCodec::default();
        let mut reader = FramedRead::new(receiver, decoder);
        while let Some(frame) = reader.next().await {
            let frame = frame?;
            tracing::debug!("receiving response: request_id={}", frame.header.id);
            let tx: oneshot::Sender<MessageFrame> =
                match requests.write().await.remove(&frame.header.id) {
                    Some(tx) => tx,
                    None => continue, // we may have received the response already
                };
            let _ = tx.send(frame);
        }
        tracing::warn!("connection closed, receive_message_task quit");
        Ok(())
    }

    async fn send_message_task(
        mut sender: OwnedWriteHalf,
        mut input: Receiver<Message>,
    ) -> Result<(), RpcError> {
        while let Some(message) = input.recv().await {
            match message {
                Message::Bytes(mut bytes) => {
                    sender.write_all_buf(&mut bytes).await.unwrap();
                }
                Message::Frame(mut frame) => {
                    let mut header_bytes = BytesMut::with_capacity(MessageHeader::SIZE);
                    frame.header.encode(&mut header_bytes);
                    sender.write_all_buf(&mut header_bytes).await.unwrap();
                    if !frame.body.is_empty() {
                        sender.write_all_buf(&mut frame.body).await.unwrap();
                    }
                }
            }
            sender.flush().await.unwrap();
        }
        Ok(())
    }

    pub async fn send_request(&self, id: u32, msg: Message) -> Result<MessageFrame, RpcError> {
        self.sender.send(msg).await.unwrap();
        tracing::debug!("request sent from handler: request_id={id}");

        let (tx, rx) = oneshot::channel();
        self.requests.write().await.insert(id, tx);
        let res = rx.await.map_err(RpcError::OneshotRecvError);
        if res.is_err() {
            tracing::error!("oneshot error for id={id}");
        }
        res
    }

    pub fn gen_request_id(&self) -> u32 {
        self.next_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }
}

#[cfg(feature = "rss")]
#[derive(Clone)]
pub struct ArcRpcClient(pub Arc<RpcClient>);

#[cfg(feature = "rss")]
impl KvClient for ArcRpcClient {
    type Error = RpcError;
    async fn put(&mut self, key: String, value: Versioned<String>) -> Result<(), Self::Error> {
        self.0.put(value.version, key, value.data).await
    }

    async fn get(&mut self, key: String) -> Result<Versioned<String>, Self::Error> {
        self.0.get(key).await.map(|x| x.into())
    }

    async fn delete(&mut self, key: String) -> Result<(), Self::Error> {
        self.0.delete(key).await
    }

    async fn list(&mut self, prefix: String) -> Result<Vec<String>, Self::Error> {
        self.0.list(prefix).await
    }

    async fn put_with_extra(
        &mut self,
        key: String,
        value: Versioned<String>,
        extra_key: String,
        extra_value: Versioned<String>,
    ) -> Result<(), Self::Error> {
        self.0
            .put_with_extra(
                value.version,
                key,
                value.data,
                extra_value.version,
                extra_key,
                extra_value.data,
            )
            .await
    }

    async fn delete_with_extra(
        &mut self,
        key: String,
        extra_key: String,
        extra_value: Versioned<String>,
    ) -> Result<(), Self::Error> {
        self.0
            .delete_with_extra(key, extra_value.version, extra_key, extra_value.data)
            .await
    }
}
