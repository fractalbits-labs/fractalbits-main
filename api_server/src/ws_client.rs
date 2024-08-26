use bytes::Buf;
use bytes::Bytes;
use soketto::{
    connection::{Receiver, Sender},
    handshake::{Client, ServerResponse},
};
use std::collections::HashMap;
use std::io;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use thiserror::Error;
use tokio::{
    net::TcpStream,
    sync::{oneshot, RwLock},
};
use tokio_util::compat::{Compat, TokioAsyncReadCompatExt};

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
    requests: Arc<RwLock<HashMap<u64, oneshot::Sender<Vec<u8>>>>>,
    sender: tokio::sync::mpsc::Sender<Bytes>,
    next_id: AtomicU64,
}

impl RpcClient {
    pub async fn new(url: &str) -> Result<Self, WebSocketError> {
        let socket = TcpStream::connect(url).await?;
        socket.set_nodelay(true)?;
        let mut client = Client::new(socket.compat(), url, "/");
        let (sender, receiver) = match client.handshake().await {
            Ok(ServerResponse::Accepted { .. }) => client.into_builder().finish(),
            Ok(ServerResponse::Redirect { .. }) => return Err(WebSocketError::Redirect),
            Ok(ServerResponse::Rejected { status_code }) => {
                return Err(WebSocketError::RejectedWithStatusCode(status_code))
            }
            Err(err) => return Err(WebSocketError::HandShakeError(err)),
        };

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
            next_id: AtomicU64::new(1),
        })
    }

    async fn receive_message_task(
        mut receiver: Receiver<Compat<TcpStream>>,
        requests: Arc<RwLock<HashMap<u64, oneshot::Sender<Vec<u8>>>>>,
    ) -> Result<(), WebSocketError> {
        loop {
            let mut message = Vec::new();
            receiver
                .receive_data(&mut message)
                .await
                .map_err(WebSocketError::ConnectionErr)?;
            let request_id = RpcClient::extract_request_id(&mut message.as_slice())?;
            tracing::info!("response received from task: request_id={request_id}");
            let tx: oneshot::Sender<Vec<u8>> = match requests.write().await.remove(&request_id) {
                Some(tx) => tx,
                None => {
                    return Err(WebSocketError::InternalResponseError(format!(
                        "unknown request_id: {request_id}"
                    )))
                }
            };
            tx.send(message).map_err(|_| {
                WebSocketError::InternalResponseError("receiver dropped".to_string())
            })?;
        }
    }

    async fn send_message_task(
        mut sender: Sender<Compat<TcpStream>>,
        mut input: tokio::sync::mpsc::Receiver<Bytes>,
    ) -> Result<(), WebSocketError> {
        while let Some(message) = input.recv().await {
            sender
                .send_binary(message)
                .await
                .map_err(WebSocketError::ConnectionErr)?;
            sender
                .flush()
                .await
                .map_err(WebSocketError::ConnectionErr)?;
        }
        Ok(())
    }

    pub async fn send_request(&self, id: u64, msg: Bytes) -> Result<Vec<u8>, WebSocketError> {
        self.sender
            .send(msg)
            .await
            .map_err(|e| WebSocketError::InternalRequestError(e.to_string()))?;
        tracing::info!("request sent from handler: request_id={id}");

        let (tx, rx) = oneshot::channel();
        self.requests.write().await.insert(id, tx);

        rx.await.map_err(WebSocketError::OneshotRecvError)
    }

    pub fn gen_request_id(&self) -> u64 {
        let request_id = self.next_id.load(std::sync::atomic::Ordering::SeqCst);
        self.next_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        request_id
    }

    fn extract_request_id(buf: &mut impl Buf) -> Result<u64, WebSocketError> {
        let (tag, wire_type) =
            prost::encoding::decode_key(buf).map_err(WebSocketError::DecodeError)?;
        assert_eq!(1, tag);
        assert_eq!(prost::encoding::WireType::Varint, wire_type);
        prost::encoding::decode_varint(buf).map_err(WebSocketError::DecodeError)
    }
}
