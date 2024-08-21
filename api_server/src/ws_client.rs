use crate::utils;
use bytes::{Buf, BytesMut};
use std::collections::HashMap;
use std::io::Result;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::BufReader;
use tokio::{
    net::TcpStream,
    sync::{oneshot, RwLock},
};
use web_socket::*;

pub struct RpcClient {
    requests: Arc<RwLock<HashMap<u64, oneshot::Sender<BytesMut>>>>,
    ws: Arc<RwLock<WebSocket<BufReader<TcpStream>>>>,
    next_id: AtomicU64,
}

impl RpcClient {
    pub async fn new(url: &str, port: u32) -> Result<Self> {
        let ws = Arc::new(RwLock::new(
            utils::connect(&format!("{url}:{port}"), "/").await?,
        ));
        let requests = Arc::new(RwLock::new(HashMap::new()));

        {
            let ws_clone = ws.clone();
            let requests_clone = requests.clone();
            tokio::spawn(async move {
                loop {
                    let mut ws = ws_clone.write().await;
                    match tokio::time::timeout(Duration::from_millis(10), ws.recv()).await {
                        Ok(Ok(Event::Data { ty, data })) => {
                            assert!(matches!(ty, DataType::Complete(MessageType::Text)));
                            let mut buf = BytesMut::from_iter(data.iter());
                            let request_id = RpcClient::extract_request_id(&mut buf);
                            let tx: oneshot::Sender<BytesMut> =
                                requests_clone.write().await.remove(&request_id).unwrap();
                            _ = tx.send(buf);
                        }
                        _ => {
                            // ignore for now
                        }
                    }
                }
            });
        }

        Ok(Self {
            requests,
            ws,
            next_id: AtomicU64::new(1),
        })
    }

    pub async fn send_request(&self, id: u64, msg: &[u8]) -> impl Buf {
        self.ws.write().await.send(msg).await.unwrap();
        let (tx, rx) = oneshot::channel();
        self.requests.write().await.insert(id, tx);
        rx.await.unwrap()
    }

    pub fn gen_request_id(&self) -> u64 {
        let request_id = self.next_id.load(std::sync::atomic::Ordering::SeqCst);
        self.next_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        request_id
    }

    fn extract_request_id(buf: &mut impl Buf) -> u64 {
        let (tag, wire_type) = prost::encoding::decode_key(buf).unwrap();
        assert_eq!(1, tag);
        assert_eq!(prost::encoding::WireType::Varint, wire_type);
        prost::encoding::decode_varint(buf).unwrap()
    }
}
