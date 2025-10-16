use std::time::Duration;
use thiserror::Error;
use tokio::sync::RwLock;
use tracing::debug;

pub mod generic_client;
#[cfg(feature = "io_uring")]
pub mod io_uring;
pub use generic_client::{RpcClient, RpcCodec};
pub use rpc_codec_common::{MessageFrame, MessageHeaderTrait};

use generic_client::RpcClient as GenericRpcClient;

pub trait ErrorRetryable {
    fn retryable(&self) -> bool;
}

#[derive(Error, Debug)]
pub enum RpcError {
    #[error(transparent)]
    IoError(#[from] std::io::Error),
    #[error(transparent)]
    OneshotRecvError(tokio::sync::oneshot::error::RecvError),
    #[error("Internal request sending error: {0}")]
    InternalRequestError(String),
    #[error("Internal response error: {0}")]
    InternalResponseError(String),
    #[error("Entry not found")]
    NotFound,
    #[error("Entry already exists")]
    AlreadyExists,
    #[error("Send error: {0}")]
    SendError(String),
    #[error("Encode error: {0}")]
    EncodeError(String),
    #[error("Decode error: {0}")]
    DecodeError(String),
    #[error("Retry")]
    Retry,
    #[error("Connection closed")]
    ConnectionClosed,
}

impl<T> From<tokio::sync::mpsc::error::SendError<T>> for RpcError {
    fn from(e: tokio::sync::mpsc::error::SendError<T>) -> Self {
        RpcError::SendError(e.to_string())
    }
}

impl ErrorRetryable for RpcError {
    fn retryable(&self) -> bool {
        matches!(
            self,
            RpcError::OneshotRecvError(_)
                | RpcError::InternalRequestError(_)
                | RpcError::InternalResponseError(_)
                | RpcError::ConnectionClosed
        )
    }
}

pub struct AutoReconnectRpcClient<Codec, Header>
where
    Codec: RpcCodec<Header>,
    Header: MessageHeaderTrait + Clone + Send + Sync + 'static,
{
    inner: RwLock<Option<GenericRpcClient<Codec, Header>>>,
    address: String,
}

impl<Codec, Header> AutoReconnectRpcClient<Codec, Header>
where
    Codec: RpcCodec<Header>,
    Header: MessageHeaderTrait + Clone + Send + Sync + 'static + Default,
{
    pub fn new_from_address(address: String) -> Self {
        Self {
            inner: RwLock::new(None),
            address,
        }
    }

    async fn ensure_connected(&self) -> Result<(), RpcError> {
        {
            let read = self.inner.read().await;
            if let Some(client) = read.as_ref()
                && !client.is_closed()
            {
                return Ok(());
            }
        }

        let mut write = self.inner.write().await;
        if let Some(client) = write.as_ref()
            && !client.is_closed()
        {
            return Ok(());
        }

        debug!("Reconnecting to RPC server at {}", self.address);
        let new_client =
            GenericRpcClient::<Codec, Header>::establish_connection(self.address.clone())
                .await
                .map_err(|e| {
                    tracing::error!(
                        rpc_type = Codec::RPC_TYPE,
                        address = %self.address,
                        error = %e,
                        "Failed to establish RPC connection"
                    );
                    RpcError::ConnectionClosed
                })?;

        *write = Some(new_client);
        Ok(())
    }

    pub fn gen_request_id(&self) -> u32 {
        rand::random()
    }

    pub async fn send_request(
        &self,
        request_id: u32,
        frame: MessageFrame<Header>,
        timeout: Option<Duration>,
    ) -> Result<MessageFrame<Header>, RpcError> {
        self.ensure_connected().await?;
        let read = self.inner.read().await;
        read.as_ref()
            .unwrap()
            .send_request(request_id, frame, timeout)
            .await
    }
}

#[cfg(feature = "metrics")]
pub struct InflightRpcGuard {
    start: std::time::Instant,
    gauge: metrics::Gauge,
    rpc_type: &'static str,
    rpc_name: &'static str,
}

#[cfg(not(feature = "metrics"))]
pub struct InflightRpcGuard;

#[cfg(feature = "metrics")]
impl InflightRpcGuard {
    pub fn new(rpc_type: &'static str, rpc_name: &'static str) -> Self {
        let gauge = metrics::gauge!("inflight_rpc", "type" => rpc_type, "name" => rpc_name);
        gauge.increment(1.0);
        metrics::counter!("rpc_request_sent", "type" => rpc_type, "name" => rpc_name).increment(1);

        Self {
            start: std::time::Instant::now(),
            gauge,
            rpc_type,
            rpc_name,
        }
    }
}

#[cfg(not(feature = "metrics"))]
impl InflightRpcGuard {
    #[inline(always)]
    pub fn new(_rpc_type: &'static str, _rpc_name: &'static str) -> Self {
        Self
    }
}

#[cfg(feature = "metrics")]
impl Drop for InflightRpcGuard {
    fn drop(&mut self) {
        metrics::histogram!("rpc_duration_nanos", "type" => self.rpc_type, "name" => self.rpc_name)
            .record(self.start.elapsed().as_nanos() as f64);
        self.gauge.decrement(1.0);
    }
}

#[macro_export]
macro_rules! rpc_retry {
    ($rpc_type:expr, $client:expr, $method:ident($($args:expr),*)) => {
        async {
            use $crate::ErrorRetryable;
            let mut retries = 3;
            let mut backoff = std::time::Duration::from_millis(5);
            let mut retry_count = 0u32;
            loop {
                match $client.$method($($args,)* retry_count).await {
                    Ok(val) => {
                        return Ok(val);
                    },
                    Err(e) => {
                        if e.retryable() && retries > 0 {
                            retries -= 1;
                            retry_count += 1;
                            tokio::time::sleep(backoff).await;
                            backoff = backoff.saturating_mul(2);
                        } else {
                            if e.retryable() {
                                tracing::error!(
                                    rpc_type=%$rpc_type,
                                    method=stringify!($method),
                                    error=%e,
                                    "RPC call failed after multiple retries"
                                );
                            }
                            return Err(e);
                        }
                    }
                }
            }
        }
    };
}

#[macro_export]
macro_rules! bss_rpc_retry {
    ($client:expr, $method:ident($($args:expr),*)) => {
        $crate::rpc_retry!("bss", $client, $method($($args),*))
    };
}

#[macro_export]
macro_rules! nss_rpc_retry {
    ($client:expr, $method:ident($($args:expr),*)) => {
        $crate::rpc_retry!("nss", $client, $method($($args),*))
    };
}

#[macro_export]
macro_rules! rss_rpc_retry {
    ($client:expr, $method:ident($($args:expr),*)) => {
        $crate::rpc_retry!("rss", $client, $method($($args),*))
    };
}
