use thiserror::Error;

pub mod generic_client;
pub use generic_client::{RpcClient, RpcCodec};

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
}

impl<T> From<tokio::sync::mpsc::error::SendError<T>> for RpcError {
    fn from(e: tokio::sync::mpsc::error::SendError<T>) -> Self {
        RpcError::SendError(e.to_string())
    }
}

impl ErrorRetryable for RpcError {
    fn retryable(&self) -> bool {
        matches!(self, RpcError::OneshotRecvError(_))
    }
}

pub struct InflightRpcGuard {
    start: std::time::Instant,
    gauge: metrics::Gauge,
    rpc_type: &'static str,
    rpc_name: &'static str,
}

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

impl Drop for InflightRpcGuard {
    fn drop(&mut self) {
        metrics::histogram!("rpc_duration_nanos", "type" => self.rpc_type, "name" => self.rpc_name)
            .record(self.start.elapsed().as_nanos() as f64);
        self.gauge.decrement(1.0);
    }
}

#[macro_export]
macro_rules! rpc_retry {
    ($pool:expr, $checkout:ident($($addr:expr),*), $method:ident($($args:expr),*)) => {
        async {
            use $crate::ErrorRetryable;
            let mut retries = 3;
            let mut backoff = std::time::Duration::from_millis(5);
            loop {
                let rpc_client = $pool.$checkout($($addr),*).await.unwrap();
                match rpc_client.$method($($args),*).await {
                    Ok(val) => return Ok(val),
                    Err(e) => {
                        if e.retryable() && retries > 0 {
                            retries -= 1;
                            tokio::time::sleep(backoff).await;
                            backoff = backoff.saturating_mul(2);
                        } else {
                            if e.retryable() {
                                tracing::error!(
                                    "RPC call failed after multiple retries. Error: {}",
                                    e
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
    ($pool:expr, $method:ident($($args:expr),*)) => {
        $crate::rpc_retry!($pool, checkout_rpc_client_bss(), $method($($args),*))
    };
}

#[macro_export]
macro_rules! nss_rpc_retry {
    ($pool:expr, $method:ident($($args:expr),*)) => {
        $crate::rpc_retry!($pool, checkout_rpc_client_nss(), $method($($args),*))
    };
}

#[macro_export]
macro_rules! rss_rpc_retry {
    ($pool:expr, $method:ident($($args:expr),*)) => {
        $crate::rpc_retry!($pool, checkout_rpc_client_rss(), $method($($args),*))
    };
}

#[macro_export]
macro_rules! nss_rpc_retry_with_session {
    ($app_state:expr, $method:ident($($args:expr),*)) => {
        async {
            use $crate::ErrorRetryable;
            let mut retries = 3;
            let mut backoff = std::time::Duration::from_millis(5);
            let mut stable_request_id: Option<u32> = None;

            loop {
                let rpc_client = $app_state.checkout_rpc_client_nss().await.unwrap();

                // Generate or reuse stable request_id
                let request_id = stable_request_id.unwrap_or_else(|| {
                    let id = rpc_client.gen_request_id();
                    stable_request_id = Some(id);
                    id
                });

                // Try to call the _with_stable_request_id variant if available
                let method_name = stringify!($method);
                match method_name {
                    "put_inode" => {
                        match rpc_client.put_inode_with_stable_request_id($($args,)* Some(request_id)).await {
                            Ok(val) => return Ok(val),
                            Err(e) => {
                                if e.retryable() && retries > 0 {
                                    retries -= 1;
                                    tokio::time::sleep(backoff).await;
                                    backoff = backoff.saturating_mul(2);
                                } else {
                                    if e.retryable() {
                                        tracing::error!(
                                            "RPC call failed after multiple retries. Error: {}",
                                            e
                                        );
                                    }
                                    return Err(e);
                                }
                            }
                        }
                    },
                    _ => {
                        // Fallback to regular method call for methods without stable_request_id support
                        match rpc_client.$method($($args),*).await {
                            Ok(val) => return Ok(val),
                            Err(e) => {
                                if e.retryable() && retries > 0 {
                                    retries -= 1;
                                    tokio::time::sleep(backoff).await;
                                    backoff = backoff.saturating_mul(2);
                                } else {
                                    if e.retryable() {
                                        tracing::error!(
                                            "RPC call failed after multiple retries. Error: {}",
                                            e
                                        );
                                    }
                                    return Err(e);
                                }
                            }
                        }
                    }
                }
            }
        }
    };
}

#[macro_export]
macro_rules! rss_rpc_retry_with_session {
    ($app_state:expr, $method:ident($($args:expr),*)) => {
        async {
            use $crate::ErrorRetryable;
            let mut retries = 3;
            let mut backoff = std::time::Duration::from_millis(5);

            loop {
                let rpc_client = $app_state.checkout_rpc_client_rss().await.unwrap();

                match rpc_client.$method($($args),*).await {
                    Ok(val) => return Ok(val),
                    Err(e) => {
                        if e.retryable() && retries > 0 {
                            retries -= 1;
                            tokio::time::sleep(backoff).await;
                            backoff = backoff.saturating_mul(2);
                        } else {
                            if e.retryable() {
                                tracing::error!(
                                    "RPC call failed after multiple retries. Error: {}",
                                    e
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
