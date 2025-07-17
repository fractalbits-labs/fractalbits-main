pub trait ErrorRetryable {
    fn retryable(&self) -> bool;
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
        rpc_retry!($pool, checkout_rpc_client_bss(), $method($($args),*))
    };
}

#[macro_export]
macro_rules! nss_rpc_retry {
    ($pool:expr, $method:ident($($args:expr),*)) => {
        rpc_retry!($pool, checkout_rpc_client_nss(), $method($($args),*))
    };
}

#[macro_export]
macro_rules! rss_rpc_retry {
    ($pool:expr, $method:ident($($args:expr),*)) => {
        rpc_retry!($pool, checkout_rpc_client_rss(), $method($($args),*))
    };
}
