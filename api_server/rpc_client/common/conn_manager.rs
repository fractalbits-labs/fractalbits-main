use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};
use tokio::net::TcpStream;
use tokio_retry::{strategy::ExponentialBackoff, Retry};
use tracing::{info, instrument, warn};

use super::rpc_client::RpcClient;

const MAX_CONNECTION_RETRIES: usize = 5; // Max attempts to connect to an RPC server

#[derive(Debug, Clone)]
pub struct RpcConnectionManager {
    // Now stores multiple remote addresses
    remote_addrs: Arc<Vec<SocketAddr>>,
    // Used to cycle through the addresses for connection attempts
    current_addr_idx: Arc<AtomicUsize>,
}

impl RpcConnectionManager {
    // Constructor now takes a Vec of SocketAddr
    pub fn new(remote_addrs: Vec<SocketAddr>) -> Self {
        RpcConnectionManager {
            remote_addrs: Arc::new(remote_addrs),
            current_addr_idx: Arc::new(AtomicUsize::new(0)),
        }
    }
}

impl bb8::ManageConnection for RpcConnectionManager {
    type Connection = RpcClient;
    type Error = Box<dyn std::error::Error + Send + Sync>; // Using Box<dyn Error> for simplicity

    #[instrument(skip(self), fields(peer_addrs = ?self.remote_addrs))]
    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        info!("Attempting to establish new RPC connection from list of addresses...");

        let num_addrs = self.remote_addrs.len();
        if num_addrs == 0 {
            return Err("No addresses to connect to".into());
        }

        // Define the retry strategy: exponential backoff with a limit on attempts.
        // The retry strategy applies to each individual connection attempt until successful or limit reached.
        let retry_strategy = ExponentialBackoff::from_millis(100) // Initial delay
            .factor(2) // Doubles the delay each time
            .take(MAX_CONNECTION_RETRIES); // Max retries for a single connection attempt

        // Clone Arc components for the closure
        let remote_addrs_clone = Arc::clone(&self.remote_addrs);
        let current_addr_idx_clone = Arc::clone(&self.current_addr_idx);

        let stream = Retry::spawn(retry_strategy, move || {
            // This closure is executed for each retry attempt.
            let num_addrs_inner = remote_addrs_clone.len();
            // Get the current index and increment it for the next attempt (simple round-robin)
            let current_idx =
                current_addr_idx_clone.fetch_add(1, Ordering::SeqCst) % num_addrs_inner;
            let target_addr = remote_addrs_clone[current_idx];

            info!("Attempting to connect to RPC server at: {}", target_addr);

            // Return a Future that represents the connection attempt.
            async move {
                TcpStream::connect(target_addr).await.map_err(|e| {
                    warn!("Failed to connect to RPC server at {}: {}", target_addr, e);
                    // Return the error so `tokio_retry` can decide to retry
                    e
                })
            }
        })
        .await?; // Await the result of the retry attempts.

        Self::Connection::new(stream).await.map_err(|e| e.into())
    }

    #[instrument(skip(self, _conn))]
    async fn is_valid(&self, _conn: &mut Self::Connection) -> Result<(), Self::Error> {
        // TODO:
        // A simple way to check validity: try to write a zero-length frame
        // or send a "ping" if the protocol supported it.
        // For simplicity, we'll just check if the underlying stream is still writable.
        // A more robust check might involve sending a lightweight "ping" RPC.
        Ok(())
    }

    fn has_broken(&self, conn: &mut Self::Connection) -> bool {
        !conn.tasks_running()
    }
}
