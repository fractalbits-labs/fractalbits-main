mod extract;
pub mod handler;
mod response_xml;

use nss_rpc_client::rpc_client::{RpcClient, RpcError};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::net::SocketAddr;

pub struct AppState {
    pub rpc_clients: Vec<RpcClient>,
}

impl AppState {
    const MAX_NSS_CONNECTION: usize = 8;

    pub async fn new(nss_ip: &str) -> Result<Self, RpcError> {
        let mut rpc_clients = Vec::with_capacity(Self::MAX_NSS_CONNECTION);
        for _i in 0..AppState::MAX_NSS_CONNECTION {
            let rpc_client = RpcClient::new(nss_ip).await?;
            rpc_clients.push(rpc_client);
        }
        Ok(Self { rpc_clients })
    }

    pub fn get_rpc_client(&self, addr: SocketAddr) -> &RpcClient {
        fn calculate_hash<T: Hash>(t: &T) -> usize {
            let mut s = DefaultHasher::new();
            t.hash(&mut s);
            s.finish() as usize
        }
        let hash = calculate_hash(&addr) % Self::MAX_NSS_CONNECTION;
        &self.rpc_clients[hash]
    }
}
