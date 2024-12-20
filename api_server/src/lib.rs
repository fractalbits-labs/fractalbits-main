mod extract;
pub mod handler;
mod object_layout;
mod response_xml;

use rpc_client_bss::{RpcClientBss, RpcErrorBss};
use rpc_client_nss::RpcClientNss;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::net::SocketAddr;
use tokio::sync::mpsc::{self, Receiver, Sender};

pub type BlobId = uuid::Uuid;

pub struct AppState {
    pub rpc_clients_nss: Vec<RpcClientNss>,
    pub rpc_clients_bss: Vec<RpcClientBss>,
    pub blob_deletion: Sender<(BlobId, usize)>,
}

impl AppState {
    const MAX_NSS_CONNECTION: usize = 8;
    const MAX_BSS_CONNECTION: usize = 8;

    pub async fn new(nss_ip: String, bss_ip: String) -> Self {
        let mut rpc_clients_nss = Vec::with_capacity(Self::MAX_NSS_CONNECTION);
        for _i in 0..AppState::MAX_NSS_CONNECTION {
            let rpc_client_nss = RpcClientNss::new(&nss_ip)
                .await
                .expect("rpc client nss failure");
            rpc_clients_nss.push(rpc_client_nss);
        }

        let mut rpc_clients_bss = Vec::with_capacity(Self::MAX_BSS_CONNECTION);
        for _i in 0..AppState::MAX_BSS_CONNECTION {
            let rpc_client_bss = RpcClientBss::new(&bss_ip)
                .await
                .expect("rpc client bss failure");
            rpc_clients_bss.push(rpc_client_bss);
        }

        let (tx, rx) = mpsc::channel(1024 * 1024);
        tokio::spawn(async move {
            if let Err(e) = Self::blob_deletion_task(&bss_ip, rx).await {
                tracing::error!("FATAL: blob deletion task error: {e}");
            }
        });

        Self {
            rpc_clients_nss,
            rpc_clients_bss,
            blob_deletion: tx,
        }
    }

    pub fn get_rpc_client_nss(&self, addr: SocketAddr) -> &RpcClientNss {
        let hash = Self::calculate_hash(&addr) % Self::MAX_NSS_CONNECTION;
        &self.rpc_clients_nss[hash]
    }

    pub fn get_rpc_client_bss(&self, addr: SocketAddr) -> &RpcClientBss {
        let hash = Self::calculate_hash(&addr) % Self::MAX_BSS_CONNECTION;
        &self.rpc_clients_bss[hash]
    }

    #[inline]
    fn calculate_hash<T: Hash>(t: &T) -> usize {
        let mut s = DefaultHasher::new();
        t.hash(&mut s);
        s.finish() as usize
    }

    async fn blob_deletion_task(
        bss_ip: &str,
        mut input: Receiver<(BlobId, usize)>,
    ) -> Result<(), RpcErrorBss> {
        let rpc_client_bss = RpcClientBss::new(bss_ip).await?;
        while let Some((blob_id, block_numbers)) = input.recv().await {
            for block_number in 0..block_numbers {
                rpc_client_bss
                    .delete_blob(blob_id, block_number as u32)
                    .await
                    .inspect_err(|e| {
                        tracing::error!("delete {blob_id}-p{block_number} failed: {e}")
                    })?;
            }
        }
        Ok(())
    }
}
