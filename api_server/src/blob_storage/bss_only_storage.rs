use super::{BlobStorage, BlobStorageError};
use bytes::Bytes;
use metrics::histogram;
use rpc_client_bss::RpcClientBss;
use rpc_client_common::{bss_rpc_retry, rpc_retry};
use slotmap_conn_pool::{ConnPool, Poolable};
use std::{
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};
use tracing::info;
use uuid::Uuid;

pub struct BssOnlyStorage {
    rpc_clients_bss: ConnPool<Arc<RpcClientBss>, SocketAddr>,
    bss_addr: SocketAddr,
    rpc_timeout: Duration,
}

impl BssOnlyStorage {
    pub async fn new(bss_addr: SocketAddr, bss_conn_num: u16, rpc_timeout: Duration) -> Self {
        let clients_bss = ConnPool::new();

        for i in 0..bss_conn_num as usize {
            info!(
                "Connecting to BSS server at {bss_addr} (connection {}/{})",
                i + 1,
                bss_conn_num
            );
            let client = Arc::new(
                <RpcClientBss as slotmap_conn_pool::Poolable>::new(bss_addr)
                    .await
                    .unwrap(),
            );
            clients_bss.pooled(bss_addr, client);
        }

        info!("BSS RPC client pool initialized with {bss_conn_num} connections.");

        Self {
            rpc_clients_bss: clients_bss,
            bss_addr,
            rpc_timeout,
        }
    }

    pub async fn checkout_rpc_client_bss(
        &self,
    ) -> Result<Arc<RpcClientBss>, <RpcClientBss as Poolable>::Error> {
        let start = Instant::now();
        let res = self.rpc_clients_bss.checkout(self.bss_addr).await?;
        histogram!("checkout_rpc_client_nanos", "type" => "bss")
            .record(start.elapsed().as_nanos() as f64);
        Ok(res)
    }
}

impl BlobStorage for BssOnlyStorage {
    async fn put_blob(
        &self,
        blob_id: Uuid,
        block_number: u32,
        body: Bytes,
    ) -> Result<(), BlobStorageError> {
        histogram!("blob_size", "operation" => "put").record(body.len() as f64);

        bss_rpc_retry!(
            self,
            put_blob(blob_id, block_number, body.clone(), Some(self.rpc_timeout))
        )
        .await?;

        Ok(())
    }

    async fn get_blob(
        &self,
        blob_id: Uuid,
        block_number: u32,
        body: &mut Bytes,
    ) -> Result<(), BlobStorageError> {
        bss_rpc_retry!(
            self,
            get_blob(blob_id, block_number, body, Some(self.rpc_timeout))
        )
        .await?;

        histogram!("blob_size", "operation" => "get").record(body.len() as f64);
        Ok(())
    }

    async fn delete_blob(&self, blob_id: Uuid, block_number: u32) -> Result<(), BlobStorageError> {
        bss_rpc_retry!(
            self,
            delete_blob(blob_id, block_number, Some(self.rpc_timeout))
        )
        .await?;

        Ok(())
    }
}
