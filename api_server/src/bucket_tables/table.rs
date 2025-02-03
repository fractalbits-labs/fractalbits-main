#![allow(dead_code)]

use rpc_client_rss::*;
use std::{marker::PhantomData, sync::Arc};

pub trait Entry: serde::Serialize {
    fn key(&self) -> String;
}

pub trait TableSchema {
    const TABLE_NAME: &'static str;

    type E: Entry;
}

pub struct Table<F: TableSchema> {
    rpc_client: Arc<RpcClientRss>,
    phantom: PhantomData<F>,
}

impl<F: TableSchema> Table<F> {
    pub fn new(rpc_client: Arc<RpcClientRss>) -> Self {
        Self {
            rpc_client,
            phantom: PhantomData,
        }
    }

    pub async fn put(&self, e: &F::E) {
        let full_key = format!("{}/{}", F::TABLE_NAME, e.key());
        self.rpc_client
            .put(full_key.into(), serde_json::to_string(e).unwrap().into())
            .await
            .unwrap();
    }

    pub async fn get(&self, key: String) -> F::E
    where
        <F as TableSchema>::E: for<'a> serde::Deserialize<'a>,
    {
        let full_key = format!("{}/{}", F::TABLE_NAME, key);
        let json = self.rpc_client.get(full_key.into()).await.unwrap();
        serde_json::from_slice(&json).unwrap()
    }

    pub async fn delete(&self, e: &F::E) {
        let full_key = format!("{}/{}", F::TABLE_NAME, e.key());
        self.rpc_client.delete(full_key.into()).await.unwrap();
    }
}
