#![allow(dead_code)]

use bytes::Bytes;
use rpc_client_rss::*;
use std::{marker::PhantomData, sync::Arc};

pub trait Entry: serde::Serialize {
    fn key(&self) -> String;
}

pub trait TableSchema {
    const TABLE_NAME: &'static str;

    type E: Entry;
}

pub trait KvClient {
    async fn put(&mut self, key: String, value: Bytes) -> Bytes;
    async fn get(&mut self, key: String) -> Bytes;
    async fn delete(&mut self, key: String) -> Bytes;
}

impl KvClient for Arc<RpcClientRss> {
    async fn put(&mut self, key: String, value: Bytes) -> Bytes {
        RpcClientRss::put(self, key.into(), value).await.unwrap()
    }

    async fn get(&mut self, key: String) -> Bytes {
        RpcClientRss::get(self, key.into()).await.unwrap()
    }

    async fn delete(&mut self, key: String) -> Bytes {
        RpcClientRss::delete(self, key.into()).await.unwrap()
    }
}

pub struct Table<C: KvClient, F: TableSchema> {
    kv_client: C,
    phantom: PhantomData<F>,
}

impl<C: KvClient, F: TableSchema> Table<C, F> {
    pub fn new(kv_client: C) -> Self {
        Self {
            kv_client,
            phantom: PhantomData,
        }
    }

    pub async fn put(&mut self, e: &F::E) {
        let full_key = Self::get_full_key(F::TABLE_NAME, &e.key());
        self.kv_client
            .put(full_key.into(), serde_json::to_string(e).unwrap().into())
            .await;
    }

    pub async fn get(&mut self, key: String) -> F::E
    where
        <F as TableSchema>::E: for<'a> serde::Deserialize<'a>,
    {
        let full_key = Self::get_full_key(F::TABLE_NAME, &key);
        let json = self.kv_client.get(full_key.into()).await;
        serde_json::from_slice(&json).unwrap()
    }

    pub async fn delete(&mut self, e: &F::E) {
        let full_key = Self::get_full_key(F::TABLE_NAME, &e.key());
        self.kv_client.delete(full_key.into()).await;
    }

    #[inline]
    fn get_full_key(table_name: &str, key: &str) -> String {
        format!("/{table_name}/{key}")
    }
}
