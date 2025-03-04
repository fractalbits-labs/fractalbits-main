#![allow(dead_code)]

use bytes::Bytes;
use std::marker::PhantomData;

pub trait Entry: serde::Serialize {
    fn key(&self) -> String;
}

pub trait TableSchema {
    const TABLE_NAME: &'static str;

    type E: Entry;
}

#[allow(async_fn_in_trait)]
pub trait KvClient {
    type Error: std::error::Error;
    async fn put(&mut self, key: String, value: Bytes) -> Result<Bytes, Self::Error>;
    async fn get(&mut self, key: String) -> Result<Bytes, Self::Error>;
    async fn delete(&mut self, key: String) -> Result<Bytes, Self::Error>;
    async fn list(&mut self, prefix: String) -> Result<Vec<Bytes>, Self::Error>;
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

    pub async fn put(&mut self, e: &F::E) -> Result<(), C::Error> {
        let full_key = Self::get_full_key(F::TABLE_NAME, &e.key());
        self.kv_client
            .put(full_key, serde_json::to_string(e).unwrap().into())
            .await?;
        Ok(())
    }

    pub async fn get(&mut self, key: String) -> Result<F::E, C::Error>
    where
        <F as TableSchema>::E: for<'a> serde::Deserialize<'a>,
    {
        let full_key = Self::get_full_key(F::TABLE_NAME, &key);
        let json = self.kv_client.get(full_key).await?;
        Ok(serde_json::from_slice(&json).unwrap())
    }

    pub async fn list(&mut self) -> Result<Vec<F::E>, C::Error>
    where
        <F as TableSchema>::E: for<'a> serde::Deserialize<'a>,
    {
        let prefix = Self::get_prefix(F::TABLE_NAME);
        let kvs = self.kv_client.list(prefix).await?;
        Ok(kvs
            .iter()
            .map(|x| serde_json::from_slice(x).unwrap())
            .collect())
    }

    pub async fn delete(&mut self, e: &F::E) -> Result<(), C::Error> {
        let full_key = Self::get_full_key(F::TABLE_NAME, &e.key());
        self.kv_client.delete(full_key).await?;
        Ok(())
    }

    #[inline]
    fn get_full_key(table_name: &str, key: &str) -> String {
        format!("/{table_name}/{key}")
    }

    #[inline]
    fn get_prefix(table_name: &str) -> String {
        format!("/{table_name}/")
    }
}
