use std::{ops::Deref, sync::Arc};

use async_trait::async_trait;
use rpc_client_rss::{RpcClientRss, RpcErrorRss};

#[derive(Clone)]
pub struct Versioned<T: Sized> {
    pub version: i64,
    pub data: T,
}

impl<T: Sized> Versioned<T> {
    pub fn new(version: i64, data: T) -> Self {
        Self { version, data }
    }
}

impl<T: Sized> From<(i64, T)> for Versioned<T> {
    fn from(value: (i64, T)) -> Self {
        Self {
            version: value.0,
            data: value.1,
        }
    }
}

#[async_trait]
pub trait KvClient {
    type Error: std::error::Error;
    async fn put(&self, key: &str, value: &Versioned<String>) -> Result<(), Self::Error>;
    async fn put_with_extra(
        &self,
        key: &str,
        value: &Versioned<String>,
        extra_key: &str,
        extra_value: &Versioned<String>,
    ) -> Result<(), Self::Error>;
    async fn get(&self, key: &str) -> Result<Versioned<String>, Self::Error>;
    async fn delete(&self, key: &str) -> Result<(), Self::Error>;
    async fn delete_with_extra(
        &self,
        key: &str,
        extra_key: &str,
        extra_value: &Versioned<String>,
    ) -> Result<(), Self::Error>;
    async fn list(&self, prefix: &str) -> Result<Vec<String>, Self::Error>;
}

#[async_trait]
impl<T: KvClient + Sync + Send> KvClient for Arc<T> {
    type Error = T::Error;

    async fn put(&self, key: &str, value: &Versioned<String>) -> Result<(), Self::Error> {
        self.deref().put(key, value).await
    }

    async fn put_with_extra(
        &self,
        key: &str,
        value: &Versioned<String>,
        extra_key: &str,
        extra_value: &Versioned<String>,
    ) -> Result<(), Self::Error> {
        self.deref()
            .put_with_extra(key, value, extra_key, extra_value)
            .await
    }

    async fn get(&self, key: &str) -> Result<Versioned<String>, Self::Error> {
        self.deref().get(key).await
    }

    async fn delete(&self, key: &str) -> Result<(), Self::Error> {
        self.deref().delete(key).await
    }

    async fn delete_with_extra(
        &self,
        key: &str,
        extra_key: &str,
        extra_value: &Versioned<String>,
    ) -> Result<(), Self::Error> {
        self.deref()
            .delete_with_extra(key, extra_key, extra_value)
            .await
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, Self::Error> {
        self.deref().list(prefix).await
    }
}

#[async_trait]
impl KvClient for RpcClientRss {
    type Error = RpcErrorRss;
    async fn put(&self, key: &str, value: &Versioned<String>) -> Result<(), Self::Error> {
        Self::put(self, value.version, key, &value.data).await
    }

    async fn get(&self, key: &str) -> Result<Versioned<String>, Self::Error> {
        Self::get(self, key).await.map(|x| x.into())
    }

    async fn delete(&self, key: &str) -> Result<(), Self::Error> {
        Self::delete(self, key).await
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, Self::Error> {
        Self::list(self, prefix).await
    }

    async fn put_with_extra(
        &self,
        key: &str,
        value: &Versioned<String>,
        extra_key: &str,
        extra_value: &Versioned<String>,
    ) -> Result<(), Self::Error> {
        Self::put_with_extra(
            self,
            value.version,
            key,
            &value.data,
            extra_value.version,
            extra_key,
            &extra_value.data,
        )
        .await
    }

    async fn delete_with_extra(
        &self,
        key: &str,
        extra_key: &str,
        extra_value: &Versioned<String>,
    ) -> Result<(), Self::Error> {
        Self::delete_with_extra(self, key, extra_value.version, extra_key, &extra_value.data).await
    }
}
