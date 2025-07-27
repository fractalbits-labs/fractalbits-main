use std::{ops::Deref, sync::Arc, time::Duration};

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
    async fn put(
        &self,
        key: &str,
        value: &Versioned<String>,
        timeout: Option<Duration>,
    ) -> Result<(), Self::Error>;
    async fn put_with_extra(
        &self,
        key: &str,
        value: &Versioned<String>,
        extra_key: &str,
        extra_value: &Versioned<String>,
        timeout: Option<Duration>,
    ) -> Result<(), Self::Error>;
    async fn get(
        &self,
        key: &str,
        timeout: Option<Duration>,
    ) -> Result<Versioned<String>, Self::Error>;
    async fn delete(&self, key: &str, timeout: Option<Duration>) -> Result<(), Self::Error>;
    async fn delete_with_extra(
        &self,
        key: &str,
        extra_key: &str,
        extra_value: &Versioned<String>,
        timeout: Option<Duration>,
    ) -> Result<(), Self::Error>;
    async fn list(
        &self,
        prefix: &str,
        timeout: Option<Duration>,
    ) -> Result<Vec<String>, Self::Error>;
}

#[async_trait]
impl<T: KvClient + Sync + Send> KvClient for Arc<T> {
    type Error = T::Error;

    async fn put(
        &self,
        key: &str,
        value: &Versioned<String>,
        timeout: Option<Duration>,
    ) -> Result<(), Self::Error> {
        self.deref().put(key, value, timeout).await
    }

    async fn put_with_extra(
        &self,
        key: &str,
        value: &Versioned<String>,
        extra_key: &str,
        extra_value: &Versioned<String>,
        timeout: Option<Duration>,
    ) -> Result<(), Self::Error> {
        self.deref()
            .put_with_extra(key, value, extra_key, extra_value, timeout)
            .await
    }

    async fn get(
        &self,
        key: &str,
        timeout: Option<Duration>,
    ) -> Result<Versioned<String>, Self::Error> {
        self.deref().get(key, timeout).await
    }

    async fn delete(&self, key: &str, timeout: Option<Duration>) -> Result<(), Self::Error> {
        self.deref().delete(key, timeout).await
    }

    async fn delete_with_extra(
        &self,
        key: &str,
        extra_key: &str,
        extra_value: &Versioned<String>,
        timeout: Option<Duration>,
    ) -> Result<(), Self::Error> {
        self.deref()
            .delete_with_extra(key, extra_key, extra_value, timeout)
            .await
    }

    async fn list(
        &self,
        prefix: &str,
        timeout: Option<Duration>,
    ) -> Result<Vec<String>, Self::Error> {
        self.deref().list(prefix, timeout).await
    }
}

#[async_trait]
impl KvClient for RpcClientRss {
    type Error = RpcErrorRss;
    async fn put(
        &self,
        key: &str,
        value: &Versioned<String>,
        timeout: Option<Duration>,
    ) -> Result<(), Self::Error> {
        Self::put(self, value.version, key, &value.data, timeout).await
    }

    async fn get(
        &self,
        key: &str,
        timeout: Option<Duration>,
    ) -> Result<Versioned<String>, Self::Error> {
        Self::get(self, key, timeout).await.map(|x| x.into())
    }

    async fn delete(&self, key: &str, timeout: Option<Duration>) -> Result<(), Self::Error> {
        Self::delete(self, key, timeout).await
    }

    async fn list(
        &self,
        prefix: &str,
        timeout: Option<Duration>,
    ) -> Result<Vec<String>, Self::Error> {
        Self::list(self, prefix, timeout).await
    }

    async fn put_with_extra(
        &self,
        key: &str,
        value: &Versioned<String>,
        extra_key: &str,
        extra_value: &Versioned<String>,
        timeout: Option<Duration>,
    ) -> Result<(), Self::Error> {
        Self::put_with_extra(
            self,
            value.version,
            key,
            &value.data,
            extra_value.version,
            extra_key,
            &extra_value.data,
            timeout,
        )
        .await
    }

    async fn delete_with_extra(
        &self,
        key: &str,
        extra_key: &str,
        extra_value: &Versioned<String>,
        timeout: Option<Duration>,
    ) -> Result<(), Self::Error> {
        Self::delete_with_extra(
            self,
            key,
            extra_value.version,
            extra_key,
            &extra_value.data,
            timeout,
        )
        .await
    }
}
