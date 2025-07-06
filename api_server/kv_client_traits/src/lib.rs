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

#[allow(async_fn_in_trait)]
pub trait KvClient {
    type Error: std::error::Error;
    async fn put(&self, key: String, value: Versioned<String>) -> Result<(), Self::Error>;
    async fn put_with_extra(
        &self,
        key: String,
        value: Versioned<String>,
        extra_key: String,
        extra_value: Versioned<String>,
    ) -> Result<(), Self::Error>;
    async fn get(&self, key: String) -> Result<Versioned<String>, Self::Error>;
    async fn delete(&self, key: String) -> Result<(), Self::Error>;
    async fn delete_with_extra(
        &self,
        key: String,
        extra_key: String,
        extra_value: Versioned<String>,
    ) -> Result<(), Self::Error>;
    async fn list(&self, prefix: String) -> Result<Vec<String>, Self::Error>;
}

impl KvClient for RpcClientRss {
    type Error = RpcErrorRss;
    async fn put(&self, key: String, value: Versioned<String>) -> Result<(), Self::Error> {
        Self::put(self, value.version, key, value.data).await
    }

    async fn get(&self, key: String) -> Result<Versioned<String>, Self::Error> {
        Self::get(self, key).await.map(|x| x.into())
    }

    async fn delete(&self, key: String) -> Result<(), Self::Error> {
        Self::delete(self, key).await
    }

    async fn list(&self, prefix: String) -> Result<Vec<String>, Self::Error> {
        Self::list(self, prefix).await
    }

    async fn put_with_extra(
        &self,
        key: String,
        value: Versioned<String>,
        extra_key: String,
        extra_value: Versioned<String>,
    ) -> Result<(), Self::Error> {
        Self::put_with_extra(
            self,
            value.version,
            key,
            value.data,
            extra_value.version,
            extra_key,
            extra_value.data,
        )
        .await
    }

    async fn delete_with_extra(
        &self,
        key: String,
        extra_key: String,
        extra_value: Versioned<String>,
    ) -> Result<(), Self::Error> {
        Self::delete_with_extra(self, key, extra_value.version, extra_key, extra_value.data).await
    }
}
