#![allow(dead_code)]

use std::marker::PhantomData;

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
    async fn put(&mut self, key: String, value: Versioned<String>) -> Result<(), Self::Error>;
    async fn put_with_extra(
        &mut self,
        key: String,
        value: Versioned<String>,
        extra_key: String,
        extra_value: Versioned<String>,
    ) -> Result<(), Self::Error>;
    async fn get(&mut self, key: String) -> Result<Versioned<String>, Self::Error>;
    async fn delete(&mut self, key: String) -> Result<(), Self::Error>;
    async fn delete_with_extra(
        &mut self,
        key: String,
        extra_key: String,
        extra_value: Versioned<String>,
    ) -> Result<(), Self::Error>;
    async fn list(&mut self, prefix: String) -> Result<Vec<String>, Self::Error>;
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

    pub async fn put(&mut self, e: &Versioned<F::E>) -> Result<(), C::Error> {
        let full_key = Self::get_full_key(F::TABLE_NAME, &e.data.key());
        let data: String = serde_json::to_string(&e.data).unwrap();
        self.kv_client
            .put(full_key, (e.version, data).into())
            .await?;
        Ok(())
    }

    pub async fn put_with_extra<F2>(
        &mut self,
        e: &Versioned<F::E>,
        extra: &Versioned<F2::E>,
    ) -> Result<(), C::Error>
    where
        F2: TableSchema,
    {
        let full_key = Self::get_full_key(F::TABLE_NAME, &e.data.key());
        let data: String = serde_json::to_string(&e.data).unwrap();
        let extra_full_key = Self::get_full_key(F2::TABLE_NAME, &extra.data.key());
        let extra_data: String = serde_json::to_string(&extra.data).unwrap();
        self.kv_client
            .put_with_extra(
                full_key,
                (e.version, data).into(),
                extra_full_key,
                (extra.version, extra_data).into(),
            )
            .await?;
        Ok(())
    }

    pub async fn get(&mut self, key: String) -> Result<Versioned<F::E>, C::Error>
    where
        <F as TableSchema>::E: for<'a> serde::Deserialize<'a>,
    {
        let full_key = Self::get_full_key(F::TABLE_NAME, &key);
        let json = self.kv_client.get(full_key).await?;
        Ok((
            json.version,
            serde_json::from_slice(json.data.as_bytes()).unwrap(),
        )
            .into())
    }

    pub async fn list(&mut self) -> Result<Vec<F::E>, C::Error>
    where
        <F as TableSchema>::E: for<'a> serde::Deserialize<'a>,
    {
        let prefix = Self::get_prefix(F::TABLE_NAME);
        let kvs = self.kv_client.list(prefix).await?;
        Ok(kvs
            .iter()
            .map(|x| serde_json::from_slice(x.as_bytes()).unwrap())
            .collect())
    }

    pub async fn delete(&mut self, e: &F::E) -> Result<(), C::Error> {
        let full_key = Self::get_full_key(F::TABLE_NAME, &e.key());
        self.kv_client.delete(full_key).await?;
        Ok(())
    }

    pub async fn delete_with_extra<F2>(
        &mut self,
        e: &F::E,
        extra: &Versioned<F2::E>,
    ) -> Result<(), C::Error>
    where
        F2: TableSchema,
    {
        let full_key = Self::get_full_key(F::TABLE_NAME, &e.key());
        let extra_full_key = Self::get_full_key(F2::TABLE_NAME, &extra.data.key());
        let extra_data: String = serde_json::to_string(&extra.data).unwrap();
        self.kv_client
            .delete_with_extra(full_key, extra_full_key, (extra.version, extra_data).into())
            .await?;
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
