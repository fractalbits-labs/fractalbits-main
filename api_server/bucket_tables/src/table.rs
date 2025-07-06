#![allow(dead_code)]

use crate::Versioned;
use kv_client_traits::KvClient;
use metrics::counter;
use moka::future::Cache;
use std::{marker::PhantomData, sync::Arc};

pub trait Entry: serde::Serialize {
    fn key(&self) -> String;
}

pub trait TableSchema {
    const TABLE_NAME: &'static str;

    type E: Entry;
}

pub struct Table<'a, C: KvClient, F: TableSchema> {
    kv_client: &'a C,
    phantom: PhantomData<F>,
    cache: Option<Arc<Cache<String, Versioned<String>>>>,
}

impl<'a, C: KvClient, F: TableSchema> Table<'a, C, F> {
    pub fn new(kv_client: &'a C, cache: Option<Arc<Cache<String, Versioned<String>>>>) -> Self {
        Self {
            kv_client,
            cache,
            phantom: PhantomData,
        }
    }

    pub async fn put(&self, e: &Versioned<F::E>) -> Result<(), C::Error> {
        let full_key = Self::get_full_key(F::TABLE_NAME, &e.data.key());
        let data: String = serde_json::to_string(&e.data).unwrap();
        let versioned_data: Versioned<String> = (e.version, data).into();
        match self
            .kv_client
            .put(full_key.clone(), versioned_data.clone())
            .await
        {
            Ok(()) => {
                if let Some(ref cache) = self.cache {
                    tracing::debug!("caching data with full_key: {full_key}");
                    cache.insert(full_key, versioned_data).await;
                }
            }
            Err(e) => return Err(e),
        }
        Ok(())
    }

    pub async fn put_with_extra<F2>(
        &self,
        e: &Versioned<F::E>,
        extra: &Versioned<F2::E>,
    ) -> Result<(), C::Error>
    where
        F2: TableSchema,
    {
        let full_key = Self::get_full_key(F::TABLE_NAME, &e.data.key());
        let data: String = serde_json::to_string(&e.data).unwrap();
        let versioned_data: Versioned<String> = (e.version, data.clone()).into();
        let extra_full_key = Self::get_full_key(F2::TABLE_NAME, &extra.data.key());
        let extra_data: String = serde_json::to_string(&extra.data).unwrap();
        let extra_versioned_data: Versioned<String> = (extra.version, extra_data.clone()).into();
        match self
            .kv_client
            .put_with_extra(
                full_key.clone(),
                versioned_data.clone(),
                extra_full_key.clone(),
                extra_versioned_data.clone(),
            )
            .await
        {
            Ok(_) => {
                if let Some(ref cache) = self.cache {
                    cache.insert(full_key, versioned_data).await;
                    cache.insert(extra_full_key, extra_versioned_data).await;
                }
            }
            Err(e) => return Err(e),
        };
        Ok(())
    }

    pub async fn get(&self, key: String, try_cache: bool) -> Result<Versioned<F::E>, C::Error>
    where
        <F as TableSchema>::E: for<'s> serde::Deserialize<'s>,
    {
        let full_key = Self::get_full_key(F::TABLE_NAME, &key);
        if try_cache {
            if let Some(ref cache) = self.cache {
                if let Some(json) = cache.get(&full_key).await {
                    counter!("table_cache_hit", "table_name" => F::TABLE_NAME).increment(1);
                    tracing::debug!("get cached data with full_key: {full_key}");
                    return Ok((
                        json.version,
                        serde_json::from_slice(json.data.as_bytes()).unwrap(),
                    )
                        .into());
                }
            } else {
                counter!("table_cache_miss", "table_name" => F::TABLE_NAME).increment(1);
            }
        }

        let json = self.kv_client.get(full_key).await?;
        Ok((
            json.version,
            serde_json::from_slice(json.data.as_bytes()).unwrap(),
        )
            .into())
    }

    pub async fn list(&self) -> Result<Vec<F::E>, C::Error>
    where
        <F as TableSchema>::E: for<'s> serde::Deserialize<'s>,
    {
        let prefix = Self::get_prefix(F::TABLE_NAME);
        let kvs = self.kv_client.list(prefix).await?;
        Ok(kvs
            .iter()
            .map(|x| serde_json::from_slice(x.as_bytes()).unwrap())
            .collect())
    }

    pub async fn delete(&self, e: &F::E) -> Result<(), C::Error> {
        let full_key = Self::get_full_key(F::TABLE_NAME, &e.key());
        match self.kv_client.delete(full_key.clone()).await {
            Ok(()) => {
                if let Some(ref cache) = self.cache {
                    cache.invalidate(&full_key).await;
                }
            }
            Err(e) => return Err(e),
        }
        Ok(())
    }

    pub async fn delete_with_extra<F2>(
        &self,
        e: &F::E,
        extra: &Versioned<F2::E>,
    ) -> Result<(), C::Error>
    where
        F2: TableSchema,
    {
        let full_key = Self::get_full_key(F::TABLE_NAME, &e.key());
        let extra_full_key = Self::get_full_key(F2::TABLE_NAME, &extra.data.key());
        let extra_data: String = serde_json::to_string(&extra.data).unwrap();
        let extra_versioned_data: Versioned<String> = (extra.version, extra_data.clone()).into();
        match self
            .kv_client
            .delete_with_extra(
                full_key.clone(),
                extra_full_key.clone(),
                extra_versioned_data.clone(),
            )
            .await
        {
            Ok(()) => {
                if let Some(ref cache) = self.cache {
                    cache.invalidate(&full_key).await;
                    cache.insert(extra_full_key, extra_versioned_data).await;
                }
            }
            Err(e) => return Err(e),
        }
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
