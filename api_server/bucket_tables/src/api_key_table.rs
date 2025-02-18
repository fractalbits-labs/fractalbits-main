#![allow(dead_code)]
use super::table::{Entry, TableSchema};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

type BucketKeyPerm = bool; // TODO: real bucket key permissions

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct ApiKey {
    pub key_id: String,
    pub secret_key: String,
    pub name: String,
    pub allow_create_bucket: bool,
    pub authorized_buckets: HashMap<String /* bucket name */, BucketKeyPerm>,
    pub is_deleted: bool,
}

impl Entry for ApiKey {
    fn key(&self) -> String {
        self.key_id.clone()
    }
}

pub struct ApiKeyTable;

impl TableSchema for ApiKeyTable {
    const TABLE_NAME: &'static str = "api_keys";

    type E = ApiKey;
}
