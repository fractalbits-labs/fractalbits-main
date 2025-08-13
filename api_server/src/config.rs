use serde::Deserialize;
use std::{net::SocketAddr, time::Duration};

#[derive(Deserialize, Debug, Clone, Default)]
#[serde(rename_all = "snake_case")]
pub enum BlobStorageBackend {
    BssOnly,
    S3Express,
    #[default]
    Hybrid,
}

#[derive(Deserialize, Debug, Clone)]
pub struct BlobStorageConfig {
    #[serde(default)]
    pub backend: BlobStorageBackend,

    pub bss: Option<BssConfig>,
    pub s3_cache: Option<S3CacheConfig>,
    pub s3_express: Option<S3ExpressConfig>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct BssConfig {
    pub addr: SocketAddr,
    pub conn_num: u16,
}

#[derive(Deserialize, Debug, Clone)]
pub struct S3ExpressConfig {
    pub bucket: String,
    pub region: String,
    pub az: String,
    #[serde(default = "default_express_session_auth")]
    pub express_session_auth: bool,
}

fn default_express_session_auth() -> bool {
    true
}

#[derive(serde::Deserialize, Debug, Clone)]
pub struct Config {
    pub nss_addr: SocketAddr,
    pub rss_addr: SocketAddr,
    pub nss_conn_num: u16,
    pub rss_conn_num: u16,

    pub port: u16,
    pub region: String,
    pub root_domain: String,
    pub with_metrics: bool,
    pub http_request_timeout_seconds: u64,
    pub rpc_timeout_seconds: u64,

    pub blob_storage: BlobStorageConfig,
    pub allow_missing_or_bad_signature: bool,
    pub web_root: Option<String>,
}

impl Config {
    pub fn rpc_timeout(&self) -> Duration {
        Duration::from_secs(self.rpc_timeout_seconds)
    }

    pub fn http_request_timeout(&self) -> Duration {
        Duration::from_secs(self.http_request_timeout_seconds)
    }
}

#[derive(serde::Deserialize, Debug, Clone)]
pub struct S3CacheConfig {
    pub s3_host: String,
    pub s3_port: u16,
    pub s3_region: String,
    pub s3_bucket: String,
}

impl Default for S3CacheConfig {
    fn default() -> Self {
        Self {
            s3_host: "http://127.0.0.1".into(),
            s3_port: 9000, // local minio port
            s3_region: "us-east-1".into(),
            s3_bucket: "fractalbits-bucket".into(),
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            nss_addr: "127.0.0.1:8087".parse().unwrap(),
            rss_addr: "127.0.0.1:8086".parse().unwrap(),
            nss_conn_num: 2,
            rss_conn_num: 1,
            port: 8080,
            region: "us-west-1".into(),
            root_domain: ".localhost".into(),
            with_metrics: true,
            http_request_timeout_seconds: 5,
            rpc_timeout_seconds: 4,
            blob_storage: BlobStorageConfig {
                backend: BlobStorageBackend::Hybrid,
                bss: Some(BssConfig {
                    addr: "127.0.0.1:8088".parse().unwrap(),
                    conn_num: 2,
                }),
                s3_cache: Some(S3CacheConfig::default()),
                s3_express: None,
            },
            allow_missing_or_bad_signature: false,
            web_root: None,
        }
    }
}
