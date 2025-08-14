use serde::Deserialize;
use std::{net::SocketAddr, time::Duration};

#[derive(Deserialize, Debug, Clone, Default)]
#[serde(rename_all = "snake_case")]
pub enum BlobStorageBackend {
    BssOnly,
    S3Express,
    #[default]
    S3ExpressWithTracking,
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
    pub local_az_host: String,
    pub local_az_port: u16,
    pub s3_region: String,
    pub local_az_bucket: String,
    pub remote_az_bucket: String,
    pub remote_az_host: Option<String>,
    pub remote_az_port: Option<u16>,
    pub az: String,
    #[serde(default = "default_express_session_auth")]
    pub express_session_auth: bool,
}

fn default_express_session_auth() -> bool {
    false // Default to false for local testing with minio
}

impl Default for S3ExpressConfig {
    fn default() -> Self {
        Self {
            local_az_host: "http://127.0.0.1".into(),
            local_az_port: 9001, // local AZ minio port
            s3_region: "us-west-1".into(),
            local_az_bucket: "fractalbits-local-az-data-bucket".into(),
            remote_az_bucket: "fractalbits-remote-az-data-bucket".into(),
            remote_az_host: None,
            remote_az_port: None,
            az: "us-west-1a".into(),
            express_session_auth: false,
        }
    }
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
                backend: BlobStorageBackend::S3Express,
                bss: None,
                s3_cache: None,
                s3_express: Some(S3ExpressConfig::default()),
            },
            allow_missing_or_bad_signature: false,
            web_root: None,
        }
    }
}
