use std::{net::SocketAddr, time::Duration};

#[derive(serde::Deserialize, Debug, Clone)]
pub struct Config {
    pub bss_addr: SocketAddr,
    pub nss_addr: SocketAddr,
    pub rss_addr: SocketAddr,
    pub bss_conn_num: u16,
    pub nss_conn_num: u16,
    pub rss_conn_num: u16,

    pub port: u16,
    pub region: String,
    pub root_domain: String,
    pub with_metrics: bool,
    pub http_request_timeout_seconds: u64,
    pub rpc_timeout_seconds: u64,

    pub s3_cache: S3CacheConfig,
    pub allow_missing_or_bad_signature: bool,
    pub web_root: String,
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
            bss_addr: "127.0.0.1:8088".parse().unwrap(),
            nss_addr: "127.0.0.1:8087".parse().unwrap(),
            rss_addr: "127.0.0.1:8086".parse().unwrap(),
            bss_conn_num: 2,
            nss_conn_num: 2,
            rss_conn_num: 1,
            port: 8080,
            region: "us-west-1".into(),
            root_domain: ".localhost".into(),
            s3_cache: S3CacheConfig::default(),
            with_metrics: true,
            http_request_timeout_seconds: 5,
            rpc_timeout_seconds: 4,
            allow_missing_or_bad_signature: false,
            web_root: "../ui/dist".into(),
        }
    }
}
