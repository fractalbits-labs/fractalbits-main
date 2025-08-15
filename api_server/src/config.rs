use serde::Deserialize;
use std::{net::SocketAddr, time::Duration};

#[derive(Deserialize, Debug, Clone, Default)]
#[serde(rename_all = "snake_case")]
pub enum BlobStorageBackend {
    BssOnlySingleAz,
    HybridSingleAz,
    #[default]
    S3ExpressSingleAz,
    S3ExpressMultiAz,
    S3ExpressMultiAzWithTracking,
}

#[derive(Deserialize, Debug, Clone)]
pub struct BlobStorageConfig {
    pub backend: BlobStorageBackend,

    pub bss: Option<BssConfig>,
    pub s3_hybrid_single_az: Option<S3HybridConfig>,
    pub s3_express_multi_az: Option<S3ExpressMultiAzConfig>,
    pub s3_express_single_az: Option<S3ExpressSingleAzConfig>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct BssConfig {
    pub addr: SocketAddr,
    pub conn_num: u16,
}

#[derive(Deserialize, Debug, Clone)]
pub struct RatelimitConfig {
    pub enabled: bool,
    pub put_qps: u32,
    pub get_qps: u32,
    pub delete_qps: u32,
}

impl Default for RatelimitConfig {
    fn default() -> Self {
        Self {
            enabled: false, // Default to disabled for local testing
            put_qps: 7000,
            get_qps: 10000,
            delete_qps: 5000,
        }
    }
}

#[derive(Deserialize, Debug, Clone)]
pub struct RetryConfig {
    pub retry_mode: String, // "standard" | "adaptive" | "disabled"
    pub max_attempts: u32,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            retry_mode: "disabled".to_string(), // Default to disabled for local testing
            max_attempts: 3,
        }
    }
}

#[derive(Deserialize, Debug, Clone)]
pub struct S3ExpressMultiAzConfig {
    pub local_az_host: String,
    pub local_az_port: u16,
    pub s3_region: String,
    pub local_az_bucket: String,
    pub remote_az_bucket: String,
    pub remote_az_host: Option<String>,
    pub remote_az_port: Option<u16>,
    pub az: String,
    #[serde(default)]
    pub ratelimit: RatelimitConfig,
    #[serde(default)]
    pub retry: RetryConfig,
}

#[derive(Deserialize, Debug, Clone)]
pub struct S3ExpressSingleAzConfig {
    pub s3_host: String,
    pub s3_port: u16,
    pub s3_region: String,
    pub s3_bucket: String,
    pub az: String,
    #[serde(default = "default_force_path_style")]
    pub force_path_style: bool,
    #[serde(default)]
    pub ratelimit: RatelimitConfig,
    #[serde(default)]
    pub retry: RetryConfig,
}

fn default_force_path_style() -> bool {
    true // Default to true for local testing with minio
}

impl Default for S3ExpressSingleAzConfig {
    fn default() -> Self {
        Self {
            s3_host: "http://127.0.0.1".into(),
            s3_port: 9000, // local minio port
            s3_region: "us-west-1".into(),
            s3_bucket: "fractalbits-bucket".into(),
            az: "us-west-1a".into(),
            force_path_style: true,
            ratelimit: RatelimitConfig::default(),
            retry: RetryConfig::default(),
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
pub struct S3HybridConfig {
    pub s3_host: String,
    pub s3_port: u16,
    pub s3_region: String,
    pub s3_bucket: String,
    #[serde(default)]
    pub ratelimit: RatelimitConfig,
    #[serde(default)]
    pub retry: RetryConfig,
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
                backend: BlobStorageBackend::S3ExpressSingleAz,
                bss: None,
                s3_hybrid_single_az: None,
                s3_express_multi_az: None,
                s3_express_single_az: Some(S3ExpressSingleAzConfig::default()),
            },
            allow_missing_or_bad_signature: false,
        }
    }
}
