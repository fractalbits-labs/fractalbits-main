use crate::blob_storage::S3RetryConfig;
use serde::Deserialize;
use std::time::Duration;

#[derive(Deserialize, Debug, Clone, Default)]
#[serde(rename_all = "snake_case")]
pub enum BlobStorageBackend {
    BssOnlySingleAz,
    #[default]
    HybridSingleAz,
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
    pub addr: String,
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
pub struct S3ExpressMultiAzConfig {
    pub local_az_host: String,
    pub local_az_port: u16,
    pub s3_region: String,
    pub local_az_bucket: String,
    pub remote_az_bucket: String,
    pub remote_az_host: String,
    pub remote_az_port: u16,
    pub local_az: String,
    pub remote_az: String,
    #[serde(default)]
    pub ratelimit: RatelimitConfig,
    #[serde(default)]
    pub retry_config: S3RetryConfig,
}

impl Default for S3ExpressMultiAzConfig {
    fn default() -> Self {
        Self {
            local_az_host: "http://127.0.0.1".into(),
            local_az_port: 9001,
            s3_region: "localdev".into(),
            local_az_bucket: "fractalbits-localdev-az1-data-bucket".into(),
            remote_az_bucket: "fractalbits-localdev-az2-data-bucket".into(),
            remote_az_host: "http://127.0.0.1".into(),
            remote_az_port: 9002,
            local_az: "localdev-az1".into(),
            remote_az: "localdev-az2".into(),
            ratelimit: RatelimitConfig::default(),
            retry_config: S3RetryConfig::default(),
        }
    }
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
    pub retry_config: S3RetryConfig,
}

fn default_force_path_style() -> bool {
    true // Default to true for local testing with minio
}

impl Default for S3ExpressSingleAzConfig {
    fn default() -> Self {
        Self {
            s3_host: "http://127.0.0.1".into(),
            s3_port: 9000, // local minio port
            s3_region: "localdev".into(),
            s3_bucket: "fractalbits-bucket".into(),
            az: "localdev-az1".into(),
            force_path_style: true,
            ratelimit: RatelimitConfig::default(),
            retry_config: S3RetryConfig::default(),
        }
    }
}

#[derive(Deserialize, Debug, Clone)]
pub struct HttpsConfig {
    pub enabled: bool,
    pub port: u16,
    pub cert_file: String,
    pub key_file: String,
    pub force_http1_only: bool,
}

impl Default for HttpsConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            port: 8443,
            cert_file: "etc/cert.pem".to_string(),
            key_file: "etc/key.pem".to_string(),
            force_http1_only: false,
        }
    }
}

#[derive(serde::Deserialize, Debug, Clone)]
pub struct Config {
    pub nss_addr: String,
    pub rss_addr: String,
    pub nss_conn_num: u16,
    pub rss_conn_num: u16,

    pub port: u16,
    pub mgmt_port: u16,
    pub https: HttpsConfig,
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
    pub retry_config: S3RetryConfig,
}

impl Default for Config {
    fn default() -> Self {
        Self::hybrid_single_az()
    }
}

impl Config {
    pub fn s3_express_multi_az_with_tracking() -> Self {
        Self {
            nss_addr: "127.0.0.1:8087".to_string(),
            rss_addr: "127.0.0.1:8086".to_string(),
            nss_conn_num: 2,
            rss_conn_num: 1,
            port: 8080,
            mgmt_port: 18080,
            https: HttpsConfig::default(),
            region: "localdev".into(),
            root_domain: ".localhost".into(),
            with_metrics: true,
            http_request_timeout_seconds: 30,
            rpc_timeout_seconds: 10,
            blob_storage: BlobStorageConfig {
                backend: BlobStorageBackend::S3ExpressMultiAzWithTracking,
                bss: None,
                s3_hybrid_single_az: None,
                s3_express_multi_az: Some(S3ExpressMultiAzConfig::default()),
                s3_express_single_az: None,
            },
            allow_missing_or_bad_signature: false,
        }
    }

    pub fn s3_express_multi_az() -> Self {
        Self {
            nss_addr: "127.0.0.1:8087".to_string(),
            rss_addr: "127.0.0.1:8086".to_string(),
            nss_conn_num: 2,
            rss_conn_num: 1,
            port: 8080,
            mgmt_port: 18080,
            https: HttpsConfig::default(),
            region: "localdev".into(),
            root_domain: ".localhost".into(),
            with_metrics: true,
            http_request_timeout_seconds: 30,
            rpc_timeout_seconds: 10,
            blob_storage: BlobStorageConfig {
                backend: BlobStorageBackend::S3ExpressMultiAz,
                bss: None,
                s3_hybrid_single_az: None,
                s3_express_multi_az: Some(S3ExpressMultiAzConfig::default()),
                s3_express_single_az: None,
            },
            allow_missing_or_bad_signature: false,
        }
    }

    pub fn s3_express_single_az() -> Self {
        Self {
            nss_addr: "127.0.0.1:8087".to_string(),
            rss_addr: "127.0.0.1:8086".to_string(),
            nss_conn_num: 2,
            rss_conn_num: 1,
            port: 8080,
            mgmt_port: 18080,
            https: HttpsConfig::default(),
            region: "localdev".into(),
            root_domain: ".localhost".into(),
            with_metrics: true,
            http_request_timeout_seconds: 30,
            rpc_timeout_seconds: 10,
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

    pub fn hybrid_single_az() -> Self {
        Self {
            nss_addr: "127.0.0.1:8087".to_string(),
            rss_addr: "127.0.0.1:8086".to_string(),
            nss_conn_num: 2,
            rss_conn_num: 1,
            port: 8080,
            mgmt_port: 18080,
            https: HttpsConfig::default(),
            region: "localdev".into(),
            root_domain: ".localhost".into(),
            with_metrics: true,
            http_request_timeout_seconds: 30,
            rpc_timeout_seconds: 10,
            blob_storage: BlobStorageConfig {
                backend: BlobStorageBackend::HybridSingleAz,
                bss: Some(BssConfig {
                    addr: "127.0.0.1:8088".to_string(),
                    conn_num: 2,
                }),
                s3_hybrid_single_az: Some(S3HybridConfig {
                    s3_host: "http://127.0.0.1".into(),
                    s3_port: 9000,
                    s3_region: "localdev".into(),
                    s3_bucket: "fractalbits-bucket".into(),
                    ratelimit: RatelimitConfig::default(),
                    retry_config: S3RetryConfig::default(),
                }),
                s3_express_multi_az: None,
                s3_express_single_az: None,
            },
            allow_missing_or_bad_signature: false,
        }
    }
}
