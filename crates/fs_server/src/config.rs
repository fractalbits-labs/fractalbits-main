use serde::Deserialize;
use std::time::Duration;

#[derive(Deserialize, Debug, Clone)]
pub struct Config {
    pub rss_addrs: Vec<String>,
    pub bucket_name: String,
    pub mount_point: String,

    #[serde(default = "default_rpc_request_timeout")]
    pub rpc_request_timeout_seconds: u64,
    #[serde(default = "default_rpc_connection_timeout")]
    #[allow(dead_code)]
    pub rpc_connection_timeout_seconds: u64,
    #[serde(default = "default_rss_rpc_timeout")]
    pub rss_rpc_timeout_seconds: u64,
    #[serde(default = "default_worker_threads")]
    #[allow(dead_code)]
    pub worker_threads: usize,
    #[serde(default)]
    pub allow_other: bool,
    #[serde(default)]
    #[allow(dead_code)]
    pub auto_unmount: bool,

    #[serde(default = "default_dir_cache_ttl")]
    pub dir_cache_ttl_seconds: u64,
    #[serde(default = "default_attr_cache_ttl")]
    #[allow(dead_code)]
    pub attr_cache_ttl_seconds: u64,
    #[serde(default = "default_block_cache_size_mb")]
    pub block_cache_size_mb: u64,
    #[serde(default)]
    pub read_write: bool,
}

fn default_rpc_request_timeout() -> u64 {
    30
}
fn default_rpc_connection_timeout() -> u64 {
    5
}
fn default_rss_rpc_timeout() -> u64 {
    30
}
fn default_worker_threads() -> usize {
    2
}
fn default_dir_cache_ttl() -> u64 {
    5
}
fn default_attr_cache_ttl() -> u64 {
    5
}
fn default_block_cache_size_mb() -> u64 {
    256
}

impl Config {
    pub fn rpc_request_timeout(&self) -> Duration {
        Duration::from_secs(self.rpc_request_timeout_seconds)
    }

    #[allow(dead_code)]
    pub fn rpc_connection_timeout(&self) -> Duration {
        Duration::from_secs(self.rpc_connection_timeout_seconds)
    }

    pub fn rss_rpc_timeout(&self) -> Duration {
        Duration::from_secs(self.rss_rpc_timeout_seconds)
    }

    pub fn dir_cache_ttl(&self) -> Duration {
        Duration::from_secs(self.dir_cache_ttl_seconds)
    }

    #[allow(dead_code)]
    pub fn attr_cache_ttl(&self) -> Duration {
        Duration::from_secs(self.attr_cache_ttl_seconds)
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            rss_addrs: vec!["127.0.0.1:8086".to_string()],
            bucket_name: "default".to_string(),
            mount_point: "/mnt/fractalbits".to_string(),
            rpc_request_timeout_seconds: default_rpc_request_timeout(),
            rpc_connection_timeout_seconds: default_rpc_connection_timeout(),
            rss_rpc_timeout_seconds: default_rss_rpc_timeout(),
            worker_threads: default_worker_threads(),
            allow_other: false,
            auto_unmount: false,
            dir_cache_ttl_seconds: default_dir_cache_ttl(),
            attr_cache_ttl_seconds: default_attr_cache_ttl(),
            block_cache_size_mb: default_block_cache_size_mb(),
            read_write: false,
        }
    }
}
