use serde::Deserialize;
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ServerMode {
    #[default]
    Fuse,
    Nfs,
}

#[derive(Deserialize, Debug, Clone)]
pub struct Config {
    pub rss_addrs: Vec<String>,
    pub bucket_name: String,
    pub mount_point: String,

    pub nfs_port: u16,

    pub rpc_request_timeout_seconds: u64,
    pub rpc_connection_timeout_seconds: u64,
    pub rss_rpc_timeout_seconds: u64,
    #[allow(dead_code)]
    pub worker_threads: usize,
    pub allow_other: bool,
    #[allow(dead_code)]
    pub auto_unmount: bool,

    pub dir_cache_ttl_seconds: u64,
    #[allow(dead_code)]
    pub attr_cache_ttl_seconds: u64,
    #[allow(dead_code)]
    pub block_cache_size_mb: u64,
    pub read_write: bool,

    pub disk_cache_enabled: bool,
    pub disk_cache_path: String,
    pub disk_cache_size_gb: u64,
    pub passthrough_enabled: bool,
    pub passthrough_max_object_size_gb: u64,
}

impl Config {
    pub fn rpc_request_timeout(&self) -> Duration {
        Duration::from_secs(self.rpc_request_timeout_seconds)
    }

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

    /// Override config fields from FS_SERVER_* environment variables.
    pub fn apply_env_overrides(&mut self) {
        if let Ok(v) = std::env::var("FS_SERVER_BUCKET_NAME") {
            self.bucket_name = v;
        }
        if let Ok(v) = std::env::var("FS_SERVER_MOUNT_POINT") {
            self.mount_point = v;
        }
        if let Ok(v) = std::env::var("FS_SERVER_READ_WRITE") {
            self.read_write = v.parse().unwrap_or(self.read_write);
        }
        if let Ok(v) = std::env::var("FS_SERVER_DISK_CACHE_ENABLED") {
            self.disk_cache_enabled = v.parse().unwrap_or(self.disk_cache_enabled);
        }
        if let Ok(v) = std::env::var("FS_SERVER_DISK_CACHE_PATH") {
            self.disk_cache_path = v;
        }
        if let Ok(v) = std::env::var("FS_SERVER_DISK_CACHE_SIZE_GB") {
            self.disk_cache_size_gb = v.parse().unwrap_or(self.disk_cache_size_gb);
        }
        if let Ok(v) = std::env::var("FS_SERVER_NFS_PORT") {
            self.nfs_port = v.parse().unwrap_or(self.nfs_port);
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            rss_addrs: vec!["127.0.0.1:8086".to_string()],
            bucket_name: "default".to_string(),
            mount_point: "/mnt/fractalbits".to_string(),
            rpc_request_timeout_seconds: 30,
            rpc_connection_timeout_seconds: 5,
            rss_rpc_timeout_seconds: 30,
            worker_threads: 2,
            allow_other: false,
            auto_unmount: false,
            dir_cache_ttl_seconds: 5,
            attr_cache_ttl_seconds: 5,
            block_cache_size_mb: 256,
            nfs_port: 2049,
            read_write: false,
            disk_cache_enabled: false,
            disk_cache_path: "/var/cache/fractalbits/".to_string(),
            disk_cache_size_gb: 50,
            passthrough_enabled: false,
            passthrough_max_object_size_gb: 10,
        }
    }
}
