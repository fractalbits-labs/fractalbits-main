use std::fs;
use std::path::PathBuf;

use serde::Deserialize;

#[derive(Deserialize, Debug, Clone)]
pub struct Config {
    #[serde(default = "default_nss_addr")]
    pub nss_addr: String,
    #[serde(default = "default_bss_addr")]
    pub bss_addr: String,
    #[serde(default = "default_rss_addr")]
    pub rss_addr: String,

    #[serde(default = "default_port")]
    pub port: u16,
}

fn default_nss_addr() -> String {
    "127.0.0.1:9224".into()
}

fn default_bss_addr() -> String {
    "127.0.0.1:9225".into()
}

fn default_rss_addr() -> String {
    "127.0.0.1:8888".into()
}

fn default_port() -> u16 {
    3000
}

impl Default for Config {
    fn default() -> Self {
        toml::from_str("").unwrap()
    }
}
pub fn read_config(config_file: PathBuf) -> Config {
    let config = fs::read_to_string(config_file).unwrap();

    toml::from_str(&config).unwrap()
}
