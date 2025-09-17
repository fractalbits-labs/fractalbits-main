#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DataVgInfo {
    pub volumes: Vec<DataVolume>,
    pub quorum: Option<QuorumConfig>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DataVolume {
    pub volume_id: u16,
    pub bss_nodes: Vec<BssNode>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct BssNode {
    pub node_id: String,
    pub ip: String,
    pub port: u16,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct QuorumConfig {
    pub n: u32,
    pub r: u32,
    pub w: u32,
}
