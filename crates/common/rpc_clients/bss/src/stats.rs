use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tokio::task::JoinHandle;
use tracing::{debug, error, warn};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OperationType {
    PutData,
    GetData,
    DeleteData,
    PutMeta,
    GetMeta,
    DeleteMeta,
}

impl OperationType {
    pub fn as_str(&self) -> &'static str {
        match self {
            OperationType::PutData => "put_data",
            OperationType::GetData => "get_data",
            OperationType::DeleteData => "del_data",
            OperationType::PutMeta => "put_meta",
            OperationType::GetMeta => "get_meta",
            OperationType::DeleteMeta => "del_meta",
        }
    }

    pub fn all() -> [OperationType; 6] {
        [
            OperationType::PutData,
            OperationType::GetData,
            OperationType::DeleteData,
            OperationType::PutMeta,
            OperationType::GetMeta,
            OperationType::DeleteMeta,
        ]
    }

    pub fn data_only() -> [OperationType; 3] {
        [
            OperationType::PutData,
            OperationType::GetData,
            OperationType::DeleteData,
        ]
    }
}

#[derive(Default)]
pub struct BssNodeStats {
    operations: DashMap<OperationType, AtomicU64>,
}

impl BssNodeStats {
    pub fn new() -> Self {
        let operations = DashMap::new();
        for op in OperationType::all() {
            operations.insert(op, AtomicU64::new(0));
        }
        Self { operations }
    }

    pub fn increment(&self, op: OperationType) {
        if let Some(counter) = self.operations.get(&op) {
            counter.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn decrement(&self, op: OperationType) {
        if let Some(counter) = self.operations.get(&op) {
            counter.fetch_sub(1, Ordering::Relaxed);
        }
    }

    pub fn get_count(&self, op: OperationType) -> u64 {
        self.operations
            .get(&op)
            .map(|counter| counter.load(Ordering::Relaxed))
            .unwrap_or(0)
    }
}

pub struct BssStatsRegistry {
    nodes: Arc<DashMap<String, Arc<BssNodeStats>>>,
}

impl Default for BssStatsRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl BssStatsRegistry {
    pub fn new() -> Self {
        Self {
            nodes: Arc::new(DashMap::new()),
        }
    }

    pub fn register_node(&self, address: String) -> Arc<BssNodeStats> {
        self.nodes
            .entry(address.clone())
            .or_insert_with(|| {
                debug!("Registering BSS node for stats: {}", address);
                Arc::new(BssNodeStats::new())
            })
            .clone()
    }

    pub fn get_node_stats(&self, address: &str) -> Option<Arc<BssNodeStats>> {
        self.nodes.get(address).map(|entry| entry.value().clone())
    }

    pub fn get_all_nodes(&self) -> Vec<(String, Arc<BssNodeStats>)> {
        self.nodes
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect()
    }
}

static GLOBAL_STATS_REGISTRY: OnceLock<BssStatsRegistry> = OnceLock::new();

pub fn get_global_registry() -> &'static BssStatsRegistry {
    GLOBAL_STATS_REGISTRY.get_or_init(BssStatsRegistry::new)
}

pub struct BssStatsWriter {
    stats_file_path: String,
    shutdown_tx: Option<tokio::sync::oneshot::Sender<()>>,
    task_handle: Option<JoinHandle<()>>,
}

impl BssStatsWriter {
    pub fn new(stats_dir: String) -> Self {
        let stats_file_path = format!("{}/bss_client.stats", stats_dir);
        Self {
            stats_file_path,
            shutdown_tx: None,
            task_handle: None,
        }
    }

    pub async fn start(&mut self) -> Result<(), std::io::Error> {
        std::fs::create_dir_all(
            std::path::Path::new(&self.stats_file_path)
                .parent()
                .unwrap(),
        )?;

        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        self.shutdown_tx = Some(shutdown_tx);

        let stats_file_path = self.stats_file_path.clone();
        let task_handle = tokio::spawn(async move {
            if let Err(e) = Self::stats_writer_task(stats_file_path, shutdown_rx).await {
                error!("BSS stats writer task failed: {}", e);
            }
        });

        self.task_handle = Some(task_handle);
        debug!(
            "BSS stats writer started, writing to {}",
            self.stats_file_path
        );
        Ok(())
    }

    async fn stats_writer_task(
        stats_file_path: String,
        mut shutdown_rx: tokio::sync::oneshot::Receiver<()>,
    ) -> Result<(), std::io::Error> {
        use std::io::Write;

        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&stats_file_path)?;

        let mut interval = tokio::time::interval(Duration::from_secs(1));
        let mut lines = 0usize;

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let registry = get_global_registry();
                    let all_nodes = registry.get_all_nodes();

                    if all_nodes.is_empty() {
                        continue;
                    }

                    if lines.is_multiple_of(30) {
                        let mut header = format!("{:<22} ", "Time");
                        for (address, _) in &all_nodes {
                            let short_addr = address.replace(":", "_");
                            for op in OperationType::data_only() {
                                header.push_str(&format!("{}_{} ", short_addr, op.as_str()));
                            }
                        }
                        writeln!(file, "{}", header.trim_end())?;
                        file.flush()?;
                    }

                    lines += 1;

                    let now = chrono::Local::now();
                    let timestamp = now.format("%Y-%m-%dT%H:%M:%S");

                    let mut line = format!("{:<22} ", timestamp);
                    for (_, stats) in &all_nodes {
                        for op in OperationType::data_only() {
                            let count = stats.get_count(op);
                            line.push_str(&format!("{:<16}", count));
                        }
                    }

                    writeln!(file, "{}", line.trim_end())?;
                }
                _ = &mut shutdown_rx => {
                    debug!("BSS stats writer shutting down");
                    break;
                }
            }
        }

        file.flush()?;
        Ok(())
    }

    pub async fn stop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        if let Some(handle) = self.task_handle.take() {
            let _ = handle.await;
        }
    }
}

impl Drop for BssStatsWriter {
    fn drop(&mut self) {
        if self.shutdown_tx.is_some() {
            warn!("BssStatsWriter dropped without calling stop()");
        }
    }
}

pub async fn init_stats_writer(stats_dir: String) -> Result<BssStatsWriter, std::io::Error> {
    let mut writer = BssStatsWriter::new(stats_dir);
    writer.start().await?;
    Ok(writer)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stats_registry() {
        let registry = BssStatsRegistry::new();

        let node1 = "127.0.0.1:8001";
        let node2 = "127.0.0.1:8002";

        let stats1 = registry.register_node(node1.to_string());
        let stats2 = registry.register_node(node2.to_string());

        stats1.increment(OperationType::PutData);
        stats1.increment(OperationType::PutData);
        stats1.increment(OperationType::GetData);

        stats2.increment(OperationType::GetData);
        stats2.increment(OperationType::DeleteData);

        assert_eq!(stats1.get_count(OperationType::PutData), 2);
        assert_eq!(stats1.get_count(OperationType::GetData), 1);
        assert_eq!(stats2.get_count(OperationType::GetData), 1);
        assert_eq!(stats2.get_count(OperationType::DeleteData), 1);

        let all_nodes = registry.get_all_nodes();
        assert_eq!(all_nodes.len(), 2);
    }

    #[test]
    fn test_get_global_registry() {
        let registry = get_global_registry();
        let node = "127.0.0.1:9001";
        let stats = registry.register_node(node.to_string());
        stats.increment(OperationType::PutMeta);

        assert_eq!(stats.get_count(OperationType::PutMeta), 1);
    }

    #[tokio::test]
    async fn test_stats_writer_initialization() {
        let temp_dir = tempfile::tempdir().unwrap();
        let stats_dir = temp_dir.path().to_str().unwrap().to_string();

        let mut writer = BssStatsWriter::new(stats_dir.clone());
        writer.start().await.unwrap();

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        let stats_file = format!("{}/bss_client.stats", stats_dir);
        assert!(std::path::Path::new(&stats_file).exists());

        writer.stop().await;
    }
}
