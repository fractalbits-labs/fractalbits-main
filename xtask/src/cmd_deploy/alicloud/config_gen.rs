use std::collections::HashMap;
use std::io::Error;

use chrono::Utc;
use uuid::Uuid;
use xtask_common::{
    BootstrapClusterConfig, ClusterAlicloudConfig, ClusterEtcdConfig, ClusterGlobalConfig,
    DataBlobStorage,
};

pub struct AlicloudDeployParams<'a> {
    pub region: &'a str,
    pub zone_id: &'a str,
    pub vpc_id: &'a str,
    pub vswitch_id: &'a str,
    pub security_group_id: &'a str,
    pub rss_backend: xtask_common::RssBackend,
    pub rss_ha_enabled: bool,
    pub num_bss_nodes: usize,
    pub num_api_servers: usize,
    pub num_bench_clients: usize,
    pub with_bench: bool,
    pub use_generic_binaries: bool,
    pub bss_storage_alloc_mode: xtask_common::BssStorageAllocMode,
}

/// Generate a global-only BootstrapClusterConfig before Terraform apply.
///
/// Only static parameters are included -- no instance IDs/IPs, no NSS endpoint,
/// no per-node data. Each instance gets its role via --role in its startup script.
pub fn generate_bootstrap_config(
    params: &AlicloudDeployParams,
) -> Result<BootstrapClusterConfig, Error> {
    let workflow_cluster_id = Utc::now().format("%Y%m%d-%H%M%S").to_string();

    // Pre-generate a cluster-scoped journal UUID for NSS (embedded in startup script)
    let journal_uuid = Uuid::now_v7().to_string();

    let alicloud_config = ClusterAlicloudConfig {
        region: params.region.to_string(),
        zone_id: params.zone_id.to_string(),
        vpc_id: params.vpc_id.to_string(),
        vswitch_id: params.vswitch_id.to_string(),
        security_group_id: params.security_group_id.to_string(),
        polardb_ddb_endpoint: None,
    };

    let config = BootstrapClusterConfig {
        global: ClusterGlobalConfig {
            deploy_target: xtask_common::DeployTarget::Alicloud,
            region: params.region.to_string(),
            for_bench: params.with_bench,
            data_blob_storage: DataBlobStorage::AllInBssSingleAz,
            rss_ha_enabled: params.rss_ha_enabled,
            rss_backend: params.rss_backend,
            // Alicloud uses cloud disk (ESSD) which is handled via the Ebs code path.
            journal_type: xtask_common::JournalType::Ebs,
            num_nss_nodes: Some(2), // Terraform always creates nss-a and nss-b unconditionally
            num_bss_nodes: Some(params.num_bss_nodes),
            num_api_servers: Some(params.num_api_servers),
            num_bench_clients: if params.with_bench {
                Some(params.num_bench_clients)
            } else {
                None
            },
            workflow_cluster_id: Some(workflow_cluster_id),
            meta_stack_testing: false,
            use_generic_binaries: params.use_generic_binaries,
            journal_uuid: Some(journal_uuid),
            bss_storage_alloc_mode: params.bss_storage_alloc_mode,
        },
        aws: None,
        gcp: None,
        alicloud: Some(alicloud_config),
        endpoints: None,
        resources: None,
        etcd: if params.rss_backend == xtask_common::RssBackend::Etcd {
            Some(ClusterEtcdConfig {
                enabled: true,
                cluster_size: params.num_bss_nodes,
                endpoints: None,
            })
        } else {
            None
        },
        nodes: HashMap::new(),
        bootstrap_bucket: format!("fractalbits-deploy-{}", params.region),
    };

    Ok(config)
}
