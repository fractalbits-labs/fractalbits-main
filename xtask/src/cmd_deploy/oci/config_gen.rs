use std::collections::HashMap;
use std::io::Error;

use chrono::Utc;
use uuid::Uuid;
use xtask_common::{
    BootstrapClusterConfig, ClusterEtcdConfig, ClusterGlobalConfig, ClusterOciConfig,
    DataBlobStorage,
};

pub struct OciDeployParams<'a> {
    pub tenancy_ocid: &'a str,
    pub compartment_ocid: &'a str,
    pub region: &'a str,
    pub availability_domain: &'a str,
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
/// Only static parameters are included -- no instance names/IPs, no NSS endpoint,
/// no per-node data. Each instance gets its role via --role in its cloud-init script.
pub fn generate_bootstrap_config(
    params: &OciDeployParams,
) -> Result<BootstrapClusterConfig, Error> {
    let workflow_cluster_id = Utc::now().format("%Y%m%d-%H%M%S").to_string();
    let journal_uuid = Uuid::now_v7().to_string();

    let oci_config = ClusterOciConfig {
        tenancy_ocid: params.tenancy_ocid.to_string(),
        compartment_ocid: params.compartment_ocid.to_string(),
        region: params.region.to_string(),
        availability_domain: params.availability_domain.to_string(),
        remote_ad: None,
        vcn_id: String::new(),
        subnet_id: String::new(),
        nosql_compartment: Some(params.compartment_ocid.to_string()),
    };

    let config = BootstrapClusterConfig {
        global: ClusterGlobalConfig {
            deploy_target: xtask_common::DeployTarget::Oci,
            region: params.region.to_string(),
            for_bench: params.with_bench,
            data_blob_storage: DataBlobStorage::AllInBssSingleAz,
            rss_ha_enabled: params.rss_ha_enabled,
            rss_backend: params.rss_backend,
            // OCI uses Block Volume which is handled via the Ebs code path.
            journal_type: xtask_common::JournalType::Ebs,
            num_nss_nodes: Some(2),
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
        oci: Some(oci_config),
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
        bootstrap_bucket: format!(
            "fractalbits-deploy-staging-{}",
            super::vpc::ocid_suffix(params.compartment_ocid)
        ),
    };

    Ok(config)
}
