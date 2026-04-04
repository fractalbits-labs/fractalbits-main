use crate::*;

use super::super::common::{DeployTarget, VpcConfig, upload_config_and_blueprint};
use super::super::{bootstrap_progress, upload};
use super::config_gen;

/// Extract a short unique suffix from an OCI OCID for bucket naming.
/// OCIDs have the unique portion at the end; we take the last 12 chars.
pub(super) fn ocid_suffix(ocid: &str) -> &str {
    let len = ocid.len();
    &ocid[len.saturating_sub(12)..len]
}
const TERRAFORM_DIR: &str = "infra/oci-terraform";

pub fn create_vpc(config: VpcConfig) -> CmdResult {
    let tenancy_ocid = super::resolve_oci_tenancy(None)?;
    let compartment_ocid = super::resolve_oci_compartment(config.oci_compartment.as_deref())?;
    let region = super::resolve_oci_region(config.oci_region.as_deref());

    // Resolve availability domain (first AD in the region by default)
    let availability_domain = resolve_availability_domain(&compartment_ocid)?;

    // 1. Upload binaries to OCI Object Storage
    let bucket_name = format!(
        "fractalbits-deploy-staging-{}",
        ocid_suffix(&compartment_ocid)
    );
    if !config.skip_upload {
        info!("Uploading binaries to OCI Object Storage...");
        upload::upload_oci(&compartment_ocid)?;
    }

    // 2. Generate and upload bootstrap config BEFORE Terraform
    info!("Generating bootstrap config (pre-deploy)...");
    let params = config_gen::OciDeployParams {
        tenancy_ocid: &tenancy_ocid,
        compartment_ocid: &compartment_ocid,
        region: &region,
        availability_domain: &availability_domain,
        rss_backend: config.rss_backend,
        rss_ha_enabled: config.root_server_ha,
        num_bss_nodes: config.num_bss_nodes as usize,
        num_api_servers: config.num_api_servers as usize,
        num_bench_clients: config.num_bench_clients as usize,
        with_bench: config.with_bench,
        use_generic_binaries: config.use_generic_binaries,
        bss_storage_alloc_mode: config.bss_storage_alloc_mode,
    };
    let bootstrap_config = config_gen::generate_bootstrap_config(&params)?;
    let config_toml = bootstrap_config
        .to_toml()
        .map_err(|e| std::io::Error::other(format!("Failed to serialize config: {e}")))?;
    let oci_bucket = format!("oci://{bucket_name}");
    upload_config_and_blueprint(&oci_bucket, &config_toml, &bootstrap_config)?;
    info!("Bootstrap config uploaded. Starting Terraform apply...");

    // 3. Terraform init + apply
    let tf_vars = build_terraform_vars(
        &config,
        &tenancy_ocid,
        &compartment_ocid,
        &region,
        &availability_domain,
    );

    let tf_state_bucket = std::env::var("OCI_TF_STATE_BUCKET").unwrap_or_else(|_| {
        format!(
            "fractalbits-tf-state-{}",
            ocid_suffix(&compartment_ocid)
        )
    });
    let namespace = get_oci_namespace()?;

    run_cmd!(
        cd $TERRAFORM_DIR;
        terraform init
            -backend-config="bucket=$tf_state_bucket"
            -backend-config="namespace=$namespace"
            -backend-config="region=$region"
            -backend-config="key=vpc/terraform.tfstate"
            -reconfigure
            -input=false 2>&1
    )?;
    run_cmd!(
        cd $TERRAFORM_DIR;
        terraform apply $[tf_vars] -auto-approve 2>&1
    )?;
    info!("Terraform apply completed");

    // 4. Instances self-bootstrap via cloud-init scripts

    // 5. Watch bootstrap progress via OCI Object Storage
    if config.watch_bootstrap {
        bootstrap_progress::show_progress_with_bucket(DeployTarget::Oci, None, Some(&bucket_name))?;
    } else {
        info!("To monitor bootstrap progress, run: just deploy bootstrap-progress --target oci");
    }

    Ok(())
}

pub fn destroy_vpc(oci_compartment: Option<String>, oci_region: Option<String>) -> CmdResult {
    use colored::*;
    use dialoguer::Input;

    let compartment = super::resolve_oci_compartment(oci_compartment.as_deref())?;
    let region = super::resolve_oci_region(oci_region.as_deref());

    warn!("This will permanently destroy the OCI VCN and all associated resources!");
    warn!("This action cannot be undone.");

    let _confirmation: String = Input::new()
        .with_prompt(format!(
            "Type {} to confirm VCN destruction",
            "permanent destroy".bold()
        ))
        .validate_with(|input: &String| -> Result<(), String> {
            if input == "permanent destroy" {
                Ok(())
            } else {
                Err(format!(
                    "You must type {} exactly to confirm",
                    "permanent destroy".bold()
                ))
            }
        })
        .interact_text()
        .map_err(|e| std::io::Error::other(format!("Failed to read confirmation: {e}")))?;

    let tf_state_bucket = std::env::var("OCI_TF_STATE_BUCKET").unwrap_or_else(|_| {
        format!(
            "fractalbits-tf-state-{}",
            ocid_suffix(&compartment)
        )
    });
    let namespace = get_oci_namespace()?;

    run_cmd! {
        info "Initializing Terraform backend...";
        cd $TERRAFORM_DIR;
        terraform init
            -backend-config="bucket=$tf_state_bucket"
            -backend-config="namespace=$namespace"
            -backend-config="region=$region"
            -backend-config="key=vpc/terraform.tfstate"
            -reconfigure
            -input=false 2>&1;
        info "Destroying Terraform resources...";
        terraform destroy -auto-approve 2>&1;
        info "Terraform destroy completed";
    }?;

    info!("OCI VCN destruction completed");
    Ok(())
}

fn build_terraform_vars(
    config: &VpcConfig,
    tenancy_ocid: &str,
    compartment_ocid: &str,
    region: &str,
    availability_domain: &str,
) -> Vec<String> {
    let mut vars = Vec::new();
    let mut add = |key: &str, value: &str| {
        vars.push("-var".to_string());
        vars.push(format!("{key}={value}"));
    };

    let cluster_id = std::env::var("OCI_CLUSTER_ID").unwrap_or_else(|_| {
        let existing = cmd_lib::run_fun!(
            cd $TERRAFORM_DIR;
            terraform output -raw cluster_id 2>/dev/null
        );
        existing
            .ok()
            .filter(|s| !s.is_empty() && s.chars().all(|c| c.is_ascii_alphanumeric() || c == '-'))
            .unwrap_or_else(|| {
                format!(
                    "{}-{}",
                    chrono::Local::now().format("%Y%m%d-%H%M%S"),
                    &compartment_ocid[..compartment_ocid.len().min(8)]
                )
            })
    });

    add("tenancy_ocid", tenancy_ocid);
    add("compartment_ocid", compartment_ocid);
    add("cluster_id", &cluster_id);
    add("region", region);
    add("availability_domain", availability_domain);
    add("num_api_servers", &config.num_api_servers.to_string());
    add("num_bss_nodes", &config.num_bss_nodes.to_string());
    add("root_server_ha", &config.root_server_ha.to_string());
    add(
        "rss_backend",
        match config.rss_backend {
            RssBackend::Etcd => "etcd",
            RssBackend::Ddb => "ddb",
            RssBackend::Firestore => "firestore",
            RssBackend::OciNosql => "oci_nosql",
        },
    );
    if config.with_bench {
        add("with_bench", "true");
        add("num_bench_clients", &config.num_bench_clients.to_string());
    }
    if let Some(ref template_val) = config.template {
        add("vpc_template", template_val.as_ref());
    }

    // Pass deploy staging bucket name to Terraform for cloud-init templates
    let bucket_name = format!(
        "fractalbits-deploy-staging-{}",
        ocid_suffix(compartment_ocid)
    );
    add("deploy_staging_bucket", &bucket_name);

    vars
}

fn resolve_availability_domain(compartment_ocid: &str) -> Result<String, std::io::Error> {
    if let Ok(ad) = std::env::var("OCI_AVAILABILITY_DOMAIN")
        && !ad.is_empty()
    {
        return Ok(ad);
    }
    let ad = cmd_lib::run_fun!(
        oci iam availability-domain list
            --compartment-id $compartment_ocid
            --query "data[0].name"
            --raw-output 2>/dev/null
    )?;
    if ad.is_empty() {
        return Err(std::io::Error::other(
            "Could not resolve OCI availability domain. Set OCI_AVAILABILITY_DOMAIN env var.",
        ));
    }
    Ok(ad.trim().to_string())
}

fn get_oci_namespace() -> Result<String, std::io::Error> {
    if let Ok(ns) = std::env::var("OCI_NAMESPACE")
        && !ns.is_empty()
    {
        return Ok(ns);
    }
    let ns = cmd_lib::run_fun!(
        oci os ns get --query "data" --raw-output 2>/dev/null
    )?;
    if ns.is_empty() {
        return Err(std::io::Error::other(
            "Could not resolve OCI Object Storage namespace. Set OCI_NAMESPACE env var.",
        ));
    }
    Ok(ns.trim().to_string())
}
