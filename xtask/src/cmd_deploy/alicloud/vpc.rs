use crate::*;

use super::super::common::{DeployTarget, VpcConfig, upload_config_and_blueprint};
use super::super::{bootstrap_progress, upload};
use super::config_gen;

const TERRAFORM_DIR: &str = "infra/alicloud-terraform";

pub fn create_vpc(config: VpcConfig) -> CmdResult {
    let region = super::resolve_alicloud_region(config.alicloud_region.as_deref());
    let zone_id = super::resolve_alicloud_zone(config.alicloud_zone.as_deref(), &region);

    // 1. Upload binaries directly to OSS
    let oss_bucket = format!("fractalbits-deploy-{region}");
    if !config.skip_upload {
        info!("Uploading binaries to OSS...");
        upload::upload_alicloud(&region)?;
    }

    // 2. Generate and upload bootstrap config BEFORE Terraform so instances find it immediately on boot
    info!("Generating bootstrap config (pre-deploy)...");

    // Terraform outputs are not available yet; use placeholder values.
    // The actual VPC/vSwitch/SG IDs will be populated by Terraform in the startup scripts.
    let params = config_gen::AlicloudDeployParams {
        region: &region,
        zone_id: &zone_id,
        vpc_id: "",
        vswitch_id: "",
        security_group_id: "",
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
    let oss_bucket_uri = format!("oss://{oss_bucket}");
    upload_config_and_blueprint(&oss_bucket_uri, &config_toml, &bootstrap_config)?;
    info!("Bootstrap config uploaded. Starting Terraform apply...");

    // 3. Terraform init + apply
    let tf_vars = build_terraform_vars(&config, &region, &zone_id);
    let tf_state_bucket = std::env::var("ALICLOUD_TF_STATE_BUCKET")
        .unwrap_or_else(|_| format!("fractalbits-tfstate-{region}"));
    run_cmd!(
        cd $TERRAFORM_DIR;
        terraform init
            -backend-config="bucket=$tf_state_bucket"
            -backend-config="prefix=vpc"
            -backend-config="region=$region"
            -reconfigure
            -input=false 2>&1
    )?;
    run_cmd!(
        cd $TERRAFORM_DIR;
        terraform apply $[tf_vars] -auto-approve 2>&1
    )?;
    info!("Terraform apply completed");

    // 4. Instances self-bootstrap via startup scripts (download binary from OSS)

    // 5. Watch bootstrap progress via OSS
    if config.watch_bootstrap {
        bootstrap_progress::show_progress(DeployTarget::Alicloud, None)?;
    } else {
        info!(
            "To monitor bootstrap progress, run: just deploy bootstrap-progress --target alicloud"
        );
    }

    Ok(())
}

pub fn destroy_vpc(alicloud_region: Option<String>, alicloud_zone: Option<String>) -> CmdResult {
    use colored::*;
    use dialoguer::Input;

    let region = super::resolve_alicloud_region(alicloud_region.as_deref());
    let _zone = super::resolve_alicloud_zone(alicloud_zone.as_deref(), &region);

    warn!("This will permanently destroy the Alicloud VPC and all associated resources!");
    warn!("This action cannot be undone.");

    let _confirmation: String = Input::new()
        .with_prompt(format!(
            "Type {} to confirm VPC destruction",
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

    run_cmd! {
        info "Destroying Terraform resources...";
        cd $TERRAFORM_DIR;
        terraform destroy -auto-approve 2>&1;
        info "Terraform destroy completed";
    }?;

    info!("Alicloud VPC destruction completed");

    Ok(())
}

fn build_terraform_vars(config: &VpcConfig, region: &str, zone_id: &str) -> Vec<String> {
    let mut vars = Vec::new();
    let mut add = |key: &str, value: &str| {
        vars.push("-var".to_string());
        vars.push(format!("{key}={value}"));
    };

    add("region", region);
    add("zone_id", zone_id);
    add("num_api_servers", &config.num_api_servers.to_string());
    add("num_bss_nodes", &config.num_bss_nodes.to_string());
    add("root_server_ha", &config.root_server_ha.to_string());
    add(
        "rss_backend",
        match config.rss_backend {
            RssBackend::Etcd => "etcd",
            RssBackend::Ddb => "ddb",
            RssBackend::Firestore => "firestore",
        },
    );
    if config.with_bench {
        add("with_bench", "true");
        add("num_bench_clients", &config.num_bench_clients.to_string());
    }
    if let Some(ref template_val) = config.template {
        add("vpc_template", template_val.as_ref());
    }

    vars
}
