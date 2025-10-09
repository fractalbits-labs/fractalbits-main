use super::common::*;
use cmd_lib::*;
use std::io::Error;

pub fn bootstrap(meta_stack_testing: bool, for_bench: bool) -> CmdResult {
    install_rpms(&["nvme-cli", "mdadm"])?;
    format_local_nvme_disks(false)?; // no twp support since experiment is done
    create_ddb_register_and_deregister_service("bss-server")?;
    download_binaries(&["bss_server"])?;

    create_coredump_config()?;

    info!("Creating directories for bss_server");
    run_cmd!(mkdir -p "/data/local/stats")?;

    if meta_stack_testing || for_bench {
        download_binaries(&["rewrk_rpc"])?;
        xtask_tools::gen_uuids(1_000_000, "/data/uuids.data")?;
    }

    create_logrotate_for_stats()?;
    setup_volume_directories()?;
    create_bss_config()?;
    create_systemd_unit_file("bss", true)?;

    run_cmd! {
        info "Syncing file system changes";
        sync;
    }?;

    Ok(())
}

fn setup_volume_directories() -> CmdResult {
    let instance_id = get_instance_id()?;
    info!("BSS instance ID: {}", instance_id);

    match wait_for_volume_configs() {
        Ok(()) => match get_volume_assignments(&instance_id) {
            Ok(assignments) => {
                info!("Volume assignments for {}: {:?}", instance_id, assignments);

                if assignments.has_assignments() {
                    create_assigned_volume_directories(&assignments)?;
                } else {
                    cmd_die!("No volume assignments found for $instance_id");
                }
            }
            Err(e) => {
                cmd_die!("Failed to get volume assignments: $e");
            }
        },
        Err(e) => {
            cmd_die!("Get volume assignments failed: $e");
        }
    }

    Ok(())
}

fn create_bss_config() -> CmdResult {
    let num_threads = run_fun!(nproc)?;
    let config_content = format!(
        r##"working_dir = "/data"
server_port = 8088
num_threads = {num_threads}
log_level = "warn"
use_direct_io = true
"##
    );
    run_cmd! {
        mkdir -p $ETC_PATH;
        echo $config_content > $ETC_PATH/$BSS_SERVER_CONFIG;
    }?;

    Ok(())
}

fn wait_for_volume_configs() -> CmdResult {
    const TIMEOUT_SECS: u64 = 300;
    const POLL_INTERVAL_SECS: u64 = 1;

    info!("Waiting for BSS volume configurations in DDB...");
    let start = std::time::Instant::now();

    while start.elapsed().as_secs() < TIMEOUT_SECS {
        // Check if both configs exist AND contain valid JSON
        if let (Ok(data_config), Ok(metadata_config)) = (
            get_ddb_config(BSS_DATA_VG_CONFIG_KEY),
            get_ddb_config(BSS_METADATA_VG_CONFIG_KEY),
        ) {
            // Verify the configs contain valid JSON
            if serde_json::from_str::<serde_json::Value>(&data_config).is_ok()
                && serde_json::from_str::<serde_json::Value>(&metadata_config).is_ok()
            {
                info!("Volume configurations found in DDB");
                return Ok(());
            }
        }
        std::thread::sleep(std::time::Duration::from_secs(POLL_INTERVAL_SECS));
    }

    Err(Error::other("Timeout waiting for volume configs"))
}

fn get_ddb_config(service_key: &str) -> FunResult {
    let key = format!(r#"{{"service_id":{{"S":"{}"}}}}"#, service_key);
    let result = run_fun!(
        aws dynamodb get-item
            --table-name $DDB_SERVICE_DISCOVERY_TABLE
            --key $key
            --query "Item.value.S"
            --output text
            2>/dev/null
    )?;

    if result.is_empty() || result == "None" || result == "null" {
        return Err(Error::other(format!(
            "Config {} not found in DDB",
            service_key
        )));
    }

    Ok(result)
}

#[derive(Debug, Default)]
struct VolumeAssignments {
    data_volume: Option<usize>,
    metadata_volume: Option<usize>,
}

impl VolumeAssignments {
    fn has_assignments(&self) -> bool {
        self.data_volume.is_some() || self.metadata_volume.is_some()
    }
}

fn get_volume_assignments(instance_id: &str) -> Result<VolumeAssignments, Error> {
    let mut assignments = VolumeAssignments::default();

    let data_config = get_ddb_config(BSS_DATA_VG_CONFIG_KEY)?;
    assignments.data_volume = find_volume_for_instance(&data_config, instance_id)?;

    let metadata_config = get_ddb_config(BSS_METADATA_VG_CONFIG_KEY)?;
    assignments.metadata_volume = find_volume_for_instance(&metadata_config, instance_id)?;

    Ok(assignments)
}

fn find_volume_for_instance(config_json: &str, instance_id: &str) -> Result<Option<usize>, Error> {
    let config: serde_json::Value = serde_json::from_str(config_json)
        .map_err(|e| Error::other(format!("Failed to parse config: {}", e)))?;

    if let Some(volumes) = config["volumes"].as_array() {
        for volume in volumes {
            if let Some(nodes) = volume["bss_nodes"].as_array() {
                for node in nodes {
                    if node["node_id"].as_str() == Some(instance_id) {
                        return Ok(volume["volume_id"].as_u64().map(|v| v as usize));
                    }
                }
            }
        }
    }

    Ok(None)
}

fn create_assigned_volume_directories(assignments: &VolumeAssignments) -> CmdResult {
    if let Some(vol_id) = assignments.data_volume {
        create_volume_directories("data", vol_id)?;
    } else {
        info!("No data volume assigned");
    }

    if let Some(vol_id) = assignments.metadata_volume {
        create_volume_directories("metadata", vol_id)?;
    } else {
        info!("No metadata volume assigned");
    }

    Ok(())
}

fn create_volume_directories(volume_type: &str, volume_id: usize) -> CmdResult {
    info!("Creating {} volume {} directories", volume_type, volume_id);

    let base_dir = format!("/data/local/blobs/{}_volume{}", volume_type, volume_id);
    run_cmd!(mkdir -p $base_dir)?;

    for i in 0..256 {
        run_cmd!(mkdir -p $base_dir/dir$i)?;
    }

    Ok(())
}
