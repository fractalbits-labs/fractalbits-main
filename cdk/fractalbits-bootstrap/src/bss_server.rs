use super::common::*;
use cmd_lib::*;

pub fn bootstrap(
    bss_service_id: Option<String>,
    meta_stack_testing: bool,
    for_bench: bool,
) -> CmdResult {
    install_rpms(&["nvme-cli", "mdadm", "perf", "lldb"])?;
    // no twp support since experiment done
    format_local_nvme_disks(false)?;
    download_binaries(&["bss_server"])?;

    create_coredump_config()?;

    info!("Creating directories for bss_server");
    for i in 0..256 {
        run_cmd!(mkdir -p /data/local/bss/dir$i)?;
    }

    create_bss_config()?;
    create_systemd_unit_file("bss_server", true)?;

    if meta_stack_testing || for_bench {
        download_binaries(&["rewrk_rpc"])?;
        xtask_tools::gen_uuids(1_000_000, "/data/uuids.data")?;
    }

    run_cmd! {
        info "Syncing file system changes";
        sync;
    }?;

    if let Some(bss_service_id) = bss_service_id {
        register_service(&bss_service_id)?;
    }

    Ok(())
}

fn create_bss_config() -> CmdResult {
    let num_threads = run_fun!(nproc)?;
    let config_content = format!(
        r##"server_port = 8088
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
