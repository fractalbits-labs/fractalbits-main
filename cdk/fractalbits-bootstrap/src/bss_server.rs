use super::common::*;
use cmd_lib::*;

pub fn bootstrap(meta_stack_testing: bool, _for_bench: bool) -> CmdResult {
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

    if meta_stack_testing {
        download_binaries(&["rewrk_rpc"])?;
        xtask_tools::gen_uuids(1_000_000, "/data/uuids.data")?;
    }

    run_cmd! {
        info "Syncing file system changes";
        sync;
    }?;

    Ok(())
}

fn create_bss_config() -> CmdResult {
    let num_threads = run_fun!(nproc)?;
    let config_content = format!(
        r##"server_port = 8088
num_threads = {num_threads}
log_level= "warn""##
    );
    run_cmd! {
        mkdir -p $ETC_PATH;
        echo $config_content > $ETC_PATH/$BSS_SERVER_CONFIG;
    }?;
    Ok(())
}
