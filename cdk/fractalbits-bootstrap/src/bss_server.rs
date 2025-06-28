use super::common::*;
use cmd_lib::*;

pub fn bootstrap(num_nvme_disks: usize, bench: bool) -> CmdResult {
    assert_ne!(num_nvme_disks, 0);
    install_rpms(&["nvme-cli", "mdadm", "perf", "lldb"])?;
    format_local_nvme_disks(num_nvme_disks)?;
    download_binaries(&["bss_server"])?;

    create_coredump_config()?;

    info!("Creating directories for bss_server");
    for i in 0..256 {
        run_cmd!(mkdir -p /data/local/bss/dir$i)?;
    }

    create_systemd_unit_file("bss_server", true)?;

    if bench {
        download_binaries(&["rewrk_rpc"])?;
        xtask_tools::gen_uuids(1_000_000, "/data/uuids.data")?;
    }

    run_cmd! {
        info "Syncing file system changes";
        sync;
    }?;

    Ok(())
}
