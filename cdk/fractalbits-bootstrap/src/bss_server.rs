use super::common::*;
use cmd_lib::*;

pub fn bootstrap(num_nvme_disks: usize, bench: bool) -> CmdResult {
    assert_ne!(num_nvme_disks, 0);
    install_rpms(&["nvme-cli", "mdadm", "perf", "lldb"])?;
    format_local_nvme_disks(num_nvme_disks)?;
    download_binaries(&["bss_server"])?;

    create_coredump_config()?;
    create_systemd_unit_file("bss_server", true)?;

    if bench {
        download_binaries(&["xtask", "rewrk_rpc"])?;
        run_cmd! {
            cd /data;
            /opt/fractalbits/bin/xtask tool gen_uuids;
        }?;
    }
    Ok(())
}
