use super::{common::*, nss_server};
use cmd_lib::*;

pub fn bootstrap(bucket_name: &str, volume_id: &str, num_nvme_disks: usize) -> CmdResult {
    assert_ne!(num_nvme_disks, 0);

    install_rpms(&["nvme-cli", "mdadm", "perf", "lldb"])?;
    format_local_nvme_disks(num_nvme_disks)?;
    download_binaries(&[
        "nss_server",
        "mkfs",
        "fbs",
        "test_art",
        "rewrk_rpc",
        "format-nss",
    ])?;
    nss_server::setup_configs(bucket_name, volume_id, "nss_bench")?;

    let volume_dev = get_volume_dev(volume_id);
    run_cmd! {
        info "Formatting nss with ebs $volume_dev (see detailed logs with `journalctl _COMM=format-nss`)";
        /opt/fractalbits/bin/format-nss --dev_mode --ebs_dev $volume_dev;
    }?;
    Ok(())
}
