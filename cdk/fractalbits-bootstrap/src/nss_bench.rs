use super::{common::*, nss_server};
use cmd_lib::*;

pub fn bootstrap(bucket_name: &str, volume_id: &str, num_nvme_disks: usize) -> CmdResult {
    assert_ne!(num_nvme_disks, 0);
    install_rpms(&["nvme-cli", "mdadm", "perf", "lldb"])?;
    format_local_nvme_disks(num_nvme_disks)?;
    create_coredump_config()?;

    for bin in [
        "nss_server",
        "mkfs",
        "fbs",
        "test_art",
        "rewrk_rpc",
        "format-ebs",
    ] {
        download_binary(bin)?;
    }

    let service_name = "nss_bench";
    nss_server::setup_configs(bucket_name, volume_id, service_name)?;

    let volume_dev = get_volume_dev(volume_id);
    run_cmd! {
        cd /data;

        info "Generating random 10_000_000 keys";
        /opt/fractalbits/bin/test_art --gen --size 10000000;

        info "Formatting EBS: $volume_dev (see detailed logs with `journalctl _COMM=format-ebs`)";
        /opt/fractalbits/bin/format-ebs --dev_mode --ebs_dev $volume_dev;
    }?;
    Ok(())
}
