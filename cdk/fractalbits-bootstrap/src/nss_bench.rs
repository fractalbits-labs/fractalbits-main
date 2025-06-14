use super::common::*;
use cmd_lib::*;

const TEST_BUCKET_ROOT_BLOB_NAME: &str = "947ef2be-44b2-4ac2-969b-2574eb85662b";

pub fn bootstrap(bucket_name: &str, volume_id: &str, num_nvme_disks: usize) -> CmdResult {
    assert_ne!(num_nvme_disks, 0);
    install_rpms()?;
    format_local_nvme_disks(num_nvme_disks)?;
    create_coredump_conf()?;

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
    super::nss_server::create_nss_config(bucket_name)?;
    create_systemd_unit_file(service_name)?;

    let ebs_dev = format! {
        "/dev/disk/by-id/nvme-Amazon_Elastic_Block_Store_{}",
        volume_id.replace("-", "")
    };
    run_cmd! {
        info "Formatting EBS: $ebs_dev (see detailed logs with `journalctl _COMM=format-ebs`)";
        /opt/fractalbits/bin/format-ebs $ebs_dev;

        cd /data;

        info "Running nss mkfs";
        /opt/fractalbits/bin/mkfs;

        info "Running nss fbs";
        /opt/fractalbits/bin/fbs --new_tree $TEST_BUCKET_ROOT_BLOB_NAME;

        info "Generating random 10_000_000 keys";
        /opt/fractalbits/bin/test_art --gen --size 10000000;

        info "Starting ${service_name}.service";
        systemctl enable --now ${service_name}.service;
    }?;
    Ok(())
}

fn install_rpms() -> CmdResult {
    let rpms = ["nvme-cli", "mdadm", "gdb", "perf"];
    run_cmd! {
        info "Installing ${rpms:?}";
        yum install -y -q $[rpms] >/dev/null;
    }?;

    Ok(())
}
