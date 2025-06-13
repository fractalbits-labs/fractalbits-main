use super::common::*;
use cmd_lib::*;

const TEST_BUCKET_ROOT_BLOB_NAME: &str = "947ef2be-44b2-4ac2-969b-2574eb85662b";

pub fn bootstrap(volume_id: &str, num_nvme_disks: usize) -> CmdResult {
    assert_ne!(num_nvme_disks, 0);
    format_local_nvme_disks(num_nvme_disks)?;

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
    create_nss_bench_config()?;
    create_systemd_unit_file(service_name)?;

    let ebs_dev = format! {
        "/dev/disk/by-id/nvme-Amazon_Elastic_Block_Store_{}",
        volume_id.replace("-", "")
    };
    run_cmd! {
        info "Formatting EBS: $ebs_dev (see detailed logs with `journalctl -f _COMM=format-ebs`)";
        /opt/fractalbits/bin/format-ebs $ebs_dev;

        mkdir -p /data/local;
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

fn create_nss_bench_config() -> CmdResult {
    let config_content = include_str!("../../../etc/nss_server_dev_config.toml");
    run_cmd! {
        mkdir -p $ETC_PATH;
        echo $config_content > ${ETC_PATH}${NSS_SERVER_CONFIG};
    }?;
    Ok(())
}
