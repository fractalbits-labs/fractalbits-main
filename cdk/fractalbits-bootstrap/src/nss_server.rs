use super::common::*;
use cmd_lib::*;

pub fn bootstrap(bucket_name: &str, volume_id: &str, num_nvme_disks: usize) -> CmdResult {
    install_rpms(&["nvme-cli", "mdadm", "perf", "lldb"])?;
    if num_nvme_disks != 0 {
        format_local_nvme_disks(num_nvme_disks)?;
    }

    for bin in ["nss_server", "mkfs", "format-ebs"] {
        download_binary(bin)?;
    }

    let service_name = "nss_server";
    setup_configs(bucket_name, volume_id, service_name)?;

    // Note the nss_server service is not started until EBS formatted from root_server
    Ok(())
}

pub fn setup_configs(bucket_name: &str, volume_id: &str, service_name: &str) -> CmdResult {
    let volume_dev = get_volume_dev(volume_id);
    create_nss_config(bucket_name)?;
    create_mount_unit(&volume_dev, "/data/ebs", "ext4")?;
    create_ebs_udev_rule(volume_id, service_name)?;
    create_systemd_unit_file(service_name)?;
    Ok(())
}

fn create_nss_config(bucket_name: &str) -> CmdResult {
    let aws_region = get_current_aws_region()?;
    let config_content = format!(
        r##"[s3_cache]
s3_host = "s3.{aws_region}.amazonaws.com"
s3_port = 80
s3_region = "{aws_region}"
s3_bucket = "{bucket_name}"
"##
    );
    run_cmd! {
        mkdir -p $ETC_PATH;
        echo $config_content > $ETC_PATH/$NSS_SERVER_CONFIG
    }?;
    Ok(())
}

fn create_ebs_udev_rule(volume_id: &str, service_name: &str) -> CmdResult {
    let content = format!(
        r##"KERNEL=="nvme*n*", SUBSYSTEM=="block", ENV{{ID_SERIAL}}=="Amazon_Elastic_Block_Store_{}_1", TAG+="systemd", ENV{{SYSTEMD_WANTS}}="{service_name}.service""##,
        volume_id.replace("-", "")
    );
    run_cmd! {
        echo $content > $ETC_PATH/99-ebs.rules;
        ln -s $ETC_PATH/99-ebs.rules /etc/udev/rules.d/;
    }?;

    Ok(())
}
