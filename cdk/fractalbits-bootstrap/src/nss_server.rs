use super::common::*;
use cmd_lib::*;

pub fn bootstrap(bucket_name: &str, volume_id: &str) -> CmdResult {
    // Sanitize: convert vol-07451bc901d5e1e09 → vol07451bc901d5e1e09
    let volume_id = &volume_id.replace("-", "");
    let service_name = "nss_server";
    for bin in ["nss_server", "mkfs", "format-ebs"] {
        download_binary(bin)?;
    }
    create_config(bucket_name)?;
    create_ebs_mount_unit(volume_id)?;
    create_udev_rule(volume_id)?;
    create_systemd_unit_file(service_name)?;
    run_cmd! {
        info "Enabling ${service_name}.service";
        systemctl enable ${service_name}.service;
    }?;
    // Note the nss_server service is not started until EBS formatted from root_server
    Ok(())
}

fn create_config(bucket_name: &str) -> CmdResult {
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

fn create_ebs_mount_unit(volume_id: &str) -> CmdResult {
    let content = format!(
        r##"[Unit]
Description=Mount EBS Volume at /data

[Mount]
What=/dev/disk/by-id/nvme-Amazon_Elastic_Block_Store_{volume_id}
Where=/data
Type=xfs
Options=defaults,nofail

[Install]
WantedBy=multi-user.target
"##
    );
    run_cmd! {
        echo $content > /etc/systemd/system/data.mount;
    }?;

    Ok(())
}

fn create_udev_rule(volume_id: &str) -> CmdResult {
    let content = format!(
        r##"KERNEL=="nvme*n*", SUBSYSTEM=="block", ENV{{ID_SERIAL}}=="Amazon_Elastic_Block_Store_{volume_id}_1", TAG+="systemd", ENV{{SYSTEMD_WANTS}}="nss_server.service""##
    );
    run_cmd! {
        echo $content > $ETC_PATH/99-ebs.rules;
        ln -s $ETC_PATH/99-ebs.rules /etc/udev/rules.d/;
    }?;

    Ok(())
}
