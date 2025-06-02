use super::common::*;
use cmd_lib::*;

pub fn bootstrap(bucket_name: &str) -> CmdResult {
    download_binary("mkfs")?;
    run_cmd! {
        mkdir -p /var/data;
        cd /var/data;
        $BIN_PATH/mkfs;
    }?;

    let service = super::Service::NssServer;
    download_binary(service.as_ref())?;
    create_config(bucket_name)?;
    create_systemd_unit_file(service)?;
    run_cmd! {
        info "Starting nss_server.service";
        systemctl start nss_server.service;
    }?;
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
