use super::common::*;
use cmd_lib::*;

pub fn bootstrap(bucket_name: &str) -> CmdResult {
    let service = super::Service::ApiServer;
    download_binary(service.as_ref())?;
    create_config(bucket_name)?;
    create_systemd_unit_file(service)?;
    run_cmd! {
        info "Sleep 10s to wait for other ec2 instances";
        sleep 10;
        info "Starting api_server.service";
        systemctl start api_server.service;
    }?;
    Ok(())
}

fn create_config(bucket_name: &str) -> CmdResult {
    let aws_region = get_current_aws_region()?;
    let config_content = format!(
        r##"bss_addr = "10.0.1.10:9225"
nss_addr = "10.0.1.100:9224"
rss_addr = "10.0.1.254:8888"
region = "{aws_region}"
port = 3000
root_domain = ".localhost"

[s3_cache]
s3_host = "http://s3.{aws_region}.amazonaws.com"
s3_port = 80
s3_region = "{aws_region}"
s3_bucket = "{bucket_name}"
"##
    );
    run_cmd! {
        mkdir -p $ETC_PATH;
        echo $config_content > $ETC_PATH/$API_SERVER_CONFIG
    }?;
    Ok(())
}
