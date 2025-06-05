use super::common::*;
use cmd_lib::*;

pub fn bootstrap(
    primary_instance_id: &str,
    secondary_instance_id: &str,
    volume_id: &str,
) -> CmdResult {
    download_binary("rss_admin")?;
    run_cmd!($BIN_PATH/rss_admin api-key init-test)?;

    let service_name = "root_server";
    download_binary(service_name)?;
    create_systemd_unit_file(service_name)?;
    run_cmd! {
        info "Starting root_server.service";
        systemctl start root_server.service;
    }?;

    bootstrap_ebs_failover_service(primary_instance_id, secondary_instance_id, volume_id)?;

    Ok(())
}

fn bootstrap_ebs_failover_service(
    primary_instance_id: &str,
    secondary_instance_id: &str,
    volume_id: &str,
) -> CmdResult {
    let service_name = "ebs-failover";
    download_binary(service_name)?;
    create_systemd_unit_file(service_name)?;

    let config_content = format!(
        r##"primary_instance_id = "{primary_instance_id}"    # Primary instance ID
secondary_instance_id = "{secondary_instance_id}"  # Secondary instance ID
volume_id = "{volume_id}"            # EBS volume ID
device_name = "/dev/xvdf"                      # Device name for OS

dynamodb_table_name = "ebs-failover-state"     # Name of the pre-created DynamoDB table

check_interval_seconds = 30                    # How often to perform health checks
health_check_timeout_seconds = 15              # Timeout for each health check attempt
post_failover_delay_seconds = 60               # Pause after a failover before resuming checks

enable_fencing = true                          # Set to true to enable STONITH
fencing_action = "stop"                        # "stop" or "terminate"
fencing_timeout_seconds = 300                  # Max time to wait for instance to stop/terminate
"##
    );
    run_cmd! {
        mkdir -p $ETC_PATH;
        echo $config_content > $ETC_PATH/${service_name}-config.toml;
        info "Starting ${service_name}.service";
        systemctl start ${service_name}.service;
    }?;

    Ok(())
}
