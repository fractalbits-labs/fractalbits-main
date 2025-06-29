use super::common::*;
use cmd_lib::*;
use std::io::Error;

const COMMAND_TIMEOUT_SECONDS: u64 = 300;
const POLL_INTERVAL_SECONDS: u64 = 5;
const MAX_POLL_ATTEMPTS: u64 = 60;

pub fn bootstrap(
    primary_instance_id: &str,
    secondary_instance_id: &str,
    volume_id: &str,
) -> CmdResult {
    download_binaries(&["rss_admin", "root_server", "ebs-failover"])?;
    run_cmd!($BIN_PATH/rss_admin api-key init-test)?;

    create_rss_config()?;
    create_systemd_unit_file("root_server", true)?;

    // Format EBS with SSM
    let ebs_dev = get_volume_dev(volume_id);
    wait_for_ssm_ready(primary_instance_id);
    run_cmd_with_ssm(
        primary_instance_id,
        &format! {"sudo /opt/fractalbits/bin/format-nss --ebs_dev {ebs_dev}"},
    )?;

    if secondary_instance_id != "null" {
        bootstrap_ebs_failover_service(primary_instance_id, secondary_instance_id, volume_id)?;
    }

    Ok(())
}

fn wait_for_ssm_ready(instance_id: &str) {
    let mut attempt = 0;
    loop {
        attempt += 1;
        let result = run_fun! {
            aws ssm describe-instance-information
                --filters "Key=InstanceIds,Values=$instance_id"

                --output json | jq -r ".InstanceInformationList[0].PingStatus"
        };
        info!("Ping {instance_id} status: {result:?}");
        match result {
            Ok(ref s) if s == "Online" => break,
            _ => std::thread::sleep(std::time::Duration::from_secs(POLL_INTERVAL_SECONDS)),
        };

        if attempt >= MAX_POLL_ATTEMPTS {
            cmd_die!("Timed out while waiting for SSM after $MAX_POLL_ATTEMPTS attempts.");
        }
    }

    let mut attempt = 0;
    loop {
        attempt += 1;
        let result = run_cmd_with_ssm(instance_id, &format!("test -f {CLOUD_INIT_DONE_FILE}"));
        match result {
            Ok(()) => break,
            _ => {
                info!("Waiting for {instance_id} cloud init to be done");
                std::thread::sleep(std::time::Duration::from_secs(POLL_INTERVAL_SECONDS));
            }
        }

        if attempt >= MAX_POLL_ATTEMPTS {
            cmd_die!("Timed out while waiting for cloud init after $MAX_POLL_ATTEMPTS attempts.");
        }
    }
}

fn run_cmd_with_ssm(instance_id: &str, cmd: &str) -> CmdResult {
    let command_id = run_fun! {
        info "Running ${cmd} on ${instance_id} with SSM";
        aws ssm send-command
            --instance-ids "$instance_id"
            --document-name "AWS-RunShellScript"
            --parameters "commands=[\'$cmd\']"
            --timeout-seconds "$COMMAND_TIMEOUT_SECONDS"
            --query "Command.CommandId"
            --output text
    }?;
    info!("Command sent to {instance_id} successfully. Command ID: {command_id}. Polling for results...");
    let mut attempt = 0;
    loop {
        attempt += 1;
        let invocation_json = run_fun! {
            aws ssm get-command-invocation
                --command-id "$command_id"
                --instance-id "$instance_id"
        }?;
        let status = run_fun!(echo $invocation_json | jq -r .Status)?;

        info!("Command status from {instance_id} is: {status}");
        match status.as_ref() {
            "Success" => break,
            "Failed" => {
                error!("Command execution failed on the remote instance.");
                let error_output = run_fun!(echo $invocation_json | jq -r .StandardErrorContent)?;
                return Err(Error::other(format!(
                    "Remote Error Output:\n---\n{error_output}---"
                )));
            }
            "TimedOut" => {
                return Err(Error::other(format!(
                            "Command timed out on the remote instance after {COMMAND_TIMEOUT_SECONDS} seconds.")));
            }
            _ => {
                // Status is Pending, InProgress, Cancelling, etc.
                std::thread::sleep(std::time::Duration::from_secs(POLL_INTERVAL_SECONDS));
            }
        }
        if attempt >= MAX_POLL_ATTEMPTS {
            return Err(Error::other(format!(
                "Timed out polling for command result after {MAX_POLL_ATTEMPTS} attempts."
            )));
        }
    }

    Ok(())
}

fn bootstrap_ebs_failover_service(
    primary_instance_id: &str,
    secondary_instance_id: &str,
    volume_id: &str,
) -> CmdResult {
    let service_name = "ebs-failover";

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
    }?;

    create_systemd_unit_file(service_name, true)?;
    Ok(())
}

fn create_rss_config() -> CmdResult {
    let config_content = r##"server_port = 8088"##.to_string();
    run_cmd! {
        mkdir -p $ETC_PATH;
        echo $config_content > $ETC_PATH/$ROOT_SERVER_CONFIG;
    }?;
    Ok(())
}
