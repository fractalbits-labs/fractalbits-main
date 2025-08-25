use super::common::*;
use cmd_lib::*;
use std::io::Error;

const COMMAND_TIMEOUT_SECONDS: u64 = 300;
const POLL_INTERVAL_SECONDS: u64 = 5;
const MAX_POLL_ATTEMPTS: u64 = 60;

#[allow(clippy::too_many_arguments)]
pub fn bootstrap(
    nss_endpoint: &str,
    nss_a_id: &str,
    nss_b_id: &str,
    volume_a_id: &str,
    volume_b_id: &str,
    follower_id: Option<&str>,
    remote_az: Option<&str>,
    _for_bench: bool,
) -> CmdResult {
    // download_binaries(&["rss_admin", "root_server", "ebs-failover"])?;
    download_binaries(&["rss_admin", "root_server"])?;
    let region = get_current_aws_region()?;
    run_cmd!($BIN_PATH/rss_admin --region=$region api-key init-test)?;

    // Initialize NSS role states in DynamoDB
    initialize_nss_roles_in_ddb(nss_a_id, nss_b_id)?;

    // Initialize AZ status if this is a multi-AZ deployment
    if let Some(remote_az) = remote_az {
        initialize_az_status_in_ddb(remote_az)?;
    }

    create_rss_config(nss_endpoint)?;
    // setup_cloudwatch_agent()?;
    create_systemd_unit_file("root_server", follower_id.is_some())?;

    // Initialize NSS formatting and root server startup if follower_id is provided
    if let Some(follower_id) = follower_id {
        // Create S3 Express buckets if remote_az is provided
        if let Some(remote_az) = remote_az {
            // Create local S3 Express bucket
            let local_az = get_current_aws_az_id()?;
            create_s3_express_bucket(&local_az, S3EXPRESS_LOCAL_BUCKET_CONFIG)?;

            // Create remote S3 Express bucket
            create_s3_express_bucket(remote_az, S3EXPRESS_REMOTE_BUCKET_CONFIG)?;
        }

        for (nss_id, volume_id, role) in [
            (nss_b_id, volume_b_id, "standby"),
            (nss_a_id, volume_a_id, "active"),
        ] {
            info!("Formatting NSS instance {nss_id} ({role}) with volume {volume_id}");
            // Format EBS with SSM
            let ebs_dev = get_volume_dev(volume_id);
            wait_for_ssm_ready(nss_id);
            let bootstrap_bin = "/opt/fractalbits/bin/fractalbits-bootstrap";
            info!("Running format_nss on {nss_id} ({role}) with device {ebs_dev}");
            run_cmd_with_ssm(
                nss_id,
                &format!(
                    r##"sudo bash -c "{bootstrap_bin} format_nss --ebs_dev {ebs_dev} &>>{CLOUD_INIT_LOG}""##
                ),
            )?;
            info!("Successfully formatted {nss_id} ({role})");
        }

        wait_for_leadership()?;
        start_follower_root_server(follower_id)?;

        // bootstrap_ebs_failover_service(nss_a_id, nss_b_id, volume_id)?;
    }
    Ok(())
}

fn initialize_nss_roles_in_ddb(nss_a_id: &str, nss_b_id: &str) -> CmdResult {
    const DDB_SERVICE_DISCOVERY_TABLE: &str = "fractalbits-service-discovery";
    let region = get_current_aws_region()?;

    info!("Initializing NSS role states in service-discovery table");
    info!("Setting {nss_a_id} as active");
    info!("Setting {nss_b_id} as standby");

    // Create nss_roles entry with both instance states
    let nss_roles_item = format!(
        r#"{{"service_id":{{"S":"nss_roles"}},"states":{{"M":{{"{nss_a_id}":{{"S":"active"}},"{nss_b_id}":{{"S":"standby"}}}}}}}}"#
    );

    // Put nss_roles entry with states map
    run_cmd! {
        aws dynamodb put-item
            --table-name $DDB_SERVICE_DISCOVERY_TABLE
            --item $nss_roles_item
            --region $region
    }?;

    info!("NSS roles initialized in service-discovery table");
    Ok(())
}

fn initialize_az_status_in_ddb(remote_az: &str) -> CmdResult {
    const DDB_SERVICE_DISCOVERY_TABLE: &str = "fractalbits-service-discovery";
    let region = get_current_aws_region()?;
    let local_az = get_current_aws_az_id()?;

    info!("Initializing AZ status in service-discovery table");
    info!("Setting {local_az} and {remote_az} to Normal");

    let az_status_item = format!(
        r#"{{"service_id":{{"S":"az_status"}},"status":{{"M":{{"{local_az}":{{"S":"Normal"}},"{remote_az}":{{"S":"Normal"}}}}}}}}"#
    );

    // Put az_status entry with status map
    run_cmd! {
        aws dynamodb put-item
            --table-name $DDB_SERVICE_DISCOVERY_TABLE
            --item $az_status_item
            --region $region
    }?;

    info!("AZ status initialized in service-discovery table ({local_az}: Normal, {remote_az}: Normal)");
    Ok(())
}

fn wait_for_leadership() -> CmdResult {
    info!("Waiting for local root_server to become leader...");
    let mut attempt = 0;
    const HEALTH_PORT: u16 = 18088;

    loop {
        attempt += 1;

        // Check if the health endpoint is responding and reports leadership
        let health_url = format!("http://localhost:{HEALTH_PORT}");
        let result = run_fun!(curl -s $health_url 2>/dev/null | jq -r ".is_leader");

        match result {
            Ok(ref response) if response.trim() == "true" => {
                info!("Local root_server has become the leader");
                break;
            }
            Ok(ref response) => {
                info!(
                    "Root_server not yet leader (is_leader: {}), waiting...",
                    response.trim()
                );
            }
            Err(_) => {
                info!("Health endpoint not yet responding, waiting...");
            }
        }

        if attempt >= MAX_POLL_ATTEMPTS {
            cmd_die!("Timed out waiting for root_server to become leader");
        }

        std::thread::sleep(std::time::Duration::from_secs(1));
    }

    Ok(())
}

fn start_follower_root_server(follower_id: &str) -> CmdResult {
    info!("Starting root_server service on follower instance {follower_id}");
    wait_for_ssm_ready(follower_id);

    // The follower instance should have already run its own bootstrap process
    // (with no follower_id parameter) to set up configs and systemd unit file
    // We just need to start the service
    run_cmd_with_ssm(follower_id, "sudo systemctl start root_server.service")?;

    info!("Successfully started root_server service on follower {follower_id}");
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
        let result = run_cmd_with_ssm(instance_id, &format!("test -f {BOOTSTRAP_DONE_FILE}"));
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

#[allow(unused)]
fn bootstrap_ebs_failover_service(nss_a_id: &str, nss_b_id: &str, volume_id: &str) -> CmdResult {
    let service_name = "ebs-failover";

    let config_content = format!(
        r##"nss_a_id = "{nss_a_id}"    # Primary instance ID
nss_b_id = "{nss_b_id}"  # Secondary instance ID
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

fn create_rss_config(nss_endpoint: &str) -> CmdResult {
    let config_content = format!(
        r##"# Root Server Configuration

# Server port
server_port = 8088

# Server health port
health_port = 18088

# API Server management port
api_server_mgmt_port = 18088

# Nss server rpc server address
nss_addr = "{nss_endpoint}:8088"

# Leader Election Configuration
[leader_election]
# DynamoDB table name for leader election
table_name = "fractalbits-leader-election"

# Key used in DynamoDB table to identify this leader election group
leader_key = "root-server-leader"

# How long a leader holds the lease before it expires (in seconds)
# Increased to 60s for better stability against transient network issues
lease_duration_secs = 60

# How often to send heartbeats and check leadership status (in seconds)
# Set to 15s for less aggressive DynamoDB polling while maintaining responsiveness
# With 50% renewal threshold, renewal happens at 30s, giving 30s buffer
heartbeat_interval_secs = 15

# Maximum number of retry attempts for DynamoDB operations
# Increased to 5 for better startup resilience
max_retry_attempts = 5

# Enable monitoring and metrics collection
enable_monitoring = true
"##
    );
    run_cmd! {
        mkdir -p $ETC_PATH;
        echo $config_content > $ETC_PATH/$ROOT_SERVER_CONFIG;
    }?;
    Ok(())
}
