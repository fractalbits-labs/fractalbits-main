use cmd_lib::*;
use std::io::Error;
use std::time::{Duration, Instant};

pub const BIN_PATH: &str = "/opt/fractalbits/bin/";
pub const ETC_PATH: &str = "/opt/fractalbits/etc/";
pub const GUI_WEB_ROOT: &str = "/opt/fractalbits/www/";
pub const API_SERVER_CONFIG: &str = "api_server_cloud_config.toml";
pub const BSS_SERVER_CONFIG: &str = "bss_server_cloud_config.toml";
pub const NSS_SERVER_CONFIG: &str = "nss_server_cloud_config.toml";
pub const MIRRORD_CONFIG: &str = "mirrord_cloud_config.toml";
pub const ROOT_SERVER_CONFIG: &str = "root_server_cloud_config.toml";
pub const NSS_ROLE_AGENT_CONFIG: &str = "nss_role_agent_cloud_config.toml";
pub const BENCH_SERVER_BENCH_START_SCRIPT: &str = "bench_start.sh";
pub const BOOTSTRAP_DONE_FILE: &str = "/opt/fractalbits/.bootstrap_done";
pub const STATS_LOGROTATE_CONFIG: &str = "/etc/logrotate.d/stats_logs";
pub const DDB_SERVICE_DISCOVERY_TABLE: &str = "fractalbits-service-discovery";
pub const NETWORK_TUNING_SYS_CONFIG: &str = "99-network-tuning.conf";

// DDB Service Discovery Keys
pub const BSS_DATA_VG_CONFIG_KEY: &str = "bss-data-vg-config";
pub const BSS_METADATA_VG_CONFIG_KEY: &str = "bss-metadata-vg-config";
pub const BSS_SERVER_KEY: &str = "bss-server";
pub const NSS_ROLES_KEY: &str = "nss_roles";
pub const AZ_STATUS_KEY: &str = "az_status";
#[allow(dead_code)]
pub const CLOUDWATCH_AGENT_CONFIG: &str = "cloudwatch_agent_config.json";
pub const CLOUD_INIT_LOG: &str = "/var/log/cloud-init-output.log";
pub const EXT4_MKFS_OPTS: [&str; 4] = ["-O", "bigalloc", "-C", "16384"];
pub const S3EXPRESS_LOCAL_BUCKET_CONFIG: &str = "s3express-local-bucket-config.json";
pub const S3EXPRESS_REMOTE_BUCKET_CONFIG: &str = "s3express-remote-bucket-config.json";

pub fn common_setup() -> CmdResult {
    create_network_tuning_sysctl_file()?;
    install_rpms(&["amazon-cloudwatch-agent", "nmap-ncat", "perf", "lldb"])?;
    setup_serial_console_password()?;
    Ok(())
}

pub fn download_binaries(file_list: &[&str]) -> CmdResult {
    for file_name in file_list {
        download_binary(file_name)?;
    }
    Ok(())
}

fn download_binary(file_name: &str) -> CmdResult {
    let builds_bucket = get_builds_bucket()?;
    let cpu_arch = run_fun!(arch)?;

    let download_path = if file_name == "fractalbits-bootstrap" || file_name == "warp" {
        format!("{builds_bucket}/{cpu_arch}/{file_name}")
    } else {
        let instance_type = get_ec2_instance_type()?;
        let cpu_target = get_cpu_target_from_instance_type(&instance_type);
        format!("{builds_bucket}/{cpu_arch}/{cpu_target}/{file_name}")
    };

    run_cmd! {
        info "Downloading $file_name from $download_path to $BIN_PATH";
        aws s3 cp --no-progress $download_path $BIN_PATH;
        chmod +x $BIN_PATH/$file_name
    }?;
    Ok(())
}

pub fn get_builds_bucket() -> FunResult {
    let builds_bucket = format!(
        "s3://fractalbits-builds-{}-{}",
        get_current_aws_region()?,
        get_account_id()?
    );
    Ok(builds_bucket)
}

pub fn create_systemd_unit_file(service_name: &str, enable_now: bool) -> CmdResult {
    let working_dir = "/data";
    let mut requires = "";
    let mut env_settings = String::new();
    let mut managed_service = false;
    let exec_start = match service_name {
        "api_server" => {
            env_settings = r##"
Environment="RUST_LOG=info""##
                .to_string();
            format!("{BIN_PATH}{service_name} -c {ETC_PATH}{API_SERVER_CONFIG}")
        }
        "gui_server" => {
            env_settings = r##"
Environment="RUST_LOG=info"
Environment="GUI_WEB_ROOT={GUI_WEB_ROOT}"
"##
            .to_string();
            format!("{BIN_PATH}api_server -c {ETC_PATH}{API_SERVER_CONFIG}")
        }
        "nss" => {
            managed_service = true;
            requires = "data-ebs.mount data-local.mount";
            format!("{BIN_PATH}nss_server serve -c {ETC_PATH}{NSS_SERVER_CONFIG}")
        }
        "mirrord" => {
            managed_service = true;
            requires = "data-ebs.mount data-local.mount";
            format!("{BIN_PATH}{service_name} -c {ETC_PATH}{MIRRORD_CONFIG}")
        }
        "rss" => {
            env_settings = r##"
Environment="RUST_LOG=info""##
                .to_string();
            format!("{BIN_PATH}root_server -c {ETC_PATH}{ROOT_SERVER_CONFIG}")
        }
        "bss" => {
            requires = "data-local.mount";
            format!("{BIN_PATH}bss_server -c {ETC_PATH}{BSS_SERVER_CONFIG}")
        }
        "bench_client" => {
            format!("{BIN_PATH}warp client")
        }
        "nss_role_agent" => {
            env_settings = r##"
Environment="RUST_LOG=info""##
                .to_string();
            format!("{BIN_PATH}{service_name} -c {ETC_PATH}{NSS_ROLE_AGENT_CONFIG}")
        }
        // "ebs-failover" => {
        //     env_settings = r##"
        // Environment="RUST_LOG=info""##
        //         .to_string();
        //     format!("{BIN_PATH}{service_name} -r {aws_region}")
        // }
        _ => unreachable!(),
    };
    let (restart_settings, auto_restart) = if managed_service {
        ("", "")
    } else {
        (
            r##"# Limit to 3 restarts within a 10-minute (600 second) interval
StartLimitIntervalSec=600
StartLimitBurst=3
        "##,
            "Restart=on-failure\nRestartSec=5",
        )
    };
    let systemd_unit_content = format!(
        r##"[Unit]
Description={service_name} Service
After=network-online.target {requires}
Requires={requires}
BindsTo={requires}
{restart_settings}

[Service]
{auto_restart}
LimitNOFILE=1000000
LimitCORE=infinity
WorkingDirectory={working_dir}{env_settings}
ExecStart={exec_start}

[Install]
WantedBy=multi-user.target
"##
    );

    let service_file = format!("{service_name}.service");
    let enable_now_opt = if enable_now { "--now" } else { "" };
    run_cmd! {
        mkdir -p /data;
        mkdir -p $ETC_PATH;
        echo $systemd_unit_content > ${ETC_PATH}${service_file};
    }?;

    run_cmd! {
        info "Enabling ${ETC_PATH}${service_file} (enable_now=${enable_now})";
        systemctl enable ${ETC_PATH}${service_file} --force --quiet ${enable_now_opt};
    }?;
    Ok(())
}

pub fn create_logrotate_for_stats() -> CmdResult {
    let rotate_config_content = r##"/data/local/stats/*.stats {
    size 50M
    rotate 10
    notifempty
    missingok
    nocreate
    copytruncate
}
"##;

    run_cmd! {
        info "Enabling stats log rotate";
        echo $rotate_config_content > ${STATS_LOGROTATE_CONFIG};
    }?;

    Ok(())
}

pub fn get_current_aws_region() -> FunResult {
    run_fun!(ec2-metadata --region | awk r"{print $2}")
}

pub fn get_instance_id() -> FunResult {
    run_fun!(ec2-metadata --instance-id | awk r"{print $2}")
}

pub fn get_ec2_instance_type() -> FunResult {
    run_fun!(ec2-metadata --instance-type | awk r"{print $2}")
}

pub fn get_cpu_target_from_instance_type(instance_type: &str) -> &'static str {
    let family = instance_type.split('.').next().unwrap_or("");

    match family {
        "i3" => "i3",
        "i3en" => "i3en",
        "c7g" | "m7g" | "m7gd" | "r7g" | "r7gd" => "graviton3",
        "c8g" | "m8g" | "m8gd" | "r8g" | "r8gd" => "graviton4",
        _ => {
            let arch = run_fun!(arch).unwrap_or_default();
            if arch == "aarch64" { "graviton3" } else { "i3" }
        }
    }
}

pub fn get_s3_express_bucket_name(az: &str) -> FunResult {
    // Generate bucket name in format: fractalbits-data-{account}--{az}--x-s3
    let account = get_account_id()?;
    Ok(format!("fractalbits-data-{account}--{az}--x-s3"))
}

pub fn get_current_aws_az_id() -> FunResult {
    let token = run_fun! {
        curl -X PUT "http://169.254.169.254/latest/api/token"
            -H "X-aws-ec2-metadata-token-ttl-seconds: 21600" -s
    }?;
    run_fun! {
        curl -H "X-aws-ec2-metadata-token: $token"
            "http://169.254.169.254/latest/meta-data/placement/availability-zone-id"
    }
}

// Get AWS account ID using IMDS (Instance Metadata Service)
pub fn get_account_id() -> FunResult {
    let token = run_fun! {
        curl -X PUT "http://169.254.169.254/latest/api/token"
            -H "X-aws-ec2-metadata-token-ttl-seconds: 21600" -s
    }?;
    let account = run_fun! {
        curl -H "X-aws-ec2-metadata-token: $token"
            "http://169.254.169.254/latest/dynamic/instance-identity/document" -s
        | grep accountId
        | sed r#"s/.*"accountId" : "\([0-9]*\)".*/\1/"#
    }?;
    if account.is_empty() {
        return Err(std::io::Error::other(
            "Failed to extract account ID from IMDS",
        ));
    }
    Ok(account)
}

pub fn create_s3_express_bucket(az: &str, config_file_name: &str) -> CmdResult {
    let bucket_name = get_s3_express_bucket_name(az)?;

    // Check if bucket already exists
    info!("Checking if S3 Express bucket {bucket_name} already exists...");
    let bucket_exists = run_cmd! {
        aws s3api head-bucket --bucket $bucket_name 2>/dev/null
    }
    .is_ok();

    if bucket_exists {
        info!("S3 Express bucket {bucket_name} already exists, skipping creation");
        return Ok(());
    }

    // Generate S3 Express bucket configuration JSON
    let config_content = format!(
        r#"{{
  "Location": {{
    "Type": "AvailabilityZone",
    "Name": "{az}"
  }},
  "Bucket": {{
    "DataRedundancy": "SingleAvailabilityZone",
    "Type": "Directory"
  }}
}}"#
    );

    let config_file_path = format!("{ETC_PATH}{config_file_name}");
    run_cmd! {
        echo $config_content > $config_file_path
    }?;

    // Create the S3 Express bucket
    info!("Creating S3 Express bucket: {bucket_name} in AZ: {az}");
    let config_file_opt = format!("file://{config_file_path}");
    run_cmd! {
        aws s3api create-bucket
            --bucket $bucket_name
            --create-bucket-configuration $config_file_opt
    }?;

    info!("S3 Express bucket ready: {bucket_name}");
    Ok(())
}

// https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/storage-twp.html
pub fn format_local_nvme_disks(support_storage_twp: bool) -> CmdResult {
    let nvme_disks = run_fun! {
        nvme list | grep -v "Amazon Elastic Block Store"
            | awk r##"/nvme[0-9]n[0-9]/ {print $1}"##
    }?;
    let nvme_disks: &Vec<&str> = &nvme_disks.split("\n").collect();
    let num_nvme_disks = nvme_disks.len();
    if num_nvme_disks == 0 {
        cmd_die!("Could not find any nvme disks");
    }
    if support_storage_twp {
        assert_eq!(1, num_nvme_disks);
    }

    if num_nvme_disks == 1 {
        if support_storage_twp {
            run_cmd! {
                info "Creating ext4 on local nvme disks: ${nvme_disks:?} to support torn write prevention";
                mkfs.ext4 -q $[EXT4_MKFS_OPTS] $[nvme_disks];
            }?;
        } else {
            run_cmd! {
                info "Creating XFS on local nvme disks: ${nvme_disks:?}";
                mkfs.xfs -f -q $[nvme_disks];
            }?;
        }

        run_cmd! {
            info "Mounting to $DATA_LOCAL_MNT";
            mkdir -p $DATA_LOCAL_MNT;
            mount $[nvme_disks] $DATA_LOCAL_MNT;
        }?;

        let uuid = run_fun!(blkid -s UUID -o value $[nvme_disks])?;
        create_mount_unit(&format!("/dev/disk/by-uuid/{uuid}"), DATA_LOCAL_MNT, "xfs")?;
        return Ok(());
    }

    const DATA_LOCAL_MNT: &str = "/data/local";
    run_cmd! {
        info "Zeroing superblocks";
        mdadm -q --zero-superblock $[nvme_disks];

        info "Creating md0";
        mdadm -q --create /dev/md0 --level=0 --raid-devices=${num_nvme_disks} $[nvme_disks];

        info "Creating XFS on /dev/md0";
        mkfs.xfs -q /dev/md0;

        info "Mounting to $DATA_LOCAL_MNT";
        mkdir -p $DATA_LOCAL_MNT;
        mount /dev/md0 $DATA_LOCAL_MNT;

        info "Updating /etc/mdadm/mdadm.conf";
        mkdir -p /etc/mdadm;
        mdadm --detail --scan > /etc/mdadm/mdadm.conf;
    }?;

    let md0_uuid = run_fun!(blkid -s UUID -o value /dev/md0)?;
    create_mount_unit(
        &format!("/dev/disk/by-uuid/{md0_uuid}"),
        DATA_LOCAL_MNT,
        "xfs",
    )?;

    Ok(())
}

pub fn create_mount_unit(what: &str, mount_point: &str, fs_type: &str) -> CmdResult {
    let content = format!(
        r##"[Unit]
Description=Mount {what} at {mount_point}

[Mount]
What={what}
Where={mount_point}
Type={fs_type}
Options=defaults,nofail

[Install]
WantedBy=multi-user.target
"##
    );
    let mount_unit_name = mount_point.trim_start_matches("/").replace("/", "-");
    run_cmd! {
        info "Creating systemd unit ${mount_unit_name}.mount";
        mkdir -p $ETC_PATH;
        echo $content > ${ETC_PATH}${mount_unit_name}.mount;
        systemctl enable ${ETC_PATH}${mount_unit_name}.mount;
    }?;

    Ok(())
}

pub fn create_coredump_config() -> CmdResult {
    let cores_location = "/data/local/coredumps";
    let file = "99-coredump.conf";
    let content = format!("kernel.core_pattern={cores_location}/core.%e.%p.%t");
    run_cmd! {
        info "Setting up coredump location ($cores_location)";
        mkdir -p $cores_location;
        echo $content > ${ETC_PATH}${file};
        ln -sf ${ETC_PATH}${file} /etc/sysctl.d;
        sysctl -p /etc/sysctl.d/${file};
    }
}

pub fn get_volume_dev(volume_id: &str) -> String {
    // Sanitize: convert vol-07451bc901d5e1e09 → vol07451bc901d5e1e09
    let volume_id = &volume_id.replace("-", "");
    format!("/dev/disk/by-id/nvme-Amazon_Elastic_Block_Store_{volume_id}")
}

pub fn install_rpms(rpms: &[&str]) -> CmdResult {
    run_cmd! {
        info "Installing ${rpms:?}";
        yum install -y -q $[rpms] >/dev/null;
    }?;

    Ok(())
}

#[allow(dead_code)]
fn create_cloudwatch_agent_config() -> CmdResult {
    let aws_region = get_current_aws_region()?;
    let content = format!(
        r##"{{
  "agent": {{
    "region": "{aws_region}",
    "run_as_user": "root",
    "debug": false
  }},
  "metrics": {{
    "namespace": "Vpc/Fractalbits",
    "metrics_collected": {{
      "statsd": {{
        "metrics_aggregation_interval": 60,
        "metrics_collection_interval": 10,
        "service_address": ":8125"
      }},
      "cpu": {{
        "measurement": [
          "cpu_usage_idle"
        ],
        "metrics_collection_interval": 60,
        "resources": [
          "*"
        ],
        "totalcpu": true
      }}
    }}
  }}
}}"##
    );

    run_cmd! {
        echo $content > $ETC_PATH/$CLOUDWATCH_AGENT_CONFIG;
    }?;

    Ok(())
}

#[allow(dead_code)]
pub fn setup_cloudwatch_agent() -> CmdResult {
    create_cloudwatch_agent_config()?;

    run_cmd! {
        info "Creating CloudWatch agent configuration files";
        /opt/aws/amazon-cloudwatch-agent/bin/amazon-cloudwatch-agent-ctl
            -a fetch-config -m ec2 -c file:$ETC_PATH/$CLOUDWATCH_AGENT_CONFIG;
        info "Enabling Cloudwatch agent service";
        systemctl enable --now amazon-cloudwatch-agent;
    }?;

    Ok(())
}

pub fn create_ddb_register_and_deregister_service(service_id: &str) -> CmdResult {
    create_ddb_register_service(service_id)?;
    create_ddb_deregister_service(service_id)?;
    Ok(())
}

fn create_ddb_register_service(service_id: &str) -> CmdResult {
    let ddb_register_script = format!("{BIN_PATH}ddb-register.sh");
    let systemd_unit_content = format!(
        r##"[Unit]
Description=DynamoDB Service Registration
After=network-online.target

[Service]
Type=oneshot
ExecStart={ddb_register_script}

[Install]
WantedBy=multi-user.target
"##
    );

    let register_script_content = format!(
        r##"#!/bin/bash
set -e
service_id={service_id}
instance_id=$(ec2-metadata -i | awk '{{print $2}}')
private_ip=$(ec2-metadata -o | awk '{{print $2}}')

echo "Registering itself ($instance_id,$private_ip) to ddb table {DDB_SERVICE_DISCOVERY_TABLE} with service_id $service_id" >&2

# Retry mechanism with exponential backoff to handle race conditions
MAX_RETRIES=10
retry_count=0
success=false

while [ $retry_count -lt $MAX_RETRIES ] && [ "$success" = "false" ]; do
    retry_count=$((retry_count + 1))

    # Try to update existing item first
    if aws dynamodb update-item \
        --table-name {DDB_SERVICE_DISCOVERY_TABLE} \
        --key "{{\"service_id\": {{ \"S\": \"$service_id\"}}}} " \
        --update-expression "SET #instances.#instance_id = :ip" \
        --expression-attribute-names "{{\"#instances\": \"instances\", \"#instance_id\": \"$instance_id\"}}" \
        --expression-attribute-values "{{\":ip\": {{ \"S\": \"$private_ip\"}}}}" \
        --condition-expression "attribute_exists(service_id)" 2>/dev/null; then
        echo "Updated existing service entry on attempt $retry_count" >&2
        success=true
    else
        # Try to create new item if update failed
        echo "Attempting to create new service entry (attempt $retry_count)" >&2
        if aws dynamodb put-item \
            --table-name {DDB_SERVICE_DISCOVERY_TABLE} \
            --item "{{\"service_id\": {{\"S\": \"$service_id\"}}, \"instances\": {{\"M\": {{\"$instance_id\": {{\"S\": \"$private_ip\"}}}}}}}}" \
            --condition-expression "attribute_not_exists(service_id)" 2>/dev/null; then
            echo "Created new service entry on attempt $retry_count" >&2
            success=true
        else
            echo "Both update and create failed on attempt $retry_count, retrying..." >&2
            # Exponential backoff with jitter
            sleep_time=$((retry_count + RANDOM % 3))
            sleep $sleep_time
        fi
    fi
done

if [ "$success" = "false" ]; then
    echo "FATAL: Failed to register service $service_id after $MAX_RETRIES attempts" >&2
    exit 1
fi

echo "Done" >&2
"##
    );

    run_cmd! {
        echo $register_script_content > $ddb_register_script;
        chmod +x $ddb_register_script;

        echo $systemd_unit_content > ${ETC_PATH}ddb-register.service;
        systemctl enable --now ${ETC_PATH}ddb-register.service;
    }?;
    Ok(())
}

fn create_ddb_deregister_service(service_id: &str) -> CmdResult {
    let ddb_deregister_script = format!("{BIN_PATH}ddb-deregister.sh");
    let systemd_unit_content = format!(
        r##"[Unit]
Description=DynamoDB Service Deregistration
After=network-online.target
Before=reboot.target halt.target poweroff.target kexec.target

DefaultDependencies=no

[Service]
Type=oneshot
RemainAfterExit=yes
ExecStart={ddb_deregister_script}

[Install]
WantedBy=reboot.target halt.target poweroff.target kexec.target
"##
    );

    let deregister_script_content = format!(
        r##"#!/bin/bash
set -e
service_id={service_id}
instance_id=$(ec2-metadata -i | awk '{{print $2}}')
private_ip=$(ec2-metadata -o | awk '{{print $2}}')

echo "Deregistering itself ($instance_id, $private_ip) from ddb table {DDB_SERVICE_DISCOVERY_TABLE} with service_id $service_id" >&2
aws dynamodb update-item \
    --table-name {DDB_SERVICE_DISCOVERY_TABLE} \
    --key "{{\"service_id\": {{ \"S\": \"$service_id\"}}}} " \
    --update-expression "REMOVE instances.#instance_id" \
    --expression-attribute-names "{{\"#instance_id\": \"$instance_id\"}}"
echo "Done" >&2
"##
    );

    run_cmd! {
        echo $deregister_script_content > $ddb_deregister_script;
        chmod +x $ddb_deregister_script;

        echo $systemd_unit_content > ${ETC_PATH}ddb-deregister.service;
        systemctl enable ${ETC_PATH}ddb-deregister.service;
    }?;
    Ok(())
}

pub fn get_service_ips(service_id: &str, expected_min_count: usize) -> Vec<String> {
    info!("Waiting for {expected_min_count} {service_id} service(s)");
    let start_time = Instant::now();
    let timeout = Duration::from_secs(300);
    loop {
        if start_time.elapsed() > timeout {
            cmd_die!("Timeout waiting for ${service_id} service(s)");
        }
        let key = format!(r#"{{"service_id":{{"S":"{service_id}"}}}}"#);
        let res = run_fun! {
             aws dynamodb get-item
                 --table-name ${DDB_SERVICE_DISCOVERY_TABLE}
                 --key $key
                 --projection-expression "instances"
                 --query "Item.instances.M"
                 --output json
        };
        match res {
            Ok(output) if !output.is_empty() && output != "None" && output != "null" => {
                let instances: serde_json::Value =
                    serde_json::from_str(&output).unwrap_or(serde_json::json!({}));
                let mut ips = Vec::new();
                if let Some(obj) = instances.as_object() {
                    for (_instance_id, value) in obj {
                        if let Some(ip_obj) = value.get("S")
                            && let Some(ip) = ip_obj.as_str()
                        {
                            ips.push(ip.to_string());
                        }
                    }
                }
                if ips.len() >= expected_min_count {
                    info!("Found a list of {service_id} services: {ips:?}");
                    return ips;
                }
            }
            _ => std::thread::sleep(std::time::Duration::from_secs(1)),
        }
    }
}

fn create_network_tuning_sysctl_file() -> CmdResult {
    let content = r##"# Should be a symlink file in /etc/sysctl.d
# allow TCP with buffers up to 128MB
net.core.rmem_max = 134217728
net.core.wmem_max = 134217728
# increase TCP autotuning buffer limits.
net.ipv4.tcp_rmem = 4096 87380 67108864
net.ipv4.tcp_wmem = 4096 65536 67108864
# recommended for hosts with jumbo frames enabled
net.ipv4.tcp_mtu_probing=1
# recommended to enable 'fair queueing'
net.core.default_qdisc = fq
"##;

    run_cmd! {
        info "Applying network tunning configs";
        mkdir -p $ETC_PATH;
        echo $content > $ETC_PATH/$NETWORK_TUNING_SYS_CONFIG;
        ln -nsf $ETC_PATH/$NETWORK_TUNING_SYS_CONFIG /etc/sysctl.d/;
        sysctl --system --quiet &> /dev/null;

    }?;
    Ok(())
}

pub fn num_cpus() -> Result<u64, Error> {
    let num_cpus_str = run_fun!(nproc)?;
    let num_cpus = num_cpus_str
        .trim()
        .parse::<u64>()
        .map_err(|_| Error::other(format!("invalid num_cores: {num_cpus_str}")))?;
    Ok(num_cpus)
}

pub fn create_ena_irq_affinity_service() -> CmdResult {
    let script_path = format!("{BIN_PATH}configure-ena-irq-affinity.sh");
    let systemd_unit_content = format!(
        r##"[Unit]
Description=ENA IRQ Affinity Configuration
After=network-online.target
Before=api_server.service bss.service nss.service mirrord.service bench_client.service

[Service]
Type=oneshot
ExecStart={script_path}

[Install]
WantedBy=multi-user.target
"##
    );

    let script_content = r##"#!/bin/bash
set -e

echo "Configuring ENA interrupt affinity" >&2

echo "Disabling irqbalance" >&2
systemctl disable --now irqbalance 2>/dev/null || true

iface=$(grep -o "ens[0-9]*-Tx-Rx" /proc/interrupts | head -1 | sed "s/-Tx-Rx//")
if [ -z "$iface" ]; then
    echo "ERROR: Could not detect ENA interface" >&2
    exit 1
fi
echo "Detected ENA interface: $iface" >&2

num_queues=$(grep "$iface-Tx-Rx-" /proc/interrupts | wc -l)
if [ "$num_queues" -eq 0 ]; then
    echo "ERROR: Could not detect ENA queues" >&2
    exit 1
fi
echo "Found $num_queues queues" >&2

num_cpus=$(nproc)
echo "System has $num_cpus CPUs" >&2

echo "Attempting to configure RSS" >&2
if ethtool -X $iface equal $num_cpus 2>&1; then
    echo "RSS configured successfully" >&2
else
    echo "RSS configuration failed or not supported (using hardware default)" >&2
fi

cpus_per_queue=$((num_cpus / num_queues))
echo "Spreading $num_queues queues across $num_cpus CPUs ($cpus_per_queue CPUs per queue)" >&2

for queue in $(seq 0 $((num_queues - 1))); do
    irq=$(grep "$iface-Tx-Rx-$queue" /proc/interrupts | awk -F: '{print $1}' | tr -d ' ')

    if [ -n "$irq" ]; then
        start_cpu=$((queue * cpus_per_queue))
        end_cpu=$(((queue + 1) * cpus_per_queue))
        if [ $end_cpu -gt $num_cpus ]; then
            end_cpu=$num_cpus
        fi

        mask_low=0
        mask_high=0
        for cpu in $(seq $start_cpu $((end_cpu - 1))); do
            if [ $cpu -lt 32 ]; then
                mask_low=$((mask_low | (1 << cpu)))
            else
                mask_high=$((mask_high | (1 << (cpu - 32))))
            fi
        done

        if [ $mask_high -gt 0 ]; then
            mask_str=$(printf "%08x,%08x" $mask_high $mask_low)
        else
            mask_str=$(printf "%x" $mask_low)
        fi

        echo $mask_str > /proc/irq/$irq/smp_affinity
        echo "IRQ $irq ($iface-Tx-Rx-$queue) -> CPUs $start_cpu-$end_cpu (mask: $mask_str)" >&2

        xps_path="/sys/class/net/$iface/queues/tx-$queue/xps_cpus"
        echo $mask_str > $xps_path 2>/dev/null || true
    fi
done

echo 32768 > /proc/sys/net/core/rps_sock_flow_entries || true
for queue in $(seq 0 $((num_queues - 1))); do
    rps_path="/sys/class/net/$iface/queues/rx-$queue/rps_flow_cnt"
    echo 4096 > $rps_path 2>/dev/null || true
done

mgmt_irq=$(grep -E "ena-mgmnt|$iface-mgmnt" /proc/interrupts | awk -F: '{print $1}' | tr -d ' ' | head -1)
if [ -n "$mgmt_irq" ]; then
    echo 1 > /proc/irq/$mgmt_irq/smp_affinity
    echo "Management IRQ $mgmt_irq -> CPU 0" >&2
fi

echo "Done! Current ENA IRQ affinity:" >&2
for queue in $(seq 0 $((num_queues - 1))); do
    irq=$(grep "$iface-Tx-Rx-$queue" /proc/interrupts | awk -F: '{print $1}' | tr -d ' ')
    if [ -n "$irq" ]; then
        affinity=$(cat /proc/irq/$irq/smp_affinity)
        start_cpu=$((queue * cpus_per_queue))
        end_cpu=$(((queue + 1) * cpus_per_queue))
        if [ $end_cpu -gt $num_cpus ]; then
            end_cpu=$num_cpus
        fi
        echo "Queue $queue (IRQ $irq): CPUs $start_cpu-$end_cpu, mask = 0x$affinity" >&2
    fi
done

echo "RFS configured: 32768 global flow entries, 4096 per queue" >&2
echo "XPS configured for TX steering" >&2
"##;

    run_cmd! {
        echo $script_content > $script_path;
        chmod +x $script_path;

        mkdir -p $ETC_PATH;
        echo $systemd_unit_content > ${ETC_PATH}ena-irq-affinity.service;
        systemctl enable --now ${ETC_PATH}ena-irq-affinity.service;
    }?;
    Ok(())
}

pub fn setup_serial_console_password() -> CmdResult {
    run_cmd! {
        info "Setting password for ec2-user to enable serial console access";
        echo "ec2-user:fractalbits!" | chpasswd;
    }?;
    Ok(())
}
