use cmd_lib::*;
use std::time::{Duration, Instant};

pub const BIN_PATH: &str = "/opt/fractalbits/bin/";
pub const ETC_PATH: &str = "/opt/fractalbits/etc/";
pub const WEB_ROOT: &str = "/opt/fractalbits/www/";
pub const API_SERVER_CONFIG: &str = "api_server_cloud_config.toml";
pub const BSS_SERVER_CONFIG: &str = "bss_server_cloud_config.toml";
pub const NSS_SERVER_CONFIG: &str = "nss_server_cloud_config.toml";
pub const ROOT_SERVER_CONFIG: &str = "root_server_cloud_config.toml";
pub const BENCH_SERVER_BENCH_START_SCRIPT: &str = "bench_start.sh";
pub const BOOTSTRAP_DONE_FILE: &str = "/opt/fractalbits/.bootstrap_done";
pub const STATS_LOGROTATE_CONFIG: &str = "/etc/logrotate.d/stats_logs";
pub const DDB_SERVICE_DISCOVERY_TABLE: &str = "fractalbits-service-discovery";
pub const NETWORK_TUNING_SYS_CONFIG: &str = "99-network-tuning.conf";
#[allow(dead_code)]
pub const CLOUDWATCH_AGENT_CONFIG: &str = "cloudwatch_agent_config.json";
pub const TEST_BUCKET_ROOT_BLOB_NAME: &str = "947ef2be-44b2-4ac2-969b-2574eb85662b";
pub const CLOUD_INIT_LOG: &str = "/var/log/cloud-init-output.log";
pub const EXT4_MKFS_OPTS: [&str; 4] = ["-O", "bigalloc", "-C", "16384"];

pub fn common_setup() -> CmdResult {
    create_network_tuning_sysctl_file()?;
    Ok(())
}

pub fn download_binaries(file_list: &[&str]) -> CmdResult {
    for file_name in file_list {
        download_binary(file_name)?;
    }
    Ok(())
}

fn download_binary(file_name: &str) -> CmdResult {
    if run_cmd!(test -f $BIN_PATH/$file_name).is_ok() {
        info!("{file_name} has already been downloaded");
        return Ok(());
    }

    let builds_bucket = format!("s3://fractalbits-builds-{}", get_current_aws_region()?);
    let cpu_arch = run_fun!(arch)?;
    run_cmd! {
        info "Downloading $file_name from $builds_bucket to $BIN_PATH";
        aws s3 cp --no-progress $builds_bucket/$cpu_arch/$file_name $BIN_PATH;
        chmod +x $BIN_PATH/$file_name
    }?;
    Ok(())
}

pub fn create_systemd_unit_file(service_name: &str, enable_now: bool) -> CmdResult {
    let aws_region = get_current_aws_region()?;
    let working_dir = "/data";
    let mut requires = "";
    let mut env_settings = String::new();
    let exec_start = match service_name {
        "api_server" => {
            env_settings = r##"
Environment="RUST_LOG=warn""##
                .to_string();
            format!("{BIN_PATH}{service_name} -c {ETC_PATH}{API_SERVER_CONFIG}")
        }
        "nss_server" => {
            requires = "data-ebs.mount data-local.mount";
            format!("{BIN_PATH}nss_server serve -c {ETC_PATH}{NSS_SERVER_CONFIG}")
        }
        "root_server" => {
            env_settings = r##"
Environment="RUST_LOG=warn""##
                .to_string();
            format!("{BIN_PATH}{service_name} -r {aws_region} -c {ETC_PATH}{ROOT_SERVER_CONFIG}")
        }
        "bss_server" => {
            requires = "data-local.mount";
            format!("{BIN_PATH}{service_name} -c {ETC_PATH}{BSS_SERVER_CONFIG}")
        }
        "bench_client" => {
            format!("{BIN_PATH}warp client")
        }
        // "ebs-failover" => {
        //     env_settings = r##"
        // Environment="RUST_LOG=warn""##
        //         .to_string();
        //     format!("{BIN_PATH}{service_name} -r {aws_region}")
        // }
        _ => unreachable!(),
    };
    let systemd_unit_content = format!(
        r##"[Unit]
Description={service_name} Service
After=network-online.target {requires}
Requires={requires}
BindsTo={requires}
# Limit to 3 restarts within a 10-minute (600 second) interval
StartLimitIntervalSec=600
StartLimitBurst=3

[Service]
LimitNOFILE=1000000
LimitCORE=infinity
Restart=on-failure
RestartSec=5
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

pub fn get_current_aws_az() -> FunResult {
    run_fun!(ec2-metadata --availability-zone | awk r"{print $2}")
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
    run_cmd!(echo $service_id > ${ETC_PATH}service_id)?;
    create_ddb_register_service()?;
    create_ddb_deregister_service()?;
    Ok(())
}

fn create_ddb_register_service() -> CmdResult {
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
service_id=$(cat {ETC_PATH}service_id) || exit 0 # not registered yet
instance_id=$(ec2-metadata -i | awk '{{print $2}}')
private_ip=$(ec2-metadata -o | awk '{{print $2}}')

echo "Registering itself ($instance_id,$private_ip) to ddb table {DDB_SERVICE_DISCOVERY_TABLE} with service_id $service_id" >&2
aws dynamodb update-item \
    --table-name {DDB_SERVICE_DISCOVERY_TABLE} \
    --key "{{\"service_id\": {{ \"S\": \"$service_id\"}}}} " \
    --update-expression "ADD ips :ip" \
    --expression-attribute-values "{{\":ip\": {{ \"SS\": [\"$private_ip\"]}}}} "
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

fn create_ddb_deregister_service() -> CmdResult {
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
service_id=$(cat {ETC_PATH}service_id) || exit 0 # not registered yet
instance_id=$(ec2-metadata -i | awk '{{print $2}}')
private_ip=$(ec2-metadata -o | awk '{{print $2}}')

echo "Deregistering itself ($instance_id, $private_ip) from ddb table {DDB_SERVICE_DISCOVERY_TABLE} with service_id $service_id" >&2
aws dynamodb update-item \
    --table-name {DDB_SERVICE_DISCOVERY_TABLE} \
    --key "{{\"service_id\": {{ \"S\": \"$service_id\"}}}} " \
    --update-expression "DELETE ips :ip" \
    --expression-attribute-values "{{\":ip\": {{ \"SS\": [\"$private_ip\"]}}}} "
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
            cmd_die!("Timeout waiting for {service_id} service(s)");
        }
        let key = format!(r#"{{"service_id":{{"S":"{service_id}"}}}}"#);
        let res = run_fun! {
             aws dynamodb get-item
                 --table-name ${DDB_SERVICE_DISCOVERY_TABLE}
                 --key $key
                 --projection-expression "ips"
                 --query "Item.ips.SS"
                 --output text
        };
        match res {
            Ok(output) if !output.is_empty() && output != "None" => {
                let ips: Vec<String> = output.split_whitespace().map(String::from).collect();
                if ips.len() >= expected_min_count {
                    info!("Found a list of {service_id} clients: {ips:?}");
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
        sysctl --system --quiet;

    }?;
    Ok(())
}
