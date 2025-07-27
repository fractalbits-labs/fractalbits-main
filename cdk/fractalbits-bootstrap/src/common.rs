use cmd_lib::*;

pub const BIN_PATH: &str = "/opt/fractalbits/bin/";
pub const ETC_PATH: &str = "/opt/fractalbits/etc/";
pub const WEB_ROOT: &str = "/opt/fractalbits/www/";
pub const API_SERVER_CONFIG: &str = "api_server_cloud_config.toml";
pub const BSS_SERVER_CONFIG: &str = "bss_server_cloud_config.toml";
pub const NSS_SERVER_CONFIG: &str = "nss_server_cloud_config.toml";
pub const ROOT_SERVER_CONFIG: &str = "root_server_cloud_config.toml";
pub const BENCH_SERVER_WORKLOAD_CONFIG: &str = "bench_workload.yaml";
pub const BENCH_SERVER_BENCH_START_SCRIPT: &str = "bench_start.sh";
pub const BOOTSTRAP_DONE_FILE: &str = "/opt/fractalbits/.bootstrap_done";
#[allow(dead_code)]
pub const CLOUDWATCH_AGENT_CONFIG: &str = "cloudwatch_agent_config.json";
pub const TEST_BUCKET_ROOT_BLOB_NAME: &str = "947ef2be-44b2-4ac2-969b-2574eb85662b";
pub const CLOUD_INIT_LOG: &str = "/var/log/cloud-init-output.log";
pub const EXT4_MKFS_OPTS: [&str; 4] = ["-O", "bigalloc", "-C", "16384"];

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
Environment="RUST_LOG=info""##
                .to_string();
            format!("{BIN_PATH}{service_name} -c {ETC_PATH}{API_SERVER_CONFIG}")
        }
        "nss_server" => {
            requires = "data-ebs.mount data-local.mount";
            format!("{BIN_PATH}nss_server serve -c {ETC_PATH}{NSS_SERVER_CONFIG}")
        }
        "root_server" => {
            env_settings = r##"
Environment="RUST_LOG=info""##
                .to_string();
            format!("{BIN_PATH}{service_name} -r {aws_region} -c {ETC_PATH}{ROOT_SERVER_CONFIG}")
        }
        "bss_server" => {
            requires = "data-local.mount";
            format!("{BIN_PATH}{service_name} -c {ETC_PATH}{BSS_SERVER_CONFIG}")
        }
        "bench_server" => {
            format!("{BIN_PATH}warp run {ETC_PATH}{BENCH_SERVER_WORKLOAD_CONFIG}")
        }
        "bench_client" => {
            format!("{BIN_PATH}warp client")
        }
        "ebs-failover" => {
            env_settings = r##"
Environment="RUST_LOG=info""##
                .to_string();
            format!("{BIN_PATH}{service_name} -r {aws_region}")
        }
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

pub fn get_current_aws_region() -> FunResult {
    run_fun!(ec2-metadata --region | awk r"{print $2}")
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

pub fn create_cloudmap_register_and_deregister_service(service_id: &str) -> CmdResult {
    run_cmd!(echo $service_id > ${ETC_PATH}service_id)?;
    create_cloudmap_register_service()?;
    create_cloudmap_deregister_service()?;
    Ok(())
}

fn create_cloudmap_register_service() -> CmdResult {
    let cloudmap_register_script = format!("{BIN_PATH}cloudmap-register.sh");
    let systemd_unit_content = format!(
        r##"[Unit]
Description=Cloud Map Registration Service
After=network-online.target

[Service]
Type=oneshot
ExecStart={cloudmap_register_script}

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

echo "Registering itself ($instance_id,$private_ip) to cloudmap $service_id" >&2
aws servicediscovery register-instance \
    --service-id $service_id \
    --instance-id $instance_id \
    --attributes AWS_INSTANCE_IPV4=$private_ip
echo "Done" >&2
"##
    );

    run_cmd! {
        echo $register_script_content > $cloudmap_register_script;
        chmod +x $cloudmap_register_script;

        echo $systemd_unit_content > ${ETC_PATH}cloudmap-register.service;
        systemctl enable --now ${ETC_PATH}cloudmap-register.service;
    }?;
    Ok(())
}

fn create_cloudmap_deregister_service() -> CmdResult {
    let cloudmap_deregister_script = format!("{BIN_PATH}cloudmap-deregister.sh");
    let systemd_unit_content = format!(
        r##"[Unit]
Description=Cloud Map Deregistration Service
After=network-online.target
Before=reboot.target halt.target poweroff.target kexec.target

DefaultDependencies=no

[Service]
Type=oneshot
RemainAfterExit=yes
ExecStart=/bin/true
ExecStop={cloudmap_deregister_script}

[Install]
WantedBy=reboot.target halt.target poweroff.target kexec.target
"##
    );

    let deregister_script_content = format!(
        r##"#!/bin/bash
set -e
service_id=$(cat {ETC_PATH}service_id) || exit 0 # not registered yet
instance_id=$(ec2-metadata -i | awk '{{print $2}}')

echo "Deregistering itself ($instance_id) from cloudmap $service_id" >&2
op_id=$(aws servicediscovery deregister-instance \
    --service-id $service_id \
    --instance-id $instance_id \
    --output text \
    --query 'OperationId')

if [ -z "$op_id" ]; then
    echo "Failed to get operation ID for deregister-instance" >&2
    exit 1
fi

echo "Waiting for deregistration to complete (operation: $op_id)..." >&2
while true; do
    status=$(aws servicediscovery get-operation \
        --operation-id "$op_id" \
        --output text \
        --query 'Operation.Status')
    case "$status" in
        SUCCESS)
            echo "Deregistration successful." >&2
            break
            ;;
        FAIL)
            echo "Deregistration failed." >&2
            exit 1
            ;;
        SUBMITTED|PENDING)
            sleep 1
            ;;
        *)
            echo "Unknown status: $status" >&2
            exit 1
            ;;
    esac
done
echo "Done" >&2
"##
    );

    run_cmd! {
        echo $deregister_script_content > $cloudmap_deregister_script;
        chmod +x $cloudmap_deregister_script;

        echo $systemd_unit_content > ${ETC_PATH}cloudmap-deregister.service;
        systemctl enable ${ETC_PATH}cloudmap-deregister.service;
    }?;
    Ok(())
}
