use super::common::*;
use cmd_lib::*;
use std::io::Error;

const BLOB_DRAM_MEM_PERCENT: f64 = 0.8;
const EBS_SPACE_PERCENT: f64 = 0.9;

pub fn bootstrap(
    bucket_name: &str,
    volume_id: &str,
    num_nvme_disks: usize,
    meta_stack_testing: bool,
    for_bench: bool,
) -> CmdResult {
    assert_ne!(num_nvme_disks, 0);

    install_rpms(&["nvme-cli", "mdadm", "perf", "lldb"])?;
    if meta_stack_testing || for_bench {
        download_binaries(&["fbs", "test_art", "rewrk_rpc"])?;
    }
    format_local_nvme_disks(num_nvme_disks, false)?;
    download_binaries(&["nss_server"])?;
    setup_configs(bucket_name, volume_id, "nss_server")?;

    // Note for normal deployment, the nss_server service is not started
    // until EBS/nss formatted from root_server
    if meta_stack_testing {
        let volume_dev = get_volume_dev(volume_id);
        format_nss(volume_dev, true)?;
    }
    Ok(())
}

fn setup_configs(bucket_name: &str, volume_id: &str, service_name: &str) -> CmdResult {
    let volume_dev = get_volume_dev(volume_id);
    create_nss_config(bucket_name, &volume_dev)?;
    create_mount_unit(&volume_dev, "/data/ebs", "ext4")?;
    create_ebs_udev_rule(volume_id, service_name)?;
    create_coredump_config()?;
    create_systemd_unit_file(service_name, false)?;
    Ok(())
}

fn create_nss_config(bucket_name: &str, volume_dev: &str) -> CmdResult {
    let aws_region = get_current_aws_region()?;

    // Get total memory in kilobytes from /proc/meminfo
    let total_mem_kb_str = run_fun!(cat /proc/meminfo | grep MemTotal | awk r"{print $2}")?;
    let total_mem_kb = total_mem_kb_str
        .trim()
        .parse::<u64>()
        .map_err(|_| Error::other(format!("invalid total_mem_kb: {total_mem_kb_str}")))?;

    // Calculate total memory for blob_dram_kilo_bytes
    let blob_dram_kilo_bytes = (total_mem_kb as f64 * BLOB_DRAM_MEM_PERCENT) as u64;

    // Get total size of volume_dev in 1M blocks (which rounds down)
    let ebs_blockdev_size_str = run_fun!(blockdev --getsize64 ${volume_dev})?;
    let ebs_blockdev_size = ebs_blockdev_size_str.trim().parse::<u64>().map_err(|_| {
        Error::other(format!(
            "invalid ebs blockdev size: {ebs_blockdev_size_str}"
        ))
    })?;
    let ebs_blockdev_mb = ebs_blockdev_size / 1024 / 1024;
    let art_journal_segment_size =
        (ebs_blockdev_mb as f64 * EBS_SPACE_PERCENT) as u64 * 1024 * 1024;

    let config_content = format!(
        r##"server_port = 8088
blob_dram_kilo_bytes = {blob_dram_kilo_bytes}
art_journal_segment_size = {art_journal_segment_size}
log_level = "info"

[s3_cache]
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

pub fn format_nss(ebs_dev: String, testing_mode: bool) -> CmdResult {
    run_cmd! {
        info "Disabling udev rules for EBS";
        ln -sf /dev/null /etc/udev/rules.d/99-ebs.rules;

        info "Formatting $ebs_dev to ext4 file system (${EXT4_MKFS_OPTS:?})";
        mkfs.ext4 $[EXT4_MKFS_OPTS] $ebs_dev;

        info "Mounting $ebs_dev to /data/ebs";
        mkdir -p /data/ebs;
        mount $ebs_dev /data/ebs;
    }?;

    let mut wait_secs = 0;
    while run_cmd!(mountpoint -q "/data/local").is_err() {
        wait_secs += 1;
        info!("Waiting for /data/local to be mounted ({wait_secs}s)");
        std::thread::sleep(std::time::Duration::from_secs(1));
        if wait_secs >= 120 {
            cmd_die!("Timeout when waiting for /data/local to be mounted (120s)");
        }
    }

    info!("Creating directories for nss_server");
    for i in 0..256 {
        run_cmd!(mkdir -p /data/local/meta_cache/blobs/dir$i)?;
    }
    run_cmd! {
        info "Syncing file system changes";
        sync;
    }?;

    run_cmd! {
        info "Running format for nss_server";
        cd /data;
        /opt/fractalbits/bin/nss_server format -c /opt/fractalbits/etc/nss_server_cloud_config.toml;
    }?;

    if testing_mode {
        run_cmd! {
            cd /data;

            info "Running nss fbs";
            /opt/fractalbits/bin/fbs --new_tree $TEST_BUCKET_ROOT_BLOB_NAME;

            info "Generating random 10_000_000 keys";
            /opt/fractalbits/bin/test_art --gen --size 10000000;
        }?;
    }

    run_cmd! {
        info "Enabling udev rules for EBS";
        ln -sf /opt/fractalbits/etc/99-ebs.rules /etc/udev/rules.d/99-ebs.rules;
        udevadm control --reload-rules;
        udevadm trigger;

        info "${ebs_dev} is formatted successfully.";
    }?;

    Ok(())
}
