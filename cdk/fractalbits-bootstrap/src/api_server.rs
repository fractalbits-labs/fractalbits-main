use crate::*;

pub fn bootstrap(
    bucket_name: &str,
    remote_bucket: Option<&str>,
    nss_ip: &str,
    rss_ip: &str,
    for_bench: bool,
) -> CmdResult {
    install_rpms(&["amazon-cloudwatch-agent", "nmap-ncat", "perf"])?;
    download_binaries(&["api_server"])?;

    // Check if we're using S3 Express One Zone by looking at bucket name
    let is_s3_express = bucket_name.ends_with("--x-s3");

    let bss_ip = if is_s3_express {
        info!("Using S3 Express One Zone storage, skipping BSS server");
        String::new()
    } else {
        info!("Waiting for bss");
        let bss_ip = get_service_ips("bss-server", 1)[0].clone();
        for (role, ip) in [("bss", bss_ip.as_str()), ("rss", rss_ip), ("nss", nss_ip)] {
            info!("Waiting for {role} node with ip {ip} to be ready");
            while run_cmd!(nc -z $ip 8088 &>/dev/null).is_err() {
                std::thread::sleep(std::time::Duration::from_secs(1));
            }
            info!("{role} node can be reached (`nc -z {ip} 8088` is ok)");
        }
        bss_ip
    };

    // For S3 Express, only wait for RSS and NSS
    if is_s3_express {
        for (role, ip) in [("rss", rss_ip), ("nss", nss_ip)] {
            info!("Waiting for {role} node with ip {ip} to be ready");
            while run_cmd!(nc -z $ip 8088 &>/dev/null).is_err() {
                std::thread::sleep(std::time::Duration::from_secs(1));
            }
            info!("{role} node can be reached (`nc -z {ip} 8088` is ok)");
        }
    }

    create_config(bucket_name, remote_bucket, &bss_ip, nss_ip, rss_ip)?;

    if for_bench {
        // Try to download tools for micro-benchmarking
        download_binaries(&["rewrk_rpc", "fbs", "test_art"])?;
        // Testing data for bss-rpc
        xtask_tools::gen_uuids(1_000_000, "/data/uuids.data")?;
    }

    // setup_cloudwatch_agent()?;
    create_systemd_unit_file("api_server", true)?;
    create_ddb_register_and_deregister_service("api-server")?;

    Ok(())
}

pub fn create_config(
    bucket_name: &str,
    remote_bucket: Option<&str>,
    bss_ip: &str,
    nss_ip: &str,
    rss_ip: &str,
) -> CmdResult {
    let aws_region = get_current_aws_region()?;
    let aws_az = get_current_aws_az()?;
    let num_cores = run_fun!(nproc)?;
    let is_s3_express = bucket_name.ends_with("--x-s3");
    let is_multi_az = remote_bucket.is_some();

    // Extract zone ID from S3 Express bucket name (format: bucket-base-name--zone-id--x-s3)
    let extract_zone_id = |bucket: &str| -> Option<String> {
        if bucket.ends_with("--x-s3") {
            let parts: Vec<&str> = bucket.split("--").collect();
            if parts.len() >= 3 {
                // Zone ID is the second-to-last part (before --x-s3)
                return Some(parts[parts.len() - 2].to_string());
            }
        }
        None
    };

    let config_content = if is_s3_express && is_multi_az {
        // S3 Express Multi-AZ configuration
        let remote_bucket = remote_bucket.unwrap();

        // Extract zone IDs from bucket names
        let local_zone_id = extract_zone_id(bucket_name)
            .unwrap_or_else(|| panic!("Failed to extract zone ID from bucket name: {bucket_name}"));
        let remote_zone_id = extract_zone_id(remote_bucket).unwrap_or_else(|| {
            panic!("Failed to extract zone ID from remote bucket name: {remote_bucket}")
        });

        format!(
            r##"nss_addr = "{nss_ip}:8088"
rss_addr = "{rss_ip}:8088"
nss_conn_num = {num_cores}
rss_conn_num = 1
region = "{aws_region}"
port = 80
root_domain = ".localhost"
with_metrics = true
http_request_timeout_seconds = 5
rpc_timeout_seconds = 4
allow_missing_or_bad_signature = false

[blob_storage]
backend = "s3_express_multi_az"

[blob_storage.s3_express_multi_az]
local_az_host = "http://s3express-{local_zone_id}.{aws_region}.amazonaws.com"
local_az_port = 80
remote_az_host = "http://s3express-{remote_zone_id}.{aws_region}.amazonaws.com"
remote_az_port = 80
s3_region = "{aws_region}"
local_az_bucket = "{bucket_name}"
remote_az_bucket = "{remote_bucket}"
az = "{aws_az}"
force_path_style = false

[blob_storage.s3_express_multi_az.ratelimit]
enabled = false
put_qps = 7000
get_qps = 10000
delete_qps = 5000

[blob_storage.s3_express_multi_az.retry]
retry_mode = "standard"
max_attempts = 15
"##
        )
    } else if is_s3_express {
        // S3 Express Single-AZ configuration

        // Extract zone ID from bucket name
        let zone_id = extract_zone_id(bucket_name)
            .unwrap_or_else(|| panic!("Failed to extract zone ID from bucket name: {bucket_name}"));

        format!(
            r##"nss_addr = "{nss_ip}:8088"
rss_addr = "{rss_ip}:8088"
nss_conn_num = {num_cores}
rss_conn_num = 1
region = "{aws_region}"
port = 80
root_domain = ".localhost"
with_metrics = true
http_request_timeout_seconds = 5
rpc_timeout_seconds = 4
allow_missing_or_bad_signature = false

[blob_storage]
backend = "s3_express_single_az"

[blob_storage.s3_express_single_az]
s3_host = "http://s3express-{zone_id}.{aws_region}.amazonaws.com"
s3_port = 80
s3_region = "{aws_region}"
s3_bucket = "{bucket_name}"
az = "{aws_az}"
force_path_style = false

[blob_storage.s3_express_single_az.ratelimit]
enabled = false
put_qps = 7000
get_qps = 10000
delete_qps = 5000

[blob_storage.s3_express_single_az.retry]
retry_mode = "standard"
max_attempts = 15
"##
        )
    } else {
        // Hybrid single az configuration
        format!(
            r##"bss_addr = "{bss_ip}:8088"
nss_addr = "{nss_ip}:8088"
rss_addr = "{rss_ip}:8088"
bss_conn_num = {num_cores}
nss_conn_num = {num_cores}
rss_conn_num = 1
region = "{aws_region}"
port = 80
root_domain = ".localhost"
with_metrics = false
http_request_timeout_seconds = 5
rpc_timeout_seconds = 4
allow_missing_or_bad_signature = false

[blob_storage]
backend = "hybrid_single_az"

[blob_storage.bss]
addr = "{bss_ip}:8088"
conn_num = {num_cores}

[blob_storage.s3_hybrid]
s3_host = "http://s3.{aws_region}.amazonaws.com"
s3_port = 80
s3_region = "{aws_region}"
s3_bucket = "{bucket_name}"

[blob_storage.s3_hybrid.ratelimit]
enabled = false
put_qps = 7000
get_qps = 10000
delete_qps = 5000

[blob_storage.s3_hybrid.retry]
retry_mode = "standard"
max_attempts = 15
"##
        )
    };

    run_cmd! {
        mkdir -p $ETC_PATH;
        echo $config_content > $ETC_PATH/$API_SERVER_CONFIG
    }?;
    Ok(())
}
