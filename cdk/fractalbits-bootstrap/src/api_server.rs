use crate::*;

pub fn bootstrap(
    bucket: Option<&str>,
    nss_endpoint: &str,
    rss_endpoint: &str,
    remote_az: Option<&str>,
    for_bench: bool,
) -> CmdResult {
    download_binaries(&["api_server"])?;

    // Check if we're using S3 Express by checking if remote_az is provided
    let is_s3_express = remote_az.is_some();

    let bss_ip = if is_s3_express {
        info!("Using S3 Express One Zone storage, skipping BSS server");
        String::new()
    } else {
        info!("Waiting for bss");
        let bss_ip = get_service_ips("bss-server", 1)[0].clone();
        for (role, endpoint) in [
            ("bss", bss_ip.as_str()),
            ("rss", rss_endpoint),
            ("nss", nss_endpoint),
        ] {
            info!("Waiting for {role} node {endpoint} to be ready");
            while run_cmd!(nc -z $endpoint 8088 &>/dev/null).is_err() {
                std::thread::sleep(std::time::Duration::from_secs(1));
            }
            info!("{role} node can be reached (`nc -z {endpoint} 8088` is ok)");
        }
        bss_ip
    };

    // For S3 Express, only wait for RSS and NSS
    if is_s3_express {
        for (role, ip) in [("rss", rss_endpoint), ("nss", nss_endpoint)] {
            info!("Waiting for {role} node {ip} to be ready");
            while run_cmd!(nc -z $ip 8088 &>/dev/null).is_err() {
                std::thread::sleep(std::time::Duration::from_secs(1));
            }
            info!("{role} node can be reached (`nc -z {ip} 8088` is ok)");
        }
    }

    create_config(bucket, &bss_ip, nss_endpoint, rss_endpoint, remote_az)?;

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
    bucket: Option<&str>,
    bss_ip: &str,
    nss_endpoint: &str,
    rss_endpoint: &str,
    remote_az: Option<&str>,
) -> CmdResult {
    let aws_region = get_current_aws_region()?;
    let num_cores = run_fun!(nproc)?;
    let config_content = if let Some(remote_az) = remote_az {
        // S3 Express Multi-AZ configuration
        let local_az = get_current_aws_az_id()?;
        let local_bucket = get_s3_express_bucket_name(&local_az)?;
        let remote_bucket = get_s3_express_bucket_name(remote_az)?;

        format!(
            r##"nss_addr = "{nss_endpoint}:8088"
rss_addr = "{rss_endpoint}:8088"
nss_conn_num = {num_cores}
rss_conn_num = 1
region = "{aws_region}"
port = 80
mgmt_port = 18088
root_domain = ".localhost"
with_metrics = true
http_request_timeout_seconds = 5
rpc_timeout_seconds = 4
allow_missing_or_bad_signature = false

[https]
enabled = false
port = 443
cert_file = "/opt/fractalbits/etc/cert.pem"
key_file = "/opt/fractalbits/etc/key.pem"
force_http1_only = false

[blob_storage]
backend = "s3_express_multi_az_with_tracking"

[blob_storage.s3_express_multi_az]
local_az_host = "http://s3.{aws_region}.amazonaws.com"
local_az_port = 80
remote_az_host = "http://s3.{aws_region}.amazonaws.com"
remote_az_port = 80
s3_region = "{aws_region}"
local_az_bucket = "{local_bucket}"
remote_az_bucket = "{remote_bucket}"
local_az = "{local_az}"
remote_az = "{remote_az}"
force_path_style = false

[blob_storage.s3_express_multi_az.ratelimit]
enabled = false
put_qps = 7000
get_qps = 10000
delete_qps = 5000

[blob_storage.s3_express_multi_az.retry_config]
enabled = false
max_attempts = 15
initial_backoff_us = 50
max_backoff_us = 500
backoff_multiplier = 1.0
"##
        )
    } else {
        // Hybrid single az configuration
        let bucket_name =
            bucket.ok_or_else(|| std::io::Error::other("Bucket name required for hybrid mode"))?;
        format!(
            r##"bss_addr = "{bss_ip}:8088"
nss_addr = "{nss_endpoint}:8088"
rss_addr = "{rss_endpoint}:8088"
bss_conn_num = {num_cores}
nss_conn_num = {num_cores}
rss_conn_num = 1
region = "{aws_region}"
port = 80
mgmt_port = 18088
root_domain = ".localhost"
with_metrics = false
http_request_timeout_seconds = 5
rpc_timeout_seconds = 4
allow_missing_or_bad_signature = false

[https]
enabled = false
port = 443
cert_file = "/opt/fractalbits/etc/cert.pem"
key_file = "/opt/fractalbits/etc/key.pem"
force_http1_only = false

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

[blob_storage.s3_hybrid.retry_config]
enabled = true
max_attempts = 8
initial_backoff_us = 15000
max_backoff_us = 2000000
backoff_multiplier = 1.8
"##
        )
    };

    run_cmd! {
        mkdir -p $ETC_PATH;
        echo $config_content > $ETC_PATH/$API_SERVER_CONFIG
    }?;
    Ok(())
}
