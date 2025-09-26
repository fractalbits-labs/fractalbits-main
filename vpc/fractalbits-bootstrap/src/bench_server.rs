mod yaml_get;
mod yaml_mixed;
mod yaml_put;

use super::common::*;
use cmd_lib::*;
use {yaml_get::*, yaml_mixed::*, yaml_put::*};

pub fn bootstrap(api_server_endpoint: String, bench_client_num: usize) -> CmdResult {
    download_binaries(&["warp"])?;

    let client_ips = get_service_ips("bench-client", bench_client_num);

    let region = get_current_aws_region()?;
    let mut warp_client_ips = String::new();
    for ip in client_ips.iter() {
        warp_client_ips.push_str(&format!("  - {ip}:7761\n"));
    }

    create_bench_start_script(&region, &api_server_endpoint)?;

    create_put_workload_config(&warp_client_ips, &region, &api_server_endpoint, "1m", "")?;
    create_get_workload_config(&warp_client_ips, &region, &api_server_endpoint, "1m", "")?;
    create_mixed_workload_config(&warp_client_ips, &region, &api_server_endpoint, "1m")?;

    Ok(())
}

fn create_bench_start_script(region: &str, api_server_ip: &str) -> CmdResult {
    let script_content = format!(
        r##"#!/bin/bash

export AWS_ACCESS_KEY_ID=test_api_key
export AWS_SECRET_ACCESS_KEY=test_api_secret

set -ex
export AWS_DEFAULT_REGION={region}
export AWS_ENDPOINT_URL_S3=http://{api_server_ip}
bench_bucket=warp-benchmark-bucket

aws s3api create-bucket --bucket $bench_bucket &> /dev/null || true

/opt/fractalbits/bin/warp run /opt/fractalbits/etc/bench_${{WORKLOAD:-put}}.yml
"##
    );
    run_cmd! {
        mkdir -p $BIN_PATH;
        echo $script_content > $BIN_PATH/$BENCH_SERVER_BENCH_START_SCRIPT;
        chmod +x $BIN_PATH/$BENCH_SERVER_BENCH_START_SCRIPT;
    }?;
    Ok(())
}
