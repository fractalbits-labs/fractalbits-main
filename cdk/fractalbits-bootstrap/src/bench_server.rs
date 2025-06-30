use super::common::*;
use cmd_lib::*;

pub fn bootstrap(service_endpoint: &str) -> CmdResult {
    download_binaries(&["warp"])?;
    create_workload_config(service_endpoint)?;
    create_systemd_unit_file("bench_server", false)?;
    Ok(())
}

fn create_workload_config(service_endpoint: &str) -> CmdResult {
    let config_content = format!(
        r##"benchmark: mixed
host: {service_endpoint}
access-key: test_api_key
secret-key: test_api_secret
bucket: warp-benchmark-bucket
tls: false

warp-client:
  - warp-client-1:7761
  - warp-client-2:7761

duration: 10m
obj.size: 4KB
concurrent: 50
autoterm: true
"##
    );
    run_cmd! {
        mkdir -p $ETC_PATH;
        echo $config_content > $ETC_PATH/$BENCH_SERVER_WORKLOAD_CONFIG;
    }?;
    Ok(())
}
