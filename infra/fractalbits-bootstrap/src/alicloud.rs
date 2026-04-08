use cmd_lib::*;

/// Query the Alicloud ECS instance metadata service.
pub fn alicloud_imds_get(path: &str) -> FunResult {
    let url = format!("http://100.100.100.200/latest/meta-data/{path}");
    run_fun!(curl -sf $url)
}

pub fn get_alicloud_instance_id() -> FunResult {
    alicloud_imds_get("instance-id")
}

pub fn get_alicloud_private_ip() -> FunResult {
    alicloud_imds_get("private-ipv4")
}
