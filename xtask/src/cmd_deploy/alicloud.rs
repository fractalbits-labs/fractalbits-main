mod config_gen;
mod vpc;

pub use vpc::{create_vpc, destroy_vpc};

pub fn resolve_alicloud_region(cli_arg: Option<&str>) -> String {
    if let Some(r) = cli_arg.filter(|s| !s.is_empty()) {
        return r.to_string();
    }
    if let Ok(r) = std::env::var("ALICLOUD_REGION")
        && !r.is_empty()
    {
        return r;
    }
    "cn-shanghai".to_string()
}

/// Resolve Alicloud zone from: CLI arg > ALICLOUD_ZONE env > default (region + "-b")
pub fn resolve_alicloud_zone(cli_arg: Option<&str>, region: &str) -> String {
    if let Some(z) = cli_arg.filter(|s| !s.is_empty()) {
        return z.to_string();
    }
    if let Ok(z) = std::env::var("ALICLOUD_ZONE")
        && !z.is_empty()
    {
        return z;
    }
    format!("{region}-b")
}
