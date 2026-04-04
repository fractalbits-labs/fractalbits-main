mod config_gen;
mod vpc;

use cmd_lib::*;
pub use vpc::{create_vpc, destroy_vpc};

/// Resolve OCI compartment OCID from: CLI arg > OCI_COMPARTMENT_OCID env var
pub fn resolve_oci_compartment(cli_arg: Option<&str>) -> Result<String, std::io::Error> {
    if let Some(c) = cli_arg.filter(|s| !s.is_empty()) {
        return Ok(c.to_string());
    }
    if let Ok(c) = std::env::var("OCI_COMPARTMENT_OCID")
        && !c.is_empty()
    {
        return Ok(c);
    }
    // Try reading from OCI CLI config
    if let Ok(c) = run_fun!(oci iam compartment list --query "data[0].id" --raw-output 2>/dev/null)
        && !c.is_empty()
        && c.starts_with("ocid1.")
    {
        return Ok(c);
    }
    Err(std::io::Error::other(
        "OCI compartment OCID required. Set via --oci-compartment or OCI_COMPARTMENT_OCID env var",
    ))
}

/// Resolve OCI tenancy OCID from: CLI arg > OCI_TENANCY_OCID env var > OCI CLI config
pub fn resolve_oci_tenancy(cli_arg: Option<&str>) -> Result<String, std::io::Error> {
    if let Some(t) = cli_arg.filter(|s| !s.is_empty()) {
        return Ok(t.to_string());
    }
    if let Ok(t) = std::env::var("OCI_TENANCY_OCID")
        && !t.is_empty()
    {
        return Ok(t);
    }
    if let Ok(t) = run_fun!(oci iam tenancy get --query "data.id" --raw-output 2>/dev/null)
        && !t.is_empty()
        && t.starts_with("ocid1.")
    {
        return Ok(t);
    }
    Err(std::io::Error::other(
        "OCI tenancy OCID required. Set via --oci-tenancy or OCI_TENANCY_OCID env var",
    ))
}

/// Resolve OCI region from: CLI arg > OCI_REGION env > OCI CLI config > default
pub fn resolve_oci_region(cli_arg: Option<&str>) -> String {
    if let Some(r) = cli_arg.filter(|s| !s.is_empty()) {
        return r.to_string();
    }
    if let Ok(r) = std::env::var("OCI_REGION")
        && !r.is_empty()
    {
        return r;
    }
    if let Ok(r) = run_fun!(oci iam region-subscription list --query "data[0].\"region-name\"" --raw-output 2>/dev/null)
        && !r.is_empty()
    {
        return r;
    }
    "us-ashburn-1".to_string()
}
