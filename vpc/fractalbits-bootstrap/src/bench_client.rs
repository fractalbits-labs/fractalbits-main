use super::common::*;
use cmd_lib::*;

pub fn bootstrap() -> CmdResult {
    download_binaries(&["warp"])?;
    create_systemd_unit_file("bench_client", true)?;
    create_ddb_register_and_deregister_service("bench-client")?;
    Ok(())
}
