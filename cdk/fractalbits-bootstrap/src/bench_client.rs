use super::common::*;
use cmd_lib::*;

pub fn bootstrap() -> CmdResult {
    download_binaries(&["warp"])?;
    create_systemd_unit_file("bench_client", true)?;
    Ok(())
}
