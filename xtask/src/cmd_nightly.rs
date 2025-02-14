use crate::TS_FMT;
use cmd_lib::*;

pub fn run_cmd_nightly() -> CmdResult {
    run_cmd! {
        info "Building ...";
        zig build 2>&1;
    }?;

    let nightly_log = "test_art_nightly.log";
    let size = 1000000;
    let ops = 80000000;
    let depth = 100;
    run_cmd! {
        info "Running art tests (random) with log $nightly_log ...";
        ./zig-out/bin/mkfs;
        ./zig-out/bin/test_art --tests random --size $size --ops $ops -d $depth |& ts -m $TS_FMT >$nightly_log;
    }
    .map_err(|e| {
        run_cmd!(tail $nightly_log).unwrap();
        e
    })?;
    Ok(())
}
