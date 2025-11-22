use crate::*;

pub fn run_cmd_nightly() -> CmdResult {
    run_cmd! {
        info "Building ...";
        cd ./core;
        zig build -p ../$ZIG_DEBUG_OUT 2>&1;
    }?;

    let nightly_log = "test_fractal_art_nightly.log";
    let size = 1000000;
    let ops = 80000000;
    let threads_num = 100;
    run_cmd! {
        info "Running fractal art tests (random) with log $nightly_log ...";
        ./$ZIG_DEBUG_OUT/bin/nss_server format;
        ./$ZIG_DEBUG_OUT/bin/test_fractal_art --tests random --size $size --ops $ops --threads $threads_num |& ts -m $TS_FMT >$nightly_log;
    }
    .map_err(|e| {
        run_cmd!(tail $nightly_log).unwrap();
        e
    })?;
    Ok(())
}
