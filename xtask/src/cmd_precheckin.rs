use cmd_lib::*;

pub fn run_precheckin() -> CmdResult {
    run_cmd! {
        info "Building ...";
        zig build;
    }?;

    run_cmd! {
        info "Running zig unit tests ...";
        zig build test --summary all;
    }?;

    run_cmd! {
        info "Running cargo unit tests ...";
        cargo test;
    }?;

    let rand_log = "test_art_random.log";
    run_cmd! {
        info "Running art tests (random) with log $rand_log ...";
        ./zig-out/bin/test_art --tests random --size 400000 --ops 400000 -d 20 &> $rand_log;
    }
    .map_err(|e| {
        run_cmd!(tail $rand_log).unwrap();
        e
    })?;

    let fat_log = "test_art_fat.log";
    run_cmd! {
        info "Running art tests (fat) with log $fat_log ...";
        ./zig-out/bin/test_art --tests fat --ops 1000000 &> $fat_log;
    }
    .map_err(|e| {
        run_cmd!(tail $fat_log).unwrap();
        e
    })?;

    let async_art_log = "test_async_art.log";
    run_cmd! {
        info "Running async art tests with log $async_art_log ...";
        ./zig-out/bin/test_async_art --fresh -p 20 &> $async_art_log;
        ./zig-out/bin/test_async_art -p 20 &>> $async_art_log;
        ./zig-out/bin/test_async_art -p 20 &>> $async_art_log;
    }
    .map_err(|e| {
        run_cmd!(tail $async_art_log).unwrap();
        e
    })?;

    info!("Precheckin is OK");
    Ok(())
}
