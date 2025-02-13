use cmd_lib::*;

pub fn run_cmd_precheckin() -> CmdResult {
    run_cmd! {
        info "Building ...";
        zig build 2>&1;
    }?;

    run_cmd! {
        info "Running zig unit tests ...";
        mkdir -p data;
        mkdir -p cache;
        mkdir -p ebs;
        ./zig-out/bin/mkfs;
        zig build test --summary all 2>&1;
    }?;

    run_cmd! {
        info "Running cargo unit tests ...";
        cargo test --lib --workspace --exclude rewrk --exclude rewrk_rpc --exclude xtask;
    }?;

    let rand_log = "test_art_random.log";
    run_cmd! {
        info "Running art tests (random) with log $rand_log ...";
        ./zig-out/bin/mkfs;
        ./zig-out/bin/test_art --tests random --size 400000 --ops 1000000 -d 20 &> $rand_log;
    }
    .map_err(|e| {
        run_cmd!(tail $rand_log).unwrap();
        e
    })?;

    let fat_log = "test_art_fat.log";
    run_cmd! {
        info "Running art tests (fat) with log $fat_log ...";
        ./zig-out/bin/mkfs;
        ./zig-out/bin/test_art --tests fat --ops 1000000 &> $fat_log;
    }
    .map_err(|e| {
        run_cmd!(tail $fat_log).unwrap();
        e
    })?;

    let async_art_log = "test_async_art.log";
    run_cmd! {
        info "Running async art tests with log $async_art_log ...";
        ./zig-out/bin/mkfs;
        ./zig-out/bin/fbs --new_tree 947ef2be-44b2-4ac2-969b-2574eb85662b
        ./zig-out/bin/test_async_art -p 20 &> $async_art_log;
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
