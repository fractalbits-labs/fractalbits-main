use super::build::*;
use super::cmd_service::*;
use super::{BenchService, BenchWorkload, ServiceAction, ServiceName};
use cmd_lib::*;

pub fn prepare_bench(with_flame_graph: bool) -> CmdResult {
    if with_flame_graph && run_cmd!(bash -c "type addr2line" | grep -q .cargo).is_err() {
        // From https://github.com/iced-rs/iced/issues/2394
        run_cmd! {
            info "Try to install addr2line to make perf script work with rust binary ...";
            cargo install addr2line --features="bin";
        }?;
    }
    Ok(())
}

pub fn run_cmd_bench(
    service: BenchService,
    workload: BenchWorkload,
    with_flame_graph: bool,
    service_name: &mut ServiceName,
) -> CmdResult {
    let http_method = match workload {
        BenchWorkload::Write => "put",
        BenchWorkload::Read => "get",
    };
    let build_mode = BuildMode::Release;
    let uri;
    let bench_exe;
    let workload = workload.as_ref();
    let mut bench_opts = Vec::new();
    let mut keys_limit = 10_000_000.to_string();

    build_bss_nss_server(build_mode)?;
    match service {
        BenchService::ApiServer => {
            *service_name = ServiceName::All;
            build_api_server(build_mode)?;
            build_rewrk()?;
            run_cmd_service(build_mode, ServiceAction::Restart, *service_name)?;
            uri = "http://mybucket.localhost:3000";
            bench_exe = "./target/release/rewrk";
            keys_limit = 1_500_000.to_string(); // api server is slower
            bench_opts.extend_from_slice(&[
                "-t",
                "24",
                "-c",
                "1152",
                "-m",
                http_method,
                "-k",
                &keys_limit,
            ]);
        }
        BenchService::NssRpc => {
            *service_name = ServiceName::Nss;
            build_rewrk_rpc()?;
            start_nss_service(build_mode)?;
            uri = "127.0.0.1:9224";
            bench_exe = "./target/release/rewrk_rpc";
            bench_opts.extend_from_slice(&[
                "-t",
                "24",
                "-c",
                "1152",
                "-w",
                workload,
                "-k",
                &keys_limit,
            ]);
        }
        BenchService::BssRpc => {
            *service_name = ServiceName::Bss;
            build_rewrk_rpc()?;
            start_bss_service(build_mode)?;
            uri = "127.0.0.1:9225";
            bench_exe = "./target/release/rewrk_rpc";
            bench_opts.extend_from_slice(&[
                "-t",
                "24",
                "-c",
                "1152",
                "-w",
                workload,
                "-p",
                "bss",
                "-k",
                &keys_limit,
            ]);
        }
    }

    let duration_secs = 30;
    let perf_handle = if with_flame_graph {
        run_cmd! {
            info "Start perf in the background ...";
            sudo bash -c "echo 0 > /proc/sys/kernel/kptr_restrict";
            sudo bash -c "echo -1 > /proc/sys/kernel/perf_event_paranoid";
        }?;

        Some(spawn!(perf record -F 99 --call-graph dwarf -a -g -- sleep $duration_secs)?)
    } else {
        None
    };

    run_cmd! {
        info "Starting benchmark ...";
        $bench_exe $[bench_opts] -d ${duration_secs}s -h $uri --pct;
    }?;

    if let Some(mut handle) = perf_handle {
        handle.wait()?;
        let flamegraph_path = run_fun!(brew --prefix flamegraph)?;
        run_cmd! {
            info "Post-processing perf data ...";
            perf script > out.perf;
            ${flamegraph_path}/bin/stackcollapse-perf.pl out.perf > out.folded;
            ${flamegraph_path}/bin/flamegraph.pl out.folded > out_perf.svg;
            info "Flamegraph \"out_perf.svg\" is generated";
        }?;
    }

    // stop service after benchmark to save cpu power
    run_cmd_service(BuildMode::Release, ServiceAction::Stop, *service_name)?;

    Ok(())
}
