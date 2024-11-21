use std::collections::{HashMap, VecDeque};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::time::Duration;

// use anyhow::{anyhow, Result};
use bytes::Bytes;
use futures::future::join_all;
use futures_util::stream::FuturesUnordered;
use rpc_client_bss::RpcClientBss;
use rpc_client_nss::RpcClientNss;
use tokio::task::JoinHandle;
use tokio::time::{timeout_at, Instant};
use uuid::Uuid;

use self::usage::Usage;
use crate::results::WorkerResult;

mod usage;
mod user_input;

pub type Handle = JoinHandle<anyhow::Result<WorkerResult>>;

fn read_keys(filename: &str, num_tasks: usize, keys_limit: usize) -> Vec<VecDeque<String>> {
    let file = File::open(filename).unwrap();
    let mut res = vec![VecDeque::new(); num_tasks];
    let mut i = 0;
    let mut total = 0;
    for line in BufReader::new(file).lines() {
        if let Ok(line) = line {
            res[i].push_back(line);
            i = (i + 1) % num_tasks;
            total += 1;
        }
        if total >= keys_limit {
            break;
        }
    }
    res
}

pub async fn start_tasks_for_nss(
    time_for: Duration,
    keys_limit: usize,
    connections: usize,
    uri_string: String,
    io_depth: usize,
    input: String,
    workload: String,
) -> anyhow::Result<FuturesUnordered<Handle>> {
    let deadline = Instant::now() + time_for;

    let handles = FuturesUnordered::new();

    println!("Fetching keys from {input} for {connections} connections, io_depth={io_depth}");
    let mut gen_keys = read_keys(&input, connections, keys_limit)
        .into_iter()
        .collect::<Vec<_>>();

    for _i in 0..connections {
        let keys = gen_keys.pop().unwrap();
        let connector = RewrkConnector::new(deadline, uri_string.clone());
        let rpc_client = connector.connect_nss().await.unwrap();
        let workload = workload.clone();
        let handle = match workload.as_str() {
            "read" => tokio::spawn(benchmark_nss_read(deadline, rpc_client, keys, io_depth)),
            "write" => tokio::spawn(benchmark_nss_write(deadline, rpc_client, keys, io_depth)),
            _ => unimplemented!(),
        };

        handles.push(handle);
    }

    Ok(handles)
}

pub async fn start_tasks_for_bss(
    time_for: Duration,
    keys_limit: usize,
    connections: usize,
    uri_string: String,
    io_depth: usize,
    input: String,
    workload: String,
) -> anyhow::Result<FuturesUnordered<Handle>> {
    let deadline = Instant::now() + time_for;

    let handles = FuturesUnordered::new();

    println!("Fetching uuids from {input} for {connections} connections, io_depth={io_depth}");
    let mut gen_uuids = read_keys(&input, connections, keys_limit)
        .into_iter()
        .collect::<Vec<_>>();

    for _i in 0..connections {
        let uuids = gen_uuids.pop().unwrap();
        let connector = RewrkConnector::new(deadline, uri_string.clone());
        let rpc_client = connector.connect_bss().await.unwrap();
        let workload = workload.clone();
        let handle = match workload.as_str() {
            "read" => tokio::spawn(benchmark_bss_read(deadline, rpc_client, uuids, io_depth)),
            "write" => tokio::spawn(benchmark_bss_write(deadline, rpc_client, uuids, io_depth)),
            _ => unimplemented!(),
        };

        handles.push(handle);
    }

    Ok(handles)
}

// Futures must not be awaited without timeout.
async fn benchmark_nss_read(
    deadline: Instant,
    rpc_client: RpcClientNss,
    mut keys: VecDeque<String>,
    io_depth: usize,
) -> anyhow::Result<WorkerResult> {
    let benchmark_start = Instant::now();
    let mut request_times = Vec::new();
    let mut error_map = HashMap::new();

    // Benchmark loop.
    // Futures must not be awaited without timeout.
    loop {
        // ResponseFuture of send_request might return channel closed error instead of real error
        // in the case of connection_task being finished. This future will check if connection_task
        // is finished first.

        let mut futures = Vec::new();
        for _ in 0..io_depth {
            let mut key: String = match keys.pop_front() {
                Some(key) => key,
                None => break,
            };
            key.push('\0');

            let future = async { rpc_client.get_inode(key).await };
            futures.push(future);
        }
        if futures.is_empty() {
            break;
        }
        let request_start = Instant::now();

        // Try to resolve future before benchmark deadline is elapsed.
        if let Ok(results) = timeout_at(deadline, join_all(futures)).await {
            for result in results.iter() {
                if let Err(e) = result {
                    let error = e.to_string();

                    // Insert/add error string to error log.
                    match error_map.get_mut(&error) {
                        Some(count) => *count += 1,
                        None => {
                            error_map.insert(error, 1);
                        }
                    }
                } else {
                    request_times.push(request_start.elapsed());
                }
            }
        } else {
            // Benchmark deadline is elapsed. Break the loop.
            break;
        }
    }

    Ok(WorkerResult {
        total_times: vec![benchmark_start.elapsed()],
        request_times,
        error_map,
    })
}

// Futures must not be awaited without timeout.
async fn benchmark_bss_write(
    deadline: Instant,
    rpc_client: RpcClientBss,
    mut uuids: VecDeque<String>,
    io_depth: usize,
) -> anyhow::Result<WorkerResult> {
    let benchmark_start = Instant::now();
    let mut request_times = Vec::new();
    let mut error_map = HashMap::new();

    // Benchmark loop.
    // Futures must not be awaited without timeout.
    loop {
        // ResponseFuture of send_request might return channel closed error instead of real error
        // in the case of connection_task being finished. This future will check if connection_task
        // is finished first.

        let mut futures = Vec::new();
        for _ in 0..io_depth {
            let content = Bytes::from(vec![0; 4096 - 256]);
            let uuid = match uuids.pop_front() {
                Some(uuid) => uuid,
                None => break,
            };
            let future = async {
                let uuid = uuid;
                let blob_id = Uuid::parse_str(&uuid).unwrap();
                rpc_client.put_blob(blob_id, content).await
            };
            futures.push(future);
        }
        if futures.is_empty() {
            break;
        }
        let request_start = Instant::now();

        // Try to resolve future before benchmark deadline is elapsed.
        if let Ok(results) = timeout_at(deadline, join_all(futures)).await {
            for result in results.iter() {
                if let Err(e) = result {
                    let error = e.to_string();

                    // Insert/add error string to error log.
                    match error_map.get_mut(&error) {
                        Some(count) => *count += 1,
                        None => {
                            error_map.insert(error, 1);
                        }
                    }
                } else {
                    request_times.push(request_start.elapsed());
                }
            }
        } else {
            // Benchmark deadline is elapsed. Break the loop.
            break;
        }
    }

    Ok(WorkerResult {
        total_times: vec![benchmark_start.elapsed()],
        request_times,
        error_map,
    })
}

// Futures must not be awaited without timeout.
async fn benchmark_bss_read(
    deadline: Instant,
    rpc_client: RpcClientBss,
    mut uuids: VecDeque<String>,
    io_depth: usize,
) -> anyhow::Result<WorkerResult> {
    let benchmark_start = Instant::now();
    let mut request_times = Vec::new();
    let mut error_map = HashMap::new();

    // Benchmark loop.
    // Futures must not be awaited without timeout.
    loop {
        // ResponseFuture of send_request might return channel closed error instead of real error
        // in the case of connection_task being finished. This future will check if connection_task
        // is finished first.

        let mut futures = Vec::new();
        for _ in 0..io_depth {
            let uuid = match uuids.pop_front() {
                Some(uuid) => uuid,
                None => break,
            };
            let future = async {
                let uuid = uuid;
                let blob_id = Uuid::parse_str(&uuid).unwrap();
                let mut content = Bytes::new();
                rpc_client.get_blob(blob_id, &mut content).await
            };
            futures.push(future);
        }
        if futures.is_empty() {
            break;
        }
        let request_start = Instant::now();

        // Try to resolve future before benchmark deadline is elapsed.
        if let Ok(results) = timeout_at(deadline, join_all(futures)).await {
            for result in results.iter() {
                if let Err(e) = result {
                    let error = e.to_string();

                    // Insert/add error string to error log.
                    match error_map.get_mut(&error) {
                        Some(count) => *count += 1,
                        None => {
                            error_map.insert(error, 1);
                        }
                    }
                } else {
                    request_times.push(request_start.elapsed());
                }
            }
        } else {
            // Benchmark deadline is elapsed. Break the loop.
            break;
        }
    }

    Ok(WorkerResult {
        total_times: vec![benchmark_start.elapsed()],
        request_times,
        error_map,
    })
}
async fn benchmark_nss_write(
    deadline: Instant,
    rpc_client: RpcClientNss,
    mut keys: VecDeque<String>,
    io_depth: usize,
) -> anyhow::Result<WorkerResult> {
    let benchmark_start = Instant::now();
    let mut request_times = Vec::new();
    let mut error_map = HashMap::new();

    // Benchmark loop.
    // Futures must not be awaited without timeout.
    loop {
        // ResponseFuture of send_request might return channel closed error instead of real error
        // in the case of connection_task being finished. This future will check if connection_task
        // is finished first.

        let mut futures = Vec::new();
        for _ in 0..io_depth {
            let mut key: String = match keys.pop_front() {
                Some(key) => key,
                None => break,
            };
            key.push('\0');

            let value = Bytes::from(key.clone());
            let future = async { rpc_client.put_inode(key, value).await };
            futures.push(future);
        }
        if futures.is_empty() {
            break;
        }
        let request_start = Instant::now();

        // Try to resolve future before benchmark deadline is elapsed.
        if let Ok(results) = timeout_at(deadline, join_all(futures)).await {
            for result in results.iter() {
                if let Err(e) = result {
                    let error = e.to_string();

                    // Insert/add error string to error log.
                    match error_map.get_mut(&error) {
                        Some(count) => *count += 1,
                        None => {
                            error_map.insert(error, 1);
                        }
                    }
                } else {
                    request_times.push(request_start.elapsed());
                }
            }
        } else {
            // Benchmark deadline is elapsed. Break the loop.
            break;
        }
    }

    Ok(WorkerResult {
        total_times: vec![benchmark_start.elapsed()],
        request_times,
        error_map,
    })
}

struct RewrkConnector {
    #[allow(unused)]
    deadline: Instant,
    host: String,
    #[allow(unused)]
    usage: Usage,
}

impl RewrkConnector {
    fn new(deadline: Instant, host: String) -> Self {
        let usage = Usage::new();

        Self {
            deadline,
            host,
            usage,
        }
    }

    async fn connect_nss(&self) -> anyhow::Result<RpcClientNss> {
        Ok(RpcClientNss::new(&self.host).await.unwrap())
    }

    async fn connect_bss(&self) -> anyhow::Result<RpcClientBss> {
        Ok(RpcClientBss::new(&self.host).await.unwrap())
    }

    #[allow(dead_code)]
    fn get_received_bytes(&self) -> usize {
        self.usage.get_received_bytes()
    }
}
