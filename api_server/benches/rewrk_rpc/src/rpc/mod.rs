use std::collections::{HashMap, VecDeque};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};

// use anyhow::{anyhow, Result};
use fake::{Fake, StringFaker};
use futures::future::join_all;
use futures_util::stream::FuturesUnordered;
use nss_rpc_client::rpc_client::RpcClient;
use rand::rngs::StdRng;
use rand::SeedableRng;
use tokio::task::JoinHandle;
use tokio::time::{timeout_at, Instant};

use self::usage::Usage;
use crate::results::WorkerResult;

mod usage;
mod user_input;

pub type Handle = JoinHandle<anyhow::Result<WorkerResult>>;

enum GenKeys {
    Seed(u64),
    FromInputFile(VecDeque<String>),
}

fn read_keys(filename: &str, num_tasks: usize) -> Vec<VecDeque<String>> {
    let file = File::open(filename).unwrap();
    let mut res = vec![VecDeque::new(); num_tasks];
    let mut i = 0;
    for line in BufReader::new(file).lines() {
        if let Ok(line) = line {
            res[i].push_back(line);
            i = (i + 1) % num_tasks;
        }
    }
    res
}

pub async fn start_tasks(
    time_for: Duration,
    connections: usize,
    uri_string: String,
    io_depth: usize,
    input: String,
    workload: String,
) -> anyhow::Result<FuturesUnordered<Handle>> {
    let deadline = Instant::now() + time_for;

    let handles = FuturesUnordered::new();

    // Generate fake keys
    let mut gen_keys = if !input.is_empty() {
        println!("Fetching keys from {input} for {connections} connections, io_depth={io_depth}");
        read_keys(&input, connections)
            .into_iter()
            .map(GenKeys::FromInputFile)
            .collect::<Vec<_>>()
    } else {
        let seed_ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        println!(
            "Generating keys for {connections} connections, io_depth={io_depth}, with seeds: [{}, {}]",
            seed_ts,
            seed_ts + connections as u64 - 1
        );
        (seed_ts..seed_ts + connections as u64)
            .map(GenKeys::Seed)
            .collect::<Vec<_>>()
    };

    for _i in 0..connections {
        let keys = gen_keys.pop().unwrap();
        let connector = RewrkConnector::new(deadline, uri_string.clone());
        let rpc_client = connector.connect().await.unwrap();
        let workload = workload.clone();
        let handle = match workload.as_str() {
            "read" => tokio::spawn(benchmark_read(deadline, rpc_client, keys, io_depth)),
            "write" => tokio::spawn(benchmark_write(deadline, rpc_client, keys, io_depth)),
            _ => unimplemented!(),
        };

        handles.push(handle);
    }

    Ok(handles)
}

// Futures must not be awaited without timeout.
async fn benchmark_read(
    deadline: Instant,
    rpc_client: RpcClient,
    mut keys: GenKeys,
    io_depth: usize,
) -> anyhow::Result<WorkerResult> {
    let benchmark_start = Instant::now();
    let mut request_times = Vec::new();
    let mut error_map = HashMap::new();

    let mut rng = None;
    if let GenKeys::Seed(seed) = keys {
        rng = Some(StdRng::seed_from_u64(seed));
    }
    // From https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html
    const ASCII: &str = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ!-_.*'()";
    let faker = StringFaker::with(Vec::from(ASCII), 4..30);

    // Benchmark loop.
    // Futures must not be awaited without timeout.
    loop {
        // ResponseFuture of send_request might return channel closed error instead of real error
        // in the case of connection_task being finished. This future will check if connection_task
        // is finished first.

        let mut futures = Vec::new();
        for _ in 0..io_depth {
            // Create request from **parsed** data.
            let mut key: String = match &mut keys {
                GenKeys::Seed(_) => {
                    format!(
                        "/{}",
                        faker.fake_with_rng::<String, _>(rng.as_mut().unwrap())
                    )
                }
                GenKeys::FromInputFile(keys) => match keys.pop_front() {
                    Some(key) => key,
                    None => break,
                },
            };
            key.push('\0');

            let future = async { nss_rpc_client::nss::nss_get_inode(&rpc_client, key).await };
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
async fn benchmark_write(
    deadline: Instant,
    rpc_client: RpcClient,
    mut keys: GenKeys,
    io_depth: usize,
) -> anyhow::Result<WorkerResult> {
    let benchmark_start = Instant::now();
    let mut request_times = Vec::new();
    let mut error_map = HashMap::new();

    let mut rng = None;
    if let GenKeys::Seed(seed) = keys {
        rng = Some(StdRng::seed_from_u64(seed));
    }
    // From https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html
    const ASCII: &str = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ!-_.*'()";
    let faker = StringFaker::with(Vec::from(ASCII), 4..30);

    // Benchmark loop.
    // Futures must not be awaited without timeout.
    loop {
        // ResponseFuture of send_request might return channel closed error instead of real error
        // in the case of connection_task being finished. This future will check if connection_task
        // is finished first.

        let mut futures = Vec::new();
        for _ in 0..io_depth {
            // Create request from **parsed** data.
            let mut key: String = match &mut keys {
                GenKeys::Seed(_) => {
                    format!(
                        "/{}",
                        faker.fake_with_rng::<String, _>(rng.as_mut().unwrap())
                    )
                }
                GenKeys::FromInputFile(keys) => match keys.pop_front() {
                    Some(key) => key,
                    None => break,
                },
            };
            key.push('\0');

            let value = key.clone();
            let future =
                async { nss_rpc_client::nss::nss_put_inode(&rpc_client, key, value).await };
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

    async fn connect(&self) -> anyhow::Result<RpcClient> {
        Ok(RpcClient::new(&self.host).await.unwrap())
    }

    #[allow(dead_code)]
    fn get_received_bytes(&self) -> usize {
        self.usage.get_received_bytes()
    }
}
