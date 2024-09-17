use std::collections::HashMap;
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};

// use anyhow::{anyhow, Result};
use fake::{Fake, StringFaker};
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

pub async fn start_tasks(
    time_for: Duration,
    connections: usize,
    uri_string: String,
    _predicted_size: usize,
) -> anyhow::Result<FuturesUnordered<Handle>> {
    let deadline = Instant::now() + time_for;
    let user_input = uri_string;

    let handles = FuturesUnordered::new();

    // Generate fake keys
    let seed_ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    println!(
        "Generating keys for {} connections, with seeds: [{}, {}]",
        connections,
        seed_ts,
        seed_ts + connections as u64 - 1
    );

    for i in 0..connections {
        let handle = tokio::spawn(benchmark(deadline, user_input.clone(), seed_ts + i as u64));

        handles.push(handle);
    }

    Ok(handles)
}

// Futures must not be awaited without timeout.
async fn benchmark(
    deadline: Instant,
    user_input: String,
    seed: u64,
) -> anyhow::Result<WorkerResult> {
    let benchmark_start = Instant::now();
    let connector = RewrkConnector::new(deadline, user_input);
    let mut rpc_client = connector.connect().await.unwrap();

    let mut request_times = Vec::new();
    let mut error_map = HashMap::new();

    let ref mut rng = StdRng::seed_from_u64(seed);
    // From https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html
    const ASCII: &str = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ!-_.*'()";
    let faker = StringFaker::with(Vec::from(ASCII), 4..30);

    // Benchmark loop.
    // Futures must not be awaited without timeout.
    loop {
        // Create request from **parsed** data.
        let key: String = faker.fake_with_rng(rng);

        // ResponseFuture of send_request might return channel closed error instead of real error
        // in the case of connection_task being finished. This future will check if connection_task
        // is finished first.
        let future =
            async { nss_rpc_client::nss_put_inode(&mut rpc_client, key.clone(), key).await };

        let request_start = Instant::now();

        // Try to resolve future before benchmark deadline is elapsed.
        if let Ok(result) = timeout_at(deadline, future).await {
            if let Err(e) = result {
                let error = e.to_string();

                // Insert/add error string to error log.
                match error_map.get_mut(&error) {
                    Some(count) => *count += 1,
                    None => {
                        error_map.insert(error, 1);
                    }
                }
            }
        } else {
            // Benchmark deadline is elapsed. Break the loop.
            break;
        }

        request_times.push(request_start.elapsed());
    }

    Ok(WorkerResult {
        total_times: vec![benchmark_start.elapsed()],
        request_times,
        buffer_sizes: vec![connector.get_received_bytes()],
        error_map,
    })
}

struct RewrkConnector {
    #[allow(unused)]
    deadline: Instant,
    host: String,
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

    fn get_received_bytes(&self) -> usize {
        self.usage.get_received_bytes()
    }
}
