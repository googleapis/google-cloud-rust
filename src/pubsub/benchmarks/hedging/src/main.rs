// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
use google_cloud_pubsub::client::Publisher;
use google_cloud_pubsub::model::Message;
use google_cloud_pubsub::publisher::HedgingOptions;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};
use tokio::task::JoinSet;

mod args;
mod mock_server;
mod stats;

const PAYLOAD_SIZE: usize = 1024;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = args::parse_args();

    println!("================================================================================");
    println!("             Starting Google Cloud Pub/Sub Hedging Benchmark                    ");
    println!("================================================================================");
    println!("Parameters:");
    println!("  Warmup:               {:.2?}", args.warmup);
    println!("  Duration:             {:.2?}", args.duration);
    println!("  Message Rate:         {} msg/s", args.message_rate);
    println!(
        "  Payload Size:         {} bytes (unbatched: 1 msg/batch)",
        PAYLOAD_SIZE
    );
    println!("  Enable Hedging:       {}", args.enable_hedging);
    if args.enable_hedging {
        println!("  Hedge Delay:          {:.2?}", args.hedge_delay);
        println!("  Hedge Max Tokens:     {}", args.hedge_max_tokens);
        println!("  Hedge Refill Ratio:   {}", args.hedge_refill_ratio);
    }

    let (endpoint, mock_handle) =
        mock_server::start_mock_server(mock_server::MockServerConfig::default()).await?;
    println!("  Mock Server Endpoint: {}", endpoint);
    println!("================================================================================\n");

    let hedging_options = if args.enable_hedging {
        Some(
            HedgingOptions::new()
                .set_delay(args.hedge_delay)
                .set_max_tokens(args.hedge_max_tokens)
                .set_refill_ratio(args.hedge_refill_ratio),
        )
    } else {
        None
    };

    let topic_name = "projects/test-project/topics/test-topic";
    let builder = Publisher::builder(topic_name)
        .with_endpoint(&endpoint)
        .with_credentials(Anonymous::default().build())
        .set_message_count_threshold(1)
        .set_delay_threshold(Duration::ZERO)
        .set_or_clear_hedging_options(hedging_options);

    let publisher = builder.build().await?;
    let payload = bytes::Bytes::from(vec![b'x'; PAYLOAD_SIZE]);

    let interval_duration = Duration::from_secs_f64(1.0 / args.message_rate as f64);
    let mut interval = tokio::time::interval(interval_duration);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Burst);

    if !args.warmup.is_zero() {
        println!("Starting warmup phase for {:.2?}...", args.warmup);
        let warmup_start = Instant::now();
        let mut warmup_join_set = JoinSet::new();

        while warmup_start.elapsed() < args.warmup {
            interval.tick().await;
            if warmup_start.elapsed() >= args.warmup {
                break;
            }

            let pub_clone = publisher.clone();
            let msg_payload = payload.clone();
            warmup_join_set.spawn(async move {
                let msg = Message::new().set_data(msg_payload);
                let _ = pub_clone.publish(msg).await;
            });
        }

        tokio::time::timeout(Duration::from_secs(10), async {
            while warmup_join_set.join_next().await.is_some() {}
        })
        .await?;

        mock_handle.stats.reset();
        println!("Warmup complete. Starting benchmark...\n");
    }

    let mut interval = tokio::time::interval(interval_duration);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Burst);

    let mut join_set = JoinSet::new();
    let bench_start = Instant::now();
    let mut total_sent = 0u64;

    while bench_start.elapsed() < args.duration {
        interval.tick().await;
        if bench_start.elapsed() >= args.duration {
            break;
        }

        total_sent += 1;
        let pub_clone = publisher.clone();
        let msg_payload = payload.clone();

        join_set.spawn(async move {
            let send_start = Instant::now();
            let msg = Message::new().set_data(msg_payload);
            let res = pub_clone.publish(msg).await;
            let elapsed = send_start.elapsed();
            (res.is_ok(), elapsed)
        });
    }

    println!(
        "Benchmark duration reached. Sent {} messages. Waiting for in-flight requests...",
        total_sent
    );

    let mut latencies = Vec::with_capacity(total_sent as usize);
    let mut total_succeeded = 0u64;
    let mut total_errors = 0u64;

    let drain_timeout = Duration::from_secs(10);
    let drain_deadline = Instant::now() + drain_timeout;

    while !join_set.is_empty() {
        let timeout_dur = drain_deadline.saturating_duration_since(Instant::now());
        if timeout_dur.is_zero() {
            eprintln!("Warning: Timed out waiting for remaining publish tasks to finish.");
            break;
        }
        match tokio::time::timeout(timeout_dur, join_set.join_next()).await {
            Ok(Some(res)) => match res {
                Ok((true, latency)) => {
                    total_succeeded += 1;
                    latencies.push(latency);
                }
                Ok((false, _)) | Err(_) => {
                    total_errors += 1;
                }
            },
            Ok(None) => break,
            Err(_) => {
                eprintln!("Warning: Timed out waiting for remaining publish tasks to finish.");
                break;
            }
        }
    }

    let total_elapsed = bench_start.elapsed();

    let total_rpcs = mock_handle
        .stats
        .total_publish_requests
        .load(Ordering::Relaxed);
    let hedged_rpcs = mock_handle
        .stats
        .hedged_publish_requests
        .load(Ordering::Relaxed);

    let mut benchmark_stats = stats::BenchmarkStats {
        elapsed: total_elapsed,
        total_sent,
        total_succeeded,
        total_errors,
        payload_size: PAYLOAD_SIZE,
        latencies,
        total_rpc_requests: total_rpcs,
        hedged_rpc_requests: hedged_rpcs,
    };

    benchmark_stats.print_summary();

    mock_handle.server_task.abort();

    Ok(())
}
