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

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Instant;

use google_cloud_pubsub::client::Subscriber;

use crate::args::SubscriberArgs;
use crate::{done, print_result};

/// Thread-safe counters for tracking subscriber performance metrics.
#[derive(Default)]
struct Stats {
    /// Total messages received and processed.
    recv_count: AtomicI64,
    /// Total bytes received and processed.
    recv_bytes: AtomicI64,
    /// Total number of subscription errors encountered.
    error_count: AtomicI64,
}

/// Entry point for the subscriber benchmark.
pub async fn run(config: SubscriberArgs) -> Result<(), anyhow::Error> {
    let subscription_name = format!(
        "projects/{}/subscriptions/{}",
        config.common.project, config.subscription_id
    );

    let subscriber = Subscriber::builder()
        .with_grpc_subchannel_count(config.common.grpc_channels)
        .build()
        .await
        .unwrap();

    run_subscriber(Arc::new(config.clone()), subscriber, &subscription_name).await;

    Ok(())
}

/// A background task that processes messages from a single subscriber stream.
///
/// This function pulls messages as fast as possible, increments counters,
/// and immediately acknowledges each message.
async fn subscriber_task(
    subscriber: Subscriber,
    subscription_name: String,
    max_outstanding_messages: usize,
    stats: Arc<Stats>,
) {
    let mut stream = subscriber
        .subscribe(subscription_name)
        .set_max_outstanding_messages(max_outstanding_messages as i64)
        .set_max_outstanding_bytes(1_000_000_000) // 1 GB
        .build();
    while let Some(result) = stream.next().await {
        match result {
            Ok((m, h)) => {
                stats.recv_count.fetch_add(1, Ordering::Relaxed);
                stats.recv_bytes.fetch_add(m.data.len() as i64, Ordering::Relaxed);
                h.ack();
            }
            Err(e) => {
                eprintln!("Error: {}", e);
                stats.error_count.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

/// Orchestrates the subscriber benchmark by spawning multiple processing streams.
async fn run_subscriber(
    config: Arc<SubscriberArgs>,
    subscriber: Subscriber,
    subscription_name: &str,
) {
    let stats = Arc::new(Stats::default());
    let mut tasks = Vec::new();
    let max_outstanding_per_task = config.common.max_outstanding_messages / config.streams;
    if max_outstanding_per_task > 0 {
        for _ in 0..config.streams {
            tasks.push(tokio::spawn(subscriber_task(
                subscriber.clone(),
                subscription_name.to_string(),
                max_outstanding_per_task,
                stats.clone(),
            )));
        }
    }

    let start = Instant::now();
    for i in 0.. {
        if done(config.common.duration, start) {
            break;
        }
        let timer = Instant::now();
        let start_recv_count = stats.recv_count.load(Ordering::Relaxed);
        let start_recv_bytes = stats.recv_bytes.load(Ordering::Relaxed);
        let start_error_count = stats.error_count.load(Ordering::Relaxed);

        tokio::time::sleep(config.common.report_interval).await;

        // Calculate deltas since the last report interval.
        let recv_count_last = stats.recv_count.load(Ordering::Relaxed) - start_recv_count;
        let recv_bytes_last = stats.recv_bytes.load(Ordering::Relaxed) - start_recv_bytes;
        let error_count_last = stats.error_count.load(Ordering::Relaxed) - start_error_count;
        let usage = timer.elapsed();

        print_result(
            "Recv",
            i,
            recv_count_last,
            recv_bytes_last,
            error_count_last,
            usage,
        );
    }

    for task in tasks {
        task.abort();
    }

    println!(
        "# Subscriber: recv_count={}, error_count={}",
        stats.recv_count.load(Ordering::Relaxed),
        stats.error_count.load(Ordering::Relaxed)
    );
}
