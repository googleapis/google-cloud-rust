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

use google_cloud_pubsub::client::Publisher;
use google_cloud_pubsub::model::Message;

use crate::args::PublisherArgs;
use crate::{done, print_result};

/// Thread-safe counters for tracking publisher performance metrics.
#[derive(Default)]
struct Stats {
    /// Total messages sent to the client library.
    send_count: AtomicI64,
    /// Total bytes sent to the client library.
    send_bytes: AtomicI64,
    /// Total messages successfully received by the Pub/Sub service.
    recv_count: AtomicI64,
    /// Total bytes successfully received by the Pub/Sub service.
    recv_bytes: AtomicI64,
    /// Total number of publishing errors encountered.
    error_count: AtomicI64,
}

/// Entry point for the publisher benchmark.
pub async fn run(config: PublisherArgs) -> Result<(), anyhow::Error> {
    let topic_name = format!(
        "projects/{}/topics/{}",
        config.common.project, config.topic_id
    );

    let publisher = Publisher::builder(topic_name)
        .set_byte_threshold(config.batch_bytes)
        .set_message_count_threshold(config.batch_size)
        .set_delay_threshold(config.batch_delay)
        .with_grpc_subchannel_count(config.common.grpc_channels)
        .build()
        .await?;

    run_publisher(Arc::new(config.clone()), publisher).await?;

    Ok(())
}

/// Orchestrates the publishing loop and reporting logic.
///
/// This function spawns a background task to continuously publish messages
/// and uses the main thread to sleep and report metrics at fixed intervals.
async fn run_publisher(config: Arc<PublisherArgs>, publisher: Publisher) -> anyhow::Result<()> {
    let payload_size = config.payload_size;
    let data = bytes::Bytes::from(vec![0u8; payload_size as usize]);
    let semaphore = Arc::new(tokio::sync::Semaphore::new(
        config.common.max_outstanding_messages,
    ));
    let stats = Arc::new(Stats::default());

    // Start a background task to publish messages.
    let publisher_stats = stats.clone();
    let publisher_handle = tokio::task::spawn(async move {
        loop {
            // Respect the max_outstanding_messages limit.
            let permit = match semaphore.clone().acquire_owned().await {
                Ok(p) => p,
                Err(e) => {
                    eprintln!("Error acquiring permit: {}", e);
                    break;
                }
            };
            let p = publisher.publish(Message::new().set_data(data.clone()));
            publisher_stats.send_count.fetch_add(1, Ordering::Relaxed);
            publisher_stats
                .send_bytes
                .fetch_add(payload_size, Ordering::Relaxed);

            let recv_stats = publisher_stats.clone();
            tokio::spawn(async move {
                let _permit = permit;
                match p.await {
                    Ok(_) => {
                        recv_stats.recv_count.fetch_add(1, Ordering::Relaxed);
                        recv_stats
                            .recv_bytes
                            .fetch_add(payload_size, Ordering::Relaxed);
                    }
                    Err(e) => {
                        eprintln!("Error: {}", e);
                        recv_stats.error_count.fetch_add(1, Ordering::Relaxed);
                    }
                }
            });
        }
    });

    let start = Instant::now();
    for i in 0.. {
        if done(config.common.duration, start) {
            break;
        }
        let timer = Instant::now();
        let start_send_count = stats.send_count.load(Ordering::Relaxed);
        let start_send_bytes = stats.send_bytes.load(Ordering::Relaxed);
        let start_recv_count = stats.recv_count.load(Ordering::Relaxed);
        let start_recv_bytes = stats.recv_bytes.load(Ordering::Relaxed);
        let start_error_count = stats.error_count.load(Ordering::Relaxed);

        tokio::time::sleep(config.common.report_interval).await;

        // Calculate deltas since the last report interval.
        let send_count_last = stats.send_count.load(Ordering::Relaxed) - start_send_count;
        let send_bytes_last = stats.send_bytes.load(Ordering::Relaxed) - start_send_bytes;
        let recv_count_last = stats.recv_count.load(Ordering::Relaxed) - start_recv_count;
        let recv_bytes_last = stats.recv_bytes.load(Ordering::Relaxed) - start_recv_bytes;
        let error_count_last = stats.error_count.load(Ordering::Relaxed) - start_error_count;
        let usage = timer.elapsed();

        print_result("Pub", i, send_count_last, send_bytes_last, 0, usage);
        print_result(
            "Recv",
            i,
            recv_count_last,
            recv_bytes_last,
            error_count_last,
            usage,
        );
    }

    publisher_handle.abort();

    println!(
        "# Publisher: error_count={}, received_count={}, send_count={}",
        stats.error_count.load(Ordering::Relaxed),
        stats.recv_count.load(Ordering::Relaxed),
        stats.send_count.load(Ordering::Relaxed)
    );

    Ok(())
}
