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

// [START pubsub_subscriber_concurrency_control]
use google_cloud_pubsub::client::Subscriber;
use google_cloud_pubsub::subscriber::MessageStream;
use std::time::Duration;

pub async fn sample(project_id: &str, subscription_id: &str) -> anyhow::Result<()> {
    // The available concurrency of your machine.
    const NCPU: usize = 2;

    let subscription_name = format!("projects/{project_id}/subscriptions/{subscription_id}");
    let client = Subscriber::builder()
        // Configure the subscriber to use multiple gRPC channels. This lets the
        // client multiplex its open streams and its acknowledgement RPCs.
        //
        // Anecdotally, having one channel per CPU of your machine yields high
        // throughput.
        .with_grpc_subchannel_count(NCPU)
        .build()
        .await?;
    let tasks: Vec<_> = (0..2 * NCPU)
        .map(|i| {
            // Pub/Sub caps the throughput of a single stream to 10 MB/s. To achieve
            // higher throughput, you should open multiple streams.
            //
            // Anecdotally, setting the number of streams to twice the available
            // concurrency yields high throughput.
            let stream = client.subscribe(&subscription_name).build();
            tokio::spawn(subscribe_task(i, stream))
        })
        .collect();

    for t in tasks {
        t.await??;
    }
    println!("done listening for messages");
    Ok(())
}

async fn subscribe_task(index: usize, mut stream: MessageStream) -> anyhow::Result<()> {
    // Terminate the example after 10 seconds.
    let shutdown_token = stream.shutdown_token();
    let shutdown = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(10)).await;
        shutdown_token.shutdown().await;
    });

    println!("listening for messages on stream {index}...");

    while let Some((m, h)) = stream.next().await.transpose()? {
        println!("received message: {m:?}");
        h.ack();
    }
    shutdown.await?;

    Ok(())
}
// [END pubsub_subscriber_concurrency_control]
