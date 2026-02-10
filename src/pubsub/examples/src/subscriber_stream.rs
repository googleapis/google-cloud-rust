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

// [START rust_pubsub_subscriber_stream]
use futures::StreamExt as _;
use futures::TryStreamExt as _;
use google_cloud_pubsub::client::Subscriber;
use std::time::Duration;

pub async fn sample(project_id: &str, subscription_id: &str) -> anyhow::Result<()> {
    let subscription_name = format!("projects/{project_id}/subscriptions/{subscription_id}");
    let client = Subscriber::builder().build().await?;

    // In simple scenarios, like this example, a `Session` can be converted into
    // a `futures::Stream`.
    let session = client.streaming_pull(subscription_name).start();

    println!("listening for messages using streams...");

    // Terminate the example after 10 seconds. Applications typically process
    // messages indefinitely in a long-running loop.
    let deadline = tokio::time::sleep(Duration::from_secs(10));
    session
        .into_stream()
        .take_until(deadline)
        .try_for_each(|(message, handler)| {
            println!(
                "received message: {}",
                String::from_utf8_lossy(&message.data)
            );
            // Acknowledgments are required for At-Least-Once delivery.
            handler.ack();
            async { Ok(()) }
        })
        .await?;

    println!("done listening for messages");

    Ok(())
}
// [END rust_pubsub_subscriber_stream]
