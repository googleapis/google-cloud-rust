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

// [START pubsub_quickstart_subscriber]
use google_cloud_pubsub::client::Subscriber;

pub async fn sample(project_id: &str, subscription_id: &str) -> anyhow::Result<()> {
    let subscription_name = format!("projects/{project_id}/subscriptions/{subscription_id}");
    let client = Subscriber::builder().build().await?;
    let mut session = client.streaming_pull(subscription_name).start();

    println!("listening for messages...");

    // For demonstration purposes, this example terminates after 10 seconds.
    // In production, applications typically process messages indefinitely in a long-running loop.
    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(10);

    while let Ok(Some(item)) = tokio::time::timeout_at(deadline, session.next()).await {
        let (message, handler) = item?;

        let data = String::from_utf8_lossy(&message.data);
        println!("received message: {data}");

        // Acknowledge the message so it isn't redelivered.
        handler.ack();
    }

    println!("done listening for messages");
    Ok(())
}
// [END pubsub_quickstart_subscriber]
