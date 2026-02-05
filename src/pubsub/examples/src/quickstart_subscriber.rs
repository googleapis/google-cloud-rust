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

    // Terminate the example after 10 seconds. Applications typically process
    // messages indefinitely in a long-running loop.
    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(10);

    while let Ok(Some(item)) = tokio::time::timeout_at(deadline, session.next()).await {
        let (message, handler) = item?;
        println!("received message: {message:?}");
        handler.ack();
    }

    println!("done listening for messages");
    Ok(())
}
// [END pubsub_quickstart_subscriber]
