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

// [START pubsub_subscriber_error_listener]
use google_cloud_pubsub::client::Subscriber;
use std::time::Duration;

pub async fn sample(project_id: &str, subscription_id: &str) -> anyhow::Result<()> {
    let subscription_name = format!("projects/{project_id}/subscriptions/{subscription_id}");
    let client = Subscriber::builder().build().await?;
    let mut stream = client.subscribe(subscription_name).build();

    // Terminate the example after 10 seconds.
    let shutdown_token = stream.shutdown_token();
    let shutdown = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(10)).await;
        shutdown_token.shutdown().await;
    });

    println!("listening for messages...");

    while let Some((m, h)) = stream
        .next()
        .await
        .transpose()
        .inspect_err(|e| println!("error receiving messages: {e:?}"))?
    {
        println!("received message: {m:?}");
        h.ack();
    }
    shutdown.await?;

    println!("done listening for messages");
    Ok(())
}
// [END pubsub_subscriber_error_listener]
