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

// [START pubsub_subscriber_stream]
use futures::StreamExt as _;
use google_cloud_pubsub::client::Subscriber;
use std::time::Duration;

pub async fn sample(project_id: &str, subscription_id: &str) -> anyhow::Result<()> {
    let subscription_name = format!("projects/{project_id}/subscriptions/{subscription_id}");

    // Create a new subscriber client.
    let client = Subscriber::builder().build().await?;

    // Create a streaming pull session.
    let session = client.streaming_pull(subscription_name).start();

    println!("listening for messages using streams...");

    // Collect messages for 10 seconds.
    let deadline = tokio::time::sleep(Duration::from_secs(10));
    let stream = session.into_stream().take_until(deadline);
    tokio::pin!(stream);

    while let Some((message, handler)) = stream.next().await.transpose()? {
        println!(
            "received message: {:?}",
            String::from_utf8_lossy(&message.data)
        );
        handler.ack();
    }

    println!("done listening for messages");

    Ok(())
}
// [END pubsub_subscriber_stream]
