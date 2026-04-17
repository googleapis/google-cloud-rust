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

// [START pubsub_subscriber_exactly_once]
use google_cloud_pubsub::client::Subscriber;
use google_cloud_pubsub::subscriber::handler::Handler;
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

    let mut tasks = Vec::new();
    println!("Listening for messages...");
    while let Some((m, Handler::ExactlyOnce(h))) = stream.next().await.transpose()? {
        println!("Received message: {m:?}");
        tasks.push(tokio::spawn(async move {
            // We spawn a background task so that receiving the next message
            // is not blocked on the server confirming the ack for this
            // message.
            //
            // You might choose to use a work queue in your application.
            match h.confirmed_ack().await {
                Ok(()) => println!(
                    "Confirmed ack for message={m:?}. The message will not be redelivered."
                ),
                Err(e) => println!("Failed to confirm ack for message={m:?} with error={e:?}"),
            }
        }));
    }
    shutdown.await?;
    println!("Done listening for messages");

    for t in tasks {
        t.await?;
    }
    println!("Done acking messages");

    Ok(())
}
// [END pubsub_subscriber_exactly_once]
