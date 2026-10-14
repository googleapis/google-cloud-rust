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

// [START pubsub_optimistic_subscribe]
use google_cloud_gax::error::rpc::Code;
use google_cloud_pubsub::client::{Subscriber, SubscriptionAdmin};
use std::time::Duration;

pub async fn sample(project_id: &str, topic_id: &str, subscription_id: &str) -> anyhow::Result<()> {
    let subscription_name = format!("projects/{project_id}/subscriptions/{subscription_id}");
    let client = Subscriber::builder().build().await?;

    // Instead of checking if the subscription exists, optimistically try to
    // receive messages from it.
    let Err(e) = receive(&client, &subscription_name).await else {
        return Ok(());
    };
    // The stream returns a NOT_FOUND error if the subscription does not exist.
    if e.status().is_none_or(|s| s.code != Code::NotFound) {
        return Err(e.into());
    }

    // Since the subscription does not exist, create the subscription.
    let admin = SubscriptionAdmin::builder().build().await?;
    admin
        .create_subscription()
        .set_name(&subscription_name)
        .set_topic(format!("projects/{project_id}/topics/{topic_id}"))
        .send()
        .await?;
    println!("created subscription {subscription_name}");

    // Receive messages from the new subscription.
    receive(&client, &subscription_name).await?;
    Ok(())
}

async fn receive(client: &Subscriber, subscription_name: &str) -> google_cloud_pubsub::Result<()> {
    let mut stream = client.subscribe(subscription_name).build();

    // Terminate the example after 10 seconds.
    let shutdown_token = stream.shutdown_token();
    tokio::spawn({
        let shutdown_token = shutdown_token.clone();
        async move {
            tokio::time::sleep(Duration::from_secs(10)).await;
            shutdown_token.shutdown().await;
        }
    });

    println!("listening for messages on {subscription_name}...");
    while let Some((message, handler)) = stream.next().await.transpose()? {
        println!("received message: {message:?}");
        handler.ack();
    }
    // Wait for the shutdown to complete, so pending acks are flushed.
    shutdown_token.shutdown().await;

    println!("done listening for messages");
    Ok(())
}
// [END pubsub_optimistic_subscribe]
