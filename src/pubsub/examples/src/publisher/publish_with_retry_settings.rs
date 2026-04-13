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

// [START pubsub_publisher_retry_settings]
use google_cloud_gax::exponential_backoff::ExponentialBackoffBuilder;
use google_cloud_gax::retry_policy::{AlwaysRetry, RetryPolicyExt};
use google_cloud_pubsub::client::Publisher;
use google_cloud_pubsub::model::Message;
use std::time::Duration;

pub async fn sample(project_id: &str, topic_id: &str) -> anyhow::Result<()> {
    let topic_name = format!("projects/{project_id}/topics/{topic_id}");

    // Configure custom retry settings.
    // In this example, we retry with a time limit of 10 minutes.
    let retry_policy = AlwaysRetry.with_time_limit(Duration::from_secs(600));

    // Configure custom backoff settings.
    let backoff_policy = ExponentialBackoffBuilder::new()
        .with_initial_delay(Duration::from_millis(200))
        .with_maximum_delay(Duration::from_secs(45))
        .with_scaling(2.0)
        .clamp();

    let publisher = Publisher::builder(topic_name)
        .with_retry_policy(retry_policy)
        .with_backoff_policy(backoff_policy)
        .build()
        .await?;

    let message = Message::new().set_data("Hello, World!");
    let message_id = publisher.publish(message).await?;

    println!("published message with ID: {message_id}");
    Ok(())
}
// [END pubsub_publisher_retry_settings]
