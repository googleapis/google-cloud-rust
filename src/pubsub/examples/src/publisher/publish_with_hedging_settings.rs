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

// [START pubsub_publish_hedging_settings]
use google_cloud_pubsub::client::Publisher;
use google_cloud_pubsub::model::Message;
use google_cloud_pubsub::publisher::HedgingOptions;
use std::time::Duration;

pub async fn sample(project_id: &str, topic_id: &str) -> anyhow::Result<()> {
    let topic_name = format!("projects/{project_id}/topics/{topic_id}");

    // Configure custom hedging options.
    // Hedging sends duplicate requests for batches exceeding the delay threshold,
    // constrained by a token bucket.
    let hedging_options = HedgingOptions::new()
        .set_delay(Duration::from_millis(500)) // default 1s
        .set_max_tokens(100_u32) // default 50
        .set_refill_ratio(0.05_f32); // default 0.1

    let publisher = Publisher::builder(topic_name)
        .set_hedging_options(hedging_options)
        .build()
        .await?;

    let message = Message::new().set_data("Hello, World!");
    let message_id = publisher.publish(message).await?;

    println!("published message with ID: {message_id}");
    Ok(())
}
// [END pubsub_publish_hedging_settings]
