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

// [START pubsub_publisher_batch_settings]
use google_cloud_pubsub::client::Publisher;
use google_cloud_pubsub::model::Message;
use std::time::Duration;

pub async fn sample(project_id: &str, topic_id: &str) -> anyhow::Result<()> {
    let topic_name = format!("projects/{project_id}/topics/{topic_id}");

    // The publisher will send a batch when:
    // - 10 messages are buffered, OR
    // - total size reaches 1 KiB, OR
    // - 100 milliseconds have passed.
    let publisher = Publisher::builder(topic_name)
        .set_message_count_threshold(10)
        .set_byte_threshold(1024) // 1 KiB
        .set_delay_threshold(Duration::from_millis(100))
        .build()
        .await?;

    let mut futures = vec![];
    for i in 0..10 {
        futures.push(publisher.publish(Message::new().set_data(format!("Message {i}"))));
    }

    for (i, future) in futures.into_iter().enumerate() {
        match future.await {
            Ok(message_id) => println!("Published message {i}; message ID: {message_id}"),
            Err(error) => println!("Failed to publish message {i}: {error}"),
        }
    }

    Ok(())
}
// [END pubsub_publisher_batch_settings]
