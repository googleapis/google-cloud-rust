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

// [START pubsub_publish_with_error_handler]
use futures::future::join_all;
use google_cloud_pubsub::client::Publisher;
use google_cloud_pubsub::model::Message;

pub async fn sample(project_id: &str, topic_id: &str) -> anyhow::Result<()> {
    let topic_name = format!("projects/{project_id}/topics/{topic_id}");
    let publisher = Publisher::builder(topic_name).build().await?;

    let results = join_all(
        (0..10).map(|i| publisher.publish(Message::new().set_data(format!("message {i}")))),
    )
    .await;

    for (i, result) in results.into_iter().enumerate() {
        match result {
            Ok(message_id) => println!("published message {i}; message ID: {message_id}"),
            // Handle the error, e.g., log it, retry, or move the message to a queue.
            Err(e) => eprintln!("failed to publish message {i}: {e}"),
        }
    }

    Ok(())
}
// [END pubsub_publish_with_error_handler]
