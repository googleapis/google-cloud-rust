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

// [START pubsub_resume_publish_with_ordering_keys]
use google_cloud_pubsub::client::Publisher;
use google_cloud_pubsub::model::Message;

pub async fn sample(project: &str, topic_id: &str) -> anyhow::Result<()> {
    let publisher = Publisher::builder(format!("projects/{project}/topics/{topic_id}"))
        // Pub/Sub's ordered delivery guarantee only applies when publishes for
        // an ordering key are in the same region.
        .with_endpoint("https://us-east1-pubsub.googleapis.com")
        .build()
        .await?;

    let ordering_key = "ordering-key";
    let result = publisher.publish(
        Message::new()
            .set_data("message")
            .set_ordering_key(ordering_key),
    );

    match result.await {
        Ok(message_id) => println!("published message with ID: {message_id}"),
        Err(e) => {
            // Fix internal state to make sure publishes with errors are not
            // published out of order. This might mean moving messages to a queue
            // and retrying those messages before publishing subsequent messages.
            println!("error publishing message: {e}");

            // Resume publish on an ordering key that has had unrecoverable errors.
            // After an error, publishes with this ordering key will fail
            // until this method is called.
            publisher.resume_publish(ordering_key);
        }
    }

    Ok(())
}
// [END pubsub_resume_publish_with_ordering_keys]
