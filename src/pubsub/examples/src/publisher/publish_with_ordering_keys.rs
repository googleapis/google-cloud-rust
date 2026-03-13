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

// [START pubsub_publish_with_ordering_keys]
use google_cloud_pubsub::client::Publisher;
use google_cloud_pubsub::model::Message;

pub async fn sample(project: &str, topic_id: &str) -> anyhow::Result<()> {
    let publisher = Publisher::builder(format!("projects/{project}/topics/{topic_id}"))
        // Sending messages to the same region ensures they are received in order
        // even when multiple publishers are used.
        .with_endpoint("us-east1-pubsub.googleapis.com:443")
        .build()
        .await?;

    let messages = vec![
        ("message1", "key1"),
        ("message2", "key2"),
        ("message3", "key1"),
        ("message4", "key2"),
    ];

    let mut futures = vec![];
    for (data, ordering_key) in messages {
        let message = Message::new()
            .set_data(data.as_bytes())
            .set_ordering_key(ordering_key);
        futures.push(publisher.publish(message));
    }

    for future in futures {
        match future.await {
            Ok(message_id) => println!("published message with ID: {message_id}"),
            Err(e) => println!("error publishing message: {e}"),
        }
    }

    Ok(())
}
// [END pubsub_publish_with_ordering_keys]
