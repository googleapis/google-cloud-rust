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

// [START pubsub_use_emulator]
use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
use google_cloud_pubsub::client::{Publisher, TopicAdmin};
use google_cloud_pubsub::model::Message;

pub async fn sample(project_id: &str, topic_id: &str) -> anyhow::Result<()> {
    let emulator_host = std::env::var("PUBSUB_EMULATOR_HOST")?;
    let endpoint = format!("http://{emulator_host}");

    let topic_admin = TopicAdmin::builder()
        .with_endpoint(&endpoint)
        .with_credentials(Anonymous::new().build())
        .build()
        .await?;
    let topic = topic_admin
        .create_topic()
        .set_name(format!("projects/{project_id}/topics/{topic_id}"))
        .send()
        .await?;
    println!("created topic: {}", topic.name);

    let publisher = Publisher::builder(topic.name)
        .with_endpoint(&endpoint)
        .with_credentials(Anonymous::new().build())
        .build()
        .await?;
    let message_id = publisher
        .publish(Message::new().set_data("Hello, Emulator!"))
        .await?;
    println!("published message with ID: {message_id}");
    Ok(())
}
// [END pubsub_use_emulator]
