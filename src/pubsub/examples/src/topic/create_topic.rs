// Copyright 2025 Google LLC
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

// [START pubsub_create_topic]
use google_cloud_pubsub::{client::TopicAdmin, model::Topic};

pub async fn sample(client: &TopicAdmin, project_id: &str, topic_id: &str) -> anyhow::Result<()> {
    let topic: Topic = client
        .create_topic()
        .set_name(format!("projects/{project_id}/topics/{topic_id}"))
        .send()
        .await?;

    println!("successfully created topic {topic:?}");
    Ok(())
}
// [END pubsub_create_topic]
