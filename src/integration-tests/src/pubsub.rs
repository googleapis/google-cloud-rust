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

use pubsub::{client::TopicAdmin, model::Topic};
use rand::{Rng, distr::Alphanumeric};

use crate::Result;

pub async fn basic_topic() -> Result<()> {
    // Enable a basic subscriber. Useful to troubleshoot problems and visually
    // verify tracing is doing something.
    #[cfg(feature = "log-integration-tests")]
    let _guard = {
        use tracing_subscriber::fmt::format::FmtSpan;
        let subscriber = tracing_subscriber::fmt()
            .with_level(true)
            .with_thread_ids(true)
            .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
            .finish();

        tracing::subscriber::set_default(subscriber)
    };

    let project = crate::project_id()?;
    let (client, topic) = create_test_topic().await?;

    tracing::info!("testing list_topics()");
    let topics = client
        .list_topics()
        .set_project(format!("projects/{project}"))
        .send()
        .await?;
    tracing::info!("success with list_topics={topics:?}");
    assert!(topics.topics.iter().any(|x| x.name == topic.name));

    cleanup_test_topic(client, topic.name).await?;

    Ok(())
}

pub const TOPIC_ID_LENGTH: usize = 255;

fn random_topic_id(project: String) -> String {
    let prefix = "topic-";
    let topic_id: String = rand::rng()
        .sample_iter(&Alphanumeric)
        .take(TOPIC_ID_LENGTH - prefix.len())
        .map(char::from)
        .collect();
    format!("projects/{project}/topics/{prefix}{topic_id}")
}

pub async fn create_test_topic() -> Result<(TopicAdmin, Topic)> {
    let project = crate::project_id()?;
    let client = pubsub::client::TopicAdmin::builder()
        .with_tracing()
        .build()
        .await?;
    let topic_id = random_topic_id(project);

    tracing::info!("testing create_topic()");
    let topic = client
        .create_topic()
        .set_name(topic_id)
        .set_labels([("integration-test", "true")])
        .send()
        .await?;
    tracing::info!("success on create_topic: {topic:?}");

    Ok((client, topic))
}

pub async fn cleanup_test_topic(client: TopicAdmin, topic_name: String) -> Result<()> {
    tracing::info!("testing delete_topic()");
    client.delete_topic().set_topic(topic_name).send().await?;
    tracing::info!("success on delete_topic");
    Ok(())
}
