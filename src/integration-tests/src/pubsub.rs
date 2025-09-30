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

use crate::{Error, Result};

use gax::paginator::ItemPaginator as _;
use pubsub::client::{Publisher, TopicAdmin};
use pubsub::model::{PubsubMessage, Topic};
use rand::{Rng, distr::Alphanumeric};

pub async fn basic_publisher() -> Result<()> {
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

    let (topic_admin, topic) = create_test_topic().await?;

    tracing::info!("testing publish()");
    let publisher = Publisher::builder().build().await?;
    let messages: [PubsubMessage; 2] = [
        PubsubMessage::new().set_data("Hello"),
        PubsubMessage::new().set_data("World"),
    ];
    let resp = publisher
        .publish()
        .set_topic(&topic.name)
        .set_messages(messages)
        .send()
        .await?;
    assert_eq!(resp.message_ids.len(), 2);
    tracing::info!("successfully published messages: {:#?}", resp.message_ids);

    cleanup_test_topic(&topic_admin, topic.name).await?;

    Ok(())
}

pub async fn basic_topic() -> Result<()> {
    let (client, topic) = create_test_topic().await?;

    tracing::info!("testing get_topic()");
    let get_topic = client
        .get_topic()
        .set_topic(topic.name.clone())
        .send()
        .await?;
    tracing::info!("success with get_topic={get_topic:?}");
    assert_eq!(get_topic.name, topic.name);

    cleanup_test_topic(&client, topic.name).await?;

    Ok(())
}

pub const TOPIC_ID_LENGTH: usize = 255;

fn random_topic_name(project: String) -> String {
    let prefix = "topic-";
    let topic_id: String = rand::rng()
        .sample_iter(&Alphanumeric)
        .take(TOPIC_ID_LENGTH - prefix.len())
        .map(char::from)
        .collect();
    format!("projects/{project}/topics/{prefix}{topic_id}")
}

pub async fn create_test_topic() -> Result<(TopicAdmin, Topic)> {
    let project_id = crate::project_id()?;
    let client = pubsub::client::TopicAdmin::builder()
        .with_tracing()
        .build()
        .await?;

    cleanup_stale_topics(&client, &project_id).await?;

    let topic_name = random_topic_name(project_id);
    let now = chrono::Utc::now().timestamp().to_string();

    tracing::info!("testing create_topic()");
    let topic = client
        .create_topic()
        .set_name(topic_name)
        .set_labels([("integration-test", "true"), ("create-time", &now)])
        .send()
        .await?;
    tracing::info!("success on create_topic: {topic:?}");

    Ok((client, topic))
}

pub async fn cleanup_test_topic(client: &TopicAdmin, topic_name: String) -> Result<()> {
    tracing::info!("testing delete_topic()");
    client.delete_topic().set_topic(topic_name).send().await?;
    tracing::info!("success on delete_topic");
    Ok(())
}

pub async fn cleanup_stale_topics(client: &TopicAdmin, project_id: &str) -> Result<()> {
    let stale_deadline = chrono::Utc::now() - chrono::Duration::hours(48);

    let mut topics = client
        .list_topics()
        .set_project(format!("projects/{project_id}"))
        .by_item();

    let mut pending = Vec::new();
    let mut names = Vec::new();
    while let Some(topic) = topics.next().await {
        let topic = topic?;
        if topic
            .labels
            .get("integration-test")
            .is_some_and(|v| v == "true")
            && topic
                .labels
                .get("create-time")
                .and_then(|v| v.parse::<i64>().ok())
                .and_then(|s| chrono::DateTime::from_timestamp(s, 0))
                .is_some_and(|create_time| create_time < stale_deadline)
        {
            let client = client.clone();
            let name = topic.name.clone();
            pending.push(tokio::spawn(async move {
                cleanup_test_topic(&client, name).await
            }));
            names.push(topic.name);
        }
    }

    let r: std::result::Result<Vec<_>, _> = futures::future::join_all(pending)
        .await
        .into_iter()
        .collect();
    r.map_err(Error::from)?
        .into_iter()
        .zip(names)
        .for_each(|(r, name)| tracing::info!("deleting topic {name} resulted in {r:?}"));

    Ok(())
}
