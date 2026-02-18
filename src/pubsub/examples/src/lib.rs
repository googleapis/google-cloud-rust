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

pub mod quickstart_publisher;
pub mod quickstart_subscriber;
mod schema;
pub mod subscriber_stream;
mod subscription;
mod topic;

use google_cloud_gax::paginator::ItemPaginator as _;
use google_cloud_pubsub::client::SchemaService;
use google_cloud_pubsub::{client::SubscriptionAdmin, model::Subscription};
use google_cloud_pubsub::{client::TopicAdmin, model::Topic};
use rand::{RngExt, distr::Alphanumeric};
use tokio::task::JoinSet;

pub async fn run_topic_samples(topic_names: &mut Vec<String>) -> anyhow::Result<()> {
    let client = TopicAdmin::builder().build().await?;
    let project_id = std::env::var("GOOGLE_CLOUD_PROJECT")?;

    let id = random_topic_id();
    topic_names.push(format!("projects/{project_id}/topics/{id}"));
    topic::create_topic::sample(&client, &project_id, &id).await?;
    topic::list_topics::sample(&client, &project_id).await?;
    topic::list_topic_subscriptions::sample(&client, &project_id, &id).await?;

    Ok(())
}

pub async fn run_subscription_samples(
    subscription_names: &mut Vec<String>,
    topic_name: &str,
) -> anyhow::Result<()> {
    let client = SubscriptionAdmin::builder().build().await?;
    let project_id = std::env::var("GOOGLE_CLOUD_PROJECT")?;

    let topic_id = topic_name.split("/").last().unwrap();
    let id = random_subscription_id();
    subscription_names.push(format!("projects/{project_id}/subscriptions/{id}"));
    subscription::create_pull_subscription::sample(&client, &project_id, topic_id, &id).await?;
    subscription::list_subscriptions::sample(&client, &project_id).await?;

    quickstart_publisher::sample(&project_id, topic_id).await?;
    quickstart_subscriber::sample(&project_id, &id).await?;
    subscriber_stream::sample(&project_id, &id).await?;

    Ok(())
}

pub async fn run_schema_samples(schema_names: &mut Vec<String>) -> anyhow::Result<()> {
    let client = SchemaService::builder().build().await?;
    let project = std::env::var("GOOGLE_CLOUD_PROJECT")?;

    cleanup_stale_schemas(&client, &project).await?;

    let id = random_schema_id();
    schema_names.push(format!("projects/{project}/schemas/{id}"));
    schema::create_avro_schema::sample(&client, &project, &id).await?;
    schema::list_schemas::sample(&client, &project).await?;
    schema::list_schema_revisions::sample(&client, &project, &id).await?;

    Ok(())
}

pub async fn cleanup_test_topic(client: &TopicAdmin, topic_name: &str) -> anyhow::Result<()> {
    client.delete_topic().set_topic(topic_name).send().await?;
    Ok(())
}

pub async fn create_test_topic() -> anyhow::Result<(TopicAdmin, Topic)> {
    let project_id = std::env::var("GOOGLE_CLOUD_PROJECT")?;
    let client = TopicAdmin::builder().with_tracing().build().await?;

    let _ = cleanup_stale_topics(&client, &project_id).await;

    let topic_id = random_topic_id();
    let now = chrono::Utc::now().timestamp().to_string();

    let topic = client
        .create_topic()
        .set_name(format!("projects/{project_id}/topics/{topic_id}"))
        .set_labels([("integration-test", "true"), ("create-time", &now)])
        .send()
        .await?;
    println!("success on create_topic(): {topic:?}");

    Ok((client, topic))
}

pub async fn cleanup_stale_topics(client: &TopicAdmin, project_id: &str) -> anyhow::Result<()> {
    let stale_deadline = chrono::Utc::now() - chrono::Duration::hours(48);

    let mut topics = client
        .list_topics()
        .set_project(format!("projects/{project_id}"))
        .by_item();

    let mut pending = JoinSet::new();
    while let Some(topic) = topics.next().await.transpose()? {
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
            pending.spawn(async move {
                let name = topic.name.clone();
                (topic.name, cleanup_test_topic(&client, &name).await)
            });
        }
    }

    for (name, result) in pending.join_all().await {
        tracing::info!("deleting topic {name} resulted in {result:?}");
    }

    Ok(())
}

pub const TOPIC_ID_LENGTH: usize = 255;

fn random_topic_id() -> String {
    let prefix = "topic-";
    let topic_id: String = rand::rng()
        .sample_iter(&Alphanumeric)
        .take(TOPIC_ID_LENGTH - prefix.len())
        .map(char::from)
        .collect();
    format!("{prefix}{topic_id}")
}

pub async fn cleanup_test_subscription(
    client: &SubscriptionAdmin,
    subscription_name: &str,
) -> anyhow::Result<()> {
    client
        .delete_subscription()
        .set_subscription(subscription_name)
        .send()
        .await?;
    Ok(())
}

pub async fn create_test_subscription(
    topic_name: &str,
) -> anyhow::Result<(SubscriptionAdmin, Subscription)> {
    create_test_subscription_with_request(topic_name, Subscription::new()).await
}

pub async fn create_ordered_test_subscription(
    topic_name: &str,
) -> anyhow::Result<(SubscriptionAdmin, Subscription)> {
    create_test_subscription_with_request(
        topic_name,
        Subscription::new().set_enable_message_ordering(true),
    )
    .await
}

async fn create_test_subscription_with_request(
    topic_name: &str,
    request: Subscription,
) -> anyhow::Result<(SubscriptionAdmin, Subscription)> {
    let project_id = std::env::var("GOOGLE_CLOUD_PROJECT")?;
    let client = SubscriptionAdmin::builder().with_tracing().build().await?;

    let _ = cleanup_stale_subscriptions(&client, &project_id).await;

    let subscription_id = random_subscription_id();
    let now = chrono::Utc::now().timestamp().to_string();

    let request = request
        .set_name(format!(
            "projects/{project_id}/subscriptions/{subscription_id}"
        ))
        .set_topic(topic_name)
        .set_labels([("integration-test", "true"), ("create-time", &now)]);
    let subscription = client
        .create_subscription()
        .with_request(request)
        .send()
        .await?;
    println!("success on create_subscription(): {subscription:?}");

    Ok((client, subscription))
}

pub async fn cleanup_stale_subscriptions(
    client: &SubscriptionAdmin,
    project_id: &str,
) -> anyhow::Result<()> {
    let stale_deadline = chrono::Utc::now() - chrono::Duration::hours(48);

    let mut subscriptions = client
        .list_subscriptions()
        .set_project(format!("projects/{project_id}"))
        .by_item();

    let mut pending = JoinSet::new();
    while let Some(subscription) = subscriptions.next().await.transpose()? {
        if subscription
            .labels
            .get("integration-test")
            .is_some_and(|v| v == "true")
            && subscription
                .labels
                .get("create-time")
                .and_then(|v| v.parse::<i64>().ok())
                .and_then(|s| chrono::DateTime::from_timestamp(s, 0))
                .is_some_and(|create_time| create_time < stale_deadline)
        {
            let client = client.clone();
            pending.spawn(async move {
                let name = subscription.name.clone();
                (
                    subscription.name,
                    cleanup_test_subscription(&client, &name).await,
                )
            });
        }
    }

    for (name, result) in pending.join_all().await {
        tracing::info!("deleting subscription {name} resulted in {result:?}");
    }

    Ok(())
}

pub const SUBSCRIPTION_ID_LENGTH: usize = 255;

fn random_subscription_id() -> String {
    let prefix = "subscription-";
    let subscription_id: String = rand::rng()
        .sample_iter(&Alphanumeric)
        .take(TOPIC_ID_LENGTH - prefix.len())
        .map(char::from)
        .collect();
    format!("{prefix}{subscription_id}")
}

pub const SCHEMA_ID_LENGTH: usize = 255;

fn random_schema_id() -> String {
    let prefix = "schema-integration-test-";
    let schema_id: String = rand::rng()
        .sample_iter(&Alphanumeric)
        .take(SCHEMA_ID_LENGTH - prefix.len())
        .map(char::from)
        .collect();
    format!("{prefix}{schema_id}")
}

pub async fn cleanup_test_schema(client: &SchemaService, schema_name: &str) -> anyhow::Result<()> {
    client.delete_schema().set_name(schema_name).send().await?;
    Ok(())
}

pub async fn cleanup_stale_schemas(client: &SchemaService, project_id: &str) -> anyhow::Result<()> {
    use std::time::{Duration, SystemTime, UNIX_EPOCH};
    let stale_deadline = SystemTime::now().duration_since(UNIX_EPOCH)?;
    let stale_deadline = stale_deadline - Duration::from_secs(48 * 60 * 60);
    let stale_deadline = google_cloud_wkt::Timestamp::clamp(stale_deadline.as_secs() as i64, 0);

    let mut schemas = client
        .list_schemas()
        .set_parent(format!("projects/{project_id}"))
        .by_item();

    let mut pending = JoinSet::new();
    while let Some(schema) = schemas.next().await.transpose()? {
        if schema.name.contains("schema-integration-test-")
            && schema
                .revision_create_time
                .is_some_and(|create_time| create_time < stale_deadline)
        {
            let client = client.clone();
            pending.spawn(async move {
                let name = schema.name.clone();
                (schema.name, cleanup_test_schema(&client, &name).await)
            });
        }
    }

    for (name, result) in pending.join_all().await {
        tracing::info!("deleting schema {name} resulted in {result:?}");
    }

    Ok(())
}
