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

use google_cloud_pubsub::client::*;
use pubsub_samples::*;

pub async fn run_topic_examples_test() -> anyhow::Result<()> {
    let client = TopicAdmin::builder().build().await?;

    let mut topics = Vec::new();
    let result = run_topic_examples(&mut topics).await;
    // Ignore cleanup errors.
    for name in topics {
        if let Err(e) = cleanup_test_topic(&client, &name).await {
            println!("Error cleaning up test topic {name}: {e:?}");
        }
    }
    result
}

pub async fn run_sample_tests() -> anyhow::Result<()> {
    let (topic_admin, topic) = pubsub_samples::create_test_topic().await?;
    let subscription_admin = SubscriptionAdmin::builder().build().await?;

    let mut subscriptions = Vec::new();
    let project_id = std::env::var("GOOGLE_CLOUD_PROJECT")?;

    let result = run_sample_tests_inner(&project_id, &topic.name, &mut subscriptions).await;

    for name in subscriptions {
        if let Err(e) = cleanup_test_subscription(&subscription_admin, &name).await {
            println!("Error cleaning up test subscription {name}: {e:?}");
        }
    }
    if let Err(e) = cleanup_test_topic(&topic_admin, &topic.name).await {
        println!("Error cleaning up test topic: {e:?}");
    }
    result
}

async fn run_sample_tests_inner(
    project_id: &str,
    topic_name: &str,
    subscriptions: &mut Vec<String>,
) -> anyhow::Result<()> {
    let topic_id = topic_name.split('/').next_back().unwrap();
    quickstart_publisher::sample(project_id, topic_id).await?;

    run_subscription_examples(subscriptions, topic_name).await?;

    let (_, subscription) =
        pubsub_samples::create_test_subscription(topic_name.to_string()).await?;
    subscriptions.push(subscription.name.clone());
    let subscription_id = subscription.name.split('/').next_back().unwrap();

    quickstart_subscriber::sample(project_id, subscription_id).await?;

    #[cfg(feature = "unstable-stream")]
    subscriber_stream::sample(project_id, subscription_id).await?;

    Ok(())
}

#[cfg(all(test, feature = "run-integration-tests"))]
mod tests {
    use super::*;

    #[tokio::test]
    async fn topic_examples() -> anyhow::Result<()> {
        run_topic_examples_test().await
    }

    #[tokio::test]
    async fn sample_tests() -> anyhow::Result<()> {
        run_sample_tests().await
    }

    #[tokio::test]
    async fn quickstart_publisher() -> anyhow::Result<()> {
        let project_id = std::env::var("GOOGLE_CLOUD_PROJECT")?;
        let (topic_admin, topic) = pubsub_samples::create_test_topic().await?;
        let topic_id = topic.name.split('/').next_back().unwrap();

        let result = quickstart_publisher::sample(&project_id, topic_id).await;

        if let Err(e) = cleanup_test_topic(&topic_admin, &topic.name).await {
            println!("Error cleaning up test topic {e:?}");
        }
        result
    }

    #[tokio::test]
    async fn quickstart_subscriber() -> anyhow::Result<()> {
        let project_id = std::env::var("GOOGLE_CLOUD_PROJECT")?;

        let (topic_admin, topic) = pubsub_samples::create_test_topic().await?;
        let (subscription_admin, subscription) =
            pubsub_samples::create_test_subscription(&topic.name).await?;

        let subscription_id = subscription.name.split('/').next_back().unwrap();

        let result = quickstart_subscriber::sample(&project_id, subscription_id).await;

        if let Err(e) = cleanup_test_subscription(&subscription_admin, &subscription.name).await {
            println!("Error cleaning up test subscription {e:?}");
        }
        if let Err(e) = cleanup_test_topic(&topic_admin, &topic.name).await {
            println!("Error cleaning up test topic {e:?}");
        }
        result
    }

    #[cfg(feature = "unstable-stream")]
    #[tokio::test]
    async fn subscriber_stream() -> anyhow::Result<()> {
        let project_id = std::env::var("GOOGLE_CLOUD_PROJECT")?;

        let (topic_admin, topic) = pubsub_samples::create_test_topic().await?;
        let (subscription_admin, subscription) =
            pubsub_samples::create_test_subscription(topic.name.clone()).await?;

        let subscription_id = subscription.name.split('/').next_back().unwrap();

        let result = subscriber_stream::sample(&project_id, subscription_id).await;

        if let Err(e) = cleanup_test_subscription(&subscription_admin, &subscription.name).await {
            println!("Error cleaning up test subscription {e:?}");
        }
        if let Err(e) = cleanup_test_topic(&topic_admin, &topic.name).await {
            println!("Error cleaning up test topic {e:?}");
        }
        result
    }
}
