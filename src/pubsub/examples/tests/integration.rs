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

#[cfg(all(test, feature = "run-integration-tests"))]
mod tests {
    use google_cloud_pubsub::client::*;
    use pubsub_samples::*;

    #[tokio::test]
    async fn topic_examples() -> anyhow::Result<()> {
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

    #[tokio::test]
    async fn subscription_examples() -> anyhow::Result<()> {
        let (topic_admin, topic) = pubsub_samples::create_test_topic().await?;
        let client = SubscriptionAdmin::builder().build().await?;

        let mut subscriptions = Vec::new();
        let result = run_subscription_examples(&mut subscriptions, &topic.name).await;

        for name in subscriptions {
            if let Err(e) = cleanup_test_subscription(&client, &name).await {
                println!("Error cleaning up test subscription {name}: {e:?}");
            }
        }
        if let Err(e) = cleanup_test_topic(&topic_admin, &topic.name).await {
            println!("Error cleaning up test topic {e:?}");
        }
        result
    }
}
