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

#[cfg(all(test, feature = "run-integration-tests"))]
mod pubsub {
    use google_cloud_test_utils::tracing::enable_tracing;

    #[tokio::test]
    async fn run_pubsub_basic_topic() -> anyhow::Result<()> {
        let _guard = enable_tracing();
        integration_tests_pubsub::basic_topic().await
    }

    #[tokio::test]
    async fn run_pubsub_basic_roundtrip() -> anyhow::Result<()> {
        let _guard = enable_tracing();
        let (topic_admin, topic) = pubsub_samples::create_test_topic().await?;
        let (sub_admin, sub) = pubsub_samples::create_test_subscription(topic.name.clone()).await?;

        integration_tests_pubsub::basic_publisher(topic.name.clone()).await?;
        integration_tests_pubsub::basic_subscriber(sub.name.clone()).await?;

        if let Err(e) = pubsub_samples::cleanup_test_subscription(&sub_admin, sub.name).await {
            tracing::info!("Error cleaning up test subscription: {e:?}");
        }
        if let Err(e) = pubsub_samples::cleanup_test_topic(&topic_admin, topic.name).await {
            tracing::info!("Error cleaning up test topic: {e:?}");
        }
        Ok(())
    }
}
