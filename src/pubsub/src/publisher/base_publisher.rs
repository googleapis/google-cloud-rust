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

use crate::publisher::builder::PublisherPartialBuilder;

/// Creates [`Publisher`](crate::client::Publisher) instances.
///
/// A single `BasePublisher` can be used to create multiple `Publisher` clients
/// for different topics. It manages the underlying gRPC connection and
/// authentication.
///
/// # Example
///
/// ```
/// # async fn sample() -> anyhow::Result<()> {
/// # use google_cloud_pubsub::client::BasePublisher;
/// # use google_cloud_pubsub::model::Message;
///
/// // Create a client.
/// let client: BasePublisher = BasePublisher::builder().build().await?;
///
/// // Create a publisher for a specific topic.
/// let publisher = client.publisher("projects/my-project/topics/my-topic").build();
///
/// // Publish a message.
/// let handle = publisher.publish(Message::new().set_data("hello world"));
/// let message_id = handle.await?;
/// println!("Message sent with ID: {}", message_id);
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct BasePublisher {
    pub(crate) inner: crate::generated::gapic_dataplane::client::Publisher,
    pub(crate) total_timeout: Option<std::time::Duration>,
}

pub use super::client_builder::BasePublisherBuilder;

impl BasePublisher {
    /// Returns a builder for [BasePublisher].
    ///
    /// ```
    /// # async fn sample() -> anyhow::Result<()> {
    /// # use google_cloud_pubsub::client::BasePublisher;
    /// let client: BasePublisher = BasePublisher::builder().build().await?;
    /// # Ok(()) }
    /// ```
    pub fn builder() -> BasePublisherBuilder {
        BasePublisherBuilder::new()
    }

    /// Creates a new Pub/Sub publisher client with the given configuration.
    pub(crate) async fn new(builder: BasePublisherBuilder) -> crate::ClientBuilderResult<Self> {
        let total_timeout = builder
            .config
            .retry_policy
            .as_ref()
            .and_then(|p| p.remaining_time(&google_cloud_gax::retry_state::RetryState::new(false)));
        let inner =
            crate::generated::gapic_dataplane::client::Publisher::new(builder.config).await?;
        std::result::Result::Ok(Self {
            inner,
            total_timeout,
        })
    }

    /// Creates a new `Publisher` for a given topic.
    ///
    /// ```
    /// # async fn sample() -> anyhow::Result<()> {
    /// # use google_cloud_pubsub::*;
    /// # use builder::publisher::BasePublisherBuilder;
    /// # use client::BasePublisher;
    /// # use model::Message;
    /// let client = BasePublisher::builder().build().await?;
    /// let publisher = client.publisher("projects/my-project/topics/my-topic").build();
    /// let message_id = publisher.publish(Message::new().set_data("Hello, World")).await?;
    /// # Ok(()) }
    /// ```
    pub fn publisher<T>(&self, topic: T) -> PublisherPartialBuilder
    where
        T: Into<String>,
    {
        PublisherPartialBuilder::new(self.inner.clone(), topic.into())
            .with_total_timeout(self.total_timeout)
    }
}

#[cfg(test)]
mod tests {
    use super::BasePublisher;
    use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
    use google_cloud_gax::retry_policy::{AlwaysRetry, RetryPolicyExt};
    use std::time::Duration;

    #[tokio::test]
    async fn builder() -> anyhow::Result<()> {
        let client = BasePublisher::builder()
            .with_credentials(Anonymous::new().build())
            .build()
            .await?;
        let _ = client.publisher("projects/my-project/topics/my-topic".to_string());
        Ok(())
    }

    #[tokio::test]
    async fn default_total_timeout() -> anyhow::Result<()> {
        let client = BasePublisher::builder()
            .with_credentials(Anonymous::new().build())
            .build()
            .await?;
        let timeout = client.total_timeout.expect("default total_timeout should be present");
        assert!(timeout <= Duration::from_secs(600) && timeout >= Duration::from_secs(590));

        let partial_builder = client.publisher("projects/my-project/topics/my-topic");
        assert_eq!(partial_builder.total_timeout, client.total_timeout);
        Ok(())
    }

    #[tokio::test]
    async fn custom_total_timeout() -> anyhow::Result<()> {
        let client = BasePublisher::builder()
            .with_credentials(Anonymous::new().build())
            .with_retry_policy(AlwaysRetry.with_time_limit(Duration::from_secs(45)))
            .build()
            .await?;
        let timeout = client.total_timeout.expect("custom total_timeout should be present");
        assert!(timeout <= Duration::from_secs(45) && timeout >= Duration::from_secs(40));

        let partial_builder = client.publisher("projects/my-project/topics/my-topic");
        assert_eq!(partial_builder.total_timeout, client.total_timeout);
        Ok(())
    }

    #[tokio::test]
    async fn attempt_limit_only_has_no_total_timeout() -> anyhow::Result<()> {
        let client = BasePublisher::builder()
            .with_credentials(Anonymous::new().build())
            .with_retry_policy(AlwaysRetry.with_attempt_limit(3))
            .build()
            .await?;
        assert_eq!(client.total_timeout, None);
        let partial_builder = client.publisher("projects/my-project/topics/my-topic");
        assert_eq!(partial_builder.total_timeout, None);
        Ok(())
    }
}
