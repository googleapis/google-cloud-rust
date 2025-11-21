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

use crate::publisher::publisher::BatchedPublisherBuilder;

/// Creates [`Publisher`](super::client::Publisher) instances.
///
/// This is the main entry point for the publisher API. A single `Publisher`
/// can be used to create multiple `BatchedPublisher` for different topics.
/// It manages the underlying gRPC connection and authentication.
///
/// # Example
///
/// ```
/// # async fn sample() -> anyhow::Result<()> {
/// # use google_cloud_pubsub::client::Publisher;
/// # use google_cloud_pubsub::model::PubsubMessage;
///
/// // Create a client.
/// let publisher = Publisher::builder().build().await?;
///
/// // Create a batched publisher for a specific topic.
/// let batched_publisher = publisher.batched_publisher("projects/my-project/topics/my-topic").build();
///
/// // Publish a message.
/// let handle = batched_publisher.publish(PubsubMessage::new().set_data("hello world"));
/// let message_id = handle.await?;
/// println!("Message sent with ID: {}", message_id);
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct Publisher {
    pub(crate) inner: crate::generated::gapic_dataplane::client::Publisher,
}

/// A builder for [Publisher].
///
/// ```
/// # async fn sample() -> anyhow::Result<()> {
/// # use google_cloud_pubsub::*;
/// # use builder::publisher::PublisherBuilder;
/// # use client::Publisher;
/// let builder: PublisherBuilder = Publisher::builder();
/// let publisher = builder
///     .with_endpoint("https://pubsub.googleapis.com")
///     .build().await?;
/// # Ok(()) }
/// ```
pub type PublisherBuilder =
    gax::client_builder::ClientBuilder<client_builder::Factory, gaxi::options::Credentials>;

pub(crate) mod client_builder {
    use super::Publisher;

    pub struct Factory;
    impl gax::client_builder::internal::ClientFactory for Factory {
        type Client = Publisher;
        type Credentials = gaxi::options::Credentials;
        #[allow(unused_mut)]
        async fn build(
            self,
            mut config: gaxi::options::ClientConfig,
        ) -> gax::client_builder::Result<Self::Client> {
            // TODO(#3019): Pubsub default retry policy goes here.
            Self::Client::new(config).await
        }
    }
}

impl Publisher {
    /// Returns a builder for [Publisher].
    ///
    /// ```no_run
    /// # tokio_test::block_on(async {
    /// # use google_cloud_pubsub::client::Publisher;
    /// let publisher = Publisher::builder().build().await?;
    /// # gax::client_builder::Result::<()>::Ok(()) });
    /// ```
    pub fn builder() -> PublisherBuilder {
        gax::client_builder::internal::new_builder(client_builder::Factory)
    }

    /// Creates a new Pub/Sub publisher client with the given configuration.
    pub(crate) async fn new(
        config: gaxi::options::ClientConfig,
    ) -> Result<Self, gax::client_builder::Error> {
        let inner = crate::generated::gapic_dataplane::client::Publisher::new(config).await?;
        std::result::Result::Ok(Self { inner })
    }

    // // TODO(NOW)
    // /// Adds one or more messages to the topic. Returns `NOT_FOUND` if the topic
    // /// does not exist.
    // pub fn publish(&self) -> super::builder::publisher::Publish {
    //     super::builder::publisher::Publish::new(self.inner.clone())
    // }

    /// Creates a new `Publisher` for a given topic.
    ///
    /// ```
    /// # async fn sample() -> anyhow::Result<()> {
    /// # use google_cloud_pubsub::*;
    /// # use builder::publisher::PublisherBuilder;
    /// # use client::Publisher;
    /// # use model::PubsubMessage;
    /// let publisher = Publisher::builder().build().await?;
    /// let batched_publisher = publisher.batched_publisher("projects/my-project/topics/my-topic").build();
    /// let message_id = batched_publisher.publish(PubsubMessage::new().set_data("Hello, World")).await?;
    /// # Ok(()) }
    /// ```
    pub fn batched_publisher<T>(&self, topic: T) -> BatchedPublisherBuilder
    where
        T: Into<String>,
    {
        BatchedPublisherBuilder::new(self.inner.clone(), topic.into())
    }
}

#[cfg(test)]
mod tests {
    use super::Publisher;

    #[tokio::test]
    async fn builder() -> anyhow::Result<()> {
        let client = Publisher::builder()
            .with_credentials(auth::credentials::anonymous::Builder::new().build())
            .build()
            .await?;
        let _ = client.batched_publisher("projects/my-project/topics/my-topic".to_string());
        Ok(())
    }
}
