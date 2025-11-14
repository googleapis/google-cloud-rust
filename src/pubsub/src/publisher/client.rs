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

use crate::publisher::publisher::PublisherBuilder;

/// Creates [`Publisher`](super::publisher::Publisher) instances.
///
/// This is the main entry point for the publisher API. A single `PublisherFactory`
/// can be used to create multiple `Publisher` clients for different topics.
/// It manages the underlying gRPC connection and authentication.
///
/// # Example
///
/// ```
/// # async fn sample() -> anyhow::Result<()> {
/// # use google_cloud_pubsub::client::PublisherFactory;
/// # use google_cloud_pubsub::model::PubsubMessage;
///
/// // Create a factory.
/// let factory = PublisherFactory::builder().build().await?;
///
/// // Create a publisher for a specific topic.
/// let publisher = factory.publisher("projects/my-project/topics/my-topic").build();
///
/// // Publish a message.
/// let handle = publisher.publish(PubsubMessage::new().set_data("hello world"));
/// let message_id = handle.await?;
/// println!("Message sent with ID: {}", message_id);
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct PublisherFactory {
    pub(crate) inner: crate::generated::gapic_dataplane::client::Publisher,
}

/// A builder for [PublisherFactory].
///
/// ```
/// # async fn sample() -> anyhow::Result<()> {
/// # use google_cloud_pubsub::*;
/// # use builder::publisher::PublisherFactoryBuilder;
/// # use client::PublisherFactory;
/// let builder: PublisherFactoryBuilder = PublisherFactory::builder();
/// let factory = builder
///     .with_endpoint("https://pubsub.googleapis.com")
///     .build().await?;
/// # Ok(()) }
/// ```
pub type PublisherFactoryBuilder =
    gax::client_builder::ClientBuilder<client_builder::Factory, gaxi::options::Credentials>;

pub(crate) mod client_builder {
    use super::PublisherFactory;

    pub struct Factory;
    impl gax::client_builder::internal::ClientFactory for Factory {
        type Client = PublisherFactory;
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

impl PublisherFactory {
    /// Returns a builder for [PublisherFactory].
    ///
    /// ```no_run
    /// # tokio_test::block_on(async {
    /// # use google_cloud_pubsub::client::PublisherFactory;
    /// let factory = PublisherFactory::builder().build().await?;
    /// # gax::client_builder::Result::<()>::Ok(()) });
    /// ```
    pub fn builder() -> PublisherFactoryBuilder {
        gax::client_builder::internal::new_builder(client_builder::Factory)
    }

    /// Creates a new Pub/Sub publisher client with the given configuration.
    pub(crate) async fn new(
        config: gaxi::options::ClientConfig,
    ) -> Result<Self, gax::client_builder::Error> {
        let inner = crate::generated::gapic_dataplane::client::Publisher::new(config).await?;
        std::result::Result::Ok(Self { inner })
    }

    /// Creates a new `Publisher` for a given topic.
    ///
    /// ```
    /// # async fn sample() -> anyhow::Result<()> {
    /// # use google_cloud_pubsub::*;
    /// # use builder::publisher::PublisherFactoryBuilder;
    /// # use client::PublisherFactory;
    /// # use model::PubsubMessage;
    /// let factory = PublisherFactory::builder().build().await?;
    /// let publisher = factory.publisher("projects/my-project/topics/my-topic").build();
    /// let message_id = publisher.publish(PubsubMessage::new().set_data("Hello, World")).await?;
    /// # Ok(()) }
    /// ```
    pub fn publisher<T>(&self, topic: T) -> PublisherBuilder
    where
        T: Into<String>,
    {
        PublisherBuilder::new(self.inner.clone(), topic.into())
    }
}

#[cfg(test)]
mod tests {
    use super::PublisherFactory;

    #[tokio::test]
    async fn builder() -> anyhow::Result<()> {
        let client = PublisherFactory::builder()
            .with_credentials(auth::credentials::anonymous::Builder::new().build())
            .build()
            .await?;
        let _ = client.publisher("projects/my-project/topics/my-topic".to_string());
        Ok(())
    }
}
