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

use std::result::Result::*;
/// Client for publishing messages to Pub/Sub topics.
#[derive(Clone, Debug)]
pub struct PublisherClient {
    pub(crate) inner: crate::generated::gapic_dataplane::client::Publisher,
}

/// A builder for [PublisherClient].
///
/// ```
/// # async fn sample() -> anyhow::Result<()> {
/// # use google_cloud_pubsub::*;
/// # use builder::publisher::ClientBuilder;
/// # use client::PublisherClient;
/// let builder : ClientBuilder = PublisherClient::builder();
/// let client = builder
///     .with_endpoint("https://pubsub.googleapis.com")
///     .build().await?;
/// let publisher_a = client.publisher("projects/my-project/topics/topic-a");
/// let publisher_b = client.publisher("projects/my-project/topics/topic-b");
/// # Ok(()) }
/// ```
pub type ClientBuilder =
    gax::client_builder::ClientBuilder<client_builder::Factory, gaxi::options::Credentials>;

pub(crate) mod client_builder {
    use super::PublisherClient;

    pub struct Factory;
    impl gax::client_builder::internal::ClientFactory for Factory {
        type Client = PublisherClient;
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

impl PublisherClient {
    /// Returns a builder for [PublisherClient].
    ///
    /// ```no_run
    /// # tokio_test::block_on(async {
    /// # use google_cloud_pubsub::client::PublisherClient;
    /// let client = PublisherClient::builder().build().await?;
    /// # gax::client_builder::Result::<()>::Ok(()) });
    /// ```
    pub fn builder() -> ClientBuilder {
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
    /// # use builder::publisher::ClientBuilder;
    /// # use client::PublisherClient;
    /// # use model::PubsubMessage;
    /// let client = PublisherClient::builder()
    ///     .with_endpoint("https://pubsub.googleapis.com")
    ///     .build().await?;
    /// let publisher = client.publisher("projects/my-project/topics/my-topic");
    /// let message_id = publisher.publish(PubsubMessage::new().set_data("Hello, World")).await?;
    /// # Ok(()) }
    /// ```
    pub fn publisher<T>(&self, topic: T) -> Publisher
    where
        T: Into<String>,
    {
        // In order to configure the Publisher, we will probably want
        // to return a builder.
        Publisher {
            inner: self.inner.clone(),
            topic: topic.into(),
        }
    }
}

/// Publishes messages to a single topic.
///
/// ```
/// # async fn sample() -> anyhow::Result<()> {
/// # use google_cloud_pubsub::*;
/// # use builder::publisher::ClientBuilder;
/// # use client::PublisherClient;
/// # use model::PubsubMessage;
/// let client = PublisherClient::builder()
///     .with_endpoint("https://pubsub.googleapis.com")
///     .build().await?;
/// let publisher = client.publisher("projects/my-project/topics/my-topic");
/// let message_id = publisher.publish(PubsubMessage::new().set_data("Hello, World"));
/// # Ok(()) }
/// ```
pub struct Publisher {
    pub(crate) inner: crate::generated::gapic_dataplane::client::Publisher,
    topic: String,
}

impl Publisher {
    /// Publishes a message to the topic.
    ///
    /// ```
    /// # use google_cloud_pubsub::client::Publisher;
    /// # async fn sample(publisher: Publisher) -> anyhow::Result<()> {
    /// # use google_cloud_pubsub::model::PubsubMessage;
    /// let message_id = publisher.publish(PubsubMessage::new().set_data("Hello, World")).await?;
    /// # Ok(()) }
    /// ```
    // This function will eventually return a type that implements Future instead,
    // which will remove the warning.
    #[allow(clippy::manual_async_fn)]
    pub fn publish(
        &self,
        msg: crate::model::PubsubMessage,
    ) -> impl Future<Output = crate::Result<String>> {
        async {
            // This will need to be done on the background task. For now, just
            // do it here to make the types work.
            let resp = self
                .inner
                .publish()
                .set_topic(self.topic.clone())
                .set_messages([msg])
                .send()
                .await?;
            match resp.message_ids.first() {
                Some(value) => Ok(value.to_owned()),
                _ => Err(crate::Error::io("service returned no message ID")),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::PublisherClient;

    #[tokio::test]
    async fn builder() -> anyhow::Result<()> {
        let client = PublisherClient::builder()
            .with_credentials(auth::credentials::anonymous::Builder::new().build())
            .build()
            .await?;
        let _ = client.publisher("projects/my-project/topics/my-topic".to_string());
        Ok(())
    }
}
