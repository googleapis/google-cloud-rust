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

use crate::publisher::options::BatchingOptions;
use std::result::Result::*;

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
/// let publisher = client.publisher("projects/my-project/topics/my-topic").build();
/// let message_id = publisher.publish(PubsubMessage::new().set_data("Hello, World"));
/// # Ok(()) }
/// ```
#[derive(Debug)]
pub struct Publisher {
    pub(crate) inner: crate::generated::gapic_dataplane::client::Publisher,
    topic: String,
    #[allow(dead_code)]
    pub(crate) batching_options: BatchingOptions,
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

/// A builder for [Publisher].
///
/// ```
/// # async fn sample() -> anyhow::Result<()> {
/// # use google_cloud_pubsub::*;
/// # use builder::publisher::ClientBuilder;
/// # use client::PublisherClient;
/// # use options::publisher::BatchingOptions;
/// let builder : ClientBuilder = PublisherClient::builder();
/// let client = builder
///     .with_endpoint("https://pubsub.googleapis.com")
///     .build().await?;
/// let publisher = client.publisher("projects/my-project/topics/topic")
///     .with_batching(BatchingOptions::new().set_message_count_threshold(1_u32))
///     .build();
/// # Ok(()) }
/// ```
#[derive(Clone, Debug)]
pub struct PublisherBuilder {
    pub(crate) inner: crate::generated::gapic_dataplane::client::Publisher,
    topic: String,
    batching_options: BatchingOptions,
}

impl PublisherBuilder {
    /// Creates a new Pub/Sub publisher builder for topic.
    pub(crate) fn new(
        client: crate::generated::gapic_dataplane::client::Publisher,
        topic: String,
    ) -> Self {
        Self {
            inner: client,
            topic,
            batching_options: BatchingOptions::default(),
        }
    }

    /// Change the message batching options.
    /// TODO(#3015): Provide example.
    pub fn with_batching(mut self, options: BatchingOptions) -> PublisherBuilder {
        self.batching_options = options;
        self
    }

    pub fn build(self) -> Publisher {
        Publisher {
            inner: self.inner,
            topic: self.topic,
            batching_options: self.batching_options,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{client::PublisherClient, publisher::options::BatchingOptions};

    #[tokio::test]
    async fn builder() -> anyhow::Result<()> {
        let client = PublisherClient::builder().build().await?;
        let builder = client.publisher("projects/my-project/topics/my-topic".to_string());
        let publisher = builder
            .with_batching(BatchingOptions::new().set_message_count_threshold(1_u32))
            .build();
        assert_eq!(publisher.batching_options.message_count_threshold, 1_u32);
        Ok(())
    }

    #[tokio::test]
    async fn default_batching() -> anyhow::Result<()> {
        let client = PublisherClient::builder().build().await?;
        let publisher = client
            .publisher("projects/my-project/topics/my-topic".to_string())
            .build();
        assert_ne!(publisher.batching_options.message_count_threshold, 1_u32);
        Ok(())
    }
}
