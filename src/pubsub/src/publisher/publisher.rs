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

use crate::generated::gapic_dataplane;
use crate::publisher::options::BatchingOptions;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::{mpsc, oneshot};

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
    #[allow(dead_code)]
    pub(crate) batching_options: BatchingOptions,
    tx: UnboundedSender<BundledMessage>,
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
    pub fn publish(&self, msg: crate::model::PubsubMessage) -> crate::model_ext::PublishHandle {
        let (tx, rx) = tokio::sync::oneshot::channel();

        // If this fails, the worker is gone, which indicates something bad has happened.
        // The PublishHandle will automatically receive an error when `tx` is dropped.
        if self.tx.send(BundledMessage { msg, tx }).is_err() {
            // `tx` is dropped here if the send errors.
        }
        crate::model_ext::PublishHandle { rx }
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
    pub fn with_batching(mut self, options: BatchingOptions) -> PublisherBuilder {
        // TODO(#3015): Provide example in the public documentation.
        self.batching_options = options;
        self
    }

    pub fn build(self) -> Publisher {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        // Create the batching worker that will run in the background.
        // We don't need to keep track of a handle to the worker.
        // Dropping the Publisher will drop the only sender to the channel.
        // This will cause worker.run() to read None from the channel and close.
        let worker = Worker::new(self.topic, self.inner, self.batching_options.clone(), rx);
        tokio::spawn(worker.run());

        Publisher {
            batching_options: self.batching_options,
            tx,
        }
    }
}

/// Object that is passed to the worker task over the
/// main channel. This represents a single message and the sender
/// half of the channel to resolve the [PublishHandle].
struct BundledMessage {
    pub msg: crate::model::PubsubMessage,
    pub tx: oneshot::Sender<crate::Result<String>>,
}

/// The worker is spawned in a background task and handles
/// batching and publishing all messages that are sent to the publisher.
#[derive(Debug)]
struct Worker {
    topic_name: String,
    client: gapic_dataplane::client::Publisher,
    #[allow(dead_code)]
    batching_options: BatchingOptions,
    rx: mpsc::UnboundedReceiver<BundledMessage>,
}

impl Worker {
    fn new(
        topic_name: String,
        client: gapic_dataplane::client::Publisher,
        batching_options: BatchingOptions,
        rx: mpsc::UnboundedReceiver<BundledMessage>,
    ) -> Self {
        Self {
            topic_name,
            client,
            rx,
            batching_options,
        }
    }

    async fn run(mut self) {
        while let Some(msg) = self.rx.recv().await {
            let client = self.client.clone();
            let topic = self.topic_name.clone();
            // In the future, we may also want to keep track of JoinHandles in order to
            // flush the results.
            let _handle = tokio::spawn(async move {
                // For now, we just send the message immediately.
                // We will want to batch these requests.
                let request = client
                    .publish()
                    .set_topic(topic)
                    .set_messages(vec![msg.msg]);

                // Handle the response by extracting the message ID on success.
                let result = request
                    .send()
                    .await
                    .map(|response| response.message_ids.get(0).cloned().unwrap_or_default());

                // The user may have dropped the handle, so it is ok if this fails.
                let _ = msg.tx.send(result);
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{client::PublisherClient, publisher::options::BatchingOptions};
    use crate::{
        generated::gapic_dataplane::client::Publisher as GapicPublisher,
        model::{PublishResponse, PubsubMessage},
    };

    mockall::mock! {
        #[derive(Debug)]
        GapicPublisher {}
        impl crate::generated::gapic_dataplane::stub::Publisher for GapicPublisher {
            async fn publish(&self, req: crate::model::PublishRequest, _options: gax::options::RequestOptions) -> gax::Result<gax::response::Response<crate::model::PublishResponse>>;
        }
    }

    #[tokio::test]
    async fn test_worker_success() {
        let mut mock = MockGapicPublisher::new();
        mock.expect_publish()
            .returning({
                |r, _| {
                    assert_eq!(r.topic, "my-topic");
                    assert_eq!(r.messages.len(), 1);
                    let id = String::from_utf8(r.messages[0].data.to_vec()).unwrap();
                    Ok(gax::response::Response::from(
                        PublishResponse::new().set_message_ids(vec![id]),
                    ))
                }
            })
            .times(2);

        let client = GapicPublisher::from_stub(mock);
        let (tx_worker, rx_worker) = tokio::sync::mpsc::unbounded_channel();
        let worker = Worker::new(
            "my-topic".to_string(),
            client,
            BatchingOptions::default(),
            rx_worker,
        );
        tokio::spawn(worker.run());

        let messages = vec![
            PubsubMessage::new().set_data("hello".to_string()),
            PubsubMessage::new().set_data("world".to_string()),
        ];

        let mut handles = Vec::new();
        for msg in messages {
            let (tx, rx) = tokio::sync::oneshot::channel();
            let bundled = BundledMessage {
                msg: msg.clone(),
                tx,
            };
            tx_worker
                .send(bundled)
                .expect("channel should not be dropped");
            handles.push((msg, rx));
        }

        for (id, rx) in handles.into_iter() {
            let got = rx
                .await
                .expect("expected successful receive")
                .expect("expected message id");
            let id = String::from_utf8(id.data.to_vec()).unwrap();
            assert_eq!(got, id);
        }
    }

    #[tokio::test]
    async fn test_worker_error() {
        let mut mock = MockGapicPublisher::new();
        mock.expect_publish()
            .returning({
                |r, _| {
                    assert_eq!(r.topic, "my-topic");
                    assert_eq!(r.messages.len(), 1);
                    Err(gax::error::Error::io("io error"))
                }
            })
            .times(2);

        let client = GapicPublisher::from_stub(mock);
        let (tx_worker, rx_worker) = tokio::sync::mpsc::unbounded_channel();
        let worker = Worker::new(
            "my-topic".to_string(),
            client,
            BatchingOptions {
                message_count_threshold: 2,
                ..Default::default()
            },
            rx_worker,
        );
        tokio::spawn(worker.run());

        let messages = vec![
            PubsubMessage::new().set_data("hello".to_string()),
            PubsubMessage::new().set_data("world".to_string()),
        ];

        let mut handles = Vec::new();
        for msg in messages {
            let (tx, rx) = tokio::sync::oneshot::channel();
            let bundled = BundledMessage { msg, tx };
            tx_worker
                .send(bundled)
                .expect("channel should not be dropped");
            handles.push(rx);
        }

        for rx in handles.into_iter() {
            let got = rx.await.expect("expected successful receive");
            assert!(got.is_err());
        }
    }

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

        assert_eq!(
            publisher.batching_options.message_count_threshold,
            BatchingOptions::default().message_count_threshold
        );
        assert_eq!(
            publisher.batching_options.byte_threshold,
            BatchingOptions::default().byte_threshold
        );
        assert_eq!(
            publisher.batching_options.delay_threshold,
            BatchingOptions::default().delay_threshold
        );
        Ok(())
    }
}
