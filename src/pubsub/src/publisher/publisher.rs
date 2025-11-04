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

use crate::generated::gapic_dataplane::client::Publisher as GapicPublisher;
use crate::publisher::options::BatchingOptions;
use std::time::Duration;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::{mpsc, oneshot};

const MAX_DELAY: Duration = Duration::from_secs(60 * 60 * 24); // 1 day
// These limits come from https://cloud.google.com/pubsub/quotas.
const MAX_MESSAGES: u32 = 1000;
const MAX_BYTES: u32 = 1e7 as u32; // 10MB

/// Publishes messages to a single topic.
///
/// ```
/// # async fn sample() -> anyhow::Result<()> {
/// # use google_cloud_pubsub::*;
/// # use builder::publisher::ClientBuilder;
/// # use client::PublisherFactory;
/// # use model::PubsubMessage;
/// let client = PublisherFactory::builder()
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
/// # use client::PublisherFactory;
/// # use options::publisher::BatchingOptions;
/// let builder : ClientBuilder = PublisherFactory::builder();
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
    pub(crate) inner: GapicPublisher,
    topic: String,
    batching_options: BatchingOptions,
}

impl PublisherBuilder {
    /// Creates a new Pub/Sub publisher builder for topic.
    pub(crate) fn new(client: GapicPublisher, topic: String) -> Self {
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
        // Enforce limits by clamping the user-provided options.
        let batching_options = BatchingOptions::new()
            .set_delay_threshold(std::cmp::min(
                self.batching_options.delay_threshold,
                MAX_DELAY,
            ))
            .set_message_count_threshold(std::cmp::min(
                self.batching_options.message_count_threshold,
                MAX_MESSAGES,
            ))
            .set_byte_threshold(std::cmp::min(
                self.batching_options.byte_threshold,
                MAX_BYTES,
            ));

        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        // Create the batching worker that will run in the background.
        // We don't need to keep track of a handle to the worker.
        // Dropping the Publisher will drop the only sender to the channel.
        // This will cause worker.run() to read None from the channel and close.
        let worker = Worker::new(self.topic, self.inner, batching_options.clone(), rx);
        tokio::spawn(worker.run());

        Publisher {
            batching_options,
            tx,
        }
    }
}

/// Object that is passed to the worker task over the
/// main channel. This represents a single message and the sender
/// half of the channel to resolve the [PublishHandle].
#[derive(Debug)]
struct BundledMessage {
    pub msg: crate::model::PubsubMessage,
    pub tx: oneshot::Sender<crate::Result<String>>,
}

/// The worker is spawned in a background task and handles
/// batching and publishing all messages that are sent to the publisher.
#[derive(Debug)]
struct Worker {
    topic_name: String,
    client: GapicPublisher,
    #[allow(dead_code)]
    batching_options: BatchingOptions,
    rx: mpsc::UnboundedReceiver<BundledMessage>,
}

impl Worker {
    fn new(
        topic_name: String,
        client: GapicPublisher,
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
        let mut batch = Batch::new();
        // Set max delay at 1 year to avoid overflow. This should probably be in the setting itself.
        // Would be reasonable to make it much less.
        let delay = self.batching_options.delay_threshold.min(
            Duration::from_secs(60 * 60 * 24 * 365), // 1 year.
        );
        let message_limit = self.batching_options.message_count_threshold;

        let timer = tokio::time::sleep(delay);
        // Pin the timer to the stack.
        tokio::pin!(timer);
        loop {
            tokio::select! {
                // Handle timer events.
                // This branch will only be checked when there is a non-empty batch,
                // so this will not fire continuously.
                _ = &mut timer, if !batch.batch.is_empty() => {
                    let batch_to_send = std::mem::take(&mut batch);
                    tokio::spawn(batch_to_send.send(self.client.clone(), self.topic_name.clone()));
                }
                // Handle receiving a message from the channel.
                msg = self.rx.recv() => {
                    match msg {
                        Some(msg) => {
                            // Reset the timer if this is the first message to be added to the batch.
                            if batch.batch.is_empty() {
                                timer.as_mut().reset(tokio::time::Instant::now() + delay);
                            }
                            batch.push(msg);
                            if batch.batch.len() as u32 >= message_limit {
                                let batch_to_send = std::mem::take(&mut batch);
                                // In the future, we may also want to keep track of JoinHandles in order to
                                // flush the results.
                                let _handle =
                                    tokio::spawn(batch_to_send.send(self.client.clone(), self.topic_name.clone()));
                            }
                        },
                        None => {
                            // The sender has been dropped, stop running.
                            break;
                        }
                    }
                }
            }
        }
    }
}
#[derive(Debug)]
struct Batch {
    // TODO(#3686): A batch should also keep track of its total size
    // for improved performance.
    batch: Vec<BundledMessage>,
}

impl Default for Batch {
    fn default() -> Self {
        Self::new()
    }
}

impl Batch {
    fn new() -> Self {
        Batch { batch: Vec::new() }
    }

    fn push(&mut self, msg: BundledMessage) {
        self.batch.push(msg);
    }

    /// Send the batch to the service and process the results.
    async fn send(self, client: GapicPublisher, topic: String) {
        let (msgs, txs): (Vec<_>, Vec<_>) =
            self.batch.into_iter().map(|msg| (msg.msg, msg.tx)).unzip();
        let request = client.publish().set_topic(topic).set_messages(msgs);

        // Handle the response by extracting the message ID on success.
        match request.send().await {
            Err(e) => {
                let e = Arc::new(e);
                txs.into_iter().for_each(|tx| {
                    // The user may have dropped the handle, so it is ok if this fails.
                    // TODO(#3689): The error type for this is incorrect, will need to handle
                    // this error propagation more fully.
                    let _ = tx.send(Err(gax::error::Error::io(Arc::clone(&e))));
                });
            }
            Ok(result) => {
                txs.into_iter()
                    .zip(result.message_ids.into_iter())
                    .for_each(|(tx, result)| {
                        // The user may have dropped the handle, so it is ok if this fails.
                        let _ = tx.send(Ok(result));
                    });
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{client::PublisherFactory, publisher::options::BatchingOptions};
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
        let publisher = PublisherBuilder::new(client, "my-topic".to_string())
            .with_batching(BatchingOptions::default().set_message_count_threshold(1_u32))
            .build();

        let messages = vec![
            PubsubMessage::new().set_data("hello".to_string()),
            PubsubMessage::new().set_data("world".to_string()),
        ];
        let mut handles = Vec::new();
        for msg in messages {
            let handle = publisher.publish(msg.clone());
            handles.push((msg, handle));
        }

        for (id, rx) in handles.into_iter() {
            let got = rx.await.expect("expected message id");
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
        let publisher = PublisherBuilder::new(client, "my-topic".to_string())
            .with_batching(BatchingOptions::default().set_message_count_threshold(1_u32))
            .build();

        let messages = vec![
            PubsubMessage::new().set_data("hello".to_string()),
            PubsubMessage::new().set_data("world".to_string()),
        ];

        let mut handles = Vec::new();
        for msg in messages {
            let handle = publisher.publish(msg.clone());
            handles.push(handle);
        }

        for rx in handles.into_iter() {
            let got = rx.await;
            assert!(got.is_err());
        }
    }

    #[tokio::test]
    async fn test_batching_message_count_success() {
        // Make sure all messages in a batch receive the correct message ID.
        let mut mock = MockGapicPublisher::new();
        mock.expect_publish().return_once({
            |r, _| {
                assert_eq!(r.topic, "my-topic");
                assert_eq!(r.messages.len(), 2);
                let ids = r
                    .messages
                    .iter()
                    .map(|m| String::from_utf8(m.data.to_vec()).unwrap());
                Ok(gax::response::Response::from(
                    PublishResponse::new().set_message_ids(ids),
                ))
            }
        });

        let client = GapicPublisher::from_stub(mock);
        let publisher = PublisherBuilder::new(client, "my-topic".to_string())
            .with_batching(BatchingOptions {
                message_count_threshold: 2_u32,
                // Maximizing byte and delay threshold to make sure that
                // message count is the metric used.
                byte_threshold: u32::MAX,
                delay_threshold: std::time::Duration::MAX,
            })
            .build();

        let messages = vec![
            PubsubMessage::new().set_data("hello".to_string()),
            PubsubMessage::new().set_data("world".to_string()),
        ];
        let mut handles = Vec::new();
        for msg in messages {
            let handle = publisher.publish(msg.clone());
            handles.push((msg, handle));
        }

        for (id, rx) in handles.into_iter() {
            let got = rx.await.expect("expected message id");
            let id = String::from_utf8(id.data.to_vec()).unwrap();
            assert_eq!(got, id);
        }
    }

    #[tokio::test]
    async fn test_batching_message_count_error() {
        // Make sure all messages in a batch receive an error.
        let mut mock = MockGapicPublisher::new();
        mock.expect_publish().return_once({
            |r, _| {
                assert_eq!(r.topic, "my-topic");
                assert_eq!(r.messages.len(), 2);
                Err(gax::error::Error::io("io error"))
            }
        });

        let client = GapicPublisher::from_stub(mock);
        let publisher = PublisherBuilder::new(client, "my-topic".to_string())
            .with_batching(BatchingOptions {
                message_count_threshold: 2_u32,
                // Maximizing byte and delay threshold to make sure that
                // message count is the metric used.
                byte_threshold: u32::MAX,
                delay_threshold: std::time::Duration::MAX,
            })
            .build();

        let messages = vec![
            PubsubMessage::new().set_data("hello".to_string()),
            PubsubMessage::new().set_data("world".to_string()),
        ];
        let mut handles = Vec::new();
        for msg in messages {
            let handle = publisher.publish(msg.clone());
            handles.push(handle);
        }

        for rx in handles.into_iter() {
            let got = rx.await;
            assert!(got.is_err());
        }
    }

    #[tokio::test]
    async fn test_batching_messages_send_on_timeout() {
        let mut mock = MockGapicPublisher::new();
        mock.expect_publish().returning({
            |r, _| {
                assert_eq!(r.topic, "my-topic");
                let ids = r
                    .messages
                    .iter()
                    .map(|m| String::from_utf8(m.data.to_vec()).unwrap());
                Ok(gax::response::Response::from(
                    PublishResponse::new().set_message_ids(ids),
                ))
            }
        });

        let client = GapicPublisher::from_stub(mock);
        let publisher = PublisherBuilder::new(client, "my-topic".to_string())
            .with_batching(BatchingOptions {
                delay_threshold: std::time::Duration::from_millis(10),
                // Maximizing byte and message count threshold to make sure that
                // delay is used.
                byte_threshold: u32::MAX,
                message_count_threshold: u32::MAX,
            })
            .build();

        // Test that messages eventually all send.
        for _ in 0..3 {
            let messages = vec![
                PubsubMessage::new().set_data("hello".to_string()),
                PubsubMessage::new().set_data("world".to_string()),
            ];
            let mut handles = Vec::new();
            for msg in messages {
                let handle = publisher.publish(msg.clone());
                handles.push((msg, handle));
            }

            for (id, rx) in handles.into_iter() {
                let got = rx.await.expect("expected message id");
                let id = String::from_utf8(id.data.to_vec()).unwrap();
                assert_eq!(got, id);
            }
        }
    }

    #[tokio::test]
    async fn builder() -> anyhow::Result<()> {
        let client = PublisherFactory::builder().build().await?;
        let builder = client.publisher("projects/my-project/topics/my-topic".to_string());
        let publisher = builder
            .with_batching(BatchingOptions::new().set_message_count_threshold(1_u32))
            .build();
        assert_eq!(publisher.batching_options.message_count_threshold, 1_u32);
        Ok(())
    }

    #[tokio::test]
    async fn default_batching() -> anyhow::Result<()> {
        let client = PublisherFactory::builder().build().await?;
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

    #[tokio::test]
    async fn test_builder_clamping() -> anyhow::Result<()> {
        // Test values that are too high and should be clamped.
        let oversized_options = BatchingOptions::new()
            .set_delay_threshold(MAX_DELAY + Duration::from_secs(1))
            .set_message_count_threshold(MAX_MESSAGES + 1)
            .set_byte_threshold(MAX_BYTES + 1);

        let client = PublisherFactory::builder().build().await?;
        let publisher = client
            .publisher("projects/my-project/topics/my-topic".to_string())
            .with_batching(oversized_options)
            .build();
        let got = publisher.batching_options;

        assert_eq!(got.delay_threshold, MAX_DELAY);
        assert_eq!(got.message_count_threshold, MAX_MESSAGES);
        assert_eq!(got.byte_threshold, MAX_BYTES);

        // Test values that are within limits and should not be changed.
        let normal_options = BatchingOptions::new()
            .set_delay_threshold(Duration::from_secs(10))
            .set_message_count_threshold(10_u32)
            .set_byte_threshold(100_u32);

        let publisher = client
            .publisher("projects/my-project/topics/my-topic".to_string())
            .with_batching(normal_options.clone())
            .build();
        let got = publisher.batching_options;

        assert_eq!(got.delay_threshold, normal_options.delay_threshold);
        assert_eq!(
            got.message_count_threshold,
            normal_options.message_count_threshold
        );
        assert_eq!(got.byte_threshold, normal_options.byte_threshold);

        Ok(())
    fn create_bundled_message_helper(
        data: String,
    ) -> (
        BundledMessage,
        tokio::sync::oneshot::Receiver<crate::Result<String>>,
    ) {
        let (tx, rx) = tokio::sync::oneshot::channel();
        (
            BundledMessage {
                tx,
                msg: PubsubMessage::new().set_data(data),
            },
            rx,
        )
    }

    #[tokio::test]
    async fn test_push_batch() {
        let mut batch = Batch::new();
        assert!(batch.batch.is_empty());

        let (message_a, _rx_a) = create_bundled_message_helper("hello".to_string());
        batch.push(message_a);
        assert_eq!(batch.batch.len(), 1);

        let (message_b, _rx_b) = create_bundled_message_helper(", ".to_string());
        batch.push(message_b);
        assert_eq!(batch.batch.len(), 2);

        let (message_c, _rx_c) = create_bundled_message_helper("world".to_string());
        batch.push(message_c);
        assert_eq!(batch.batch.len(), 3);
    }
}
