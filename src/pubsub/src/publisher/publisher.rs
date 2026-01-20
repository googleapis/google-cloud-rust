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

use super::options::BatchingOptions;
use crate::generated::gapic_dataplane::client::Publisher as GapicPublisher;
use crate::publisher::worker::BundledMessage;
use crate::publisher::worker::ToWorker;
use crate::publisher::worker::Worker;
use std::time::Duration;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::oneshot;

const MAX_DELAY: Duration = Duration::from_secs(60 * 60 * 24); // 1 day
// These limits come from https://cloud.google.com/pubsub/docs/batch-messaging#quotas_and_limits_on_batch_messaging.
// Client libraries are expected to enforce these limits on batch siziing.
const MAX_MESSAGES: u32 = 1000;
const MAX_BYTES: u32 = 1e7 as u32; // 10MB

/// A `Publisher` sends messages to a specific topic. It manages message batching
/// and sending in a background task.
///
/// Publishers are created via a [`Client`](crate::client::Client).
///
/// ```
/// # async fn sample() -> anyhow::Result<()> {
/// # use google_cloud_pubsub::*;
/// # use client::Client;
/// # use model::PubsubMessage;
/// let client = Client::builder().build().await?;
/// let publisher = client.publisher("projects/my-project/topics/my-topic").build();
/// let message_id = publisher.publish(PubsubMessage::new().set_data("Hello, World"));
/// # Ok(()) }
/// ```
#[derive(Debug, Clone)]
pub struct Publisher {
    #[allow(dead_code)]
    pub(crate) batching_options: BatchingOptions,
    tx: UnboundedSender<ToWorker>,
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
        if self
            .tx
            .send(ToWorker::Publish(BundledMessage { msg, tx }))
            .is_err()
        {
            // `tx` is dropped here if the send errors.
        }
        crate::model_ext::PublishHandle { rx }
    }

    /// Flushes all outstanding messages.
    ///
    /// This method sends any messages that have been published but not yet sent,
    /// regardless of the configured batching options (`delay_threshold`, etc.).
    ///
    /// This method is `async` and returns only after all publish attempts for the
    /// messages in the snapshot have completed. A "completed" attempt means the
    /// message has either been successfully sent, or has failed permanently after
    /// exhausting any applicable retry policies.
    ///
    /// After flush()` returns, the final result of each individual publish
    /// operation (i.e., a success with a message ID or a terminal error) will
    /// be available on its corresponding [PublishHandle](crate::model_ext::PublishHandle).
    ///
    /// Messages published after `flush()` is called will be buffered for a
    /// subsequent batch and are not included in this flush operation.
    ///
    /// # Example
    ///
    /// ```
    /// # use google_cloud_pubsub::model::PubsubMessage;
    /// # async fn sample(publisher: google_cloud_pubsub::client::Publisher) -> anyhow::Result<()> {
    /// // Publish some messages. They will be buffered according to batching options.
    /// let handle1 = publisher.publish(PubsubMessage::new().set_data("foo".to_string()));
    /// let handle2 = publisher.publish(PubsubMessage::new().set_data("bar".to_string()));
    ///
    /// // Flush ensures that these messages are sent immediately and waits for
    /// // the send to complete.
    /// publisher.flush().await;
    ///
    /// // The results for handle1 and handle2 are available.
    /// let id1 = handle1.await?;
    /// let id2 = handle2.await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn flush(&self) {
        let (tx, rx) = oneshot::channel();
        if self.tx.send(ToWorker::Flush(tx)).is_err() {
            // `tx` is dropped here if the send errors.
        }
        rx.await
            .expect("the client library should not release the sender");
    }
}

/// Creates `Publisher`s.
///
/// Publishers are created via a [`Client`][crate::client::Client].
///
/// # Example
///
/// ```
/// # async fn sample() -> anyhow::Result<()> {
/// # use google_cloud_pubsub::*;
/// # use builder::publisher::ClientBuilder;
/// # use client::Client;
/// let client = Client::builder().build().await?;
/// let publisher = client.publisher("projects/my-project/topics/topic").build();
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

    /// Sets the maximum number of messages to be batched together for a single `Publish` call.
    /// When this number is reached, the batch is sent.
    ///
    /// Setting this to `1` disables batching by message count.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_pubsub::client::Client;
    /// # async fn sample() -> anyhow::Result<()> {
    /// # let client = Client::builder().build().await?;
    /// let publisher = client.publisher("projects/my-project/topics/my-topic")
    ///     .set_message_count_threshold(100)
    ///     .build();
    /// # Ok(()) }
    /// ```
    pub fn set_message_count_threshold(mut self, threshold: u32) -> PublisherBuilder {
        self.batching_options = self.batching_options.set_message_count_threshold(threshold);
        self
    }

    /// Sets the byte threshold for batching in a single `Publish` call.
    /// When this many bytes are accumulated, the batch is sent.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_pubsub::client::Client;
    /// # async fn sample() -> anyhow::Result<()> {
    /// # let client = Client::builder().build().await?;
    /// let publisher = client.publisher("projects/my-project/topics/my-topic")
    ///     .set_byte_threshold(100)
    ///     .build();
    /// # Ok(()) }
    /// ```
    pub fn set_byte_threshold(mut self, threshold: u32) -> PublisherBuilder {
        self.batching_options = self.batching_options.set_byte_threshold(threshold);
        self
    }

    /// Sets the maximum amount of time the publisher will wait before sending a
    /// batch. When this delay is reached, the current batch is sent, regardless
    /// of the number of messages or total byte size.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_pubsub::client::Client;
    /// # use std::time::Duration;
    /// # async fn sample() -> anyhow::Result<()> {
    /// # let client = Client::builder().build().await?;
    /// let publisher = client.publisher("projects/my-project/topics/my-topic")
    ///     .set_delay_threshold(Duration::from_millis(50))
    ///     .build();
    /// # Ok(()) }
    /// ```
    pub fn set_delay_threshold(mut self, threshold: Duration) -> PublisherBuilder {
        self.batching_options = self.batching_options.set_delay_threshold(threshold);
        self
    }

    /// Creates a new [`Publisher`] from the builder's configuration.
    // This method starts a background task to manage the batching
    // and sending of messages. The returned `Publisher` is a
    // lightweight handle for sending messages to that background task
    // over a channel.
    pub fn build(self) -> Publisher {
        // Enforce limits by clamping the user-provided options.
        let batching_options = BatchingOptions::new()
            .set_delay_threshold(
                self.batching_options
                    .delay_threshold
                    .clamp(Duration::ZERO, MAX_DELAY),
            )
            .set_message_count_threshold(
                self.batching_options
                    .message_count_threshold
                    .clamp(0, MAX_MESSAGES),
            )
            .set_byte_threshold(self.batching_options.byte_threshold.clamp(0, MAX_BYTES));

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{client::Client, publisher::options::BatchingOptions};
    use crate::{
        generated::gapic_dataplane::client::Publisher as GapicPublisher,
        model::{PublishResponse, PubsubMessage},
    };
    use mockall::Sequence;
    use std::error::Error;

    mockall::mock! {
        #[derive(Debug)]
        GapicPublisher {}
        impl crate::generated::gapic_dataplane::stub::Publisher for GapicPublisher {
            async fn publish(&self, req: crate::model::PublishRequest, _options: gax::options::RequestOptions) -> gax::Result<gax::response::Response<crate::model::PublishResponse>>;
        }
    }

    // Similar to GapicPublisher but returns impl Future instead.
    // This is useful for mocking a response with delays/timeouts.
    // See https://github.com/asomers/mockall/issues/189 for more
    // detail on why this is needed.
    // While this can used inplace of GapicPublisher, it makes the
    // normal usage without async closure much more cumbersome.
    mockall::mock! {
        #[derive(Debug)]
        GapicPublisherWithFuture {}
        impl crate::generated::gapic_dataplane::stub::Publisher for GapicPublisherWithFuture {
            fn publish(&self, req: crate::model::PublishRequest, _options: gax::options::RequestOptions) -> impl Future<Output=gax::Result<gax::response::Response<crate::model::PublishResponse>>> + Send;
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
            .set_message_count_threshold(1_u32)
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

    #[tokio::test(start_paused = true)]
    async fn test_drop_publisher() {
        // If we hold on to the handles returned from the publisher, it should
        // be safe to drop the publisher and .await on the handles.
        let mut mock = MockGapicPublisher::new();
        mock.expect_publish().return_once({
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
            .set_message_count_threshold(1000_u32)
            .set_delay_threshold(Duration::from_secs(60))
            .build();

        let start = tokio::time::Instant::now();
        let messages = vec![
            PubsubMessage::new().set_data("hello".to_string()),
            PubsubMessage::new().set_data("world".to_string()),
        ];
        let mut handles = Vec::new();
        for msg in messages {
            let handle = publisher.publish(msg.clone());
            handles.push((msg, handle));
        }
        drop(publisher); // This should trigger the batch to send, no delay.

        for (id, rx) in handles.into_iter() {
            let got = rx.await.expect("expected message id");
            let id = String::from_utf8(id.data.to_vec()).unwrap();
            assert_eq!(got, id);
            assert_eq!(start.elapsed(), Duration::ZERO);
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
            .set_message_count_threshold(1_u32)
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

    #[tokio::test(start_paused = true)]
    async fn test_worker_flush() {
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
            // Set a long delay.
            .set_message_count_threshold(1000_u32)
            .set_delay_threshold(Duration::from_secs(60))
            .build();

        let start = tokio::time::Instant::now();
        let messages = vec![
            PubsubMessage::new().set_data("hello".to_string()),
            PubsubMessage::new().set_data("world".to_string()),
        ];
        let mut handles = Vec::new();
        for msg in messages {
            let handle = publisher.publish(msg.clone());
            handles.push((msg, handle));
        }

        publisher.flush().await;
        assert_eq!(start.elapsed(), Duration::ZERO);

        let post = publisher.publish(PubsubMessage::new().set_data("after".to_string()));
        for (id, rx) in handles.into_iter() {
            let got = rx.await.expect("expected message id");
            let id = String::from_utf8(id.data.to_vec()).unwrap();
            assert_eq!(got, id);
            assert_eq!(start.elapsed(), Duration::ZERO);
        }

        // The last message is only sent after the next timeout
        // (worker does not continue to flush).
        let got = post.await.expect("expected message id");
        assert_eq!(got, "after");
        assert_eq!(start.elapsed(), Duration::from_secs(60));
    }

    // User's should be able to drop handles and the messages will still send.
    #[tokio::test(start_paused = true)]
    async fn test_worker_drop_handles() {
        let mut mock = MockGapicPublisher::new();
        mock.expect_publish().return_once({
            move |r, _| {
                assert_eq!(r.topic, "my-topic");
                let ids = r
                    .messages
                    .iter()
                    .map(|m| String::from_utf8(m.data.to_vec()).unwrap());
                assert_eq!(ids.len(), 2);
                let ids = ids.collect::<Vec<_>>();
                assert_eq!(ids.clone(), vec!["hello", "world"]);
                Ok(gax::response::Response::from(
                    PublishResponse::new().set_message_ids(ids),
                ))
            }
        });

        let client = GapicPublisher::from_stub(mock);
        let publisher = PublisherBuilder::new(client, "my-topic".to_string())
            // Set a long delay.
            .set_message_count_threshold(1000_u32)
            .set_delay_threshold(Duration::from_secs(60))
            .build();

        let start = tokio::time::Instant::now();
        let messages = vec![
            PubsubMessage::new().set_data("hello".to_string()),
            PubsubMessage::new().set_data("world".to_string()),
        ];
        for msg in messages {
            publisher.publish(msg.clone());
        }

        publisher.flush().await;
        assert_eq!(start.elapsed(), Duration::ZERO);
    }

    #[tokio::test(start_paused = true)]
    async fn test_empty_flush() {
        let mock = MockGapicPublisher::new();

        let client = GapicPublisher::from_stub(mock);
        let publisher = PublisherBuilder::new(client, "my-topic".to_string()).build();

        let start = tokio::time::Instant::now();
        publisher.flush().await;
        assert_eq!(start.elapsed(), Duration::ZERO);
    }

    #[tokio::test]
    async fn test_batching_send_on_message_count_threshold_success() {
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
            .set_message_count_threshold(2_u32)
            .set_byte_threshold(MAX_BYTES)
            .set_delay_threshold(std::time::Duration::MAX)
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
    async fn test_batching_send_on_message_count_threshold_error() {
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
            .set_message_count_threshold(2_u32)
            .set_byte_threshold(MAX_BYTES)
            .set_delay_threshold(std::time::Duration::MAX)
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
    async fn test_batching_send_on_byte_threshold() {
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
        // Ensure that the first message does not pass the threshold.
        let byte_threshold = "my-topic".len() + "hello".len() + 1;
        let publisher = PublisherBuilder::new(client, "my-topic".to_string())
            .set_message_count_threshold(MAX_MESSAGES)
            .set_byte_threshold(byte_threshold as u32)
            .set_delay_threshold(std::time::Duration::MAX)
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

    #[tokio::test(start_paused = true)]
    async fn test_batching_send_on_delay_threshold() {
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
        let delay = std::time::Duration::from_millis(10);
        let publisher = PublisherBuilder::new(client, "my-topic".to_string())
            .set_message_count_threshold(u32::MAX)
            .set_byte_threshold(MAX_BYTES)
            .set_delay_threshold(delay)
            .build();

        // Test that messages send after delay.
        for _ in 0..3 {
            let start = tokio::time::Instant::now();
            let messages = vec![
                PubsubMessage::new().set_data("hello 0".to_string()),
                PubsubMessage::new().set_data("hello 1".to_string()),
                PubsubMessage::new()
                    .set_data("hello 2".to_string())
                    .set_ordering_key("ordering key 1"),
                PubsubMessage::new()
                    .set_data("hello 3".to_string())
                    .set_ordering_key("ordering key 2"),
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
                assert_eq!(
                    start.elapsed(),
                    delay,
                    "batch of messages should have sent after {:?}",
                    delay
                )
            }
        }
    }

    #[tokio::test(start_paused = true)]
    #[allow(clippy::get_first)]
    async fn test_batching_on_ordering_key() {
        // Publish messages with different ordering key and validate that they are in different batches.
        let mut mock = MockGapicPublisher::new();
        mock.expect_publish().returning({
            |r, _| {
                assert_eq!(r.topic, "my-topic");
                assert_eq!(r.messages.len(), 2);
                assert_eq!(
                    r.messages.get(0).unwrap().ordering_key,
                    r.messages.get(1).unwrap().ordering_key
                );
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
        // Use a low message count to trigger batch sends.
        let message_count_threshold = 2_u32;
        let publisher = PublisherBuilder::new(client, "my-topic".to_string())
            .set_message_count_threshold(message_count_threshold)
            .set_byte_threshold(MAX_BYTES)
            .set_delay_threshold(std::time::Duration::MAX)
            .build();

        let num_ordering_keys = 3;
        let mut messages = Vec::new();
        // We want the number of messages to be a multiple of num_ordering_keys
        // and message_count_threshold. Otherwise, the final batch of each
        // ordering key may fail the message len assertion.
        for i in 0..(2 * message_count_threshold * num_ordering_keys) {
            messages.push(
                PubsubMessage::new()
                    .set_data(format!("test message {}", i))
                    .set_ordering_key(format!("ordering key: {}", i % num_ordering_keys)),
            );
        }
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

    #[tokio::test(start_paused = true)]
    #[allow(clippy::get_first)]
    async fn test_batching_empty_ordering_key() {
        // Publish messages with different ordering key and validate that they are in different batches.
        let mut mock = MockGapicPublisher::new();
        mock.expect_publish().returning({
            |r, _| {
                assert_eq!(r.topic, "my-topic");
                assert_eq!(r.messages.len(), 2);
                assert_eq!(
                    r.messages.get(0).unwrap().ordering_key,
                    r.messages.get(1).unwrap().ordering_key
                );
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
        // Use a low message count to trigger batch sends.
        let publisher = PublisherBuilder::new(client, "my-topic".to_string())
            .set_message_count_threshold(2_u32)
            .set_byte_threshold(MAX_BYTES)
            .set_delay_threshold(std::time::Duration::MAX)
            .build();

        let messages = vec![
            PubsubMessage::new().set_data("hello 1".to_string()),
            PubsubMessage::new()
                .set_data("hello 2".to_string())
                .set_ordering_key(""),
            PubsubMessage::new()
                .set_data("hello 3".to_string())
                .set_ordering_key("ordering key :1"),
            PubsubMessage::new()
                .set_data("hello 4".to_string())
                .set_ordering_key("ordering key :1"),
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

    #[tokio::test(start_paused = true)]
    #[allow(clippy::get_first)]
    async fn test_ordering_key_only_one_outstanding_batch() {
        // Verify that Publisher must only have 1 outstanding batch inflight at a time.
        // This is done by validating that the 2 expected publish calls are done in sequence
        // with a sleep delay in the first Publish reply.
        let mut seq = Sequence::new();
        let mut mock = MockGapicPublisherWithFuture::new();
        mock.expect_publish()
            .times(1)
            .in_sequence(&mut seq)
            .returning({
                |r, _| {
                    Box::pin(async move {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                        assert_eq!(r.topic, "my-topic");
                        assert_eq!(r.messages.len(), 1);
                        let ids = r
                            .messages
                            .iter()
                            .map(|m| String::from_utf8(m.data.to_vec()).unwrap());
                        Ok(gax::response::Response::from(
                            PublishResponse::new().set_message_ids(ids),
                        ))
                    })
                }
            });

        mock.expect_publish()
            .times(1)
            .in_sequence(&mut seq)
            .returning({
                |r, _| {
                    Box::pin(async move {
                        assert_eq!(r.topic, "my-topic");
                        assert_eq!(r.messages.len(), 1);
                        let ids = r
                            .messages
                            .iter()
                            .map(|m| String::from_utf8(m.data.to_vec()).unwrap());
                        Ok(gax::response::Response::from(
                            PublishResponse::new().set_message_ids(ids),
                        ))
                    })
                }
            });

        let client = GapicPublisher::from_stub(mock);
        // Use a low message count to trigger batch sends.
        let publisher = PublisherBuilder::new(client, "my-topic".to_string())
            .set_message_count_threshold(1_u32)
            .set_byte_threshold(MAX_BYTES)
            .set_delay_threshold(std::time::Duration::MAX)
            .build();

        let messages = [
            PubsubMessage::new()
                .set_data("hello 1".to_string())
                .set_ordering_key("ordering key"),
            PubsubMessage::new()
                .set_data("hello 2".to_string())
                .set_ordering_key("ordering key"),
        ];

        let start = tokio::time::Instant::now();
        let msg1_handle = publisher.publish(messages.get(0).unwrap().clone());
        let msg2_handle = publisher.publish(messages.get(1).unwrap().clone());
        assert_eq!(msg2_handle.await.expect("expected message id"), "hello 2");
        assert_eq!(
            start.elapsed(),
            Duration::from_millis(10),
            "the second batch of messages should have sent after the first which is has been delayed by {:?}",
            Duration::from_millis(10)
        );
        // Also validate the content of the first publish.
        assert_eq!(msg1_handle.await.expect("expected message id"), "hello 1");
    }

    #[tokio::test(start_paused = true)]
    #[allow(clippy::get_first)]
    async fn test_empty_ordering_key_concurrent_batches() {
        // Verify that for empty ordering key, the Publisher will send multiple batches without
        // awaiting for the results.
        // This is done by adding a delay in the first Publish reply and validating that
        // the second batch does not await for the first batch.
        let mut seq = Sequence::new();
        let mut mock = MockGapicPublisherWithFuture::new();
        mock.expect_publish()
            .times(1)
            .in_sequence(&mut seq)
            .returning({
                |r, _| {
                    Box::pin(async move {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                        assert_eq!(r.topic, "my-topic");
                        assert_eq!(r.messages.len(), 1);
                        let ids = r
                            .messages
                            .iter()
                            .map(|m| String::from_utf8(m.data.to_vec()).unwrap());
                        Ok(gax::response::Response::from(
                            PublishResponse::new().set_message_ids(ids),
                        ))
                    })
                }
            });

        mock.expect_publish()
            .times(1)
            .in_sequence(&mut seq)
            .returning({
                |r, _| {
                    Box::pin(async move {
                        assert_eq!(r.topic, "my-topic");
                        assert_eq!(r.messages.len(), 1);
                        let ids = r
                            .messages
                            .iter()
                            .map(|m| String::from_utf8(m.data.to_vec()).unwrap());
                        Ok(gax::response::Response::from(
                            PublishResponse::new().set_message_ids(ids),
                        ))
                    })
                }
            });

        let client = GapicPublisher::from_stub(mock);
        // Use a low message count to trigger batch sends.
        let publisher = PublisherBuilder::new(client, "my-topic".to_string())
            .set_message_count_threshold(1_u32)
            .set_byte_threshold(MAX_BYTES)
            .set_delay_threshold(std::time::Duration::MAX)
            .build();

        let messages = [
            PubsubMessage::new()
                .set_data("hello 1".to_string())
                .set_ordering_key(""),
            PubsubMessage::new()
                .set_data("hello 2".to_string())
                .set_ordering_key(""),
        ];

        let start = tokio::time::Instant::now();
        let msg1_handle = publisher.publish(messages.get(0).unwrap().clone());
        let msg2_handle = publisher.publish(messages.get(1).unwrap().clone());
        assert_eq!(msg2_handle.await.expect("expected message id"), "hello 2");
        assert_eq!(
            start.elapsed(),
            Duration::from_millis(0),
            "the second batch of messages should have sent without any delay"
        );
        // Also validate the content of the first publish.
        assert_eq!(msg1_handle.await.expect("expected message id"), "hello 1");
    }

    #[tokio::test]
    async fn builder() -> anyhow::Result<()> {
        let client = Client::builder().build().await?;
        let builder = client.publisher("projects/my-project/topics/my-topic".to_string());
        let publisher = builder.set_message_count_threshold(1_u32).build();
        assert_eq!(publisher.batching_options.message_count_threshold, 1_u32);
        Ok(())
    }

    #[tokio::test]
    async fn default_batching() -> anyhow::Result<()> {
        let client = Client::builder().build().await?;
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
    async fn test_ordering_error_pause_publisher() {
        // Verify that a Publish send error will pause the publisher for an ordering key.
        let mut seq = Sequence::new();
        let mut mock = MockGapicPublisher::new();
        mock.expect_publish()
            .times(1)
            .in_sequence(&mut seq)
            .returning({
                |r, _| {
                    assert_eq!(r.topic, "my-topic");
                    assert_eq!(r.messages.len(), 1);
                    Err(gax::error::Error::service(
                        gax::error::rpc::Status::default()
                            .set_code(gax::error::rpc::Code::Unknown)
                            .set_message("unknown error has occurred"),
                    ))
                }
            });

        mock.expect_publish()
            .times(2)
            .in_sequence(&mut seq)
            .returning({
                |r, _| {
                    assert_eq!(r.topic, "my-topic");
                    assert_eq!(r.messages.len(), 1);
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
            .set_message_count_threshold(1_u32)
            .build();

        let msg_0_handle = publisher.publish(
            PubsubMessage::new()
                .set_ordering_key("ordering key with error")
                .set_data("msg 0"),
        );
        // Publish an additional message so that there's an additional pending message in the worker.
        let msg_1_handle = publisher.publish(
            PubsubMessage::new()
                .set_ordering_key("ordering key with error")
                .set_data("msg 1"),
        );

        // Assert the error is caused by the Publish send operation.
        let mut got_err = msg_0_handle.await.unwrap_err();
        // TODO(#3689): Validate the error structure when Publisher error structure is better defined.
        assert!(got_err.is_transport(), "{got_err:?}");

        // Assert that the pending message error is caused by the Publisher being paused.
        got_err = msg_1_handle.await.unwrap_err();
        let source = got_err
            .source()
            .and_then(|e| e.downcast_ref::<crate::error::PublishError>());
        assert!(
            matches!(
                source,
                Some(crate::error::PublishError::OrderingKeyPaused(()))
            ),
            "{got_err:?}"
        );

        // Assert that new publish messages returns an error because the Publisher being paused.
        got_err = publisher
            .publish(
                PubsubMessage::new()
                    .set_ordering_key("ordering key with error")
                    .set_data("msg 2"),
            )
            .await
            .unwrap_err();
        let source = got_err
            .source()
            .and_then(|e| e.downcast_ref::<crate::error::PublishError>());
        assert!(
            matches!(
                source,
                Some(crate::error::PublishError::OrderingKeyPaused(()))
            ),
            "{got_err:?}"
        );

        // Verify that the other ordering keys are not paused.
        let mut got = publisher
            .publish(PubsubMessage::new().set_ordering_key("").set_data("msg 3"))
            .await;
        assert_eq!(got.expect("expected message id"), "msg 3");

        got = publisher
            .publish(
                PubsubMessage::new()
                    .set_ordering_key("ordering key without error")
                    .set_data("msg 4"),
            )
            .await;
        assert_eq!(got.expect("expected message id"), "msg 4");
    }

    #[tokio::test]
    async fn test_ordering_error_pause_then_flush() {
        // Verify that Flush on a paused ordering key returns an error.
        let mut seq = Sequence::new();
        let mut mock = MockGapicPublisher::new();
        mock.expect_publish()
            .times(1)
            .in_sequence(&mut seq)
            .returning({
                |r, _| {
                    assert_eq!(r.topic, "my-topic");
                    assert_eq!(r.messages.len(), 1);
                    Err(gax::error::Error::service(
                        gax::error::rpc::Status::default()
                            .set_code(gax::error::rpc::Code::Unknown)
                            .set_message("unknown error has occurred"),
                    ))
                }
            });

        mock.expect_publish()
            .times(2)
            .in_sequence(&mut seq)
            .returning({
                |r, _| {
                    assert_eq!(r.topic, "my-topic");
                    assert_eq!(r.messages.len(), 1);
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
            .set_message_count_threshold(MAX_MESSAGES)
            .set_byte_threshold(MAX_BYTES)
            .set_delay_threshold(std::time::Duration::from_millis(10))
            .build();

        // Cause "ordering key with error" ordering key to pause.
        publisher.publish(
            PubsubMessage::new()
                .set_ordering_key("ordering key with error")
                .set_data("msg 0"),
        );
        publisher.flush().await;

        // Validate that new Publish on the paused ordering key will result in an error.
        let got_err = publisher
            .publish(
                PubsubMessage::new()
                    .set_ordering_key("ordering key with error")
                    .set_data("msg 1"),
            )
            .await
            .unwrap_err();
        let source = got_err
            .source()
            .and_then(|e| e.downcast_ref::<crate::error::PublishError>());
        assert!(
            matches!(
                source,
                Some(crate::error::PublishError::OrderingKeyPaused(()))
            ),
            "{got_err:?}"
        );

        // Verify that the other ordering keys are not paused.
        let mut got = publisher
            .publish(PubsubMessage::new().set_ordering_key("").set_data("msg 2"))
            .await;
        assert_eq!(got.expect("expected message id"), "msg 2");

        got = publisher
            .publish(
                PubsubMessage::new()
                    .set_ordering_key("ordering key without error")
                    .set_data("msg 3"),
            )
            .await;
        assert_eq!(got.expect("expected message id"), "msg 3");
    }

    #[tokio::test]
    async fn test_builder_clamping() -> anyhow::Result<()> {
        // Test values that are too high and should be clamped.
        let oversized_options = BatchingOptions::new()
            .set_delay_threshold(MAX_DELAY + Duration::from_secs(1))
            .set_message_count_threshold(MAX_MESSAGES + 1)
            .set_byte_threshold(MAX_BYTES + 1);

        let client = Client::builder().build().await?;
        let publisher = client
            .publisher("projects/my-project/topics/my-topic".to_string())
            .set_delay_threshold(oversized_options.delay_threshold)
            .set_message_count_threshold(oversized_options.message_count_threshold)
            .set_byte_threshold(oversized_options.byte_threshold)
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
            .set_delay_threshold(normal_options.delay_threshold)
            .set_message_count_threshold(normal_options.message_count_threshold)
            .set_byte_threshold(normal_options.byte_threshold)
            .build();
        let got = publisher.batching_options;

        assert_eq!(got.delay_threshold, normal_options.delay_threshold);
        assert_eq!(
            got.message_count_threshold,
            normal_options.message_count_threshold
        );
        assert_eq!(got.byte_threshold, normal_options.byte_threshold);

        Ok(())
    }
}
