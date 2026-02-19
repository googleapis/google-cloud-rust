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
use crate::publisher::actor::BundledMessage;
use crate::publisher::actor::ToDispatcher;
use crate::publisher::builder::PublisherBuilder;

use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::oneshot;

/// A Publisher client for the [Cloud Pub/Sub] API.
///
/// A `Publisher` sends messages to a specific topic. It manages message batching
/// and sending in a background task.
///
/// ```
/// # async fn sample() -> anyhow::Result<()> {
/// # use google_cloud_pubsub::*;
/// # use google_cloud_pubsub::client::Publisher;
/// # use model::Message;
/// let publisher = Publisher::builder("projects/my-project/topics/my-topic").build().await?;
/// let message_id_future = publisher.publish(Message::new().set_data("Hello, World"));
/// # Ok(()) }
/// ```
///
/// [cloud pub/sub]: https://docs.cloud.google.com/pubsub/docs/overview
#[derive(Debug, Clone)]
pub struct Publisher {
    #[allow(dead_code)]
    pub(crate) batching_options: BatchingOptions,
    pub(crate) tx: UnboundedSender<ToDispatcher>,
}

impl Publisher {
    /// Returns a builder for [Publisher].
    ///
    /// # Example
    ///
    /// ```
    /// # async fn sample() -> anyhow::Result<()> {
    /// # use google_cloud_pubsub::*;
    /// # use google_cloud_pubsub::client::Publisher;
    /// let publisher = Publisher::builder("projects/my-project/topics/topic").build().await?;
    /// # Ok(()) }
    /// ```
    pub fn builder(topic: impl Into<String>) -> PublisherBuilder {
        PublisherBuilder::new(topic.into())
    }

    /// Publishes a message to the topic.
    ///
    /// When this method encounters a non-recoverable error publishing for an ordering key,
    /// it will pause publishing on all new messages on that ordering key. Any outstanding
    /// messages that have not yet been published may return an error.
    ///
    /// ```
    /// # use google_cloud_pubsub::client::Publisher;
    /// # async fn sample(publisher: Publisher) -> anyhow::Result<()> {
    /// # use google_cloud_pubsub::model::Message;
    /// let message_id = publisher.publish(Message::new().set_data("Hello, World")).await?;
    /// # Ok(()) }
    /// ```
    pub fn publish(&self, msg: crate::model::Message) -> crate::model_ext::PublishFuture {
        let (tx, rx) = tokio::sync::oneshot::channel();

        // If this fails, the Dispatcher is gone, which indicates it has been dropped,
        // possibly due to the background task being stopped by the runtime.
        // The PublishFuture will automatically receive an error when `tx` is dropped.
        if self
            .tx
            .send(ToDispatcher::Publish(BundledMessage { msg, tx }))
            .is_err()
        {
            // `tx` is dropped here if the send errors.
        }
        crate::model_ext::PublishFuture { rx }
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
    /// be available on its corresponding [PublishFuture](crate::model_ext::PublishFuture).
    ///
    /// Messages published after `flush()` is called will be buffered for a
    /// subsequent batch and are not included in this flush operation.
    ///
    /// # Example
    ///
    /// ```
    /// # use google_cloud_pubsub::model::Message;
    /// # async fn sample(publisher: google_cloud_pubsub::client::Publisher) -> anyhow::Result<()> {
    /// // Publish some messages. They will be buffered according to batching options.
    /// let handle1 = publisher.publish(Message::new().set_data("foo"));
    /// let handle2 = publisher.publish(Message::new().set_data("bar"));
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
        if self.tx.send(ToDispatcher::Flush(tx)).is_err() {
            // `tx` is dropped here if the send errors.
        }
        rx.await
            .expect("the client library should not release the sender");
    }

    /// Resume accepting publish for a paused ordering key.
    ///
    /// Publishing using an ordering key might be paused if an error is encountered while publishing, to prevent messages from being published out of order.
    /// If the ordering key is not currently paused, this function is a no-op.
    ///
    /// # Example
    ///
    /// ```
    /// # use google_cloud_pubsub::model::Message;
    /// # async fn sample(publisher: google_cloud_pubsub::client::Publisher) -> anyhow::Result<()> {
    /// if let Err(_) = publisher.publish(Message::new().set_data("foo").set_ordering_key("bar")).await {
    ///     // Error handling code can go here.
    ///     publisher.resume_publish("bar");
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn resume_publish<T: std::convert::Into<std::string::String>>(&self, ordering_key: T) {
        let _ = self
            .tx
            .send(ToDispatcher::ResumePublish(ordering_key.into()));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::publisher::builder::PublisherPartialBuilder;
    use crate::publisher::client::BasePublisher;
    use crate::publisher::constants::*;
    use crate::publisher::options::BatchingOptions;
    use crate::{
        generated::gapic_dataplane::client::Publisher as GapicPublisher,
        model::{Message, PublishResponse},
    };
    use mockall::Sequence;
    use rand::{RngExt, distr::Alphanumeric};
    use std::error::Error;
    use std::time::Duration;

    static TOPIC: &str = "my-topic";

    mockall::mock! {
        #[derive(Debug)]
        GapicPublisher {}
        impl crate::generated::gapic_dataplane::stub::Publisher for GapicPublisher {
            async fn publish(&self, req: crate::model::PublishRequest, _options: crate::RequestOptions) -> crate::Result<crate::Response<crate::model::PublishResponse>>;
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
            fn publish(&self, req: crate::model::PublishRequest, _options: crate::RequestOptions) -> impl Future<Output=crate::Result<crate::Response<crate::model::PublishResponse>>> + Send;
        }
    }

    fn publish_ok(
        req: crate::model::PublishRequest,
        _options: crate::RequestOptions,
    ) -> crate::Result<crate::Response<crate::model::PublishResponse>> {
        let ids = req
            .messages
            .iter()
            .map(|m| String::from_utf8(m.data.to_vec()).unwrap());
        Ok(crate::Response::from(
            PublishResponse::new().set_message_ids(ids),
        ))
    }

    fn publish_err(
        _req: crate::model::PublishRequest,
        _options: crate::RequestOptions,
    ) -> crate::Result<crate::Response<crate::model::PublishResponse>> {
        Err(crate::Error::service(
            google_cloud_gax::error::rpc::Status::default()
                .set_code(google_cloud_gax::error::rpc::Code::Unknown)
                .set_message("unknown error has occurred"),
        ))
    }

    #[track_caller]
    fn assert_publish_err(got_err: crate::error::PublishError) {
        assert!(
            matches!(got_err, crate::error::PublishError::SendError(_)),
            "{got_err:?}"
        );
        let source = got_err
            .source()
            .and_then(|e| e.downcast_ref::<std::sync::Arc<crate::Error>>())
            .expect("send error should contain a source");
        assert!(source.status().is_some(), "{got_err:?}");
        assert_eq!(
            source.status().unwrap().code,
            google_cloud_gax::error::rpc::Code::Unknown,
            "{got_err:?}"
        );
    }

    fn generate_random_data() -> String {
        rand::rng()
            .sample_iter(&Alphanumeric)
            .take(16)
            .map(char::from)
            .collect()
    }

    macro_rules! assert_publishing_is_ok {
        ($publisher:ident, $($ordering_key:expr),+) => {
            $(
                let msg = generate_random_data();
                let got = $publisher
                    .publish(
                        Message::new()
                            .set_ordering_key($ordering_key)
                            .set_data(msg.clone()),
                    )
                    .await;
                assert_eq!(got?, msg);
            )+
        };
    }

    macro_rules! assert_publishing_is_paused {
        ($publisher:ident, $($ordering_key:expr),+) => {
            $(
                let got_err = $publisher
                    .publish(
                        Message::new()
                            .set_ordering_key($ordering_key)
                            .set_data(generate_random_data()),
                    )
                    .await;
                assert!(
                    matches!(got_err, Err(crate::error::PublishError::OrderingKeyPaused(()))),
                    "{got_err:?}"
                );
            )+
        };
    }

    #[tokio::test]
    async fn publisher_publish_successfully() -> anyhow::Result<()> {
        let mut mock = MockGapicPublisher::new();
        mock.expect_publish()
            .times(2)
            .withf(|req, _o| req.topic == TOPIC)
            .returning(publish_ok);

        let client = GapicPublisher::from_stub(mock);
        let publisher = PublisherPartialBuilder::new(client, TOPIC.to_string())
            .set_message_count_threshold(1_u32)
            .build();

        let messages = [
            Message::new().set_data("hello"),
            Message::new().set_data("world"),
        ];
        let mut handles = Vec::new();
        for msg in messages {
            let handle = publisher.publish(msg.clone());
            handles.push((msg, handle));
        }

        for (id, rx) in handles.into_iter() {
            let got = rx.await?;
            let id = String::from_utf8(id.data.to_vec())?;
            assert_eq!(got, id);
        }

        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn worker_handles_forced_shutdown_gracefully() -> anyhow::Result<()> {
        let mock = MockGapicPublisher::new();

        let client = GapicPublisher::from_stub(mock);
        let (publisher, background_task_handle) =
            PublisherPartialBuilder::new(client, TOPIC.to_string())
                .set_message_count_threshold(100_u32)
                .build_return_handle();

        let messages = [
            Message::new().set_data("hello"),
            Message::new().set_data("world"),
        ];
        let mut handles = Vec::new();
        for msg in messages {
            let handle = publisher.publish(msg);
            handles.push(handle);
        }

        background_task_handle.abort();

        for rx in handles.into_iter() {
            rx.await
                .expect_err("expected error when background task canceled");
        }

        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn dropping_publisher_flushes_pending_messages() -> anyhow::Result<()> {
        // If we hold on to the handles returned from the publisher, it should
        // be safe to drop the publisher and .await on the handles.
        let mut mock = MockGapicPublisher::new();
        mock.expect_publish()
            .withf(|req, _o| req.topic == TOPIC)
            .times(2)
            .returning(publish_ok);

        let client = GapicPublisher::from_stub(mock);
        let publisher = PublisherPartialBuilder::new(client, TOPIC.to_string())
            .set_message_count_threshold(1000_u32)
            .set_delay_threshold(Duration::from_secs(60))
            .build();

        let start = tokio::time::Instant::now();
        let messages = [
            Message::new().set_data("hello"),
            Message::new().set_data("world"),
            Message::new().set_data("hello").set_ordering_key("key"),
            Message::new().set_data("world").set_ordering_key("key"),
        ];
        let mut handles = Vec::new();
        for msg in messages {
            let handle = publisher.publish(msg.clone());
            handles.push((msg, handle));
        }
        drop(publisher); // This should trigger the publisher to send all pending messages.

        for (id, rx) in handles.into_iter() {
            let got = rx.await?;
            let id = String::from_utf8(id.data.to_vec())?;
            assert_eq!(got, id);
            assert_eq!(start.elapsed(), Duration::ZERO);
        }

        Ok(())
    }

    #[tokio::test]
    async fn publisher_handles_publish_errors() -> anyhow::Result<()> {
        let mut mock = MockGapicPublisher::new();
        mock.expect_publish()
            .times(2)
            .withf(|req, _o| req.topic == TOPIC)
            .returning(publish_err);

        let client = GapicPublisher::from_stub(mock);
        let publisher = PublisherPartialBuilder::new(client, TOPIC.to_string())
            .set_message_count_threshold(1_u32)
            .build();

        let messages = [
            Message::new().set_data("hello"),
            Message::new().set_data("world"),
        ];

        let mut handles = Vec::new();
        for msg in messages {
            let handle = publisher.publish(msg.clone());
            handles.push(handle);
        }

        for rx in handles.into_iter() {
            let got = rx.await;
            assert!(got.is_err(), "{got:?}");
        }

        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn flush_sends_pending_messages_immediately() -> anyhow::Result<()> {
        let mut mock = MockGapicPublisher::new();
        mock.expect_publish()
            .withf(|req, _o| req.topic == TOPIC)
            .returning(publish_ok);

        let client = GapicPublisher::from_stub(mock);
        let publisher = PublisherPartialBuilder::new(client, TOPIC.to_string())
            // Set a long delay.
            .set_message_count_threshold(1000_u32)
            .set_delay_threshold(Duration::from_secs(60))
            .build();

        let start = tokio::time::Instant::now();
        let messages = [
            Message::new().set_data("hello"),
            Message::new().set_data("world"),
        ];
        let mut handles = Vec::new();
        for msg in messages {
            let handle = publisher.publish(msg.clone());
            handles.push((msg, handle));
        }

        publisher.flush().await;
        assert_eq!(start.elapsed(), Duration::ZERO);

        let post = publisher.publish(Message::new().set_data("after"));
        for (id, rx) in handles.into_iter() {
            let got = rx.await?;
            let id = String::from_utf8(id.data.to_vec())?;
            assert_eq!(got, id);
            assert_eq!(start.elapsed(), Duration::ZERO);
        }

        // Validate that the post message is only sent after the next timeout.
        // I.e., the Publisher does not continuously flush new messages.
        let got = post.await?;
        assert_eq!(got, "after");
        assert_eq!(start.elapsed(), Duration::from_secs(60));

        Ok(())
    }

    #[cfg_attr(
        tokio_unstable,
        tokio::test(
            start_paused = true,
            flavor = "current_thread",
            unhandled_panic = "shutdown_runtime"
        )
    )]
    #[cfg_attr(not(tokio_unstable), tokio::test(start_paused = true))]
    // User's should be able to drop handles and the messages will still send.
    async fn dropping_handles_does_not_prevent_publishing() -> anyhow::Result<()> {
        let mut mock = MockGapicPublisher::new();
        mock.expect_publish()
            .withf(|r, _| {
                r.messages.len() == 2
                    && r.messages[0].data == "hello"
                    && r.messages[1].data == "world"
            })
            .return_once(publish_ok);

        let client = GapicPublisher::from_stub(mock);
        let publisher = PublisherPartialBuilder::new(client, TOPIC.to_string())
            // Set a long delay.
            .set_message_count_threshold(1000_u32)
            .set_delay_threshold(Duration::from_secs(60))
            .build();

        let start = tokio::time::Instant::now();
        let messages = [
            Message::new().set_data("hello"),
            Message::new().set_data("world"),
        ];
        for msg in messages {
            publisher.publish(msg.clone());
        }

        publisher.flush().await;
        assert_eq!(start.elapsed(), Duration::ZERO);

        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn flush_with_no_messages_is_noop() -> anyhow::Result<()> {
        let mock = MockGapicPublisher::new();

        let client = GapicPublisher::from_stub(mock);
        let publisher = PublisherPartialBuilder::new(client, TOPIC.to_string()).build();

        let start = tokio::time::Instant::now();
        publisher.flush().await;
        assert_eq!(start.elapsed(), Duration::ZERO);

        Ok(())
    }

    #[tokio::test]
    async fn batch_sends_on_message_count_threshold_success() -> anyhow::Result<()> {
        // Make sure all messages in a batch receive the correct message ID.
        let mut mock = MockGapicPublisher::new();
        mock.expect_publish()
            .withf(|r, _| r.messages.len() == 2)
            .return_once(publish_ok);

        let client = GapicPublisher::from_stub(mock);
        let publisher = PublisherPartialBuilder::new(client, TOPIC.to_string())
            .set_message_count_threshold(2_u32)
            .set_byte_threshold(MAX_BYTES)
            .set_delay_threshold(std::time::Duration::MAX)
            .build();

        let messages = [
            Message::new().set_data("hello"),
            Message::new().set_data("world"),
        ];
        let mut handles = Vec::new();
        for msg in messages {
            let handle = publisher.publish(msg.clone());
            handles.push((msg, handle));
        }

        for (id, rx) in handles.into_iter() {
            let got = rx.await?;
            let id = String::from_utf8(id.data.to_vec())?;
            assert_eq!(got, id);
        }

        Ok(())
    }

    #[tokio::test]
    async fn batch_sends_on_message_count_threshold_error() -> anyhow::Result<()> {
        // Make sure all messages in a batch receive an error.
        let mut mock = MockGapicPublisher::new();
        mock.expect_publish()
            .withf(|r, _| r.messages.len() == 2)
            .return_once(publish_err);

        let client = GapicPublisher::from_stub(mock);
        let publisher = PublisherPartialBuilder::new(client, TOPIC.to_string())
            .set_message_count_threshold(2_u32)
            .set_byte_threshold(MAX_BYTES)
            .set_delay_threshold(std::time::Duration::MAX)
            .build();

        let messages = [
            Message::new().set_data("hello"),
            Message::new().set_data("world"),
        ];
        let mut handles = Vec::new();
        for msg in messages {
            let handle = publisher.publish(msg.clone());
            handles.push(handle);
        }

        for rx in handles.into_iter() {
            let got = rx.await;
            assert!(got.is_err(), "{got:?}");
        }

        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn batch_sends_on_byte_threshold() -> anyhow::Result<()> {
        // Make sure all messages in a batch receive the correct message ID.
        let mut mock = MockGapicPublisher::new();
        mock.expect_publish()
            .withf(|r, _| r.messages.len() == 1)
            .times(2)
            .returning(publish_ok);

        let client = GapicPublisher::from_stub(mock);
        // Ensure that the first message does not pass the threshold.
        let byte_threshold: usize = TOPIC.len() + "hello".len() + "key".len() + 1;
        let publisher = PublisherPartialBuilder::new(client, TOPIC.to_string())
            .set_message_count_threshold(MAX_MESSAGES)
            .set_byte_threshold(byte_threshold as u32)
            .set_delay_threshold(std::time::Duration::MAX)
            .build();

        // Validate without ordering key.
        let handle = publisher.publish(Message::new().set_data("hello"));
        // Publish a second message to trigger send on threshold.
        publisher.publish(Message::new().set_data("world"));
        assert_eq!(handle.await?, "hello");

        // Validate with ordering key.
        let handle = publisher.publish(Message::new().set_data("hello").set_ordering_key("key"));
        // Publish a second message to trigger send on threshold.
        publisher.publish(Message::new().set_data("world").set_ordering_key("key"));
        assert_eq!(handle.await?, "hello");

        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn batch_sends_on_delay_threshold() -> anyhow::Result<()> {
        let mut mock = MockGapicPublisher::new();
        mock.expect_publish()
            .withf(|req, _| req.topic == TOPIC)
            .returning(publish_ok);

        let client = GapicPublisher::from_stub(mock);
        let delay = std::time::Duration::from_millis(10);
        let publisher = PublisherPartialBuilder::new(client, TOPIC.to_string())
            .set_message_count_threshold(u32::MAX)
            .set_byte_threshold(MAX_BYTES)
            .set_delay_threshold(delay)
            .build();

        // Test that messages send after delay.
        for _ in 0..3 {
            let start = tokio::time::Instant::now();
            let messages = [
                Message::new().set_data("hello 0"),
                Message::new().set_data("hello 1"),
                Message::new()
                    .set_data("hello 2")
                    .set_ordering_key("ordering key 1"),
                Message::new()
                    .set_data("hello 3")
                    .set_ordering_key("ordering key 2"),
            ];
            let mut handles = Vec::new();
            for msg in messages {
                let handle = publisher.publish(msg.clone());
                handles.push((msg, handle));
            }

            for (id, rx) in handles.into_iter() {
                let got = rx.await?;
                let id = String::from_utf8(id.data.to_vec())?;
                assert_eq!(got, id);
                assert_eq!(
                    start.elapsed(),
                    delay,
                    "batch of messages should have sent after {:?}",
                    delay
                )
            }
        }

        Ok(())
    }

    #[tokio::test(start_paused = true)]
    #[allow(clippy::get_first)]
    async fn batching_separates_by_ordering_key() -> anyhow::Result<()> {
        // Publish messages with different ordering key and validate that they are in different batches.
        let mut mock = MockGapicPublisher::new();
        mock.expect_publish()
            .withf(|r, _| {
                r.messages.len() == 2 && r.messages[0].ordering_key == r.messages[1].ordering_key
            })
            .returning(publish_ok);

        let client = GapicPublisher::from_stub(mock);
        // Use a low message count to trigger batch sends.
        let message_count_threshold = 2_u32;
        let publisher = PublisherPartialBuilder::new(client, TOPIC.to_string())
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
                Message::new()
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
            let got = rx.await?;
            let id = String::from_utf8(id.data.to_vec())?;
            assert_eq!(got, id);
        }

        Ok(())
    }

    #[tokio::test(start_paused = true)]
    #[allow(clippy::get_first)]
    async fn batching_handles_empty_ordering_key() -> anyhow::Result<()> {
        // Publish messages with different ordering key and validate that they are in different batches.
        let mut mock = MockGapicPublisher::new();
        mock.expect_publish()
            .withf(|r, _| {
                r.messages.len() == 2 && r.messages[0].ordering_key == r.messages[1].ordering_key
            })
            .returning(publish_ok);

        let client = GapicPublisher::from_stub(mock);
        // Use a low message count to trigger batch sends.
        let publisher = PublisherPartialBuilder::new(client, TOPIC.to_string())
            .set_message_count_threshold(2_u32)
            .set_byte_threshold(MAX_BYTES)
            .set_delay_threshold(std::time::Duration::MAX)
            .build();

        let messages = [
            Message::new().set_data("hello 1"),
            Message::new().set_data("hello 2").set_ordering_key(""),
            Message::new()
                .set_data("hello 3")
                .set_ordering_key("ordering key :1"),
            Message::new()
                .set_data("hello 4")
                .set_ordering_key("ordering key :1"),
        ];

        let mut handles = Vec::new();
        for msg in messages {
            let handle = publisher.publish(msg.clone());
            handles.push((msg, handle));
        }

        for (id, rx) in handles.into_iter() {
            let got = rx.await?;
            let id = String::from_utf8(id.data.to_vec())?;
            assert_eq!(got, id);
        }

        Ok(())
    }

    #[tokio::test(start_paused = true)]
    #[allow(clippy::get_first)]
    async fn ordering_key_limits_to_one_outstanding_batch() -> anyhow::Result<()> {
        // Verify that Publisher must only have 1 outstanding batch inflight at a time.
        // This is done by validating that the 2 expected publish calls are done in sequence
        // with a sleep delay in the first Publish reply.
        let mut seq = Sequence::new();
        let mut mock = MockGapicPublisherWithFuture::new();
        mock.expect_publish()
            .times(1)
            .in_sequence(&mut seq)
            .withf(|r, _| r.messages.len() == 1)
            .returning({
                |r, o| {
                    Box::pin(async move {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                        publish_ok(r, o)
                    })
                }
            });

        mock.expect_publish()
            .times(1)
            .in_sequence(&mut seq)
            .withf(|r, _| r.messages.len() == 1)
            .returning(|r, o| Box::pin(async move { publish_ok(r, o) }));

        let client = GapicPublisher::from_stub(mock);
        // Use a low message count to trigger batch sends.
        let publisher = PublisherPartialBuilder::new(client, TOPIC.to_string())
            .set_message_count_threshold(1_u32)
            .set_byte_threshold(MAX_BYTES)
            .set_delay_threshold(std::time::Duration::MAX)
            .build();

        let messages = [
            Message::new()
                .set_data("hello 1")
                .set_ordering_key("ordering key"),
            Message::new()
                .set_data("hello 2")
                .set_ordering_key("ordering key"),
        ];

        let start = tokio::time::Instant::now();
        let msg1_handle = publisher.publish(messages.get(0).unwrap().clone());
        let msg2_handle = publisher.publish(messages.get(1).unwrap().clone());
        assert_eq!(msg2_handle.await?, "hello 2");
        assert_eq!(
            start.elapsed(),
            Duration::from_millis(10),
            "the second batch of messages should have sent after the first which is has been delayed by {:?}",
            Duration::from_millis(10)
        );
        // Also validate the content of the first publish.
        assert_eq!(msg1_handle.await?, "hello 1");

        Ok(())
    }

    #[tokio::test(start_paused = true)]
    #[allow(clippy::get_first)]
    async fn empty_ordering_key_allows_concurrent_batches() -> anyhow::Result<()> {
        // Verify that for empty ordering key, the Publisher will send multiple batches without
        // awaiting for the results.
        // This is done by adding a delay in the first Publish reply and validating that
        // the second batch does not await for the first batch.
        let mut seq = Sequence::new();
        let mut mock = MockGapicPublisherWithFuture::new();
        mock.expect_publish()
            .times(1)
            .in_sequence(&mut seq)
            .withf(|r, _| r.messages.len() == 1)
            .returning(|r, o| {
                Box::pin(async move {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    publish_ok(r, o)
                })
            });

        mock.expect_publish()
            .times(1)
            .in_sequence(&mut seq)
            .withf(|r, _| r.topic == TOPIC && r.messages.len() == 1)
            .returning(|r, o| Box::pin(async move { publish_ok(r, o) }));

        let client = GapicPublisher::from_stub(mock);
        // Use a low message count to trigger batch sends.
        let publisher = PublisherPartialBuilder::new(client, TOPIC.to_string())
            .set_message_count_threshold(1_u32)
            .set_byte_threshold(MAX_BYTES)
            .set_delay_threshold(std::time::Duration::MAX)
            .build();

        let messages = [
            Message::new().set_data("hello 1").set_ordering_key(""),
            Message::new().set_data("hello 2").set_ordering_key(""),
        ];

        let start = tokio::time::Instant::now();
        let msg1_handle = publisher.publish(messages.get(0).unwrap().clone());
        let msg2_handle = publisher.publish(messages.get(1).unwrap().clone());
        assert_eq!(msg2_handle.await?, "hello 2");
        assert_eq!(
            start.elapsed(),
            Duration::from_millis(0),
            "the second batch of messages should have sent without any delay"
        );
        // Also validate the content of the first publish.
        assert_eq!(msg1_handle.await?, "hello 1");

        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn ordering_key_error_pauses_publisher() -> anyhow::Result<()> {
        // Verify that a Publish send error will pause the publisher for an ordering key.
        let mut seq = Sequence::new();
        let mut mock = MockGapicPublisher::new();
        mock.expect_publish()
            .withf(|req, _o| req.topic == TOPIC)
            .times(1)
            .in_sequence(&mut seq)
            .returning(publish_err);

        mock.expect_publish()
            .withf(|req, _o| req.topic == TOPIC)
            .times(2)
            .in_sequence(&mut seq)
            .returning(publish_ok);

        let client = GapicPublisher::from_stub(mock);
        let publisher = PublisherPartialBuilder::new(client, TOPIC.to_string())
            .set_message_count_threshold(1_u32)
            .build();

        let key = "ordering_key";
        let msg_0_handle =
            publisher.publish(Message::new().set_ordering_key(key).set_data("msg 0"));
        // Publish an additional message so that there are pending messages.
        let msg_1_handle =
            publisher.publish(Message::new().set_ordering_key(key).set_data("msg 1"));

        // Assert the error is caused by the Publish send operation.
        let mut got_err = msg_0_handle.await.unwrap_err();
        assert_publish_err(got_err);

        // Assert that the pending message error is caused by the Publisher being paused.
        got_err = msg_1_handle.await.unwrap_err();
        assert!(
            matches!(got_err, crate::error::PublishError::OrderingKeyPaused(())),
            "{got_err:?}"
        );

        // Assert that new publish messages return errors because the Publisher is paused.
        for _ in 0..3 {
            assert_publishing_is_paused!(publisher, key);
        }

        // Verify that the other ordering keys are not paused.
        assert_publishing_is_ok!(publisher, "", "without_error");

        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn batch_error_pauses_ordering_key() -> anyhow::Result<()> {
        // Verify that all messages in the same batch receives the Send error for that batch.
        let mut mock = MockGapicPublisher::new();
        mock.expect_publish()
            .times(1)
            .withf(|r, _| r.topic == TOPIC && r.messages.len() == 2)
            .returning(publish_err);

        let client = GapicPublisher::from_stub(mock);
        let publisher = PublisherPartialBuilder::new(client, TOPIC.to_string())
            .set_message_count_threshold(2_u32)
            .build();

        let key = "ordering_key";
        // Publish 2 messages so they are in the same batch.
        let msg_0_handle =
            publisher.publish(Message::new().set_ordering_key(key).set_data("msg 0"));
        let msg_1_handle =
            publisher.publish(Message::new().set_ordering_key(key).set_data("msg 1"));

        // Validate that they both receives the Send error.
        let mut got_err = msg_0_handle.await.unwrap_err();
        assert_publish_err(got_err);
        got_err = msg_1_handle.await.unwrap_err();
        assert_publish_err(got_err);

        // Assert that new publish messages returns an error because the Publisher is paused.
        assert_publishing_is_paused!(publisher, key);

        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn flush_on_paused_ordering_key_returns_error() -> anyhow::Result<()> {
        // Verify that Flush on a paused ordering key returns an error.
        let mut seq = Sequence::new();
        let mut mock = MockGapicPublisher::new();
        mock.expect_publish()
            .withf(|req, _o| req.topic == TOPIC)
            .times(1)
            .in_sequence(&mut seq)
            .returning(publish_err);

        mock.expect_publish()
            .withf(|req, _o| req.topic == TOPIC)
            .times(2)
            .in_sequence(&mut seq)
            .returning(publish_ok);

        let client = GapicPublisher::from_stub(mock);
        let publisher = PublisherPartialBuilder::new(client, TOPIC.to_string()).build();

        let key = "ordering_key";
        // Cause an ordering key to be paused.
        let handle = publisher.publish(Message::new().set_ordering_key(key).set_data("msg 0"));
        publisher.flush().await;
        // Assert the error is caused by the Publish send operation.
        let got_err = handle.await.unwrap_err();
        assert_publish_err(got_err);

        // Validate that new Publish on the paused ordering key will result in an error.
        assert_publishing_is_paused!(publisher, key);

        // Verify that the other ordering keys are not paused.
        assert_publishing_is_ok!(publisher, "", "without_error");

        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn resuming_non_paused_ordering_key_is_noop() -> anyhow::Result<()> {
        let mut mock = MockGapicPublisher::new();
        mock.expect_publish()
            .withf(|req, _o| req.topic == TOPIC)
            .times(4)
            .returning(publish_ok);

        let client = GapicPublisher::from_stub(mock);
        let publisher = PublisherPartialBuilder::new(client, TOPIC.to_string()).build();

        // Test resume and publish for empty ordering key.
        publisher.resume_publish("");
        assert_publishing_is_ok!(publisher, "");

        // Test resume and publish after the BatchActor has been created for the empty ordering key.
        publisher.resume_publish("");
        assert_publishing_is_ok!(publisher, "");

        // Test resume and publish before the BatchActor has been created.
        let key = "without_error";
        publisher.resume_publish(key);
        assert_publishing_is_ok!(publisher, key);

        // Test resume and publish after the BatchActor has been created.
        publisher.resume_publish(key);
        assert_publishing_is_ok!(publisher, key);

        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn resuming_paused_ordering_key_allows_publishing() -> anyhow::Result<()> {
        let mut seq = Sequence::new();
        let mut mock = MockGapicPublisher::new();
        mock.expect_publish()
            .withf(|req, _o| req.topic == TOPIC)
            .times(1)
            .in_sequence(&mut seq)
            .returning(publish_err);

        mock.expect_publish()
            .withf(|req, _o| req.topic == TOPIC)
            .times(3)
            .in_sequence(&mut seq)
            .returning(publish_ok);

        let client = GapicPublisher::from_stub(mock);
        let publisher = PublisherPartialBuilder::new(client, TOPIC.to_string()).build();

        let key = "ordering_key";
        // Cause an ordering key to be paused.
        let handle = publisher.publish(Message::new().set_ordering_key(key).set_data("msg 0"));
        // Assert the error is caused by the Publish send operation.
        let got_err = handle.await.unwrap_err();
        assert_publish_err(got_err);

        // Validate that new Publish on the paused ordering key will result in an error.
        assert_publishing_is_paused!(publisher, key);

        // Resume and validate the key is no longer paused.
        publisher.resume_publish(key);
        assert_publishing_is_ok!(publisher, key);

        // Verify that the other ordering keys continue to work as expected.
        assert_publishing_is_ok!(publisher, "", "without_error");

        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn resuming_ordering_key_twice_is_safe() -> anyhow::Result<()> {
        // Validate that resuming twice sequentially does not have bad side effects.
        let mut seq = Sequence::new();
        let mut mock = MockGapicPublisher::new();
        mock.expect_publish()
            .withf(|req, _o| req.topic == TOPIC)
            .in_sequence(&mut seq)
            .times(1)
            .returning(publish_err);

        mock.expect_publish()
            .withf(|req, _o| req.topic == TOPIC)
            .in_sequence(&mut seq)
            .return_once(publish_ok);

        let client = GapicPublisher::from_stub(mock);
        let publisher = PublisherPartialBuilder::new(client, TOPIC.to_string()).build();

        let key = "ordering_key";
        // Cause an ordering key to be paused.
        let handle = publisher.publish(Message::new().set_ordering_key(key).set_data("msg 0"));
        publisher.flush().await;
        // Assert the error is caused by the Publish send operation.
        let got_err = handle.await.unwrap_err();
        assert_publish_err(got_err);

        // Validate that new Publish on the paused ordering key will result in an error.
        assert_publishing_is_paused!(publisher, key);

        // Resume twice on the paused ordering key.
        publisher.resume_publish(key);
        publisher.resume_publish(key);
        assert_publishing_is_ok!(publisher, key);

        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn resuming_one_ordering_key_does_not_resume_others() -> anyhow::Result<()> {
        // Validate that resume_publish only resumes the paused ordering key .
        let mut seq = Sequence::new();
        let mut mock = MockGapicPublisher::new();
        mock.expect_publish()
            .withf(|req, _o| req.topic == TOPIC)
            .times(2)
            .in_sequence(&mut seq)
            .returning(publish_err);

        mock.expect_publish()
            .withf(|req, _o| req.topic == TOPIC)
            .times(1)
            .in_sequence(&mut seq)
            .returning(publish_ok);

        let client = GapicPublisher::from_stub(mock);
        let publisher = PublisherPartialBuilder::new(client, TOPIC.to_string()).build();

        let key_0 = "ordering_key_0";
        let key_1 = "ordering_key_1";
        // Cause both ordering keys to pause.
        let handle_0 = publisher.publish(Message::new().set_ordering_key(key_0).set_data("msg 0"));
        let handle_1 = publisher.publish(Message::new().set_ordering_key(key_1).set_data("msg 1"));
        publisher.flush().await;
        let mut got_err = handle_0.await.unwrap_err();
        assert_publish_err(got_err);
        got_err = handle_1.await.unwrap_err();
        assert_publish_err(got_err);

        // Assert that both ordering keys are paused.
        assert_publishing_is_paused!(publisher, key_0, key_1);

        // Resume on one of the ordering key.
        publisher.resume_publish(key_0);

        // Validate that only the correct ordering key is resumed.
        assert_publishing_is_ok!(publisher, key_0);

        // Validate the other ordering key is still paused.
        assert_publishing_is_paused!(publisher, key_1);

        Ok(())
    }

    #[tokio::test]
    async fn publisher_builder_clamps_batching_options() -> anyhow::Result<()> {
        // Test values that are too high and should be clamped.
        let oversized_options = BatchingOptions::new()
            .set_delay_threshold(MAX_DELAY + Duration::from_secs(1))
            .set_message_count_threshold(MAX_MESSAGES + 1)
            .set_byte_threshold(MAX_BYTES + 1);

        let publishers = vec![
            BasePublisher::builder()
                .build()
                .await?
                .publisher("projects/my-project/topics/my-topic")
                .set_delay_threshold(oversized_options.delay_threshold)
                .set_message_count_threshold(oversized_options.message_count_threshold)
                .set_byte_threshold(oversized_options.byte_threshold)
                .build(),
            Publisher::builder("projects/my-project/topics/my-topic".to_string())
                .set_delay_threshold(oversized_options.delay_threshold)
                .set_message_count_threshold(oversized_options.message_count_threshold)
                .set_byte_threshold(oversized_options.byte_threshold)
                .build()
                .await?,
        ];

        for publisher in publishers {
            let got = publisher.batching_options;
            assert_eq!(got.delay_threshold, MAX_DELAY);
            assert_eq!(got.message_count_threshold, MAX_MESSAGES);
            assert_eq!(got.byte_threshold, MAX_BYTES);
        }

        // Test values that are within limits and should not be changed.
        let normal_options = BatchingOptions::new()
            .set_delay_threshold(Duration::from_secs(10))
            .set_message_count_threshold(10_u32)
            .set_byte_threshold(100_u32);

        let publishers = vec![
            BasePublisher::builder()
                .build()
                .await?
                .publisher("projects/my-project/topics/my-topic")
                .set_delay_threshold(normal_options.delay_threshold)
                .set_message_count_threshold(normal_options.message_count_threshold)
                .set_byte_threshold(normal_options.byte_threshold)
                .build(),
            Publisher::builder("projects/my-project/topics/my-topic".to_string())
                .set_delay_threshold(normal_options.delay_threshold)
                .set_message_count_threshold(normal_options.message_count_threshold)
                .set_byte_threshold(normal_options.byte_threshold)
                .build()
                .await?,
        ];

        for publisher in publishers {
            let got = publisher.batching_options;

            assert_eq!(got.delay_threshold, normal_options.delay_threshold);
            assert_eq!(
                got.message_count_threshold,
                normal_options.message_count_threshold
            );
            assert_eq!(got.byte_threshold, normal_options.byte_threshold);
        }
        Ok(())
    }
}
