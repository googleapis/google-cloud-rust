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
use crate::publisher::base_publisher::{BasePublisher, BasePublisherBuilder};
use crate::publisher::worker::BundledMessage;
use crate::publisher::worker::ToWorker;
use crate::publisher::worker::Worker;
use gax::backoff_policy::BackoffPolicyArg;
use gax::retry_policy::RetryPolicyArg;
use gax::retry_throttler::RetryThrottlerArg;
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
/// ```
/// # async fn sample() -> anyhow::Result<()> {
/// # use google_cloud_pubsub::*;
/// # use google_cloud_pubsub::client::Publisher;
/// # use model::PubsubMessage;
/// let publisher = Publisher::builder("projects/my-project/topics/my-topic").build().await?;
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
    /// let handle1 = publisher.publish(PubsubMessage::new().set_data("foo"));
    /// let handle2 = publisher.publish(PubsubMessage::new().set_data("bar"));
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

    /// Resume accepting publish for a paused ordering key.
    ///
    /// Publishing using an ordering key might be paused if an error is encountered while publishing, to prevent messages from being published out of order.
    /// If the ordering key is not currently paused, this function is a no-op.
    ///
    /// # Example
    ///
    /// ```
    /// # use google_cloud_pubsub::model::PubsubMessage;
    /// # async fn sample(publisher: google_cloud_pubsub::client::Publisher) -> anyhow::Result<()> {
    /// if let Err(_) = publisher.publish(PubsubMessage::new().set_data("foo").set_ordering_key("bar")).await {
    ///     // Error handling code can go here.
    ///     publisher.resume_publish("bar");
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn resume_publish<T: std::convert::Into<std::string::String>>(&self, ordering_key: T) {
        let _ = self.tx.send(ToWorker::ResumePublish(ordering_key.into()));
    }
}

/// A builder for a `Publisher`.
#[derive(Clone, Debug)]
pub struct PublisherBuilder {
    topic: String,
    batching_options: BatchingOptions,
    base_builder: BasePublisherBuilder,
}

impl PublisherBuilder {
    pub(crate) fn new(topic: String) -> Self {
        Self {
            topic,
            batching_options: BatchingOptions::default(),
            base_builder: BasePublisher::builder(),
        }
    }

    /// Creates a new [`Publisher`] from the builder's configuration.
    pub async fn build(self) -> Result<Publisher, gax::client_builder::Error> {
        let base_publisher = self.base_builder.build().await?;
        let publisher = base_publisher
            .publisher(&self.topic)
            .set_message_count_threshold(self.batching_options.message_count_threshold)
            .set_byte_threshold(self.batching_options.byte_threshold)
            .set_delay_threshold(self.batching_options.delay_threshold)
            .build();
        Ok(publisher)
    }

    /// Sets the maximum number of messages to be batched together for a single `Publish` call.
    /// When this number is reached, the batch is sent.
    ///
    /// Setting this to `1` disables batching by message count.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_pubsub::client::Publisher;
    /// # async fn sample() -> anyhow::Result<()> {
    /// let publisher = Publisher::builder("projects/my-project/topics/my-topic")
    ///     .set_message_count_threshold(100)
    ///     .build().await?;
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
    /// # use google_cloud_pubsub::client::Publisher;
    /// # async fn sample() -> anyhow::Result<()> {
    /// let publisher = Publisher::builder("projects/my-project/topics/my-topic")
    ///     .set_byte_threshold(100)
    ///     .build().await?;
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
    /// # use google_cloud_pubsub::client::Publisher;
    /// # use std::time::Duration;
    /// # async fn sample() -> anyhow::Result<()> {
    /// let publisher = Publisher::builder("projects/my-project/topics/my-topic")
    ///     .set_delay_threshold(Duration::from_millis(50))
    ///     .build().await?;
    /// # Ok(()) }
    /// ```
    pub fn set_delay_threshold(mut self, threshold: Duration) -> PublisherBuilder {
        self.batching_options = self.batching_options.set_delay_threshold(threshold);
        self
    }

    /// Sets the endpoint.
    ///
    /// ```
    /// # use google_cloud_pubsub::client::Publisher;
    /// # async fn sample() -> anyhow::Result<()> {
    /// let client = Publisher::builder("projects/my-project/topics/my-topic")
    ///     .with_endpoint("http://private.googleapis.com")
    ///     .build().await?;
    /// # Ok(()) }
    /// ```
    pub fn with_endpoint<V: Into<String>>(mut self, v: V) -> Self {
        self.base_builder = self.base_builder.with_endpoint(v);
        self
    }

    /// Enables tracing.
    ///
    /// The client libraries can be dynamically instrumented with the Tokio
    /// [tracing] framework. Setting this flag enables this instrumentation.
    ///
    /// ```
    /// # use google_cloud_pubsub::client::Publisher;
    /// # async fn sample() -> anyhow::Result<()> {
    /// let client = Publisher::builder("projects/my-project/topics/my-topic")
    ///     .with_tracing()
    ///     .build().await?;
    /// # Ok(()) }
    /// ```
    ///
    /// [tracing]: https://docs.rs/tracing/latest/tracing/
    pub fn with_tracing(mut self) -> Self {
        self.base_builder = self.base_builder.with_tracing();
        self
    }

    /// Configure the authentication credentials.
    ///
    /// Most Google Cloud services require authentication, though some services
    /// allow for anonymous access, and some services provide emulators where
    /// no authentication is required. More information about valid credentials
    /// types can be found in the [google-cloud-auth] crate documentation.
    ///
    /// ```
    /// # use google_cloud_pubsub::client::Publisher;
    /// # async fn sample() -> anyhow::Result<()> {
    /// use auth::credentials::mds;
    /// let client = Publisher::builder("projects/my-project/topics/my-topic")
    ///     .with_credentials(
    ///         mds::Builder::default()
    ///             .with_scopes(["https://www.googleapis.com/auth/cloud-platform.read-only"])
    ///             .build()?)
    ///     .build().await?;
    /// # Ok(()) }
    /// ```
    ///
    /// [google-cloud-auth]: https://docs.rs/google-cloud-auth
    pub fn with_credentials<T: Into<gaxi::options::Credentials>>(mut self, v: T) -> Self {
        self.base_builder = self.base_builder.with_credentials(v);
        self
    }

    /// Configure the retry policy.
    ///
    /// The client libraries can automatically retry operations that fail. The
    /// retry policy controls what errors are considered retryable, sets limits
    /// on the number of attempts or the time trying to make attempts.
    ///
    /// ```
    /// # use google_cloud_pubsub::client::Publisher;
    /// # async fn sample() -> anyhow::Result<()> {
    /// use gax::retry_policy::{AlwaysRetry, RetryPolicyExt};
    /// let client = Publisher::builder("projects/my-project/topics/my-topic")
    ///     .with_retry_policy(AlwaysRetry.with_attempt_limit(3))
    ///     .build().await?;
    /// # Ok(()) };
    /// ```
    pub fn with_retry_policy<V: Into<RetryPolicyArg>>(mut self, v: V) -> Self {
        self.base_builder = self.base_builder.with_retry_policy(v);
        self
    }

    /// Configure the retry backoff policy.
    ///
    /// The client libraries can automatically retry operations that fail. The
    /// backoff policy controls how long to wait in between retry attempts.
    ///
    /// ```
    /// # use google_cloud_pubsub::client::Publisher;
    /// # async fn sample() -> anyhow::Result<()> {
    /// use gax::exponential_backoff::ExponentialBackoff;
    /// use std::time::Duration;
    /// let policy = ExponentialBackoff::default();
    /// let client = Publisher::builder("projects/my-project/topics/my-topic")
    ///     .with_backoff_policy(policy)
    ///     .build()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    pub fn with_backoff_policy<V: Into<BackoffPolicyArg>>(mut self, v: V) -> Self {
        self.base_builder = self.base_builder.with_backoff_policy(v);
        self
    }

    /// Configure the retry throttler.
    ///
    /// Advanced applications may want to configure a retry throttler to
    /// [Address Cascading Failures] and when [Handling Overload] conditions.
    /// The client libraries throttle their retry loop, using a policy to
    /// control the throttling algorithm. Use this method to fine tune or
    /// customize the default retry throtler.
    ///
    /// [Handling Overload]: https://sre.google/sre-book/handling-overload/
    /// [Address Cascading Failures]: https://sre.google/sre-book/addressing-cascading-failures/
    ///
    /// ```
    /// # use google_cloud_pubsub::client::Publisher;
    /// # async fn sample() -> anyhow::Result<()> {
    /// use gax::retry_throttler::AdaptiveThrottler;
    /// let client = Publisher::builder("projects/my-project/topics/my-topic")
    ///     .with_retry_throttler(AdaptiveThrottler::default())
    ///     .build().await?;
    /// # Ok(()) };
    /// ```
    pub fn with_retry_throttler<V: Into<RetryThrottlerArg>>(mut self, v: V) -> Self {
        self.base_builder = self.base_builder.with_retry_throttler(v);
        self
    }
}

/// Creates `Publisher`s with a preconfigured client.
///
/// # Example
///
/// ```
/// # async fn sample() -> anyhow::Result<()> {
/// # use google_cloud_pubsub::*;
/// # use google_cloud_pubsub::client::BasePublisher;
/// let client = BasePublisher::builder().build().await?;
/// let publisher = client.publisher("projects/my-project/topics/topic").build();
/// # Ok(()) }
/// ```
#[derive(Clone, Debug)]
pub struct PublisherPartialBuilder {
    pub(crate) inner: GapicPublisher,
    topic: String,
    batching_options: BatchingOptions,
}

impl PublisherPartialBuilder {
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
    /// # use google_cloud_pubsub::client::BasePublisher;
    /// # async fn sample() -> anyhow::Result<()> {
    /// # let client = BasePublisher::builder().build().await?;
    /// let publisher = client.publisher("projects/my-project/topics/my-topic")
    ///     .set_message_count_threshold(100)
    ///     .build();
    /// # Ok(()) }
    /// ```
    pub fn set_message_count_threshold(mut self, threshold: u32) -> PublisherPartialBuilder {
        self.batching_options = self.batching_options.set_message_count_threshold(threshold);
        self
    }

    /// Sets the byte threshold for batching in a single `Publish` call.
    /// When this many bytes are accumulated, the batch is sent.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_pubsub::client::BasePublisher;
    /// # async fn sample() -> anyhow::Result<()> {
    /// # let client = BasePublisher::builder().build().await?;
    /// let publisher = client.publisher("projects/my-project/topics/my-topic")
    ///     .set_byte_threshold(100)
    ///     .build();
    /// # Ok(()) }
    /// ```
    pub fn set_byte_threshold(mut self, threshold: u32) -> PublisherPartialBuilder {
        self.batching_options = self.batching_options.set_byte_threshold(threshold);
        self
    }

    /// Sets the maximum amount of time the publisher will wait before sending a
    /// batch. When this delay is reached, the current batch is sent, regardless
    /// of the number of messages or total byte size.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_pubsub::client::BasePublisher;
    /// # use std::time::Duration;
    /// # async fn sample() -> anyhow::Result<()> {
    /// # let client = BasePublisher::builder().build().await?;
    /// let publisher = client.publisher("projects/my-project/topics/my-topic")
    ///     .set_delay_threshold(Duration::from_millis(50))
    ///     .build();
    /// # Ok(()) }
    /// ```
    pub fn set_delay_threshold(mut self, threshold: Duration) -> PublisherPartialBuilder {
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
    use crate::client::BasePublisher;
    use crate::publisher::options::BatchingOptions;
    use crate::{
        generated::gapic_dataplane::client::Publisher as GapicPublisher,
        model::{PublishResponse, PubsubMessage},
    };
    use gax::retry_policy::AlwaysRetry;
    use mockall::Sequence;
    use rand::{Rng, distr::Alphanumeric};
    use std::error::Error;

    static TOPIC: &str = "my-topic";

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

    fn publish_ok(
        req: crate::model::PublishRequest,
        _options: gax::options::RequestOptions,
    ) -> gax::Result<gax::response::Response<crate::model::PublishResponse>> {
        assert_eq!(req.topic, TOPIC);
        let ids = req
            .messages
            .iter()
            .map(|m| String::from_utf8(m.data.to_vec()).unwrap());
        Ok(gax::response::Response::from(
            PublishResponse::new().set_message_ids(ids),
        ))
    }

    fn publish_err(
        req: crate::model::PublishRequest,
        _options: gax::options::RequestOptions,
    ) -> gax::Result<gax::response::Response<crate::model::PublishResponse>> {
        assert_eq!(req.topic, TOPIC);
        Err(gax::error::Error::service(
            gax::error::rpc::Status::default()
                .set_code(gax::error::rpc::Code::Unknown)
                .set_message("unknown error has occurred"),
        ))
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
                        PubsubMessage::new()
                            .set_ordering_key($ordering_key)
                            .set_data(msg.clone()),
                    )
                    .await;
                assert_eq!(got.expect("expected message id"), msg);
            )+
        };
    }

    macro_rules! assert_publishing_is_paused {
        ($publisher:ident, $($ordering_key:expr),+) => {
            $(
                let got_err = $publisher
                    .publish(
                        PubsubMessage::new()
                            .set_ordering_key($ordering_key)
                            .set_data(generate_random_data()),
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
            )+
        };
    }

    #[tokio::test]
    async fn test_worker_success() {
        let mut mock = MockGapicPublisher::new();
        mock.expect_publish().returning(publish_ok).times(2);

        let client = GapicPublisher::from_stub(mock);
        let publisher = PublisherPartialBuilder::new(client, TOPIC.to_string())
            .set_message_count_threshold(1_u32)
            .build();

        let messages = [
            PubsubMessage::new().set_data("hello"),
            PubsubMessage::new().set_data("world"),
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
        mock.expect_publish().return_once(publish_ok);
        let client = GapicPublisher::from_stub(mock);
        let publisher = PublisherPartialBuilder::new(client, TOPIC.to_string())
            .set_message_count_threshold(1000_u32)
            .set_delay_threshold(Duration::from_secs(60))
            .build();

        let start = tokio::time::Instant::now();
        let messages = [
            PubsubMessage::new().set_data("hello"),
            PubsubMessage::new().set_data("world"),
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
        mock.expect_publish().returning(publish_err).times(2);

        let client = GapicPublisher::from_stub(mock);
        let publisher = PublisherPartialBuilder::new(client, TOPIC.to_string())
            .set_message_count_threshold(1_u32)
            .build();

        let messages = [
            PubsubMessage::new().set_data("hello"),
            PubsubMessage::new().set_data("world"),
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
        mock.expect_publish().returning(publish_ok);

        let client = GapicPublisher::from_stub(mock);
        let publisher = PublisherPartialBuilder::new(client, TOPIC.to_string())
            // Set a long delay.
            .set_message_count_threshold(1000_u32)
            .set_delay_threshold(Duration::from_secs(60))
            .build();

        let start = tokio::time::Instant::now();
        let messages = [
            PubsubMessage::new().set_data("hello"),
            PubsubMessage::new().set_data("world"),
        ];
        let mut handles = Vec::new();
        for msg in messages {
            let handle = publisher.publish(msg.clone());
            handles.push((msg, handle));
        }

        publisher.flush().await;
        assert_eq!(start.elapsed(), Duration::ZERO);

        let post = publisher.publish(PubsubMessage::new().set_data("after"));
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
            move |r, o| {
                assert_eq!(r.messages.len(), 2);
                assert_eq!(
                    r.messages,
                    vec![
                        PubsubMessage::new().set_data("hello"),
                        PubsubMessage::new().set_data("world")
                    ]
                );
                publish_ok(r, o)
            }
        });

        let client = GapicPublisher::from_stub(mock);
        let publisher = PublisherPartialBuilder::new(client, TOPIC.to_string())
            // Set a long delay.
            .set_message_count_threshold(1000_u32)
            .set_delay_threshold(Duration::from_secs(60))
            .build();

        let start = tokio::time::Instant::now();
        let messages = [
            PubsubMessage::new().set_data("hello"),
            PubsubMessage::new().set_data("world"),
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
        let publisher = PublisherPartialBuilder::new(client, TOPIC.to_string()).build();

        let start = tokio::time::Instant::now();
        publisher.flush().await;
        assert_eq!(start.elapsed(), Duration::ZERO);
    }

    #[tokio::test]
    async fn test_batching_send_on_message_count_threshold_success() {
        // Make sure all messages in a batch receive the correct message ID.
        let mut mock = MockGapicPublisher::new();
        mock.expect_publish().return_once({
            |r, o| {
                assert_eq!(r.messages.len(), 2);
                publish_ok(r, o)
            }
        });

        let client = GapicPublisher::from_stub(mock);
        let publisher = PublisherPartialBuilder::new(client, TOPIC.to_string())
            .set_message_count_threshold(2_u32)
            .set_byte_threshold(MAX_BYTES)
            .set_delay_threshold(std::time::Duration::MAX)
            .build();

        let messages = [
            PubsubMessage::new().set_data("hello"),
            PubsubMessage::new().set_data("world"),
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
            |r, o| {
                assert_eq!(r.messages.len(), 2);
                publish_err(r, o)
            }
        });

        let client = GapicPublisher::from_stub(mock);
        let publisher = PublisherPartialBuilder::new(client, TOPIC.to_string())
            .set_message_count_threshold(2_u32)
            .set_byte_threshold(MAX_BYTES)
            .set_delay_threshold(std::time::Duration::MAX)
            .build();

        let messages = [
            PubsubMessage::new().set_data("hello"),
            PubsubMessage::new().set_data("world"),
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
            |r, o| {
                assert_eq!(r.messages.len(), 2);
                publish_ok(r, o)
            }
        });

        let client = GapicPublisher::from_stub(mock);
        // Ensure that the first message does not pass the threshold.
        let byte_threshold = TOPIC.len() + "hello".len() + 1;
        let publisher = PublisherPartialBuilder::new(client, TOPIC.to_string())
            .set_message_count_threshold(MAX_MESSAGES)
            .set_byte_threshold(byte_threshold as u32)
            .set_delay_threshold(std::time::Duration::MAX)
            .build();

        let messages = [
            PubsubMessage::new().set_data("hello"),
            PubsubMessage::new().set_data("world"),
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
        mock.expect_publish().returning(publish_ok);

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
                PubsubMessage::new().set_data("hello 0"),
                PubsubMessage::new().set_data("hello 1"),
                PubsubMessage::new()
                    .set_data("hello 2")
                    .set_ordering_key("ordering key 1"),
                PubsubMessage::new()
                    .set_data("hello 3")
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
            |r, o| {
                assert_eq!(r.messages.len(), 2);
                assert_eq!(
                    r.messages.get(0).unwrap().ordering_key,
                    r.messages.get(1).unwrap().ordering_key
                );
                publish_ok(r, o)
            }
        });

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
            |r, o| {
                assert_eq!(r.messages.len(), 2);
                assert_eq!(
                    r.messages.get(0).unwrap().ordering_key,
                    r.messages.get(1).unwrap().ordering_key
                );
                publish_ok(r, o)
            }
        });

        let client = GapicPublisher::from_stub(mock);
        // Use a low message count to trigger batch sends.
        let publisher = PublisherPartialBuilder::new(client, TOPIC.to_string())
            .set_message_count_threshold(2_u32)
            .set_byte_threshold(MAX_BYTES)
            .set_delay_threshold(std::time::Duration::MAX)
            .build();

        let messages = [
            PubsubMessage::new().set_data("hello 1"),
            PubsubMessage::new()
                .set_data("hello 2")
                .set_ordering_key(""),
            PubsubMessage::new()
                .set_data("hello 3")
                .set_ordering_key("ordering key :1"),
            PubsubMessage::new()
                .set_data("hello 4")
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
                |r, o| {
                    Box::pin(async move {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                        assert_eq!(r.messages.len(), 1);
                        publish_ok(r, o)
                    })
                }
            });

        mock.expect_publish()
            .times(1)
            .in_sequence(&mut seq)
            .returning({
                |r, o| {
                    Box::pin(async move {
                        assert_eq!(r.messages.len(), 1);
                        publish_ok(r, o)
                    })
                }
            });

        let client = GapicPublisher::from_stub(mock);
        // Use a low message count to trigger batch sends.
        let publisher = PublisherPartialBuilder::new(client, TOPIC.to_string())
            .set_message_count_threshold(1_u32)
            .set_byte_threshold(MAX_BYTES)
            .set_delay_threshold(std::time::Duration::MAX)
            .build();

        let messages = [
            PubsubMessage::new()
                .set_data("hello 1")
                .set_ordering_key("ordering key"),
            PubsubMessage::new()
                .set_data("hello 2")
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
                |r, o| {
                    Box::pin(async move {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                        assert_eq!(r.messages.len(), 1);
                        publish_ok(r, o)
                    })
                }
            });

        mock.expect_publish()
            .times(1)
            .in_sequence(&mut seq)
            .returning({
                |r, o| {
                    Box::pin(async move {
                        assert_eq!(r.topic, TOPIC);
                        assert_eq!(r.messages.len(), 1);
                        publish_ok(r, o)
                    })
                }
            });

        let client = GapicPublisher::from_stub(mock);
        // Use a low message count to trigger batch sends.
        let publisher = PublisherPartialBuilder::new(client, TOPIC.to_string())
            .set_message_count_threshold(1_u32)
            .set_byte_threshold(MAX_BYTES)
            .set_delay_threshold(std::time::Duration::MAX)
            .build();

        let messages = [
            PubsubMessage::new()
                .set_data("hello 1")
                .set_ordering_key(""),
            PubsubMessage::new()
                .set_data("hello 2")
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
        let client = BasePublisher::builder().build().await?;
        let builder = client.publisher("projects/my-project/topics/my-topic");
        let publisher = builder.set_message_count_threshold(1_u32).build();
        assert_eq!(publisher.batching_options.message_count_threshold, 1_u32);

        let publisher = Publisher::builder("projects/my-project/topics/my-topic")
            .set_message_count_threshold(1_u32)
            .build()
            .await?;
        assert_eq!(publisher.batching_options.message_count_threshold, 1_u32);
        Ok(())
    }

    #[tokio::test]
    async fn default_batching() -> anyhow::Result<()> {
        // Test that default values for BasePublisher and Publisher are the same.
        let topic_name = "projects/my-project/topics/my-topic";
        let publishers = vec![
            BasePublisher::builder()
                .build()
                .await?
                .publisher(topic_name)
                .build(),
            Publisher::builder(topic_name).build().await?,
        ];

        for publisher in publishers {
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
        }
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn test_ordering_error_pause_publisher() {
        // Verify that a Publish send error will pause the publisher for an ordering key.
        let mut seq = Sequence::new();
        let mut mock = MockGapicPublisher::new();
        mock.expect_publish()
            .times(1)
            .in_sequence(&mut seq)
            .returning(publish_err);

        mock.expect_publish()
            .times(2)
            .in_sequence(&mut seq)
            .returning(publish_ok);

        let client = GapicPublisher::from_stub(mock);
        let publisher = PublisherPartialBuilder::new(client, TOPIC.to_string())
            .set_message_count_threshold(1_u32)
            .build();

        let key = "ordering_key";
        let msg_0_handle =
            publisher.publish(PubsubMessage::new().set_ordering_key(key).set_data("msg 0"));
        // Publish an additional message so that there's an additional pending message in the worker.
        let msg_1_handle =
            publisher.publish(PubsubMessage::new().set_ordering_key(key).set_data("msg 1"));

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

        // Assert that new publish messages return errors because the Publisher is paused.
        for _ in 0..3 {
            assert_publishing_is_paused!(publisher, key);
        }

        // Verify that the other ordering keys are not paused.
        assert_publishing_is_ok!(publisher, "", "without_error");
    }

    #[tokio::test(start_paused = true)]
    async fn test_ordering_error_pause_batch_errors() {
        // Verify that all messages in the same batch receives the Send error for that batch.
        let mut mock = MockGapicPublisher::new();
        mock.expect_publish().times(1).returning({
            |r, o| {
                assert_eq!(r.messages.len(), 2);
                publish_err(r, o)
            }
        });

        let client = GapicPublisher::from_stub(mock);
        let publisher = PublisherPartialBuilder::new(client, TOPIC.to_string())
            .set_message_count_threshold(2_u32)
            .build();

        let key = "ordering_key";
        // Publish 2 messages so they are in the same batch.
        let msg_0_handle =
            publisher.publish(PubsubMessage::new().set_ordering_key(key).set_data("msg 0"));
        let msg_1_handle =
            publisher.publish(PubsubMessage::new().set_ordering_key(key).set_data("msg 1"));

        // Validate that they both receives the Send error.
        // TODO(#3689): Validate the error structure when Publisher error structure is better defined.
        let mut got_err = msg_0_handle.await.unwrap_err();
        assert!(got_err.is_transport(), "{got_err:?}");
        got_err = msg_1_handle.await.unwrap_err();
        assert!(got_err.is_transport(), "{got_err:?}");

        // Assert that new publish messages returns an error because the Publisher is paused.
        assert_publishing_is_paused!(publisher, key);
    }

    #[tokio::test(start_paused = true)]
    async fn test_ordering_error_pause_then_flush() {
        // Verify that Flush on a paused ordering key returns an error.
        let mut seq = Sequence::new();
        let mut mock = MockGapicPublisher::new();
        mock.expect_publish()
            .times(1)
            .in_sequence(&mut seq)
            .returning(publish_err);

        mock.expect_publish()
            .times(2)
            .in_sequence(&mut seq)
            .returning(publish_ok);

        let client = GapicPublisher::from_stub(mock);
        let publisher = PublisherPartialBuilder::new(client, TOPIC.to_string()).build();

        let key = "ordering_key";
        // Cause an ordering key to be paused.
        let handle =
            publisher.publish(PubsubMessage::new().set_ordering_key(key).set_data("msg 0"));
        publisher.flush().await;
        // Assert the error is caused by the Publish send operation.
        let got_err = handle.await.unwrap_err();
        // TODO(#3689): Validate the error structure when Publisher error structure is better defined.
        assert!(got_err.is_transport(), "{got_err:?}");

        // Validate that new Publish on the paused ordering key will result in an error.
        assert_publishing_is_paused!(publisher, key);

        // Verify that the other ordering keys are not paused.
        assert_publishing_is_ok!(publisher, "", "without_error");
    }

    #[tokio::test(start_paused = true)]
    async fn test_ordering_resume_without_error() {
        let mut mock = MockGapicPublisher::new();
        mock.expect_publish().times(4).returning(publish_ok);

        let client = GapicPublisher::from_stub(mock);
        let publisher = PublisherPartialBuilder::new(client, TOPIC.to_string()).build();

        // Test resume and publish for empty ordering key.
        publisher.resume_publish("");
        assert_publishing_is_ok!(publisher, "");

        // Test resume and publish after the BatchWorker has been created for the empty ordering key.
        publisher.resume_publish("");
        assert_publishing_is_ok!(publisher, "");

        // Test resume and publish before the BatchWorker has been created.
        let key = "without_error";
        publisher.resume_publish(key);
        assert_publishing_is_ok!(publisher, key);

        // Test resume and publish after the BatchWorker has been created.
        publisher.resume_publish(key);
        assert_publishing_is_ok!(publisher, key);
    }

    #[tokio::test(start_paused = true)]
    async fn test_ordering_resume_publish() {
        let mut seq = Sequence::new();
        let mut mock = MockGapicPublisher::new();
        mock.expect_publish()
            .times(1)
            .in_sequence(&mut seq)
            .returning(publish_err);

        mock.expect_publish()
            .times(3)
            .in_sequence(&mut seq)
            .returning(publish_ok);

        let client = GapicPublisher::from_stub(mock);
        let publisher = PublisherPartialBuilder::new(client, TOPIC.to_string()).build();

        let key = "ordering_key";
        // Cause an ordering key to be paused.
        let handle =
            publisher.publish(PubsubMessage::new().set_ordering_key(key).set_data("msg 0"));
        // Assert the error is caused by the Publish send operation.
        let got_err = handle.await.unwrap_err();
        // TODO(#3689): Validate the error structure when Publisher error structure is better defined.
        assert!(got_err.is_transport(), "{got_err:?}");

        // Validate that new Publish on the paused ordering key will result in an error.
        assert_publishing_is_paused!(publisher, key);

        // Resume and validate the key is no longer paused.
        publisher.resume_publish(key);
        assert_publishing_is_ok!(publisher, key);

        // Verify that the other ordering keys continue to work as expected.
        assert_publishing_is_ok!(publisher, "", "without_error");
    }

    #[tokio::test(start_paused = true)]
    async fn test_ordering_resume_twice() {
        // Validate that resuming twice sequentially does not have bad side effects.
        let mut seq = Sequence::new();
        let mut mock = MockGapicPublisher::new();
        mock.expect_publish()
            .times(1)
            .in_sequence(&mut seq)
            .returning(publish_err);

        mock.expect_publish()
            .times(3)
            .in_sequence(&mut seq)
            .returning(publish_ok);

        let client = GapicPublisher::from_stub(mock);
        let publisher = PublisherPartialBuilder::new(client, TOPIC.to_string()).build();

        let key = "ordering_key";
        // Cause an ordering key to be paused.
        let handle =
            publisher.publish(PubsubMessage::new().set_ordering_key(key).set_data("msg 0"));
        publisher.flush().await;
        // Assert the error is caused by the Publish send operation.
        let got_err = handle.await.unwrap_err();
        // TODO(#3689): Validate the error structure when Publisher error structure is better defined.
        assert!(got_err.is_transport(), "{got_err:?}");

        // Validate that new Publish on the paused ordering key will result in an error.
        assert_publishing_is_paused!(publisher, key);

        // Resume twice on the paused ordering key.
        publisher.resume_publish(key);
        publisher.resume_publish(key);
        assert_publishing_is_ok!(publisher, key);
    }

    #[tokio::test(start_paused = true)]
    async fn test_ordering_resume_isolated() {
        // Validate that resume_publish only resumes the paused ordering key .
        let mut seq = Sequence::new();
        let mut mock = MockGapicPublisher::new();
        mock.expect_publish()
            .times(2)
            .in_sequence(&mut seq)
            .returning(publish_err);

        mock.expect_publish()
            .times(1)
            .in_sequence(&mut seq)
            .returning(publish_ok);

        let client = GapicPublisher::from_stub(mock);
        let publisher = PublisherPartialBuilder::new(client, TOPIC.to_string()).build();

        let key_0 = "ordering_key_0";
        let key_1 = "ordering_key_1";
        // Cause both ordering keys to pause.
        let handle_0 = publisher.publish(
            PubsubMessage::new()
                .set_ordering_key(key_0)
                .set_data("msg 0"),
        );
        let handle_1 = publisher.publish(
            PubsubMessage::new()
                .set_ordering_key(key_1)
                .set_data("msg 1"),
        );
        publisher.flush().await;
        let mut got_err = handle_0.await.unwrap_err();
        // TODO(#3689): Validate the error structure when Publisher error structure is better defined.
        assert!(got_err.is_transport(), "{got_err:?}");
        got_err = handle_1.await.unwrap_err();
        // TODO(#3689): Validate the error structure when Publisher error structure is better defined.
        assert!(got_err.is_transport(), "{got_err:?}");

        // Assert that both ordering keys are paused.
        assert_publishing_is_paused!(publisher, key_0, key_1);

        // Resume on one of the ordering key.
        publisher.resume_publish(key_0);

        // Validate that only the correct ordering key is resumed.
        assert_publishing_is_ok!(publisher, key_0);

        // Validate the other ordering key is still paused.
        assert_publishing_is_paused!(publisher, key_1);
    }

    #[tokio::test]
    async fn test_builder_clamping() -> anyhow::Result<()> {
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
