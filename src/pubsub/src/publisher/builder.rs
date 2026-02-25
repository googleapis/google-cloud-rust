// Copyright 2026 Google LLC
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
use super::constants::*;
use super::options::BatchingOptions;
use crate::client::Publisher;
use crate::generated::gapic_dataplane::client::Publisher as GapicPublisher;
use crate::publisher::actor::Dispatcher;
use crate::publisher::base_publisher::{BasePublisher, BasePublisherBuilder};
use google_cloud_gax::{
    backoff_policy::BackoffPolicyArg, retry_policy::RetryPolicyArg,
    retry_throttler::RetryThrottlerArg,
};
use std::time::Duration;

/// A builder for a [`Publisher`].
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
    pub async fn build(self) -> crate::ClientBuilderResult<Publisher> {
        let base_publisher = self.base_builder.build().await?;
        let publisher = base_publisher
            .publisher(&self.topic)
            .set_message_count_threshold(self.batching_options.message_count_threshold)
            .set_byte_threshold(self.batching_options.byte_threshold)
            .set_delay_threshold(self.batching_options.delay_threshold)
            .build();
        Ok(publisher)
    }

    /// Sets the message count threshold for batching.
    ///
    /// The publisher will send a batch of messages when the number of messages
    /// in the batch reaches this threshold.
    ///
    /// # Example
    ///
    /// ```
    /// # use google_cloud_pubsub::client::Publisher;
    /// # async fn sample() -> anyhow::Result<()> {
    /// let publisher = Publisher::builder("projects/my-project/topics/my-topic")
    ///     .set_message_count_threshold(100)
    ///     .build()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    pub fn set_message_count_threshold(mut self, threshold: u32) -> PublisherBuilder {
        self.batching_options = self.batching_options.set_message_count_threshold(threshold);
        self
    }

    /// Sets the byte threshold for batching.
    ///
    /// The publisher will send a batch of messages when the total size of the
    /// messages in the batch reaches this threshold.
    ///
    /// # Example
    ///
    /// ```
    /// # use google_cloud_pubsub::client::Publisher;
    /// # async fn sample() -> anyhow::Result<()> {
    /// let publisher = Publisher::builder("projects/my-project/topics/my-topic")
    ///     .set_byte_threshold(1024) // 1 KiB
    ///     .build()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    pub fn set_byte_threshold(mut self, threshold: u32) -> PublisherBuilder {
        self.batching_options = self.batching_options.set_byte_threshold(threshold);
        self
    }

    /// Sets the delay threshold for batching.
    ///
    /// The publisher will wait a maximum of this amount of time before
    /// sending a batch of messages.
    ///
    /// # Example
    ///
    /// ```
    /// # use google_cloud_pubsub::client::Publisher;
    /// # use std::time::Duration;
    /// # async fn sample() -> anyhow::Result<()> {
    /// let publisher = Publisher::builder("projects/my-project/topics/my-topic")
    ///     .set_delay_threshold(Duration::from_millis(50))
    ///     .build()
    ///     .await?;
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
    /// use google_cloud_auth::credentials::mds;
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
    /// use google_cloud_gax::retry_policy::{AlwaysRetry, RetryPolicyExt};
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
    /// use google_cloud_gax::exponential_backoff::ExponentialBackoff;
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
    /// customize the default retry throttler.
    ///
    /// [Handling Overload]: https://sre.google/sre-book/handling-overload/
    /// [Address Cascading Failures]: https://sre.google/sre-book/addressing-cascading-failures/
    ///
    /// ```
    /// # use google_cloud_pubsub::client::Publisher;
    /// # async fn sample() -> anyhow::Result<()> {
    /// use google_cloud_gax::retry_throttler::AdaptiveThrottler;
    /// let client = Publisher::builder("projects/my-project/topics/my-topic")
    ///     .with_retry_throttler(AdaptiveThrottler::default())
    ///     .build().await?;
    /// # Ok(()) };
    /// ```
    pub fn with_retry_throttler<V: Into<RetryThrottlerArg>>(mut self, v: V) -> Self {
        self.base_builder = self.base_builder.with_retry_throttler(v);
        self
    }

    /// Configure the number of gRPC subchannels.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_pubsub::client::Publisher;
    /// # async fn sample() -> anyhow::Result<()> {
    /// let client = Publisher::builder("projects/my-project/topics/my-topic")
    ///     .with_grpc_subchannel_count(4)
    ///     .build()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    ///
    /// gRPC-based clients may exhibit high latency if many requests need to be
    /// demuxed over a single HTTP/2 connection (often called a *subchannel* in
    /// gRPC).
    ///
    /// Consider using more subchannels if your application makes many
    /// concurrent requests. Consider using fewer subchannels if your
    /// application needs the file descriptors for other purposes.
    pub fn with_grpc_subchannel_count(mut self, v: usize) -> Self {
        self.base_builder = self.base_builder.with_grpc_subchannel_count(v);
        self
    }
}

/// Creates [`Publisher`]s with a preconfigured client.
///
/// # Example
///
/// ```
/// # async fn sample() -> anyhow::Result<()> {
/// # use google_cloud_pubsub::*;
/// # use google_cloud_pubsub::publisher::client::BasePublisher;
/// let client: BasePublisher = BasePublisher::builder().build().await?;
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

    /// Sets the message count threshold for batching.
    ///
    /// The publisher will send a batch of messages when the number of messages
    /// in the batch reaches this threshold.
    ///
    /// # Example
    ///
    /// ```
    /// # use google_cloud_pubsub::publisher::client::BasePublisher;
    /// # async fn sample() -> anyhow::Result<()> {
    /// # let client: BasePublisher = BasePublisher::builder().build().await?;
    /// let publisher = client
    ///     .publisher("projects/my-project/topics/my-topic")
    ///     .set_message_count_threshold(100)
    ///     .build();
    /// # Ok(()) }
    /// ```
    pub fn set_message_count_threshold(mut self, threshold: u32) -> PublisherPartialBuilder {
        self.batching_options = self.batching_options.set_message_count_threshold(threshold);
        self
    }

    /// Sets the byte threshold for batching.
    ///
    /// The publisher will send a batch of messages when the total size of the
    /// messages in the batch reaches this threshold.
    ///
    /// # Example
    ///
    /// ```
    /// # use google_cloud_pubsub::publisher::client::BasePublisher;
    /// # async fn sample() -> anyhow::Result<()> {
    /// # let client: BasePublisher = BasePublisher::builder().build().await?;
    /// let publisher = client
    ///     .publisher("projects/my-project/topics/my-topic")
    ///     .set_byte_threshold(1024) // 1 KiB
    ///     .build();
    /// # Ok(()) }
    /// ```
    pub fn set_byte_threshold(mut self, threshold: u32) -> PublisherPartialBuilder {
        self.batching_options = self.batching_options.set_byte_threshold(threshold);
        self
    }

    /// Sets the delay threshold for batching.
    ///
    /// The publisher will wait a maximum of this amount of time before
    /// sending a batch of messages.
    ///
    /// # Example
    ///
    /// ```
    /// # use google_cloud_pubsub::publisher::client::BasePublisher;
    /// # use std::time::Duration;
    /// # async fn sample() -> anyhow::Result<()> {
    /// # let client: BasePublisher = BasePublisher::builder().build().await?;
    /// let publisher = client
    ///     .publisher("projects/my-project/topics/my-topic")
    ///     .set_delay_threshold(Duration::from_millis(50))
    ///     .build();
    /// # Ok(()) }
    /// ```
    pub fn set_delay_threshold(mut self, threshold: Duration) -> PublisherPartialBuilder {
        self.batching_options = self.batching_options.set_delay_threshold(threshold);
        self
    }

    /// Creates a new [`Publisher`] from the builder's configuration.
    pub fn build(self) -> Publisher {
        self.build_return_handle().0
    }

    // This method starts a background task to manage the batching
    // and sending of messages. The returned `Publisher` is a
    // lightweight handle for sending messages to that background task
    // over a channel.
    //
    // This also returns a handle to the background task, which can be
    // used in testing to manage the task's lifecycle.
    pub(crate) fn build_return_handle(self) -> (Publisher, tokio::task::JoinHandle<()>) {
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
        // Create the Dispatcher that will run in the background.
        // We don't need to keep track of a handle to the dispatcher.
        // Dropping the Publisher will drop the only sender to the channel.
        // This will cause the dispatcher to gracefully exit.
        let dispatcher = Dispatcher::new(self.topic, self.inner, batching_options.clone(), rx);
        let handle = tokio::spawn(dispatcher.run());

        (
            Publisher {
                batching_options,
                tx,
            },
            handle,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn builder() -> anyhow::Result<()> {
        let client: BasePublisher = BasePublisher::builder().build().await?;
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

    fn assert_eq_client_config(
        pub_config: &gaxi::options::ClientConfig,
        base_config: &gaxi::options::ClientConfig,
    ) {
        assert_eq!(pub_config.endpoint, base_config.endpoint);
        assert_eq!(pub_config.cred.is_some(), base_config.cred.is_some());
        assert_eq!(pub_config.tracing, base_config.tracing);
        assert_eq!(
            pub_config.retry_policy.is_some(),
            base_config.retry_policy.is_some()
        );
        assert_eq!(
            pub_config.backoff_policy.is_some(),
            base_config.backoff_policy.is_some()
        );
        assert_eq!(
            pub_config.grpc_subchannel_count,
            base_config.grpc_subchannel_count
        );
    }

    #[test]
    fn publisher_has_default_client_config() {
        let pub_builder = Publisher::builder("projects/my-project/topics/my-topic");
        let base_builder = BasePublisher::builder();
        let pub_config = &pub_builder.base_builder.config;
        let base_config = &base_builder.config;

        assert_eq_client_config(pub_config, base_config);
    }

    #[tokio::test]
    async fn publisher_builder_sets_client_config() -> anyhow::Result<()> {
        use google_cloud_auth::credentials::anonymous::Builder as Anonymous;

        use google_cloud_gax::retry_policy::{AlwaysRetry, RetryPolicyExt};
        let throttler = google_cloud_gax::retry_throttler::CircuitBreaker::default();
        let pub_builder = Publisher::builder("projects/my-project/topics/my-topic")
            .with_endpoint("test-endpoint.com")
            .with_credentials(Anonymous::new().build())
            .with_tracing()
            .with_retry_policy(AlwaysRetry.with_attempt_limit(3))
            .with_backoff_policy(
                google_cloud_gax::exponential_backoff::ExponentialBackoff::default(),
            )
            .with_retry_throttler(throttler.clone())
            .with_grpc_subchannel_count(16);
        let base_builder = BasePublisher::builder()
            .with_endpoint("test-endpoint.com")
            .with_credentials(Anonymous::new().build())
            .with_tracing()
            .with_retry_policy(AlwaysRetry.with_attempt_limit(3))
            .with_backoff_policy(
                google_cloud_gax::exponential_backoff::ExponentialBackoff::default(),
            )
            .with_retry_throttler(throttler)
            .with_grpc_subchannel_count(16);

        let pub_config = &pub_builder.base_builder.config;
        let base_config = &base_builder.config;

        assert_eq_client_config(pub_config, base_config);

        Ok(())
    }
}
