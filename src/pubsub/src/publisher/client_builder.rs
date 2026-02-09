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

use super::base_publisher::BasePublisher;
use gaxi::options::ClientConfig;
use google_cloud_gax::backoff_policy::BackoffPolicyArg;
use google_cloud_gax::client_builder::Result as BuilderResult;
use google_cloud_gax::retry_policy::RetryPolicyArg;
use google_cloud_gax::retry_throttler::RetryThrottlerArg;

/// A builder for [BasePublisher].
///
/// # Example
/// ```
/// # use google_cloud_pubsub::publisher::client::BasePublisher;
/// # async fn sample() -> anyhow::Result<()> {
/// let builder = BasePublisher::builder();
/// let client = builder
///     .with_endpoint("https://pubsub.googleapis.com")
///     .build()
///     .await?;
/// # Ok(()) }
/// ```
#[derive(Clone, Debug)]
pub struct ClientBuilder {
    pub(super) config: ClientConfig,
}

impl ClientBuilder {
    pub(super) fn new() -> Self {
        let mut config = ClientConfig::default();
        config.backoff_policy = Some(std::sync::Arc::new(
            super::backoff_policy::default_backoff_policy(),
        ));
        config.retry_policy = Some(std::sync::Arc::new(
            super::retry_policy::default_retry_policy(),
        ));
        Self { config }
    }

    /// Creates a new client.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_pubsub::publisher::client::BasePublisher;
    /// # async fn sample() -> anyhow::Result<()> {
    /// let client = BasePublisher::builder().build().await?;
    /// # Ok(()) }
    /// ```
    pub async fn build(self) -> BuilderResult<BasePublisher> {
        BasePublisher::new(self).await
    }

    /// Sets the endpoint.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_pubsub::publisher::client::BasePublisher;
    /// # async fn sample() -> anyhow::Result<()> {
    /// let client = BasePublisher::builder()
    ///     .with_endpoint("https://private.googleapis.com")
    ///     .build()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    pub fn with_endpoint<V: Into<String>>(mut self, v: V) -> Self {
        self.config.endpoint = Some(v.into());
        self
    }

    /// Enables tracing.
    ///
    /// The client libraries can be dynamically instrumented with the Tokio
    /// [tracing] framework. Setting this flag enables this instrumentation.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_pubsub::publisher::client::BasePublisher;
    /// # async fn sample() -> anyhow::Result<()> {
    /// let client = BasePublisher::builder()
    ///     .with_tracing()
    ///     .build()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    ///
    /// [tracing]: https://docs.rs/tracing/latest/tracing/
    pub fn with_tracing(mut self) -> Self {
        self.config.tracing = true;
        self
    }

    /// Configure the authentication credentials.
    ///
    /// Most Google Cloud services require authentication, though some services
    /// allow for anonymous access, and some services provide emulators where
    /// no authentication is required. More information about valid credentials
    /// types can be found in the [google-cloud-auth] crate documentation.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_pubsub::publisher::client::BasePublisher;
    /// # async fn sample() -> anyhow::Result<()> {
    /// use google_cloud_auth::credentials::mds;
    /// let client = BasePublisher::builder()
    ///     .with_credentials(
    ///         mds::Builder::default()
    ///             .with_scopes(["https://www.googleapis.com/auth/cloud-platform.read-only"])
    ///             .build()?)
    ///     .build()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    ///
    /// [google-cloud-auth]: https://docs.rs/google-cloud-auth
    pub fn with_credentials<V: Into<gaxi::options::Credentials>>(mut self, v: V) -> Self {
        self.config.cred = Some(v.into());
        self
    }

    /// Configure the retry policy.
    ///
    /// The client libraries can automatically retry operations that fail. The
    /// retry policy controls what errors are considered retryable, sets limits
    /// on the number of attempts or the time trying to make attempts.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_pubsub::publisher::client::BasePublisher;
    /// # async fn sample() -> anyhow::Result<()> {
    /// use google_cloud_gax::retry_policy::{AlwaysRetry, RetryPolicyExt};
    /// let client = BasePublisher::builder()
    ///     .with_retry_policy(AlwaysRetry.with_attempt_limit(3))
    ///     .build()
    ///     .await?;
    /// # Ok(()) };
    /// ```
    pub fn with_retry_policy<V: Into<RetryPolicyArg>>(mut self, v: V) -> Self {
        self.config.retry_policy = Some(v.into().into());
        self
    }

    /// Configure the retry backoff policy.
    ///
    /// The client libraries can automatically retry operations that fail. The
    /// backoff policy controls how long to wait in between retry attempts.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_pubsub::publisher::client::BasePublisher;
    /// # async fn sample() -> anyhow::Result<()> {
    /// use google_cloud_gax::exponential_backoff::ExponentialBackoff;
    /// use std::time::Duration;
    /// let policy = ExponentialBackoff::default();
    /// let client = BasePublisher::builder()
    ///     .with_backoff_policy(policy)
    ///     .build()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    pub fn with_backoff_policy<V: Into<BackoffPolicyArg>>(mut self, v: V) -> Self {
        self.config.backoff_policy = Some(v.into().into());
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
    /// # Example
    /// ```
    /// # use google_cloud_pubsub::publisher::client::BasePublisher;
    /// # async fn sample() -> anyhow::Result<()> {
    /// use google_cloud_gax::retry_throttler::AdaptiveThrottler;
    /// let client = BasePublisher::builder()
    ///     .with_retry_throttler(AdaptiveThrottler::default())
    ///     .build()
    ///     .await?;
    /// # Ok(()) };
    /// ```
    pub fn with_retry_throttler<V: Into<RetryThrottlerArg>>(mut self, v: V) -> Self {
        self.config.retry_throttler = v.into().into();
        self
    }

    /// Configure the number of gRPC subchannels.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_pubsub::publisher::client::BasePublisher;
    /// # async fn sample() -> anyhow::Result<()> {
    /// let client = BasePublisher::builder()
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
        self.config.grpc_subchannel_count = Some(v);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use google_cloud_auth::credentials::anonymous::Builder as Anonymous;

    #[test]
    fn defaults() -> anyhow::Result<()> {
        let builder = ClientBuilder::new();
        assert!(builder.config.endpoint.is_none(), "{builder:?}");
        assert!(builder.config.cred.is_none(), "{builder:?}");
        assert!(!builder.config.tracing);
        assert!(
            format!("{:?}", &builder.config).contains("AdaptiveThrottler"),
            "{:?}",
            builder.config
        );
        assert!(builder.config.backoff_policy.is_some(), "{builder:?}");
        let debug_str = format!("{:?}", &builder.config);
        assert!(
            debug_str.contains("initial_delay: 100ms"),
            "actual: {debug_str}"
        );
        assert!(
            debug_str.contains("maximum_delay: 60s"),
            "actual: {debug_str}"
        );
        assert!(debug_str.contains("scaling: 4.0"), "actual: {debug_str}");
        assert!(builder.config.retry_policy.is_some(), "{builder:?}");
        assert!(
            builder.config.grpc_subchannel_count.is_none(),
            "{builder:?}"
        );

        Ok(())
    }

    #[tokio::test]
    async fn setters() -> anyhow::Result<()> {
        use google_cloud_gax::retry_policy::{AlwaysRetry, RetryPolicyExt};
        let builder = ClientBuilder::new()
            .with_endpoint("test-endpoint.com")
            .with_credentials(Anonymous::new().build())
            .with_tracing()
            .with_retry_policy(AlwaysRetry.with_attempt_limit(3))
            .with_backoff_policy(
                google_cloud_gax::exponential_backoff::ExponentialBackoff::default(),
            )
            .with_retry_throttler(google_cloud_gax::retry_throttler::CircuitBreaker::default())
            .with_grpc_subchannel_count(16);
        assert_eq!(
            builder.config.endpoint,
            Some("test-endpoint.com".to_string())
        );
        assert!(builder.config.cred.is_some(), "{builder:?}");
        assert!(builder.config.tracing);
        assert!(
            format!("{:?}", &builder.config).contains("CircuitBreaker"),
            "{:?}",
            builder.config
        );
        assert!(builder.config.retry_policy.is_some(), "{builder:?}");
        assert!(builder.config.backoff_policy.is_some(), "{builder:?}");
        assert_eq!(builder.config.grpc_subchannel_count, Some(16));

        Ok(())
    }
}
