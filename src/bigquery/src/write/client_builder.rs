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

use super::pool::StreamPoolOptions;
use crate::ClientBuilderResult as BuilderResult;
use crate::client::Write;
use gaxi::options::ClientConfig;
use google_cloud_auth::credentials::Credentials;

/// A builder for [Write].
///
/// # Example
/// ```
/// # use google_cloud_bigquery::client::Write;
/// # async fn sample() -> anyhow::Result<()> {
/// let builder = Write::builder();
/// let client = builder
///     .with_endpoint("https://bigquerystoragewrite.googleapis.com")
///     .build()
///     .await?;
/// # Ok(()) }
/// ```
#[derive(Debug)]
pub struct ClientBuilder {
    pub(super) config: ClientConfig,
    pub(super) pool_options: StreamPoolOptions,
}

impl ClientBuilder {
    pub(super) fn new() -> Self {
        Self {
            config: ClientConfig::default(),
            pool_options: StreamPoolOptions::default(),
        }
    }

    /// Creates a new client.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_bigquery::client::Write;
    /// # async fn sample() -> anyhow::Result<()> {
    /// let client = Write::builder().build().await?;
    /// # Ok(()) }
    /// ```
    pub async fn build(self) -> BuilderResult<Write> {
        Write::new(self).await
    }

    /// Sets the endpoint.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_bigquery::client::Write;
    /// # async fn sample() -> anyhow::Result<()> {
    /// let client = Write::builder()
    ///     .with_endpoint("https://private.googleapis.com")
    ///     .build()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    pub fn with_endpoint<V: Into<String>>(mut self, v: V) -> Self {
        self.config.endpoint = Some(v.into());
        self
    }

    /// Configure the universe domain.
    ///
    /// The universe domain is the default service domain for a given cloud universe.
    /// The default value is "googleapis.com".
    ///
    /// # Example
    /// ```
    /// # use google_cloud_bigquery::client::Write;
    /// # async fn sample() -> anyhow::Result<()> {
    /// let client = Write::builder()
    ///     .with_universe_domain("googleapis.com")
    ///     .build()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    pub fn with_universe_domain<V: Into<String>>(mut self, v: V) -> Self {
        self.config.universe_domain = Some(v.into());
        self
    }

    /// Configures the authentication credentials.
    ///
    /// More information about valid credentials types can be found in the
    /// [google-cloud-auth] crate documentation.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_bigquery::client::Write;
    /// # async fn sample() -> anyhow::Result<()> {
    /// use google_cloud_auth::credentials::mds;
    /// let client = Write::builder()
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
    pub fn with_credentials<V: Into<Credentials>>(mut self, v: V) -> Self {
        self.config.cred = Some(v.into());
        self
    }

    /// Configure the number of subchannels used by the client.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_bigquery::client::Write;
    /// # async fn sample() -> anyhow::Result<()> {
    /// let count = std::thread::available_parallelism()?.get();
    /// let client = Write::builder()
    ///     .with_grpc_subchannel_count(count)
    ///     .build()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    ///
    /// gRPC-based clients may exhibit high latency if many requests need to be
    /// demuxed over a single HTTP/2 connection (often called a *subchannel* in
    /// gRPC).
    ///
    /// Consider using more subchannels if your application creates many
    /// writers. Consider using fewer subchannels if your application needs the
    /// file descriptors for other purposes.
    pub fn with_grpc_subchannel_count(mut self, v: usize) -> Self {
        self.config.grpc_subchannel_count = Some(v);
        self
    }

    // TODO(#6765) - make public, add example
    #[allow(dead_code)]
    /// Configure the maximum streams in the client's multiplexed stream pool.
    ///
    /// This stream pool is shared by default writers with multiplexing enabled.
    ///
    /// The client scales the stream pool up to this limit as the streams in the
    /// pool encounter load.
    ///
    /// The default is 8 streams.
    pub(crate) fn with_pool_size_limit(mut self, v: usize) -> Self {
        self.pool_options.max_streams = v;
        self
    }

    // TODO(#6765) - make public
    #[allow(dead_code)]
    /// Configure the maximum outstanding requests in the client's multiplexed
    /// stream pool.
    ///
    /// As streams in the stream pool approach this limit, the client
    /// dynamically adds more streams to the stream pool, up to the limit
    /// configured by `with_pool_size_limit`.
    ///
    /// The default is 1000 requests.
    pub(crate) fn with_max_outstanding_requests(mut self, v: u64) -> Self {
        self.pool_options.max_outstanding_requests = Some(v);
        self
    }

    // TODO(#6765) - make public
    #[allow(dead_code)]
    /// Configure the maximum outstanding bytes in the client's multiplexed
    /// stream pool.
    ///
    /// As streams in the stream pool approach this limit, the client
    /// dynamically adds more streams to the stream pool, up to the limit
    /// configured by `with_pool_size_limit`.
    pub(crate) fn with_max_outstanding_bytes(mut self, v: u64) -> Self {
        self.pool_options.max_outstanding_bytes = Some(v);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use google_cloud_auth::credentials::anonymous::Builder as Anonymous;

    #[test]
    fn defaults() {
        let builder = ClientBuilder::new();
        assert!(builder.config.endpoint.is_none(), "{:?}", builder.config);
        assert!(builder.config.cred.is_none(), "{:?}", builder.config);
        assert!(
            builder.config.universe_domain.is_none(),
            "{:?}",
            builder.config
        );
        assert!(
            builder.config.grpc_subchannel_count.is_none(),
            "{:?}",
            builder.config
        );
        assert_eq!(builder.pool_options.max_streams, 8);
        assert_eq!(builder.pool_options.max_outstanding_requests, Some(1000));
        assert_eq!(builder.pool_options.max_outstanding_bytes, None);
    }

    #[test]
    fn setters() {
        let builder = ClientBuilder::new()
            .with_endpoint("test-endpoint.com")
            .with_universe_domain("test-ud.com")
            .with_credentials(Anonymous::new().build())
            .with_grpc_subchannel_count(16)
            .with_pool_size_limit(10)
            .with_max_outstanding_requests(900)
            .with_max_outstanding_bytes(1_000_000);
        assert_eq!(
            builder.config.endpoint,
            Some("test-endpoint.com".to_string())
        );
        assert_eq!(
            builder.config.universe_domain,
            Some("test-ud.com".to_string())
        );
        assert!(builder.config.cred.is_some(), "{:?}", builder.config);
        assert_eq!(builder.config.grpc_subchannel_count, Some(16));
        assert_eq!(builder.pool_options.max_streams, 10);
        assert_eq!(builder.pool_options.max_outstanding_requests, Some(900));
        assert_eq!(builder.pool_options.max_outstanding_bytes, Some(1_000_000));
    }
}
