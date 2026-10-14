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

use crate::client::BigQuery;
use gaxi::options::ClientConfig;
use google_cloud_auth::credentials::Credentials;
use google_cloud_gax::client_builder::Result;

/// A builder for [`BigQuery`][crate::client::BigQuery].
///
/// # Example
/// ```
/// # use google_cloud_bigquery::client::BigQuery;
/// # async fn sample() -> anyhow::Result<()> {
/// let builder = BigQuery::builder();
/// let client = builder
///     .with_endpoint("https://bigquery.googleapis.com")
///     .build()
///     .await?;
/// # Ok(()) }
/// ```
#[derive(Clone, Debug)]
pub struct ClientBuilder {
    pub(crate) config: ClientConfig,
    pub(crate) project_id: Option<String>,
    pub(crate) storage_read_enabled: bool,
    pub(crate) storage_read_endpoint: Option<String>,
}

impl Default for ClientBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl ClientBuilder {
    /// Creates a new default [`ClientBuilder`].
    pub fn new() -> Self {
        Self {
            config: ClientConfig::default(),
            project_id: None,
            storage_read_enabled: false,
            storage_read_endpoint: None,
        }
    }

    /// Sets the default Google Cloud project ID for the client.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_bigquery::client::BigQuery;
    /// # async fn sample() -> anyhow::Result<()> {
    /// let client = BigQuery::builder()
    ///     .with_project_id("my-project-id")
    ///     .build()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    pub fn with_project_id<V: Into<String>>(mut self, project_id: V) -> Self {
        self.project_id = Some(project_id.into());
        self
    }

    /// Sets the [BigQuery v2] API endpoint.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_bigquery::client::BigQuery;
    /// # async fn sample() -> anyhow::Result<()> {
    /// let client = BigQuery::builder()
    ///     .with_endpoint("https://private.googleapis.com")
    ///     .build()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    ///
    /// [BigQuery v2]: https://docs.cloud.google.com/bigquery/docs/reference/rest
    pub fn with_endpoint<V: Into<String>>(mut self, v: V) -> Self {
        self.config.endpoint = Some(v.into());
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
    /// # use google_cloud_bigquery::client::BigQuery;
    /// # async fn sample() -> anyhow::Result<()> {
    /// use google_cloud_auth::credentials::mds;
    /// let client = BigQuery::builder()
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
    pub fn with_credentials<V: Into<Credentials>>(mut self, credentials: V) -> Self {
        self.config.cred = Some(credentials.into());
        self
    }

    /// Configure the universe domain.
    ///
    /// The universe domain is the default service domain for a given cloud universe.
    /// The default value is "googleapis.com".
    ///
    /// # Example
    /// ```
    /// # use google_cloud_bigquery::client::BigQuery;
    /// # async fn sample() -> anyhow::Result<()> {
    /// let client = BigQuery::builder()
    ///     .with_universe_domain("googleapis.com")
    ///     .build()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    pub fn with_universe_domain<V: Into<String>>(mut self, v: V) -> Self {
        self.config.universe_domain = Some(v.into());
        self
    }

    /// Enables tracing.
    ///
    /// The client libraries can be dynamically instrumented with the Tokio
    /// [tracing] framework. Setting this flag enables this instrumentation.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_bigquery::client::BigQuery;
    /// # async fn sample() -> anyhow::Result<()> {
    /// let client = BigQuery::builder()
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

    /// Configure the retry policy.
    ///
    /// The client libraries can automatically retry operations that fail. The
    /// retry policy controls what errors are considered retryable, sets limits
    /// on the number of attempts or the time trying to make attempts.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_bigquery::client::BigQuery;
    /// # async fn sample() -> anyhow::Result<()> {
    /// use google_cloud_bigquery::query::retry_policy::RetryableErrors;
    /// use google_cloud_gax::retry_policy::RetryPolicyExt;
    /// let client = BigQuery::builder()
    ///     .with_retry_policy(RetryableErrors.with_attempt_limit(3))
    ///     .build()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    pub fn with_retry_policy<V: Into<google_cloud_gax::retry_policy::RetryPolicyArg>>(
        mut self,
        v: V,
    ) -> Self {
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
    /// # use google_cloud_bigquery::client::BigQuery;
    /// # async fn sample() -> anyhow::Result<()> {
    /// use google_cloud_gax::exponential_backoff::ExponentialBackoff;
    /// use std::time::Duration;
    /// let policy = ExponentialBackoff::default();
    /// let client = BigQuery::builder()
    ///     .with_backoff_policy(policy)
    ///     .build()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    pub fn with_backoff_policy<V: Into<google_cloud_gax::backoff_policy::BackoffPolicyArg>>(
        mut self,
        v: V,
    ) -> Self {
        self.config.backoff_policy = Some(v.into().into());
        self
    }

    /// Enables or disables BigQuery Storage Read API acceleration for query result reading.
    ///
    /// When enabled, large query result sets will be streamed directly using the high-performance
    /// gRPC Storage Read API in Arrow format, using the same [`Row`][crate::query::Row] interface.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_bigquery::client::BigQuery;
    /// # async fn sample() -> anyhow::Result<()> {
    /// let client = BigQuery::builder()
    ///     .with_storage_read(true)
    ///     .build()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    #[cfg(google_cloud_unstable_bigquery_storage_read)]
    pub fn with_storage_read(mut self, enabled: bool) -> Self {
        self.storage_read_enabled = enabled;
        self
    }

    /// Sets the endpoint for the BigQuery Storage Read API.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_bigquery::client::BigQuery;
    /// # async fn sample() -> anyhow::Result<()> {
    /// let client = BigQuery::builder()
    ///     .with_storage_read(true)
    ///     .with_storage_read_endpoint("https://bigquerystorage.googleapis.com")
    ///     .build()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    #[cfg(google_cloud_unstable_bigquery_storage_read)]
    pub fn with_storage_read_endpoint<V: Into<String>>(mut self, endpoint: V) -> Self {
        self.storage_read_endpoint = Some(endpoint.into());
        self
    }

    /// Creates a new [`BigQuery`] client.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_bigquery::client::BigQuery;
    /// # async fn sample() -> anyhow::Result<()> {
    /// let client = BigQuery::builder().build().await?;
    /// # Ok(()) }
    /// ```
    pub async fn build(self) -> Result<BigQuery> {
        BigQuery::new(self).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::retry_policy::RetryableErrors;
    use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
    use google_cloud_gax::exponential_backoff::ExponentialBackoff;

    #[test]
    fn defaults() -> anyhow::Result<()> {
        let builder = ClientBuilder::new();
        assert!(builder.config.endpoint.is_none(), "{builder:?}");
        assert!(builder.config.universe_domain.is_none(), "{builder:?}");
        assert!(builder.config.cred.is_none(), "{builder:?}");
        assert!(!builder.config.tracing);
        assert!(builder.config.retry_policy.is_none(), "{builder:?}");
        assert!(builder.config.backoff_policy.is_none(), "{builder:?}");
        assert!(builder.project_id.is_none(), "{builder:?}");
        assert!(!builder.storage_read_enabled, "{builder:?}");
        assert!(builder.storage_read_endpoint.is_none(), "{builder:?}");

        Ok(())
    }

    #[tokio::test]
    async fn setters() -> anyhow::Result<()> {
        let builder = ClientBuilder::new()
            .with_project_id("test-project")
            .with_endpoint("test-endpoint.com")
            .with_universe_domain("test-universe.com")
            .with_credentials(Anonymous::new().build())
            .with_retry_policy(RetryableErrors)
            .with_backoff_policy(ExponentialBackoff::default())
            .with_tracing();
        #[cfg(google_cloud_unstable_bigquery_storage_read)]
        let builder = builder
            .with_storage_read(true)
            .with_storage_read_endpoint("test-storage-endpoint.com");

        assert_eq!(builder.project_id, Some("test-project".to_string()));
        assert_eq!(
            builder.config.endpoint,
            Some("test-endpoint.com".to_string())
        );
        assert_eq!(
            builder.config.universe_domain,
            Some("test-universe.com".to_string())
        );
        assert!(builder.config.cred.is_some(), "{builder:?}");
        assert!(builder.config.tracing);
        assert!(builder.config.retry_policy.is_some(), "{builder:?}");
        assert!(builder.config.backoff_policy.is_some(), "{builder:?}");
        #[cfg(google_cloud_unstable_bigquery_storage_read)]
        {
            assert!(builder.storage_read_enabled);
            assert_eq!(
                builder.storage_read_endpoint,
                Some("test-storage-endpoint.com".to_string())
            );
        }

        Ok(())
    }
}
