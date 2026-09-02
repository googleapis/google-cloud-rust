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
}

impl Default for ClientBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl ClientBuilder {
    /// Creates a new default [`ClientBuilder`].
    pub fn new() -> Self {
        let mut config = ClientConfig::default();

        if let Some(endpoint) = std::env::var("BIGQUERY_EMULATOR_HOST")
            .ok()
            .filter(|s| !s.is_empty())
        {
            let endpoint = if endpoint.contains("://") {
                endpoint.clone()
            } else {
                format!("http://{}", endpoint)
            };
            config.endpoint = Some(endpoint);
            // The emulator does not require valid credentials.
            config.cred = Some(google_cloud_auth::credentials::anonymous::Builder::new().build());
        }

        Self {
            config,
            project_id: None,
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
    use google_cloud_auth::credentials::anonymous::Builder as Anonymous;

    #[test]
    #[serial_test::serial]
    fn defaults() -> anyhow::Result<()> {
        let builder = ClientBuilder::new();
        assert!(builder.config.endpoint.is_none(), "{builder:?}");
        assert!(builder.config.universe_domain.is_none(), "{builder:?}");
        assert!(builder.config.cred.is_none(), "{builder:?}");
        assert!(!builder.config.tracing);
        assert!(builder.project_id.is_none(), "{builder:?}");

        Ok(())
    }

    #[tokio::test]
    async fn setters() -> anyhow::Result<()> {
        let builder = ClientBuilder::new()
            .with_project_id("test-project")
            .with_endpoint("test-endpoint.com")
            .with_universe_domain("test-universe.com")
            .with_credentials(Anonymous::new().build())
            .with_tracing();

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

        Ok(())
    }
}

#[test]
#[serial_test::serial]
fn test_emulator_host() {
    // Safety: This test modifies the global environment variable `BIGQUERY_EMULATOR_HOST`.
    // We use `#[serial]` to ensure it runs in isolation and does not interfere with other tests.
    let _env = scoped_env::ScopedEnv::set("BIGQUERY_EMULATOR_HOST", "localhost:9050");
    let builder = ClientBuilder::new();
    assert_eq!(
        builder.config.endpoint,
        Some("http://localhost:9050".to_string())
    );
}

#[test]
#[serial_test::serial]
fn test_emulator_host_with_scheme() {
    // Safety: This test modifies the global environment variable `BIGQUERY_EMULATOR_HOST`.
    // We use `#[serial]` to ensure it runs in isolation and does not interfere with other tests.
    let _env = scoped_env::ScopedEnv::set("BIGQUERY_EMULATOR_HOST", "http://localhost:9050");
    let builder = ClientBuilder::new();
    assert_eq!(
        builder.config.endpoint,
        Some("http://localhost:9050".to_string())
    );
}
