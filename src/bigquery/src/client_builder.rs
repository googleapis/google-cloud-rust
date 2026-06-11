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
use google_cloud_gax::backoff_policy::BackoffPolicyArg;
use google_cloud_gax::client_builder::Result;
use google_cloud_gax::retry_policy::RetryPolicyArg;
use google_cloud_gax::retry_throttler::RetryThrottlerArg;

/// A builder for creating and configuring a BigQuery client instance.
#[derive(Clone, Debug)]
pub struct ClientBuilder {
    pub(crate) config: ClientConfig,
}

impl Default for ClientBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl ClientBuilder {
    /// Creates a new default `ClientBuilder`.
    pub fn new() -> Self {
        Self {
            config: ClientConfig::default(),
        }
    }

    /// Sets the [BigQuery v2] API endpoint.
    ///
    /// [BigQuery v2]: https://docs.cloud.google.com/bigquery/docs/reference/rest
    pub fn with_endpoint<V: Into<String>>(mut self, v: V) -> Self {
        self.config.endpoint = Some(v.into());
        self
    }

    /// Sets custom credentials for the client.
    pub fn with_credentials<V: Into<Credentials>>(mut self, credentials: V) -> Self {
        self.config.cred = Some(credentials.into());
        self
    }

    /// Configure the universe domain.
    ///
    /// The universe domain is the default service domain for a given cloud universe.
    /// The default value is "googleapis.com".
    pub fn with_universe_domain<V: Into<String>>(mut self, v: V) -> Self {
        self.config.universe_domain = Some(v.into());
        self
    }

    /// Enables observability signals for the client.
    pub fn with_tracing(mut self) -> Self {
        self.config.tracing = true;
        self
    }

    /// Builds the `BigQuery` client instance.
    pub async fn build(self) -> Result<BigQuery> {
        BigQuery::new(self).await
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
        assert!(builder.config.universe_domain.is_none(), "{builder:?}");
        assert!(builder.config.cred.is_none(), "{builder:?}");
        assert!(!builder.config.tracing);

        Ok(())
    }

    #[tokio::test]
    async fn setters() -> anyhow::Result<()> {
        use google_cloud_gax::retry_policy::{AlwaysRetry, RetryPolicyExt};
        let builder = ClientBuilder::new()
            .with_endpoint("test-endpoint.com")
            .with_universe_domain("test-universe.com")
            .with_credentials(Anonymous::new().build())
            .with_tracing();

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
