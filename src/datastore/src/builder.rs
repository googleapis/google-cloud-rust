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

//! Client and request builders for Cloud Datastore.

use crate::ClientBuilderResult;
use crate::Result;
use crate::client::Datastore;
use crate::key::{IncompleteKey, Key};
use google_cloud_auth::credentials::Credentials;
use google_cloud_gax::backoff_policy::BackoffPolicyArg;
use google_cloud_gax::retry_policy::RetryPolicyArg;
use google_cloud_gax::retry_throttler::RetryThrottlerArg;

/// Builder for constructing a [`Datastore`] client.
#[derive(Clone, Debug)]
pub struct ClientBuilder {
    pub(crate) project_id: String,
    pub(crate) database_id: String,
    pub(crate) namespace_id: String,
    pub(crate) config: gaxi::options::ClientConfig,
}

impl ClientBuilder {
    pub(crate) fn new(project_id: impl Into<String>) -> Self {
        let mut config = gaxi::options::ClientConfig::default();
        config.retry_policy = Some(std::sync::Arc::new(
            crate::retry_policy::default_retry_policy(),
        ));
        config.backoff_policy = Some(std::sync::Arc::new(
            crate::retry_policy::default_backoff_policy(),
        ));
        Self {
            project_id: project_id.into(),
            database_id: String::new(),
            namespace_id: String::new(),
            config,
        }
    }

    /// Sets the target Datastore database ID (defaults to `""` for the `(default)` database).
    pub fn with_database_id(mut self, database_id: impl Into<String>) -> Self {
        self.database_id = database_id.into();
        self
    }

    /// Sets the default multi-tenant namespace ID applied to keys and queries
    /// created through this client (defaults to `""`).
    pub fn with_namespace_id(mut self, namespace_id: impl Into<String>) -> Self {
        self.namespace_id = namespace_id.into();
        self
    }

    /// Sets a custom service endpoint URL (defaults to `https://datastore.googleapis.com`).
    pub fn with_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.config.endpoint = Some(endpoint.into());
        self
    }

    /// Configures explicit authentication [Credentials].
    pub fn with_credentials(mut self, credentials: impl Into<Credentials>) -> Self {
        self.config.cred = Some(credentials.into());
        self
    }

    /// Configures the RPC retry policy.
    pub fn with_retry_policy(mut self, policy: impl Into<RetryPolicyArg>) -> Self {
        self.config.retry_policy = Some(policy.into().into());
        self
    }

    /// Configures the RPC exponential backoff policy.
    pub fn with_backoff_policy(mut self, policy: impl Into<BackoffPolicyArg>) -> Self {
        self.config.backoff_policy = Some(policy.into().into());
        self
    }

    /// Configures the RPC retry throttling policy.
    pub fn with_retry_throttling(mut self, policy: impl Into<RetryThrottlerArg>) -> Self {
        self.config.retry_throttler = policy.into().into();
        self
    }

    /// Enables OpenTelemetry tracing on the underlying HTTP transport.
    pub fn with_tracing(mut self) -> Self {
        self.config.tracing = true;
        self
    }

    /// Builds the [`Datastore`] client.
    pub async fn build(self) -> ClientBuilderResult<Datastore> {
        Datastore::from_builder(self).await
    }
}

/// Request builder for [`Datastore::allocate_ids`].
#[derive(Clone, Debug)]
#[allow(dead_code)]
pub struct AllocateIdsBuilder {
    pub(crate) client: Datastore,
    pub(crate) keys: Vec<IncompleteKey>,
    pub(crate) options: crate::RequestOptions,
}

impl AllocateIdsBuilder {
    /// Overrides the per-request [RequestOptions][google_cloud_gax::options::RequestOptions].
    pub fn with_options(mut self, options: impl Into<crate::RequestOptions>) -> Self {
        self.options = options.into();
        self
    }

    /// Sends the `AllocateIds` request and returns the newly allocated complete [Key]s.
    pub async fn send(self) -> Result<Vec<Key>> {
        unimplemented!()
    }
}

/// Request builder for [`Datastore::reserve_ids`] and [`Datastore::reserve_ids_sequential`].
#[derive(Clone, Debug)]
#[allow(dead_code)]
pub struct ReserveIdsBuilder {
    pub(crate) client: Datastore,
    pub(crate) keys: Vec<Key>,
    pub(crate) options: crate::RequestOptions,
}

impl ReserveIdsBuilder {
    /// Overrides the per-request [RequestOptions][google_cloud_gax::options::RequestOptions].
    pub fn with_options(mut self, options: impl Into<crate::RequestOptions>) -> Self {
        self.options = options.into();
        self
    }

    /// Sends the `ReserveIds` request.
    pub async fn send(self) -> Result<()> {
        unimplemented!()
    }
}
