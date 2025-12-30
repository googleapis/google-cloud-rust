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

use super::client::Subscriber;
use gax::client_builder::Result as BuilderResult;
use gaxi::options::ClientConfig;

/// A builder for [Subscriber].
///
/// ```no_rust
/// # use google_cloud_pubsub::client::Subscriber;
/// # async fn sample() -> anyhow::Result<()> {
/// let builder = Subscriber::builder();
/// let client = builder
///     .with_endpoint("https://pubsub.googleapis.com")
///     .build()
///     .await?;
/// # Ok(()) }
/// ```
pub struct ClientBuilder {
    pub(crate) config: ClientConfig,
}

impl ClientBuilder {
    pub(crate) fn new() -> Self {
        Self {
            config: ClientConfig::default(),
        }
    }

    /// Creates a new client.
    ///
    /// # Example
    /// ```no_rust
    /// # use google_cloud_pubsub::client::Subscriber;
    /// # async fn sample() -> anyhow::Result<()> {
    /// let client = Subscriber::builder().build().await?;
    /// # Ok(()) }
    /// ```
    pub async fn build(self) -> BuilderResult<Subscriber> {
        Subscriber::new(self).await
    }

    /// Sets the endpoint.
    ///
    /// # Example
    /// ```no_rust
    /// # use google_cloud_pubsub::client::Subscriber;
    /// # async fn sample() -> anyhow::Result<()> {
    /// let client = Subscriber::builder()
    ///     .with_endpoint("https://private.googleapis.com")
    ///     .build()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    pub fn with_endpoint<V: Into<String>>(mut self, v: V) -> Self {
        self.config.endpoint = Some(v.into());
        self
    }

    /// Configures the authentication credentials.
    ///
    /// More information about valid credentials types can be found in the
    /// [google-cloud-auth] crate documentation.
    ///
    /// # Example
    /// ```no_rust
    /// # use google_cloud_pubsub::client::Subscriber;
    /// # async fn sample() -> anyhow::Result<()> {
    /// use auth::credentials::mds;
    /// let client = Subscriber::builder()
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
    pub fn with_credentials<V: Into<auth::credentials::Credentials>>(mut self, v: V) -> Self {
        self.config.cred = Some(v.into());
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use auth::credentials::anonymous::Builder as Anonymous;

    #[test]
    fn defaults() {
        let builder = ClientBuilder::new();
        assert!(builder.config.endpoint.is_none());
        assert!(builder.config.cred.is_none());
    }

    #[test]
    fn setters() {
        let builder = ClientBuilder::new()
            .with_endpoint("test-endpoint.com")
            .with_credentials(Anonymous::new().build());
        assert_eq!(
            builder.config.endpoint,
            Some("test-endpoint.com".to_string())
        );
        assert!(builder.config.cred.is_some());
    }
}
