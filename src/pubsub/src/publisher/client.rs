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

/// Client for publishing messages to Pub/Sub topics.
#[derive(Clone, Debug)]
pub struct Publisher {
    pub(crate) inner: crate::generated::gapic_dataplane::client::Publisher,
}

/// A builder for [Publisher].
///
/// ```
/// # async fn sample() -> anyhow::Result<()> {
/// # use google_cloud_pubsub::*;
/// # use builder::publisher::ClientBuilder;
/// # use client::Publisher;
/// let builder : ClientBuilder = Publisher::builder();
/// let client = builder
///     .with_endpoint("https://pubsub.googleapis.com")
///     .build().await?;
/// # Ok(()) }
/// ```
pub type ClientBuilder =
    gax::client_builder::ClientBuilder<client_builder::Factory, gaxi::options::Credentials>;

pub(crate) mod client_builder {
    use super::Publisher;

    pub struct Factory;
    impl gax::client_builder::internal::ClientFactory for Factory {
        type Client = Publisher;
        type Credentials = gaxi::options::Credentials;
        #[allow(unused_mut)]
        async fn build(
            self,
            mut config: gaxi::options::ClientConfig,
        ) -> gax::client_builder::Result<Self::Client> {
            // TODO(#3019): Pubsub default retry policy goes here.
            Self::Client::new(config).await
        }
    }
}

impl Publisher {
    /// Returns a builder for [Publisher].
    ///
    /// ```no_run
    /// # tokio_test::block_on(async {
    /// # use google_cloud_pubsub::client::Publisher;
    /// let client = Publisher::builder().build().await?;
    /// # gax::client_builder::Result::<()>::Ok(()) });
    /// ```
    pub fn builder() -> ClientBuilder {
        gax::client_builder::internal::new_builder(client_builder::Factory)
    }

    /// Creates a new Pub/Sub publisher client with the given configuration.
    pub async fn new(
        config: gaxi::options::ClientConfig,
    ) -> Result<Self, gax::client_builder::Error> {
        let inner = crate::generated::gapic_dataplane::client::Publisher::new(config).await?;
        Ok(Self { inner })
    }

    /// Adds one or more messages to the topic. Returns `NOT_FOUND` if the topic
    /// does not exist.
    pub fn publish(&self) -> crate::builder::publisher::Publish {
        self.inner.publish()
    }
}

#[cfg(test)]
mod tests {
    use super::Publisher;

    #[tokio::test]
    async fn builder() -> anyhow::Result<()> {
        let _ = Publisher::builder().build().await?;
        Ok(())
    }
}
