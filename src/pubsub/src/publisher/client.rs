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

impl Publisher {
    /// Creates a new Pub/Sub publisher client with default configuration.
    pub async fn new() -> Result<Self, gax::client_builder::Error> {
        let config = gaxi::options::ClientConfig::default();
        Self::with_config(config).await
    }

    /// Creates a new Pub/Sub publisher client with the given configuration.
    pub async fn with_config(
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

// NOWNOW: Add mod tests.
