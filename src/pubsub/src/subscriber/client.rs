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

use super::client_builder::ClientBuilder;
use super::transport::Transport;
use gax::client_builder::Result as BuilderResult;
use std::sync::Arc;

/// A Subscriber client for the [Cloud Pub/Sub] API.
///
/// Use this client to receive messages from a [pull subscription] on a topic.
///
// TODO(#3941) - add basic subscribe example
//
/// # Configuration
///
/// To configure a `Subscriber` use the `with_*` methods in the type returned by
/// [builder()][Storage::builder]. The default configuration should work for
/// most applications. Common configuration changes include:
///
/// * [with_endpoint()]: by default this client uses the global default endpoint
///   (`https://pubsub.googleapis.com`). Applications using regional endpoints
///   or running in restricted networks (e.g. a network configured with
///   [Private Google Access with VPC Service Controls]) may want to override
///   this default.
/// * [with_credentials()]: by default this client uses
///   [Application Default Credentials]. Applications using custom
///   authentication may need to override this default.
///
/// # Pooling and Cloning
///
/// `Subscriber` holds a connection pool internally, it is advised to
/// create one and then reuse it.  You do not need to wrap `Subscriber` in
/// an [Rc](std::rc::Rc) or [Arc] to reuse it, because it already uses an `Arc`
/// internally.
///
/// [application default credentials]: https://cloud.google.com/docs/authentication#adc
/// [cloud pub/sub]: https://docs.cloud.google.com/pubsub/docs/overview
/// [private google access with vpc service controls]: https://cloud.google.com/vpc-service-controls/docs/private-connectivity
/// [pull subscription]: https://docs.cloud.google.com/pubsub/docs/pull
/// [with_endpoint()]: ClientBuilder::with_endpoint
/// [with_credentials()]: ClientBuilder::with_credentials
#[derive(Clone, Debug)]
pub struct Subscriber {
    inner: Arc<Transport>,
}

impl Subscriber {
    /// Returns a builder for [Subscriber].
    ///
    /// # Example
    /// ```no_rust
    /// # use google_cloud_pubsub::client::Subscriber;
    /// # async fn sample() -> anyhow::Result<()> {
    /// let client = Subscriber::builder().build().await?;
    /// # Ok(()) }
    /// ```
    pub fn builder() -> ClientBuilder {
        ClientBuilder::new()
    }

    pub(super) async fn new(builder: ClientBuilder) -> BuilderResult<Self> {
        let transport = Transport::new(builder.config).await?;
        Ok(Self {
            inner: Arc::new(transport),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn basic() -> anyhow::Result<()> {
        let _ = Subscriber::builder().build().await?;
        Ok(())
    }
}
