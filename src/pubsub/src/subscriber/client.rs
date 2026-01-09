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

use super::builder::StreamingPull;
use super::client_builder::ClientBuilder;
use super::transport::Transport;
use gax::client_builder::Result as BuilderResult;
use std::sync::Arc;

/// A Subscriber client for the [Cloud Pub/Sub] API.
///
/// Use this client to receive messages from a [pull subscription] on a topic.
///
/// # Example
/// ```
/// # use google_cloud_pubsub::client::Subscriber;
/// # async fn sample() -> anyhow::Result<()> {
/// let client = Subscriber::builder().build().await?;
/// let mut session = client
///     .streaming_pull("projects/my-project/subscriptions/my-subscription")
///     .start()
///     .await?;
/// while let Some((m, h)) = session.next().await.transpose()? {
///     println!("Received message m={m:?}");
///     h.ack();
/// }
/// # Ok(()) }
/// ```
///
/// # Configuration
///
/// To configure a `Subscriber` use the `with_*` methods in the type returned by
/// [builder()][Subscriber::builder]. The default configuration should work for
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
    /// ```
    /// # use google_cloud_pubsub::client::Subscriber;
    /// # async fn sample() -> anyhow::Result<()> {
    /// let client = Subscriber::builder().build().await?;
    /// # Ok(()) }
    /// ```
    pub fn builder() -> ClientBuilder {
        ClientBuilder::new()
    }

    /// Receive messages from a [subscription].
    ///
    /// The `subscription` is the full name, in the format of
    /// `projects/*/subscriptions/*`.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_pubsub::client::Subscriber;
    /// # async fn sample(client: Subscriber) -> anyhow::Result<()> {
    /// let mut session = client
    ///     .streaming_pull("projects/my-project/subscriptions/my-subscription")
    ///     .start()
    ///     .await?;
    /// while let Some((m, h)) = session.next().await.transpose()? {
    ///     println!("Received message m={m:?}");
    ///     h.ack();
    /// }
    /// # Ok(()) }
    /// ```
    ///
    /// [subscription]: https://docs.cloud.google.com/pubsub/docs/subscription-overview
    pub fn streaming_pull<T>(&self, subscription: T) -> StreamingPull
    where
        T: Into<String>,
    {
        StreamingPull::new(self.inner.clone(), subscription.into())
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
    use auth::credentials::anonymous::Builder as Anonymous;
    use pubsub_grpc_mock::{MockSubscriber, start};

    #[tokio::test]
    async fn basic() -> anyhow::Result<()> {
        let _ = Subscriber::builder().build().await?;
        Ok(())
    }

    #[tokio::test]
    async fn streaming_pull() -> anyhow::Result<()> {
        let mut mock = MockSubscriber::new();
        mock.expect_streaming_pull()
            .return_once(|_| Err(tonic::Status::internal("fail")));
        let (endpoint, _server) = start("0.0.0.0:0", mock).await?;
        let client = Subscriber::builder()
            .with_endpoint(endpoint)
            .with_credentials(Anonymous::new().build())
            .build()
            .await?;
        let err = client
            .streaming_pull("projects/p/subscriptions/s")
            .start()
            .await
            .expect_err("Session should not be created.");
        assert!(err.status().is_some(), "{err:?}");
        let status = err.status().unwrap();
        assert_eq!(status.code, gax::error::rpc::Code::Internal);
        assert_eq!(status.message, "fail");

        Ok(())
    }
}
