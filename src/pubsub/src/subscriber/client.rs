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

use super::builder::Subscribe;
use super::client_builder::ClientBuilder;
use super::transport::Transport;
use crate::ClientBuilderResult as BuilderResult;
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
/// let mut stream = client
///     .subscribe("projects/my-project/subscriptions/my-subscription")
///     .build();
/// while let Some((m, h)) = stream.next().await.transpose()? {
///     println!("Received message m={m:?}");
///     h.ack();
/// }
/// # Ok(()) }
/// ```
///
/// # Ordered Delivery
///
/// The subscriber returns messages in order if [ordered delivery] is enabled on
/// the subscription. The client provides the same guarantees as the service.
///
/// For more details on how the service works, see:
///
/// - [Considerations when using ordered delivery][considerations]
/// - [Google Cloud Pub/Sub Ordered Delivery][medium]
///
/// [considerations]: https://docs.cloud.google.com/pubsub/docs/ordering#considerations_when_using_ordered_messaging
/// [medium]: https://medium.com/google-cloud/google-cloud-pub-sub-ordered-delivery-1e4181f60bc8
/// [ordered delivery]: https://docs.cloud.google.com/pubsub/docs/ordering
///
/// # Exactly-once Delivery
///
/// The subscriber supports [exactly-once] delivery.
///
/// If you enable exactly-once delivery for a subscription, your application
/// can be opinionated about the delivery type, by destructuring the handler
/// into its [`Handler::ExactlyOnce`][eo-branch] branch.
///
/// ```
/// use google_cloud_pubsub::subscriber::MessageStream;
/// use google_cloud_pubsub::subscriber::handler::Handler;
/// async fn exactly_once_stream(mut stream: MessageStream) -> anyhow::Result<()> {
///   while let Some((m, Handler::ExactlyOnce(h))) = stream.next().await.transpose()? {
///       println!("Received message m={m:?}");
///
///       // Await the result of the ack. Typically you would not block the loop
///       // with an `await` point like this.
///       h.confirmed_ack().await?;
///   }
///   unreachable!("Oops, my subscription must have at-least-once semantics")
/// }
/// ```
///
/// You should not change the delivery type of a subscription midstream. If you
/// do, the subscriber will honor the delivery setting at the time each message
/// was received.
///
/// [exactly-once]: https://docs.cloud.google.com/pubsub/docs/exactly-once-delivery
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
/// [eo-branch]: crate::subscriber::handler::Handler::ExactlyOnce
/// [private google access with vpc service controls]: https://cloud.google.com/vpc-service-controls/docs/private-connectivity
/// [pull subscription]: https://docs.cloud.google.com/pubsub/docs/pull
/// [with_endpoint()]: ClientBuilder::with_endpoint
/// [with_credentials()]: ClientBuilder::with_credentials
#[derive(Clone, Debug)]
pub struct Subscriber {
    inner: Arc<Transport>,
    client_id: String,
    grpc_subchannel_count: usize,
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
    /// let mut stream = client
    ///     .subscribe("projects/my-project/subscriptions/my-subscription")
    ///     .build();
    /// while let Some((m, h)) = stream.next().await.transpose()? {
    ///     println!("Received message m={m:?}");
    ///     h.ack();
    /// }
    /// # Ok(()) }
    /// ```
    ///
    /// [subscription]: https://docs.cloud.google.com/pubsub/docs/subscription-overview
    pub fn subscribe<T>(&self, subscription: T) -> Subscribe
    where
        T: Into<String>,
    {
        Subscribe::new(
            self.inner.clone(),
            subscription.into(),
            self.client_id.clone(),
            self.grpc_subchannel_count,
        )
    }

    pub(super) async fn new(builder: ClientBuilder) -> BuilderResult<Self> {
        let grpc_subchannel_count =
            std::cmp::max(1, builder.config.grpc_subchannel_count.unwrap_or(1));
        let transport = Transport::new(builder.config).await?;
        Ok(Self {
            inner: Arc::new(transport),
            client_id: uuid::Uuid::new_v4().to_string(),
            grpc_subchannel_count,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gaxi::grpc::tonic::Status as TonicStatus;
    use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
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
            .return_once(|_| Err(TonicStatus::failed_precondition("fail")));
        let (endpoint, _server) = start("0.0.0.0:0", mock).await?;
        let client = Subscriber::builder()
            .with_endpoint(endpoint)
            .with_credentials(Anonymous::new().build())
            .build()
            .await?;
        let err = client
            .subscribe("projects/p/subscriptions/s")
            .build()
            .next()
            .await
            .expect("stream should not be empty")
            .expect_err("the first streamed item should be an error");
        assert!(err.status().is_some(), "{err:?}");
        let status = err.status().unwrap();
        assert_eq!(
            status.code,
            google_cloud_gax::error::rpc::Code::FailedPrecondition
        );
        assert_eq!(status.message, "fail");

        Ok(())
    }

    #[tokio::test]
    async fn grpc_subchannel_count() -> anyhow::Result<()> {
        let client = Subscriber::builder()
            .with_credentials(Anonymous::new().build())
            .build()
            .await?;
        assert_eq!(client.grpc_subchannel_count, 1);

        let client = Subscriber::builder()
            .with_credentials(Anonymous::new().build())
            .with_grpc_subchannel_count(0)
            .build()
            .await?;
        assert_eq!(client.grpc_subchannel_count, 1);

        let client = Subscriber::builder()
            .with_credentials(Anonymous::new().build())
            .with_grpc_subchannel_count(8)
            .build()
            .await?;
        assert_eq!(client.grpc_subchannel_count, 8);

        let builder = client.subscribe("projects/p/subscriptions/s");
        assert_eq!(builder.grpc_subchannel_count, 8);

        Ok(())
    }
}
