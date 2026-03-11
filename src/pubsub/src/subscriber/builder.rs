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

use super::MessageStream;
use super::ShutdownToken;
use super::transport::Transport;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

const MIB: i64 = 1024 * 1024;

pub use super::client_builder::ClientBuilder;

/// Builder for the [`client::Subscriber::subscribe`][crate::client::Subscriber::subscribe] method.
pub struct Subscribe {
    pub(super) inner: Arc<Transport>,
    pub(super) subscription: String,
    pub(super) client_id: String,
    pub(super) grpc_subchannel_count: usize,
    pub(super) ack_deadline_seconds: i32,
    pub(super) max_outstanding_messages: i64,
    pub(super) max_outstanding_bytes: i64,
    pub(super) shutdown: CancellationToken,
}

impl Subscribe {
    pub(super) fn new(
        inner: Arc<Transport>,
        subscription: String,
        client_id: String,
        grpc_subchannel_count: usize,
    ) -> Self {
        Self {
            inner,
            subscription,
            client_id,
            grpc_subchannel_count,
            ack_deadline_seconds: 10,
            max_outstanding_messages: 1000,
            max_outstanding_bytes: 100 * MIB,
            shutdown: CancellationToken::new(),
        }
    }

    /// Creates a new stream to receive messages from the subscription.
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
    /// Note that the underlying connection with the server is lazy-initialized.
    /// It is not established until [`MessageStream::next()`] is called.
    pub fn build(self) -> MessageStream {
        MessageStream::new(self)
    }

    /// Sets the maximum duration to extend lease deadlines by.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_pubsub::client::Subscriber;
    /// # use std::time::Duration;
    /// # async fn sample() -> anyhow::Result<()> {
    /// # let client = Subscriber::builder().build().await?;
    /// let stream = client.subscribe("projects/my-project/subscriptions/my-subscription")
    ///     .set_max_lease_extension(Duration::from_secs(20))
    ///     .build();
    /// # Ok(()) }
    /// ```
    ///
    /// The client extends lease deadlines by at most this amount.
    ///
    /// If the server does not hear back from the client within this deadline
    /// (e.g. if an application crashes), it will resend any unacknowledged
    /// messages to another subscriber.
    ///
    /// Note that this value is independent of the ack deadline configured on
    /// the subscription.
    ///
    /// The minimum deadline you can specify is 10 seconds. The maximum deadline
    /// you can specify is 10 minutes. The client clamps the supplied value to
    /// this range.
    ///
    /// The default value is 10 seconds.
    pub fn set_max_lease_extension<T: Into<Duration>>(mut self, v: T) -> Self {
        self.ack_deadline_seconds = v.into().as_secs().clamp(10, 600) as i32;
        self
    }

    /// Flow control settings for the maximum number of outstanding messages.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_pubsub::client::Subscriber;
    /// # async fn sample() -> anyhow::Result<()> {
    /// # let client = Subscriber::builder().build().await?;
    /// let stream = client.subscribe("projects/my-project/subscriptions/my-subscription")
    ///     .set_max_outstanding_messages(2000)
    ///     .build();
    /// # Ok(()) }
    /// ```
    ///
    /// The server will stop sending messages to a client when this many
    /// messages are outstanding (i.e. that have not been acked). The server
    /// resumes sending messages when the outstanding message count drops below
    /// this value.
    ///
    /// The limit applies per-stream. It is not a global limit.
    ///
    /// Use a value <= 0 to set no limit on the number of outstanding messages.
    ///
    /// The default value is 1000 messages.
    pub fn set_max_outstanding_messages<T: Into<i64>>(mut self, v: T) -> Self {
        self.max_outstanding_messages = v.into();
        self
    }

    /// Flow control settings for the maximum number of outstanding bytes.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_pubsub::client::Subscriber;
    /// # async fn sample() -> anyhow::Result<()> {
    /// # let client = Subscriber::builder().build().await?;
    /// const MIB: i64 = 1024 * 1024;
    /// let stream = client.subscribe("projects/my-project/subscriptions/my-subscription")
    ///     .set_max_outstanding_bytes(200 * MIB)
    ///     .build();
    /// # Ok(()) }
    /// ```
    ///
    /// The server will stop sending messages to a client when this many bytes
    /// of messages are outstanding (i.e. that have not been acked). The server
    /// resumes sending messages when the outstanding byte count drops below
    /// this value.
    ///
    /// The limit applies per-stream. It is not a global limit.
    ///
    /// Use a value <= 0 to set no limit on the number of outstanding bytes.
    ///
    /// The default value is 100 MiB.
    pub fn set_max_outstanding_bytes<T: Into<i64>>(mut self, v: T) -> Self {
        self.max_outstanding_bytes = v.into();
        self
    }

    /// Supplies a token to signal a stream shutdown.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_pubsub::client::Subscriber;
    /// # use google_cloud_pubsub::subscriber::ShutdownToken;
    /// # use std::time::Duration;
    /// # async fn sample() -> anyhow::Result<()> {
    /// # let client = Subscriber::builder().build().await?;
    /// let token = ShutdownToken::new();
    /// let stream = client.subscribe("projects/my-project/subscriptions/my-subscription")
    ///     .set_shutdown_token(token)
    ///     .build();
    /// # Ok(()) }
    /// ```
    pub fn set_shutdown_token(mut self, token: ShutdownToken) -> Self {
        self.shutdown = token.inner;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gaxi::options::ClientConfig;
    use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
    use test_case::test_case;

    const KIB: i64 = 1024;

    async fn test_inner() -> anyhow::Result<Arc<Transport>> {
        let mut config = ClientConfig::default();
        config.cred = Some(Anonymous::new().build());
        let transport = Transport::new(config).await?;
        Ok(Arc::new(transport))
    }

    #[tokio::test]
    async fn reasonable_defaults() -> anyhow::Result<()> {
        let builder = Subscribe::new(
            test_inner().await?,
            "projects/my-project/subscriptions/my-subscription".to_string(),
            "client-id".to_string(),
            1_usize,
        );
        assert_eq!(
            builder.subscription,
            "projects/my-project/subscriptions/my-subscription"
        );
        assert_eq!(builder.grpc_subchannel_count, 1);
        assert_eq!(builder.ack_deadline_seconds, 10);
        assert!(
            100_000 > builder.max_outstanding_messages && builder.max_outstanding_messages > 100,
            "max_outstanding_messages={}",
            builder.max_outstanding_messages
        );
        assert!(
            builder.max_outstanding_bytes > 100 * KIB,
            "max_outstanding_bytes={}",
            builder.max_outstanding_bytes
        );

        Ok(())
    }

    #[tokio::test]
    async fn options() -> anyhow::Result<()> {
        let builder = Subscribe::new(
            test_inner().await?,
            "projects/my-project/subscriptions/my-subscription".to_string(),
            "client-id".to_string(),
            2_usize,
        )
        .set_max_lease_extension(Duration::from_secs(20))
        .set_max_outstanding_messages(12345)
        .set_max_outstanding_bytes(6789 * KIB);
        assert_eq!(
            builder.subscription,
            "projects/my-project/subscriptions/my-subscription"
        );
        assert_eq!(builder.grpc_subchannel_count, 2);
        assert_eq!(builder.ack_deadline_seconds, 20);
        assert_eq!(builder.max_outstanding_messages, 12345);
        assert_eq!(builder.max_outstanding_bytes, 6789 * KIB);

        Ok(())
    }

    #[test_case(Duration::ZERO, 10)]
    #[test_case(Duration::from_secs(42), 42)]
    #[test_case(Duration::from_secs(4200), 600)]
    #[tokio::test]
    async fn clamp_ack_deadline(v: Duration, want: i32) -> anyhow::Result<()> {
        let builder = Subscribe::new(
            test_inner().await?,
            "projects/my-project/subscriptions/my-subscription".to_string(),
            "client-id".to_string(),
            1_usize,
        )
        .set_max_lease_extension(v);
        assert_eq!(builder.ack_deadline_seconds, want);

        Ok(())
    }

    #[tokio::test]
    async fn shutdown_token() -> anyhow::Result<()> {
        let token = ShutdownToken::new();
        let builder = Subscribe::new(
            test_inner().await?,
            "projects/my-project/subscriptions/my-subscription".to_string(),
            "client-id".to_string(),
            2_usize,
        )
        .set_shutdown_token(token.clone());
        assert!(!builder.shutdown.is_cancelled(), "{:?}", builder.shutdown);

        Ok(())
    }
}
