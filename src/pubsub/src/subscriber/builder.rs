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

use super::session::Session;
use super::transport::Transport;
use crate::Result;
use std::sync::Arc;

const MIB: i64 = 1024 * 1024;

/// Builder for the `client::Subscriber::streaming_pull` method.
pub struct StreamingPull {
    pub(super) inner: Arc<Transport>,
    pub(super) subscription: String,
    pub(super) ack_deadline_seconds: i32,
    pub(super) max_outstanding_messages: i64,
    pub(super) max_outstanding_bytes: i64,
}

impl StreamingPull {
    pub(super) fn new(inner: Arc<Transport>, subscription: String) -> Self {
        Self {
            inner,
            subscription,
            ack_deadline_seconds: 10,
            max_outstanding_messages: 1000,
            max_outstanding_bytes: 100 * MIB,
        }
    }

    /// Creates a new session to receive messages from the subscription.
    ///
    /// # Example
    /// ```no_rust
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
    pub async fn start(self) -> Result<Session> {
        Session::new(self).await
    }

    /// Sets the ack deadline to use for the stream.
    ///
    /// This value represents how long the application has to ack or nack an
    /// incoming message. Note that this value is independent of the deadline
    /// configured on the server-side subscription.
    ///
    /// If the server does not hear back from the client within this deadline
    /// (e.g. if an application crashes), it will resend any unacknowledged
    /// messages to another subscriber.
    ///
    /// The minimum deadline you can specify is 10 seconds. The maximum deadline
    /// you can specify is 600 seconds (10 minutes).
    ///
    /// The default value is 10 seconds.
    ///
    /// # Example
    /// ```no_rust
    /// # use google_cloud_pubsub::client::Subscriber;
    /// # async fn sample() -> anyhow::Result<()> {
    /// # let client = Subscriber::builder().build().await?;
    /// let session = client.streaming_pull("projects/my-project/subscriptions/my-subscription")
    ///     .set_ack_deadline_seconds(20)
    ///     .start();
    /// # Ok(()) }
    /// ```
    pub fn set_ack_deadline_seconds<T: Into<i32>>(mut self, v: T) -> Self {
        self.ack_deadline_seconds = v.into();
        self
    }

    /// Flow control settings for the maximum number of outstanding messages.
    ///
    /// The server will stop sending messages to a client when this many
    /// messages are outstanding (i.e. that have not been acked or nacked).
    ///
    /// The server resumes sending messages when the outstanding message count
    /// drops below this value.
    ///
    /// Use a value <= 0 to set no limit on the number of outstanding messages.
    ///
    /// The default value is 1000 messages.
    ///
    /// # Example
    /// ```no_rust
    /// # use google_cloud_pubsub::client::Subscriber;
    /// # async fn sample() -> anyhow::Result<()> {
    /// # let client = Subscriber::builder().build().await?;
    /// let session = client.streaming_pull("projects/my-project/subscriptions/my-subscription")
    ///     .set_max_outstanding_messages(2000)
    ///     .start();
    /// # Ok(()) }
    /// ```
    pub fn set_max_outstanding_messages<T: Into<i64>>(mut self, v: T) -> Self {
        self.max_outstanding_messages = v.into();
        self
    }

    /// Flow control settings for the maximum number of outstanding bytes.
    ///
    /// The server will stop sending messages to a client when this many bytes
    /// of messages are outstanding (i.e. that have not been acked or nacked).
    ///
    /// The server resumes sending messages when the outstanding byte count
    /// drops below this value.
    ///
    /// Use a value <= 0 to set no limit on the number of outstanding bytes.
    ///
    /// The default value is 100 MiB.
    ///
    /// # Example
    /// ```no_rust
    /// # use google_cloud_pubsub::client::Subscriber;
    /// # async fn sample() -> anyhow::Result<()> {
    /// # let client = Subscriber::builder().build().await?;
    /// const MIB: i64 = 1024 * 1024;
    /// let session = client.streaming_pull("projects/my-project/subscriptions/my-subscription")
    ///     .set_max_outstanding_bytes(200 * MIB)
    ///     .start();
    /// # Ok(()) }
    /// ```
    pub fn set_max_outstanding_bytes<T: Into<i64>>(mut self, v: T) -> Self {
        self.max_outstanding_bytes = v.into();
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use auth::credentials::anonymous::Builder as Anonymous;
    use gaxi::options::ClientConfig;

    const KIB: i64 = 1024;

    async fn test_inner() -> anyhow::Result<Arc<Transport>> {
        let mut config = ClientConfig::default();
        config.cred = Some(Anonymous::new().build());
        let transport = Transport::new(config).await?;
        Ok(Arc::new(transport))
    }

    #[tokio::test]
    async fn reasonable_defaults() -> anyhow::Result<()> {
        let builder = StreamingPull::new(
            test_inner().await?,
            "projects/my-project/subscriptions/my-subscription".to_string(),
        );
        assert_eq!(
            builder.subscription,
            "projects/my-project/subscriptions/my-subscription"
        );
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
        let builder = StreamingPull::new(
            test_inner().await?,
            "projects/my-project/subscriptions/my-subscription".to_string(),
        )
        .set_ack_deadline_seconds(20)
        .set_max_outstanding_messages(12345)
        .set_max_outstanding_bytes(6789 * KIB);
        assert_eq!(
            builder.subscription,
            "projects/my-project/subscriptions/my-subscription"
        );
        assert_eq!(builder.ack_deadline_seconds, 20);
        assert_eq!(builder.max_outstanding_messages, 12345);
        assert_eq!(builder.max_outstanding_bytes, 6789 * KIB);

        Ok(())
    }
}
