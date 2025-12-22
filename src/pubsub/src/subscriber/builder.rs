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

use super::stub::Stub;
use std::sync::Arc;

/// Builder for the `client::Subscriber::streaming_pull` method.
pub struct StreamingPull<S>
where
    S: Stub,
{
    // TODO(#4061) - Use a dynamic stub to remove the generic.
    pub(crate) inner: Arc<S>,
    pub(crate) subscription: String,
    pub(crate) ack_deadline_seconds: i32,
    pub(crate) max_outstanding_messages: i64,
    pub(crate) max_outstanding_bytes: i64,
}

impl<S> StreamingPull<S>
where
    S: Stub,
{
    pub(crate) fn new(inner: Arc<S>, subscription: String) -> Self {
        Self {
            inner,
            subscription,
            ack_deadline_seconds: 10,
            max_outstanding_messages: 1000,
            max_outstanding_bytes: 100 * 1024 * 1024,
        }
    }

    /// Sets the ack deadline to use for the stream.
    ///
    /// This value represents how long the application has to ack or nack an
    /// incoming message. Note that this value is independent of the deadline
    /// configured on the server-side subscription.
    ///
    /// The minimum deadline you can specify is 10 seconds. The maximum deadline
    /// you can specify is 600 seconds (10 minutes).
    ///
    /// The default value is 10 seconds.
    ///
    /// To use the server-side subscription deadline, specify a value of `0`.
    ///
    /// # Example
    ///
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
    /// # Example
    ///
    /// ```no_rust
    /// # use google_cloud_pubsub::client::Subscriber;
    /// # async fn sample() -> anyhow::Result<()> {
    /// # let client = Subscriber::builder().build().await?;
    /// let session = client.streaming_pull("projects/my-project/subscriptions/my-subscription")
    ///     .set_max_outstanding_messages(100000)
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
    /// # Example
    ///
    /// ```no_rust
    /// # use google_cloud_pubsub::client::Subscriber;
    /// # async fn sample() -> anyhow::Result<()> {
    /// # let client = Subscriber::builder().build().await?;
    /// let session = client.streaming_pull("projects/my-project/subscriptions/my-subscription")
    ///     .set_max_outstanding_bytes(1024 * 1024 * 1024)
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
    use super::super::stub::tests::MockStub;
    use super::*;

    #[test]
    fn reasonable_defaults() {
        let mock = MockStub::new();
        let builder = StreamingPull::new(
            Arc::new(mock),
            "projects/my-project/subscriptions/my-subscription".to_string(),
        );
        assert_eq!(
            builder.subscription,
            "projects/my-project/subscriptions/my-subscription"
        );
        assert_eq!(builder.ack_deadline_seconds, 10);
        assert!(
            1000000 > builder.max_outstanding_messages && builder.max_outstanding_messages > 100,
            "{}",
            builder.max_outstanding_messages
        );
        assert!(
            builder.max_outstanding_bytes > 100000,
            "{}",
            builder.max_outstanding_messages
        );
    }

    #[test]
    fn options() {
        let mock = MockStub::new();
        let builder = StreamingPull::new(
            Arc::new(mock),
            "projects/my-project/subscriptions/my-subscription".to_string(),
        )
        .set_ack_deadline_seconds(20)
        .set_max_outstanding_messages(12345)
        .set_max_outstanding_bytes(6789000);
        assert_eq!(
            builder.subscription,
            "projects/my-project/subscriptions/my-subscription"
        );
        assert_eq!(builder.ack_deadline_seconds, 20);
        assert_eq!(builder.max_outstanding_messages, 12345);
        assert_eq!(builder.max_outstanding_bytes, 6789000);
    }
}
