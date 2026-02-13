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

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, ready};
use tokio::sync::oneshot;

/// A [`Future`] representing an in-flight publish operation.
///
/// This is returned by [`Publisher::publish`](crate::client::Publisher::publish).
/// Awaiting this future returns the server-assigned message ID on success, or an
/// error if the publish failed.
///
/// # Example
///
/// ```
/// # use google_cloud_pubsub::client::Publisher;
/// # use google_cloud_pubsub::model::Message;
/// # async fn sample(publisher: Publisher) -> anyhow::Result<()> {
/// // publish() returns a future immediately.
/// let publish_future = publisher.publish(Message::new().set_data("hello world"));
///
/// // The future can be awaited to get the result.
/// match publish_future.await {
///     Ok(message_id) => println!("Message published with ID: {message_id}"),
///     Err(e) => eprintln!("Failed to publish message: {e:?}"),
/// }
/// # Ok(())
/// # }
/// ```
pub struct PublishFuture {
    pub(crate) rx: oneshot::Receiver<std::result::Result<String, crate::error::PublishError>>,
}

impl Future for PublishFuture {
    /// The result of the publish operation.
    /// - `Ok(String)`: The server-assigned message ID.
    /// - `Err(Arc<Error>)`: An error indicating the publish failed.
    type Output = std::result::Result<String, crate::error::PublishError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let result = ready!(Pin::new(&mut self.rx).poll(cx));
        // An error will only occur if the sender of the self.rx was dropped,
        // which can happen when the Dispatcher is dropped.
        match result {
            Ok(result) => Poll::Ready(result),
            Err(_) => Poll::Ready(Err(crate::error::PublishError::ShutdownError(()))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn resolve_publish_future_success() -> anyhow::Result<()> {
        let (tx, rx) = oneshot::channel();
        let handle = PublishFuture { rx };
        let _ = tx.send(Ok("message_id".to_string()));
        assert_eq!(handle.await?, "message_id");

        Ok(())
    }

    #[tokio::test]
    async fn resolve_publish_future_error() -> anyhow::Result<()> {
        let (tx, rx) = oneshot::channel();
        let fut = PublishFuture { rx };
        let _ = tx.send(Err(crate::error::PublishError::OrderingKeyPaused(())));
        let res = fut.await;
        assert!(
            matches!(res, Err(crate::error::PublishError::OrderingKeyPaused(()))),
            "{res:?}"
        );

        Ok(())
    }

    #[tokio::test]
    async fn resolve_publish_future_error_send_error() -> anyhow::Result<()> {
        let (tx, rx) = oneshot::channel();
        let fut = PublishFuture { rx };
        drop(tx);
        let res = fut.await;
        assert!(
            matches!(res, Err(crate::error::PublishError::ShutdownError(()))),
            "{res:?}"
        );

        Ok(())
    }
}
