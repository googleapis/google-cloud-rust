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
use std::sync::Arc;
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
/// # use google_cloud_pubsub::model::PubsubMessage;
/// # async fn sample(publisher: Publisher) -> anyhow::Result<()> {
/// // publish() returns a future immediately.
/// let publish_future = publisher.publish(PubsubMessage::new().set_data("hello world"));
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
    type Output = std::result::Result<String, Arc<crate::Error>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let result = ready!(Pin::new(&mut self.rx).poll(cx));
        // An error will only occur if the sender of the self.rx was dropped,
        // which can happen when the background worker is dropped.
        match result {
            Ok(result) => Poll::Ready(result.map_err(convert_error)),
            Err(_) => Poll::Ready(Err(Arc::new(google_cloud_gax::error::Error::io(
                "publisher is shutdown",
            )))),
        }
    }
}

fn convert_error(e: crate::error::PublishError) -> Arc<crate::Error> {
    // TODO(#3689): The error type for these are not ideal, we will need will
    // need to handle error propagation better.
    match e {
        crate::error::PublishError::SendError(s) => s,
        crate::error::PublishError::OrderingKeyPaused(e) => Arc::new(crate::Error::io(
            crate::error::PublishError::OrderingKeyPaused(e),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn resolve_publish_future_success() {
        let (tx, rx) = oneshot::channel();
        let handle = PublishFuture { rx };
        let _ = tx.send(Ok("message_id".to_string()));
        let id = handle.await.expect("should have received a message ID");
        assert_eq!(id, "message_id");
    }

    #[tokio::test]
    async fn resolve_publish_future_error() {
        use std::error::Error as _;

        let (tx, rx) = oneshot::channel();
        let fut = PublishFuture { rx };
        let _ = tx.send(Err(crate::error::PublishError::OrderingKeyPaused(())));
        let err = fut
            .await
            .expect_err("errors on channel should resolve to error");
        let err = err
            .source()
            .unwrap()
            .downcast_ref::<crate::error::PublishError>()
            .unwrap();
        match err {
            crate::error::PublishError::OrderingKeyPaused(_) => {}
            _ => panic!("expected OrderingKeyPaused error"),
        }
    }

    #[tokio::test]
    async fn resolve_publish_future_error_send_error() {
        let (tx, rx) = oneshot::channel();
        let fut = PublishFuture { rx };
        drop(tx);
        let err = fut
            .await
            .expect_err("dropped channel should resolve to error");
        assert!(err.to_string().contains("shutdown"));
    }
}
