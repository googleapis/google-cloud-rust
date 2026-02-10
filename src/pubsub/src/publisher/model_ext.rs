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
    type Output = std::result::Result<String, Arc<crate::Error>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let result = ready!(Pin::new(&mut self.rx).poll(cx));
        // An error will only occur if the sender of the self.rx was dropped,
        // which would be a bug.
        Poll::Ready(
            result
                .expect("publisher should not close the sender for PublishFuture")
                .map_err(convert_error),
        )
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
