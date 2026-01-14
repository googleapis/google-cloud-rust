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

/// A handle that represents an in-flight publish operation.
///
/// This struct is a `Future`. You can `.await` it to get the final
/// result of the publish call: either a server-assigned message ID `String`
/// or an `Error` if the publish failed.
///
/// A `PublishHandle` is returned from every call to [`Publisher::publish`][crate::client::Publisher::publish]
///
/// # Example
///
/// ```
/// # use google_cloud_pubsub::client::Publisher;
/// # use google_cloud_pubsub::model::PubsubMessage;
/// # async fn sample(publisher: Publisher) -> anyhow::Result<()> {
/// // publish() returns a handle immediately.
/// let handle = publisher.publish(PubsubMessage::new().set_data("hello world"));
///
/// // The handle can be awaited later to get the result.
/// match handle.await {
///     Ok(message_id) => println!("Message published with ID: {message_id}"),
///     Err(e) => eprintln!("Failed to publish message: {e:?}"),
/// }
/// # Ok(())
/// # }
/// ```
pub struct PublishHandle {
    pub(crate) rx: oneshot::Receiver<std::result::Result<String, crate::error::PublishError>>,
}

impl Future for PublishHandle {
    /// The result of the publish operation.
    /// - `Ok(String)`: The server-assigned message ID.
    /// - `Err(Error)`: An error indicating the publish failed.
    type Output = std::result::Result<String, crate::error::PublishError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let result = ready!(Pin::new(&mut self.rx).poll(cx));
        // An error will only occur if the sender of the self.rx was dropped,
        // which would be a bug.
        Poll::Ready(result.expect("the client library should not release the sender"))
    }
}
