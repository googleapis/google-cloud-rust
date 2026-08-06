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

//! Types for gRPC streaming requests and responses.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::mpsc;

/// A type-erased asynchronous function that sends a request item over a stream.
///
/// This closure takes an owned request item and returns a boxed, pinned future
/// producing a [`crate::Result<()>`]. It allows [`RequestSender`] to perform
/// pre-send transformations (such as converting high-level domain models to
/// low-level Protobuf wire types) without exposing the underlying transport or
/// wire types in the public API.
type SenderFn<Req> =
    dyn Fn(Req) -> Pin<Box<dyn Future<Output = crate::Result<()>> + Send>> + Send + Sync;

/// A handle for sending outbound request items over a gRPC stream.
#[derive(Clone)]
pub struct RequestSender<Req> {
    inner: Arc<SenderFn<Req>>,
}

impl<Req> std::fmt::Debug for RequestSender<Req> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RequestSender").finish()
    }
}

impl<Req> RequestSender<Req> {
    /// Creates a new [`RequestSender`].
    pub fn new(req_tx: mpsc::Sender<Req>) -> Self
    where
        Req: Send + Sync + 'static,
    {
        Self::from_fn(move |item| {
            let req_tx = req_tx.clone();
            async move {
                req_tx
                    .send(item)
                    .await
                    .map_err(|_| crate::error::Error::io("cannot send request: stream is closed"))
            }
        })
    }

    /// Sends a request item over the stream.
    pub async fn send(&self, item: Req) -> Result<(), crate::error::Error> {
        (self.inner)(item).await
    }

    #[doc(hidden)]
    pub fn from_fn<F, Fut>(f: F) -> Self
    where
        F: Fn(Req) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = crate::Result<()>> + Send + 'static,
    {
        Self {
            inner: Arc::new(move |item| Box::pin(f(item))),
        }
    }
}

/// A handle for receiving inbound response items from a gRPC stream.
#[derive(Debug)]
pub struct ResponseReceiver<Resp> {
    rx: mpsc::Receiver<crate::Result<Resp>>,
}

impl<Resp> ResponseReceiver<Resp> {
    /// Creates a new [`ResponseReceiver`].
    pub fn new(rx: mpsc::Receiver<crate::Result<Resp>>) -> Self {
        Self { rx }
    }

    /// Receives the next response item from the stream.
    pub async fn recv(&mut self) -> Option<crate::Result<Resp>> {
        self.rx.recv().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_request_sender_and_response_receiver() {
        let (req_tx, mut req_rx) = mpsc::channel::<String>(16);
        let (resp_tx, resp_rx) = mpsc::channel::<crate::Result<String>>(16);

        let sender = RequestSender::new(req_tx);
        let mut receiver = ResponseReceiver::new(resp_rx);

        sender.send("hello".to_string()).await.unwrap();
        assert_eq!(req_rx.recv().await.unwrap(), "hello");

        resp_tx.send(Ok("world".to_string())).await.unwrap();
        assert_eq!(receiver.recv().await.unwrap().unwrap(), "world");

        drop(resp_tx);
        assert!(receiver.recv().await.is_none());
    }

    #[tokio::test]
    async fn test_request_sender_send_error() {
        use std::error::Error as _;

        let (req_tx, req_rx) = mpsc::channel::<String>(16);
        let sender = RequestSender::new(req_tx);

        drop(req_rx);
        let err = sender.send("hello".to_string()).await.unwrap_err();
        assert!(err.is_io());
        assert_eq!(
            err.source().unwrap().to_string(),
            "cannot send request: stream is closed"
        );
    }

    #[tokio::test]
    async fn test_request_sender_from_fn() {
        let sender = RequestSender::from_fn(|item: i32| async move {
            if item < 0 {
                Err(crate::error::Error::ser("negative number"))
            } else {
                Ok(())
            }
        });

        assert!(sender.send(42).await.is_ok());
        let err = sender.send(-1).await.unwrap_err();
        assert!(err.is_serialization());
        assert_eq!(format!("{sender:?}"), "RequestSender");
    }
}
