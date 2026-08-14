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

//! Defines types and handles for streaming RPCs.
//!
//! This module provides [`RequestSender`] and [`ResponseReceiver`], concrete stream
//! wrapper types used by generated client libraries to manage outbound request streams
//! and inbound response streams without exposing raw transport or protobuf wire types.

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

/// A handle for sending outbound request items over an active bidirectional streaming RPC.
///
/// `RequestSender` manages message transmission across bidirectional streaming
/// RPCs. Outbound messages are passed to an internal bounded channel and processed by the
/// underlying transport.
///
/// ### Flow Control & Backpressure
/// Calling [`.send()`](Self::send) asynchronously yields if the internal channel buffer
/// is full (configured via [`BidiStreamOptions::set_request_channel_capacity`](crate::options::BidiStreamOptions::set_request_channel_capacity)),
/// providing natural backpressure against fast producers.
///
/// ### Half-Closing the Stream
/// In gRPC bidirectional streaming, dropping the `RequestSender` handle (or letting it exit scope)
/// closes the outbound channel and transmits an HTTP/2 `END_STREAM` frame to the server, signaling
/// that the client has finished sending data while leaving the [`ResponseReceiver`] open for responses.
///
/// ### Mocking in Unit Tests
/// `RequestSender` implements [`From<tokio::sync::mpsc::Sender<Req>>`], allowing unit tests
/// and mock stubs to construct a sender directly from standard Tokio channels.
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
    /// Sends a request item over the stream.
    ///
    /// # Errors
    /// * Returns [`Error::io`](crate::error::Error::io) if the receiver was dropped or the stream connection closed.
    /// * Returns [`Error::ser`](crate::error::Error::ser) if payload serialization or model conversion fails.
    ///
    /// # Example
    /// ```rust
    /// # use google_cloud_gax::streaming::RequestSender;
    /// # use tokio::sync::mpsc;
    /// # async fn sample<Req>(sender: RequestSender<Req>, req: Req) -> google_cloud_gax::Result<()> {
    /// sender.send(req).await?;
    /// # Ok(()) }
    /// ```
    pub async fn send(&self, item: Req) -> Result<(), crate::error::Error> {
        (self.inner)(item).await
    }

    /// Creates a [`RequestSender`] from an asynchronous send function.
    ///
    /// This constructor is `doc(hidden)` (except when `_internal-semver` is enabled)
    /// so that generated client transports can construct [`RequestSender`] instances
    /// that perform pre-send transformations without exposing the closure types or
    /// wire models in the public API documentation.
    #[cfg_attr(not(feature = "_internal-semver"), doc(hidden))]
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

impl<Req> From<mpsc::Sender<Req>> for RequestSender<Req>
where
    Req: Send + 'static,
{
    fn from(req_tx: mpsc::Sender<Req>) -> RequestSender<Req> {
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
}

/// A type-erased stream of incoming responses from a gRPC stream.
///
/// This wraps an underlying `futures::Stream` in a boxed pinned trait object,
/// enabling [`ResponseReceiver`] to perform asynchronous transformations (such
/// as Protobuf deserialization) without exposing `futures::Stream` in the public API.
type ResponseStream<Resp> = Pin<Box<dyn futures::Stream<Item = crate::Result<Resp>> + Send>>;

/// A handle for receiving inbound response items from a bidirectional streaming RPC.
///
/// ```rust
/// use google_cloud_gax::streaming::ResponseReceiver;
/// use tokio::sync::mpsc;
///
/// let (tx, rx) = mpsc::channel::<google_cloud_gax::Result<String>>(16);
/// let receiver = ResponseReceiver::from(rx); // Or `rx.into()`
/// ```
///
/// ResponseReceiver` provides an inherent [`.recv()`](Self::recv) method to consume
/// incoming messages sequentially. It is used for bidirectional streaming RPCs.
///
/// ### Mocking in Unit Tests
/// `ResponseReceiver` implements [`From<tokio::sync::mpsc::Receiver<crate::Result<Resp>>>`],
/// enabling unit test stubs to construct a receiver directly from standard Tokio channels:
///
pub struct ResponseReceiver<Resp> {
    inner: ResponseStream<Resp>,
}

impl<Resp> std::fmt::Debug for ResponseReceiver<Resp> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResponseReceiver").finish()
    }
}

impl<Resp> ResponseReceiver<Resp> {
    /// Receives the next response item from the stream.
    ///
    /// Returns `None` once the stream has finished.
    ///
    /// # Example
    /// ```rust
    /// # use google_cloud_gax::streaming::ResponseReceiver;
    /// # use tokio::sync::mpsc;
    /// # async fn sample<Resp: std::fmt::Debug>(mut receiver: ResponseReceiver<Resp>) -> google_cloud_gax::Result<()> {
    /// while let Some(item) = receiver.recv().await {
    ///     let item = item?;
    ///     println!("Received: {item:?}");
    /// }
    /// # Ok(()) }
    /// ```
    pub async fn recv(&mut self) -> Option<crate::Result<Resp>> {
        use futures::StreamExt as _;
        self.inner.next().await
    }

    /// Converts the receiver into an asynchronous [`Stream`][futures::Stream].
    ///
    /// # Example
    /// ```rust
    /// # use google_cloud_gax::streaming::ResponseReceiver;
    /// # use tokio::sync::mpsc;
    /// # use futures::StreamExt as _;
    /// # async fn sample<Resp: std::fmt::Debug>(mut receiver: ResponseReceiver<Resp>) -> google_cloud_gax::Result<()> {
    /// let mut stream = receiver.into_stream();
    /// while let Some(item) = stream.next().await {
    ///     let item = item?;
    ///     println!("Received: {item:?}");
    /// }
    /// # Ok(()) }
    /// ```
    #[cfg(feature = "unstable-stream")]
    #[cfg_attr(docsrs, doc(cfg(feature = "unstable-stream")))]
    pub fn into_stream(self) -> impl futures::Stream<Item = crate::Result<Resp>> + Send + Unpin {
        self.inner
    }

    /// Creates a [`ResponseReceiver`] from an asynchronous stream.
    ///
    /// This constructor is `doc(hidden)` (except when `_internal-semver` is enabled)
    /// so that generated client transports can construct [`ResponseReceiver`] instances
    /// directly from gRPC response streams without exposing `futures::Stream` in the
    /// public API documentation.
    #[cfg_attr(not(feature = "_internal-semver"), doc(hidden))]
    pub fn from_stream<S>(stream: S) -> Self
    where
        S: futures::Stream<Item = crate::Result<Resp>> + Send + 'static,
    {
        Self {
            inner: Box::pin(stream),
        }
    }
}

impl<Resp> From<mpsc::Receiver<crate::Result<Resp>>> for ResponseReceiver<Resp>
where
    Resp: Send + 'static,
{
    fn from(rx: mpsc::Receiver<crate::Result<Resp>>) -> Self {
        Self::from_stream(tokio_stream::wrappers::ReceiverStream::new(rx))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn request_sender_and_response_receiver() -> Result<(), Box<dyn std::error::Error>> {
        let (req_tx, mut req_rx) = mpsc::channel::<String>(16);
        let (resp_tx, resp_rx) = mpsc::channel::<crate::Result<String>>(16);

        let sender: RequestSender<_> = req_tx.into();
        let mut receiver: ResponseReceiver<_> = resp_rx.into();

        sender.send("hello".to_string()).await?;
        assert_eq!(req_rx.recv().await.as_deref(), Some("hello"));

        resp_tx.send(Ok("world".to_string())).await?;
        assert_eq!(receiver.recv().await.transpose()?.as_deref(), Some("world"));

        drop(resp_tx);
        assert!(receiver.recv().await.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn request_sender_send_error() {
        use std::error::Error as _;

        let (req_tx, req_rx) = mpsc::channel::<String>(16);
        let sender = RequestSender::from(req_tx);

        drop(req_rx);
        let err = sender
            .send("hello".to_string())
            .await
            .expect_err("send should fail when receiver is dropped");
        assert!(err.is_io());
        assert_eq!(
            err.source().map(|e| e.to_string()).as_deref(),
            Some("cannot send request: stream is closed")
        );
    }

    #[tokio::test]
    async fn request_sender_from_fn() -> Result<(), Box<dyn std::error::Error>> {
        let sender = RequestSender::from_fn(|item: i32| async move {
            if item < 0 {
                Err(crate::error::Error::ser("negative number"))
            } else {
                Ok(())
            }
        });

        sender.send(42).await?;
        let err = sender
            .send(-1)
            .await
            .expect_err("negative number should trigger serialization error");
        assert!(err.is_serialization());
        assert_eq!(format!("{sender:?}"), "RequestSender");
        Ok(())
    }

    #[tokio::test]
    async fn response_receiver_from_stream() -> Result<(), Box<dyn std::error::Error>> {
        let stream = futures::stream::iter(vec![
            Ok("first".to_string()),
            Err(crate::error::Error::deser("bad data")),
            Ok("second".to_string()),
        ]);
        let mut receiver = ResponseReceiver::from_stream(stream);

        assert_eq!(
            receiver
                .recv()
                .await
                .expect("expected first response")?
                .as_str(),
            "first"
        );
        let err = receiver
            .recv()
            .await
            .expect("expected error item")
            .expect_err("item should be Err");
        assert!(err.is_deserialization());
        assert_eq!(
            receiver
                .recv()
                .await
                .expect("expected second response")?
                .as_str(),
            "second"
        );
        assert!(receiver.recv().await.is_none());
        assert_eq!(format!("{receiver:?}"), "ResponseReceiver");
        Ok(())
    }

    #[tokio::test]
    async fn response_receiver_generator_mapping_pipeline() -> Result<(), Box<dyn std::error::Error>>
    {
        use futures::StreamExt as _;

        #[derive(Debug, PartialEq)]
        struct RawProto {
            text: String,
            valid: bool,
        }

        #[derive(Debug, PartialEq)]
        struct DomainModel {
            text: String,
        }

        fn from_proto(raw: RawProto) -> Result<DomainModel, &'static str> {
            if raw.valid {
                Ok(DomainModel { text: raw.text })
            } else {
                Err("invalid proto payload")
            }
        }

        let status = crate::error::rpc::Status::default()
            .set_code(crate::error::rpc::Code::Unavailable)
            .set_message("transport unavailable");

        // Simulates a tonic gRPC response stream yielding Result<RawProto, StatusError>
        let raw_stream = futures::stream::iter(vec![
            Ok(RawProto {
                text: "hello".to_string(),
                valid: true,
            }),
            Err(crate::error::Error::service(status)),
            Ok(RawProto {
                text: "corrupted".to_string(),
                valid: false,
            }),
            Ok(RawProto {
                text: "world".to_string(),
                valid: true,
            }),
        ]);

        // Exact mapping pattern used by the generated transport:
        let response_stream = raw_stream
            .map(|res| res.and_then(|raw| from_proto(raw).map_err(crate::error::Error::deser)));

        let mut receiver = ResponseReceiver::from_stream(response_stream);

        // 1. Success
        let item1 = receiver.recv().await.expect("expected item 1")?;
        assert_eq!(
            item1,
            DomainModel {
                text: "hello".to_string()
            }
        );

        // 2. Stream transport error
        let err2 = receiver
            .recv()
            .await
            .expect("expected item 2")
            .expect_err("item 2 should be Err");
        assert_eq!(
            err2.status().map(|s| s.code),
            Some(crate::error::rpc::Code::Unavailable)
        );

        // 3. Deserialization error
        let err3 = receiver
            .recv()
            .await
            .expect("expected item 3")
            .expect_err("item 3 should be Err");
        assert!(err3.is_deserialization());

        // 4. Success after recoverable error
        let item4 = receiver.recv().await.expect("expected item 4")?;
        assert_eq!(
            item4,
            DomainModel {
                text: "world".to_string()
            }
        );

        // 5. Stream finished
        assert!(receiver.recv().await.is_none());
        Ok(())
    }

    #[cfg(feature = "unstable-stream")]
    #[tokio::test]
    async fn response_receiver_into_stream() -> Result<(), Box<dyn std::error::Error>> {
        use futures::StreamExt as _;

        let stream = futures::stream::iter(vec![Ok("first".to_string()), Ok("second".to_string())]);
        let receiver = ResponseReceiver::from_stream(stream);
        let mut stream = receiver.into_stream();

        assert_eq!(stream.next().await.transpose()?.as_deref(), Some("first"));
        assert_eq!(stream.next().await.transpose()?.as_deref(), Some("second"));
        assert!(stream.next().await.is_none());
        Ok(())
    }
}
