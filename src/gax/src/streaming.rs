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

/// An error returned when sending a request over a stream fails.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum SendError {
    /// The stream was closed by the server or receiver.
    ///
    /// The underlying server status error (e.g., `InvalidArgument`, `PermissionDenied`)
    /// is returned by [`ResponseReceiver::recv`].
    #[error("cannot send request: stream is closed; inspect ResponseReceiver for details")]
    StreamClosed,

    /// Serialization / proto conversion of the request failed.
    #[error(transparent)]
    Serialization(#[from] crate::error::Error),
}

impl SendError {
    /// Not part of the public API, subject to change without notice.
    #[cfg_attr(not(feature = "_internal-semver"), doc(hidden))]
    pub fn stream_closed() -> Self {
        Self::StreamClosed
    }

    /// Not part of the public API, subject to change without notice.
    #[cfg_attr(not(feature = "_internal-semver"), doc(hidden))]
    pub fn ser<T>(source: T) -> Self
    where
        T: Into<Box<dyn std::error::Error + Send + Sync>>,
    {
        Self::Serialization(crate::error::Error::ser(source))
    }
}

/// A type-erased asynchronous function that sends a request item over a stream.
///
/// This closure takes an owned request item and returns a boxed, pinned future
/// producing a `Result<(), SendError>`. It allows [`RequestSender`] to perform
/// pre-send transformations (such as converting high-level domain models to
/// low-level Protobuf wire types) without exposing the underlying transport or
/// wire types in the public API.
type SenderFn<Req> =
    dyn Fn(Req) -> Pin<Box<dyn Future<Output = Result<(), SendError>> + Send>> + Send + Sync;

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
    /// Sends a request item over the stream.
    pub async fn send(&self, item: Req) -> Result<(), SendError> {
        (self.inner)(item).await
    }

    /// Creates a [`RequestSender`] from an asynchronous send function.
    ///
    /// This constructor is `doc(hidden)` (except when `_internal-semver` is enabled)
    /// so that generated client transports can construct [`RequestSender`] instances
    /// that perform pre-send transformations without exposing the closure types or
    /// wire models in the public API documentation.
    #[cfg_attr(not(feature = "_internal-semver"), doc(hidden))]
    pub fn from_fn<F, Fut, E>(f: F) -> Self
    where
        F: Fn(Req) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: Into<SendError> + 'static,
    {
        Self {
            inner: Arc::new(move |item| {
                let fut = f(item);
                Box::pin(async move { fut.await.map_err(Into::into) })
            }),
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
                    .map_err(|_| SendError::stream_closed())
            }
        })
    }
}

/// A boxed pinned future that resolves to an incoming response stream or an error.
type ConnectingFuture<Resp> =
    Pin<Box<dyn Future<Output = Result<ResponseStream<Resp>, crate::error::Error>> + Send>>;

/// A type-erased stream of incoming responses from a gRPC stream.
///
/// This wraps an underlying `futures::Stream` in a boxed pinned trait object,
/// enabling [`ResponseReceiver`] to perform asynchronous transformations (such
/// as Protobuf deserialization) without exposing `futures::Stream` in the public API.
type ResponseStream<Resp> =
    Pin<Box<dyn futures::Stream<Item = Result<Resp, crate::error::Error>> + Send>>;

/// Internal state machine for [`ResponseReceiver`].
enum ResponseState<Resp> {
    /// Awaiting the initial connection future to resolve the response stream.
    Connecting(ConnectingFuture<Resp>),
    /// Response stream established; actively streaming incoming messages.
    Connected(ResponseStream<Resp>),
    /// Stream has completed or encountered a terminal error.
    Closed,
}

/// A handle for receiving inbound response items from a gRPC stream.
pub struct ResponseReceiver<Resp> {
    state: ResponseState<Resp>,
}

impl<Resp> std::fmt::Debug for ResponseReceiver<Resp> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResponseReceiver").finish()
    }
}

impl<Resp> ResponseReceiver<Resp> {
    /// Receives the next response message from the stream.
    ///
    /// On the first invocation, this awaits the server's response headers and connection
    /// establishment if constructed via [`from_future`][Self::from_future]. If the server
    /// rejected the stream during setup, the error is returned here.
    pub async fn recv(&mut self) -> Option<Result<Resp, crate::error::Error>> {
        use futures::StreamExt as _;
        loop {
            match &mut self.state {
                ResponseState::Connecting(fut) => match fut.await {
                    Ok(stream) => {
                        self.state = ResponseState::Connected(stream);
                    }
                    Err(e) => {
                        self.state = ResponseState::Closed;
                        return Some(Err(e));
                    }
                },
                ResponseState::Connected(stream) => {
                    let item = stream.next().await;
                    if item.is_none() {
                        self.state = ResponseState::Closed;
                    }
                    return item;
                }
                ResponseState::Closed => return None,
            }
        }
    }

    #[cfg(feature = "unstable-stream")]
    #[cfg_attr(docsrs, doc(cfg(feature = "unstable-stream")))]
    /// Converts the receiver into an asynchronous [`Stream`][futures::Stream].
    pub fn into_stream(
        self,
    ) -> impl futures::Stream<Item = Result<Resp, crate::error::Error>> + Send + Unpin {
        Box::pin(futures::stream::unfold(self, |mut rx| async move {
            let item = rx.recv().await?;
            Some((item, rx))
        }))
    }

    /// Creates a [`ResponseReceiver`] from an asynchronous connection future.
    ///
    /// This constructor is `doc(hidden)` (except when `_internal-semver` is enabled)
    /// so that generated client transports can construct [`ResponseReceiver`] instances
    /// that lazily await HTTP/2 response headers.
    #[cfg_attr(not(feature = "_internal-semver"), doc(hidden))]
    pub fn from_future<Fut, S>(fut: Fut) -> Self
    where
        Fut: Future<Output = Result<S, crate::error::Error>> + Send + 'static,
        S: futures::Stream<Item = Result<Resp, crate::error::Error>> + Send + 'static,
    {
        let connecting: ConnectingFuture<Resp> = Box::pin(async move {
            let stream = fut.await?;
            Ok(Box::pin(stream) as ResponseStream<Resp>)
        });
        Self {
            state: ResponseState::Connecting(connecting),
        }
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
        S: futures::Stream<Item = Result<Resp, crate::error::Error>> + Send + 'static,
    {
        Self {
            state: ResponseState::Connected(Box::pin(stream)),
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
    use std::sync::atomic::{AtomicBool, Ordering};

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
    async fn request_sender_send_error_stream_closed() {
        let (req_tx, req_rx) = mpsc::channel::<String>(16);
        let sender = RequestSender::from(req_tx);

        drop(req_rx);
        let err = sender
            .send("hello".to_string())
            .await
            .expect_err("send should fail when receiver is dropped");
        assert!(matches!(err, SendError::StreamClosed));
        assert_eq!(
            err.to_string(),
            "cannot send request: stream is closed; inspect ResponseReceiver for details"
        );

        let constructed = SendError::stream_closed();
        assert!(matches!(constructed, SendError::StreamClosed));
    }

    #[tokio::test]
    async fn request_sender_send_error_serialization() {
        let sender = RequestSender::from_fn(|item: i32| async move {
            if item < 0 {
                Err(SendError::ser("negative number"))
            } else {
                Ok(())
            }
        });

        sender.send(42).await.expect("send should succeed");
        let err = sender
            .send(-1)
            .await
            .expect_err("negative number should trigger serialization error");
        assert!(matches!(err, SendError::Serialization(_)));
        assert_eq!(
            err.to_string(),
            "cannot serialize the request negative number"
        );
        assert_eq!(format!("{sender:?}"), "RequestSender");

        let ser_err = SendError::ser("test ser");
        assert!(matches!(ser_err, SendError::Serialization(_)));
    }

    #[tokio::test]
    async fn response_receiver_lazy_future_success() -> Result<(), Box<dyn std::error::Error>> {
        let future_polled = Arc::new(AtomicBool::new(false));
        let polled_clone = future_polled.clone();

        let lazy_fut = async move {
            polled_clone.store(true, Ordering::SeqCst);
            let stream =
                futures::stream::iter(vec![Ok("item-1".to_string()), Ok("item-2".to_string())]);
            Ok(stream)
        };

        let mut receiver = ResponseReceiver::from_future(lazy_fut);
        // Ensure future is NOT polled before recv() is called
        assert!(!future_polled.load(Ordering::SeqCst));

        // First recv() awaits connection future and yields first item
        let first = receiver.recv().await.expect("expected first response")?;
        assert!(future_polled.load(Ordering::SeqCst));
        assert_eq!(first, "item-1");

        // Second recv() yields second item
        let second = receiver.recv().await.expect("expected second response")?;
        assert_eq!(second, "item-2");

        // Third recv() returns None (clean EOF)
        assert!(receiver.recv().await.is_none());

        // Subsequent recv() calls on closed receiver return None
        assert!(receiver.recv().await.is_none());
        assert!(receiver.recv().await.is_none());
        assert_eq!(format!("{receiver:?}"), "ResponseReceiver");

        Ok(())
    }

    #[tokio::test]
    async fn response_receiver_lazy_future_connecting_error() {
        let status = crate::error::rpc::Status::default()
            .set_code(crate::error::rpc::Code::PermissionDenied)
            .set_message("permission denied");

        let lazy_fut = async move {
            let res: Result<futures::stream::Empty<crate::Result<String>>, _> =
                Err(crate::error::Error::service(status));
            res
        };

        let mut receiver = ResponseReceiver::<String>::from_future(lazy_fut);

        // First recv() should return the connection setup error
        let err = receiver
            .recv()
            .await
            .expect("expected error item")
            .expect_err("should be Err");
        assert_eq!(
            err.status().map(|s| s.code),
            Some(crate::error::rpc::Code::PermissionDenied)
        );

        // After error during connection, stream is closed and returns None
        assert!(receiver.recv().await.is_none());
        assert!(receiver.recv().await.is_none());
    }

    #[tokio::test]
    async fn response_receiver_stream_item_error_recovery() -> Result<(), Box<dyn std::error::Error>>
    {
        let stream = futures::stream::iter(vec![
            Ok("item-1".to_string()),
            Err(crate::error::Error::deser("corrupted item")),
            Ok("item-2".to_string()),
        ]);
        let mut receiver = ResponseReceiver::from_stream(stream);

        let item1 = receiver.recv().await.expect("expected item 1")?;
        assert_eq!(item1, "item-1");

        let err = receiver
            .recv()
            .await
            .expect("expected item 2")
            .expect_err("item 2 should be deserialization error");
        assert!(err.is_deserialization());

        let item2 = receiver.recv().await.expect("expected item 3")?;
        assert_eq!(item2, "item-2");

        assert!(receiver.recv().await.is_none());
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
