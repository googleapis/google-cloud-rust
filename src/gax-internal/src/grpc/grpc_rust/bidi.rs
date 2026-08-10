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

pub use super::receive::ReceiveTask;
pub use super::receive::RecvItem;
pub use super::send::GrpcRustSend;
use super::send::SendTask;
use grpc::client::{CallOptions, Invoke};
use grpc::core::RequestHeaders;
use prost::Message;
use tokio::sync::mpsc::Receiver;

/// A `tonic` adapter for `grpc-rust` RPCs.
///
/// Incoming server response messages are pulled via [`GrpcRustStreaming::message`].
///
/// Internally, this stream orchestrates:
/// - A background [`ReceiveTask`](super::receive::ReceiveTask) that decodes inbound protobuf messages into `Response`s.
/// - A background [`SendTask`](super::send::SendTask) that pumps outbound request messages.
///
/// `grpc-rust` stream operations ([`RecvStream::recv`](grpc::client::RecvStream::recv)
/// and [`SendStream::send`](grpc::client::SendStream::send)) are not cancellation-safe. Isolating them in
/// dedicated background tasks ensures that cancelling [`GrpcRustStreaming::message`] or dropping
/// [`GrpcRustStreaming`] is safe and prevents stream corruption.
pub struct GrpcRustStreaming<Response> {
    responses: Option<Receiver<tonic::Result<Option<RecvItem<Response>>>>>,
    receive_task: Option<ReceiveTask>,
    send_task: SendTask,
    /// Holds a pre-decoded initial response message (e.g. from stream setup or handshake)
    /// to yield on the first call to [`GrpcRustStreaming::message`] before polling `responses`.
    pending_response: Option<Response>,
}

impl<Response> GrpcRustStreaming<Response>
where
    Response: Message + Default + Send + 'static,
{
    /// Creates a new [`GrpcRustStreaming`] instance with an active response channel and [`ReceiveTask`].
    pub(super) fn new(
        responses: Receiver<tonic::Result<Option<RecvItem<Response>>>>,
        receive_task: ReceiveTask,
        send_task: SendTask,
    ) -> Self {
        Self {
            responses: Some(responses),
            receive_task: Some(receive_task),
            send_task,
            pending_response: None,
        }
    }

    /// Creates a new [`GrpcRustStreaming`] instance with a pre-decoded initial response message.
    ///
    /// The first call to [`GrpcRustStreaming::message`] will yield `pending` before returning
    /// subsequent items from the response channel.
    pub(super) fn new_with_pending(
        responses: Receiver<tonic::Result<Option<RecvItem<Response>>>>,
        receive_task: ReceiveTask,
        send_task: SendTask,
        pending: Response,
    ) -> Self {
        let mut stream = Self::new(responses, receive_task, send_task);
        stream.pending_response = Some(pending);
        stream
    }

    /// Creates a terminal (already closed) [`GrpcRustStreaming`] instance holding the outbound task.
    ///
    /// Subsequent calls to [`GrpcRustStreaming::message`] will return `Ok(None)`.
    pub(super) fn new_terminal(send_task: SendTask) -> Self {
        Self {
            responses: None,
            receive_task: None,
            send_task,
            pending_response: None,
        }
    }

    /// Yields the next response message from the stream, or `Ok(None)` if the stream has ended.
    pub async fn message(&mut self) -> tonic::Result<Option<Response>> {
        if let Some(message) = self.pending_response.take() {
            return Ok(Some(message));
        }
        // If the response stream is absent, signal end-of-stream.
        let Some(responses) = self.responses.as_mut() else {
            return Ok(None);
        };

        // Check for inbound responses or the status of the outbound send.
        loop {
            tokio::select! {
                // Prioritize the response arm: a send failure may result
                // from the server closing after sending its final response,
                // and we want to inspect that response to know what the
                // error is.
                biased;
                // Get the next response from the background receive task.
                response = responses.recv() => match response {
                    // Successfully received a response message.
                    Some(Ok(Some(RecvItem::Message(message)))) => return Ok(Some(message)),
                    // Ignore additional headers received mid-stream.
                    // TODO(#5991): Consider logging (e.g. tracing::debug!) for unexpected mid-stream headers.
                    Some(Ok(Some(RecvItem::Headers(_)))) => continue,
                    // Terminal with clean termination.
                    Some(Ok(None)) => {
                        self.terminate();
                        return Ok(None);
                    }
                    // Terminal with error.
                    Some(Err(status)) => {
                        self.terminate();
                        return Err(status);
                    }
                    // Response channel closed unexpectedly without sending a terminal item.
                    // Join the background receive task to extract the final exit status/error.
                    None => {
                        self.responses.take();
                        self.send_task.abort();
                        let status = self
                            .receive_task
                            .as_mut()
                            .expect("active streams must have a receive task")
                            .join()
                            .await;
                        return Err(status);
                    }
                },
                // Monitor the background send task. If sending failed, fail early and terminate the stream.
                status = self.send_task.join(), if self.send_task.is_joinable() => {
                    if let Err(status) = status {
                        // TODO(#5991): Consider waiting until the receive
                        // side terminates, so server responses that haven't
                        // arrived are not lost.
                        self.terminate();
                        return Err(status);
                    }
                }
            }
        }
    }

    /// Terminates the stream, cleaning up background tasks and response channels.
    fn terminate(&mut self) {
        self.responses.take();
        self.receive_task.take();
        self.send_task.abort();
    }
}

impl<Response> std::fmt::Debug for GrpcRustStreaming<Response> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GrpcRustStreaming")
            .field("pending_response", &self.pending_response.is_some())
            .field("is_terminal", &self.responses.is_none())
            .finish_non_exhaustive()
    }
}

/// Invokes a bidirectional streaming gRPC call on `invoker`.
///
/// Returns a [`tonic::Response`] wrapping a [`GrpcRustStreaming`] on success.
pub async fn invoke_bidi<Request, Response, T>(
    invoker: &T,
    request_headers: RequestHeaders,
    request: impl tokio_stream::Stream<Item = Request> + Send + 'static,
) -> tonic::Result<tonic::Response<GrpcRustStreaming<Response>>>
where
    Request: Message + 'static,
    Response: Message + Default + Send + 'static,
    T: Invoke,
{
    let (send, recv) = invoker
        .invoke(request_headers, CallOptions::default())
        .await;
    let mut send_task = SendTask::start(send, request);
    let (mut responses, mut receive_task) = ReceiveTask::start(recv);

    loop {
        tokio::select! {
            biased;
            item = responses.recv() => match item {
                Some(Ok(Some(RecvItem::Headers(metadata)))) => {
                    return Ok(tonic::Response::from_parts(
                        metadata,
                        GrpcRustStreaming::new(responses, receive_task, send_task),
                        tonic::Extensions::new(),
                    ));
                }
                Some(Ok(Some(RecvItem::Message(pending)))) => {
                    return Ok(tonic::Response::from_parts(
                        tonic::metadata::MetadataMap::new(),
                        GrpcRustStreaming::new_with_pending(responses, receive_task, send_task, pending),
                        tonic::Extensions::new(),
                    ));
                }
                Some(Ok(None)) => {
                    return Ok(tonic::Response::from_parts(
                        tonic::metadata::MetadataMap::new(),
                        GrpcRustStreaming::new_terminal(send_task),
                        tonic::Extensions::new(),
                    ));
                }
                Some(Err(status)) => return Err(status),
                None => {
                    send_task.abort();
                    let status = receive_task.join().await;
                    return Err(status);
                }
            },
            // TODO(#5991): Consider waiting until the receive
            // side terminates, so server responses that haven't
            // arrived are not lost. Perhaps we can removed biased;
            // once that's in place.
            status = send_task.join(), if send_task.is_joinable() => {
                status?;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use grpc::client::{RecvStream, ResponseStreamItem, SendOptions, SendStream};
    use grpc::core::{RecvMessage, ResponseHeaders, SendMessage, Trailers};
    use grpc::metadata::MetadataValue;
    use grpc::{StatusCodeError, StatusError};
    use pretty_assertions::assert_eq;
    use std::sync::{Arc, Mutex};

    #[derive(Clone, PartialEq, Message)]
    struct TestMessage {
        #[prost(string, tag = "1")]
        value: String,
    }

    #[derive(Clone, Default)]
    struct MockSendStream {
        observed_messages: Arc<Mutex<Vec<TestMessage>>>,
        notify: Option<Arc<tokio::sync::Notify>>,
    }

    impl MockSendStream {
        fn capturing(
            observed_messages: Arc<Mutex<Vec<TestMessage>>>,
            notify: Arc<tokio::sync::Notify>,
        ) -> Self {
            Self {
                observed_messages,
                notify: Some(notify),
            }
        }

        fn noop() -> Self {
            Self::default()
        }
    }

    impl SendStream for MockSendStream {
        async fn send(
            &mut self,
            message: &dyn SendMessage,
            _options: SendOptions,
        ) -> Result<(), ()> {
            let mut encoded = message.encode().map_err(|_| ())?;
            let decoded = TestMessage::decode(&mut encoded).map_err(|_| ())?;
            self.observed_messages
                .lock()
                .expect("lock observed messages")
                .push(decoded);
            if let Some(notify) = &self.notify {
                notify.notify_one();
            }
            Ok(())
        }
    }

    enum MockRecvAction {
        WaitForMessage {
            observed_messages: Arc<Mutex<Vec<TestMessage>>>,
            notify: Arc<tokio::sync::Notify>,
        },
        Headers(ResponseHeaders),
        Message(TestMessage),
        Trailers(Trailers),
    }

    struct MockRecvStream {
        actions: std::collections::VecDeque<MockRecvAction>,
    }

    impl MockRecvStream {
        fn new(actions: impl IntoIterator<Item = MockRecvAction>) -> Self {
            Self {
                actions: actions.into_iter().collect(),
            }
        }

        fn immediate_trailers(trailers: Trailers) -> Self {
            Self::new([MockRecvAction::Trailers(trailers)])
        }

        fn headers_and_trailers(headers: ResponseHeaders, trailers: Trailers) -> Self {
            Self::new([
                MockRecvAction::Headers(headers),
                MockRecvAction::Trailers(trailers),
            ])
        }
    }

    impl RecvStream for MockRecvStream {
        async fn recv(&mut self, message: &mut dyn RecvMessage) -> ResponseStreamItem {
            while let Some(action) = self.actions.pop_front() {
                match action {
                    MockRecvAction::WaitForMessage {
                        observed_messages,
                        notify,
                    } => {
                        while observed_messages
                            .lock()
                            .expect("lock observed messages")
                            .is_empty()
                        {
                            notify.notified().await;
                        }
                    }
                    MockRecvAction::Headers(headers) => {
                        return ResponseStreamItem::Headers(headers);
                    }
                    MockRecvAction::Message(response) => {
                        let mut encoded = Bytes::from(response.encode_to_vec());
                        message
                            .decode(&mut encoded)
                            .expect("decode response message");
                        return ResponseStreamItem::Message;
                    }
                    MockRecvAction::Trailers(trailers) => {
                        return ResponseStreamItem::Trailers(trailers);
                    }
                }
            }
            ResponseStreamItem::StreamClosed
        }
    }

    // TODO(#5991): Refactor common stream test mocks into shared test_helpers across grpc_rust tests.
    struct MockInvoker<S = MockSendStream, R = MockRecvStream> {
        send_stream: Mutex<Option<S>>,
        recv_stream: Mutex<Option<R>>,
        observed_headers: Arc<Mutex<Option<RequestHeaders>>>,
    }

    impl<S, R> MockInvoker<S, R> {
        fn new(send_stream: S, recv_stream: R) -> Self {
            Self {
                send_stream: Mutex::new(Some(send_stream)),
                recv_stream: Mutex::new(Some(recv_stream)),
                observed_headers: Arc::new(Mutex::new(None)),
            }
        }

        fn with_observed_headers(
            send_stream: S,
            recv_stream: R,
            observed_headers: Arc<Mutex<Option<RequestHeaders>>>,
        ) -> Self {
            Self {
                send_stream: Mutex::new(Some(send_stream)),
                recv_stream: Mutex::new(Some(recv_stream)),
                observed_headers,
            }
        }
    }

    impl<S, R> Invoke for MockInvoker<S, R>
    where
        S: SendStream + Send + 'static,
        R: RecvStream + Send + 'static,
    {
        type SendStream = S;
        type RecvStream = R;

        async fn invoke(
            &self,
            headers: RequestHeaders,
            _options: CallOptions,
        ) -> (Self::SendStream, Self::RecvStream) {
            *self.observed_headers.lock().expect("lock observed headers") = Some(headers);
            (
                self.send_stream
                    .lock()
                    .expect("lock send stream")
                    .take()
                    .expect("send stream should only be invoked once"),
                self.recv_stream
                    .lock()
                    .expect("lock recv stream")
                    .take()
                    .expect("recv stream should only be invoked once"),
            )
        }
    }

    #[tokio::test]
    async fn bidi_call_yields_response_messages() -> anyhow::Result<()> {
        // Arrange
        const METHOD_NAME: &str = "/google.test.v1.Test/Bidi";
        const HEADER_KEY: &str = "x-response-header";
        const HEADER_VALUE: &str = "response-value";
        const REQUEST_VALUE: &str = "request";
        const RESPONSE_VALUE: &str = "response";

        let observed_headers = Arc::new(Mutex::new(None));
        let observed_messages = Arc::new(Mutex::new(Vec::new()));
        let notify = Arc::new(tokio::sync::Notify::new());

        let mut metadata = grpc::metadata::MetadataMap::new();
        metadata.insert(HEADER_KEY, MetadataValue::from_static(HEADER_VALUE));

        let invoker = MockInvoker::with_observed_headers(
            MockSendStream::capturing(observed_messages.clone(), notify.clone()),
            MockRecvStream::new([
                MockRecvAction::WaitForMessage {
                    observed_messages: observed_messages.clone(),
                    notify,
                },
                MockRecvAction::Headers(ResponseHeaders::new().with_metadata(metadata)),
                MockRecvAction::Message(TestMessage {
                    value: RESPONSE_VALUE.to_string(),
                }),
                MockRecvAction::Trailers(Trailers::new(Ok(()))),
            ]),
            observed_headers.clone(),
        );
        let headers = RequestHeaders::new().with_method_name(METHOD_NAME);
        let request = TestMessage {
            value: REQUEST_VALUE.to_string(),
        };

        // Act
        let response = invoke_bidi::<TestMessage, TestMessage, _>(
            &invoker,
            headers,
            tokio_stream::iter([request.clone()]),
        )
        .await?;

        // Assert
        assert_eq!(
            response
                .metadata()
                .get(HEADER_KEY)
                .and_then(|value| value.to_str().ok()),
            Some(HEADER_VALUE)
        );
        let mut stream = response.into_inner();
        assert_eq!(
            stream.message().await?,
            Some(TestMessage {
                value: RESPONSE_VALUE.to_string()
            })
        );
        assert_eq!(stream.message().await?, None);
        assert_eq!(
            observed_headers
                .lock()
                .expect("lock observed headers")
                .as_ref()
                .expect("observed headers should be set")
                .method_name(),
            METHOD_NAME
        );
        assert_eq!(
            *observed_messages.lock().expect("lock observed messages"),
            [request]
        );
        Ok(())
    }

    #[tokio::test]
    async fn bidi_call_yields_error_on_server_error_status() -> anyhow::Result<()> {
        // Arrange
        const METHOD_NAME: &str = "/google.test.v1.Test/Bidi";
        const ERROR_MESSAGE: &str = "stream aborted";

        let err = StatusError::new(StatusCodeError::Aborted, ERROR_MESSAGE);
        let invoker = MockInvoker::new(
            MockSendStream::noop(),
            MockRecvStream::headers_and_trailers(ResponseHeaders::new(), Trailers::new(Err(err))),
        );
        let headers = RequestHeaders::new().with_method_name(METHOD_NAME);

        // Act
        let response =
            invoke_bidi::<TestMessage, TestMessage, _>(&invoker, headers, tokio_stream::empty())
                .await?;

        // Assert
        let mut stream = response.into_inner();
        let err = stream
            .message()
            .await
            .expect_err("should return status error from trailers");
        assert_eq!(err.code(), tonic::Code::Aborted);
        assert_eq!(err.message(), ERROR_MESSAGE);
        assert_eq!(stream.message().await?, None);

        Ok(())
    }

    #[tokio::test]
    async fn bidi_call_returns_error_on_immediate_trailers_only_status() -> anyhow::Result<()> {
        // Arrange
        const METHOD_NAME: &str = "/google.test.v1.Test/Bidi";
        const ERROR_MESSAGE: &str = "immediate failure";

        let err = StatusError::new(StatusCodeError::Aborted, ERROR_MESSAGE);
        let invoker = MockInvoker::new(
            MockSendStream::noop(),
            MockRecvStream::immediate_trailers(Trailers::new(Err(err))),
        );
        let headers = RequestHeaders::new().with_method_name(METHOD_NAME);

        // Act
        let err =
            invoke_bidi::<TestMessage, TestMessage, _>(&invoker, headers, tokio_stream::empty())
                .await
                .expect_err("invoke_bidi should fail immediately on trailers-only error");

        // Assert
        assert_eq!(err.code(), tonic::Code::Aborted);
        assert_eq!(err.message(), ERROR_MESSAGE);

        Ok(())
    }

    #[tokio::test]
    async fn bidi_call_immediately_fails_if_initial_request_fails() -> anyhow::Result<()> {
        // Arrange
        const METHOD_NAME: &str = "/google.test.v1.Test/Bidi";
        const ERROR_MESSAGE_STREAM_CLOSED: &str = "grpc-rust request stream closed";

        struct FailingSendStream;
        impl SendStream for FailingSendStream {
            async fn send(
                &mut self,
                _message: &dyn SendMessage,
                _options: SendOptions,
            ) -> Result<(), ()> {
                Err(())
            }
        }

        struct PendingRecvStream;
        impl RecvStream for PendingRecvStream {
            async fn recv(&mut self, _message: &mut dyn RecvMessage) -> ResponseStreamItem {
                std::future::pending().await
            }
        }

        let invoker = MockInvoker::new(FailingSendStream, PendingRecvStream);
        let headers = RequestHeaders::new().with_method_name(METHOD_NAME);
        let request = TestMessage {
            value: "msg".to_string(),
        };

        // Act
        let err = invoke_bidi::<TestMessage, TestMessage, _>(
            &invoker,
            headers,
            tokio_stream::iter([request]),
        )
        .await
        .expect_err("invoke_bidi should fail immediately when initial outbound send fails");

        // Assert
        assert_eq!(err.code(), tonic::Code::Internal);
        assert_eq!(err.message(), ERROR_MESSAGE_STREAM_CLOSED);

        Ok(())
    }
}
