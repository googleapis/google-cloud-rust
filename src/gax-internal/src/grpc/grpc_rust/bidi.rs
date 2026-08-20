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
use super::send::{SendState, SendTask};
use grpc::client::{CallOptions, Invoke, RequestHeaders};
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
    send_state: SendState,
    /// Holds a pre-decoded initial response message (e.g. from stream setup or handshake)
    /// to yield on the first call to [`GrpcRustStreaming::message`] before polling `responses`.
    pending_response: Option<Response>,
}

impl<Response> GrpcRustStreaming<Response>
where
    Response: Message + Default + Send + 'static,
{
    /// Creates a new [`GrpcRustStreaming`] instance with an active response channel and [`ReceiveTask`].
    fn new(
        responses: Receiver<tonic::Result<Option<RecvItem<Response>>>>,
        receive_task: ReceiveTask,
        send_state: SendState,
    ) -> Self {
        Self {
            responses: Some(responses),
            receive_task: Some(receive_task),
            send_state,
            pending_response: None,
        }
    }

    /// Creates a new [`GrpcRustStreaming`] instance with a pre-decoded initial response message.
    ///
    /// The first call to [`GrpcRustStreaming::message`] will yield `pending` before returning
    /// subsequent items from the response channel.
    fn new_with_pending(
        responses: Receiver<tonic::Result<Option<RecvItem<Response>>>>,
        receive_task: ReceiveTask,
        send_state: SendState,
        pending: Response,
    ) -> Self {
        let mut stream = Self::new(responses, receive_task, send_state);
        stream.pending_response = Some(pending);
        stream
    }

    /// Creates a terminal (already closed) [`GrpcRustStreaming`] instance.
    ///
    /// Subsequent calls to [`GrpcRustStreaming::message`] will return `Ok(None)`.
    fn new_terminal() -> Self {
        Self {
            responses: None,
            receive_task: None,
            send_state: SendState::Complete,
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
                    // Terminal with clean termination. If sending failed, some requests were not
                    // delivered, so return that failure.
                    Some(Ok(None)) => {
                        self.send_state.join_if_finished().await;
                        let send_error = self.send_state.failure().cloned();
                        self.terminate();
                        return send_error.map_or(Ok(None), Err);
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
                        self.send_state.abort();
                        let status = self
                            .receive_task
                            .as_mut()
                            .expect("active streams must have a receive task")
                            .join()
                            .await;
                        return Err(status);
                    }
                },
                // Concurrently wait for the send task to finish. When it completes:
                // 1. `join()` records completion or failure into `self.send_state`.
                // 2. We keep reading responses so the server's true error (if any) isn't masked.
                //    The send error is only returned if the server finishes cleanly.
                () = self.send_state.join(), if self.send_state.is_active() => {}
            }
        }
    }

    /// Terminates the stream, cleaning up background tasks and response channels.
    fn terminate(&mut self) {
        self.responses.take();
        self.receive_task.take();
        self.send_state.abort();
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
    let mut send_state = SendState::new(SendTask::start(send, request));
    let (mut responses, mut receive_task) = ReceiveTask::start(recv);

    loop {
        tokio::select! {
            biased;
            item = responses.recv() => match item {
                Some(Ok(Some(RecvItem::Headers(metadata)))) => {
                    return Ok(tonic::Response::from_parts(
                        metadata,
                        GrpcRustStreaming::new(responses, receive_task, send_state),
                        tonic::Extensions::new(),
                    ));
                }
                Some(Ok(Some(RecvItem::Message(pending)))) => {
                    return Ok(tonic::Response::from_parts(
                        tonic::metadata::MetadataMap::new(),
                        GrpcRustStreaming::new_with_pending(
                            responses,
                            receive_task,
                            send_state,
                            pending,
                        ),
                        tonic::Extensions::new(),
                    ));
                }
                Some(Ok(None)) => {
                    send_state.join_if_finished().await;
                    if let Some(status) = send_state.failure().cloned() {
                        return Err(status);
                    }
                    send_state.abort();
                    return Ok(tonic::Response::from_parts(
                        tonic::metadata::MetadataMap::new(),
                        GrpcRustStreaming::new_terminal(),
                        tonic::Extensions::new(),
                    ));
                }
                Some(Err(status)) => return Err(status),
                None => {
                    send_state.abort();
                    let status = receive_task.join().await;
                    return Err(status);
                }
            },
            // Concurrently wait for the send task to finish. When it completes:
            // 1. `join()` records completion or failure into `send_state`.
            // 2. We keep waiting so server headers, initial messages, or status trailers take priority.
            () = send_state.join(), if send_state.is_active() => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::testing::*;
    use super::*;
    use grpc::client::{ResponseHeaders, Trailers};
    use grpc::metadata::MetadataValue;
    use grpc::{StatusCodeError, StatusError};
    use pretty_assertions::assert_eq;
    use std::sync::Arc;

    #[tokio::test]
    async fn bidi_call_yields_response_messages() -> anyhow::Result<()> {
        // Arrange
        const METHOD_NAME: &str = "/google.test.v1.Test/Bidi";
        const HEADER_KEY: &str = "x-response-header";
        const HEADER_VALUE: &str = "response-value";
        const REQUEST_VALUE: &str = "request";
        const RESPONSE_VALUE: &str = "response";

        let mut metadata = grpc::metadata::MetadataMap::new();
        metadata.insert(HEADER_KEY, MetadataValue::from_static(HEADER_VALUE));

        let send_stream = MockSendStream::new();
        let invoker = MockInvoker::new(
            send_stream.clone(),
            MockRecvStream::new([
                MockRecvAction::Wait(send_stream.notify_handle()),
                MockRecvAction::Headers(ResponseHeaders::new().with_metadata(metadata)),
                MockRecvAction::Message(TestMessage {
                    value: RESPONSE_VALUE.to_string(),
                }),
                MockRecvAction::Trailers(Trailers::new(Ok(()))),
            ]),
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
            invoker
                .observed_headers()
                .as_ref()
                .expect("observed headers should be set")
                .method_name(),
            METHOD_NAME
        );
        assert_eq!(send_stream.observed_messages(), [request]);
        Ok(())
    }

    #[tokio::test]
    async fn bidi_call_yields_error_on_server_error_status() -> anyhow::Result<()> {
        // Arrange
        const METHOD_NAME: &str = "/google.test.v1.Test/Bidi";
        const ERROR_MESSAGE: &str = "stream aborted";

        let err = StatusError::new(StatusCodeError::Aborted, ERROR_MESSAGE);
        let invoker = MockInvoker::new(
            MockSendStream::default(),
            MockRecvStream::with_headers_and_trailers(
                ResponseHeaders::new(),
                Trailers::new(Err(err)),
            ),
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
            MockSendStream::default(),
            MockRecvStream::with_immediate_trailers(Trailers::new(Err(err))),
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

    #[tokio::test(start_paused = true)]
    async fn bidi_call_continues_receiving_even_after_initial_send_request_fails()
    -> anyhow::Result<()> {
        // Arrange
        const METHOD_NAME: &str = "/google.test.v1.Test/Bidi";

        let invoker = MockInvoker::new(FailingSendStream, PendingRecvStream);
        let headers = RequestHeaders::new().with_method_name(METHOD_NAME);
        let request = TestMessage {
            value: "msg".to_string(),
        };

        // Act
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            invoke_bidi::<TestMessage, TestMessage, _>(
                &invoker,
                headers,
                tokio_stream::iter([request]),
            ),
        )
        .await;

        // Assert
        // We use `result.is_err()` to verify if the timeout indeed elapsed.
        // If instead `invoke_bidi` had returned the send error rather
        // than continuing to receive (and thereby timing out), `result` would
        // have been `Ok(Err(...))`.
        assert!(
            result.is_err(),
            "receive should still be pending after initial send failure"
        );

        Ok(())
    }

    #[tokio::test]
    async fn bidi_call_returns_server_error() -> anyhow::Result<()> {
        // Arrange
        const METHOD_NAME: &str = "/google.test.v1.Test/Bidi";
        const ERROR_MESSAGE: &str = "request rejected by server";
        let server_error = StatusError::new(StatusCodeError::InvalidArgument, ERROR_MESSAGE);
        let invoker = MockInvoker::new(
            FailingSendStream,
            MockRecvStream::with_immediate_trailers(Trailers::new(Err(server_error))),
        );
        let headers = RequestHeaders::new().with_method_name(METHOD_NAME);
        let request = TestMessage {
            value: "request".to_string(),
        };

        // Act
        let err = invoke_bidi::<TestMessage, TestMessage, _>(
            &invoker,
            headers,
            tokio_stream::iter([request]),
        )
        .await
        .expect_err("should fail with server error");

        // Assert
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert_eq!(err.message(), ERROR_MESSAGE);
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn bidi_stream_preserves_send_failure_and_server_response_across_cancelled_receive()
    -> anyhow::Result<()> {
        // Arrange
        const RESPONSE_VALUE: &str = "buffered response";
        const ERROR_MESSAGE_STREAM_CLOSED: &str = "grpc-rust request stream closed";
        let (mut stream, receive_gate) = setup_failed_send_bidi_stream([
            MockRecvAction::Message(TestMessage {
                value: RESPONSE_VALUE.to_string(),
            }),
            MockRecvAction::Trailers(Trailers::new(Ok(()))),
        ])
        .await?;

        // Act: cancel a read after the send task has failed but before the server completes.
        let cancelled =
            tokio::time::timeout(std::time::Duration::from_secs(1), stream.message()).await;
        assert!(cancelled.is_err(), "the receive should still be pending");

        // Unblock the server
        receive_gate.notify_one();

        // Assert
        // We still receive the response from the server...
        assert_eq!(
            stream.message().await?,
            Some(TestMessage {
                value: RESPONSE_VALUE.to_string()
            })
        );
        // ...and we report the send side failure.
        let err = stream
            .message()
            .await
            .expect_err("clean server completion should not mask outbound send failure");
        assert_eq!(err.code(), tonic::Code::Internal);
        assert_eq!(err.message(), ERROR_MESSAGE_STREAM_CLOSED);

        // Subsequent reads signal end-of-stream.
        assert_eq!(stream.message().await?, None);
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn bidi_stream_server_error_overrides_preserved_send_failure() -> anyhow::Result<()> {
        // Arrange
        const ERROR_MESSAGE: &str = "server rejected the stream";
        let server_error = StatusError::new(StatusCodeError::PermissionDenied, ERROR_MESSAGE);
        let (mut stream, receive_gate) = setup_failed_send_bidi_stream([MockRecvAction::Trailers(
            Trailers::new(Err(server_error)),
        )])
        .await?;

        // Act: cancel a read after the send task has failed but before the server completes.
        let cancelled =
            tokio::time::timeout(std::time::Duration::from_secs(1), stream.message()).await;
        assert!(cancelled.is_err(), "the receive should still be pending");

        // Unblock the server
        receive_gate.notify_one();

        // Assert
        let err = stream.message().await.expect_err("expected an error");

        // If it had been a send side error, then the code would instead be `tonic::Code::Internal` (see
        // `bidi_stream_preserves_send_failure_and_server_response_across_cancelled_receive`).
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
        assert_eq!(err.message(), ERROR_MESSAGE);

        // Subsequent reads signal end-of-stream.
        assert_eq!(stream.message().await?, None);
        Ok(())
    }

    /// Sets up a bidi stream test fixture where the outbound send pump has failed and
    /// the inbound receive stream is paused at a synchronization gate after initial headers.
    ///
    /// Returns the active [`GrpcRustStreaming`] and the [`Arc<tokio::sync::Notify>`] gate
    /// used to unblock subsequent `server_actions`.
    async fn setup_failed_send_bidi_stream(
        server_actions: impl IntoIterator<Item = MockRecvAction>,
    ) -> anyhow::Result<(GrpcRustStreaming<TestMessage>, Arc<tokio::sync::Notify>)> {
        const METHOD_NAME: &str = "/google.test.v1.Test/Bidi";
        let (send_stream, fail_gate, failed) = FailAfterFirstSendStream::gated();
        let receive_gate = Arc::new(tokio::sync::Notify::new());

        // Emit initial response headers and pause the server stream until unblocked by `receive_gate`.
        let mut actions = vec![
            MockRecvAction::Headers(ResponseHeaders::new()),
            MockRecvAction::Wait(receive_gate.clone()),
        ];
        actions.extend(server_actions);
        let invoker = MockInvoker::new(send_stream, MockRecvStream::new(actions));
        let headers = RequestHeaders::new().with_method_name(METHOD_NAME);

        // Send two requests: the first succeeds; the second blocks on `fail_gate`.
        let requests = [
            TestMessage {
                value: "request-1".to_string(),
            },
            TestMessage {
                value: "request-2".to_string(),
            },
        ];
        let response = invoke_bidi::<TestMessage, TestMessage, _>(
            &invoker,
            headers,
            tokio_stream::iter(requests),
        )
        .await?;
        let stream = response.into_inner();

        // Fail the outbound send stream so the caller receives a stream with an already-failed send task.
        fail_gate.notify_one();
        failed.notified().await; // Wait for failure to actually happen.

        Ok((stream, receive_gate))
    }
}
