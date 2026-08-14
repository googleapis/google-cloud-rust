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

use super::receive::{GrpcRustRecv, trailers_to_tonic_status};
use super::send::GrpcRustSend;
use bytes::Buf;
use grpc::client::stream_util::RecvStreamValidator;
use grpc::client::{CallOptions, Invoke, RecvStream, ResponseStreamItem, SendOptions, SendStream};
use grpc::core::{RecvMessage, RequestHeaders};
use prost::Message;
use tonic::metadata::MetadataMap;

/// Invokes a unary gRPC call on `invoker`.
///
/// Returns a [`tonic::Response`] wrapping the decoded response message on success.
pub(super) async fn invoke_unary<Request, Response, T>(
    invoker: &T,
    request_headers: RequestHeaders,
    request: Request,
    options: CallOptions,
) -> tonic::Result<tonic::Response<Response>>
where
    Request: Message,
    Response: Message + Default,
    T: Invoke,
{
    let (send, recv) = invoker.invoke(request_headers, options).await;

    if send_unary_request(send, request).await.is_err() {
        return Err(drain_recv_stream(recv).await);
    }

    recv_unary_response(recv).await
}

/// Sends the unary request message and drops the outbound stream, thereby closing it.
async fn send_unary_request<Request>(mut send: impl SendStream, request: Request) -> Result<(), ()>
where
    Request: Message,
{
    send.send(
        &GrpcRustSend(request),
        SendOptions::new().with_final_msg(true),
    )
    .await
}

/// Drains the receive stream after a send failure to extract any error status sent by the server.
async fn drain_recv_stream(mut recv: impl RecvStream) -> tonic::Status {
    struct DrainRecv;
    impl RecvMessage for DrainRecv {
        fn decode(&mut self, _data: &mut dyn Buf) -> Result<(), String> {
            Ok(())
        }
    }

    loop {
        match recv.recv(&mut DrainRecv).await {
            ResponseStreamItem::Trailers(trailers) => {
                if let Some(status) = trailers_to_tonic_status(trailers) {
                    return status;
                }
                break;
            }
            ResponseStreamItem::StreamClosed => break,
            ResponseStreamItem::Headers(_) | ResponseStreamItem::Message => {}
        }
    }
    tonic::Status::internal("grpc-rust unary send failed without server error trailers")
}

/// Receives the response message and metadata.
async fn recv_unary_response<Response>(
    recv: impl RecvStream,
) -> tonic::Result<tonic::Response<Response>>
where
    Response: Message + Default,
{
    let mut recv = RecvStreamValidator::new(recv, /* unary= */ true);
    let mut metadata = MetadataMap::new();
    let mut message: Option<Response> = None;
    let mut slot = GrpcRustRecv::<Response>::default();

    loop {
        match recv.recv(&mut slot).await {
            ResponseStreamItem::Headers(headers) => {
                metadata = headers.metadata().clone().into();
            }
            ResponseStreamItem::Message => {
                message = Some(slot.take()?);
            }
            ResponseStreamItem::Trailers(trailers) => {
                // The trailer metadata will be merged with the header metadata.
                let trailer_metadata: MetadataMap = trailers.metadata().clone().into();
                // If the server returned an error status in trailers, return that error.
                if let Some(status) = trailers_to_tonic_status(trailers) {
                    return Err(status);
                }
                let Some(msg) = message else {
                    return Err(tonic::Status::internal(
                        "grpc-rust unary call finished without response message",
                    ));
                };

                let mut headers = metadata.into_headers();
                headers.extend(trailer_metadata.into_headers());
                let metadata = MetadataMap::from_headers(headers);
                return Ok(tonic::Response::from_parts(
                    metadata,
                    msg,
                    tonic::Extensions::new(),
                ));
            }
            ResponseStreamItem::StreamClosed => {
                return Err(tonic::Status::internal(
                    "grpc-rust unary response stream closed without trailers",
                ));
            }
        }
    }
}

// TODO(#5991): More unit tests including those covering failure cases to be added in upcoming PRs.
#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use grpc::StatusCodeError;
    use grpc::StatusError;
    use grpc::client::{RecvStream, SendStream};
    use grpc::core::{RecvMessage, ResponseHeaders, SendMessage, Trailers};
    use grpc::metadata::MetadataValue;
    use pretty_assertions::assert_eq;
    use std::sync::{Arc, Mutex};

    const METHOD_NAME: &str = "/test.Service/Method";
    const RESPONSE_HEADER_KEY: &str = "x-test-header";
    const RESPONSE_HEADER_VALUE: &str = "test-val";
    const RESPONSE_TRAILER_KEY: &str = "x-test-trailer";
    const RESPONSE_TRAILER_VALUE: &str = "test-trailer-val";

    #[derive(Clone, PartialEq, Message)]
    struct TestMessage {
        #[prost(string, tag = "1")]
        value: String,
    }

    struct MockInvoker {
        observed_headers: Arc<Mutex<Option<RequestHeaders>>>,
        observed_messages: Arc<Mutex<Vec<TestMessage>>>,
        observed_send_options: Arc<Mutex<Option<SendOptions>>>,
        response_type: TestResponseType,
    }

    impl MockInvoker {
        fn new(response_type: TestResponseType) -> Self {
            Self {
                observed_headers: Arc::new(Mutex::new(None)),
                observed_messages: Arc::new(Mutex::new(Vec::new())),
                observed_send_options: Arc::new(Mutex::new(None)),
                response_type,
            }
        }

        fn observed_headers(&self) -> Option<RequestHeaders> {
            self.observed_headers
                .lock()
                .expect("lock observed headers")
                .clone()
        }

        fn observed_messages(&self) -> Vec<TestMessage> {
            self.observed_messages
                .lock()
                .expect("lock observed messages")
                .clone()
        }

        fn observed_send_options(&self) -> Option<SendOptions> {
            self.observed_send_options
                .lock()
                .expect("lock observed send options")
                .clone()
        }
    }

    #[derive(Clone)]
    enum TestResponseType {
        Success {
            value: String,
        },
        ErrorStatus {
            code: StatusCodeError,
            message: String,
        },
        MissingMessage,
        SendErrorWithServerStatus {
            code: StatusCodeError,
            message: String,
        },
    }

    impl Invoke for MockInvoker {
        type SendStream = TestSendStream;
        type RecvStream = TestRecvStream;

        async fn invoke(
            &self,
            headers: RequestHeaders,
            _options: CallOptions,
        ) -> (Self::SendStream, Self::RecvStream) {
            *self.observed_headers.lock().expect("lock observed headers") = Some(headers);
            let send_error = matches!(
                self.response_type,
                TestResponseType::SendErrorWithServerStatus { .. }
            );
            (
                TestSendStream {
                    observed_messages: self.observed_messages.clone(),
                    observed_send_options: self.observed_send_options.clone(),
                    send_error,
                },
                TestRecvStream {
                    response_type: self.response_type.clone(),
                    state: StreamState::default(),
                },
            )
        }
    }

    struct TestSendStream {
        observed_messages: Arc<Mutex<Vec<TestMessage>>>,
        observed_send_options: Arc<Mutex<Option<SendOptions>>>,
        send_error: bool,
    }

    impl SendStream for TestSendStream {
        async fn send(
            &mut self,
            message: &dyn SendMessage,
            options: SendOptions,
        ) -> Result<(), ()> {
            if self.send_error {
                return Err(());
            }
            let mut encoded = message.encode().map_err(|_| ())?;
            let decoded = TestMessage::decode(&mut encoded).map_err(|_| ())?;
            self.observed_messages
                .lock()
                .expect("lock observed messages")
                .push(decoded);
            *self
                .observed_send_options
                .lock()
                .expect("lock observed send options") = Some(options);
            Ok(())
        }
    }

    // TODO(#5991): Refactor common stream state test mocks across grpc_rust tests.
    #[derive(Default)]
    enum StreamState {
        #[default]
        Initial,
        HeadersSent,
        MessageSent,
        Done,
    }

    struct TestRecvStream {
        response_type: TestResponseType,
        state: StreamState,
    }

    impl RecvStream for TestRecvStream {
        async fn recv(&mut self, msg: &mut dyn RecvMessage) -> ResponseStreamItem {
            match (&self.response_type, &self.state) {
                (TestResponseType::Success { .. }, StreamState::Initial) => {
                    self.state = StreamState::HeadersSent;
                    let mut headers = ResponseHeaders::new();
                    headers.metadata_mut().append(
                        RESPONSE_HEADER_KEY,
                        MetadataValue::from_static(RESPONSE_HEADER_VALUE),
                    );
                    ResponseStreamItem::Headers(headers)
                }
                (TestResponseType::Success { value }, StreamState::HeadersSent) => {
                    self.state = StreamState::MessageSent;
                    let resp = TestMessage {
                        value: value.clone(),
                    };
                    let mut bytes = Bytes::from(resp.encode_to_vec());
                    msg.decode(&mut bytes).expect("decode message");
                    ResponseStreamItem::Message
                }
                (TestResponseType::Success { .. }, StreamState::MessageSent) => {
                    self.state = StreamState::Done;
                    let mut trailers = Trailers::new(Ok(()));
                    trailers.metadata_mut().append(
                        RESPONSE_TRAILER_KEY,
                        MetadataValue::from_static(RESPONSE_TRAILER_VALUE),
                    );
                    ResponseStreamItem::Trailers(trailers)
                }
                (TestResponseType::ErrorStatus { code, message }, StreamState::Initial) => {
                    self.state = StreamState::Done;
                    let trailers = Trailers::new(Err(StatusError::new(*code, message)));
                    ResponseStreamItem::Trailers(trailers)
                }
                (TestResponseType::MissingMessage, StreamState::Initial) => {
                    self.state = StreamState::HeadersSent;
                    ResponseStreamItem::Headers(ResponseHeaders::new())
                }
                (TestResponseType::MissingMessage, StreamState::HeadersSent) => {
                    self.state = StreamState::Done;
                    ResponseStreamItem::Trailers(Trailers::new(Ok(())))
                }
                (
                    TestResponseType::SendErrorWithServerStatus { code, message },
                    StreamState::Initial,
                ) => {
                    self.state = StreamState::Done;
                    let trailers = Trailers::new(Err(StatusError::new(*code, message)));
                    ResponseStreamItem::Trailers(trailers)
                }
                _ => ResponseStreamItem::StreamClosed,
            }
        }
    }

    #[tokio::test]
    async fn test_unary_success() {
        // Arrange
        let invoker = MockInvoker::new(TestResponseType::Success {
            value: "response-value".to_string(),
        });
        let request = TestMessage {
            value: "request-value".to_string(),
        };

        // Act
        let response: tonic::Response<TestMessage> = invoke_unary(
            &invoker,
            RequestHeaders::new().with_method_name(METHOD_NAME),
            request,
            CallOptions::default(),
        )
        .await
        .expect("unary invocation should succeed");

        // Assert
        assert_eq!(
            response.metadata().get(RESPONSE_HEADER_KEY),
            Some(&tonic::metadata::MetadataValue::from_static(
                RESPONSE_HEADER_VALUE
            ))
        );
        assert_eq!(
            response.metadata().get(RESPONSE_TRAILER_KEY),
            Some(&tonic::metadata::MetadataValue::from_static(
                RESPONSE_TRAILER_VALUE
            ))
        );
        assert_eq!(response.into_inner().value, "response-value");
        assert_eq!(
            invoker.observed_messages().as_slice(),
            [TestMessage {
                value: "request-value".to_string()
            }]
        );
        assert_eq!(
            invoker
                .observed_headers()
                .expect("observed headers should be set")
                .method_name(),
            METHOD_NAME
        );
        assert!(
            invoker
                .observed_send_options()
                .expect("observed send options should be set")
                .final_msg
        );
    }

    #[tokio::test]
    async fn test_unary_error_status() {
        // Arrange
        const INVALID_TOKEN_ERROR: &str = "invalid token";

        let invoker = MockInvoker::new(TestResponseType::ErrorStatus {
            code: StatusCodeError::Unauthenticated,
            message: INVALID_TOKEN_ERROR.to_string(),
        });

        // Act
        let status = invoke_unary::<_, TestMessage, _>(
            &invoker,
            RequestHeaders::new().with_method_name(METHOD_NAME),
            TestMessage {
                value: "test".to_string(),
            },
            CallOptions::default(),
        )
        .await
        .expect_err("unary invocation should fail");

        // Assert
        assert_eq!(status.code(), tonic::Code::Unauthenticated);
        assert_eq!(status.message(), INVALID_TOKEN_ERROR);
    }

    #[tokio::test]
    async fn test_unary_missing_message() {
        // Arrange
        let invoker = MockInvoker::new(TestResponseType::MissingMessage);

        // Act
        let status = invoke_unary::<_, TestMessage, _>(
            &invoker,
            RequestHeaders::new().with_method_name(METHOD_NAME),
            TestMessage {
                value: "test".to_string(),
            },
            CallOptions::default(),
        )
        .await
        .expect_err("unary invocation should fail when response message is missing");

        // Assert
        assert_eq!(status.code(), tonic::Code::Internal);
        assert!(
            status.message().contains("zero messages"),
            "expected status message to contain 'zero messages', got: {:?}",
            status.message()
        );
    }

    #[tokio::test]
    async fn test_unary_send_error_returns_server_status() {
        // Arrange
        const ACCESS_DENIED_ERROR: &str = "access denied";

        let invoker = MockInvoker::new(TestResponseType::SendErrorWithServerStatus {
            code: StatusCodeError::PermissionDenied,
            message: ACCESS_DENIED_ERROR.to_string(),
        });

        // Act
        let status = invoke_unary::<_, TestMessage, _>(
            &invoker,
            RequestHeaders::new().with_method_name(METHOD_NAME),
            TestMessage {
                value: "test".to_string(),
            },
            CallOptions::default(),
        )
        .await
        .expect_err("unary invocation should fail on send error");

        // Assert
        assert_eq!(status.code(), tonic::Code::PermissionDenied);
        assert_eq!(status.message(), ACCESS_DENIED_ERROR);
    }
}
