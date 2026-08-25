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

use super::metadata::to_tonic_map;
use super::receive::{GrpcRustRecv, trailers_to_tonic_status};
use super::send::GrpcRustSend;
use bytes::Buf;
use grpc::client::stream_util::RecvStreamValidator;
use grpc::client::{
    CallOptions, Invoke, RecvStream, RequestHeaders, ResponseStreamItem, SendOptions, SendStream,
};
use grpc::core::RecvMessage;
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
                metadata = to_tonic_map(headers.metadata());
            }
            ResponseStreamItem::Message => {
                message = Some(slot.take()?);
            }
            ResponseStreamItem::Trailers(trailers) => {
                // The trailer metadata will be merged with the header metadata.
                let trailer_metadata = to_tonic_map(trailers.metadata());
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
    use super::super::testing::*;
    use super::*;
    use grpc::client::{ResponseHeaders, Trailers};
    use grpc::metadata::MetadataValue;
    use grpc::{StatusCodeError, StatusError};
    use pretty_assertions::assert_eq;

    const METHOD_NAME: &str = "/test.Service/Method";
    const RESPONSE_HEADER_KEY: &str = "x-test-header";
    const RESPONSE_HEADER_VALUE: &str = "test-val";
    const RESPONSE_TRAILER_KEY: &str = "x-test-trailer";
    const RESPONSE_TRAILER_VALUE: &str = "test-trailer-val";

    #[tokio::test]
    async fn test_unary_success() {
        // Arrange
        let send_stream = MockSendStream::new();

        let mut headers = ResponseHeaders::new();
        headers.metadata_mut().append(
            RESPONSE_HEADER_KEY,
            MetadataValue::from_static(RESPONSE_HEADER_VALUE),
        );
        let mut trailers = Trailers::new(Ok(()));
        trailers.metadata_mut().append(
            RESPONSE_TRAILER_KEY,
            MetadataValue::from_static(RESPONSE_TRAILER_VALUE),
        );
        let recv_stream = MockRecvStream::new([
            MockRecvAction::Headers(headers),
            MockRecvAction::Message(TestMessage::new("response-value")),
            MockRecvAction::Trailers(trailers),
        ]);

        let invoker = MockInvoker::new(send_stream.clone(), recv_stream);
        let request = TestMessage::new("request-value");

        // Act
        let response: tonic::Response<TestMessage> = invoke_unary(
            &invoker,
            RequestHeaders::new().with_method_name(METHOD_NAME),
            request.clone(),
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
        assert_eq!(send_stream.observed_messages(), [request]);
        assert_eq!(
            invoker
                .observed_headers()
                .as_ref()
                .expect("observed headers should be set")
                .method_name(),
            METHOD_NAME
        );
        assert!(
            send_stream
                .observed_send_options()
                .expect("observed send options should be set")
                .final_msg
        );
    }

    #[tokio::test]
    async fn test_unary_error_status() {
        // Arrange
        const INVALID_TOKEN_ERROR: &str = "invalid token";

        let invoker = MockInvoker::new(
            MockSendStream::default(),
            MockRecvStream::with_immediate_trailers(Trailers::new(Err(StatusError::new(
                StatusCodeError::Unauthenticated,
                INVALID_TOKEN_ERROR,
            )))),
        );

        // Act
        let status = invoke_unary::<_, TestMessage, _>(
            &invoker,
            RequestHeaders::new().with_method_name(METHOD_NAME),
            TestMessage::new("test"),
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
        let invoker = MockInvoker::new(
            MockSendStream::default(),
            MockRecvStream::with_headers_and_trailers(
                ResponseHeaders::new(),
                Trailers::new(Ok(())),
            ),
        );

        // Act
        let status = invoke_unary::<_, TestMessage, _>(
            &invoker,
            RequestHeaders::new().with_method_name(METHOD_NAME),
            TestMessage::new("test"),
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

        let invoker = MockInvoker::new(
            FailingSendStream,
            MockRecvStream::with_immediate_trailers(Trailers::new(Err(StatusError::new(
                StatusCodeError::PermissionDenied,
                ACCESS_DENIED_ERROR,
            )))),
        );

        // Act
        let status = invoke_unary::<_, TestMessage, _>(
            &invoker,
            RequestHeaders::new().with_method_name(METHOD_NAME),
            TestMessage::new("test"),
            CallOptions::default(),
        )
        .await
        .expect_err("unary invocation should fail on send error");

        // Assert
        assert_eq!(status.code(), tonic::Code::PermissionDenied);
        assert_eq!(status.message(), ACCESS_DENIED_ERROR);
    }
}
