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

#[cfg(all(
    test,
    feature = "_internal-grpc-client",
    google_cloud_unstable_grpc_rust
))]
mod tests {
    // TODO(#5991): Consider refactoring some tests to run against both `grpc::Client` and `grpc::GrpcRustClient`.

    use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
    use google_cloud_gax::options::RequestOptions;
    use google_cloud_gax_internal::grpc::GrpcRustClient;
    use google_cloud_gax_internal::grpc::grpc_rust::GrpcRustStreaming;
    use google_cloud_gax_internal::options::ClientConfig;
    use grpc_server::google::test::v1::{EchoRequest, EchoResponse};
    use grpc_server::start_echo_server;
    use pretty_assertions::assert_eq;

    const MSG1: &str = "msg1";
    const MSG2: &str = "msg2";

    #[tokio::test]
    async fn test_bidi_stream() -> anyhow::Result<()> {
        // Arrange
        let mut session = start_bidi_stream().await?;
        assert!(
            !session.metadata.is_empty(),
            "expected initial metadata headers from response"
        );

        // Act
        send_echo_request(&session.tx, MSG1).await?;

        // Assert
        let res1 = recv_echo_response(&mut session.stream).await?;
        assert_eq!(res1.message, MSG1);

        // Act
        send_echo_request(&session.tx, MSG2).await?;

        // Assert
        let res2 = recv_echo_response(&mut session.stream).await?;
        assert_eq!(res2.message, MSG2);

        // Act
        drop(session.tx);

        // Assert
        let end_res = session.stream.message().await?;
        assert_eq!(end_res, None, "stream should yield None upon completion");

        Ok(())
    }

    #[tokio::test]
    async fn test_bidi_stream_remains_usable_even_after_cancellation() -> anyhow::Result<()> {
        // Arrange
        let mut session = start_bidi_stream().await?;

        // Act
        // Attempt to receive the next item with a short timeout before any message is
        // sent, thereby cancelling recv_echo_response
        let cancelled_read = tokio::time::timeout(
            std::time::Duration::from_millis(50),
            recv_echo_response(&mut session.stream),
        )
        .await;

        // Assert
        assert!(
            cancelled_read.is_err(),
            "cancelled_read should time out when no request is sent"
        );

        // Act
        send_echo_request(&session.tx, MSG1).await?;

        // Assert
        let res = recv_echo_response(&mut session.stream).await?;
        assert_eq!(res.message, MSG1);

        Ok(())
    }

    #[tokio::test]
    async fn test_bidi_stream_drop_closes_channel() -> anyhow::Result<()> {
        // Arrange
        let mut session = start_bidi_stream().await?;

        // Act
        send_echo_request(&session.tx, MSG1).await?;

        // Assert
        let res = recv_echo_response(&mut session.stream).await?;
        assert_eq!(res.message, MSG1);

        // Act
        drop(session.stream);

        // Assert
        tokio::time::timeout(std::time::Duration::from_secs(5), session.tx.closed())
            .await
            .expect("dropping the stream should close the request channel");

        Ok(())
    }

    #[tokio::test]
    async fn test_bidi_stream_server_error_mid_stream() -> anyhow::Result<()> {
        // Arrange
        let mut session = start_bidi_stream().await?;

        // Act
        send_echo_request(&session.tx, MSG1).await?;

        // Assert
        let res1 = recv_echo_response(&mut session.stream).await?;
        assert_eq!(res1.message, MSG1);

        // Act
        // Sending an empty message causes our test echo server to return InvalidArgument and close the stream
        send_echo_request(&session.tx, "").await?;

        // Assert
        let err = session
            .stream
            .message()
            .await
            .expect_err("stream should return status error when server fails mid-stream");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(
            err.message().contains("empty message"),
            "expected 'empty message' in error message, got '{}'",
            err.message()
        );

        // Assert
        let subsequent = session.stream.message().await?;
        assert_eq!(
            subsequent, None,
            "subsequent calls after stream termination should yield None"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_bidi_stream_initial_error() -> anyhow::Result<()> {
        // Act
        let res = start_bidi_stream_with_params("resource=error").await;

        // Assert
        let err = res
            .expect_err("bidi_stream should fail when server returns an initial error")
            .downcast::<google_cloud_gax::error::Error>()
            .expect("expected a google_cloud_gax::error::Error");

        let status = err.status().expect("expected status");
        assert_eq!(status.code, google_cloud_gax::error::rpc::Code::Aborted);
        assert_eq!(status.message, "test with initial error");

        Ok(())
    }

    #[tokio::test]
    async fn test_bidi_stream_with_status() -> anyhow::Result<()> {
        // Arrange
        let mut session = start_bidi_stream_with_status()
            .await?
            .expect("should succeed");
        assert!(
            !session.metadata.is_empty(),
            "expected initial metadata headers from response"
        );

        // Act
        send_echo_request(&session.tx, MSG1).await?;

        // Assert
        let res1 = recv_echo_response(&mut session.stream).await?;
        assert_eq!(res1.message, MSG1);

        // Act
        send_echo_request(&session.tx, MSG2).await?;

        // Assert
        let res2 = recv_echo_response(&mut session.stream).await?;
        assert_eq!(res2.message, MSG2);

        // Act
        drop(session.tx);

        // Assert
        let end_res = session.stream.message().await?;
        assert_eq!(end_res, None, "stream should yield None upon completion");

        Ok(())
    }

    #[tokio::test]
    async fn test_bidi_stream_with_status_initial_error() -> anyhow::Result<()> {
        // Act
        let result = start_bidi_stream_with_status_params("resource=error").await?;

        // Assert
        let err = result.expect_err("should return Err(status) on initial error");
        assert_eq!(err.code(), tonic::Code::Aborted);
        assert!(
            err.message().contains("test with initial error"),
            "expected 'test with initial error' in error message, got '{}'",
            err.message()
        );

        Ok(())
    }

    struct TestBidiSession {
        /// For sending outbound request messages.
        tx: tokio::sync::mpsc::Sender<EchoRequest>,
        /// For reading inbound response messages.
        stream: GrpcRustStreaming<EchoResponse>,
        /// Initial server response metadata.
        metadata: tonic::metadata::MetadataMap,
        _client: GrpcRustClient,
        /// Task handle for the echo server.
        _server_task: tokio::task::JoinHandle<()>,
    }

    impl std::fmt::Debug for TestBidiSession {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("TestBidiSession")
                .field("tx", &self.tx)
                .field("metadata", &self.metadata)
                .finish_non_exhaustive()
        }
    }

    /// Starts an echo server and initializes a bidirectional streaming RPC using `bidi_stream`.
    async fn start_bidi_stream() -> anyhow::Result<TestBidiSession> {
        start_bidi_stream_with_params("").await
    }

    /// Starts an echo server and initializes a bidirectional streaming RPC using `bidi_stream` with custom request parameters.
    async fn start_bidi_stream_with_params(
        request_params: &str,
    ) -> anyhow::Result<TestBidiSession> {
        let (endpoint, server_task) = start_echo_server().await?;
        let mut config = ClientConfig::default();
        config.cred = Some(Anonymous::new().build());
        let client = GrpcRustClient::new(config, &endpoint).await?;

        let (tx, rx) = tokio::sync::mpsc::channel::<EchoRequest>(10);
        let request_stream = tokio_stream::wrappers::ReceiverStream::new(rx);

        let response = client
            .bidi_stream::<EchoRequest, EchoResponse>(
                tonic::Extensions::new(),
                http::uri::PathAndQuery::from_static("/google.test.v1.EchoService/Chat"),
                request_stream,
                RequestOptions::default(),
                "test-only-api-client/1.0",
                request_params,
            )
            .await?;

        let (metadata, stream, _) = response.into_parts();
        Ok(TestBidiSession {
            tx,
            stream,
            metadata,
            _client: client,
            _server_task: server_task,
        })
    }

    /// Starts an echo server and initializes a bidirectional streaming RPC using `bidi_stream_with_status`.
    async fn start_bidi_stream_with_status() -> anyhow::Result<tonic::Result<TestBidiSession>> {
        start_bidi_stream_with_status_params("").await
    }

    /// Starts an echo server and initializes a bidirectional streaming RPC using `bidi_stream_with_status` with custom request parameters.
    async fn start_bidi_stream_with_status_params(
        request_params: &str,
    ) -> anyhow::Result<tonic::Result<TestBidiSession>> {
        let (endpoint, server_task) = start_echo_server().await?;
        let mut config = ClientConfig::default();
        config.cred = Some(Anonymous::new().build());
        let client = GrpcRustClient::new(config, &endpoint).await?;

        let (tx, rx) = tokio::sync::mpsc::channel::<EchoRequest>(10);
        let request_stream = tokio_stream::wrappers::ReceiverStream::new(rx);

        let result = client
            .bidi_stream_with_status::<EchoRequest, EchoResponse>(
                tonic::Extensions::new(),
                http::uri::PathAndQuery::from_static("/google.test.v1.EchoService/Chat"),
                request_stream,
                RequestOptions::default(),
                "test-only-api-client/1.0",
                request_params,
            )
            .await?;

        match result {
            Ok(response) => {
                let (metadata, stream, _) = response.into_parts();
                Ok(Ok(TestBidiSession {
                    tx,
                    stream,
                    metadata,
                    _client: client,
                    _server_task: server_task,
                }))
            }
            Err(status) => Ok(Err(status)),
        }
    }

    /// Sends an echo request with the given message string.
    async fn send_echo_request(
        tx: &tokio::sync::mpsc::Sender<EchoRequest>,
        msg: &str,
    ) -> anyhow::Result<()> {
        tx.send(EchoRequest {
            message: msg.to_string(),
            ..Default::default()
        })
        .await
        .map_err(|_| anyhow::anyhow!("failed to send message '{msg}'"))
    }

    /// Receives the next echo response from the stream.
    async fn recv_echo_response(
        stream: &mut GrpcRustStreaming<EchoResponse>,
    ) -> anyhow::Result<EchoResponse> {
        stream
            .message()
            .await?
            .ok_or_else(|| anyhow::anyhow!("expected response message, got end of stream"))
    }
}
