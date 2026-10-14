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
