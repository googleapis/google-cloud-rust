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
    use google_cloud_gax_internal::grpc::GrpcRustClient;
    use google_cloud_gax_internal::grpc::grpc_rust::bidi::{GrpcRustSend, ReceiveTask};
    use google_cloud_gax_internal::options::ClientConfig;
    use grpc::client::{CallOptions, Invoke, SendOptions, SendStream};
    use grpc::core::RequestHeaders;
    use grpc_server::google::test::v1::{EchoRequest, EchoResponse};
    use grpc_server::start_echo_server;
    use pretty_assertions::assert_eq;

    const MSG1: &str = "msg1";
    const MSG2: &str = "msg2";

    #[tokio::test]
    async fn test_grpc_rust_client_streaming_response_pump() -> anyhow::Result<()> {
        // Arrange
        let (_client, mut send_stream, mut rx, _pump_task, _server_task) =
            start_client_stream().await?;

        // Act
        send_echo_request(&mut send_stream, MSG1).await?;

        // Assert
        let res1 = recv_echo_response(&mut rx).await?;
        assert_eq!(res1.message, MSG1);

        // Act
        send_echo_request(&mut send_stream, MSG2).await?;

        // Assert
        let res2 = recv_echo_response(&mut rx).await?;
        assert_eq!(res2.message, MSG2);

        // Act
        drop(send_stream);

        // Assert
        let end_res = rx
            .recv()
            .await
            .expect("response pump should yield stream termination")?;
        assert_eq!(end_res, None);

        Ok(())
    }

    #[tokio::test]
    async fn test_grpc_rust_client_streaming_stream_remains_usable_even_after_cancellation()
    -> anyhow::Result<()> {
        // Arrange
        let (_client, mut send_stream, mut rx, _pump_task, _server_task) =
            start_client_stream().await?;

        // Act
        // Attempt to receive with a short timeout before any message is
        // sent, thereby cancelling rx.recv()
        let recv_result =
            tokio::time::timeout(std::time::Duration::from_millis(50), rx.recv()).await;

        // Assert
        assert!(
            recv_result.is_err(),
            "recv should time out when no message is sent"
        );

        // Act
        // Send request message after read cancellation
        send_echo_request(&mut send_stream, MSG1).await?;

        // Assert
        // Receiver should still successfully yield the sent message
        let res = recv_echo_response(&mut rx).await?;
        assert_eq!(res.message, MSG1);

        Ok(())
    }

    #[tokio::test]
    async fn test_grpc_rust_client_streaming_cancel_pump_task() -> anyhow::Result<()> {
        // Arrange
        let (_client, mut send_stream, mut rx, pump_task, _server_task) =
            start_client_stream().await?;

        // Act
        send_echo_request(&mut send_stream, MSG1).await?;

        // Assert
        let res1 = recv_echo_response(&mut rx).await?;
        assert_eq!(res1.message, MSG1);

        // Act
        drop(pump_task);

        // Assert
        let end_res = rx.recv().await;
        assert!(
            end_res.is_none(),
            "channel should return None after pump_task is dropped"
        );

        Ok(())
    }

    /// Starts an echo server and initializes a streaming RPC for testing.
    async fn start_client_stream() -> anyhow::Result<(
        GrpcRustClient,
        impl SendStream,
        tokio::sync::mpsc::Receiver<tonic::Result<Option<EchoResponse>>>,
        ReceiveTask,
        tokio::task::JoinHandle<()>,
    )> {
        let (endpoint, server_task) = start_echo_server().await?;
        let mut config = ClientConfig::default();
        config.cred = Some(Anonymous::new().build());
        let client = GrpcRustClient::new(config, &endpoint).await?;

        // Start the RPC
        let headers = RequestHeaders::new().with_method_name("/google.test.v1.EchoService/Chat");
        let (send_stream, recv_stream) = client
            .invoker()
            .invoke(headers, CallOptions::default())
            .await;

        // Start the response pump
        let (rx, pump_task) = ReceiveTask::start::<EchoResponse, _>(recv_stream);

        Ok((client, send_stream, rx, pump_task, server_task))
    }

    /// Sends the given message to the echo server
    async fn send_echo_request(send_stream: &mut impl SendStream, msg: &str) -> anyhow::Result<()> {
        send_stream
            .send(
                &GrpcRustSend(EchoRequest {
                    message: msg.to_string(),
                    ..Default::default()
                }),
                SendOptions::default(),
            )
            .await
            .map_err(|_| anyhow::anyhow!("failed to send message '{msg}'"))
    }

    /// Receives a response item from the response pump
    async fn recv_echo_response(
        rx: &mut tokio::sync::mpsc::Receiver<tonic::Result<Option<EchoResponse>>>,
    ) -> anyhow::Result<EchoResponse> {
        let res = rx
            .recv()
            .await
            .expect("response pump should yield a response item")?
            .expect("expected a response message");
        Ok(res)
    }
}
