// Copyright 2025 Google LLC
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

#[cfg(all(test, feature = "_internal-grpc-client"))]
mod tests {
    use google_cloud_auth::credentials::{
        Credentials, anonymous::Builder as Anonymous, testing::error_credentials,
    };
    use google_cloud_gax::error::rpc::Code;
    use google_cloud_gax::options::*;
    use google_cloud_gax::retry_policy::NeverRetry;
    use google_cloud_gax_internal::attempt_interceptor::AttemptInterceptor;
    use google_cloud_gax_internal::grpc;
    use grpc_server::google::test::v1::{EchoRequest, EchoResponse};
    use grpc_server::transport::{EchoStream, EchoTransport, transport_builder};
    use grpc_server::{both_transports, builder, start_echo_server};
    use http::HeaderMap;
    use pretty_assertions::assert_eq;
    use std::sync::Arc;

    fn test_credentials() -> Credentials {
        Anonymous::new().build()
    }

    both_transports!(basic_stream);
    async fn basic_stream<C: EchoTransport>() -> anyhow::Result<()> {
        // Arrange
        let (endpoint, _server) = start_echo_server().await?;
        let client = transport_builder::<C>(endpoint)
            .with_credentials(test_credentials())
            .build()
            .await?;
        let (tx, rx) = tokio::sync::mpsc::channel(100);
        tx.send(simple_request("msg0")).await?;

        // Act
        let response = send_streaming_request(client.clone(), rx, "resource=test").await?;
        let (metadata, mut stream, _) = response.into_parts();

        // Assert
        assert!(
            !metadata.is_empty(),
            "expected initial metadata in the response"
        );
        let r = stream.message().await;
        assert!(matches!(r, Ok(Some(ref m)) if m.message == "msg0"), "{r:?}");
        tx.send(simple_request("msg1")).await?;
        let r = stream.message().await;
        assert!(matches!(r, Ok(Some(ref m)) if m.message == "msg1"), "{r:?}");
        tx.send(simple_request("msg2")).await?;
        let r = stream.message().await;
        assert!(matches!(r, Ok(Some(ref m)) if m.message == "msg2"), "{r:?}");

        drop(tx);
        let r = stream.message().await;
        assert!(matches!(r, Ok(None)), "{r:?}");

        Ok(())
    }

    both_transports!(ends_with_error);
    async fn ends_with_error<C: EchoTransport>() -> anyhow::Result<()> {
        // Arrange
        let (endpoint, _server) = start_echo_server().await?;
        let client = transport_builder::<C>(endpoint)
            .with_credentials(test_credentials())
            .build()
            .await?;
        let (tx, rx) = tokio::sync::mpsc::channel(100);
        tx.send(simple_request("msg0")).await?;

        // Act
        let response = send_streaming_request(client.clone(), rx, "resource=test").await?;
        let (_metadata, mut stream, _) = response.into_parts();

        // Assert
        let r = stream.message().await;
        assert!(matches!(r, Ok(Some(ref m)) if m.message == "msg0"), "{r:?}");
        // An empty message makes the echo server return InvalidArgument and close the stream.
        tx.send(simple_request("")).await?;
        let err = stream
            .message()
            .await
            .expect_err("mid-stream empty message should fail");
        assert_eq!(err.code(), tonic::Code::InvalidArgument, "{err:?}");
        assert!(err.message().contains("empty message"), "{err:?}");

        // Keep `tx` open to verify that the server error alone ends the stream.
        let r = stream.message().await;
        assert!(matches!(r, Ok(None)), "{r:?}");
        drop(tx);

        Ok(())
    }

    both_transports!(request_error);
    async fn request_error<C: EchoTransport>() -> anyhow::Result<()> {
        // Arrange
        let (endpoint, _server) = start_echo_server().await?;
        let client = transport_builder::<C>(endpoint)
            .with_credentials(test_credentials())
            .build()
            .await?;
        let (_tx, rx) = tokio::sync::mpsc::channel(100);

        // Act
        let response = send_streaming_request(client.clone(), rx, "resource=error").await;

        // Assert
        let err = response.err().expect("the stream should fail to start");
        let status = err.status().expect("the error should have a status");
        assert_eq!(status.code, Code::Aborted, "{err:?}");
        assert_eq!(status.message, "test with initial error");
        Ok(())
    }

    both_transports!(request_error_status);
    async fn request_error_status<C: EchoTransport>() -> anyhow::Result<()> {
        // Arrange
        let (endpoint, _server) = start_echo_server().await?;
        let client = transport_builder::<C>(endpoint)
            .with_credentials(test_credentials())
            .build()
            .await?;
        let (_tx, rx) = tokio::sync::mpsc::channel(100);

        // Act
        let response =
            send_streaming_request_with_status(client.clone(), rx, "resource=error").await?;

        // Assert
        let status = response.err().expect("the stream should fail to start");
        assert_eq!(status.code(), tonic::Code::Aborted, "{status:?}");
        assert_eq!(status.message(), "test with initial error", "{status:?}");
        Ok(())
    }

    both_transports!(with_status_basic);
    async fn with_status_basic<C: EchoTransport>() -> anyhow::Result<()> {
        // Arrange
        let (endpoint, _server) = start_echo_server().await?;
        let client = transport_builder::<C>(endpoint)
            .with_credentials(test_credentials())
            .build()
            .await?;
        let (tx, rx) = tokio::sync::mpsc::channel(100);
        tx.send(simple_request("msg0")).await?;

        // Act
        // The first `?` handles transport errors, the second one the RPC status.
        let response =
            send_streaming_request_with_status(client.clone(), rx, "resource=test").await??;
        let (metadata, mut stream, _) = response.into_parts();

        // Assert
        assert!(
            !metadata.is_empty(),
            "expected initial metadata in the response"
        );
        let r = stream.message().await;
        assert!(matches!(r, Ok(Some(ref m)) if m.message == "msg0"), "{r:?}");
        drop(tx);
        let r = stream.message().await;
        assert!(matches!(r, Ok(None)), "{r:?}");
        Ok(())
    }

    #[tokio::test]
    async fn with_status_api_client_header() -> anyhow::Result<()> {
        check_with_status_api_client_header::<grpc::Client>("tonic").await
    }

    #[cfg(google_cloud_unstable_grpc_rust)]
    #[tokio::test]
    async fn with_status_api_client_header_grpc_rust() -> anyhow::Result<()> {
        check_with_status_api_client_header::<grpc_server::transport::GrpcRustClient>("grpc-rust")
            .await
    }

    async fn check_with_status_api_client_header<C: EchoTransport>(
        expected_transport: &str,
    ) -> anyhow::Result<()> {
        // Arrange
        let (endpoint, _server) = start_echo_server().await?;
        let client = transport_builder::<C>(endpoint)
            .with_credentials(test_credentials())
            .with_extension(google_cloud_gax_internal::api_header::XGoogApiClient {
                name: "storage",
                library_type: google_cloud_gax_internal::api_header::GCCL,
                version: "1.0.0",
            })
            .build()
            .await?;
        let (tx, rx) = tokio::sync::mpsc::channel(100);
        tx.send(simple_request("msg0")).await?;

        // Act
        let response =
            send_streaming_request_with_status(client.clone(), rx, "resource=test").await??;
        let (_metadata, mut stream, _) = response.into_parts();
        let res = stream
            .message()
            .await?
            .expect("stream should yield a message");

        // Assert
        let header = res
            .metadata
            .get("x-goog-api-client")
            .expect("header present");
        let want_grpc = format!("grpc/{}-{expected_transport}", env!("CARGO_PKG_VERSION"));
        assert!(header.contains("gccl/1.0.0"), "{header}");
        assert!(header.contains(&want_grpc), "{header}");
        assert!(!header.contains("gapic/"), "{header}");
        Ok(())
    }

    #[tokio::test]
    async fn attempt_interceptor() -> anyhow::Result<()> {
        use http::header::{HeaderName, HeaderValue};

        #[derive(Debug)]
        struct TestInterceptor;
        impl AttemptInterceptor for TestInterceptor {
            fn intercept(&self, headers: &mut HeaderMap, attempt: u32) {
                headers.insert(
                    HeaderName::from_static("x-test-attempt"),
                    HeaderValue::from_str(&attempt.to_string()).expect("valid attempt number"),
                );
            }
        }

        let (endpoint, _server) = start_echo_server().await?;

        let mut config = google_cloud_gax_internal::options::ClientConfig::default();
        config.cred = Some(test_credentials());
        config.endpoint = Some(endpoint);

        let mut client = grpc::Client::new(config, "https://test-only.googleapis.com").await?;
        client.set_attempt_interceptor(Arc::new(TestInterceptor));

        let (tx, rx) = tokio::sync::mpsc::channel(100);
        tx.send(simple_request("msg0")).await?;
        let response = send_streaming_request(client, rx, "").await?;
        let (_, mut stream, _) = response.into_parts();
        let first_msg = stream
            .message()
            .await?
            .expect("stream should yield a message");
        assert_eq!(
            first_msg.metadata.get("x-test-attempt").map(String::as_str),
            Some("1")
        );

        Ok(())
    }

    both_transports!(credentials_error);
    async fn credentials_error<C: EchoTransport>() -> anyhow::Result<()> {
        // Arrange
        let (endpoint, _server) = start_echo_server().await?;
        let client = transport_builder::<C>(endpoint)
            .with_credentials(error_credentials(false))
            .build()
            .await?;
        let (tx, rx) = tokio::sync::mpsc::channel(100);
        tx.send(simple_request("msg0")).await?;

        // Act
        let response = send_streaming_request(client.clone(), rx, "").await;

        // Assert
        let err = response
            .err()
            .expect("the credentials error should be returned");
        assert!(err.is_authentication(), "{err:?}");
        Ok(())
    }

    both_transports!(connection_error);
    async fn connection_error<C: EchoTransport>() -> anyhow::Result<()> {
        // Arrange
        let client = transport_builder::<C>("http://127.0.0.1:1")
            .with_credentials(test_credentials())
            .build()
            .await;
        let client = client.expect("clients should use lazy connections");
        let (tx, rx) = tokio::sync::mpsc::channel(100);
        tx.send(simple_request("msg0")).await?;

        // Act
        let response = send_streaming_request(client.clone(), rx, "").await;

        // Assert
        let err = response.err().expect("the connection should fail");
        assert!(err.is_connect(), "{err:?}");
        Ok(())
    }

    both_transports!(remains_usable_after_cancellation);
    async fn remains_usable_after_cancellation<C: EchoTransport>() -> anyhow::Result<()> {
        // Arrange
        let (endpoint, _server) = start_echo_server().await?;
        let client = transport_builder::<C>(endpoint)
            .with_credentials(test_credentials())
            .build()
            .await?;
        let (tx, rx) = tokio::sync::mpsc::channel(100);
        let mut stream = send_streaming_request(client.clone(), rx, "")
            .await?
            .into_inner();

        // Act
        // Nothing was sent yet, so this receive times out and is cancelled.
        let timeout = std::time::Duration::from_millis(50);
        let cancelled = tokio::time::timeout(timeout, stream.message()).await;

        // Assert
        assert!(
            cancelled.is_err(),
            "the first receive should time out: {cancelled:?}"
        );

        // Act
        tx.send(simple_request("msg0")).await?;
        let r = stream.message().await;

        // Assert
        assert!(matches!(r, Ok(Some(ref m)) if m.message == "msg0"), "{r:?}");
        Ok(())
    }

    // NOTE(#5991): Only `GrpcRustClient` closes the request stream when the
    // response stream is dropped. With Tonic, dropping the response releases
    // only the receiving half of the HTTP/2 stream, and hyper keeps sending
    // the requests until they end, so the request stream stays open.
    #[cfg(google_cloud_unstable_grpc_rust)]
    #[tokio::test]
    async fn drop_closes_channel() -> anyhow::Result<()> {
        // Arrange
        let (endpoint, _server) = start_echo_server().await?;
        let client = transport_builder::<grpc_server::transport::GrpcRustClient>(endpoint)
            .with_credentials(test_credentials())
            .build()
            .await?;
        let (tx, rx) = tokio::sync::mpsc::channel(100);
        tx.send(simple_request("msg0")).await?;
        let mut stream = send_streaming_request(client.clone(), rx, "")
            .await?
            .into_inner();
        let r = stream.message().await;
        assert!(matches!(r, Ok(Some(ref m)) if m.message == "msg0"), "{r:?}");

        // Act
        drop(stream);

        // Assert
        tokio::time::timeout(std::time::Duration::from_secs(5), tx.closed())
            .await
            .expect("dropping the stream should close the request channel");
        Ok(())
    }

    fn simple_request(msg: &str) -> EchoRequest {
        EchoRequest {
            message: msg.into(),
            ..Default::default()
        }
    }

    async fn send_streaming_request<C: EchoTransport>(
        client: C,
        rx: tokio::sync::mpsc::Receiver<EchoRequest>,
        request_params: &str,
    ) -> google_cloud_gax::Result<tonic::Response<C::Stream>> {
        let requests = tokio_stream::wrappers::ReceiverStream::new(rx);
        client.chat(requests, never_retry(), request_params).await
    }

    async fn send_streaming_request_with_status<C: EchoTransport>(
        client: C,
        rx: tokio::sync::mpsc::Receiver<EchoRequest>,
        request_params: &str,
    ) -> google_cloud_gax::Result<tonic::Result<tonic::Response<C::Stream>>> {
        let requests = tokio_stream::wrappers::ReceiverStream::new(rx);
        client
            .chat_with_status(requests, never_retry(), request_params)
            .await
    }

    /// Returns request options that disable retries.
    fn never_retry() -> RequestOptions {
        let mut options = RequestOptions::default();
        options.set_retry_policy(NeverRetry);
        options
    }

    #[derive(Clone, Debug, PartialEq)]
    struct DomainEchoRequest {
        message: String,
    }

    impl google_cloud_gax_internal::prost::ToProto<EchoRequest> for DomainEchoRequest {
        type Output = EchoRequest;
        fn to_proto(
            self,
        ) -> std::result::Result<EchoRequest, google_cloud_gax_internal::prost::ConvertError>
        {
            Ok(EchoRequest {
                message: self.message,
                ..Default::default()
            })
        }
    }

    #[derive(Clone, Debug, PartialEq)]
    struct DomainEchoResponse {
        message: String,
    }

    impl google_cloud_gax_internal::prost::FromProto<DomainEchoResponse> for EchoResponse {
        fn cnv(
            self,
        ) -> std::result::Result<DomainEchoResponse, google_cloud_gax_internal::prost::ConvertError>
        {
            Ok(DomainEchoResponse {
                message: self.message,
            })
        }
    }

    #[tokio::test]
    async fn execute_bidi_streaming_basic() -> anyhow::Result<()> {
        let (endpoint, _server) = start_echo_server().await?;
        let client = builder(endpoint)
            .with_credentials(test_credentials())
            .build()
            .await?;

        let extensions = {
            let mut e = tonic::Extensions::new();
            e.insert(tonic::GrpcMethod::new(
                "google.test.v1.EchoServices",
                "Chat",
            ));
            e
        };
        let request_options = {
            let mut o = RequestOptions::default();
            o.set_retry_policy(NeverRetry);
            o
        };

        let (sender, mut resp_stream) = client
            .execute_bidi_streaming::<DomainEchoRequest, DomainEchoResponse, EchoRequest, EchoResponse>(
                extensions,
                http::uri::PathAndQuery::from_static("/google.test.v1.EchoService/Chat"),
                request_options,
                "test-only-api-client/1.0",
                "resource=test",
            );

        sender
            .send(DomainEchoRequest {
                message: "msg0".to_string(),
            })
            .await?;

        let r = resp_stream.next().await;
        assert_eq!(
            r.transpose()?,
            Some(DomainEchoResponse {
                message: "msg0".to_string()
            })
        );

        sender
            .send(DomainEchoRequest {
                message: "msg1".to_string(),
            })
            .await?;
        let r = resp_stream.next().await;
        assert_eq!(
            r.transpose()?,
            Some(DomainEchoResponse {
                message: "msg1".to_string()
            })
        );

        drop(sender);
        let r = resp_stream.next().await;
        assert!(r.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn large_response() -> anyhow::Result<()> {
        let (endpoint, _server) = start_echo_server().await?;
        let client = builder(endpoint)
            .with_credentials(test_credentials())
            .build()
            .await?;

        // Tonic's default decoding limit is 4 MiB; verify streamed messages > 4 MiB succeed.
        let large_msg = "a".repeat(5 * 1024 * 1024);
        let (tx, rx) = tokio::sync::mpsc::channel(100);
        tx.send(simple_request(&large_msg)).await?;
        drop(tx);

        let response = send_streaming_request(client.clone(), rx, "resource=test").await?;
        let (_metadata, mut stream, _) = response.into_parts();
        let r = stream.message().await?.expect("expected a message");
        assert_eq!(r.message.len(), large_msg.len());
        assert!(stream.message().await?.is_none());

        Ok(())
    }
}
