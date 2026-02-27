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

#[cfg(all(test, feature = "_internal-grpc-client", google_cloud_unstable_grpc_server_streaming))]
mod tests {
    use google_cloud_auth::credentials::{
        Credentials, anonymous::Builder as Anonymous, testing::error_credentials,
    };
    use google_cloud_gax::options::*;
    use google_cloud_gax::retry_policy::NeverRetry;
    use google_cloud_gax_internal::grpc;
    use grpc_server::google::test::v1::{EchoRequest, EchoResponse};
    use grpc_server::{builder, start_echo_server};

    fn test_credentials() -> Credentials {
        Anonymous::new().build()
    }

    #[tokio::test]
    async fn basic_stream() -> anyhow::Result<()> {
        let (endpoint, _server) = start_echo_server().await?;
        let client = builder(endpoint)
            .with_credentials(test_credentials())
            .build()
            .await?;

        let response =
            send_server_streaming_request(client.clone(), "msg0 msg1", "resource=test").await?;
        let (_metadata, mut stream, _extensions) = response.into_parts();
        let r: tonic::Result<Option<EchoResponse>> = stream.message().await;
        assert!(matches!(r, Ok(Some(ref m)) if m.message == "msg0"), "{r:?}");
        let r: tonic::Result<Option<EchoResponse>> = stream.message().await;
        assert!(matches!(r, Ok(Some(ref m)) if m.message == "msg1"), "{r:?}");

        let r: tonic::Result<Option<EchoResponse>> = stream.message().await;
        assert!(matches!(r, Ok(None)), "{r:?}");

        Ok(())
    }

    #[tokio::test]
    async fn ends_with_error() -> anyhow::Result<()> {
        let (endpoint, _server) = start_echo_server().await?;
        let client = builder(endpoint)
            .with_credentials(test_credentials())
            .build()
            .await?;

        // The server is configured to return an error if the message is empty.
        // However, "Expand" splits by space, so "msg0 " will result in "msg0" then "".
        // "".to_string() will cause the server to return invalid argument.
        let response =
            send_server_streaming_request(client.clone(), "msg0 ", "resource=test").await?;
        let (_metadata, mut stream, _extensions) = response.into_parts();
        let r: tonic::Result<Option<EchoResponse>> = stream.message().await;
        assert!(matches!(r, Ok(Some(ref m)) if m.message == "msg0"), "{r:?}");
        let r: tonic::Result<Option<EchoResponse>> = stream.message().await;
        assert!(
            matches!(r, Err(ref e) if e.code() == tonic::Code::InvalidArgument),
            "{r:?}"
        );

        let r: tonic::Result<Option<EchoResponse>> = stream.message().await;
        assert!(matches!(r, Ok(None)), "{r:?}");

        Ok(())
    }

    #[tokio::test]
    async fn request_error() -> anyhow::Result<()> {
        let (endpoint, _server) = start_echo_server().await?;
        let client = builder(endpoint)
            .with_credentials(test_credentials())
            .build()
            .await?;

        let response =
            send_server_streaming_request(client.clone(), "msg0", "resource=error").await;
        let err = response.unwrap_err();
        assert!(err.status().is_some(), "{err:?}");

        Ok(())
    }

    #[tokio::test]
    async fn request_error_status() -> anyhow::Result<()> {
        let (endpoint, _server) = start_echo_server().await?;
        let client = builder(endpoint)
            .with_credentials(test_credentials())
            .build()
            .await?;

        // Uses the public server_streaming, so we expect a gax::Error wrapping the status
        let response = send_server_streaming_request_with_options(
            client.clone(),
            "msg0",
            "resource=error",
            RequestOptions::default(),
        )
        .await;

        let err = response.unwrap_err();
        if let Some(status) = err.status() {
            assert_eq!(
                status.code,
                google_cloud_gax::error::rpc::Code::Aborted,
                "{err:?}"
            );
        } else {
            panic!("expected status, got {err:?}");
        }

        Ok(())
    }

    #[tokio::test]
    async fn server_streaming_timeout() -> anyhow::Result<()> {
        let (endpoint, _server) = start_echo_server().await?;
        let client = builder(endpoint)
            .with_credentials(test_credentials())
            .build()
            .await?;

        // Set a very short timeout to ensure it triggers DeadlineExceeded
        let duration = std::time::Duration::from_nanos(1);
        let mut options = RequestOptions::default();
        options.set_attempt_timeout(duration);
        let response = send_server_streaming_request_with_options(
            client.clone(),
            "msg0",
            "resource=test",
            options,
        )
        .await;

        let err = response.unwrap_err();
        // The error might be DeadlineExceeded (Code 4) or Cancelled (Code 1) depending on timing,
        // or a local Timeout error.
        if let Some(status) = err.status() {
            assert!(
                matches!(
                    status.code,
                    google_cloud_gax::error::rpc::Code::DeadlineExceeded
                        | google_cloud_gax::error::rpc::Code::Cancelled
                ),
                "{err:?}"
            );
        } else {
             // If it's not a Status, it should be a local Timeout error
             assert!(err.is_timeout(), "expected timeout error, got {err:?}");
        }

        Ok(())
    }

    #[tokio::test]
    async fn credentials_error() -> anyhow::Result<()> {
        let (endpoint, _server) = start_echo_server().await?;

        let client = builder(endpoint)
            .with_credentials(error_credentials(false))
            .build()
            .await?;
        let response = send_server_streaming_request(client.clone(), "msg0", "").await;
        let err = response.unwrap_err();
        assert!(err.is_authentication(), "{err:?}");
        Ok(())
    }

    #[tokio::test]
    async fn connection_error() -> anyhow::Result<()> {
        let client = builder("http://127.0.0.1:1")
            .with_credentials(test_credentials())
            .build()
            .await;
        let client = client.expect("clients should use lazy connections");
        let response = send_server_streaming_request(client.clone(), "msg0", "").await;
        let err = response.unwrap_err();
        assert!(err.is_connect(), "{err:?}");
        Ok(())
    }

    async fn send_server_streaming_request(
        client: grpc::Client,
        msg: &str,
        request_params: &str,
    ) -> google_cloud_gax::Result<tonic::Response<tonic::codec::Streaming<EchoResponse>>> {
        let extensions = {
            let mut e = tonic::Extensions::new();
            e.insert(tonic::GrpcMethod::new(
                "google.test.v1.EchoServices",
                "Expand",
            ));
            e
        };
        let request_options = {
            let mut o = RequestOptions::default();
            o.set_retry_policy(NeverRetry);
            o
        };
        let request = EchoRequest {
            message: msg.into(),
            ..Default::default()
        };
        client
            .server_streaming::<EchoRequest, EchoResponse>(
                extensions,
                http::uri::PathAndQuery::from_static("/google.test.v1.EchoService/Expand"),
                request,
                request_options,
                "test-only-api-client/1.0",
                request_params,
            )
            .await
    }

    async fn send_server_streaming_request_with_options(
        client: grpc::Client,
        msg: &str,
        request_params: &str,
        request_options: RequestOptions,
    ) -> google_cloud_gax::Result<tonic::Response<tonic::codec::Streaming<EchoResponse>>> {
        let extensions = {
            let mut e = tonic::Extensions::new();
            e.insert(tonic::GrpcMethod::new(
                "google.test.v1.EchoServices",
                "Expand",
            ));
            e
        };
        let request = EchoRequest {
            message: msg.into(),
            ..Default::default()
        };
        client
            .server_streaming::<EchoRequest, EchoResponse>(
                extensions,
                http::uri::PathAndQuery::from_static("/google.test.v1.EchoService/Expand"),
                request,
                request_options,
                "test-only-api-client/1.0",
                request_params,
            )
            .await
    }
}
