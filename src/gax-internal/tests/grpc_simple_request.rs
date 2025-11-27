// Copyright 2024 Google LLC
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
    use gax::options::*;
    use gax::retry_policy::NeverRetry;
    use google_cloud_gax_internal::grpc;
    use grpc_server::{builder, google, start_echo_server, start_echo_server_with_address};

    fn test_credentials() -> auth::credentials::Credentials {
        auth::credentials::anonymous::Builder::new().build()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn default_endpoint() -> anyhow::Result<()> {
        let (endpoint, _server) = start_echo_server().await?;

        let client = builder(endpoint)
            .with_credentials(test_credentials())
            .build()
            .await?;
        check_simple_request(client).await
    }

    #[tokio::test]
    async fn non_default_endpoint_ipv6() -> anyhow::Result<()> {
        let (endpoint, _server) = start_echo_server_with_address("[::]:0").await?;

        let client = builder("https://storage.googleapis.com")
            .with_endpoint(endpoint)
            .with_credentials(test_credentials())
            .build()
            .await?;
        check_simple_request(client).await
    }

    #[tokio::test]
    async fn non_default_endpoint_ipv4() -> anyhow::Result<()> {
        let (endpoint, _server) = start_echo_server_with_address("0.0.0.0:0").await?;

        let client = builder("https://storage.googleapis.com")
            .with_endpoint(endpoint)
            .with_credentials(test_credentials())
            .build()
            .await?;
        check_simple_request(client).await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn no_request_params() -> anyhow::Result<()> {
        let (endpoint, _server) = start_echo_server().await?;

        let client = builder(endpoint)
            .with_credentials(test_credentials())
            .build()
            .await?;
        let response = send_request(client, "test message", "").await?;
        assert_eq!(&response.message, "test message");
        assert_eq!(
            response
                .metadata
                .get("x-goog-api-client")
                .map(String::as_str),
            Some("test-only-api-client/1.0")
        );
        assert_eq!(response.metadata.get("x-goog-request-params"), None);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn override_endpoint() -> anyhow::Result<()> {
        let (endpoint, _server) = start_echo_server().await?;

        let client = builder("https://test-only.googleapis.com")
            .with_endpoint(endpoint)
            .with_credentials(test_credentials())
            .build()
            .await?;

        check_simple_request(client).await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn credentials_error() -> anyhow::Result<()> {
        let (endpoint, _server) = start_echo_server().await?;

        let client = builder(endpoint)
            .with_credentials(auth::credentials::testing::error_credentials(false))
            .build()
            .await?;
        let response = send_request(client, "credentials error", "").await;
        let err = response.unwrap_err();
        assert!(err.is_authentication(), "{err:?}");
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn connection_error() -> anyhow::Result<()> {
        let client = builder("http://127.0.0.1:1")
            .with_credentials(test_credentials())
            .build()
            .await;
        let client = client.expect("clients should use lazy connections");
        let response = send_request(client, "connection error", "").await;
        let err = response.unwrap_err();
        assert!(err.is_connect(), "{err:?}");
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn endpoint_error() -> anyhow::Result<()> {
        let client = builder("http:/invalid-invalid")
            .with_credentials(test_credentials())
            .build()
            .await;
        let err = client.unwrap_err();
        assert!(err.is_transport(), "{err:?}");
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn uds_and_tls() -> anyhow::Result<()> {
        let client = builder("uds://invalid")
            .with_credentials(test_credentials())
            .build()
            .await;
        let client = client.expect("clients should use lazy connections");
        let response = send_request(client, "connection error", "").await;
        let err = response.unwrap_err();
        assert!(err.is_connect(), "{err:?}");
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn request_error() -> anyhow::Result<()> {
        let (endpoint, _server) = start_echo_server().await?;

        let client = builder(endpoint)
            .with_credentials(test_credentials())
            .build()
            .await?;

        let response = send_request(client, "", "").await;
        let err = response.unwrap_err();
        assert_eq!(
            err.status().map(|s| s.code),
            Some(gax::error::rpc::Code::InvalidArgument)
        );
        Ok(())
    }

    async fn send_request(
        client: grpc::Client,
        msg: &str,
        request_params: &str,
    ) -> gax::Result<google::test::v1::EchoResponse> {
        let extensions = {
            let mut e = tonic::Extensions::new();
            e.insert(tonic::GrpcMethod::new(
                "google.test.v1.EchoServices",
                "Echo",
            ));
            e
        };
        let request = google::test::v1::EchoRequest {
            message: msg.into(),
            ..google::test::v1::EchoRequest::default()
        };
        let request_options = {
            let mut o = RequestOptions::default();
            o.set_retry_policy(NeverRetry);
            o
        };
        client
            .execute(
                extensions,
                http::uri::PathAndQuery::from_static("/google.test.v1.EchoService/Echo"),
                request,
                request_options,
                "test-only-api-client/1.0",
                request_params,
            )
            .await
            .map(tonic::Response::into_inner)
    }

    async fn check_simple_request(client: grpc::Client) -> anyhow::Result<()> {
        let response = send_request(client, "test message", "name=test-only").await?;
        assert_eq!(&response.message, "test message");
        assert_eq!(
            response
                .metadata
                .get("x-goog-api-client")
                .map(String::as_str),
            Some("test-only-api-client/1.0")
        );
        assert_eq!(
            response
                .metadata
                .get("x-goog-request-params")
                .map(String::as_str),
            Some("name=test-only")
        );
        let got_user_agent = response.metadata.get("user-agent").unwrap();
        assert!(got_user_agent.contains("tonic/"), "{got_user_agent:?}");
        Ok(())
    }
}
