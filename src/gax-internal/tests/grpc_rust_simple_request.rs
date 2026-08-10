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
    use google_cloud_auth::credentials::{Credentials, anonymous::Builder as Anonymous};
    use google_cloud_gax::options::RequestOptions;
    use google_cloud_gax::retry_policy::NeverRetry;
    use google_cloud_gax_internal::grpc::GrpcRustClient;
    use google_cloud_gax_internal::options::ClientConfig;
    use grpc_server::{google, start_echo_server};
    use pretty_assertions::assert_eq;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn default_endpoint() -> anyhow::Result<()> {
        // Arrange
        let (endpoint, _server) = start_echo_server().await?;
        let client =
            GrpcRustClient::new(test_config(endpoint), "https://storage.googleapis.com").await?;

        // Act & Assert
        check_simple_request(client).await
    }

    #[tokio::test]
    async fn no_request_params() -> anyhow::Result<()> {
        // Arrange
        let (endpoint, _server) = start_echo_server().await?;
        let client =
            GrpcRustClient::new(test_config(endpoint), "https://test-only.googleapis.com").await?;

        // Act
        let response = send_request(client, "test message", "").await?;

        // Assert
        assert_eq!(
            response
                .metadata
                .get("x-goog-request-params")
                .map(String::as_str),
            None
        );
        Ok(())
    }

    #[tokio::test]
    async fn override_endpoint() -> anyhow::Result<()> {
        // Arrange
        let (endpoint, _server) = start_echo_server().await?;
        let client =
            GrpcRustClient::new(test_config(endpoint), "https://invalid.example.com").await?;

        // Act & Assert
        check_simple_request(client).await
    }

    fn test_credentials() -> Credentials {
        Anonymous::new().build()
    }

    fn test_config(endpoint: impl Into<String>) -> ClientConfig {
        let mut config = ClientConfig::default();
        config.cred = Some(test_credentials());
        config.endpoint = Some(endpoint.into());
        config
    }

    async fn send_request(
        client: GrpcRustClient,
        msg: &str,
        request_params: &str,
    ) -> google_cloud_gax::Result<google::test::v1::EchoResponse> {
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
                tonic::Extensions::new(),
                http::uri::PathAndQuery::from_static("/google.test.v1.EchoService/Echo"),
                request,
                request_options,
                "test-only-api-client/1.0",
                request_params,
            )
            .await
            .map(tonic::Response::into_inner)
    }

    async fn check_simple_request(client: GrpcRustClient) -> anyhow::Result<()> {
        // Act
        let response = send_request(client, "test message", "name=test-only").await?;

        // Assert
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
        Ok(())
    }
}
