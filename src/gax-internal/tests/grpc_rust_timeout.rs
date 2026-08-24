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
    use google_cloud_gax_internal::grpc::GrpcRustClient;
    use google_cloud_gax_internal::options::ClientConfig;
    use grpc_server::{google, start_echo_server};
    use pretty_assertions::assert_eq;
    use std::time::Duration;

    #[tokio::test]
    async fn no_timeout() -> anyhow::Result<()> {
        // Arrange
        let (endpoint, _server) = start_echo_server().await?;
        let client = GrpcRustClient::new(test_config(), &endpoint).await?;

        let delay = Duration::from_millis(50);
        let request_options = RequestOptions::default();

        // Act
        let response = send_request(client, request_options, "great success!", Some(delay)).await?;

        // Assert
        assert_eq!(response.message, "great success!");
        Ok(())
    }

    #[tokio::test]
    async fn timeout_does_not_expire() -> anyhow::Result<()> {
        // Arrange
        let (endpoint, _server) = start_echo_server().await?;
        let client = GrpcRustClient::new(test_config(), &endpoint).await?;

        let delay = Duration::from_millis(50);
        let timeout = Duration::from_millis(2000);
        let mut request_options = RequestOptions::default();
        request_options.set_attempt_timeout(timeout);

        // Act
        let response = send_request(client, request_options, "great success!", Some(delay)).await?;

        // Assert
        assert_eq!(response.message, "great success!");
        Ok(())
    }

    #[tokio::test]
    async fn timeout_expires() -> anyhow::Result<()> {
        // Arrange
        let (endpoint, _server) = start_echo_server().await?;
        let client = GrpcRustClient::new(test_config(), &endpoint).await?;

        let delay = Duration::from_millis(500);
        let timeout = Duration::from_millis(50);
        let mut request_options = RequestOptions::default();
        request_options.set_attempt_timeout(timeout);

        // Act
        let response = send_request(client, request_options, "should timeout", Some(delay)).await;

        // Assert
        let err = response.expect_err("should timeout");
        assert!(err.is_timeout(), "{err:?}");
        Ok(())
    }

    #[tokio::test]
    async fn client_config_timeout() -> anyhow::Result<()> {
        // Arrange
        let (endpoint, _server) = start_echo_server().await?;

        let mut config = test_config();
        config.attempt_timeout = Some(Duration::from_millis(50));

        let client = GrpcRustClient::new(config, &endpoint).await?;

        let delay = Duration::from_millis(500);
        let request_options = RequestOptions::default();

        // Act
        let response = send_request(client, request_options, "should timeout", Some(delay)).await;

        // Assert
        let err = response.expect_err("should timeout");
        assert!(err.is_timeout(), "{err:?}");
        Ok(())
    }

    fn test_credentials() -> Credentials {
        Anonymous::new().build()
    }

    fn test_config() -> ClientConfig {
        let mut config = ClientConfig::default();
        config.cred = Some(test_credentials());
        config
    }

    async fn send_request(
        client: GrpcRustClient,
        options: RequestOptions,
        msg: &str,
        delay: Option<Duration>,
    ) -> google_cloud_gax::Result<google::test::v1::EchoResponse> {
        let delay_ms = delay.map(|d| d.as_millis() as u64);
        let request = google::test::v1::EchoRequest {
            message: msg.into(),
            delay_ms,
        };
        client
            .execute(
                tonic::Extensions::new(),
                http::uri::PathAndQuery::from_static("/google.test.v1.EchoService/Echo"),
                request,
                options,
                "test-only-api-client/1.0",
                "",
            )
            .await
            .map(tonic::Response::into_inner)
    }
}
