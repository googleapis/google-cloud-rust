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
    use google_cloud_gax::backoff_policy::BackoffPolicy;
    use google_cloud_gax::error::rpc::Code;
    use google_cloud_gax::exponential_backoff::ExponentialBackoffBuilder;
    use google_cloud_gax::options::RequestOptions;
    use google_cloud_gax::retry_policy::{Aip194Strict, RetryPolicyExt};
    use google_cloud_gax_internal::grpc::GrpcRustClient;
    use google_cloud_gax_internal::options::ClientConfig;
    use grpc_server::google::test::v1::EchoResponse;
    use grpc_server::{google, start_fixed_responses};
    use std::sync::Arc;

    #[tokio::test]
    async fn no_retry_immediate_success() -> anyhow::Result<()> {
        // Arrange
        let (endpoint, _server) = start_fixed_responses(vec![success()]).await?;
        let client = GrpcRustClient::new(test_config(), &endpoint).await?;

        // Act
        let _response = send_request(client, "no_retry_immediate_success").await?;

        // Assert
        Ok(())
    }

    #[tokio::test]
    async fn no_retry_immediate_error() -> anyhow::Result<()> {
        // Arrange
        let (endpoint, _server) = start_fixed_responses(vec![transient()]).await?;
        let client = GrpcRustClient::new(test_config(), &endpoint).await?;

        // Act
        let response = send_request(client, "no_retry_immediate_error").await;

        // Assert
        assert!(response.is_err(), "{response:?}");
        Ok(())
    }

    #[tokio::test]
    async fn retry_then_success() -> anyhow::Result<()> {
        // Arrange
        let (endpoint, _server) =
            start_fixed_responses(vec![transient(), transient(), success()]).await?;

        let mut config = test_config();
        config.backoff_policy = Some(Arc::new(test_backoff()));

        let client = GrpcRustClient::new(config, &endpoint).await?;

        // Act
        let _response = send_request(client, "retry_then_success").await?;

        // Assert
        Ok(())
    }

    #[tokio::test]
    async fn retry_then_error() -> anyhow::Result<()> {
        // Arrange
        let (endpoint, _server) =
            start_fixed_responses(vec![transient(), transient(), permanent()]).await?;

        let mut config = test_config();
        config.backoff_policy = Some(Arc::new(test_backoff()));

        let client = GrpcRustClient::new(config, &endpoint).await?;

        // Act
        let response = send_request(client, "retry_then_error").await;

        // Assert
        assert!(response.is_err(), "{response:?}");
        Ok(())
    }

    #[tokio::test]
    async fn retry_policy_exhausted() -> anyhow::Result<()> {
        // Arrange
        let (endpoint, _server) = start_fixed_responses((0..3).map(|_| transient())).await?;

        let mut config = test_config();
        config.retry_policy = Some(Arc::new(Aip194Strict.with_attempt_limit(3)));
        config.backoff_policy = Some(Arc::new(test_backoff()));

        let client = GrpcRustClient::new(config, &endpoint).await?;

        // Act
        let response = send_request(client, "retry_policy_exhausted").await;

        // Assert
        let err = response.expect_err("should fail");
        let status = err.status().expect("expected status");
        assert_eq!(status.code, Code::Unavailable);
        assert_eq!(status.message, "try-again");
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

    fn test_backoff() -> impl BackoffPolicy {
        use std::time::Duration;
        ExponentialBackoffBuilder::new()
            .with_initial_delay(Duration::from_micros(1))
            .with_maximum_delay(Duration::from_micros(1))
            .build()
            .expect("a valid backoff policy")
    }

    /// Returns a successful [`EchoResponse`].
    fn success() -> tonic::Result<tonic::Response<EchoResponse>> {
        Ok(tonic::Response::new(EchoResponse {
            message: "success!".into(),
            metadata: std::collections::HashMap::default(),
            ..EchoResponse::default()
        }))
    }

    /// Returns a transient (`Unavailable`) gRPC error.
    fn transient() -> tonic::Result<tonic::Response<EchoResponse>> {
        Err(tonic::Status::unavailable("try-again"))
    }

    /// Returns a permanent (`PermissionDenied`) gRPC error.
    fn permanent() -> tonic::Result<tonic::Response<EchoResponse>> {
        Err(tonic::Status::permission_denied("uh-oh"))
    }

    pub async fn send_request(
        client: GrpcRustClient,
        msg: &str,
    ) -> google_cloud_gax::Result<EchoResponse> {
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
            o.set_idempotency(true);
            o
        };
        client
            .execute(
                extensions,
                http::uri::PathAndQuery::from_static("/google.test.v1.EchoService/Echo"),
                request,
                request_options,
                "test-only-api-client/1.0",
                "name=test-only",
            )
            .await
            .map(tonic::Response::into_inner)
    }
}
