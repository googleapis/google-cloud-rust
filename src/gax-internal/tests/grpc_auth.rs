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

mod mock_credentials;

#[cfg(all(test, feature = "_internal-grpc-client"))]
mod tests {
    use super::mock_credentials::{MockCredentials, mock_credentials};
    use google_cloud_auth::credentials::Credentials;
    use google_cloud_auth::errors::CredentialsError;
    use google_cloud_gax::backoff_policy::BackoffPolicy;
    use google_cloud_gax::exponential_backoff::ExponentialBackoffBuilder;
    use google_cloud_gax::options::*;
    use google_cloud_gax::retry_policy::{Aip194Strict, RetryPolicyExt};
    use grpc_server::transport::{EchoTransport, transport_builder};
    use grpc_server::{both_transports, google, start_echo_server};
    use pretty_assertions::assert_eq;
    use std::error::Error as _;

    both_transports!(
        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        test_auth_headers
    );
    async fn test_auth_headers<C: EchoTransport>() -> anyhow::Result<()> {
        // Arrange
        let (endpoint, _server) = start_echo_server().await?;
        let client = transport_builder::<C>(endpoint)
            .with_credentials(mock_credentials())
            .build()
            .await?;

        // Act
        let response = send_request(client, "great success!").await?;

        // Assert
        assert_eq!(
            response.metadata.get("auth-key-1").map(String::as_str),
            Some("auth-value-1")
        );
        assert_eq!(
            response.metadata.get("auth-key-2").map(String::as_str),
            Some("auth-value-2")
        );
        Ok(())
    }

    both_transports!(
        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        auth_error_retryable
    );
    async fn auth_error_retryable<C: EchoTransport>() -> anyhow::Result<()> {
        // Arrange
        let (endpoint, _server) = start_echo_server().await?;

        let retry_count = 3;
        let mut mock = MockCredentials::new();
        mock.expect_headers()
            .times(retry_count)
            .returning(|_extensions| Err(CredentialsError::from_msg(true, "mock retryable error")));
        mock.expect_universe_domain().returning(|| None);

        let retry_policy = Aip194Strict.with_attempt_limit(retry_count as u32);
        let client = transport_builder::<C>(endpoint)
            .with_credentials(Credentials::from(mock))
            .with_retry_policy(retry_policy)
            .with_backoff_policy(test_backoff())
            .build()
            .await?;

        // Act
        let result = send_request(client, "auth fail").await;

        // Assert
        let e = result
            .as_ref()
            .err()
            .and_then(|e| e.source())
            .and_then(|e| e.downcast_ref::<CredentialsError>());
        assert!(matches!(e, Some(e) if e.is_transient()), "{result:?}");
        Ok(())
    }

    both_transports!(auth_error_non_retryable);
    async fn auth_error_non_retryable<C: EchoTransport>() -> anyhow::Result<()> {
        // Arrange
        let headers_response = Err(CredentialsError::from_msg(
            false,
            "mock non-retryable error",
        ));
        let (endpoint, _server) = start_echo_server().await?;

        let mut mock = MockCredentials::new();
        mock.expect_headers()
            .times(1)
            .returning(move |_extensions| headers_response.clone());
        mock.expect_universe_domain().returning(|| None);

        let client = transport_builder::<C>(endpoint)
            .with_credentials(Credentials::from(mock))
            .build()
            .await?;

        // Act
        let result = send_request(client, "auth fail").await;

        // Assert
        let e = result
            .as_ref()
            .err()
            .and_then(|e| e.source())
            .and_then(|e| e.downcast_ref::<CredentialsError>());
        assert!(matches!(e, Some(e) if !e.is_transient()), "{result:?}");

        Ok(())
    }

    fn test_backoff() -> impl BackoffPolicy {
        use std::time::Duration;
        ExponentialBackoffBuilder::new()
            .with_initial_delay(Duration::from_micros(1))
            .with_maximum_delay(Duration::from_micros(1))
            .build()
            .expect("a valid backoff policy")
    }

    async fn send_request<C: EchoTransport>(
        client: C,
        msg: &str,
    ) -> google_cloud_gax::Result<google::test::v1::EchoResponse> {
        let request = google::test::v1::EchoRequest {
            message: msg.into(),
            ..Default::default()
        };
        client
            .echo(request, RequestOptions::default(), "name=test-only")
            .await
            .map(tonic::Response::into_inner)
    }
}
