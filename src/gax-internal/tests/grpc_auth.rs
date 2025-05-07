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
mod test {
    use auth::credentials::{Credentials, CredentialsProvider};
    use auth::errors::CredentialsError;
    use auth::token::Token;
    use gax::options::*;
    use gax::retry_policy::{Aip194Strict, RetryPolicyExt};
    use google_cloud_gax_internal::grpc;
    use grpc_server::{builder, google, start_echo_server};
    use http::header::{HeaderName, HeaderValue};

    type AuthResult<T> = std::result::Result<T, CredentialsError>;
    type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

    mockall::mock! {
        #[derive(Debug)]
        Credentials {}

        impl CredentialsProvider for Credentials {
            async fn token(&self) -> AuthResult<Token>;
            async fn headers(&self) -> AuthResult<Vec<(HeaderName, HeaderValue)>>;
            async fn universe_domain(&self) -> Option<String>;
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_auth_headers() -> Result<()> {
        let (endpoint, _server) = start_echo_server().await?;

        // We use mock credentials instead of fake credentials, because
        // 1. we can test that multiple headers are included in the request
        // 2. it gives us extra confidence that our interfaces are called
        let mut mock = MockCredentials::new();
        mock.expect_headers().return_once(|| {
            Ok(vec![
                (
                    HeaderName::from_static("auth-key-1"),
                    HeaderValue::from_static("auth-value-1"),
                ),
                (
                    HeaderName::from_static("auth-key-2"),
                    HeaderValue::from_static("auth-value-2"),
                ),
            ])
        });

        let client = builder(endpoint)
            .with_credentials(Credentials::from(mock))
            .build()
            .await?;

        let response = send_request(client, "great success!").await?;
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn auth_error_retryable() -> Result<()> {
        let (endpoint, _server) = start_echo_server().await?;

        let retry_count = 3;
        let mut mock = MockCredentials::new();
        mock.expect_headers()
            .times(retry_count..)
            .returning(|| Err(CredentialsError::from_str(true, "mock retryable error")));

        let retry_policy = Aip194Strict.with_attempt_limit(retry_count as u32);
        let client = builder(endpoint)
            .with_credentials(Credentials::from(mock))
            .with_retry_policy(retry_policy)
            .with_backoff_policy(test_backoff())
            .build()
            .await?;

        let result = send_request(client, "auth fail").await;
        assert!(result.is_err());

        if let Err(e) = result {
            if let Some(cred_err) = e.as_inner::<CredentialsError>() {
                assert!(
                    cred_err.is_retryable(),
                    "Expected a retryable CredentialsError, but got non-retryable"
                );
            } else {
                panic!("Expected a CredentialsError, but got some other error: {e:?}");
            }
        }

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn auth_error_non_retryable() -> Result<()> {
        let (endpoint, _server) = start_echo_server().await?;

        let mut mock = MockCredentials::new();
        mock.expect_headers().times(1).returning(|| {
            Err(CredentialsError::from_str(
                false,
                "mock non-retryable error",
            ))
        });

        let client = builder(endpoint)
            .with_credentials(Credentials::from(mock))
            .build()
            .await?;

        let result = send_request(client, "auth fail").await;
        assert!(result.is_err());

        if let Err(e) = result {
            if let Some(cred_err) = e.as_inner::<CredentialsError>() {
                assert!(
                    !cred_err.is_retryable(),
                    "Expected a non-retryable CredentialsError, but got retryable"
                );
            } else {
                panic!("Expected a CredentialsError, but got another error type: {e:?}");
            }
        }

        Ok(())
    }

    fn test_backoff() -> impl gax::backoff_policy::BackoffPolicy {
        use std::time::Duration;
        gax::exponential_backoff::ExponentialBackoffBuilder::new()
            .with_initial_delay(Duration::from_micros(1))
            .with_maximum_delay(Duration::from_micros(1))
            .build()
            .expect("a valid backoff policy")
    }

    async fn send_request(
        client: grpc::Client,
        msg: &str,
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
            ..Default::default()
        };
        client
            .execute(
                extensions,
                http::uri::PathAndQuery::from_static("/google.test.v1.EchoService/Echo"),
                request,
                RequestOptions::default(),
                "test-only-api-client/1.0",
                "name=test-only",
            )
            .await
            .map(tonic::Response::into_inner)
    }
}
