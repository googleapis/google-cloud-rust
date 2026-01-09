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
    use gax::options::*;
    use gax::retry_policy::{Aip194Strict, RetryPolicyExt};
    use google_cloud_auth::credentials::{
        CacheableResource, Credentials, CredentialsProvider, EntityTag,
    };
    use google_cloud_auth::errors::CredentialsError;
    use google_cloud_gax_internal::grpc;
    use grpc_server::{builder, google, start_echo_server};
    use http::header::{HeaderName, HeaderValue};
    use http::{Extensions, HeaderMap};
    use std::error::Error as _;

    type AuthResult<T> = std::result::Result<T, CredentialsError>;
    type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

    mockall::mock! {
        #[derive(Debug)]
        Credentials {}

        impl CredentialsProvider for Credentials {
            async fn headers(&self, extensions: Extensions) -> AuthResult<CacheableResource<HeaderMap>>;
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
        let headers = HeaderMap::from_iter([
            (
                HeaderName::from_static("auth-key-1"),
                HeaderValue::from_static("auth-value-1"),
            ),
            (
                HeaderName::from_static("auth-key-2"),
                HeaderValue::from_static("auth-value-2"),
            ),
        ]);
        mock.expect_headers().return_once(|_extensions| {
            Ok(CacheableResource::New {
                entity_tag: EntityTag::default(),
                data: headers,
            })
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
            .returning(|_extensions| Err(CredentialsError::from_msg(true, "mock retryable error")));

        let retry_policy = Aip194Strict.with_attempt_limit(retry_count as u32);
        let client = builder(endpoint)
            .with_credentials(Credentials::from(mock))
            .with_retry_policy(retry_policy)
            .with_backoff_policy(test_backoff())
            .build()
            .await?;

        let result = send_request(client, "auth fail").await;
        let e = result
            .as_ref()
            .err()
            .and_then(|e| e.source())
            .and_then(|e| e.downcast_ref::<CredentialsError>());
        assert!(matches!(e, Some(e) if e.is_transient()), "{result:?}");
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[test_case::test_case(Err(CredentialsError::from_msg(
        false,
        "mock non-retryable error",
    )); "on_error_response")]
    async fn auth_error_non_retryable(
        headers_response: AuthResult<CacheableResource<HeaderMap>>,
    ) -> Result<()> {
        let (endpoint, _server) = start_echo_server().await?;

        let mut mock = MockCredentials::new();
        mock.expect_headers()
            .times(1)
            .returning(move |_extensions| headers_response.clone());

        let client = builder(endpoint)
            .with_credentials(Credentials::from(mock))
            .build()
            .await?;

        let result = send_request(client, "auth fail").await;
        let e = result
            .as_ref()
            .err()
            .and_then(|e| e.source())
            .and_then(|e| e.downcast_ref::<CredentialsError>());
        assert!(matches!(e, Some(e) if !e.is_transient()), "{result:?}");

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
