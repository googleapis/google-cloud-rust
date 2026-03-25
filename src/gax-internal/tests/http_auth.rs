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

#[cfg(all(test, feature = "_internal-http-client"))]
mod tests {
    use super::mock_credentials::{MockCredentials, mock_credentials};
    use google_cloud_auth::credentials::Credentials;
    use google_cloud_auth::errors::CredentialsError;
    use google_cloud_gax::backoff_policy::BackoffPolicy;
    use google_cloud_gax::exponential_backoff::ExponentialBackoffBuilder;
    use google_cloud_gax::options::RequestOptions;
    use google_cloud_gax::retry_policy::{Aip194Strict, RetryPolicyExt};
    use serde_json::json;
    use std::error::Error as _;

    #[tokio::test]
    async fn auth_headers_simple() -> anyhow::Result<()> {
        let (endpoint, _server) = echo_server::start().await?;
        let mock = mock_credentials();

        let client = echo_server::builder(endpoint)
            .with_credentials(mock)
            .build()
            .await?;

        let builder = client.builder(reqwest::Method::GET, "/echo".into());
        let body = json!({});
        let response: serde_json::Value = client
            .execute(builder, Some(body), RequestOptions::default())
            .await?
            .into_body();
        assert_eq!(
            get_header_value(&response, "auth-key-1"),
            Some("auth-value-1".to_string())
        );
        assert_eq!(
            get_header_value(&response, "auth-key-2"),
            Some("auth-value-2".to_string())
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn auth_error_retryable() -> anyhow::Result<()> {
        let (endpoint, _server) = echo_server::start().await?;
        let retry_count = 3;
        let mut mock = MockCredentials::new();
        mock.expect_headers()
            .times(retry_count..)
            .returning(|_extensions| Err(CredentialsError::from_msg(true, "mock retryable error")));

        let retry_policy = Aip194Strict.with_attempt_limit(retry_count as u32);
        let client = echo_server::builder(endpoint)
            .with_credentials(Credentials::from(mock))
            .with_backoff_policy(test_backoff())
            .with_retry_policy(retry_policy)
            .build()
            .await?;

        let builder = client.builder(reqwest::Method::GET, "/echo".into());
        let body = json!({});
        let options = RequestOptions::default();
        let result = client
            .execute::<serde_json::Value, serde_json::Value>(builder, Some(body), options)
            .await;
        let e = result
            .as_ref()
            .err()
            .and_then(|e| e.source())
            .and_then(|e| e.downcast_ref::<CredentialsError>());
        assert!(matches!(e, Some(e) if e.is_transient()), "{result:?}");

        Ok(())
    }

    #[tokio::test]
    async fn auth_error_non_retryable() -> anyhow::Result<()> {
        let headers_response = Err(CredentialsError::from_msg(
            false,
            "mock non-retryable error",
        ));
        let (endpoint, _server) = echo_server::start().await?;
        let mut mock = MockCredentials::new();
        mock.expect_headers()
            .times(1)
            .returning(move |_extensions| headers_response.clone());

        let client = echo_server::builder(endpoint)
            .with_credentials(Credentials::from(mock))
            .build()
            .await?;

        let builder = client.builder(reqwest::Method::GET, "/echo".into());
        let body = serde_json::json!({});

        let result = client
            .execute::<serde_json::Value, serde_json::Value>(
                builder,
                Some(body),
                RequestOptions::default(),
            )
            .await;

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

    fn get_header_value(response: &serde_json::Value, name: &str) -> Option<String> {
        response
            .as_object()
            .and_then(|o| o.get("headers"))
            .and_then(|h| h.get(name))
            .and_then(|v| v.as_str())
            .map(str::to_string)
    }
}
