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

#[cfg(all(test, feature = "_internal_http_client"))]
mod test {
    use auth::credentials::{Credentials, CredentialsTrait};
    use auth::errors::CredentialsError;
    use auth::token::Token;
    use gax::options::*;
    use gax::retry_policy::{Aip194Strict, RetryPolicyExt};
    use http::header::{HeaderName, HeaderValue};
    use serde_json::json;

    type AuthResult<T> = std::result::Result<T, CredentialsError>;
    type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

    mockall::mock! {
        #[derive(Debug)]
        Credentials {}

        impl CredentialsTrait for Credentials {
            async fn get_token(&self) -> AuthResult<Token>;
            async fn get_headers(&self) -> AuthResult<Vec<(HeaderName, HeaderValue)>>;
            async fn get_universe_domain(&self) -> Option<String>;
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_auth_headers() -> Result<()> {
        let (endpoint, _server) = echo_server::start().await?;

        // We use mock credentials instead of fake credentials, because
        // 1. we can test that multiple headers are included in the request
        // 2. it gives us extra confidence that our interfaces are called
        let mut mock = MockCredentials::new();
        mock.expect_get_headers().return_once(|| {
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

        let client = echo_server::builder(endpoint)
            .with_credentials(Credentials::from(mock))
            .build()
            .await?;

        let builder = client.builder(reqwest::Method::GET, "/echo".into());
        let body = json!({});
        let response: serde_json::Value = client
            .execute(builder, Some(body), RequestOptions::default())
            .await?;
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
    async fn auth_error_retryable() -> Result<()> {
        let (endpoint, _server) = echo_server::start().await?;
        let retry_count = 3;
        let mut mock = MockCredentials::new();
        mock.expect_get_headers()
            .times(retry_count..)
            .returning(|| Err(CredentialsError::from_str(true, "mock retryable error")));

        let retry_policy = Aip194Strict.with_attempt_limit(retry_count as u32);
        let client = echo_server::builder(endpoint)
            .with_credentials(Credentials::from(mock))
            .with_retry_policy(retry_policy)
            .build()
            .await?;

        let builder = client.builder(reqwest::Method::GET, "/echo".into());
        let body = json!({});
        let options = RequestOptions::default();
        let result: std::result::Result<serde_json::Value, gax::error::Error> =
            client.execute(builder, Some(body), options).await;

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
        let (endpoint, _server) = echo_server::start().await?;
        let mut mock = MockCredentials::new();
        mock.expect_get_headers().times(1).returning(|| {
            Err(CredentialsError::from_str(
                false,
                "mock non-retryable error",
            ))
        });

        let client = echo_server::builder(endpoint)
            .with_credentials(Credentials::from(mock))
            .build()
            .await?;

        let builder = client.builder(reqwest::Method::GET, "/echo".into());
        let body = serde_json::json!({});

        let result: std::result::Result<serde_json::Value, gax::error::Error> = client
            .execute(builder, Some(body), RequestOptions::default())
            .await;

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

    fn get_header_value(response: &serde_json::Value, name: &str) -> Option<String> {
        response
            .as_object()
            .map(|o| o.get("headers"))
            .flatten()
            .map(|h| h.get(name))
            .flatten()
            .map(|v| v.as_str())
            .flatten()
            .map(str::to_string)
    }
}
