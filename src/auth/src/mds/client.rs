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

use crate::errors::CredentialsError;
use crate::token::Token;
use google_cloud_gax::backoff_policy::BackoffPolicyArg;
use google_cloud_gax::error::Error as GaxError;
use google_cloud_gax::exponential_backoff::ExponentialBackoff;
use google_cloud_gax::retry_loop_internal::retry_loop;
use google_cloud_gax::retry_policy::{NeverRetry, RetryPolicyArg};
use google_cloud_gax::retry_throttler::{
    AdaptiveThrottler, RetryThrottlerArg, SharedRetryThrottler,
};
use reqwest::{Client as ReqwestClient, RequestBuilder};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::time::Instant;

/// A client for GCP Compute Engine Metadata Service (MDS).
#[derive(Clone, Debug)]
pub(crate) struct Client {
    endpoint: String,
    /// True if the endpoint was NOT overridden by env var or constructor arg.
    pub(crate) is_default_endpoint: bool,
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub(crate) struct MDSTokenResponse {
    pub(crate) access_token: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) expires_in: Option<u64>,
    pub(crate) token_type: String,
}

impl Client {
    /// Creates a new client for the Metadata Service.
    pub(crate) fn new(endpoint_override: Option<String>) -> Self {
        let (endpoint, is_default_endpoint) = Self::resolve_endpoint(endpoint_override);
        let endpoint = endpoint.trim_end_matches('/').to_string();

        Self {
            endpoint,
            is_default_endpoint,
        }
    }

    /// Determine the endpoint and whether it was overridden
    fn resolve_endpoint(endpoint_override: Option<String>) -> (String, bool) {
        if let Ok(host) = std::env::var(super::GCE_METADATA_HOST_ENV_VAR) {
            // Check GCE_METADATA_HOST environment variable first
            (format!("http://{host}"), false)
        } else if let Some(e) = endpoint_override {
            // Else, check if an endpoint was provided to the mds::Builder
            (e, false)
        } else {
            // Else, use the default metadata root
            (super::METADATA_ROOT.to_string(), true)
        }
    }

    /// Creates a GET request to the MDS service with the correct headers.
    fn get(&self, path: &str) -> RequestBuilder {
        let url = format!("{}{}", self.endpoint, path);
        ReqwestClient::new()
            .get(url)
            .header(super::METADATA_FLAVOR, super::METADATA_FLAVOR_VALUE)
    }

    /// Fetches an access token for the default service account.
    pub(crate) fn access_token(&self, scopes: Option<Vec<String>>) -> AccessTokenRequest {
        AccessTokenRequest {
            client: self.clone(),
            scopes,
        }
    }

    /// Fetches an ID token for the default service account.
    /// Used by idtoken feature.
    #[cfg(feature = "idtoken")]
    pub(crate) fn id_token(
        &self,
        target_audience: &str,
        format: Option<String>,
        licenses: Option<String>,
    ) -> IdTokenRequest {
        IdTokenRequest {
            client: self.clone(),
            target_audience: target_audience.to_string(),
            format,
            licenses,
        }
    }

    /// Fetches the email address of the service account from the Metadata Service.
    pub(crate) fn email(&self) -> EmailRequest {
        EmailRequest {
            client: self.clone(),
        }
    }

    /// Fetches the universe domain from the Metadata Service.
    #[allow(dead_code)]
    pub(crate) fn universe_domain(&self) -> UniverseDomainRequest {
        UniverseDomainRequest {
            client: self.clone(),
            retry_config: RetryConfig::default(),
        }
    }

    async fn send(
        &self,
        request: reqwest::RequestBuilder,
        error_message: &'static str,
        retry_config: RetryConfig,
    ) -> crate::Result<reqwest::Response> {
        let sleep = async |d| tokio::time::sleep(d).await;

        let error_message_str = error_message.to_string().clone();
        retry_loop(
            async move |_| {
                let req = request
                    .try_clone()
                    .expect("client libraries only create builders where `try_clone()` succeeds");
                let response = req.send().await.map_err(GaxError::io)?;

                let response =
                    Self::check_response_status(response, error_message_str.clone()).await?;

                Ok(response)
            },
            sleep,
            true, // GET requests are idempotent
            retry_config.retry_throttler,
            retry_config.retry_policy.into(),
            retry_config.backoff_policy.into(),
        )
        .await
        .map_err(|e| crate::errors::from_gax_error(e, error_message))
    }

    async fn check_response_status(
        response: reqwest::Response,
        error_message: String,
    ) -> Result<reqwest::Response, GaxError> {
        let status = response.status();
        if !status.is_success() {
            let err_headers = response.headers().clone();
            let err_payload = response
                .bytes()
                .await
                .map_err(|e| GaxError::transport(err_headers.clone(), e))?;
            return Err(GaxError::http(
                status.as_u16(),
                err_headers,
                format!("{error_message} :{err_payload:?}").into(),
            ));
        }
        Ok(response)
    }
}
#[derive(Clone)]
struct RetryConfig {
    retry_policy: RetryPolicyArg,
    backoff_policy: BackoffPolicyArg,
    retry_throttler: SharedRetryThrottler,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            retry_policy: NeverRetry.into(),
            backoff_policy: ExponentialBackoff::default().into(),
            retry_throttler: Arc::new(Mutex::new(AdaptiveThrottler::default())),
        }
    }
}

impl RetryConfig {
    fn with_retry_policy(mut self, retry_policy: RetryPolicyArg) -> Self {
        self.retry_policy = retry_policy;
        self
    }

    fn with_backoff_policy(mut self, backoff_policy: BackoffPolicyArg) -> Self {
        self.backoff_policy = backoff_policy;
        self
    }

    fn with_retry_throttler(mut self, retry_throttler: RetryThrottlerArg) -> Self {
        self.retry_throttler = retry_throttler.into();
        self
    }
}

#[derive(Clone)]
pub(crate) struct AccessTokenRequest {
    client: Client,
    scopes: Option<Vec<String>>,
}

impl AccessTokenRequest {
    pub(crate) async fn send(self) -> crate::Result<Token> {
        let path = format!("{}/token", super::MDS_DEFAULT_URI);
        let request = self.client.get(&path);

        // Use the `scopes` option if set, otherwise let the MDS use the default
        // scopes.
        let scopes = self.scopes.as_ref().map(|v| v.join(","));
        let request = scopes
            .into_iter()
            .fold(request, |r, s| r.query(&[("scopes", s)]));

        let error_message = "failed to fetch access token";

        // If the connection to MDS was not successful, it is useful to retry when really
        // running on MDS environments and not useful if there is no MDS. We will mark the error
        // as retryable and let the retry policy determine whether to retry or not. Whenever we
        // define a default retry policy, we can skip retrying this case.
        let response = self
            .client
            .send(request, error_message, RetryConfig::default())
            .await?;

        let response = response.json::<MDSTokenResponse>().await.map_err(|e| {
            // Decoding errors are not transient. Typically they indicate a badly
            // configured MDS endpoint, or DNS redirecting the request to a random
            // server, e.g., ISPs that redirect unknown services to HTTP.
            CredentialsError::from_source(!e.is_decode(), e)
        })?;

        Ok(Token {
            token: response.access_token,
            token_type: response.token_type,
            expires_at: response
                .expires_in
                .map(|d| Instant::now() + Duration::from_secs(d)),
            metadata: None,
        })
    }
}

#[cfg(feature = "idtoken")]
#[derive(Clone)]
pub(crate) struct IdTokenRequest {
    client: Client,
    target_audience: String,
    format: Option<String>,
    licenses: Option<String>,
}

#[cfg(feature = "idtoken")]
impl IdTokenRequest {
    pub(crate) async fn send(self) -> crate::Result<String> {
        let path = format!("{}/identity", super::MDS_DEFAULT_URI);
        let request = self
            .client
            .get(&path)
            .query(&[("audience", &self.target_audience)]);
        let request = self.format.iter().fold(request, |builder, format| {
            builder.query(&[("format", format)])
        });
        let request = self.licenses.iter().fold(request, |builder, licenses| {
            builder.query(&[("licenses", licenses)])
        });

        let error_message = "failed to fetch id token";
        let response = self
            .client
            .send(request, error_message, RetryConfig::default())
            .await?;

        let token = response
            .text()
            .await
            .map_err(|e| CredentialsError::from_source(!e.is_decode(), e))?;

        Ok(token)
    }
}

#[derive(Clone)]
pub(crate) struct EmailRequest {
    client: Client,
}

impl EmailRequest {
    pub(crate) async fn send(self) -> crate::Result<String> {
        let path = format!("{}/email", super::MDS_DEFAULT_URI);
        let request = self.client.get(&path);
        let error_message = "failed to fetch email";

        let response = self
            .client
            .send(request, error_message, RetryConfig::default())
            .await?;

        let email = response
            .text()
            .await
            .map_err(|e| CredentialsError::from_source(!e.is_decode(), e))?;

        Ok(email)
    }
}

#[derive(Clone)]
#[allow(dead_code)]
pub(crate) struct UniverseDomainRequest {
    client: Client,
    retry_config: RetryConfig,
}

impl UniverseDomainRequest {
    #[allow(dead_code)]
    pub(crate) fn with_retry_policy(mut self, retry_policy: RetryPolicyArg) -> Self {
        self.retry_config = self.retry_config.with_retry_policy(retry_policy);
        self
    }

    #[allow(dead_code)]
    pub(crate) fn with_backoff_policy(mut self, backoff_policy: BackoffPolicyArg) -> Self {
        self.retry_config = self.retry_config.with_backoff_policy(backoff_policy);
        self
    }

    #[allow(dead_code)]
    pub(crate) fn with_retry_throttler(mut self, retry_throttler: RetryThrottlerArg) -> Self {
        self.retry_config = self.retry_config.with_retry_throttler(retry_throttler);
        self
    }

    #[allow(dead_code)]
    pub(crate) async fn send(self) -> crate::Result<String> {
        let path = super::MDS_UNIVERSE_DOMAIN_URI;
        let request = self.client.get(path);
        let error_message = "failed to fetch universe domain";

        let response = self
            .client
            .send(request, error_message, self.retry_config)
            .await?;

        let universe_domain = response
            .text()
            .await
            .map_err(|e| CredentialsError::from_source(!e.is_decode(), e))?;

        Ok(universe_domain.trim().to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::credentials::tests::{
        get_mock_auth_retry_policy, get_mock_backoff_policy, get_mock_retry_throttler,
    };
    use crate::mds::{MDS_DEFAULT_URI, MDS_UNIVERSE_DOMAIN_URI};
    use httptest::{Expectation, Server, matchers::*, responders::*};
    use scoped_env::ScopedEnv;
    use serial_test::{parallel, serial};

    type TestResult = anyhow::Result<()>;

    #[tokio::test]
    #[parallel]
    async fn test_access_token_success() -> TestResult {
        let server = Server::run();
        let client = Client::new(Some(format!("http://{}", server.addr())));
        let response = MDSTokenResponse {
            access_token: "test-token".to_string(),
            expires_in: Some(3600),
            token_type: "Bearer".to_string(),
        };

        server.expect(
            Expectation::matching(all_of![
                request::method("GET"),
                request::path(format!("{}/token", MDS_DEFAULT_URI)),
                request::query(url_decoded(contains((
                    "scopes",
                    "scope1,scope2".to_string()
                )))),
            ])
            .respond_with(
                status_code(200)
                    .insert_header("Content-Type", "application/json")
                    .body(serde_json::to_string(&response)?),
            ),
        );

        let token = client
            .access_token(Some(vec!["scope1".to_string(), "scope2".to_string()]))
            .send()
            .await?;
        assert_eq!(token.token, "test-token");
        assert_eq!(token.token_type, "Bearer");

        Ok(())
    }

    #[tokio::test]
    #[parallel]
    async fn test_access_token_failure() {
        let server = Server::run();
        let client = Client::new(Some(format!("http://{}", server.addr())));

        server.expect(
            Expectation::matching(all_of![
                request::method("GET"),
                request::path(format!("{}/token", MDS_DEFAULT_URI)),
            ])
            .respond_with(status_code(404).body("Not Found")),
        );

        let err = client.access_token(None).send().await.unwrap_err();
        assert!(err.to_string().contains("failed to fetch access token"));
    }

    #[tokio::test]
    #[parallel]
    #[cfg(feature = "idtoken")]
    async fn test_id_token_success() -> TestResult {
        let server = Server::run();
        let client = Client::new(Some(format!("http://{}", server.addr())));

        server.expect(
            Expectation::matching(all_of![
                request::method("GET"),
                request::path(format!("{}/identity", MDS_DEFAULT_URI)),
                request::query(url_decoded(contains(("audience", "test-aud".to_string())))),
                request::query(url_decoded(contains(("format", "full".to_string())))),
                request::query(url_decoded(contains(("licenses", "TRUE".to_string())))),
            ])
            .respond_with(status_code(200).body("test-id-token")),
        );

        let token = client
            .id_token(
                "test-aud",
                Some("full".to_string()),
                Some("TRUE".to_string()),
            )
            .send()
            .await?;
        assert_eq!(token, "test-id-token");

        Ok(())
    }

    #[tokio::test]
    #[parallel]
    #[cfg(feature = "idtoken")]
    async fn test_id_token_failure() {
        let server = Server::run();
        let client = Client::new(Some(format!("http://{}", server.addr())));

        server.expect(
            Expectation::matching(all_of![
                request::method("GET"),
                request::path(format!("{}/identity", MDS_DEFAULT_URI)),
            ])
            .respond_with(status_code(404).body("Not Found")),
        );

        let err = client
            .id_token("test-aud", None, None)
            .send()
            .await
            .unwrap_err();
        assert!(err.to_string().contains("failed to fetch id token"));
    }

    #[tokio::test]
    #[parallel]
    async fn test_email_success() -> TestResult {
        let server = Server::run();
        let client = Client::new(Some(format!("http://{}", server.addr())));

        server.expect(
            Expectation::matching(all_of![
                request::method("GET"),
                request::path(format!("{}/email", MDS_DEFAULT_URI)),
            ])
            .respond_with(status_code(200).body("test@example.com")),
        );

        let email = client.email().send().await?;
        assert_eq!(email, "test@example.com");

        Ok(())
    }

    #[tokio::test]
    #[parallel]
    async fn test_email_failure() {
        let server = Server::run();
        let client = Client::new(Some(format!("http://{}", server.addr())));

        server.expect(
            Expectation::matching(all_of![
                request::method("GET"),
                request::path(format!("{}/email", MDS_DEFAULT_URI)),
            ])
            .respond_with(status_code(404).body("Not Found")),
        );

        let err = client.email().send().await.unwrap_err();
        assert!(err.to_string().contains("failed to fetch email"));
    }

    #[tokio::test]
    #[parallel]
    async fn test_universe_domain_success() -> TestResult {
        let server = Server::run();
        let client = Client::new(Some(format!("http://{}", server.addr())));

        server.expect(
            Expectation::matching(all_of![
                request::method("GET"),
                request::path(MDS_UNIVERSE_DOMAIN_URI),
            ])
            .respond_with(status_code(200).body("my-universe-domain.com")),
        );

        let domain = client.universe_domain().send().await?;
        assert_eq!(domain, "my-universe-domain.com");

        Ok(())
    }

    #[tokio::test]
    #[parallel]
    async fn test_universe_domain_failure() {
        let server = Server::run();
        let client = Client::new(Some(format!("http://{}", server.addr())));

        server.expect(
            Expectation::matching(all_of![
                request::method("GET"),
                request::path(MDS_UNIVERSE_DOMAIN_URI),
            ])
            .respond_with(status_code(404).body("Not Found")),
        );

        let err = client.universe_domain().send().await.unwrap_err();
        assert!(err.to_string().contains("failed to fetch universe domain"));
    }

    #[test]
    #[parallel]
    fn test_resolve_endpoint_default() {
        let client = Client::new(None);
        assert_eq!(client.endpoint, "http://metadata.google.internal");
    }

    #[test]
    #[parallel]
    fn test_resolve_endpoint_override() {
        let client = Client::new(Some("http://custom.endpoint".to_string()));
        assert_eq!(client.endpoint, "http://custom.endpoint");
    }

    #[test]
    #[serial]
    fn test_resolve_endpoint_env_var() {
        let _s = ScopedEnv::set(super::super::GCE_METADATA_HOST_ENV_VAR, "env.var.host");
        let client = Client::new(None);
        assert_eq!(client.endpoint, "http://env.var.host");
    }

    #[test]
    #[serial]
    fn test_resolve_endpoint_priority() {
        let _s = ScopedEnv::set(super::super::GCE_METADATA_HOST_ENV_VAR, "env.priority.host");
        // Env var should take precedence over constructor argument
        let client = Client::new(Some("http://custom.endpoint".to_string()));
        assert_eq!(client.endpoint, "http://env.priority.host");
    }

    #[tokio::test]
    #[parallel]
    async fn test_universe_domain_retry_success() -> TestResult {
        let server = Server::run();
        let client = Client::new(Some(format!("http://{}", server.addr())));

        // First request fails, second succeeds
        let responses: Vec<Box<dyn Responder>> = vec![
            Box::new(status_code(500)),
            Box::new(status_code(200).body("my-universe-domain.com")),
        ];
        server.expect(
            Expectation::matching(all_of![
                request::method("GET"),
                request::path(MDS_UNIVERSE_DOMAIN_URI),
            ])
            .times(2)
            .respond_with(cycle(responses)),
        );

        let domain = client
            .universe_domain()
            .with_retry_policy(get_mock_auth_retry_policy(2).into())
            .with_backoff_policy(get_mock_backoff_policy().into())
            .with_retry_throttler(get_mock_retry_throttler().into())
            .send()
            .await?;

        assert_eq!(domain, "my-universe-domain.com");

        Ok(())
    }

    #[tokio::test]
    #[parallel]
    async fn test_universe_domain_retry_failure() -> TestResult {
        let server = Server::run();
        let client = Client::new(Some(format!("http://{}", server.addr())));

        // All requests fail
        server.expect(
            Expectation::matching(all_of![
                request::method("GET"),
                request::path(MDS_UNIVERSE_DOMAIN_URI),
            ])
            .times(2)
            .respond_with(status_code(500)),
        );

        let err = client
            .universe_domain()
            .with_retry_policy(get_mock_auth_retry_policy(2).into())
            .with_backoff_policy(get_mock_backoff_policy().into())
            .with_retry_throttler(get_mock_retry_throttler().into())
            .send()
            .await
            .unwrap_err();

        assert!(err.to_string().contains("failed to fetch universe domain"));

        Ok(())
    }
}
