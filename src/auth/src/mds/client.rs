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

use crate::errors::{self, CredentialsError};
use crate::token::Token;
use google_cloud_gax::backoff_policy::BackoffPolicy;
use google_cloud_gax::backoff_policy::BackoffPolicyArg;
use google_cloud_gax::exponential_backoff::ExponentialBackoff;
use google_cloud_gax::retry_loop_internal::retry_loop;
use google_cloud_gax::retry_policy::RetryPolicyArg;
use google_cloud_gax::retry_policy::{Aip194Strict, RetryPolicy, RetryPolicyExt};
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
    ) -> crate::Result<reqwest::Response> {
        let response = request
            .send()
            .await
            .map_err(|e| errors::from_http_error(e, error_message))?;

        let response = Self::check_response_status(response, error_message).await?;

        Ok(response)
    }

    async fn send_with_retry(
        &self,
        request: reqwest::RequestBuilder,
        error_message: &'static str,
        retry_config: RetryConfig,
    ) -> crate::Result<reqwest::Response> {
        let sleep = async |d| tokio::time::sleep(d).await;

        if !retry_config.has_retry_config() {
            return self.send(request, error_message).await;
        }

        let (retry_policy, backoff_policy, retry_throttler) = retry_config.build();

        retry_loop(
            async move |_| {
                let req = request
                    .try_clone()
                    .expect("client libraries only create builders where `try_clone()` succeeds");
                let response = req
                    .send()
                    .await
                    .map_err(google_cloud_gax::error::Error::io)?;

                let status = response.status();
                if !status.is_success() {
                    let err_headers = response.headers().clone();
                    let err_payload = response.bytes().await.map_err(|e| {
                        google_cloud_gax::error::Error::transport(err_headers.clone(), e)
                    })?;
                    return Err(google_cloud_gax::error::Error::http(
                        status.as_u16(),
                        err_headers,
                        err_payload,
                    ));
                }

                Ok(response)
            },
            sleep,
            true, // GET requests are idempotent
            retry_throttler,
            retry_policy,
            backoff_policy,
        )
        .await
        .map_err(|e| errors::CredentialsError::new(false, error_message, e))
    }

    async fn check_response_status(
        response: reqwest::Response,
        error_message: &str,
    ) -> crate::Result<reqwest::Response> {
        if !response.status().is_success() {
            let err = errors::from_http_response(response, error_message).await;
            Err(err)
        } else {
            Ok(response)
        }
    }
}

#[derive(Clone, Default)]
struct RetryConfig {
    retry_policy: Option<RetryPolicyArg>,
    backoff_policy: Option<BackoffPolicyArg>,
    retry_throttler: Option<RetryThrottlerArg>,
}

impl RetryConfig {
    fn with_retry_policy(mut self, retry_policy: RetryPolicyArg) -> Self {
        self.retry_policy = Some(retry_policy);
        self
    }

    fn with_backoff_policy(mut self, backoff_policy: BackoffPolicyArg) -> Self {
        self.backoff_policy = Some(backoff_policy);
        self
    }

    fn with_retry_throttler(mut self, retry_throttler: RetryThrottlerArg) -> Self {
        self.retry_throttler = Some(retry_throttler);
        self
    }

    fn has_retry_config(&self) -> bool {
        self.retry_policy.is_some()
            || self.backoff_policy.is_some()
            || self.retry_throttler.is_some()
    }

    fn build(
        self,
    ) -> (
        Arc<dyn RetryPolicy>,
        Arc<dyn BackoffPolicy>,
        SharedRetryThrottler,
    ) {
        let backoff_policy: Arc<dyn BackoffPolicy> = match self.backoff_policy {
            Some(p) => p.into(),
            None => Arc::new(ExponentialBackoff::default()),
        };
        let retry_throttler: SharedRetryThrottler = match self.retry_throttler {
            Some(p) => p.into(),
            None => Arc::new(Mutex::new(AdaptiveThrottler::default())),
        };

        let retry_policy = self
            .retry_policy
            .unwrap_or_else(|| Aip194Strict.with_time_limit(Duration::from_secs(60)).into())
            .into();

        (retry_policy, backoff_policy, retry_throttler)
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
        let response = self.client.send(request, error_message).await?;

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
        let response = self.client.send(request, error_message).await?;

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

        let response = self.client.send(request, error_message).await?;

        let email = response
            .text()
            .await
            .map_err(|e| CredentialsError::from_source(!e.is_decode(), e))?;

        Ok(email)
    }
}

#[derive(Clone)]
pub(crate) struct UniverseDomainRequest {
    client: Client,
    retry_config: RetryConfig,
}

impl UniverseDomainRequest {
    pub(crate) fn with_retry_policy(mut self, retry_policy: RetryPolicyArg) -> Self {
        self.retry_config = self.retry_config.with_retry_policy(retry_policy);
        self
    }

    pub(crate) fn with_backoff_policy(mut self, backoff_policy: BackoffPolicyArg) -> Self {
        self.retry_config = self.retry_config.with_backoff_policy(backoff_policy);
        self
    }

    pub(crate) fn with_retry_throttler(mut self, retry_throttler: RetryThrottlerArg) -> Self {
        self.retry_config = self.retry_config.with_retry_throttler(retry_throttler);
        self
    }

    pub(crate) async fn send(self) -> crate::Result<String> {
        let path = super::MDS_UNIVERSE_DOMAIN_URI;
        let request = self.client.get(path);
        let error_message = "failed to fetch universe domain";

        let response = self
            .client
            .send_with_retry(request, error_message, self.retry_config)
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
    use crate::mds::MDS_DEFAULT_URI;
    use httptest::{Expectation, Server, matchers::*, responders::*};
    use scoped_env::ScopedEnv;
    use serial_test::{parallel, serial};

    #[tokio::test]
    #[parallel]
    async fn test_access_token_success() {
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
                    .body(serde_json::to_string(&response).unwrap()),
            ),
        );

        let token = client
            .access_token(Some(vec!["scope1".to_string(), "scope2".to_string()]))
            .send()
            .await
            .unwrap();
        assert_eq!(token.token, "test-token");
        assert_eq!(token.token_type, "Bearer");
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
    async fn test_id_token_success() {
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
            .await
            .unwrap();
        assert_eq!(token, "test-id-token");
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
    async fn test_email_success() {
        let server = Server::run();
        let client = Client::new(Some(format!("http://{}", server.addr())));

        server.expect(
            Expectation::matching(all_of![
                request::method("GET"),
                request::path(format!("{}/email", MDS_DEFAULT_URI)),
            ])
            .respond_with(status_code(200).body("test@example.com")),
        );

        let email = client.email().send().await.unwrap();
        assert_eq!(email, "test@example.com");
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
}
