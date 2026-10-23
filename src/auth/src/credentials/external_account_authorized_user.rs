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

//! [Workforce Identity Federation] user Credentials type.
//!
//! Workforce Identity Federation lets users from an external identity
//! provider (IdP), such as Microsoft Entra ID or Okta, access Google Cloud
//! resources. When such a user signs in using
//! `gcloud auth application-default login --login-config=...`, gcloud writes
//! an `external_account_authorized_user` credentials file. This file contains
//! an OAuth 2.0 refresh token issued by the [Security Token Service] (STS).
//!
//! This module provides [Credentials] that use that refresh token to obtain
//! access tokens from STS.
//!
//! The Google Cloud client libraries for Rust will typically find and use these
//! credentials automatically if a credentials file exists in the
//! standard ADC search paths. You might instantiate these credentials
//! directly using the [`Builder`] if you need to:
//! * Load credentials from a non-standard location or source.
//! * Override the **quota project ID** for billing and quota management.
//! * Override the **token URL** used to refresh access tokens.
//! * Customize the **retry behavior** when fetching access tokens.
//!
//! ## Example: Creating credentials from a JSON object
//!
//! ```
//! # use google_cloud_auth::credentials::external_account_authorized_user::Builder;
//! # use google_cloud_auth::credentials::Credentials;
//! # use http::Extensions;
//! # async fn sample() -> anyhow::Result<()> {
//! let authorized_user = serde_json::json!({
//!     "type": "external_account_authorized_user",
//!     "audience": "//iam.googleapis.com/locations/global/workforcePools/POOL_ID/providers/PROVIDER_ID",
//!     "client_id": "YOUR_CLIENT_ID",
//!     "client_secret": "YOUR_CLIENT_SECRET", // LOAD SECURELY!
//!     "refresh_token": "YOUR_REFRESH_TOKEN", // LOAD SECURELY!
//!     "token_url": "https://sts.googleapis.com/v1/oauthtoken",
//!     "token_info_url": "https://sts.googleapis.com/v1/introspect",
//!     // "quota_project_id": "your-billing-project-id", // Optional: Set if needed
//! });
//! let credentials: Credentials = Builder::new(authorized_user).build()?;
//! let headers = credentials.headers(Extensions::new()).await?;
//! println!("Headers: {headers:?}");
//! # Ok(()) }
//! ```
//!
//! [Security Token Service]: https://cloud.google.com/iam/docs/reference/sts/rest
//! [Workforce Identity Federation]: https://cloud.google.com/iam/docs/workforce-identity-federation

use crate::access_boundary::{CredentialsWithAccessBoundary, external_account_lookup_url};
use crate::build_errors::Error as BuilderError;
use crate::constants::STS_OAUTH_TOKEN_URL;
use crate::credentials::dynamic::{AccessTokenCredentialsProvider, CredentialsProvider};
use crate::credentials::{AccessToken, AccessTokenCredentials, CacheableResource, Credentials};
use crate::errors::{self, CredentialsError};
use crate::headers_util::AuthHeadersBuilder;
use crate::retry::Builder as RetryTokenProviderBuilder;
use crate::token::{CachedTokenProvider, Token, TokenProvider};
use crate::token_cache::TokenCache;
use crate::{BuildResult, Result};
use google_cloud_gax::backoff_policy::BackoffPolicyArg;
use google_cloud_gax::retry_policy::RetryPolicyArg;
use google_cloud_gax::retry_throttler::RetryThrottlerArg;
use http::{Extensions, HeaderMap};
use reqwest::Client;
use serde_json::Value;
use std::sync::Mutex;
use tokio::time::{Duration, Instant};

/// A builder for constructing `external_account_authorized_user` [Credentials].
///
/// # Example
/// ```
/// # use google_cloud_auth::credentials::external_account_authorized_user::Builder;
/// # async fn sample() -> anyhow::Result<()> {
/// let authorized_user = serde_json::json!({ /* add details here */ });
/// let credentials = Builder::new(authorized_user).build()?;
/// # Ok(()) }
/// ```
pub struct Builder {
    authorized_user: Value,
    quota_project_id: Option<String>,
    token_url: Option<String>,
    universe_domain: Option<String>,
    retry_builder: RetryTokenProviderBuilder,
    iam_endpoint_override: Option<String>,
}

impl Builder {
    /// Creates a new builder using an `external_account_authorized_user` JSON value.
    ///
    /// This JSON is typically generated when a workforce identity federation
    /// user authenticates using `gcloud auth application-default login`.
    pub fn new(authorized_user: Value) -> Self {
        Self {
            authorized_user,
            quota_project_id: None,
            token_url: None,
            universe_domain: None,
            retry_builder: RetryTokenProviderBuilder::default(),
            iam_endpoint_override: None,
        }
    }

    /// Sets the URL of the STS endpoint used to refresh access tokens.
    ///
    /// Any value provided here overrides a `token_url` value from the input JSON.
    /// Defaults to `https://sts.googleapis.com/v1/oauthtoken` if not
    /// specified here or in the JSON.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_auth::credentials::external_account_authorized_user::Builder;
    /// let authorized_user = serde_json::json!({ /* add details here */ });
    /// let credentials = Builder::new(authorized_user)
    ///     .with_token_url("https://sts-FOOBAR.p.googleapis.com/v1/oauthtoken")
    ///     .build();
    /// ```
    pub fn with_token_url<S: Into<String>>(mut self, token_url: S) -> Self {
        self.token_url = Some(token_url.into());
        self
    }

    /// Sets the [quota project] for these credentials.
    ///
    /// Any value set here overrides a `quota_project_id` value from the
    /// input JSON.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_auth::credentials::external_account_authorized_user::Builder;
    /// let authorized_user = serde_json::json!({ /* add details here */ });
    /// let credentials = Builder::new(authorized_user)
    ///     .with_quota_project_id("my-project")
    ///     .build();
    /// ```
    ///
    /// [quota project]: https://cloud.google.com/docs/quotas/quota-project
    pub fn with_quota_project_id<S: Into<String>>(mut self, quota_project_id: S) -> Self {
        self.quota_project_id = Some(quota_project_id.into());
        self
    }

    /// Sets the universe domain for these credentials.
    ///
    /// Any value set here overrides a `universe_domain` value from the
    /// input JSON.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_auth::credentials::external_account_authorized_user::Builder;
    /// let authorized_user = serde_json::json!({ /* add details here */ });
    /// let credentials = Builder::new(authorized_user)
    ///     .with_universe_domain("googleapis.com")
    ///     .build();
    /// ```
    pub fn with_universe_domain<S: Into<String>>(mut self, universe_domain: S) -> Self {
        self.universe_domain = Some(universe_domain.into());
        self
    }

    // Only to satisfy ADC credential builder. The STS refresh token grant
    // does not accept scopes; the token has the scopes granted at login.
    pub(crate) fn with_scopes<I, S>(self, _scopes: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self
    }

    /// Configure the retry policy for fetching tokens.
    ///
    /// The retry policy controls how to handle retries, and sets limits on
    /// the number of attempts or the total time spent retrying.
    ///
    /// ```
    /// # use google_cloud_auth::credentials::external_account_authorized_user::Builder;
    /// # async fn sample() -> anyhow::Result<()> {
    /// use google_cloud_gax::retry_policy::{AlwaysRetry, RetryPolicyExt};
    /// let authorized_user = serde_json::json!({
    ///     "type": "external_account_authorized_user",
    ///     "client_id": "YOUR_CLIENT_ID",
    ///     "client_secret": "YOUR_CLIENT_SECRET",
    ///     "refresh_token": "YOUR_REFRESH_TOKEN",
    /// });
    /// let credentials = Builder::new(authorized_user)
    ///     .with_retry_policy(AlwaysRetry.with_attempt_limit(3))
    ///     .build()?;
    /// # Ok(()) }
    /// ```
    pub fn with_retry_policy<V: Into<RetryPolicyArg>>(mut self, v: V) -> Self {
        self.retry_builder = self.retry_builder.with_retry_policy(v.into());
        self
    }

    /// Configure the retry backoff policy.
    ///
    /// The backoff policy controls how long to wait in between retry attempts.
    ///
    /// ```
    /// # use google_cloud_auth::credentials::external_account_authorized_user::Builder;
    /// # async fn sample() -> anyhow::Result<()> {
    /// use google_cloud_gax::exponential_backoff::ExponentialBackoff;
    /// let authorized_user = serde_json::json!({
    ///     "type": "external_account_authorized_user",
    ///     "client_id": "YOUR_CLIENT_ID",
    ///     "client_secret": "YOUR_CLIENT_SECRET",
    ///     "refresh_token": "YOUR_REFRESH_TOKEN",
    /// });
    /// let credentials = Builder::new(authorized_user)
    ///     .with_backoff_policy(ExponentialBackoff::default())
    ///     .build()?;
    /// # Ok(()) }
    /// ```
    pub fn with_backoff_policy<V: Into<BackoffPolicyArg>>(mut self, v: V) -> Self {
        self.retry_builder = self.retry_builder.with_backoff_policy(v.into());
        self
    }

    /// Configure the retry throttler.
    ///
    /// Advanced applications may want to configure a retry throttler to
    /// [Address Cascading Failures] and when [Handling Overload] conditions.
    /// The authentication library throttles its retry loop, using a policy to
    /// control the throttling algorithm. Use this method to fine tune or
    /// customize the default retry throttler.
    ///
    /// [Handling Overload]: https://sre.google/sre-book/handling-overload/
    /// [Address Cascading Failures]: https://sre.google/sre-book/addressing-cascading-failures/
    ///
    /// ```
    /// # use google_cloud_auth::credentials::external_account_authorized_user::Builder;
    /// # async fn sample() -> anyhow::Result<()> {
    /// use google_cloud_gax::retry_throttler::AdaptiveThrottler;
    /// let authorized_user = serde_json::json!({
    ///     "type": "external_account_authorized_user",
    ///     "client_id": "YOUR_CLIENT_ID",
    ///     "client_secret": "YOUR_CLIENT_SECRET",
    ///     "refresh_token": "YOUR_REFRESH_TOKEN",
    /// });
    /// let credentials = Builder::new(authorized_user)
    ///     .with_retry_throttler(AdaptiveThrottler::default())
    ///     .build()?;
    /// # Ok(()) }
    /// ```
    pub fn with_retry_throttler<V: Into<RetryThrottlerArg>>(mut self, v: V) -> Self {
        self.retry_builder = self.retry_builder.with_retry_throttler(v.into());
        self
    }

    #[cfg(all(test, google_cloud_unstable_trust_boundaries))]
    fn maybe_iam_endpoint_override(mut self, iam_endpoint_override: Option<String>) -> Self {
        self.iam_endpoint_override = iam_endpoint_override;
        self
    }

    /// Returns a [Credentials] instance with the configured settings.
    ///
    /// # Errors
    ///
    /// Returns a [BuilderError] if the JSON provided to [`Builder::new`]
    /// cannot be deserialized into the expected format. This typically happens
    /// if the JSON value is malformed or missing required fields.
    pub fn build(self) -> BuildResult<Credentials> {
        Ok(self.build_credentials()?.into())
    }

    /// Returns an [AccessTokenCredentials] instance with the configured settings.
    ///
    /// # Example
    ///
    /// ```
    /// # use google_cloud_auth::credentials::external_account_authorized_user::Builder;
    /// # use google_cloud_auth::credentials::{AccessTokenCredentials, AccessTokenCredentialsProvider};
    /// # async fn sample() -> anyhow::Result<()> {
    /// let authorized_user = serde_json::json!({
    ///     "type": "external_account_authorized_user",
    ///     "client_id": "YOUR_CLIENT_ID",
    ///     "client_secret": "YOUR_CLIENT_SECRET",
    ///     "refresh_token": "YOUR_REFRESH_TOKEN",
    /// });
    /// let credentials: AccessTokenCredentials = Builder::new(authorized_user)
    ///     .build_access_token_credentials()?;
    /// let access_token = credentials.access_token().await?;
    /// println!("Token: {}", access_token.token);
    /// # Ok(()) }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns a [BuilderError] if the JSON provided to [`Builder::new`]
    /// cannot be deserialized into the expected format. This typically happens
    /// if the JSON value is malformed or missing required fields.
    pub fn build_access_token_credentials(self) -> BuildResult<AccessTokenCredentials> {
        Ok(self.build_credentials()?.into())
    }

    fn build_credentials(
        self,
    ) -> BuildResult<
        CredentialsWithAccessBoundary<ExternalAccountAuthorizedUserCredentials<TokenCache>>,
    > {
        let authorized_user =
            serde_json::from_value::<ExternalAccountAuthorizedUser>(self.authorized_user)
                .map_err(BuilderError::parsing)?;

        let universe_domain = self.universe_domain.or(authorized_user.universe_domain);
        let token_url = self
            .token_url
            .or(authorized_user.token_url)
            .unwrap_or_else(|| STS_OAUTH_TOKEN_URL.to_string());
        let quota_project_id = self.quota_project_id.or(authorized_user.quota_project_id);

        let access_boundary_url = authorized_user.audience.as_deref().and_then(|audience| {
            external_account_lookup_url(audience, self.iam_endpoint_override.as_deref())
        });

        let token_provider = ExternalAccountAuthorizedUserTokenProvider {
            client: Client::new(),
            client_id: authorized_user.client_id,
            client_secret: authorized_user.client_secret,
            refresh_token: Mutex::new(authorized_user.refresh_token),
            token_url,
        };
        let token_provider = TokenCache::new(self.retry_builder.build(token_provider));

        let creds = ExternalAccountAuthorizedUserCredentials {
            token_provider,
            quota_project_id,
            universe_domain,
        };

        Ok(CredentialsWithAccessBoundary::new(
            creds,
            access_boundary_url,
        ))
    }
}

struct ExternalAccountAuthorizedUserTokenProvider {
    client: Client,
    client_id: String,
    client_secret: String,
    // STS may rotate the refresh token, returning a new one with each access
    // token. The most recent refresh token is used for the next request.
    refresh_token: Mutex<String>,
    token_url: String,
}

impl std::fmt::Debug for ExternalAccountAuthorizedUserTokenProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExternalAccountAuthorizedUserTokenProvider")
            .field("client_id", &self.client_id)
            .field("client_secret", &"[censored]")
            .field("refresh_token", &"[censored]")
            .field("token_url", &self.token_url)
            .finish()
    }
}

#[async_trait::async_trait]
impl TokenProvider for ExternalAccountAuthorizedUserTokenProvider {
    async fn token(&self) -> Result<Token> {
        let refresh_token = self
            .refresh_token
            .lock()
            .expect("refresh token mutex is never poisoned")
            .clone();
        let params = [
            ("grant_type", "refresh_token"),
            ("refresh_token", refresh_token.as_str()),
        ];
        let resp = self
            .client
            .post(self.token_url.as_str())
            .basic_auth(&self.client_id, Some(&self.client_secret))
            .form(&params)
            .send()
            .await
            .map_err(|e| errors::from_http_error(e, MSG))?;

        if !resp.status().is_success() {
            let err = errors::from_http_response(resp, MSG).await;
            return Err(err);
        }
        let response = resp.json::<RefreshResponse>().await.map_err(|e| {
            let retryable = !e.is_decode();
            CredentialsError::from_source(retryable, e)
        })?;

        if let Some(new_refresh_token) = response.refresh_token {
            *self
                .refresh_token
                .lock()
                .expect("refresh token mutex is never poisoned") = new_refresh_token;
        }

        Ok(Token {
            token: response.access_token,
            token_type: response.token_type,
            expires_at: Some(Instant::now() + Duration::from_secs(response.expires_in)),
            metadata: None,
        })
    }
}

const MSG: &str = "failed to refresh external account authorized user access token";

#[derive(Debug)]
struct ExternalAccountAuthorizedUserCredentials<T>
where
    T: CachedTokenProvider,
{
    token_provider: T,
    quota_project_id: Option<String>,
    universe_domain: Option<String>,
}

#[async_trait::async_trait]
impl<T> CredentialsProvider for ExternalAccountAuthorizedUserCredentials<T>
where
    T: CachedTokenProvider,
{
    async fn headers(&self, extensions: Extensions) -> Result<CacheableResource<HeaderMap>> {
        let token = self.token_provider.token(extensions).await?;

        AuthHeadersBuilder::new(&token)
            .maybe_quota_project_id(self.quota_project_id.as_deref())
            .build()
    }

    async fn universe_domain(&self) -> Option<String> {
        self.universe_domain.clone()
    }
}

#[async_trait::async_trait]
impl<T> AccessTokenCredentialsProvider for ExternalAccountAuthorizedUserCredentials<T>
where
    T: CachedTokenProvider,
{
    async fn access_token(&self) -> Result<AccessToken> {
        let token = self.token_provider.token(Extensions::new()).await?;
        token.into()
    }
}

/// The `external_account_authorized_user` file format.
///
/// The file may also contain `token_info_url` and `revoke_url`, which are
/// not needed to refresh access tokens and are ignored.
#[derive(Debug, PartialEq, serde::Deserialize)]
struct ExternalAccountAuthorizedUser {
    audience: Option<String>,
    client_id: String,
    client_secret: String,
    refresh_token: String,
    token_url: Option<String>,
    quota_project_id: Option<String>,
    universe_domain: Option<String>,
}

#[derive(Debug, PartialEq, serde::Deserialize, serde::Serialize)]
struct RefreshResponse {
    access_token: String,
    token_type: String,
    expires_in: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    refresh_token: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::credentials::QUOTA_PROJECT_KEY;
    use crate::credentials::tests::{
        get_headers_from_cache, get_mock_auth_retry_policy, get_mock_backoff_policy,
        get_mock_retry_throttler, get_token_from_headers,
    };
    use base64::Engine;
    use http::header::AUTHORIZATION;
    use httptest::matchers::{all_of, contains, request, url_decoded};
    use httptest::responders::{json_encoded, status_code};
    use httptest::{Expectation, Server, cycle};

    type TestResult = anyhow::Result<()>;

    fn authorized_user_json(token_url: String) -> Value {
        serde_json::json!({
            "type": "external_account_authorized_user",
            "audience": "//iam.googleapis.com/locations/global/workforcePools/pool/providers/provider",
            "client_id": "test-client-id",
            "client_secret": "test-client-secret",
            "refresh_token": "test-refresh-token",
            "token_url": token_url,
            "token_info_url": "https://sts.googleapis.com/v1/introspect",
        })
    }

    fn expected_basic_auth() -> String {
        let encoded =
            base64::engine::general_purpose::STANDARD.encode("test-client-id:test-client-secret");
        format!("Basic {encoded}")
    }

    fn response(refresh_token: Option<&str>) -> RefreshResponse {
        RefreshResponse {
            access_token: "test-access-token".to_string(),
            token_type: "Bearer".to_string(),
            expires_in: 3600,
            refresh_token: refresh_token.map(str::to_string),
        }
    }

    #[test]
    fn from_json_full() {
        let json = serde_json::json!({
            "type": "external_account_authorized_user",
            "audience": "test-audience",
            "client_id": "test-client-id",
            "client_secret": "test-client-secret",
            "refresh_token": "test-refresh-token",
            "token_url": "test-token-url",
            "token_info_url": "test-token-info-url",
            "revoke_url": "test-revoke-url",
            "quota_project_id": "test-project",
            "universe_domain": "test-universe",
        });
        let expected = ExternalAccountAuthorizedUser {
            audience: Some("test-audience".to_string()),
            client_id: "test-client-id".to_string(),
            client_secret: "test-client-secret".to_string(),
            refresh_token: "test-refresh-token".to_string(),
            token_url: Some("test-token-url".to_string()),
            quota_project_id: Some("test-project".to_string()),
            universe_domain: Some("test-universe".to_string()),
        };
        let actual = serde_json::from_value::<ExternalAccountAuthorizedUser>(json).unwrap();
        assert_eq!(actual, expected);
    }

    #[test]
    fn builder_missing_required_fields() {
        let json_full = authorized_user_json("test-token-url".to_string());
        for required_field in ["client_id", "client_secret", "refresh_token"] {
            let mut json = json_full.clone();
            json[required_field].take();
            let err = Builder::new(json).build().unwrap_err();
            assert!(err.is_parsing(), "{required_field}: {err:?}");
        }
    }

    #[test]
    fn debug_token_provider() {
        let provider = ExternalAccountAuthorizedUserTokenProvider {
            client: Client::new(),
            client_id: "test-client-id".to_string(),
            client_secret: "test-client-secret".to_string(),
            refresh_token: Mutex::new("test-refresh-token".to_string()),
            token_url: STS_OAUTH_TOKEN_URL.to_string(),
        };
        let fmt = format!("{provider:?}");
        assert!(fmt.contains("test-client-id"), "{fmt}");
        assert!(!fmt.contains("test-client-secret"), "{fmt}");
        assert!(!fmt.contains("test-refresh-token"), "{fmt}");
        assert!(fmt.contains(STS_OAUTH_TOKEN_URL), "{fmt}");
    }

    #[tokio::test]
    async fn universe_domain() -> TestResult {
        let json = authorized_user_json("test-token-url".to_string());
        let creds = Builder::new(json.clone()).build()?;
        assert_eq!(creds.universe_domain().await, None);

        let mut json = json;
        json["universe_domain"] = "my-universe.com".into();
        let creds = Builder::new(json.clone()).build()?;
        assert_eq!(
            creds.universe_domain().await.as_deref(),
            Some("my-universe.com")
        );

        let creds = Builder::new(json)
            .with_universe_domain("my-override.com")
            .build()?;
        assert_eq!(
            creds.universe_domain().await.as_deref(),
            Some("my-override.com")
        );
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn token_provider_success() -> TestResult {
        let server = Server::run();
        server.expect(
            Expectation::matching(all_of![
                request::method_path("POST", "/token"),
                request::headers(contains(("authorization", expected_basic_auth()))),
                request::headers(contains((
                    "content-type",
                    "application/x-www-form-urlencoded"
                ))),
                request::body(url_decoded(contains(("grant_type", "refresh_token")))),
                request::body(url_decoded(contains((
                    "refresh_token",
                    "test-refresh-token"
                )))),
            ])
            .respond_with(json_encoded(response(None))),
        );

        let tp = ExternalAccountAuthorizedUserTokenProvider {
            client: Client::new(),
            client_id: "test-client-id".to_string(),
            client_secret: "test-client-secret".to_string(),
            refresh_token: Mutex::new("test-refresh-token".to_string()),
            token_url: server.url("/token").to_string(),
        };
        let now = Instant::now();
        let token = tp.token().await?;
        assert_eq!(token.token, "test-access-token");
        assert_eq!(token.token_type, "Bearer");
        assert_eq!(token.expires_at, Some(now + Duration::from_secs(3600)));
        Ok(())
    }

    #[tokio::test]
    async fn token_provider_missing_response_fields_fails() -> TestResult {
        let server = Server::run();
        server.expect(
            Expectation::matching(request::path("/token")).respond_with(cycle![
                json_encoded(serde_json::json!({
                    "access_token": "test-access-token",
                    "expires_in": 3600,
                })),
                json_encoded(serde_json::json!({
                    "access_token": "test-access-token",
                    "token_type": "Bearer",
                })),
            ]),
        );

        let creds = Builder::new(authorized_user_json(server.url("/token").to_string())).build()?;
        for _ in 0..2 {
            let err = creds.headers(Extensions::new()).await.unwrap_err();
            assert!(!err.is_transient(), "{err:?}");
        }
        Ok(())
    }

    #[tokio::test]
    async fn token_provider_rotates_refresh_token() -> TestResult {
        let server = Server::run();
        server.expect(
            Expectation::matching(all_of![
                request::path("/token"),
                request::body(url_decoded(contains((
                    "refresh_token",
                    "test-refresh-token"
                )))),
            ])
            .times(1)
            .respond_with(json_encoded(response(Some("rotated-refresh-token")))),
        );
        server.expect(
            Expectation::matching(all_of![
                request::path("/token"),
                request::body(url_decoded(contains((
                    "refresh_token",
                    "rotated-refresh-token"
                )))),
            ])
            .times(1)
            .respond_with(json_encoded(response(None))),
        );

        let tp = ExternalAccountAuthorizedUserTokenProvider {
            client: Client::new(),
            client_id: "test-client-id".to_string(),
            client_secret: "test-client-secret".to_string(),
            refresh_token: Mutex::new("test-refresh-token".to_string()),
            token_url: server.url("/token").to_string(),
        };
        tp.token().await?;
        tp.token().await?;
        assert_eq!(
            *tp.refresh_token.lock().unwrap(),
            "rotated-refresh-token".to_string()
        );
        Ok(())
    }

    #[tokio::test]
    async fn credentials_with_quota_project_and_caching() -> TestResult {
        let mut server = Server::run();
        server.expect(
            Expectation::matching(request::path("/token"))
                .times(1)
                .respond_with(json_encoded(response(None))),
        );

        let mut json = authorized_user_json("unused".to_string());
        json["quota_project_id"] = "json-project".into();
        let creds = Builder::new(json)
            .with_token_url(server.url("/token").to_string())
            .with_quota_project_id("test-project")
            .build()?;

        for _ in 0..2 {
            let headers = get_headers_from_cache(creds.headers(Extensions::new()).await?)?;
            assert_eq!(headers.len(), 2, "{headers:?}");
            let token = headers.get(AUTHORIZATION).unwrap();
            assert_eq!(token, "Bearer test-access-token");
            assert!(token.is_sensitive());
            assert_eq!(headers.get(QUOTA_PROJECT_KEY).unwrap(), "test-project");
        }
        server.verify_and_clear();
        Ok(())
    }

    #[tokio::test]
    async fn access_token_credentials() -> TestResult {
        let server = Server::run();
        server.expect(
            Expectation::matching(request::path("/token"))
                .respond_with(json_encoded(response(None))),
        );

        let creds = Builder::new(authorized_user_json(server.url("/token").to_string()))
            .build_access_token_credentials()?;
        let token = creds.access_token().await?;
        assert_eq!(token.token, "test-access-token");
        Ok(())
    }

    #[tokio::test]
    async fn retries_on_transient_failures() -> TestResult {
        let mut server = Server::run();
        server.expect(
            Expectation::matching(request::path("/token"))
                .times(3)
                .respond_with(cycle![
                    status_code(503).body("try-again"),
                    status_code(503).body("try-again"),
                    json_encoded(response(None)),
                ]),
        );

        let creds = Builder::new(authorized_user_json(server.url("/token").to_string()))
            .with_retry_policy(get_mock_auth_retry_policy(3))
            .with_backoff_policy(get_mock_backoff_policy())
            .with_retry_throttler(get_mock_retry_throttler())
            .build()?;
        let token = get_token_from_headers(creds.headers(Extensions::new()).await?);
        assert_eq!(token.as_deref(), Some("test-access-token"));
        server.verify_and_clear();
        Ok(())
    }

    #[tokio::test]
    async fn does_not_retry_on_non_transient_failures() -> TestResult {
        let mut server = Server::run();
        server.expect(
            Expectation::matching(request::path("/token"))
                .times(1)
                .respond_with(status_code(401)),
        );

        let creds = Builder::new(authorized_user_json(server.url("/token").to_string()))
            .with_retry_policy(get_mock_auth_retry_policy(3))
            .with_backoff_policy(get_mock_backoff_policy())
            .with_retry_throttler(get_mock_retry_throttler())
            .build()?;
        let err = creds.headers(Extensions::new()).await.unwrap_err();
        assert!(!err.is_transient(), "{err:?}");
        server.verify_and_clear();
        Ok(())
    }

    #[tokio::test]
    async fn malformed_response_is_not_retryable() -> TestResult {
        let server = Server::run();
        server.expect(
            Expectation::matching(request::path("/token"))
                .respond_with(json_encoded("bad json".to_string())),
        );

        let creds = Builder::new(authorized_user_json(server.url("/token").to_string())).build()?;
        let err = creds.headers(Extensions::new()).await.unwrap_err();
        assert!(!err.is_transient(), "{err:?}");
        Ok(())
    }

    #[tokio::test]
    #[cfg(google_cloud_unstable_trust_boundaries)]
    async fn e2e_access_boundary() -> TestResult {
        use crate::credentials::tests::get_access_boundary_from_headers;

        let server = Server::run();
        server.expect(
            Expectation::matching(request::method_path("POST", "/token"))
                .respond_with(json_encoded(response(None))),
        );
        server.expect(
            Expectation::matching(request::method_path(
                "GET",
                "/v1/locations/global/workforcePools/pool/allowedLocations",
            ))
            .respond_with(json_encoded(serde_json::json!({
                "locations": ["us-central1"],
                "encodedLocations": "0x1234"
            }))),
        );

        let iam_endpoint = server.url("").to_string().trim_end_matches('/').to_string();

        let creds = Builder::new(authorized_user_json(server.url("/token").to_string()))
            .maybe_iam_endpoint_override(Some(iam_endpoint))
            .build_credentials()?;

        creds.wait_for_boundary().await;

        let headers = creds.headers(Extensions::new()).await?;
        let token = get_token_from_headers(headers.clone());
        let access_boundary = get_access_boundary_from_headers(headers);

        assert_eq!(token.as_deref(), Some("test-access-token"));
        assert_eq!(access_boundary.as_deref(), Some("0x1234"));
        Ok(())
    }
}
