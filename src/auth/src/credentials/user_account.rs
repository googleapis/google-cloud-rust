// Copyright 2024 Google LLC
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

//! [User Account] Credentials type.
//!
//! User accounts represent a developer, administrator, or any other person who
//! interacts with Google APIs and services. User accounts are managed as
//! [Google Accounts], either via [Google Workspace] or [Cloud Identity].
//!
//! This module provides [Credentials] derived from user account
//! information, specifically utilizing an OAuth 2.0 refresh token.
//!
//! This module is designed for refresh tokens obtained via the standard
//! [Authorization Code grant]. Acquiring the initial refresh token (e.g., through
//! user consent) is outside the scope of this library.
//! See [RFC 6749 Section 4.1] for flow details.
//!
//! The Google Cloud client libraries for Rust will typically find and use these
//! credentials automatically if a credentials file exists in the
//! standard ADC search paths. This file is often created by running:
//! `gcloud auth application-default login`. You might instantiate these credentials
//! directly using the [`Builder`] if you need to:
//! * Load credentials from a non-standard location or source.
//! * Override the OAuth 2.0 **scopes** being requested for the access token.
//! * Override the **quota project ID** for billing and quota management.
//! * Override the **token URI** used to fetch access tokens.
//! * Customize the **retry behavior** when fetching access tokens.
//!
//! ## Example: Creating credentials from a JSON object
//!
//! ```
//! # use google_cloud_auth::credentials::user_account::Builder;
//! # use google_cloud_auth::credentials::Credentials;
//! # use http::Extensions;
//! # tokio_test::block_on(async {
//! let authorized_user = serde_json::json!({
//!     "client_id": "YOUR_CLIENT_ID.apps.googleusercontent.com", // Replace with your actual Client ID
//!     "client_secret": "YOUR_CLIENT_SECRET", // Replace with your actual Client Secret - LOAD SECURELY!
//!     "refresh_token": "YOUR_REFRESH_TOKEN", // Replace with the user's refresh token - LOAD SECURELY!
//!     "type": "authorized_user",
//!     // "quota_project_id": "your-billing-project-id", // Optional: Set if needed
//!     // "token_uri" : "test-token-uri", // Optional: Set if needed
//! });
//! let credentials: Credentials = Builder::new(authorized_user).build()?;
//! let headers = credentials.headers(Extensions::new()).await?;
//! println!("Headers: {headers:?}");
//! # Ok::<(), anyhow::Error>(())
//! # });
//! ```
//!
//! ## Example: Creating credentials with custom retry behavior
//!
//! ```
//! # use google_cloud_auth::credentials::user_account::Builder;
//! # use google_cloud_auth::credentials::Credentials;
//! # use http::Extensions;
//! # use std::time::Duration;
//! # tokio_test::block_on(async {
//! use gax::retry_policy::{AlwaysRetry, RetryPolicyExt};
//! use gax::exponential_backoff::ExponentialBackoff;
//! let authorized_user = serde_json::json!({
//!     "client_id": "YOUR_CLIENT_ID.apps.googleusercontent.com",
//!     "client_secret": "YOUR_CLIENT_SECRET",
//!     "refresh_token": "YOUR_REFRESH_TOKEN",
//!     "type": "authorized_user",
//! });
//! let backoff = ExponentialBackoff::default();
//! let credentials: Credentials = Builder::new(authorized_user)
//!     .with_retry_policy(AlwaysRetry.with_attempt_limit(3))
//!     .with_backoff_policy(backoff)
//!     .build()?;
//! let headers = credentials.headers(Extensions::new()).await?;
//! println!("Headers: {headers:?}");
//! # Ok::<(), anyhow::Error>(())
//! # });
//! ```
//!
//! [Authorization Code grant]: https://tools.ietf.org/html/rfc6749#section-1.3.1
//! [Cloud Identity]: https://cloud.google.com/identity
//! [Google Accounts]: https://myaccount.google.com/
//! [Google Workspace]: https://workspace.google.com/
//! [RFC 6749 Section 4.1]: https://datatracker.ietf.org/doc/html/rfc6749#section-4.1
//! [User Account]: https://cloud.google.com/docs/authentication#user-accounts
//! [Workforce Identity Federation]: https://cloud.google.com/iam/docs/workforce-identity-federation

use crate::build_errors::Error as BuilderError;
use crate::constants::OAUTH2_TOKEN_SERVER_URL;
use crate::credentials::dynamic::{AccessTokenCredentialsProvider, CredentialsProvider};
use crate::credentials::{AccessToken, AccessTokenCredentials, CacheableResource, Credentials};
use crate::errors::{self, CredentialsError};
use crate::headers_util::build_cacheable_headers;
use crate::retry::Builder as RetryTokenProviderBuilder;
use crate::token::{CachedTokenProvider, Token, TokenProvider};
use crate::token_cache::TokenCache;
use crate::{BuildResult, Result};
use gax::backoff_policy::BackoffPolicyArg;
use gax::retry_policy::RetryPolicyArg;
use gax::retry_throttler::RetryThrottlerArg;
use http::header::CONTENT_TYPE;
use http::{Extensions, HeaderMap, HeaderValue};
use reqwest::{Client, Method};
use serde_json::Value;
use std::sync::Arc;
use tokio::time::{Duration, Instant};

/// A builder for constructing `user_account` [Credentials] instance.
///
/// # Example
/// ```
/// # use google_cloud_auth::credentials::user_account::Builder;
/// # tokio_test::block_on(async {
/// let authorized_user = serde_json::json!({ /* add details here */ });
/// let credentials = Builder::new(authorized_user).build();
/// })
/// ```
pub struct Builder {
    authorized_user: Value,
    scopes: Option<Vec<String>>,
    quota_project_id: Option<String>,
    token_uri: Option<String>,
    retry_builder: RetryTokenProviderBuilder,
}

impl Builder {
    /// Creates a new builder using `authorized_user` JSON value.
    ///
    /// The `authorized_user` JSON is typically generated when a user
    /// authenticates using the [application-default login] process.
    ///
    /// [application-default login]: https://cloud.google.com/sdk/gcloud/reference/auth/application-default/login
    pub fn new(authorized_user: Value) -> Self {
        Self {
            authorized_user,
            scopes: None,
            quota_project_id: None,
            token_uri: None,
            retry_builder: RetryTokenProviderBuilder::default(),
        }
    }

    /// Sets the URI for the token endpoint used to fetch access tokens.
    ///
    /// Any value provided here overrides a `token_uri` value from the input `authorized_user` JSON.
    /// Defaults to `https://oauth2.googleapis.com/token` if not specified here or in the `authorized_user` JSON.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_auth::credentials::user_account::Builder;
    /// let authorized_user = serde_json::json!({ /* add details here */ });
    /// let credentials = Builder::new(authorized_user)
    ///     .with_token_uri("https://oauth2-FOOBAR.p.googleapis.com")
    ///     .build();
    /// ```
    pub fn with_token_uri<S: Into<String>>(mut self, token_uri: S) -> Self {
        self.token_uri = Some(token_uri.into());
        self
    }

    /// Sets the [scopes] for these credentials.
    ///
    /// `scopes` define the *permissions being requested* for this specific access token
    /// when interacting with a service. For example, `https://www.googleapis.com/auth/devstorage.read_write`.
    /// IAM permissions, on the other hand, define the *underlying capabilities*
    /// the user account possesses within a system. For example, `storage.buckets.delete`.
    /// When a token generated with specific scopes is used, the request must be permitted
    /// by both the user account's underlying IAM permissions and the scopes requested
    /// for the token. Therefore, scopes act as an additional restriction on what the token
    /// can be used for.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_auth::credentials::user_account::Builder;
    /// let authorized_user = serde_json::json!({ /* add details here */ });
    /// let credentials = Builder::new(authorized_user)
    ///     .with_scopes(["https://www.googleapis.com/auth/pubsub"])
    ///     .build();
    /// ```
    /// [scopes]: https://developers.google.com/identity/protocols/oauth2/scopes
    pub fn with_scopes<I, S>(mut self, scopes: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.scopes = Some(scopes.into_iter().map(|s| s.into()).collect());
        self
    }

    /// Sets the [quota project] for these credentials.
    ///
    /// In some services, you can use an account in
    /// one project for authentication and authorization, and charge
    /// the usage to a different project. This requires that the
    /// user has `serviceusage.services.use` permissions on the quota project.
    ///
    /// Any value set here overrides a `quota_project_id` value from the
    /// input `authorized_user` JSON.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_auth::credentials::user_account::Builder;
    /// let authorized_user = serde_json::json!("{ /* add details here */ }");
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

    /// Configure the retry policy for fetching tokens.
    ///
    /// The retry policy controls how to handle retries, and sets limits on
    /// the number of attempts or the total time spent retrying.
    ///
    /// ```
    /// # use google_cloud_auth::credentials::user_account::Builder;
    /// # tokio_test::block_on(async {
    /// use gax::retry_policy::{AlwaysRetry, RetryPolicyExt};
    /// let authorized_user = serde_json::json!({
    ///     "client_id": "YOUR_CLIENT_ID.apps.googleusercontent.com",
    ///     "client_secret": "YOUR_CLIENT_SECRET",
    ///     "refresh_token": "YOUR_REFRESH_TOKEN",
    ///     "type": "authorized_user",
    /// });
    /// let credentials = Builder::new(authorized_user)
    ///     .with_retry_policy(AlwaysRetry.with_attempt_limit(3))
    ///     .build();
    /// # });
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
    /// # use google_cloud_auth::credentials::user_account::Builder;
    /// # use std::time::Duration;
    /// # tokio_test::block_on(async {
    /// use gax::exponential_backoff::ExponentialBackoff;
    /// let authorized_user = serde_json::json!({
    ///     "client_id": "YOUR_CLIENT_ID.apps.googleusercontent.com",
    ///     "client_secret": "YOUR_CLIENT_SECRET",
    ///     "refresh_token": "YOUR_REFRESH_TOKEN",
    ///     "type": "authorized_user",
    /// });
    /// let credentials = Builder::new(authorized_user)
    ///     .with_backoff_policy(ExponentialBackoff::default())
    ///     .build();
    /// # });
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
    /// # use google_cloud_auth::credentials::user_account::Builder;
    /// # tokio_test::block_on(async {
    /// use gax::retry_throttler::AdaptiveThrottler;
    /// let authorized_user = serde_json::json!({
    ///     "client_id": "YOUR_CLIENT_ID.apps.googleusercontent.com",
    ///     "client_secret": "YOUR_CLIENT_SECRET",
    ///     "refresh_token": "YOUR_REFRESH_TOKEN",
    ///     "type": "authorized_user",
    /// });
    /// let credentials = Builder::new(authorized_user)
    ///     .with_retry_throttler(AdaptiveThrottler::default())
    ///     .build();
    /// # });
    /// ```
    pub fn with_retry_throttler<V: Into<RetryThrottlerArg>>(mut self, v: V) -> Self {
        self.retry_builder = self.retry_builder.with_retry_throttler(v.into());
        self
    }

    /// Returns a [Credentials] instance with the configured settings.
    ///
    /// # Errors
    ///
    /// Returns a [CredentialsError] if the `authorized_user`
    /// provided to [`Builder::new`] cannot be successfully deserialized into the
    /// expected format. This typically happens if the JSON value is malformed or
    /// missing required fields. For more information, on how to generate
    /// `authorized_user` json, consult the relevant section in the
    /// [application-default credentials] guide.
    ///
    /// [application-default credentials]: https://cloud.google.com/docs/authentication/application-default-credentials
    pub fn build(self) -> BuildResult<Credentials> {
        Ok(self.build_access_token_credentials()?.into())
    }

    /// Returns a [AccessTokenCredentials] instance with the configured settings.
    ///
    /// # Example
    ///
    /// ```
    /// # use google_cloud_auth::credentials::user_account::Builder;
    /// # use google_cloud_auth::credentials::{AccessTokenCredentials, AccessTokenCredentialsProvider};
    /// # use serde_json::json;
    /// # tokio_test::block_on(async {
    /// let authorized_user = json!({
    ///     "client_id": "YOUR_CLIENT_ID.apps.googleusercontent.com",
    ///     "client_secret": "YOUR_CLIENT_SECRET",
    ///     "refresh_token": "YOUR_REFRESH_TOKEN",
    ///     "type": "authorized_user",
    /// });
    /// let credentials: AccessTokenCredentials = Builder::new(authorized_user)
    ///     .build_access_token_credentials()?;
    /// // let token = credentials.token().await?;
    /// // println!("Token: {}", token.token);
    /// # Ok::<(), anyhow::Error>(())
    /// # });
    /// ```
    ///
    /// # Errors
    ///
    /// Returns a [CredentialsError] if the `authorized_user`
    /// provided to [`Builder::new`] cannot be successfully deserialized into the
    /// expected format. This typically happens if the JSON value is malformed or
    /// missing required fields. For more information, on how to generate
    /// `authorized_user` json, consult the relevant section in the
    /// [application-default credentials] guide.
    ///
    /// [application-default credentials]: https://cloud.google.com/docs/authentication/application-default-credentials
    pub fn build_access_token_credentials(self) -> BuildResult<AccessTokenCredentials> {
        let authorized_user = serde_json::from_value::<AuthorizedUser>(self.authorized_user)
            .map_err(BuilderError::parsing)?;
        let endpoint = self
            .token_uri
            .or(authorized_user.token_uri)
            .unwrap_or(OAUTH2_TOKEN_SERVER_URL.to_string());
        let quota_project_id = self.quota_project_id.or(authorized_user.quota_project_id);

        let token_provider = UserTokenProvider {
            client_id: authorized_user.client_id,
            client_secret: authorized_user.client_secret,
            refresh_token: authorized_user.refresh_token,
            endpoint,
            scopes: self.scopes.map(|scopes| scopes.join(" ")),
            source: UserTokenSource::AccessToken,
        };

        let token_provider = TokenCache::new(self.retry_builder.build(token_provider));

        Ok(AccessTokenCredentials {
            inner: Arc::new(UserCredentials {
                token_provider,
                quota_project_id,
            }),
        })
    }
}

#[derive(PartialEq)]
pub(crate) struct UserTokenProvider {
    client_id: String,
    client_secret: String,
    refresh_token: String,
    endpoint: String,
    scopes: Option<String>,
    source: UserTokenSource,
}

#[allow(dead_code)]
#[derive(PartialEq)]
enum UserTokenSource {
    IdToken,
    AccessToken,
}

impl std::fmt::Debug for UserTokenProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UserCredentials")
            .field("client_id", &self.client_id)
            .field("client_secret", &"[censored]")
            .field("refresh_token", &"[censored]")
            .field("endpoint", &self.endpoint)
            .field("scopes", &self.scopes)
            .finish()
    }
}

impl UserTokenProvider {
    #[cfg(google_cloud_unstable_id_token)]
    pub(crate) fn new_id_token_provider(
        authorized_user: AuthorizedUser,
        token_uri: Option<String>,
    ) -> UserTokenProvider {
        let endpoint = token_uri
            .or(authorized_user.token_uri)
            .unwrap_or(OAUTH2_TOKEN_SERVER_URL.to_string());
        UserTokenProvider {
            client_id: authorized_user.client_id,
            client_secret: authorized_user.client_secret,
            refresh_token: authorized_user.refresh_token,
            endpoint,
            source: UserTokenSource::IdToken,
            scopes: None,
        }
    }
}

#[async_trait::async_trait]
impl TokenProvider for UserTokenProvider {
    async fn token(&self) -> Result<Token> {
        let client = Client::new();

        // Make the request
        let req = Oauth2RefreshRequest {
            grant_type: RefreshGrantType::RefreshToken,
            client_id: self.client_id.clone(),
            client_secret: self.client_secret.clone(),
            refresh_token: self.refresh_token.clone(),
            scopes: self.scopes.clone(),
        };
        let header = HeaderValue::from_static("application/json");
        let builder = client
            .request(Method::POST, self.endpoint.as_str())
            .header(CONTENT_TYPE, header)
            .json(&req);
        let resp = builder
            .send()
            .await
            .map_err(|e| errors::from_http_error(e, MSG))?;

        // Process the response
        if !resp.status().is_success() {
            let err = errors::from_http_response(resp, MSG).await;
            return Err(err);
        }
        let response = resp.json::<Oauth2RefreshResponse>().await.map_err(|e| {
            let retryable = !e.is_decode();
            CredentialsError::from_source(retryable, e)
        })?;

        let token = match self.source {
            UserTokenSource::AccessToken => Ok(response.access_token),
            UserTokenSource::IdToken => response
                .id_token
                .ok_or_else(|| CredentialsError::from_msg(false, MISSING_ID_TOKEN_MSG)),
        }?;
        let token = Token {
            token,
            token_type: response.token_type,
            expires_at: response
                .expires_in
                .map(|d| Instant::now() + Duration::from_secs(d)),
            metadata: None,
        };
        Ok(token)
    }
}

const MSG: &str = "failed to refresh user access token";
const MISSING_ID_TOKEN_MSG: &str = "UserCredentials can obtain an id token only when authenticated through \
gcloud running 'gcloud auth application-default login`";

/// Data model for a UserCredentials
///
/// See: https://cloud.google.com/docs/authentication#user-accounts
#[derive(Debug)]
pub(crate) struct UserCredentials<T>
where
    T: CachedTokenProvider,
{
    token_provider: T,
    quota_project_id: Option<String>,
}

#[async_trait::async_trait]
impl<T> CredentialsProvider for UserCredentials<T>
where
    T: CachedTokenProvider,
{
    async fn headers(&self, extensions: Extensions) -> Result<CacheableResource<HeaderMap>> {
        let token = self.token_provider.token(extensions).await?;
        build_cacheable_headers(&token, &self.quota_project_id)
    }
}

#[async_trait::async_trait]
impl<T> AccessTokenCredentialsProvider for UserCredentials<T>
where
    T: CachedTokenProvider,
{
    async fn token(&self) -> Result<AccessToken> {
        let token = self.token_provider.token(Extensions::new()).await?;
        token.into()
    }
}

#[derive(Debug, PartialEq, serde::Deserialize)]
pub(crate) struct AuthorizedUser {
    #[serde(rename = "type")]
    cred_type: String,
    client_id: String,
    client_secret: String,
    refresh_token: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    token_uri: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    quota_project_id: Option<String>,
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub(crate) enum RefreshGrantType {
    #[serde(rename = "refresh_token")]
    RefreshToken,
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub(crate) struct Oauth2RefreshRequest {
    pub(crate) grant_type: RefreshGrantType,
    pub(crate) client_id: String,
    pub(crate) client_secret: String,
    pub(crate) refresh_token: String,
    scopes: Option<String>,
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub(crate) struct Oauth2RefreshResponse {
    pub(crate) access_token: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) id_token: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) scope: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) expires_in: Option<u64>,
    pub(crate) token_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) refresh_token: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::credentials::tests::{
        find_source_error, get_headers_from_cache, get_mock_auth_retry_policy,
        get_mock_backoff_policy, get_mock_retry_throttler, get_token_from_headers,
        get_token_type_from_headers,
    };
    use crate::credentials::{DEFAULT_UNIVERSE_DOMAIN, QUOTA_PROJECT_KEY};
    use crate::errors::CredentialsError;
    use crate::token::tests::MockTokenProvider;
    use http::StatusCode;
    use http::header::AUTHORIZATION;
    use httptest::cycle;
    use httptest::matchers::{all_of, json_decoded, request};
    use httptest::responders::{json_encoded, status_code};
    use httptest::{Expectation, Server};

    type TestResult = anyhow::Result<()>;

    fn authorized_user_json(token_uri: String) -> Value {
        serde_json::json!({
            "client_id": "test-client-id",
            "client_secret": "test-client-secret",
            "refresh_token": "test-refresh-token",
            "type": "authorized_user",
            "token_uri": token_uri,
        })
    }

    #[tokio::test]
    async fn test_user_account_retries_on_transient_failures() -> TestResult {
        let mut server = Server::run();
        server.expect(
            Expectation::matching(request::path("/token"))
                .times(3)
                .respond_with(status_code(503)),
        );

        let credentials = Builder::new(authorized_user_json(server.url("/token").to_string()))
            .with_retry_policy(get_mock_auth_retry_policy(3))
            .with_backoff_policy(get_mock_backoff_policy())
            .with_retry_throttler(get_mock_retry_throttler())
            .build()?;

        let err = credentials.headers(Extensions::new()).await.unwrap_err();
        assert!(!err.is_transient());
        server.verify_and_clear();
        Ok(())
    }

    #[tokio::test]
    async fn test_user_account_does_not_retry_on_non_transient_failures() -> TestResult {
        let mut server = Server::run();
        server.expect(
            Expectation::matching(request::path("/token"))
                .times(1)
                .respond_with(status_code(401)),
        );

        let credentials = Builder::new(authorized_user_json(server.url("/token").to_string()))
            .with_retry_policy(get_mock_auth_retry_policy(1))
            .with_backoff_policy(get_mock_backoff_policy())
            .with_retry_throttler(get_mock_retry_throttler())
            .build()?;

        let err = credentials.headers(Extensions::new()).await.unwrap_err();
        assert!(!err.is_transient());
        server.verify_and_clear();
        Ok(())
    }

    #[tokio::test]
    async fn test_user_account_retries_for_success() -> TestResult {
        let mut server = Server::run();
        let response = Oauth2RefreshResponse {
            access_token: "test-access-token".to_string(),
            id_token: None,
            expires_in: Some(3600),
            refresh_token: Some("test-refresh-token".to_string()),
            scope: Some("scope1 scope2".to_string()),
            token_type: "test-token-type".to_string(),
        };

        server.expect(
            Expectation::matching(request::path("/token"))
                .times(3)
                .respond_with(cycle![
                    status_code(503).body("try-again"),
                    status_code(503).body("try-again"),
                    status_code(200)
                        .append_header("Content-Type", "application/json")
                        .body(serde_json::to_string(&response).unwrap()),
                ]),
        );

        let credentials = Builder::new(authorized_user_json(server.url("/token").to_string()))
            .with_retry_policy(get_mock_auth_retry_policy(3))
            .with_backoff_policy(get_mock_backoff_policy())
            .with_retry_throttler(get_mock_retry_throttler())
            .build()?;

        let token = get_token_from_headers(credentials.headers(Extensions::new()).await.unwrap());
        assert_eq!(token.unwrap(), "test-access-token");

        server.verify_and_clear();
        Ok(())
    }

    #[test]
    fn debug_token_provider() {
        let expected = UserTokenProvider {
            client_id: "test-client-id".to_string(),
            client_secret: "test-client-secret".to_string(),
            refresh_token: "test-refresh-token".to_string(),
            endpoint: OAUTH2_TOKEN_SERVER_URL.to_string(),
            scopes: Some("https://www.googleapis.com/auth/pubsub".to_string()),
            source: UserTokenSource::AccessToken,
        };
        let fmt = format!("{expected:?}");
        assert!(fmt.contains("test-client-id"), "{fmt}");
        assert!(!fmt.contains("test-client-secret"), "{fmt}");
        assert!(!fmt.contains("test-refresh-token"), "{fmt}");
        assert!(fmt.contains(OAUTH2_TOKEN_SERVER_URL), "{fmt}");
        assert!(
            fmt.contains("https://www.googleapis.com/auth/pubsub"),
            "{fmt}"
        );
    }

    #[test]
    fn authorized_user_full_from_json_success() {
        let json = serde_json::json!({
            "account": "",
            "client_id": "test-client-id",
            "client_secret": "test-client-secret",
            "refresh_token": "test-refresh-token",
            "type": "authorized_user",
            "universe_domain": "googleapis.com",
            "quota_project_id": "test-project",
            "token_uri" : "test-token-uri",
        });

        let expected = AuthorizedUser {
            cred_type: "authorized_user".to_string(),
            client_id: "test-client-id".to_string(),
            client_secret: "test-client-secret".to_string(),
            refresh_token: "test-refresh-token".to_string(),
            quota_project_id: Some("test-project".to_string()),
            token_uri: Some("test-token-uri".to_string()),
        };
        let actual = serde_json::from_value::<AuthorizedUser>(json).unwrap();
        assert_eq!(actual, expected);
    }

    #[test]
    fn authorized_user_partial_from_json_success() {
        let json = serde_json::json!({
            "client_id": "test-client-id",
            "client_secret": "test-client-secret",
            "refresh_token": "test-refresh-token",
            "type": "authorized_user",
        });

        let expected = AuthorizedUser {
            cred_type: "authorized_user".to_string(),
            client_id: "test-client-id".to_string(),
            client_secret: "test-client-secret".to_string(),
            refresh_token: "test-refresh-token".to_string(),
            quota_project_id: None,
            token_uri: None,
        };
        let actual = serde_json::from_value::<AuthorizedUser>(json).unwrap();
        assert_eq!(actual, expected);
    }

    #[test]
    fn authorized_user_from_json_parse_fail() {
        let json_full = serde_json::json!({
            "client_id": "test-client-id",
            "client_secret": "test-client-secret",
            "refresh_token": "test-refresh-token",
            "type": "authorized_user",
            "quota_project_id": "test-project"
        });

        for required_field in ["client_id", "client_secret", "refresh_token"] {
            let mut json = json_full.clone();
            // Remove a required field from the JSON
            json[required_field].take();
            serde_json::from_value::<AuthorizedUser>(json)
                .err()
                .unwrap();
        }
    }

    #[tokio::test]
    async fn default_universe_domain_success() {
        let mock = TokenCache::new(MockTokenProvider::new());

        let uc = UserCredentials {
            token_provider: mock,
            quota_project_id: None,
        };
        assert_eq!(uc.universe_domain().await.unwrap(), DEFAULT_UNIVERSE_DOMAIN);
    }

    #[tokio::test]
    async fn headers_success() -> TestResult {
        let token = Token {
            token: "test-token".to_string(),
            token_type: "Bearer".to_string(),
            expires_at: None,
            metadata: None,
        };

        let mut mock = MockTokenProvider::new();
        mock.expect_token().times(1).return_once(|| Ok(token));

        let uc = UserCredentials {
            token_provider: TokenCache::new(mock),
            quota_project_id: None,
        };

        let mut extensions = Extensions::new();
        let cached_headers = uc.headers(extensions.clone()).await.unwrap();
        let (headers, entity_tag) = match cached_headers {
            CacheableResource::New { entity_tag, data } => (data, entity_tag),
            CacheableResource::NotModified => unreachable!("expecting new headers"),
        };
        let token = headers.get(AUTHORIZATION).unwrap();

        assert_eq!(headers.len(), 1, "{headers:?}");
        assert_eq!(token, HeaderValue::from_static("Bearer test-token"));
        assert!(token.is_sensitive());

        extensions.insert(entity_tag);

        let cached_headers = uc.headers(extensions).await?;

        match cached_headers {
            CacheableResource::New { .. } => unreachable!("expecting new headers"),
            CacheableResource::NotModified => CacheableResource::<HeaderMap>::NotModified,
        };
        Ok(())
    }

    #[tokio::test]
    async fn headers_failure() {
        let mut mock = MockTokenProvider::new();
        mock.expect_token()
            .times(1)
            .return_once(|| Err(errors::non_retryable_from_str("fail")));

        let uc = UserCredentials {
            token_provider: TokenCache::new(mock),
            quota_project_id: None,
        };
        assert!(uc.headers(Extensions::new()).await.is_err());
    }

    #[tokio::test]
    async fn headers_with_quota_project_success() -> TestResult {
        let token = Token {
            token: "test-token".to_string(),
            token_type: "Bearer".to_string(),
            expires_at: None,
            metadata: None,
        };

        let mut mock = MockTokenProvider::new();
        mock.expect_token().times(1).return_once(|| Ok(token));

        let uc = UserCredentials {
            token_provider: TokenCache::new(mock),
            quota_project_id: Some("test-project".to_string()),
        };

        let headers = get_headers_from_cache(uc.headers(Extensions::new()).await.unwrap())?;
        let token = headers.get(AUTHORIZATION).unwrap();
        let quota_project_header = headers.get(QUOTA_PROJECT_KEY).unwrap();

        assert_eq!(headers.len(), 2, "{headers:?}");
        assert_eq!(token, HeaderValue::from_static("Bearer test-token"));
        assert!(token.is_sensitive());
        assert_eq!(
            quota_project_header,
            HeaderValue::from_static("test-project")
        );
        assert!(!quota_project_header.is_sensitive());
        Ok(())
    }

    #[test]
    fn oauth2_request_serde() {
        let request = Oauth2RefreshRequest {
            grant_type: RefreshGrantType::RefreshToken,
            client_id: "test-client-id".to_string(),
            client_secret: "test-client-secret".to_string(),
            refresh_token: "test-refresh-token".to_string(),
            scopes: Some("scope1 scope2".to_string()),
        };

        let json = serde_json::to_value(&request).unwrap();
        let expected = serde_json::json!({
            "grant_type": "refresh_token",
            "client_id": "test-client-id",
            "client_secret": "test-client-secret",
            "refresh_token": "test-refresh-token",
            "scopes": "scope1 scope2",
        });
        assert_eq!(json, expected);
        let roundtrip = serde_json::from_value::<Oauth2RefreshRequest>(json).unwrap();
        assert_eq!(request, roundtrip);
    }

    #[test]
    fn oauth2_response_serde_full() {
        let response = Oauth2RefreshResponse {
            access_token: "test-access-token".to_string(),
            id_token: None,
            scope: Some("scope1 scope2".to_string()),
            expires_in: Some(3600),
            token_type: "test-token-type".to_string(),
            refresh_token: Some("test-refresh-token".to_string()),
        };

        let json = serde_json::to_value(&response).unwrap();
        let expected = serde_json::json!({
            "access_token": "test-access-token",
            "scope": "scope1 scope2",
            "expires_in": 3600,
            "token_type": "test-token-type",
            "refresh_token": "test-refresh-token"
        });
        assert_eq!(json, expected);
        let roundtrip = serde_json::from_value::<Oauth2RefreshResponse>(json).unwrap();
        assert_eq!(response, roundtrip);
    }

    #[test]
    fn oauth2_response_serde_partial() {
        let response = Oauth2RefreshResponse {
            access_token: "test-access-token".to_string(),
            id_token: None,
            scope: None,
            expires_in: None,
            token_type: "test-token-type".to_string(),
            refresh_token: None,
        };

        let json = serde_json::to_value(&response).unwrap();
        let expected = serde_json::json!({
            "access_token": "test-access-token",
            "token_type": "test-token-type",
        });
        assert_eq!(json, expected);
        let roundtrip = serde_json::from_value::<Oauth2RefreshResponse>(json).unwrap();
        assert_eq!(response, roundtrip);
    }

    fn check_request(request: &Oauth2RefreshRequest, expected_scopes: Option<String>) -> bool {
        request.client_id == "test-client-id"
            && request.client_secret == "test-client-secret"
            && request.refresh_token == "test-refresh-token"
            && request.grant_type == RefreshGrantType::RefreshToken
            && request.scopes == expected_scopes
    }

    #[tokio::test(start_paused = true)]
    async fn token_provider_full() -> TestResult {
        let server = Server::run();
        let response = Oauth2RefreshResponse {
            access_token: "test-access-token".to_string(),
            id_token: None,
            expires_in: Some(3600),
            refresh_token: Some("test-refresh-token".to_string()),
            scope: Some("scope1 scope2".to_string()),
            token_type: "test-token-type".to_string(),
        };
        server.expect(
            Expectation::matching(all_of![
                request::path("/token"),
                request::body(json_decoded(|req: &Oauth2RefreshRequest| {
                    check_request(req, Some("scope1 scope2".to_string()))
                }))
            ])
            .respond_with(json_encoded(response)),
        );

        let tp = UserTokenProvider {
            client_id: "test-client-id".to_string(),
            client_secret: "test-client-secret".to_string(),
            refresh_token: "test-refresh-token".to_string(),
            endpoint: server.url("/token").to_string(),
            scopes: Some("scope1 scope2".to_string()),
            source: UserTokenSource::AccessToken,
        };
        let now = Instant::now();
        let token = tp.token().await?;
        assert_eq!(token.token, "test-access-token");
        assert_eq!(token.token_type, "test-token-type");
        assert!(
            token
                .expires_at
                .is_some_and(|d| d == now + Duration::from_secs(3600)),
            "now: {:?}, expires_at: {:?}",
            now,
            token.expires_at
        );

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn credential_full_with_quota_project() -> TestResult {
        let server = Server::run();
        let response = Oauth2RefreshResponse {
            access_token: "test-access-token".to_string(),
            id_token: None,
            expires_in: Some(3600),
            refresh_token: Some("test-refresh-token".to_string()),
            scope: None,
            token_type: "test-token-type".to_string(),
        };
        server.expect(
            Expectation::matching(all_of![
                request::path("/token"),
                request::body(json_decoded(|req: &Oauth2RefreshRequest| {
                    check_request(req, None)
                }))
            ])
            .respond_with(json_encoded(response)),
        );

        let authorized_user = serde_json::json!({
            "client_id": "test-client-id",
            "client_secret": "test-client-secret",
            "refresh_token": "test-refresh-token",
            "type": "authorized_user",
            "token_uri": server.url("/token").to_string(),
        });
        let cred = Builder::new(authorized_user)
            .with_quota_project_id("test-project")
            .build()?;

        let headers = get_headers_from_cache(cred.headers(Extensions::new()).await.unwrap())?;
        let token = headers.get(AUTHORIZATION).unwrap();
        let quota_project_header = headers.get(QUOTA_PROJECT_KEY).unwrap();

        assert_eq!(headers.len(), 2, "{headers:?}");
        assert_eq!(
            token,
            HeaderValue::from_static("test-token-type test-access-token")
        );
        assert!(token.is_sensitive());
        assert_eq!(
            quota_project_header,
            HeaderValue::from_static("test-project")
        );
        assert!(!quota_project_header.is_sensitive());

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn creds_from_json_custom_uri_with_caching() -> TestResult {
        let mut server = Server::run();
        let response = Oauth2RefreshResponse {
            access_token: "test-access-token".to_string(),
            id_token: None,
            expires_in: Some(3600),
            refresh_token: Some("test-refresh-token".to_string()),
            scope: Some("scope1 scope2".to_string()),
            token_type: "test-token-type".to_string(),
        };
        server.expect(
            Expectation::matching(all_of![
                request::path("/token"),
                request::body(json_decoded(|req: &Oauth2RefreshRequest| {
                    check_request(req, Some("scope1 scope2".to_string()))
                }))
            ])
            .times(1)
            .respond_with(json_encoded(response)),
        );

        let json = serde_json::json!({
            "client_id": "test-client-id",
            "client_secret": "test-client-secret",
            "refresh_token": "test-refresh-token",
            "type": "authorized_user",
            "universe_domain": "googleapis.com",
            "quota_project_id": "test-project",
            "token_uri": server.url("/token").to_string(),
        });

        let cred = Builder::new(json)
            .with_scopes(vec!["scope1", "scope2"])
            .build()?;

        let token = get_token_from_headers(cred.headers(Extensions::new()).await?);
        assert_eq!(token.unwrap(), "test-access-token");

        let token = get_token_from_headers(cred.headers(Extensions::new()).await?);
        assert_eq!(token.unwrap(), "test-access-token");

        server.verify_and_clear();

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn credential_provider_partial() -> TestResult {
        let server = Server::run();
        let response = Oauth2RefreshResponse {
            access_token: "test-access-token".to_string(),
            id_token: None,
            expires_in: None,
            refresh_token: None,
            scope: None,
            token_type: "test-token-type".to_string(),
        };
        server.expect(
            Expectation::matching(all_of![
                request::path("/token"),
                request::body(json_decoded(|req: &Oauth2RefreshRequest| {
                    check_request(req, None)
                }))
            ])
            .respond_with(json_encoded(response)),
        );

        let authorized_user = serde_json::json!({
            "client_id": "test-client-id",
            "client_secret": "test-client-secret",
            "refresh_token": "test-refresh-token",
            "type": "authorized_user",
            "token_uri": server.url("/token").to_string()
        });

        let uc = Builder::new(authorized_user).build()?;
        let headers = uc.headers(Extensions::new()).await?;
        assert_eq!(
            get_token_from_headers(headers.clone()).unwrap(),
            "test-access-token"
        );
        assert_eq!(
            get_token_type_from_headers(headers).unwrap(),
            "test-token-type"
        );

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn credential_provider_with_token_uri() -> TestResult {
        let server = Server::run();
        let response = Oauth2RefreshResponse {
            access_token: "test-access-token".to_string(),
            id_token: None,
            expires_in: None,
            refresh_token: None,
            scope: None,
            token_type: "test-token-type".to_string(),
        };
        server.expect(
            Expectation::matching(all_of![
                request::path("/token"),
                request::body(json_decoded(|req: &Oauth2RefreshRequest| {
                    check_request(req, None)
                }))
            ])
            .respond_with(json_encoded(response)),
        );

        let authorized_user = serde_json::json!({
            "client_id": "test-client-id",
            "client_secret": "test-client-secret",
            "refresh_token": "test-refresh-token",
            "type": "authorized_user",
            "token_uri": "test-endpoint"
        });

        let uc = Builder::new(authorized_user)
            .with_token_uri(server.url("/token").to_string())
            .build()?;
        let headers = uc.headers(Extensions::new()).await?;
        assert_eq!(
            get_token_from_headers(headers.clone()).unwrap(),
            "test-access-token"
        );
        assert_eq!(
            get_token_type_from_headers(headers).unwrap(),
            "test-token-type"
        );

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn credential_provider_with_scopes() -> TestResult {
        let server = Server::run();
        let response = Oauth2RefreshResponse {
            access_token: "test-access-token".to_string(),
            id_token: None,
            expires_in: None,
            refresh_token: None,
            scope: Some("scope1 scope2".to_string()),
            token_type: "test-token-type".to_string(),
        };
        server.expect(
            Expectation::matching(all_of![
                request::path("/token"),
                request::body(json_decoded(|req: &Oauth2RefreshRequest| {
                    check_request(req, Some("scope1 scope2".to_string()))
                }))
            ])
            .respond_with(json_encoded(response)),
        );

        let authorized_user = serde_json::json!({
            "client_id": "test-client-id",
            "client_secret": "test-client-secret",
            "refresh_token": "test-refresh-token",
            "type": "authorized_user",
            "token_uri": "test-endpoint"
        });

        let uc = Builder::new(authorized_user)
            .with_token_uri(server.url("/token").to_string())
            .with_scopes(vec!["scope1", "scope2"])
            .build()?;
        let headers = uc.headers(Extensions::new()).await?;
        assert_eq!(
            get_token_from_headers(headers.clone()).unwrap(),
            "test-access-token"
        );
        assert_eq!(
            get_token_type_from_headers(headers).unwrap(),
            "test-token-type"
        );

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn credential_provider_retryable_error() -> TestResult {
        let server = Server::run();
        server
            .expect(Expectation::matching(request::path("/token")).respond_with(status_code(503)));

        let authorized_user = serde_json::json!({
            "client_id": "test-client-id",
            "client_secret": "test-client-secret",
            "refresh_token": "test-refresh-token",
            "type": "authorized_user",
            "token_uri": server.url("/token").to_string()
        });

        let uc = Builder::new(authorized_user).build()?;
        let err = uc.headers(Extensions::new()).await.unwrap_err();
        let original_err = find_source_error::<CredentialsError>(&err).unwrap();
        assert!(original_err.is_transient());

        let source = find_source_error::<reqwest::Error>(&err);
        assert!(
            matches!(source, Some(e) if e.status() == Some(StatusCode::SERVICE_UNAVAILABLE)),
            "{err:?}"
        );

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn token_provider_nonretryable_error() -> TestResult {
        let server = Server::run();
        server
            .expect(Expectation::matching(request::path("/token")).respond_with(status_code(401)));

        let authorized_user = serde_json::json!({
            "client_id": "test-client-id",
            "client_secret": "test-client-secret",
            "refresh_token": "test-refresh-token",
            "type": "authorized_user",
            "token_uri": server.url("/token").to_string()
        });

        let uc = Builder::new(authorized_user).build()?;
        let err = uc.headers(Extensions::new()).await.unwrap_err();
        let original_err = find_source_error::<CredentialsError>(&err).unwrap();
        assert!(!original_err.is_transient());

        let source = find_source_error::<reqwest::Error>(&err);
        assert!(
            matches!(source, Some(e) if e.status() == Some(StatusCode::UNAUTHORIZED)),
            "{err:?}"
        );

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn token_provider_malformed_response_is_nonretryable() -> TestResult {
        let server = Server::run();
        server.expect(
            Expectation::matching(request::path("/token"))
                .respond_with(json_encoded("bad json".to_string())),
        );

        let authorized_user = serde_json::json!({
            "client_id": "test-client-id",
            "client_secret": "test-client-secret",
            "refresh_token": "test-refresh-token",
            "type": "authorized_user",
            "token_uri": server.url("/token").to_string()
        });

        let uc = Builder::new(authorized_user).build()?;
        let e = uc.headers(Extensions::new()).await.err().unwrap();
        assert!(!e.is_transient(), "{e}");

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn builder_malformed_authorized_json_nonretryable() -> TestResult {
        let authorized_user = serde_json::json!({
            "client_secret": "test-client-secret",
            "refresh_token": "test-refresh-token",
            "type": "authorized_user",
        });

        let e = Builder::new(authorized_user).build().unwrap_err();
        assert!(e.is_parsing(), "{e}");

        Ok(())
    }
}
