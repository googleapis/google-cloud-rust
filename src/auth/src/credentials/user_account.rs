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
//!
//! Example usage:
//!
//! ```
//! # use google_cloud_auth::credentials::user_account::Builder;
//! # use google_cloud_auth::credentials::Credentials;
//! # use google_cloud_auth::errors::CredentialsError;
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
//! let token = credentials.token().await?;
//! println!("Token: {}", token.token);
//! # Ok::<(), CredentialsError>(())
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

use crate::credentials::dynamic::CredentialsProvider;
use crate::credentials::{Credentials, Result};
use crate::errors::{self, CredentialsError, is_retryable};
use crate::headers_util::build_bearer_headers;
use crate::token::{Token, TokenProvider};
use crate::token_cache::TokenCache;
use http::header::{CONTENT_TYPE, HeaderName, HeaderValue};
use reqwest::{Client, Method};
use serde_json::Value;
use std::sync::Arc;
use std::time::Duration;

const OAUTH2_ENDPOINT: &str = "https://oauth2.googleapis.com/token";

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
    pub fn build(self) -> Result<Credentials> {
        let authorized_user = serde_json::from_value::<AuthorizedUser>(self.authorized_user)
            .map_err(errors::non_retryable)?;
        let endpoint = self
            .token_uri
            .or(authorized_user.token_uri)
            .unwrap_or(OAUTH2_ENDPOINT.to_string());
        let quota_project_id = self.quota_project_id.or(authorized_user.quota_project_id);

        let token_provider = UserTokenProvider {
            client_id: authorized_user.client_id,
            client_secret: authorized_user.client_secret,
            refresh_token: authorized_user.refresh_token,
            endpoint,
            scopes: self.scopes.map(|scopes| scopes.join(" ")),
        };
        let token_provider = TokenCache::new(token_provider);

        Ok(Credentials {
            inner: Arc::new(UserCredentials {
                token_provider,
                quota_project_id,
            }),
        })
    }
}

#[derive(PartialEq)]
struct UserTokenProvider {
    client_id: String,
    client_secret: String,
    refresh_token: String,
    endpoint: String,
    scopes: Option<String>,
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
        let resp = builder.send().await.map_err(errors::retryable)?;

        // Process the response
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp
                .text()
                .await
                .map_err(|e| CredentialsError::new(is_retryable(status), e))?;
            return Err(CredentialsError::from_str(
                is_retryable(status),
                format!("Failed to fetch token. {body}"),
            ));
        }
        let response = resp.json::<Oauth2RefreshResponse>().await.map_err(|e| {
            let retryable = !e.is_decode();
            CredentialsError::new(retryable, e)
        })?;
        let token = Token {
            token: response.access_token,
            token_type: response.token_type,
            expires_at: response
                .expires_in
                .map(|d| std::time::Instant::now() + Duration::from_secs(d)),
            metadata: None,
        };
        Ok(token)
    }
}

/// Data model for a UserCredentials
///
/// See: https://cloud.google.com/docs/authentication#user-accounts
#[derive(Debug)]
pub(crate) struct UserCredentials<T>
where
    T: TokenProvider,
{
    token_provider: T,
    quota_project_id: Option<String>,
}

#[async_trait::async_trait]
impl<T> CredentialsProvider for UserCredentials<T>
where
    T: TokenProvider,
{
    async fn token(&self) -> Result<Token> {
        self.token_provider.token().await
    }

    async fn headers(&self) -> Result<Vec<(HeaderName, HeaderValue)>> {
        let token = self.token().await?;
        build_bearer_headers(&token, &self.quota_project_id)
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
enum RefreshGrantType {
    #[serde(rename = "refresh_token")]
    RefreshToken,
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
struct Oauth2RefreshRequest {
    grant_type: RefreshGrantType,
    client_id: String,
    client_secret: String,
    refresh_token: String,
    scopes: Option<String>,
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
struct Oauth2RefreshResponse {
    access_token: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    scope: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    expires_in: Option<u64>,
    token_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    refresh_token: Option<String>,
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::credentials::QUOTA_PROJECT_KEY;
    use crate::credentials::test::HV;
    use crate::token::test::MockTokenProvider;
    use axum::extract::Json;
    use http::StatusCode;
    use http::header::AUTHORIZATION;
    use std::error::Error;
    use std::sync::Mutex;
    use tokio::task::JoinHandle;

    type TestResult = std::result::Result<(), Box<dyn std::error::Error>>;

    #[test]
    fn debug_token_provider() {
        let expected = UserTokenProvider {
            client_id: "test-client-id".to_string(),
            client_secret: "test-client-secret".to_string(),
            refresh_token: "test-refresh-token".to_string(),
            endpoint: OAUTH2_ENDPOINT.to_string(),
            scopes: Some("https://www.googleapis.com/auth/pubsub".to_string()),
        };
        let fmt = format!("{expected:?}");
        assert!(fmt.contains("test-client-id"), "{fmt}");
        assert!(!fmt.contains("test-client-secret"), "{fmt}");
        assert!(!fmt.contains("test-refresh-token"), "{fmt}");
        assert!(fmt.contains(OAUTH2_ENDPOINT), "{fmt}");
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
    async fn token_success() {
        let expected = Token {
            token: "test-token".to_string(),
            token_type: "Bearer".to_string(),
            expires_at: None,
            metadata: None,
        };
        let expected_clone = expected.clone();

        let mut mock = MockTokenProvider::new();
        mock.expect_token()
            .times(1)
            .return_once(|| Ok(expected_clone));

        let uc = UserCredentials {
            token_provider: mock,
            quota_project_id: None,
        };
        let actual = uc.token().await.unwrap();
        assert_eq!(actual, expected);
    }

    #[tokio::test]
    async fn token_failure() {
        let mut mock = MockTokenProvider::new();
        mock.expect_token()
            .times(1)
            .return_once(|| Err(errors::non_retryable_from_str("fail")));

        let uc = UserCredentials {
            token_provider: mock,
            quota_project_id: None,
        };
        assert!(uc.token().await.is_err());
    }

    #[tokio::test]
    async fn headers_success() {
        let token = Token {
            token: "test-token".to_string(),
            token_type: "Bearer".to_string(),
            expires_at: None,
            metadata: None,
        };

        let mut mock = MockTokenProvider::new();
        mock.expect_token().times(1).return_once(|| Ok(token));

        let uc = UserCredentials {
            token_provider: mock,
            quota_project_id: None,
        };
        let headers: Vec<HV> = HV::from(uc.headers().await.unwrap());

        assert_eq!(
            headers,
            vec![HV {
                header: AUTHORIZATION.to_string(),
                value: "Bearer test-token".to_string(),
                is_sensitive: true,
            }]
        );
    }

    #[tokio::test]
    async fn headers_failure() {
        let mut mock = MockTokenProvider::new();
        mock.expect_token()
            .times(1)
            .return_once(|| Err(errors::non_retryable_from_str("fail")));

        let uc = UserCredentials {
            token_provider: mock,
            quota_project_id: None,
        };
        assert!(uc.headers().await.is_err());
    }

    #[tokio::test]
    async fn headers_with_quota_project_success() {
        let token = Token {
            token: "test-token".to_string(),
            token_type: "Bearer".to_string(),
            expires_at: None,
            metadata: None,
        };

        let mut mock = MockTokenProvider::new();
        mock.expect_token().times(1).return_once(|| Ok(token));

        let uc = UserCredentials {
            token_provider: mock,
            quota_project_id: Some("test-project".to_string()),
        };
        let headers: Vec<HV> = HV::from(uc.headers().await.unwrap());
        assert_eq!(
            headers,
            vec![
                HV {
                    header: AUTHORIZATION.to_string(),
                    value: "Bearer test-token".to_string(),
                    is_sensitive: true,
                },
                HV {
                    header: QUOTA_PROJECT_KEY.to_string(),
                    value: "test-project".to_string(),
                    is_sensitive: false,
                }
            ]
        );
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

    // Starts a server running locally. Returns an (endpoint, handler) pair.
    async fn start(
        response_code: StatusCode,
        response_body: Value,
        call_count: Arc<Mutex<i32>>,
    ) -> (String, JoinHandle<()>) {
        let code = response_code;
        let body = response_body.clone();
        let handler = move |req| async move { handle_token_factory(code, body, call_count)(req) };
        let app = axum::Router::new().route("/token", axum::routing::post(handler));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async {
            axum::serve(listener, app).await.unwrap();
        });

        (
            format!("http://{}:{}/token", addr.ip(), addr.port()),
            server,
        )
    }

    // Creates a handler that
    // - verifies fields in an Oauth2RefreshRequest
    // - returns a pre-canned HTTP response
    fn handle_token_factory(
        response_code: StatusCode,
        response_body: Value,
        call_count: Arc<std::sync::Mutex<i32>>,
    ) -> impl Fn(Json<Oauth2RefreshRequest>) -> (StatusCode, String) {
        move |request: Json<Oauth2RefreshRequest>| -> (StatusCode, String) {
            let mut count = call_count.lock().unwrap();
            *count += 1;
            assert_eq!(request.client_id, "test-client-id");
            assert_eq!(request.client_secret, "test-client-secret");
            assert_eq!(request.refresh_token, "test-refresh-token");
            assert_eq!(request.grant_type, RefreshGrantType::RefreshToken);
            assert_eq!(
                request.scopes,
                response_body["scope"].as_str().map(|s| s.to_owned())
            );

            (response_code, response_body.to_string())
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn token_provider_full() -> TestResult {
        let response = Oauth2RefreshResponse {
            access_token: "test-access-token".to_string(),
            expires_in: Some(3600),
            refresh_token: Some("test-refresh-token".to_string()),
            scope: Some("scope1 scope2".to_string()),
            token_type: "test-token-type".to_string(),
        };
        let response_body = serde_json::to_value(&response).unwrap();
        let (endpoint, _server) =
            start(StatusCode::OK, response_body, Arc::new(Mutex::new(0))).await;
        println!("endpoint = {endpoint}");

        let authorized_user = serde_json::json!({
            "client_id": "test-client-id",
            "client_secret": "test-client-secret",
            "refresh_token": "test-refresh-token",
            "type": "authorized_user",
            "token_uri": endpoint,
        });
        let cred = Builder::new(authorized_user)
            .with_scopes(vec!["scope1", "scope2"])
            .build()?;

        let now = std::time::Instant::now();
        let token = cred.token().await?;
        assert_eq!(token.token, "test-access-token");
        assert_eq!(token.token_type, "test-token-type");
        assert!(
            token
                .expires_at
                .is_some_and(|d| d >= now + Duration::from_secs(3600)),
            "now: {:?}, expires_at: {:?}",
            now,
            token.expires_at
        );

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn token_provider_full_with_quota_project() -> TestResult {
        let response = Oauth2RefreshResponse {
            access_token: "test-access-token".to_string(),
            expires_in: Some(3600),
            refresh_token: Some("test-refresh-token".to_string()),
            scope: None,
            token_type: "test-token-type".to_string(),
        };
        let response_body = serde_json::to_value(&response).unwrap();
        let (endpoint, _server) =
            start(StatusCode::OK, response_body, Arc::new(Mutex::new(0))).await;
        println!("endpoint = {endpoint}");

        let authorized_user = serde_json::json!({
            "client_id": "test-client-id",
            "client_secret": "test-client-secret",
            "refresh_token": "test-refresh-token",
            "type": "authorized_user",
            "token_uri": endpoint,
        });
        let cred = Builder::new(authorized_user)
            .with_quota_project_id("test-project")
            .build()?;

        let headers: Vec<HV> = HV::from(cred.headers().await.unwrap());
        assert_eq!(
            headers,
            vec![
                HV {
                    header: AUTHORIZATION.to_string(),
                    value: "test-token-type test-access-token".to_string(),
                    is_sensitive: true,
                },
                HV {
                    header: QUOTA_PROJECT_KEY.to_string(),
                    value: "test-project".to_string(),
                    is_sensitive: false,
                }
            ]
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn creds_from_json_custom_uri_with_caching() -> TestResult {
        let response = Oauth2RefreshResponse {
            access_token: "test-access-token".to_string(),
            expires_in: Some(3600),
            refresh_token: Some("test-refresh-token".to_string()),
            scope: Some("scope1 scope2".to_string()),
            token_type: "test-token-type".to_string(),
        };
        let response_body = serde_json::to_value(&response).unwrap();
        let call_count = Arc::new(Mutex::new(0));
        let (endpoint, _server) = start(StatusCode::OK, response_body, call_count.clone()).await;
        println!("endpoint = {endpoint}");

        let json = serde_json::json!({
            "client_id": "test-client-id",
            "client_secret": "test-client-secret",
            "refresh_token": "test-refresh-token",
            "type": "authorized_user",
            "universe_domain": "googleapis.com",
            "quota_project_id": "test-project",
            "token_uri": endpoint,
        });

        let cred = Builder::new(json)
            .with_scopes(vec!["scope1", "scope2"])
            .build()?;

        let token = cred.token().await?;
        assert_eq!(token.token, "test-access-token");

        let token = cred.token().await?;
        assert_eq!(token.token, "test-access-token");

        // Test that the inner token provider was called only
        // once even though token was called twice.
        assert_eq!(*call_count.lock().unwrap(), 1);

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn token_provider_partial() -> TestResult {
        let response = Oauth2RefreshResponse {
            access_token: "test-access-token".to_string(),
            expires_in: None,
            refresh_token: None,
            scope: None,
            token_type: "test-token-type".to_string(),
        };
        let response_body = serde_json::to_value(&response).unwrap();
        let (endpoint, _server) =
            start(StatusCode::OK, response_body, Arc::new(Mutex::new(0))).await;
        println!("endpoint = {endpoint}");

        let authorized_user = serde_json::json!({
            "client_id": "test-client-id",
            "client_secret": "test-client-secret",
            "refresh_token": "test-refresh-token",
            "type": "authorized_user",
            "token_uri": endpoint});

        let uc = Builder::new(authorized_user).build()?;
        let token = uc.token().await?;
        assert_eq!(token.token, "test-access-token");
        assert_eq!(token.token_type, "test-token-type");
        assert_eq!(token.expires_at, None);

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn token_provider_with_token_uri() -> TestResult {
        let response = Oauth2RefreshResponse {
            access_token: "test-access-token".to_string(),
            expires_in: None,
            refresh_token: None,
            scope: None,
            token_type: "test-token-type".to_string(),
        };
        let response_body = serde_json::to_value(&response).unwrap();
        let (endpoint, _server) =
            start(StatusCode::OK, response_body, Arc::new(Mutex::new(0))).await;
        println!("endpoint = {endpoint}");

        let authorized_user = serde_json::json!({
            "client_id": "test-client-id",
            "client_secret": "test-client-secret",
            "refresh_token": "test-refresh-token",
            "type": "authorized_user",
            "token_uri": "test-endpoint"});

        let uc = Builder::new(authorized_user)
            .with_token_uri(endpoint)
            .build()?;
        let token = uc.token().await?;
        assert_eq!(token.token, "test-access-token");
        assert_eq!(token.token_type, "test-token-type");
        assert_eq!(token.expires_at, None);

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn token_provider_with_scopes() -> TestResult {
        let response = Oauth2RefreshResponse {
            access_token: "test-access-token".to_string(),
            expires_in: None,
            refresh_token: None,
            scope: Some("scope1 scope2".to_string()),
            token_type: "test-token-type".to_string(),
        };
        let response_body = serde_json::to_value(&response).unwrap();
        let (endpoint, _server) =
            start(StatusCode::OK, response_body, Arc::new(Mutex::new(0))).await;
        println!("endpoint = {endpoint}");

        let authorized_user = serde_json::json!({
            "client_id": "test-client-id",
            "client_secret": "test-client-secret",
            "refresh_token": "test-refresh-token",
            "type": "authorized_user",
            "token_uri": "test-endpoint"});

        let uc = Builder::new(authorized_user)
            .with_token_uri(endpoint)
            .with_scopes(vec!["scope1", "scope2"])
            .build()?;
        let token = uc.token().await?;
        assert_eq!(token.token, "test-access-token");
        assert_eq!(token.token_type, "test-token-type");
        assert_eq!(token.expires_at, None);

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn token_provider_retryable_error() -> TestResult {
        let (endpoint, _server) = start(
            StatusCode::SERVICE_UNAVAILABLE,
            serde_json::to_value("try again".to_string())?,
            Arc::new(Mutex::new(0)),
        )
        .await;
        println!("endpoint = {endpoint}");

        let authorized_user = serde_json::json!({
            "client_id": "test-client-id",
            "client_secret": "test-client-secret",
            "refresh_token": "test-refresh-token",
            "type": "authorized_user",
            "token_uri": endpoint});

        let uc = Builder::new(authorized_user).build()?;
        let e = uc.token().await.err().unwrap();
        assert!(e.is_retryable(), "{e}");
        assert!(e.source().unwrap().to_string().contains("try again"), "{e}");

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn token_provider_nonretryable_error() -> TestResult {
        let (endpoint, _server) = start(
            StatusCode::UNAUTHORIZED,
            serde_json::to_value("epic fail".to_string())?,
            Arc::new(Mutex::new(0)),
        )
        .await;
        println!("endpoint = {endpoint}");

        let authorized_user = serde_json::json!({
            "client_id": "test-client-id",
            "client_secret": "test-client-secret",
            "refresh_token": "test-refresh-token",
            "type": "authorized_user",
            "token_uri": endpoint});

        let uc = Builder::new(authorized_user).build()?;
        let e = uc.token().await.err().unwrap();
        assert!(!e.is_retryable(), "{e}");
        assert!(e.source().unwrap().to_string().contains("epic fail"), "{e}");

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn token_provider_malformed_response_is_nonretryable() -> TestResult {
        let (endpoint, _server) = start(
            StatusCode::OK,
            serde_json::to_value("bad json".to_string())?,
            Arc::new(Mutex::new(0)),
        )
        .await;
        println!("endpoint = {endpoint}");

        let authorized_user = serde_json::json!({
            "client_id": "test-client-id",
            "client_secret": "test-client-secret",
            "refresh_token": "test-refresh-token",
            "type": "authorized_user",
            "token_uri": endpoint});

        let uc = Builder::new(authorized_user).build()?;
        let e = uc.token().await.err().unwrap();
        assert!(!e.is_retryable(), "{e}");

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
        assert!(!e.is_retryable(), "{e}");

        Ok(())
    }

    pub enum CacheableResource<T> {
        NotModified,
        New {entity_tag: String, data: T},
    }
    
    struct Credential;

    impl Credential {
        pub fn header(&self, tag: Option<String>) -> CacheableResource<String> {
            match tag == "old_value" {
            true => CacheableResource::NotModified,
            false => CacheableResource::New(entity_tag: "old_value", data: "headers")
        }
    }
}
