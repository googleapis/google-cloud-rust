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

//! Credentials for authenticating with [ID tokens] from an [user account].
//!
//! This module provides a builder for `IDTokenCredentials` from
//! authorized user credentials, which are typically obtained by running
//! `gcloud auth application-default login`.
//!
//! These credentials are commonly used for [service to service authentication].
//! For example, when services are hosted in Cloud Run or mediated by Identity-Aware Proxy (IAP).
//! ID tokens are only used to verify the identity of a principal. Google Cloud APIs do not use ID tokens
//! for authorization, and therefore cannot be used to access Google Cloud APIs.
//!
//! ## Example: Creating user account sourced credentials from a JSON object with target audience and sending ID Tokens.
//!
//! ```
//! # use google_cloud_auth::credentials::idtoken;
//! # use reqwest;
//! # tokio_test::block_on(async {
//! let authorized_user = serde_json::json!({
//!     "client_id": "YOUR_CLIENT_ID.apps.googleusercontent.com", // Replace with your actual Client ID
//!     "client_secret": "YOUR_CLIENT_SECRET", // Replace with your actual Client Secret - LOAD SECURELY!
//!     "refresh_token": "YOUR_REFRESH_TOKEN", // Replace with the user's refresh token - LOAD SECURELY!
//!     "type": "authorized_user",
//! });
//! let credentials = idtoken::user_account::Builder::new(authorized_user).build()?;
//! let id_token = credentials.id_token().await?;
//!
//! // Make request with ID Token as Bearer Token.
//! let client = reqwest::Client::new();
//! let target_url = format!("{audience}/api/method");
//! client.get(target_url)
//!     .bearer_auth(id_token)
//!     .send()
//!     .await?;
//! # Ok::<(), anyhow::Error>(())
//! # });
//! ```
//!
//! [ID tokens]: https://cloud.google.com/docs/authentication/token-types#identity-tokens
//! [user account]: https://cloud.google.com/docs/authentication#user-accounts
//! [Service to Service Authentication]: https://cloud.google.com/run/docs/authenticating/service-to-service

use crate::build_errors::Error as BuilderError;
use crate::credentials::user_account::UserTokenProvider;
use crate::{
    BuildResult, Result,
    credentials::{
        idtoken::{IDTokenCredentials, dynamic::IDTokenCredentialsProvider},
        user_account::AuthorizedUser,
    },
    token::TokenProvider,
};
use async_trait::async_trait;
use serde_json::Value;
use std::sync::Arc;

#[derive(Debug)]
struct UserAccountCredentials<T>
where
    T: TokenProvider,
{
    token_provider: T,
}

#[async_trait]
impl<T> IDTokenCredentialsProvider for UserAccountCredentials<T>
where
    T: TokenProvider,
{
    async fn id_token(&self) -> Result<String> {
        self.token_provider.token().await.map(|token| token.token)
    }
}

/// A builder for [`IDTokenCredentials`] instances backed by user account credentials.
pub struct Builder {
    authorized_user: Value,
    token_uri: Option<String>,
}

impl Builder {
    /// Creates a new builder for `IDTokenCredentials` from a `serde_json::Value`
    /// representing the authorized user credentials.
    ///
    /// The `authorized_user` JSON is typically generated when a user
    /// authenticates using the [application-default login] process.
    ///
    /// [application-default login]: https://cloud.google.com/sdk/gcloud/reference/auth/application-default/login
    pub fn new(authorized_user: Value) -> Self {
        Self {
            authorized_user,
            token_uri: None,
        }
    }

    /// Sets the URI for the token endpoint used to fetch access tokens.
    ///
    /// Any value provided here overrides a `token_uri` value from the input `authorized_user` JSON.
    /// Defaults to `https://oauth2.googleapis.com/token` if not specified here or in the `authorized_user` JSON.
    pub fn with_token_uri<S: Into<String>>(mut self, token_uri: S) -> Self {
        self.token_uri = Some(token_uri.into());
        self
    }

    fn build_token_provider(self) -> BuildResult<UserTokenProvider> {
        let authorized_user = serde_json::from_value::<AuthorizedUser>(self.authorized_user)
            .map_err(BuilderError::parsing)?;
        Ok(UserTokenProvider::new_id_token_provider(
            authorized_user,
            self.token_uri,
        ))
    }

    /// Returns an [`IDTokenCredentials`] instance with the configured
    /// settings.
    ///
    /// # Errors
    ///
    /// Returns a `BuildError` if the `authorized_user`
    /// provided to [`Builder::new`] cannot be successfully deserialized into the
    /// expected format. This typically happens if the JSON value is malformed or
    /// missing required fields. For more information on how to generate
    /// `authorized_user` json, consult the relevant section in the
    /// [application-default credentials] guide.
    ///
    /// [application-default credentials]: https://cloud.google.com/docs/authentication/application-default-credentials
    pub fn build(self) -> BuildResult<IDTokenCredentials> {
        let creds = UserAccountCredentials {
            token_provider: self.build_token_provider()?,
        };
        Ok(IDTokenCredentials {
            inner: Arc::new(creds),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::credentials::tests::find_source_error;
    use crate::credentials::user_account::{
        Oauth2RefreshRequest, Oauth2RefreshResponse, RefreshGrantType,
    };
    use http::StatusCode;
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

    fn check_request(request: &Oauth2RefreshRequest) -> bool {
        request.client_id == "test-client-id"
            && request.client_secret == "test-client-secret"
            && request.refresh_token == "test-refresh-token"
            && request.grant_type == RefreshGrantType::RefreshToken
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn id_token_success() -> TestResult {
        let server = Server::run();
        let response = Oauth2RefreshResponse {
            access_token: "test-access-token".to_string(),
            id_token: Some("test-id-token".to_string()),
            expires_in: Some(3600),
            refresh_token: Some("test-refresh-token".to_string()),
            scope: None,
            token_type: "Bearer".to_string(),
        };
        server.expect(
            Expectation::matching(all_of![
                request::path("/token"),
                request::body(json_decoded(|req: &Oauth2RefreshRequest| {
                    check_request(req)
                }))
            ])
            .respond_with(json_encoded(response)),
        );

        let authorized_user = authorized_user_json(server.url("/token").to_string());
        let creds = Builder::new(authorized_user).build()?;
        let id_token = creds.id_token().await?;
        assert_eq!(id_token, "test-id-token");
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn id_token_missing_id_token_in_response() -> TestResult {
        let server = Server::run();
        let response = Oauth2RefreshResponse {
            access_token: "test-access-token".to_string(),
            id_token: None, // Missing ID token
            expires_in: Some(3600),
            refresh_token: Some("test-refresh-token".to_string()),
            scope: None,
            token_type: "Bearer".to_string(),
        };
        server.expect(
            Expectation::matching(all_of![
                request::path("/token"),
                request::body(json_decoded(|req: &Oauth2RefreshRequest| {
                    check_request(req)
                }))
            ])
            .respond_with(json_encoded(response)),
        );

        let authorized_user = authorized_user_json(server.url("/token").to_string());
        let creds = Builder::new(authorized_user).build()?;
        let err = creds.id_token().await.unwrap_err();
        assert!(!err.is_transient());
        assert!(
            err.to_string()
                .contains("can obtain an id token only when authenticated through gcloud")
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn id_token_builder_malformed_authorized_json_nonretryable() -> TestResult {
        let authorized_user = serde_json::json!({
            "client_secret": "test-client-secret",
            "refresh_token": "test-refresh-token",
            "type": "authorized_user",
        });

        let e = Builder::new(authorized_user).build().unwrap_err();
        assert!(e.is_parsing(), "{e}");
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn id_token_retryable_error() -> TestResult {
        let server = Server::run();
        server
            .expect(Expectation::matching(request::path("/token")).respond_with(status_code(503)));

        let authorized_user = authorized_user_json(server.url("/token").to_string());
        let creds = Builder::new(authorized_user).build()?;
        let err = creds.id_token().await.unwrap_err();
        assert!(err.is_transient());

        let source = find_source_error::<reqwest::Error>(&err);
        assert!(
            matches!(source, Some(e) if e.status() == Some(StatusCode::SERVICE_UNAVAILABLE)),
            "{err:?}"
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn id_token_nonretryable_error() -> TestResult {
        let server = Server::run();
        server
            .expect(Expectation::matching(request::path("/token")).respond_with(status_code(401)));

        let authorized_user = authorized_user_json(server.url("/token").to_string());
        let creds = Builder::new(authorized_user).build()?;
        let err = creds.id_token().await.unwrap_err();
        assert!(!err.is_transient());

        let source = find_source_error::<reqwest::Error>(&err);
        assert!(
            matches!(source, Some(e) if e.status() == Some(StatusCode::UNAUTHORIZED)),
            "{err:?}"
        );
        Ok(())
    }
}
