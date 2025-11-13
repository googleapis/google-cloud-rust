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

//! Obtain [OIDC ID tokens] using [impersonated service accounts].
//!
//! When the principal you are using doesn't have the permissions you need to
//! accomplish your task, or you want to use a service account in a development
//! environment, you can use service account impersonation. The typical principals
//! used to impersonate a service account are [User Account] or another [Service Account].
//!
//! The principal that is trying to impersonate a target service account should have
//! [Service Account Token Creator Role] on the target service account.
//!
//! ## Example: Creating impersonated credentials from a JSON object with target audience and sending ID Tokens.
//!
//! ```
//! # use google_cloud_auth::credentials::idtoken;
//! # use serde_json::json;
//! # use reqwest;    
//! # tokio_test::block_on(async {
//! let source_credentials = json!({
//!     "type": "authorized_user",
//!     "client_id": "test-client-id",
//!     "client_secret": "test-client-secret",
//!     "refresh_token": "test-refresh-token"
//! });
//!
//! let impersonated_credential = json!({
//!     "type": "impersonated_service_account",
//!     "service_account_impersonation_url": "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/test-principal:generateAccessToken",
//!     "source_credentials": source_credentials,
//! });
//!
//! let audience = "https://example.com"
//! let credentials = idtoken::impersonated::Builder::new(audience, impersonated_credential)
//!     .build()?;
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
//! [Impersonated service accounts]: https://cloud.google.com/docs/authentication/use-service-account-impersonation
//! [ID tokens]: https://cloud.google.com/docs/authentication/token-types#identity-tokens
//! [User Account]: https://cloud.google.com/docs/authentication#user-accounts
//! [Service Account]: https://cloud.google.com/iam/docs/service-account-overview
//! [Service Account Token Creator Role]: https://cloud.google.com/docs/authentication/use-service-account-impersonation#required-roles

use crate::{
    BuildResult, Result,
    credentials::{
        CacheableResource, Credentials,
        idtoken::{
            IDTokenCredentials, dynamic::IDTokenCredentialsProvider, parse_id_token_from_str,
        },
        impersonated::{
            BuilderSource, IMPERSONATED_CREDENTIAL_TYPE, MSG, build_components_from_credentials,
            build_components_from_json,
        },
    },
    errors,
    headers_util::{self, ID_TOKEN_REQUEST_TYPE, metrics_header_value},
    token::{CachedTokenProvider, Token, TokenProvider},
    token_cache::TokenCache,
};
use async_trait::async_trait;
use gax::error::CredentialsError;
use http::{Extensions, HeaderMap};
use reqwest::Client;
use serde_json::Value;
use std::sync::Arc;

/// A builder for constructing Impersonated Service Account [IDTokenCredentials] instance.
pub struct Builder {
    source: BuilderSource,
    delegates: Option<Vec<String>>,
    include_email: Option<bool>,
    target_audience: String,
    service_account_impersonation_url: Option<String>,
}

impl Builder {
    /// Creates a new builder using `impersonated_service_account` JSON value.
    ///
    /// The `impersonated_service_account` JSON is typically generated using
    /// [application default login] with the [impersonation flag].
    ///
    /// [impersonation flag]: https://cloud.google.com/docs/authentication/use-service-account-impersonation#adc
    /// [application default login]: https://cloud.google.com/sdk/gcloud/reference/auth/application-default/login  
    pub fn new<S: Into<String>>(target_audience: S, impersonated_credential: Value) -> Self {
        Self {
            source: BuilderSource::FromJson(impersonated_credential),
            delegates: None,
            include_email: None,
            target_audience: target_audience.into(),
            service_account_impersonation_url: None,
        }
    }

    /// Creates a new builder with a source [Credentials] object, target principal and audience.
    /// Target principal is the email of the service account to impersonate.
    pub fn from_source_credentials<SA: Into<String>, SP: Into<String>>(
        target_audience: SA,
        target_principal: SP,
        source_credentials: Credentials,
    ) -> Self {
        Self {
            source: BuilderSource::FromCredentials(source_credentials),
            delegates: None,
            include_email: None,
            target_audience: target_audience.into(),
            service_account_impersonation_url: Some(format!(
                "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/{}:generateIdToken",
                target_principal.into()
            )),
        }
    }

    #[cfg(test)]
    // just used for tests when from_source_credentials is used and we need to override the impersonation url.
    pub(crate) fn with_impersonation_url_host<S: Into<String>>(mut self, host: S) -> Self {
        self.service_account_impersonation_url = self
            .service_account_impersonation_url
            .map(|s| s.replace("https://iamcredentials.googleapis.com/", &host.into()));
        self
    }

    /// Should include email claims in the ID Token.
    pub fn with_include_email(mut self, include_email: bool) -> Self {
        self.include_email = Some(include_email);
        self
    }

    /// Sets the chain of delegates.
    pub fn with_delegates<I, S>(mut self, delegates: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.delegates = Some(delegates.into_iter().map(|s| s.into()).collect());
        self
    }

    /// Returns a [Credentials] instance with the configured settings.
    ///
    /// # Errors
    ///
    /// Returns an Error for one of the following cases:
    /// - If the `impersonated_service_account` provided to [`Builder::new`] cannot
    ///   be successfully deserialized into the expected format. This typically happens
    ///   if the JSON value is malformed or missing required fields. For more information,
    ///   see the guide on how to [use service account impersonation].
    /// - If the `impersonated_service_account` provided to [`Builder::new`] has a
    ///   `source_credentials` of `impersonated_service_account` type.
    ///
    /// [use service account impersonation]: https://cloud.google.com/docs/authentication/use-service-account-impersonation#adc
    pub fn build(self) -> BuildResult<IDTokenCredentials> {
        let components = match self.source {
            BuilderSource::FromJson(json) => {
                let mut components = build_components_from_json(json)?;
                components.service_account_impersonation_url = components
                    .service_account_impersonation_url
                    .replace("generateAccessToken", "generateIdToken");
                components
            }
            BuilderSource::FromCredentials(source_credentials) => {
                build_components_from_credentials(
                    source_credentials,
                    self.service_account_impersonation_url,
                )?
            }
        };
        let token_provider = ImpersonatedTokenProvider {
            source_credentials: components.source_credentials,
            service_account_impersonation_url: components.service_account_impersonation_url,
            delegates: self.delegates.or(components.delegates),
            include_email: self.include_email,
            target_audience: self.target_audience,
        };
        Ok(IDTokenCredentials {
            inner: Arc::new(ImpersonatedServiceAccount {
                token_provider: TokenCache::new(token_provider),
            }),
        })
    }
}

#[derive(Debug)]
struct ImpersonatedServiceAccount<T>
where
    T: CachedTokenProvider,
{
    token_provider: T,
}

#[async_trait::async_trait]
impl<T> IDTokenCredentialsProvider for ImpersonatedServiceAccount<T>
where
    T: CachedTokenProvider,
{
    async fn id_token(&self) -> Result<String> {
        let cached_token = self.token_provider.token(Extensions::new()).await?;
        match cached_token {
            CacheableResource::New { data, .. } => Ok(data.token),
            CacheableResource::NotModified => {
                Err(CredentialsError::from_msg(false, "failed to fetch token"))
            }
        }
    }
}

#[derive(Debug)]
pub(crate) struct ImpersonatedTokenProvider {
    pub(crate) source_credentials: Credentials,
    pub(crate) service_account_impersonation_url: String,
    pub(crate) delegates: Option<Vec<String>>,
    pub(crate) target_audience: String,
    pub(crate) include_email: Option<bool>,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq)]
struct GenerateIdTokenRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    delegates: Option<Vec<String>>,
    audience: String,
    #[serde(skip_serializing_if = "Option::is_none", rename = "includeEmail")]
    include_email: Option<bool>,
}

async fn generate_id_token(
    source_headers: HeaderMap,
    delegates: Option<Vec<String>>,
    audience: String,
    include_email: Option<bool>,
    service_account_impersonation_url: &str,
) -> Result<Token> {
    let client = Client::new();

    let body = GenerateIdTokenRequest {
        audience,
        delegates,
        include_email,
    };

    let response = client
        .post(service_account_impersonation_url)
        .header("Content-Type", "application/json")
        .header(
            headers_util::X_GOOG_API_CLIENT,
            metrics_header_value(ID_TOKEN_REQUEST_TYPE, IMPERSONATED_CREDENTIAL_TYPE),
        )
        .headers(source_headers)
        .json(&body)
        .send()
        .await
        .map_err(|e| errors::from_http_error(e, MSG))?;

    if !response.status().is_success() {
        let err = errors::from_http_response(response, MSG).await;
        return Err(err);
    }

    let token_response = response
        .json::<GenerateIdTokenResponse>()
        .await
        .map_err(|e| {
            let retryable = !e.is_decode();
            CredentialsError::from_source(retryable, e)
        })?;

    parse_id_token_from_str(token_response.token)
}

#[async_trait]
impl TokenProvider for ImpersonatedTokenProvider {
    async fn token(&self) -> Result<Token> {
        let source_headers = self.source_credentials.headers(Extensions::new()).await?;
        let source_headers = match source_headers {
            CacheableResource::New { data, .. } => data,
            CacheableResource::NotModified => {
                unreachable!("requested source credentials without a caching etag")
            }
        };

        generate_id_token(
            source_headers,
            self.delegates.clone(),
            self.target_audience.clone(),
            self.include_email,
            &self.service_account_impersonation_url,
        )
        .await
    }
}

#[derive(serde::Deserialize)]
struct GenerateIdTokenResponse {
    #[serde(rename = "token")]
    token: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::credentials::idtoken::tests::generate_test_id_token;
    use httptest::{Expectation, Server, matchers::*, responders::*};
    use serde_json::json;

    type TestResult = anyhow::Result<()>;

    #[tokio::test]
    async fn test_impersonated_service_account_id_token() -> TestResult {
        let audience = "test-audience";
        let token_string = generate_test_id_token(audience);
        let server = Server::run();
        server.expect(
            Expectation::matching(request::method_path("POST", "/token")).respond_with(
                json_encoded(json!({
                    "access_token": "test-user-account-token",
                    "expires_in": 3600,
                    "token_type": "Bearer",
                })),
            ),
        );

        server.expect(
            Expectation::matching(all_of![
                request::method_path(
                    "POST",
                    "/v1/projects/-/serviceAccounts/test-principal:generateIdToken"
                ),
                request::headers(contains((
                    "authorization",
                    "Bearer test-user-account-token"
                ))),
                request::body(json_decoded(eq(json!({
                    "audience": audience,
                }))))
            ])
            .respond_with(json_encoded(json!({
                "token": token_string,
            }))),
        );

        let impersonated_credential = json!({
            "type": "impersonated_service_account",
            "service_account_impersonation_url": server.url("/v1/projects/-/serviceAccounts/test-principal:generateAccessToken").to_string(),
            "source_credentials": {
                "type": "authorized_user",
                "client_id": "test-client-id",
                "client_secret": "test-client-secret",
                "refresh_token": "test-refresh-token",
                "token_uri": server.url("/token").to_string()
            }
        });
        let creds = Builder::new(audience, impersonated_credential.clone()).build()?;

        let token = creds.id_token().await?;
        assert_eq!(token, token_string);

        Ok(())
    }

    #[tokio::test]
    async fn test_impersonated_id_token_with_delegates_and_email() -> TestResult {
        let audience = "test-audience";
        let token_string = generate_test_id_token(audience);
        let server = Server::run();
        server.expect(
            Expectation::matching(request::method_path("POST", "/token")).respond_with(
                json_encoded(json!({
                    "access_token": "test-user-account-token",
                    "expires_in": 3600,
                    "token_type": "Bearer",
                })),
            ),
        );

        server.expect(
            Expectation::matching(all_of![
                request::method_path(
                    "POST",
                    "/v1/projects/-/serviceAccounts/test-principal:generateIdToken"
                ),
                request::headers(contains((
                    "authorization",
                    "Bearer test-user-account-token"
                ))),
                request::body(json_decoded(eq(json!({
                    "audience": audience,
                    "delegates": ["delegate1", "delegate2"],
                    "includeEmail": true
                }))))
            ])
            .respond_with(json_encoded(json!({
                "token": token_string,
            }))),
        );

        let impersonated_credential = json!({
            "type": "impersonated_service_account",
            "service_account_impersonation_url": server.url("/v1/projects/-/serviceAccounts/test-principal:generateIdToken").to_string(),
            "source_credentials": {
                "type": "authorized_user",
                "client_id": "test-client-id",
                "client_secret": "test-client-secret",
                "refresh_token": "test-refresh-token",
                "token_uri": server.url("/token").to_string()
            }
        });
        let creds = Builder::new("test-audience", impersonated_credential)
            .with_delegates(vec!["delegate1", "delegate2"])
            .with_include_email(true)
            .build()?;

        let token = creds.id_token().await?;
        assert_eq!(token, token_string);

        Ok(())
    }

    #[tokio::test]
    async fn test_impersonated_id_token_from_source_credentials() -> TestResult {
        let audience = "test-audience";
        let token_string = generate_test_id_token(audience);
        let server = Server::run();
        server.expect(
            Expectation::matching(request::method_path("POST", "/token")).respond_with(
                json_encoded(json!({
                    "access_token": "test-user-account-token",
                    "expires_in": 3600,
                    "token_type": "Bearer",
                })),
            ),
        );

        server.expect(
            Expectation::matching(all_of![
                request::method_path(
                    "POST",
                    "/v1/projects/-/serviceAccounts/test-principal:generateIdToken"
                ),
                request::headers(contains((
                    "authorization",
                    "Bearer test-user-account-token"
                ))),
                request::body(json_decoded(eq(json!({
                    "audience": audience,
                }))))
            ])
            .respond_with(json_encoded(json!({
                "token": token_string,
            }))),
        );

        let source_credentials = crate::credentials::user_account::Builder::new(json!({
            "type": "authorized_user",
            "client_id": "test-client-id",
            "client_secret": "test-client-secret",
            "refresh_token": "test-refresh-token",
            "token_uri": server.url("/token").to_string()
        }))
        .build()?;

        let creds =
            Builder::from_source_credentials(audience, "test-principal", source_credentials)
                .with_impersonation_url_host(server.url("/").to_string())
                .build()?;

        let token = creds.id_token().await?;
        assert_eq!(token, token_string);

        Ok(())
    }

    #[tokio::test]
    async fn test_impersonated_id_token_fail() -> TestResult {
        let server = Server::run();
        server.expect(
            Expectation::matching(request::method_path("POST", "/token")).respond_with(
                json_encoded(json!({
                    "access_token": "test-user-account-token",
                    "expires_in": 3600,
                    "token_type": "Bearer",
                })),
            ),
        );
        server.expect(
            Expectation::matching(request::method_path(
                "POST",
                "/v1/projects/-/serviceAccounts/test-principal:generateIdToken",
            ))
            .respond_with(status_code(500)),
        );

        let impersonated_credential = json!({
            "type": "impersonated_service_account",
            "service_account_impersonation_url": server.url("/v1/projects/-/serviceAccounts/test-principal:generateAccessToken").to_string(),
            "source_credentials": {
                "type": "authorized_user",
                "client_id": "test-client-id",
                "client_secret": "test-client-secret",
                "refresh_token": "test-refresh-token",
                "token_uri": server.url("/token").to_string()
            }
        });
        let creds = Builder::new("test-audience", impersonated_credential).build()?;

        let err = creds.id_token().await.unwrap_err();
        assert!(err.is_transient());

        Ok(())
    }

    #[tokio::test]
    async fn test_impersonated_id_token_metrics_header() -> TestResult {
        let audience = "test-audience";
        let token_string = generate_test_id_token(audience);
        let server = Server::run();
        server.expect(
            Expectation::matching(request::method_path("POST", "/token")).respond_with(
                json_encoded(json!({
                    "access_token": "test-user-account-token",
                    "expires_in": 3600,
                    "token_type": "Bearer",
                })),
            ),
        );

        server.expect(
            Expectation::matching(all_of![
                request::method_path(
                    "POST",
                    "/v1/projects/-/serviceAccounts/test-principal:generateIdToken"
                ),
                request::headers(contains(("x-goog-api-client", matches("cred-type/imp")))),
                request::headers(contains((
                    "x-goog-api-client",
                    matches("auth-request-type/it")
                )))
            ])
            .respond_with(json_encoded(json!({
                "token": token_string,
            }))),
        );

        let impersonated_credential = json!({
            "type": "impersonated_service_account",
            "service_account_impersonation_url": server.url("/v1/projects/-/serviceAccounts/test-principal:generateAccessToken").to_string(),
            "source_credentials": {
                "type": "authorized_user",
                "client_id": "test-client-id",
                "client_secret": "test-client-secret",
                "refresh_token": "test-refresh-token",
                "token_uri": server.url("/token").to_string()
            }
        });
        let creds = Builder::new(audience, impersonated_credential).build()?;

        let token = creds.id_token().await?;
        assert_eq!(token, token_string);

        Ok(())
    }
}
