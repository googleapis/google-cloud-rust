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

//! Obtain [OIDC ID tokens] using [Service Accounts].
//!
//! While the Google Cloud client libraries for Rust automatically use the types
//! in this module when ADC finds a service account key file, you may want to
//! use these types directly when the service account key is obtained from
//! Cloud Secret Manager or a similar service.
//!
//! `IDTokenCredentials` obtain OIDC ID tokens, which are commonly
//! used for [service to service authentication]. For example, when the
//! target service is hosted in Cloud Run or mediated by Identity-Aware Proxy (IAP).
//!
//! Unlike access tokens, ID tokens are not used to authorize access to
//! Google Cloud APIs but to verify the identity of a principal.
//!
//! # Example: Creating Service Account sourced credentials with target audience and sending ID Tokens.
//! ```
//! # use google_cloud_auth::credentials::idtoken;
//! # use reqwest;
//! # tokio_test::block_on(async {
//! let service_account_key = serde_json::json!({
//!     "client_email": "test-client-email",
//!     "private_key_id": "test-private-key-id",
//!     "private_key": "<YOUR_PKCS8_PEM_KEY_HERE>",
//!     "project_id": "test-project-id",
//!     "universe_domain": "test-universe-domain",
//! });
//! let audience = "https://example.com";
//! let credentials: Credentials = idtoken::service_account::Builder::new(audience, service_account_key)
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
//! [Best practices for using service accounts]: https://cloud.google.com/iam/docs/best-practices-service-accounts#choose-when-to-use
//! [ID tokens]: https://cloud.google.com/docs/authentication/token-types#identity-tokens
//! [create a service account key]: https://cloud.google.com/iam/docs/keys-create-delete#creating
//! [Service Accounts]: https://cloud.google.com/iam/docs/service-account-overview
//! [service account key]: https://cloud.google.com/iam/docs/keys-create-delete#creating
//! [Service to Service Authentication]: https://cloud.google.com/run/docs/authenticating/service-to-service

use crate::Result;
use crate::build_errors::Error as BuilderError;
use crate::constants::{JWT_BEARER_GRANT_TYPE, OAUTH2_TOKEN_SERVER_URL};
use crate::credentials::CacheableResource;
use crate::credentials::idtoken::dynamic::IDTokenCredentialsProvider;
use crate::credentials::service_account::{ServiceAccountKey, ServiceAccountTokenGenerator};
use crate::token::{CachedTokenProvider, Token, TokenProvider};
use crate::token_cache::TokenCache;
use crate::{BuildResult, credentials::idtoken::IDTokenCredentials};
use async_trait::async_trait;
use gax::error::CredentialsError;
use http::Extensions;
use reqwest::Client;
use serde_json::Value;
use std::sync::Arc;

#[derive(Debug)]
struct ServiceAccountCredentials<T>
where
    T: CachedTokenProvider,
{
    token_provider: T,
}

#[async_trait]
impl<T> IDTokenCredentialsProvider for ServiceAccountCredentials<T>
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
struct ServiceAccountTokenProvider {
    service_account_key: ServiceAccountKey,
    audience: String,
    target_audience: String,
    token_server_url: String,
}

#[async_trait]
impl TokenProvider for ServiceAccountTokenProvider {
    async fn token(&self) -> Result<Token> {
        let audience = self.audience.clone();
        let target_audience = Some(self.target_audience.clone());
        let service_account_key = self.service_account_key.clone();
        let tg = ServiceAccountTokenGenerator {
            audience: Some(audience),
            service_account_key,
            target_audience,
            scopes: None,
        };
        let assertion = tg.generate()?;

        let client = Client::new();
        let request = client.post(&self.token_server_url).form(&[
            ("grant_type", JWT_BEARER_GRANT_TYPE.to_string()),
            ("assertion", assertion),
        ]);

        let response = request
            .send()
            .await
            .map_err(|e| crate::errors::from_http_error(e, "failed to exchange id token"))?;

        if !response.status().is_success() {
            let err = crate::errors::from_http_response(response, "failed to fetch id token").await;
            return Err(err);
        }

        let token = response
            .text()
            .await
            .map_err(|e| CredentialsError::from_source(!e.is_decode(), e))?;

        Ok(Token {
            token,
            token_type: "Bearer".to_string(),
            expires_at: None,
            metadata: None,
        })
    }
}

/// Creates [`IDTokenCredentials`] instances that fetch ID tokens using
/// service accounts.
pub struct Builder {
    service_account_key: Value,
    target_audience: String,
    token_server_url: String,
}

impl Builder {
    /// The `target_audience` is a required parameter that specifies the
    /// intended audience of the ID token. This is typically the URL of the
    /// service that will be receiving the token.
    pub fn new<S: Into<String>>(target_audience: S, service_account_key: Value) -> Self {
        Self {
            service_account_key,
            target_audience: target_audience.into(),
            token_server_url: OAUTH2_TOKEN_SERVER_URL.to_string(),
        }
    }

    #[cfg(test)]
    pub(crate) fn with_token_server_url<S: Into<String>>(mut self, url: S) -> Self {
        self.token_server_url = url.into();
        self
    }

    fn build_token_provider(
        self,
        target_audience: String,
    ) -> BuildResult<ServiceAccountTokenProvider> {
        let service_account_key =
            serde_json::from_value::<ServiceAccountKey>(self.service_account_key)
                .map_err(BuilderError::parsing)?;
        Ok(ServiceAccountTokenProvider {
            service_account_key,
            audience: OAUTH2_TOKEN_SERVER_URL.to_string(),
            target_audience,
            token_server_url: self.token_server_url,
        })
    }

    /// Returns an [`IDTokenCredentials`] instance with the configured
    /// settings.
    pub fn build(self) -> BuildResult<IDTokenCredentials> {
        let target_audience = self.target_audience.clone();
        let creds = ServiceAccountCredentials {
            token_provider: TokenCache::new(self.build_token_provider(target_audience)?),
        };
        Ok(IDTokenCredentials {
            inner: Arc::new(creds),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::constants::JWT_BEARER_GRANT_TYPE;
    use crate::credentials::tests::PKCS8_PK;
    use httptest::{
        Expectation, Server,
        matchers::{all_of, any, contains, request, url_decoded},
        responders::*,
    };
    use serde_json::Value;
    use serde_json::json;

    type TestResult = std::result::Result<(), Box<dyn std::error::Error>>;

    fn get_mock_service_key() -> Value {
        json!({
            "client_email": "test-client-email",
            "private_key_id": "test-private-key-id",
            "private_key": "",
            "project_id": "test-project-id",
        })
    }

    #[tokio::test]
    async fn idtoken_success() -> TestResult {
        let server = Server::run();
        server.expect(
            Expectation::matching(all_of![
                request::method("POST"),
                request::path("/"),
                request::body(url_decoded(contains(("grant_type", JWT_BEARER_GRANT_TYPE)))),
                request::body(url_decoded(contains(("assertion", any())))),
            ])
            .respond_with(status_code(200).body("test-id-token")),
        );

        let mut service_account_key = get_mock_service_key();
        service_account_key["private_key"] = Value::from(PKCS8_PK.clone());

        let creds = Builder::new("test-audience", service_account_key)
            .with_token_server_url(server.url("/").to_string())
            .build()?;

        let token = creds.id_token().await?;
        assert_eq!(token, "test-id-token");
        Ok(())
    }

    #[tokio::test]
    async fn idtoken_http_error() -> TestResult {
        let server = Server::run();
        server.expect(
            Expectation::matching(all_of![request::method("POST"), request::path("/"),])
                .respond_with(status_code(501)),
        );

        let mut service_account_key = get_mock_service_key();
        service_account_key["private_key"] = Value::from(PKCS8_PK.clone());

        let creds = Builder::new("test-audience", service_account_key)
            .with_token_server_url(server.url("/").to_string())
            .build()?;

        let err = creds.id_token().await.unwrap_err();
        assert!(!err.is_transient());
        Ok(())
    }

    #[tokio::test]
    async fn idtoken_caching() -> TestResult {
        let server = Server::run();
        server.expect(
            Expectation::matching(all_of![
                request::method("POST"),
                request::path("/"),
                request::body(url_decoded(contains(("grant_type", JWT_BEARER_GRANT_TYPE)))),
                request::body(url_decoded(contains(("assertion", any())))),
            ])
            .times(1)
            .respond_with(status_code(200).body("test-id-token")),
        );

        let mut service_account_key = get_mock_service_key();
        service_account_key["private_key"] = Value::from(PKCS8_PK.clone());

        let creds = Builder::new("test-audience", service_account_key)
            .with_token_server_url(format!("http://{}", server.addr()))
            .build()?;

        let id_token = creds.id_token().await?;
        assert_eq!(id_token, "test-id-token");

        let id_token = creds.id_token().await?;
        assert_eq!(id_token, "test-id-token");

        Ok(())
    }
}
