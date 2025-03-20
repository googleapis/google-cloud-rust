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

//! Provides functionality for interacting with the Metadata Service (MDS) to obtain access token.
//!
//! This module defines the `MDSCredentialBuilder` struct for creating credentials
//! that can retrieve access tokens from the Google Cloud Metadata Service.
//!
//! # Overview
//!
//! The Metadata Service is a component of Google Cloud environments that provides
//! information about the running instance and its configuration. This module leverages
//! the Metadata Service to dynamically obtain access tokens, which can then be used
//! to authenticate with other Google Cloud services.
//!
//! # Key Components
//!
//! *   **`MDSCredentialBuilder`**: A builder pattern implementation for constructing
//!     credentials that obtain tokens from the Metadata Service. It allows customization
//!     of the Metadata Service endpoint, quota project id, scopes, and universe domain.
//!
//! If no value is provided for `endpoint`, `quota_project_id` or `universe_domain` the default will be used.
//! If scopes are not provided we will try to obtain it from the metadata server.
//! # Usage
//!
//! The `MDSCredentialBuilder` is the primary entry point for creating MDS credentials.
//! Here's a basic example:
//!
//! ```
//! # use google_cloud_auth::credentials::mds_credential::MDSCredentialBuilder;
//! # use google_cloud_auth::credentials::Credential;
//! # async fn doc() -> Result<(), Box<dyn std::error::Error>>{
//! // Create a new Credential that will use the Metadata Service with default settings.
//! let credential: Credential = MDSCredentialBuilder::default().build();
//!
//! // Customize the credential with a specific endpoint and quota project ID.
//! let credential_customized: Credential = MDSCredentialBuilder::default()
//!     .endpoint("http://metadata.google.internal/computeMetadata/v1")
//!     .quota_project_id("my-quota-project")
//!     .build();
//! # Ok(())
//! # }
//! ```
//!
//! # Note
//!
//! This module is primarily intended for use within Google Cloud environments where
//! the Metadata Service is available. Attempting to use it outside of such
//! environments may result in errors.

use crate::credentials::dynamic::CredentialTrait;
use crate::credentials::{Credential, Result, DEFAULT_UNIVERSE_DOMAIN, QUOTA_PROJECT_KEY};
use crate::errors::{is_retryable, CredentialError};
use crate::token::{Token, TokenProvider};
use async_trait::async_trait;
use bon::Builder;
use http::header::{HeaderName, HeaderValue, AUTHORIZATION};
use reqwest::Client;
use std::default::Default;
use std::sync::Arc;
use std::time::Duration;

const METADATA_FLAVOR_VALUE: &str = "Google";
const METADATA_FLAVOR: &str = "metadata-flavor";
const METADATA_ROOT: &str = "http://metadata.google.internal/computeMetadata/v1";

pub(crate) fn new() -> Credential {
    MDSCredentialBuilder::default().build()
}

#[derive(Debug)]
struct MDSCredential<T>
where
    T: TokenProvider,
{
    quota_project_id: Option<String>,
    universe_domain: Option<String>,
    token_provider: T,
}

/// A builder for creating `MDSCredential` instances.
///
/// This struct provides an interface for customizing the creation of
/// `MDSCredential` instances. It allows you to specify the Metadata Service
/// endpoint, quota project id, scopes, and universe domain.
#[derive(Debug, Default)]
pub struct MDSCredentialBuilder {
    endpoint: Option<String>,
    quota_project_id: Option<String>,
    scopes: Option<Vec<String>>,
    universe_domain: Option<String>,
}

#[allow(dead_code)]
impl MDSCredentialBuilder {
    /// Sets the endpoint that this credential will use.
    /// # Default
    ///
    /// if `endpoint` is not specified then  `http://metadata.google.internal/computeMetadata/v1` is used.
    pub fn endpoint<S: Into<String>>(mut self, endpoint: S) -> Self {
        self.endpoint = Some(endpoint.into());
        self
    }

    /// Sets the quota project that this credential will use.
    pub fn quota_project_id<S: Into<String>>(mut self, quota_project_id: S) -> Self {
        self.quota_project_id = Some(quota_project_id.into());
        self
    }

    /// Sets the universe domain that this credential will use.
    pub fn universe_domain<S: Into<String>>(mut self, universe_domain: S) -> Self {
        self.universe_domain = Some(universe_domain.into());
        self
    }

    /// Sets the scopes that this credential will use.
    /// # Default
    ///
    /// If scopes are not specified, they are fetched from the Metadata Server.
    pub fn scopes<S: Into<String>>(mut self, scopes: Vec<S>) -> Self {
        self.scopes = Some(scopes.into_iter().map(|s| s.into()).collect());
        self
    }

    /// Builds and returns a `Credential` instance with the configured settings.
    ///
    /// # Returns
    ///
    /// A new `Credential` instance that will use Metadata Service.
    pub fn build(self) -> Credential {
        let endpoint = self.endpoint.clone().unwrap_or(METADATA_ROOT.to_string());

        let token_provider = MDSAccessTokenProvider::builder()
            .endpoint(endpoint.clone())
            .maybe_scopes(self.scopes.clone())
            .build();

        let mdsc = MDSCredential {
            quota_project_id: self.quota_project_id.clone(),
            token_provider,
            universe_domain: self.universe_domain,
        };
        Credential {
            inner: Arc::new(mdsc),
        }
    }
}

#[async_trait::async_trait]
impl<T> CredentialTrait for MDSCredential<T>
where
    T: TokenProvider,
{
    async fn get_token(&self) -> Result<Token> {
        self.token_provider.get_token().await
    }

    async fn get_headers(&self) -> Result<Vec<(HeaderName, HeaderValue)>> {
        let token = self.get_token().await?;
        let mut value = HeaderValue::from_str(&format!("{} {}", token.token_type, token.token))
            .map_err(CredentialError::non_retryable)?;
        value.set_sensitive(true);
        let mut headers = vec![(AUTHORIZATION, value)];
        if let Some(project) = &self.quota_project_id {
            headers.push((
                HeaderName::from_static(QUOTA_PROJECT_KEY),
                HeaderValue::from_str(project).map_err(CredentialError::non_retryable)?,
            ));
        }
        Ok(headers)
    }

    async fn get_universe_domain(&self) -> Option<String> {
        if self.universe_domain.is_some() {
            return self.universe_domain.clone();
        }
        return Some(DEFAULT_UNIVERSE_DOMAIN.to_string());
    }
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
struct ServiceAccountInfo {
    email: String,
    scopes: Option<Vec<String>>,
    aliases: Option<Vec<String>>,
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
struct MDSTokenResponse {
    access_token: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    expires_in: Option<u64>,
    token_type: String,
}

#[derive(Debug, Clone, Default, Builder)]
struct MDSAccessTokenProvider {
    #[builder(into)]
    scopes: Option<Vec<String>>,
    #[builder(into)]
    endpoint: String,
}

impl MDSAccessTokenProvider {
    async fn get_service_account_info(&self, client: &Client) -> Result<ServiceAccountInfo> {
        let request = client
            .get(format!(
                "{}/instance/service-accounts/default/",
                self.endpoint
            ))
            .query(&[("recursive", "true")])
            .header(
                METADATA_FLAVOR,
                HeaderValue::from_static(METADATA_FLAVOR_VALUE),
            );

        let response = request.send().await.map_err(CredentialError::retryable)?;

        response
            .json::<ServiceAccountInfo>()
            .await
            .map_err(CredentialError::non_retryable)
    }
}

#[async_trait]
impl TokenProvider for MDSAccessTokenProvider {
    async fn get_token(&self) -> Result<Token> {
        let client = Client::new();
        // Determine scopes, fetching from metadata server if needed.
        let scopes = match &self.scopes {
            Some(s) => s.clone().join(","),
            None => {
                let service_account_info = self.get_service_account_info(&client).await?;
                service_account_info.scopes.unwrap_or_default().join(",")
            }
        };

        let request = client
            .get(format!(
                "{}/instance/service-accounts/default/token",
                self.endpoint
            ))
            .query(&[("scopes", scopes)])
            .header(
                METADATA_FLAVOR,
                HeaderValue::from_static(METADATA_FLAVOR_VALUE),
            );

        let response = request.send().await.map_err(CredentialError::retryable)?;
        // Process the response
        if !response.status().is_success() {
            let status = response.status();
            let body = response
                .text()
                .await
                .map_err(|e| CredentialError::new(is_retryable(status), e))?;
            return Err(CredentialError::from_str(
                is_retryable(status),
                format!("Failed to fetch token. {body}"),
            ));
        }
        let response = response.json::<MDSTokenResponse>().await.map_err(|e| {
            let retryable = !e.is_decode();
            CredentialError::new(retryable, e)
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

#[cfg(test)]
mod test {
    use super::*;
    use crate::credentials::test::HV;
    use crate::token::test::MockTokenProvider;
    use axum::extract::Query;
    use axum::response::IntoResponse;
    use reqwest::header::HeaderMap;
    use reqwest::StatusCode;
    use serde::Deserialize;
    use serde_json::Value;
    use std::collections::HashMap;
    use std::error::Error;
    use tokio::task::JoinHandle;

    type TestResult = std::result::Result<(), Box<dyn std::error::Error>>;

    // Define a struct to capture query parameters
    #[derive(Debug, Clone, Deserialize, PartialEq)]
    struct TokenQueryParams {
        scopes: Option<String>,
        recursive: Option<String>,
    }

    #[tokio::test]
    async fn get_token_success() {
        let expected = Token {
            token: "test-token".to_string(),
            token_type: "Bearer".to_string(),
            expires_at: None,
            metadata: None,
        };
        let expected_clone = expected.clone();

        let mut mock = MockTokenProvider::new();
        mock.expect_get_token()
            .times(1)
            .return_once(|| Ok(expected_clone));

        let mdsc = MDSCredential {
            quota_project_id: None,
            universe_domain: None,
            token_provider: mock,
        };
        let actual = mdsc.get_token().await.unwrap();
        assert_eq!(actual, expected);
    }

    #[tokio::test]
    async fn get_token_failure() {
        let mut mock = MockTokenProvider::new();
        mock.expect_get_token()
            .times(1)
            .return_once(|| Err(CredentialError::non_retryable_from_str("fail")));

        let mdsc = MDSCredential {
            quota_project_id: None,
            universe_domain: None,
            token_provider: mock,
        };
        assert!(mdsc.get_token().await.is_err());
    }

    #[tokio::test]
    async fn get_headers_success() {
        let token = Token {
            token: "test-token".to_string(),
            token_type: "Bearer".to_string(),
            expires_at: None,
            metadata: None,
        };

        let mut mock = MockTokenProvider::new();
        mock.expect_get_token().times(1).return_once(|| Ok(token));

        let mdsc = MDSCredential {
            quota_project_id: None,
            universe_domain: None,
            token_provider: mock,
        };
        let headers: Vec<HV> = HV::from(mdsc.get_headers().await.unwrap());

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
    async fn get_headers_failure() {
        let mut mock = MockTokenProvider::new();
        mock.expect_get_token()
            .times(1)
            .return_once(|| Err(CredentialError::non_retryable_from_str("fail")));

        let mdsc = MDSCredential {
            quota_project_id: None,
            universe_domain: None,
            token_provider: mock,
        };
        assert!(mdsc.get_headers().await.is_err());
    }

    fn handle_token_factory(
        response_code: StatusCode,
        response_headers: HeaderMap,
        response_body: Value,
    ) -> impl IntoResponse {
        (response_code, response_headers, response_body.to_string()).into_response()
    }

    // Starts a server running locally that responds on multiple paths.
    // Returns an (endpoint, server) pair.
    async fn start(
        path_handlers: HashMap<String, (StatusCode, Value, TokenQueryParams)>,
    ) -> (String, JoinHandle<()>) {
        let mut app = axum::Router::new();

        for (path, (code, body, expected_query)) in path_handlers {
            let header_map = HeaderMap::new();
            let handler = move |Query(query): Query<TokenQueryParams>| {
                let code = code.clone();
                let body = body.clone();
                let header_map = header_map.clone();
                async move {
                    assert_eq!(expected_query, query);
                    handle_token_factory(code, header_map, body)
                }
            };
            app = app.route(&path, axum::routing::get(handler));
        }

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (format!("http://{}:{}", addr.ip(), addr.port()), server)
    }

    #[tokio::test]
    async fn get_default_service_account_info_success() {
        let service_account = "default";
        let path = format!("/instance/service-accounts/{}/", service_account);
        let service_account_info = ServiceAccountInfo {
            email: "test@test.com".to_string(),
            scopes: Some(vec!["scope 1".to_string(), "scope 2".to_string()]),
            aliases: None,
        };
        let service_account_info_json = serde_json::to_value(service_account_info.clone()).unwrap();
        let (endpoint, _server) = start(HashMap::from([(
            path,
            (
                StatusCode::OK,
                service_account_info_json,
                TokenQueryParams {
                    scopes: None,
                    recursive: Some("true".to_string()),
                },
            ),
        )]))
        .await;

        let request = Client::new();
        let token_provider = MDSAccessTokenProvider::builder().endpoint(endpoint).build();

        let result = token_provider.get_service_account_info(&request).await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), service_account_info);
    }

    #[tokio::test]
    async fn get_service_account_info_server_error() {
        let path = "/instance/service-accounts/default/".to_string();
        let (endpoint, _server) = start(HashMap::from([(
            path,
            (
                StatusCode::SERVICE_UNAVAILABLE,
                serde_json::to_value("try again").unwrap(),
                TokenQueryParams {
                    scopes: None,
                    recursive: Some("true".to_string()),
                },
            ),
        )]))
        .await;

        let request = Client::new();
        let token_provider = MDSAccessTokenProvider::builder().endpoint(endpoint).build();

        let result = token_provider.get_service_account_info(&request).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn get_headers_success_with_quota_project() {
        let scopes = vec!["scope1".to_string()];
        let response = MDSTokenResponse {
            access_token: "test-access-token".to_string(),
            expires_in: Some(3600),
            token_type: "test-token-type".to_string(),
        };
        let response_body = serde_json::to_value(&response).unwrap();
        let path = "/instance/service-accounts/default/token";

        let (endpoint, _server) = start(HashMap::from([(
            path.to_string(),
            (
                StatusCode::OK,
                response_body,
                TokenQueryParams {
                    scopes: Some(scopes.join(",")),
                    recursive: None,
                },
            ),
        )]))
        .await;

        let mdsc = MDSCredentialBuilder::default()
            .scopes(scopes)
            .endpoint(endpoint)
            .quota_project_id("test-project")
            .build();

        let headers: Vec<HV> = HV::from(mdsc.get_headers().await.unwrap());
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
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn token_provider_full() -> TestResult {
        let scopes = vec!["scope1".to_string()];
        let response = MDSTokenResponse {
            access_token: "test-access-token".to_string(),
            expires_in: Some(3600),
            token_type: "test-token-type".to_string(),
        };
        let response_body = serde_json::to_value(&response).unwrap();
        let path = "/instance/service-accounts/default/token";

        let (endpoint, _server) = start(HashMap::from([(
            path.to_string(),
            (
                StatusCode::OK,
                response_body,
                TokenQueryParams {
                    scopes: Some(scopes.join(",")),
                    recursive: None,
                },
            ),
        )]))
        .await;
        println!("endpoint = {endpoint}");

        let mdsc = MDSCredentialBuilder::default()
            .scopes(scopes)
            .endpoint(endpoint)
            .build();
        let now = std::time::Instant::now();
        let token = mdsc.get_token().await?;
        assert_eq!(token.token, "test-access-token");
        assert_eq!(token.token_type, "test-token-type");
        assert!(token
            .expires_at
            .is_some_and(|d| d >= now + Duration::from_secs(3600)));

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn token_provider_full_no_scopes() -> TestResult {
        let service_account_info_path = "/instance/service-accounts/default/".to_string();
        let scopes = vec!["scope 1".to_string(), "scope 2".to_string()];
        let service_account_info = ServiceAccountInfo {
            email: "test@test.com".to_string(),
            scopes: Some(scopes.clone()),
            aliases: None,
        };
        let service_account_info_json = serde_json::to_value(service_account_info.clone()).unwrap();

        let response = MDSTokenResponse {
            access_token: "test-access-token".to_string(),
            expires_in: Some(3600),
            token_type: "test-token-type".to_string(),
        };
        let response_body = serde_json::to_value(&response).unwrap();
        let path = "/instance/service-accounts/default/token".to_string();

        let (endpoint, _server) = start(HashMap::from([
            (
                service_account_info_path,
                (
                    StatusCode::OK,
                    service_account_info_json,
                    TokenQueryParams {
                        scopes: None,
                        recursive: Some("true".to_string()),
                    },
                ),
            ),
            (
                path,
                (
                    StatusCode::OK,
                    response_body,
                    TokenQueryParams {
                        scopes: Some(scopes.join(",")),
                        recursive: None,
                    },
                ),
            ),
        ]))
        .await;
        println!("endpoint = {endpoint}");

        let mdsc = MDSCredentialBuilder::default().endpoint(endpoint).build();
        let now = std::time::Instant::now();
        let token = mdsc.get_token().await?;
        assert_eq!(token.token, "test-access-token");
        assert_eq!(token.token_type, "test-token-type");
        assert!(token
            .expires_at
            .is_some_and(|d| d >= now + Duration::from_secs(3600)));

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn token_provider_partial() -> TestResult {
        let scopes = vec!["scope1".to_string()];
        let response = MDSTokenResponse {
            access_token: "test-access-token".to_string(),
            expires_in: None,
            token_type: "test-token-type".to_string(),
        };
        let response_body = serde_json::to_value(&response).unwrap();
        let path = "/instance/service-accounts/default/token";
        let (endpoint, _server) = start(HashMap::from([(
            path.to_string(),
            (
                StatusCode::OK,
                response_body,
                TokenQueryParams {
                    scopes: Some(scopes.join(",")),
                    recursive: None,
                },
            ),
        )]))
        .await;
        println!("endpoint = {endpoint}");

        let mdsc = MDSCredentialBuilder::default()
            .endpoint(endpoint)
            .scopes(scopes)
            .build();
        let token = mdsc.get_token().await?;
        assert_eq!(token.token, "test-access-token");
        assert_eq!(token.token_type, "test-token-type");
        assert_eq!(token.expires_at, None);

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn token_provider_retryable_error() -> TestResult {
        let path = "/instance/service-accounts/default/token";
        let scopes = vec!["scope1".to_string()];
        let (endpoint, _server) = start(HashMap::from([(
            path.to_string(),
            (
                StatusCode::SERVICE_UNAVAILABLE,
                serde_json::to_value("try again")?,
                TokenQueryParams {
                    scopes: Some(scopes.join(",")),
                    recursive: None,
                },
            ),
        )]))
        .await;

        let mdsc = MDSCredentialBuilder::default()
            .endpoint(endpoint)
            .scopes(scopes)
            .build();
        let e = mdsc.get_token().await.err().unwrap();
        assert!(e.is_retryable());
        assert!(e.source().unwrap().to_string().contains("try again"));

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn token_provider_nonretryable_error() -> TestResult {
        let path = "/instance/service-accounts/default/token";
        let scopes = vec!["scope1".to_string()];
        let (endpoint, _server) = start(HashMap::from([(
            path.to_string(),
            (
                StatusCode::UNAUTHORIZED,
                serde_json::to_value("epic fail".to_string())?,
                TokenQueryParams {
                    scopes: Some(scopes.join(",")),
                    recursive: None,
                },
            ),
        )]))
        .await;

        let mdsc = MDSCredentialBuilder::default()
            .endpoint(endpoint)
            .scopes(scopes)
            .build();

        let e = mdsc.get_token().await.err().unwrap();
        assert!(!e.is_retryable());
        assert!(e.source().unwrap().to_string().contains("epic fail"));

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn token_provider_malformed_response_is_nonretryable() -> TestResult {
        let path = "/instance/service-accounts/default/token";
        let scopes = vec!["scope1".to_string()];
        let (endpoint, _server) = start(HashMap::from([(
            path.to_string(),
            (
                StatusCode::OK,
                serde_json::to_value("bad json".to_string())?,
                TokenQueryParams {
                    scopes: Some(scopes.join(",")),
                    recursive: None,
                },
            ),
        )]))
        .await;

        let mdsc = MDSCredentialBuilder::default()
            .endpoint(endpoint)
            .scopes(scopes)
            .build();

        let e = mdsc.get_token().await.err().unwrap();
        assert!(!e.is_retryable());

        Ok(())
    }
}
