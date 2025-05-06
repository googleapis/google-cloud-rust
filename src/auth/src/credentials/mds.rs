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

//! [Metadata Service] Credentials type.
//!
//! Google Cloud environments such as [Google Compute Engine (GCE)][gce-link],
//! [Google Kubernetes Engine (GKE)][gke-link], or [Cloud Run] provide a metadata service.
//! This is a local service to the VM (or pod) which (as the name implies) provides
//! metadata information about the VM. The service also provides access
//! tokens associated with the [default service account] for the corresponding
//! VM.
//!
//! The default host name of the metadata service is `metadata.google.internal`.
//! If you would like to use a different hostname, you can set it using the
//! `GCE_METADATA_HOST` environment variable.
//!
//! You can use this access token to securely authenticate with Google Cloud,
//! without having to download secrets or other credentials. The types in this
//! module allow you to retrieve these access tokens, and can be used with
//! the Google Cloud client libraries for Rust.
//!
//! While the Google Cloud client libraries for Rust default to
//! using the types defined in this module. You may want to use said types directly
//! to customize some of the properties of these credentials.
//!
//! Example usage:
//!
//! ```
//! # use google_cloud_auth::credentials::mds::Builder;
//! # use google_cloud_auth::credentials::Credentials;
//! # use google_cloud_auth::errors::CredentialsError;
//! # tokio_test::block_on(async {
//! let credentials: Credentials = Builder::default()
//!     .with_quota_project_id("my-quota-project")
//!     .build()?;
//! let token = credentials.token().await?;
//! println!("Token: {}", token.token);
//! # Ok::<(), CredentialsError>(())
//! # });
//! ```
//!
//! [Cloud Run]: https://cloud.google.com/run
//! [default service account]: https://cloud.google.com/iam/docs/service-account-types#default
//! [gce-link]: https://cloud.google.com/products/compute
//! [gke-link]: https://cloud.google.com/kubernetes-engine
//! [Metadata Service]: https://cloud.google.com/compute/docs/metadata/overview

use crate::credentials::dynamic::CredentialsProvider;
use crate::credentials::{Credentials, DEFAULT_UNIVERSE_DOMAIN, Result};
use crate::errors::{self, CredentialsError, is_retryable};
use crate::headers_util::build_bearer_headers;
use crate::token::{Token, TokenProvider};
use async_trait::async_trait;
use bon::Builder;
use http::header::{HeaderName, HeaderValue};
use reqwest::Client;
use std::default::Default;
use std::sync::Arc;
use std::time::Duration;

const METADATA_FLAVOR_VALUE: &str = "Google";
const METADATA_FLAVOR: &str = "metadata-flavor";
const METADATA_ROOT: &str = "http://metadata.google.internal";
const MDS_DEFAULT_URI: &str = "/computeMetadata/v1/instance/service-accounts/default";
const GCE_METADATA_HOST_ENV_VAR: &str = "GCE_METADATA_HOST";

#[derive(Debug)]
struct MDSCredentials<T>
where
    T: TokenProvider,
{
    quota_project_id: Option<String>,
    universe_domain: Option<String>,
    token_provider: T,
}

/// Creates [Credentials] instances backed by the [Metadata Service].
///
/// While the Google Cloud client libraries for Rust default to credentials
/// backed by the metadata service, some applications may need to:
/// * Customize the metadata service credentials in some way
/// * Bypass the [Application Default Credentials] lookup and only
///   use the metadata server credentials
/// * Use the credentials directly outside the client libraries
///
/// [Application Default Credentials]: https://cloud.google.com/docs/authentication/application-default-credentials
/// [Metadata Service]: https://cloud.google.com/compute/docs/metadata/overview
#[derive(Debug, Default)]
pub struct Builder {
    endpoint: Option<String>,
    quota_project_id: Option<String>,
    scopes: Option<Vec<String>>,
    universe_domain: Option<String>,
}

impl Builder {
    /// Sets the endpoint for this credentials.
    ///
    /// A trailing slash is significant, so specify the base URL without a trailing  
    /// slash. If not set, the credentials use `http://metadata.google.internal`.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_auth::credentials::mds::Builder;
    /// # tokio_test::block_on(async {
    /// let credentials = Builder::default()
    ///     .with_endpoint("https://metadata.google.foobar")
    ///     .build();
    /// # });
    /// ```
    pub fn with_endpoint<S: Into<String>>(mut self, endpoint: S) -> Self {
        self.endpoint = Some(endpoint.into());
        self
    }

    /// Set the [quota project] for this credentials.
    ///
    /// In some services, you can use a service account in
    /// one project for authentication and authorization, and charge
    /// the usage to a different project. This may require that the
    /// service account has `serviceusage.services.use` permissions on the quota project.
    ///
    /// [quota project]: https://cloud.google.com/docs/quotas/quota-project
    pub fn with_quota_project_id<S: Into<String>>(mut self, quota_project_id: S) -> Self {
        self.quota_project_id = Some(quota_project_id.into());
        self
    }

    /// Sets the universe domain for this credentials.
    ///
    /// Client libraries use `universe_domain` to determine
    /// the API endpoints to use for making requests.
    /// If not set, then credentials use `${service}.googleapis.com`,
    /// otherwise they use `${service}.${universe_domain}.
    pub fn with_universe_domain<S: Into<String>>(mut self, universe_domain: S) -> Self {
        self.universe_domain = Some(universe_domain.into());
        self
    }

    /// Sets the [scopes] for this credentials.
    ///
    /// Metadata server issues tokens based on the requested scopes.
    /// If no scopes are specified, the credentials defaults to all
    /// scopes configured for the [default service account] on the instance.
    ///
    /// [default service account]: https://cloud.google.com/iam/docs/service-account-types#default
    /// [scopes]: https://developers.google.com/identity/protocols/oauth2/scopes
    pub fn with_scopes<I, S>(mut self, scopes: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.scopes = Some(scopes.into_iter().map(|s| s.into()).collect());
        self
    }

    /// Returns a [Credentials] instance with the configured settings.
    pub fn build(self) -> Result<Credentials> {
        let endpoint = match std::env::var(GCE_METADATA_HOST_ENV_VAR) {
            Ok(endpoint) => format!("http://{}", endpoint),
            _ => self.endpoint.clone().unwrap_or(METADATA_ROOT.to_string()),
        };

        let token_provider = MDSAccessTokenProvider::builder()
            .endpoint(endpoint)
            .maybe_scopes(self.scopes)
            .build();
        let cached_token_provider = crate::token_cache::TokenCache::new(token_provider);

        let mdsc = MDSCredentials {
            quota_project_id: self.quota_project_id,
            token_provider: cached_token_provider,
            universe_domain: self.universe_domain,
        };
        Ok(Credentials {
            inner: Arc::new(mdsc),
        })
    }
}

#[async_trait::async_trait]
impl<T> CredentialsProvider for MDSCredentials<T>
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

    async fn universe_domain(&self) -> Option<String> {
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
            .get(format!("{}{}", self.endpoint, MDS_DEFAULT_URI))
            .query(&[("recursive", "true")])
            .header(
                METADATA_FLAVOR,
                HeaderValue::from_static(METADATA_FLAVOR_VALUE),
            );

        let response = request.send().await.map_err(errors::retryable)?;

        response
            .json::<ServiceAccountInfo>()
            .await
            .map_err(errors::non_retryable)
    }
}

#[async_trait]
impl TokenProvider for MDSAccessTokenProvider {
    async fn token(&self) -> Result<Token> {
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
            .get(format!("{}{}/token", self.endpoint, MDS_DEFAULT_URI))
            .query(&[("scopes", scopes)])
            .header(
                METADATA_FLAVOR,
                HeaderValue::from_static(METADATA_FLAVOR_VALUE),
            );

        let response = request.send().await.map_err(errors::retryable)?;
        // Process the response
        if !response.status().is_success() {
            let status = response.status();
            let body = response
                .text()
                .await
                .map_err(|e| CredentialsError::new(is_retryable(status), e))?;
            return Err(CredentialsError::from_str(
                is_retryable(status),
                format!("Failed to fetch token. {body}"),
            ));
        }
        let response = response.json::<MDSTokenResponse>().await.map_err(|e| {
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

#[cfg(test)]
mod test {
    use super::*;
    use crate::credentials::QUOTA_PROJECT_KEY;
    use crate::credentials::test::HV;
    use crate::token::test::MockTokenProvider;
    use axum::extract::Query;
    use axum::response::IntoResponse;
    use http::header::AUTHORIZATION;
    use reqwest::StatusCode;
    use reqwest::header::HeaderMap;
    use scoped_env::ScopedEnv;
    use serde::Deserialize;
    use serde_json::Value;
    use serial_test::{parallel, serial};
    use std::collections::HashMap;
    use std::error::Error;
    use std::sync::Mutex;
    use tokio::task::JoinHandle;
    use url::Url;

    type TestResult = std::result::Result<(), Box<dyn std::error::Error>>;

    // Define a struct to capture query parameters
    #[derive(Debug, Clone, Deserialize, PartialEq)]
    struct TokenQueryParams {
        scopes: Option<String>,
        recursive: Option<String>,
    }

    #[test]
    fn validate_default_endpoint_urls() {
        let default_endpoint_address = Url::parse(&format!("{}{}", METADATA_ROOT, MDS_DEFAULT_URI));
        assert!(default_endpoint_address.is_ok());

        let token_endpoint_address =
            Url::parse(&format!("{}{}/token", METADATA_ROOT, MDS_DEFAULT_URI));
        assert!(token_endpoint_address.is_ok());
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

        let mdsc = MDSCredentials {
            quota_project_id: None,
            universe_domain: None,
            token_provider: mock,
        };
        let actual = mdsc.token().await.unwrap();
        assert_eq!(actual, expected);
    }

    #[tokio::test]
    async fn token_failure() {
        let mut mock = MockTokenProvider::new();
        mock.expect_token()
            .times(1)
            .return_once(|| Err(errors::non_retryable_from_str("fail")));

        let mdsc = MDSCredentials {
            quota_project_id: None,
            universe_domain: None,
            token_provider: mock,
        };
        assert!(mdsc.token().await.is_err());
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

        let mdsc = MDSCredentials {
            quota_project_id: None,
            universe_domain: None,
            token_provider: mock,
        };
        let headers: Vec<HV> = HV::from(mdsc.headers().await.unwrap());

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

        let mdsc = MDSCredentials {
            quota_project_id: None,
            universe_domain: None,
            token_provider: mock,
        };
        assert!(mdsc.headers().await.is_err());
    }

    fn handle_token_factory(
        response_code: StatusCode,
        response_headers: HeaderMap,
        response_body: Value,
    ) -> impl IntoResponse {
        (response_code, response_headers, response_body.to_string()).into_response()
    }

    type Handlers = HashMap<String, (StatusCode, Value, TokenQueryParams, Arc<Mutex<i32>>)>;

    // Starts a server running locally that responds on multiple paths.
    // Returns an (endpoint, server) pair.
    async fn start(path_handlers: Handlers) -> (String, JoinHandle<()>) {
        let mut app = axum::Router::new();

        for (path, (code, body, expected_query, call_count)) in path_handlers {
            let header_map = HeaderMap::new();
            let handler = move |Query(query): Query<TokenQueryParams>| {
                let body = body.clone();
                let header_map = header_map.clone();
                async move {
                    assert_eq!(expected_query, query);
                    let mut count = call_count.lock().unwrap();
                    *count += 1;
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
    #[serial]
    async fn test_gce_metadata_host_env_var() {
        let scopes = ["scope1".to_string(), "scope2".to_string()];
        let response = MDSTokenResponse {
            access_token: "test-access-token".to_string(),
            expires_in: Some(3600),
            token_type: "test-token-type".to_string(),
        };
        let response_body = serde_json::to_value(&response).unwrap();

        let (endpoint, _server) = start(Handlers::from([(
            format!("{}/token", MDS_DEFAULT_URI),
            (
                StatusCode::OK,
                response_body,
                TokenQueryParams {
                    scopes: Some(scopes.join(",")),
                    recursive: None,
                },
                Arc::new(Mutex::new(0)),
            ),
        )]))
        .await;

        // Trim out 'http://' from the endpoint provided by the fake server
        let _e = ScopedEnv::set(
            super::GCE_METADATA_HOST_ENV_VAR,
            endpoint.strip_prefix("http://").unwrap_or(&endpoint),
        );
        let mdsc = Builder::default()
            .with_scopes(["scope1", "scope2"])
            .build()
            .unwrap();
        let token = mdsc.token().await.unwrap();
        let _e = ScopedEnv::remove(super::GCE_METADATA_HOST_ENV_VAR);

        assert_eq!(token.token, "test-access-token");
    }

    #[tokio::test]
    #[parallel]
    async fn get_default_service_account_info_success() {
        let service_account_info = ServiceAccountInfo {
            email: "test@test.com".to_string(),
            scopes: Some(vec!["scope 1".to_string(), "scope 2".to_string()]),
            aliases: None,
        };
        let service_account_info_json = serde_json::to_value(service_account_info.clone()).unwrap();
        let (endpoint, _server) = start(Handlers::from([(
            MDS_DEFAULT_URI.to_string(),
            (
                StatusCode::OK,
                service_account_info_json,
                TokenQueryParams {
                    scopes: None,
                    recursive: Some("true".to_string()),
                },
                Arc::new(Mutex::new(0)),
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
    #[parallel]
    async fn get_service_account_info_server_error() {
        let (endpoint, _server) = start(Handlers::from([(
            MDS_DEFAULT_URI.to_string(),
            (
                StatusCode::SERVICE_UNAVAILABLE,
                serde_json::to_value("try again").unwrap(),
                TokenQueryParams {
                    scopes: None,
                    recursive: Some("true".to_string()),
                },
                Arc::new(Mutex::new(0)),
            ),
        )]))
        .await;

        let request = Client::new();
        let token_provider = MDSAccessTokenProvider::builder().endpoint(endpoint).build();

        let result = token_provider.get_service_account_info(&request).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    #[parallel]
    async fn headers_success_with_quota_project() -> TestResult {
        let scopes = ["scope1".to_string(), "scope2".to_string()];
        let response = MDSTokenResponse {
            access_token: "test-access-token".to_string(),
            expires_in: Some(3600),
            token_type: "test-token-type".to_string(),
        };
        let response_body = serde_json::to_value(&response).unwrap();

        let (endpoint, _server) = start(Handlers::from([(
            format!("{}/token", MDS_DEFAULT_URI),
            (
                StatusCode::OK,
                response_body,
                TokenQueryParams {
                    scopes: Some(scopes.join(",")),
                    recursive: None,
                },
                Arc::new(Mutex::new(0)),
            ),
        )]))
        .await;

        let mdsc = Builder::default()
            .with_scopes(["scope1", "scope2"])
            .with_endpoint(endpoint)
            .with_quota_project_id("test-project")
            .build()?;

        let headers: Vec<HV> = HV::from(mdsc.headers().await.unwrap());
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
    #[parallel]
    async fn token_caching() -> TestResult {
        let scopes = vec!["scope1".to_string()];
        let response = MDSTokenResponse {
            access_token: "test-access-token".to_string(),
            expires_in: Some(3600),
            token_type: "test-token-type".to_string(),
        };
        let response_body = serde_json::to_value(&response).unwrap();

        let call_count = Arc::new(Mutex::new(0));
        let (endpoint, _server) = start(Handlers::from([(
            format!("{}/token", MDS_DEFAULT_URI),
            (
                StatusCode::OK,
                response_body,
                TokenQueryParams {
                    scopes: Some(scopes.join(",")),
                    recursive: None,
                },
                call_count.clone(),
            ),
        )]))
        .await;

        let mdsc = Builder::default()
            .with_scopes(scopes)
            .with_endpoint(endpoint)
            .build()?;
        let token = mdsc.token().await?;
        assert_eq!(token.token, "test-access-token");
        let token = mdsc.token().await?;
        assert_eq!(token.token, "test-access-token");

        // validate that the inner token provider is called only once
        assert_eq!(*call_count.lock().unwrap(), 1);

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[parallel]
    async fn token_provider_full() -> TestResult {
        let scopes = vec!["scope1".to_string()];
        let response = MDSTokenResponse {
            access_token: "test-access-token".to_string(),
            expires_in: Some(3600),
            token_type: "test-token-type".to_string(),
        };
        let response_body = serde_json::to_value(&response).unwrap();

        let (endpoint, _server) = start(Handlers::from([(
            format!("{}/token", MDS_DEFAULT_URI),
            (
                StatusCode::OK,
                response_body,
                TokenQueryParams {
                    scopes: Some(scopes.join(",")),
                    recursive: None,
                },
                Arc::new(Mutex::new(0)),
            ),
        )]))
        .await;
        println!("endpoint = {endpoint}");

        let mdsc = Builder::default()
            .with_scopes(scopes)
            .with_endpoint(endpoint)
            .build()?;
        let now = std::time::Instant::now();
        let token = mdsc.token().await?;
        assert_eq!(token.token, "test-access-token");
        assert_eq!(token.token_type, "test-token-type");
        assert!(
            token
                .expires_at
                .is_some_and(|d| d >= now + Duration::from_secs(3600))
        );

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[parallel]
    async fn token_provider_full_no_scopes() -> TestResult {
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

        let (endpoint, _server) = start(Handlers::from([
            (
                MDS_DEFAULT_URI.to_string(),
                (
                    StatusCode::OK,
                    service_account_info_json,
                    TokenQueryParams {
                        scopes: None,
                        recursive: Some("true".to_string()),
                    },
                    Arc::new(Mutex::new(0)),
                ),
            ),
            (
                format!("{}/token", MDS_DEFAULT_URI),
                (
                    StatusCode::OK,
                    response_body,
                    TokenQueryParams {
                        scopes: Some(scopes.join(",")),
                        recursive: None,
                    },
                    Arc::new(Mutex::new(0)),
                ),
            ),
        ]))
        .await;
        println!("endpoint = {endpoint}");

        let mdsc = Builder::default().with_endpoint(endpoint).build()?;
        let now = std::time::Instant::now();
        let token = mdsc.token().await?;
        assert_eq!(token.token, "test-access-token");
        assert_eq!(token.token_type, "test-token-type");
        assert!(
            token
                .expires_at
                .is_some_and(|d| d >= now + Duration::from_secs(3600))
        );

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[parallel]
    async fn token_provider_partial() -> TestResult {
        let scopes = vec!["scope1".to_string()];
        let response = MDSTokenResponse {
            access_token: "test-access-token".to_string(),
            expires_in: None,
            token_type: "test-token-type".to_string(),
        };
        let response_body = serde_json::to_value(&response).unwrap();
        let (endpoint, _server) = start(Handlers::from([(
            format!("{}/token", MDS_DEFAULT_URI),
            (
                StatusCode::OK,
                response_body,
                TokenQueryParams {
                    scopes: Some(scopes.join(",")),
                    recursive: None,
                },
                Arc::new(Mutex::new(0)),
            ),
        )]))
        .await;
        println!("endpoint = {endpoint}");

        let mdsc = Builder::default()
            .with_endpoint(endpoint)
            .with_scopes(scopes)
            .build()?;
        let token = mdsc.token().await?;
        assert_eq!(token.token, "test-access-token");
        assert_eq!(token.token_type, "test-token-type");
        assert_eq!(token.expires_at, None);

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[parallel]
    async fn token_provider_retryable_error() -> TestResult {
        let scopes = vec!["scope1".to_string()];
        let (endpoint, _server) = start(Handlers::from([(
            format!("{}/token", MDS_DEFAULT_URI),
            (
                StatusCode::SERVICE_UNAVAILABLE,
                serde_json::to_value("try again")?,
                TokenQueryParams {
                    scopes: Some(scopes.join(",")),
                    recursive: None,
                },
                Arc::new(Mutex::new(0)),
            ),
        )]))
        .await;

        let mdsc = Builder::default()
            .with_endpoint(endpoint)
            .with_scopes(scopes)
            .build()?;
        let e = mdsc.token().await.err().unwrap();
        assert!(e.is_retryable());
        assert!(e.source().unwrap().to_string().contains("try again"));

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[parallel]
    async fn token_provider_nonretryable_error() -> TestResult {
        let scopes = vec!["scope1".to_string()];
        let (endpoint, _server) = start(Handlers::from([(
            format!("{}/token", MDS_DEFAULT_URI),
            (
                StatusCode::UNAUTHORIZED,
                serde_json::to_value("epic fail".to_string())?,
                TokenQueryParams {
                    scopes: Some(scopes.join(",")),
                    recursive: None,
                },
                Arc::new(Mutex::new(0)),
            ),
        )]))
        .await;

        let mdsc = Builder::default()
            .with_endpoint(endpoint)
            .with_scopes(scopes)
            .build()?;

        let e = mdsc.token().await.err().unwrap();
        assert!(!e.is_retryable());
        assert!(e.source().unwrap().to_string().contains("epic fail"));

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[parallel]
    async fn token_provider_malformed_response_is_nonretryable() -> TestResult {
        let scopes = vec!["scope1".to_string()];
        let (endpoint, _server) = start(Handlers::from([(
            format!("{}/token", MDS_DEFAULT_URI),
            (
                StatusCode::OK,
                serde_json::to_value("bad json".to_string())?,
                TokenQueryParams {
                    scopes: Some(scopes.join(",")),
                    recursive: None,
                },
                Arc::new(Mutex::new(0)),
            ),
        )]))
        .await;

        let mdsc = Builder::default()
            .with_endpoint(endpoint)
            .with_scopes(scopes)
            .build()?;

        let e = mdsc.token().await.err().unwrap();
        assert!(!e.is_retryable());

        Ok(())
    }

    #[tokio::test]
    async fn get_default_universe_domain_success() -> TestResult {
        let universe_domain_response = Builder::default().build()?.universe_domain().await.unwrap();
        assert_eq!(universe_domain_response, DEFAULT_UNIVERSE_DOMAIN);
        Ok(())
    }

    #[tokio::test]
    async fn get_custom_universe_domain_success() -> TestResult {
        let universe_domain = "test-universe";
        let universe_domain_response = Builder::default()
            .with_universe_domain(universe_domain)
            .build()?
            .universe_domain()
            .await
            .unwrap();
        assert_eq!(universe_domain_response, universe_domain);

        Ok(())
    }
}
