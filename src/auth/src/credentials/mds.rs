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
//! # use google_cloud_auth::credentials::Credential;
//! # use google_cloud_auth::errors::CredentialError;
//! # tokio_test::block_on(async {
//! let credential: Credential = Builder::default().quota_project_id("my-quota-project").build();
//! let token = credential.get_token().await?;
//! println!("Token: {}", token.token);
//! # Ok::<(), CredentialError>(())
//! # });
//! ```
//!
//! [Cloud Run]: https://cloud.google.com/run
//! [default service account]: https://cloud.google.com/iam/docs/service-account-types#default
//! [gce-link]: https://cloud.google.com/products/compute
//! [gke-link]: https://cloud.google.com/kubernetes-engine
//! [Metadata Service]: https://cloud.google.com/compute/docs/metadata/overview

use crate::credentials::dynamic::CredentialTrait;
use crate::credentials::{Credential, DEFAULT_UNIVERSE_DOMAIN, QUOTA_PROJECT_KEY, Result};
use crate::errors::{CredentialError, is_retryable};
use crate::token::{Token, TokenProvider};
use async_trait::async_trait;
use bon::Builder;
use http::header::{AUTHORIZATION, HeaderName, HeaderValue};
use reqwest::{Client, StatusCode};
use std::default::Default;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Notify, watch};

const METADATA_FLAVOR_VALUE: &str = "Google";
const METADATA_FLAVOR: &str = "metadata-flavor";
const METADATA_ROOT: &str = "http://metadata.google.internal/";

pub(crate) fn new() -> Credential {
    Builder::default().build()
}

#[derive(Debug)]
struct MDSCredential<T>
where
    T: TokenProvider,
{
    quota_project_id: Option<String>,
    universe_domain_rx: watch::Receiver<Option<Result<String>>>,
    wakeup_signal: Arc<Notify>,
    token_provider: T,
}

/// Creates [Credential] instances backed by the [Metadata Service].
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
    /// Sets the endpoint for this credential.
    ///
    /// If not set, the credentials use `http://metadata.google.internal/`.
    pub fn endpoint<S: Into<String>>(mut self, endpoint: S) -> Self {
        self.endpoint = Some(endpoint.into());
        self
    }

    /// Set the [quota project] for this credential.
    ///
    /// In some services, you can use a service account in
    /// one project for authentication and authorization, and charge
    /// the usage to a different project. This may require that the
    /// service account has `serviceusage.services.use` permissions on the quota project.
    ///
    /// [quota project]: https://cloud.google.com/docs/quotas/quota-project
    pub fn quota_project_id<S: Into<String>>(mut self, quota_project_id: S) -> Self {
        self.quota_project_id = Some(quota_project_id.into());
        self
    }

    /// Sets the universe domain for this credential.
    ///
    /// Client libraries use `universe_domain` to determine
    /// the API endpoints to use for making requests.
    /// If not set, credentials fetch the `universe_domain` from the [Metadata Service]
    /// when a call to [get_universe_domain](Credential::get_universe_domain) is made.
    ///
    /// [Metadata Service]: https://cloud.google.com/compute/docs/metadata/overview
    pub fn universe_domain<S: Into<String>>(mut self, universe_domain: S) -> Self {
        self.universe_domain = Some(universe_domain.into());
        self
    }

    /// Sets the [scopes] for this credential.
    ///
    /// Metadata server issues tokens based on the requested scopes.
    /// If no scopes are specified, the credential defaults to all
    /// scopes configured for the [default service account] on the instance.
    ///
    /// [default service account]: https://cloud.google.com/iam/docs/service-account-types#default
    /// [scopes]: https://developers.google.com/identity/protocols/oauth2/scopes
    pub fn scopes<I, S>(mut self, scopes: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.scopes = Some(scopes.into_iter().map(|s| s.into()).collect());
        self
    }

    /// Returns a [Credential] instance with the configured settings.
    pub fn build(self) -> Credential {
        let endpoint = self.endpoint.clone().unwrap_or(METADATA_ROOT.to_string());

        let token_provider = MDSAccessTokenProvider::builder()
            .endpoint(endpoint.clone())
            .maybe_scopes(self.scopes)
            .build();

        let notify = Arc::new(Notify::new());
        let (universe_domain_tx, universe_domain_rx) =
            watch::channel::<Option<Result<String>>>(None);

        if let Some(universe_domain) = self.universe_domain {
            let _ = universe_domain_tx.send(Some(Ok(universe_domain)));
        } else {
            let notify_clone = notify.clone();
            tokio::spawn(async move {
                universe_domain_background_task(endpoint, universe_domain_tx, notify_clone).await;
            });
        }

        let mdsc = MDSCredential {
            quota_project_id: self.quota_project_id,
            token_provider,
            universe_domain_rx,
            wakeup_signal: notify,
        };

        Credential {
            inner: Arc::new(mdsc),
        }
    }
}

async fn get_universe_domain_from_mds(endpoint: &String) -> Result<String> {
    let client = Client::new();
    let request = client
        .get(format!("{}/universe/universe-domain", endpoint))
        .header(
            METADATA_FLAVOR,
            HeaderValue::from_static(METADATA_FLAVOR_VALUE),
        );

    let response = request.send().await.map_err(CredentialError::retryable)?;

    if !response.status().is_success() {
        let status = response.status();
        if status == StatusCode::NOT_FOUND {
            return Ok(DEFAULT_UNIVERSE_DOMAIN.to_string());
        }
        let body = response
            .text()
            .await
            .map_err(|e| CredentialError::new(is_retryable(status), e))?;
        return Err(CredentialError::from_str(
            is_retryable(status),
            format!("Failed to fetch universe domain. Status: {status}. Body: {body}"),
        ));
    }
    let universe_domain = response.text().await.map_err(CredentialError::retryable)?;

    // Earlier versions of MDS that supports universe_domain return empty string instead of GDU.
    if universe_domain.is_empty() {
        return Ok(DEFAULT_UNIVERSE_DOMAIN.to_string());
    }
    Ok(universe_domain)
}

async fn universe_domain_background_task(
    endpoint: String,
    universe_domain_tx: watch::Sender<Option<Result<String>>>,
    wakeup_signal: Arc<Notify>,
) {
    // Wait to be woken up
    let _ = wakeup_signal.notified().await;

    // Obtain the universe domain from the MDS.
    let universe_domain = get_universe_domain_from_mds(&endpoint).await;

    // Push it onto the watch channel.
    let _ = universe_domain_tx.send(Some(universe_domain)); // We ignore the error if the receiver is dropped.
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

    async fn get_universe_domain(&self) -> Result<String> {
        let mut universe_domain_rx = self.universe_domain_rx.clone();

        if universe_domain_rx.borrow_and_update().is_none() {
            self.wakeup_signal.notify_one();

            universe_domain_rx.changed().await.map_err(|e| {
                CredentialError::non_retryable_from_str(format!(
                    "Failed to read universe domain due to: {e}"
                ))
            })?;
        }

        return universe_domain_rx.borrow().clone().unwrap();
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
                "{}/computeMetadata/v1/instance/service-accounts/default/",
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
                "{}/computeMetadata/v1/instance/service-accounts/default/token",
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
    use axum::extract::State;
    use axum::response::IntoResponse;
    use axum::routing::get;
    use reqwest::StatusCode;
    use reqwest::header::HeaderMap;
    use serde::Deserialize;
    use std::collections::HashMap;
    use std::error::Error;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use tokio::task::JoinHandle;

    type TestResult = std::result::Result<(), Box<dyn std::error::Error>>;
    const MDS_TOKEN_URI: &str = "/computeMetadata/v1/instance/service-accounts/default/token";
    const UNIVERSE_DOMAIN_URI: &str = "/universe/universe-domain";

    // Define a struct to capture query parameters
    #[derive(Debug, Clone, Deserialize, PartialEq)]
    struct TokenQueryParams {
        scopes: Option<String>,
        recursive: Option<String>,
    }

    #[derive(Clone)]
    struct AppState {
        target_endpoint_counter: Arc<AtomicUsize>,
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

        let (_, universe_domain_rx) = watch::channel(None);

        let mdsc = MDSCredential {
            quota_project_id: None,
            universe_domain_rx,
            wakeup_signal: Arc::new(Notify::new()),
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

        let (_, universe_domain_rx) = watch::channel(None);

        let mdsc = MDSCredential {
            quota_project_id: None,
            universe_domain_rx,
            wakeup_signal: Arc::new(Notify::new()),
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

        let (_, universe_domain_rx) = watch::channel(None);

        let mdsc = MDSCredential {
            quota_project_id: None,
            universe_domain_rx,
            wakeup_signal: Arc::new(Notify::new()),
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

        let (_, universe_domain_rx) = watch::channel(None);
        let mdsc = MDSCredential {
            quota_project_id: None,
            universe_domain_rx,
            wakeup_signal: Arc::new(Notify::new()),
            token_provider: mock,
        };
        assert!(mdsc.get_headers().await.is_err());
    }

    async fn get_count_handler(
        State(state): State<AppState>, // Extract the shared state
    ) -> impl IntoResponse {
        // Read the current value of the counter atomically
        let current_count = state.target_endpoint_counter.load(Ordering::Relaxed);

        (StatusCode::OK, current_count.to_string())
    }

    fn handle_token_factory(
        response_code: StatusCode,
        response_headers: HeaderMap,
        response_body: String,
    ) -> impl IntoResponse {
        (response_code, response_headers, response_body).into_response()
    }

    // Starts a server running locally that responds on multiple paths.
    // Returns an (endpoint, server) pair.
    async fn start(
        path_handlers: HashMap<String, (StatusCode, String, TokenQueryParams)>,
    ) -> (String, JoinHandle<()>) {
        let mut app = axum::Router::new();
        for (path, (code, body, expected_query)) in path_handlers {
            let header_map = HeaderMap::new();
            let handler = move |Query(query): Query<TokenQueryParams>,
                                State(state): State<AppState>| {
                let code = code.clone();
                let body = body.clone();
                let header_map = header_map.clone();
                state
                    .target_endpoint_counter
                    .fetch_add(1, Ordering::Relaxed);
                async move {
                    assert_eq!(expected_query, query);
                    handle_token_factory(code, header_map, body)
                }
            };
            app = app.route(&path, get(handler));
        }
        let shared_state = AppState {
            target_endpoint_counter: Arc::new(AtomicUsize::new(0)), // Start counter at 0
        };
        let final_router = app
            .route("/count", get(get_count_handler))
            // Provide the shared state to all routes
            .with_state(shared_state);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(listener, final_router).await.unwrap();
        });
        (format!("http://{}:{}", addr.ip(), addr.port()), server)
    }

    #[tokio::test]
    async fn get_default_service_account_info_success() {
        let service_account = "default";
        let path = format!(
            "/computeMetadata/v1/instance/service-accounts/{}/",
            service_account
        );
        let service_account_info = ServiceAccountInfo {
            email: "test@test.com".to_string(),
            scopes: Some(vec!["scope 1".to_string(), "scope 2".to_string()]),
            aliases: None,
        };
        let service_account_info_response = serde_json::to_string(&service_account_info).unwrap();
        let (endpoint, _server) = start(HashMap::from([(
            path,
            (
                StatusCode::OK,
                service_account_info_response,
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
        let path = "/computeMetadata/v1/instance/service-accounts/default/".to_string();
        let (endpoint, _server) = start(HashMap::from([(
            path,
            (
                StatusCode::SERVICE_UNAVAILABLE,
                "try again".to_string(),
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
        let scopes = vec!["scope1".to_string(), "scope2".to_string()];
        let response = MDSTokenResponse {
            access_token: "test-access-token".to_string(),
            expires_in: Some(3600),
            token_type: "test-token-type".to_string(),
        };
        let response_body = serde_json::to_string(&response).unwrap();

        let (endpoint, _server) = start(HashMap::from([(
            MDS_TOKEN_URI.to_string(),
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

        let mdsc = Builder::default()
            .scopes(["scope1", "scope2"])
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
        let response_body = serde_json::to_string(&response).unwrap();

        let (endpoint, _server) = start(HashMap::from([(
            MDS_TOKEN_URI.to_string(),
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

        let mdsc = Builder::default().scopes(scopes).endpoint(endpoint).build();
        let now = std::time::Instant::now();
        let token = mdsc.get_token().await?;
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
    async fn token_provider_full_no_scopes() -> TestResult {
        let service_account_info_path =
            "/computeMetadata/v1/instance/service-accounts/default/".to_string();
        let scopes = vec!["scope 1".to_string(), "scope 2".to_string()];
        let service_account_info = ServiceAccountInfo {
            email: "test@test.com".to_string(),
            scopes: Some(scopes.clone()),
            aliases: None,
        };
        let service_account_info_response = serde_json::to_string(&service_account_info).unwrap();

        let response = MDSTokenResponse {
            access_token: "test-access-token".to_string(),
            expires_in: Some(3600),
            token_type: "test-token-type".to_string(),
        };
        let response_body = serde_json::to_string(&response).unwrap();

        let (endpoint, _server) = start(HashMap::from([
            (
                service_account_info_path,
                (
                    StatusCode::OK,
                    service_account_info_response,
                    TokenQueryParams {
                        scopes: None,
                        recursive: Some("true".to_string()),
                    },
                ),
            ),
            (
                MDS_TOKEN_URI.to_string(),
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

        let mdsc = Builder::default().endpoint(endpoint).build();
        let now = std::time::Instant::now();
        let token = mdsc.get_token().await?;
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
    async fn token_provider_partial() -> TestResult {
        let scopes = vec!["scope1".to_string()];
        let response = MDSTokenResponse {
            access_token: "test-access-token".to_string(),
            expires_in: None,
            token_type: "test-token-type".to_string(),
        };
        let response_body = serde_json::to_string(&response).unwrap();
        let (endpoint, _server) = start(HashMap::from([(
            MDS_TOKEN_URI.to_string(),
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

        let mdsc = Builder::default().endpoint(endpoint).scopes(scopes).build();
        let token = mdsc.get_token().await?;
        assert_eq!(token.token, "test-access-token");
        assert_eq!(token.token_type, "test-token-type");
        assert_eq!(token.expires_at, None);

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn token_provider_retryable_error() -> TestResult {
        let scopes = vec!["scope1".to_string()];
        let (endpoint, _server) = start(HashMap::from([(
            MDS_TOKEN_URI.to_string(),
            (
                StatusCode::SERVICE_UNAVAILABLE,
                "try again".to_string(),
                TokenQueryParams {
                    scopes: Some(scopes.join(",")),
                    recursive: None,
                },
            ),
        )]))
        .await;

        let mdsc = Builder::default().endpoint(endpoint).scopes(scopes).build();
        let e = mdsc.get_token().await.err().unwrap();
        assert!(e.is_retryable());
        assert!(e.source().unwrap().to_string().contains("try again"));

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn token_provider_nonretryable_error() -> TestResult {
        let scopes = vec!["scope1".to_string()];
        let (endpoint, _server) = start(HashMap::from([(
            MDS_TOKEN_URI.to_string(),
            (
                StatusCode::UNAUTHORIZED,
                "epic fail".to_string(),
                TokenQueryParams {
                    scopes: Some(scopes.join(",")),
                    recursive: None,
                },
            ),
        )]))
        .await;

        let mdsc = Builder::default().endpoint(endpoint).scopes(scopes).build();

        let e = mdsc.get_token().await.err().unwrap();
        assert!(!e.is_retryable());
        assert!(e.source().unwrap().to_string().contains("epic fail"));

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn token_provider_malformed_response_is_nonretryable() -> TestResult {
        let scopes = vec!["scope1".to_string()];
        let (endpoint, _server) = start(HashMap::from([(
            MDS_TOKEN_URI.to_string(),
            (
                StatusCode::OK,
                "bad json".to_string(),
                TokenQueryParams {
                    scopes: Some(scopes.join(",")),
                    recursive: None,
                },
            ),
        )]))
        .await;

        let mdsc = Builder::default().endpoint(endpoint).scopes(scopes).build();

        let e = mdsc.get_token().await.err().unwrap();
        assert!(!e.is_retryable());

        Ok(())
    }

    #[tokio::test]
    async fn get_default_universe_domain_success() {
        let (endpoint, _server) = start(HashMap::from([(
            UNIVERSE_DOMAIN_URI.to_string(),
            (
                StatusCode::NOT_FOUND,
                "not found".to_string(),
                TokenQueryParams {
                    scopes: None,
                    recursive: None,
                },
            ),
        )]))
        .await;
        let universe_domain_response = Builder::default()
            .endpoint(endpoint)
            .build()
            .get_universe_domain()
            .await
            .unwrap();
        assert_eq!(universe_domain_response, DEFAULT_UNIVERSE_DOMAIN);
    }

    #[tokio::test]
    async fn get_universe_domain_empty_response_success() {
        let (endpoint, _server) = start(HashMap::from([(
            UNIVERSE_DOMAIN_URI.to_string(),
            (
                StatusCode::OK,
                String::default(),
                TokenQueryParams {
                    scopes: None,
                    recursive: None,
                },
            ),
        )]))
        .await;
        let universe_domain_response = Builder::default()
            .endpoint(endpoint)
            .build()
            .get_universe_domain()
            .await
            .unwrap();
        assert_eq!(universe_domain_response, DEFAULT_UNIVERSE_DOMAIN);
    }

    #[tokio::test]
    async fn get_custom_universe_domain_success() {
        let universe_domain = "test-universe";
        let universe_domain_response = Builder::default()
            .universe_domain(universe_domain)
            .build()
            .get_universe_domain()
            .await
            .unwrap();
        assert_eq!(universe_domain_response, universe_domain);
    }

    #[tokio::test]
    async fn get_universe_domain_error() {
        let universe_domain_response = "invalid_response";

        let (endpoint, _server) = start(HashMap::from([(
            UNIVERSE_DOMAIN_URI.to_string(),
            (
                StatusCode::SERVICE_UNAVAILABLE,
                universe_domain_response.to_string(),
                TokenQueryParams {
                    scopes: None,
                    recursive: None,
                },
            ),
        )]))
        .await;

        let e = Builder::default()
            .endpoint(endpoint)
            .build()
            .get_universe_domain()
            .await
            .err()
            .unwrap();
        assert!(e.is_retryable(), "{e}");
        assert!(
            e.source()
                .unwrap()
                .to_string()
                .contains(universe_domain_response),
            "{e}"
        );
    }

    #[tokio::test]
    async fn get_universe_domain_dropped_watcher() {
        let mock = MockTokenProvider::new();

        let (universe_domain_tx, universe_domain_rx) = watch::channel(None);
        let mdsc = MDSCredential {
            quota_project_id: None,
            universe_domain_rx,
            wakeup_signal: Arc::new(Notify::new()),
            token_provider: mock,
        };
        drop(universe_domain_tx);
        let e = mdsc.get_universe_domain().await.err().unwrap();

        assert!(!e.is_retryable(), "{e}");
        assert!(
            e.source()
                .unwrap()
                .to_string()
                .contains("Failed to read universe domain due to: channel closed"),
            "{e}"
        );
    }

    async fn get_number_of_http_calls(endpoint: String) -> i32 {
        let client = Client::new();
        let request = client.get(format!("{}/count", endpoint));

        let response = request.send().await.unwrap();
        let number_of_calls = response.text().await.unwrap();
        number_of_calls.parse::<i32>().unwrap()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn get_universe_domain_herd_success() {
        let ud = "test-universe-domain";

        let (endpoint, _) = start(HashMap::from([(
            UNIVERSE_DOMAIN_URI.to_string(),
            (
                StatusCode::OK,
                ud.to_string(),
                TokenQueryParams {
                    scopes: None,
                    recursive: None,
                },
            ),
        )]))
        .await;

        let mdcs = Builder::default().endpoint(endpoint.clone()).build();

        // Spawn N tasks, all asking for a universe domain at once
        let tasks = (0..100)
            .map(|_| {
                let mdcs = mdcs.clone();
                tokio::spawn(async move { mdcs.get_universe_domain().await })
            })
            .collect::<Vec<_>>();

        // Wait for the N get_universe_domain requests to complete.
        for task in tasks {
            let actual = task.await.unwrap();
            assert!(actual.is_ok(), "{}", actual.err().unwrap());
            assert_eq!(actual.unwrap(), ud);
        }

        let calls = get_number_of_http_calls(endpoint).await;
        // Only one call to get_universe_domain_mds should have been made
        assert_eq!(calls, 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn get_universe_domain_herd_failure_shares_same_error() {
        let error = "invalid request";

        let (endpoint, _) = start(HashMap::from([(
            UNIVERSE_DOMAIN_URI.to_string(),
            (
                StatusCode::SERVICE_UNAVAILABLE,
                error.to_string(),
                TokenQueryParams {
                    scopes: None,
                    recursive: None,
                },
            ),
        )]))
        .await;

        let mdcs = Builder::default().endpoint(endpoint.clone()).build();

        // Spawn N tasks, all asking for a universe domain at once
        let tasks = (0..100)
            .map(|_| {
                let mdcs = mdcs.clone();
                tokio::spawn(async move { mdcs.get_universe_domain().await })
            })
            .collect::<Vec<_>>();

        // Wait for the N get_universe_domain requests to complete, verifying the returned error.
        for task in tasks {
            let actual = task.await.unwrap();
            assert!(actual.is_err(), "{:?}", actual.unwrap());
            let e = format!("{}", actual.err().unwrap());
            assert!(e.contains(error), "{e}");
        }

        let calls = get_number_of_http_calls(endpoint).await;
        // Only one call to get_universe_domain_mds should have been made
        assert_eq!(calls, 1);
    }
}
