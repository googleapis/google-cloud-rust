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
//! # Example
//! ```
//! # use google_cloud_auth::credentials::mds::Builder;
//! # use google_cloud_auth::credentials::Credentials;
//! # use http::Extensions;
//! # tokio_test::block_on(async {
//! let credentials: Credentials = Builder::default()
//!     .with_quota_project_id("my-quota-project")
//!     .build()?;
//! let headers = credentials.headers(Extensions::new()).await?;
//! println!("Headers: {headers:?}");
//! # Ok::<(), anyhow::Error>(())
//! # });
//! ```
//!
//! [Cloud Run]: https://cloud.google.com/run
//! [default service account]: https://cloud.google.com/iam/docs/service-account-types#default
//! [gce-link]: https://cloud.google.com/products/compute
//! [gke-link]: https://cloud.google.com/kubernetes-engine
//! [Metadata Service]: https://cloud.google.com/compute/docs/metadata/overview

use crate::credentials::dynamic::CredentialsProvider;
use crate::credentials::{
    BackoffPolicyArg, CacheableResource, Credentials, DEFAULT_UNIVERSE_DOMAIN, RetryPolicyArg,
    RetryThrottlerArg,
};
use crate::errors::CredentialsError;
use crate::headers_util::build_cacheable_headers;
use crate::retry::{Builder as RetryTokenProviderBuilder, TokenProviderWithRetry};
use crate::token::{CachedTokenProvider, Token, TokenProvider};
use crate::token_cache::TokenCache;
use crate::{BuildResult, Result};
use async_trait::async_trait;
use bon::Builder;
use http::{Extensions, HeaderMap, HeaderValue};
use reqwest::Client;
use std::default::Default;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;

const METADATA_FLAVOR_VALUE: &str = "Google";
const METADATA_FLAVOR: &str = "metadata-flavor";
const METADATA_ROOT: &str = "http://metadata.google.internal";
const MDS_DEFAULT_URI: &str = "/computeMetadata/v1/instance/service-accounts/default";
const GCE_METADATA_HOST_ENV_VAR: &str = "GCE_METADATA_HOST";
// TODO(#2235) - Improve this message by talking about retries when really running with MDS
const MDS_NOT_FOUND_ERROR: &str = concat!(
    "Could not fetch an auth token to authenticate with Google Cloud. ",
    "The most common reason for this problem is that you are not running in a Google Cloud Environment ",
    "and you have not configured local credentials for development and testing. ",
    "To setup local credentials, run `gcloud auth application-default login`. ",
    "More information on how to authenticate client libraries can be found at https://cloud.google.com/docs/authentication/client-libraries"
);

#[derive(Debug)]
struct MDSCredentials<T>
where
    T: CachedTokenProvider,
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
    created_by_adc: bool,
    retry_builder: RetryTokenProviderBuilder<MDSAccessTokenProvider>,
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
    /// Configure the retry policy for fetching tokens.
    ///
    /// The authentication library can automatically retry operations that fail. The
    /// retry policy controls how to handle retryable and non-retryable errors, and sets
    /// limits on the number of attempts or the total time spent retrying.
    ///
    /// ```
    /// # use google_cloud_auth::credentials::mds::Builder;
    /// # use google_cloud_auth::gax;
    /// # use gax::retry_policy;
    /// # use gax::retry_policy::RetryPolicyExt;
    /// # tokio_test::block_on(async {
    /// let credentials = Builder::default()
    ///     .with_retry_policy(retry_policy::AlwaysRetry.with_attempt_limit(3))
    ///     .build();
    /// # });
    /// ```
    pub fn with_retry_policy<V: Into<RetryPolicyArg>>(mut self, v: V) -> Self {
        self.retry_builder = self.retry_builder.with_retry_policy(v.into().0);
        self
    }

    /// Configure the retry backoff policy.
    ///
    /// The authentication library can automatically retry operations that fail. The
    /// backoff policy controls how long to wait in between retry attempts.
    ///
    /// ```
    /// # use google_cloud_auth::credentials::mds::Builder;
    /// # use google_cloud_auth::gax;
    /// # use gax::exponential_backoff::ExponentialBackoffBuilder;
    /// # use std::time::Duration;
    /// # tokio_test::block_on(async {
    /// let policy = ExponentialBackoffBuilder::new()
    ///     .with_initial_delay(Duration::from_millis(100))
    ///     .with_maximum_delay(Duration::from_secs(5))
    ///     .with_scaling(4.0)
    ///     .build().expect("well-known policy values should succeed");
    /// let credentials = Builder::default()
    ///     .with_backoff_policy(policy)
    ///     .build();
    /// # });
    /// ```
    pub fn with_backoff_policy<V: Into<BackoffPolicyArg>>(mut self, v: V) -> Self {
        self.retry_builder = self.retry_builder.with_backoff_policy(v.into().0);
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
    /// [Addressing Cascading Failures]: https://sre.google/sre-book/addressing-cascading-failures/
    ///
    /// ```
    /// # use google_cloud_auth::credentials::mds::Builder;
    /// # use google_cloud_auth::gax;
    /// # use gax::retry_throttler::AdaptiveThrottler;
    /// # tokio_test::block_on(async {
    /// let credentials = Builder::default()
    ///     .with_retry_throttler(AdaptiveThrottler::new(2.0)
    ///         .expect("well-known policy values should succeed"))
    ///     .build();
    /// # });
    /// ```
    pub fn with_retry_throttler<V: Into<RetryThrottlerArg>>(mut self, v: V) -> Self {
        self.retry_builder = self.retry_builder.with_retry_throttler(v.into().0);
        self
    }

    // This method is used to build mds credentials from ADC
    pub(crate) fn from_adc() -> Self {
        Self {
            created_by_adc: true,
            ..Default::default()
        }
    }

    fn build_token_provider(self) -> BuildResult<TokenProviderWithRetry<MDSAccessTokenProvider>> {
        let final_endpoint: String;
        let endpoint_overridden: bool;

        // Determine the endpoint and whether it was overridden
        if let Ok(host_from_env) = std::env::var(GCE_METADATA_HOST_ENV_VAR) {
            // Check GCE_METADATA_HOST environment variable first
            final_endpoint = format!("http://{host_from_env}");
            endpoint_overridden = true;
        } else if let Some(builder_endpoint) = self.endpoint {
            // Else, check if an endpoint was provided to the mds::Builder
            final_endpoint = builder_endpoint;
            endpoint_overridden = true;
        } else {
            // Else, use the default metadata root
            final_endpoint = METADATA_ROOT.to_string();
            endpoint_overridden = false;
        };

        let tp = MDSAccessTokenProvider::builder()
            .endpoint(final_endpoint)
            .maybe_scopes(self.scopes)
            .endpoint_overridden(endpoint_overridden)
            .created_by_adc(self.created_by_adc)
            .build();
        self.retry_builder.with_token_provider(tp).build()
    }

    /// Returns a [Credentials] instance with the configured settings.
    pub fn build(self) -> BuildResult<Credentials> {
        let mdsc = MDSCredentials {
            quota_project_id: self.quota_project_id.clone(),
            universe_domain: self.universe_domain.clone(),
            token_provider: TokenCache::new(self.build_token_provider()?),
        };
        Ok(Credentials {
            inner: Arc::new(mdsc),
        })
    }
}

#[async_trait::async_trait]
impl<T> CredentialsProvider for MDSCredentials<T>
where
    T: CachedTokenProvider,
{
    async fn headers(&self, extensions: Extensions) -> Result<CacheableResource<HeaderMap>> {
        let cached_token = self.token_provider.token(extensions).await?;
        build_cacheable_headers(&cached_token, &self.quota_project_id)
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
    endpoint_overridden: bool,
    created_by_adc: bool,
}

impl MDSAccessTokenProvider {
    // During ADC, if no credentials are found in the well-known location and the GOOGLE_APPLICATION_CREDENTIALS
    // environment variable is not set, we default to MDS credentials without checking if the code is really
    // running in an environment with MDS. To help users who got to this state because of lack of credentials
    // setup on their machines, we provide a detailed error message to them talking about local setup and other
    // auth mechanisms available to them.
    // If the endpoint is overridden, even if ADC was used to create the MDS credentials, we do not give a detailed
    // error message because they deliberately wanted to use an MDS.
    fn error_message(&self) -> &str {
        if self.use_adc_message() {
            MDS_NOT_FOUND_ERROR
        } else {
            "failed to fetch token"
        }
    }

    fn use_adc_message(&self) -> bool {
        self.created_by_adc && !self.endpoint_overridden
    }
}

#[async_trait]
impl TokenProvider for MDSAccessTokenProvider {
    async fn token(&self) -> Result<Token> {
        let client = Client::new();
        let request = client
            .get(format!("{}{}/token", self.endpoint, MDS_DEFAULT_URI))
            .header(
                METADATA_FLAVOR,
                HeaderValue::from_static(METADATA_FLAVOR_VALUE),
            );
        // Use the `scopes` option if set, otherwise let the MDS use the default
        // scopes.
        let scopes = self.scopes.as_ref().map(|v| v.join(","));
        let request = scopes
            .into_iter()
            .fold(request, |r, s| r.query(&[("scopes", s)]));

        // If the connection to MDS was not successful, it is useful to retry when really
        // running on MDS environments and not useful if there is no MDS. We will mark the error
        // as retryable and let the retry policy determine whether to retry or not. Whenever we
        // define a default retry policy, we can skip retrying this case.
        let response = request
            .send()
            .await
            .map_err(|e| crate::errors::from_http_error(e, self.error_message()))?;
        // Process the response
        if !response.status().is_success() {
            let err = crate::errors::from_http_response(response, self.error_message()).await;
            return Err(err);
        }
        let response = response.json::<MDSTokenResponse>().await.map_err(|e| {
            // Decoding errors are not transient. Typically they indicate a badly
            // configured MDS endpoint, or DNS redirecting the request to a random
            // server, e.g., ISPs that redirect unknown services to HTTP.
            CredentialsError::from_source(!e.is_decode(), e)
        })?;
        let token = Token {
            token: response.access_token,
            token_type: response.token_type,
            expires_at: response
                .expires_in
                .map(|d| Instant::now() + Duration::from_secs(d)),
            metadata: None,
        };
        Ok(token)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::credentials::QUOTA_PROJECT_KEY;
    use crate::credentials::test::{
        get_headers_from_cache, get_token_from_headers, get_token_type_from_headers,
    };
    use crate::errors;
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
    use test_case::test_case;
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
        let default_endpoint_address = Url::parse(&format!("{METADATA_ROOT}{MDS_DEFAULT_URI}"));
        assert!(default_endpoint_address.is_ok());

        let token_endpoint_address = Url::parse(&format!("{METADATA_ROOT}{MDS_DEFAULT_URI}/token"));
        assert!(token_endpoint_address.is_ok());
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

        let mdsc = MDSCredentials {
            quota_project_id: None,
            universe_domain: None,
            token_provider: TokenCache::new(mock),
        };

        let mut extensions = Extensions::new();
        let cached_headers = mdsc.headers(extensions.clone()).await.unwrap();
        let (headers, entity_tag) = match cached_headers {
            CacheableResource::New { entity_tag, data } => (data, entity_tag),
            CacheableResource::NotModified => unreachable!("expecting new headers"),
        };
        let token = headers.get(AUTHORIZATION).unwrap();
        assert_eq!(headers.len(), 1, "{headers:?}");
        assert_eq!(token, HeaderValue::from_static("Bearer test-token"));
        assert!(token.is_sensitive());

        extensions.insert(entity_tag);

        let cached_headers = mdsc.headers(extensions).await?;

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

        let mdsc = MDSCredentials {
            quota_project_id: None,
            universe_domain: None,
            token_provider: TokenCache::new(mock),
        };
        assert!(mdsc.headers(Extensions::new()).await.is_err());
    }

    #[test]
    fn error_message_with_adc() {
        let provider = MDSAccessTokenProvider::builder()
            .endpoint("http://127.0.0.1")
            .created_by_adc(true)
            .endpoint_overridden(false)
            .build();

        let want = MDS_NOT_FOUND_ERROR;
        let got = provider.error_message();
        assert!(got.contains(want), "{got}, {provider:?}");
    }

    #[test_case(false, false)]
    #[test_case(false, true)]
    #[test_case(true, true)]
    fn error_message_without_adc(adc: bool, overridden: bool) {
        let provider = MDSAccessTokenProvider::builder()
            .endpoint("http://127.0.0.1")
            .created_by_adc(adc)
            .endpoint_overridden(overridden)
            .build();

        let not_want = MDS_NOT_FOUND_ERROR;
        let got = provider.error_message();
        assert!(!got.contains(not_want), "{got}, {provider:?}");
    }

    #[tokio::test]
    #[serial]
    async fn adc_no_mds() -> TestResult {
        let err = Builder::from_adc()
            .build_token_provider()?
            .token()
            .await
            .unwrap_err();

        assert!(err.is_transient(), "{err:?}");
        assert!(
            err.to_string().contains("application-default"),
            "display={err}, debug={err:?}"
        );
        let source = err
            .source()
            .and_then(|e| e.downcast_ref::<reqwest::Error>());
        assert!(matches!(source, Some(e) if e.status().is_none()), "{err:?}");

        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn adc_overridden_mds() -> TestResult {
        let _e = ScopedEnv::set(super::GCE_METADATA_HOST_ENV_VAR, "metadata.overridden");

        let err = Builder::from_adc()
            .build_token_provider()?
            .token()
            .await
            .unwrap_err();

        let _e = ScopedEnv::remove(super::GCE_METADATA_HOST_ENV_VAR);

        assert!(err.is_transient(), "{err:?}");
        assert!(
            !err.to_string().contains("application-default"),
            "display={err}, debug={err:?}"
        );
        let source = err
            .source()
            .and_then(|e| e.downcast_ref::<reqwest::Error>());
        assert!(matches!(source, Some(e) if e.status().is_none()), "{err:?}");

        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn builder_no_mds() -> TestResult {
        let e = Builder::default()
            .build_token_provider()?
            .token()
            .await
            .err()
            .unwrap();

        assert!(e.is_transient(), "{e:?}");
        assert!(
            !format!("{:?}", e.source()).contains("application-default"),
            "{e:?}"
        );

        Ok(())
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
            format!("{MDS_DEFAULT_URI}/token"),
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
        let headers = mdsc.headers(Extensions::new()).await.unwrap();
        let _e = ScopedEnv::remove(super::GCE_METADATA_HOST_ENV_VAR);

        assert_eq!(
            get_token_from_headers(headers).unwrap(),
            "test-access-token"
        );
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
            format!("{MDS_DEFAULT_URI}/token"),
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

        let headers = get_headers_from_cache(mdsc.headers(Extensions::new()).await.unwrap())?;
        let token = headers.get(AUTHORIZATION).unwrap();
        let quota_project = headers.get(QUOTA_PROJECT_KEY).unwrap();

        assert_eq!(headers.len(), 2, "{headers:?}");
        assert_eq!(
            token,
            HeaderValue::from_static("test-token-type test-access-token")
        );
        assert!(token.is_sensitive());
        assert_eq!(quota_project, HeaderValue::from_static("test-project"));
        assert!(!quota_project.is_sensitive());

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
            format!("{MDS_DEFAULT_URI}/token"),
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
        let headers = mdsc.headers(Extensions::new()).await?;
        assert_eq!(
            get_token_from_headers(headers).unwrap(),
            "test-access-token"
        );
        let headers = mdsc.headers(Extensions::new()).await?;
        assert_eq!(
            get_token_from_headers(headers).unwrap(),
            "test-access-token"
        );

        // validate that the inner token provider is called only once
        assert_eq!(*call_count.lock().unwrap(), 1);

        Ok(())
    }

    #[tokio::test(start_paused = true)]
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
            format!("{MDS_DEFAULT_URI}/token"),
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

        let token = Builder::default()
            .with_endpoint(endpoint)
            .with_scopes(scopes)
            .build_token_provider()?
            .token()
            .await?;

        let now = tokio::time::Instant::now();
        assert_eq!(token.token, "test-access-token");
        assert_eq!(token.token_type, "test-token-type");
        assert!(
            token
                .expires_at
                .is_some_and(|d| d >= now + Duration::from_secs(3600))
        );

        Ok(())
    }

    #[tokio::test(start_paused = true)]
    #[parallel]
    async fn token_provider_full_no_scopes() -> TestResult {
        let response = MDSTokenResponse {
            access_token: "test-access-token".to_string(),
            expires_in: Some(3600),
            token_type: "test-token-type".to_string(),
        };
        let response_body = serde_json::to_value(&response).unwrap();

        let (endpoint, _server) = start(Handlers::from([(
            format!("{MDS_DEFAULT_URI}/token"),
            (
                StatusCode::OK,
                response_body,
                TokenQueryParams {
                    scopes: None,
                    recursive: None,
                },
                Arc::new(Mutex::new(0)),
            ),
        )]))
        .await;
        println!("endpoint = {endpoint}");
        let token = Builder::default()
            .with_endpoint(endpoint)
            .build_token_provider()?
            .token()
            .await?;

        let now = Instant::now();
        assert_eq!(token.token, "test-access-token");
        assert_eq!(token.token_type, "test-token-type");
        assert!(
            token
                .expires_at
                .is_some_and(|d| d == now + Duration::from_secs(3600))
        );

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[parallel]
    async fn credential_provider_full() -> TestResult {
        let scopes = vec!["scope1".to_string()];
        let response = MDSTokenResponse {
            access_token: "test-access-token".to_string(),
            expires_in: None,
            token_type: "test-token-type".to_string(),
        };
        let response_body = serde_json::to_value(&response).unwrap();
        let (endpoint, _server) = start(Handlers::from([(
            format!("{MDS_DEFAULT_URI}/token"),
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
        let headers = mdsc.headers(Extensions::new()).await?;
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
    #[parallel]
    async fn credentials_headers_retryable_error() -> TestResult {
        let scopes = vec!["scope1".to_string()];
        let (endpoint, _server) = start(Handlers::from([(
            format!("{MDS_DEFAULT_URI}/token"),
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
        let err = mdsc.headers(Extensions::new()).await.unwrap_err();
        assert!(err.is_transient());
        assert!(err.to_string().contains("try again"), "{err:?}");
        let source = err
            .source()
            .and_then(|e| e.downcast_ref::<reqwest::Error>());
        assert!(
            matches!(source, Some(e) if e.status() == Some(StatusCode::SERVICE_UNAVAILABLE)),
            "{err:?}"
        );

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[parallel]
    async fn credentials_headers_nonretryable_error() -> TestResult {
        let scopes = vec!["scope1".to_string()];
        let (endpoint, _server) = start(Handlers::from([(
            format!("{MDS_DEFAULT_URI}/token"),
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

        let err = mdsc.headers(Extensions::new()).await.unwrap_err();
        assert!(!err.is_transient());
        assert!(err.to_string().contains("epic fail"), "{err:?}");
        let source = err
            .source()
            .and_then(|e| e.downcast_ref::<reqwest::Error>());
        assert!(
            matches!(source, Some(e) if e.status() == Some(StatusCode::UNAUTHORIZED)),
            "{err:?}"
        );

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[parallel]
    async fn credentials_headers_malformed_response_is_nonretryable() -> TestResult {
        let scopes = vec!["scope1".to_string()];
        let (endpoint, _server) = start(Handlers::from([(
            format!("{MDS_DEFAULT_URI}/token"),
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

        let e = mdsc.headers(Extensions::new()).await.err().unwrap();
        assert!(!e.is_transient());

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
