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
//! ## Example: Creating credentials with a custom quota project
//!
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
//! ## Example: Creating credentials with custom retry behavior
//!
//! ```
//! # use google_cloud_auth::credentials::mds::Builder;
//! # use google_cloud_auth::credentials::Credentials;
//! # use http::Extensions;
//! # use std::time::Duration;
//! # tokio_test::block_on(async {
//! use gax::retry_policy::{AlwaysRetry, RetryPolicyExt};
//! use gax::exponential_backoff::ExponentialBackoff;
//! let backoff = ExponentialBackoff::default();
//! let credentials: Credentials = Builder::default()
//!     .with_retry_policy(AlwaysRetry.with_attempt_limit(3))
//!     .with_backoff_policy(backoff)
//!     .build()?;
//! let headers = credentials.headers(Extensions::new()).await?;
//! println!("Headers: {headers:?}");
//! # Ok::<(), anyhow::Error>(())
//! # });
//! ```
//!
//! [Application Default Credentials]: https://cloud.google.com/docs/authentication/application-default-credentials
//! [Cloud Run]: https://cloud.google.com/run
//! [default service account]: https://cloud.google.com/iam/docs/service-account-types#default
//! [gce-link]: https://cloud.google.com/products/compute
//! [gke-link]: https://cloud.google.com/kubernetes-engine
//! [Metadata Service]: https://cloud.google.com/compute/docs/metadata/overview

use crate::credentials::dynamic::CredentialsProvider;
use crate::credentials::{CacheableResource, Credentials};
use crate::errors::CredentialsError;
use crate::headers_util::build_cacheable_headers;
use crate::retry::{Builder as RetryTokenProviderBuilder, TokenProviderWithRetry};
use crate::token::{CachedTokenProvider, Token, TokenProvider};
use crate::token_cache::TokenCache;
use crate::{BuildResult, Result};
use async_trait::async_trait;
use bon::Builder;
use gax::backoff_policy::BackoffPolicyArg;
use gax::retry_policy::RetryPolicyArg;
use gax::retry_throttler::RetryThrottlerArg;
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
    created_by_adc: bool,
    retry_builder: RetryTokenProviderBuilder,
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
    /// The retry policy controls how to handle retries, and sets limits on
    /// the number of attempts or the total time spent retrying.
    ///
    /// ```
    /// # use google_cloud_auth::credentials::mds::Builder;
    /// # tokio_test::block_on(async {
    /// use gax::retry_policy::{AlwaysRetry, RetryPolicyExt};
    /// let credentials = Builder::default()
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
    /// # use google_cloud_auth::credentials::mds::Builder;
    /// # use std::time::Duration;
    /// # tokio_test::block_on(async {
    /// use gax::exponential_backoff::ExponentialBackoff;
    /// let policy = ExponentialBackoff::default();
    /// let credentials = Builder::default()
    ///     .with_backoff_policy(policy)
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
    /// # use google_cloud_auth::credentials::mds::Builder;
    /// # tokio_test::block_on(async {
    /// use gax::retry_throttler::AdaptiveThrottler;
    /// let credentials = Builder::default()
    ///     .with_retry_throttler(AdaptiveThrottler::default())
    ///     .build();
    /// # });
    /// ```
    pub fn with_retry_throttler<V: Into<RetryThrottlerArg>>(mut self, v: V) -> Self {
        self.retry_builder = self.retry_builder.with_retry_throttler(v.into());
        self
    }

    // This method is used to build mds credentials from ADC
    pub(crate) fn from_adc() -> Self {
        Self {
            created_by_adc: true,
            ..Default::default()
        }
    }

    fn build_token_provider(self) -> TokenProviderWithRetry<MDSAccessTokenProvider> {
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
        self.retry_builder.build(tp)
    }

    /// Returns a [Credentials] instance with the configured settings.
    pub fn build(self) -> BuildResult<Credentials> {
        let mdsc = MDSCredentials {
            quota_project_id: self.quota_project_id.clone(),
            token_provider: TokenCache::new(self.build_token_provider()),
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

pub mod idtoken {
    //! Types for fetching ID tokens from the metadata service.
    use std::sync::Arc;

    use super::{
        GCE_METADATA_HOST_ENV_VAR, MDS_DEFAULT_URI, METADATA_FLAVOR, METADATA_FLAVOR_VALUE,
        METADATA_ROOT,
    };
    use crate::Result;
    use crate::errors::CredentialsError;
    use crate::token::{Token, TokenProvider};
    use crate::{
        BuildResult,
        credentials::idtoken::{IDTokenCredentials, dynamic::IDTokenCredentialsProvider},
    };
    use async_trait::async_trait;
    use http::HeaderValue;
    use reqwest::Client;

    #[derive(Debug)]
    pub(crate) struct MDSCredentials<T>
    where
        T: TokenProvider,
    {
        token_provider: T,
    }

    #[async_trait]
    impl<T> IDTokenCredentialsProvider for MDSCredentials<T>
    where
        T: TokenProvider,
    {
        async fn id_token(&self) -> Result<Token> {
            self.token_provider.token().await
        }
    }

    /// Creates [`IDTokenCredentials`] instances that fetch ID tokens from the
    /// metadata service.
    ///
    /// ## Example
    ///
    /// ```
    /// # use google_cloud_auth::credentials::mds::idtoken::Builder;
    /// # tokio_test::block_on(async {
    /// let credentials = Builder::new("https://example.com")
    ///     .with_format("full")
    ///     .build();
    /// # assert!(credentials.is_ok());
    /// # });
    /// ```
    #[derive(Debug, Default)]
    pub struct Builder {
        endpoint: Option<String>,
        format: Option<String>,
        licenses: Option<String>,
        target_audience: String,
    }

    impl Builder {
        /// Creates a new `Builder`.
        ///
        /// The `target_audience` is a required parameter that specifies the
        /// intended audience of the ID token. This is typically the URL of the
        /// service that will be receiving the token.
        ///
        /// # Example
        /// ```
        /// # use google_cloud_auth::credentials::mds::idtoken::Builder;
        /// # tokio_test::block_on(async {
        /// let credentials = Builder::new("https://example.com")
        ///     .with_endpoint("https://metadata.google.foobar")
        ///     .build();
        /// # });
        /// ```
        pub fn new<S: Into<String>>(target_audience: S) -> Self {
            Builder {
                format: None,
                endpoint: None,
                licenses: None,
                target_audience: target_audience.into(),
            }
        }

        /// Sets the endpoint for this credentials.
        ///
        /// A trailing slash is significant, so specify the base URL without a trailing  
        /// slash. If not set, the credentials use `http://metadata.google.internal`.
        ///
        /// # Example
        /// ```
        /// # use google_cloud_auth::credentials::mds::idtoken::Builder;
        /// # tokio_test::block_on(async {
        /// let credentials = Builder::new("https://example.com")
        ///     .with_endpoint("https://metadata.google.foobar")
        ///     .build();
        /// # });
        /// ```
        pub fn with_endpoint<S: Into<String>>(mut self, endpoint: S) -> Self {
            self.endpoint = Some(endpoint.into());
            self
        }

        /// Sets the [format] of the token.
        ///
        /// Specifies whether or not the project and instance details are included in the payload.
        /// Specify `full` to include this information in the payload or `standard` to omit the information
        /// from the payload. The default value is `standard``.
        ///
        /// [format]: https://cloud.google.com/compute/docs/instances/verifying-instance-identity#token_format
        ///
        /// # Example
        /// ```
        /// # use google_cloud_auth::credentials::mds::idtoken::Builder;
        /// # tokio_test::block_on(async {
        /// let credentials = Builder::new("https://example.com")
        ///     .with_format("full")
        ///     .build();
        /// # });
        /// ```
        pub fn with_format<S: Into<String>>(mut self, format: S) -> Self {
            self.format = Some(format.into());
            self
        }

        /// Whether to include the [license codes] of the instance in the token.
        ///
        /// Specify `true` to include this information or `false` to omit this information from the payload.
        /// The default value is `false`. Has no effect unless format is `full`.
        ///
        /// [license codes]: https://cloud.google.com/compute/docs/reference/rest/v1/images/get#body.Image.FIELDS.license_code
        ///
        /// # Example
        /// ```
        /// # use google_cloud_auth::credentials::mds::idtoken::Builder;
        /// # tokio_test::block_on(async {
        /// let credentials = Builder::new("https://example.com")
        ///     .with_format("full")
        ///     .with_licenses(true)
        ///     .build();
        /// # });
        /// ```
        pub fn with_licenses(mut self, licenses: bool) -> Self {
            self.licenses = if licenses {
                Some("TRUE".to_string())
            } else {
                Some("FALSE".to_string())
            };
            self
        }

        fn build_token_provider(self) -> MDSTokenProvider {
            let final_endpoint: String;

            // Determine the endpoint and whether it was overridden
            if let Ok(host_from_env) = std::env::var(GCE_METADATA_HOST_ENV_VAR) {
                // Check GCE_METADATA_HOST environment variable first
                final_endpoint = format!("http://{host_from_env}");
            } else if let Some(builder_endpoint) = self.endpoint {
                // Else, check if an endpoint was provided to the mds::Builder
                final_endpoint = builder_endpoint;
            } else {
                // Else, use the default metadata root
                final_endpoint = METADATA_ROOT.to_string();
            };

            MDSTokenProvider {
                format: self.format,
                licenses: self.licenses,
                endpoint: final_endpoint,
                target_audience: self.target_audience,
            }
        }

        /// Returns an [`IDTokenCredentials`] instance with the configured
        /// settings.
        pub fn build(self) -> BuildResult<IDTokenCredentials> {
            let creds = MDSCredentials {
                token_provider: self.build_token_provider(),
            };
            Ok(IDTokenCredentials {
                inner: Arc::new(creds),
            })
        }
    }

    #[derive(Debug, Clone, Default)]
    struct MDSTokenProvider {
        endpoint: String,
        format: Option<String>,
        licenses: Option<String>,
        target_audience: String,
    }

    #[async_trait]
    impl TokenProvider for MDSTokenProvider {
        async fn token(&self) -> Result<Token> {
            let client = Client::new();
            let audience = self.target_audience.clone();
            let request = client
                .get(format!("{}{}/identity", self.endpoint, MDS_DEFAULT_URI))
                .header(
                    METADATA_FLAVOR,
                    HeaderValue::from_static(METADATA_FLAVOR_VALUE),
                )
                .query(&[("audience", audience)]);
            let request = self.format.iter().fold(request, |builder, format| {
                builder.query(&[("format", format)])
            });
            let request = self.licenses.iter().fold(request, |builder, licenses| {
                builder.query(&[("licenses", licenses)])
            });

            let response = request
                .send()
                .await
                .map_err(|e| crate::errors::from_http_error(e, "failed to fetch token"))?;

            if !response.status().is_success() {
                let err =
                    crate::errors::from_http_response(response, "failed to fetch token").await;
                return Err(err);
            }

            let token = response
                .text()
                .await
                .map_err(|e| CredentialsError::from_source(!e.is_decode(), e))?;

            Ok(Token {
                token,
                token_type: "Bearer".to_string(),
                // ID tokens from MDS do not have an expiry.
                expires_at: None,
                metadata: None,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::idtoken;
    use super::*;
    use crate::credentials::DEFAULT_UNIVERSE_DOMAIN;
    use crate::credentials::QUOTA_PROJECT_KEY;
    use crate::credentials::tests::{
        find_source_error, get_headers_from_cache, get_mock_auth_retry_policy,
        get_mock_backoff_policy, get_mock_retry_throttler, get_token_from_headers,
        get_token_type_from_headers,
    };
    use crate::errors;
    use crate::errors::CredentialsError;
    use crate::token::tests::MockTokenProvider;
    use http::HeaderValue;
    use http::header::AUTHORIZATION;
    use httptest::cycle;
    use httptest::matchers::{all_of, contains, request, url_decoded};
    use httptest::responders::{json_encoded, status_code};
    use httptest::{Expectation, Server};
    use reqwest::StatusCode;
    use scoped_env::ScopedEnv;
    use serial_test::{parallel, serial};
    use std::error::Error;
    use test_case::test_case;
    use url::Url;

    type TestResult = anyhow::Result<()>;

    #[tokio::test]
    #[parallel]
    async fn test_mds_retries_on_transient_failures() -> TestResult {
        let mut server = Server::run();
        server.expect(
            Expectation::matching(request::path(format!("{MDS_DEFAULT_URI}/token")))
                .times(3)
                .respond_with(status_code(503)),
        );

        let provider = Builder::default()
            .with_endpoint(format!("http://{}", server.addr()))
            .with_retry_policy(get_mock_auth_retry_policy(3))
            .with_backoff_policy(get_mock_backoff_policy())
            .with_retry_throttler(get_mock_retry_throttler())
            .build_token_provider();

        let err = provider.token().await.unwrap_err();
        assert!(!err.is_transient());
        server.verify_and_clear();
        Ok(())
    }

    #[tokio::test]
    #[parallel]
    async fn test_mds_does_not_retry_on_non_transient_failures() -> TestResult {
        let mut server = Server::run();
        server.expect(
            Expectation::matching(request::path(format!("{MDS_DEFAULT_URI}/token")))
                .times(1)
                .respond_with(status_code(401)),
        );

        let provider = Builder::default()
            .with_endpoint(format!("http://{}", server.addr()))
            .with_retry_policy(get_mock_auth_retry_policy(1))
            .with_backoff_policy(get_mock_backoff_policy())
            .with_retry_throttler(get_mock_retry_throttler())
            .build_token_provider();

        let err = provider.token().await.unwrap_err();
        assert!(!err.is_transient());
        server.verify_and_clear();
        Ok(())
    }

    #[tokio::test]
    #[parallel]
    async fn test_mds_retries_for_success() -> TestResult {
        let mut server = Server::run();
        let response = MDSTokenResponse {
            access_token: "test-access-token".to_string(),
            expires_in: Some(3600),
            token_type: "test-token-type".to_string(),
        };

        server.expect(
            Expectation::matching(request::path(format!("{MDS_DEFAULT_URI}/token")))
                .times(3)
                .respond_with(cycle![
                    status_code(503).body("try-again"),
                    status_code(503).body("try-again"),
                    status_code(200)
                        .append_header("Content-Type", "application/json")
                        .body(serde_json::to_string(&response).unwrap()),
                ]),
        );

        let provider = Builder::default()
            .with_endpoint(format!("http://{}", server.addr()))
            .with_retry_policy(get_mock_auth_retry_policy(3))
            .with_backoff_policy(get_mock_backoff_policy())
            .with_retry_throttler(get_mock_retry_throttler())
            .build_token_provider();

        let token = provider.token().await?;
        assert_eq!(token.token, "test-access-token");

        server.verify_and_clear();
        Ok(())
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
        let Err(err) = Builder::from_adc().build_token_provider().token().await else {
            // The environment has an MDS, skip the test.
            return Ok(());
        };

        let original_err = find_source_error::<CredentialsError>(&err).unwrap();
        assert!(
            original_err.to_string().contains("application-default"),
            "display={err}, debug={err:?}"
        );

        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn adc_overridden_mds() -> TestResult {
        let _e = ScopedEnv::set(super::GCE_METADATA_HOST_ENV_VAR, "metadata.overridden");

        let err = Builder::from_adc()
            .build_token_provider()
            .token()
            .await
            .unwrap_err();

        let _e = ScopedEnv::remove(super::GCE_METADATA_HOST_ENV_VAR);

        let original_err = find_source_error::<CredentialsError>(&err).unwrap();
        assert!(original_err.is_transient());
        assert!(
            !original_err.to_string().contains("application-default"),
            "display={err}, debug={err:?}"
        );
        let source = find_source_error::<reqwest::Error>(&err);
        assert!(matches!(source, Some(e) if e.status().is_none()), "{err:?}");

        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn builder_no_mds() -> TestResult {
        let Err(e) = Builder::default().build_token_provider().token().await else {
            // The environment has an MDS, skip the test.
            return Ok(());
        };

        let original_err = find_source_error::<CredentialsError>(&e).unwrap();
        assert!(
            !format!("{:?}", original_err.source()).contains("application-default"),
            "{e:?}"
        );

        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn test_gce_metadata_host_env_var() -> TestResult {
        let server = Server::run();
        let scopes = ["scope1", "scope2"];
        let response = MDSTokenResponse {
            access_token: "test-access-token".to_string(),
            expires_in: Some(3600),
            token_type: "test-token-type".to_string(),
        };
        server.expect(
            Expectation::matching(all_of![
                request::path(format!("{MDS_DEFAULT_URI}/token")),
                request::query(url_decoded(contains(("scopes", scopes.join(",")))))
            ])
            .respond_with(json_encoded(response)),
        );

        let addr = server.addr().to_string();
        let _e = ScopedEnv::set(super::GCE_METADATA_HOST_ENV_VAR, &addr);
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
        Ok(())
    }

    #[tokio::test]
    #[parallel]
    async fn headers_success_with_quota_project() -> TestResult {
        let server = Server::run();
        let scopes = ["scope1", "scope2"];
        let response = MDSTokenResponse {
            access_token: "test-access-token".to_string(),
            expires_in: Some(3600),
            token_type: "test-token-type".to_string(),
        };
        server.expect(
            Expectation::matching(all_of![
                request::path(format!("{MDS_DEFAULT_URI}/token")),
                request::query(url_decoded(contains(("scopes", scopes.join(",")))))
            ])
            .respond_with(json_encoded(response)),
        );

        let mdsc = Builder::default()
            .with_scopes(["scope1", "scope2"])
            .with_endpoint(format!("http://{}", server.addr()))
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
        let mut server = Server::run();
        let scopes = vec!["scope1".to_string()];
        let response = MDSTokenResponse {
            access_token: "test-access-token".to_string(),
            expires_in: Some(3600),
            token_type: "test-token-type".to_string(),
        };
        server.expect(
            Expectation::matching(all_of![
                request::path(format!("{MDS_DEFAULT_URI}/token")),
                request::query(url_decoded(contains(("scopes", scopes.join(",")))))
            ])
            .times(1)
            .respond_with(json_encoded(response)),
        );

        let mdsc = Builder::default()
            .with_scopes(scopes)
            .with_endpoint(format!("http://{}", server.addr()))
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
        server.verify_and_clear();

        Ok(())
    }

    #[tokio::test(start_paused = true)]
    #[parallel]
    async fn token_provider_full() -> TestResult {
        let server = Server::run();
        let scopes = vec!["scope1".to_string()];
        let response = MDSTokenResponse {
            access_token: "test-access-token".to_string(),
            expires_in: Some(3600),
            token_type: "test-token-type".to_string(),
        };
        server.expect(
            Expectation::matching(all_of![
                request::path(format!("{MDS_DEFAULT_URI}/token")),
                request::query(url_decoded(contains(("scopes", scopes.join(",")))))
            ])
            .respond_with(json_encoded(response)),
        );

        let token = Builder::default()
            .with_endpoint(format!("http://{}", server.addr()))
            .with_scopes(scopes)
            .build_token_provider()
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
        let server = Server::run();
        let response = MDSTokenResponse {
            access_token: "test-access-token".to_string(),
            expires_in: Some(3600),
            token_type: "test-token-type".to_string(),
        };
        server.expect(
            Expectation::matching(request::path(format!("{MDS_DEFAULT_URI}/token")))
                .respond_with(json_encoded(response)),
        );

        let token = Builder::default()
            .with_endpoint(format!("http://{}", server.addr()))
            .build_token_provider()
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
        let server = Server::run();
        let scopes = vec!["scope1".to_string()];
        let response = MDSTokenResponse {
            access_token: "test-access-token".to_string(),
            expires_in: None,
            token_type: "test-token-type".to_string(),
        };
        server.expect(
            Expectation::matching(all_of![
                request::path(format!("{MDS_DEFAULT_URI}/token")),
                request::query(url_decoded(contains(("scopes", scopes.join(",")))))
            ])
            .respond_with(json_encoded(response)),
        );

        let mdsc = Builder::default()
            .with_endpoint(format!("http://{}", server.addr()))
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
        let server = Server::run();
        let scopes = vec!["scope1".to_string()];
        server.expect(
            Expectation::matching(all_of![
                request::path(format!("{MDS_DEFAULT_URI}/token")),
                request::query(url_decoded(contains(("scopes", scopes.join(",")))))
            ])
            .respond_with(status_code(503)),
        );

        let mdsc = Builder::default()
            .with_endpoint(format!("http://{}", server.addr()))
            .with_scopes(scopes)
            .build()?;
        let err = mdsc.headers(Extensions::new()).await.unwrap_err();
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
    #[parallel]
    async fn credentials_headers_nonretryable_error() -> TestResult {
        let server = Server::run();
        let scopes = vec!["scope1".to_string()];
        server.expect(
            Expectation::matching(all_of![
                request::path(format!("{MDS_DEFAULT_URI}/token")),
                request::query(url_decoded(contains(("scopes", scopes.join(",")))))
            ])
            .respond_with(status_code(401)),
        );

        let mdsc = Builder::default()
            .with_endpoint(format!("http://{}", server.addr()))
            .with_scopes(scopes)
            .build()?;

        let err = mdsc.headers(Extensions::new()).await.unwrap_err();
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
    #[parallel]
    async fn credentials_headers_malformed_response_is_nonretryable() -> TestResult {
        let server = Server::run();
        let scopes = vec!["scope1".to_string()];
        server.expect(
            Expectation::matching(all_of![
                request::path(format!("{MDS_DEFAULT_URI}/token")),
                request::query(url_decoded(contains(("scopes", scopes.join(",")))))
            ])
            .respond_with(json_encoded("bad json")),
        );

        let mdsc = Builder::default()
            .with_endpoint(format!("http://{}", server.addr()))
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
    #[parallel]
    async fn test_idtoken_builder_build() -> TestResult {
        let server = Server::run();
        let audience = "test-audience";
        let token_string = "test-id-token";
        server.expect(
            Expectation::matching(all_of![
                request::path(format!("{MDS_DEFAULT_URI}/identity")),
                request::query(url_decoded(contains(("audience", audience))))
            ])
            .respond_with(status_code(200).body(token_string)),
        );

        let creds = idtoken::Builder::new(audience)
            .with_endpoint(format!("http://{}", server.addr()))
            .build()?;

        let token = creds.id_token().await?;
        assert_eq!(token.token, token_string);
        assert_eq!(token.token_type, "Bearer");
        assert!(token.expires_at.is_none());
        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn test_idtoken_builder_build_with_env_var() -> TestResult {
        let server = Server::run();
        let audience = "test-audience";
        let token_string = "test-id-token";
        server.expect(
            Expectation::matching(all_of![
                request::path(format!("{MDS_DEFAULT_URI}/identity")),
                request::query(url_decoded(contains(("audience", audience))))
            ])
            .respond_with(status_code(200).body(token_string)),
        );

        let addr = server.addr().to_string();
        let _e = ScopedEnv::set(super::GCE_METADATA_HOST_ENV_VAR, &addr);

        let creds = idtoken::Builder::new(audience).build()?;

        let token = creds.id_token().await?;
        assert_eq!(token.token, token_string);

        let _e = ScopedEnv::remove(super::GCE_METADATA_HOST_ENV_VAR);
        Ok(())
    }

    #[tokio::test]
    #[parallel]
    async fn test_idtoken_provider_http_error() -> TestResult {
        let server = Server::run();
        let audience = "test-audience";
        server.expect(
            Expectation::matching(all_of![
                request::path(format!("{MDS_DEFAULT_URI}/identity")),
                request::query(url_decoded(contains(("audience", audience))))
            ])
            .respond_with(status_code(503)),
        );

        let creds = idtoken::Builder::new(audience)
            .with_endpoint(format!("http://{}", server.addr()))
            .build()?;

        let err = creds.id_token().await.unwrap_err();
        let source = find_source_error::<reqwest::Error>(&err);
        assert!(
            matches!(source, Some(e) if e.status() == Some(StatusCode::SERVICE_UNAVAILABLE)),
            "{err:?}"
        );
        Ok(())
    }
}
