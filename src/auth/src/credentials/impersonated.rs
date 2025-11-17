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

//! [Impersonated service account] credentials.
//!
//! When the principal you are using doesn't have the permissions you need to
//! accomplish your task, or you want to use a service account in a development
//! environment, you can use service account impersonation. The typical principals
//! used to impersonate a service account are [User Account] or another [Service Account].
//!
//! The principal that is trying to impersonate a target service account should have
//! [Service Account Token Creator Role] on the target service account.
//!
//! ## Example: Creating credentials from a JSON object
//!
//! ```
//! # use google_cloud_auth::credentials::impersonated;
//! # use serde_json::json;
//! # use std::time::Duration;
//! # use http::Extensions;
//! #
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
//! let credentials = impersonated::Builder::new(impersonated_credential)
//!     .with_lifetime(Duration::from_secs(500))
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
//! # use google_cloud_auth::credentials::impersonated;
//! # use serde_json::json;
//! # use std::time::Duration;
//! # use http::Extensions;
//! # tokio_test::block_on(async {
//! use gax::retry_policy::{AlwaysRetry, RetryPolicyExt};
//! use gax::exponential_backoff::ExponentialBackoff;
//! # let source_credentials = json!({
//! #     "type": "authorized_user",
//! #     "client_id": "test-client-id",
//! #     "client_secret": "test-client-secret",
//! #     "refresh_token": "test-refresh-token"
//! # });
//! #
//! # let impersonated_credential = json!({
//! #     "type": "impersonated_service_account",
//! #     "service_account_impersonation_url": "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/test-principal:generateAccessToken",
//! #     "source_credentials": source_credentials,
//! # });
//! let backoff = ExponentialBackoff::default();
//! let credentials = impersonated::Builder::new(impersonated_credential)
//!     .with_retry_policy(AlwaysRetry.with_attempt_limit(3))
//!     .with_backoff_policy(backoff)
//!     .build()?;
//! let headers = credentials.headers(Extensions::new()).await?;
//! println!("Headers: {headers:?}");
//! # Ok::<(), anyhow::Error>(())
//! # });
//! ```
//!
//! [Impersonated service account]: https://cloud.google.com/docs/authentication/use-service-account-impersonation
//! [User Account]: https://cloud.google.com/docs/authentication#user-accounts
//! [Service Account]: https://cloud.google.com/iam/docs/service-account-overview
//! [Service Account Token Creator Role]: https://cloud.google.com/docs/authentication/use-service-account-impersonation#required-roles

use crate::build_errors::Error as BuilderError;
use crate::constants::DEFAULT_SCOPE;
use crate::credentials::dynamic::{AccessTokenCredentialsProvider, CredentialsProvider};
use crate::credentials::{
    AccessToken, AccessTokenCredentials, CacheableResource, Credentials, build_credentials,
    extract_credential_type,
};
use crate::errors::{self, CredentialsError};
use crate::headers_util::{
    self, ACCESS_TOKEN_REQUEST_TYPE, build_cacheable_headers, metrics_header_value,
};
use crate::retry::{Builder as RetryTokenProviderBuilder, TokenProviderWithRetry};
use crate::token::{CachedTokenProvider, Token, TokenProvider};
use crate::token_cache::TokenCache;
use crate::{BuildResult, Result};
use async_trait::async_trait;
use gax::backoff_policy::BackoffPolicyArg;
use gax::retry_policy::RetryPolicyArg;
use gax::retry_throttler::RetryThrottlerArg;
use http::{Extensions, HeaderMap};
use reqwest::Client;
use serde_json::Value;
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;
use time::OffsetDateTime;
use tokio::time::Instant;

pub(crate) const IMPERSONATED_CREDENTIAL_TYPE: &str = "imp";
pub(crate) const DEFAULT_LIFETIME: Duration = Duration::from_secs(3600);
pub(crate) const MSG: &str = "failed to fetch token";

pub(crate) enum BuilderSource {
    FromJson(Value),
    FromCredentials(Credentials),
}

/// A builder for constructing Impersonated Service Account [Credentials] instance.
///
/// # Example
/// ```
/// # use google_cloud_auth::credentials::impersonated::Builder;
/// # tokio_test::block_on(async {
/// let impersonated_credential = serde_json::json!({
///     "type": "impersonated_service_account",
///     "service_account_impersonation_url": "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/test-principal:generateAccessToken",
///     "source_credentials": {
///         "type": "authorized_user",
///         "client_id": "test-client-id",
///         "client_secret": "test-client-secret",
///         "refresh_token": "test-refresh-token"
///     }
/// });
/// let credentials = Builder::new(impersonated_credential).build();
/// # });
/// ```
pub struct Builder {
    source: BuilderSource,
    service_account_impersonation_url: Option<String>,
    delegates: Option<Vec<String>>,
    scopes: Option<Vec<String>>,
    quota_project_id: Option<String>,
    lifetime: Option<Duration>,
    retry_builder: RetryTokenProviderBuilder,
}

impl Builder {
    /// Creates a new builder using `impersonated_service_account` JSON value.
    ///
    /// The `impersonated_service_account` JSON is typically generated using
    /// a [gcloud command] for [application default login].
    ///
    /// [gcloud command]: https://cloud.google.com/docs/authentication/use-service-account-impersonation#adc
    /// [application default login]: https://cloud.google.com/sdk/gcloud/reference/auth/application-default/login
    pub fn new(impersonated_credential: Value) -> Self {
        Self {
            source: BuilderSource::FromJson(impersonated_credential),
            service_account_impersonation_url: None,
            delegates: None,
            scopes: None,
            quota_project_id: None,
            lifetime: None,
            retry_builder: RetryTokenProviderBuilder::default(),
        }
    }

    /// Creates a new builder with a source credentials object.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_auth::credentials::impersonated;
    /// # use google_cloud_auth::credentials::user_account;
    /// # use serde_json::json;
    /// #
    /// # tokio_test::block_on(async {
    /// let source_credentials = user_account::Builder::new(json!({ /* add details here */ })).build()?;
    ///
    /// let creds = impersonated::Builder::from_source_credentials(source_credentials)
    ///     .with_target_principal("test-principal")
    ///     .build()?;
    /// # Ok::<(), anyhow::Error>(())
    /// # });
    /// ```
    pub fn from_source_credentials(source_credentials: Credentials) -> Self {
        Self {
            source: BuilderSource::FromCredentials(source_credentials),
            service_account_impersonation_url: None,
            delegates: None,
            scopes: None,
            quota_project_id: None,
            lifetime: None,
            retry_builder: RetryTokenProviderBuilder::default(),
        }
    }

    /// Sets the target principal. This is required when using `from_source_credentials`.
    /// Target principal is the email of the service account to impersonate.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_auth::credentials::impersonated;
    /// # use serde_json::json;
    /// #
    /// # tokio_test::block_on(async {
    /// let impersonated_credential = json!({ /* add details here */ });
    ///
    /// let creds = impersonated::Builder::new(impersonated_credential.into())
    ///     .with_target_principal("test-principal")
    ///     .build();
    /// # });
    /// ```
    pub fn with_target_principal<S: Into<String>>(mut self, target_principal: S) -> Self {
        self.service_account_impersonation_url = Some(format!(
            "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/{}:generateAccessToken",
            target_principal.into()
        ));
        self
    }

    /// Sets the chain of delegates.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_auth::credentials::impersonated;
    /// # use serde_json::json;
    /// #
    /// # tokio_test::block_on(async {
    /// let impersonated_credential = json!({ /* add details here */ });
    ///
    /// let creds = impersonated::Builder::new(impersonated_credential.into())
    ///     .with_delegates(["delegate1", "delegate2"])
    ///     .build();
    /// # });
    /// ```
    pub fn with_delegates<I, S>(mut self, delegates: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.delegates = Some(delegates.into_iter().map(|s| s.into()).collect());
        self
    }

    /// Sets the [scopes] for these credentials.
    ///
    /// Any value set here overrides a `scopes` value from the
    /// input `impersonated_service_account` JSON.
    ///
    /// By default `https://www.googleapis.com/auth/cloud-platform` scope is used.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_auth::credentials::impersonated;
    /// # use serde_json::json;
    /// #
    /// # tokio_test::block_on(async {
    /// let impersonated_credential = json!({ /* add details here */ });
    ///
    /// let creds = impersonated::Builder::new(impersonated_credential.into())
    ///     .with_scopes(["https://www.googleapis.com/auth/pubsub"])
    ///     .build();
    /// # });
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
    /// For some services, you can use an account in
    /// one project for authentication and authorization, and charge
    /// the usage to a different project. This requires that the
    /// target service account has `serviceusage.services.use`
    /// permissions on the quota project.
    ///
    /// Any value set here overrides a `quota_project_id` value from the
    /// input `impersonated_service_account` JSON.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_auth::credentials::impersonated;
    /// # use serde_json::json;
    /// #
    /// # tokio_test::block_on(async {
    /// let impersonated_credential = json!({ /* add details here */ });
    ///
    /// let creds = impersonated::Builder::new(impersonated_credential.into())
    ///     .with_quota_project_id("my-project")
    ///     .build();
    /// # });
    /// ```
    /// [quota project]: https://cloud.google.com/docs/quotas/quota-project
    pub fn with_quota_project_id<S: Into<String>>(mut self, quota_project_id: S) -> Self {
        self.quota_project_id = Some(quota_project_id.into());
        self
    }

    /// Sets the lifetime for the impersonated credentials.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_auth::credentials::impersonated;
    /// # use serde_json::json;
    /// # use std::time::Duration;
    /// #
    /// # tokio_test::block_on(async {
    /// let impersonated_credential = json!({ /* add details here */ });
    ///
    /// let creds = impersonated::Builder::new(impersonated_credential.into())
    ///     .with_lifetime(Duration::from_secs(500))
    ///     .build();
    /// # });
    /// ```
    pub fn with_lifetime(mut self, lifetime: Duration) -> Self {
        self.lifetime = Some(lifetime);
        self
    }

    /// Configure the retry policy for fetching tokens.
    ///
    /// The retry policy controls how to handle retries, and sets limits on
    /// the number of attempts or the total time spent retrying.
    ///
    /// ```
    /// # use google_cloud_auth::credentials::impersonated::Builder;
    /// # use serde_json::json;
    /// # tokio_test::block_on(async {
    /// use gax::retry_policy::{AlwaysRetry, RetryPolicyExt};
    /// let impersonated_credential = json!({ /* add details here */ });
    /// let credentials = Builder::new(impersonated_credential.into())
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
    /// # use google_cloud_auth::credentials::impersonated::Builder;
    /// # use serde_json::json;
    /// # use std::time::Duration;
    /// # tokio_test::block_on(async {
    /// use gax::exponential_backoff::ExponentialBackoff;
    /// let policy = ExponentialBackoff::default();
    /// let impersonated_credential = json!({ /* add details here */ });
    /// let credentials = Builder::new(impersonated_credential.into())
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
    /// # use google_cloud_auth::credentials::impersonated::Builder;
    /// # use serde_json::json;
    /// # tokio_test::block_on(async {
    /// use gax::retry_throttler::AdaptiveThrottler;
    /// let impersonated_credential = json!({ /* add details here */ });
    /// let credentials = Builder::new(impersonated_credential.into())
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
    /// Returns a [BuilderError] for one of the following cases:
    /// - If the `impersonated_service_account` provided to [`Builder::new`] cannot
    ///   be successfully deserialized into the expected format. This typically happens
    ///   if the JSON value is malformed or missing required fields. For more information,
    ///   on how to generate `impersonated_service_account` json, consult the relevant
    ///   section in the [application-default credentials] guide.
    /// - If the `impersonated_service_account` provided to [`Builder::new`] has a
    ///   `source_credentials` of `impersonated_service_account` type.
    /// - If `service_account_impersonation_url` is not provided after initializing
    ///   the builder with [`Builder::from_source_credentials`].
    ///
    /// [application-default credentials]: https://cloud.google.com/docs/authentication/application-default-credentials
    pub fn build(self) -> BuildResult<Credentials> {
        Ok(self.build_access_token_credentials()?.into())
    }

    /// Returns an [AccessTokenCredentials] instance with the configured settings.
    ///
    /// # Example
    ///
    /// ```
    /// # use google_cloud_auth::credentials::impersonated::Builder;
    /// # use google_cloud_auth::credentials::{AccessTokenCredentials, AccessTokenCredentialsProvider};
    /// # use serde_json::json;
    /// # tokio_test::block_on(async {
    /// let impersonated_credential = json!({
    ///     "type": "impersonated_service_account",
    ///     "service_account_impersonation_url": "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/test-principal:generateAccessToken",
    ///     "source_credentials": {
    ///         "type": "authorized_user",
    ///         "client_id": "test-client-id",
    ///         "client_secret": "test-client-secret",
    ///         "refresh_token": "test-refresh-token"
    ///     }
    /// });
    /// let credentials: AccessTokenCredentials = Builder::new(impersonated_credential.into())
    ///     .build_access_token_credentials()?;
    /// let access_token = credentials.access_token().await?;
    /// println!("Token: {}", access_token.token);
    /// # Ok::<(), anyhow::Error>(())
    /// # });
    /// ```
    ///
    /// # Errors
    ///
    /// Returns a [BuilderError] for one of the following cases:
    /// - If the `impersonated_service_account` provided to [`Builder::new`] cannot
    ///   be successfully deserialized into the expected format. This typically happens
    ///   if the JSON value is malformed or missing required fields. For more information,
    ///   on how to generate `impersonated_service_account` json, consult the relevant
    ///   section in the [application-default credentials] guide.
    /// - If the `impersonated_service_account` provided to [`Builder::new`] has a
    ///   `source_credentials` of `impersonated_service_account` type.
    /// - If `service_account_impersonation_url` is not provided after initializing
    ///   the builder with [`Builder::from_source_credentials`].
    ///
    /// [application-default credentials]: https://cloud.google.com/docs/authentication/application-default-credentials
    pub fn build_access_token_credentials(self) -> BuildResult<AccessTokenCredentials> {
        let (token_provider, quota_project_id) = self.build_components()?;
        Ok(AccessTokenCredentials {
            inner: Arc::new(ImpersonatedServiceAccount {
                token_provider: TokenCache::new(token_provider),
                quota_project_id,
            }),
        })
    }

    fn build_components(
        self,
    ) -> BuildResult<(
        TokenProviderWithRetry<ImpersonatedTokenProvider>,
        Option<String>,
    )> {
        let components = match self.source {
            BuilderSource::FromJson(json) => build_components_from_json(json)?,
            BuilderSource::FromCredentials(source_credentials) => {
                build_components_from_credentials(
                    source_credentials,
                    self.service_account_impersonation_url,
                )?
            }
        };

        let scopes = self
            .scopes
            .or(components.scopes)
            .unwrap_or_else(|| vec![DEFAULT_SCOPE.to_string()]);

        let quota_project_id = self.quota_project_id.or(components.quota_project_id);
        let delegates = self.delegates.or(components.delegates);

        let token_provider = ImpersonatedTokenProvider {
            source_credentials: components.source_credentials,
            service_account_impersonation_url: components.service_account_impersonation_url,
            delegates,
            scopes,
            lifetime: self.lifetime.unwrap_or(DEFAULT_LIFETIME),
        };
        let token_provider = self.retry_builder.build(token_provider);
        Ok((token_provider, quota_project_id))
    }
}

pub(crate) struct ImpersonatedCredentialComponents {
    pub(crate) source_credentials: Credentials,
    pub(crate) service_account_impersonation_url: String,
    pub(crate) delegates: Option<Vec<String>>,
    pub(crate) quota_project_id: Option<String>,
    pub(crate) scopes: Option<Vec<String>>,
}

pub(crate) fn build_components_from_json(
    json: Value,
) -> BuildResult<ImpersonatedCredentialComponents> {
    let config =
        serde_json::from_value::<ImpersonatedConfig>(json).map_err(BuilderError::parsing)?;

    let source_credential_type = extract_credential_type(&config.source_credentials)?;
    if source_credential_type == "impersonated_service_account" {
        return Err(BuilderError::parsing(
            "source credential of type `impersonated_service_account` is not supported. \
                        Use the `delegates` field to specify a delegation chain.",
        ));
    }

    // Do not pass along scopes and quota project to the source credentials.
    // It is not necessary that the source and target credentials have same permissions on
    // the quota project and they typically need different scopes.
    // If user does want some specific scopes or quota, they can build using the
    // from_source_credentials method.
    let source_credentials = build_credentials(Some(config.source_credentials), None, None)?.into();

    Ok(ImpersonatedCredentialComponents {
        source_credentials,
        service_account_impersonation_url: config.service_account_impersonation_url,
        delegates: config.delegates,
        quota_project_id: config.quota_project_id,
        scopes: config.scopes,
    })
}

pub(crate) fn build_components_from_credentials(
    source_credentials: Credentials,
    service_account_impersonation_url: Option<String>,
) -> BuildResult<ImpersonatedCredentialComponents> {
    let url = service_account_impersonation_url.ok_or_else(|| {
        BuilderError::parsing(
            "`service_account_impersonation_url` is required when building from source credentials",
        )
    })?;
    Ok(ImpersonatedCredentialComponents {
        source_credentials,
        service_account_impersonation_url: url,
        delegates: None,
        quota_project_id: None,
        scopes: None,
    })
}

#[derive(serde::Deserialize, Debug, PartialEq)]
struct ImpersonatedConfig {
    service_account_impersonation_url: String,
    source_credentials: Value,
    delegates: Option<Vec<String>>,
    quota_project_id: Option<String>,
    scopes: Option<Vec<String>>,
}

#[derive(Debug)]
struct ImpersonatedServiceAccount<T>
where
    T: CachedTokenProvider,
{
    token_provider: T,
    quota_project_id: Option<String>,
}

#[async_trait::async_trait]
impl<T> CredentialsProvider for ImpersonatedServiceAccount<T>
where
    T: CachedTokenProvider,
{
    async fn headers(&self, extensions: Extensions) -> Result<CacheableResource<HeaderMap>> {
        let token = self.token_provider.token(extensions).await?;
        build_cacheable_headers(&token, &self.quota_project_id)
    }
}

#[async_trait::async_trait]
impl<T> AccessTokenCredentialsProvider for ImpersonatedServiceAccount<T>
where
    T: CachedTokenProvider,
{
    async fn access_token(&self) -> Result<AccessToken> {
        let token = self.token_provider.token(Extensions::new()).await?;
        token.into()
    }
}

struct ImpersonatedTokenProvider {
    source_credentials: Credentials,
    service_account_impersonation_url: String,
    delegates: Option<Vec<String>>,
    scopes: Vec<String>,
    lifetime: Duration,
}

impl Debug for ImpersonatedTokenProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ImpersonatedTokenProvider")
            .field("source_credentials", &self.source_credentials)
            .field(
                "service_account_impersonation_url",
                &self.service_account_impersonation_url,
            )
            .field("delegates", &self.delegates)
            .field("scopes", &self.scopes)
            .field("lifetime", &self.lifetime)
            .finish()
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq)]
struct GenerateAccessTokenRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    delegates: Option<Vec<String>>,
    scope: Vec<String>,
    lifetime: String,
}

pub(crate) async fn generate_access_token(
    source_headers: HeaderMap,
    delegates: Option<Vec<String>>,
    scopes: Vec<String>,
    lifetime: Duration,
    service_account_impersonation_url: &str,
) -> Result<Token> {
    let client = Client::new();
    let body = GenerateAccessTokenRequest {
        delegates,
        scope: scopes,
        lifetime: format!("{}s", lifetime.as_secs_f64()),
    };

    let response = client
        .post(service_account_impersonation_url)
        .header("Content-Type", "application/json")
        .header(
            headers_util::X_GOOG_API_CLIENT,
            metrics_header_value(ACCESS_TOKEN_REQUEST_TYPE, IMPERSONATED_CREDENTIAL_TYPE),
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
        .json::<GenerateAccessTokenResponse>()
        .await
        .map_err(|e| {
            let retryable = !e.is_decode();
            CredentialsError::from_source(retryable, e)
        })?;

    let parsed_dt = OffsetDateTime::parse(
        &token_response.expire_time,
        &time::format_description::well_known::Rfc3339,
    )
    .map_err(errors::non_retryable)?;

    let remaining_duration = parsed_dt - OffsetDateTime::now_utc();
    let expires_at = Instant::now() + remaining_duration.try_into().unwrap();

    let token = Token {
        token: token_response.access_token,
        token_type: "Bearer".to_string(),
        expires_at: Some(expires_at),
        metadata: None,
    };
    Ok(token)
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
        generate_access_token(
            source_headers,
            self.delegates.clone(),
            self.scopes.clone(),
            self.lifetime,
            &self.service_account_impersonation_url,
        )
        .await
    }
}

#[derive(serde::Deserialize)]
struct GenerateAccessTokenResponse {
    #[serde(rename = "accessToken")]
    access_token: String,
    #[serde(rename = "expireTime")]
    expire_time: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::credentials::tests::{
        find_source_error, get_mock_auth_retry_policy, get_mock_backoff_policy,
        get_mock_retry_throttler,
    };
    use crate::errors::CredentialsError;
    use httptest::cycle;
    use httptest::{Expectation, Server, matchers::*, responders::*};
    use serde_json::json;

    type TestResult = anyhow::Result<()>;

    #[tokio::test]
    async fn test_generate_access_token_success() -> TestResult {
        let server = Server::run();
        let expire_time = (OffsetDateTime::now_utc() + time::Duration::hours(1))
            .format(&time::format_description::well_known::Rfc3339)
            .unwrap();
        server.expect(
            Expectation::matching(all_of![
                request::method_path(
                    "POST",
                    "/v1/projects/-/serviceAccounts/test-principal:generateAccessToken"
                ),
                request::headers(contains(("authorization", "Bearer test-token"))),
            ])
            .respond_with(json_encoded(json!({
                "accessToken": "test-impersonated-token",
                "expireTime": expire_time
            }))),
        );

        let mut headers = HeaderMap::new();
        headers.insert("authorization", "Bearer test-token".parse().unwrap());
        let token = generate_access_token(
            headers,
            None,
            vec!["scope".to_string()],
            DEFAULT_LIFETIME,
            &server
                .url("/v1/projects/-/serviceAccounts/test-principal:generateAccessToken")
                .to_string(),
        )
        .await?;

        assert_eq!(token.token, "test-impersonated-token");
        Ok(())
    }

    #[tokio::test]
    async fn test_generate_access_token_403() -> TestResult {
        let server = Server::run();
        server.expect(
            Expectation::matching(all_of![
                request::method_path(
                    "POST",
                    "/v1/projects/-/serviceAccounts/test-principal:generateAccessToken"
                ),
                request::headers(contains(("authorization", "Bearer test-token"))),
            ])
            .respond_with(status_code(403)),
        );

        let mut headers = HeaderMap::new();
        headers.insert("authorization", "Bearer test-token".parse().unwrap());
        let err = generate_access_token(
            headers,
            None,
            vec!["scope".to_string()],
            DEFAULT_LIFETIME,
            &server
                .url("/v1/projects/-/serviceAccounts/test-principal:generateAccessToken")
                .to_string(),
        )
        .await
        .unwrap_err();

        assert!(!err.is_transient());
        Ok(())
    }

    #[tokio::test]
    async fn test_generate_access_token_no_auth_header() -> TestResult {
        let server = Server::run();
        server.expect(
            Expectation::matching(request::method_path(
                "POST",
                "/v1/projects/-/serviceAccounts/test-principal:generateAccessToken",
            ))
            .respond_with(status_code(401)),
        );

        let err = generate_access_token(
            HeaderMap::new(),
            None,
            vec!["scope".to_string()],
            DEFAULT_LIFETIME,
            &server
                .url("/v1/projects/-/serviceAccounts/test-principal:generateAccessToken")
                .to_string(),
        )
        .await
        .unwrap_err();

        assert!(!err.is_transient());
        Ok(())
    }

    #[tokio::test]
    async fn test_impersonated_service_account() -> TestResult {
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
        let expire_time = (OffsetDateTime::now_utc() + time::Duration::hours(1))
            .format(&time::format_description::well_known::Rfc3339)
            .unwrap();
        server.expect(
            Expectation::matching(all_of![
                request::method_path(
                    "POST",
                    "/v1/projects/-/serviceAccounts/test-principal:generateAccessToken"
                ),
                request::headers(contains((
                    "authorization",
                    "Bearer test-user-account-token"
                ))),
                request::body(json_decoded(eq(json!({
                    "scope": ["scope1", "scope2"],
                    "lifetime": "3600s"
                }))))
            ])
            .respond_with(json_encoded(json!({
                "accessToken": "test-impersonated-token",
                "expireTime": expire_time
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
        let (token_provider, _) = Builder::new(impersonated_credential)
            .with_scopes(vec!["scope1", "scope2"])
            .build_components()?;

        let token = token_provider.token().await?;
        assert_eq!(token.token, "test-impersonated-token");
        assert_eq!(token.token_type, "Bearer");

        Ok(())
    }

    #[tokio::test]
    async fn test_impersonated_service_account_default_scope() -> TestResult {
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
        let expire_time = (OffsetDateTime::now_utc() + time::Duration::hours(1))
            .format(&time::format_description::well_known::Rfc3339)
            .unwrap();
        server.expect(
            Expectation::matching(all_of![
                request::method_path(
                    "POST",
                    "/v1/projects/-/serviceAccounts/test-principal:generateAccessToken"
                ),
                request::headers(contains((
                    "authorization",
                    "Bearer test-user-account-token"
                ))),
                request::body(json_decoded(eq(json!({
                    "scope": [DEFAULT_SCOPE],
                    "lifetime": "3600s"
                }))))
            ])
            .respond_with(json_encoded(json!({
                "accessToken": "test-impersonated-token",
                "expireTime": expire_time
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
        let (token_provider, _) = Builder::new(impersonated_credential).build_components()?;

        let token = token_provider.token().await?;
        assert_eq!(token.token, "test-impersonated-token");
        assert_eq!(token.token_type, "Bearer");

        Ok(())
    }

    #[tokio::test]
    async fn test_impersonated_service_account_with_custom_lifetime() -> TestResult {
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
        let expire_time = (OffsetDateTime::now_utc() + time::Duration::seconds(500))
            .format(&time::format_description::well_known::Rfc3339)
            .unwrap();
        server.expect(
            Expectation::matching(all_of![
                request::method_path(
                    "POST",
                    "/v1/projects/-/serviceAccounts/test-principal:generateAccessToken"
                ),
                request::headers(contains((
                    "authorization",
                    "Bearer test-user-account-token"
                ))),
                request::body(json_decoded(eq(json!({
                    "scope": ["scope1", "scope2"],
                    "lifetime": "3.5s"
                }))))
            ])
            .respond_with(json_encoded(json!({
                "accessToken": "test-impersonated-token",
                "expireTime": expire_time
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
        let (token_provider, _) = Builder::new(impersonated_credential)
            .with_scopes(vec!["scope1", "scope2"])
            .with_lifetime(Duration::from_secs_f32(3.5))
            .build_components()?;

        let token = token_provider.token().await?;
        assert_eq!(token.token, "test-impersonated-token");

        Ok(())
    }

    #[tokio::test]
    async fn test_with_delegates() -> TestResult {
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
        let expire_time = (OffsetDateTime::now_utc() + time::Duration::hours(1))
            .format(&time::format_description::well_known::Rfc3339)
            .unwrap();
        server.expect(
            Expectation::matching(all_of![
                request::method_path(
                    "POST",
                    "/v1/projects/-/serviceAccounts/test-principal:generateAccessToken"
                ),
                request::headers(contains((
                    "authorization",
                    "Bearer test-user-account-token"
                ))),
                request::body(json_decoded(eq(json!({
                    "scope": [DEFAULT_SCOPE],
                    "lifetime": "3600s",
                    "delegates": ["delegate1", "delegate2"]
                }))))
            ])
            .respond_with(json_encoded(json!({
                "accessToken": "test-impersonated-token",
                "expireTime": expire_time
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
        let (token_provider, _) = Builder::new(impersonated_credential)
            .with_delegates(vec!["delegate1", "delegate2"])
            .build_components()?;

        let token = token_provider.token().await?;
        assert_eq!(token.token, "test-impersonated-token");
        assert_eq!(token.token_type, "Bearer");

        Ok(())
    }

    #[tokio::test]
    async fn test_impersonated_service_account_fail() -> TestResult {
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
                "/v1/projects/-/serviceAccounts/test-principal:generateAccessToken",
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
        let (token_provider, _) = Builder::new(impersonated_credential).build_components()?;

        let err = token_provider.token().await.unwrap_err();
        let original_err = find_source_error::<CredentialsError>(&err).unwrap();
        assert!(original_err.is_transient());

        Ok(())
    }

    #[tokio::test]
    async fn debug_token_provider() {
        let source_credentials = crate::credentials::user_account::Builder::new(json!({
            "type": "authorized_user",
            "client_id": "test-client-id",
            "client_secret": "test-client-secret",
            "refresh_token": "test-refresh-token"
        }))
        .build()
        .unwrap();

        let expected = ImpersonatedTokenProvider {
            source_credentials,
            service_account_impersonation_url: "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/test-principal:generateAccessToken".to_string(),
            delegates: Some(vec!["delegate1".to_string()]),
            scopes: vec!["scope1".to_string()],
            lifetime: Duration::from_secs(3600),
        };
        let fmt = format!("{expected:?}");
        assert!(fmt.contains("UserCredentials"), "{fmt}");
        assert!(fmt.contains("test-client-id"), "{fmt}");
        assert!(!fmt.contains("test-client-secret"), "{fmt}");
        assert!(!fmt.contains("test-refresh-token"), "{fmt}");
        assert!(fmt.contains("https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/test-principal:generateAccessToken"), "{fmt}");
        assert!(fmt.contains("delegate1"), "{fmt}");
        assert!(fmt.contains("scope1"), "{fmt}");
        assert!(fmt.contains("3600s"), "{fmt}");
    }

    #[test]
    fn impersonated_config_full_from_json_success() {
        let source_credentials_json = json!({
            "type": "authorized_user",
            "client_id": "test-client-id",
            "client_secret": "test-client-secret",
            "refresh_token": "test-refresh-token"
        });
        let json = json!({
            "type": "impersonated_service_account",
            "service_account_impersonation_url": "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/test-principal:generateAccessToken",
            "source_credentials": source_credentials_json,
            "delegates": ["delegate1"],
            "quota_project_id": "test-project-id",
            "scopes": ["scope1"],
        });

        let expected = ImpersonatedConfig {
            service_account_impersonation_url: "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/test-principal:generateAccessToken".to_string(),
            source_credentials: source_credentials_json,
            delegates: Some(vec!["delegate1".to_string()]),
            quota_project_id: Some("test-project-id".to_string()),
            scopes: Some(vec!["scope1".to_string()]),
        };
        let actual: ImpersonatedConfig = serde_json::from_value(json).unwrap();
        assert_eq!(actual, expected);
    }

    #[test]
    fn impersonated_config_partial_from_json_success() {
        let source_credentials_json = json!({
            "type": "authorized_user",
            "client_id": "test-client-id",
            "client_secret": "test-client-secret",
            "refresh_token": "test-refresh-token"
        });
        let json = json!({
            "type": "impersonated_service_account",
            "service_account_impersonation_url": "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/test-principal:generateAccessToken",
            "source_credentials": source_credentials_json
        });

        let config: ImpersonatedConfig = serde_json::from_value(json).unwrap();
        assert_eq!(
            config.service_account_impersonation_url,
            "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/test-principal:generateAccessToken"
        );
        assert_eq!(config.source_credentials, source_credentials_json);
        assert_eq!(config.delegates, None);
        assert_eq!(config.quota_project_id, None);
        assert_eq!(config.scopes, None);
    }

    #[tokio::test]
    async fn test_impersonated_service_account_source_fail() -> TestResult {
        #[derive(Debug)]
        struct MockSourceCredentialsFail;

        #[async_trait]
        impl CredentialsProvider for MockSourceCredentialsFail {
            async fn headers(
                &self,
                _extensions: Extensions,
            ) -> Result<CacheableResource<HeaderMap>> {
                Err(errors::non_retryable_from_str("source failed"))
            }
        }

        let source_credentials = Credentials {
            inner: Arc::new(MockSourceCredentialsFail),
        };

        let token_provider = ImpersonatedTokenProvider {
            source_credentials,
            service_account_impersonation_url: "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/test-principal:generateAccessToken".to_string(),
            delegates: Some(vec!["delegate1".to_string()]),
            scopes: vec!["scope1".to_string()],
            lifetime: DEFAULT_LIFETIME,
        };

        let err = token_provider.token().await.unwrap_err();
        assert!(err.to_string().contains("source failed"));

        Ok(())
    }

    #[tokio::test]
    async fn test_missing_impersonation_url_fail() {
        let source_credentials = crate::credentials::user_account::Builder::new(json!({
            "type": "authorized_user",
            "client_id": "test-client-id",
            "client_secret": "test-client-secret",
            "refresh_token": "test-refresh-token"
        }))
        .build()
        .unwrap();

        let result = Builder::from_source_credentials(source_credentials).build();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.is_parsing());
        assert!(
            err.to_string()
                .contains("`service_account_impersonation_url` is required")
        );
    }

    #[tokio::test]
    async fn test_nested_impersonated_credentials_fail() {
        let nested_impersonated = json!({
            "type": "impersonated_service_account",
            "service_account_impersonation_url": "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/test-principal:generateAccessToken",
            "source_credentials": {
                "type": "impersonated_service_account",
                "service_account_impersonation_url": "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/test-principal:generateAccessToken",
                "source_credentials": {
                    "type": "authorized_user",
                    "client_id": "test-client-id",
                    "client_secret": "test-client-secret",
                    "refresh_token": "test-refresh-token"
                }
            }
        });

        let result = Builder::new(nested_impersonated).build();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.is_parsing());
        assert!(
            err.to_string().contains(
                "source credential of type `impersonated_service_account` is not supported"
            )
        );
    }

    #[tokio::test]
    async fn test_malformed_impersonated_credentials_fail() {
        let malformed_impersonated = json!({
            "type": "impersonated_service_account",
            "service_account_impersonation_url": "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/test-principal:generateAccessToken",
        });

        let result = Builder::new(malformed_impersonated).build();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.is_parsing());
        assert!(
            err.to_string()
                .contains("missing field `source_credentials`")
        );
    }

    #[tokio::test]
    async fn test_invalid_source_credential_type_fail() {
        let invalid_source = json!({
            "type": "impersonated_service_account",
            "service_account_impersonation_url": "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/test-principal:generateAccessToken",
            "source_credentials": {
                "type": "invalid_type",
            }
        });

        let result = Builder::new(invalid_source).build();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.is_unknown_type());
    }

    #[tokio::test]
    async fn test_missing_expiry() -> TestResult {
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
                "/v1/projects/-/serviceAccounts/test-principal:generateAccessToken",
            ))
            .respond_with(json_encoded(json!({
                "accessToken": "test-impersonated-token",
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
        let (token_provider, _) = Builder::new(impersonated_credential).build_components()?;

        let err = token_provider.token().await.unwrap_err();
        assert!(!err.is_transient());

        Ok(())
    }

    #[tokio::test]
    async fn test_invalid_expiry_format() -> TestResult {
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
                "/v1/projects/-/serviceAccounts/test-principal:generateAccessToken",
            ))
            .respond_with(json_encoded(json!({
                "accessToken": "test-impersonated-token",
                "expireTime": "invalid-format"
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
        let (token_provider, _) = Builder::new(impersonated_credential).build_components()?;

        let err = token_provider.token().await.unwrap_err();
        assert!(!err.is_transient());

        Ok(())
    }

    #[tokio::test]
    async fn token_provider_malformed_response_is_nonretryable() -> TestResult {
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
                "/v1/projects/-/serviceAccounts/test-principal:generateAccessToken",
            ))
            .respond_with(json_encoded(json!("bad json"))),
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
        let (token_provider, _) = Builder::new(impersonated_credential).build_components()?;

        let e = token_provider.token().await.err().unwrap();
        assert!(!e.is_transient(), "{e}");

        Ok(())
    }

    #[tokio::test]
    async fn token_provider_nonretryable_error() -> TestResult {
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
                "/v1/projects/-/serviceAccounts/test-principal:generateAccessToken",
            ))
            .respond_with(status_code(401)),
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
        let (token_provider, _) = Builder::new(impersonated_credential).build_components()?;

        let err = token_provider.token().await.unwrap_err();
        assert!(!err.is_transient());

        Ok(())
    }

    #[tokio::test]
    async fn credential_full_with_quota_project_from_builder() -> TestResult {
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
        let expire_time = (OffsetDateTime::now_utc() + time::Duration::hours(1))
            .format(&time::format_description::well_known::Rfc3339)
            .unwrap();
        server.expect(
            Expectation::matching(request::method_path(
                "POST",
                "/v1/projects/-/serviceAccounts/test-principal:generateAccessToken",
            ))
            .respond_with(json_encoded(json!({
                "accessToken": "test-impersonated-token",
                "expireTime": expire_time
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
        let creds = Builder::new(impersonated_credential)
            .with_quota_project_id("test-project")
            .build()?;

        let headers = creds.headers(Extensions::new()).await?;
        match headers {
            CacheableResource::New { data, .. } => {
                assert_eq!(data.get("x-goog-user-project").unwrap(), "test-project");
            }
            CacheableResource::NotModified => panic!("Expected new headers, but got NotModified"),
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_with_target_principal() {
        let source_credentials = crate::credentials::user_account::Builder::new(json!({
            "type": "authorized_user",
            "client_id": "test-client-id",
            "client_secret": "test-client-secret",
            "refresh_token": "test-refresh-token"
        }))
        .build()
        .unwrap();

        let (token_provider, _) = Builder::from_source_credentials(source_credentials)
            .with_target_principal("test-principal@example.iam.gserviceaccount.com")
            .build_components()
            .unwrap();

        assert_eq!(
            token_provider.inner.service_account_impersonation_url,
            "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/test-principal@example.iam.gserviceaccount.com:generateAccessToken"
        );
    }

    #[tokio::test]
    async fn credential_full_with_quota_project_from_json() -> TestResult {
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
        let expire_time = (OffsetDateTime::now_utc() + time::Duration::hours(1))
            .format(&time::format_description::well_known::Rfc3339)
            .unwrap();
        server.expect(
            Expectation::matching(request::method_path(
                "POST",
                "/v1/projects/-/serviceAccounts/test-principal:generateAccessToken",
            ))
            .respond_with(json_encoded(json!({
                "accessToken": "test-impersonated-token",
                "expireTime": expire_time
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
            },
            "quota_project_id": "test-project-from-json",
        });

        let creds = Builder::new(impersonated_credential).build()?;

        let headers = creds.headers(Extensions::new()).await?;
        match headers {
            CacheableResource::New { data, .. } => {
                assert_eq!(
                    data.get("x-goog-user-project").unwrap(),
                    "test-project-from-json"
                );
            }
            CacheableResource::NotModified => panic!("Expected new headers, but got NotModified"),
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_impersonated_does_not_propagate_settings_to_source() -> TestResult {
        let server = Server::run();

        // Expectation for the source credential token request.
        // It should NOT have any scopes in the body.
        server.expect(
            Expectation::matching(all_of![
                request::method_path("POST", "/source_token"),
                request::body(json_decoded(
                    |body: &serde_json::Value| body["scopes"].is_null()
                ))
            ])
            .respond_with(json_encoded(json!({
                "access_token": "source-token",
                "expires_in": 3600,
                "token_type": "Bearer",
            }))),
        );

        let expire_time = (OffsetDateTime::now_utc() + time::Duration::hours(1))
            .format(&time::format_description::well_known::Rfc3339)
            .unwrap();

        // Expectation for the impersonation request.
        // It SHOULD have the scopes from the impersonated builder.
        server.expect(
            Expectation::matching(all_of![
                request::method_path(
                    "POST",
                    "/v1/projects/-/serviceAccounts/test-principal:generateAccessToken"
                ),
                request::headers(contains(("authorization", "Bearer source-token"))),
                request::body(json_decoded(eq(json!({
                    "scope": ["impersonated-scope"],
                    "lifetime": "3600s"
                }))))
            ])
            .respond_with(json_encoded(json!({
                "accessToken": "impersonated-token",
                "expireTime": expire_time
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
                "token_uri": server.url("/source_token").to_string()
            }
        });

        let creds = Builder::new(impersonated_credential)
            .with_scopes(vec!["impersonated-scope"])
            .with_quota_project_id("impersonated-quota-project")
            .build()?;

        // The quota project should be set on the final credentials object.
        let fmt = format!("{creds:?}");
        assert!(fmt.contains("impersonated-quota-project"));

        // Fetching the token will trigger the mock server expectations.
        let _token = creds.headers(Extensions::new()).await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_impersonated_metrics_header() -> TestResult {
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
        let expire_time = (OffsetDateTime::now_utc() + time::Duration::hours(1))
            .format(&time::format_description::well_known::Rfc3339)
            .unwrap();
        server.expect(
            Expectation::matching(all_of![
                request::method_path(
                    "POST",
                    "/v1/projects/-/serviceAccounts/test-principal:generateAccessToken"
                ),
                request::headers(contains(("x-goog-api-client", matches("cred-type/imp")))),
                request::headers(contains((
                    "x-goog-api-client",
                    matches("auth-request-type/at")
                )))
            ])
            .respond_with(json_encoded(json!({
                "accessToken": "test-impersonated-token",
                "expireTime": expire_time
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
        let (token_provider, _) = Builder::new(impersonated_credential).build_components()?;

        let token = token_provider.token().await?;
        assert_eq!(token.token, "test-impersonated-token");
        assert_eq!(token.token_type, "Bearer");

        Ok(())
    }

    #[tokio::test]
    async fn test_impersonated_retries_for_success() -> TestResult {
        let mut server = Server::run();
        // Source credential token endpoint
        server.expect(
            Expectation::matching(request::method_path("POST", "/token")).respond_with(
                json_encoded(json!({
                    "access_token": "test-user-account-token",
                    "expires_in": 3600,
                    "token_type": "Bearer",
                })),
            ),
        );

        let expire_time = (OffsetDateTime::now_utc() + time::Duration::hours(1))
            .format(&time::format_description::well_known::Rfc3339)
            .unwrap();

        // Impersonation endpoint
        let impersonation_path =
            "/v1/projects/-/serviceAccounts/test-principal:generateAccessToken";
        server.expect(
            Expectation::matching(request::method_path("POST", impersonation_path))
                .times(3)
                .respond_with(cycle![
                    status_code(503).body("try-again"),
                    status_code(503).body("try-again"),
                    status_code(200)
                        .append_header("Content-Type", "application/json")
                        .body(
                            json!({
                                "accessToken": "test-impersonated-token",
                                "expireTime": expire_time
                            })
                            .to_string()
                        ),
                ]),
        );

        let impersonated_credential = json!({
            "type": "impersonated_service_account",
            "service_account_impersonation_url": server.url(impersonation_path).to_string(),
            "source_credentials": {
                "type": "authorized_user",
                "client_id": "test-client-id",
                "client_secret": "test-client-secret",
                "refresh_token": "test-refresh-token",
                "token_uri": server.url("/token").to_string()
            }
        });

        let (token_provider, _) = Builder::new(impersonated_credential)
            .with_retry_policy(get_mock_auth_retry_policy(3))
            .with_backoff_policy(get_mock_backoff_policy())
            .with_retry_throttler(get_mock_retry_throttler())
            .build_components()?;

        let token = token_provider.token().await?;
        assert_eq!(token.token, "test-impersonated-token");

        server.verify_and_clear();
        Ok(())
    }

    #[tokio::test]
    async fn test_scopes_from_json() -> TestResult {
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
        let expire_time = (OffsetDateTime::now_utc() + time::Duration::hours(1))
            .format(&time::format_description::well_known::Rfc3339)
            .unwrap();
        server.expect(
            Expectation::matching(all_of![
                request::method_path(
                    "POST",
                    "/v1/projects/-/serviceAccounts/test-principal:generateAccessToken"
                ),
                request::body(json_decoded(eq(json!({
                    "scope": ["scope-from-json"],
                    "lifetime": "3600s"
                }))))
            ])
            .respond_with(json_encoded(json!({
                "accessToken": "test-impersonated-token",
                "expireTime": expire_time
            }))),
        );

        let impersonated_credential = json!({
            "type": "impersonated_service_account",
            "service_account_impersonation_url": server.url("/v1/projects/-/serviceAccounts/test-principal:generateAccessToken").to_string(),
            "scopes": ["scope-from-json"],
            "source_credentials": {
                "type": "authorized_user",
                "client_id": "test-client-id",
                "client_secret": "test-client-secret",
                "refresh_token": "test-refresh-token",
                "token_uri": server.url("/token").to_string()
            }
        });
        let (token_provider, _) = Builder::new(impersonated_credential).build_components()?;

        let token = token_provider.token().await?;
        assert_eq!(token.token, "test-impersonated-token");

        Ok(())
    }

    #[tokio::test]
    async fn test_with_scopes_overrides_json_scopes() -> TestResult {
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
        let expire_time = (OffsetDateTime::now_utc() + time::Duration::hours(1))
            .format(&time::format_description::well_known::Rfc3339)
            .unwrap();
        server.expect(
            Expectation::matching(all_of![
                request::method_path(
                    "POST",
                    "/v1/projects/-/serviceAccounts/test-principal:generateAccessToken"
                ),
                request::body(json_decoded(eq(json!({
                    "scope": ["scope-from-with-scopes"],
                    "lifetime": "3600s"
                }))))
            ])
            .respond_with(json_encoded(json!({
                "accessToken": "test-impersonated-token",
                "expireTime": expire_time
            }))),
        );

        let impersonated_credential = json!({
            "type": "impersonated_service_account",
            "service_account_impersonation_url": server.url("/v1/projects/-/serviceAccounts/test-principal:generateAccessToken").to_string(),
            "scopes": ["scope-from-json"],
            "source_credentials": {
                "type": "authorized_user",
                "client_id": "test-client-id",
                "client_secret": "test-client-secret",
                "refresh_token": "test-refresh-token",
                "token_uri": server.url("/token").to_string()
            }
        });
        let (token_provider, _) = Builder::new(impersonated_credential)
            .with_scopes(vec!["scope-from-with-scopes"])
            .build_components()?;

        let token = token_provider.token().await?;
        assert_eq!(token.token, "test-impersonated-token");

        Ok(())
    }

    #[tokio::test]
    async fn test_impersonated_does_not_retry_on_non_transient_failures() -> TestResult {
        let mut server = Server::run();
        // Source credential token endpoint
        server.expect(
            Expectation::matching(request::method_path("POST", "/token")).respond_with(
                json_encoded(json!({
                    "access_token": "test-user-account-token",
                    "expires_in": 3600,
                    "token_type": "Bearer",
                })),
            ),
        );

        // Impersonation endpoint
        let impersonation_path =
            "/v1/projects/-/serviceAccounts/test-principal:generateAccessToken";
        server.expect(
            Expectation::matching(request::method_path("POST", impersonation_path))
                .times(1)
                .respond_with(status_code(401)),
        );

        let impersonated_credential = json!({
            "type": "impersonated_service_account",
            "service_account_impersonation_url": server.url(impersonation_path).to_string(),
            "source_credentials": {
                "type": "authorized_user",
                "client_id": "test-client-id",
                "client_secret": "test-client-secret",
                "refresh_token": "test-refresh-token",
                "token_uri": server.url("/token").to_string()
            }
        });

        let (token_provider, _) = Builder::new(impersonated_credential)
            .with_retry_policy(get_mock_auth_retry_policy(3))
            .with_backoff_policy(get_mock_backoff_policy())
            .with_retry_throttler(get_mock_retry_throttler())
            .build_components()?;

        let err = token_provider.token().await.unwrap_err();
        assert!(!err.is_transient());

        server.verify_and_clear();
        Ok(())
    }
}
