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

use crate::build_errors::Error as BuilderError;
use crate::constants::GOOGLE_CLOUD_QUOTA_PROJECT_VAR;
use crate::errors::{self, CredentialsError};
use crate::{BuildResult, Result};
use http::{Extensions, HeaderMap};
use serde_json::Value;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

pub mod anonymous;
pub mod api_key_credentials;
pub mod external_account;
pub(crate) mod external_account_sources;
#[cfg(google_cloud_unstable_id_token)]
pub mod idtoken;
pub mod impersonated;
pub(crate) mod internal;
pub mod mds;
pub mod service_account;
pub mod subject_token;
pub mod user_account;
pub(crate) const QUOTA_PROJECT_KEY: &str = "x-goog-user-project";

#[cfg(test)]
pub(crate) const DEFAULT_UNIVERSE_DOMAIN: &str = "googleapis.com";

/// Represents an Entity Tag for a [CacheableResource].
///
/// An `EntityTag` is an opaque token that can be used to determine if a
/// cached resource has changed. The specific format of this tag is an
/// implementation detail.
///
/// As the name indicates, these are inspired by the ETags prevalent in HTTP
/// caching protocols. Their implementation is very different, and are only
/// intended for use within a single program.
#[derive(Clone, Debug, PartialEq, Default)]
pub struct EntityTag(u64);

static ENTITY_TAG_GENERATOR: AtomicU64 = AtomicU64::new(0);
impl EntityTag {
    pub fn new() -> Self {
        let value = ENTITY_TAG_GENERATOR.fetch_add(1, Ordering::SeqCst);
        Self(value)
    }
}

/// Represents a resource that can be cached, along with its [EntityTag].
///
/// This enum is used to provide cacheable data to the consumers of the credential provider.
/// It allows a data provider to return either new data (with an [EntityTag]) or
/// indicate that the caller's cached version (identified by a previously provided [EntityTag])
/// is still valid.
#[derive(Clone, PartialEq, Debug)]
pub enum CacheableResource<T> {
    NotModified,
    New { entity_tag: EntityTag, data: T },
}

/// An implementation of [crate::credentials::CredentialsProvider].
///
/// Represents a [Credentials] used to obtain the auth request headers.
///
/// In general, [Credentials][credentials-link] are "digital object that provide
/// proof of identity", the archetype may be a username and password
/// combination, but a private RSA key may be a better example.
///
/// Modern authentication protocols do not send the credentials to authenticate
/// with a service. Even when sent over encrypted transports, the credentials
/// may be accidentally exposed via logging or may be captured if there are
/// errors in the transport encryption. Because the credentials are often
/// long-lived, that risk of exposure is also long-lived.
///
/// Instead, modern authentication protocols exchange the credentials for a
/// time-limited [Token][token-link], a digital object that shows the caller was
/// in possession of the credentials. Because tokens are time limited, risk of
/// misuse is also time limited. Tokens may be further restricted to only a
/// certain subset of the RPCs in the service, or even to specific resources, or
/// only when used from a given machine (virtual or not). Further limiting the
/// risks associated with any leaks of these tokens.
///
/// This struct also abstracts token sources that are not backed by a specific
/// digital object. The canonical example is the [Metadata Service]. This
/// service is available in many Google Cloud environments, including
/// [Google Compute Engine], and [Google Kubernetes Engine].
///
/// [credentials-link]: https://cloud.google.com/docs/authentication#credentials
/// [token-link]: https://cloud.google.com/docs/authentication#token
/// [Metadata Service]: https://cloud.google.com/compute/docs/metadata/overview
/// [Google Compute Engine]: https://cloud.google.com/products/compute
/// [Google Kubernetes Engine]: https://cloud.google.com/kubernetes-engine
#[derive(Clone, Debug)]
pub struct Credentials {
    // We use an `Arc` to hold the inner implementation.
    //
    // Credentials may be shared across threads (`Send + Sync`), so an `Rc`
    // will not do.
    //
    // They also need to derive `Clone`, as the
    // `gax::http_client::ReqwestClient`s which hold them derive `Clone`. So a
    // `Box` will not do.
    inner: Arc<dyn dynamic::CredentialsProvider>,
}

impl<T> std::convert::From<T> for Credentials
where
    T: crate::credentials::CredentialsProvider + Send + Sync + 'static,
{
    fn from(value: T) -> Self {
        Self {
            inner: Arc::new(value),
        }
    }
}

impl Credentials {
    pub async fn headers(&self, extensions: Extensions) -> Result<CacheableResource<HeaderMap>> {
        self.inner.headers(extensions).await
    }

    pub async fn universe_domain(&self) -> Option<String> {
        self.inner.universe_domain().await
    }
}

/// Represents a [Credentials] used to obtain auth request headers.
///
/// In general, [Credentials][credentials-link] are "digital object that
/// provide proof of identity", the archetype may be a username and password
/// combination, but a private RSA key may be a better example.
///
/// Modern authentication protocols do not send the credentials to
/// authenticate with a service. Even when sent over encrypted transports,
/// the credentials may be accidentally exposed via logging or may be
/// captured if there are errors in the transport encryption. Because the
/// credentials are often long-lived, that risk of exposure is also
/// long-lived.
///
/// Instead, modern authentication protocols exchange the credentials for a
/// time-limited [Token][token-link], a digital object that shows the caller
/// was in possession of the credentials. Because tokens are time limited,
/// risk of misuse is also time limited. Tokens may be further restricted to
/// only a certain subset of the RPCs in the service, or even to specific
/// resources, or only when used from a given machine (virtual or not).
/// Further limiting the risks associated with any leaks of these tokens.
///
/// This struct also abstracts token sources that are not backed by a
/// specific digital object. The canonical example is the
/// [Metadata Service]. This service is available in many Google Cloud
/// environments, including [Google Compute Engine], and
/// [Google Kubernetes Engine].
///
/// # Notes
///
/// Application developers who directly use the Auth SDK can use this trait,
/// along with [crate::credentials::Credentials::from()] to mock the credentials.
/// Application developers who use the Google Cloud Rust SDK directly should not
/// need this functionality.
///
/// [credentials-link]: https://cloud.google.com/docs/authentication#credentials
/// [token-link]: https://cloud.google.com/docs/authentication#token
/// [Metadata Service]: https://cloud.google.com/compute/docs/metadata/overview
/// [Google Compute Engine]: https://cloud.google.com/products/compute
/// [Google Kubernetes Engine]: https://cloud.google.com/kubernetes-engine
pub trait CredentialsProvider: std::fmt::Debug {
    /// Asynchronously constructs the auth headers.
    ///
    /// Different auth tokens are sent via different headers. The
    /// [Credentials] constructs the headers (and header values) that should be
    /// sent with a request.
    ///
    /// # Parameters
    /// * `extensions` - An `http::Extensions` map that can be used to pass additional
    ///   context to the credential provider. For caching purposes, this can include
    ///   an [EntityTag] from a previously returned [`CacheableResource<HeaderMap>`].
    ///   If a valid `EntityTag` is provided and the underlying authentication data
    ///   has not changed, this method returns `Ok(CacheableResource::NotModified)`.
    ///
    /// # Returns
    /// A `Future` that resolves to a `Result` containing:
    /// * `Ok(CacheableResource::New { entity_tag, data })`: If new or updated headers
    ///   are available.
    /// * `Ok(CacheableResource::NotModified)`: If the headers have not changed since
    ///   the ETag provided via `extensions` was issued.
    /// * `Err(CredentialsError)`: If an error occurs while trying to fetch or
    ///   generating the headers.
    fn headers(
        &self,
        extensions: Extensions,
    ) -> impl Future<Output = Result<CacheableResource<HeaderMap>>> + Send;

    /// Retrieves the universe domain associated with the credentials, if any.
    fn universe_domain(&self) -> impl Future<Output = Option<String>> + Send;
}

pub(crate) mod dynamic {
    use super::Result;
    use super::{CacheableResource, Extensions, HeaderMap};

    /// A dyn-compatible, crate-private version of `CredentialsProvider`.
    #[async_trait::async_trait]
    pub trait CredentialsProvider: Send + Sync + std::fmt::Debug {
        /// Asynchronously constructs the auth headers.
        ///
        /// Different auth tokens are sent via different headers. The
        /// [Credentials] constructs the headers (and header values) that should be
        /// sent with a request.
        ///
        /// # Parameters
        /// * `extensions` - An `http::Extensions` map that can be used to pass additional
        ///   context to the credential provider. For caching purposes, this can include
        ///   an [EntityTag] from a previously returned [CacheableResource<HeaderMap>].
        ///   If a valid `EntityTag` is provided and the underlying authentication data
        ///   has not changed, this method returns `Ok(CacheableResource::NotModified)`.
        ///
        /// # Returns
        /// A `Future` that resolves to a `Result` containing:
        /// * `Ok(CacheableResource::New { entity_tag, data })`: If new or updated headers
        ///   are available.
        /// * `Ok(CacheableResource::NotModified)`: If the headers have not changed since
        ///   the ETag provided via `extensions` was issued.
        /// * `Err(CredentialsError)`: If an error occurs while trying to fetch or
        ///   generating the headers.
        async fn headers(&self, extensions: Extensions) -> Result<CacheableResource<HeaderMap>>;

        /// Retrieves the universe domain associated with the credentials, if any.
        async fn universe_domain(&self) -> Option<String> {
            Some("googleapis.com".to_string())
        }
    }

    /// The public CredentialsProvider implements the dyn-compatible CredentialsProvider.
    #[async_trait::async_trait]
    impl<T> CredentialsProvider for T
    where
        T: super::CredentialsProvider + Send + Sync,
    {
        async fn headers(&self, extensions: Extensions) -> Result<CacheableResource<HeaderMap>> {
            T::headers(self, extensions).await
        }
        async fn universe_domain(&self) -> Option<String> {
            T::universe_domain(self).await
        }
    }
}

/// A builder for constructing [`Credentials`] instances.
///
/// This builder loads credentials according to the standard
/// [Application Default Credentials (ADC)][ADC-link] strategy.
/// ADC is the recommended approach for most applications and conforms to
/// [AIP-4110]. If you need to load credentials from a non-standard location
/// or source, you can use Builders on the specific credential types.
///
/// Common use cases where using ADC would is useful include:
/// - Your application is deployed to a Google Cloud environment such as
///   [Google Compute Engine (GCE)][gce-link],
///   [Google Kubernetes Engine (GKE)][gke-link], or [Cloud Run]. Each of these
///   deployment environments provides a default service account to the
///   application, and offers mechanisms to change this default service account
///   without any code changes to your application.
/// - You are testing or developing the application on a workstation (physical
///   or virtual). These credentials will use your preferences as set with
///   [gcloud auth application-default]. These preferences can be your own
///   Google Cloud user credentials, or some service account.
/// - Regardless of where your application is running, you can use the
///   `GOOGLE_APPLICATION_CREDENTIALS` environment variable to override the
///   defaults. This environment variable should point to a file containing a
///   service account key file, or a JSON object describing your user
///   credentials.
///
/// The headers returned by these credentials should be used in the
/// Authorization HTTP header.
///
/// The Google Cloud client libraries for Rust will typically find and use these
/// credentials automatically if a credentials file exists in the standard ADC
/// search paths. You might instantiate these credentials if you need to:
/// - Override the OAuth 2.0 **scopes** being requested for the access token.
/// - Override the **quota project ID** for billing and quota management.
///
/// # Example: fetching headers using ADC
/// ```
/// # use google_cloud_auth::credentials::Builder;
/// # use http::Extensions;
/// # tokio_test::block_on(async {
/// let credentials = Builder::default()
///     .with_quota_project_id("my-project")
///     .build()?;
/// let headers = credentials.headers(Extensions::new()).await?;
/// println!("Headers: {headers:?}");
/// # Ok::<(), anyhow::Error>(())
/// # });
/// ```
///
/// [ADC-link]: https://cloud.google.com/docs/authentication/application-default-credentials
/// [AIP-4110]: https://google.aip.dev/auth/4110
/// [Cloud Run]: https://cloud.google.com/run
/// [gce-link]: https://cloud.google.com/products/compute
/// [gcloud auth application-default]: https://cloud.google.com/sdk/gcloud/reference/auth/application-default
/// [gke-link]: https://cloud.google.com/kubernetes-engine
#[derive(Debug)]
pub struct Builder {
    quota_project_id: Option<String>,
    scopes: Option<Vec<String>>,
}

impl Default for Builder {
    /// Creates a new builder where credentials will be obtained via [application-default login].
    ///
    /// # Example
    /// ```
    /// # use google_cloud_auth::credentials::Builder;
    /// # tokio_test::block_on(async {
    /// let credentials = Builder::default().build();
    /// # });
    /// ```
    ///
    /// [application-default login]: https://cloud.google.com/sdk/gcloud/reference/auth/application-default/login
    fn default() -> Self {
        Self {
            quota_project_id: None,
            scopes: None,
        }
    }
}

impl Builder {
    /// Sets the [quota project] for these credentials.
    ///
    /// In some services, you can use an account in one project for authentication
    /// and authorization, and charge the usage to a different project. This requires
    /// that the user has `serviceusage.services.use` permissions on the quota project.
    ///
    /// ## Important: Precedence
    /// If the `GOOGLE_CLOUD_QUOTA_PROJECT` environment variable is set,
    /// its value will be used **instead of** the value provided to this method.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_auth::credentials::Builder;
    /// # tokio_test::block_on(async {
    /// let credentials = Builder::default()
    ///     .with_quota_project_id("my-project")
    ///     .build();
    /// # });
    /// ```
    ///
    /// [quota project]: https://cloud.google.com/docs/quotas/quota-project
    pub fn with_quota_project_id<S: Into<String>>(mut self, quota_project_id: S) -> Self {
        self.quota_project_id = Some(quota_project_id.into());
        self
    }

    /// Sets the [scopes] for these credentials.
    ///
    /// `scopes` act as an additional restriction in addition to the IAM permissions
    /// granted to the principal (user or service account) that creates the token.
    ///
    /// `scopes` define the *permissions being requested* for this specific access token
    /// when interacting with a service. For example,
    /// `https://www.googleapis.com/auth/devstorage.read_write`.
    ///
    /// IAM permissions, on the other hand, define the *underlying capabilities*
    /// the principal possesses within a system. For example, `storage.buckets.delete`.
    ///
    /// The credentials certify that a particular token was created by a certain principal.
    ///
    /// When a token generated with specific scopes is used, the request must be permitted
    /// by both the the principals's underlying IAM permissions and the scopes requested
    /// for the token.
    ///
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
    ///
    /// # Errors
    ///
    /// Returns a [CredentialsError] if a unsupported credential type is provided
    /// or if the JSON value is either malformed
    /// or missing required fields. For more information, on how to generate
    /// json, consult the relevant section in the [application-default credentials] guide.
    ///
    /// [application-default credentials]: https://cloud.google.com/docs/authentication/application-default-credentials
    pub fn build(self) -> BuildResult<Credentials> {
        let json_data = match load_adc()? {
            AdcContents::Contents(contents) => {
                Some(serde_json::from_str(&contents).map_err(BuilderError::parsing)?)
            }
            AdcContents::FallbackToMds => None,
        };
        let quota_project_id = std::env::var(GOOGLE_CLOUD_QUOTA_PROJECT_VAR)
            .ok()
            .or(self.quota_project_id);
        build_credentials(json_data, quota_project_id, self.scopes)
    }

    #[cfg(google_cloud_unstable_signed_url)]
    pub fn build_signer(self) -> BuildResult<crate::signer::Signer> {
        let json_data = match load_adc()? {
            AdcContents::Contents(contents) => {
                Some(serde_json::from_str(&contents).map_err(BuilderError::parsing)?)
            }
            AdcContents::FallbackToMds => None,
        };
        let quota_project_id = std::env::var(GOOGLE_CLOUD_QUOTA_PROJECT_VAR)
            .ok()
            .or(self.quota_project_id);
        build_signer(json_data, quota_project_id, self.scopes)
    }
}

#[derive(Debug, PartialEq)]
enum AdcPath {
    FromEnv(String),
    WellKnown(String),
}

#[derive(Debug, PartialEq)]
enum AdcContents {
    Contents(String),
    FallbackToMds,
}

fn extract_credential_type(json: &Value) -> BuildResult<&str> {
    json.get("type")
        .ok_or_else(|| BuilderError::parsing("no `type` field found."))?
        .as_str()
        .ok_or_else(|| BuilderError::parsing("`type` field is not a string."))
}

/// Applies common optional configurations (quota project ID, scopes) to a
/// specific credential builder instance and then builds it.
///
/// This macro centralizes the logic for optionally calling `.with_quota_project_id()`
/// and `.with_scopes()` on different underlying credential builders (like
/// `mds::Builder`, `service_account::Builder`, etc.) before calling `.build()`.
/// It helps avoid repetitive code in the `build_credentials` function.
macro_rules! config_builder {
    ($builder_instance:expr, $quota_project_id_option:expr, $scopes_option:expr, $apply_scopes_closure:expr) => {{
        let builder = $builder_instance;
        let builder = $quota_project_id_option
            .into_iter()
            .fold(builder, |b, qp| b.with_quota_project_id(qp));

        let builder = $scopes_option
            .into_iter()
            .fold(builder, |b, s| $apply_scopes_closure(b, s));

        builder.build()
    }};
}

/// Applies common optional configurations (quota project ID, scopes) to a
/// specific credential builder instance and then return a signer for it.
#[cfg(google_cloud_unstable_signed_url)]
macro_rules! config_signer {
    ($builder_instance:expr, $quota_project_id_option:expr, $scopes_option:expr, $apply_scopes_closure:expr) => {{
        let builder = $builder_instance;
        let builder = $quota_project_id_option
            .into_iter()
            .fold(builder, |b, qp| b.with_quota_project_id(qp));

        let builder = $scopes_option
            .into_iter()
            .fold(builder, |b, s| $apply_scopes_closure(b, s));

        builder.signer()
    }};
}

fn build_credentials(
    json: Option<Value>,
    quota_project_id: Option<String>,
    scopes: Option<Vec<String>>,
) -> BuildResult<Credentials> {
    match json {
        None => config_builder!(
            mds::Builder::from_adc(),
            quota_project_id,
            scopes,
            |b: mds::Builder, s: Vec<String>| b.with_scopes(s)
        ),
        Some(json) => {
            let cred_type = extract_credential_type(&json)?;
            match cred_type {
                "authorized_user" => {
                    config_builder!(
                        user_account::Builder::new(json),
                        quota_project_id,
                        scopes,
                        |b: user_account::Builder, s: Vec<String>| b.with_scopes(s)
                    )
                }
                "service_account" => config_builder!(
                    service_account::Builder::new(json),
                    quota_project_id,
                    scopes,
                    |b: service_account::Builder, s: Vec<String>| b
                        .with_access_specifier(service_account::AccessSpecifier::from_scopes(s))
                ),
                "impersonated_service_account" => {
                    config_builder!(
                        impersonated::Builder::new(json),
                        quota_project_id,
                        scopes,
                        |b: impersonated::Builder, s: Vec<String>| b.with_scopes(s)
                    )
                }
                "external_account" => config_builder!(
                    external_account::Builder::new(json),
                    quota_project_id,
                    scopes,
                    |b: external_account::Builder, s: Vec<String>| b.with_scopes(s)
                ),
                _ => Err(BuilderError::unknown_type(cred_type)),
            }
        }
    }
}

#[cfg(google_cloud_unstable_signed_url)]
fn build_signer(
    json: Option<Value>,
    quota_project_id: Option<String>,
    scopes: Option<Vec<String>>,
) -> BuildResult<crate::signer::Signer> {
    match json {
        None => config_signer!(
            mds::Builder::from_adc(),
            quota_project_id,
            scopes,
            |b: mds::Builder, s: Vec<String>| b.with_scopes(s)
        ),
        Some(json) => {
            let cred_type = extract_credential_type(&json)?;
            match cred_type {
                "authorized_user" => config_signer!(
                    user_account::Builder::new(json),
                    quota_project_id,
                    scopes,
                    |b: user_account::Builder, s: Vec<String>| b.with_scopes(s)
                ),
                "service_account" => config_signer!(
                    service_account::Builder::new(json),
                    quota_project_id,
                    scopes,
                    |b: service_account::Builder, s: Vec<String>| b
                        .with_access_specifier(service_account::AccessSpecifier::from_scopes(s))
                ),
                "impersonated_service_account" => {
                    config_signer!(
                        impersonated::Builder::new(json),
                        quota_project_id,
                        scopes,
                        |b: impersonated::Builder, s: Vec<String>| b.with_scopes(s)
                    )
                }
                "external_account" => panic!("external account signer not supported yet"),
                _ => Err(BuilderError::unknown_type(cred_type)),
            }
        }
    }
}

fn path_not_found(path: String) -> BuilderError {
    BuilderError::loading(format!(
        "{path}. {}",
        concat!(
            "This file name was found in the `GOOGLE_APPLICATION_CREDENTIALS` ",
            "environment variable. Verify this environment variable points to ",
            "a valid file."
        )
    ))
}

fn load_adc() -> BuildResult<AdcContents> {
    match adc_path() {
        None => Ok(AdcContents::FallbackToMds),
        Some(AdcPath::FromEnv(path)) => match std::fs::read_to_string(&path) {
            Ok(contents) => Ok(AdcContents::Contents(contents)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Err(path_not_found(path)),
            Err(e) => Err(BuilderError::loading(e)),
        },
        Some(AdcPath::WellKnown(path)) => match std::fs::read_to_string(path) {
            Ok(contents) => Ok(AdcContents::Contents(contents)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(AdcContents::FallbackToMds),
            Err(e) => Err(BuilderError::loading(e)),
        },
    }
}

/// The path to Application Default Credentials (ADC), as specified in [AIP-4110].
///
/// [AIP-4110]: https://google.aip.dev/auth/4110
fn adc_path() -> Option<AdcPath> {
    if let Ok(path) = std::env::var("GOOGLE_APPLICATION_CREDENTIALS") {
        return Some(AdcPath::FromEnv(path));
    }
    Some(AdcPath::WellKnown(adc_well_known_path()?))
}

/// The well-known path to ADC on Windows, as specified in [AIP-4113].
///
/// [AIP-4113]: https://google.aip.dev/auth/4113
#[cfg(target_os = "windows")]
fn adc_well_known_path() -> Option<String> {
    std::env::var("APPDATA")
        .ok()
        .map(|root| root + "/gcloud/application_default_credentials.json")
}

/// The well-known path to ADC on Linux and Mac, as specified in [AIP-4113].
///
/// [AIP-4113]: https://google.aip.dev/auth/4113
#[cfg(not(target_os = "windows"))]
fn adc_well_known_path() -> Option<String> {
    std::env::var("HOME")
        .ok()
        .map(|root| root + "/.config/gcloud/application_default_credentials.json")
}

/// A module providing invalid credentials where authentication does not matter.
///
/// These credentials are a convenient way to avoid errors from loading
/// Application Default Credentials in tests.
///
/// This module is mainly relevant to other `google-cloud-*` crates, but some
/// external developers (i.e. consumers, not developers of `google-cloud-rust`)
/// may find it useful.
// Skipping mutation testing for this module. As it exclusively provides
// hardcoded credential stubs for testing purposes.
#[cfg_attr(test, mutants::skip)]
#[doc(hidden)]
pub mod testing {
    use super::CacheableResource;
    use crate::Result;
    use crate::credentials::Credentials;
    use crate::credentials::dynamic::CredentialsProvider;
    use http::{Extensions, HeaderMap};
    use std::sync::Arc;

    /// A simple credentials implementation to use in tests.
    ///
    /// Always return an error in `headers()`.
    pub fn error_credentials(retryable: bool) -> Credentials {
        Credentials {
            inner: Arc::from(ErrorCredentials(retryable)),
        }
    }

    #[derive(Debug, Default)]
    struct ErrorCredentials(bool);

    #[async_trait::async_trait]
    impl CredentialsProvider for ErrorCredentials {
        async fn headers(&self, _extensions: Extensions) -> Result<CacheableResource<HeaderMap>> {
            Err(super::CredentialsError::from_msg(self.0, "test-only"))
        }

        async fn universe_domain(&self) -> Option<String> {
            None
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use base64::Engine;
    use gax::backoff_policy::BackoffPolicy;
    use gax::retry_policy::RetryPolicy;
    use gax::retry_result::RetryResult;
    use gax::retry_state::RetryState;
    use gax::retry_throttler::RetryThrottler;
    use mockall::mock;
    use reqwest::header::AUTHORIZATION;
    use rsa::BigUint;
    use rsa::RsaPrivateKey;
    use rsa::pkcs8::{EncodePrivateKey, LineEnding};
    use scoped_env::ScopedEnv;
    use std::error::Error;
    use std::sync::LazyLock;
    use test_case::test_case;
    use tokio::time::Duration;

    pub(crate) fn find_source_error<'a, T: Error + 'static>(
        error: &'a (dyn Error + 'static),
    ) -> Option<&'a T> {
        let mut source = error.source();
        while let Some(err) = source {
            if let Some(target_err) = err.downcast_ref::<T>() {
                return Some(target_err);
            }
            source = err.source();
        }
        None
    }

    mock! {
        #[derive(Debug)]
        pub RetryPolicy {}
        impl RetryPolicy for RetryPolicy {
            fn on_error(
                &self,
                state: &RetryState,
                error: gax::error::Error,
            ) -> RetryResult;
        }
    }

    mock! {
        #[derive(Debug)]
        pub BackoffPolicy {}
        impl BackoffPolicy for BackoffPolicy {
            fn on_failure(&self, state: &RetryState) -> std::time::Duration;
        }
    }

    mockall::mock! {
        #[derive(Debug)]
        pub RetryThrottler {}
        impl RetryThrottler for RetryThrottler {
            fn throttle_retry_attempt(&self) -> bool;
            fn on_retry_failure(&mut self, error: &RetryResult);
            fn on_success(&mut self);
        }
    }

    type TestResult = std::result::Result<(), Box<dyn std::error::Error>>;

    pub(crate) fn get_mock_auth_retry_policy(attempts: usize) -> MockRetryPolicy {
        let mut retry_policy = MockRetryPolicy::new();
        retry_policy
            .expect_on_error()
            .returning(move |state, error| {
                if state.attempt_count >= attempts as u32 {
                    return RetryResult::Exhausted(error);
                }
                let is_transient = error
                    .source()
                    .and_then(|e| e.downcast_ref::<CredentialsError>())
                    .is_some_and(|ce| ce.is_transient());
                if is_transient {
                    RetryResult::Continue(error)
                } else {
                    RetryResult::Permanent(error)
                }
            });
        retry_policy
    }

    pub(crate) fn get_mock_backoff_policy() -> MockBackoffPolicy {
        let mut backoff_policy = MockBackoffPolicy::new();
        backoff_policy
            .expect_on_failure()
            .return_const(Duration::from_secs(0));
        backoff_policy
    }

    pub(crate) fn get_mock_retry_throttler() -> MockRetryThrottler {
        let mut throttler = MockRetryThrottler::new();
        throttler.expect_on_retry_failure().return_const(());
        throttler
            .expect_throttle_retry_attempt()
            .return_const(false);
        throttler.expect_on_success().return_const(());
        throttler
    }

    pub(crate) fn get_headers_from_cache(
        headers: CacheableResource<HeaderMap>,
    ) -> Result<HeaderMap> {
        match headers {
            CacheableResource::New { data, .. } => Ok(data),
            CacheableResource::NotModified => Err(CredentialsError::from_msg(
                false,
                "Expecting headers to be present",
            )),
        }
    }

    pub(crate) fn get_token_from_headers(headers: CacheableResource<HeaderMap>) -> Option<String> {
        match headers {
            CacheableResource::New { data, .. } => data
                .get(AUTHORIZATION)
                .and_then(|token_value| token_value.to_str().ok())
                .and_then(|s| s.split_whitespace().nth(1))
                .map(|s| s.to_string()),
            CacheableResource::NotModified => None,
        }
    }

    pub(crate) fn get_token_type_from_headers(
        headers: CacheableResource<HeaderMap>,
    ) -> Option<String> {
        match headers {
            CacheableResource::New { data, .. } => data
                .get(AUTHORIZATION)
                .and_then(|token_value| token_value.to_str().ok())
                .and_then(|s| s.split_whitespace().next())
                .map(|s| s.to_string()),
            CacheableResource::NotModified => None,
        }
    }

    pub static RSA_PRIVATE_KEY: LazyLock<RsaPrivateKey> = LazyLock::new(|| {
        let p_str: &str = "141367881524527794394893355677826002829869068195396267579403819572502936761383874443619453704612633353803671595972343528718438130450055151198231345212263093247511629886734453413988207866331439612464122904648042654465604881130663408340669956544709445155137282157402427763452856646879397237752891502149781819597";
        let q_str: &str = "179395413952110013801471600075409598322058038890563483332288896635704255883613060744402506322679437982046475766067250097809676406576067239936945362857700460740092421061356861438909617220234758121022105150630083703531219941303688818533566528599328339894969707615478438750812672509434761181735933851075292740309";
        let e_str: &str = "65537";

        let p = BigUint::parse_bytes(p_str.as_bytes(), 10).expect("Failed to parse prime P");
        let q = BigUint::parse_bytes(q_str.as_bytes(), 10).expect("Failed to parse prime Q");
        let public_exponent =
            BigUint::parse_bytes(e_str.as_bytes(), 10).expect("Failed to parse public exponent");

        RsaPrivateKey::from_primes(vec![p, q], public_exponent)
            .expect("Failed to create RsaPrivateKey from primes")
    });

    pub static PKCS8_PK: LazyLock<String> = LazyLock::new(|| {
        RSA_PRIVATE_KEY
            .to_pkcs8_pem(LineEnding::LF)
            .expect("Failed to encode key to PKCS#8 PEM")
            .to_string()
    });

    pub fn b64_decode_to_json(s: String) -> serde_json::Value {
        let decoded = String::from_utf8(
            base64::engine::general_purpose::URL_SAFE_NO_PAD
                .decode(s)
                .unwrap(),
        )
        .unwrap();
        serde_json::from_str(&decoded).unwrap()
    }

    #[cfg(target_os = "windows")]
    #[test]
    #[serial_test::serial]
    fn adc_well_known_path_windows() {
        let _creds = ScopedEnv::remove("GOOGLE_APPLICATION_CREDENTIALS");
        let _appdata = ScopedEnv::set("APPDATA", "C:/Users/foo");
        assert_eq!(
            adc_well_known_path(),
            Some("C:/Users/foo/gcloud/application_default_credentials.json".to_string())
        );
        assert_eq!(
            adc_path(),
            Some(AdcPath::WellKnown(
                "C:/Users/foo/gcloud/application_default_credentials.json".to_string()
            ))
        );
    }

    #[cfg(target_os = "windows")]
    #[test]
    #[serial_test::serial]
    fn adc_well_known_path_windows_no_appdata() {
        let _creds = ScopedEnv::remove("GOOGLE_APPLICATION_CREDENTIALS");
        let _appdata = ScopedEnv::remove("APPDATA");
        assert_eq!(adc_well_known_path(), None);
        assert_eq!(adc_path(), None);
    }

    #[cfg(not(target_os = "windows"))]
    #[test]
    #[serial_test::serial]
    fn adc_well_known_path_posix() {
        let _creds = ScopedEnv::remove("GOOGLE_APPLICATION_CREDENTIALS");
        let _home = ScopedEnv::set("HOME", "/home/foo");
        assert_eq!(
            adc_well_known_path(),
            Some("/home/foo/.config/gcloud/application_default_credentials.json".to_string())
        );
        assert_eq!(
            adc_path(),
            Some(AdcPath::WellKnown(
                "/home/foo/.config/gcloud/application_default_credentials.json".to_string()
            ))
        );
    }

    #[cfg(not(target_os = "windows"))]
    #[test]
    #[serial_test::serial]
    fn adc_well_known_path_posix_no_home() {
        let _creds = ScopedEnv::remove("GOOGLE_APPLICATION_CREDENTIALS");
        let _appdata = ScopedEnv::remove("HOME");
        assert_eq!(adc_well_known_path(), None);
        assert_eq!(adc_path(), None);
    }

    #[test]
    #[serial_test::serial]
    fn adc_path_from_env() {
        let _creds = ScopedEnv::set(
            "GOOGLE_APPLICATION_CREDENTIALS",
            "/usr/bar/application_default_credentials.json",
        );
        assert_eq!(
            adc_path(),
            Some(AdcPath::FromEnv(
                "/usr/bar/application_default_credentials.json".to_string()
            ))
        );
    }

    #[test]
    #[serial_test::serial]
    fn load_adc_no_well_known_path_fallback_to_mds() {
        let _e1 = ScopedEnv::remove("GOOGLE_APPLICATION_CREDENTIALS");
        let _e2 = ScopedEnv::remove("HOME"); // For posix
        let _e3 = ScopedEnv::remove("APPDATA"); // For windows
        assert_eq!(load_adc().unwrap(), AdcContents::FallbackToMds);
    }

    #[test]
    #[serial_test::serial]
    fn load_adc_no_file_at_well_known_path_fallback_to_mds() {
        // Create a new temp directory. There is not an ADC file in here.
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().to_str().unwrap();
        let _e1 = ScopedEnv::remove("GOOGLE_APPLICATION_CREDENTIALS");
        let _e2 = ScopedEnv::set("HOME", path); // For posix
        let _e3 = ScopedEnv::set("APPDATA", path); // For windows
        assert_eq!(load_adc().unwrap(), AdcContents::FallbackToMds);
    }

    #[test]
    #[serial_test::serial]
    fn load_adc_no_file_at_env_is_error() {
        let _e = ScopedEnv::set("GOOGLE_APPLICATION_CREDENTIALS", "file-does-not-exist.json");
        let err = load_adc().unwrap_err();
        assert!(err.is_loading(), "{err:?}");
        let msg = format!("{err:?}");
        assert!(msg.contains("file-does-not-exist.json"), "{err:?}");
        assert!(msg.contains("GOOGLE_APPLICATION_CREDENTIALS"), "{err:?}");
    }

    #[test]
    #[serial_test::serial]
    fn load_adc_success() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let path = file.into_temp_path();
        std::fs::write(&path, "contents").expect("Unable to write to temporary file.");
        let _e = ScopedEnv::set("GOOGLE_APPLICATION_CREDENTIALS", path.to_str().unwrap());

        assert_eq!(
            load_adc().unwrap(),
            AdcContents::Contents("contents".to_string())
        );
    }

    #[test_case(true; "retryable")]
    #[test_case(false; "non-retryable")]
    #[tokio::test]
    async fn error_credentials(retryable: bool) {
        let credentials = super::testing::error_credentials(retryable);
        assert!(
            credentials.universe_domain().await.is_none(),
            "{credentials:?}"
        );
        let err = credentials.headers(Extensions::new()).await.err().unwrap();
        assert_eq!(err.is_transient(), retryable, "{err:?}");
        let err = credentials.headers(Extensions::new()).await.err().unwrap();
        assert_eq!(err.is_transient(), retryable, "{err:?}");
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn create_access_token_credentials_fallback_to_mds_with_quota_project_override() {
        let _e1 = ScopedEnv::remove("GOOGLE_APPLICATION_CREDENTIALS");
        let _e2 = ScopedEnv::remove("HOME"); // For posix
        let _e3 = ScopedEnv::remove("APPDATA"); // For windows
        let _e4 = ScopedEnv::set(GOOGLE_CLOUD_QUOTA_PROJECT_VAR, "env-quota-project");

        let mds = Builder::default()
            .with_quota_project_id("test-quota-project")
            .build()
            .unwrap();
        let fmt = format!("{mds:?}");
        assert!(fmt.contains("MDSCredentials"));
        assert!(
            fmt.contains("env-quota-project"),
            "Expected 'env-quota-project', got: {fmt}"
        );
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn create_access_token_credentials_with_quota_project_from_builder() {
        let _e1 = ScopedEnv::remove("GOOGLE_APPLICATION_CREDENTIALS");
        let _e2 = ScopedEnv::remove("HOME"); // For posix
        let _e3 = ScopedEnv::remove("APPDATA"); // For windows
        let _e4 = ScopedEnv::remove(GOOGLE_CLOUD_QUOTA_PROJECT_VAR);

        let creds = Builder::default()
            .with_quota_project_id("test-quota-project")
            .build()
            .unwrap();
        let fmt = format!("{creds:?}");
        assert!(
            fmt.contains("test-quota-project"),
            "Expected 'test-quota-project', got: {fmt}"
        );
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn create_access_token_service_account_credentials_with_scopes() -> TestResult {
        let _e1 = ScopedEnv::remove(GOOGLE_CLOUD_QUOTA_PROJECT_VAR);
        let mut service_account_key = serde_json::json!({
            "type": "service_account",
            "project_id": "test-project-id",
            "private_key_id": "test-private-key-id",
            "private_key": "-----BEGIN PRIVATE KEY-----\nBLAHBLAHBLAH\n-----END PRIVATE KEY-----\n",
            "client_email": "test-client-email",
            "universe_domain": "test-universe-domain"
        });

        let scopes =
            ["https://www.googleapis.com/auth/pubsub, https://www.googleapis.com/auth/translate"];

        service_account_key["private_key"] = Value::from(PKCS8_PK.clone());

        let file = tempfile::NamedTempFile::new().unwrap();
        let path = file.into_temp_path();
        std::fs::write(&path, service_account_key.to_string())
            .expect("Unable to write to temporary file.");
        let _e = ScopedEnv::set("GOOGLE_APPLICATION_CREDENTIALS", path.to_str().unwrap());

        let sac = Builder::default()
            .with_quota_project_id("test-quota-project")
            .with_scopes(scopes)
            .build()
            .unwrap();

        let headers = sac.headers(Extensions::new()).await?;
        let token = get_token_from_headers(headers).unwrap();
        let parts: Vec<_> = token.split('.').collect();
        assert_eq!(parts.len(), 3);
        let claims = b64_decode_to_json(parts.get(1).unwrap().to_string());

        let fmt = format!("{sac:?}");
        assert!(fmt.contains("ServiceAccountCredentials"));
        assert!(fmt.contains("test-quota-project"));
        assert_eq!(claims["scope"], scopes.join(" "));

        Ok(())
    }
}
