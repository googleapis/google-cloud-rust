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

mod api_key_credentials;
// Export API Key factory function and options
pub use api_key_credentials::{ApiKeyOptions, create_api_key_credentials};

pub mod mds;
pub mod service_account;
pub mod user_account;

use crate::Result;
use crate::errors::{self, CredentialsError};
use http::header::{HeaderName, HeaderValue};
use serde_json::Value;
use std::future::Future;
use std::sync::Arc;

pub(crate) const QUOTA_PROJECT_KEY: &str = "x-goog-user-project";
pub(crate) const DEFAULT_UNIVERSE_DOMAIN: &str = "googleapis.com";

/// An implementation of [crate::credentials::CredentialsTrait].
///
/// Represents a [Credentials] used to obtain auth [Token][crate::token::Token]s
/// and the corresponding request headers.
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
    inner: Arc<dyn dynamic::CredentialsTrait>,
}

impl<T> std::convert::From<T> for Credentials
where
    T: crate::credentials::CredentialsTrait + Send + Sync + 'static,
{
    fn from(value: T) -> Self {
        Self {
            inner: Arc::new(value),
        }
    }
}

impl Credentials {
    pub async fn token(&self) -> Result<crate::token::Token> {
        self.inner.token().await
    }

    pub async fn headers(&self) -> Result<Vec<(HeaderName, HeaderValue)>> {
        self.inner.headers().await
    }

    pub async fn universe_domain(&self) -> Option<String> {
        self.inner.universe_domain().await
    }
}

/// Represents a [Credentials] used to obtain auth
/// [Token][crate::token::Token]s and the corresponding request headers.
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
pub trait CredentialsTrait: std::fmt::Debug {
    /// Asynchronously retrieves a token.
    ///
    /// Returns a [Token][crate::token::Token] for the current credentials.
    /// The underlying implementation refreshes the token as needed.
    fn token(&self) -> impl Future<Output = Result<crate::token::Token>> + Send;

    /// Asynchronously constructs the auth headers.
    ///
    /// Different auth tokens are sent via different headers. The
    /// [Credentials] constructs the headers (and header values) that should be
    /// sent with a request.
    ///
    /// The underlying implementation refreshes the token as needed.
    fn headers(&self) -> impl Future<Output = Result<Vec<(HeaderName, HeaderValue)>>> + Send;

    /// Retrieves the universe domain associated with the credentials, if any.
    fn universe_domain(&self) -> impl Future<Output = Option<String>> + Send;
}

pub(crate) mod dynamic {
    use super::Result;
    use super::{HeaderName, HeaderValue};

    /// A dyn-compatible, crate-private version of `CredentialsTrait`.
    #[async_trait::async_trait]
    pub trait CredentialsTrait: Send + Sync + std::fmt::Debug {
        /// Asynchronously retrieves a token.
        ///
        /// Returns a [Token][crate::token::Token] for the current credentials.
        /// The underlying implementation refreshes the token as needed.
        async fn token(&self) -> Result<crate::token::Token>;

        /// Asynchronously constructs the auth headers.
        ///
        /// Different auth tokens are sent via different headers. The
        /// [Credentials] constructs the headers (and header values) that should be
        /// sent with a request.
        ///
        /// The underlying implementation refreshes the token as needed.
        async fn headers(&self) -> Result<Vec<(HeaderName, HeaderValue)>>;

        /// Retrieves the universe domain associated with the credentials, if any.
        async fn universe_domain(&self) -> Option<String> {
            Some("googleapis.com".to_string())
        }
    }

    /// The public CredentialsTrait implements the dyn-compatible CredentialsTrait.
    #[async_trait::async_trait]
    impl<T> CredentialsTrait for T
    where
        T: super::CredentialsTrait + Send + Sync,
    {
        async fn token(&self) -> Result<crate::token::Token> {
            T::token(self).await
        }
        async fn headers(&self) -> Result<Vec<(HeaderName, HeaderValue)>> {
            T::headers(self).await
        }
        async fn universe_domain(&self) -> Option<String> {
            T::universe_domain(self).await
        }
    }
}

#[derive(Debug, Clone)]
enum CredentialsSource {
    CredentialsJson(Value),
    DefaultCredentials,
}

/// A builder for constructing [`Credentials`] instances.
///
/// By default (using [`Builder::default`]), the builder is configured to load
/// credentials according to the standard [Application Default Credentials (ADC)][ADC-link]
/// strategy. ADC is the recommended approach for most applications and conforms to
/// [AIP-4110]. If you need to load credentials from a non-standard location or source,
/// you can provide specific credential JSON directly using [`Builder::new`].
///
/// Common use cases where using ADC would is useful include:
/// - Your application is deployed to a Google Cloud environment such as
///   [Google Compute Engine (GCE)][gce-link],
///   [Google Kubernetes Engine (GKE)][gke-link], or [Cloud Run]. Each of these
///   deployment environments provides a default service account to the
///   application, and offers mechanisms to change this default service account
///   without any code changes to your application.
/// - You are testing or developing the application on a workstation (physical or
///   virtual). These credentials will use your preferences as set with
///   [gcloud auth application-default]. These preferences can be your own Gooogle
///   Cloud user credentials, or some service account.
/// - Regardless of where your application is running, you can use the
///   `GOOGLE_APPLICATION_CREDENTIALS` environment variable to override the
///   defaults. This environment variable should point to a file containing a
///   service account key file, or a JSON object describing your user
///   credentials.
///
/// The access tokens returned by these credentials should be used in the
/// Authorization HTTP header.
///
/// The Google Cloud client libraries for Rust will typically find and use these
/// credentials automatically if a credentials file exists in the
/// standard ADC search paths. You might instantiate these credentials either
/// via ADC or a specific JSON file, if you need to:
/// * Override the OAuth 2.0 **scopes** being requested for the access token.
/// * Override the **quota project ID** for billing and quota management.
///
/// Example usage:
///
/// Fetching token using ADC
/// ```
/// # use google_cloud_auth::credentials::Builder;
/// # use google_cloud_auth::errors::CredentialsError;
/// # tokio_test::block_on(async {
/// let creds = Builder::default()
///     .with_quota_project_id("my-project")
///     .build()?;
/// let token = creds.token().await?;
/// println!("Token: {}", token.token);
/// # Ok::<(), CredentialsError>(())
/// # });
/// ```
///
/// Fetching token using custom JSON
/// ```
/// # use google_cloud_auth::credentials::Builder;
/// # use google_cloud_auth::errors::CredentialsError;
/// # tokio_test::block_on(async {
/// # use google_cloud_auth::credentials::Builder;
/// let authorized_user = serde_json::json!({
///     "client_id": "YOUR_CLIENT_ID.apps.googleusercontent.com", // Replace with your actual Client ID
///     "client_secret": "YOUR_CLIENT_SECRET", // Replace with your actual Client Secret - LOAD SECURELY!
///     "refresh_token": "YOUR_REFRESH_TOKEN", // Replace with the user's refresh token - LOAD SECURELY!
///     "type": "authorized_user",
///     // "quota_project_id": "your-billing-project-id", // Optional: Set if needed
///     // "token_uri" : "test-token-uri", // Optional: Set if needed
/// });
///
/// let creds = Builder::new(authorized_user)
///     .with_quota_project_id("my-project")
///     .build()?;
/// let token = creds.token().await?;
/// println!("Token: {}", token.token);
/// # Ok::<(), CredentialsError>(())
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
    credentials_source: CredentialsSource,
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
            credentials_source: CredentialsSource::DefaultCredentials,
            quota_project_id: None,
            scopes: None,
        }
    }
}

impl Builder {
    /// Creates a new builder with given credentials json.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_auth::credentials::Builder;
    /// let authorized_user = serde_json::json!({ /* add details here */ });
    /// let credentials = Builder::new(authorized_user).build();
    ///```
    ///
    pub fn new(json: serde_json::Value) -> Self {
        Self {
            credentials_source: CredentialsSource::CredentialsJson(json),
            quota_project_id: None,
            scopes: None,
        }
    }

    /// Sets the [quota project] for these credentials.
    ///
    /// In some services, you can use an account in one project for authentication
    /// and authorization, and charge the usage to a different project. This requires
    /// that the user has `serviceusage.services.use` permissions on the quota project.
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
    /// or if the `json` provided to [Builder::new] cannot be successfully deserialized
    /// into the expected format. This typically happens if the JSON value is malformed
    /// or missing required fields. For more information, on how to generate
    /// json, consult the relevant section in the [application-default credentials] guide.
    ///
    /// [application-default credentials]: https://cloud.google.com/docs/authentication/application-default-credentials
    pub fn build(self) -> Result<Credentials> {
        let json_data = match self.credentials_source {
            CredentialsSource::CredentialsJson(json) => Some(json),
            CredentialsSource::DefaultCredentials => match load_adc()? {
                AdcContents::Contents(contents) => {
                    Some(serde_json::from_str(&contents).map_err(errors::non_retryable)?)
                }
                AdcContents::FallbackToMds => None,
            },
        };
        build_credentials(json_data, self.quota_project_id, self.scopes)
    }
}

/// Create access token credentials.
///
/// Returns [Application Default Credentials (ADC)][ADC-link]. These are the
/// most commonly used credentials, and are expected to meet the needs of most
/// applications. They conform to [AIP-4110].
///
/// The access tokens returned by these credentials are to be used in the
/// `Authorization` HTTP header.
///
/// Consider using these credentials when:
///
/// - Your application is deployed to a Google Cloud environment such as
///   [Google Compute Engine (GCE)][gce-link],
///   [Google Kubernetes Engine (GKE)][gke-link], or [Cloud Run]. Each of these
///   deployment environments provides a default service account to the
///   application, and offers mechanisms to change this default service account
///   without any code changes to your application.
/// - You are testing or developing the application on a workstation (physical or
///   virtual). These credentials will use your preferences as set with
///   [gcloud auth application-default]. These preferences can be your own Google
///   Cloud user credentials, or some service account.
/// - Regardless of where your application is running, you can use the
///   `GOOGLE_APPLICATION_CREDENTIALS` environment variable to override the
///   defaults. This environment variable should point to a file containing a
///   service account key file, or a JSON object describing your user
///   credentials.
///
/// Example usage:
///
/// ```
/// # use google_cloud_auth::credentials::create_access_token_credentials;
/// # use google_cloud_auth::errors::CredentialsError;
/// # tokio_test::block_on(async {
/// let mut creds = create_access_token_credentials().await?;
/// let token = creds.token().await?;
/// println!("Token: {}", token.token);
/// # Ok::<(), CredentialsError>(())
/// # });
/// ```
///
/// [ADC-link]: https://cloud.google.com/docs/authentication/application-default-credentials
/// [AIP-4110]: https://google.aip.dev/auth/4110
/// [Cloud Run]: https://cloud.google.com/run
/// [gce-link]: https://cloud.google.com/products/compute
/// [gcloud auth application-default]: https://cloud.google.com/sdk/gcloud/reference/auth/application-default
/// [gke-link]: https://cloud.google.com/kubernetes-engine
pub async fn create_access_token_credentials() -> Result<Credentials> {
    Builder::default().build()
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

fn extract_credential_type(json: &Value) -> Result<&str> {
    json.get("type")
        .ok_or_else(|| {
            errors::non_retryable_from_str(
                "Failed to parse Credentials JSON. No `type` field found.",
            )
        })?
        .as_str()
        .ok_or_else(|| {
            errors::non_retryable_from_str(
                "Failed to parse Credentials JSON. `type` field is not a string.",
            )
        })
}

/// Applies common optional configurations (quota project ID, scopes) to a
/// specific credential builder instance and then builds it.
///
/// This macro centralizes the logic for optionally calling `.with_quota_project_id()`
/// and `.with_scopes()` on different underlying credential builders (like
/// `mds::Builder`, `service_account::Builder`, etc.) before calling `.build()`.
/// It helps avoid repetitive code in the `build_credentials` function.
macro_rules! config_builder {
    ($builder_instance:expr, $quota_project_id:expr, $scopes:expr) => {{
        let builder = $builder_instance;
        let builder = $quota_project_id
            .into_iter()
            .fold(builder, |b, qp| b.with_quota_project_id(qp));

        let builder = $scopes.into_iter().fold(builder, |b, s| b.with_scopes(s));

        builder.build()
    }};
}

fn build_credentials(
    json: Option<Value>,
    quota_project_id: Option<String>,
    scopes: Option<Vec<String>>,
) -> Result<Credentials> {
    match json {
        None => config_builder!(mds::Builder::default(), quota_project_id, scopes),
        Some(json) => {
            let cred_type = extract_credential_type(&json)?;
            match cred_type {
                "authorized_user" => {
                    config_builder!(user_account::Builder::new(json), quota_project_id, scopes)
                }
                "service_account" => config_builder!(
                    service_account::Builder::new(json),
                    quota_project_id,
                    scopes
                ),
                _ => Err(errors::non_retryable_from_str(format!(
                    "Invalid or unsupported credentials type found in JSON: {cred_type}"
                ))),
            }
        }
    }
}

fn path_not_found(path: String) -> CredentialsError {
    errors::non_retryable_from_str(format!(
        "Failed to load Application Default Credentials (ADC) from {path}. Check that the `GOOGLE_APPLICATION_CREDENTIALS` environment variable points to a valid file."
    ))
}

fn load_adc() -> Result<AdcContents> {
    match adc_path() {
        None => Ok(AdcContents::FallbackToMds),
        Some(AdcPath::FromEnv(path)) => match std::fs::read_to_string(&path) {
            Ok(contents) => Ok(AdcContents::Contents(contents)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Err(path_not_found(path)),
            Err(e) => Err(errors::non_retryable(e)),
        },
        Some(AdcPath::WellKnown(path)) => match std::fs::read_to_string(path) {
            Ok(contents) => Ok(AdcContents::Contents(contents)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(AdcContents::FallbackToMds),
            Err(e) => Err(errors::non_retryable(e)),
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
pub mod testing {
    use crate::Result;
    use crate::credentials::Credentials;
    use crate::credentials::dynamic::CredentialsTrait;
    use crate::token::Token;
    use http::header::{HeaderName, HeaderValue};
    use std::sync::Arc;

    /// A simple credentials implementation to use in tests where authentication does not matter.
    ///
    /// Always returns a "Bearer" token, with "test-only-token" as the value.
    pub fn test_credentials() -> Credentials {
        Credentials {
            inner: Arc::from(TestCredentials {}),
        }
    }

    #[derive(Debug)]
    struct TestCredentials;

    #[async_trait::async_trait]
    impl CredentialsTrait for TestCredentials {
        async fn token(&self) -> Result<Token> {
            Ok(Token {
                token: "test-only-token".to_string(),
                token_type: "Bearer".to_string(),
                expires_at: None,
                metadata: None,
            })
        }

        async fn headers(&self) -> Result<Vec<(HeaderName, HeaderValue)>> {
            Ok(Vec::new())
        }

        async fn universe_domain(&self) -> Option<String> {
            None
        }
    }

    /// A simple credentials implementation to use in tests.
    ///
    /// Always return an error in `token()` and `headers()`.
    pub fn error_credentials(retryable: bool) -> Credentials {
        Credentials {
            inner: Arc::from(ErrorCredentials(retryable)),
        }
    }

    #[derive(Debug, Default)]
    struct ErrorCredentials(bool);

    #[async_trait::async_trait]
    impl CredentialsTrait for ErrorCredentials {
        async fn token(&self) -> Result<Token> {
            Err(super::CredentialsError::from_str(self.0, "test-only"))
        }

        async fn headers(&self) -> Result<Vec<(HeaderName, HeaderValue)>> {
            Err(super::CredentialsError::from_str(self.0, "test-only"))
        }

        async fn universe_domain(&self) -> Option<String> {
            None
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use base64::Engine;
    use rsa::RsaPrivateKey;
    use rsa::pkcs8::{EncodePrivateKey, LineEnding};
    use scoped_env::ScopedEnv;
    use std::error::Error;
    use test_case::test_case;

    type TestResult = std::result::Result<(), Box<dyn std::error::Error>>;

    // Convenience struct for verifying (HeaderName, HeaderValue) pairs.
    #[derive(Debug, Eq, Ord, PartialEq, PartialOrd)]
    pub struct HV {
        pub header: String,
        pub value: String,
        pub is_sensitive: bool,
    }

    impl HV {
        pub fn from(headers: Vec<(HeaderName, HeaderValue)>) -> Vec<HV> {
            let mut hvs: Vec<HV> = headers
                .into_iter()
                .map(|(h, v)| HV {
                    header: h.to_string(),
                    value: v.to_str().unwrap().to_string(),
                    is_sensitive: v.is_sensitive(),
                })
                .collect();

            // We want to verify the contents of the headers. We do not care
            // what order they are in.
            hvs.sort();
            hvs
        }
    }

    fn generate_pkcs8_private_key() -> String {
        let mut rng = rand::thread_rng();
        let bits = 2048;
        let priv_key = RsaPrivateKey::new(&mut rng, bits).expect("failed to generate a key");
        priv_key
            .to_pkcs8_pem(LineEnding::LF)
            .expect("Failed to encode key to PKCS#8 PEM")
            .to_string()
    }

    fn b64_decode_to_json(s: String) -> serde_json::Value {
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
        let err = load_adc().err().unwrap();
        let msg = err.source().unwrap().to_string();
        assert!(msg.contains("Failed to load Application Default Credentials"));
        assert!(msg.contains("file-does-not-exist.json"));
        assert!(msg.contains("GOOGLE_APPLICATION_CREDENTIALS"));
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
        let err = credentials.token().await.err().unwrap();
        assert_eq!(err.is_retryable(), retryable, "{err:?}");
        let err = credentials.headers().await.err().unwrap();
        assert_eq!(err.is_retryable(), retryable, "{err:?}");
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn create_access_token_credentials_fallback_to_mds_with_quota_project() {
        let _e1 = ScopedEnv::remove("GOOGLE_APPLICATION_CREDENTIALS");
        let _e2 = ScopedEnv::remove("HOME"); // For posix
        let _e3 = ScopedEnv::remove("APPDATA"); // For windows

        let mds = Builder::default()
            .with_quota_project_id("test-quota-project")
            .build()
            .unwrap();
        let fmt = format!("{:?}", mds);
        assert!(fmt.contains("MDSCredentials"));
        assert!(fmt.contains("test-quota-project"));
    }

    #[tokio::test]
    async fn create_access_token_service_account_credentials_with_scopes() -> TestResult {
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

        service_account_key["private_key"] = Value::from(generate_pkcs8_private_key());

        let sac = Builder::new(service_account_key)
            .with_quota_project_id("test-quota-project")
            .with_scopes(scopes)
            .build()
            .unwrap();

        let token = sac.token().await?;
        let parts: Vec<_> = token.token.split('.').collect();
        assert_eq!(parts.len(), 3);
        let claims = b64_decode_to_json(parts.get(1).unwrap().to_string());

        let fmt = format!("{:?}", sac);
        assert!(fmt.contains("ServiceAccountCredentials"));
        assert!(fmt.contains("test-quota-project"));
        assert_eq!(claims["scope"], scopes.join(" "));

        Ok(())
    }
}
