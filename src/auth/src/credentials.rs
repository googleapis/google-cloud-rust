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

pub(crate) mod mds_credential;
pub(crate) mod user_credential;

use crate::errors::CredentialError;
use crate::Result;
use http::header::{HeaderName, HeaderValue};
use std::future::Future;
use std::sync::Arc;

pub(crate) const QUOTA_PROJECT_KEY: &str = "x-goog-user-project";

/// An implementation of [crate::credentials::CredentialTrait].
///
/// Represents a [Credential] used to obtain auth [Token][crate::token::Token]s
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
pub struct Credential {
    // We use an `Arc` to hold the inner implementation.
    //
    // Credentials may be shared across threads (`Send + Sync`), so an `Rc`
    // will not do.
    //
    // They also need to derive `Clone`, as the
    // `gax::http_client::ReqwestClient`s which hold them derive `Clone`. So a
    // `Box` will not do.
    inner: Arc<dyn dynamic::CredentialTrait>,
}

impl<T> std::convert::From<T> for Credential
where
    T: crate::credentials::CredentialTrait + Send + Sync + 'static,
{
    fn from(value: T) -> Self {
        Self {
            inner: Arc::new(value),
        }
    }
}

impl Credential {
    pub async fn get_token(&self) -> Result<crate::token::Token> {
        self.inner.get_token().await
    }

    pub async fn get_headers(&self) -> Result<Vec<(HeaderName, HeaderValue)>> {
        self.inner.get_headers().await
    }

    pub async fn get_universe_domain(&self) -> Option<String> {
        self.inner.get_universe_domain().await
    }
}

/// Represents a [Credential] used to obtain auth
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
/// along with [crate::credentials::Credential::from()] to mock the credentials.
/// Application developers who use the Google Cloud Rust SDK directly should not
/// need this functionality.
///
/// [credentials-link]: https://cloud.google.com/docs/authentication#credentials
/// [token-link]: https://cloud.google.com/docs/authentication#token
/// [Metadata Service]: https://cloud.google.com/compute/docs/metadata/overview
/// [Google Compute Engine]: https://cloud.google.com/products/compute
/// [Google Kubernetes Engine]: https://cloud.google.com/kubernetes-engine
pub trait CredentialTrait: std::fmt::Debug {
    /// Asynchronously retrieves a token.
    ///
    /// Returns a [Token][crate::token::Token] for the current credentials.
    /// The underlying implementation refreshes the token as needed.
    fn get_token(&self) -> impl Future<Output = Result<crate::token::Token>> + Send;

    /// Asynchronously constructs the auth headers.
    ///
    /// Different auth tokens are sent via different headers. The
    /// [Credential] constructs the headers (and header values) that should be
    /// sent with a request.
    ///
    /// The underlying implementation refreshes the token as needed.
    fn get_headers(&self) -> impl Future<Output = Result<Vec<(HeaderName, HeaderValue)>>> + Send;

    /// Retrieves the universe domain associated with the credential, if any.
    fn get_universe_domain(&self) -> impl Future<Output = Option<String>> + Send;
}

pub(crate) mod dynamic {
    use super::Result;
    use super::{HeaderName, HeaderValue};

    /// A dyn-compatible, crate-private version of `CredentialTrait`.
    #[async_trait::async_trait]
    pub trait CredentialTrait: Send + Sync + std::fmt::Debug {
        /// Asynchronously retrieves a token.
        ///
        /// Returns a [Token][crate::token::Token] for the current credentials.
        /// The underlying implementation refreshes the token as needed.
        async fn get_token(&self) -> Result<crate::token::Token>;

        /// Asynchronously constructs the auth headers.
        ///
        /// Different auth tokens are sent via different headers. The
        /// [Credential] constructs the headers (and header values) that should be
        /// sent with a request.
        ///
        /// The underlying implementation refreshes the token as needed.
        async fn get_headers(&self) -> Result<Vec<(HeaderName, HeaderValue)>>;

        /// Retrieves the universe domain associated with the credential, if any.
        async fn get_universe_domain(&self) -> Option<String> {
            Some("googleapis.com".to_string())
        }
    }

    /// The public CredentialTrait implements the dyn-compatible CredentialTrait.
    #[async_trait::async_trait]
    impl<T> CredentialTrait for T
    where
        T: super::CredentialTrait + Send + Sync,
    {
        async fn get_token(&self) -> Result<crate::token::Token> {
            T::get_token(self).await
        }
        async fn get_headers(&self) -> Result<Vec<(HeaderName, HeaderValue)>> {
            T::get_headers(self).await
        }
        async fn get_universe_domain(&self) -> Option<String> {
            T::get_universe_domain(self).await
        }
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
///   [gcloud auth application-default]. These preferences can be your own GCP
///   user credentials, or some service account.
/// - Regardless of where your application is running, you can use the
///   `GOOGLE_APPLICATION_CREDENTIALS` environment variable to override the
///   defaults. This environment variable should point to a file containing a
///   service account key file, or a JSON object describing your user
///   credentials.
///
/// Example usage:
///
/// ```
/// # use gcp_sdk_auth::credentials::create_access_token_credential;
/// # use gcp_sdk_auth::errors::CredentialError;
/// # tokio_test::block_on(async {
/// let mut creds = create_access_token_credential().await?;
/// let token = creds.get_token().await?;
/// println!("Token: {}", token.token);
/// # Ok::<(), CredentialError>(())
/// # });
/// ```
///
/// [ADC-link]: https://cloud.google.com/docs/authentication/application-default-credentials
/// [AIP-4110]: https://google.aip.dev/auth/4110
/// [Cloud Run]: https://cloud.google.com/run
/// [gce-link]: https://cloud.google.com/products/compute
/// [gcloud auth application-default]: https://cloud.google.com/sdk/gcloud/reference/auth/application-default
/// [gke-link]: https://cloud.google.com/kubernetes-engine
pub async fn create_access_token_credential() -> Result<Credential> {
    let contents = match load_adc()? {
        AdcContents::Contents(contents) => contents,
        AdcContents::FallbackToMds => return Ok(mds_credential::new()),
    };
    let js: serde_json::Value =
        serde_json::from_str(&contents).map_err(CredentialError::non_retryable)?;
    let cred_type = js
        .get("type")
        .ok_or_else(|| CredentialError::non_retryable("Failed to parse Application Default Credentials (ADC). No `type` field found."))?
        .as_str()
        .ok_or_else(|| CredentialError::non_retryable("Failed to parse Application Default Credentials (ADC). `type` field is not a string.")
        )?;
    match cred_type {
        "authorized_user" => user_credential::creds_from(js),
        _ => Err(CredentialError::non_retryable(format!(
            "Unimplemented credential type: {cred_type}"
        ))),
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

fn path_not_found(path: String) -> CredentialError {
    CredentialError::non_retryable(
        format!(
            "Failed to load Application Default Credentials (ADC) from {path}. Check that the `GOOGLE_APPLICATION_CREDENTIALS` environment variable points to a valid file."
        ))
}

fn load_adc() -> Result<AdcContents> {
    match adc_path() {
        None => Ok(AdcContents::FallbackToMds),
        Some(AdcPath::FromEnv(path)) => match std::fs::read_to_string(&path) {
            Ok(contents) => Ok(AdcContents::Contents(contents)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Err(path_not_found(path)),
            Err(e) => Err(CredentialError::non_retryable(e)),
        },
        Some(AdcPath::WellKnown(path)) => match std::fs::read_to_string(path) {
            Ok(contents) => Ok(AdcContents::Contents(contents)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(AdcContents::FallbackToMds),
            Err(e) => Err(CredentialError::non_retryable(e)),
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
/// This module is mainly relevant to other `gcp-sdk-*` crates, but some
/// external developers (i.e. consumers, not developers of `google-cloud-rust`)
/// may find it useful.
pub mod testing {
    use crate::credentials::dynamic::CredentialTrait;
    use crate::credentials::Credential;
    use crate::token::Token;
    use crate::Result;
    use http::header::{HeaderName, HeaderValue};
    use std::sync::Arc;

    /// A simple credentials implementation to use in tests where authentication does not matter.
    ///
    /// Always returns a "Bearer" token, with "test-only-token" as the value.
    pub fn test_credentials() -> Credential {
        Credential {
            inner: Arc::from(TestCredential {}),
        }
    }

    #[derive(Debug)]
    struct TestCredential;

    #[async_trait::async_trait]
    impl CredentialTrait for TestCredential {
        async fn get_token(&self) -> Result<Token> {
            Ok(Token {
                token: "test-only-token".to_string(),
                token_type: "Bearer".to_string(),
                expires_at: None,
                metadata: None,
            })
        }

        async fn get_headers(&self) -> Result<Vec<(HeaderName, HeaderValue)>> {
            Ok(Vec::new())
        }

        async fn get_universe_domain(&self) -> Option<String> {
            None
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use scoped_env::ScopedEnv;
    use std::error::Error;

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
}
