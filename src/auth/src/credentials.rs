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

/// An implementation of [crate::credentials::traits::Credential].
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
pub struct Credential {
    inner: Box<dyn traits::dynamic::Credential>,
}

impl traits::Credential for Credential {
    fn get_token(&mut self) -> impl Future<Output = Result<crate::token::Token>> + Send {
        self.inner.get_token()
    }

    fn get_headers(
        &mut self,
    ) -> impl Future<Output = Result<Vec<(HeaderName, HeaderValue)>>> + Send {
        self.inner.get_headers()
    }

    fn get_universe_domain(&mut self) -> impl Future<Output = Option<String>> + Send {
        self.inner.get_universe_domain()
    }
}

pub mod traits {
    use super::Future;
    use super::Result;
    use super::{HeaderName, HeaderValue};

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
    /// Application developers who directly use the Auth SDK can use this trait
    /// to mock the credentials. Application developers who use the Google Cloud
    /// Rust SDK directly should not need this functionality.
    ///
    /// [credentials-link]: https://cloud.google.com/docs/authentication#credentials
    /// [token-link]: https://cloud.google.com/docs/authentication#token
    /// [Metadata Service]: https://cloud.google.com/compute/docs/metadata/overview
    /// [Google Compute Engine]: https://cloud.google.com/products/compute
    /// [Google Kubernetes Engine]: https://cloud.google.com/kubernetes-engine
    pub trait Credential {
        /// Asynchronously retrieves a token.
        ///
        /// Returns a [Token][crate::token::Token] for the current credentials.
        /// The underlying implementation refreshes the token as needed.
        fn get_token(&mut self) -> impl Future<Output = Result<crate::token::Token>> + Send;

        /// Asynchronously constructs the auth headers.
        ///
        /// Different auth tokens are sent via different headers. The
        /// [Credential] constructs the headers (and header values) that should be
        /// sent with a request.
        ///
        /// The underlying implementation refreshes the token as needed.
        fn get_headers(
            &mut self,
        ) -> impl Future<Output = Result<Vec<(HeaderName, HeaderValue)>>> + Send;

        /// Retrieves the universe domain associated with the credential, if any.
        fn get_universe_domain(&mut self) -> impl Future<Output = Option<String>> + Send;
    }

    pub(crate) mod dynamic {
        use super::Result;
        use super::{HeaderName, HeaderValue};

        /// A dyn-compatible, crate-private version of `Credential`.
        #[async_trait::async_trait]
        pub trait Credential: Send + Sync {
            /// Asynchronously retrieves a token.
            ///
            /// Returns a [Token][crate::token::Token] for the current credentials.
            /// The underlying implementation refreshes the token as needed.
            async fn get_token(&mut self) -> Result<crate::token::Token>;

            /// Asynchronously constructs the auth headers.
            ///
            /// Different auth tokens are sent via different headers. The
            /// [Credential] constructs the headers (and header values) that should be
            /// sent with a request.
            ///
            /// The underlying implementation refreshes the token as needed.
            async fn get_headers(&mut self) -> Result<Vec<(HeaderName, HeaderValue)>>;

            /// Retrieves the universe domain associated with the credential, if any.
            async fn get_universe_domain(&mut self) -> Option<String>;
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
/// - Your application is deployed to a GCP environment such as GCE, GKE, or
///   Cloud Run. Each of these deployment environments provides a default service
///   account to the application, and offers mechanisms to change the default
///   credentials without any code changes to your application.
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
/// # use gcp_sdk_auth::credentials::traits::Credential;
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
/// [gcloud auth application-default]: https://cloud.google.com/sdk/gcloud/reference/auth/application-default
pub async fn create_access_token_credential() -> Result<Credential> {
    let adc_path = adc_path().ok_or(
        // TODO(#442) - This should (successfully) fall back to MDS Credentials. We will temporarily return an error.
        CredentialError::new(false, Box::from("Unable to find ADC.")),
    )?;

    let contents = std::fs::read_to_string(adc_path).map_err(|e| {
        match e.kind() {
            std::io::ErrorKind::NotFound => {
                // TODO(#442) - This should (successfully) fall back to MDS Credentials. We will temporarily return an error.
                CredentialError::new(false, Box::from("Unable to find ADC."))
            }
            _ => CredentialError::new(false, e.into()),
        }
    })?;
    let js: serde_json::Value =
        serde_json::from_str(&contents).map_err(|e| CredentialError::new(false, e.into()))?;
    let cred_type = js
        .get("type")
        .ok_or(CredentialError::new(
            false,
            Box::from("Failed to parse ADC. No `type` field found."),
        ))?
        .as_str()
        .ok_or(CredentialError::new(
            false,
            Box::from("Failed to parse ADC. `type` field is not a string."),
        ))?;
    match cred_type {
        "authorized_user" => user_credential::creds_from(js),
        _ => Err(CredentialError::new(
            false,
            Box::from(format! {"Unimplemented credential type: {cred_type}"}),
        )),
    }
}

/// The path to Application Default Credentials (ADC), as specified in [AIP-4110].
///
/// [AIP-4110]: https://google.aip.dev/auth/4110
fn adc_path() -> Option<String> {
    std::env::var("GOOGLE_APPLICATION_CREDENTIALS")
        .ok()
        .or_else(adc_well_known_path)
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

#[cfg(test)]
mod test {
    use super::*;
    use scoped_env::ScopedEnv;

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
            Some("C:/Users/foo/gcloud/application_default_credentials.json".to_string())
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
            Some("/home/foo/.config/gcloud/application_default_credentials.json".to_string())
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
            Some("/usr/bar/application_default_credentials.json".to_string())
        );
    }
}
