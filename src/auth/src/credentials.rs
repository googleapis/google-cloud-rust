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

mod api_key_credential;
// Export API Key factory function and options
pub use api_key_credential::ApiKeyOptions;
pub use api_key_credential::create_api_key_credential;

pub mod mds;
mod service_account_credential;
pub(crate) mod user_credential;

use crate::Result;
use gax::error::CredentialError;
use http::header::{HeaderName, HeaderValue};
use std::future::Future;
use std::sync::Arc;
use gax::credentials::Credential;
use gax::credentials::dynamic::CredentialTrait;

pub(crate) const QUOTA_PROJECT_KEY: &str = "x-goog-user-project";
pub(crate) const DEFAULT_UNIVERSE_DOMAIN: &str = "googleapis.com";

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
/// # use google_cloud_auth::credentials::create_access_token_credential;
/// # use google_cloud_auth::errors::CredentialError;
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
        AdcContents::FallbackToMds => return Ok(mds::new()),
    };
    let js: serde_json::Value =
        serde_json::from_str(&contents).map_err(CredentialError::non_retryable)?;
    let cred_type = js
        .get("type")
        .ok_or_else(|| CredentialError::non_retryable_from_str("Failed to parse Application Default Credentials (ADC). No `type` field found."))?
        .as_str()
        .ok_or_else(|| CredentialError::non_retryable_from_str("Failed to parse Application Default Credentials (ADC). `type` field is not a string.")
        )?;
    match cred_type {
        "authorized_user" => user_credential::creds_from(js),
        "service_account" => service_account_credential::creds_from(js),
        _ => Err(CredentialError::non_retryable_from_str(format!(
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
    CredentialError::non_retryable_from_str(format!(
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
/// This module is mainly relevant to other `google-cloud-*` crates, but some
/// external developers (i.e. consumers, not developers of `google-cloud-rust`)
/// may find it useful.
pub mod testing {
    use crate::Result;
    use crate::credentials::Credential;
    use gax::credentials::dynamic::CredentialTrait;
    use gax::token::Token;
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
