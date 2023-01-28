// Copyright 2021 Google LLC
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

//! The `google-cloud-auth` crate provides a convenient way of fetch credentials
//! from various environments. This process is of finding credentials from the
//! environment is called [Application Default Credentials](https://google.aip.dev/auth/4110).

use chrono::Utc;
use chrono::{DateTime, Duration};
use serde::Deserialize;
use source::*;
use std::error::Error as StdError;
use std::path::{Path, PathBuf};

mod metadata;
mod oauth2;
mod source;

const GOOGLE_APPLICATION_CREDENTIALS_ENV: &str = "GOOGLE_APPLICATION_CREDENTIALS";
const WINDOWS_APPDATA_ENV: &str = "APPDATA";
const UNIX_HOME_ENV: &str = "HOME";
const USER_CREDENTIAL_FILE: &str = "application_default_credentials.json";
const GCLOUD_PATH_PART: &str = "gcloud";
const CONFIG_PATH_PART: &str = ".config";

#[derive(Debug)]
pub struct Error {
    inner_error: Option<Box<dyn StdError + Send + Sync>>,
    message: Option<String>,
    kind: ErrorKind,
}

impl Error {
    /// Returns a reference to the inner error wrapped if, if there is one.
    pub fn get_ref(&self) -> Option<&(dyn StdError + Send + Sync + 'static)> {
        match &self.inner_error {
            Some(err) => Some(err.as_ref()),
            None => None,
        }
    }

    pub fn kind(&self) -> ErrorKind {
        self.kind
    }

    fn new(msg: impl Into<String>, kind: ErrorKind) -> Self {
        Self {
            inner_error: None,
            message: Some(msg.into()),
            kind,
        }
    }

    fn new_with_error<E>(msg: impl Into<String>, error: E, kind: ErrorKind) -> Self
    where
        E: StdError + Send + Sync + 'static,
    {
        Self {
            inner_error: Some(Box::new(error)),
            message: Some(msg.into()),
            kind,
        }
    }

    fn wrap<E>(error: E, kind: ErrorKind) -> Self
    where
        E: StdError + Send + Sync + 'static,
    {
        Self {
            inner_error: Some(Box::new(error)),
            message: None,
            kind,
        }
    }

    fn wrap_io<E>(error: E) -> Self
    where
        E: StdError + Send + Sync + 'static,
    {
        Self::wrap(error, ErrorKind::IO)
    }

    fn wrap_serialization<E>(error: E) -> Self
    where
        E: StdError + Send + Sync + 'static,
    {
        Self::wrap(error, ErrorKind::Serialization)
    }

    fn wrap_http<E>(error: E) -> Self
    where
        E: StdError + Send + Sync + 'static,
    {
        Self::wrap(error, ErrorKind::Http)
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(inner_error) = &self.inner_error {
            inner_error.fmt(f)
        } else if let Some(msg) = &self.message {
            write!(f, "{}", msg)
        } else {
            write!(f, "unknown error")
        }
    }
}

impl StdError for Error {}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub enum ErrorKind {
    /// An error related to accessing environment details such as environment
    /// variables.
    Environment,
    /// An error related to make network requests or a failed request.
    Http,
    /// An error related to IO.
    IO,
    /// An error related to serde serialization/deserialization.
    Serialization,
    /// An error related to improper state.
    Validation,
    /// A custom error that does not currently fall into other categories.
    Other,
}

pub type Result<T> = std::result::Result<T, Error>;

/// AccessToken holds a token value that can be used in Authorization headers to
/// authenticate with Google Cloud APIs.
#[derive(Clone)]
#[non_exhaustive]
pub struct AccessToken {
    /// The actual token.
    pub value: String,
    expires: Option<DateTime<Utc>>,
}

impl AccessToken {
    /// Returns true if the token should be considered valid, compensating for
    /// clock skew with by ten seconds.
    pub(crate) fn is_validish(&self) -> bool {
        if let Some(expires) = self.expires {
            let now = Utc::now();
            // Avoid clock skew with 10 second diff of now.
            let expiresish = expires - Duration::seconds(10);
            expiresish > now
        } else {
            false
        }
    }
}

/// Configuration for various authentication flows.
pub struct CredentialConfig {
    /// The scopes that the minted [AccessToken] should have.
    scopes: Vec<String>,
}

impl CredentialConfig {
    pub fn builder() -> CredentialConfigBuilder {
        CredentialConfigBuilder::new()
    }
}

/// A builder for instantiating a [CredentialConfig].
pub struct CredentialConfigBuilder {
    scopes: Vec<String>,
}

impl CredentialConfigBuilder {
    /// Instantiates a new builder.
    pub fn new() -> Self {
        Self { scopes: Vec::new() }
    }

    /// Sets scopes used for credential authorization.
    pub fn scopes(mut self, value: Vec<String>) -> Self {
        self.scopes = value;
        self
    }

    /// Builds a [CredentialConfig].
    pub fn build(self) -> Result<CredentialConfig> {
        Ok(CredentialConfig {
            scopes: self.scopes,
        })
    }
}

impl Default for CredentialConfigBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// A [AccessToken] producer that is automatically refreshed and can be shared across
/// threads.
#[derive(Clone)]
pub struct Credential {
    source: Box<dyn Source + Send + Sync>,
}

// TODO(codyoss): This is currently needed to make generated code easier to generate. Not
//       great that it is an empty cred. Either this should do a partial ADC lookup
//       or we should find a different way and remove this. If we did not need
//       an api call for compute credentials this could just work. But today we
//       need to make async network requests.
impl Default for Credential {
    fn default() -> Self {
        Self {
            source: Box::new(NoOpSource {}),
        }
    }
}

impl Credential {
    /// Fetches a [AccessToken] based on environment and/or configuration settings.
    pub async fn access_token(&self) -> Result<AccessToken> {
        self.source.token().await
    }

    /// Creates a Credential that uses [Application Default Credentials](https://google.aip.dev/auth/4110)
    /// to figure out how a to produce a [AccessToken].
    pub async fn find_default(config: CredentialConfig) -> Result<Credential> {
        let base_source = Credential::base_source(config).await?;
        let refreshed_source = RefresherSource {
            source: base_source,
            ..Default::default()
        };
        Ok(Credential {
            source: Box::new(refreshed_source),
        })
    }

    /// Finds a Source from which to create tokens.
    async fn base_source(
        config: CredentialConfig,
    ) -> Result<Box<dyn Source + Send + Sync + 'static>> {
        // 1: Known environment variable.
        if let Ok(file_name) = std::env::var(GOOGLE_APPLICATION_CREDENTIALS_ENV) {
            let source = Credential::file_source(file_name, config).await?;
            return Ok(source);
        }
        // 2: Well-known file.
        if let Ok(path) = Credential::well_known_file() {
            if path.exists() {
                let source = Credential::file_source(path, config).await?;
                return Ok(source);
            }
        }
        // 3: Check if in an environment with on Google Cloud
        if metadata::is_running_on_gce().await {
            let source = ComputeSource::new(ComputeSourceConfig {
                scopes: config.scopes,
            });
            let source = Box::new(source);
            return Ok(source);
        }

        Err(Error::new(
            "unable to detect default credentials",
            ErrorKind::Validation,
        ))
    }

    /// Creates a source from a file type credential such as a Service Account
    /// Key file or a gcloud user credential.
    async fn file_source(
        file_path: impl AsRef<Path>,
        config: CredentialConfig,
    ) -> Result<Box<dyn Source + Send + Sync + 'static>> {
        let contents = tokio::fs::read(file_path).await.map_err(Error::wrap_io)?;
        let file: Key = serde_json::from_slice(&contents).map_err(Error::wrap_serialization)?;
        let source: Box<dyn Source + Send + Sync + 'static> = match file.cred_type {
            "authorized_user" => {
                let source = UserSource::from_file_contents(
                    &contents,
                    UserSourceConfig {
                        scopes: config.scopes,
                    },
                )?;
                Box::new(source)
            }
            "service_account" => {
                let source = ServiceAccountKeySource::from_file_contents(
                    &contents,
                    ServiceAccountKeySourceConfig {
                        scopes: config.scopes,
                    },
                )?;
                Box::new(source)
            }
            _ => {
                return Err(Error::new(
                    format!("unsupported credential type found: {}", file.cred_type),
                    ErrorKind::Validation,
                ));
            }
        };
        Ok(source)
    }

    /// Returns the path to a gcloud user credential.
    fn well_known_file() -> Result<PathBuf> {
        let mut path = PathBuf::new();
        if cfg!(windows) {
            let appdata = std::env::var(WINDOWS_APPDATA_ENV).map_err(|e| {
                Error::new_with_error("unable to find APPDATA", e, ErrorKind::Environment)
            })?;
            path.push(appdata);
            path.push(GCLOUD_PATH_PART);
        } else {
            let home = std::env::var(UNIX_HOME_ENV).map_err(|e| {
                Error::new_with_error("unable to lookup HOME", e, ErrorKind::Environment)
            })?;
            path.push(home);
            path.push(CONFIG_PATH_PART);
        }

        path.push(GCLOUD_PATH_PART);
        path.push(USER_CREDENTIAL_FILE);
        Ok(path)
    }
}

/// A minimal representation of a file credential to determine its type.
#[derive(Deserialize)]
struct Key<'a> {
    #[serde(rename = "type")]
    cred_type: &'a str,
}

#[cfg(test)]
mod tests {
    use crate::Credential;

    #[tokio::main]
    #[test]
    async fn test_refresher() {
        let cred = Credential::find_default(crate::CredentialConfig {
            scopes: vec!["https://www.googleapis.com/auth/cloud-platform".into()],
        })
        .await
        .unwrap();
        let tok1 = cred.access_token().await.unwrap();
        let tok2 = cred.access_token().await.unwrap();
        assert_eq!(tok1.value, tok2.value)
    }
}
