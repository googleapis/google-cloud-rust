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
use std::path::{Path, PathBuf};

mod metadata;
mod oauth2;
mod source;

const GOOGLE_APPLICATION_CREDENTIALS: &str = "GOOGLE_APPLICATION_CREDENTIALS";
const WINDOWS_APPDATA: &str = "APPDATA";
const UNIX_HOME: &str = "HOME";
const USER_CREDENTIAL_FILE: &str = "application_default_credentials.json";

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("unable to read file")]
    Io(#[from] std::io::Error),
    #[error("unable to deserialize value")]
    Serde(#[from] serde_json::Error),
    #[error("unable to process request")]
    Http(#[from] reqwest::Error),
    #[error("{0}")]
    Other(String),
}

pub type Result<T> = std::result::Result<T, Error>;

/// AccessToken holds a token value that can be used in Authorization headers to
/// authenticate with Google Cloud APIs. If the token
#[derive(Clone)]
pub struct AccessToken {
    /// The actual token.
    pub value: String,
    // TODO(codyoss): Token leaks chrono lib, should we have our own type or is this okay...
    //                We could just use a unix int64 timestamp from zulu?
    /// The time when a token expires, if known.
    pub expires: Option<DateTime<Utc>>,
}

impl AccessToken {
    pub(crate) fn needs_refresh(&self) -> bool {
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
// TODO(codyoss): make non-exhaustive
pub struct CredentialConfig {
    /// The scopes that the minted [AccessToken] should have.
    // TODO(codyoss): This should be optional.
    pub scopes: Vec<String>,
}

/// A [AccessToken] producer that is automatically refreshed and can be shared across
/// threads.
pub struct Credential {
    source: Box<dyn Source + Send + Sync + 'static>,
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
    // TODO(codyoss): maybe make this take a builder?
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
        if let Ok(file_name) = std::env::var(GOOGLE_APPLICATION_CREDENTIALS) {
            let source = Credential::file_source(file_name.into(), config)?;
            return Ok(source);
        }
        // 2: Well-known file.
        if let Ok(path) = Credential::well_known_file() {
            if path.exists() {
                let source = Credential::file_source(path, config)?;
                return Ok(source);
            }
        }
        // 3: Check if in an environment with on Google Cloud
        if metadata::on_gce().await {
            let source = ComputeSource::builder().scopes(config.scopes).build()?;
            let source = Box::new(source);
            return Ok(source);
        }

        Err(Error::Other("unable to detect default credentials".into()))
    }

    /// Creates a source from a file type credential such as a Service Account
    /// Key file or a gcloud user credential.
    fn file_source(
        file_path: PathBuf,
        config: CredentialConfig,
    ) -> Result<Box<dyn Source + Send + Sync + 'static>> {
        let contents = std::fs::read(file_path)?;
        let file: Key = serde_json::from_slice(&contents)?;
        let source: Box<dyn Source + Send + Sync + 'static> = match file.cred_type {
            "authorized_user" => {
                let source = UserSource::from_file_contents(&contents)
                    .scopes(config.scopes)
                    .build()?;
                Box::new(source)
            }
            "service_account" => {
                let source = ServiceAccountKeySource::from_file_contents(&contents)
                    .scopes(config.scopes)
                    .build()?;
                Box::new(source)
            }
            _ => {
                return Err(Error::Other(format!(
                    "unsupported credential type found: {}",
                    file.cred_type
                )));
            }
        };
        Ok(source)
    }

    /// Returns the path to a gcloud user credential.
    fn well_known_file() -> Result<PathBuf> {
        let mut path = PathBuf::new();
        if cfg!(windows) {
            if let Ok(appdata) = std::env::var(WINDOWS_APPDATA) {
                path.push(appdata);
            } else {
                return Err(Error::Other("unable to find APPDATA".into()));
            }
            path.push("gcloud");
        } else if let Ok(home) = std::env::var(UNIX_HOME) {
            path.push(home);
            path.push(".config");
        } else {
            return Err(Error::Other("unable to lookup HOME".into()));
        }

        path.push("gcloud");
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
