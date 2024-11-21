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

#![allow(dead_code)]

use super::metadata;
use crate::oauth2::{JwsClaims, JwsHeader};
use crate::{AccessToken, Error, ErrorKind, Result};
use async_trait::async_trait;
use chrono::{Duration, Utc};
use rustls::sign::Signer;
use rustls::sign::SigningKey;
use rustls_pemfile::Item;
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::Arc;
use tokio::sync::Mutex;

const DEFAULT_HEADER: JwsHeader = JwsHeader {
    alg: "RS256",
    typ: "JWT",
    kid: None,
};
const DEFAULT_OAUTH_GRANT: &str = "urn:ietf:params:oauth:grant-type:jwt-bearer";
const DEFAULT_USER_GRANT: &str = "refresh_token";
const GOOGLE_OAUTH2_TOKEN_ENDPOINT: &str = "https://oauth2.googleapis.com/token";

/// An producer of az [AccessToken].
#[async_trait]
pub trait Source: SourceClone {
    async fn token(&self) -> Result<AccessToken>;
}

pub trait SourceClone {
    fn clone_box(&self) -> Box<dyn Source + Send + Sync>;
}

impl<T> SourceClone for T
where
    T: Source + Send + Sync + Clone + 'static,
{
    fn clone_box(&self) -> Box<dyn Source + Send + Sync> {
        Box::new(self.clone())
    }
}

impl Clone for Box<dyn Source + Send + Sync + 'static> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}

/// Configuration for building a [ServiceAccountKeySource].
#[derive(Clone)]
pub struct ServiceAccountKeySourceConfig {
    pub scopes: Vec<String>,
}

/// A [Source] derived from a Service Account Key.
#[derive(Clone)]
pub struct ServiceAccountKeySource {
    file: ServiceAccountKeyFile,
    scopes: Vec<String>,
}

/// A representation of a Service Account File. See [Service Account Keys](https://google.aip.dev/auth/4112)
/// for more details.
#[derive(Clone, Deserialize)]
struct ServiceAccountKeyFile {
    #[serde(rename = "type")]
    cred_type: String,
    client_email: String,
    private_key_id: String,
    private_key: String,
    auth_uri: String,
    token_uri: String,
    project_id: String,
}

impl ServiceAccountKeySource {
    /// Create a [ServiceAccountKeySource] from a file path.
    pub async fn from_file(
        path: impl AsRef<Path>,
        config: ServiceAccountKeySourceConfig,
    ) -> Result<Self> {
        if config.scopes.is_empty() {
            return Err(Error::new("scopes must be provided", ErrorKind::Validation));
        }
        let sa: ServiceAccountKeyFile =
            serde_json::from_slice(&tokio::fs::read(path).await.map_err(Error::wrap_io)?)
                .map_err(Error::wrap_serialization)?;
        Ok(ServiceAccountKeySource {
            file: sa,
            scopes: config.scopes,
        })
    }

    /// Create a [ServiceAccountKeySource] from bytes.
    pub fn from_file_contents(
        contents: &[u8],
        config: ServiceAccountKeySourceConfig,
    ) -> Result<Self> {
        if config.scopes.is_empty() {
            return Err(Error::new("scopes must be provided", ErrorKind::Validation));
        }
        let sa: ServiceAccountKeyFile =
            serde_json::from_slice(contents).map_err(Error::wrap_serialization)?;
        Ok(ServiceAccountKeySource {
            file: sa,
            scopes: config.scopes,
        })
    }

    /// Retrieves an [AccessToken] based on configured source.
    async fn _fetch_access_token(&self) -> Result<AccessToken> {
        let signer = self.signer()?;
        let payload = self.create_payload(signer)?;
        let client = reqwest::Client::new();
        let res = client
            .post(self.file.token_uri.as_str())
            .form(&ServiceAccountTokenRequest {
                grant_type: DEFAULT_OAUTH_GRANT.into(),
                assertion: payload,
            })
            .send()
            .await
            .map_err(|e| {
                Error::new_with_error(
                    "unable to make request to oauth endpoint",
                    e,
                    ErrorKind::Http,
                )
            })?;
        if !res.status().is_success() {
            return Err(Error::new(
                format!("bad request with status: {}", res.status()),
                ErrorKind::Http,
            ));
        }
        let token_response: TokenResponse = res.json().await.map_err(Error::wrap_serialization)?;

        Ok(AccessToken {
            value: token_response.access_token,
            expires: Some(Utc::now() + Duration::seconds(token_response.expires_in)),
        })
    }

    // Creates a signer using the private key stored in the service account file.
    fn signer(&self) -> Result<Box<dyn Signer>> {
        let pk = rustls_pemfile::read_one(&mut self.file.private_key.as_bytes())
            .map_err(|e| Error::wrap(e, ErrorKind::Other))?
            .ok_or_else(|| {
                Error::new("unable to parse service account key", ErrorKind::Validation)
            })?;
        let pk = match pk {
            Item::RSAKey(item) => item,
            Item::PKCS8Key(item) => item,
            other => {
                return Err(Error::new(
                    format!(
                        "expected key to be in form of RSA or PKCS8, found {:?}",
                        other
                    ),
                    ErrorKind::Validation,
                ))
            }
        };
        rustls::sign::RsaSigningKey::new(&rustls::PrivateKey(pk))
            .map_err(|e| Error::new_with_error("unable to create signer", e, ErrorKind::Other))?
            .choose_scheme(&[rustls::SignatureScheme::RSA_PKCS1_SHA256])
            .ok_or_else(|| Error::new("invalid signing scheme", ErrorKind::Validation))
    }

    /// Uses the provide signer to sign JWS Claims then base64 encodes the data
    /// to a string.
    fn create_payload(&self, signer: Box<dyn Signer>) -> Result<String> {
        let scopes = self.scopes.join(" ");
        let mut claims = JwsClaims {
            iss: self.file.client_email.as_str(),
            scope: Some(scopes.as_str()),
            aud: self.file.token_uri.as_str(),
            exp: None,
            iat: None,
            sub: None,
            typ: None,
        };
        let header = DEFAULT_HEADER;

        let ss = format!("{}.{}", header.encode()?, claims.encode()?);
        let sig = signer
            .sign(ss.as_bytes())
            .map_err(|_| Error::new("unable to sign bytes", ErrorKind::Other))?;

        use base64::prelude::{Engine as _, BASE64_URL_SAFE_NO_PAD};
        Ok(format!("{}.{}", ss, &BASE64_URL_SAFE_NO_PAD.encode(sig)))
    }
}

#[async_trait]
impl Source for ServiceAccountKeySource {
    async fn token(&self) -> Result<AccessToken> {
        self._fetch_access_token().await
    }
}

/// The request body of a Service Account Key token exchange.
#[derive(Serialize)]
struct ServiceAccountTokenRequest {
    grant_type: String,
    assertion: String,
}

/// The response of a Service Account Key token exchange.
#[derive(Deserialize)]
struct TokenResponse {
    access_token: String,
    token_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    id_token: Option<String>,
    expires_in: i64,
}

/// Options for building a [UserSource].
pub struct UserSourceConfig {
    pub scopes: Vec<String>,
}

/// A [Source] derived from a gcloud user credential.
#[derive(Clone, Deserialize)]
pub struct UserSource {
    file: UserCredentialFile,
    scopes: Vec<String>,
}

/// A representation of a Service Account File.
#[derive(Clone, Deserialize)]
struct UserCredentialFile {
    #[serde(rename = "type")]
    cred_type: String,
    client_id: String,
    client_secret: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    quota_project_id: Option<String>,
    refresh_token: String,
}

impl UserSource {
    /// Create a [UserSource] from a file path.
    pub async fn from_file(path: impl AsRef<Path>, config: UserSourceConfig) -> Result<Self> {
        if config.scopes.is_empty() {
            return Err(Error::new("scopes must be provided", ErrorKind::Validation));
        }
        let user: UserCredentialFile =
            serde_json::from_slice(&tokio::fs::read(path).await.map_err(Error::wrap_io)?)
                .map_err(Error::wrap_serialization)?;
        Ok(Self {
            file: user,
            scopes: config.scopes,
        })
    }

    /// Create a [UserSource] from bytes.
    pub fn from_file_contents(contents: &[u8], config: UserSourceConfig) -> Result<Self> {
        if config.scopes.is_empty() {
            return Err(Error::new("scopes must be provided", ErrorKind::Validation));
        }
        let user: UserCredentialFile =
            serde_json::from_slice(contents).map_err(Error::wrap_serialization)?;
        Ok(Self {
            file: user,
            scopes: config.scopes,
        })
    }

    /// Retrieves an [AccessToken] based on configured source.
    async fn _fetch_access_token(&self) -> Result<AccessToken> {
        let client = reqwest::Client::new();
        let res = client
            .post(GOOGLE_OAUTH2_TOKEN_ENDPOINT)
            .header("Content-Type", "application/x-www-form-urlencoded")
            .form(&UserTokenRequest {
                grant_type: DEFAULT_USER_GRANT,
                refresh_token: &self.file.refresh_token,
                client_id: &self.file.client_id,
                client_secret: &self.file.client_secret,
            })
            .send()
            .await
            .map_err(|e| {
                Error::new_with_error(
                    "unable to make request to oauth endpoint",
                    e,
                    ErrorKind::Http,
                )
            })?;
        if !res.status().is_success() {
            return Err(Error::new(
                format!("bad request with status: {}", res.status()),
                ErrorKind::Http,
            ));
        }
        let token_response: TokenResponse = res.json().await.map_err(Error::wrap_serialization)?;
        Ok(AccessToken {
            value: token_response.access_token,
            expires: Some(Utc::now() + Duration::seconds(token_response.expires_in)),
        })
    }
}

#[async_trait]
impl Source for UserSource {
    async fn token(&self) -> Result<AccessToken> {
        self._fetch_access_token().await
    }
}

/// The request body for talking to `https://oauth2.googleapis.com/token`.
#[derive(Serialize)]
struct UserTokenRequest<'a> {
    grant_type: &'a str,
    refresh_token: &'a str,
    client_id: &'a str,
    client_secret: &'a str,
}

pub struct ComputeSourceConfig {
    pub scopes: Vec<String>,
}

/// A [Source] derived from the Google Cloud metadata service.
#[derive(Clone)]
pub struct ComputeSource {
    scopes: Vec<String>,
}

impl ComputeSource {
    /// Creates a [ComputeSource] from the provided config.
    pub fn new(config: ComputeSourceConfig) -> Self {
        Self {
            scopes: config.scopes,
        }
    }

    ///Retrieves an [AccessToken] based on configured source.
    async fn _fetch_access_token(&self) -> Result<AccessToken> {
        let token = metadata::fetch_access_token(None, self.scopes.clone()).await?;
        Ok(AccessToken {
            value: token.access_token,
            expires: Some(Utc::now() + Duration::seconds(token.expires_in)),
        })
    }
}

#[async_trait]
impl Source for ComputeSource {
    async fn token(&self) -> Result<AccessToken> {
        self._fetch_access_token().await
    }
}

/// A noop source used for default credentials. It will never produce tokens.
#[derive(Clone)]
pub struct NoOpSource {}

#[async_trait]
impl Source for NoOpSource {
    async fn token(&self) -> Result<AccessToken> {
        Err(Error::new(
            "use Credential.find_default to find a credential from the env",
            ErrorKind::Other,
        ))
    }
}

/// This type is meant to wrap another [Source] and keep returning the same [AccessToken]
// as long as it is valid.
#[derive(Clone)]
pub struct RefresherSource {
    pub current_token: Arc<Mutex<AccessToken>>,
    pub source: Box<dyn Source + Send + Sync>,
}

impl Default for RefresherSource {
    fn default() -> Self {
        Self {
            current_token: Arc::new(Mutex::new(AccessToken {
                value: String::new(),
                expires: None,
            })),
            source: Box::new(NoOpSource {}),
        }
    }
}

#[async_trait]
impl Source for RefresherSource {
    async fn token(&self) -> Result<AccessToken> {
        let mut cur_token = self.current_token.lock().await;
        if cur_token.is_validish() {
            return Ok(cur_token.clone());
        }
        let new_token = self.source.token().await.unwrap();
        cur_token.value = new_token.value;
        cur_token.expires = new_token.expires;
        return Ok(cur_token.clone());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::DateTime;
    use serde_json::json;

    fn test_file_contents() -> Vec<String> {
        let without_quota_project = json!({"type": "authorized_user", "client_id": "test-only-id.apps.googleusercontent.com", "client_secret": "test-only-client-secret", "refresh_token": "test-only-refresh-token"});
        let with_quota_project = json!({"type": "authorized_user", "client_id": "test-only-id.apps.googleusercontent.com", "client_secret": "test-only-client-secret", "refresh_token": "test-only-refresh-token"});
        let items: std::result::Result<Vec<String>, _> =
            vec![without_quota_project, with_quota_project]
                .into_iter()
                .map(|v| serde_json::to_string(&v))
                .collect();
        items.unwrap()
    }

    #[test]
    fn read_user_credentials_file() {
        for input in test_file_contents() {
            let config = UserSourceConfig {
                scopes: vec!["test-only".to_string()],
            };
            let source = UserSource::from_file_contents(input.as_bytes(), config);
            assert!(
                source.is_ok(),
                "got error {:?} when parsing {input}",
                source.err()
            );
        }
    }

    #[tokio::main]
    #[test]
    #[ignore]
    async fn service_account_source_fetch_access_token() {
        todo!("write a good test");
    }

    #[tokio::main]
    #[test]
    #[ignore]
    async fn user_source_fetch_access_token() {
        todo!("write a good test");
    }

    #[tokio::main]
    #[test]
    #[ignore]
    async fn compute_source_fetch_access_token() {
        todo!("write a good test");
    }

    #[derive(Clone)]
    struct FakeSource {
        static_time: DateTime<Utc>,
        counter: Arc<Mutex<i64>>,
    }

    #[async_trait]
    impl Source for FakeSource {
        async fn token(&self) -> Result<AccessToken> {
            let mut count = self.counter.lock().await;
            let cur_count = *count;
            *count += 1;
            Ok(AccessToken {
                value: format!("token-{}", cur_count),
                expires: Some(self.static_time),
            })
        }
    }

    #[tokio::main]
    #[test]
    async fn test_refresher_returns_same_value() {
        let it = RefresherSource {
            current_token: Arc::new(Mutex::new(AccessToken {
                value: String::new(),
                expires: None,
            })),
            source: Box::new(FakeSource {
                static_time: Utc::now() + chrono::Duration::seconds(20),
                counter: Arc::new(Mutex::new(0)),
            }),
        };
        let tok1 = it.token().await.unwrap();
        let tok2 = it.token().await.unwrap();
        assert_eq!(tok1.value, "token-0");
        assert_eq!(tok1.value, tok2.value);
    }

    #[tokio::main]
    #[test]
    async fn test_refresher_returns_new_value() {
        let it = RefresherSource {
            current_token: Arc::new(Mutex::new(AccessToken {
                value: String::new(),
                expires: None,
            })),
            source: Box::new(FakeSource {
                static_time: Utc::now() - chrono::Duration::seconds(20),
                counter: Arc::new(Mutex::new(0)),
            }),
        };
        let tok1 = it.token().await.unwrap();
        let tok2 = it.token().await.unwrap();
        assert_eq!(tok1.value, "token-0");
        assert_ne!(tok1.value, tok2.value);
    }
}
