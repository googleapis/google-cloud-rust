#![allow(dead_code)]

use super::metadata;
use crate::oauth2::{JwsClaims, JwsHeader};
use crate::{Error, Result, Token};
use async_trait::async_trait;
use chrono::{Duration, Utc};
use rustls::sign::Signer;
use rustls::sign::SigningKey;
use rustls_pemfile::Item;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::Mutex;

const DEFAULT_HEADER: JwsHeader = JwsHeader {
    alg: "RS256",
    typ: "JWT",
    kid: None,
};
const DEFAULT_OAUTH_GRANT: &str = "urn:ietf:params:oauth:grant-type:jwt-bearer";
const DEFAULT_USER_GRANT: &str = "refresh_token";

/// An producer of a [Token].
#[async_trait]
pub trait Source {
    async fn token(&self) -> Result<Token>;
}

/// Options for building a [ServiceAccountKeySource].
pub struct ServiceAccountKeySourceBuilder<'a> {
    file_path: Option<PathBuf>,
    contents: Option<&'a [u8]>,

    scopes: Vec<String>,
}

impl ServiceAccountKeySourceBuilder<'_> {
    pub fn scopes(mut self, scopes: Vec<String>) -> Self {
        self.scopes.extend(scopes);
        self
    }
    pub fn build(self) -> Result<ServiceAccountKeySource> {
        let sa: ServiceAccountKeyFile = if let Some(contents) = self.contents {
            serde_json::from_slice(contents)?
        } else if let Some(file_path) = self.file_path {
            serde_json::from_slice(&std::fs::read(file_path)?)?
        } else {
            return Err(Error::Other("".into()));
        };
        if self.scopes.is_empty() {
            return Err(Error::Other("".into()));
        }
        Ok(ServiceAccountKeySource {
            file: sa,
            scopes: self.scopes,
        })
    }
}

/// A [Source] derived from a Service Account Key.
pub struct ServiceAccountKeySource {
    file: ServiceAccountKeyFile,
    scopes: Vec<String>,
}

/// A representation of a Service Account File.
#[derive(Deserialize)]
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
    pub fn from_file(path: impl AsRef<Path>) -> ServiceAccountKeySourceBuilder<'static> {
        ServiceAccountKeySourceBuilder {
            file_path: Some(path.as_ref().to_owned()),
            contents: None,
            scopes: Vec::new(),
        }
    }
    pub fn from_file_contents(contents: &[u8]) -> ServiceAccountKeySourceBuilder {
        ServiceAccountKeySourceBuilder {
            file_path: None,
            contents: Some(contents),
            scopes: Vec::new(),
        }
    }

    async fn _access_token(&self) -> Result<Token> {
        let signer = self.signer()?;
        let payload = self.payload(signer)?;
        let client = reqwest::Client::new();
        let res = client
            .post(self.file.token_uri.as_str())
            .form(&ServiceAccountTokenRequest {
                grant_type: DEFAULT_OAUTH_GRANT.into(),
                assertion: payload,
            })
            .send()
            .await
            .map_err(|_| Error::Other("unable to make request to oauth endpoint".into()))?;
        if !res.status().is_success() {
            return Err(Error::Other(format!(
                "bad request with status: {}",
                res.status()
            )));
        }
        let token_response: TokenResponse = res
            .json()
            .await
            .map_err(|_| Error::Other("unable to decode response".into()))?;

        Ok(Token {
            value: token_response.access_token,
            expires: Some(Utc::now() + Duration::seconds(token_response.expires_in)),
        })
    }

    fn signer(&self) -> Result<Box<dyn Signer>> {
        let pk = rustls_pemfile::read_one(&mut self.file.private_key.as_bytes())?
            .ok_or_else(|| Error::Other("".into()))?;
        let pk = match pk {
            Item::RSAKey(item) => item,
            Item::PKCS8Key(item) => item,
            _ => return Err(Error::Other("expected key in a different format".into())),
        };
        rustls::sign::RsaSigningKey::new(&rustls::PrivateKey(pk))
            .map_err(|_| Error::Other("unable to create signer".into()))?
            .choose_scheme(&[rustls::SignatureScheme::RSA_PKCS1_SHA256])
            .ok_or_else(|| Error::Other("invalid signing scheme".into()))
    }

    fn payload(&self, signer: Box<dyn Signer>) -> Result<String> {
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
            .map_err(|_| Error::Other("unable to sign bytes".into()))?;
        Ok(format!(
            "{}.{}",
            ss,
            base64::encode_config(sig, base64::URL_SAFE_NO_PAD)
        ))
    }
}

#[async_trait]
impl Source for ServiceAccountKeySource {
    async fn token(&self) -> Result<Token> {
        self._access_token().await
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
pub struct UserSourceBuilder<'a> {
    file_path: Option<PathBuf>,
    contents: Option<&'a [u8]>,

    scopes: Vec<String>,
}

impl UserSourceBuilder<'_> {
    pub fn scopes(mut self, scopes: Vec<String>) -> Self {
        self.scopes.extend(scopes);
        self
    }
    pub fn build(self) -> Result<UserSource> {
        let user: UserCredentialFile = if let Some(contents) = self.contents {
            serde_json::from_slice(contents)?
        } else if let Some(file_path) = self.file_path {
            serde_json::from_slice(&std::fs::read(file_path)?)?
        } else {
            return Err(Error::Other("".into()));
        };
        if self.scopes.is_empty() {
            return Err(Error::Other("".into()));
        }
        Ok(UserSource {
            file: user,
            scopes: self.scopes,
        })
    }
}

/// A [Source] derived from a gcloud user credential.
#[derive(Deserialize)]
pub struct UserSource {
    file: UserCredentialFile,
    scopes: Vec<String>,
}

/// A representation of a Service Account File.
#[derive(Deserialize)]
struct UserCredentialFile {
    #[serde(rename = "type")]
    cred_type: String,
    client_id: String,
    client_secret: String,
    quota_project_id: String,
    refresh_token: String,
}

impl UserSource {
    pub fn from_file(path: impl AsRef<Path>) -> UserSourceBuilder<'static> {
        UserSourceBuilder {
            file_path: Some(path.as_ref().to_owned()),
            contents: None,
            scopes: Vec::new(),
        }
    }
    pub fn from_file_contents(contents: &[u8]) -> UserSourceBuilder {
        UserSourceBuilder {
            file_path: None,
            contents: Some(contents),
            scopes: Vec::new(),
        }
    }
    async fn _access_token(&self) -> Result<Token> {
        let client = reqwest::Client::new();
        let res = client
            .post("https://oauth2.googleapis.com/token")
            .header("Content-Type", "application/x-www-form-urlencoded")
            .form(&UserTokenRequest {
                grant_type: DEFAULT_USER_GRANT,
                refresh_token: &self.file.refresh_token,
                client_id: &self.file.client_id,
                client_secret: &self.file.client_secret,
            })
            .send()
            .await
            .map_err(|_| Error::Other("unable to make request to oauth endpoint".into()))?;
        if !res.status().is_success() {
            return Err(Error::Other(format!(
                "bad request with status: {}",
                res.status()
            )));
        }
        let token_response: TokenResponse = res
            .json()
            .await
            .map_err(|_| Error::Other("unable to decode response".into()))?;
        Ok(Token {
            value: token_response.access_token,
            expires: Some(Utc::now() + Duration::seconds(token_response.expires_in)),
        })
    }
}

#[async_trait]
impl Source for UserSource {
    async fn token(&self) -> Result<Token> {
        self._access_token().await
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

pub struct ComputeSourceBuilder {
    scopes: Vec<String>,
}

impl ComputeSourceBuilder {
    pub fn scopes(mut self, scopes: Vec<String>) -> Self {
        self.scopes.extend(scopes);
        self
    }

    pub fn build(self) -> Result<ComputeSource> {
        Ok(ComputeSource {
            scopes: self.scopes,
        })
    }
}

/// A [Source] derived from the Google Cloud metadata service.
pub struct ComputeSource {
    scopes: Vec<String>,
}

impl ComputeSource {
    pub fn builder() -> ComputeSourceBuilder {
        ComputeSourceBuilder { scopes: Vec::new() }
    }

    async fn _access_token(&self) -> Result<Token> {
        let token = metadata::access_token(None, self.scopes.clone())
            .await
            .map_err(|e| Error::Other(e.to_string()))?;
        Ok(Token {
            value: token.access_token,
            expires: Some(Utc::now() + Duration::seconds(token.expires_in)),
        })
    }
}

#[async_trait]
impl Source for ComputeSource {
    async fn token(&self) -> Result<Token> {
        self._access_token().await
    }
}

/// A noop source used for default credentials. It will never produce tokens.
pub(crate) struct NoOpSource {}

#[async_trait]
impl Source for NoOpSource {
    async fn token(&self) -> Result<Token> {
        Err(Error::Other(
            "use Credential.find_default to find a credential from the env".into(),
        ))
    }
}

/// This type is meant to wrap another [Source] and keep returning the same [Token]
// as long as it is valid.
pub(crate) struct RefresherSource {
    pub(crate) current_token: Arc<Mutex<Token>>,
    pub(crate) source: Box<dyn Source + Send + Sync>,
}

impl Default for RefresherSource {
    fn default() -> Self {
        Self {
            current_token: Arc::new(Mutex::new(Token {
                value: String::new(),
                expires: None,
            })),
            source: Box::new(NoOpSource {}),
        }
    }
}

#[async_trait]
impl Source for RefresherSource {
    async fn token(&self) -> Result<Token> {
        let mut cur_token = self.current_token.lock().await;
        if cur_token.is_valid() {
            return Ok(cur_token.clone());
        }
        let new_token = self.source.token().await.unwrap();
        (*cur_token).value = new_token.value;
        (*cur_token).expires = new_token.expires;
        return Ok(cur_token.clone());
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicI64, Ordering};

    use chrono::DateTime;

    use super::*;

    #[tokio::main]
    #[test]
    async fn service_account_test() {
        todo!("write a good test");
    }

    #[tokio::test]
    async fn user_account_test() {
        todo!("write a good test");
    }

    struct FakeSource {
        static_time: DateTime<Utc>,
        counter: AtomicI64,
    }

    #[async_trait]
    impl Source for FakeSource {
        async fn token(&self) -> Result<Token> {
            let count = self.counter.fetch_add(1, Ordering::SeqCst);
            Ok(Token {
                value: format!("token-{}", count),
                expires: Some(self.static_time),
            })
        }
    }

    #[tokio::main]
    #[test]
    async fn test_refresher_returns_same_value() {
        let it = RefresherSource {
            current_token: Arc::new(Mutex::new(Token {
                value: String::new(),
                expires: None,
            })),
            source: Box::new(FakeSource {
                static_time: Utc::now() + chrono::Duration::seconds(10),
                counter: AtomicI64::new(0),
            }),
        };
        let tok1 = it.token().await.unwrap();
        let tok2 = it.token().await.unwrap();
        assert_eq!(tok1.value, tok2.value)
    }

    #[tokio::main]
    #[test]
    async fn test_refresher_returns_new_value() {
        let it = RefresherSource {
            current_token: Arc::new(Mutex::new(Token {
                value: String::new(),
                expires: None,
            })),
            source: Box::new(FakeSource {
                static_time: Utc::now() - chrono::Duration::seconds(20),
                counter: AtomicI64::new(0),
            }),
        };
        let tok1 = it.token().await.unwrap();
        let tok2 = it.token().await.unwrap();
        assert_ne!(tok1.value, tok2.value)
    }
}
