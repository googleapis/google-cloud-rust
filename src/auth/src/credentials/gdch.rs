// Copyright 2026 Google LLC
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

//! [Google Distributed Cloud] service identity authentication.
//!
//! A [Google Distributed Cloud] (GDC) service identity credential allows applications to
//! authenticate and access services securely within private or hybrid cloud environments.
//! Workloads authenticate using service identity keys to obtain short-lived access tokens
//! from a dedicated token exchange endpoint.
//!
//! ## Example: Creating credentials from a JSON object
//!
//! ```
//! # use google_cloud_auth::credentials::gdch;
//! # use serde_json::json;
//! # use http::Extensions;
//!
//! # async fn sample() -> anyhow::Result<()> {
//! let gdch_key = json!({
//!     "type": "gdch_service_account",
//!     "format_version": "1",
//!     "project": "my-project",
//!     "private_key_id": "my-key-id",
//!     "private_key": "-----BEGIN EC PRIVATE KEY-----\n...key bytes...\n-----END EC PRIVATE KEY-----\n",
//!     "name": "my-service-identity",
//!     "token_uri": "https://service-accounts.my-domain.com/authenticate"
//! });
//!
//! let credentials = gdch::Builder::new("my-target-service-audience", gdch_key)
//!     .build()?;
//! let headers = credentials.headers(Extensions::new()).await?;
//! println!("Headers: {headers:?}");
//! # Ok(()) }
//! ```
//!
//! [Google Distributed Cloud]: https://docs.cloud.google.com/distributed-cloud/docs

use super::internal::sts_exchange::{ExchangeTokenRequest, STSHandler};
use crate::Result;
use crate::access_boundary::CredentialsWithAccessBoundary;
use crate::constants::{GDCH_SERVICEACCOUNT_TOKEN_TYPE, TOKEN_EXCHANGE_TOKEN_TYPE};
use crate::credentials::dynamic::{AccessTokenCredentialsProvider, CredentialsProvider};
use crate::credentials::errors::CredentialsError;
use crate::credentials::{AccessToken, AccessTokenCredentials, CacheableResource, Credentials};
use crate::headers_util::AuthHeadersBuilder;
use crate::token::{CachedTokenProvider, Token, TokenProvider};
use crate::token_cache::TokenCache;
use async_trait::async_trait;
use base64::prelude::{BASE64_URL_SAFE_NO_PAD, Engine as _};
use http::{Extensions, HeaderMap};
use rustls::crypto::CryptoProvider;
use rustls::sign::Signer;
use rustls_pki_types::PrivateKeyDer;
use rustls_pki_types::pem::PemObject;
use serde::Deserialize;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::time::Instant;

/// Represents a Google Distributed Cloud service account key.
#[derive(Deserialize, Clone)]
struct GdchServiceAccountKey {
    /// The credential type, must be "gdch_service_account".
    #[serde(rename = "type")]
    #[allow(dead_code)]
    cred_type: String,
    /// The format version of the JSON file.
    format_version: String,
    /// The project ID.
    project: String,
    /// The ID of the private key.
    private_key_id: String,
    /// The PEM-encoded private key (SEC1 format).
    private_key: String,
    /// The name of the service identity.
    name: String,
    /// Optional path to custom CA certificate for TLS verification.
    #[allow(dead_code)]
    ca_cert_path: Option<String>,
    /// The URI to exchange the JWT for a token.
    token_uri: String,
}

impl GdchServiceAccountKey {
    pub(crate) fn signer(&self) -> std::result::Result<Box<dyn Signer>, CredentialsError> {
        let private_key = self.private_key.clone();
        let key_provider = CryptoProvider::get_default().map(|p| p.key_provider);
        #[cfg(feature = "default-rustls-provider")]
        let key_provider = key_provider
            .unwrap_or_else(|| rustls::crypto::aws_lc_rs::default_provider().key_provider);
        #[cfg(not(feature = "default-rustls-provider"))]
        let key_provider = key_provider
            .expect("The default rustls::CryptoProvider should be configured by the application.");

        let key_der = PrivateKeyDer::from_pem_slice(private_key.as_bytes()).map_err(|e| {
            CredentialsError::from_msg(
                false,
                format!(
                    "Failed to parse GDCH service account private key PEM: {}",
                    e
                ),
            )
        })?;

        let pk = key_provider
            .load_private_key(key_der)
            .map_err(|e| CredentialsError::from_source(false, e))?;

        pk.choose_scheme(&[rustls::SignatureScheme::ECDSA_NISTP256_SHA256])
            .ok_or_else(|| {
                CredentialsError::from_msg(
                    false,
                    "Unable to choose ECDSA_NISTP256_SHA256 signing scheme as it is not supported by current signer",
                )
            })
    }
}

impl std::fmt::Debug for GdchServiceAccountKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GdchServiceAccountKey")
            .field("type", &self.cred_type)
            .field("format_version", &self.format_version)
            .field("project", &self.project)
            .field("name", &self.name)
            .field("ca_cert_path", &self.ca_cert_path)
            .field("private_key_id", &self.private_key_id)
            .field("private_key", &"[censored]")
            .field("token_uri", &self.token_uri)
            .finish()
    }
}

/// A token provider for Google Distributed Cloud service accounts.
#[derive(Debug)]
struct GdchServiceAccountTokenProvider {
    audience: String,
    key: GdchServiceAccountKey,
}

impl GdchServiceAccountTokenProvider {
    /// Creates a new token provider with the given key and audience.
    pub(crate) fn new(audience: String, key: GdchServiceAccountKey) -> Self {
        Self { audience, key }
    }

    fn generate_subject_token(&self) -> Result<String> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| CredentialsError::from_source(false, e))?
            .as_secs();
        let exp = now + 3600; // 1 hour

        let header = serde_json::json!({
            "alg": "ES256",
            "typ": "JWT",
            "kid": self.key.private_key_id,
        });

        let iss = format!(
            "system:serviceaccount:{}:{}",
            self.key.project, self.key.name
        );
        let claims = serde_json::json!({
            "iss": iss,
            "sub": iss,
            "aud": self.key.token_uri,
            "iat": now,
            "exp": exp,
        });

        let encoded_header = BASE64_URL_SAFE_NO_PAD.encode(serde_json::to_string(&header).unwrap());
        let encoded_claims = BASE64_URL_SAFE_NO_PAD.encode(serde_json::to_string(&claims).unwrap());

        let to_sign = format!("{}.{}", encoded_header, encoded_claims);

        let signer = self.key.signer()?;

        let sig_der = signer
            .sign(to_sign.as_bytes())
            .map_err(|e| CredentialsError::from_source(false, e))?;
        let sig = p256::ecdsa::Signature::from_der(&sig_der).map_err(|e| {
            CredentialsError::from_msg(false, format!("failed to parse ecdsa DER signature: {}", e))
        })?;
        let encoded_sig = BASE64_URL_SAFE_NO_PAD.encode(&sig.to_bytes()[..]);

        Ok(format!("{}.{}", to_sign, encoded_sig))
    }
}

#[async_trait]
impl TokenProvider for GdchServiceAccountTokenProvider {
    async fn token(&self) -> Result<Token> {
        let subject_token = self.generate_subject_token()?;

        let req = ExchangeTokenRequest {
            url: self.key.token_uri.clone(),
            subject_token,
            subject_token_type: GDCH_SERVICEACCOUNT_TOKEN_TYPE.to_string(),
            audience: Some(self.audience.clone()),
            grant_type: Some(TOKEN_EXCHANGE_TOKEN_TYPE.to_string()),
            ..ExchangeTokenRequest::default()
        };

        let resp = STSHandler::default()
            .with_json_body()
            .with_ca_cert_path(self.key.ca_cert_path.clone())
            .exchange_token(req)
            .await?;

        let expires_at = Instant::now() + tokio::time::Duration::from_secs(resp.expires_in);

        Ok(Token {
            token: resp.access_token,
            token_type: resp.token_type,
            expires_at: Some(expires_at),
            metadata: None,
        })
    }
}

/// Credentials backed by a Google Distributed Cloud service account.
#[derive(Debug)]
struct GdchServiceAccountCredentials {
    token_provider: TokenCache,
    quota_project_id: Option<String>,
}

/// A builder for [`GdchServiceAccountCredentials`].
pub struct Builder {
    key: serde_json::Value,
    quota_project_id: Option<String>,
    audience: String,
}

impl Builder {
    /// Creates a new builder with the given key and audience.
    pub fn new<S: Into<String>>(audience: S, key: serde_json::Value) -> Self {
        Self {
            key,
            quota_project_id: None,
            audience: audience.into(),
        }
    }

    /// Sets the quota project ID.
    pub fn with_quota_project_id<S: Into<String>>(mut self, quota_project_id: S) -> Self {
        self.quota_project_id = Some(quota_project_id.into());
        self
    }

    fn build_credentials(
        self,
    ) -> crate::BuildResult<CredentialsWithAccessBoundary<GdchServiceAccountCredentials>> {
        let key = serde_json::from_value::<GdchServiceAccountKey>(self.key)
            .map_err(crate::build_errors::Error::parsing)?;

        if key.format_version != "1" {
            return Err(crate::build_errors::Error::parsing(format!(
                "unsupported format_version: {}. Expected '1'",
                key.format_version
            )));
        }

        let creds = GdchServiceAccountCredentials {
            token_provider: TokenCache::new(GdchServiceAccountTokenProvider::new(
                self.audience,
                key,
            )),
            quota_project_id: self.quota_project_id,
        };

        Ok(CredentialsWithAccessBoundary::new_no_op(creds))
    }

    /// Builds the credentials.
    pub fn build(self) -> crate::BuildResult<Credentials> {
        Ok(self.build_credentials()?.into())
    }

    /// Builds the access token credentials.
    pub fn build_access_token_credentials(self) -> crate::BuildResult<AccessTokenCredentials> {
        Ok(self.build_credentials()?.into())
    }
}

#[async_trait]
impl CredentialsProvider for GdchServiceAccountCredentials {
    async fn headers(&self, extensions: Extensions) -> Result<CacheableResource<HeaderMap>> {
        let token = self.token_provider.token(extensions).await?;
        AuthHeadersBuilder::new(&token)
            .maybe_quota_project_id(self.quota_project_id.as_deref())
            .build()
    }
}

#[async_trait]
impl AccessTokenCredentialsProvider for GdchServiceAccountCredentials {
    async fn access_token(&self) -> Result<AccessToken> {
        let token = self.token_provider.token(Extensions::new()).await?;
        token.into()
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use httptest::{Expectation, Server, matchers::*, responders::*};
    use serde_json::json;

    fn get_mock_key() -> GdchServiceAccountKey {
        GdchServiceAccountKey {
            cred_type: "gdch_service_account".to_string(),
            format_version: "1".to_string(),
            project: "test-project".to_string(),
            private_key_id: "test-key-id".to_string(),
            private_key: (*crate::credentials::tests::ES256_PEM).clone(),
            name: "test-name".to_string(),
            ca_cert_path: None,
            token_uri: "http://localhost/token".to_string(),
        }
    }
    #[test]
    fn debug_gdch_service_account_key() {
        let key = get_mock_key();
        let fmt = format!("{key:?}");
        assert!(fmt.contains("GdchServiceAccountKey"));
        assert!(fmt.contains("test-project"));
        assert!(fmt.contains("test-name"));
        assert!(fmt.contains("test-key-id"));
        assert!(fmt.contains("[censored]"));
        assert!(!fmt.contains(crate::credentials::tests::ES256_PEM.as_str()));
    }

    #[test]
    fn parse_valid_json() {
        let json = json!({
            "type": "gdch_service_account",
            "format_version": "1",
            "project": "test-project",
            "private_key_id": "test-key-id",
            "private_key": crate::credentials::tests::ES256_PEM.as_str(),
            "name": "test-name",
            "token_uri": "http://localhost/token"
        });

        let key: GdchServiceAccountKey = serde_json::from_value(json).unwrap();
        assert_eq!(key.cred_type, "gdch_service_account");
        assert_eq!(key.project, "test-project");
    }

    #[test]
    fn generate_subject_token() {
        let key = get_mock_key();
        let provider = GdchServiceAccountTokenProvider::new("test-audience".to_string(), key);
        let jwt = provider.generate_subject_token().unwrap();

        let parts: Vec<&str> = jwt.split('.').collect();
        assert_eq!(parts.len(), 3);

        let header = String::from_utf8(BASE64_URL_SAFE_NO_PAD.decode(parts[0]).unwrap()).unwrap();
        let claims = String::from_utf8(BASE64_URL_SAFE_NO_PAD.decode(parts[1]).unwrap()).unwrap();

        let header_json: serde_json::Value = serde_json::from_str(&header).unwrap();
        let claims_json: serde_json::Value = serde_json::from_str(&claims).unwrap();

        assert_eq!(header_json["alg"], "ES256");
        assert_eq!(header_json["typ"], "JWT");
        assert_eq!(header_json["kid"], "test-key-id");

        assert_eq!(
            claims_json["iss"],
            "system:serviceaccount:test-project:test-name"
        );
        assert_eq!(
            claims_json["sub"],
            "system:serviceaccount:test-project:test-name"
        );
        assert_eq!(claims_json["aud"], "http://localhost/token");
    }

    #[tokio::test]
    async fn token_exchange() {
        let server = Server::run();

        let mut key = get_mock_key();
        key.token_uri = server.url("/token").to_string();

        let provider = GdchServiceAccountTokenProvider::new("test-audience".to_string(), key);

        let response_body = json!({
            "access_token": "sts-token",
            "issued_token_type": "urn:ietf:params:oauth:token-type:access_token",
            "token_type": "Bearer",
            "expires_in": 3600
        })
        .to_string();

        server.expect(
            Expectation::matching(all_of![
                request::method_path("POST", "/token"),
                request::body(json_decoded(|body: &serde_json::Value| {
                    body["grant_type"] == TOKEN_EXCHANGE_TOKEN_TYPE
                        && body["subject_token_type"] == GDCH_SERVICEACCOUNT_TOKEN_TYPE
                        && body["audience"] == "test-audience"
                })),
            ])
            .respond_with(status_code(200).body(response_body)),
        );

        let token = provider.token().await.unwrap();
        assert_eq!(token.token, "sts-token");
        assert_eq!(token.token_type, "Bearer");
        assert!(token.expires_at.is_some());
    }

    #[test]
    fn invalid_version() {
        let json = json!({
            "type": "gdch_service_account",
            "format_version": "2", // Invalid
            "project": "test-project",
            "private_key_id": "test-key-id",
            "private_key": crate::credentials::tests::ES256_PEM.as_str(),
            "name": "test-name",
            "token_uri": "http://localhost/token"
        });

        let builder = Builder::new("test-audience", json);
        let err = builder.build().unwrap_err();
        assert!(err.is_parsing());
        assert!(err.to_string().contains("unsupported format_version: 2"));
    }
}
