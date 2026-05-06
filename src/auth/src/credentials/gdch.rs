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
//! let credentials = gdch::Builder::new("https://my-target-service-audience.com", gdch_key)
//!     .build()?;
//! let headers = credentials.headers(Extensions::new()).await?;
//! println!("Headers: {headers:?}");
//! # Ok(()) }
//! ```
//!
//! [Google Distributed Cloud]: https://docs.cloud.google.com/distributed-cloud/docs

use super::internal::sts_exchange::{ExchangeTokenRequest, STSHandler};
use crate::constants::{GDCH_SERVICEACCOUNT_TOKEN_TYPE, TOKEN_EXCHANGE_TOKEN_TYPE};
use crate::credentials::dynamic::{AccessTokenCredentialsProvider, CredentialsProvider};
use crate::credentials::errors::CredentialsError;
use crate::credentials::service_account::jws::{JwsClaims, JwsHeader};
use crate::credentials::{AccessToken, AccessTokenCredentials, CacheableResource, Credentials};
use crate::headers_util::AuthHeadersBuilder;
use crate::token::{CachedTokenProvider, Token, TokenProvider};
use crate::token_cache::TokenCache;
use crate::{Result, errors};
use async_trait::async_trait;
use base64::prelude::{BASE64_URL_SAFE_NO_PAD, Engine as _};
use http::{Extensions, HeaderMap};
use rustls::sign::Signer;
use rustls_pki_types::PrivateKeyDer;
use rustls_pki_types::pem::PemObject;
use serde::Deserialize;
use std::sync::Arc;
use tokio::time::Instant;

/// Represents a Google Distributed Cloud service account key.
#[derive(Deserialize, Clone)]
struct GdchServiceAccountKey {
    /// The credential type, must be "gdch_service_account".
    #[serde(rename = "type")]
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
    ca_cert_path: Option<String>,
    /// The URI to exchange the JWT for a token.
    token_uri: String,
}

impl GdchServiceAccountKey {
    fn signer(&self) -> std::result::Result<Box<dyn Signer>, CredentialsError> {
        let private_key = self.private_key.clone();
        let key_provider = crate::credentials::crypto_provider::get_key_provider();

        let key_der = PrivateKeyDer::from_pem_slice(private_key.as_bytes()).map_err(|e| {
            errors::non_retryable_from_str(format!(
                "failed to parse GDCH service account private key PEM: {}",
                e,
            ))
        })?;

        let pk = key_provider
            .load_private_key(key_der)
            .map_err(|e| CredentialsError::from_source(false, e))?;

        pk.choose_scheme(&[rustls::SignatureScheme::ECDSA_NISTP256_SHA256])
            .ok_or_else(|| errors::non_retryable_from_str(
                "unable to choose ECDSA_NISTP256_SHA256 signing scheme as it is not supported by current signer",
            ))
    }
}

// Implements `Debug` for `GdchServiceAccountKey` to avoid printing the private key.
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
    fn new(audience: String, key: GdchServiceAccountKey) -> Self {
        Self { audience, key }
    }

    fn generate_subject_token(&self) -> Result<String> {
        let current_time = time::OffsetDateTime::now_utc();

        let header = JwsHeader {
            alg: "ES256",
            typ: "JWT",
            kid: Some(self.key.private_key_id.clone()),
        };

        let iss = format!(
            "system:serviceaccount:{}:{}",
            self.key.project, self.key.name
        );
        let claims = JwsClaims {
            iss: iss.clone(),
            sub: Some(iss),
            aud: Some(self.key.token_uri.clone()),
            iat: current_time,
            exp: current_time + std::time::Duration::from_secs(3600),
            scope: None,
            typ: None,
            target_audience: None,
        };

        let encoded_header = header.encode()?;
        let encoded_claims = claims.encode()?;

        let to_sign = format!("{}.{}", encoded_header, encoded_claims);

        let signer = self.key.signer()?;

        let sig_der = signer
            .sign(to_sign.as_bytes())
            .map_err(errors::non_retryable)?;
        let sig = p256::ecdsa::Signature::from_der(&sig_der).map_err(|e| {
            errors::non_retryable_from_str(format!("failed to parse ecdsa DER signature: {}", e))
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
            .with_body_encoding(super::internal::sts_exchange::BodyEncoding::Json)
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

/// A builder for constructing Google Distributed Cloud (GDC) service account
/// [Credentials] instances.
///
/// # Example
/// ```
/// # use google_cloud_auth::credentials::gdch;
/// # use serde_json::json;
/// # use http::Extensions;
///
/// # async fn sample() -> anyhow::Result<()> {
/// let gdch_key = json!({
///     "type": "gdch_service_account",
///     "format_version": "1",
///     "project": "my-project",
///     "private_key_id": "my-key-id",
///     "private_key": "-----BEGIN EC PRIVATE KEY-----\n...key bytes...\n-----END EC PRIVATE KEY-----\n",
///     "name": "my-service-identity",
///     "token_uri": "https://service-accounts.my-domain.com/authenticate"
/// });
///
/// let credentials = gdch::Builder::new("https://my-target-service-audience.com", gdch_key).build()?;
/// # Ok(()) }
/// ```
impl Builder {
    /// Creates a new builder using [Google Distributed Cloud service account key] JSON value and the
    /// `audience`. The `audience` specifies the service that the token is intended for.
    ///
    /// [cloud-platform]:https://cloud.google.com/compute/docs/access/service-accounts#scopes_best_practice
    /// [Google Distributed Cloud service account key]: https://docs.cloud.google.com/distributed-cloud/hosted/docs/latest/gdcag/platform/pa-user/service-identity#auth-and-use-sa
    pub fn new<S: Into<String>>(audience: S, key: serde_json::Value) -> Self {
        Self {
            key,
            quota_project_id: None,
            audience: audience.into(),
        }
    }

    /// Sets the [quota project] for this credentials.
    ///
    /// In some services, you can use a service account in
    /// one project for authentication and authorization, and charge
    /// the usage to a different project. This requires that the
    /// service account has `serviceusage.services.use` permissions on the quota project.
    ///
    /// [quota project]: https://cloud.google.com/docs/quotas/quota-project
    pub fn with_quota_project_id<S: Into<String>>(mut self, quota_project_id: S) -> Self {
        self.quota_project_id = Some(quota_project_id.into());
        self
    }

    fn build_credentials(self) -> crate::BuildResult<GdchServiceAccountCredentials> {
        let key = serde_json::from_value::<GdchServiceAccountKey>(self.key)
            .map_err(crate::build_errors::Error::parsing)?;

        if key.format_version != "1" {
            return Err(crate::build_errors::Error::not_supported(format!(
                "unsupported format_version: {}. Expected '1'",
                key.format_version
            )));
        }

        Ok(GdchServiceAccountCredentials {
            token_provider: TokenCache::new(GdchServiceAccountTokenProvider::new(
                self.audience,
                key,
            )),
            quota_project_id: self.quota_project_id,
        })
    }

    /// Returns a [Credentials] instance with the configured settings.
    ///
    /// # Errors
    ///
    /// Returns an error if the `key` provided to [`Builder::new`]
    /// cannot be successfully deserialized into the expected format for a
    /// Google Distributed Cloud service account key. This typically happens if
    /// the JSON value is malformed or missing required fields.
    ///
    /// For more information, on the expected format for a Google Distributed Cloud
    /// service account key, see the [Google Distributed Cloud service account identity] page.
    ///
    /// [Google Distributed Cloud service account identity]: https://docs.cloud.google.com/distributed-cloud/hosted/docs/latest/gdcag/platform/pa-user/service-identity#auth-and-use-sa
    pub fn build(self) -> crate::BuildResult<Credentials> {
        Ok(Credentials {
            inner: Arc::new(self.build_credentials()?),
        })
    }

    /// Returns an [AccessTokenCredentials] instance with the configured settings.
    ///
    /// # Errors
    ///
    /// Returns an error if the `key` provided to [`Builder::new`]
    /// cannot be successfully deserialized into the expected format for a
    /// Google Distributed Cloud service account key. This typically happens if
    /// the JSON value is malformed or missing required fields.
    ///
    /// For more information, on the expected format for a Google Distributed Cloud
    /// service account key, see the [Google Distributed Cloud service account identity] page.
    ///
    /// [Google Distributed Cloud service account identity]: https://docs.cloud.google.com/distributed-cloud/hosted/docs/latest/gdcag/platform/pa-user/service-identity#auth-and-use-sa
    pub fn build_access_token_credentials(self) -> crate::BuildResult<AccessTokenCredentials> {
        Ok(AccessTokenCredentials {
            inner: Arc::new(self.build_credentials()?),
        })
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
mod tests {
    use super::*;
    use httptest::{Expectation, Server, matchers::*, responders::*};
    use serde_json::json;

    type TestResult = anyhow::Result<()>;

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
    fn parse_valid_json() -> TestResult {
        let json = json!({
            "type": "gdch_service_account",
            "format_version": "1",
            "project": "test-project",
            "private_key_id": "test-key-id",
            "private_key": crate::credentials::tests::ES256_PEM.as_str(),
            "name": "test-name",
            "token_uri": "http://localhost/token"
        });

        let key: GdchServiceAccountKey = serde_json::from_value(json)?;
        assert_eq!(key.cred_type, "gdch_service_account");
        assert_eq!(key.project, "test-project");
        Ok(())
    }

    #[test]
    fn generate_subject_token() -> TestResult {
        let key = get_mock_key();
        let provider = GdchServiceAccountTokenProvider::new("test-audience".to_string(), key);
        let jwt = provider.generate_subject_token()?;

        let parts: Vec<&str> = jwt.split('.').collect();
        assert_eq!(parts.len(), 3);

        let header = String::from_utf8(BASE64_URL_SAFE_NO_PAD.decode(parts[0])?)?;
        let claims = String::from_utf8(BASE64_URL_SAFE_NO_PAD.decode(parts[1])?)?;

        let header_json: serde_json::Value = serde_json::from_str(&header)?;
        let claims_json: serde_json::Value = serde_json::from_str(&claims)?;

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
        Ok(())
    }

    #[tokio::test]
    async fn token_exchange() -> TestResult {
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

        let token = provider.token().await?;
        assert_eq!(token.token, "sts-token");
        assert_eq!(token.token_type, "Bearer");
        assert!(token.expires_at.is_some(), "{token:?}");
        Ok(())
    }

    #[test_case::test_case(None, 1; "without quota project")]
    #[test_case::test_case(Some("test-quota-project"), 2; "with quota project")]
    #[tokio::test]
    async fn headers_success(quota_project: Option<&str>, expected_len: usize) -> TestResult {
        let token = Token {
            token: "test-token".to_string(),
            token_type: "Bearer".to_string(),
            expires_at: None,
            metadata: None,
        };

        let mut mock = crate::token::tests::MockTokenProvider::new();
        mock.expect_token().times(1).return_once(|| Ok(token));

        let credentials = GdchServiceAccountCredentials {
            token_provider: TokenCache::new(mock),
            quota_project_id: quota_project.map(|s| s.to_string()),
        };

        let cached_headers = credentials.headers(Extensions::new()).await?;
        let headers = crate::credentials::tests::get_headers_from_cache(cached_headers)?;
        let token_val = headers
            .get(http::header::AUTHORIZATION)
            .ok_or_else(|| anyhow::anyhow!("missing auth header"))?;

        assert_eq!(headers.len(), expected_len);
        assert_eq!(
            token_val,
            http::HeaderValue::from_static("Bearer test-token")
        );

        if let Some(qp) = quota_project {
            let quota_project_header = headers.get(crate::credentials::QUOTA_PROJECT_KEY).unwrap();
            assert_eq!(quota_project_header, http::HeaderValue::from_str(qp)?);
        }
        Ok(())
    }

    #[tokio::test]
    async fn headers_failure() -> TestResult {
        let mut mock = crate::token::tests::MockTokenProvider::new();
        mock.expect_token()
            .times(1)
            .return_once(|| Err(errors::non_retryable_from_str("fail")));

        let credentials = GdchServiceAccountCredentials {
            token_provider: TokenCache::new(mock),
            quota_project_id: None,
        };

        let res = credentials.headers(Extensions::new()).await;
        assert!(res.is_err(), "{res:?}");
        Ok(())
    }

    #[tokio::test]
    async fn access_token_success() -> TestResult {
        let token = Token {
            token: "test-token".to_string(),
            token_type: "Bearer".to_string(),
            expires_at: None,
            metadata: None,
        };

        let mut mock = crate::token::tests::MockTokenProvider::new();
        mock.expect_token().times(1).return_once(|| Ok(token));

        let credentials = GdchServiceAccountCredentials {
            token_provider: TokenCache::new(mock),
            quota_project_id: None,
        };

        let access_token = credentials.access_token().await?;
        assert_eq!(access_token.token, "test-token");
        Ok(())
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
        assert!(err.is_not_supported());
    }
}
