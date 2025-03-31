// Copyright 2025 Google LLC
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

//! [Service Account] Credentials type.
//!
//! A service account is an account for an application or compute workload
//! instead of an individual end user. The recommended practice is to use
//! Google Default Credentials, which relies on the configuration of the Google
//! Cloud system hosting your application (GCE[gce-link], GKE[gke-link], [Cloud Run]) to authenticate
//! your workload or application.  But sometimes you may need to create and
//! download a [service account key], for example, to use a service account
//! when running your application on a system that is not part of Google Cloud.
//!
//! Service account credentials are used in this latter case.
//!
//! You can create multiple service account keys for a single service account.
//! When you create a service account key, the key is returned as string, in the
//! format described by [aip/4112]. This string contains an id for the service
//! account, as well as the cryptographical materials (a RSA private key)
//! required to authenticate the caller.
//!
//! Therefore, services account keys should be treated as any other secret
//! with security implications. Think of them as unencrypted passwords. Do not
//! store them where unauthorized persons or programs may read them.
//!
//! The credentials in this module use [Self-signed JWTs] to bypass the
//! intermediate step of exchanging client assertions for OAuth tokens.
//!
//! The types in this module allow you to retrieve access tokens, and
//! can be used with the Google Cloud client libraries for Rust.
//!
//! While the Google Cloud client libraries for Rust default to
//! using the types defined in this module. You may want to use said types directly
//! when the service account key is obtained from Cloud Secret Manager or a similar service.
//!
//! Example usage:
//!
//! ```
//! # use google_cloud_auth::credentials::service_account::{Builder, ServiceAccountKey};
//! # use google_cloud_auth::credentials::Credential;
//! # use google_cloud_auth::errors::CredentialError;
//! # tokio_test::block_on(async {
//! let service_account_key = serde_json::from_value::<ServiceAccountKey>(serde_json::json!({
//! "client_email": "test-client-email",
//! "private_key_id": "test-private-key-id",
//! "private_key": "", // <-- Provide valid PKCS#8 PEM key here
//! "project_id": "test-project-id",
//! "universe_domain": "test-universe-domain",
//! })).unwrap();
//! let credential: Credential = Builder::default().service_account_key(service_account_key).quota_project_id("my-quota-project").build();
//! let token = credential.get_token().await?;
//! println!("Token: {}", token.token);
//! # Ok::<(), CredentialError>(())
//! # });
//! ```
//!
//! [aip/4112]: https://google.aip.dev/auth/4112
//! [Cloud Run]: https://cloud.google.com/run
//! [gce-link]: https://cloud.google.com/products/compute
//! [gke-link]: https://cloud.google.com/kubernetes-engine
//! [Self-signed JWTs]: https://google.aip.dev/auth/4111
//! [Service Account]: https://cloud.google.com/iam/docs/service-account-creds
//! [service account key]: https://cloud.google.com/iam/docs/keys-create-delete#creating

mod jws;

use crate::credentials::QUOTA_PROJECT_KEY;
use crate::credentials::dynamic::CredentialTrait;
use crate::credentials::{Credential, Result};
use crate::errors::CredentialError;
use crate::token::{Token, TokenProvider};
use crate::token_cache::TokenCache;
use async_trait::async_trait;
use http::header::{AUTHORIZATION, HeaderName, HeaderValue};
use jws::{CLOCK_SKEW_FUDGE, DEFAULT_TOKEN_TIMEOUT, JwsClaims, JwsHeader};
use rustls::crypto::CryptoProvider;
use rustls::sign::Signer;
use rustls_pemfile::Item;
use std::sync::Arc;
use time::OffsetDateTime;

const DEFAULT_SCOPES: &str = "https://www.googleapis.com/auth/cloud-platform";

pub(crate) fn creds_from(js: serde_json::Value) -> Result<Credential> {
    let service_account_key =
        serde_json::from_value::<ServiceAccountKey>(js).map_err(CredentialError::non_retryable)?;
    Ok(Builder::default()
        .service_account_key(service_account_key)
        .build())
}

#[derive(Default)]
pub struct Builder {
    service_account_key: ServiceAccountKey,
    aud: Option<String>,
    scopes: Option<String>,
    quota_project_id: Option<String>,
}

impl Builder {
    /// Sets the [service account key] in [aip/4112] format for this credential.
    ///
    /// [aip/4112]: https://google.aip.dev/auth/4112
    /// [service account key]: https://cloud.google.com/iam/docs/keys-create-delete#creating
    pub fn service_account_key(mut self, service_account_key: ServiceAccountKey) -> Self {
        self.service_account_key = service_account_key;
        self
    }

    /// Sets the audience for this credential.
    ///
    /// aud is a [JWT] claim specifying intended recipient(s) of the token
    /// that is, a service(s) or resource(s).
    /// This cannot be used at the same time as the scopes claim.
    /// The value should be https://[SERVICE]/. (e.g. https://pubsub.googleapis.com/)
    ///
    /// [JWT]: https://google.aip.dev/auth/4111
    pub fn aud<S: Into<String>>(mut self, aud: S) -> Self {
        self.aud = Some(aud.into());
        self
    }

    /// Sets the [scopes] for this credentials.
    ///
    /// scope is a [JWT] claim specifying requested permission(s) for the token.
    /// This cannot be used at the same time as the aud claim.
    /// Multiple scopes can be specified using single space (" ") as delimiter.
    ///
    /// [JWT]: https://google.aip.dev/auth/4111
    /// [scopes]: https://developers.google.com/identity/protocols/oauth2/scopes
    pub fn scopes<S: Into<String>>(mut self, scopes: S) -> Self {
        self.scopes = Some(scopes.into());
        self
    }

    /// Set the [quota project] for this credential.
    ///
    /// In some services, you can use a service account in
    /// one project for authentication and authorization, and charge
    /// the usage to a different project. This may require that the
    /// service account has `serviceusage.services.use` permissions on the quota project.
    ///
    /// [quota project]: https://cloud.google.com/docs/quotas/quota-project
    pub fn quota_project_id<S: Into<String>>(mut self, quota_project_id: S) -> Self {
        self.quota_project_id = Some(quota_project_id.into());
        self
    }

    /// Returns a [Credential] instance with the configured settings.
    pub fn build(self) -> Credential {
        let token_provider = ServiceAccountTokenProvider {
            service_account_key: self.service_account_key,
            aud: self.aud,
            scopes: self.scopes,
        };
        let token_provider = TokenCache::new(token_provider);

        Credential {
            inner: Arc::new(ServiceAccountCredential {
                token_provider,
                quota_project_id: self.quota_project_id,
            }),
        }
    }
}

/// A representation of a [service account key] in the format described by [aip/4112].
///
/// This type is typically created by
/// deserializing the JSON key data, for example, when the service account key
/// is obtained from [Cloud Secret Manager] or a similar service.
/// This key can then be used to create a [Credential] e.g., by passing it to the [Builder].
///
/// [aip/4112]: https://google.aip.dev/auth/4112
/// [Cloud Secret Manager]: https://cloud.google.com/secret-manager/docs
/// [Service Account Key]: https://cloud.google.com/iam/docs/keys-create-delete#creating
#[derive(serde::Deserialize, Default, Clone)]
pub struct ServiceAccountKey {
    /// The client email address of the service account.
    /// (e.g., "my-sa@my-project.iam.gserviceaccount.com").
    pub client_email: String,
    /// ID of the service account's private key.
    pub private_key_id: String,
    /// The PEM-encoded PKCS#8 private key string associated with the service account.
    /// Begins with `-----BEGIN PRIVATE KEY-----`.
    pub private_key: String,
    /// The project id the service account belongs to.
    pub project_id: String,
    /// The universe domain this service account belongs to.
    pub universe_domain: String,
}

impl std::fmt::Debug for ServiceAccountKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServiceAccountKey")
            .field("client_email", &self.client_email)
            .field("private_key_id", &self.private_key_id)
            .field("private_key", &"[censored]")
            .field("project_id", &self.project_id)
            .field("universe_domain", &self.universe_domain)
            .finish()
    }
}

#[derive(Debug)]
struct ServiceAccountCredential<T>
where
    T: TokenProvider,
{
    token_provider: T,
    quota_project_id: Option<String>,
}

#[derive(Debug)]
struct ServiceAccountTokenProvider {
    service_account_key: ServiceAccountKey,
    aud: Option<String>,
    scopes: Option<String>,
}

#[async_trait]
impl TokenProvider for ServiceAccountTokenProvider {
    async fn get_token(&self) -> Result<Token> {
        let signer = self.signer(&self.service_account_key.private_key)?;

        let expires_at = std::time::Instant::now() - CLOCK_SKEW_FUDGE + DEFAULT_TOKEN_TIMEOUT;
        // The claims encode a unix timestamp. `std::time::Instant` has no
        // epoch, so we use `time::OffsetDateTime`, which reads system time, in
        // the implementation.
        let now = OffsetDateTime::now_utc() - CLOCK_SKEW_FUDGE;
        let exp = now + DEFAULT_TOKEN_TIMEOUT;
        let scopes = if self.aud.is_none() && self.scopes.is_none() {
            Some(DEFAULT_SCOPES.to_string())
        } else {
            self.scopes.clone()
        };

        let claims = JwsClaims {
            iss: self.service_account_key.client_email.clone(),
            scope: scopes,
            aud: self.aud.clone(),
            exp,
            iat: now,
            typ: None,
            sub: Some(self.service_account_key.client_email.clone()),
        };

        let header = JwsHeader {
            alg: "RS256",
            typ: "JWT",
            kid: &self.service_account_key.private_key_id,
        };
        let encoded_header_claims = format!("{}.{}", header.encode()?, claims.encode()?);
        let sig = signer
            .sign(encoded_header_claims.as_bytes())
            .map_err(CredentialError::non_retryable)?;
        use base64::prelude::{BASE64_URL_SAFE_NO_PAD, Engine as _};
        let token = format!(
            "{}.{}",
            encoded_header_claims,
            &BASE64_URL_SAFE_NO_PAD.encode(sig)
        );

        let token = Token {
            token,
            token_type: "Bearer".to_string(),
            expires_at: Some(expires_at),
            metadata: None,
        };
        Ok(token)
    }
}

impl ServiceAccountTokenProvider {
    // Creates a signer using the private key stored in the service account file.
    fn signer(&self, private_key: &String) -> Result<Box<dyn Signer>> {
        let key_provider = CryptoProvider::get_default().map_or_else(
            || rustls::crypto::ring::default_provider().key_provider,
            |p| p.key_provider,
        );

        let private_key = rustls_pemfile::read_one(&mut private_key.as_bytes())
            .map_err(CredentialError::non_retryable)?
            .ok_or_else(|| {
                CredentialError::non_retryable_from_str(
                    "missing PEM section in service account key",
                )
            })?;
        let pk = match private_key {
            Item::Pkcs8Key(item) => key_provider.load_private_key(item.into()),
            other => {
                return Err(Self::unexpected_private_key_error(other));
            }
        };
        let sk = pk.map_err(CredentialError::non_retryable)?;

        sk.choose_scheme(&[rustls::SignatureScheme::RSA_PKCS1_SHA256])
            .ok_or_else(|| CredentialError::non_retryable_from_str("Unable to choose RSA_PKCS1_SHA256 signing scheme as it is not supported by current signer"))
    }

    fn unexpected_private_key_error(private_key_format: Item) -> CredentialError {
        CredentialError::non_retryable_from_str(format!(
            "expected key to be in form of PKCS8, found {:?}",
            private_key_format
        ))
    }
}

#[async_trait::async_trait]
impl<T> CredentialTrait for ServiceAccountCredential<T>
where
    T: TokenProvider,
{
    async fn get_token(&self) -> Result<Token> {
        self.token_provider.get_token().await
    }

    async fn get_headers(&self) -> Result<Vec<(HeaderName, HeaderValue)>> {
        let token = self.get_token().await?;
        let mut value = HeaderValue::from_str(&format!("{} {}", token.token_type, token.token))
            .map_err(CredentialError::non_retryable)?;
        value.set_sensitive(true);
        let mut headers = vec![(AUTHORIZATION, value)];
        if let Some(project) = &self.quota_project_id {
            headers.push((
                HeaderName::from_static(QUOTA_PROJECT_KEY),
                HeaderValue::from_str(project).map_err(CredentialError::non_retryable)?,
            ));
        }
        Ok(headers)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::credentials::test::HV;
    use crate::token::test::MockTokenProvider;
    use base64::Engine;
    use rsa::RsaPrivateKey;
    use rsa::pkcs1::EncodeRsaPrivateKey;
    use rsa::pkcs8::EncodePrivateKey;
    use rsa::pkcs8::LineEnding;
    use rustls_pemfile::Item;
    use serde_json::Value;
    use serde_json::json;
    use std::time::Duration;

    type TestResult = std::result::Result<(), Box<dyn std::error::Error>>;

    const SSJ_REGEX: &str = r"(?<header>[^\.]+)\.(?<claims>[^\.]+)\.(?<sig>[^\.]+)";

    #[test]
    fn debug_token_provider() {
        let expected = ServiceAccountKey {
            client_email: "test-client-email".to_string(),
            private_key_id: "test-private-key-id".to_string(),
            private_key: "super-duper-secret-private-key".to_string(),
            project_id: "test-project-id".to_string(),
            universe_domain: "test-universe-domain".to_string(),
        };
        let fmt = format!("{expected:?}");
        assert!(fmt.contains("test-client-email"), "{fmt}");
        assert!(fmt.contains("test-private-key-id"), "{fmt}");
        assert!(!fmt.contains("super-duper-secret-private-key"), "{fmt}");
        assert!(fmt.contains("test-project-id"), "{fmt}");
        assert!(fmt.contains("test-universe-domain"), "{fmt}");
    }

    #[tokio::test]
    async fn get_token_success() {
        let expected = Token {
            token: "test-token".to_string(),
            token_type: "Bearer".to_string(),
            expires_at: None,
            metadata: None,
        };
        let expected_clone = expected.clone();

        let mut mock = MockTokenProvider::new();
        mock.expect_get_token()
            .times(1)
            .return_once(|| Ok(expected_clone));

        let sac = ServiceAccountCredential {
            token_provider: mock,
            quota_project_id: None,
        };
        let actual = sac.get_token().await.unwrap();
        assert_eq!(actual, expected);
    }

    #[tokio::test]
    async fn get_token_failure() {
        let mut mock = MockTokenProvider::new();
        mock.expect_get_token()
            .times(1)
            .return_once(|| Err(CredentialError::non_retryable_from_str("fail")));

        let sac = ServiceAccountCredential {
            token_provider: mock,
            quota_project_id: None,
        };
        assert!(sac.get_token().await.is_err());
    }

    #[tokio::test]
    async fn get_headers_success_without_quota_project() {
        let token = Token {
            token: "test-token".to_string(),
            token_type: "Bearer".to_string(),
            expires_at: None,
            metadata: None,
        };

        let mut mock = MockTokenProvider::new();
        mock.expect_get_token().times(1).return_once(|| Ok(token));

        let sac = ServiceAccountCredential {
            token_provider: mock,
            quota_project_id: None,
        };
        let headers: Vec<HV> = HV::from(sac.get_headers().await.unwrap());

        assert_eq!(
            headers,
            vec![HV {
                header: AUTHORIZATION.to_string(),
                value: "Bearer test-token".to_string(),
                is_sensitive: true,
            }]
        );
    }

    #[tokio::test]
    async fn get_headers_success_with_quota_project() {
        let token = Token {
            token: "test-token".to_string(),
            token_type: "Bearer".to_string(),
            expires_at: None,
            metadata: None,
        };

        let quota_project = "test-quota-project";

        let mut mock = MockTokenProvider::new();
        mock.expect_get_token().times(1).return_once(|| Ok(token));

        let sac = ServiceAccountCredential {
            token_provider: mock,
            quota_project_id: Some(quota_project.to_string()),
        };
        let headers: Vec<HV> = HV::from(sac.get_headers().await.unwrap());

        assert_eq!(
            headers,
            vec![
                HV {
                    header: AUTHORIZATION.to_string(),
                    value: "Bearer test-token".to_string(),
                    is_sensitive: true,
                },
                HV {
                    header: QUOTA_PROJECT_KEY.to_string(),
                    value: quota_project.to_string(),
                    is_sensitive: false,
                }
            ]
        );
    }

    #[tokio::test]
    async fn get_headers_failure() {
        let mut mock = MockTokenProvider::new();
        mock.expect_get_token()
            .times(1)
            .return_once(|| Err(CredentialError::non_retryable_from_str("fail")));

        let sac = ServiceAccountCredential {
            token_provider: mock,
            quota_project_id: None,
        };
        assert!(sac.get_headers().await.is_err());
    }

    fn get_mock_service_key() -> ServiceAccountKey {
        let service_account_key_json = json!({
            "client_email": "test-client-email",
            "private_key_id": "test-private-key-id",
            "private_key": "",
            "project_id": "test-project-id",
            "universe_domain": "test-universe-domain",
        });
        serde_json::from_value::<ServiceAccountKey>(service_account_key_json)
            .map_err(CredentialError::non_retryable)
            .unwrap()
    }

    fn generate_pkcs1_private_key() -> String {
        let mut rng = rand::thread_rng();
        let bits = 2048;
        let priv_key = RsaPrivateKey::new(&mut rng, bits).expect("failed to generate a key");
        priv_key
            .to_pkcs1_pem(LineEnding::LF)
            .expect("Failed to encode key to PKCS#1 PEM")
            .to_string()
    }

    #[tokio::test]
    async fn get_service_account_token_pkcs1_private_key_failure() -> TestResult {
        let mut service_account_key = get_mock_service_key();
        service_account_key.private_key = generate_pkcs1_private_key();
        let cred = Builder::default()
            .service_account_key(service_account_key)
            .build();
        let expected_error_message = "expected key to be in form of PKCS8, found Pkcs1Key";
        assert!(
            cred.get_token()
                .await
                .is_err_and(|e| e.to_string().contains(expected_error_message))
        );
        Ok(())
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

    #[tokio::test]
    async fn get_service_account_token_pkcs8_key_success() -> TestResult {
        let mut service_account_key = get_mock_service_key();
        service_account_key.private_key = generate_pkcs8_private_key();
        let cred = Builder::default()
            .service_account_key(service_account_key.clone())
            .build();
        let token = cred.get_token().await?;
        let re = regex::Regex::new(SSJ_REGEX).unwrap();
        let captures = re.captures(&token.token).ok_or_else(|| {
            format!(
                r#"Expected token in form: "<header>.<claims>.<sig>". Found token: {}"#,
                token.token
            )
        })?;
        let header = b64_decode_to_json(captures["header"].to_string());
        assert_eq!(header["alg"], "RS256");
        assert_eq!(header["typ"], "JWT");
        assert_eq!(header["kid"], service_account_key.private_key_id);

        let claims = b64_decode_to_json(captures["claims"].to_string());
        assert_eq!(claims["iss"], service_account_key.client_email);
        assert_eq!(claims["scope"], DEFAULT_SCOPES);
        assert!(claims["iat"].is_number());
        assert!(claims["exp"].is_number());
        assert_eq!(claims["sub"], service_account_key.client_email);

        Ok(())
    }

    #[tokio::test]
    async fn token_caching() -> TestResult {
        let private_key = generate_pkcs8_private_key();

        let json_value = json!({
            "client_email": "test-client-email",
            "private_key_id": "test-private-key-id",
            "private_key": private_key,
            "project_id": "test-project-id",
            "universe_domain": "test-universe-domain"
        });

        let credential = creds_from(json_value)?;
        let token = credential.get_token().await?;

        let re = regex::Regex::new(SSJ_REGEX).unwrap();
        let captures = re.captures(&token.token).unwrap();

        let claims = b64_decode_to_json(captures["claims"].to_string());
        let first_iat = claims["iat"].as_i64().unwrap();

        // The issued at claim (`iat`) encodes a unix timestamp, in seconds.
        // Sleeping for one second ensures that a subsequent claim has a
        // different `iat`. We need a real sleep, because we cannot fake the
        // current unix timestamp.
        std::thread::sleep(Duration::from_secs(1));

        // Get the token again.
        let token = credential.get_token().await?;
        let captures = re.captures(&token.token).unwrap();

        let claims = b64_decode_to_json(captures["claims"].to_string());
        let second_iat = claims["iat"].as_i64().unwrap();

        // Validate that the issued at claim is the same for the two tokens. If
        // the 2nd token is not from the cache, its `iat` will be different.
        assert_eq!(first_iat, second_iat);

        Ok(())
    }

    #[tokio::test]
    async fn get_service_account_token_invalid_key_failure() -> TestResult {
        let mut service_account_key = get_mock_service_key();
        let pem_data = "-----BEGIN PRIVATE KEY-----\nMIGkAg==\n-----END PRIVATE KEY-----";
        service_account_key.private_key = pem_data.to_string();
        let cred = Builder::default()
            .service_account_key(service_account_key)
            .build();

        let token = cred.get_token().await;
        let expected_error_message = "failed to parse private key";
        assert!(token.is_err_and(|e| e.to_string().contains(expected_error_message)));
        Ok(())
    }

    #[test]
    fn signer_failure() -> TestResult {
        let tp = ServiceAccountTokenProvider {
            service_account_key: get_mock_service_key(),
            aud: None,
            scopes: None,
        };
        let signer = tp.signer(&tp.service_account_key.private_key);
        let expected_error_message = "missing PEM section in service account key";
        assert!(signer.is_err_and(|e| e.to_string().contains(expected_error_message)));
        Ok(())
    }

    #[test]
    fn unexpected_private_key_error_message() -> TestResult {
        let expected_message = format!(
            "expected key to be in form of PKCS8, found {:?}",
            Item::Crl(Vec::new().into()) // Example unsupported key type
        );

        let error =
            ServiceAccountTokenProvider::unexpected_private_key_error(Item::Crl(Vec::new().into()));
        assert!(error.to_string().contains(&expected_message));
        Ok(())
    }

    #[tokio::test]
    async fn get_service_account_token_with_audience() -> TestResult {
        let mut service_account_key = get_mock_service_key();
        service_account_key.private_key = generate_pkcs8_private_key();
        let token = Builder::default()
            .service_account_key(service_account_key.clone())
            .aud("test-audience")
            .build()
            .get_token()
            .await?;

        let re = regex::Regex::new(SSJ_REGEX).unwrap();
        let captures = re.captures(&token.token).ok_or_else(|| {
            format!(
                r#"Expected token in form: "<header>.<claims>.<sig>". Found token: {}"#,
                token.token
            )
        })?;
        let header = b64_decode_to_json(captures["header"].to_string());
        assert_eq!(header["alg"], "RS256");
        assert_eq!(header["typ"], "JWT");
        assert_eq!(header["kid"], service_account_key.private_key_id);

        let claims = b64_decode_to_json(captures["claims"].to_string());
        assert_eq!(claims["iss"], service_account_key.client_email);
        assert_eq!(claims["scope"], Value::Null);
        assert_eq!(claims["aud"], "test-audience");
        assert!(claims["iat"].is_number());
        assert!(claims["exp"].is_number());
        assert_eq!(claims["sub"], service_account_key.client_email);
        Ok(())
    }

    #[tokio::test]
    async fn get_service_account_token_with_custom_scopes() -> TestResult {
        let mut service_account_key = get_mock_service_key();
        let scopes =
            "https://www.googleapis.com/auth/pubsub https://www.googleapis.com/auth/translate";
        service_account_key.private_key = generate_pkcs8_private_key();
        let token = Builder::default()
            .service_account_key(service_account_key.clone())
            .scopes(scopes)
            .build()
            .get_token()
            .await?;

        let re = regex::Regex::new(SSJ_REGEX).unwrap();
        let captures = re.captures(&token.token).ok_or_else(|| {
            format!(
                r#"Expected token in form: "<header>.<claims>.<sig>". Found token: {}"#,
                token.token
            )
        })?;
        let header = b64_decode_to_json(captures["header"].to_string());
        assert_eq!(header["alg"], "RS256");
        assert_eq!(header["typ"], "JWT");
        assert_eq!(header["kid"], service_account_key.private_key_id);

        let claims = b64_decode_to_json(captures["claims"].to_string());
        assert_eq!(claims["iss"], service_account_key.client_email);
        assert_eq!(claims["scope"], scopes);
        assert_eq!(claims["aud"], Value::Null);
        assert!(claims["iat"].is_number());
        assert!(claims["exp"].is_number());
        assert_eq!(claims["sub"], service_account_key.client_email);
        Ok(())
    }
}
