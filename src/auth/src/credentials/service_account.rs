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
//! instead of an individual end user. The default credentials used by the
//! client libraries may be, and often are, associated with a service account.
//! Therefore, you can use service accounts by configuring your environment,
//! without any code changes.
//!
//! Sometimes the application needs to use a [service account key] directly.
//! The types in this module will help you in this case. For more information
//! on when service account keys are appropriate, consult the
//! relevant section in the [Best practices for using service accounts] guide.
//!
//! You can create multiple service account keys for a single service account.
//! When you [create a service account key], the key is returned as a string.
//! This string contains an ID for the service account, as well as the
//! cryptographic materials (an RSA private key) required to authenticate the caller.
//!
//! Therefore, service account keys should be treated as any other secret
//! with security implications. Think of them as unencrypted passwords. Do not
//! store them where unauthorized persons or programs may read them.
//!
//! The types in this module allow you to create access tokens, based on
//! service account keys and can be used with the Google Cloud client
//! libraries for Rust.
//!
//! While the Google Cloud client libraries for Rust automatically use the types
//! in this module when ADC finds a service account key file, you may want to
//! use these types directly when the service account key is obtained from
//! Cloud Secret Manager or a similar service.
//!
//! Example usage:
//!
//! ```
//! # use google_cloud_auth::credentials::service_account::Builder;
//! # use google_cloud_auth::credentials::Credentials;
//! # use google_cloud_auth::errors::CredentialsError;
//! # tokio_test::block_on(async {
//! let service_account_key = serde_json::json!({
//! "client_email": "test-client-email",
//! "private_key_id": "test-private-key-id",
//! "private_key": "<YOUR_PKCS8_PEM_KEY_HERE>",
//! "project_id": "test-project-id",
//! "universe_domain": "test-universe-domain",
//! });
//! let credentials: Credentials = Builder::new(service_account_key).with_quota_project_id("my-quota-project").build()?;
//! let token = credentials.get_token().await?;
//! println!("Token: {}", token.token);
//! # Ok::<(), CredentialsError>(())
//! # });
//! ```
//!
//! [Best practices for using service accounts]: https://cloud.google.com/iam/docs/best-practices-service-accounts#choose-when-to-use
//! [create a service account key]: https://cloud.google.com/iam/docs/keys-create-delete#creating
//! [Service Account]: https://cloud.google.com/iam/docs/service-account-overview
//! [service account key]: https://cloud.google.com/iam/docs/keys-create-delete#creating

mod jws;

use crate::credentials::QUOTA_PROJECT_KEY;
use crate::credentials::dynamic::CredentialsTrait;
use crate::credentials::{Credentials, Result};
use crate::errors::{self, CredentialsError};
use crate::token::{Token, TokenProvider};
use crate::token_cache::TokenCache;
use async_trait::async_trait;
use http::header::{AUTHORIZATION, HeaderName, HeaderValue};
use jws::{CLOCK_SKEW_FUDGE, DEFAULT_TOKEN_TIMEOUT, JwsClaims, JwsHeader};
use rustls::crypto::CryptoProvider;
use rustls::sign::Signer;
use rustls_pemfile::Item;
use serde_json::Value;
use std::sync::Arc;
use time::OffsetDateTime;

const DEFAULT_SCOPE: &str = "https://www.googleapis.com/auth/cloud-platform";

pub(crate) fn creds_from(js: Value) -> Result<Credentials> {
    Builder::new(js).build()
}

#[derive(Debug)]
enum ServiceAccountRestrictions {
    Audience(String),
    Scopes(Vec<String>),
}

impl ServiceAccountRestrictions {
    fn get_audience(&self) -> Option<&String> {
        match self {
            ServiceAccountRestrictions::Audience(aud) => Some(aud),
            ServiceAccountRestrictions::Scopes(_) => None,
        }
    }

    fn get_scopes(&self) -> Option<&[String]> {
        match self {
            ServiceAccountRestrictions::Scopes(scopes) => Some(scopes),
            ServiceAccountRestrictions::Audience(_) => None,
        }
    }
}

/// A builder for constructing service account [Credentials] instances.
///
/// # Example
/// ```
/// # use google_cloud_auth::credentials::service_account::Builder;
/// # tokio_test::block_on(async {
/// let key = serde_json::json!({
/// "client_email": "test-client-email",
/// "private_key_id": "test-private-key-id",
/// "private_key": "<YOUR_PKCS8_PEM_KEY_HERE>",
/// "project_id": "test-project-id",
/// "universe_domain": "test-universe-domain",
/// });
/// let credentials = Builder::new(key).with_aud("https://pubsub.googleapis.com").build();
/// })
/// ```
pub struct Builder {
    service_account_key: Value,
    restrictions: ServiceAccountRestrictions,
    quota_project_id: Option<String>,
}

impl Builder {
    /// Creates a new builder using [service_account_key] JSON value.
    /// By default, the builder is configured with [cloud-platform] scope.
    /// This can be overridden using [with_aud][Builder::with_aud]
    /// or [with_scopes][Builder::with_scopes] methods.
    ///
    /// [cloud-platform]:https://cloud.google.com/compute/docs/access/service-accounts#scopes_best_practice
    /// [service_account_key]: https://cloud.google.com/iam/docs/keys-create-delete#creating
    pub fn new(service_account_key: Value) -> Self {
        Self {
            service_account_key,
            restrictions: ServiceAccountRestrictions::Scopes(
                [DEFAULT_SCOPE].map(str::to_string).to_vec(),
            ),
            quota_project_id: None,
        }
    }

    /// Sets the audience for this credentials.
    ///
    /// `aud` is a [JWT] claim specifying intended recipient(s) of the token,
    /// that is, a service(s).
    /// Only one of audience or scopes can be specified for a credentials.
    /// Setting the audience will replace any previously configured scopes.
    /// The value should be `https://{SERVICE}/`, e.g., `https://pubsub.googleapis.com/`
    ///
    /// # Example
    /// ```
    /// # use google_cloud_auth::credentials::service_account::Builder;
    /// let service_account_key = serde_json::json!("{ /* add details here */ }");
    /// let credentials = Builder::new(service_account_key).with_aud("https://bigtable.googleapis.com/").build();
    /// ```
    ///
    /// [JWT]: https://google.aip.dev/auth/4111
    pub fn with_aud<V: Into<String>>(mut self, v: V) -> Self {
        self.restrictions = ServiceAccountRestrictions::Audience(v.into());
        self
    }

    /// Sets the [scopes] for this credentials.
    ///
    /// `scopes` is a [JWT] claim specifying requested permission(s) for the token.
    /// Only one of audience or scopes can be specified for a credentials.
    /// Setting the scopes will replace any previously configured audience.
    ///
    /// `scopes` define the *permissions being requested* for this specific session
    /// when interacting with a service. For example, `https://www.googleapis.com/auth/devstorage.read_write`.
    /// IAM permissions, on the other hand, define the *underlying capabilities*
    /// the service account possesses within a system. For example, `storage.buckets.delete`.
    /// When a token generated with specific scopes is used, the request must be permitted
    /// by both the service account's underlying IAM permissions and the scopes requested
    /// for the token. Therefore, scopes act as an additional restriction on what the token
    /// can be used for. Please see relevant section in [service account authorization] to learn
    /// more about scopes and IAM permissions.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_auth::credentials::service_account::Builder;
    /// let service_account_key = serde_json::json!("{ /* add details here */ }");
    /// let credentials = Builder::new(service_account_key).with_scopes(vec!["https://www.googleapis.com/auth/pubsub"]).build();
    /// ```
    ///
    /// [JWT]: https://google.aip.dev/auth/4111
    /// [scopes]: https://developers.google.com/identity/protocols/oauth2/scopes
    /// [service account authorization]: https://cloud.google.com/compute/docs/access/service-accounts#authorization
    pub fn with_scopes<I, S>(mut self, scopes: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.restrictions =
            ServiceAccountRestrictions::Scopes(scopes.into_iter().map(|s| s.into()).collect());
        self
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

    /// Returns a [Credentials] instance with the configured settings.
    ///
    /// # Errors
    ///
    /// Returns a [CredentialsError] if the `service_account_key`
    /// provided to [`Builder::new`] cannot be successfully deserialized into the
    /// expected format for a service account key. This typically happens if the
    /// JSON value is malformed or missing required fields. For more information,
    /// on the expected format for a service account key, consult the
    /// relevant section in the [service account keys] guide.
    ///
    /// [creating service account keys]: https://cloud.google.com/iam/docs/keys-create-delete#creating
    pub fn build(self) -> Result<Credentials> {
        let service_account_key =
            serde_json::from_value::<ServiceAccountKey>(self.service_account_key)
                .map_err(errors::non_retryable)?;
        let token_provider = ServiceAccountTokenProvider {
            service_account_key,
            restrictions: self.restrictions,
        };
        let token_provider = TokenCache::new(token_provider);

        Ok(Credentials {
            inner: Arc::new(ServiceAccountCredentials {
                token_provider,
                quota_project_id: self.quota_project_id,
            }),
        })
    }
}

/// A representation of a [service account key].
///
/// [Service Account Key]: https://cloud.google.com/iam/docs/keys-create-delete#creating
#[derive(serde::Deserialize, Default, Clone)]
struct ServiceAccountKey {
    /// The client email address of the service account.
    /// (e.g., "my-sa@my-project.iam.gserviceaccount.com").
    client_email: String,
    /// ID of the service account's private key.
    private_key_id: String,
    /// The PEM-encoded PKCS#8 private key string associated with the service account.
    /// Begins with `-----BEGIN PRIVATE KEY-----`.
    private_key: String,
    /// The project id the service account belongs to.
    project_id: String,
    /// The universe domain this service account belongs to.
    universe_domain: Option<String>,
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
struct ServiceAccountCredentials<T>
where
    T: TokenProvider,
{
    token_provider: T,
    quota_project_id: Option<String>,
}

#[derive(Debug)]
struct ServiceAccountTokenProvider {
    service_account_key: ServiceAccountKey,
    restrictions: ServiceAccountRestrictions,
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

        let claims = JwsClaims {
            iss: self.service_account_key.client_email.clone(),
            scope: self
                .restrictions
                .get_scopes()
                .map(|scopes| scopes.join(" ")),
            aud: self.restrictions.get_audience().cloned(),
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
            .map_err(errors::non_retryable)?;
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
            .map_err(errors::non_retryable)?
            .ok_or_else(|| {
                errors::non_retryable_from_str("missing PEM section in service account key")
            })?;
        let pk = match private_key {
            Item::Pkcs8Key(item) => key_provider.load_private_key(item.into()),
            other => {
                return Err(Self::unexpected_private_key_error(other));
            }
        };
        let sk = pk.map_err(errors::non_retryable)?;
        sk.choose_scheme(&[rustls::SignatureScheme::RSA_PKCS1_SHA256])
            .ok_or_else(|| errors::non_retryable_from_str("Unable to choose RSA_PKCS1_SHA256 signing scheme as it is not supported by current signer"))
    }

    fn unexpected_private_key_error(private_key_format: Item) -> CredentialsError {
        errors::non_retryable_from_str(format!(
            "expected key to be in form of PKCS8, found {:?}",
            private_key_format
        ))
    }
}

#[async_trait::async_trait]
impl<T> CredentialsTrait for ServiceAccountCredentials<T>
where
    T: TokenProvider,
{
    async fn get_token(&self) -> Result<Token> {
        self.token_provider.get_token().await
    }

    async fn get_headers(&self) -> Result<Vec<(HeaderName, HeaderValue)>> {
        //TODO(#1686) Refactor the common logic out of the individual get_headers methods.
        let token = self.get_token().await?;
        let mut value = HeaderValue::from_str(&format!("{} {}", token.token_type, token.token))
            .map_err(errors::non_retryable)?;
        value.set_sensitive(true);
        let mut headers = vec![(AUTHORIZATION, value)];
        if let Some(project) = &self.quota_project_id {
            headers.push((
                HeaderName::from_static(QUOTA_PROJECT_KEY),
                HeaderValue::from_str(project).map_err(errors::non_retryable)?,
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
    use rsa::pkcs8::{EncodePrivateKey, LineEnding};
    use rustls_pemfile::Item;
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
            universe_domain: Some("test-universe-domain".to_string()),
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

        let sac = ServiceAccountCredentials {
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
            .return_once(|| Err(errors::non_retryable_from_str("fail")));

        let sac = ServiceAccountCredentials {
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

        let sac = ServiceAccountCredentials {
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

        let sac = ServiceAccountCredentials {
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
            .return_once(|| Err(errors::non_retryable_from_str("fail")));

        let sac = ServiceAccountCredentials {
            token_provider: mock,
            quota_project_id: None,
        };
        assert!(sac.get_headers().await.is_err());
    }

    fn get_mock_service_key() -> Value {
        json!({
            "client_email": "test-client-email",
            "private_key_id": "test-private-key-id",
            "private_key": "",
            "project_id": "test-project-id",
        })
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
        service_account_key["private_key"] = Value::from(generate_pkcs1_private_key());
        let cred = Builder::new(service_account_key).build()?;
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
        service_account_key["private_key"] = Value::from(generate_pkcs8_private_key());
        let cred = Builder::new(service_account_key.clone()).build()?;
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
        assert_eq!(header["kid"], service_account_key["private_key_id"]);

        let claims = b64_decode_to_json(captures["claims"].to_string());
        assert_eq!(claims["iss"], service_account_key["client_email"]);
        assert_eq!(claims["scope"], DEFAULT_SCOPE);
        assert!(claims["iat"].is_number());
        assert!(claims["exp"].is_number());
        assert_eq!(claims["sub"], service_account_key["client_email"]);

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
        service_account_key["private_key"] = Value::from(pem_data);
        let cred = Builder::new(service_account_key).build()?;

        let token = cred.get_token().await;
        let expected_error_message = "failed to parse private key";
        assert!(token.is_err_and(|e| e.to_string().contains(expected_error_message)));
        Ok(())
    }

    #[tokio::test]
    async fn get_service_account_token_invalid_json_failure() -> TestResult {
        let service_account_key = Value::from(" ");
        let e = Builder::new(service_account_key).build().err().unwrap();

        assert!(!e.is_retryable());

        Ok(())
    }

    #[test]
    fn signer_failure() -> TestResult {
        let tp = ServiceAccountTokenProvider {
            service_account_key:
                serde_json::from_value::<ServiceAccountKey>(get_mock_service_key()).unwrap(),
            restrictions: ServiceAccountRestrictions::Scopes(vec![]),
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
        service_account_key["private_key"] = Value::from(generate_pkcs8_private_key());
        let token = Builder::new(service_account_key.clone())
            .with_aud("test-audience")
            .build()?
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
        assert_eq!(header["kid"], service_account_key["private_key_id"]);

        let claims = b64_decode_to_json(captures["claims"].to_string());
        assert_eq!(claims["iss"], service_account_key["client_email"]);
        assert_eq!(claims["scope"], Value::Null);
        assert_eq!(claims["aud"], "test-audience");
        assert!(claims["iat"].is_number());
        assert!(claims["exp"].is_number());
        assert_eq!(claims["sub"], service_account_key["client_email"]);
        Ok(())
    }

    #[tokio::test]
    async fn get_service_account_token_with_custom_scopes() -> TestResult {
        let mut service_account_key = get_mock_service_key();
        let scopes = vec![
            "https://www.googleapis.com/auth/pubsub, https://www.googleapis.com/auth/translate",
        ];
        service_account_key["private_key"] = Value::from(generate_pkcs8_private_key());
        let token = Builder::new(service_account_key.clone())
            .with_scopes(scopes.clone())
            .build()?
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
        assert_eq!(header["kid"], service_account_key["private_key_id"]);

        let claims = b64_decode_to_json(captures["claims"].to_string());
        assert_eq!(claims["iss"], service_account_key["client_email"]);
        assert_eq!(claims["scope"], scopes.join(" "));
        assert_eq!(claims["aud"], Value::Null);
        assert!(claims["iat"].is_number());
        assert!(claims["exp"].is_number());
        assert_eq!(claims["sub"], service_account_key["client_email"]);
        Ok(())
    }
}
