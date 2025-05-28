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
//! # Example
//! ```
//! # use google_cloud_auth::credentials::service_account::Builder;
//! # use google_cloud_auth::credentials::Credentials;
//! # use http::Extensions;
//! # tokio_test::block_on(async {
//! let service_account_key = serde_json::json!({
//!     "client_email": "test-client-email",
//!     "private_key_id": "test-private-key-id",
//!     "private_key": "<YOUR_PKCS8_PEM_KEY_HERE>",
//!     "project_id": "test-project-id",
//!     "universe_domain": "test-universe-domain",
//! });
//! let credentials: Credentials = Builder::new(service_account_key)
//!     .with_quota_project_id("my-quota-project")
//!     .build()?;
//! let headers = credentials.headers(Extensions::new()).await?;
//! println!("Headers: {headers:?}");
//! # Ok::<(), anyhow::Error>(())
//! # });
//! ```
//!
//! [Best practices for using service accounts]: https://cloud.google.com/iam/docs/best-practices-service-accounts#choose-when-to-use
//! [create a service account key]: https://cloud.google.com/iam/docs/keys-create-delete#creating
//! [Service Account]: https://cloud.google.com/iam/docs/service-account-overview
//! [service account key]: https://cloud.google.com/iam/docs/keys-create-delete#creating

mod jws;

use crate::build_errors::Error as BuilderError;
use crate::credentials::dynamic::CredentialsProvider;
use crate::credentials::{CacheableResource, Credentials};
use crate::errors::{self, CredentialsError};
use crate::headers_util::build_cacheable_headers;
use crate::token::{CachedTokenProvider, Token, TokenProvider};
use crate::token_cache::TokenCache;
use crate::{BuildResult, Result};
use async_trait::async_trait;
use http::{Extensions, HeaderMap};
use jws::{CLOCK_SKEW_FUDGE, DEFAULT_TOKEN_TIMEOUT, JwsClaims, JwsHeader};
use rustls::crypto::CryptoProvider;
use rustls::sign::Signer;
use rustls_pemfile::Item;
use serde_json::Value;
use std::sync::Arc;
use time::OffsetDateTime;
use tokio::time::Instant;

const DEFAULT_SCOPE: &str = "https://www.googleapis.com/auth/cloud-platform";

/// Represents the access specifier for a service account based token,
/// specifying either OAuth 2.0 [scopes] or a [JWT] audience.
///
/// It ensures that only one of these access specifiers can be applied
/// for a given credential setup.
///
/// [JWT]: https://google.aip.dev/auth/4111
/// [scopes]: https://developers.google.com/identity/protocols/oauth2/scopes
#[derive(Clone, Debug, PartialEq)]
pub enum AccessSpecifier {
    /// Use [AccessSpecifier::Audience] for setting audience in the token.
    /// `aud` is a [JWT] claim specifying intended recipient of the token,
    /// that is, a service.
    /// Only one of audience or scopes can be specified for a credentials.
    ///
    /// [JWT]: https://google.aip.dev/auth/4111
    Audience(String),

    /// Use [AccessSpecifier::Scopes] for setting [scopes] in the token.
    ///
    /// `scopes` is a [JWT] claim specifying requested permission(s) for the token.
    /// Only one of audience or scopes can be specified for a credentials.
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
    /// [JWT]: https://google.aip.dev/auth/4111
    /// [service account authorization]: https://cloud.google.com/compute/docs/access/service-accounts#authorization
    /// [scopes]: https://developers.google.com/identity/protocols/oauth2/scopes
    Scopes(Vec<String>),
}

impl AccessSpecifier {
    fn audience(&self) -> Option<&String> {
        match self {
            AccessSpecifier::Audience(aud) => Some(aud),
            AccessSpecifier::Scopes(_) => None,
        }
    }

    fn scopes(&self) -> Option<&[String]> {
        match self {
            AccessSpecifier::Scopes(scopes) => Some(scopes),
            AccessSpecifier::Audience(_) => None,
        }
    }

    /// Creates [AccessSpecifier] with [scopes].
    ///
    /// # Example
    /// ```
    /// # use google_cloud_auth::credentials::service_account::{AccessSpecifier, Builder};
    /// let access_specifier = AccessSpecifier::from_scopes(["https://www.googleapis.com/auth/pubsub"]);
    /// let service_account_key = serde_json::json!({ /* add details here */ });
    /// let credentials = Builder::new(service_account_key)
    ///     .with_access_specifier(access_specifier)
    ///     .build();
    /// ```
    ///
    /// [scopes]: https://developers.google.com/identity/protocols/oauth2/scopes
    pub fn from_scopes<I, S>(scopes: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        AccessSpecifier::Scopes(scopes.into_iter().map(|s| s.into()).collect())
    }

    /// Creates [AccessSpecifier] with an audience.
    ///
    /// The value should be `https://{SERVICE}/`, e.g., `https://pubsub.googleapis.com/`
    ///
    /// # Example
    /// ```
    /// # use google_cloud_auth::credentials::service_account::{AccessSpecifier, Builder};
    /// let access_specifier = AccessSpecifier::from_audience("https://bigtable.googleapis.com/");
    /// let service_account_key = serde_json::json!({ /* add details here */ });
    /// let credentials = Builder::new(service_account_key)
    ///     .with_access_specifier(access_specifier)
    ///     .build();
    /// ```
    pub fn from_audience<S: Into<String>>(audience: S) -> Self {
        AccessSpecifier::Audience(audience.into())
    }
}

/// A builder for constructing service account [Credentials] instances.
///
/// # Example
/// ```
/// # use google_cloud_auth::credentials::service_account::{AccessSpecifier, Builder};
/// # tokio_test::block_on(async {
/// let key = serde_json::json!({
///     "client_email": "test-client-email",
///     "private_key_id": "test-private-key-id",
///     "private_key": "<YOUR_PKCS8_PEM_KEY_HERE>",
///     "project_id": "test-project-id",
///     "universe_domain": "test-universe-domain",
/// });
/// let credentials = Builder::new(key)
///     .with_access_specifier(AccessSpecifier::from_audience("https://pubsub.googleapis.com"))
///     .build();
/// })
/// ```
pub struct Builder {
    service_account_key: Value,
    access_specifier: AccessSpecifier,
    quota_project_id: Option<String>,
}

impl Builder {
    /// Creates a new builder using [service_account_key] JSON value.
    /// By default, the builder is configured with [cloud-platform] scope.
    /// This can be overridden using the [with_access_specifier][Builder::with_access_specifier] method.
    ///
    /// [cloud-platform]:https://cloud.google.com/compute/docs/access/service-accounts#scopes_best_practice
    /// [service_account_key]: https://cloud.google.com/iam/docs/keys-create-delete#creating
    pub fn new(service_account_key: Value) -> Self {
        Self {
            service_account_key,
            access_specifier: AccessSpecifier::Scopes([DEFAULT_SCOPE].map(str::to_string).to_vec()),
            quota_project_id: None,
        }
    }

    /// Sets the [AccessSpecifier] representing either scopes or audience for this credentials.
    ///
    /// # Example for setting audience
    /// ```
    /// # use google_cloud_auth::credentials::service_account::{AccessSpecifier, Builder};
    /// let access_specifier = AccessSpecifier::from_audience("https://bigtable.googleapis.com/");
    /// let service_account_key = serde_json::json!({ /* add details here */ });
    /// let credentials = Builder::new(service_account_key)
    ///     .with_access_specifier(access_specifier)
    ///     .build();
    /// ```
    ///
    /// # Example for setting scopes
    /// ```
    /// # use google_cloud_auth::credentials::service_account::{AccessSpecifier, Builder};
    /// let access_specifier = AccessSpecifier::from_scopes(["https://www.googleapis.com/auth/pubsub"]);
    /// let service_account_key = serde_json::json!({ /* add details here */ });
    /// let credentials = Builder::new(service_account_key)
    ///     .with_access_specifier(access_specifier)
    ///     .build();
    /// ```
    pub fn with_access_specifier(mut self, access_specifier: AccessSpecifier) -> Self {
        self.access_specifier = access_specifier;
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

    fn build_token_provider(self) -> BuildResult<ServiceAccountTokenProvider> {
        let service_account_key =
            serde_json::from_value::<ServiceAccountKey>(self.service_account_key)
                .map_err(BuilderError::parsing)?;

        Ok(ServiceAccountTokenProvider {
            service_account_key,
            access_specifier: self.access_specifier,
        })
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
    pub fn build(self) -> BuildResult<Credentials> {
        Ok(Credentials {
            inner: Arc::new(ServiceAccountCredentials {
                quota_project_id: self.quota_project_id.clone(),
                token_provider: TokenCache::new(self.build_token_provider()?),
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
    T: CachedTokenProvider,
{
    token_provider: T,
    quota_project_id: Option<String>,
}

#[derive(Debug)]
struct ServiceAccountTokenProvider {
    service_account_key: ServiceAccountKey,
    access_specifier: AccessSpecifier,
}

fn token_issue_time(current_time: OffsetDateTime) -> OffsetDateTime {
    current_time - CLOCK_SKEW_FUDGE
}

fn token_expiry_time(current_time: OffsetDateTime) -> OffsetDateTime {
    current_time + CLOCK_SKEW_FUDGE + DEFAULT_TOKEN_TIMEOUT
}

#[async_trait]
impl TokenProvider for ServiceAccountTokenProvider {
    async fn token(&self) -> Result<Token> {
        let signer = self.signer(&self.service_account_key.private_key)?;

        let expires_at = Instant::now() + CLOCK_SKEW_FUDGE + DEFAULT_TOKEN_TIMEOUT;
        // The claims encode a unix timestamp. `std::time::Instant` has no
        // epoch, so we use `time::OffsetDateTime`, which reads system time, in
        // the implementation.
        let current_time = OffsetDateTime::now_utc();

        let claims = JwsClaims {
            iss: self.service_account_key.client_email.clone(),
            scope: self
                .access_specifier
                .scopes()
                .map(|scopes| scopes.join(" ")),
            aud: self.access_specifier.audience().cloned(),
            exp: token_expiry_time(current_time),
            iat: token_issue_time(current_time),
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
impl<T> CredentialsProvider for ServiceAccountCredentials<T>
where
    T: CachedTokenProvider,
{
    async fn headers(&self, extensions: Extensions) -> Result<CacheableResource<HeaderMap>> {
        let token = self.token_provider.token(extensions).await?;
        build_cacheable_headers(&token, &self.quota_project_id)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::credentials::QUOTA_PROJECT_KEY;
    use crate::credentials::test::{
        PKCS8_PK, b64_decode_to_json, get_headers_from_cache, get_token_from_headers,
    };
    use crate::token::test::MockTokenProvider;
    use http::HeaderValue;
    use http::header::AUTHORIZATION;
    use rsa::pkcs1::EncodeRsaPrivateKey;
    use rsa::pkcs8::LineEnding;
    use rustls_pemfile::Item;
    use serde_json::json;
    use std::error::Error as _;
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

    #[test]
    fn validate_token_issue_time() {
        let current_time = OffsetDateTime::now_utc();
        let token_issue_time = token_issue_time(current_time);
        assert!(token_issue_time == current_time - CLOCK_SKEW_FUDGE);
    }

    #[test]
    fn validate_token_expiry_time() {
        let current_time = OffsetDateTime::now_utc();
        let token_issue_time = token_expiry_time(current_time);
        assert!(token_issue_time == current_time + CLOCK_SKEW_FUDGE + DEFAULT_TOKEN_TIMEOUT);
    }

    #[tokio::test]
    async fn headers_success_without_quota_project() -> TestResult {
        let token = Token {
            token: "test-token".to_string(),
            token_type: "Bearer".to_string(),
            expires_at: None,
            metadata: None,
        };

        let mut mock = MockTokenProvider::new();
        mock.expect_token().times(1).return_once(|| Ok(token));

        let sac = ServiceAccountCredentials {
            token_provider: TokenCache::new(mock),
            quota_project_id: None,
        };

        let mut extensions = Extensions::new();
        let cached_headers = sac.headers(extensions.clone()).await.unwrap();
        let (headers, entity_tag) = match cached_headers {
            CacheableResource::New { entity_tag, data } => (data, entity_tag),
            CacheableResource::NotModified => unreachable!("expecting new headers"),
        };
        let token = headers.get(AUTHORIZATION).unwrap();

        assert_eq!(headers.len(), 1, "{headers:?}");
        assert_eq!(token, HeaderValue::from_static("Bearer test-token"));
        assert!(token.is_sensitive());

        extensions.insert(entity_tag);

        let cached_headers = sac.headers(extensions).await?;

        match cached_headers {
            CacheableResource::New { .. } => unreachable!("expecting new headers"),
            CacheableResource::NotModified => CacheableResource::<HeaderMap>::NotModified,
        };
        Ok(())
    }

    #[tokio::test]
    async fn headers_success_with_quota_project() -> TestResult {
        let token = Token {
            token: "test-token".to_string(),
            token_type: "Bearer".to_string(),
            expires_at: None,
            metadata: None,
        };

        let quota_project = "test-quota-project";

        let mut mock = MockTokenProvider::new();
        mock.expect_token().times(1).return_once(|| Ok(token));

        let sac = ServiceAccountCredentials {
            token_provider: TokenCache::new(mock),
            quota_project_id: Some(quota_project.to_string()),
        };

        let headers = get_headers_from_cache(sac.headers(Extensions::new()).await.unwrap())?;
        let token = headers.get(AUTHORIZATION).unwrap();
        let quota_project_header = headers.get(QUOTA_PROJECT_KEY).unwrap();

        assert_eq!(headers.len(), 2, "{headers:?}");
        assert_eq!(token, HeaderValue::from_static("Bearer test-token"));
        assert!(token.is_sensitive());
        assert_eq!(
            quota_project_header,
            HeaderValue::from_static(quota_project)
        );
        assert!(!quota_project_header.is_sensitive());
        Ok(())
    }

    #[tokio::test]
    async fn headers_failure() {
        let mut mock = MockTokenProvider::new();
        mock.expect_token()
            .times(1)
            .return_once(|| Err(errors::non_retryable_from_str("fail")));

        let sac = ServiceAccountCredentials {
            token_provider: TokenCache::new(mock),
            quota_project_id: None,
        };
        assert!(sac.headers(Extensions::new()).await.is_err());
    }

    fn get_mock_service_key() -> Value {
        json!({
            "client_email": "test-client-email",
            "private_key_id": "test-private-key-id",
            "private_key": "",
            "project_id": "test-project-id",
        })
    }

    #[tokio::test]
    async fn get_service_account_headers_pkcs1_private_key_failure() -> TestResult {
        let mut service_account_key = get_mock_service_key();

        let key = crate::credentials::test::RSA_PRIVATE_KEY
            .to_pkcs1_pem(LineEnding::LF)
            .expect("Failed to encode key to PKCS#1 PEM")
            .to_string();

        service_account_key["private_key"] = Value::from(key);
        let cred = Builder::new(service_account_key).build()?;
        let expected_error_message = "expected key to be in form of PKCS8, found Pkcs1Key";
        assert!(
            cred.headers(Extensions::new())
                .await
                .is_err_and(|e| e.to_string().contains(expected_error_message))
        );
        Ok(())
    }

    #[tokio::test]
    async fn get_service_account_token_pkcs8_key_success() -> TestResult {
        let mut service_account_key = get_mock_service_key();
        service_account_key["private_key"] = Value::from(PKCS8_PK.clone());
        let tp = Builder::new(service_account_key.clone()).build_token_provider()?;

        let token = tp.token().await?;
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
    async fn header_caching() -> TestResult {
        let private_key = PKCS8_PK.clone();

        let json_value = json!({
            "client_email": "test-client-email",
            "private_key_id": "test-private-key-id",
            "private_key": private_key,
            "project_id": "test-project-id",
            "universe_domain": "test-universe-domain"
        });

        let credentials = Builder::new(json_value).build()?;

        let headers = credentials.headers(Extensions::new()).await?;

        let re = regex::Regex::new(SSJ_REGEX).unwrap();
        let token = get_token_from_headers(headers).unwrap();

        let captures = re.captures(&token).unwrap();

        let claims = b64_decode_to_json(captures["claims"].to_string());
        let first_iat = claims["iat"].as_i64().unwrap();

        // The issued at claim (`iat`) encodes a unix timestamp, in seconds.
        // Sleeping for one second ensures that a subsequent claim has a
        // different `iat`. We need a real sleep, because we cannot fake the
        // current unix timestamp.
        std::thread::sleep(Duration::from_secs(1));

        // Get the token again.
        let token = get_token_from_headers(credentials.headers(Extensions::new()).await?).unwrap();
        let captures = re.captures(&token).unwrap();

        let claims = b64_decode_to_json(captures["claims"].to_string());
        let second_iat = claims["iat"].as_i64().unwrap();

        // Validate that the issued at claim is the same for the two tokens. If
        // the 2nd token is not from the cache, its `iat` will be different.
        assert_eq!(first_iat, second_iat);

        Ok(())
    }

    #[tokio::test]
    async fn get_service_account_headers_invalid_key_failure() -> TestResult {
        let mut service_account_key = get_mock_service_key();
        let pem_data = "-----BEGIN PRIVATE KEY-----\nMIGkAg==\n-----END PRIVATE KEY-----";
        service_account_key["private_key"] = Value::from(pem_data);
        let cred = Builder::new(service_account_key).build()?;

        let token = cred.headers(Extensions::new()).await;
        let err = token.unwrap_err();
        assert!(!err.is_transient(), "{err:?}");
        let source = err.source().and_then(|e| e.downcast_ref::<rustls::Error>());
        assert!(matches!(source, Some(rustls::Error::General(_))), "{err:?}");
        Ok(())
    }

    #[tokio::test]
    async fn get_service_account_invalid_json_failure() -> TestResult {
        let service_account_key = Value::from(" ");
        let e = Builder::new(service_account_key).build().unwrap_err();
        assert!(e.is_parsing(), "{e:?}");

        Ok(())
    }

    #[test]
    fn signer_failure() -> TestResult {
        let tp = Builder::new(get_mock_service_key()).build_token_provider()?;

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
    async fn get_service_account_headers_with_audience() -> TestResult {
        let mut service_account_key = get_mock_service_key();
        service_account_key["private_key"] = Value::from(PKCS8_PK.clone());
        let headers = Builder::new(service_account_key.clone())
            .with_access_specifier(AccessSpecifier::from_audience("test-audience"))
            .build()?
            .headers(Extensions::new())
            .await?;

        let re = regex::Regex::new(SSJ_REGEX).unwrap();
        let token = get_token_from_headers(headers).unwrap();
        let captures = re.captures(&token).ok_or_else(|| {
            format!(
                r#"Expected token in form: "<header>.<claims>.<sig>". Found token: {}"#,
                token
            )
        })?;
        let token_header = b64_decode_to_json(captures["header"].to_string());
        assert_eq!(token_header["alg"], "RS256");
        assert_eq!(token_header["typ"], "JWT");
        assert_eq!(token_header["kid"], service_account_key["private_key_id"]);

        let claims = b64_decode_to_json(captures["claims"].to_string());
        assert_eq!(claims["iss"], service_account_key["client_email"]);
        assert_eq!(claims["scope"], Value::Null);
        assert_eq!(claims["aud"], "test-audience");
        assert!(claims["iat"].is_number());
        assert!(claims["exp"].is_number());
        assert_eq!(claims["sub"], service_account_key["client_email"]);
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn get_service_account_token_verify_expiry_time() -> TestResult {
        let now = Instant::now();
        let mut service_account_key = get_mock_service_key();
        service_account_key["private_key"] = Value::from(PKCS8_PK.clone());
        let token = Builder::new(service_account_key)
            .build_token_provider()?
            .token()
            .await?;

        let expected_expiry = now + CLOCK_SKEW_FUDGE + DEFAULT_TOKEN_TIMEOUT;

        assert_eq!(token.expires_at.unwrap(), expected_expiry);
        Ok(())
    }

    #[tokio::test]
    async fn get_service_account_headers_with_custom_scopes() -> TestResult {
        let mut service_account_key = get_mock_service_key();
        let scopes = vec![
            "https://www.googleapis.com/auth/pubsub, https://www.googleapis.com/auth/translate",
        ];
        service_account_key["private_key"] = Value::from(PKCS8_PK.clone());
        let headers = Builder::new(service_account_key.clone())
            .with_access_specifier(AccessSpecifier::from_scopes(scopes.clone()))
            .build()?
            .headers(Extensions::new())
            .await?;

        let re = regex::Regex::new(SSJ_REGEX).unwrap();
        let token = get_token_from_headers(headers).unwrap();
        let captures = re.captures(&token).ok_or_else(|| {
            format!(
                r#"Expected token in form: "<header>.<claims>.<sig>". Found token: {}"#,
                token
            )
        })?;
        let token_header = b64_decode_to_json(captures["header"].to_string());
        assert_eq!(token_header["alg"], "RS256");
        assert_eq!(token_header["typ"], "JWT");
        assert_eq!(token_header["kid"], service_account_key["private_key_id"]);

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
