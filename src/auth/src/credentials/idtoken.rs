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

//! Obtain [OIDC ID Tokens].
//!
//! `IDTokenCredentials` provide a way to obtain OIDC ID tokens, which are
//! commonly used for [service to service authentication], like when services are
//! hosted in Cloud Run or mediated by Identity-Aware Proxy (IAP).
//! Unlike access tokens, ID tokens are not used to authorize access to
//! Google Cloud APIs but to verify the identity of a principal.
//!
//! This module provides `IDTokenCredentials` which serves as a wrapper around
//! different credential types that can produce ID tokens, such as service
//! accounts or metadata server credentials.
//!
//! ## Example: Generating ID Tokens using Application Default Credentials
//!
//! This example shows how to create `IDTokenCredentials` using the
//! Application Default Credentials (ADC) flow. The builder will locate
//! and use the credentials from the environment.
//!
//! ```
//! # use google_cloud_auth::credentials::idtoken;
//! # use reqwest;
//! #
//! # async fn send_id_token() -> anyhow::Result<()> {
//! let audience = "https://my-service.a.run.app";
//! let credentials = idtoken::Builder::new(audience).build()?;
//! let id_token = credentials.id_token().await?;
//!
//! // Make request with ID Token as Bearer Token.
//! let client = reqwest::Client::new();
//! let target_url = format!("{audience}/api/method");
//! client.get(target_url)
//!     .bearer_auth(id_token)
//!     .send()
//!     .await?;
//! # Ok(())
//! # }
//! ```
//!
//! [OIDC ID Tokens]: https://cloud.google.com/docs/authentication/token-types#identity-tokens
//! [Service to Service Authentication]: https://cloud.google.com/run/docs/authenticating/service-to-service

use crate::build_errors::Error as BuilderError;
use crate::credentials::{AdcContents, extract_credential_type, load_adc};
use crate::token::Token;
use crate::{BuildResult, Result};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use gax::error::CredentialsError;
use serde_json::Value;
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time::Instant;

pub mod impersonated;
pub mod mds;

pub mod service_account {
    //! Credentials for authenticating with [ID tokens] using [Service Accounts].
    //!
    //! The types in this module allow you to create id tokens, based on
    //! service account keys and can be used for [service to service authentication].
    //! For example, when services are hosted in Cloud Run or mediated by Identity-Aware Proxy (IAP).
    //! ID tokens are only used to verify the identity of a principal. Google Cloud APIs do not use ID tokens
    //! for authorization, and therefore cannot be used to access Google Cloud APIs.
    //!
    //! While the Google Cloud client libraries for Rust automatically use the types
    //! in this module when ADC finds a service account key file, you may want to
    //! use these types directly when the service account key is obtained from
    //! Cloud Secret Manager or a similar service.
    //!
    //! # Example
    //! ```
    //! # use google_cloud_auth::credentials::idtoken;
    //! # use reqwest;
    //! # tokio_test::block_on(async {
    //! let service_account_key = serde_json::json!({
    //!     "client_email": "test-client-email",
    //!     "private_key_id": "test-private-key-id",
    //!     "private_key": "<YOUR_PKCS8_PEM_KEY_HERE>",
    //!     "project_id": "test-project-id",
    //!     "universe_domain": "test-universe-domain",
    //! });
    //! let audience = "https://example.com";
    //! let credentials: Credentials = idtoken::service_account::Builder::new(audience, service_account_key)
    //!     .build()?;
    //! let id_token = credentials.id_token().await?;
    //!
    //! // Make request with ID Token as Bearer Token.
    //! let client = reqwest::Client::new();
    //! let target_url = format!("{audience}/api/method");
    //! client.get(target_url)
    //!     .bearer_auth(id_token)
    //!     .send()
    //!     .await?;
    //! # Ok::<(), anyhow::Error>(())
    //! # });
    //! ```
    //!
    //! [Best practices for using service accounts]: https://cloud.google.com/iam/docs/best-practices-service-accounts#choose-when-to-use
    //! [ID tokens]: https://cloud.google.com/docs/authentication/token-types#identity-tokens
    //! [create a service account key]: https://cloud.google.com/iam/docs/keys-create-delete#creating
    //! [Service Accounts]: https://cloud.google.com/iam/docs/service-account-overview
    //! [service account key]: https://cloud.google.com/iam/docs/keys-create-delete#creating
    //! [Service to Service Authentication]: https://cloud.google.com/run/docs/authenticating/service-to-service
    pub use crate::credentials::service_account::idtoken::Builder;
}
pub mod user_account {
    //! Credentials for authenticating with [ID tokens] from an [user account].
    //!
    //! This module provides a builder for `IDTokenCredentials` from
    //! authorized user credentials, which are typically obtained by running
    //! `gcloud auth application-default login`.
    //!
    //! These credentials are commonly used for [service to service authentication].
    //! For example, when services are hosted in Cloud Run or mediated by Identity-Aware Proxy (IAP).
    //! ID tokens are only used to verify the identity of a principal. Google Cloud APIs do not use ID tokens
    //! for authorization, and therefore cannot be used to access Google Cloud APIs.
    //!
    //! ## Example: Creating user account sourced credentials from a JSON object with target audience and sending ID Tokens.
    //!
    //! ```
    //! # use google_cloud_auth::credentials::idtoken;
    //! # use reqwest;
    //! # tokio_test::block_on(async {
    //! let authorized_user = serde_json::json!({
    //!     "client_id": "YOUR_CLIENT_ID.apps.googleusercontent.com", // Replace with your actual Client ID
    //!     "client_secret": "YOUR_CLIENT_SECRET", // Replace with your actual Client Secret - LOAD SECURELY!
    //!     "refresh_token": "YOUR_REFRESH_TOKEN", // Replace with the user's refresh token - LOAD SECURELY!
    //!     "type": "authorized_user",
    //! });
    //! let credentials = idtoken::user_account::Builder::new(authorized_user).build()?;
    //! let id_token = credentials.id_token().await?;
    //!
    //! // Make request with ID Token as Bearer Token.
    //! let client = reqwest::Client::new();
    //! let target_url = format!("{audience}/api/method");
    //! client.get(target_url)
    //!     .bearer_auth(id_token)
    //!     .send()
    //!     .await?;
    //! # Ok::<(), anyhow::Error>(())
    //! # });
    //! ```
    //!
    //! [ID tokens]: https://cloud.google.com/docs/authentication/token-types#identity-tokens
    //! [user account]: https://cloud.google.com/docs/authentication#user-accounts
    //! [Service to Service Authentication]: https://cloud.google.com/run/docs/authenticating/service-to-service

    pub use crate::credentials::user_account::idtoken::Builder;
}

/// Obtain [OIDC ID Tokens].
///
/// `IDTokenCredentials` provide a way to obtain OIDC ID tokens, which are
/// commonly used for [service to service authentication], like when services are
/// hosted in Cloud Run or mediated by Identity-Aware Proxy (IAP).
/// Unlike access tokens, ID tokens are not used to authorize access to
/// Google Cloud APIs but to verify the identity of a principal.
///
/// This struct serves as a wrapper around different credential types that can
/// produce ID tokens, such as service accounts or metadata server credentials.
///
/// [OIDC ID Tokens]: https://cloud.google.com/docs/authentication/token-types#identity-tokens
/// [Service to Service Authentication]: https://cloud.google.com/run/docs/authenticating/service-to-service
#[derive(Clone, Debug)]
pub struct IDTokenCredentials {
    pub(crate) inner: Arc<dyn dynamic::IDTokenCredentialsProvider>,
}

impl<T> From<T> for IDTokenCredentials
where
    T: IDTokenCredentialsProvider + Send + Sync + 'static,
{
    fn from(value: T) -> Self {
        Self {
            inner: Arc::new(value),
        }
    }
}

impl IDTokenCredentials {
    /// Asynchronously retrieves an ID token.
    ///
    /// Obtains an ID token. If one is cached, returns the cached value.
    pub async fn id_token(&self) -> Result<String> {
        self.inner.id_token().await
    }
}

/// A trait for credential types that can provide OIDC ID tokens.
///
/// Implement this trait to create custom ID token providers.
/// For example, if you are working with an authentication system not
/// supported by this crate. Or if you are trying to write a test and need
/// to mock the existing `IDTokenCredentialsProvider` implementations.
pub trait IDTokenCredentialsProvider: std::fmt::Debug {
    /// Asynchronously retrieves an ID token.
    fn id_token(&self) -> impl Future<Output = Result<String>> + Send;
}

/// A module containing the dynamically-typed, dyn-compatible version of the
/// `IDTokenCredentialsProvider` trait. This is an internal implementation detail.
pub(crate) mod dynamic {
    use crate::Result;

    /// A dyn-compatible, crate-private version of `IDTokenCredentialsProvider`.
    #[async_trait::async_trait]
    pub trait IDTokenCredentialsProvider: Send + Sync + std::fmt::Debug {
        /// Asynchronously retrieves an ID token.
        async fn id_token(&self) -> Result<String>;
    }

    /// The public `IDTokenCredentialsProvider` implements the dyn-compatible `IDTokenCredentialsProvider`.
    #[async_trait::async_trait]
    impl<T> IDTokenCredentialsProvider for T
    where
        T: super::IDTokenCredentialsProvider + Send + Sync,
    {
        async fn id_token(&self) -> Result<String> {
            T::id_token(self).await
        }
    }
}

/// Creates [`IDTokenCredentials`] instances that
/// fetch ID tokens using the loaded credential.
///
/// This builder loads credentials according to the standard
/// [Application Default Credentials (ADC)][ADC-link] strategy.
/// ADC is the recommended approach for most applications and conforms to
/// [AIP-4110]. If you need to load credentials from a non-standard location
/// or source, you can use the builder for the desired credential type.
///
/// [ADC-link]: https://cloud.google.com/docs/authentication/application-default-credentials
/// [AIP-4110]: https://google.aip.dev/auth/4110
pub struct Builder {
    target_audience: String,
}

impl Builder {
    /// Creates a new builder where id tokens will be obtained via [gcloud auth application-default login].
    ///
    /// The `target_audience` is a required parameter that specifies the
    /// intended audience of the ID token. This is typically the URL of the
    /// service that will be receiving the token.
    ///
    /// [gcloud auth application-default login]: https://cloud.google.com/sdk/gcloud/reference/auth/application-default/login
    pub fn new<S: Into<String>>(target_audience: S) -> Self {
        Self {
            target_audience: target_audience.into(),
        }
    }

    /// Returns a [IDTokenCredentials] instance with the configured settings.
    ///
    /// # Errors
    ///
    /// Returns a [BuilderError] if a unsupported credential type is provided
    /// or if the JSON value is either malformed
    /// or missing required fields. For more information, on how to generate
    /// json, consult the relevant section in the [application-default credentials] guide.
    ///
    /// [application-default credentials]: https://cloud.google.com/docs/authentication/application-default-credentials
    pub fn build(self) -> BuildResult<IDTokenCredentials> {
        let json_data = match load_adc()? {
            AdcContents::Contents(contents) => {
                Some(serde_json::from_str(&contents).map_err(BuilderError::parsing)?)
            }
            AdcContents::FallbackToMds => None,
        };

        build_id_token_credentials(self.target_audience, json_data)
    }
}

fn build_id_token_credentials(
    audience: String,
    json: Option<Value>,
) -> BuildResult<IDTokenCredentials> {
    match json {
        None => {
            // TODO(#3587): pass context that is being built from ADC flow.
            mds::Builder::new(audience).with_format("full").build()
        }
        Some(json) => {
            let cred_type = extract_credential_type(&json)?;
            match cred_type {
                "authorized_user" => Err(BuilderError::not_supported(format!(
                    "{cred_type}, use idtoken::user_account::Builder directly."
                ))),
                "service_account" => service_account::Builder::new(audience, json).build(),
                "impersonated_service_account" => {
                    impersonated::Builder::new(audience, json).build()
                }
                "external_account" => {
                    // never gonna be supported for id tokens
                    Err(BuilderError::not_supported(cred_type))
                }
                _ => Err(BuilderError::unknown_type(cred_type)),
            }
        }
    }
}

/// parse JWT ID Token string as google_cloud_auth::token::Token
pub(crate) fn parse_id_token_from_str(token: String) -> Result<Token> {
    let parts: Vec<&str> = token.split('.').collect();
    if parts.len() != 3 {
        return Err(CredentialsError::from_msg(false, "invalid JWT token"));
    }
    let payload = URL_SAFE_NO_PAD
        .decode(parts[1])
        .map_err(|e| CredentialsError::from_source(false, e))?;

    let claims: HashMap<String, Value> =
        serde_json::from_slice(&payload).map_err(|e| CredentialsError::from_source(false, e))?;

    let expires_at = claims["exp"].as_u64().and_then(instant_from_epoch_seconds);

    Ok(Token {
        token,
        token_type: "Bearer".to_string(),
        expires_at,
        metadata: None,
    })
}

fn instant_from_epoch_seconds(secs: u64) -> Option<Instant> {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(now) => {
            let diff = now.abs_diff(Duration::from_secs(secs));
            Some(Instant::now() + diff)
        }
        Err(_) => None,
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::parse_id_token_from_str;
    use super::*;
    use base64::prelude::BASE64_URL_SAFE_NO_PAD;
    use serial_test::parallel;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    type TestResult = anyhow::Result<()>;

    const DEFAULT_TEST_TOKEN_EXPIRATION: Duration = Duration::from_secs(3600);

    /// Function to be used in tests to generate a fake, but valid enough, id token.
    pub(crate) fn generate_test_id_token<S: Into<String>>(audience: S) -> String {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        let then = now + DEFAULT_TEST_TOKEN_EXPIRATION;
        let claims = serde_json::json!({
            "iss": "test_iss".to_string(),
            "aud": Some(audience.into()),
            "exp": then.as_secs(),
            "iat": now.as_secs(),
        });

        let json = serde_json::to_string(&claims).expect("failed to encode jwt claims");
        let payload = BASE64_URL_SAFE_NO_PAD.encode(json.as_bytes());

        format!("test_header.{}.test_signature", payload)
    }

    #[tokio::test]
    #[parallel]
    async fn test_parse_id_token() -> TestResult {
        let audience = "https://example.com";
        let id_token = generate_test_id_token(audience);

        let token = parse_id_token_from_str(id_token.clone()).expect("should parse id token");

        assert_eq!(token.token, id_token);
        assert!(token.expires_at.is_some());

        let expires_at = token.expires_at.unwrap();
        let now = Instant::now();
        let skew = Duration::from_secs(1);
        let duration = expires_at.duration_since(now);
        assert!(duration > DEFAULT_TEST_TOKEN_EXPIRATION - skew);
        assert!(duration < DEFAULT_TEST_TOKEN_EXPIRATION + skew);

        Ok(())
    }

    #[tokio::test]
    #[parallel]
    async fn test_build_id_token_credentials_authorized_user_not_supported() -> TestResult {
        let audience = "test_audience".to_string();
        let json = serde_json::json!({
            "type": "authorized_user",
            "client_id": "test_client_id",
            "client_secret": "test_client_secret",
            "refresh_token": "test_refresh_token",
        });

        let result = build_id_token_credentials(audience, Some(json));
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.is_not_supported());
        assert!(
            err.to_string()
                .contains("authorized_user, use idtoken::user_account::Builder directly.")
        );
        Ok(())
    }

    #[tokio::test]
    #[parallel]
    async fn test_build_id_token_credentials_external_account_not_supported() -> TestResult {
        let audience = "test_audience".to_string();
        let json = serde_json::json!({
            "type": "external_account",
            "audience": "//iam.googleapis.com/projects/123/locations/global/workloadIdentityPools/my-pool/providers/my-provider",
            "subject_token_type": "urn:ietf:params:oauth:token-type:jwt",
            "token_url": "https://sts.googleapis.com/v1/token",
            "credential_source": {
                "file": "/path/to/file",
                "format": {
           "type": "text"
                }
            }
        });

        let result = build_id_token_credentials(audience, Some(json));
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.is_not_supported());
        assert!(err.to_string().contains("external_account"));
        Ok(())
    }

    #[tokio::test]
    #[parallel]
    async fn test_build_id_token_credentials_unknown_type() -> TestResult {
        let audience = "test_audience".to_string();
        let json = serde_json::json!({
            "type": "unknown_credential_type",
        });

        let result = build_id_token_credentials(audience, Some(json));
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.is_unknown_type());
        assert!(err.to_string().contains("unknown_credential_type"));
        Ok(())
    }
}
