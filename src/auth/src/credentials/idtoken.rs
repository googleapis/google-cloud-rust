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

use crate::Result;
use crate::token::Token;
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use gax::error::CredentialsError;
use serde_json::Value;
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time::Instant;

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
    pub async fn id_token(&self) -> Result<Token> {
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
    fn id_token(&self) -> impl Future<Output = Result<Token>> + Send;
}

/// A module containing the dynamically-typed, dyn-compatible version of the
/// `IDTokenCredentialsProvider` trait. This is an internal implementation detail.
pub(crate) mod dynamic {
    use crate::Result;
    use crate::token::Token;

    /// A dyn-compatible, crate-private version of `IDTokenCredentialsProvider`.
    #[async_trait::async_trait]
    pub trait IDTokenCredentialsProvider: Send + Sync + std::fmt::Debug {
        /// Asynchronously retrieves an ID token.
        async fn id_token(&self) -> Result<Token>;
    }

    /// The public `IDTokenCredentialsProvider` implements the dyn-compatible `IDTokenCredentialsProvider`.
    #[async_trait::async_trait]
    impl<T> IDTokenCredentialsProvider for T
    where
        T: super::IDTokenCredentialsProvider + Send + Sync,
    {
        async fn id_token(&self) -> Result<Token> {
            T::id_token(self).await
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
}
