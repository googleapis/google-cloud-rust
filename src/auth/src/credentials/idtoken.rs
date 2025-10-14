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
use crate::credentials::internal::jwk_client::JwkClient;
use crate::errors::CredentialsError;
use crate::token::Token;
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use jsonwebtoken::Validation;
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

#[derive(Debug)]
pub(crate) struct Verifier {
    jwk_client: JwkClient,
    audience: Option<String>,
    email: Option<String>,
    jwks_url: Option<String>,
    clock_skew: Duration,
}

impl Default for Verifier {
    fn default() -> Self {
        Self {
            jwk_client: JwkClient::new(),
            audience: None,
            email: None,
            jwks_url: None,
            clock_skew: Duration::from_secs(10),
        }
    }
}

impl Verifier {
    pub fn with_audience<S: Into<String>>(mut self, audience: S) -> Self {
        self.audience = Some(audience.into());
        self
    }

    pub fn with_email<S: Into<String>>(mut self, email: S) -> Self {
        self.email = Some(email.into());
        self
    }

    pub fn with_jwks_url<S: Into<String>>(mut self, jwks_url: S) -> Self {
        self.jwks_url = Some(jwks_url.into());
        self
    }

    pub fn with_clock_skew(mut self, clock_skew: Duration) -> Self {
        self.clock_skew = clock_skew;
        self
    }

    pub async fn verify<S: Into<String>>(&self, token: S) -> Result<HashMap<String, Value>> {
        let token = token.into();

        let header = jsonwebtoken::decode_header(token.clone())
            .map_err(|e| CredentialsError::new(false, "failed to decode JWT header", e))?;

        let key_id = header
            .kid
            .ok_or_else(|| CredentialsError::from_msg(false, "JWT token missing `kid` field"))?;

        let mut validation = Validation::new(header.alg);
        validation.leeway = self.clock_skew.as_secs();
        validation.set_issuer(&["https://accounts.google.com", "accounts.google.com"]);
        if let Some(audience) = self.audience.clone() {
            validation.set_audience(&[audience]);
        }
        let expected_email = self.email.clone();
        let jwks_url = self.jwks_url.clone();

        let cert = self
            .jwk_client
            .get_or_load_cert(key_id, header.alg, jwks_url)
            .await?;

        let token = jsonwebtoken::decode::<HashMap<String, Value>>(&token, &cert, &validation)
            .map_err(|e| CredentialsError::new(false, "invalid id token", e))?;

        let claims = token.claims;
        if let Some(email) = expected_email {
            let email_verified = claims["email_verified"].as_bool().unwrap_or(false);
            if !email_verified {
                return Err(CredentialsError::from_msg(false, "email not verified"));
            }
            let token_email = claims["email"]
                .as_str()
                .ok_or_else(|| CredentialsError::from_msg(false, "missing `email` field in JWT"))?;
            if !email.eq(token_email) {
                return Err(CredentialsError::from_msg(false, "invalid email"));
            }
        }

        Ok(claims)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::parse_id_token_from_str;
    use super::*;
    use base64::Engine;
    use httptest::matchers::{all_of, request};
    use httptest::responders::json_encoded;
    use httptest::{Expectation, Server};
    use jsonwebtoken::{Algorithm, EncodingKey, Header};
    use rsa::pkcs1::EncodeRsaPrivateKey;
    use rsa::traits::PublicKeyParts;
    use serial_test::parallel;
    use std::collections::HashMap;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    type TestResult = anyhow::Result<()>;

    const DEFAULT_TEST_TOKEN_EXPIRATION: Duration = Duration::from_secs(3600);
    const TEST_KEY_ID: &str = "test-key-id";

    /// Function to be used in tests to generate a fake, but valid enough, id token.
    pub(crate) fn generate_test_id_token<S: Into<String>>(audience: S) -> String {
        generate_test_id_token_with_claims(audience, HashMap::new())
    }

    fn generate_test_id_token_with_claims<S: Into<String>>(
        audience: S,
        claims_to_add: HashMap<&str, Value>,
    ) -> String {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        let then = now + DEFAULT_TEST_TOKEN_EXPIRATION;

        let mut header = Header::new(Algorithm::RS256);
        header.kid = Some(TEST_KEY_ID.to_string());

        let mut claims: HashMap<&str, Value> = HashMap::new();
        claims.insert("aud", Value::String(audience.into()));
        claims.insert("iss", "accounts.google.com".into());
        claims.insert("exp", then.as_secs().into());
        claims.insert("iat", now.as_secs().into());

        for (k, v) in claims_to_add {
            claims.insert(k, v);
        }

        let private_cert = crate::credentials::tests::RSA_PRIVATE_KEY
            .to_pkcs1_der()
            .expect("Failed to encode private key to PKCS#1 DER");

        let private_key = EncodingKey::from_rsa_der(private_cert.as_bytes());

        jsonwebtoken::encode(&header, &claims, &private_key).expect("failed to encode jwt")
    }

    fn create_jwk_set_response() -> serde_json::Value {
        let pub_cert = crate::credentials::tests::RSA_PRIVATE_KEY.to_public_key();
        serde_json::json!({
            "keys": [
                {
                    "e": base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(pub_cert.e().to_bytes_be()),
                    "kid": TEST_KEY_ID,
                    "use": "sig",
                    "kty": "RSA",
                    "n": base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(pub_cert.n().to_bytes_be()),
                    "alg": "RS256"
                }
            ]
        })
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
    async fn test_verify_success() -> TestResult {
        let server = Server::run();
        let response = create_jwk_set_response();
        server.expect(
            Expectation::matching(all_of![request::path("/certs"),])
                .times(1)
                .respond_with(json_encoded(response.clone())),
        );

        let audience = "https://example.com";
        let token = generate_test_id_token(audience);

        let verifier = Verifier::default()
            .with_jwks_url(format!("http://{}/certs", server.addr()))
            .with_audience(audience);

        let claims = verifier.verify(token.clone()).await?;
        assert!(!claims.is_empty());

        let claims = verifier.verify(token).await?;
        assert!(!claims.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn test_verify_invalid_audience() -> TestResult {
        let server = Server::run();
        server.expect(
            Expectation::matching(all_of![request::path("/certs"),])
                .times(1)
                .respond_with(json_encoded(create_jwk_set_response())),
        );

        let audience = "https://example.com";
        let token = generate_test_id_token(audience);

        let verifier = Verifier::default()
            .with_jwks_url(format!("http://{}/certs", server.addr()))
            .with_audience("https://wrong-audience.com");

        let result = verifier.verify(token).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("invalid id token"));

        Ok(())
    }

    #[tokio::test]
    async fn test_verify_invalid_issuer() -> TestResult {
        let server = Server::run();
        server.expect(
            Expectation::matching(all_of![request::path("/certs"),])
                .times(1)
                .respond_with(json_encoded(create_jwk_set_response())),
        );

        let audience = "https://example.com";
        let mut claims = HashMap::new();
        claims.insert("iss", "https://wrong-issuer.com".into());
        let token = generate_test_id_token_with_claims(audience, claims);

        let verifier = Verifier::default()
            .with_jwks_url(format!("http://{}/certs", server.addr()))
            .with_audience(audience);

        let result = verifier.verify(token).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("invalid id token"));

        Ok(())
    }

    #[tokio::test]
    async fn test_verify_expired_token() -> TestResult {
        let server = Server::run();
        server.expect(
            Expectation::matching(all_of![request::path("/certs"),])
                .times(1)
                .respond_with(json_encoded(create_jwk_set_response())),
        );

        let audience = "https://example.com";
        let mut claims = HashMap::new();
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        claims.insert("exp", (now.as_secs() - 3600).into()); // expired 1 hour ago
        let token = generate_test_id_token_with_claims(audience, claims);

        let verifier = Verifier::default()
            .with_jwks_url(format!("http://{}/certs", server.addr()))
            .with_audience(audience);

        let result = verifier.verify(token).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("invalid id token"));

        Ok(())
    }

    #[tokio::test]
    async fn test_verify_email_not_verified() -> TestResult {
        let server = Server::run();
        server.expect(
            Expectation::matching(all_of![request::path("/certs"),])
                .times(1)
                .respond_with(json_encoded(create_jwk_set_response())),
        );

        let audience = "https://example.com";
        let email = "test@example.com";
        let mut claims = HashMap::new();
        claims.insert("email", email.into());
        claims.insert("email_verified", false.into());
        let token = generate_test_id_token_with_claims(audience, claims);

        let verifier = Verifier::default()
            .with_jwks_url(format!("http://{}/certs", server.addr()))
            .with_audience(audience)
            .with_email(email);

        let result = verifier.verify(token).await;
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("email not verified")
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_verify_email_mismatch() -> TestResult {
        let server = Server::run();
        server.expect(
            Expectation::matching(all_of![request::path("/certs"),])
                .times(1)
                .respond_with(json_encoded(create_jwk_set_response())),
        );

        let audience = "https://example.com";
        let email = "test@example.com";
        let mut claims = HashMap::new();
        claims.insert("email", email.into());
        claims.insert("email_verified", true.into());
        let token = generate_test_id_token_with_claims(audience, claims);

        let verifier = Verifier::default()
            .with_jwks_url(format!("http://{}/certs", server.addr()))
            .with_audience(audience)
            .with_email("wrong@example.com");

        let result = verifier.verify(token).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("invalid email"));

        Ok(())
    }

    #[tokio::test]
    async fn test_verify_email_success() -> TestResult {
        let server = Server::run();
        server.expect(
            Expectation::matching(all_of![request::path("/certs"),])
                .times(1)
                .respond_with(json_encoded(create_jwk_set_response())),
        );

        let audience = "https://example.com";
        let email = "test@example.com";
        let mut claims = HashMap::new();
        claims.insert("email", email.into());
        claims.insert("email_verified", true.into());
        let token = generate_test_id_token_with_claims(audience, claims);

        let verifier = Verifier::default()
            .with_jwks_url(format!("http://{}/certs", server.addr()))
            .with_audience(audience)
            .with_email(email);

        let result = verifier.verify(token).await;
        assert!(result.is_ok());
        let claims = result.unwrap();
        assert_eq!(claims["email"].as_str().unwrap(), email);

        Ok(())
    }
}
