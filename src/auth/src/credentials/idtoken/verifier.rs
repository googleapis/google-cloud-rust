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

//! Verify [OIDC ID tokens].
//!
//! [Verifier] is used to validate an OIDC ID token.
//! This includes verifying the token's signature against the appropriate
//! JSON Web Key Set (JWKS), and validating its claims, such as audience and issuer.
//!
//! ## Example: Verifying an ID token
//!
//! ```no_run
//! # use google_cloud_auth::credentials::idtoken;
//! # use std::time::Duration;
//! let audience = "https://my-service.a.run.app";
//! let verifier = idtoken::verifier::Builder::new(audience).build();
//!
//! async fn verify_my_token(token: &str) -> anyhow::Result<()> {
//!     let claims = verifier.verify(token).await?;
//!     let email = claims["email"].as_str()?;
//!
//!     println!("Hello: {:?}", email);
//! #   Ok(())
//! }
//! ```
//! [OIDC ID Tokens]: https://cloud.google.com/docs/authentication/token-types#identity-tokens

use crate::credentials::internal::jwk_client::JwkClient;
use jsonwebtoken::Validation;
pub use serde_json::{Map, Value};
use std::time::Duration;

/// Builder is used construct a [Verifier] of id tokens.
#[derive(Debug, Default)]
pub struct Builder {
    audience: String,
    email: Option<String>,
    jwks_url: Option<String>,
    clock_skew: Option<Duration>,
}

impl Builder {
    /// Create a [Verifier] for ID Tokens with a target audience
    /// for the token verification.
    pub fn new<S: Into<String>>(audience: S) -> Self {
        Self {
            audience: audience.into(),
            ..Self::default()
        }
    }

    /// The email address of the service account that signed the ID token.
    ///
    /// If provided, the verifier will check that the `email` claim in the
    /// ID token matches this value and that the `email_verified` claim is `true`.
    ///
    /// # Example
    ///
    /// ```
    /// # use google_cloud_auth::credentials::idtoken::verifier::Builder;
    /// let audience = "https://example.com";
    /// let verifier = Builder::new(audience)
    ///     .with_email("service-account@example.com")
    ///     .build();
    /// ```
    pub fn with_email<S: Into<String>>(mut self, email: S) -> Self {
        self.email = Some(email.into());
        self
    }

    /// The URL of the JSON Web Key Set (JWKS) that contains the public keys
    /// that can be used to verify the signature of the ID token.
    ///
    /// If not provided, the default Google certs URL will be used.
    ///
    /// # Example
    ///
    /// ```
    /// # use google_cloud_auth::credentials::idtoken::verifier::Builder;
    /// let audience = "https://example.com";
    /// let verifier = Builder::new(audience)
    ///     .with_jwks_url("https://www.googleapis.com/oauth2/v3/certs")
    ///     .build();    
    /// ```
    pub fn with_jwks_url<S: Into<String>>(mut self, jwks_url: S) -> Self {
        self.jwks_url = Some(jwks_url.into());
        self
    }

    /// The acceptable clock skew when verifying the token's timestamps.
    ///
    /// This value is used to account for clock differences between the token
    /// issuer and the verifier. The default value is 10 seconds.
    ///
    /// # Example
    ///
    /// ```
    /// # use google_cloud_auth::credentials::idtoken::Builder;
    /// # use std::time::Duration;
    /// let audience = "https://example.com";
    /// let verifier = Builder::new(audience)
    ///     .with_clock_skew(Duration::from_secs(60))
    ///     .build();
    /// ```
    pub fn with_clock_skew(mut self, clock_skew: Duration) -> Self {
        self.clock_skew = Some(clock_skew);
        self
    }

    /// Verifies the ID token and returns the claims.
    pub fn build(self) -> Verifier {
        Verifier {
            jwk_client: JwkClient::new(),
            audience: self.audience.clone(),
            email: self.email.clone(),
            jwks_url: self.jwks_url.clone(),
            clock_skew: self.clock_skew.unwrap_or_else(|| Duration::from_secs(10)),
        }
    }
}

/// Verifier is used to verify OIDC ID Tokens.
///
/// # Example
///
/// ```
/// # use google_cloud_auth::credentials::idtoken::verifier::Builder;
/// # use std::time::Duration;
///
/// async fn verify_id_token(token: &str) {
///     let audience = "https://example.com";
///     let verifier = Builder::new(audience).build();
///
///     let claims = verifier.verify(token).await.expect("Failed to verify ID token");
///     println!("Verified claims: {:?}", claims);
/// }
/// ```
#[derive(Debug)]
pub struct Verifier {
    jwk_client: JwkClient,
    audience: String,
    email: Option<String>,
    jwks_url: Option<String>,
    clock_skew: Duration,
}

impl Verifier {
    /// Verifies the ID token and returns the claims.
    pub async fn verify(&self, token: &str) -> std::result::Result<Map<String, Value>, Error> {
        let header = jsonwebtoken::decode_header(token).map_err(Error::decode)?;

        let key_id = header
            .kid
            .ok_or_else(|| Error::missing_header_field("kid"))?;

        let mut validation = Validation::new(header.alg);
        validation.leeway = self.clock_skew.as_secs();
        // TODO(#3591): Support TPC/REP that can have different issuers
        validation.set_issuer(&["https://accounts.google.com", "accounts.google.com"]);
        validation.set_audience(std::slice::from_ref(&self.audience));

        let expected_email = self.email.clone();
        let jwks_url = self.jwks_url.clone();

        let cert = self
            .jwk_client
            .get_or_load_cert(key_id, header.alg, jwks_url)
            .await
            .map_err(Error::load_cert)?;

        let token = jsonwebtoken::decode::<Map<String, Value>>(&token, &cert, &validation)
            .map_err(|e| match e.clone().into_kind() {
                jsonwebtoken::errors::ErrorKind::InvalidToken
                | jsonwebtoken::errors::ErrorKind::Base64(_)
                | jsonwebtoken::errors::ErrorKind::Json(_)
                | jsonwebtoken::errors::ErrorKind::Utf8(_) => Error::decode(e),
                jsonwebtoken::errors::ErrorKind::InvalidAlgorithm => Error::invalid("algorithm", e),
                jsonwebtoken::errors::ErrorKind::InvalidIssuer => Error::invalid("issuer", e),
                jsonwebtoken::errors::ErrorKind::InvalidAudience => Error::invalid("audience", e),
                jsonwebtoken::errors::ErrorKind::InvalidSubject => Error::invalid("subject", e),
                jsonwebtoken::errors::ErrorKind::ExpiredSignature
                | jsonwebtoken::errors::ErrorKind::ImmatureSignature => {
                    Error::invalid("expiration", e)
                }
                jsonwebtoken::errors::ErrorKind::MissingRequiredClaim(field) => {
                    Error::missing_claim_field(field.as_str())
                }
                jsonwebtoken::errors::ErrorKind::InvalidSignature
                | jsonwebtoken::errors::ErrorKind::InvalidEcdsaKey
                | jsonwebtoken::errors::ErrorKind::InvalidEddsaKey
                | jsonwebtoken::errors::ErrorKind::InvalidRsaKey(_)
                | jsonwebtoken::errors::ErrorKind::RsaFailedSigning
                | jsonwebtoken::errors::ErrorKind::InvalidAlgorithmName
                | jsonwebtoken::errors::ErrorKind::InvalidKeyFormat => {
                    Error::invalid("signature", e)
                }
                _ => Error::invalid("unkown", e),
            })?;

        let claims = token.claims;
        if let Some(email) = expected_email {
            let email_verified = claims["email_verified"].as_bool().unwrap_or(false);
            if !email_verified {
                return Err(Error::invalid(
                    "email_verified",
                    "email_verified claim is missing or value is `false`",
                ));
            }
            let token_email = claims["email"]
                .as_str()
                .ok_or_else(|| Error::missing_claim_field("email"))?;
            if !email.eq(token_email) {
                let err_msg = format!("expected `{email}`, but found `{token_email}`");
                return Err(Error::invalid("email", err_msg));
            }
        }

        Ok(claims)
    }
}

type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// The error type for [Verifier] errors.
#[derive(thiserror::Error, Debug)]
#[error(transparent)]
pub struct Error(ErrorKind);

impl Error {
    /// A problem decoding JWT token.
    pub fn is_decode(&self) -> bool {
        matches!(self.0, ErrorKind::Decode(_))
    }

    /// A problem validating JWT token accordingly to set criteria.
    pub fn is_invalid(&self) -> bool {
        matches!(self.0, ErrorKind::Invalid(_, _))
    }

    /// A problem fetching certificates to validate the JWT token.
    pub fn is_load_cert(&self) -> bool {
        matches!(self.0, ErrorKind::LoadingCertificate(_))
    }

    /// A required field was missing from JWT token header.
    pub fn is_missing_header_field(&self) -> bool {
        matches!(self.0, ErrorKind::MissingHeaderField(_))
    }

    /// A required field was missing from JWT token claims.
    pub fn is_missing_claim_field(&self) -> bool {
        matches!(self.0, ErrorKind::MissingClaimField(_))
    }

    /// A problem to decode the JWT token.
    pub(crate) fn decode<T>(source: T) -> Error
    where
        T: Into<BoxError>,
    {
        Error(ErrorKind::Decode(source.into()))
    }

    /// A problem fetching certificates to validate the JWT token.
    pub(crate) fn load_cert<T>(source: T) -> Error
    where
        T: Into<BoxError>,
    {
        Error(ErrorKind::LoadingCertificate(source.into()))
    }

    /// Validation error in a given area of the JWT Token.
    pub(crate) fn invalid<T>(area: &'static str, source: T) -> Error
    where
        T: Into<BoxError>,
    {
        Error(ErrorKind::Invalid(area, source.into()))
    }

    /// A required field was missing from the JWT token header.
    pub(crate) fn missing_header_field(field: &'static str) -> Error {
        Error(ErrorKind::MissingHeaderField(field))
    }

    /// A required field was missing from the JWT token claims.
    pub(crate) fn missing_claim_field<S: Into<String>>(field: S) -> Error {
        Error(ErrorKind::MissingClaimField(field.into()))
    }
}

#[derive(thiserror::Error, Debug)]
enum ErrorKind {
    #[error("cannot decode JWT token: {0}")]
    Decode(#[source] BoxError),
    #[error("JWT token {0} is invalid: {1}")]
    Invalid(&'static str, #[source] BoxError),
    #[error("Failed to fetch certificate: {0}")]
    LoadingCertificate(#[source] BoxError),
    #[error("JWT token header is missing required field: {0}")]
    MissingHeaderField(&'static str),
    #[error("JWT token claims is missing required field: {0}")]
    MissingClaimField(String),
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::credentials::idtoken::tests::{
        TEST_KEY_ID, generate_test_id_token, generate_test_id_token_with_claims,
    };
    use base64::Engine;
    use httptest::matchers::{all_of, request};
    use httptest::responders::json_encoded;
    use httptest::{Expectation, Server};
    use rsa::traits::PublicKeyParts;
    use std::collections::HashMap;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    type TestResult = anyhow::Result<()>;

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
        let token = token.as_str();

        let verifier = Builder::new(audience)
            .with_jwks_url(format!("http://{}/certs", server.addr()))
            .build();

        let claims = verifier.verify(token).await?;
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
        let token = token.as_str();

        let verifier = Builder::new("https://wrong-audience.com")
            .with_jwks_url(format!("http://{}/certs", server.addr()))
            .build();

        let result = verifier.verify(token).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().is_invalid());

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
        let token = token.as_str();

        let verifier = Builder::new(audience)
            .with_jwks_url(format!("http://{}/certs", server.addr()))
            .build();

        let result = verifier.verify(token).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().is_invalid());

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
        let token = token.as_str();

        let verifier = Builder::new(audience)
            .with_jwks_url(format!("http://{}/certs", server.addr()))
            .with_email(email)
            .build();

        let result = verifier.verify(token).await;
        assert!(result.is_ok());
        let claims = result.unwrap();
        assert_eq!(claims["email"].as_str().unwrap(), email);

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
        let token = token.as_str();

        let verifier = Builder::new(audience)
            .with_jwks_url(format!("http://{}/certs", server.addr()))
            .with_email("wrong@example.com")
            .build();

        let result = verifier.verify(token).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().is_invalid());
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
        let token = token.as_str();

        let verifier = Builder::new(audience)
            .with_jwks_url(format!("http://{}/certs", server.addr()))
            .build();

        let result = verifier.verify(token).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().is_invalid());

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
        let token = token.as_str();

        let verifier = Builder::new(audience)
            .with_jwks_url(format!("http://{}/certs", server.addr()))
            .with_email(email)
            .build();

        let result = verifier.verify(token).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().is_invalid());
        Ok(())
    }

    #[tokio::test]
    async fn test_verify_clock_skew() -> TestResult {
        let server = Server::run();
        server.expect(
            Expectation::matching(all_of![request::path("/certs"),])
                .times(1)
                .respond_with(json_encoded(create_jwk_set_response())),
        );

        let audience = "https://example.com";
        let mut claims = HashMap::new();
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        claims.insert("exp", (now.as_secs() - 5).into()); // expired 5 seconds ago
        let token = generate_test_id_token_with_claims(audience, claims);
        let token = token.as_str();

        let verifier = Builder::new(audience)
            .with_jwks_url(format!("http://{}/certs", server.addr()))
            .with_clock_skew(Duration::from_secs(10))
            .build();

        let result = verifier.verify(token).await;
        assert!(result.is_ok());

        Ok(())
    }
}
