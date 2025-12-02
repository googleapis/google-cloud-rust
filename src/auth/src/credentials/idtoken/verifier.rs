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
//! ```
//! # use google_cloud_auth::credentials::idtoken;
//! # use google_cloud_auth::credentials::idtoken::verifier::Verifier;
//! # use std::time::Duration;
//! let audience = "https://my-service.a.run.app";
//! let verifier = idtoken::verifier::Builder::new([audience]).build();
//!
//! async fn verify_my_token(verifier: &Verifier, token: &str) -> anyhow::Result<()> {
//!     let claims = verifier.verify(token).await?;
//!
//!     println!("Hello: {:?}", claims["email"]);
//! #   Ok(())
//! }
//! ```
//! [OIDC ID Tokens]: https://cloud.google.com/docs/authentication/token-types#identity-tokens

use crate::credentials::internal::jwk_client::JwkClient;
use biscuit::SingleOrMultiple;
/// Represents the claims in an ID token.
pub use serde_json::Map;
/// Represents a claim value in an ID token.
pub use serde_json::Value;
use std::time::Duration;

/// Builder is used construct a [Verifier] of id tokens.
pub struct Builder {
    audiences: Vec<String>,
    email: Option<String>,
    jwks_url: Option<String>,
    clock_skew: Option<Duration>,
}

impl Builder {
    /// Create a [Verifier] for ID Tokens with a list of target
    /// audiences for the token verification.
    pub fn new<I, S>(audiences: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let audiences = audiences
            .into_iter()
            .map(|s| s.into())
            .collect::<Vec<String>>();
        Self {
            audiences,
            email: None,
            jwks_url: None,
            clock_skew: None,
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
    /// let verifier = Builder::new(["https://my-service.a.run.app"])
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
    /// let verifier = Builder::new(["https://my-service.a.run.app"])
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
    /// # use google_cloud_auth::credentials::idtoken::verifier::Builder;
    /// # use std::time::Duration;
    /// let verifier = Builder::new(["https://my-service.a.run.app"])
    ///     .with_clock_skew(Duration::from_secs(60))
    ///     .build();
    /// ```
    pub fn with_clock_skew(mut self, clock_skew: Duration) -> Self {
        self.clock_skew = Some(clock_skew);
        self
    }

    /// Returns a [Verifier] instance with the configured settings.
    pub fn build(self) -> Verifier {
        Verifier {
            jwk_client: JwkClient::new(),
            audiences: self.audiences.clone(),
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
/// async fn verify_id_token(token: &str) {
///     let verifier = Builder::new(["https://my-service.a.run.app"]).build();
///
///     let claims = verifier.verify(token).await.expect("Failed to verify ID token");
///     println!("Verified claims: {:?}", claims);
/// }
/// ```
pub struct Verifier {
    jwk_client: JwkClient,
    audiences: Vec<String>,
    email: Option<String>,
    jwks_url: Option<String>,
    clock_skew: Duration,
}

impl Verifier {
    /// Verifies the ID token and returns the claims.
    pub async fn verify(&self, token: &str) -> std::result::Result<Map<String, Value>, Error> {
        let token = biscuit::JWT::<Map<String, Value>, biscuit::Empty>::new_encoded(&token);
        let header = token.unverified_header().map_err(Error::decode)?;

        let key_id = header
            .registered
            .key_id
            .ok_or_else(|| Error::invalid_field("kid", "kid header is missing"))?;

        let alg = header.registered.algorithm;
        let expected_email = self.email.clone();
        let jwks_url = self.jwks_url.clone();

        let jwk_set = self
            .jwk_client
            .get_or_load_jwk_set(key_id, alg, jwks_url)
            .await
            .map_err(Error::load_cert)?;

        let token = token
            .decode_with_jwks(&jwk_set, Some(alg))
            .map_err(Error::invalid)?;

        token
            .validate(biscuit::ValidationOptions {
                claim_presence_options: biscuit::ClaimPresenceOptions::default(),
                temporal_options: biscuit::TemporalOptions {
                    epsilon: chrono::Duration::seconds(self.clock_skew.as_secs() as i64),
                    ..Default::default()
                },
                issued_at: biscuit::Validation::Validate(chrono::TimeDelta::MAX),
                not_before: biscuit::Validation::Validate(()),
                expiry: biscuit::Validation::Validate(()),
                issuer: biscuit::Validation::Ignored,
                audience: biscuit::Validation::Ignored,
            })
            .map_err(Error::invalid)?;

        let claims = token.payload().map_err(Error::decode)?;
        // if one of the audiences matches, then the validation is successful
        let audience = self.audiences.iter().find(|audience| {
            claims
                .registered
                .validate_aud(biscuit::Validation::Validate(audience.to_string()))
                .is_ok()
        });
        if audience.is_none() {
            return Err(Error::invalid_field("aud", "audience claim is missing"));
        }
        // if one of the issuers matches, then the validation is successful
        let issuers = [&"https://accounts.google.com", "accounts.google.com"];
        let issuer = issuers.iter().find(|issuer| {
            claims
                .registered
                .validate_iss(biscuit::Validation::Validate(issuer.to_string()))
                .is_ok()
        });
        if issuer.is_none() {
            return Err(Error::invalid_field("iss", "issuer claim is missing"));
        }
        if let Some(email) = expected_email {
            let email_verified =
                claims.private["email_verified"]
                    .as_bool()
                    .ok_or(Error::invalid_field(
                        "email_verified",
                        "email_verified claim is missing",
                    ))?;
            if !email_verified {
                return Err(Error::invalid_field(
                    "email_verified",
                    "email_verified claim value is `false`",
                ));
            }
            let token_email = claims.private["email"]
                .as_str()
                .ok_or_else(|| Error::invalid_field("email", "email claim is missing"))?;
            if !email.eq(token_email) {
                let err_msg = format!("expected `{email}`, but found `{token_email}`");
                return Err(Error::invalid_field("email", err_msg));
            }
        }

        let mut all_claims: Map<String, Value> = claims.private.clone();
        claims.registered.audience.iter().for_each(|aud| {
            let aud = match aud {
                SingleOrMultiple::Single(aud) => aud,
                SingleOrMultiple::Multiple(aud) => &aud.join(","),
            };
            all_claims.insert("aud".to_string(), Value::String(aud.to_string()));
        });
        claims.registered.issuer.iter().for_each(|iss| {
            all_claims.insert("iss".to_string(), Value::String(iss.to_string()));
        });
        claims.registered.issued_at.iter().for_each(|iat| {
            all_claims.insert("iat".to_string(), Value::Number(iat.timestamp().into()));
        });
        claims.registered.not_before.iter().for_each(|nbf| {
            all_claims.insert("nbf".to_string(), Value::Number(nbf.timestamp().into()));
        });
        claims.registered.expiry.iter().for_each(|exp| {
            all_claims.insert("exp".to_string(), Value::Number(exp.timestamp().into()));
        });

        Ok(all_claims)
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
        matches!(self.0, ErrorKind::Invalid(_)) || matches!(self.0, ErrorKind::InvalidField(_, _))
    }

    /// A problem fetching certificates to validate the JWT token.
    pub fn is_load_cert(&self) -> bool {
        matches!(self.0, ErrorKind::LoadingCertificate(_))
    }

    /// A problem to decode the JWT token.
    fn decode<T>(source: T) -> Error
    where
        T: Into<BoxError>,
    {
        Error(ErrorKind::Decode(source.into()))
    }

    /// A problem fetching certificates to validate the JWT token.
    fn load_cert<T>(source: T) -> Error
    where
        T: Into<BoxError>,
    {
        Error(ErrorKind::LoadingCertificate(source.into()))
    }

    /// Validation error of the JWT Token.
    fn invalid<T>(source: T) -> Error
    where
        T: Into<BoxError>,
    {
        Error(ErrorKind::Invalid(source.into()))
    }

    /// Validation error of the JWT Token on a specific field.
    fn invalid_field<S: Into<String>, T>(field: S, source: T) -> Error
    where
        T: Into<BoxError>,
    {
        Error(ErrorKind::InvalidField(field.into(), source.into()))
    }
}

#[derive(thiserror::Error, Debug)]
enum ErrorKind {
    #[error("cannot decode JWT token: {0}")]
    Decode(#[source] BoxError),
    #[error("JWT token is invalid: {0}")]
    Invalid(#[source] BoxError),
    #[error("JWT token `{0}` field is invalid: {1}")]
    InvalidField(String, #[source] BoxError),
    #[error("Failed to fetch certificate: {0}")]
    LoadingCertificate(#[source] BoxError),
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::credentials::idtoken::tests::{
        OverrideClaims, TEST_KEY_ID, generate_test_id_token, generate_test_id_token_with_claims,
    };
    use base64::Engine;
    use biscuit::jwa::SignatureAlgorithm as Algorithm;
    use biscuit::jws::{RegisteredHeader, Secret};
    use biscuit::{ClaimsSet, JWT, RegisteredClaims, SingleOrMultiple};
    use httptest::matchers::{all_of, request};
    use httptest::responders::{json_encoded, status_code};
    use httptest::{Expectation, Server};
    use rsa::pkcs1::EncodeRsaPrivateKey;
    use rsa::traits::PublicKeyParts;
    use std::sync::Arc;
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

        let verifier = Builder::new([audience])
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

        let verifier = Builder::new(["https://wrong-audience.com"])
            .with_jwks_url(format!("http://{}/certs", server.addr()))
            .build();

        let result = verifier.verify(token).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().is_invalid());

        Ok(())
    }

    #[tokio::test]
    async fn test_verify_multiple_audience_success() -> TestResult {
        let server = Server::run();
        server.expect(
            Expectation::matching(all_of![request::path("/certs"),])
                .times(1)
                .respond_with(json_encoded(create_jwk_set_response())),
        );

        let audiences = ["https://example.com", "https://another_example.com"];
        let verifier = Builder::new(audiences)
            .with_jwks_url(format!("http://{}/certs", server.addr()))
            .build();

        for audience in audiences {
            let token = generate_test_id_token(audience);
            let token = token.as_str();
            let claims = verifier.verify(token).await?;
            assert!(!claims.is_empty());
        }

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
        let mut claims = OverrideClaims::default();
        claims.issuer = Some("https://wrong-issuer.com".into());
        let token = generate_test_id_token_with_claims(audience, claims);
        let token = token.as_str();

        let verifier = Builder::new([audience])
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
        let mut claims = OverrideClaims::default();
        claims.email = Some(email.into());
        claims.email_verified = Some(true);
        let token = generate_test_id_token_with_claims(audience, claims);
        let token = token.as_str();

        let verifier = Builder::new([audience])
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
        let mut claims = OverrideClaims::default();
        claims.email = Some(email.into());
        claims.email_verified = Some(true);
        let token = generate_test_id_token_with_claims(audience, claims);
        let token = token.as_str();

        let verifier = Builder::new([audience])
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
        let mut claims = OverrideClaims::default();
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        claims.expiry = Some((now.as_secs() - 3600) as i64); // expired 1 hour ago
        let token = generate_test_id_token_with_claims(audience, claims);
        let token = token.as_str();

        let verifier = Builder::new([audience])
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
        let mut claims = OverrideClaims::default();
        claims.email = Some(email.into());
        claims.email_verified = Some(false);
        let token = generate_test_id_token_with_claims(audience, claims);
        let token = token.as_str();

        let verifier = Builder::new([audience])
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
        let mut claims = OverrideClaims::default();
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        claims.expiry = Some((now.as_secs() - 5) as i64); // expired 5 seconds ago
        let token = generate_test_id_token_with_claims(audience, claims);
        let token = token.as_str();

        let verifier = Builder::new([audience])
            .with_jwks_url(format!("http://{}/certs", server.addr()))
            .with_clock_skew(Duration::from_secs(10))
            .build();

        let result = verifier.verify(token).await;
        assert!(result.is_ok());

        Ok(())
    }

    #[tokio::test]
    async fn test_verify_decode_error() -> TestResult {
        let audience = "https://example.com";
        let verifier = Builder::new([audience]).build();
        let invalid_token = "invalid.token.format";

        let result = verifier.verify(invalid_token).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().is_decode());

        Ok(())
    }

    #[tokio::test]
    async fn test_verify_missing_kid() -> TestResult {
        let header = RegisteredHeader {
            algorithm: Algorithm::RS256,
            ..Default::default()
        };
        let claims = ClaimsSet::<biscuit::Empty>::default();
        let jwt = JWT::<biscuit::Empty, biscuit::Empty>::new_decoded(From::from(header), claims);

        let private_cert = crate::credentials::tests::RSA_PRIVATE_KEY
            .to_pkcs1_der()
            .expect("Failed to encode private key to PKCS#1 DER");

        let key_pair = ring::signature::RsaKeyPair::from_der(private_cert.as_bytes()).unwrap();
        let private_key = Secret::RsaKeyPair(Arc::new(key_pair));

        let token = jwt
            .into_encoded(&private_key)
            .expect("failed to encode jwt")
            .unwrap_encoded()
            .to_string();

        let verifier = Builder::new(["https://example.com"]).build();

        let result = verifier.verify(&token).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().is_invalid());

        Ok(())
    }

    #[tokio::test]
    async fn test_verify_load_cert_error() -> TestResult {
        let server = Server::run();
        server.expect(
            Expectation::matching(all_of![request::path("/certs"),])
                .times(1)
                .respond_with(status_code(404)),
        );

        let audience = "https://example.com";
        let token = generate_test_id_token(audience);
        let token = token.as_str();

        let verifier = Builder::new([audience])
            .with_jwks_url(format!("http://{}/certs", server.addr()))
            .build();

        let result = verifier.verify(token).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().is_load_cert());

        Ok(())
    }
}
