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

mod jws;

use crate::credentials::dynamic::CredentialTrait;
use crate::credentials::{Credential, Result};
use crate::errors::{self, CredentialError};
use crate::token::{Token, TokenProvider};
use crate::token_cache::TokenCache;
use async_trait::async_trait;
use derive_builder::Builder;
use http::header::{AUTHORIZATION, HeaderName, HeaderValue};
use jws::{CLOCK_SKEW_FUDGE, DEFAULT_TOKEN_TIMEOUT, JwsClaims, JwsHeader};
use rustls::crypto::CryptoProvider;
use rustls::sign::Signer;
use rustls_pemfile::Item;
use std::sync::Arc;
use time::OffsetDateTime;

const DEFAULT_SCOPES: &str = "https://www.googleapis.com/auth/cloud-platform";

pub(crate) fn creds_from(js: serde_json::Value) -> Result<Credential> {
    let service_account_info =
        serde_json::from_value::<ServiceAccountInfo>(js).map_err(errors::non_retryable)?;
    let token_provider = ServiceAccountTokenProvider {
        service_account_info,
    };
    let token_provider = TokenCache::new(token_provider);

    Ok(Credential {
        inner: Arc::new(ServiceAccountCredential { token_provider }),
    })
}

/// A representation of a Service Account File. See [Service Account Keys](https://google.aip.dev/auth/4112)
/// for more details.
#[derive(serde::Deserialize, Builder)]
#[builder(setter(into))]
struct ServiceAccountInfo {
    client_email: String,
    private_key_id: String,
    private_key: String,
    project_id: String,
    universe_domain: String,
}

impl std::fmt::Debug for ServiceAccountInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServiceAccountInfo")
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
}

#[derive(Debug)]
struct ServiceAccountTokenProvider {
    service_account_info: ServiceAccountInfo,
}

#[async_trait]
impl TokenProvider for ServiceAccountTokenProvider {
    async fn get_token(&self) -> Result<Token> {
        let signer = self.signer(&self.service_account_info.private_key)?;

        let expires_at = std::time::Instant::now() - CLOCK_SKEW_FUDGE + DEFAULT_TOKEN_TIMEOUT;
        // The claims encode a unix timestamp. `std::time::Instant` has no
        // epoch, so we use `time::OffsetDateTime`, which reads system time, in
        // the implementation.
        let now = OffsetDateTime::now_utc() - CLOCK_SKEW_FUDGE;
        let exp = now + DEFAULT_TOKEN_TIMEOUT;
        let claims = JwsClaims {
            iss: self.service_account_info.client_email.clone(),
            scope: Some(DEFAULT_SCOPES.to_string()),
            aud: None,
            exp,
            iat: now,
            typ: None,
            sub: Some(self.service_account_info.client_email.clone()),
        };

        let header = JwsHeader {
            alg: "RS256",
            typ: "JWT",
            kid: &self.service_account_info.private_key_id,
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
        //TODO(#679) add support for ECDSA
        sk.choose_scheme(&[rustls::SignatureScheme::RSA_PKCS1_SHA256])
            .ok_or_else(|| errors::non_retryable_from_str("Unable to choose RSA_PKCS1_SHA256 signing scheme as it is not supported by current signer"))
    }

    fn unexpected_private_key_error(private_key_format: Item) -> CredentialError {
        errors::non_retryable_from_str(format!(
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
            .map_err(errors::non_retryable)?;
        value.set_sensitive(true);
        Ok(vec![(AUTHORIZATION, value)])
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
    use serde_json::json;
    use std::time::Duration;

    type TestResult = std::result::Result<(), Box<dyn std::error::Error>>;

    const SSJ_REGEX: &str = r"(?<header>[^\.]+)\.(?<claims>[^\.]+)\.(?<sig>[^\.]+)";

    #[test]
    fn debug_token_provider() {
        let expected = ServiceAccountInfo {
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

        let sac = ServiceAccountCredential {
            token_provider: mock,
        };
        assert!(sac.get_token().await.is_err());
    }

    #[tokio::test]
    async fn get_headers_success() {
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
    async fn get_headers_failure() {
        let mut mock = MockTokenProvider::new();
        mock.expect_get_token()
            .times(1)
            .return_once(|| Err(errors::non_retryable_from_str("fail")));

        let sac = ServiceAccountCredential {
            token_provider: mock,
        };
        assert!(sac.get_headers().await.is_err());
    }

    fn get_mock_service_account() -> ServiceAccountInfo {
        ServiceAccountInfoBuilder::default()
            .client_email("test-client-email")
            .private_key_id("test-private-key-id")
            .private_key("")
            .project_id("test-project-id")
            .universe_domain("test-universe-domain")
            .build()
            .unwrap()
    }

    fn generate_pkcs1_key() -> String {
        let mut rng = rand::thread_rng();
        let bits = 2048;
        let priv_key = RsaPrivateKey::new(&mut rng, bits).expect("failed to generate a key");
        priv_key
            .to_pkcs1_pem(LineEnding::LF)
            .expect("Failed to encode key to PKCS#1 PEM")
            .to_string()
    }

    #[tokio::test]
    async fn get_service_account_token_pkcs1_key_failure() -> TestResult {
        let mut service_account_info = get_mock_service_account();
        service_account_info.private_key = generate_pkcs1_key();
        let token_provider = ServiceAccountTokenProvider {
            service_account_info,
        };
        let expected_error_message = "expected key to be in form of PKCS8, found Pkcs1Key";
        assert!(
            token_provider
                .get_token()
                .await
                .is_err_and(|e| e.to_string().contains(expected_error_message))
        );
        Ok(())
    }

    fn generate_pkcs8_key() -> String {
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
        let mut service_account_info = get_mock_service_account();
        service_account_info.private_key = generate_pkcs8_key();
        let token_provider = ServiceAccountTokenProvider {
            service_account_info,
        };
        let token = token_provider.get_token().await?;
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
        assert_eq!(header["kid"], "test-private-key-id");

        let claims = b64_decode_to_json(captures["claims"].to_string());
        assert_eq!(claims["iss"], "test-client-email");
        assert_eq!(claims["scope"], DEFAULT_SCOPES);
        assert!(claims["iat"].is_number());
        assert!(claims["exp"].is_number());
        assert_eq!(claims["sub"], "test-client-email");

        Ok(())
    }

    #[tokio::test]
    async fn token_caching() -> TestResult {
        let private_key = generate_pkcs8_key();

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
        let mut service_account_info = get_mock_service_account();
        let pem_data = "-----BEGIN PRIVATE KEY-----\nMIGkAg==\n-----END PRIVATE KEY-----";
        service_account_info.private_key = pem_data.to_string();
        let token_provider = ServiceAccountTokenProvider {
            service_account_info,
        };
        let token = token_provider.get_token().await;
        let expected_error_message = "failed to parse private key";
        assert!(token.is_err_and(|e| e.to_string().contains(expected_error_message)));
        Ok(())
    }

    #[test]
    fn signer_failure() -> TestResult {
        let tp = ServiceAccountTokenProvider {
            service_account_info: get_mock_service_account(),
        };
        let signer = tp.signer(&tp.service_account_info.private_key);
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
}
