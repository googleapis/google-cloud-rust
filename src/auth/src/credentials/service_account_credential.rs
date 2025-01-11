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

use crate::credentials::dynamic::CredentialTrait;
use crate::credentials::jws::{JwsClaimsBuilder, JwsHeader};
use crate::credentials::Result;
use crate::errors::CredentialError;
use crate::token::{Token, TokenProvider};
use async_trait::async_trait;
use derive_builder::Builder;
use http::header::{HeaderName, HeaderValue, AUTHORIZATION};
use rustls::crypto::CryptoProvider;
use rustls::sign::Signer;
use rustls_pemfile::Item;
use std::time::Duration;
use time::OffsetDateTime;

const DEFAULT_TOKEN_TIMEOUT: Duration = Duration::from_secs(3600);
const DEFAULT_HEADER: JwsHeader = JwsHeader {
    alg: "RS256",
    typ: "JWT",
    kid: None,
};

/// A representation of a Service Account File. See [Service Account Keys](https://google.aip.dev/auth/4112)
/// for more details.
#[allow(dead_code)] // Implementation in progress
#[derive(serde::Deserialize, Debug, Builder)]
#[builder(setter(into))]
pub(crate) struct ServiceAccountInfo {
    client_email: String,
    private_key_id: String,
    private_key: String,
    auth_uri: String,
    token_uri: String,
    project_id: String,
    universe_domain: String,
}

#[allow(dead_code)] // Implementation in progress
#[derive(Debug)]
pub(crate) struct ServiceAccountCredential<T>
where
    T: TokenProvider,
{
    token_provider: T,
}

#[allow(dead_code)]
#[derive(Debug)]
pub(crate) struct ServiceAccountTokenProvider {
    service_account_info: ServiceAccountInfo,
}

#[async_trait]
#[allow(dead_code)]
impl TokenProvider for ServiceAccountTokenProvider {
    async fn get_token(&self) -> Result<Token> {
        let signer = self.signer(&self.service_account_info.private_key)?;

        let mut claims = JwsClaimsBuilder::default()
            .iss(self.service_account_info.client_email.as_str())
            .aud(self.service_account_info.token_uri.as_str())
            .build()
            .map_err(|e| CredentialError::new(false, e.into()))?;

        let header = DEFAULT_HEADER;

        let ss = format!("{}.{}", header.encode()?, claims.encode()?);
        let sig = signer
            .sign(ss.as_bytes())
            .map_err(|e| CredentialError::new(false, e.into()))?;
        use base64::prelude::{Engine as _, BASE64_URL_SAFE_NO_PAD};
        let token = format!("{}.{}", ss, &BASE64_URL_SAFE_NO_PAD.encode(sig));

        let token = Token {
            token,
            token_type: "JWT".to_string(),
            expires_at: Some(OffsetDateTime::now_utc() + DEFAULT_TOKEN_TIMEOUT),
            metadata: None,
        };
        Ok(token)
    }
}

impl ServiceAccountTokenProvider {
    // Creates a signer using the private key stored in the service account file.
    fn signer(&self, private_key: &String) -> Result<Box<dyn Signer>> {
        let crypto_provider = CryptoProvider::get_default().ok_or_else(|| {
            CredentialError::new(false, Box::from("unable to get crypto provider"))
        })?;

        let key_provider = crypto_provider.key_provider;

        let pk = rustls_pemfile::read_one(&mut private_key.as_bytes())
            .map_err(|e| CredentialError::new(false, e.into()))?
            .ok_or_else(|| {
                CredentialError::new(false, Box::from("unable to parse service account key"))
            })?;
        let pk = match pk {
            Item::Pkcs1Key(item) => key_provider.load_private_key(item.into()),
            Item::Pkcs8Key(item) => key_provider.load_private_key(item.into()),
            other => {
                return Err(CredentialError::new(
                    false,
                    Box::from(format!(
                        "expected key to be in form of RSA or PKCS8, found {:?}",
                        other
                    )),
                ))
            }
        };
        let sk = pk.map_err(|e| CredentialError::new(false, e.into()))?;
        sk.choose_scheme(&[rustls::SignatureScheme::RSA_PKCS1_SHA256])
            .ok_or_else(|| CredentialError::new(false, Box::from("invalid signing scheme")))
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
            .map_err(|e| CredentialError::new(false, e.into()))?;
        value.set_sensitive(true);
        Ok(vec![(AUTHORIZATION, value)])
    }

    async fn get_universe_domain(&self) -> Option<String> {
        Some("googleapis.com".to_string())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::token::test::MockTokenProvider;
    use std::path::Path;

    type TestResult = std::result::Result<(), Box<dyn std::error::Error>>;

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
            .return_once(|| Err(CredentialError::new(false, Box::from("fail"))));

        let sac = ServiceAccountCredential {
            token_provider: mock,
        };
        assert!(sac.get_token().await.is_err());
    }

    #[tokio::test]
    async fn get_headers_success() {
        #[derive(Debug, PartialEq)]
        struct HV {
            header: String,
            value: String,
            is_sensitive: bool,
        }

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
        let headers: Vec<HV> = sac
            .get_headers()
            .await
            .unwrap()
            .into_iter()
            .map(|(h, v)| HV {
                header: h.to_string(),
                value: v.to_str().unwrap().to_string(),
                is_sensitive: v.is_sensitive(),
            })
            .collect();

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
            .return_once(|| Err(CredentialError::new(false, Box::from("fail"))));

        let sac = ServiceAccountCredential {
            token_provider: mock,
        };
        assert!(sac.get_headers().await.is_err());
    }

    fn get_mock_service_account() -> ServiceAccountInfo {
        ServiceAccountInfoBuilder::default()
            .client_email("")
            .private_key_id("")
            .private_key("")
            .auth_uri("")
            .token_uri("")
            .project_id("")
            .universe_domain("")
            .build()
            .unwrap()
    }

    async fn from_file(path: impl AsRef<Path>) -> Result<ServiceAccountInfo> {
        let sa: ServiceAccountInfo = serde_json::from_slice(
            &tokio::fs::read(path)
                .await
                .map_err(|e| CredentialError::new(false, e.into()))?,
        )
        .map_err(|e| CredentialError::new(false, e.into()))?;
        Ok(sa)
    }

    #[tokio::test]
    async fn get_service_account_token_success() {
        let _ = CryptoProvider::install_default(rustls::crypto::aws_lc_rs::default_provider());
        // Get the path to the current crate's root directory.
        let crate_root = Path::new(env!("CARGO_MANIFEST_DIR"));

        // Construct the relative path to your test data file.
        let testdata_path = crate_root.join("testdata").join("sa_account_key.json");
        let service_account_info = from_file(testdata_path).await.unwrap();
        let token_provider = ServiceAccountTokenProvider {
            service_account_info,
        };
        assert!(token_provider.get_token().await.is_ok());
    }

    #[test]
    fn signer_failure() -> TestResult {
        let tp = ServiceAccountTokenProvider {
            service_account_info: get_mock_service_account(),
        };
        assert!(tp.signer(&tp.service_account_info.private_key).is_err());
        Ok(())
    }
}
