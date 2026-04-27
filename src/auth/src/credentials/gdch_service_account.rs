// Copyright 2026 Google LLC
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

//! [Google Distributed Cloud Hosted] service account credentials.
//!
//! [Google Distributed Cloud Hosted]: https://cloud.google.com/distributed-cloud/hosted

use crate::build_errors::Error as BuilderError;
use crate::credentials::extract_credential_type;
use crate::errors::{self, CredentialsError};
use crate::token::{Token, TokenProvider};
use crate::{BuildResult, Result};
use base64::prelude::{BASE64_URL_SAFE_NO_PAD, Engine as _};
use rustls::crypto::CryptoProvider;
use rustls::sign::Signer;
use rustls_pki_types::{PrivateKeyDer, pem::PemObject};
use serde::Serialize;
use serde_json::Value;
use tokio::time::{Duration, Instant};

const FORMAT_VERSION: &str = "1";
const TOKEN_TIMEOUT: Duration = Duration::from_secs(3600);
const GRANT_TYPE: &str = "urn:ietf:params:oauth:token-type:token-exchange";
const REQUESTED_TOKEN_TYPE: &str = "urn:ietf:params:oauth:token-type:access_token";
const SUBJECT_TOKEN_TYPE: &str = "urn:k8s:params:oauth:token-type:serviceaccount";

#[derive(Clone, serde::Deserialize)]
struct GdchServiceAccountKey {
    format_version: String,
    project: String,
    name: String,
    ca_cert_path: Option<String>,
    private_key_id: String,
    private_key: String,
    token_uri: String,
}

impl GdchServiceAccountKey {
    fn signer(&self) -> Result<Box<dyn Signer>> {
        let key_provider = CryptoProvider::get_default().map(|p| p.key_provider);
        #[cfg(feature = "default-rustls-provider")]
        let key_provider = key_provider
            .unwrap_or_else(|| rustls::crypto::aws_lc_rs::default_provider().key_provider);
        #[cfg(not(feature = "default-rustls-provider"))]
        let key_provider = key_provider.expect(
            r###"
The default rustls::CryptoProvider should be configured by the application. The
`google-cloud-auth` crate was compiled without the `default-rustls-provider`
feature. Without this feature the crate expects the application to initialize
the rustls crypto provider using `rustls::CryptoProvider::install_default()`.

Note that the application must use the exact same version of `rustls` as the
`google-cloud-auth` crate does. Otherwise `install_default()` has no effect."###,
        );

        let key_der = PrivateKeyDer::from_pem_slice(self.private_key.as_bytes()).map_err(|e| {
            errors::non_retryable_from_str(format!("Failed to parse GDCH private key PEM: {}", e))
        })?;

        let pk = key_provider
            .load_private_key(key_der)
            .map_err(errors::non_retryable)?;

        pk.choose_scheme(&[rustls::SignatureScheme::ECDSA_NISTP256_SHA256])
            .ok_or_else(|| {
                errors::non_retryable_from_str(
                    "Unable to choose ECDSA_NISTP256_SHA256 signing scheme as it is not supported by current signer",
                )
            })
    }
}

impl std::fmt::Debug for GdchServiceAccountKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GdchServiceAccountKey")
            .field("format_version", &self.format_version)
            .field("project", &self.project)
            .field("name", &self.name)
            .field("ca_cert_path", &self.ca_cert_path)
            .field("private_key_id", &self.private_key_id)
            .field("private_key", &"[censored]")
            .field("token_uri", &self.token_uri)
            .finish()
    }
}

#[derive(Debug)]
pub(crate) struct GdchServiceAccountTokenProvider {
    service_account_key: GdchServiceAccountKey,
    audience: String,
}

#[async_trait::async_trait]
impl TokenProvider for GdchServiceAccountTokenProvider {
    async fn token(&self) -> Result<Token> {
        let subject_token = self.generate_subject_token()?;
        let request = TokenRequest {
            grant_type: GRANT_TYPE,
            audience: &self.audience,
            requested_token_type: REQUESTED_TOKEN_TYPE,
            subject_token: &subject_token,
            subject_token_type: SUBJECT_TOKEN_TYPE,
        };
        let client = self.client().await?;
        let response = client
            .post(&self.service_account_key.token_uri)
            .json(&request)
            .send()
            .await
            .map_err(|e| errors::from_http_error(e, MSG))?;

        if !response.status().is_success() {
            return Err(errors::from_http_response(response, MSG).await);
        }

        let response = response
            .json::<TokenResponse>()
            .await
            .map_err(|e| CredentialsError::from_source(!e.is_decode(), e))?;

        let expires_at = Instant::now() + Duration::from_secs(response.expires_in.unwrap_or(3600));
        Ok(Token {
            token: response.access_token,
            token_type: response.token_type,
            expires_at: Some(expires_at),
            metadata: None,
        })
    }
}

impl GdchServiceAccountTokenProvider {
    pub(crate) fn from_json<S: Into<String>>(audience: S, json: Value) -> BuildResult<Self> {
        let cred_type = extract_credential_type(&json)?;
        if cred_type != "gdch_service_account" {
            return Err(BuilderError::not_supported(format!(
                "expected gdch_service_account credentials, found {cred_type}"
            )));
        }
        let service_account_key =
            serde_json::from_value::<GdchServiceAccountKey>(json).map_err(BuilderError::parsing)?;
        if service_account_key.format_version != FORMAT_VERSION {
            return Err(BuilderError::parsing(format!(
                "unsupported gdch_service_account format {:?}",
                service_account_key.format_version
            )));
        }
        Ok(Self {
            service_account_key,
            audience: audience.into(),
        })
    }

    async fn client(&self) -> Result<reqwest::Client> {
        let mut builder = reqwest::Client::builder();
        if let Some(path) = self.service_account_key.ca_cert_path.as_deref() {
            let pem = tokio::fs::read(path).await.map_err(|e| {
                CredentialsError::new(false, "failed to read GDCH CA certificate", e)
            })?;
            let cert = reqwest::Certificate::from_pem(&pem).map_err(|e| {
                CredentialsError::new(false, "failed to parse GDCH CA certificate", e)
            })?;
            builder = builder.add_root_certificate(cert);
        }
        builder
            .build()
            .map_err(|e| CredentialsError::new(false, "failed to create GDCH HTTP client", e))
    }

    fn generate_subject_token(&self) -> Result<String> {
        let signer = self.service_account_key.signer()?;
        let now = time::OffsetDateTime::now_utc();
        let service_identity = format!(
            "system:serviceaccount:{}:{}",
            self.service_account_key.project, self.service_account_key.name
        );
        let claims = GdchClaims {
            iss: &service_identity,
            sub: &service_identity,
            aud: &self.service_account_key.token_uri,
            exp: (now + TOKEN_TIMEOUT).unix_timestamp(),
            iat: now.unix_timestamp(),
        };
        let header = GdchHeader {
            alg: "ES256",
            typ: "JWT",
            kid: &self.service_account_key.private_key_id,
        };
        let encoded_header = encode_json(&header)?;
        let encoded_claims = encode_json(&claims)?;
        let signing_input = format!("{encoded_header}.{encoded_claims}");
        let signature = signer
            .sign(signing_input.as_bytes())
            .map_err(errors::non_retryable)?;
        let signature = ecdsa_der_to_jose(&signature, 32)?;
        let encoded_signature = BASE64_URL_SAFE_NO_PAD.encode(signature);
        Ok(format!("{signing_input}.{encoded_signature}"))
    }
}

#[derive(Serialize)]
struct GdchHeader<'a> {
    alg: &'a str,
    typ: &'a str,
    kid: &'a str,
}

#[derive(Serialize)]
struct GdchClaims<'a> {
    iss: &'a str,
    sub: &'a str,
    aud: &'a str,
    exp: i64,
    iat: i64,
}

#[derive(serde::Deserialize, Serialize)]
struct TokenRequest<'a> {
    #[serde(borrow)]
    grant_type: &'a str,
    #[serde(borrow)]
    audience: &'a str,
    #[serde(borrow)]
    requested_token_type: &'a str,
    #[serde(borrow)]
    subject_token: &'a str,
    #[serde(borrow)]
    subject_token_type: &'a str,
}

#[derive(serde::Deserialize)]
struct TokenResponse {
    access_token: String,
    token_type: String,
    expires_in: Option<u64>,
}

fn encode_json<T>(value: &T) -> Result<String>
where
    T: Serialize,
{
    let json = serde_json::to_string(value).map_err(errors::non_retryable)?;
    Ok(BASE64_URL_SAFE_NO_PAD.encode(json.as_bytes()))
}

fn ecdsa_der_to_jose(der: &[u8], field_len: usize) -> Result<Vec<u8>> {
    let mut pos = 0;
    read_der_tag(der, &mut pos, 0x30)?;
    let seq_len = read_der_len(der, &mut pos)?;
    if pos + seq_len != der.len() {
        return Err(errors::non_retryable_from_str(
            "invalid GDCH ECDSA signature sequence length",
        ));
    }
    let r = read_der_integer(der, &mut pos, field_len)?;
    let s = read_der_integer(der, &mut pos, field_len)?;
    if pos != der.len() {
        return Err(errors::non_retryable_from_str(
            "invalid GDCH ECDSA signature trailing data",
        ));
    }

    let mut jose = Vec::with_capacity(field_len * 2);
    jose.extend_from_slice(&r);
    jose.extend_from_slice(&s);
    Ok(jose)
}

fn read_der_tag(der: &[u8], pos: &mut usize, expected: u8) -> Result<()> {
    if der.get(*pos).copied() != Some(expected) {
        return Err(errors::non_retryable_from_str(
            "invalid GDCH ECDSA signature DER tag",
        ));
    }
    *pos += 1;
    Ok(())
}

fn read_der_len(der: &[u8], pos: &mut usize) -> Result<usize> {
    let Some(first) = der.get(*pos).copied() else {
        return Err(errors::non_retryable_from_str(
            "invalid GDCH ECDSA signature DER length",
        ));
    };
    *pos += 1;
    if first & 0x80 == 0 {
        return Ok(first as usize);
    }

    let len_len = (first & 0x7f) as usize;
    if len_len == 0 || len_len > std::mem::size_of::<usize>() || *pos + len_len > der.len() {
        return Err(errors::non_retryable_from_str(
            "invalid GDCH ECDSA signature DER length",
        ));
    }

    let mut len = 0usize;
    for byte in &der[*pos..*pos + len_len] {
        len = (len << 8) | (*byte as usize);
    }
    *pos += len_len;
    Ok(len)
}

fn read_der_integer(der: &[u8], pos: &mut usize, field_len: usize) -> Result<Vec<u8>> {
    read_der_tag(der, pos, 0x02)?;
    let len = read_der_len(der, pos)?;
    if len == 0 || *pos + len > der.len() {
        return Err(errors::non_retryable_from_str(
            "invalid GDCH ECDSA signature integer",
        ));
    }

    let mut value = &der[*pos..*pos + len];
    *pos += len;
    while value.len() > 1 && value[0] == 0 {
        value = &value[1..];
    }
    if value.len() > field_len {
        return Err(errors::non_retryable_from_str(
            "invalid GDCH ECDSA signature integer length",
        ));
    }

    let mut out = vec![0; field_len - value.len()];
    out.extend_from_slice(value);
    Ok(out)
}

const MSG: &str = "failed to exchange GDCH service account token";

#[cfg(test)]
mod tests {
    use super::*;
    use crate::credentials::tests::b64_decode_to_json;
    use httptest::{Expectation, Server, matchers::*, responders::*};
    use serde_json::json;
    use std::error::Error;

    type TestResult = std::result::Result<(), Box<dyn Error>>;

    const ES256_PRIVATE_KEY: &str = "-----BEGIN EC PRIVATE KEY-----\nMHcCAQEEIEUByN/Cd73iTqf85VeQ74wWaZr6sMnkMY25RvOIUJ94oAoGCCqGSM49\nAwEHoUQDQgAEHf1LlK7P4qdsjslUqKVx5AlEBXN9VLzYYhC700o2DOthBjBFU7Yu\nmohy0DCDBPJ9pfiCPe/lZSFlvdl8Xyz9Lg==\n-----END EC PRIVATE KEY-----\n";

    #[derive(Debug, serde::Deserialize)]
    struct TestTokenRequest {
        grant_type: String,
        audience: String,
        requested_token_type: String,
        subject_token: String,
        subject_token_type: String,
    }

    fn gdch_json(token_uri: String) -> serde_json::Value {
        json!({
            "type": "gdch_service_account",
            "format_version": "1",
            "project": "test-project",
            "private_key_id": "test-private-key-id",
            "private_key": ES256_PRIVATE_KEY,
            "name": "test-name",
            "token_uri": token_uri,
        })
    }

    #[tokio::test]
    async fn token_exchange_success() -> TestResult {
        let audience = "https://example.com/test-audience";
        let expected_audience = audience.to_string();
        let server = Server::run();
        server.expect(
            Expectation::matching(all_of![
                request::method_path("POST", "/authenticate"),
                request::body(json_decoded(move |req: &TestTokenRequest| {
                    req.grant_type == GRANT_TYPE
                        && req.audience == expected_audience
                        && req.requested_token_type == REQUESTED_TOKEN_TYPE
                        && !req.subject_token.is_empty()
                        && req.subject_token_type == SUBJECT_TOKEN_TYPE
                })),
            ])
            .respond_with(json_encoded(json!({
                "access_token": "test-access-token",
                "token_type": "Bearer",
                "expires_in": 3600_u64,
            }))),
        );
        let provider = GdchServiceAccountTokenProvider::from_json(
            audience,
            gdch_json(server.url("/authenticate").to_string()),
        )?;

        let token = provider.token().await?;
        assert_eq!(token.token, "test-access-token");
        assert_eq!(token.token_type, "Bearer");
        Ok(())
    }

    #[test]
    fn generate_subject_token_success() -> TestResult {
        let token_uri = "https://service-accounts.example.com/authenticate".to_string();
        let service_account_key =
            serde_json::from_value::<GdchServiceAccountKey>(gdch_json(token_uri.clone()))?;
        let provider = GdchServiceAccountTokenProvider {
            service_account_key,
            audience: "test-audience".to_string(),
        };

        let token = provider.generate_subject_token()?;
        let parts: Vec<_> = token.split('.').collect();
        assert_eq!(parts.len(), 3);
        let header = b64_decode_to_json(parts[0].to_string());
        assert_eq!(header["alg"], "ES256");
        assert_eq!(header["typ"], "JWT");
        assert_eq!(header["kid"], "test-private-key-id");
        let claims = b64_decode_to_json(parts[1].to_string());
        assert_eq!(
            claims["iss"],
            "system:serviceaccount:test-project:test-name"
        );
        assert_eq!(
            claims["sub"],
            "system:serviceaccount:test-project:test-name"
        );
        assert_eq!(claims["aud"], token_uri);
        Ok(())
    }

    #[test]
    fn non_gdch_adc_type_fails() {
        let err = GdchServiceAccountTokenProvider::from_json(
            "test-audience",
            serde_json::json!({
                "type": "service_account",
            }),
        )
        .unwrap_err();
        assert!(err.is_not_supported(), "{err:?}");
        assert!(err.to_string().contains("service_account"), "{err:?}");
    }
}
