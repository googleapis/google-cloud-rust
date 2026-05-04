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

//! [Google Distributed Cloud] service identity authentication.

use crate::Result;
use crate::credentials::errors::CredentialsError;
use base64::prelude::{BASE64_URL_SAFE_NO_PAD, Engine as _};
use rustls::crypto::CryptoProvider;
use rustls::sign::Signer;
use rustls_pki_types::PrivateKeyDer;
use rustls_pki_types::pem::PemObject;
use serde::Deserialize;
use std::time::{SystemTime, UNIX_EPOCH};

/// Represents a Google Distributed Cloud service account key.
#[derive(Deserialize, Clone)]
struct GdchServiceAccountKey {
    /// The credential type, must be "gdch_service_account".
    #[serde(rename = "type")]
    #[allow(dead_code)]
    cred_type: String,
    /// The format version of the JSON file.
    format_version: String,
    /// The project ID.
    project: String,
    /// The ID of the private key.
    private_key_id: String,
    /// The PEM-encoded private key (SEC1 format).
    private_key: String,
    /// The name of the service identity.
    name: String,
    /// Optional path to custom CA certificate for TLS verification.
    #[allow(dead_code)]
    ca_cert_path: Option<String>,
    /// The URI to exchange the JWT for a token.
    token_uri: String,
}

impl GdchServiceAccountKey {
    #[allow(dead_code)]
    fn signer(&self) -> std::result::Result<Box<dyn Signer>, CredentialsError> {
        let private_key = self.private_key.clone();
        let key_provider = CryptoProvider::get_default().map(|p| p.key_provider);
        #[cfg(feature = "default-rustls-provider")]
        let key_provider = key_provider
            .unwrap_or_else(|| rustls::crypto::aws_lc_rs::default_provider().key_provider);
        #[cfg(not(feature = "default-rustls-provider"))]
        let key_provider = key_provider
            .expect("The default rustls::CryptoProvider should be configured by the application.");

        let key_der = PrivateKeyDer::from_pem_slice(private_key.as_bytes()).map_err(|e| {
            CredentialsError::from_msg(
                false,
                format!(
                    "Failed to parse GDCH service account private key PEM: {}",
                    e
                ),
            )
        })?;

        let pk = key_provider
            .load_private_key(key_der)
            .map_err(|e| CredentialsError::from_source(false, e))?;

        pk.choose_scheme(&[rustls::SignatureScheme::ECDSA_NISTP256_SHA256])
            .ok_or_else(|| {
                CredentialsError::from_msg(
                    false,
                    "Unable to choose ECDSA_NISTP256_SHA256 signing scheme as it is not supported by current signer",
                )
            })
    }
}

impl std::fmt::Debug for GdchServiceAccountKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GdchServiceAccountKey")
            .field("type", &self.cred_type)
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

/// A token provider for Google Distributed Cloud service accounts.
#[derive(Debug)]
#[allow(dead_code)]
struct GdchServiceAccountTokenProvider {
    #[allow(dead_code)]
    audience: String,
    key: GdchServiceAccountKey,
}

impl GdchServiceAccountTokenProvider {
    /// Creates a new token provider with the given key and audience.
    #[allow(dead_code)]
    fn new(audience: String, key: GdchServiceAccountKey) -> Self {
        Self { audience, key }
    }

    #[allow(dead_code)]
    fn generate_subject_token(&self) -> Result<String> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| CredentialsError::from_source(false, e))?
            .as_secs();
        let exp = now + 3600; // 1 hour

        let header = serde_json::json!({
            "alg": "ES256",
            "typ": "JWT",
            "kid": self.key.private_key_id,
        });

        let iss = format!(
            "system:serviceaccount:{}:{}",
            self.key.project, self.key.name
        );
        let claims = serde_json::json!({
            "iss": iss,
            "sub": iss,
            "aud": self.key.token_uri,
            "iat": now,
            "exp": exp,
        });

        let encoded_header = BASE64_URL_SAFE_NO_PAD.encode(serde_json::to_string(&header).unwrap());
        let encoded_claims = BASE64_URL_SAFE_NO_PAD.encode(serde_json::to_string(&claims).unwrap());

        let to_sign = format!("{}.{}", encoded_header, encoded_claims);

        let signer = self.key.signer()?;

        let sig_der = signer
            .sign(to_sign.as_bytes())
            .map_err(|e| CredentialsError::from_source(false, e))?;
        let sig = p256::ecdsa::Signature::from_der(&sig_der).map_err(|e| {
            CredentialsError::from_msg(false, format!("failed to parse ecdsa DER signature: {}", e))
        })?;
        let encoded_sig = BASE64_URL_SAFE_NO_PAD.encode(&sig.to_bytes()[..]);

        Ok(format!("{}.{}", to_sign, encoded_sig))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn get_mock_key() -> GdchServiceAccountKey {
        GdchServiceAccountKey {
            cred_type: "gdch_service_account".to_string(),
            format_version: "1".to_string(),
            project: "test-project".to_string(),
            private_key_id: "test-key-id".to_string(),
            private_key: (*crate::credentials::tests::ES256_PEM).clone(),
            name: "test-name".to_string(),
            ca_cert_path: None,
            token_uri: "http://localhost/token".to_string(),
        }
    }

    #[test]
    fn debug_gdch_service_account_key() {
        let key = get_mock_key();
        let fmt = format!("{key:?}");
        assert!(fmt.contains("GdchServiceAccountKey"));
        assert!(fmt.contains("test-project"));
        assert!(fmt.contains("test-name"));
        assert!(fmt.contains("test-key-id"));
        assert!(fmt.contains("[censored]"));
        assert!(!fmt.contains(crate::credentials::tests::ES256_PEM.as_str()));
    }

    #[test]
    fn parse_valid_json() {
        let json = json!({
            "type": "gdch_service_account",
            "format_version": "1",
            "project": "test-project",
            "private_key_id": "test-key-id",
            "private_key": crate::credentials::tests::ES256_PEM.as_str(),
            "name": "test-name",
            "token_uri": "http://localhost/token"
        });

        let key: GdchServiceAccountKey = serde_json::from_value(json).unwrap();
        assert_eq!(key.cred_type, "gdch_service_account");
        assert_eq!(key.project, "test-project");
    }

    #[test]
    fn generate_subject_token() {
        let key = get_mock_key();
        let provider = GdchServiceAccountTokenProvider::new("test-audience".to_string(), key);
        let jwt = provider.generate_subject_token().unwrap();

        let parts: Vec<&str> = jwt.split('.').collect();
        assert_eq!(parts.len(), 3);

        let header = String::from_utf8(BASE64_URL_SAFE_NO_PAD.decode(parts[0]).unwrap()).unwrap();
        let claims = String::from_utf8(BASE64_URL_SAFE_NO_PAD.decode(parts[1]).unwrap()).unwrap();

        let header_json: serde_json::Value = serde_json::from_str(&header).unwrap();
        let claims_json: serde_json::Value = serde_json::from_str(&claims).unwrap();

        assert_eq!(header_json["alg"], "ES256");
        assert_eq!(header_json["typ"], "JWT");
        assert_eq!(header_json["kid"], "test-key-id");

        assert_eq!(
            claims_json["iss"],
            "system:serviceaccount:test-project:test-name"
        );
        assert_eq!(
            claims_json["sub"],
            "system:serviceaccount:test-project:test-name"
        );
        assert_eq!(claims_json["aud"], "http://localhost/token");
    }
}
