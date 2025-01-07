// Copyright 2024 Google LLC
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
use crate::credentials::Result;
use crate::token::{Token, TokenProvider};
use http::header::{HeaderName, HeaderValue, AUTHORIZATION};
use rustls::crypto::aws_lc_rs::sign;
use crate::errors::CredentialError;
use std::path::Path;
use async_trait::async_trait;
use rustls::sign::Signer;
use rustls_pemfile::Item;
use serde::Serialize;
use std::time::Duration;
use time::OffsetDateTime;
use crate::credentials::jws::{JwsClaims, JwsHeader};




const DEFAULT_TOKEN_TIMEOUT: Duration = Duration::from_secs(3600);
const DEFAULT_HEADER: JwsHeader = JwsHeader {
    alg: "RS256",
    typ: "JWT",
    kid: None,
};

/// A representation of a Service Account File. See [Service Account Keys](https://google.aip.dev/auth/4112)
/// for more details.
#[allow(dead_code)] // Implementation in progress
#[derive(serde::Deserialize)]
struct ServiceAccountKeyFile {
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
struct ServiceAccountTokenProvider {
    file_path: String,
}

#[async_trait]
#[allow(dead_code)]
impl TokenProvider for ServiceAccountTokenProvider {
    async fn get_token(&self) -> Result<Token> {
        let service_account_info = Self::from_file(&self.file_path).await?;
        let signer = self.signer(&service_account_info);

        let mut claims = JwsClaims {
            iss: service_account_info.client_email.as_str(),
            aud: service_account_info.token_uri.as_str(),
            scope: None,
            exp: None,
            iat: None,
            sub: None,
            typ: None,
        };
        let header = DEFAULT_HEADER;

        let ss = format!("{}.{}", header.encode()?, claims.encode()?);
        let sig = signer?
            .sign(ss.as_bytes())
            .map_err(|e| CredentialError::new(false, e.into()))?;
        let token = String::from_utf8(sig).map_err(|e| CredentialError::new(false, e.into()))?;
        let token = Token {
            token: token,
            token_type: "jwt".to_string(),
            expires_at: Some(OffsetDateTime::now_utc() + DEFAULT_TOKEN_TIMEOUT),
            metadata: None,
        };
        Ok(token)
    }
}

impl ServiceAccountTokenProvider {
    async fn from_file(
        path: impl AsRef<Path>,
    ) -> Result<ServiceAccountKeyFile> {
        // todo!()
        let sa: ServiceAccountKeyFile =
            serde_json::from_slice(&tokio::fs::read(path).await.map_err(|e| CredentialError::new(false, e.into()))?)
                .map_err(|e| CredentialError::new(false, e.into()))?;
        Ok(sa)
    }

    // Creates a signer using the private key stored in the service account file.
    fn signer(&self, service_account_info: &ServiceAccountKeyFile) -> Result<Box<dyn Signer>> {
        let crypto_provider = rustls::crypto::CryptoProvider::get_default()
            .ok_or_else(|| CredentialError::new(false, Box::from("unable to get crypto provider")))?;


        let key_provider = crypto_provider.key_provider;

        let pk = rustls_pemfile::read_one(&mut service_account_info.private_key.as_bytes())
        .map_err(|e| CredentialError::new(false, e.into()))?
            .ok_or_else(|| {
                CredentialError::new(false,Box::from("unable to parse service account key"))
            })?;
        let pk = match pk {
            Item::Pkcs1Key(item) => key_provider.load_private_key(item.into()),
            Item::Pkcs8Key(item) => key_provider.load_private_key(item.into()),
            other => {
                return Err(CredentialError::new(false, Box::from(
                    format!(
                        "expected key to be in form of RSA or PKCS8, found {:?}",
                        other
                    )
                )))
            }
        };
        let sk = pk.map_err(|e| {
            CredentialError::new(false, Box::from("unable to create signing key"))
        })?;
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
