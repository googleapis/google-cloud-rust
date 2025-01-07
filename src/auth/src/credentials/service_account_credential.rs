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

use crate::credentials::traits::dynamic::Credential;
use crate::credentials::Result;
use crate::token::{Token, TokenProvider};
use http::header::{HeaderName, HeaderValue, AUTHORIZATION};
use rustls::crypto::aws_lc_rs::sign;
use crate::errors::CredentialError;
use std::path::Path;
use std::fs::File;
use std::io::Read;
use async_trait::async_trait;
use rustls::sign::Signer;
use rustls_pemfile::Item;
use serde::Serialize;
use std::time::Duration;
use time::OffsetDateTime;



const DEFAULT_TOKEN_TIMEOUT: Duration = Duration::from_secs(3600);
/// JSON Web Signature for a token.
#[derive(Serialize)]
struct JwsClaims<'a> {
    pub iss: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scope: Option<&'a str>,
    pub aud: &'a str,
    pub exp: Option<i64>,
    pub iat: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub typ: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sub: Option<&'a str>,
}

#[allow(dead_code)] // Implementation in progress
struct ServiceAccountInfo {
    client_email: String,
    private_key_id: String,
    private_key: String,
    auth_uri: String,
    token_uri: String,
    project_id: String,
    universe_domain: String,
}

#[allow(dead_code)] // Implementation in progress
pub(crate) struct ServiceAccountCredential<T>
where
    T: TokenProvider,
{
    token_provider: T,
}

#[allow(dead_code)] // TODO(#442) - implementation in progress
struct ServiceAccountTokenProvider {
    service_account_info: ServiceAccountInfo,
}

#[async_trait]
#[allow(dead_code)]
impl TokenProvider for ServiceAccountTokenProvider {
    async fn get_token(&mut self) -> Result<Token> {
        let info = self.from_service_account_file().await?;
        let signer = self.signer(&info)?;
        let mut claims = JwsClaims {
            iss: info.client_email.as_str(),
            aud: info.token_uri.as_str(),
            exp: None,
            iat: None,
            sub: None,
            typ: None,
        };
        let header = DEFAULT_HEADER;

        let ss = format!("{}.{}", header.encode()?, claims.encode()?);
        let sig = signer
            .sign(ss.as_bytes())
            .map_err(|e| CredentialError::new(false, e.into()))?;
        // use the private key there to create a self signed jwt.
        let tt = String::from_utf8(sig).map_err(|e| CredentialError::new(false, e.into()))?;
        let token = Token {
            token: tt,
            token_type: "jwt",
            expires_at: OffsetDateTime::now_utc() + DEFAULT_TOKEN_TIMEOUT,
            metadata: None,
        };
        Ok(token)
    }
}

impl ServiceAccountTokenProvider {
    async fn from_service_account_file<P: AsRef<Path>>(&mut self) -> Result<ServiceAccountInfo> {
        //reads the file and returns back ServiceAccountInfo object
        todo!()
    }
        // Creates a signer using the private key stored in the service account file.
    fn signer(&self, service_account_info: &ServiceAccountInfo) -> Result<Box<dyn Signer>> {
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
impl<T> Credential for ServiceAccountCredential<T>
where
    T: TokenProvider,
{
    async fn get_token(&mut self) -> Result<Token> {
        self.token_provider.get_token().await
    }

    async fn get_headers(&mut self) -> Result<Vec<(HeaderName, HeaderValue)>> {
        let token = self.get_token().await?;
        let mut value = HeaderValue::from_str(&format!("{} {}", token.token_type, token.token))
            .map_err(|e| CredentialError::new(false, e.into()))?;
        value.set_sensitive(true);
        Ok(vec![(AUTHORIZATION, value)])
    }

    async fn get_universe_domain(&mut self) -> Option<String> {
        Some("googleapis.com".to_string())
    }
}
