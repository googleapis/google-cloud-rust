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
use crate::errors::CredentialError;
use std::path::Path;
use std::fs::File;
use std::io::Read;
use async_trait::async_trait;


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
    file_path: String,    
}

#[async_trait]
#[allow(dead_code)]
impl TokenProvider for ServiceAccountTokenProvider {
    async fn get_token(&mut self) -> Result<Token> {
        // read service account json file.
        // create a signer based on private key
        // use the private key there to create a self signed jwt.
        todo!()
    }
}

impl ServiceAccountTokenProvider {
    async fn from_service_account_file<P: AsRef<Path>>(&mut self) -> Result<ServiceAccountInfo> {
        //reads the file and returns back ServiceAccountInfo object
        todo!()
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
