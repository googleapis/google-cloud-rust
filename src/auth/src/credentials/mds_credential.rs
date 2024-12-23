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
use crate::errors::{is_retryable, CredentialError};
use crate::token::{Token, TokenProvider};
use http::header::{HeaderName, HeaderValue, AUTHORIZATION};
use reqwest::{Client, Url, Response};
use std::collections::HashMap;
use reqwest::header::HeaderMap;
use lazy_static::lazy_static;
use std::env;
use serde_json::{Value, from_str};
use async_trait::async_trait;




const METADATA_FLAVOR_VALUE: &str = "Google";
const METADATA_FLAVOR: &str = "metadata-flavor";
const CONTENT_TYPE: &str = "Content-Type";

lazy_static! {
    // Use lazy_static to initialize the metadata URLs.
    static ref _METADATA_ROOT: String = format!(
        "http://{}/computeMetadata/v1/",
        env::var("GCE_METADATA_HOST").unwrap_or_else(|_| {
            env::var("GCE_METADATA_ROOT").unwrap_or_else(|_| "metadata.google.internal".to_string())
        })
    );
}

#[allow(dead_code)] // TODO(#442) - implementation in progress
pub(crate) struct MDSCredential<T>  where T:TokenProvider{
    token_provider: T,
}

#[async_trait::async_trait]
impl<T> Credential for MDSCredential<T>
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

#[allow(dead_code)] // TODO(#442) - implementation in progress
pub struct MDSAccessTokenProvider {
    pub service_account_email: String,
    pub scopes: Option<Vec<String>>,
}

impl MDSAccessTokenProvider {

    pub async fn get(
        &self,
        request: &Client,
        path: &str,
        params: Option<HashMap<&str, &str>>,
        recursive: bool,
        headers: Option<HeaderMap>,
    ) -> Result<Value> {
    
        let base_url: Url = Url::parse(&_METADATA_ROOT)
            .unwrap()
            .join(path)
            .unwrap();
    
        let mut query_params = params.unwrap_or_default();
    
    
        let mut headers_to_use = HeaderMap::new();
        headers_to_use.insert(METADATA_FLAVOR, HeaderValue::from_static(METADATA_FLAVOR_VALUE));
    
        if let Some(custom_headers) = headers {
            headers_to_use.extend(custom_headers);
        }
    
        if recursive {
            query_params.insert("recursive", "true");
        }
    
    
        let url = reqwest::Url::parse_with_params(base_url.as_str(), query_params.iter()).map_err(|e| CredentialError::new(false, e.into()))?;     
    
        let response: Response = request
            .get(url.clone())
            .headers(headers_to_use.clone())
            .send()
            .await
            .map_err(|e| CredentialError::new(false, e.into()))?;    
    
        let status = response.status();
        let headers = response.headers().clone();
        let content = response
            .text()
            .await
            .map_err(|e| CredentialError::new(false, e.into()))?;
        if !status.is_success() {
            return Err(CredentialError::new(
                is_retryable(status),
                Box::from(format!("{content}")),
            ));
        }
    
        let content_type = headers
            .get(CONTENT_TYPE)
            .and_then(|v| v.to_str().ok());
    
        
        if let Some(ct) = content_type {
            if ct.contains("application/json") {
                let value: Value = from_str(&content).map_err(|e| CredentialError::new(false, e.into()))?;
                return Ok(value);
            } else {
                return Err(CredentialError::new(
                    false,
                    Box::from(format!("{content}")),
                ));
            }
        }
    
        Ok(Value::String(content))
    }    
    
    pub async fn get_service_account_info(
        &self,
        request: &Client,
        service_account_email: Option<String>,
    ) -> Result<Value> {
        let service_account_email: String = service_account_email.clone().unwrap_or("default".to_string());
        let path:String = format!("instance/service-accounts/{}/", service_account_email);
        let mut params = HashMap::new();
        params.insert("recursive", "true");
        self.get(request, &path, Some(params), false, None).await
    }
}

#[async_trait]
impl TokenProvider for MDSAccessTokenProvider {
    async fn get_token(&mut self) -> Result<Token> {
        todo!()
    }
}


