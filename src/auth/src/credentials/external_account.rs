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

use std::collections::HashMap;
use std::sync::Arc;

use crate::credentials::Result;
use crate::credentials::internal::sts_exchange::ClientAuthentication;
use crate::headers_util::build_bearer_headers;
use crate::token::{CachedTokenProvider, Token, TokenProvider};
use crate::token_cache::TokenCache;
use gax::error::CredentialsError;
use http::{Extensions, HeaderMap};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::time::{Duration, Instant};

use super::Credentials;
use super::dynamic::CredentialsProvider;
use super::external_account_sources::url_sourced_account::UrlSourcedCredentials;
use super::internal::sts_exchange::{ExchangeTokenRequest, STSHandler};

#[async_trait::async_trait]
pub(crate) trait SubjectTokenProvider: std::fmt::Debug + Send + Sync {
    /// Generate subject token that will be used on STS exchange.
    async fn subject_token(&self) -> Result<String>;
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CredentialSourceFormat {
    #[serde(rename = "type")]
    pub format_type: String,
    pub subject_token_field_name: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CredentialSourceHeaders {
    #[serde(flatten)]
    pub headers: HashMap<String, String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CredentialSource {
    pub url: Option<String>,
    pub headers: Option<CredentialSourceHeaders>,
    pub format: Option<CredentialSourceFormat>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ExternalAccountConfig {
    pub audience: String,
    pub subject_token_type: String,
    pub token_url: String,
    pub client_id: Option<String>,
    pub client_secret: Option<String>,
    pub credential_source: CredentialSource,
}

#[derive(Debug)]
struct ExternalAccountTokenProvider {
    subject_token_provider: Box<dyn SubjectTokenProvider>,
    config: ExternalAccountConfig,
}

impl ExternalAccountTokenProvider {
    pub fn new(config: ExternalAccountConfig) -> Result<ExternalAccountTokenProvider> {
        let subject_token_provider = subject_token_provider_from_config(config.clone())?;
        Ok(Self {
            subject_token_provider,
            config,
        })
    }
}

/// Detect which subject token provider implementation to use
fn subject_token_provider_from_config(
    config: ExternalAccountConfig,
) -> Result<Box<dyn SubjectTokenProvider>> {
    if let Some(url) = config.credential_source.url {
        let creds = UrlSourcedCredentials {
            url,
            headers: config.credential_source.headers,
            format: config.credential_source.format,
        };
        return Ok(Box::new(creds));
    }

    Err(CredentialsError::from_str(
        false,
        "unable to parse credential source",
    ))
}

#[async_trait::async_trait]
impl TokenProvider for ExternalAccountTokenProvider {
    async fn token(&self) -> Result<Token> {
        let subject_token = self.subject_token_provider.subject_token().await?;

        let audience = self.config.audience.clone();
        let subject_token_type = self.config.subject_token_type.clone();
        let url = self.config.token_url.clone();
        let req = ExchangeTokenRequest {
            url,
            audience: Some(audience),
            subject_token,
            subject_token_type,
            authentication: ClientAuthentication {
                client_id: self.config.client_id.clone(),
                client_secret: self.config.client_secret.clone(),
            },
            ..ExchangeTokenRequest::default()
        };

        let token_res = STSHandler::exchange_token(req).await?;

        let token = Token {
            token: token_res.access_token,
            token_type: token_res.token_type,
            expires_at: Some(Instant::now() + Duration::from_secs(token_res.expires_in)),
            metadata: None,
        };
        Ok(token)
    }
}

#[derive(Debug)]
pub(crate) struct ExternalAccountCredentials<T>
where
    T: CachedTokenProvider,
{
    token_provider: T,
    quota_project_id: Option<String>,
}

pub fn new(external_account_options: Value) -> Result<Credentials> {
    let options: ExternalAccountConfig = serde_json::from_value(external_account_options).unwrap();
    let token_provider = ExternalAccountTokenProvider::new(options)?;
    let credentials = ExternalAccountCredentials {
        token_provider: TokenCache::new(token_provider),
        quota_project_id: None,
    };
    Ok(Credentials {
        inner: Arc::new(credentials),
    })
}

#[async_trait::async_trait]
impl<T> CredentialsProvider for ExternalAccountCredentials<T>
where
    T: CachedTokenProvider,
{
    async fn headers(&self, extensions: Extensions) -> Result<HeaderMap> {
        let token = self.token_provider.token(extensions).await?;
        build_bearer_headers(&token, &self.quota_project_id)
    }
}

#[cfg(test)]
mod test {
    // use super::*;
}
