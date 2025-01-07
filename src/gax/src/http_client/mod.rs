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

use crate::error::Error;
use crate::error::HttpError;
use crate::Result;
use auth::credentials::{create_access_token_credential, Credential};

#[derive(Clone)]
pub struct ReqwestClient {
    inner: reqwest::Client,
    cred: Credential,
    endpoint: String,
}

impl ReqwestClient {
    pub async fn new(config: ClientConfig, default_endpoint: &str) -> Result<Self> {
        let inner = reqwest::Client::new();
        let cred = if let Some(c) = config.cred {
            c
        } else {
            create_access_token_credential()
                .await
                .map_err(Error::authentication)?
        };
        let endpoint = config
            .endpoint
            .unwrap_or_else(|| default_endpoint.to_string());
        Ok(Self {
            inner,
            cred,
            endpoint,
        })
    }

    pub fn builder(&self, method: reqwest::Method, path: String) -> reqwest::RequestBuilder {
        self.inner
            .request(method, format!("{}{path}", &self.endpoint))
    }

    pub async fn execute<I: serde::ser::Serialize, O: serde::de::DeserializeOwned>(
        &self,
        mut builder: reqwest::RequestBuilder,
        body: Option<I>,
        options: crate::options::RequestOptions,
    ) -> Result<O> {
        let auth_headers = self
            .cred
            .get_headers()
            .await
            .map_err(Error::authentication)?;
        for header in auth_headers.into_iter() {
            builder = builder.header(header.0, header.1);
        }
        if let Some(user_agent) = options.user_agent() {
            builder = builder.header(
                reqwest::header::USER_AGENT,
                reqwest::header::HeaderValue::from_str(user_agent).map_err(Error::other)?,
            );
        }
        if let Some(timeout) = options.attempt_timeout() {
            builder = builder.timeout(*timeout);
        }
        if let Some(body) = body {
            builder = builder.json(&body);
        }
        let response = builder.send().await.map_err(Error::io)?;
        if !response.status().is_success() {
            let status = response.status().as_u16();
            let headers = crate::error::convert_headers(response.headers());
            let body = response.bytes().await.map_err(Error::io)?;
            return Err(HttpError::new(status, headers, Some(body)).into());
        }
        let response = response.json::<O>().await.map_err(Error::serde)?;
        Ok(response)
    }
}

impl std::fmt::Debug for ReqwestClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        f.debug_struct("ReqwestClient")
            .field("endpoint", &self.endpoint)
            .finish()
    }
}

#[derive(serde::Serialize)]
pub struct NoBody {}

pub type ClientConfig = crate::options::ClientConfig;
