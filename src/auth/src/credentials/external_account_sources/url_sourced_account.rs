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

use gax::error::CredentialsError;
use reqwest::Client;
use serde_json::Value;
use std::time::Duration;

use crate::{
    Result,
    credentials::external_account::{
        CredentialSourceFormat, CredentialSourceHeaders, SubjectTokenProvider,
    },
};

#[derive(Debug)]
pub(crate) struct UrlSourcedCredentials {
    pub url: String,
    pub headers: Option<CredentialSourceHeaders>,
    pub format: Option<CredentialSourceFormat>,
}

#[async_trait::async_trait]
impl SubjectTokenProvider for UrlSourcedCredentials {
    async fn subject_token(&self) -> Result<String> {
        let client = Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .unwrap();

        let mut request = client.get(self.url.clone());

        if let Some(headers) = &self.headers {
            for (key, value) in &headers.headers {
                request = request.header(key.as_str(), value.as_str());
            }
        }

        let response = request.send().await.map_err(|err| {
            CredentialsError::from_str(false, format!("failed to request subject token: {}", err))
        })?;

        if !response.status().is_success() {
            return Err(CredentialsError::from_str(
                false,
                "failed to request subject token",
            ));
        }

        let response_text = response.text().await.map_err(|err| {
            CredentialsError::from_str(
                false,
                format!("failed to read subject token response: {}", err),
            )
        })?;

        match &self.format {
            Some(format) => {
                let json_response: Value = serde_json::from_str(&response_text).unwrap();
                let subject_token = json_response
                    .get(&format.subject_token_field_name)
                    .and_then(Value::as_str)
                    .map(String::from)
                    .unwrap();
                Ok(subject_token)
            }
            None => Ok(response_text),
        }
    }
}
