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
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::HashMap, time::Duration};

use crate::{
    Result, credentials::external_account::CredentialSourceFormat,
    credentials::external_account::dynamic::SubjectTokenProvider, errors,
};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub(crate) struct UrlSourcedCredentials {
    pub url: String,
    pub headers: HashMap<String, String>,
    pub format: String,
    pub subject_token_field_name: String,
}

impl UrlSourcedCredentials {
    pub(crate) fn new(
        url: String,
        headers: Option<HashMap<String, String>>,
        format_source: Option<CredentialSourceFormat>,
    ) -> Self {
        let (format, subject_token_field_name) = format_source
            .map(|f| (f.format_type, f.subject_token_field_name))
            .unwrap_or(("text".to_string(), String::new()));
        Self {
            url,
            headers: headers.unwrap_or_default(),
            format,
            subject_token_field_name,
        }
    }
}

const MSG: &str = "failed to request subject token";
const JSON_FORMAT_TYPE: &str = "json";

#[async_trait::async_trait]
impl SubjectTokenProvider for UrlSourcedCredentials {
    async fn subject_token(&self) -> Result<String> {
        let client = Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .unwrap();

        let request = client.get(self.url.clone());
        let request = self
            .headers
            .iter()
            .fold(request, |r, (k, v)| r.header(k.as_str(), v.as_str()));

        let response = request
            .send()
            .await
            .map_err(|e| errors::from_http_error(e, MSG))?;

        if !response.status().is_success() {
            let err = errors::from_http_response(response, MSG).await;
            return Err(err);
        }

        let response_text = response.text().await.map_err(|e| {
            let retryable = !e.is_body();
            CredentialsError::from_source(retryable, e)
        })?;

        match self.format.as_str() {
            JSON_FORMAT_TYPE => {
                let json_response: Value = serde_json::from_str(&response_text)
                    .map_err(|e| CredentialsError::from_source(false, e))?;

                match json_response.get(&self.subject_token_field_name) {
                    Some(Value::String(token)) => Ok(token.clone()),
                    None | Some(_) => {
                        let msg = format!(
                            "failed to read subject token field `{}` as string, body=<{}>",
                            self.subject_token_field_name, json_response,
                        );
                        Err(CredentialsError::from_msg(false, msg.as_str()))
                    }
                }
            }
            _ => Ok(response_text),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use httptest::{Expectation, Server, matchers::*, responders::*};
    use serde_json::json;
    use std::{collections::HashMap, error::Error};

    type TestResult = std::result::Result<(), Box<dyn std::error::Error>>;

    #[tokio::test]
    async fn get_json_token() -> TestResult {
        let response_body = json!({
            "access_token":"an_example_token",
        })
        .to_string();

        let server = Server::run();
        server.expect(
            Expectation::matching(all_of![
                request::method_path("GET", "/token"),
                request::headers(contains(("metadata", "True"))),
            ])
            .respond_with(status_code(200).body(response_body)),
        );

        let url = server.url("/token").to_string();
        let token_provider = UrlSourcedCredentials {
            url,
            format: "json".into(),
            subject_token_field_name: "access_token".into(),
            headers: HashMap::from([("Metadata".to_string(), "True".to_string())]),
        };
        let resp = token_provider.subject_token().await?;

        assert_eq!(resp, "an_example_token".to_string());

        Ok(())
    }

    #[tokio::test]
    async fn get_text_token() -> TestResult {
        let response_body = "an_example_token".to_string();

        let server = Server::run();
        server.expect(
            Expectation::matching(all_of![request::method_path("GET", "/token"),])
                .respond_with(status_code(200).body(response_body)),
        );

        let url = server.url("/token").to_string();
        let token_provider = UrlSourcedCredentials {
            url,
            format: "text".into(),
            subject_token_field_name: "".into(),
            headers: HashMap::new(),
        };
        let resp = token_provider.subject_token().await?;

        assert_eq!(resp, "an_example_token".to_string());

        Ok(())
    }

    #[tokio::test]
    async fn get_json_token_missing_field() -> TestResult {
        let response_body = json!({
            "wrong_field":"an_example_token",
        })
        .to_string();

        let server = Server::run();
        server.expect(
            Expectation::matching(all_of![
                request::method_path("GET", "/token"),
                request::headers(contains(("metadata", "True"))),
            ])
            .respond_with(status_code(200).body(response_body)),
        );

        let url = server.url("/token").to_string();
        let token_provider = UrlSourcedCredentials {
            url,
            format: "json".into(),
            subject_token_field_name: "access_token".into(),
            headers: HashMap::from([("Metadata".to_string(), "True".to_string())]),
        };

        let err = token_provider
            .subject_token()
            .await
            .expect_err("parsing should fail");

        assert!(!err.is_transient(), "{err:?}");
        assert!(err.source().is_none());

        assert!(err.to_string().contains("`access_token`"), "{err:?}");
        assert!(
            err.to_string()
                .contains("{\"wrong_field\":\"an_example_token\"}"),
            "{err:?}"
        );

        Ok(())
    }
}
