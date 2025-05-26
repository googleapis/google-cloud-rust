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
pub(crate) struct UrlSourcedSubjectTokenProvider {
    pub url: String,
    pub headers: Option<CredentialSourceHeaders>,
    pub format: Option<CredentialSourceFormat>,
}

#[async_trait::async_trait]
impl SubjectTokenProvider for UrlSourcedSubjectTokenProvider {
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
                    .map(String::from);

                match subject_token {
                    Some(token) => Ok(token),
                    None => Err(CredentialsError::from_str(
                        false,
                        format!(
                            "failed to read subject token field `{}` from response: {}",
                            format.subject_token_field_name, json_response
                        ),
                    )),
                }
            }
            None => Ok(response_text),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use httptest::{Expectation, Server, matchers::*, responders::*};
    use serde_json::json;
    use std::collections::HashMap;
    use tokio_test::assert_err;

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
        let token_provider = UrlSourcedSubjectTokenProvider {
            url,
            format: Some(CredentialSourceFormat {
                format_type: "json".to_string(),
                subject_token_field_name: "access_token".to_string(),
            }),
            headers: Some(CredentialSourceHeaders {
                headers: HashMap::from([("Metadata".to_string(), "True".to_string())]),
            }),
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
        let token_provider = UrlSourcedSubjectTokenProvider {
            url,
            format: None,
            headers: None,
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
        let token_provider = UrlSourcedSubjectTokenProvider {
            url,
            format: Some(CredentialSourceFormat {
                format_type: "json".to_string(),
                subject_token_field_name: "access_token".to_string(),
            }),
            headers: Some(CredentialSourceHeaders {
                headers: HashMap::from([("Metadata".to_string(), "True".to_string())]),
            }),
        };
        let err = assert_err!(token_provider.subject_token().await);

        let expected_err = crate::errors::CredentialsError::from_str(
            false,
            "failed to read subject token field `access_token` from response: {\"wrong_field\":\"an_example_token\"}",
        );
        assert_eq!(err.to_string(), expected_err.to_string());

        Ok(())
    }
}
