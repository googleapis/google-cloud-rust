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

use google_cloud_gax::error::CredentialsError;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

use crate::{
    Result,
    credentials::external_account::CredentialSourceFormat,
    credentials::subject_token::{
        Builder as SubjectTokenBuilder, SubjectToken, SubjectTokenProvider,
    },
    io::{HttpRequest, SharedHttpClientProvider},
};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub(crate) struct UrlSourcedCredentials {
    pub url: String,
    pub headers: HashMap<String, String>,
    pub format: String,
    pub subject_token_field_name: String,
    #[serde(skip)]
    pub http: SharedHttpClientProvider,
}

impl UrlSourcedCredentials {
    pub(crate) fn new(
        url: String,
        headers: Option<HashMap<String, String>>,
        format_source: Option<CredentialSourceFormat>,
        http: SharedHttpClientProvider,
    ) -> Self {
        let (format, subject_token_field_name) = format_source
            .map(|f| {
                (
                    f.format_type,
                    f.subject_token_field_name.unwrap_or_default(),
                )
            })
            .unwrap_or(("text".to_string(), String::new()));
        Self {
            url,
            headers: headers.unwrap_or_default(),
            format,
            subject_token_field_name,
            http,
        }
    }
}

const MSG: &str = "failed to request subject token";
const JSON_FORMAT_TYPE: &str = "json";

impl SubjectTokenProvider for UrlSourcedCredentials {
    type Error = CredentialsError;
    async fn subject_token(&self) -> Result<SubjectToken> {
        let mut request = HttpRequest::get(&self.url);
        for (k, v) in &self.headers {
            request = request.header(k, v);
        }

        let response = self
            .http
            .execute(request)
            .await
            .map_err(|e| crate::errors::from_http_error(e, MSG))?;

        if !response.is_success() {
            return Err(crate::errors::from_http_response(&response, MSG));
        }

        let response_text = response
            .text()
            .map_err(|e| CredentialsError::from_source(false, e))?;

        match self.format.as_str() {
            JSON_FORMAT_TYPE => {
                let json_response: Value = serde_json::from_str(&response_text)
                    .map_err(|e| CredentialsError::from_source(false, e))?;

                match json_response.get(&self.subject_token_field_name) {
                    Some(Value::String(token)) => {
                        Ok(SubjectTokenBuilder::new(token.clone()).build())
                    }
                    None | Some(_) => {
                        let msg = format!(
                            "failed to read subject token field `{}` as string, body=<{}>",
                            self.subject_token_field_name, json_response,
                        );
                        Err(CredentialsError::from_msg(false, msg.as_str()))
                    }
                }
            }
            _ => Ok(SubjectTokenBuilder::new(response_text).build()),
        }
    }
}

#[cfg(test)]
mod tests {
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
            http: SharedHttpClientProvider::default(),
        };
        let resp = token_provider.subject_token().await?;

        assert_eq!(resp.token, "an_example_token".to_string());

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
            http: SharedHttpClientProvider::default(),
        };
        let resp = token_provider.subject_token().await?;

        assert_eq!(resp.token, "an_example_token".to_string());

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
            http: SharedHttpClientProvider::default(),
        };

        let err = token_provider
            .subject_token()
            .await
            .expect_err("parsing should fail");

        assert!(!err.is_transient(), "{err:?}");
        assert!(err.source().is_none(), "{:?}", err.source());

        assert!(err.to_string().contains("`access_token`"), "{err:?}");
        assert!(
            err.to_string()
                .contains("{\"wrong_field\":\"an_example_token\"}"),
            "{err:?}"
        );

        Ok(())
    }
}
