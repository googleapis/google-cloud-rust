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
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    Result,
    credentials::external_account::CredentialSourceFormat,
    credentials::subject_token::{
        Builder as SubjectTokenBuilder, SubjectToken, SubjectTokenProvider,
    },
};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub(crate) struct FileSourcedCredentials {
    pub file: String,
    pub format: String,
    pub subject_token_field_name: String,
}

impl FileSourcedCredentials {
    pub(crate) fn new(file: String, format_source: Option<CredentialSourceFormat>) -> Self {
        let (format, subject_token_field_name) = format_source
            .map(|f| {
                (
                    f.format_type,
                    f.subject_token_field_name.unwrap_or_default(),
                )
            })
            .unwrap_or(("text".to_string(), String::new()));
        Self {
            file,
            format,
            subject_token_field_name,
        }
    }
}

const JSON_FORMAT_TYPE: &str = "json";

impl SubjectTokenProvider for FileSourcedCredentials {
    type Error = CredentialsError;
    async fn subject_token(&self) -> Result<SubjectToken> {
        let content = std::fs::read_to_string(&self.file)
            .map_err(|e| CredentialsError::from_source(false, e))?;

        match self.format.as_str() {
            JSON_FORMAT_TYPE => {
                let json_response: Value = serde_json::from_str(&content)
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
            _ => Ok(SubjectTokenBuilder::new(content).build()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::{error::Error, io::Write};
    use tempfile::NamedTempFile;

    type TestResult = std::result::Result<(), Box<dyn std::error::Error>>;

    fn create_temp_file(content: &str) -> std::io::Result<NamedTempFile> {
        let mut file = NamedTempFile::new()?;
        file.write_all(content.as_bytes())?;
        Ok(file)
    }

    #[tokio::test]
    async fn get_text_token() -> TestResult {
        let file = create_temp_file("an_example_token")?;
        let token_provider = FileSourcedCredentials {
            file: file.path().to_str().unwrap().to_string(),
            format: "text".into(),
            subject_token_field_name: "".into(),
        };
        let resp = token_provider.subject_token().await?;
        assert_eq!(resp.token, "an_example_token".to_string());
        Ok(())
    }

    #[tokio::test]
    async fn get_json_token() -> TestResult {
        let response_body = json!({
            "access_token":"an_example_token",
        })
        .to_string();
        let file = create_temp_file(&response_body)?;
        let token_provider = FileSourcedCredentials {
            file: file.path().to_str().unwrap().to_string(),
            format: "json".into(),
            subject_token_field_name: "access_token".into(),
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
        let file = create_temp_file(&response_body)?;
        let token_provider = FileSourcedCredentials {
            file: file.path().to_str().unwrap().to_string(),
            format: "json".into(),
            subject_token_field_name: "access_token".into(),
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

    #[tokio::test]
    async fn file_not_found() -> TestResult {
        let token_provider = FileSourcedCredentials {
            file: "/path/to/non/existent/file".to_string(),
            format: "text".into(),
            subject_token_field_name: "".into(),
        };
        let err = token_provider
            .subject_token()
            .await
            .expect_err("file should not exist");
        assert!(!err.is_transient(), "{err:?}");
        assert!(err.source().is_some());
        Ok(())
    }

    #[tokio::test]
    async fn get_text_token_from_empty_file() -> TestResult {
        let file = create_temp_file("")?;
        let token_provider = FileSourcedCredentials {
            file: file.path().to_str().unwrap().to_string(),
            format: "text".into(),
            subject_token_field_name: "".into(),
        };
        let resp = token_provider.subject_token().await?;
        assert_eq!(resp.token, "".to_string());
        Ok(())
    }

    #[tokio::test]
    async fn get_json_token_from_empty_file() -> TestResult {
        let file = create_temp_file("")?;
        let token_provider = FileSourcedCredentials {
            file: file.path().to_str().unwrap().to_string(),
            format: "json".into(),
            subject_token_field_name: "access_token".into(),
        };
        let err = token_provider
            .subject_token()
            .await
            .expect_err("parsing should fail");
        assert!(!err.is_transient(), "{err:?}");
        assert!(err.source().is_some());
        Ok(())
    }
}
