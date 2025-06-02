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
use tokio::process::Command;

use crate::{Result, credentials::external_account::SubjectTokenProvider};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub(crate) struct ExecutableSourcedCredentials {
    pub executable: ExecutableConfig,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub(crate) struct ExecutableConfig {
    pub command: Option<String>,
    pub timeout_millis: Option<u32>,
    pub output_file: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
struct ExecutableResponse {
    version: i32,
    success: bool,
    token_type: Option<String>,
    expiration_time: Option<i64>,
    id_token: Option<String>,
    saml_response: Option<String>,
    code: Option<String>,
    message: Option<String>,
}

impl ExecutableResponse {
    fn to_cred_error(&self) -> CredentialsError {
        match &self {
            ExecutableResponse {
                message: Some(message),
                code: Some(code),
                ..
            } => {
                let msg =
                    format!("{MSG}, response contains unsuccessful response: ({code}) {message}");
                CredentialsError::from_msg(false, msg)
            }
            _ => {
                let msg = format!(
                    "{MSG}, response must include `code` and `message` fields when unsuccessful"
                );
                CredentialsError::from_msg(false, msg)
            }
        }
    }
}

const MSG: &str = "failed to read subject token";
// const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30); TODO: enforce timeout

#[async_trait::async_trait]
impl SubjectTokenProvider for ExecutableSourcedCredentials {
    async fn subject_token(&self) -> Result<String> {
        let output = match self.executable.clone() {
            ExecutableConfig {
                output_file: Some(output_file),
                ..
            } => Self::from_output_file(output_file).await,
            ExecutableConfig {
                command: Some(command),
                ..
            } => Self::from_command(command).await,
            _ => Err(CredentialsError::from_msg(
                false,
                format!("{MSG}, either `output_file` or `command` needs to be informed"),
            )),
        }?;

        let output = output.trim().to_string();
        let subject_token = Self::parse_token(output)?;

        if subject_token.is_empty() {
            let msg = format!("{MSG}, subject token is empty");
            return Err(CredentialsError::from_msg(false, msg));
        }

        Ok(subject_token)
    }
}

impl ExecutableSourcedCredentials {
    async fn from_output_file(output_file: String) -> Result<String> {
        let content = std::fs::read_to_string(output_file)
            .map_err(|e| CredentialsError::from_source(false, e))?;
        Ok(content)
    }

    async fn from_command(command: String) -> Result<String> {
        let (command, args) = Self::split_command(command);
        let output = Command::new(command.clone())
            .args(&args)
            .output()
            .await
            .map_err(|e| CredentialsError::from_source(false, e))?;
        // let output = timeout(DEFAULT_TIMEOUT, output); // TODO: enforce timeout

        if !output.status.success() {
            let stderr = String::from_utf8(output.stderr)
                .map_err(|e| CredentialsError::from_source(false, e))?;
            let msg = format!("{MSG}, command execution failed, stderr=<{stderr}>");
            return Err(CredentialsError::from_msg(false, msg));
        }

        let subject_token = String::from_utf8(output.stdout)
            .map_err(|e| CredentialsError::from_source(false, e))? // TODO: add more details and better handling
            .to_string();

        Ok(subject_token)
    }

    fn split_command(command: String) -> (String, Vec<String>) {
        let mut parts = command.split_whitespace();

        let command = parts.next().unwrap(); // TODO: remove unwrap
        let args: Vec<String> = parts.map(String::from).collect();

        (command.to_string(), args)
    }

    fn parse_token(output: String) -> Result<String> {
        let res = serde_json::from_str::<ExecutableResponse>(output.as_str())
            .map_err(|e| CredentialsError::from_source(false, e))?; // TODO: Add details on expected format

        if !res.success {
            return Err(res.to_cred_error());
        }

        if let Some(id_token) = res.id_token {
            return Ok(id_token);
        }

        if let Some(saml_response) = res.saml_response {
            return Ok(saml_response);
        }

        Err(CredentialsError::from_msg(
            false,
            "contains unsupported token type",
        ))
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::credentials::internal::sts_exchange::JWT_TOKEN_TYPE;
    use serde_json::json;
    use tokio::time::{Duration, Instant};

    type TestResult = std::result::Result<(), Box<dyn std::error::Error>>;

    #[tokio::test]
    async fn read_token_from_command() -> TestResult {
        let expiration = (Instant::now() + Duration::from_secs(3600))
            .elapsed()
            .as_millis();
        let json_response = json!({
            "success": true,
            "version": 1,
            "expiration_time": expiration,
            "token_type": JWT_TOKEN_TYPE,
            "id_token":"an_example_token",
        })
        .to_string();
        let file = tempfile::NamedTempFile::new().unwrap();
        let path = file.into_temp_path();
        std::fs::write(&path, json_response).expect("Unable to write to temp file with command");

        let token_provider = ExecutableSourcedCredentials {
            executable: ExecutableConfig {
                command: Some(format!("cat {}", path.to_str().unwrap())),
                ..ExecutableConfig::default()
            },
        };
        let resp = token_provider.subject_token().await?;

        assert_eq!(resp, "an_example_token".to_string());

        Ok(())
    }

    #[tokio::test]
    async fn read_token_from_output_file() -> TestResult {
        let expiration = (Instant::now() + Duration::from_secs(3600))
            .elapsed()
            .as_millis();
        let json_response = json!({
            "success": true,
            "version": 1,
            "expiration_time": expiration,
            "token_type": JWT_TOKEN_TYPE,
            "id_token":"an_example_token",
        })
        .to_string();
        let file = tempfile::NamedTempFile::new().unwrap();
        let path = file.into_temp_path();
        std::fs::write(&path, json_response).expect("Unable to write to temp file with command");

        let token_provider = ExecutableSourcedCredentials {
            executable: ExecutableConfig {
                output_file: Some(path.to_str().unwrap().into()),
                ..ExecutableConfig::default()
            },
        };
        let resp = token_provider.subject_token().await?;

        assert_eq!(resp, "an_example_token".to_string());

        Ok(())
    }
}
