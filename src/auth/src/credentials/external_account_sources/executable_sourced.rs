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

use crate::{
    Result,
    constants::{ACCESS_TOKEN_TYPE, JWT_TOKEN_TYPE, SAML2_TOKEN_TYPE},
    credentials::external_account::SubjectTokenProvider,
};
use gax::error::CredentialsError;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::{process::Command, time::timeout as tokio_timeout};

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

/// Executable command should adere to this format.
/// Format is documented on [executable source credentials].
///
/// [executable sourced credentials]: https://google.aip.dev/auth/4117#determining-the-subject-token-in-executable-sourced-credentials
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
struct ExecutableResponse {
    version: i32,
    success: bool,
    token_type: String,
    expiration_time: i64,
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
// default timeout is defined by AIP-4117
const DEFAULT_TIMEOUT_SECS: u32 = 30;
const ALLOW_EXECUTABLE_ENV: &str = "GOOGLE_EXTERNAL_ACCOUNT_ALLOW_EXECUTABLES";

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
                timeout_millis,
                ..
            } => {
                let timeout = match timeout_millis {
                    Some(timeout) => Duration::from_millis(timeout.into()),
                    None => Duration::from_secs(DEFAULT_TIMEOUT_SECS.into()),
                };
                Self::from_command(command, timeout).await
            }
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

    /// See details on security reason on [executable sourced credentials].
    /// [executable sourced credentials]: https://google.aip.dev/auth/4117#determining-the-subject-token-in-executable-sourced-credentials
    async fn from_command(command: String, timeout: Duration) -> Result<String> {
        // For security reasons, we need our consumers to set this environment variable to allow executables to be run.
        let allow_executable = std::env::var(ALLOW_EXECUTABLE_ENV)
            .ok()
            .unwrap_or("0".to_string());
        if allow_executable != "1" {
            return Err(CredentialsError::from_msg(
                false,
                "executables need to be explicitly allowed (set GOOGLE_EXTERNAL_ACCOUNT_ALLOW_EXECUTABLES to '1') to run",
            ));
        }

        let (command, args) = Self::split_command(command);
        let output = Command::new(command.clone()).args(&args).output();
        let output = tokio_timeout(timeout, output.into_future())
            .await
            .map_err(|e| CredentialsError::from_source(true, e))?
            .map_err(|e| CredentialsError::from_source(true, e))?;

        if !output.status.success() {
            let stderr = String::from_utf8(output.stderr)
                .map_err(|e| CredentialsError::from_source(false, e))?;
            let msg = format!("{MSG}, command execution failed, stderr=<{stderr}>");
            if let Some(code) = output.status.code() {
                let msg = format!("{msg}, code={code}");
                return Err(CredentialsError::from_msg(true, msg));
            };
            return Err(CredentialsError::from_msg(true, msg));
        }

        let subject_token = String::from_utf8(output.stdout)
            .map_err(|e| CredentialsError::from_source(true, e))?
            .to_string();

        Ok(subject_token)
    }

    fn split_command(command: String) -> (String, Vec<String>) {
        let mut parts = command.split_whitespace();

        if let Some(command) = parts.next() {
            let args: Vec<String> = parts.map(String::from).collect();
            return (command.to_string(), args);
        }

        (command, vec![])
    }

    fn parse_token(output: String) -> Result<String> {
        let res = serde_json::from_str::<ExecutableResponse>(output.as_str())
            .map_err(|e| CredentialsError::from_source(false, e))?;

        if !res.success {
            return Err(res.to_cred_error());
        }

        match res.token_type.as_str() {
            JWT_TOKEN_TYPE | ACCESS_TOKEN_TYPE => match res.id_token {
                Some(id_token) => Ok(id_token),
                None => Err(CredentialsError::from_msg(
                    false,
                    "missing `id_token` field",
                )),
            },
            SAML2_TOKEN_TYPE => match res.saml_response {
                Some(saml_response) => Ok(saml_response),
                None => Err(CredentialsError::from_msg(
                    false,
                    "missing `saml_response` field",
                )),
            },
            _ => Err(CredentialsError::from_msg(
                false,
                "contains unsupported token type",
            )),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::constants::JWT_TOKEN_TYPE;
    use scoped_env::ScopedEnv;
    use serde_json::json;
    use serial_test::serial;
    use tokio::time::{Duration, Instant};

    type TestResult = std::result::Result<(), Box<dyn std::error::Error>>;

    #[tokio::test]
    #[serial]
    async fn read_token_from_command() -> TestResult {
        let _e = ScopedEnv::set(ALLOW_EXECUTABLE_ENV, "1");
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
    #[serial]
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

    #[cfg(not(target_os = "windows"))]
    #[tokio::test]
    #[serial]
    async fn read_token_command_timeout() -> TestResult {
        use std::error::Error;
        use std::os::unix::fs::PermissionsExt;

        let _e = ScopedEnv::set(ALLOW_EXECUTABLE_ENV, "1");

        let file_contents = "#!/bin/bash
while true;
do
    echo \"working\"
done";
        let file = tempfile::NamedTempFile::new().unwrap();
        let path = file.into_temp_path();
        std::fs::write(&path, file_contents).expect("Unable to write to temp file with command");
        let mut perms = std::fs::metadata(&path)
            .expect("Unable to get temp file metadata")
            .permissions();
        perms.set_mode(0o700);
        std::fs::set_permissions(&path, perms).expect("Unable to set exec permission");

        let token_provider = ExecutableSourcedCredentials {
            executable: ExecutableConfig {
                command: Some(path.to_str().unwrap().into()),
                timeout_millis: Some(1000),
                ..ExecutableConfig::default()
            },
        };
        let err = token_provider
            .subject_token()
            .await
            .expect_err("should fail with timeout");

        println!("{err:?}");
        assert!(err.is_transient());
        assert!(err.source().is_some());

        let source_err = err.source().unwrap();
        println!("{source_err:?}");
        assert!(source_err.to_string().contains("deadline"));

        Ok(())
    }
}
