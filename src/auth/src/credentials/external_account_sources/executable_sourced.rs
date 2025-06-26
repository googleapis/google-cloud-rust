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
    credentials::external_account::{ExecutableConfig, SubjectTokenProvider},
};
use gax::error::CredentialsError;
use serde::{Deserialize, Serialize};
use std::{
    fmt::{Display, Formatter},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{process::Command, time::timeout as tokio_timeout};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub(crate) struct ExecutableSourcedCredentials {
    pub command: String,
    pub args: Vec<String>,
    pub timeout: Duration,
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
    /// 3rd party subject token expiration time in seconds (unix epoch time).
    expiration_time: Option<i64>,
    id_token: Option<String>,
    saml_response: Option<String>,
    code: Option<String>,
    message: Option<String>,
}

/// Represents an error when executing a command line tool to fetch subject tokens.
#[derive(Debug)]
pub struct ExecutionError {
    code: Option<String>,
    message: Option<String>,
}

impl ExecutionError {
    fn new(code: Option<String>, message: Option<String>) -> Self {
        Self { code, message }
    }

    fn from_executable_response(res: ExecutableResponse) -> CredentialsError {
        CredentialsError::from_source(true, Self::new(res.code, res.message))
    }
}

impl std::error::Error for ExecutionError {}

impl Display for ExecutionError {
    /// Formats the error message to include retryability and source.
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self {
                code: Some(code),
                message: Some(message),
            } => write!(
                f,
                "{MSG}, response contains unsuccessful response, code=<{code}>, message=<{message}>",
            ),
            _ => write!(
                f,
                "{MSG}, response must include `code` and `message` fields when unsuccessful"
            ),
        }
    }
}

const MSG: &str = "failed to read subject token";
// default timeout is defined per AIP-4117
const DEFAULT_TIMEOUT_SECS: Duration = Duration::from_secs(30);
const ALLOW_EXECUTABLE_ENV: &str = "GOOGLE_EXTERNAL_ACCOUNT_ALLOW_EXECUTABLES";

#[async_trait::async_trait]
impl SubjectTokenProvider for ExecutableSourcedCredentials {
    async fn subject_token(&self) -> Result<String> {
        if let Some(output_file) = self.output_file.clone() {
            let token = Self::from_output_file(output_file).await;
            if token.is_ok() {
                return token;
            }
        }
        let token =
            Self::from_command(self.command.clone(), self.args.clone(), self.timeout).await?;
        if token.is_empty() {
            let msg = format!("{MSG}, subject token is empty");
            return Err(CredentialsError::from_msg(false, msg));
        }

        Ok(token)
    }
}

impl ExecutableSourcedCredentials {
    pub(crate) fn new(executable: ExecutableConfig) -> Self {
        let (command, args) = Self::split_command(executable.command);
        let timeout = match executable.timeout_millis {
            Some(timeout) => Duration::from_millis(timeout.into()),
            None => DEFAULT_TIMEOUT_SECS,
        };
        let output_file = executable.output_file;
        Self {
            command,
            args,
            timeout,
            output_file,
        }
    }

    async fn from_output_file(output_file: String) -> Result<String> {
        let content = std::fs::read_to_string(output_file)
            .map_err(|e| CredentialsError::from_source(false, e))?;

        Self::parse_token(content)
    }

    /// See details on security reason on [executable sourced credentials].
    /// [executable sourced credentials]: https://google.aip.dev/auth/4117#determining-the-subject-token-in-executable-sourced-credentials
    async fn from_command(command: String, args: Vec<String>, timeout: Duration) -> Result<String> {
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

        let output = String::from_utf8(output.stdout)
            .map_err(|e| CredentialsError::from_source(true, e))?
            .to_string();

        Self::parse_token(output)
    }

    /// Parses a full command string into a command and its arguments.    
    fn split_command(command: String) -> (String, Vec<String>) {
        let mut parts = command.split_whitespace();
        let Some(cmd) = parts.next() else {
            return (command, vec![]);
        };
        let args: Vec<String> = parts.map(String::from).collect();

        (cmd.to_string(), args)
    }

    /// Executable and output file should follow the format defined in [ExecutableResponse] struct.
    /// This tries to parse the output in the given format and extract just the OAuth token.
    fn parse_token(output: String) -> Result<String> {
        let res = serde_json::from_str::<ExecutableResponse>(output.as_str())
            .map_err(|e| CredentialsError::from_source(false, e))?;

        if !res.success {
            return Err(ExecutionError::from_executable_response(res));
        }

        if let Some(expiration_time) = res.expiration_time {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|e| CredentialsError::from_source(false, e))?
                .as_secs() as i64;
            if expiration_time < now {
                return Err(CredentialsError::from_msg(
                    true,
                    "the token returned by the executable is expired",
                ));
            }
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
    use std::error::Error;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};
    use test_case::test_case;

    type TestResult = anyhow::Result<(), Box<dyn std::error::Error>>;

    #[test_case("/bin/cat /tmp/file", "/bin/cat", vec!["/tmp/file"]; "command with single arg")]
    #[test_case("gcloud auth print-access-token", "gcloud", vec!["auth", "print-access-token"]; "command with multiple args")]
    #[test_case("./single-command", "./single-command", vec![]; "command without args")]
    fn parse_commands(input: &str, expected_command: &str, expected_args: Vec<&str>) -> TestResult {
        let (command, args) = ExecutableSourcedCredentials::split_command(input.to_string());

        assert_eq!(command, expected_command);
        assert_eq!(args, expected_args);

        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn read_token_from_command() -> TestResult {
        let _e = ScopedEnv::set(ALLOW_EXECUTABLE_ENV, "1");
        let expiration = SystemTime::now().duration_since(UNIX_EPOCH)?;
        let expiration = expiration + Duration::from_secs(3600);
        let expiration = expiration.as_secs();
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

        let config = ExecutableConfig {
            command: format!("cat {}", path.to_str().unwrap()),
            ..ExecutableConfig::default()
        };
        let token_provider = ExecutableSourcedCredentials::new(config);
        let resp = token_provider.subject_token().await?;

        assert_eq!(resp, "an_example_token".to_string());

        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn fail_with_tmp_dir_spaces() -> TestResult {
        let _e = ScopedEnv::set(ALLOW_EXECUTABLE_ENV, "1");
        let json_response = json!({
            "success": true,
            "version": 1,
            "token_type": JWT_TOKEN_TYPE,
            "id_token":"an_example_token",
        })
        .to_string();
        let file = tempfile::NamedTempFile::with_suffix("with space").unwrap();
        let path = file.into_temp_path();
        std::fs::write(&path, json_response).expect("Unable to write to temp file with command");

        let path = path.to_str().unwrap();
        let config = ExecutableConfig {
            command: format!("cat {path}"),
            ..ExecutableConfig::default()
        };
        let token_provider = ExecutableSourcedCredentials::new(config);
        let err = token_provider
            .subject_token()
            .await
            .expect_err("should fail to handle file with spaces");

        assert!(err.to_string().contains("No such file or directory"));

        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn read_valid_token_from_output_file() -> TestResult {
        let expiration = SystemTime::now().duration_since(UNIX_EPOCH)?;
        let expiration = expiration + Duration::from_secs(3600);
        let expiration = expiration.as_secs();
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

        let config = ExecutableConfig {
            output_file: Some(path.to_str().unwrap().into()),
            command: "do nothing".to_string(),
            ..ExecutableConfig::default()
        };
        let token_provider = ExecutableSourcedCredentials::new(config);
        let resp = token_provider.subject_token().await?;

        assert_eq!(resp, "an_example_token");

        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn fallback_to_command_invalid_token_from_output_file() -> TestResult {
        let _e = ScopedEnv::set(ALLOW_EXECUTABLE_ENV, "1");
        let invalid_input = json!({
            "success": false,
            "version": 1,
            "expiration_time": 0,
            "token_type": JWT_TOKEN_TYPE,
            "id_token":"failed exec",
        })
        .to_string();
        let invalid_file = tempfile::NamedTempFile::new().unwrap();
        let invalid_path = invalid_file.into_temp_path();
        std::fs::write(&invalid_path, invalid_input)
            .expect("Unable to write to temp file with command");

        let now = SystemTime::now().duration_since(UNIX_EPOCH)?;
        let valid_expiration = now + Duration::from_secs(3600);
        let valid_expiration = valid_expiration.as_secs();
        let valid_json_response = json!({
            "success": true,
            "version": 1,
            "expiration_time": valid_expiration,
            "token_type": JWT_TOKEN_TYPE,
            "id_token":"a_valid_token",
        })
        .to_string();
        let valid_file = tempfile::NamedTempFile::new().unwrap();
        let valid_path = valid_file.into_temp_path();
        std::fs::write(&valid_path, valid_json_response)
            .expect("Unable to write to temp file with command");

        let config = ExecutableConfig {
            output_file: Some(invalid_path.to_str().unwrap().into()),
            command: format!("cat {}", valid_path.to_str().unwrap()),
            ..ExecutableConfig::default()
        };
        let token_provider = ExecutableSourcedCredentials::new(config);
        let resp = token_provider.subject_token().await?;

        assert_eq!(resp, "a_valid_token");

        Ok(())
    }

    const EXPIRED_TIME_SENTINEL: i64 = 1;
    const VALID_TIME_SENTINEL: i64 = 2;

    #[test_case(json!({
            "success": true,
            "version": 1,
            "expiration_time": EXPIRED_TIME_SENTINEL,
            "token_type": JWT_TOKEN_TYPE,
            "id_token":"expired_token",
        }), "expired"; "expired token")]
    #[test_case(json!({
            "success": false,
            "code": "1",
            "message": "failed",
            "version": 1,
            "expiration_time": VALID_TIME_SENTINEL,
            "token_type": JWT_TOKEN_TYPE,
            "id_token":"failed_to_gen_token",
        }), "code=<1>, message=<failed>" ; "failed to generate token")]
    #[test_case(json!({
            "success": true,
            "version": 1,
            "expiration_time": VALID_TIME_SENTINEL,
            "token_type": JWT_TOKEN_TYPE,
            "saml_response":"missing_id_token",
        }), "missing `id_token`"; "missing_id_token")]
    #[test_case(json!({
            "success": true,
            "version": 1,
            "expiration_time": VALID_TIME_SENTINEL,
            "token_type": SAML2_TOKEN_TYPE,
            "id_token":"missing_saml2_token",
        }), "missing `saml_response`"; "missing_saml2_token")]
    #[serial]
    #[tokio::test]
    async fn parse_invalid_token(mut input: serde_json::Value, err_msg: &str) -> TestResult {
        let _e = ScopedEnv::set(ALLOW_EXECUTABLE_ENV, "1");

        let now = SystemTime::now().duration_since(UNIX_EPOCH)?;
        let expiration_value = input
            .get_mut("expiration_time")
            .expect("missing expiration time");
        let expiration = match expiration_value.as_i64().unwrap() {
            VALID_TIME_SENTINEL => (now + Duration::from_secs(3600)).as_secs() as i64,
            EXPIRED_TIME_SENTINEL => (now - Duration::from_secs(3600)).as_secs() as i64,
            t => t,
        };
        *expiration_value = expiration.into();

        let invalid_json_response = input.to_string();
        let err = ExecutableSourcedCredentials::parse_token(invalid_json_response)
            .expect_err("parsing should fail");

        let err = match err.source() {
            Some(source_err) => source_err.to_string(),
            None => err.to_string(),
        };
        assert!(err.contains(err_msg), "{err}");

        Ok(())
    }

    #[cfg(not(target_os = "windows"))]
    #[tokio::test]
    #[serial]
    async fn read_token_command_timeout() -> TestResult {
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

        let config = ExecutableConfig {
            command: path.to_str().unwrap().into(),
            timeout_millis: Some(1000),
            ..ExecutableConfig::default()
        };
        let token_provider = ExecutableSourcedCredentials::new(config);
        let err = token_provider
            .subject_token()
            .await
            .expect_err("should fail with timeout");

        assert!(err.is_transient());
        assert!(err.source().is_some());

        let source_err = err.source().unwrap();
        assert!(source_err.to_string().contains("deadline"));

        Ok(())
    }
}
