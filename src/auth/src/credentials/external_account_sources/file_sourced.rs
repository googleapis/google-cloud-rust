
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
    pub(crate) fn new(
        file: String,
        format_source: Option<CredentialSourceFormat>,
    ) -> Self {
        let (format, subject_token_field_name) = format_source
            .map(|f| (f.format_type, f.subject_token_field_name))
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
