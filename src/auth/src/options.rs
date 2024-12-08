use std::time::Duration;

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct AccessTokenCredentialOptions {
    #[serde(rename = "Scopes", skip_serializing_if = "Option::is_none")]
    pub scopes: Option<Vec<String>>,

    #[serde(rename = "Audience", skip_serializing_if = "Option::is_none")]
    pub audience: Option<String>,

    #[serde(rename = "Subject", skip_serializing_if = "Option::is_none")]
    pub subject: Option<String>, 

    #[serde(rename = "EarlyTokenRefresh", skip_serializing_if = "Option::is_none")]
    pub early_token_refresh: Option<Duration>,

    #[serde(rename = "DisableAsyncRefresh")]
    pub disable_async_refresh: bool,

    #[serde(rename = "TokenUrl", skip_serializing_if = "Option::is_none")]
    pub token_url: Option<String>,

    #[serde(rename = "StsAudience", skip_serializing_if = "Option::is_none")]
    pub sts_audience: Option<String>,

    #[serde(rename = "CredentialsFile", skip_serializing_if = "Option::is_none")]
    pub credentials_file: Option<String>,

    #[serde(rename = "CredentialsJSON", skip_serializing_if = "Option::is_none")]
    pub credentials_json: Option<String>,

    #[serde(rename = "UniverseDomain", skip_serializing_if = "Option::is_none")]
    pub universe_domain: Option<String>,
}

impl AccessTokenCredentialOptions {
    pub fn to_json(&self) -> String {
        serde_json::to_string(self).unwrap()
    }

    pub fn default() -> Self {
        AccessTokenCredentialOptions {
            scopes: None,
            audience: None,
            subject: None,
            early_token_refresh: None,
            disable_async_refresh: false,
            token_url: None,
            sts_audience: None,
            credentials_file: None,
            credentials_json: None,
            universe_domain: None,
        }
    }
}