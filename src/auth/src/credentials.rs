use dirs::home_dir;
use std::collections::HashMap;
use std::env::var;
use std::path::{Path, PathBuf};
use tokio::fs::read_to_string;
use std::result::Result::Ok;
use async_trait::async_trait;


mod user_credential;
use user_credential::UserCredential;

use crate::options::AccessTokenCredentialOptions;

#[async_trait]
pub trait Credential: Send + Sync {
    async fn get_token(&mut self) -> Result<crate::token::Token, anyhow::Error>;
    async fn get_headers(&mut self) -> Result<HashMap<String, String>, anyhow::Error> {
        let token = self.get_token().await?;
        let mut headers = HashMap::<String, String>::new();
        headers.insert("Authorization".to_string(), format!("{} {}", token.token_type, token.token));
        Ok(headers)
    }
    fn get_quota_project_id(&self) -> Result<String, anyhow::Error>;
    fn get_universe_domain(&self) -> Result<String, anyhow::Error>;
}

pub async fn create_access_token_credential(_: Option<AccessTokenCredentialOptions>) -> Result<Box<dyn Credential>, anyhow::Error> {
    // TODO: If options contain the cred json or file, use that instead of ADC.
    // TODO: Rewrite this ADC code below. It is just a placeholder logic.
    
    let credential_env = var("GOOGLE_APPLICATION_CREDENTIALS");
    let adc_path = {
        if let Ok(credential_env) = credential_env {
            AdcFilePath::try_from(credential_env)
        } else {
            AdcFilePath::default()
        }
    };
    if let Ok(path) = adc_path {
        let credential = from_file(&path).await?;
        return Ok(credential);
    }
    
    Err(anyhow::anyhow!(format!(
        "Could not create a credential."
    )))
}

async fn from_file(path: &AdcFilePath) -> Result<Box<dyn Credential>, anyhow::Error> {
    let data = read_to_string(path).await?;
    if let Ok(user_credential) = UserCredential::from_json(&data) {
        return Ok(Box::new(user_credential))
    }

    Err(anyhow::anyhow!(format!(
        "Could not create a credential from {:?}",
        path
    )))
}

#[derive(Debug)]
struct AdcFilePath(Box<PathBuf>);
impl AdcFilePath {
    fn default() -> Result<Self, anyhow::Error> {
        if let Some(home) = home_dir() {
            let p = home
                .join(".config")
                .join("gcloud")
                .join("application_default_credentials.json");
            if !p.exists() || !p.is_file() {
                return Err(anyhow::anyhow!(
                    "ADC file path does not exist or is not a file."
                ));
            }
            return Ok(AdcFilePath(p.into()));
        }
        Err(anyhow::anyhow!(
            "Could not find an ADC file in the gcloud config directory."
        ))
    }
}
impl TryFrom<String> for AdcFilePath {
    // TODO: Make the error type correct.
    type Error = anyhow::Error;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        let p = Path::new(&value).to_owned();
        if !p.exists() || !p.is_file() {
            return Err(anyhow::anyhow!(
                "ADC file path does not exist or is not a file."
            ));
        }
        Ok(AdcFilePath(Box::new(p)))
    }
}
impl AsRef<Path> for AdcFilePath {
    fn as_ref(&self) -> &Path {
        &self.0
    }
}