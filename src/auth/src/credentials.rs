use dirs::home_dir;
use std::env::var;
use std::path::{Path, PathBuf};
use tokio::fs::read_to_string;
use std::result::Result::Ok;
use anyhow::Result;
use http::header::{HeaderName, HeaderValue};
use std::future::Future;

mod user_credential;
use user_credential::UserCredential;

use crate::options::{self, AccessTokenCredentialOptions, AccessTokenCredentialOptions3, AccessTokenCredentialOptionsBuilder, AccessTokenCredentialOptions3Builder};

pub mod traits {
    use http::header::{HeaderName, HeaderValue};
    use std::future::Future;
    use anyhow::Result;

    pub trait Credential: Send + Sync {    
        /// Asynchronously retrieves auth headers.
        ///
        /// This function returns a `Future` that resolves to a `Result` containing
        /// either a vector of key-value pairs representing the headers or an
        /// `AuthError` if an error occurred during header construction.
        fn get_headers(
            &mut self,
        ) -> impl Future<Output = Result<Vec<(HeaderName, HeaderValue)>>> + Send;
    
        /// Retrieves the universe domain associated with the credential, if any.
        fn get_universe_domain(&mut self) -> impl Future<Output = Option<String>> + Send;
    }

    pub(crate) mod dynamic {
        use http::header::{HeaderName, HeaderValue};
        use anyhow::Result;

        #[async_trait::async_trait]
        pub trait Credential: Send + Sync {
            /// Asynchronously retrieves auth headers.
            ///
            /// This function returns a `Future` that resolves to a `Result` containing
            /// either a vector of key-value pairs representing the headers or an
            /// `AuthError` if an error occurred during header construction.
            async fn get_headers(
                &mut self,
            ) -> Result<Vec<(HeaderName, HeaderValue)>>;

            /// Retrieves the universe domain associated with the credential, if any.
            async fn get_universe_domain(&mut self) -> Option<String>;
        }

        #[async_trait::async_trait]
        impl <T: super::Credential> Credential for T {
            async fn get_headers(
                &mut self,
            ) -> Result<Vec<(HeaderName, HeaderValue)>> {
                let headers = self.get_headers().await?;
                Ok(headers)
            }

            async fn get_universe_domain(&mut self) -> Option<String> {
                let universe_domain = self.get_universe_domain().await;
                universe_domain
            }
        }
    }
}

pub struct Credential{
    inner_credential: Box<dyn traits::dynamic::Credential>,
}


impl traits::Credential for Credential {
    fn get_headers(
        &mut self,
    ) -> impl Future<Output = Result<Vec<(HeaderName, HeaderValue)>>> + Send {
        self.inner_credential.get_headers()
    }

    fn get_universe_domain(&mut self) -> impl Future<Output = Option<String>> + Send {
        self.inner_credential.get_universe_domain()
    }
}


pub async fn create_access_token_credential<T>(options: T) -> Result<Credential> 
where 
    T: Into<Option<AccessTokenCredentialOptions>>,
{
    // TODO: If options contain the cred json or file, use that instead of ADC.
    // TODO: Rewrite this ADC code below. It is just a placeholder logic.
    
    let options: Option<AccessTokenCredentialOptions> = options.into();
    let options = match options {
        Some(options) => options,
        None => AccessTokenCredentialOptionsBuilder::new().build().unwrap(),
    };

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

pub async fn create_access_token_credential2<T>(options: T) -> Result<Credential> 
where 
    T: Into<Option<AccessTokenCredentialOptions3>>,
{
    // TODO: If options contain the cred json or file, use that instead of ADC.
    // TODO: Rewrite this ADC code below. It is just a placeholder logic.
    
    let options: Option<AccessTokenCredentialOptions3> = options.into();
    let options = match options {
        Some(options) => options,
        None => AccessTokenCredentialOptions3Builder::default_credential().build().unwrap(),
    };

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

async fn from_file(path: &AdcFilePath) -> Result<Credential> {
    let data = read_to_string(path).await?;
    if let Ok(user_credential) = UserCredential::from_json(&data) {
        return Ok(Credential {
            inner_credential: Box::new(user_credential),
        })
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