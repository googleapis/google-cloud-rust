use std::{collections::HashMap, future::ready};

use anyhow::Result;
use std::future::Future;
use http::header::{HeaderName, HeaderValue, AUTHORIZATION};
use serde::{Deserialize, Serialize};
use super::Credential;
use crate::token::{Token, TokenCache, TokenProvider};
use reqwest::{Client, Method, Request, Response};
use async_trait::async_trait;

const OAUTH2_ENDPOINT: &str = "https://oauth2.googleapis.com/token";


pub(crate) struct UserCredential {
    quota_project_id: Option<String>,
    universe_domain: String,

    // TODO: make the caching configurable so that in future
    // we can have logic in FFI shim to implement a different
    // caching mechanism involving std::thread
    token_provider: Box<dyn TokenProvider>,
}


impl UserCredential {
    pub(crate) fn from_json(data: &str) -> Result<Self, anyhow::Error> {
        let user_credential_json: std::result::Result<UserCredentialJSON, serde_json::Error> =
            serde_json::from_str(&data);

        let user_credential_json = match user_credential_json {
            Ok(user_credential_json) => {
                if user_credential_json.cred_type != "authorized_user" {
                    return Err(anyhow::anyhow!(format!(
                        "Unknown credential type. {:?}",
                        user_credential_json.cred_type
                    )));
                }

                user_credential_json
            },
            Err(err) => {
                let auth_error = gax::error::Error::authentication(err);
                
                return Err(anyhow::anyhow!(format!(
                    "Could not parse the AuthorizedUser JSON. {:?}",
                    auth_error
                )))
            }
        };
        
        let user_access_token_provider = UserAccessTokenProvider {
            client_id: user_credential_json.client_id,
            client_secret: user_credential_json.client_secret,
            refresh_token: user_credential_json.refresh_token,
            token_url: "https://oauth2.googleapis.com/token".to_string(),
        };

        let token_cache =
            TokenCache::new(user_access_token_provider);

        Ok(
            UserCredential {
                quota_project_id: user_credential_json.quota_project_id,
                universe_domain: user_credential_json.universe_domain.unwrap_or("googleapis.com".to_string()),
                token_provider: Box::new(token_cache),
        })
    }

}

#[async_trait]
impl crate::credentials::traits::dynamic::Credential for UserCredential {  
    async fn get_headers(&mut self) -> Result<Vec<(HeaderName, HeaderValue)>> {
        let token = self.token_provider.get_token().await?;
        let headers_vec =  vec![
            (
                AUTHORIZATION,
                HeaderValue::from_str(&format!("{} {}", token.token_type, token.token))?
            )];
        Ok(headers_vec)
    }


    async fn get_universe_domain(&mut self) -> Option<String> {
        Some(self.universe_domain.clone())
    }
}

pub struct UserCredentialBuilder {
    quota_project_id: Option<String>,
    universe_domain: String,
    token_url: Option<String>,
    scopes: Option<Vec<String>>,
    client_id: String,
    client_secret: String,
    refresh_token: String,
}

impl UserCredentialBuilder {
    pub fn from_json<T: Into<String>>(data: T) -> Result<Self, anyhow::Error>  {
        Ok(UserCredentialBuilder {
            quota_project_id: None,
            universe_domain: "googleapis.com".to_string(),
            token_url: None,
            scopes: None,
            client_id: "".to_string(),
            client_secret: "".to_string(),
            refresh_token: "".to_string(),
        })
    }

    pub fn token_url(&mut self, token_url: String) -> &mut Self {
        self.token_url = Some(token_url);
        self
    }

    pub fn quota_project_id(&mut self, quota_project_id: String) -> &mut Self {
        self.quota_project_id = Some(quota_project_id);
        self
    }

    pub fn scopes(&mut self, scopes: Vec<String>) -> &mut Self {
        self.scopes = Some(scopes);
        self
    }

    pub fn build(&self) -> Result<Credential> {
        let token_provier: UserAccessTokenProvider = UserAccessTokenProvider {
            token_url: self.token_url.clone().unwrap_or("https://oauth2.googleapis.com/token".to_string()),
            client_id: self.client_id.clone(),
            client_secret: self.client_secret.clone(),
            refresh_token: self.refresh_token.clone(),
        };

        let token_cache = TokenCache::new(token_provier);

        Ok(
            Credential{
              inner_credential: Box::new(
                UserCredential {
                    quota_project_id: self.quota_project_id.clone(),
                    universe_domain: self.universe_domain.clone(),
                    token_provider: Box::new(token_cache),
                })
            }
        )
    }
}



// impl From<UserCredential> for UserCredentialBuilder {
//     fn from(user_credential: UserCredential) -> Self {
//         UserCredentialBuilder {
//             quota_project_id: user_credential.quota_project_id,
//             universe_domain: user_credential.universe_domain,
//             user_access_token_provider_builder: UserAccessTokenProviderBuilder::new(), // too: convert tokencache's to tokenprovider builder
//         }
//     }
// }

pub struct UserAccessTokenProvider {
    client_id: String,
    client_secret: String,
    refresh_token: String,
    token_url: String,
}

impl UserAccessTokenProvider {
    async fn prepare_token_request(&self, client: &Client) -> Result<Request, anyhow::Error> {
        let refresh_body = Oauth2RefreshRequest {
            grant_type: RefreshGrantType::RefreshToken,
            client_id: &self.client_id,
            client_secret: &self.client_secret,
            refresh_token: &self.refresh_token,
        };

        let body = serde_json::to_string(&refresh_body)?;
        let header = HeaderValue::try_from("application/json")?;
        let builder = client.request(Method::POST, OAUTH2_ENDPOINT);
        let req = builder.body(body).header("Content-Type", header).build()?;
        Ok(req)
    }

    async fn parse_token_response(&self, response: Response) -> Result<Token, anyhow::Error> {
        let response_body: Oauth2RefreshResponse = serde_json::from_str(&response.text().await?)?;

        Ok(Token {
            token: response_body.access_token,
            token_type: response_body.token_type,
            expires_in: Some(response_body.expires_in),
            metadata: None,
        })
    }
}

#[async_trait]
impl TokenProvider for UserAccessTokenProvider {
    async fn get_token(&mut self) -> Result<Token, anyhow::Error> {
        let client = Client::new();
        let req = self.prepare_token_request(&client).await?;
        let resp = client.execute(req).await?;
        self.parse_token_response(resp).await
    }
}

pub struct UserAccessTokenProviderBuilder {
    client_id: Option<String>,
    client_secret: Option<String>,
    refresh_token: Option<String>,
}

impl UserAccessTokenProviderBuilder {
    pub fn new() -> Self {
        UserAccessTokenProviderBuilder {
            client_id: None,
            client_secret: None,
            refresh_token: None,
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
enum RefreshGrantType {
    #[serde(rename = "refresh_token")]
    RefreshToken,
}

#[derive(Serialize, Deserialize, Debug)]
struct Oauth2RefreshRequest<'a> {
    grant_type: RefreshGrantType,
    client_id: &'a str,
    client_secret: &'a str,
    refresh_token: &'a str,
}

#[derive(Serialize, Deserialize, Debug)]
struct Oauth2RefreshResponse {
    access_token: String,
    scope: Option<String>,
    expires_in: i64,
    token_type: String,
    id_token: Option<String>,
    refresh_token: Option<String>,
}

#[derive(serde::Deserialize)]
struct UserCredentialJSON {
    #[serde(rename = "type")]
    cred_type: String,
    client_id: String,
    client_secret: String,
    refresh_token: String,
    quota_project_id: Option<String>,
    universe_domain: Option<String>,
}

#[cfg(test)]
mod tests {
    use crate::credentials;
    use crate::credentials::traits::Credential as CredentialTrait;
    use std::fs;
    use std::path::Path;

    #[tokio::test]
    async fn get_gcloud_token() {
        let mut cred = credentials::create_access_token_credential(None).await.unwrap();
        // println!("{:#?}", cred.get_headers().await.unwrap())
        printcred(cred).await;
    }

    #[tokio::test]
    async fn get_from_file() {
        let options =
            credentials::AccessTokenCredentialOptionsBuilder::new()
                .credentials_file("<path to service account json>".to_string())
                .build()
                .unwrap();
            
        let mut cred = credentials::create_access_token_credential(options).await.unwrap();
        println!("{:#?}", cred.get_headers().await.unwrap())
    }

    #[tokio::test]
    async fn set_scopes() {
        let options =
            credentials::AccessTokenCredentialOptionsBuilder::new()
                .scopes(vec!["https://www.googleapis.com/auth/cloud-platform".to_string()])
                .build()
                .unwrap();
        let mut cred = credentials::create_access_token_credential(options).await.unwrap();
        println!("{:#?}", cred.get_headers().await.unwrap())
    }

    #[tokio::test]
    async fn create_user_cred() {    
        let credentials_path = "/usr/local/google/home/saisunder/.config/gcloud/application_default_credentials.json";
    
        let json = fs::read_to_string(credentials_path).unwrap();

        let mut cred= super::UserCredentialBuilder::from_json(json.as_str())
            .unwrap()
            .token_url("http://abc.com".to_string())
            .scopes(vec!["https://www.googleapis.com/auth/translate".to_string()])
            .quota_project_id("my-quota-project".to_string())
            .build()
            .unwrap();

        println!("{:#?}", cred.get_headers().await.unwrap())
    }

    async fn printcred(mut cred: impl CredentialTrait) {
        println!("{:#?}", cred.get_headers().await.unwrap())
    }

    async fn create_cred_with_builder() {
        let options = crate::credentials::AccessTokenCredentialOptions3Builder::from_file("path".to_string())
            .scopes(vec!["https://www.googleapis.com/auth/translate".to_string()])
            .quota_project_id("my-quota-project".to_string())
            .build()
            .unwrap();
        
        let mut cred = credentials::create_access_token_credential2(options).await.unwrap();
        println!("{:#?}", cred.get_headers().await.unwrap())
    }
}