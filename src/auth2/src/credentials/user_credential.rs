use anyhow::anyhow;
use serde::{Deserialize, Serialize};
use super::Credential;
use crate::token::{Token, TokenCache, TokenProvider};
use reqwest::{header::HeaderValue, Client, Method, Request, Response};
use async_trait::async_trait;

const OAUTH2_ENDPOINT: &str = "https://oauth2.googleapis.com/token";


pub(crate) struct UserCredential {
    quota_project_id: Option<String>,
    universe_domain: String,
    token_cache: TokenCache<UserAccessTokenProvider>,
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
            Err(_) => return Err(anyhow::anyhow!(format!(
                "Could not parse the AuthorizedUser JSON. {:?}",
                data
            )))
        };
        
        let user_access_token_provider = UserAccessTokenProvider {
            client_id: user_credential_json.client_id,
            client_secret: user_credential_json.client_secret,
            refresh_token: user_credential_json.refresh_token,
        };

        let token_cache =
            TokenCache::new(user_access_token_provider);

        Ok(
            UserCredential {
                quota_project_id: user_credential_json.quota_project_id,
                universe_domain: user_credential_json.universe_domain.unwrap_or("googleapis.com".to_string()),
                token_cache,
        })
    }

}

#[async_trait]
impl Credential for UserCredential {
    async fn get_token(&mut self) -> anyhow::Result<Token, anyhow::Error> {
        self.token_cache.token_non_blocking().await
    }

    fn get_quota_project_id(&self) -> anyhow::Result<String, anyhow::Error> {
        match &self.quota_project_id {
            Some(quota_project_id) => Ok(quota_project_id.clone()),
            None => Err(anyhow!("No quota project id found"))
        }
    }

    fn get_universe_domain(&self) -> anyhow::Result<String, anyhow::Error> {
        Ok(self.universe_domain.clone())
    }
}

pub struct UserAccessTokenProvider {
    client_id: String,
    client_secret: String,
    refresh_token: String,
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
    async fn get_token_internal(&self) -> Result<Token, anyhow::Error> {
        let client = Client::new();
        let req = self.prepare_token_request(&client).await?;
        let resp = client.execute(req).await?;
        self.parse_token_response(resp).await
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

    #[tokio::test]
    async fn get_gcloud_token() {
        let mut cred = credentials::create_access_token_credential(None).await.unwrap();
        let token = cred.get_token().await.unwrap();
        println!("{:#?}", token);
        println!();
        println!("Authorization header:{}", cred.get_authorization_header().await.unwrap())
    }
}