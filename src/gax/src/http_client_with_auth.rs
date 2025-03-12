use httpclient::http_client::ReqwestClient;
use httpclient::{options::{ClientConfig, RequestOptions}, error::Error};
use auth::credentials::{Credential, create_access_token_credential};
use crate::Result;
pub struct ReqwestClientWithAuth {
    client: ReqwestClient,
    cred: Credential
}

impl ReqwestClientWithAuth {
    pub async fn new(config: ClientConfigWithAuth, default_endpoint: &str) -> Result<Self> {
        let client = ReqwestClient::new(config.client_config, default_endpoint).await?;
        let cred = if let Some(c) = config.cred {
            c
        } else {
            // we can send a clone of ReqwestClient here and make auth code use that
            create_access_token_credential()
                .await
                .map_err(Error::authentication)?
        };
        Ok(Self { client, cred })
    }

    pub fn builder(&self, method: reqwest::Method, path: String) -> reqwest::RequestBuilder {
        self.client.builder(method, path)
    }

    pub async fn execute<I: serde::ser::Serialize, O: serde::de::DeserializeOwned>(
        &self,
        mut builder: reqwest::RequestBuilder,
        body: Option<I>,
        options: RequestOptions,
    ) -> Result<O> {
        let auth_headers = self
            .cred
            .get_headers()
            .await
            .map_err(Error::authentication)?;
        for header in auth_headers.into_iter() {
            builder = builder.header(header.0, header.1);
        }

        self.client.execute(builder, body, options).await?
    }
}

pub struct ClientConfigWithAuth {
    client_config: ClientConfig,
    cred: Option<Credential>,
}