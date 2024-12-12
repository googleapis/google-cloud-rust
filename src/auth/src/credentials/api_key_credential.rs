use crate::credentials::traits::dynamic::Credential;
use http::header::{HeaderName, HeaderValue, AUTHORIZATION};
use anyhow::Result;


pub struct ApiKeyCredential {
    api_key: String,
}

#[async_trait::async_trait]
impl Credential for ApiKeyCredential {
    async fn get_headers(
        &mut self,
    ) -> Result<Vec<(HeaderName, HeaderValue)>> {
        Ok(vec![
            (
                HeaderName::from_static("x-goog-api-key"),
                HeaderValue::from_str(self.api_key.as_str()).unwrap()
            )
        ])
    }

    async fn get_universe_domain(&mut self) -> Option<String> {
        None
    }
}

impl ApiKeyCredential {
    pub fn new(api_key: String) -> Self {
        Self { api_key }
    }
}
