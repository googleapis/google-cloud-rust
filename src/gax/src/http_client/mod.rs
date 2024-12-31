// Copyright 2024 Google LLC
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

use crate::error::Error;
use crate::error::HttpError;
use crate::Result;
use auth::Credential;

#[derive(Clone)]
pub struct ReqwestClient {
    inner: reqwest::Client,
    cred: Credential,
    endpoint: String,
}

impl ReqwestClient {
    pub async fn new(config: ClientConfig, default_endpoint: &str) -> Result<Self> {
        let inner = reqwest::Client::new();
        let cred = if let Some(c) = config.cred {
            c
        } else {
            ClientConfig::default_credential().await?
        };
        let endpoint = config
            .endpoint
            .unwrap_or_else(|| default_endpoint.to_string());
        Ok(Self {
            inner,
            cred,
            endpoint,
        })
    }

    pub fn builder(&self, method: reqwest::Method, path: String) -> reqwest::RequestBuilder {
        self.inner
            .request(method, format!("{}{path}", &self.endpoint))
    }

    pub async fn execute<I: serde::ser::Serialize, O: serde::de::DeserializeOwned>(
        &self,
        mut builder: reqwest::RequestBuilder,
        body: Option<I>,
        options: crate::options::RequestOptions,
    ) -> Result<O> {
        builder = builder.bearer_auth(Self::fetch_token(&self.cred).await?);
        if let Some(user_agent) = options.user_agent() {
            builder = builder.header(
                reqwest::header::USER_AGENT,
                reqwest::header::HeaderValue::from_str(user_agent).map_err(Error::other)?,
            );
        }
        if let Some(timeout) = options.attempt_timeout() {
            builder = builder.timeout(*timeout);
        }
        if let Some(body) = body {
            builder = builder.json(&body);
        }
        let response = builder.send().await.map_err(Error::io)?;
        if !response.status().is_success() {
            let status = response.status().as_u16();
            let headers = crate::error::convert_headers(response.headers());
            let body = response.bytes().await.map_err(Error::io)?;
            return Err(HttpError::new(status, headers, Some(body)).into());
        }
        let response = response.json::<O>().await.map_err(Error::serde)?;
        Ok(response)
    }

    async fn fetch_token(cred: &Credential) -> Result<String> {
        let tok = cred.access_token().await.map_err(Error::authentication)?;
        Ok(tok.value)
    }
}

impl std::fmt::Debug for ReqwestClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        f.debug_struct("ReqwestClient")
            .field("endpoint", &self.endpoint)
            .finish()
    }
}

#[derive(serde::Serialize)]
pub struct NoBody {}

#[derive(Default)]
pub struct ClientConfig {
    pub(crate) endpoint: Option<String>,
    pub(crate) cred: Option<Credential>,
    pub(crate) tracing: bool,
}

const LOGGING_VAR: &str = "GOOGLE_CLOUD_RUST_LOGGING";

impl ClientConfig {
    /// Returns a default [ConfigBuilder].
    pub fn new() -> Self {
        Self::default()
    }

    pub fn tracing_enabled(&self) -> bool {
        if self.tracing {
            return true;
        }
        std::env::var(LOGGING_VAR)
            .map(|v| v == "true")
            .unwrap_or(false)
    }

    /// Sets an endpoint that overrides the default endpoint for a service.
    pub fn set_endpoint<T: Into<String>>(mut self, v: T) -> Self {
        self.endpoint = Some(v.into());
        self
    }

    /// Enables tracing.
    pub fn enable_tracing(mut self) -> Self {
        self.tracing = true;
        self
    }

    /// Disables tracing.
    pub fn disable_tracing(mut self) -> Self {
        self.tracing = false;
        self
    }

    pub fn set_credential<T: Into<Option<Credential>>>(mut self, v: T) -> Self {
        self.cred = v.into();
        self
    }

    pub(crate) async fn default_credential() -> Result<Credential> {
        let cc = auth::CredentialConfig::builder()
            .scopes(vec![
                "https://www.googleapis.com/auth/cloud-platform".to_string()
            ])
            .build()
            .map_err(Error::authentication)?;
        Credential::find_default(cc)
            .await
            .map_err(Error::authentication)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    type Result = std::result::Result<(), Box<dyn std::error::Error>>;

    // This test must run serially because `std::env::remove_var` and
    // `std::env::set_var` are unsafe otherwise.
    #[test]
    #[serial_test::serial]
    fn config_tracing() {
        unsafe {
            std::env::remove_var(LOGGING_VAR);
        }
        let config = ClientConfig::new();
        assert!(!config.tracing_enabled(), "expected tracing to be disabled");
        let config = ClientConfig::new().enable_tracing();
        assert!(config.tracing_enabled(), "expected tracing to be enabled");
        let config = config.disable_tracing();
        assert!(
            !config.tracing_enabled(),
            "expected tracing to be disaabled"
        );

        unsafe {
            std::env::set_var(LOGGING_VAR, "true");
        }
        let config = ClientConfig::new();
        assert!(config.tracing_enabled(), "expected tracing to be enabled");

        unsafe {
            std::env::set_var(LOGGING_VAR, "not-true");
        }
        let config = ClientConfig::new();
        assert!(!config.tracing_enabled(), "expected tracing to be disabled");
    }

    #[test]
    fn config_endpoint() {
        let config = ClientConfig::new().set_endpoint("http://storage.googleapis.com");
        assert_eq!(
            config.endpoint,
            Some("http://storage.googleapis.com".to_string())
        );
    }

    #[tokio::test]
    async fn config_credentials() -> Result {
        let config = ClientConfig::new().set_credential(auth::Credential::test_credentials());
        let cred = config.cred.unwrap();
        let token = cred.access_token().await?;
        assert!(
            token.value.contains("test-only"),
            "unexpected test token {}",
            token.value
        );
        Ok(())
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn config_default_credentials() -> Result {
        let dir = tempfile::tempdir()?;
        let path = dir.path().to_str().unwrap();
        unsafe {
            // This is not readable as a file and should cause the default credentials to fail.
            std::env::set_var("GOOGLE_APPLICATION_CREDENTIALS", path);
        }
        let cred = ClientConfig::default_credential().await;
        assert!(cred.is_err());
        Ok(())
    }
}
