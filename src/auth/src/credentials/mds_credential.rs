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

use crate::credentials::traits::dynamic::Credential;
use crate::credentials::Result;
use crate::errors::CredentialError;
use crate::token::{Token, TokenProvider};
use async_trait::async_trait;
use http::header::{HeaderName, HeaderValue, AUTHORIZATION};
use lazy_static::lazy_static;
use reqwest::header::HeaderMap;
use reqwest::Client;
use std::collections::HashMap;
use std::env;

const METADATA_FLAVOR_VALUE: &str = "Google";
const METADATA_FLAVOR: &str = "metadata-flavor";

lazy_static! {
    // Use lazy_static to initialize the metadata URLs.
    static ref _METADATA_ROOT: String = format!(
        "http://{}/computeMetadata/v1/",
        env::var("GCE_METADATA_HOST").unwrap_or_else(|_| {
            env::var("GCE_METADATA_ROOT").unwrap_or_else(|_| "metadata.google.internal".to_string())
        })
    );
}

#[allow(dead_code)] // TODO(#442) - implementation in progress
pub(crate) struct MDSCredential<T>
where
    T: TokenProvider,
{
    token_provider: T,
}

#[async_trait::async_trait]
impl<T> Credential for MDSCredential<T>
where
    T: TokenProvider,
{
    async fn get_token(&mut self) -> Result<Token> {
        self.token_provider.get_token().await
    }

    async fn get_headers(&mut self) -> Result<Vec<(HeaderName, HeaderValue)>> {
        let token = self.get_token().await?;
        let mut value = HeaderValue::from_str(&format!("{} {}", token.token_type, token.token))
            .map_err(|e| CredentialError::new(false, e.into()))?;
        value.set_sensitive(true);
        Ok(vec![(AUTHORIZATION, value)])
    }

    async fn get_universe_domain(&mut self) -> Option<String> {
        Some("googleapis.com".to_string())
    }
}

#[allow(dead_code)] // TODO(#442) - implementation in progress
#[derive(serde::Deserialize, serde::Serialize, PartialEq, Debug, Clone)]
struct ServiceAccountInfo {
    email: String,
    scopes: Option<Vec<String>>,
    aliases: Option<Vec<String>>,
}
#[allow(dead_code)] // TODO(#442) - implementation in progress
struct MDSAccessTokenProvider {
    token_endpoint: String,
}

#[allow(dead_code)]
impl MDSAccessTokenProvider {
    async fn get_service_account_info(
        request: &Client,
        metadata_service_endpoint: String,
        email: Option<String>,
    ) -> Result<ServiceAccountInfo> {
        let email: String = email.unwrap_or("default".to_string());
        let path: String = format!(
            "{}/instance/service-accounts/{}/",
            metadata_service_endpoint, email
        );
        let params = HashMap::from([("recursive", "true")]);

        let mut headers = HeaderMap::new();
        headers.insert(
            METADATA_FLAVOR,
            HeaderValue::from_static(METADATA_FLAVOR_VALUE),
        );

        let url = reqwest::Url::parse_with_params(path.as_str(), params.iter())
            .map_err(|e| CredentialError::new(false, e.into()))?;

        let response = request
            .get(url.clone())
            .headers(headers)
            .send()
            .await
            .map_err(|e| CredentialError::new(false, e.into()))?;

        response
            .json::<ServiceAccountInfo>()
            .await
            .map_err(|e| CredentialError::new(false, e.into()))
    }
}

#[async_trait]
#[allow(dead_code)]
impl TokenProvider for MDSAccessTokenProvider {
    async fn get_token(&mut self) -> Result<Token> {
        todo!()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::token::test::MockTokenProvider;
    use axum::response::IntoResponse;
    use reqwest::StatusCode;
    use serde_json::Value;
    use tokio::task::JoinHandle;

    #[tokio::test]
    async fn get_token_success() {
        let expected = Token {
            token: "test-token".to_string(),
            token_type: "Bearer".to_string(),
            expires_at: None,
            metadata: None,
        };
        let expected_clone = expected.clone();

        let mut mock = MockTokenProvider::new();
        mock.expect_get_token()
            .times(1)
            .return_once(|| Ok(expected_clone));

        let mut uc = MDSCredential {
            token_provider: mock,
        };
        let actual = uc.get_token().await.unwrap();
        assert_eq!(actual, expected);
    }

    #[tokio::test]
    async fn get_token_failure() {
        let mut mock = MockTokenProvider::new();
        mock.expect_get_token()
            .times(1)
            .return_once(|| Err(CredentialError::new(false, Box::from("fail"))));

        let mut uc = MDSCredential {
            token_provider: mock,
        };
        assert!(uc.get_token().await.is_err());
    }

    #[tokio::test]
    async fn get_headers_success() {
        #[derive(Debug, PartialEq)]
        struct HV {
            header: String,
            value: String,
            is_sensitive: bool,
        }

        let token = Token {
            token: "test-token".to_string(),
            token_type: "Bearer".to_string(),
            expires_at: None,
            metadata: None,
        };

        let mut mock = MockTokenProvider::new();
        mock.expect_get_token().times(1).return_once(|| Ok(token));

        let mut uc = MDSCredential {
            token_provider: mock,
        };
        let headers: Vec<HV> = uc
            .get_headers()
            .await
            .unwrap()
            .into_iter()
            .map(|(h, v)| HV {
                header: h.to_string(),
                value: v.to_str().unwrap().to_string(),
                is_sensitive: v.is_sensitive(),
            })
            .collect();

        assert_eq!(
            headers,
            vec![HV {
                header: AUTHORIZATION.to_string(),
                value: "Bearer test-token".to_string(),
                is_sensitive: true,
            }]
        );
    }

    #[test]
    fn metadata_root_from_gce_metadata_host() {
        env::set_var("GCE_METADATA_HOST", "custom-metadata-host");
        // Recreate lazy_static value which depends on env variable
        lazy_static::initialize(&_METADATA_ROOT);

        assert_eq!(
            &_METADATA_ROOT.to_string(),
            "http://metadata.google.internal/computeMetadata/v1/"
        );

        env::remove_var("GCE_METADATA_HOST"); // Clean up
    }

    #[test]
    fn metadata_root_from_gce_metadata_root() {
        env::set_var("GCE_METADATA_ROOT", "metadata.example.com");
        // Recreate lazy_static value which depends on env variable
        lazy_static::initialize(&_METADATA_ROOT);

        assert_eq!(
            &_METADATA_ROOT.to_string(),
            "http://metadata.google.internal/computeMetadata/v1/"
        );

        env::remove_var("GCE_METADATA_ROOT"); // Clean up
    }

    #[test]
    fn metadata_root_default() {
        // Remove env vars if they exist from previous tests
        env::remove_var("GCE_METADATA_ROOT");
        env::remove_var("GCE_METADATA_HOST");
        // Recreate lazy_static value which depends on env variable
        lazy_static::initialize(&_METADATA_ROOT);

        assert_eq!(
            &_METADATA_ROOT.to_string(),
            "http://metadata.google.internal/computeMetadata/v1/"
        );
    }

    #[tokio::test]
    async fn get_headers_failure() {
        let mut mock = MockTokenProvider::new();
        mock.expect_get_token()
            .times(1)
            .return_once(|| Err(CredentialError::new(false, Box::from("fail"))));

        let mut uc = MDSCredential {
            token_provider: mock,
        };
        assert!(uc.get_headers().await.is_err());
    }

    fn handle_token_factory(
        response_code: StatusCode,
        response_headers: HeaderMap,
        response_body: Value,
    ) -> impl IntoResponse {
        (response_code, response_headers, response_body.to_string()).into_response()
    }

    // Starts a server running locally. Returns an (endpoint, path, handler) pair.
    async fn start(
        response_code: StatusCode,
        response_body: Value,
        path: String,
    ) -> (String, String, JoinHandle<()>) {
        let code = response_code.clone();
        let body = response_body.clone();
        let header_map = HeaderMap::new();
        let handler = move || async move { handle_token_factory(code, header_map, body) };
        let app = axum::Router::new().route(&path, axum::routing::get(handler));
        let listener = tokio::net::TcpListener::bind("0.0.0.0:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async {
            axum::serve(listener, app).await.unwrap();
        });

        (
            format!("http://{}:{}", addr.ip(), addr.port()),
            path,
            server,
        )
    }

    #[tokio::test]
    async fn get_default_service_account_info_success() {
        let service_account = "default";
        let path = format!("/instance/service-accounts/{}/", service_account);
        let service_account_info = ServiceAccountInfo {
            email: "test@test.com".to_string(),
            scopes: Some(vec!["scope 1".to_string(), "scope 2".to_string()]),
            aliases: None,
        };
        let service_account_info_json = serde_json::to_value(service_account_info.clone()).unwrap();
        let (endpoint, _path, _server) =
            start(StatusCode::OK, service_account_info_json, path).await;
        let request = Client::new();
        let result =
            MDSAccessTokenProvider::get_service_account_info(&request, endpoint, Option::None)
                .await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), service_account_info);
    }

    #[tokio::test]
    async fn get_custom_service_account_info_success() {
        let service_account = "test@test";
        let path = format!("/instance/service-accounts/{}/", service_account);
        let service_account_info = ServiceAccountInfo {
            email: format!("{}.com", service_account),
            scopes: Some(vec!["scope 1".to_string(), "scope 2".to_string()]),
            aliases: None,
        };
        let service_account_info_json = serde_json::to_value(service_account_info.clone()).unwrap();
        let (endpoint, _path, _server) =
            start(StatusCode::OK, service_account_info_json, path).await;
        let request = Client::new();
        let result = MDSAccessTokenProvider::get_service_account_info(
            &request,
            endpoint,
            Some(service_account.to_string()),
        )
        .await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), service_account_info);
    }

    #[tokio::test]
    async fn get_service_account_info_server_error() {
        let service_account = "test@test";
        let path = format!("/instance/service-accounts/{}/", service_account);
        let (endpoint, _path, _server) = start(
            StatusCode::SERVICE_UNAVAILABLE,
            serde_json::to_value("try again").unwrap(),
            path,
        )
        .await;
        let request = Client::new();
        let result = MDSAccessTokenProvider::get_service_account_info(
            &request,
            endpoint,
            Some(service_account.to_string()),
        )
        .await;
        assert!(result.is_err());
    }
}
