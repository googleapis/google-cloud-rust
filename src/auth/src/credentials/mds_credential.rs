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

use crate::credentials::dynamic::CredentialTrait;
use crate::credentials::{Credential, Result};
use crate::errors::{is_retryable, CredentialError};
use crate::token::{Token, TokenProvider};
use async_trait::async_trait;
use http::header::{HeaderName, HeaderValue, AUTHORIZATION};
use reqwest::header::HeaderMap;
use reqwest::Client;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use time::OffsetDateTime;

const METADATA_FLAVOR_VALUE: &str = "Google";
const METADATA_FLAVOR: &str = "metadata-flavor";
const METADATA_ROOT: &str = "http://metadata.google.internal/computeMetadata/v1";

pub(crate) fn new() -> Credential {
    let token_provider = MDSAccessTokenProvider {
        endpoint: METADATA_ROOT.to_string(),
    };
    Credential {
        inner: Arc::new(MDSCredential { token_provider }),
    }
}

#[derive(Debug)]
struct MDSCredential<T>
where
    T: TokenProvider,
{
    token_provider: T,
}

#[async_trait::async_trait]
impl<T> CredentialTrait for MDSCredential<T>
where
    T: TokenProvider,
{
    async fn get_token(&self) -> Result<Token> {
        self.token_provider.get_token().await
    }

    async fn get_headers(&self) -> Result<Vec<(HeaderName, HeaderValue)>> {
        let token = self.get_token().await?;
        let mut value = HeaderValue::from_str(&format!("{} {}", token.token_type, token.token))
            .map_err(CredentialError::non_retryable)?;
        value.set_sensitive(true);
        Ok(vec![(AUTHORIZATION, value)])
    }
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
struct ServiceAccountInfo {
    email: String,
    scopes: Option<Vec<String>>,
    aliases: Option<Vec<String>>,
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
struct MDSTokenResponse {
    access_token: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    expires_in: Option<u64>,
    token_type: String,
}

#[derive(Debug)]
struct MDSAccessTokenProvider {
    endpoint: String,
}

impl MDSAccessTokenProvider {
    #[allow(dead_code)]
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
            .map_err(CredentialError::non_retryable)?;

        let response = request
            .get(url.clone())
            .headers(headers)
            .send()
            .await
            .map_err(CredentialError::non_retryable)?;

        response
            .json::<ServiceAccountInfo>()
            .await
            .map_err(CredentialError::non_retryable)
    }
}

#[async_trait]
impl TokenProvider for MDSAccessTokenProvider {
    async fn get_token(&self) -> Result<Token> {
        let client = Client::new();
        let request = client
            .get(format!(
                "{}/instance/service-accounts/default/token",
                self.endpoint
            ))
            .header(
                METADATA_FLAVOR,
                HeaderValue::from_static(METADATA_FLAVOR_VALUE),
            );

        let response = request.send().await.map_err(CredentialError::retryable)?;
        // Process the response
        if !response.status().is_success() {
            let status = response.status();
            let body = response
                .text()
                .await
                .map_err(|e| CredentialError::new(is_retryable(status), e.into()))?;
            return Err(CredentialError::new(
                is_retryable(status),
                Box::from(format!("Failed to fetch token. {body}")),
            ));
        }
        let response = response
            .json::<MDSTokenResponse>()
            .await
            .map_err(CredentialError::retryable)?;
        let token = Token {
            token: response.access_token,
            token_type: response.token_type,
            expires_at: response
                .expires_in
                .map(|d| OffsetDateTime::now_utc() + Duration::from_secs(d)),
            metadata: None,
        };
        Ok(token)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::token::test::MockTokenProvider;
    use axum::response::IntoResponse;
    use reqwest::StatusCode;
    use serde_json::Value;
    use std::error::Error;
    use tokio::task::JoinHandle;

    type TestResult = std::result::Result<(), Box<dyn std::error::Error>>;

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

        let mdsc = MDSCredential {
            token_provider: mock,
        };
        let actual = mdsc.get_token().await.unwrap();
        assert_eq!(actual, expected);
    }

    #[tokio::test]
    async fn get_token_failure() {
        let mut mock = MockTokenProvider::new();
        mock.expect_get_token()
            .times(1)
            .return_once(|| Err(CredentialError::non_retryable("fail")));

        let mdsc = MDSCredential {
            token_provider: mock,
        };
        assert!(mdsc.get_token().await.is_err());
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

        let mdsc = MDSCredential {
            token_provider: mock,
        };
        let headers: Vec<HV> = mdsc
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

    #[tokio::test]
    async fn get_headers_failure() {
        let mut mock = MockTokenProvider::new();
        mock.expect_get_token()
            .times(1)
            .return_once(|| Err(CredentialError::non_retryable("fail")));

        let mdsc = MDSCredential {
            token_provider: mock,
        };
        assert!(mdsc.get_headers().await.is_err());
    }

    fn handle_token_factory(
        response_code: StatusCode,
        response_headers: HeaderMap,
        response_body: Value,
    ) -> impl IntoResponse {
        (response_code, response_headers, response_body.to_string()).into_response()
    }

    // Starts a server running locally. Returns an (endpoint, server) pair.
    async fn start(
        response_code: StatusCode,
        response_body: Value,
        path: String,
    ) -> (String, JoinHandle<()>) {
        let code = response_code.clone();
        let body = response_body.clone();
        let header_map = HeaderMap::new();
        let handler = move || async move { handle_token_factory(code, header_map, body) };
        let app = axum::Router::new().route(&path, axum::routing::get(handler));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async {
            axum::serve(listener, app).await.unwrap();
        });

        (format!("http://{}:{}", addr.ip(), addr.port()), server)
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
        let (endpoint, _server) = start(StatusCode::OK, service_account_info_json, path).await;
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
        let (endpoint, _server) = start(StatusCode::OK, service_account_info_json, path).await;
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
        let (endpoint, _server) = start(
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn token_provider_full() -> TestResult {
        let response = MDSTokenResponse {
            access_token: "test-access-token".to_string(),
            expires_in: Some(3600),
            token_type: "test-token-type".to_string(),
        };
        let response_body = serde_json::to_value(&response).unwrap();
        let path = "/instance/service-accounts/default/token";
        let (endpoint, _server) = start(StatusCode::OK, response_body, path.to_string()).await;
        println!("endpoint = {endpoint}");

        let tp = MDSAccessTokenProvider { endpoint: endpoint };
        let mdsc = MDSCredential { token_provider: tp };
        let now = OffsetDateTime::now_utc();
        let token = mdsc.get_token().await?;
        assert_eq!(token.token, "test-access-token");
        assert_eq!(token.token_type, "test-token-type");
        assert!(token
            .expires_at
            .is_some_and(|d| d >= now + Duration::from_secs(3600)));

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn token_provider_partial() -> TestResult {
        let response = MDSTokenResponse {
            access_token: "test-access-token".to_string(),
            expires_in: None,
            token_type: "test-token-type".to_string(),
        };
        let response_body = serde_json::to_value(&response).unwrap();
        let path = "/instance/service-accounts/default/token";
        let (endpoint, _server) = start(StatusCode::OK, response_body, path.to_string()).await;
        println!("endpoint = {endpoint}");

        let tp = MDSAccessTokenProvider { endpoint: endpoint };
        let mdsc = MDSCredential { token_provider: tp };
        let token = mdsc.get_token().await?;
        assert_eq!(token.token, "test-access-token");
        assert_eq!(token.token_type, "test-token-type");
        assert_eq!(token.expires_at, None);

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn token_provider_retryable_error() -> TestResult {
        let path = "/instance/service-accounts/default/token";
        let (endpoint, _server) = start(
            StatusCode::SERVICE_UNAVAILABLE,
            serde_json::to_value("try again")?,
            path.to_string(),
        )
        .await;

        let tp = MDSAccessTokenProvider { endpoint: endpoint };
        let mdsc = MDSCredential { token_provider: tp };
        let e = mdsc.get_token().await.err().unwrap();
        assert!(e.is_retryable());
        assert!(e.source().unwrap().to_string().contains("try again"));

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn token_provider_nonretryable_error() -> TestResult {
        let path = "/instance/service-accounts/default/token";
        let (endpoint, _server) = start(
            StatusCode::UNAUTHORIZED,
            serde_json::to_value("epic fail".to_string())?,
            path.to_string(),
        )
        .await;

        let tp = MDSAccessTokenProvider { endpoint: endpoint };
        let mdsc = MDSCredential { token_provider: tp };
        let e = mdsc.get_token().await.err().unwrap();
        assert!(!e.is_retryable());
        assert!(e.source().unwrap().to_string().contains("epic fail"));

        Ok(())
    }
}
