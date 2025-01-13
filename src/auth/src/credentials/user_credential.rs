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
use crate::credentials::{Credential, Result, QUOTA_PROJECT_KEY};
use crate::errors::{is_retryable, CredentialError};
use crate::token::{Token, TokenProvider};
use http::header::{HeaderName, HeaderValue, AUTHORIZATION, CONTENT_TYPE};
use reqwest::{Client, Method};
use std::sync::Arc;
use std::time::Duration;
use time::OffsetDateTime;

const OAUTH2_ENDPOINT: &str = "https://oauth2.googleapis.com/token";

pub(crate) fn creds_from(js: serde_json::Value) -> Result<Credential> {
    let au =
        serde_json::from_value::<AuthorizedUser>(js).map_err(CredentialError::non_retryable)?;
    let token_provider = UserTokenProvider {
        client_id: au.client_id,
        client_secret: au.client_secret,
        refresh_token: au.refresh_token,
        endpoint: OAUTH2_ENDPOINT.to_string(),
    };

    Ok(Credential {
        inner: Arc::new(UserCredential {
            token_provider,
            quota_project_id: au.quota_project_id,
        }),
    })
}

#[derive(PartialEq)]
struct UserTokenProvider {
    client_id: String,
    client_secret: String,
    refresh_token: String,
    endpoint: String,
}

impl std::fmt::Debug for UserTokenProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UserTokenCredential")
            .field("client_id", &self.client_id)
            .field("client_secret", &"[censored]")
            .field("refresh_token", &"[censored]")
            .field("endpoint", &self.endpoint)
            .finish()
    }
}

#[async_trait::async_trait]
impl TokenProvider for UserTokenProvider {
    async fn get_token(&self) -> Result<Token> {
        let client = Client::new();

        // Make the request
        let req = Oauth2RefreshRequest {
            grant_type: RefreshGrantType::RefreshToken,
            client_id: self.client_id.clone(),
            client_secret: self.client_secret.clone(),
            refresh_token: self.refresh_token.clone(),
        };
        let header = HeaderValue::from_static("application/json");
        let builder = client
            .request(Method::POST, self.endpoint.as_str())
            .header(CONTENT_TYPE, header)
            .json(&req);
        let resp = builder
            .send()
            .await
            .map_err(CredentialError::non_retryable)?;

        // Process the response
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.map_err(CredentialError::non_retryable)?;
            return Err(CredentialError::new(
                is_retryable(status),
                Box::from(format!("Failed to fetch token. {body}")),
            ));
        }
        let response = resp
            .json::<Oauth2RefreshResponse>()
            .await
            .map_err(CredentialError::non_retryable)?;
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

/// Data model for a UserCredential
///
/// See: https://cloud.google.com/docs/authentication#user-accounts
#[derive(Debug)]
pub(crate) struct UserCredential<T>
where
    T: TokenProvider,
{
    token_provider: T,
    quota_project_id: Option<String>,
}

#[async_trait::async_trait]
impl<T> CredentialTrait for UserCredential<T>
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
        let mut headers = vec![(AUTHORIZATION, value)];
        if let Some(project) = &self.quota_project_id {
            headers.push((
                HeaderName::from_static(QUOTA_PROJECT_KEY),
                HeaderValue::from_str(project).map_err(CredentialError::non_retryable)?,
            ));
        }
        Ok(headers)
    }
}

#[derive(Debug, PartialEq, serde::Deserialize)]
pub(crate) struct AuthorizedUser {
    #[serde(rename = "type")]
    cred_type: String,
    client_id: String,
    client_secret: String,
    refresh_token: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    quota_project_id: Option<String>,
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
enum RefreshGrantType {
    #[serde(rename = "refresh_token")]
    RefreshToken,
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
struct Oauth2RefreshRequest {
    grant_type: RefreshGrantType,
    client_id: String,
    client_secret: String,
    refresh_token: String,
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
struct Oauth2RefreshResponse {
    access_token: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    scope: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    expires_in: Option<u64>,
    token_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    refresh_token: Option<String>,
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::token::test::MockTokenProvider;
    use axum::extract::Json;
    use http::StatusCode;
    use std::error::Error;
    use tokio::task::JoinHandle;

    type TestResult = std::result::Result<(), Box<dyn std::error::Error>>;

    #[test]
    fn debug_token_provider() {
        let expected = UserTokenProvider {
            client_id: "test-client-id".to_string(),
            client_secret: "test-client-secret".to_string(),
            refresh_token: "test-refresh-token".to_string(),
            endpoint: OAUTH2_ENDPOINT.to_string(),
        };
        let fmt = format!("{expected:?}");
        assert!(fmt.contains("test-client-id"), "{fmt}");
        assert!(!fmt.contains("test-client-secret"), "{fmt}");
        assert!(!fmt.contains("test-refresh-token"), "{fmt}");
        assert!(fmt.contains(OAUTH2_ENDPOINT), "{fmt}");
    }

    #[test]
    fn authorized_user_full_from_json_success() {
        let json = serde_json::json!({
            "account": "",
            "client_id": "test-client-id",
            "client_secret": "test-client-secret",
            "refresh_token": "test-refresh-token",
            "type": "authorized_user",
            "universe_domain": "googleapis.com",
            "quota_project_id": "test-project"
        });

        let expected = AuthorizedUser {
            cred_type: "authorized_user".to_string(),
            client_id: "test-client-id".to_string(),
            client_secret: "test-client-secret".to_string(),
            refresh_token: "test-refresh-token".to_string(),
            quota_project_id: Some("test-project".to_string()),
        };
        let actual = serde_json::from_value::<AuthorizedUser>(json).unwrap();
        assert_eq!(actual, expected);
    }

    #[test]
    fn authorized_user_partial_from_json_success() {
        let json = serde_json::json!({
            "client_id": "test-client-id",
            "client_secret": "test-client-secret",
            "refresh_token": "test-refresh-token",
            "type": "authorized_user",
        });

        let expected = AuthorizedUser {
            cred_type: "authorized_user".to_string(),
            client_id: "test-client-id".to_string(),
            client_secret: "test-client-secret".to_string(),
            refresh_token: "test-refresh-token".to_string(),
            quota_project_id: None,
        };
        let actual = serde_json::from_value::<AuthorizedUser>(json).unwrap();
        assert_eq!(actual, expected);
    }

    #[test]
    fn authorized_user_from_json_parse_fail() {
        let json_full = serde_json::json!({
            "account": "",
            "client_id": "test-client-id",
            "client_secret": "test-client-secret",
            "refresh_token": "test-refresh-token",
            "type": "authorized_user",
            "universe_domain": "googleapis.com",
            "quota_project_id": "test-project"
        });

        for required_field in ["client_id", "client_secret", "refresh_token"] {
            let mut json = json_full.clone();
            // Remove a required field from the JSON
            json[required_field].take();
            serde_json::from_value::<AuthorizedUser>(json)
                .err()
                .unwrap();
        }
    }

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

        let uc = UserCredential {
            token_provider: mock,
            quota_project_id: None,
        };
        let actual = uc.get_token().await.unwrap();
        assert_eq!(actual, expected);
    }

    #[tokio::test]
    async fn get_token_failure() {
        let mut mock = MockTokenProvider::new();
        mock.expect_get_token()
            .times(1)
            .return_once(|| Err(CredentialError::non_retryable("fail")));

        let uc = UserCredential {
            token_provider: mock,
            quota_project_id: None,
        };
        assert!(uc.get_token().await.is_err());
    }

    // Convenience struct for verifying (HeaderName, HeaderValue) pairs.
    #[derive(Debug, Eq, Ord, PartialEq, PartialOrd)]
    struct HV {
        header: String,
        value: String,
        is_sensitive: bool,
    }

    #[tokio::test]
    async fn get_headers_success() {
        let token = Token {
            token: "test-token".to_string(),
            token_type: "Bearer".to_string(),
            expires_at: None,
            metadata: None,
        };

        let mut mock = MockTokenProvider::new();
        mock.expect_get_token().times(1).return_once(|| Ok(token));

        let uc = UserCredential {
            token_provider: mock,
            quota_project_id: None,
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

    #[tokio::test]
    async fn get_headers_failure() {
        let mut mock = MockTokenProvider::new();
        mock.expect_get_token()
            .times(1)
            .return_once(|| Err(CredentialError::non_retryable("fail")));

        let uc = UserCredential {
            token_provider: mock,
            quota_project_id: None,
        };
        assert!(uc.get_headers().await.is_err());
    }

    #[tokio::test]
    async fn get_headers_with_quota_project_success() {
        let token = Token {
            token: "test-token".to_string(),
            token_type: "Bearer".to_string(),
            expires_at: None,
            metadata: None,
        };

        let mut mock = MockTokenProvider::new();
        mock.expect_get_token().times(1).return_once(|| Ok(token));

        let uc = UserCredential {
            token_provider: mock,
            quota_project_id: Some("test-project".to_string()),
        };
        let mut headers: Vec<HV> = uc
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

        // The ordering of the headers does not matter.
        headers.sort();
        assert_eq!(
            headers,
            vec![
                HV {
                    header: AUTHORIZATION.to_string(),
                    value: "Bearer test-token".to_string(),
                    is_sensitive: true,
                },
                HV {
                    header: QUOTA_PROJECT_KEY.to_string(),
                    value: "test-project".to_string(),
                    is_sensitive: false,
                }
            ]
        );
    }

    #[test]
    fn oauth2_request_serde() {
        let request = Oauth2RefreshRequest {
            grant_type: RefreshGrantType::RefreshToken,
            client_id: "test-client-id".to_string(),
            client_secret: "test-client-secret".to_string(),
            refresh_token: "test-refresh-token".to_string(),
        };

        let json = serde_json::to_value(&request).unwrap();
        let expected = serde_json::json!({
            "grant_type": "refresh_token",
            "client_id": "test-client-id",
            "client_secret": "test-client-secret",
            "refresh_token": "test-refresh-token"
        });
        assert_eq!(json, expected);
        let roundtrip = serde_json::from_value::<Oauth2RefreshRequest>(json).unwrap();
        assert_eq!(request, roundtrip);
    }

    #[test]
    fn oauth2_response_serde_full() {
        let response = Oauth2RefreshResponse {
            access_token: "test-access-token".to_string(),
            scope: Some("scope1 scope2".to_string()),
            expires_in: Some(3600),
            token_type: "test-token-type".to_string(),
            refresh_token: Some("test-refresh-token".to_string()),
        };

        let json = serde_json::to_value(&response).unwrap();
        let expected = serde_json::json!({
            "access_token": "test-access-token",
            "scope": "scope1 scope2",
            "expires_in": 3600,
            "token_type": "test-token-type",
            "refresh_token": "test-refresh-token"
        });
        assert_eq!(json, expected);
        let roundtrip = serde_json::from_value::<Oauth2RefreshResponse>(json).unwrap();
        assert_eq!(response, roundtrip);
    }

    #[test]
    fn oauth2_response_serde_partial() {
        let response = Oauth2RefreshResponse {
            access_token: "test-access-token".to_string(),
            scope: None,
            expires_in: None,
            token_type: "test-token-type".to_string(),
            refresh_token: None,
        };

        let json = serde_json::to_value(&response).unwrap();
        let expected = serde_json::json!({
            "access_token": "test-access-token",
            "token_type": "test-token-type",
        });
        assert_eq!(json, expected);
        let roundtrip = serde_json::from_value::<Oauth2RefreshResponse>(json).unwrap();
        assert_eq!(response, roundtrip);
    }

    // Starts a server running locally. Returns an (endpoint, handler) pair.
    async fn start(response_code: StatusCode, response_body: String) -> (String, JoinHandle<()>) {
        let code = response_code.clone();
        let body = response_body.clone();
        let handler = move |req| async move { handle_token_factory(code, body)(req) };
        let app = axum::Router::new().route("/token", axum::routing::post(handler));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async {
            axum::serve(listener, app).await.unwrap();
        });

        (
            format!("http://{}:{}/token", addr.ip(), addr.port()),
            server,
        )
    }

    // Creates a handler that
    // - verifies fields in an Oauth2RefreshRequest
    // - returns a pre-canned HTTP response
    fn handle_token_factory(
        response_code: StatusCode,
        response_body: String,
    ) -> impl Fn(Json<Oauth2RefreshRequest>) -> (StatusCode, String) {
        move |request: Json<Oauth2RefreshRequest>| -> (StatusCode, String) {
            assert_eq!(request.client_id, "test-client-id");
            assert_eq!(request.client_secret, "test-client-secret");
            assert_eq!(request.refresh_token, "test-refresh-token");
            assert_eq!(request.grant_type, RefreshGrantType::RefreshToken);

            (response_code, response_body.clone())
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn token_provider_full() -> TestResult {
        let response = Oauth2RefreshResponse {
            access_token: "test-access-token".to_string(),
            expires_in: Some(3600),
            refresh_token: Some("test-refresh-token".to_string()),
            scope: Some("scope1 scope2".to_string()),
            token_type: "test-token-type".to_string(),
        };
        let response_body = serde_json::to_string(&response).unwrap();
        let (endpoint, _server) = start(StatusCode::OK, response_body).await;
        println!("endpoint = {endpoint}");

        let token_provider = UserTokenProvider {
            client_id: "test-client-id".to_string(),
            client_secret: "test-client-secret".to_string(),
            refresh_token: "test-refresh-token".to_string(),
            endpoint: endpoint,
        };
        let uc = UserCredential {
            token_provider,
            quota_project_id: None,
        };
        let now = OffsetDateTime::now_utc();
        let token = uc.get_token().await?;
        assert_eq!(token.token, "test-access-token");
        assert_eq!(token.token_type, "test-token-type");
        assert!(token
            .expires_at
            .is_some_and(|d| d >= now + Duration::from_secs(3600)));

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn token_provider_partial() -> TestResult {
        let response = Oauth2RefreshResponse {
            access_token: "test-access-token".to_string(),
            expires_in: None,
            refresh_token: None,
            scope: None,
            token_type: "test-token-type".to_string(),
        };
        let response_body = serde_json::to_string(&response).unwrap();
        let (endpoint, _server) = start(StatusCode::OK, response_body).await;
        println!("endpoint = {endpoint}");

        let token_provider = UserTokenProvider {
            client_id: "test-client-id".to_string(),
            client_secret: "test-client-secret".to_string(),
            refresh_token: "test-refresh-token".to_string(),
            endpoint: endpoint,
        };
        let uc = UserCredential {
            token_provider,
            quota_project_id: None,
        };
        let token = uc.get_token().await?;
        assert_eq!(token.token, "test-access-token");
        assert_eq!(token.token_type, "test-token-type");
        assert_eq!(token.expires_at, None);

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn token_provider_retryable_error() -> TestResult {
        let (endpoint, _server) =
            start(StatusCode::SERVICE_UNAVAILABLE, "try again".to_string()).await;
        println!("endpoint = {endpoint}");

        let token_provider = UserTokenProvider {
            client_id: "test-client-id".to_string(),
            client_secret: "test-client-secret".to_string(),
            refresh_token: "test-refresh-token".to_string(),
            endpoint: endpoint,
        };
        let uc = UserCredential {
            token_provider,
            quota_project_id: None,
        };
        let e = uc.get_token().await.err().unwrap();
        assert!(e.is_retryable());
        assert!(e.source().unwrap().to_string().contains("try again"));

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn token_provider_nonretryable_error() -> TestResult {
        let (endpoint, _server) = start(StatusCode::UNAUTHORIZED, "epic fail".to_string()).await;
        println!("endpoint = {endpoint}");

        let token_provider = UserTokenProvider {
            client_id: "test-client-id".to_string(),
            client_secret: "test-client-secret".to_string(),
            refresh_token: "test-refresh-token".to_string(),
            endpoint: endpoint,
        };
        let uc = UserCredential {
            token_provider,
            quota_project_id: None,
        };
        let e = uc.get_token().await.err().unwrap();
        assert!(!e.is_retryable());
        assert!(e.source().unwrap().to_string().contains("epic fail"));

        Ok(())
    }
}
