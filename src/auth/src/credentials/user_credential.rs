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
use http::header::{HeaderName, HeaderValue, AUTHORIZATION};

/// Data model for a UserCredential
#[allow(dead_code)] // TODO(#442) - implementation in progress
pub(crate) struct UserCredential<T>
where
    T: TokenProvider,
{
    token_provider: T,
}

#[async_trait::async_trait]
impl<T> Credential for UserCredential<T>
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

        let mut uc = UserCredential {
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

        let mut uc = UserCredential {
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

        let mut uc = UserCredential {
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

    #[tokio::test]
    async fn get_headers_failure() {
        let mut mock = MockTokenProvider::new();
        mock.expect_get_token()
            .times(1)
            .return_once(|| Err(CredentialError::new(false, Box::from("fail"))));

        let mut uc = UserCredential {
            token_provider: mock,
        };
        assert!(uc.get_headers().await.is_err());
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
}
