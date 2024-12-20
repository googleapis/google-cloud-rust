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

/// Data model for a MDSCredential
#[allow(dead_code)] // TODO(#442) - implementation in progress
pub(crate) struct MDSCredential<T>
where
    T: TokenProvider,
{
    token_provider: T,
    service_account_email: String,
    quota_project_id: String,
    scopes: Vec<String>,
    default_scopes: Vec<String>,
    universe_domain: String,
}

impl MDSCredential<T>  where T:TokenProvider {
    fn retrieve_info(&mut self) {
        // TODO
        // this function calls metadata endpoint and retrieves info
        // this updates service account email and scopes.
    }
}

// #[async_trait::async_trait]
// impl TokenProvider for MDSCredential {
//   async fn get_token(&mut self) -> Result<Token> {
//     // TODO
//   }
// }

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
}
