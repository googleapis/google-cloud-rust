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
use crate::errors::{BoxError, CredentialError};
use crate::token::{Token, TokenProvider};
use http::header::{HeaderName, HeaderValue};

/// Data model for a UserCredential
#[allow(dead_code)] // TODO(#442) - implementation in progress
pub(crate) struct UserCredential {
    token_provider: Box<dyn TokenProvider>,
}

#[async_trait::async_trait]
impl Credential for UserCredential {
    async fn get_token(&mut self) -> Result<Token> {
        self.token_provider.get_token().await
    }

    async fn get_headers(&mut self) -> Result<Vec<(HeaderName, HeaderValue)>> {
        // TODO(#442) - implementation in progress
        Err(CredentialError::new(false, BoxError::from("unimplemented")))
    }

    async fn get_universe_domain(&mut self) -> Option<String> {
        Some("googleapis.com".to_string())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    struct TestTokenProvider {
        token: Token,
    }

    #[async_trait::async_trait]
    impl TokenProvider for TestTokenProvider {
        async fn get_token(&mut self) -> Result<Token> {
            Ok(self.token.clone())
        }
    }

    struct TestErrorProvider {
        is_retryable: bool,
        message: String,
    }

    #[async_trait::async_trait]
    impl TokenProvider for TestErrorProvider {
        async fn get_token(&mut self) -> Result<Token> {
            Err(CredentialError::new(
                self.is_retryable,
                BoxError::from(self.message.clone()),
            ))
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

        let tp = TestTokenProvider {
            token: expected.clone(),
        };
        let mut uc = UserCredential {
            token_provider: Box::new(tp),
        };
        let actual = uc.get_token().await.unwrap();
        assert_eq!(actual, expected);
    }

    #[tokio::test]
    async fn get_token_failure() {
        let tp = TestErrorProvider {
            is_retryable: false,
            message: "fail".to_string(),
        };
        let mut uc = UserCredential {
            token_provider: Box::new(tp),
        };
        assert!(uc.get_token().await.is_err());
    }
}
