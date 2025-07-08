// Copyright 2025 Google LLC
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

use crate::constants;
use crate::token::{Token, TokenProvider};
use gax::backoff_policy::BackoffPolicy;
use gax::error::CredentialsError;
use gax::exponential_backoff::ExponentialBackoff;
use gax::retry_loop_internal::retry_loop;
use gax::retry_policy::RetryPolicy;
use gax::retry_throttler::{AdaptiveThrottler, SharedRetryThrottler};
use std::error::Error;
use std::sync::{Arc, Mutex};

type Result<T> = std::result::Result<T, CredentialsError>;

#[derive(Debug)]
pub(crate) struct TokenProviderWithRetry<T: TokenProvider> {
    inner: Arc<T>,
    retry_policy: Option<Arc<dyn RetryPolicy>>,
    backoff_policy: Arc<dyn BackoffPolicy>,
    retry_throttler: SharedRetryThrottler,
}

#[derive(Debug)]
pub(crate) struct Builder<T: TokenProvider> {
    inner: Arc<T>,
    retry_policy: Option<Arc<dyn RetryPolicy>>,
    backoff_policy: Option<Arc<dyn BackoffPolicy>>,
}

#[allow(dead_code)]
impl<T: TokenProvider> Builder<T> {
    pub(crate) fn new(inner: T) -> Self {
        Self {
            inner: Arc::new(inner),
            retry_policy: None,
            backoff_policy: None,
        }
    }

    pub(crate) fn with_retry_policy(mut self, retry_policy: Arc<dyn RetryPolicy>) -> Self {
        self.retry_policy = Some(retry_policy);
        self
    }

    pub(crate) fn with_backoff_policy(mut self, backoff_policy: Arc<dyn BackoffPolicy>) -> Self {
        self.backoff_policy = Some(backoff_policy);
        self
    }

    pub(crate) fn build(self) -> TokenProviderWithRetry<T> {
        let backoff_policy = self
            .backoff_policy
            .unwrap_or_else(|| Arc::new(ExponentialBackoff::default()));
        TokenProviderWithRetry {
            inner: self.inner,
            retry_policy: self.retry_policy,
            backoff_policy,
            retry_throttler: Arc::new(Mutex::new(AdaptiveThrottler::default())),
        }
    }
}

#[async_trait::async_trait]
impl<T: TokenProvider + Send + Sync + 'static> TokenProvider for TokenProviderWithRetry<T> {
    async fn token(&self) -> Result<Token> {
        match self.retry_policy.clone() {
            None => self.inner.token().await,
            Some(policy) => self.execute_retry_loop(policy).await,
        }
    }
}

impl<T> TokenProviderWithRetry<T>
where
    T: TokenProvider,
{
    async fn execute_retry_loop(&self, retry_policy: Arc<dyn RetryPolicy>) -> Result<Token> {
        let inner = self.inner.clone();
        let sleep = async |d| tokio::time::sleep(d).await;
        retry_loop(
            move |_| {
                let inner = inner.clone();
                async move {
                    inner
                        .token()
                        .await
                        .map_err(gax::error::Error::authentication)
                }
            },
            sleep,
            true, // token fetching is idempotent
            self.retry_throttler.clone(),
            retry_policy,
            self.backoff_policy.clone(),
        )
        .await
        .map_err(|e| match e.is_authentication() {
            true => {
                if e.source()
                    .and_then(|s| s.downcast_ref::<CredentialsError>())
                    .map_or(false, |ce| ce.is_transient())
                {
                    CredentialsError::new(true, constants::RETRY_EXHAUSTED_ERROR, e)
                } else {
                    CredentialsError::new(false, constants::TOKEN_FETCH_FAILED_ERROR, e)
                }
            }
            false => CredentialsError::from_source(false, e),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::token::{Token, TokenProvider, test::MockTokenProvider};
    use gax::error::CredentialsError;
    use gax::retry_policy::RetryPolicy;
    use gax::retry_result::RetryResult;
    use mockall::Sequence;
    use std::sync::Arc;

    #[derive(Debug)]
    struct AuthRetryPolicy {
        max_attempts: u32,
    }

    impl RetryPolicy for AuthRetryPolicy {
        fn on_error(
            &self,
            _loop_start: std::time::Instant,
            attempt_count: u32,
            _idempotent: bool,
            error: gax::error::Error,
        ) -> RetryResult {
            if attempt_count >= self.max_attempts {
                return RetryResult::Exhausted(error);
            }

            if error.is_authentication() {
                if error
                    .source()
                    .and_then(|e| e.downcast_ref::<CredentialsError>())
                    .map_or(false, |ce| ce.is_transient())
                {
                    RetryResult::Continue(error)
                } else {
                    RetryResult::Permanent(error)
                }
            } else {
                RetryResult::Permanent(error)
            }
        }
    }

    #[tokio::test]
    async fn test_success_on_first_try() {
        let mut mock_provider = MockTokenProvider::new();

        let token = Token {
            token: "test_token".to_string(),
            token_type: "Bearer".to_string(),
            expires_at: None,
            metadata: Default::default(),
        };

        mock_provider
            .expect_token()
            .times(1)
            .return_once(|| Ok(token));

        let provider = Builder::new(mock_provider)
            .with_retry_policy(Arc::new(AuthRetryPolicy { max_attempts: 2 }))
            .build();

        let token = provider.token().await.unwrap();
        assert_eq!(token.token, "test_token");
    }

    #[tokio::test]
    async fn test_success_after_retry() {
        let mut mock_provider = MockTokenProvider::new();
        let mut seq = Sequence::new();
        mock_provider
            .expect_token()
            .times(1)
            .in_sequence(&mut seq)
            .return_once(|| Err(CredentialsError::from_msg(true, "transient error")));

        mock_provider
            .expect_token()
            .times(1)
            .in_sequence(&mut seq)
            .return_once(|| {
                Ok(Token {
                    token: "test_token".to_string(),
                    token_type: "Bearer".to_string(),
                    expires_at: None,
                    metadata: Default::default(),
                })
            });

        let provider = Builder::new(mock_provider)
            .with_retry_policy(Arc::new(AuthRetryPolicy { max_attempts: 2 }))
            .build();

        let token = provider.token().await.unwrap();
        assert_eq!(token.token, "test_token");
    }

    #[tokio::test]
    async fn test_retry_exhausted() {
        let mut mock_provider = MockTokenProvider::new();
        mock_provider
            .expect_token()
            .times(2)
            .returning(|| Err(CredentialsError::from_msg(true, "transient error")));

        let provider = Builder::new(mock_provider)
            .with_retry_policy(Arc::new(AuthRetryPolicy { max_attempts: 2 }))
            .build();

        let error = provider.token().await.unwrap_err();
        assert!(error.is_transient());
        assert_eq!(
            error.to_string(),
            format!(
                "{} but future attempts may succeed",
                constants::RETRY_EXHAUSTED_ERROR
            )
        );
    }

    #[tokio::test]
    async fn test_non_transient_error() {
        let mut mock_provider = MockTokenProvider::new();
        mock_provider
            .expect_token()
            .times(1)
            .returning(|| Err(CredentialsError::from_msg(false, "non transient error")));

        let provider = Builder::new(mock_provider)
            .with_retry_policy(Arc::new(AuthRetryPolicy { max_attempts: 2 }))
            .build();

        let error = provider.token().await.unwrap_err();
        assert!(!error.is_transient());
        assert_eq!(
            error.to_string(),
            format!(
                "{} and future attempts will not succeed",
                constants::TOKEN_FETCH_FAILED_ERROR
            )
        );
    }

    #[tokio::test]
    async fn test_no_retry_policy_success() {
        let mut mock_provider = MockTokenProvider::new();

        let token = Token {
            token: "test_token".to_string(),
            token_type: "Bearer".to_string(),
            expires_at: None,
            metadata: Default::default(),
        };

        mock_provider
            .expect_token()
            .times(1)
            .return_once(|| Ok(token));

        let provider = Builder::new(mock_provider).build();

        let token = provider.token().await.unwrap();
        assert_eq!(token.token, "test_token");
    }

    #[tokio::test]
    async fn test_no_retry_policy_failure() {
        let mut mock_provider = MockTokenProvider::new();
        mock_provider
            .expect_token()
            .times(1)
            .returning(|| Err(CredentialsError::from_msg(false, "non transient error")));

        let provider = Builder::new(mock_provider).build();

        let error = provider.token().await.unwrap_err();
        assert!(!error.is_transient());
        assert_eq!(
            error.to_string(),
            "non transient error and future attempts will not succeed"
        );
    }
}
