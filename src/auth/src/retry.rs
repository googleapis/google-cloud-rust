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

use crate::token::{Token, TokenProvider};
use crate::{Result, constants};
use gax::backoff_policy::BackoffPolicy;
use gax::error::CredentialsError;
use gax::exponential_backoff::ExponentialBackoff;
use gax::retry_loop_internal::retry_loop;
use gax::retry_policy::RetryPolicy;
use gax::retry_throttler::{AdaptiveThrottler, SharedRetryThrottler};
use std::error::Error;
use std::sync::{Arc, Mutex};

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
    retry_throttler: Option<SharedRetryThrottler>,
}

#[allow(dead_code)]
impl<T: TokenProvider> Builder<T> {
    pub(crate) fn new(inner: T) -> Self {
        Self {
            inner: Arc::new(inner),
            retry_policy: None,
            backoff_policy: None,
            retry_throttler: None,
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

    pub(crate) fn with_retry_throttler(mut self, retry_throttler: SharedRetryThrottler) -> Self {
        self.retry_throttler = Some(retry_throttler);
        self
    }

    pub(crate) fn build(self) -> TokenProviderWithRetry<T> {
        let backoff_policy = self
            .backoff_policy
            .unwrap_or_else(|| Arc::new(ExponentialBackoff::default()));
        let retry_throttler = self
            .retry_throttler
            .unwrap_or_else(|| Arc::new(Mutex::new(AdaptiveThrottler::default())));
        TokenProviderWithRetry {
            inner: self.inner,
            retry_policy: self.retry_policy,
            backoff_policy,
            retry_throttler,
        }
    }
}

#[async_trait::async_trait]
impl<T: TokenProvider + 'static> TokenProvider for TokenProviderWithRetry<T> {
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
        let fetch_token = move |_| {
            let inner = inner.clone();
            async move {
                inner
                    .token()
                    .await
                    .map_err(gax::error::Error::authentication)
            }
        };

        retry_loop(
            fetch_token,
            sleep,
            true, // token fetching is idempotent
            self.retry_throttler.clone(),
            retry_policy,
            self.backoff_policy.clone(),
        )
        .await
        .map_err(Self::map_retry_error)
    }

    fn map_retry_error(e: gax::error::Error) -> CredentialsError {
        match e {
            auth_error if auth_error.is_authentication() => {
                let (is_transient, msg) = if auth_error
                    .source()
                    .and_then(|s| s.downcast_ref::<CredentialsError>())
                    .is_some_and(|ce| ce.is_transient())
                {
                    (true, constants::RETRY_EXHAUSTED_ERROR)
                } else {
                    (false, constants::TOKEN_FETCH_FAILED_ERROR)
                };
                CredentialsError::new(is_transient, msg, auth_error)
            }
            other_error => CredentialsError::from_source(false, other_error),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::token::{Token, TokenProvider, test::MockTokenProvider};
    use gax::error::CredentialsError;
    use gax::retry_policy::RetryPolicy;
    use gax::retry_result::RetryResult;
    use gax::retry_throttler::RetryThrottler;
    use mockall::{Sequence, mock};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use test_case::test_case;

    mock! {
        #[derive(Debug)]
        pub RetryThrottler {}

        impl RetryThrottler for RetryThrottler {
            fn throttle_retry_attempt(&self) -> bool;
            fn on_retry_failure(&mut self, flow: &RetryResult);
            fn on_success(&mut self);
        }
    }

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
                    .is_some_and(|ce| ce.is_transient())
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

    #[derive(Debug)]
    struct TestBackoffPolicy {
        was_called: Arc<AtomicBool>,
    }

    impl Default for TestBackoffPolicy {
        fn default() -> Self {
            Self {
                was_called: Arc::new(AtomicBool::new(false)),
            }
        }
    }

    impl BackoffPolicy for TestBackoffPolicy {
        fn on_failure(
            &self,
            _loop_start: std::time::Instant,
            _attempt_count: u32,
        ) -> std::time::Duration {
            self.was_called.store(true, Ordering::SeqCst);
            std::time::Duration::from_millis(1)
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

    #[test_case(true, "but future attempts may succeed" ; "transient_error")]
    #[test_case(false, "and future attempts will not succeed" ; "non_transient_error")]
    #[tokio::test]
    async fn test_no_retry_policy_failure(is_transient: bool, expected_suffix: &str) {
        let mut mock_provider = MockTokenProvider::new();
        const ERROR_MESSAGE: &str = "underlying provider error";
        mock_provider
            .expect_token()
            .times(1)
            .returning(move || Err(CredentialsError::from_msg(is_transient, ERROR_MESSAGE)));

        let provider = Builder::new(mock_provider).build();

        let error = provider.token().await.unwrap_err();
        assert_eq!(error.is_transient(), is_transient);
        let expected_message = format!("{} {}", ERROR_MESSAGE, expected_suffix);
        assert_eq!(error.to_string(), expected_message);
    }

    #[test_case(
        true,
        &["AuthRetryPolicy", "max_attempts: 5", "TestBackoffPolicy", "AdaptiveThrottler", "factor: 4.0"];
        "with_custom_values"
    )]
    #[test_case(
        false,
        &["retry_policy: None", "ExponentialBackoff", "AdaptiveThrottler", "factor: 2.0"];
        "with_default_values"
    )]
    fn test_builder(use_custom_config: bool, expected_substrings: &[&str]) {
        let mock_provider = MockTokenProvider::new();
        let mut builder = Builder::new(mock_provider);

        if use_custom_config {
            let retry_policy = Arc::new(AuthRetryPolicy { max_attempts: 5 });
            let backoff_policy = Arc::new(TestBackoffPolicy::default());
            let retry_throttler = Arc::new(Mutex::new(AdaptiveThrottler::new(4.0).unwrap()));
            builder = builder
                .with_retry_policy(retry_policy)
                .with_backoff_policy(backoff_policy)
                .with_retry_throttler(retry_throttler);
        }

        let provider = builder.build();
        let debug_str = format!("{provider:?}");

        for sub in expected_substrings {
            assert!(
                debug_str.contains(sub),
                "Expected to find '{sub}' in '{debug_str:?}'"
            );
        }
    }

    #[tokio::test]
    async fn test_full_retry_mechanism() {
        // 1. Setup Mocks
        let mut mock_provider = MockTokenProvider::new();
        let mut mock_throttler = MockRetryThrottler::new();

        // Token provider fails once, then succeeds.
        let mut seq = Sequence::new();
        mock_provider
            .expect_token()
            .times(1)
            .in_sequence(&mut seq)
            .return_once(|| {
                Err(CredentialsError::from_msg(
                    true,
                    "transient error for full test",
                ))
            });
        mock_provider
            .expect_token()
            .times(1)
            .in_sequence(&mut seq)
            .return_once(|| {
                Ok(Token {
                    token: "final_token".to_string(),
                    token_type: "Bearer".to_string(),
                    expires_at: None,
                    metadata: Default::default(),
                })
            });

        // 2. Setup Throttler Expectations
        mock_throttler
            .expect_throttle_retry_attempt()
            .times(1)
            .returning(|| false);
        mock_throttler
            .expect_on_retry_failure()
            .times(1)
            .withf(|result| matches!(result, RetryResult::Continue(_)))
            .return_const(());
        mock_throttler.expect_on_success().times(1).return_const(());

        // 3. Setup other policies
        let retry_policy = Arc::new(AuthRetryPolicy { max_attempts: 2 });
        let backoff_was_called = Arc::new(AtomicBool::new(false));
        let backoff_policy = Arc::new(TestBackoffPolicy {
            was_called: backoff_was_called.clone(),
        });
        let retry_throttler = Arc::new(Mutex::new(mock_throttler));

        // 4. Build and run
        let provider = Builder::new(mock_provider)
            .with_retry_policy(retry_policy)
            .with_backoff_policy(backoff_policy)
            .with_retry_throttler(retry_throttler)
            .build();

        // 5. Assert
        let token = provider.token().await.unwrap();
        assert_eq!(token.token, "final_token");
        assert!(
            backoff_was_called.load(Ordering::SeqCst),
            "Backoff policy was not called"
        );
    }

    #[test]
    fn test_map_retry_error_non_auth_error() {
        // 1. Create a non-authentication error.
        let original_error = gax::error::Error::io("test-io-error");
        let original_error_string = original_error.to_string();

        // 2. Call the function under test.
        let credentials_error =
            TokenProviderWithRetry::<MockTokenProvider>::map_retry_error(original_error);

        // 3. Assert that the resulting error is not transient and wraps the original error.
        assert!(!credentials_error.is_transient());
        assert_eq!(
            credentials_error.source().unwrap().to_string(),
            original_error_string
        );
    }
}
