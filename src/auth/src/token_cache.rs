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
use crate::Result;
use std::sync::Arc;
use tokio::sync::{Mutex, Notify};
// Using tokio's wrapper makes the cache testable without relying on clock times.
use tokio::time::Instant;

// TODO(#1210) - implementation in progress
#[allow(dead_code)]
#[derive(Debug)]
pub(crate) struct TokenCache<T>
where
    T: TokenProvider,
{
    // The cached token, or the last seen error.
    token: Arc<Mutex<Result<Token>>>,

    // Tracks if a refresh is ongoing. If the lock is held, there is a refresh.
    refresh_in_progress: Arc<Mutex<()>>,
    // Allows us to await the result of a refresh in multiple threads.
    refresh_notify: Arc<Notify>,

    // The token provider. This thing does the refreshing.
    inner: Arc<T>,
}

// Returns true if we are holding an error, or a token that has expired.
fn invalid(token: &Result<Token>) -> bool {
    match token {
        Ok(t) => t.expires_at.is_some_and(|e| e <= Instant::now().into_std()),
        Err(_) => true,
    }
}

// We manually implement the `Clone` trait because the Rust compiler will
// squawk if `T` is not `Clone`, even though we only hold an `Arc<T>`. :shrug:
impl<T: TokenProvider> Clone for TokenCache<T> {
    fn clone(&self) -> TokenCache<T> {
        TokenCache {
            token: self.token.clone(),
            refresh_in_progress: self.refresh_in_progress.clone(),
            refresh_notify: self.refresh_notify.clone(),
            inner: self.inner.clone(),
        }
    }
}

impl<T: TokenProvider> TokenCache<T> {
    // TODO(#1210) - implementation in progress
    #[allow(dead_code)]
    pub fn new(inner: T) -> TokenCache<T> {
        TokenCache {
            token: Arc::new(Mutex::new(Err(crate::errors::CredentialError::retryable_from_str("No token in the cache. This should never happen. Something has gone wrong. Open an issue at https://github.com/googleapis/google-cloud-rust/issues/new?template=bug_report.md.")))),
            refresh_in_progress: Arc::new(Mutex::new(())),
            refresh_notify: Arc::new(Notify::new()),
            inner: Arc::new(inner),
        }
    }

    // Clones the current token, in a thread-safe manner. Releases the lock on return.
    async fn current_token(&self) -> Result<Token> {
        self.token.lock().await.clone()
    }
}

#[async_trait::async_trait]
impl<T: TokenProvider + 'static> TokenProvider for TokenCache<T> {
    async fn get_token(&self) -> Result<Token> {
        let token = self.current_token().await;

        if !invalid(&token) {
            return token;
        }

        match self.refresh_in_progress.try_lock() {
            // Check if there are any outstanding refreshes...
            Ok(guard) => {
                // No refreshes. We should start one.
                let token = self.inner.get_token().await;

                // Store the token, or an updated error.
                *self.token.lock().await = token.clone();

                // The refresh is complete. Release the refresh guard.
                drop(guard);

                // Notify any and all waiters.
                self.refresh_notify.notify_waiters();

                // Return here without asking for the token lock again.
                return token;
            }
            Err(_) => {
                // There is already a refresh. We will await its result.
                self.refresh_notify.notified().await;
            }
        }

        // The refresh operation has completed. We should have a new
        // error/token. Return it.
        self.current_token().await
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::errors::CredentialError;
    use crate::token::test::MockTokenProvider;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    static TOKEN_VALID_DURATION: Duration = Duration::from_secs(3600);

    #[tokio::test]
    async fn initial_token_success() {
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

        let cache = TokenCache::new(mock);
        let actual = cache.get_token().await.unwrap();
        assert_eq!(actual, expected);

        // Verify that we use the cached token instead of making a new request
        // to the mock token provider.
        let actual = cache.get_token().await.unwrap();
        assert_eq!(actual, expected);
    }

    #[tokio::test]
    async fn initial_token_failure() {
        let mut mock = MockTokenProvider::new();
        mock.expect_get_token()
            .times(2)
            .returning(|| Err(CredentialError::non_retryable_from_str("fail")));

        let cache = TokenCache::new(mock);
        assert!(cache.get_token().await.is_err());

        // Verify that a new request is made to the mock token provider when we
        // don't have a valid token.
        assert!(cache.get_token().await.is_err());
    }

    #[tokio::test(start_paused = true)]
    async fn expired_token_success() {
        let now = Instant::now();

        let initial = Token {
            token: "initial-token".to_string(),
            token_type: "Bearer".to_string(),
            expires_at: Some((now + TOKEN_VALID_DURATION).into_std()),
            metadata: None,
        };
        let initial_clone = initial.clone();

        let refresh = Token {
            token: "refresh-token".to_string(),
            token_type: "Bearer".to_string(),
            expires_at: Some((now + 2 * TOKEN_VALID_DURATION).into_std()),
            metadata: None,
        };
        let refresh_clone = refresh.clone();

        let mut mock = MockTokenProvider::new();
        mock.expect_get_token()
            .times(1)
            .return_once(|| Ok(initial_clone));

        mock.expect_get_token()
            .times(1)
            .return_once(|| Ok(refresh_clone));

        // fetch an initial token
        let cache = TokenCache::new(mock);
        let actual = cache.get_token().await.unwrap();
        assert_eq!(actual, initial);

        // wait long enough for the token to be expired
        let sleep = TOKEN_VALID_DURATION;
        tokio::time::advance(sleep).await;

        // make sure this is the new token
        let actual = cache.get_token().await.unwrap();
        assert_eq!(actual, refresh);
    }

    #[tokio::test(start_paused = true)]
    async fn expired_token_failure() {
        let now = Instant::now();

        let initial = Token {
            token: "initial-token".to_string(),
            token_type: "Bearer".to_string(),
            expires_at: Some((now + TOKEN_VALID_DURATION).into_std()),
            metadata: None,
        };
        let initial_clone = initial.clone();

        let mut mock = MockTokenProvider::new();
        mock.expect_get_token()
            .times(1)
            .return_once(|| Ok(initial_clone));

        mock.expect_get_token()
            .times(1)
            .return_once(|| Err(CredentialError::non_retryable_from_str("fail")));

        // fetch an initial token
        let cache = TokenCache::new(mock);
        let actual = cache.get_token().await.unwrap();
        assert_eq!(actual, initial);

        // wait long enough for the token to be expired
        let sleep = TOKEN_VALID_DURATION;
        tokio::time::advance(sleep).await;

        // make sure we return the error, not the expired token
        assert!(cache.get_token().await.is_err());
    }

    #[derive(Clone, Debug)]
    struct FakeTokenProvider {
        result: Result<Token>,
        calls: Arc<std::sync::Mutex<i32>>,
    }

    impl FakeTokenProvider {
        pub fn new(result: Result<Token>) -> Self {
            FakeTokenProvider {
                result,
                calls: Arc::new(Mutex::new(0)),
            }
        }

        pub fn calls(&self) -> i32 {
            *self.calls.lock().unwrap()
        }
    }

    #[async_trait::async_trait]
    impl TokenProvider for FakeTokenProvider {
        async fn get_token(&self) -> Result<Token> {
            // Release a token periodically. We give enough time for the
            // waiters in a thundering herd to pile up.
            tokio::time::sleep(Duration::from_millis(50)).await;

            // Track how many calls were made to the inner token provider.
            *self.calls.lock().unwrap() += 1;

            // Return the result.
            self.result.clone()
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn initial_token_thundering_herd_success() {
        let token = Token {
            token: "initial-token".to_string(),
            token_type: "Bearer".to_string(),
            expires_at: Some(Instant::now().into_std()),
            metadata: None,
        };

        let tp = FakeTokenProvider::new(Ok(token.clone()));

        let cache = TokenCache::new(tp.clone());

        // Spawn N tasks, all asking for a token at once.
        let tasks = (0..100)
            .map(|_| {
                let cache_clone = cache.clone();
                tokio::spawn(async move { cache_clone.get_token().await })
            })
            .collect::<Vec<_>>();

        // Wait for the N token requests to complete, verifying the returned token.
        for task in tasks {
            let actual = task.await.unwrap();
            assert!(actual.is_ok(), "{}", actual.err().unwrap());
            assert_eq!(actual.unwrap(), token);
        }

        // Given the N requests to the token cache, we expect that not all N
        // requests were passed along to the inner token provider. The
        // expectation is loose, to avoid races between spawning the tasks and
        // executing the first line of code in the task. In most cases, there
        // should be 1 call to the inner token provider.
        let calls = tp.calls();
        println!("Total calls to inner token provider: {calls}");
        assert!(calls < 100);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn initial_token_thundering_herd_failure_shares_error() {
        let err = Err(CredentialError::non_retryable_from_str("epic fail"));

        let tp = FakeTokenProvider::new(err);

        let cache = TokenCache::new(tp.clone());

        // Spawn N tasks, all asking for a token at once.
        let tasks = (0..100)
            .map(|_| {
                let cache_clone = cache.clone();
                tokio::spawn(async move { cache_clone.get_token().await })
            })
            .collect::<Vec<_>>();

        // Wait for the N token requests to complete, verifying the returned error.
        for task in tasks {
            let actual = task.await.unwrap();
            assert!(actual.is_err(), "{:?}", actual.unwrap());
            let e = format!("{}", actual.err().unwrap());
            assert!(e.contains("epic fail"), "{e}");
        }

        // Given the N requests to the token cache, we expect that not all N
        // requests were passed along to the inner token provider. The
        // expectation is loose, to avoid races between spawning the tasks and
        // executing the first line of code in the task. In most cases, there
        // should be 1 call to the inner token provider.
        let calls = tp.calls();
        println!("Total calls to inner token provider: {calls}");
        assert!(calls < 100);
    }
}
