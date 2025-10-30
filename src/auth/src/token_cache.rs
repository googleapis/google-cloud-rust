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

use crate::Result;
use crate::credentials::{CacheableResource, EntityTag};
use crate::token::{CachedTokenProvider, Token, TokenProvider};
use http::Extensions;
use std::sync::Arc;
use tokio::sync::watch;
use tokio::time::{Duration, Instant, sleep};

// Different MDS(Metadata Service) backends have different policies to
// determine when to refresh a token. Most MDS' refresh token 5 mins before
// expiry, except for Serverless which refresh tokens 4 mins before
// expiry. So we are using 4 mins as the staleness limit for our refresh logic.
const NORMAL_REFRESH_SLACK: Duration = Duration::from_secs(240);
const SHORT_REFRESH_SLACK: Duration = Duration::from_secs(10);

type TokenResult = Result<(Arc<Token>, EntityTag)>;

#[derive(Debug)]
pub(crate) struct TokenCache<T: TokenProvider> {
    rx_token: watch::Receiver<Option<TokenResult>>,
    token_provider: Arc<T>,
}

// The default implementation requires `T` to implement `Clone`, which is not always the case.
impl<T: TokenProvider> Clone for TokenCache<T> {
    fn clone(&self) -> Self {
        Self {
            rx_token: self.rx_token.clone(),
            token_provider: self.token_provider.clone(),
        }
    }
}

impl<T> TokenCache<T>
where
    T: TokenProvider + Send + Sync + 'static,
{
    pub(crate) fn new(inner: T) -> Self {
        let (tx_token, rx_token) = watch::channel::<Option<TokenResult>>(None);
        let token_provider = Arc::new(inner);

        tokio::spawn(refresh_task(token_provider.clone(), tx_token));

        Self {
            rx_token,
            token_provider,
        }
    }

    async fn latest_token_and_entity_tag(&self) -> Result<(Arc<Token>, EntityTag)> {
        let mut rx = self.rx_token.clone();
        let token_result = rx.borrow_and_update().clone();
        if let Some(token_result) = token_result {
            match token_result {
                Ok((token, tag)) => match token.expires_at {
                    None => Ok((token, tag)),
                    Some(e) => {
                        if e < Instant::now() {
                            // Expired token, wait for refresh
                            wait_for_next_token(rx).await
                        } else {
                            // valid token
                            Ok((token, tag))
                        }
                    }
                },
                // An error in the result is still a valid result to propagate to the client library
                Err(e) => Err(e),
            }
        } else {
            wait_for_next_token(rx).await
        }
    }
}

#[async_trait::async_trait]
impl<T> CachedTokenProvider for TokenCache<T>
where
    T: TokenProvider + Send + Sync + 'static,
{
    async fn token(&self, extensions: Extensions) -> Result<CacheableResource<Arc<Token>>> {
        let (data, entity_tag) = self.latest_token_and_entity_tag().await?;
        match extensions.get::<EntityTag>() {
            Some(tag) if entity_tag.eq(tag) => Ok(CacheableResource::NotModified),
            _ => Ok(CacheableResource::New { entity_tag, data }),
        }
    }
}

async fn wait_for_next_token(
    mut rx_token: watch::Receiver<Option<TokenResult>>,
) -> Result<(Arc<Token>, EntityTag)> {
    rx_token.changed().await.unwrap();
    let token_result = rx_token.borrow().clone();

    token_result.expect("There should always be a token or error in the channel after changed()")
}

async fn refresh_task<T>(token_provider: Arc<T>, tx_token: watch::Sender<Option<TokenResult>>)
where
    T: TokenProvider + Send + Sync + 'static,
{
    loop {
        let token_result = token_provider.token().await;
        let result = token_result
            .as_ref()
            .map(|token| {
                let entity_tag = EntityTag::new();
                (Arc::clone(token), entity_tag)
            })
            .map_err(|e| e.clone());

        let _ = tx_token.send(Some(result));

        match token_result {
            Ok(new_token) => {
                if let Some(expiry) = new_token.expires_at {
                    let time_until_expiry = expiry.checked_duration_since(Instant::now());

                    match time_until_expiry {
                        None => {
                            // We were given a token that is expired, or expires in less than 10 seconds.
                            // We will immediately restart the loop, and fetch a new token.
                        }
                        Some(time_until_expiry) => {
                            if time_until_expiry > NORMAL_REFRESH_SLACK {
                                sleep(time_until_expiry - NORMAL_REFRESH_SLACK).await;
                            } else if time_until_expiry > SHORT_REFRESH_SLACK {
                                // If expiry is less than 4 mins, try to refresh every 10 seconds
                                // This is to handle cases where MDS **repeatedly** returns about to expire tokens.
                                sleep(SHORT_REFRESH_SLACK).await;
                            }
                        }
                    }
                } else {
                    // If there is no expiry, the token is valid forever, so no need to refresh
                    // TODO(#1553): Validate that all auth backends provide expiry and make expiry not optional.
                    break;
                }
            }
            Err(_) => {
                // The retry policy has been used already by the inner token provider.
                // If it ended in an error, just quit the background task.
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::errors;
    use crate::token::tests::MockTokenProvider;
    use gax::error::CredentialsError;
    use std::ops::{Add, Sub};
    use std::sync::{Arc, Mutex};
    use tokio::time::{Duration, Instant};

    static TOKEN_VALID_DURATION: Duration = Duration::from_secs(3600);
    type TestResult = std::result::Result<(), Box<dyn std::error::Error>>;

    fn get_cached_token(cache: CacheableResource<Arc<Token>>) -> Result<Arc<Token>> {
        match cache {
            CacheableResource::New { data, .. } => Ok(data),
            CacheableResource::NotModified => Err(CredentialsError::from_msg(
                false,
                "Expecting token to be present.",
            )),
        }
    }

    #[tokio::test]
    async fn initial_token_success() -> TestResult {
        let expected = Token {
            token: "test-token".to_string(),
            token_type: "Bearer".to_string(),
            expires_at: None,
            metadata: None,
        };
        let expected_clone = expected.clone();

        let mut mock = MockTokenProvider::new();
        mock.expect_token()
            .times(1)
            .return_once(|| Ok(Arc::new(expected_clone)));

        let cache = TokenCache::new(mock);

        let mut extensions = Extensions::new();
        let cached_token = cache.token(extensions.clone()).await.unwrap();
        let (actual, entity_tag) = match cached_token {
            CacheableResource::New { entity_tag, data } => (data, entity_tag),
            CacheableResource::NotModified => unreachable!("expecting new headers"),
        };

        assert_eq!(*actual, expected);

        // Verify that we use the cached token instead of making a new request
        // to the mock token provider.
        let actual = get_cached_token(cache.token(Extensions::new()).await.unwrap())?;
        assert_eq!(*actual, expected);

        // Verify that we return no token if extension is provided.
        extensions.insert(entity_tag);

        let cached_token = cache.token(extensions).await?;

        match cached_token {
            CacheableResource::New { .. } => unreachable!("expecting new headers"),
            CacheableResource::NotModified => CacheableResource::<Arc<Token>>::NotModified,
        };
        Ok(())
    }

    #[tokio::test]
    async fn initial_token_failure() {
        let mut mock = MockTokenProvider::new();
        mock.expect_token()
            .times(1)
            .returning(|| Err(errors::non_retryable_from_str("fail")));

        let cache = TokenCache::new(mock);
        assert!(cache.token(Extensions::new()).await.is_err());

        // Verify that a new request is made to the mock token provider when we
        // don't have a valid token.
        assert!(cache.token(Extensions::new()).await.is_err());
    }

    #[tokio::test(start_paused = true)]
    async fn expired_token_success() -> TestResult {
        let now = Instant::now();

        let initial = Token {
            token: "initial-token".to_string(),
            token_type: "Bearer".to_string(),
            expires_at: Some(now + TOKEN_VALID_DURATION),
            metadata: None,
        };
        let initial_clone = initial.clone();

        let refresh = Token {
            token: "refreshed-token".to_string(),
            token_type: "Bearer".to_string(),
            expires_at: Some(now + 2 * TOKEN_VALID_DURATION),
            metadata: None,
        };
        let refresh_clone = refresh.clone();

        let mut mock = MockTokenProvider::new();
        mock.expect_token()
            .times(1)
            .return_once(|| Ok(Arc::new(initial_clone)));

        mock.expect_token()
            .times(1)
            .return_once(|| Ok(Arc::new(refresh_clone)));

        // fetch an initial token
        let cache = TokenCache::new(mock);
        let actual = get_cached_token(cache.token(Extensions::new()).await.unwrap())?;
        assert_eq!(*actual, initial);

        // wait long enough for the token to be expired
        // token should be waiting until the new token is available
        let sleep = TOKEN_VALID_DURATION.add(Duration::from_secs(100));
        tokio::time::advance(sleep).await;

        // make sure this is the new token
        let actual = get_cached_token(cache.token(Extensions::new()).await.unwrap())?;
        assert_eq!(*actual, refresh);
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn expired_token_failure() -> TestResult {
        let now = Instant::now();

        let initial = Token {
            token: "initial-token".to_string(),
            token_type: "Bearer".to_string(),
            expires_at: Some(now + TOKEN_VALID_DURATION),
            metadata: None,
        };
        let initial_clone = initial.clone();

        let mut mock = MockTokenProvider::new();
        mock.expect_token()
            .times(1)
            .return_once(|| Ok(Arc::new(initial_clone)));

        mock.expect_token()
            .times(1)
            .return_once(|| Err(errors::non_retryable_from_str("fail")));

        // fetch an initial token
        let cache = TokenCache::new(mock);
        let actual = get_cached_token(cache.token(Extensions::new()).await.unwrap())?;
        assert_eq!(*actual, initial);

        // wait long enough for the token to be expired
        let sleep = TOKEN_VALID_DURATION.add(Duration::from_secs(100));
        tokio::time::advance(sleep).await;

        // make sure we return the error, not the expired token
        assert!(cache.token(Extensions::new()).await.is_err());
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn token_cache_multiple_requests_existing_valid_token() -> TestResult {
        let now = Instant::now();

        let token = Token {
            token: "initial-token".to_string(),
            token_type: "Bearer".to_string(),
            expires_at: Some(now + TOKEN_VALID_DURATION),
            metadata: None,
        };
        let token_clone = token.clone();

        let mut mock = MockTokenProvider::new();
        mock.expect_token()
            .times(1)
            .return_once(|| Ok(Arc::new(token_clone)));

        // fetch an initial token
        let cache = TokenCache::new(mock);
        let actual = get_cached_token(cache.token(Extensions::new()).await.unwrap())?;
        assert_eq!(*actual, token);

        // Spawn N tasks, all asking for a token at once.
        let tasks = (0..1000)
            .map(|_| {
                let cache_clone = cache.clone();
                tokio::spawn(async move { cache_clone.token(Extensions::new()).await })
            })
            .collect::<Vec<_>>();

        // Wait for the N token requests to complete, verifying the returned token.
        for task in tasks {
            let actual = task.await.unwrap();
            assert!(actual.is_ok(), "{}", actual.err().unwrap());
            assert_eq!(*get_cached_token(actual.unwrap())?, token);
        }
        Ok(())
    }

    #[tokio::test]
    async fn refresh_task_expired_token_loop() {
        let now = Instant::now();

        let token1 = Token {
            token: "token1".to_string(),
            token_type: "Bearer".to_string(),
            expires_at: Some(now),
            metadata: None,
        };
        let token1_clone = token1.clone();

        let token2 = Token {
            token: "token2".to_string(),
            token_type: "Bearer".to_string(),
            expires_at: Some(now + TOKEN_VALID_DURATION),
            metadata: None,
        };
        let token2_clone = token2.clone();

        let mut mock = MockTokenProvider::new();
        mock.expect_token()
            .times(1)
            .return_once(|| Ok(Arc::new(token1_clone)));

        mock.expect_token()
            .times(1)
            .return_once(|| Ok(Arc::new(token2_clone)));

        let (tx, mut rx) = watch::channel::<Option<Result<(Arc<Token>, EntityTag)>>>(None);

        tokio::spawn(async move {
            refresh_task(Arc::new(mock), tx).await;
        });

        // Give the refresh task a chance to run
        sleep(Duration::from_millis(100)).await;

        rx.changed().await.unwrap();

        // Validate that the refresh loop tried getting new token almost immediately
        assert!(Instant::now() <= now + Duration::from_millis(500));

        let (actual, ..) = rx.borrow().clone().unwrap().unwrap();
        assert_eq!(*actual, token2.clone());
    }

    #[tokio::test(start_paused = true)]
    async fn refresh_task_loop() {
        let now = Instant::now();

        let token1 = Token {
            token: "token1".to_string(),
            token_type: "Bearer".to_string(),
            expires_at: Some(now + TOKEN_VALID_DURATION),
            metadata: None,
        };
        let token1_clone = token1.clone();

        let token2 = Token {
            token: "token2".to_string(),
            token_type: "Bearer".to_string(),
            expires_at: Some(now + 2 * TOKEN_VALID_DURATION),
            metadata: None,
        };
        let token2_clone = token2.clone();

        let token3 = Token {
            token: "token3".to_string(),
            token_type: "Bearer".to_string(),
            expires_at: Some(now + 3 * TOKEN_VALID_DURATION),
            metadata: None,
        };
        let token3_clone = token3.clone();

        let mut mock = MockTokenProvider::new();
        mock.expect_token()
            .times(1)
            .return_once(|| Ok(Arc::new(token1_clone)));

        mock.expect_token()
            .times(1)
            .return_once(|| Ok(Arc::new(token2_clone)));

        mock.expect_token()
            .times(1)
            .return_once(|| Ok(Arc::new(token3_clone)));

        let (tx, mut rx) = watch::channel::<Option<Result<(Arc<Token>, EntityTag)>>>(None);

        // check that channel has None before refresh task starts
        let actual = rx.borrow().clone();
        assert!(actual.is_none());

        tokio::spawn(async move {
            refresh_task(Arc::new(mock), tx).await;
        });

        rx.changed().await.unwrap();
        let (actual, ..) = rx.borrow().clone().unwrap().unwrap();
        assert_eq!(*actual, token1.clone());

        // Validate that it is the same token before it is stale
        let sleep = Duration::from_secs(120);
        tokio::time::advance(sleep).await;
        let (actual, ..) = rx.borrow().clone().unwrap().unwrap();
        assert_eq!(*actual, token1.clone());

        // time machine takes execution to 3 minutes before expiry
        tokio::time::advance(TOKEN_VALID_DURATION.sub(Duration::from_secs(300))).await;

        rx.changed().await.unwrap();

        // validate that the token changed less than 4 mins before expiry
        assert!(Instant::now() < now + TOKEN_VALID_DURATION);
        let (actual, ..) = rx.borrow().clone().unwrap().unwrap();
        assert_eq!(*actual, token2);

        // wait long enough for the token to be expired
        // Adding 500 secs to account for the time manipulation above
        let sleep = TOKEN_VALID_DURATION.add(Duration::from_secs(500));
        tokio::time::advance(sleep).await;

        rx.changed().await.unwrap();
        let (actual, ..) = rx.borrow().clone().unwrap().unwrap();
        assert_eq!(*actual, token3);
    }

    #[tokio::test(start_paused = true)]
    async fn refresh_task_loop_less_expiry() {
        let now = Instant::now();

        let token1 = Token {
            token: "token1".to_string(),
            token_type: "Bearer".to_string(),
            expires_at: Some(now + Duration::from_secs(120)),
            metadata: None,
        };
        let token1_clone = token1.clone();
        let token1_clone2 = token1.clone();

        let token2 = Token {
            token: "token2".to_string(),
            token_type: "Bearer".to_string(),
            expires_at: Some(now + 2 * TOKEN_VALID_DURATION),
            metadata: None,
        };
        let token2_clone = token2.clone();

        let mut mock = MockTokenProvider::new();
        mock.expect_token()
            .times(1)
            .return_once(|| Ok(Arc::new(token1_clone)));

        mock.expect_token()
            .times(1)
            .return_once(|| Ok(Arc::new(token1_clone2)));

        mock.expect_token()
            .times(1)
            .return_once(|| Ok(Arc::new(token2_clone)));

        let (tx, mut rx) = watch::channel::<Option<Result<(Arc<Token>, EntityTag)>>>(None);

        // check that channel has None before refresh task starts
        let actual = rx.borrow().clone();
        assert!(actual.is_none());

        tokio::spawn(async move {
            refresh_task(Arc::new(mock), tx).await;
        });

        rx.changed().await.unwrap();
        let (actual, ..) = rx.borrow().clone().unwrap().unwrap();
        assert_eq!(*actual, token1);

        // time machine forwards time by 10 secs
        tokio::time::advance(Duration::from_secs(10)).await;

        // validate that the same token is obtained and it was
        // attempted to be refreshed within 10ish seconds
        assert!(Instant::now() < now + Duration::from_secs(11));
        rx.changed().await.unwrap();
        let (actual, ..) = rx.borrow().clone().unwrap().unwrap();
        assert_eq!(*actual, token1);

        // time machine forwards time by 100 secs
        tokio::time::advance(Duration::from_secs(100)).await;

        rx.changed().await.unwrap();

        // validate that the token was refreshed within 10ish seconds
        // before expiry
        assert!(Instant::now() < now + Duration::from_secs(111));
        let (actual, ..) = rx.borrow().clone().unwrap().unwrap();
        assert_eq!(*actual, token2);
    }

    #[tokio::test(start_paused = true)]
    async fn refresh_task_loop_long_expiry_waits_long_time_before_refresh() {
        let now = Instant::now();

        let token1 = Token {
            token: "token1".to_string(),
            token_type: "Bearer".to_string(),
            expires_at: Some(now + 3 * NORMAL_REFRESH_SLACK),
            metadata: None,
        };
        let token1_clone = token1.clone();

        let token2 = Token {
            token: "token2".to_string(),
            token_type: "Bearer".to_string(),
            expires_at: Some(now + 2 * TOKEN_VALID_DURATION),
            metadata: None,
        };
        let token2_clone = token2.clone();

        let mut mock = MockTokenProvider::new();
        mock.expect_token()
            .times(1)
            .return_once(|| Ok(Arc::new(token1_clone)));

        mock.expect_token()
            .times(1)
            .return_once(|| Ok(Arc::new(token2_clone)));

        let (tx, mut rx) = watch::channel::<Option<Result<(Arc<Token>, EntityTag)>>>(None);

        // check that channel has None before refresh task starts
        let actual = rx.borrow().clone();
        assert!(actual.is_none());

        tokio::spawn(async move {
            refresh_task(Arc::new(mock), tx).await;
        });

        rx.changed().await.unwrap();

        tokio::time::advance(NORMAL_REFRESH_SLACK).await;

        let (actual, ..) = rx.borrow().clone().unwrap().unwrap();
        assert_eq!(*actual, token1);

        tokio::time::advance(NORMAL_REFRESH_SLACK).await;
        let (actual, ..) = rx.borrow().clone().unwrap().unwrap();
        assert_eq!(*actual, token1);

        tokio::time::advance(2 * NORMAL_REFRESH_SLACK).await;
        let (actual, ..) = rx.borrow().clone().unwrap().unwrap();
        assert_eq!(*actual, token2);
    }

    #[derive(Clone, Debug)]
    struct FakeTokenProvider {
        result: Result<Arc<Token>>,
        calls: Arc<Mutex<i32>>,
    }

    impl FakeTokenProvider {
        pub fn new(result: Result<Token>) -> Self {
            FakeTokenProvider {
                result: result.map(Arc::new),
                calls: Arc::new(Mutex::new(0)),
            }
        }

        pub fn calls(&self) -> i32 {
            *self.calls.lock().unwrap()
        }
    }

    #[async_trait::async_trait]
    impl TokenProvider for FakeTokenProvider {
        async fn token(&self) -> Result<Arc<Token>> {
            // We give enough time for a thundering herd to pile up, while
            // waiting for a change notification from the watch channel.
            sleep(Duration::from_millis(50)).await;

            // Track how many calls were made to the inner token provider.
            *self.calls.lock().unwrap() += 1;

            // Return the result.
            self.result.clone()
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn no_initial_token_thundering_herd_success() -> TestResult {
        let token = Token {
            token: "delayed-token".to_string(),
            token_type: "Bearer".to_string(),
            expires_at: Some(Instant::now()),
            metadata: None,
        };

        let tp = FakeTokenProvider::new(Ok(token.clone()));

        let cache = TokenCache::new(tp.clone());

        // Spawn N tasks, all asking for a token at once.
        let tasks = (0..100)
            .map(|_| {
                let cache_clone = cache.clone();
                tokio::spawn(async move { cache_clone.token(Extensions::new()).await })
            })
            .collect::<Vec<_>>();

        // Wait for the N token requests to complete, verifying the returned token.
        for task in tasks {
            let actual = task.await?;
            assert!(actual.is_ok(), "{}", actual.unwrap_err());
            assert_eq!(*get_cached_token(actual?)?, token);
        }

        let calls = tp.calls();
        // We expect one call to be made to the inner token provider. But if the
        // 100 tasks take longer than 50ms to launch, we may see multiple.
        assert!(calls < 10, "calls to inner token provider: {calls}");
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn no_initial_token_thundering_herd_failure_shares_error() -> TestResult {
        let err = Err(errors::non_retryable_from_str("epic fail"));

        let tp = FakeTokenProvider::new(err);

        let cache = TokenCache::new(tp.clone());

        // Spawn N tasks, all asking for a token at once.
        let tasks = (0..100)
            .map(|_| {
                let cache_clone = cache.clone();
                tokio::spawn(async move { cache_clone.token(Extensions::new()).await })
            })
            .collect::<Vec<_>>();

        // Wait for the N token requests to complete, verifying the returned error.
        for task in tasks {
            let actual = task.await?;
            assert!(actual.is_err(), "{actual:?}");
            let e = format!("{}", actual.unwrap_err());
            assert!(e.contains("epic fail"), "{e}");
        }

        let calls = tp.calls();
        // We expect one call to be made to the inner token provider. But if the
        // 100 tasks take longer than 50ms to launch, we may see multiple.
        assert!(calls < 10, "calls to inner token provider: {calls}");
        Ok(())
    }

    #[tokio::test]
    async fn debug_token_cache() {
        let mut mock_provider = MockTokenProvider::new();
        mock_provider
            .expect_token()
            .return_const(Ok(Arc::new(Token {
                token: "test-token".to_string(),
                token_type: "Bearer".to_string(),
                expires_at: None,
                metadata: None,
            })));

        let cache = TokenCache::new(mock_provider);
        let debug_output = format!("{cache:?}");

        assert!(debug_output.contains("TokenCache"));
        assert!(debug_output.contains("rx_token"));
        assert!(debug_output.contains("token_provider: MockTokenProvider")); // Check for MockTokenProvider specific output part
    }
}
