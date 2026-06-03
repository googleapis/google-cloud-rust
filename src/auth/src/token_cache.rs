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

//! The token cache

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

#[derive(Debug, Clone)]
pub(crate) struct TokenCache {
    rx_token: watch::Receiver<Option<Result<(Token, EntityTag)>>>,
}

impl TokenCache {
    pub(crate) fn new<T>(inner: T) -> Self
    where
        T: TokenProvider + Send + Sync + 'static,
    {
        let (tx_token, rx_token) = watch::channel::<Option<Result<(Token, EntityTag)>>>(None);
        let token_provider = Arc::new(inner);

        tokio::spawn(refresh_task(token_provider, tx_token));

        Self { rx_token }
    }

    async fn latest_token_and_entity_tag(&self) -> Result<(Token, EntityTag)> {
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
impl CachedTokenProvider for TokenCache {
    async fn token(&self, extensions: Extensions) -> Result<CacheableResource<Token>> {
        let (data, entity_tag) = self.latest_token_and_entity_tag().await?;
        match extensions.get::<EntityTag>() {
            Some(tag) if entity_tag.eq(tag) => Ok(CacheableResource::NotModified),
            _ => Ok(CacheableResource::New { entity_tag, data }),
        }
    }
}

async fn wait_for_next_token(
    mut rx_token: watch::Receiver<Option<Result<(Token, EntityTag)>>>,
) -> Result<(Token, EntityTag)> {
    rx_token.changed().await.unwrap();
    let token_result = rx_token.borrow().clone();

    token_result.expect("There should always be a token or error in the channel after changed()")
}

fn current_expiration(
    tx_token: &watch::Sender<Option<Result<(Token, EntityTag)>>>,
) -> Option<Instant> {
    tx_token
        .borrow()
        .as_ref()
        .and_then(|r| r.as_ref().ok())
        .and_then(|(token, _)| token.expires_at)
}

async fn refresh_task<T>(
    token_provider: Arc<T>,
    tx_token: watch::Sender<Option<Result<(Token, EntityTag)>>>,
) where
    T: TokenProvider + Send + Sync + 'static,
{
    loop {
        let (expires_at, tagged) = match token_provider.token().await {
            // The easy case, we got a new valid token, that is handled in the body of the loop. The
            // errors are handled here.
            Ok(token) => (token.expires_at, Ok((token, EntityTag::new()))),
            Err(e) if !e.is_transient() => {
                // This is a permanent error. The loop needs to terminate, but first wait until the
                // current token expires (if applicable) before setting the result.
                //
                // If the error was misclassified as permanent, that is a bug in the retry policy
                // and better fixed there than implemented as a workaround here.
                if let Some(deadline) = current_expiration(&tx_token) {
                    tokio::time::sleep_until(deadline).await;
                }
                let _ = tx_token.send(Some(Err(e)));
                return;
            }
            Err(e) => {
                // On transient errors, even if the retry policy is exhausted, we want to continue
                // running this retry loop.
                //
                // This loop cannot stop because that may leave the credentials in an unrecoverable
                // state (see #4541). We considered using a notification to wake up the next time a
                // caller wants to retrieve a token, but that seemed prone to deadlocks. We may
                // implement this as an improvement (#4593).
                let short = Instant::now() + SHORT_REFRESH_SLACK;
                // We need to sleep until the current token expires or the next short refresh slack,
                // whichever happens first.
                //
                // We need to publish the error if the current token expired, or there was no prior
                // token.
                let (deadline, publish) = match current_expiration(&tx_token) {
                    None => (short, true),
                    Some(d) if d < Instant::now() => {
                        // Already expired, replace the cached token with the error.
                        let _ = tx_token.send(Some(Err(e.clone())));
                        (short, false)
                    }
                    Some(d) if d < short => (short, true),
                    Some(_d) => (short, true),
                };
                tokio::time::sleep_until(deadline).await;
                if publish {
                    let _ = tx_token.send(Some(Err(e)));
                }
                continue;
            }
        };

        let _ = tx_token.send(Some(tagged));

        let Some(expiry) = expires_at else {
            // If there is no expiry, the token is valid forever, so no need to refresh
            // TODO(#1553): Validate that all auth backends provide expiry and make expiry not optional.
            break;
        };

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
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::errors;
    use crate::token::tests::MockTokenProvider;
    use google_cloud_gax::error::CredentialsError;
    use std::ops::{Add, Sub};
    use std::sync::{Arc, Mutex};
    use tokio::time::{Duration, Instant};

    static TOKEN_VALID_DURATION: Duration = Duration::from_secs(3600);
    type TestResult = std::result::Result<(), Box<dyn std::error::Error>>;

    fn get_cached_token(cache: CacheableResource<Token>) -> Result<Token> {
        match cache {
            CacheableResource::New { data, .. } => Ok(data),
            CacheableResource::NotModified => Err(CredentialsError::from_msg(
                false,
                "Expecting token to be present.",
            )),
        }
    }

    fn retryable_from_str<T: Into<String>>(message: T) -> CredentialsError {
        CredentialsError::from_msg(true, message)
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
            .return_once(|| Ok(expected_clone));

        let cache = TokenCache::new(mock);

        let mut extensions = Extensions::new();
        let cached_token = cache.token(extensions.clone()).await.unwrap();
        let (actual, entity_tag) = match cached_token {
            CacheableResource::New { entity_tag, data } => (data, entity_tag),
            CacheableResource::NotModified => unreachable!("expecting new headers"),
        };

        assert_eq!(actual, expected);

        // Verify that we use the cached token instead of making a new request
        // to the mock token provider.
        let actual = get_cached_token(cache.token(Extensions::new()).await.unwrap())?;
        assert_eq!(actual, expected);

        // Verify that we return no token if extension is provided.
        extensions.insert(entity_tag);

        let cached_token = cache.token(extensions).await?;

        match cached_token {
            CacheableResource::New { .. } => unreachable!("expecting new headers"),
            CacheableResource::NotModified => CacheableResource::<Token>::NotModified,
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
        let result = cache.token(Extensions::new()).await;
        assert!(result.is_err(), "{result:?}");

        // Verify that a new request is made to the mock token provider when we
        // don't have a valid token.
        let result = cache.token(Extensions::new()).await;
        assert!(result.is_err(), "{result:?}");
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
            .return_once(|| Ok(initial_clone));

        mock.expect_token()
            .times(1)
            .return_once(|| Ok(refresh_clone));

        // fetch an initial token
        let cache = TokenCache::new(mock);
        let actual = get_cached_token(cache.token(Extensions::new()).await.unwrap())?;
        assert_eq!(actual, initial);

        // wait long enough for the token to be expired
        // token should be waiting until the new token is available
        let sleep = TOKEN_VALID_DURATION.add(Duration::from_secs(100));
        tokio::time::advance(sleep).await;

        // make sure this is the new token
        let actual = get_cached_token(cache.token(Extensions::new()).await.unwrap())?;
        assert_eq!(actual, refresh);
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
            .return_once(|| Ok(initial_clone));

        mock.expect_token()
            .times(1)
            .return_once(|| Err(errors::non_retryable_from_str("fail")));

        // fetch an initial token
        let cache = TokenCache::new(mock);
        let actual = get_cached_token(cache.token(Extensions::new()).await.unwrap())?;
        assert_eq!(actual, initial);

        // wait long enough for the token to be expired
        let sleep = TOKEN_VALID_DURATION.add(Duration::from_secs(100));
        tokio::time::advance(sleep).await;

        // make sure we return the error, not the expired token
        let result = cache.token(Extensions::new()).await;
        assert!(result.is_err(), "{result:?}");
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
        mock.expect_token().times(1).return_once(|| Ok(token_clone));

        // fetch an initial token
        let cache = TokenCache::new(mock);
        let actual = get_cached_token(cache.token(Extensions::new()).await.unwrap())?;
        assert_eq!(actual, token);

        // Spawn N tasks, all asking for a token at once.
        let tasks = (0..1000)
            .map(|_| {
                let cache_clone = cache.clone();
                tokio::spawn(async move { cache_clone.token(Extensions::new()).await })
            })
            .collect::<Vec<_>>();

        // Wait for the N token requests to complete, verifying the returned token.
        for task in tasks {
            let actual = task.await??;
            assert_eq!(get_cached_token(actual)?, token);
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
            .return_once(|| Ok(token1_clone));

        mock.expect_token()
            .times(1)
            .return_once(|| Ok(token2_clone));

        let (tx, mut rx) = watch::channel::<Option<Result<(Token, EntityTag)>>>(None);

        tokio::spawn(async move {
            refresh_task(Arc::new(mock), tx).await;
        });

        // Give the refresh task a chance to run
        sleep(Duration::from_millis(100)).await;

        rx.changed().await.unwrap();

        // Validate that the refresh loop tried getting new token almost immediately
        assert!(Instant::now() <= now + Duration::from_millis(500));

        let (actual, ..) = rx.borrow().clone().unwrap().unwrap();
        assert_eq!(actual, token2.clone());
    }

    #[tokio::test(start_paused = true)]
    async fn refresh_permanent_failure_preserves_valid_token() {
        let now = Instant::now();
        let token = Token {
            token: "test-token".to_string(),
            token_type: "Bearer".to_string(),
            expires_at: Some(now + 3 * NORMAL_REFRESH_SLACK),
            metadata: None,
        };
        let mut mock = MockTokenProvider::new();
        mock.expect_token().times(1).return_once({
            let c = token.clone();
            move || Ok(c.clone())
        });
        // Refresh at T - NORMAL_REFRESH_SLACK fails with a permatransient error.
        mock.expect_token()
            .times(1..)
            .returning(|| Err(errors::non_retryable_from_str("uh oh")));

        let cache = TokenCache::new(mock);
        let got = cache.token(Extensions::new()).await;
        assert!(
            matches!(
                got.as_ref(),
                Ok(CacheableResource::New {
                    entity_tag: _,
                    data: t
                }) if t == &token
            ),
            "{got:?}"
        );

        // Advance past the refresh point. The original token is still valid for ~NORMAL_REFRESH_SLACK.
        tokio::time::sleep(2 * NORMAL_REFRESH_SLACK + SHORT_REFRESH_SLACK).await;
        let got = cache.token(Extensions::new()).await;
        assert!(got.is_ok(), "{got:?}");

        // Advance past the expiration point. The original token cannot work and this should return
        // the permanent error.
        tokio::time::sleep(NORMAL_REFRESH_SLACK + SHORT_REFRESH_SLACK).await;
        let got = cache.token(Extensions::new()).await;
        assert!(got.is_err(), "{got:?}");
    }

    #[tokio::test(start_paused = true)]
    async fn refresh_transient_failure_preserves_valid_token() {
        let now = Instant::now();
        let token = Token {
            token: "test-token".to_string(),
            token_type: "Bearer".to_string(),
            expires_at: Some(now + 3 * NORMAL_REFRESH_SLACK),
            metadata: None,
        };
        let mut mock = MockTokenProvider::new();
        mock.expect_token().times(1).return_once({
            let c = token.clone();
            move || Ok(c.clone())
        });
        // Refresh at T - NORMAL_REFRESH_SLACK fails with a transient error.
        mock.expect_token()
            .times(1..)
            .returning(|| Err(retryable_from_str("try again")));

        let cache = TokenCache::new(mock);
        let got = cache.token(Extensions::new()).await;
        assert!(
            matches!(
                got.as_ref(),
                Ok(CacheableResource::New {
                    entity_tag: _,
                    data: t
                }) if t == &token
            ),
            "{got:?}"
        );

        // Advance past the refresh point; the original token is still valid for ~NORMAL_REFRESH_SLACK.
        tokio::time::sleep(2 * NORMAL_REFRESH_SLACK + SHORT_REFRESH_SLACK).await;

        // This should succeed. The error was transient and the token has not expired.
        let got = cache.token(Extensions::new()).await;
        assert!(got.is_ok(), "{got:?}");
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
            .return_once(|| Ok(token1_clone));

        mock.expect_token()
            .times(1)
            .return_once(|| Ok(token2_clone));

        mock.expect_token()
            .times(1)
            .return_once(|| Ok(token3_clone));

        let (tx, mut rx) = watch::channel::<Option<Result<(Token, EntityTag)>>>(None);

        // check that channel has None before refresh task starts
        let actual = rx.borrow().clone();
        assert!(actual.is_none(), "{actual:?}");

        tokio::spawn(async move {
            refresh_task(Arc::new(mock), tx).await;
        });

        rx.changed().await.unwrap();
        let (actual, ..) = rx.borrow().clone().unwrap().unwrap();
        assert_eq!(actual, token1.clone());

        // Validate that it is the same token before it is stale
        let sleep = Duration::from_secs(120);
        tokio::time::advance(sleep).await;
        let (actual, ..) = rx.borrow().clone().unwrap().unwrap();
        assert_eq!(actual, token1.clone());

        // time machine takes execution to 3 minutes before expiry
        tokio::time::advance(TOKEN_VALID_DURATION.sub(Duration::from_secs(300))).await;

        rx.changed().await.unwrap();

        // validate that the token changed less than 4 mins before expiry
        assert!(Instant::now() < now + TOKEN_VALID_DURATION);
        let (actual, ..) = rx.borrow().clone().unwrap().unwrap();
        assert_eq!(actual, token2);

        // wait long enough for the token to be expired
        // Adding 500 secs to account for the time manipulation above
        let sleep = TOKEN_VALID_DURATION.add(Duration::from_secs(500));
        tokio::time::advance(sleep).await;

        rx.changed().await.unwrap();
        let (actual, ..) = rx.borrow().clone().unwrap().unwrap();
        assert_eq!(actual, token3);
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
            .return_once(|| Ok(token1_clone));

        mock.expect_token()
            .times(1)
            .return_once(|| Ok(token1_clone2));

        mock.expect_token()
            .times(1)
            .return_once(|| Ok(token2_clone));

        let (tx, mut rx) = watch::channel::<Option<Result<(Token, EntityTag)>>>(None);

        // check that channel has None before refresh task starts
        let actual = rx.borrow().clone();
        assert!(actual.is_none(), "{actual:?}");

        tokio::spawn(async move {
            refresh_task(Arc::new(mock), tx).await;
        });

        rx.changed().await.unwrap();
        let (actual, ..) = rx.borrow().clone().unwrap().unwrap();
        assert_eq!(actual, token1);

        // time machine forwards time by 10 secs
        tokio::time::advance(Duration::from_secs(10)).await;

        // validate that the same token is obtained and it was
        // attempted to be refreshed within 10ish seconds
        assert!(Instant::now() < now + Duration::from_secs(11));
        rx.changed().await.unwrap();
        let (actual, ..) = rx.borrow().clone().unwrap().unwrap();
        assert_eq!(actual, token1);

        // time machine forwards time by 100 secs
        tokio::time::advance(Duration::from_secs(100)).await;

        rx.changed().await.unwrap();

        // validate that the token was refreshed within 10ish seconds
        // before expiry
        assert!(Instant::now() < now + Duration::from_secs(111));
        let (actual, ..) = rx.borrow().clone().unwrap().unwrap();
        assert_eq!(actual, token2);
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
            .return_once(|| Ok(token1_clone));

        mock.expect_token()
            .times(1)
            .return_once(|| Ok(token2_clone));

        let (tx, mut rx) = watch::channel::<Option<Result<(Token, EntityTag)>>>(None);

        // check that channel has None before refresh task starts
        let actual = rx.borrow().clone();
        assert!(actual.is_none(), "{actual:?}");

        tokio::spawn(async move {
            refresh_task(Arc::new(mock), tx).await;
        });

        rx.changed().await.unwrap();

        tokio::time::advance(NORMAL_REFRESH_SLACK).await;

        let (actual, ..) = rx.borrow().clone().unwrap().unwrap();
        assert_eq!(actual, token1);

        tokio::time::advance(NORMAL_REFRESH_SLACK).await;
        let (actual, ..) = rx.borrow().clone().unwrap().unwrap();
        assert_eq!(actual, token1);

        tokio::time::advance(2 * NORMAL_REFRESH_SLACK).await;
        let (actual, ..) = rx.borrow().clone().unwrap().unwrap();
        assert_eq!(actual, token2);
    }

    #[tokio::test(start_paused = true)]
    async fn refresh_task_sleeps_on_transient_error_and_recovers_on_next_loop() -> TestResult {
        const TEST_INTERVAL: Duration = Duration::from_secs(60);
        let now = Instant::now();
        // Simulate a token provider that first returns token1, then returns a transient error, and then returns token2.
        //
        // The expected behavior is to return token1 until it expires. Then the transient error,
        // and then token2.
        let start_returning_error = now + TEST_INTERVAL;
        let first_token_expires = now + 2 * TEST_INTERVAL;
        let start_returning_second_token = now + 3 * TEST_INTERVAL;
        let second_token_expires = now + 4 * TEST_INTERVAL;

        let token1 = Token {
            token: "token-1".to_string(),
            token_type: "Bearer".to_string(),
            expires_at: Some(first_token_expires),
            metadata: None,
        };
        let token2 = Token {
            token: "token-2".to_string(),
            token_type: "Bearer".to_string(),
            expires_at: Some(second_token_expires),
            metadata: None,
        };

        let mut mock = MockTokenProvider::new();
        mock.expect_token().times(3..).returning({
            let t1 = token1.clone();
            let t2 = token2.clone();
            move || {
                if Instant::now() < start_returning_error {
                    return Ok(t1.clone());
                }
                if Instant::now() < start_returning_second_token {
                    return Err(CredentialsError::from_msg(true, "transient error"));
                }
                Ok(t2.clone())
            }
        });

        let cache = TokenCache::new(mock);

        let got = cache.token(Extensions::new()).await;
        assert!(
            matches!(got.as_ref(), Ok(CacheableResource::New { data: t, .. }) if t == &token1),
            "{got:?}"
        );

        // Advance time to the point where the token provider starts returning errors.
        tokio::time::sleep_until(start_returning_error + Duration::from_secs(1)).await;

        // The token is not expired yet, so it should succeed.
        let got = cache.token(Extensions::new()).await;
        assert!(
            matches!(got.as_ref(), Ok(CacheableResource::New { data: t, .. }) if t == &token1),
            "{got:?}"
        );

        // Advance time to the point where the token is expired and the token provider is
        // still returning errors.
        tokio::time::sleep_until(first_token_expires + Duration::from_secs(1)).await;
        let got = cache.token(Extensions::new()).await;
        assert!(
            matches!(got.as_ref(), Err(e) if e.is_transient()),
            "{got:?}"
        );

        // Advance time to the point where the token is expired and the token provider is
        // still returning errors.
        tokio::time::sleep_until(start_returning_second_token + Duration::from_secs(1)).await;
        let got = cache.token(Extensions::new()).await;
        assert!(
            matches!(got.as_ref(), Ok(CacheableResource::New { data: t, .. }) if t == &token2),
            "{got:?}"
        );

        Ok(())
    }

    #[derive(Clone, Debug)]
    struct FakeTokenProvider {
        result: Result<Token>,
        calls: Arc<Mutex<i32>>,
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
        async fn token(&self) -> Result<Token> {
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
            let actual = task.await??;
            assert_eq!(get_cached_token(actual)?, token);
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
        mock_provider.expect_token().return_const(Ok(Token {
            token: "test-token".to_string(),
            token_type: "Bearer".to_string(),
            expires_at: None,
            metadata: None,
        }));

        let cache = TokenCache::new(mock_provider);
        let debug_output = format!("{cache:?}");

        assert!(debug_output.contains("TokenCache"));
        assert!(debug_output.contains("rx_token"));
    }
}
