use crate::token::{Token, TokenProvider};
use tokio::time::{sleep, sleep_until, Instant, Duration};
use tokio::sync::watch;
use crate::Result;


const FIVE_MINUTES: Duration = Duration::from_secs(300);
const TEN_SECONDS: Duration = Duration::from_secs(10); 

#[derive(Debug)]
pub(crate) struct TokenCache<T>
where
    T: TokenProvider + Send + Sync + 'static
{
    rx_token: watch::Receiver<Option<Result<Token>>>,
    _phantom: std::marker::PhantomData<T>,
}

impl<T: TokenProvider> Clone for TokenCache<T> {
    fn clone(&self) -> TokenCache<T> {
        TokenCache {
            rx_token: self.rx_token.clone(),
            _phantom: std::marker::PhantomData,
        }
    }
}

// TODO 1552: Use the token cache in all creds
#[allow(dead_code)]
impl<T> TokenCache<T>
where
    T: TokenProvider + Send + Sync + 'static
{
    pub(crate) fn new(inner: T) -> Self {
        let (tx_token, rx_token) = watch::channel::<Option<Result<Token>>>(None);

        tokio::spawn(async move {
            refresh_task(inner, tx_token).await;
        });

        Self {
            rx_token,
            _phantom: std::marker::PhantomData,
        }
    }
}

#[async_trait::async_trait]
impl<T> TokenProvider for TokenCache<T>
    where 
        T: TokenProvider + Send + Sync + 'static
{
    async fn get_token(&self) -> Result<Token> {
        let mut rx = self.rx_token.clone();
        let token_result = rx.borrow_and_update().clone();

        if let Some(token_result) = token_result {
            match token_result {
                Ok(token) => match token.expires_at {
                        None => return Ok(token),
                        Some(e) => {
                            if e < Instant::now().into_std() {
                                // Expired token, wait for refresh
                                return get_token_from_channel(rx).await
                            } else {
                                // valid token
                                return Ok(token)
                            }
                        }
                    },
                // An error in the result is still a valid result to propagate to the client library
                Err(e) => Err(e),
            }
        }
        else {
            get_token_from_channel(rx).await
        }
    }
}

async fn get_token_from_channel(mut rx_token: watch::Receiver<Option<Result<Token>>>) -> Result<Token> {
    // We have already checked the current token in the channel, wait for new one
    rx_token.changed().await.unwrap();
    let token_result = rx_token.borrow().clone();

    if let Some(token_result) = token_result {
        return token_result;
    }
    else {
        panic!("There should always be a token or error in the channel after changed()")
    }
}

async fn refresh_task<T>(
    token_provider: T,
    tx_token: watch::Sender<Option<Result<Token>>>,
)
where
    T: TokenProvider + Send + Sync + 'static
{
    loop {
        let token_result = token_provider.get_token().await;

        let _ = tx_token.send(Some(token_result.clone()));

        match token_result {
            Ok(new_token) => {
                if let Some(expiry) = new_token.expires_at {
                    let time_until_expiry = expiry.duration_since(Instant::now().into_std());

                    if time_until_expiry > FIVE_MINUTES {
                        let time_to_sleep = Instant::now() + time_until_expiry - FIVE_MINUTES;
                        sleep_until(time_to_sleep.into()).await;
                    } else if time_until_expiry > TEN_SECONDS {
                        // If expiry is less than 5 mins, wait till last 10 seconds to refresh
                        // This is to handle cases where MDS returns about to refresh tokens
                        let time_to_sleep = Instant::now() + time_until_expiry - TEN_SECONDS;
                        sleep_until(time_to_sleep.into()).await;
                    }
                } else {
                    // Handle case where expiry is not available
                    // TODO 1553: Validate that all auth backends provide expiry and make expiry not optional.
                    sleep(FIVE_MINUTES).await;
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
mod test {
    use super::*;
    use crate::errors::CredentialError;
    use crate::token::test::MockTokenProvider;
    use std::ops::{Add, Sub};
    use std::sync::{Arc, Mutex};
    use tokio::time::{Instant, Duration};

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
            .times(1)
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
            token: "refreshed-token".to_string(),
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
        // get_token should be waiting until the new token is available
        let sleep = TOKEN_VALID_DURATION.add(Duration::from_secs(100));
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
        let sleep = TOKEN_VALID_DURATION.add(Duration::from_secs(100));
        tokio::time::advance(sleep).await;

        // make sure we return the error, not the expired token
        assert!(cache.get_token().await.is_err());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn token_cache_multiple_requests_existing_valid_token() {
        let now = Instant::now();

        let token = Token {
            token: "initial-token".to_string(),
            token_type: "Bearer".to_string(),
            expires_at: Some((now + TOKEN_VALID_DURATION).into_std()),
            metadata: None,
        };
        let token_clone = token.clone();

        let mut mock = MockTokenProvider::new();
        mock.expect_get_token()
            .times(1)
            .return_once(|| Ok(token_clone));

        // fetch an initial token
        let cache = TokenCache::new(mock);
        let actual = cache.get_token().await.unwrap();
        assert_eq!(actual, token);

        // Spawn N tasks, all asking for a token at once.
        let tasks = (0..1000)
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
    }

    #[tokio::test(start_paused = true)]
    async fn refresh_task_loop() {
        let now = Instant::now();

        let token1 = Token {
            token: "token1".to_string(),
            token_type: "Bearer".to_string(),
            expires_at: Some((now + TOKEN_VALID_DURATION).into_std()),
            metadata: None,
        };
        let token1_clone = token1.clone();

        let token2 = Token {
            token: "token2".to_string(),
            token_type: "Bearer".to_string(),
            expires_at: Some((now + 2 * TOKEN_VALID_DURATION).into_std()),
            metadata: None,
        };
        let token2_clone = token2.clone();

        let token3 = Token {
            token: "token3".to_string(),
            token_type: "Bearer".to_string(),
            expires_at: Some((now + 3 * TOKEN_VALID_DURATION).into_std()),
            metadata: None,
        };
        let token3_clone = token3.clone();

        let mut mock = MockTokenProvider::new();
        mock.expect_get_token()
            .times(1)
            .return_once(|| Ok(token1_clone));

        mock.expect_get_token()
            .times(1)
            .return_once(|| Ok(token2_clone));

        mock.expect_get_token()
            .times(1)
            .return_once(|| Ok(token3_clone));

        let (tx, mut rx) = watch::channel::<Option<Result<Token>>>(None);

        // check that channel has None before refresh task starts
        let actual = rx.borrow().clone();
        assert!(actual.is_none());

        tokio::spawn(async move {
            refresh_task(mock, tx).await;
        });

        rx.changed().await.unwrap();
        let actual = rx.borrow().clone().unwrap().unwrap();
        assert_eq!(actual, token1.clone());

        // Validate that it is the same token before it expires
        let sleep = Duration::from_secs(120);
        tokio::time::advance(sleep).await;
        let actual = rx.borrow().clone().unwrap().unwrap();
        assert_eq!(actual, token1.clone());

        // time machine takes execution to 3 minutes before expiry       
        tokio::time::advance(TOKEN_VALID_DURATION.sub(Duration::from_secs(300))).await;

        rx.changed().await.unwrap();

        // validate that the token changed less than 5 mins before expiry
        assert!(Instant::now() < now + TOKEN_VALID_DURATION);
        let actual = rx.borrow().clone().unwrap().unwrap();
        assert_eq!(actual, token2);

        // wait long enough for the token to be expired
        // Adding 500 secs to account for the time manipulation above
        let sleep = TOKEN_VALID_DURATION.add(Duration::from_secs(500));
        tokio::time::advance(sleep).await;

        rx.changed().await.unwrap();
        let actual = rx.borrow().clone().unwrap().unwrap();
        assert_eq!(actual, token3);
    }

    #[tokio::test(start_paused = true)]
    async fn refresh_task_loop_less_expiry() {
        let now = Instant::now();

        let token1 = Token {
            token: "token1".to_string(),
            token_type: "Bearer".to_string(),
            expires_at: Some((now + Duration::from_secs(120)).into_std()),
            metadata: None,
        };
        let token1_clone = token1.clone();

        let token2 = Token {
            token: "token2".to_string(),
            token_type: "Bearer".to_string(),
            expires_at: Some((now + 2 * TOKEN_VALID_DURATION).into_std()),
            metadata: None,
        };
        let token2_clone = token2.clone();

        let mut mock = MockTokenProvider::new();
        mock.expect_get_token()
            .times(1)
            .return_once(|| Ok(token1_clone));

        mock.expect_get_token()
            .times(1)
            .return_once(|| Ok(token2_clone));

        let (tx, mut rx) = watch::channel::<Option<Result<Token>>>(None);

        // check that channel has None before refresh task starts
        let actual = rx.borrow().clone();
        assert!(actual.is_none());

        tokio::spawn(async move {
            refresh_task(mock, tx).await;
        });

        rx.changed().await.unwrap();
        let actual = rx.borrow().clone().unwrap().unwrap();
        assert_eq!(actual, token1);

        // time machine forwards time to 10 seconds before expiry
        tokio::time::advance(Duration::from_secs(110)).await;

        rx.changed().await.unwrap();

        // validate that the token was refreshed within 10ish seconds
        // before expiry
        assert!(Instant::now() < now + Duration::from_secs(111));
        let actual = rx.borrow().clone().unwrap().unwrap();
        assert_eq!(actual, token2);
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
            // We give enough time for the a thundering herd to pile up waiting for a change notification from watch channel
            tokio::time::sleep(Duration::from_millis(50)).await;

            // Track how many calls were made to the inner token provider.
            *self.calls.lock().unwrap() += 1;

            // Return the result.
            self.result.clone()
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn no_initial_token_thundering_herd_success() {
        let token = Token {
            token: "delayed-token".to_string(),
            token_type: "Bearer".to_string(),
            expires_at: Some(std::time::Instant::now()),
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

        let calls = tp.calls();
        // Only one call to token provider should have been made
        assert_eq!(calls, 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn no_initial_token_thundering_herd_failure_shares_error() {
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

        let calls = tp.calls();
        // Only one call to token provider should have been made
        assert_eq!(calls, 1);
    }
}