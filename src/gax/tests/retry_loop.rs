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

//! These tests use mocks to verify the gax client uses the retry policy,
//! backoff policy, and retry throttler as expected. They do not test the
//! policy implementations, that is done in the unit tests. Though the may use
//! the policies where mocking would just require a lot of uninteresting code.
//!
//! The tests use an HTTP server that returns a sequence of responses. The
//! sequence is specific to each test, intended to drive the retry loop as
//! needed for that test.

#[cfg(test)]
mod test {
    use axum::extract::State;
    use axum::http::StatusCode;
    use gax::backoff_policy::{BackoffPolicy, ExponentialBackoffBuilder};
    use gax::error::Error;
    use gax::http_client::ReqwestClient;
    use gax::options::*;
    use gax::retry_policy::{LimitedAttemptCount, RetryFlow, RetryPolicy};
    use gax::retry_throttler::{CircuitBreaker, RetryThrottler};
    use gcp_sdk_gax as gax;
    use serde_json::json;
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};
    use tokio::task::JoinHandle;

    type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn retry_loop_no_retry_immediate_success() -> Result<()> {
        let (endpoint, _server) = start(vec![success()]).await?;

        let client = ReqwestClient::new(test_config(), &endpoint).await?;
        let builder = client.builder(reqwest::Method::GET, "/retry".into());
        let body = json!({});

        let response = client
            .execute::<serde_json::Value, serde_json::Value>(
                builder,
                Some(body),
                RequestOptions::default(),
            )
            .await;
        let response = response?;
        assert_eq!(response, json!({"status": "done"}));
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn retry_loop_no_retry_immediate_failure() -> Result<()> {
        let (endpoint, _server) = start(vec![permanent()]).await?;

        let client = ReqwestClient::new(test_config(), &endpoint).await?;
        let builder = client.builder(reqwest::Method::GET, "/retry".into());
        let body = json!({});

        let response = client
            .execute::<serde_json::Value, serde_json::Value>(
                builder,
                Some(body),
                RequestOptions::default(),
            )
            .await;
        assert!(response.is_err(), "{response:?}");
        let response = response.err().unwrap();
        let error = response.as_inner::<gax::error::ServiceError>().unwrap();
        assert_eq!(error.http_status_code(), &Some(permanent().0.as_u16()));
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn retry_loop_with_retry_immediate_success() -> Result<()> {
        let (endpoint, _server) = start(vec![success()]).await?;

        let mut seq = mockall::Sequence::new();
        let mut retry_policy = MockRetryPolicy::new();
        retry_policy
            .expect_remaining_time()
            .once()
            .in_sequence(&mut seq)
            .return_const(None);
        retry_policy.expect_on_error().never();

        let client = ReqwestClient::new(test_config(), &endpoint).await?;
        let builder = client.builder(reqwest::Method::GET, "/retry".into());
        let body = json!({});

        let options = {
            let mut options = RequestOptions::default();
            options.set_retry_policy(retry_policy);
            options
        };
        let response = client
            .execute::<serde_json::Value, serde_json::Value>(builder, Some(body), options)
            .await;
        let response = response?;
        assert_eq!(response, json!({"status": "done"}));
        Ok(())
    }

    // Check the `start_loop`` and `attempt_count` expectations.
    fn check_and_save(
        state: &Arc<Mutex<Instant>>,
        expected_attempt_count: u32,
        loop_start: Instant,
        attempt_count: u32,
    ) -> bool {
        let mut guard = state.lock().unwrap();
        *guard = loop_start;
        drop(guard);

        return expected_attempt_count == attempt_count;
    }

    fn expect(
        state: &Arc<Mutex<Instant>>,
        expected_attempt_count: u32,
        loop_start: Instant,
        attempt_count: u32,
    ) -> bool {
        let guard = state.lock().unwrap();
        return loop_start == *guard && expected_attempt_count == attempt_count;
    }

    fn is_transient(error: &Error) -> bool {
        if let Some(e) = error.as_inner::<gax::error::ServiceError>() {
            return e.http_status_code() == &Some(StatusCode::SERVICE_UNAVAILABLE.as_u16());
        }
        false
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn retry_loop_retry_success() -> Result<()> {
        // We create a server that will return two transient errors and then succeed.
        let (endpoint, _server) = start(vec![transient(), transient(), success()]).await?;

        // Create mocks that expect the corresponding sequence of calls. In this
        // test we will verify the calls receive the correct attempt numbers and
        // (reasonable) values for `now`.
        let expected_loop_start = Arc::new(Mutex::new(Instant::now()));
        let mut seq = mockall::Sequence::new();
        let mut backoff_policy = MockBackoffPolicy::new();
        let mut throttler = MockThrottler::new();
        let mut retry_policy = MockRetryPolicy::new();

        // Initial call...
        let state = expected_loop_start.clone();
        retry_policy
            .expect_remaining_time()
            .withf(move |s, a| check_and_save(&state, 0, *s, *a))
            .once()
            .in_sequence(&mut seq)
            .return_const(None);
        // Call results
        let state = expected_loop_start.clone();
        retry_policy
            .expect_on_error()
            .withf(move |s, a, _, e| expect(&state, 1, *s, *a) && is_transient(e))
            .once()
            .in_sequence(&mut seq)
            .returning(|_, _, _, e| RetryFlow::Continue(e));
        let state = expected_loop_start.clone();
        backoff_policy
            .expect_on_failure()
            .withf(move |s, a| expect(&state, 1, *s, *a))
            .once()
            .in_sequence(&mut seq)
            .returning(|_, _| Duration::from_millis(1));
        throttler
            .expect_on_retry_failure()
            .once()
            .in_sequence(&mut seq)
            .return_const(());

        // First retry
        let state = expected_loop_start.clone();
        retry_policy
            .expect_remaining_time()
            .withf(move |s, a| expect(&state, 1, *s, *a))
            .once()
            .in_sequence(&mut seq)
            .return_const(None);
        throttler
            .expect_throttle_retry_attempt()
            .once()
            .in_sequence(&mut seq)
            .return_const(false);
        // Call results
        let state = expected_loop_start.clone();
        retry_policy
            .expect_on_error()
            .withf(move |s, a, _, e| expect(&state, 2, *s, *a) && is_transient(e))
            .once()
            .in_sequence(&mut seq)
            .returning(|_, _, _, e| RetryFlow::Continue(e));
        let state = expected_loop_start.clone();
        backoff_policy
            .expect_on_failure()
            .withf(move |s, a| expect(&state, 2, *s, *a))
            .once()
            .in_sequence(&mut seq)
            .returning(|_, _| Duration::from_millis(1));
        throttler
            .expect_on_retry_failure()
            .once()
            .in_sequence(&mut seq)
            .return_const(());

        // Second retry
        let state = expected_loop_start.clone();
        retry_policy
            .expect_remaining_time()
            .withf(move |s, a| expect(&state, 2, *s, *a))
            .once()
            .in_sequence(&mut seq)
            .return_const(None);
        throttler
            .expect_throttle_retry_attempt()
            .once()
            .in_sequence(&mut seq)
            .return_const(false);
        // Call succeeds .
        throttler
            .expect_on_success()
            .once()
            .in_sequence(&mut seq)
            .return_const(());

        let client = ReqwestClient::new(test_config(), &endpoint).await?;
        let builder = client.builder(reqwest::Method::GET, "/retry".into());
        let body = json!({});

        let options = {
            let mut options = RequestOptions::default();
            options.set_retry_policy(retry_policy);
            options.set_backoff_policy(backoff_policy);
            options.set_retry_throttler(throttler);
            options
        };
        let response = client
            .execute::<serde_json::Value, serde_json::Value>(builder, Some(body), options)
            .await;
        let response = response?;
        assert_eq!(response, json!({"status": "done"}));
        Ok(())
    }

    async fn retry_loop_retry_idempotency(expected: bool, options: RequestOptions) -> Result<()> {
        let (endpoint, _server) = start(vec![transient(), transient(), success()]).await?;

        let mut seq = mockall::Sequence::new();
        let mut retry_policy = MockRetryPolicy::new();
        for _ in 0..2 {
            let expected = expected;
            retry_policy
                .expect_remaining_time()
                .once()
                .in_sequence(&mut seq)
                .return_const(None);
            retry_policy
                .expect_on_error()
                .withf(move |_, _, idempotent, _| idempotent == &expected)
                .once()
                .in_sequence(&mut seq)
                .returning(|_, _, _, e| RetryFlow::Continue(e));
        }
        retry_policy
            .expect_remaining_time()
            .once()
            .in_sequence(&mut seq)
            .return_const(None);

        let client = ReqwestClient::new(test_config(), &endpoint).await?;
        let builder = client.builder(reqwest::Method::GET, "/retry".into());
        let body = json!({});

        let options = {
            let mut options = options;
            options.set_retry_policy(retry_policy);
            options.set_backoff_policy(test_backoff()); // faster tests
            options.set_retry_throttler(test_retry_throttler()); // never throttle
            options
        };
        let response = client
            .execute::<serde_json::Value, serde_json::Value>(builder, Some(body), options)
            .await;
        let response = response?;
        assert_eq!(response, json!({"status": "done"}));
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn retry_loop_retry_default_idempotency() -> Result<()> {
        let options = RequestOptions::default();
        retry_loop_retry_idempotency(false, options).await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn retry_loop_retry_with_idempotent() -> Result<()> {
        let mut options = RequestOptions::default();
        options.set_idempotency(true);
        retry_loop_retry_idempotency(true, options).await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn retry_loop_retry_with_not_idempotent() -> Result<()> {
        let mut options = RequestOptions::default();
        options.set_idempotency(false);
        retry_loop_retry_idempotency(false, options).await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn retry_loop_retry_too_many_transients() -> Result<()> {
        // We create a server that will return two transient errors and then succeed.
        let (endpoint, _server) = start(vec![transient(), transient(), transient()]).await?;

        let mut seq = mockall::Sequence::new();
        let mut retry_policy = MockRetryPolicy::new();
        for _ in 0..2 {
            retry_policy
                .expect_remaining_time()
                .once()
                .in_sequence(&mut seq)
                .return_const(None);
            retry_policy
                .expect_on_error()
                .withf(|_, _, _, error| is_transient(error))
                .once()
                .in_sequence(&mut seq)
                .returning(|_, _, _, e| RetryFlow::Continue(e));
        }
        retry_policy
            .expect_remaining_time()
            .once()
            .in_sequence(&mut seq)
            .return_const(None);
        retry_policy
            .expect_on_error()
            .withf(|_, _, _, error| is_transient(error))
            .once()
            .in_sequence(&mut seq)
            .returning(|_, _, _, e| RetryFlow::Exhausted(e));

        let client = ReqwestClient::new(test_config(), &endpoint).await?;
        let builder = client.builder(reqwest::Method::GET, "/retry".into());
        let body = json!({});

        let options = {
            let mut options = RequestOptions::default();
            options.set_retry_policy(retry_policy);
            options.set_backoff_policy(test_backoff()); // faster tests
            options.set_retry_throttler(test_retry_throttler()); // never throttle
            options
        };
        let response = client
            .execute::<serde_json::Value, serde_json::Value>(builder, Some(body), options)
            .await;
        let response = response.err().unwrap();
        let error = response.as_inner::<gax::error::ServiceError>().unwrap();
        assert_eq!(error.http_status_code(), &Some(transient().0.as_u16()));
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn retry_loop_retry_transient_then_permanent() -> Result<()> {
        // We create a server that will return two transient errors and then succeed.
        let (endpoint, _server) = start(vec![transient(), permanent()]).await?;

        // Create a matching policy provider. It returns a policy that expects two
        // errors.
        let mut seq = mockall::Sequence::new();
        let mut retry_policy = MockRetryPolicy::new();
        retry_policy
            .expect_remaining_time()
            .once()
            .in_sequence(&mut seq)
            .return_const(None);
        retry_policy
            .expect_on_error()
            .withf(|_, _, _, error| is_transient(error))
            .once()
            .in_sequence(&mut seq)
            .returning(|_, _, _, e| RetryFlow::Continue(e));
        retry_policy
            .expect_remaining_time()
            .once()
            .in_sequence(&mut seq)
            .return_const(None);
        retry_policy
            .expect_on_error()
            .withf(|_, _, _, error| !is_transient(error))
            .once()
            .in_sequence(&mut seq)
            .returning(|_, _, _, e| RetryFlow::Exhausted(e));

        let client = ReqwestClient::new(test_config(), &endpoint).await?;
        let builder = client.builder(reqwest::Method::GET, "/retry".into());
        let body = json!({});

        let options = {
            let mut options = RequestOptions::default();
            options.set_retry_policy(retry_policy);
            options.set_backoff_policy(test_backoff()); // faster tests
            options.set_retry_throttler(test_retry_throttler()); // never throttle
            options
        };
        let response = client
            .execute::<serde_json::Value, serde_json::Value>(builder, Some(body), options)
            .await;
        let response = response.err().unwrap();
        let error = response.as_inner::<gax::error::ServiceError>().unwrap();
        assert_eq!(error.http_status_code(), &Some(permanent().0.as_u16()));
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn retry_loop_too_many_throttles() -> Result<()> {
        // We create a server that will return two transient errors and then succeed.
        let (endpoint, _server) = start(vec![transient()]).await?;

        let mut seq = mockall::Sequence::new();
        let mut retry_policy = MockRetryPolicy::new();
        retry_policy
            .expect_remaining_time()
            .once()
            .in_sequence(&mut seq)
            .return_const(None);
        // The first error is a HttpError.
        retry_policy
            .expect_on_error()
            .withf(|_, _, _, error| is_transient(error))
            .once()
            .in_sequence(&mut seq)
            .returning(|_, _, _, e| RetryFlow::Continue(e));

        for _ in 0..4 {
            retry_policy
                .expect_remaining_time()
                .once()
                .in_sequence(&mut seq)
                .return_const(None);
            retry_policy
                .expect_on_throttle()
                .once()
                .in_sequence(&mut seq)
                .returning(|_, _| None);
        }
        retry_policy
            .expect_remaining_time()
            .once()
            .in_sequence(&mut seq)
            .return_const(None);
        retry_policy
            .expect_on_throttle()
            .once()
            .in_sequence(&mut seq)
            .returning(|_, _| Some(Error::other(format!("exhausted"))));

        let mut throttler = MockThrottler::new();
        throttler
            .expect_throttle_retry_attempt()
            .times(5)
            .return_const(true);
        throttler
            .expect_on_retry_failure()
            .times(1)
            .return_const(());

        let client = ReqwestClient::new(test_config(), &endpoint).await?;
        let builder = client.builder(reqwest::Method::GET, "/retry".into());
        let body = json!({});

        let options = {
            let mut options = RequestOptions::default();
            options.set_retry_policy(retry_policy);
            options.set_backoff_policy(test_backoff()); // faster tests
            options.set_retry_throttler(throttler);
            options
        };
        let response = client
            .execute::<serde_json::Value, serde_json::Value>(builder, Some(body), options)
            .await;
        let response = response.err().unwrap();
        assert_eq!(response.kind(), gax::error::ErrorKind::Other);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn retry_loop_backoff() -> Result<()> {
        let (endpoint, _server) = start(vec![transient(), transient(), success()]).await?;

        let mut seq = mockall::Sequence::new();
        let mut backoff_policy = MockBackoffPolicy::new();
        for _ in 0..2 {
            backoff_policy
                .expect_on_failure()
                .once()
                .in_sequence(&mut seq)
                .return_const(Duration::from_millis(1));
        }

        let client = ReqwestClient::new(test_config(), &endpoint).await?;
        let builder = client.builder(reqwest::Method::GET, "/retry".into());
        let body = json!({});

        let options = {
            let mut options = RequestOptions::default();
            options.set_backoff_policy(backoff_policy);
            options.set_retry_policy(LimitedAttemptCount::new(5));
            options.set_retry_throttler(test_retry_throttler()); // never throttle
            options.set_idempotency(true);
            options
        };
        let response = client
            .execute::<serde_json::Value, serde_json::Value>(builder, Some(body), options)
            .await;
        let response = response?;
        assert_eq!(response, json!({"status": "done"}));
        Ok(())
    }

    // These tests count the ticks while the tokio runtime is sleeping. We are
    // fairly generous with the required ticks to avoid flakiness.
    const MIN_TICKS: i32 = 5;
    const INTERVAL: Duration = Duration::from_secs(1);
    const BACKOFF: Duration = Duration::from_secs(2 * MIN_TICKS as u64);

    #[tokio::test(start_paused = true)]
    async fn retry_loop_sleeps_on_backoff() -> Result<()> {
        // We will use a channel to verify the mock calls are completed, and
        // then expect the right collect the mock calls in this thread. The
        // senders need to be blocking, they run in the mocks. The receiver
        // needs to be asynchronous. So we create two channels and a thread to
        // connect them.
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();

        let mut seq = mockall::Sequence::new();
        let mut backoff_policy = MockBackoffPolicy::new();
        for _ in 0..2 {
            let btx = tx.clone();
            backoff_policy
                .expect_on_failure()
                .once()
                .in_sequence(&mut seq)
                .returning(move |_, _| {
                    btx.send("backoff::on_failure").unwrap();
                    BACKOFF
                });
        }

        let mut seq = mockall::Sequence::new();
        let mut retry_policy = MockRetryPolicy::new();
        for _ in 0..2 {
            let rtx = tx.clone();
            retry_policy
                .expect_remaining_time()
                .once()
                .in_sequence(&mut seq)
                .return_const(None);
            retry_policy
                .expect_on_error()
                .once()
                .in_sequence(&mut seq)
                .returning(move |_, _, _, e| {
                    rtx.send("--marker--").unwrap();
                    rtx.send("retry::on_error").unwrap();
                    RetryFlow::Continue(e)
                });
        }
        retry_policy
            .expect_remaining_time()
            .once()
            .in_sequence(&mut seq)
            .return_const(None);

        let (endpoint, server) = start(vec![transient(), transient(), success()]).await?;
        let client = ReqwestClient::new(test_config(), &endpoint).await?;
        let builder = client.builder(reqwest::Method::GET, "/retry".into());
        let body = json!({});

        let options = {
            let mut options = RequestOptions::default();
            options.set_backoff_policy(backoff_policy);
            options.set_retry_policy(retry_policy);
            options.set_retry_throttler(test_retry_throttler()); // never throttle
            options
        };
        let response =
            client.execute::<serde_json::Value, serde_json::Value>(builder, Some(body), options);

        tokio::pin!(server);
        tokio::pin!(response);
        let interval = tokio::time::interval(INTERVAL);
        tokio::pin!(interval);
        loop {
            tokio::select! {
                _ = &mut server => {},
                r = &mut response => {
                    let _ = tx.send("--marker--");
                    let _ = tx.send("success");
                    assert!(r.is_ok(), "{r:?}");
                    assert_eq!(r.ok(), Some(json!({"status": "done"})));
                    break;
                },
                _ = interval.tick() => {
                    let _ = tx.send("interval");
                }
            }
        }

        // Close the channel and collect all the results.
        drop(tx); // close the channel

        // Remove the leading intervals while the connection happens.
        while let Some("interval") = rx.recv().await {}
        // We expect a RPC that fails, and therefore a call to `on_error()` and then `on_failure()`
        assert_eq!(rx.recv().await, Some("retry::on_error"));
        assert_eq!(rx.recv().await, Some("backoff::on_failure"));
        // Count the number of times tokio needed to sleep.
        let mut ticks = 0;
        while let Some("interval") = rx.recv().await {
            ticks += 1;
        }
        assert!(
            ticks >= MIN_TICKS,
            "{ticks} should be >= {MIN_TICKS} as the interval is {INTERVAL:?} and the backoff is {BACKOFF:?}"
        );
        // We expect a RPC that fails, and therefore a call to `on_error()` and then `on_failure()`
        assert_eq!(rx.recv().await, Some("retry::on_error"));
        assert_eq!(rx.recv().await, Some("backoff::on_failure"));
        // Count the number of times tokio needed to sleep.
        let mut ticks = 0;
        while let Some("interval") = rx.recv().await {
            ticks += 1;
        }
        assert!(
            ticks >= MIN_TICKS,
            "{ticks} should be >= {MIN_TICKS} as the interval is {INTERVAL:?} and the backoff is {BACKOFF:?}"
        );
        // And then the request succeeds.
        assert_eq!(rx.recv().await, Some("success"));

        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn retry_loop_sleeps_on_throttle() -> Result<()> {
        // We will use a channel to verify the mock calls are completed, and
        // then expect the right collect the mock calls in this thread. The
        // senders need to be blocking, they run in the mocks. The receiver
        // needs to be asynchronous. So we create two channels and a thread to
        // connect them.
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();

        let mut seq = mockall::Sequence::new();
        let mut backoff_policy = MockBackoffPolicy::new();
        for _ in 0..2 {
            let btx = tx.clone();
            backoff_policy
                .expect_on_failure()
                .once()
                .in_sequence(&mut seq)
                .returning(move |_, _| {
                    btx.send("backoff::on_failure").unwrap();
                    Duration::from_secs(10)
                });
        }

        let mut seq = mockall::Sequence::new();
        let mut throttler = MockThrottler::new();
        throttler
            .expect_on_retry_failure()
            .once()
            .in_sequence(&mut seq)
            .return_const(());
        let throttle_tx = tx.clone();
        throttler
            .expect_throttle_retry_attempt()
            .once()
            .in_sequence(&mut seq)
            .returning(move || {
                throttle_tx.send("--marker--").unwrap();
                throttle_tx
                    .send("throttler::throttle_retry_attempt/true")
                    .unwrap();
                true
            });
        let throttle_tx = tx.clone();
        throttler
            .expect_throttle_retry_attempt()
            .once()
            .in_sequence(&mut seq)
            .returning(move || {
                throttle_tx.send("--marker--").unwrap();
                throttle_tx
                    .send("throttler::throttle_retry_attempt/false")
                    .unwrap();
                false
            });
        throttler
            .expect_on_success()
            .once()
            .in_sequence(&mut seq)
            .return_const(());

        let mut seq = mockall::Sequence::new();
        let mut retry_policy = MockRetryPolicy::new();
        // The first request fails and the retry policy is queried.
        retry_policy
            .expect_remaining_time()
            .once()
            .in_sequence(&mut seq)
            .return_const(None);
        let rtx = tx.clone();
        retry_policy
            .expect_on_error()
            .once()
            .in_sequence(&mut seq)
            .returning(move |_, _, _, e| {
                rtx.send("--marker--").unwrap();
                rtx.send("retry::on_error").unwrap();
                RetryFlow::Continue(e)
            });
        // The next request is throttled.
        retry_policy
            .expect_remaining_time()
            .once()
            .in_sequence(&mut seq)
            .return_const(None);
        let rtx = tx.clone();
        retry_policy
            .expect_on_throttle()
            .once()
            .in_sequence(&mut seq)
            .returning(move |_, _| {
                rtx.send("retry::on_error").unwrap();
                None
            });
        // The last request succeeds, but before issuing it, the
        // remaining time is queried.
        retry_policy
            .expect_remaining_time()
            .once()
            .in_sequence(&mut seq)
            .return_const(None);

        let (endpoint, server) = start(vec![transient(), success()]).await?;
        let client = ReqwestClient::new(test_config(), &endpoint).await?;
        let builder = client.builder(reqwest::Method::GET, "/retry".into());
        let body = json!({});

        let options = {
            let mut options = RequestOptions::default();
            options.set_backoff_policy(backoff_policy);
            options.set_retry_policy(retry_policy);
            options.set_retry_throttler(throttler);
            options
        };
        let response =
            client.execute::<serde_json::Value, serde_json::Value>(builder, Some(body), options);

        tokio::pin!(server);
        tokio::pin!(response);
        let interval = tokio::time::interval(Duration::from_secs(1));
        tokio::pin!(interval);
        loop {
            tokio::select! {
                _ = &mut server => {},
                r = &mut response => {
                    let _ = tx.send("--marker--");
                    let _ = tx.send("success");
                    assert!(r.is_ok(), "{r:?}");
                    assert_eq!(r.ok(), Some(json!({"status": "done"})));
                    break;
                },
                _ = interval.tick() => {
                    let _ = tx.send("interval");
                }
            }
        }

        // Close the channel and collect all the results.
        drop(tx); // close the channel

        // Remove the leading intervals while the connection happens.
        while let Some("interval") = rx.recv().await {}
        // The first call just gets a regular on error and then backoff.
        assert_eq!(rx.recv().await, Some("retry::on_error"));
        assert_eq!(rx.recv().await, Some("backoff::on_failure"));
        // Count the number of times tokio needed to sleep.
        let mut ticks = 0;
        while let Some("interval") = rx.recv().await {
            ticks += 1;
        }
        assert!(
            ticks >= MIN_TICKS,
            "{ticks} should be >= 5 as the interval is 1s and the backoff is 10s"
        );

        // The next attempt never happens because it is throttled.
        assert_eq!(
            rx.recv().await,
            Some("throttler::throttle_retry_attempt/true")
        );
        assert_eq!(rx.recv().await, Some("retry::on_error"));
        assert_eq!(rx.recv().await, Some("backoff::on_failure"));
        // Count the number of times tokio needed to sleep.
        let mut ticks = 0;
        while let Some("interval") = rx.recv().await {
            ticks += 1;
        }
        assert!(
            ticks >= MIN_TICKS,
            "{ticks} should be >= {MIN_TICKS} as the interval is {INTERVAL:?} and the backoff is {BACKOFF:?}"
        );

        // And then the attempt succeeds.
        assert_eq!(
            rx.recv().await,
            Some("throttler::throttle_retry_attempt/false")
        );
        // Potentially some intervals expired while the networking cycle takes
        // place.
        while let Some("interval") = rx.recv().await {}
        assert_eq!(rx.recv().await, Some("success"));

        Ok(())
    }

    mockall::mock! {
        #[derive(Debug)]
        RetryPolicy {}
        impl RetryPolicy for RetryPolicy {
            fn on_error(&self, loop_start: std::time::Instant, attempt_count: u32, idempotent: bool, error: Error) -> RetryFlow;
            fn on_throttle(&self, loop_start: std::time::Instant, attempt_count: u32) -> Option<Error>;
            fn remaining_time(&self, loop_start: std::time::Instant, attempt_count: u32) -> Option<std::time::Duration>;
        }
        impl std::clone::Clone for RetryPolicy {
            fn clone(&self) -> Self;
        }
    }

    mockall::mock! {
        #[derive(Debug)]
        BackoffPolicy {}
        impl BackoffPolicy for BackoffPolicy {
            fn on_failure(&self, loop_start: std::time::Instant, attempt_count: u32) -> std::time::Duration;
        }
    }

    mockall::mock! {
        #[derive(Debug)]
        Throttler {}
        impl RetryThrottler for Throttler {
            fn throttle_retry_attempt(&self) -> bool;
            fn on_retry_failure(&mut self, error: &RetryFlow);
            fn on_success(&mut self);
        }
    }

    fn success() -> (StatusCode, String) {
        let response = json!({
            "status": "done"
        });
        (StatusCode::OK, response.to_string())
    }

    fn transient() -> (StatusCode, String) {
        let status = json!({"error": {
            "code": StatusCode::SERVICE_UNAVAILABLE.as_u16(),
            "status": "UNAVAILABLE",
            "message": "try-again",
        }});
        (StatusCode::SERVICE_UNAVAILABLE, status.to_string())
    }

    fn permanent() -> (StatusCode, String) {
        let status = json!({"error": {
            "code": StatusCode::BAD_REQUEST.as_u16(),
            "status": "INVALID_ARGUMENT",
            "message": "uh-oh",
        }});
        (StatusCode::BAD_REQUEST, status.to_string())
    }

    fn test_config() -> ClientConfig {
        ClientConfig::default().set_credential(auth::credentials::testing::test_credentials())
    }

    fn test_backoff() -> impl BackoffPolicy {
        ExponentialBackoffBuilder::new()
            .with_initial_delay(Duration::from_millis(1))
            .with_maximum_delay(Duration::from_millis(1))
            .clamp()
    }

    fn test_retry_throttler() -> impl RetryThrottler {
        CircuitBreaker::clamp(100, 0, 1)
    }

    struct RetrySharedState {
        responses: std::collections::VecDeque<(StatusCode, String)>,
    }

    type RetryState = Arc<Mutex<RetrySharedState>>;

    pub async fn start(responses: Vec<(StatusCode, String)>) -> Result<(String, JoinHandle<()>)> {
        let state = Arc::new(Mutex::new(RetrySharedState {
            responses: responses.into(),
        }));
        let app = axum::Router::new()
            .route("/retry", axum::routing::get(retry))
            .with_state(state);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let server = tokio::spawn(async {
            axum::serve(listener, app).await.unwrap();
        });

        Ok((format!("http://{}:{}", addr.ip(), addr.port()), server))
    }

    async fn retry(State(state): State<RetryState>) -> (StatusCode, String) {
        let mut state = state.lock().expect("retry state is poisoned");
        state
            .responses
            .pop_front()
            .unwrap_or_else(|| (StatusCode::BAD_REQUEST, "exhausted retry data".to_string()))
    }
}
