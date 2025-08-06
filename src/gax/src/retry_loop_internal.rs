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

use crate::throttle_result::ThrottleResult;

use super::Result;
use super::backoff_policy::BackoffPolicy;
use super::error::Error;
use super::retry_policy::RetryPolicy;
use super::retry_result::RetryResult;
use super::retry_throttler::RetryThrottler;
use std::sync::{Arc, Mutex};
use std::time::Duration;

enum RetryLoopAttempt {
    // The first attempt
    Initial,
    // (Attempt count, backoff delay, previous error)
    Retry(u32, Duration, Error),
}

impl RetryLoopAttempt {
    fn count(&self) -> u32 {
        match self {
            RetryLoopAttempt::Initial => 0,
            RetryLoopAttempt::Retry(count, _, _) => *count,
        }
    }
}

/// Runs the retry loop for a given function.
///
/// This functions calls an inner function as long as (1) the retry policy has
/// not expired, (2) the inner function has not returned a successful request,
/// and (3) the retry throttler allows more calls.
///
/// In between calls the function waits the amount of time prescribed by the
/// backoff policy, using `sleep` to implement any sleep.
pub async fn retry_loop<F, S, Response>(
    inner: F,
    sleep: S,
    idempotent: bool,
    retry_throttler: Arc<Mutex<dyn RetryThrottler>>,
    retry_policy: Arc<dyn RetryPolicy>,
    backoff_policy: Arc<dyn BackoffPolicy>,
) -> Result<Response>
where
    F: AsyncFnMut(Option<Duration>) -> Result<Response> + Send,
    S: AsyncFn(Duration) -> () + Send,
{
    retry_loop_with_callback(
        inner,
        sleep,
        idempotent,
        retry_throttler,
        retry_policy,
        backoff_policy,
        |_, _, _| {},
    )
    .await
}

/// Runs the retry loop for a given function with a callback for retries.
///
/// This functions calls an inner function as long as (1) the retry policy has
/// not expired, (2) the inner function has not returned a successful request,
/// and (3) the retry throttler allows more calls.
///
/// In between calls the function waits the amount of time prescribed by the
/// backoff policy, using `sleep` to implement any sleep.
///
/// The `on_retry` callback is called before sleeping, with the attempt count,
/// the error, and the delay.
pub async fn retry_loop_with_callback<F, S, OnRetry, Response>(
    mut inner: F,
    sleep: S,
    idempotent: bool,
    retry_throttler: Arc<Mutex<dyn RetryThrottler>>,
    retry_policy: Arc<dyn RetryPolicy>,
    backoff_policy: Arc<dyn BackoffPolicy>,
    mut on_retry: OnRetry,
) -> Result<Response>
where
    F: AsyncFnMut(Option<Duration>) -> Result<Response> + Send,
    S: AsyncFn(Duration) -> () + Send,
    OnRetry: FnMut(u32, &Error, Duration) + Send,
{
    let loop_start = tokio::time::Instant::now().into_std();
    let mut attempt_state = RetryLoopAttempt::Initial;
    loop {
        let mut attempt_count = attempt_state.count();
        let remaining_time = retry_policy.remaining_time(loop_start, attempt_count);

        if let RetryLoopAttempt::Retry(attempt_count, delay, prev_error) = attempt_state {
            if remaining_time.is_some_and(|remaining| remaining < delay) {
                return Err(Error::exhausted(prev_error));
            }
            on_retry(attempt_count, &prev_error, delay);
            sleep(delay).await;

            if retry_throttler
                .lock()
                .expect("retry throttler lock is poisoned")
                .throttle_retry_attempt()
            {
                // This counts as an error for the purposes of the retry policy.
                let error = match retry_policy.on_throttle(loop_start, attempt_count, prev_error) {
                    ThrottleResult::Exhausted(e) => {
                        return Err(e);
                    }
                    ThrottleResult::Continue(e) => e,
                };
                let delay = backoff_policy.on_failure(loop_start, attempt_count);
                attempt_state = RetryLoopAttempt::Retry(attempt_count, delay, error);
                continue;
            }
        }
        attempt_count += 1;
        match inner(remaining_time).await {
            Ok(r) => {
                retry_throttler
                    .lock()
                    .expect("retry throttler lock is poisoned")
                    .on_success();
                return Ok(r);
            }
            Err(e) => {
                let flow = retry_policy.on_error(loop_start, attempt_count, idempotent, e);
                let delay = backoff_policy.on_failure(loop_start, attempt_count);
                retry_throttler
                    .lock()
                    .expect("retry throttler lock is poisoned")
                    .on_retry_failure(&flow);
                match flow {
                    RetryResult::Permanent(e) | RetryResult::Exhausted(e) => return Err(e),
                    RetryResult::Continue(e) => {
                        attempt_state = RetryLoopAttempt::Retry(attempt_count, delay, e);
                        continue;
                    }
                }
            }
        };
    }
}

/// A helper to compute the time remaining in a retry loop, given the attempt
/// timeout and the overall timeout.
pub fn effective_timeout(
    options: &crate::options::RequestOptions,
    remaining_time: Option<Duration>,
) -> Option<Duration> {
    match (options.attempt_timeout(), remaining_time) {
        (None, None) => None,
        (None, Some(t)) => Some(t),
        (Some(t), None) => Some(*t),
        (Some(a), Some(r)) => Some(*std::cmp::min(a, &r)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::{Error, rpc::Code, rpc::Status, rpc::StatusDetails};
    use std::error::Error as _;
    use test_case::test_case;

    #[test_case(None, None, None)]
    #[test_case(Some(Duration::from_secs(4)), Some(Duration::from_secs(4)), None)]
    #[test_case(Some(Duration::from_secs(4)), None, Some(Duration::from_secs(4)))]
    #[test_case(
        Some(Duration::from_secs(2)),
        Some(Duration::from_secs(2)),
        Some(Duration::from_secs(4))
    )]
    #[test_case(
        Some(Duration::from_secs(2)),
        Some(Duration::from_secs(4)),
        Some(Duration::from_secs(2))
    )]
    fn effective_timeouts(
        want: Option<Duration>,
        remaining: Option<Duration>,
        request: Option<Duration>,
    ) {
        let options = crate::options::RequestOptions::default();
        let options = request.into_iter().fold(options, |mut o, t| {
            o.set_attempt_timeout(t);
            o
        });
        let got = effective_timeout(&options, remaining);
        assert_eq!(want, got);
    }

    #[tokio::test]
    async fn immediate_success() -> anyhow::Result<()> {
        // This test simulates a server immediate returning a successful
        // response.
        let mut call = MockCall::new();
        call.expect_call().once().returning(|_| success());
        let inner = async move |d| call.call(d);

        let mut throttler = MockRetryThrottler::new();
        throttler.expect_on_success().once().return_const(());
        let mut retry_policy = MockRetryPolicy::new();
        retry_policy
            .expect_remaining_time()
            .once()
            .return_const(None);
        let backoff_policy = MockBackoffPolicy::new();
        let sleep = MockSleep::new();

        let backoff = async move |d| sleep.sleep(d).await;
        let response = retry_loop(
            inner,
            backoff,
            true,
            to_retry_throttler(throttler),
            to_retry_policy(retry_policy),
            to_backoff_policy(backoff_policy),
        )
        .await?;
        assert_eq!(response, "success");
        Ok(())
    }

    #[tokio::test]
    async fn immediate_failure() -> anyhow::Result<()> {
        // This test simulates a server responding with an immediate and
        // permanent error.
        let mut call = MockCall::new();
        call.expect_call().once().returning(|_| permanent());
        let inner = async move |d| call.call(d);

        let mut throttler = MockRetryThrottler::new();
        throttler.expect_on_retry_failure().once().return_const(());
        let mut retry_policy = MockRetryPolicy::new();
        retry_policy
            .expect_remaining_time()
            .once()
            .return_const(None);
        retry_policy
            .expect_on_error()
            .once()
            .returning(|_, _, _, e| RetryResult::Permanent(e));
        let mut backoff_policy = MockBackoffPolicy::new();
        backoff_policy
            .expect_on_failure()
            .once()
            .return_const(Duration::from_secs(0));
        let sleep = MockSleep::new();

        let backoff = async move |d| sleep.sleep(d).await;
        let response = retry_loop(
            inner,
            backoff,
            true,
            to_retry_throttler(throttler),
            to_retry_policy(retry_policy),
            to_backoff_policy(backoff_policy),
        )
        .await;
        assert!(response.is_err(), "{response:?}");
        Ok(())
    }

    #[test_case(true)]
    #[test_case(false)]
    #[tokio::test]
    async fn retry_success(expected_idempotency: bool) -> anyhow::Result<()> {
        // This test simulates a server responding with two transient errors and
        // then with a successful response.
        let mut call_seq = mockall::Sequence::new();
        let mut call = MockCall::new();
        call.expect_call()
            .once()
            .in_sequence(&mut call_seq)
            .withf(|got| got == &Some(Duration::from_secs(3)))
            .returning(|_| transient());
        call.expect_call()
            .once()
            .in_sequence(&mut call_seq)
            .withf(|got| got == &Some(Duration::from_secs(2)))
            .returning(|_| transient());
        call.expect_call()
            .once()
            .in_sequence(&mut call_seq)
            .withf(|got| got == &Some(Duration::from_secs(1)))
            .returning(|_| success());
        let inner = async move |d| call.call(d);

        let mut throttler_seq = mockall::Sequence::new();
        let mut throttler = MockRetryThrottler::new();
        throttler
            .expect_on_retry_failure()
            .once()
            .in_sequence(&mut throttler_seq)
            .return_const(());
        throttler
            .expect_throttle_retry_attempt()
            .once()
            .in_sequence(&mut throttler_seq)
            .return_const(false);
        throttler
            .expect_on_retry_failure()
            .once()
            .in_sequence(&mut throttler_seq)
            .return_const(());
        throttler
            .expect_throttle_retry_attempt()
            .once()
            .in_sequence(&mut throttler_seq)
            .return_const(false);
        throttler
            .expect_on_success()
            .once()
            .in_sequence(&mut throttler_seq)
            .return_const(());

        // Take the opportunity to verify the right values are provided to the
        // backoff policy and the remaining time.
        let mut retry_seq = mockall::Sequence::new();
        let mut retry_policy = MockRetryPolicy::new();
        retry_policy
            .expect_remaining_time()
            .once()
            .in_sequence(&mut retry_seq)
            .return_const(Some(Duration::from_secs(3)));
        retry_policy
            .expect_remaining_time()
            .once()
            .in_sequence(&mut retry_seq)
            .return_const(Some(Duration::from_secs(2)));
        retry_policy
            .expect_remaining_time()
            .once()
            .in_sequence(&mut retry_seq)
            .return_const(Some(Duration::from_secs(1)));
        retry_policy
            .expect_on_error()
            .times(2)
            .withf(move |_, _, idempotent, _| idempotent == &expected_idempotency)
            .returning(|_, _, _, e| RetryResult::Continue(e));

        let mut backoff_seq = mockall::Sequence::new();
        let mut backoff_policy = MockBackoffPolicy::new();
        let mut sleep_seq = mockall::Sequence::new();
        let mut sleep = MockSleep::new();

        for d in 1..=2 {
            backoff_policy
                .expect_on_failure()
                .once()
                .in_sequence(&mut backoff_seq)
                .return_const(Duration::from_millis(d));
            sleep
                .expect_sleep()
                .once()
                .in_sequence(&mut sleep_seq)
                .withf(move |got| got == &Duration::from_millis(d))
                .returning(|_| Box::pin(async {}));
        }

        let backoff = async move |d| sleep.sleep(d).await;
        let response = retry_loop(
            inner,
            backoff,
            expected_idempotency,
            to_retry_throttler(throttler),
            to_retry_policy(retry_policy),
            to_backoff_policy(backoff_policy),
        )
        .await;
        assert!(matches!(&response, Ok(s) if s == "success"), "{response:?}");
        Ok(())
    }

    #[tokio::test]
    async fn too_many_transients() -> anyhow::Result<()> {
        // This test simulates a server responding with two transient errors
        // and the retry policy stops after the second attempt.
        const ERRORS: usize = 3;
        let mut call_seq = mockall::Sequence::new();
        let mut call = MockCall::new();
        for i in 0..ERRORS {
            call.expect_call()
                .once()
                .withf(|d| d.is_none())
                .in_sequence(&mut call_seq)
                .returning(move |_| numbered_transient(i));
        }
        let inner = async move |d| call.call(d);

        let mut throttler_seq = mockall::Sequence::new();
        let mut throttler = MockRetryThrottler::new();
        for _ in 0..ERRORS - 1 {
            throttler
                .expect_on_retry_failure()
                .once()
                .in_sequence(&mut throttler_seq)
                .return_const(());
            throttler
                .expect_throttle_retry_attempt()
                .once()
                .in_sequence(&mut throttler_seq)
                .return_const(false);
        }
        throttler
            .expect_on_retry_failure()
            .once()
            .in_sequence(&mut throttler_seq)
            .return_const(());

        let mut retry_policy = MockRetryPolicy::new();
        retry_policy
            .expect_remaining_time()
            .times(ERRORS)
            .return_const(None);
        let mut retry_seq = mockall::Sequence::new();
        retry_policy
            .expect_on_error()
            .times(ERRORS - 1)
            .in_sequence(&mut retry_seq)
            .returning(|_, _, _, e| RetryResult::Continue(e));
        retry_policy
            .expect_on_error()
            .once()
            .in_sequence(&mut retry_seq)
            .returning(|_, _, _, e| RetryResult::Exhausted(e));
        let mut backoff_policy = MockBackoffPolicy::new();
        backoff_policy
            .expect_on_failure()
            .times(ERRORS)
            .return_const(Duration::from_secs(0));

        let mut sleep = MockSleep::new();
        sleep
            .expect_sleep()
            .times(ERRORS - 1)
            .returning(|_| Box::pin(async {}));

        let backoff = async move |d| sleep.sleep(d).await;
        let response = retry_loop(
            inner,
            backoff,
            true,
            to_retry_throttler(throttler),
            to_retry_policy(retry_policy),
            to_backoff_policy(backoff_policy),
        )
        .await;
        let err = response.unwrap_err();
        let status = err.status().unwrap();
        let detail = status.details.first().unwrap();
        assert!(
            matches!(detail, StatusDetails::DebugInfo(e) if e.detail == format!("count={}", ERRORS - 1) ),
            "{status:?}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn transient_then_permanent() -> anyhow::Result<()> {
        // This test simulates a server responding with a transient errors
        // and then a permanent error. The retry loop should stop on the second
        // error.
        let mut call_seq = mockall::Sequence::new();
        let mut call = MockCall::new();
        call.expect_call()
            .once()
            .in_sequence(&mut call_seq)
            .returning(|_| transient());
        call.expect_call()
            .once()
            .in_sequence(&mut call_seq)
            .returning(|_| permanent());
        let inner = async move |d| call.call(d);

        let mut throttler_seq = mockall::Sequence::new();
        let mut throttler = MockRetryThrottler::new();
        throttler
            .expect_on_retry_failure()
            .once()
            .in_sequence(&mut throttler_seq)
            .return_const(());
        throttler
            .expect_throttle_retry_attempt()
            .once()
            .in_sequence(&mut throttler_seq)
            .return_const(false);
        throttler
            .expect_on_retry_failure()
            .once()
            .in_sequence(&mut throttler_seq)
            .return_const(());

        let mut retry_policy = MockRetryPolicy::new();
        retry_policy
            .expect_remaining_time()
            .times(2)
            .return_const(None);
        let mut retry_seq = mockall::Sequence::new();
        retry_policy
            .expect_on_error()
            .once()
            .in_sequence(&mut retry_seq)
            .returning(|_, _, _, e| RetryResult::Continue(e));
        retry_policy
            .expect_on_error()
            .once()
            .in_sequence(&mut retry_seq)
            .returning(|_, _, _, e| RetryResult::Permanent(e));
        let mut backoff_policy = MockBackoffPolicy::new();
        backoff_policy
            .expect_on_failure()
            .times(2)
            .return_const(Duration::from_secs(0));

        let mut sleep = MockSleep::new();
        sleep
            .expect_sleep()
            .once()
            .returning(|_| Box::pin(async {}));

        let backoff = async move |d| sleep.sleep(d).await;
        let response = retry_loop(
            inner,
            backoff,
            true,
            to_retry_throttler(throttler),
            to_retry_policy(retry_policy),
            to_backoff_policy(backoff_policy),
        )
        .await;
        assert!(response.is_err(), "{response:?}");
        Ok(())
    }

    #[tokio::test]
    async fn throttle_then_success() -> anyhow::Result<()> {
        // This test simulates a server responding with two transient errors and
        // then with a successful response.
        let mut call_seq = mockall::Sequence::new();
        let mut call = MockCall::new();
        call.expect_call()
            .once()
            .in_sequence(&mut call_seq)
            .returning(|_| transient());
        call.expect_call()
            .once()
            .in_sequence(&mut call_seq)
            .returning(|_| success());
        let inner = async move |d| call.call(d);

        let mut throttler_seq = mockall::Sequence::new();
        let mut throttler = MockRetryThrottler::new();
        throttler
            .expect_on_retry_failure()
            .once()
            .in_sequence(&mut throttler_seq)
            .return_const(());
        // Skip one request ..
        throttler
            .expect_throttle_retry_attempt()
            .once()
            .in_sequence(&mut throttler_seq)
            .return_const(true);
        throttler
            .expect_throttle_retry_attempt()
            .once()
            .in_sequence(&mut throttler_seq)
            .return_const(false);
        throttler
            .expect_on_success()
            .once()
            .in_sequence(&mut throttler_seq)
            .return_const(());

        let mut retry_seq = mockall::Sequence::new();
        let mut retry_policy = MockRetryPolicy::new();
        retry_policy
            .expect_remaining_time()
            .times(3)
            .return_const(None);
        retry_policy
            .expect_on_error()
            .once()
            .in_sequence(&mut retry_seq)
            .returning(|_, _, _, e| RetryResult::Continue(e));
        retry_policy
            .expect_on_throttle()
            .once()
            .in_sequence(&mut retry_seq)
            .returning(|_, _, e| ThrottleResult::Continue(e));

        let mut backoff_policy = MockBackoffPolicy::new();
        backoff_policy
            .expect_on_failure()
            .times(2)
            .return_const(Duration::from_secs(0));

        let mut sleep = MockSleep::new();
        sleep
            .expect_sleep()
            .times(2)
            .returning(|_| Box::pin(async {}));

        let backoff = async move |d| sleep.sleep(d).await;
        let response = retry_loop(
            inner,
            backoff,
            true,
            to_retry_throttler(throttler),
            to_retry_policy(retry_policy),
            to_backoff_policy(backoff_policy),
        )
        .await;
        assert!(matches!(&response, Ok(s) if s == "success"), "{response:?}");
        Ok(())
    }

    #[tokio::test]
    async fn throttle_and_retry_policy_stops_loop() -> anyhow::Result<()> {
        // This test simulates a server responding with a transient error, and
        // the retry attempt is throttled, and then the retry loop stops the
        // loop.
        let mut call = MockCall::new();
        call.expect_call().once().returning(|_| transient());
        let inner = async move |d| call.call(d);

        let mut throttler_seq = mockall::Sequence::new();
        let mut throttler = MockRetryThrottler::new();
        throttler
            .expect_on_retry_failure()
            .once()
            .in_sequence(&mut throttler_seq)
            .return_const(());
        // Skip one request ..
        throttler
            .expect_throttle_retry_attempt()
            .once()
            .in_sequence(&mut throttler_seq)
            .return_const(true);

        let mut retry_seq = mockall::Sequence::new();
        let mut retry_policy = MockRetryPolicy::new();
        retry_policy
            .expect_remaining_time()
            .times(2)
            .return_const(None);
        retry_policy
            .expect_on_error()
            .once()
            .in_sequence(&mut retry_seq)
            .returning(|_, _, _, e| RetryResult::Continue(e));
        retry_policy
            .expect_on_throttle()
            .once()
            .in_sequence(&mut retry_seq)
            .returning(|_, _, e| ThrottleResult::Exhausted(e));

        let mut backoff_policy = MockBackoffPolicy::new();
        backoff_policy
            .expect_on_failure()
            .once()
            .return_const(Duration::from_secs(0));

        let mut sleep = MockSleep::new();
        sleep
            .expect_sleep()
            .once()
            .returning(|_| Box::pin(async {}));

        let backoff = async move |d| sleep.sleep(d).await;
        let response = retry_loop(
            inner,
            backoff,
            true,
            to_retry_throttler(throttler),
            to_retry_policy(retry_policy),
            to_backoff_policy(backoff_policy),
        )
        .await;
        assert!(
            matches!(&response, Err(e) if e.status() == transient().unwrap_err().status()),
            "{response:?}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn no_sleep_past_overall_timeout() -> anyhow::Result<()> {
        // This test simulates a server responding with a transient error. The
        // backoff policy wants to sleep for longer than the overall timeout. No
        // sleeps should be performed. The loop should terminate with a
        // `timeout` error.
        let mut seq = mockall::Sequence::new();
        let mut call = MockCall::new();
        let mut throttler = MockRetryThrottler::new();
        let mut retry_policy = MockRetryPolicy::new();
        let mut backoff_policy = MockBackoffPolicy::new();
        let sleep = MockSleep::new();

        // Calculate the attempt deadline
        retry_policy
            .expect_remaining_time()
            .once()
            .in_sequence(&mut seq)
            .return_const(Duration::from_millis(100));

        // Simulate a call to the server, responding with a transient error.
        call.expect_call()
            .once()
            .in_sequence(&mut seq)
            .returning(|_| transient());

        // The retry policy says we should retry this error.
        retry_policy
            .expect_on_error()
            .once()
            .in_sequence(&mut seq)
            .returning(|_, _, _, e| RetryResult::Continue(e));

        // The backoff policy wants to sleep for longer than the overall timeout.
        backoff_policy
            .expect_on_failure()
            .once()
            .in_sequence(&mut seq)
            .return_const(Duration::from_secs(10));

        // The throttler processes the result of the attempt.
        throttler
            .expect_on_retry_failure()
            .once()
            .in_sequence(&mut seq)
            .return_const(());

        // We recalculate how much time is left in the operation. This is
        // compared against the delay returned by the backoff policy.
        retry_policy
            .expect_remaining_time()
            .once()
            .in_sequence(&mut seq)
            .return_const(Duration::from_millis(100));

        // There is not enough time left to sleep, and make another attempt, so
        // the retry loop is terminated.

        let inner = async move |d| call.call(d);
        let backoff = async move |d| sleep.sleep(d).await;
        let response = retry_loop_with_callback(
            inner,
            backoff,
            true,
            to_retry_throttler(throttler),
            to_retry_policy(retry_policy),
            to_backoff_policy(backoff_policy),
            |_, _, _| (),
        )
        .await;
        let err = response.expect_err("retry loop should terminate");
        assert!(err.is_exhausted(), "{err:?}");
        // Confirm that we expose the last seen status from the operation
        let got = err
            .source()
            .and_then(|e| e.downcast_ref::<Error>())
            .and_then(|e| e.status());
        assert_eq!(got, Some(&transient_status()), "{err:?}");
        Ok(())
    }

    #[tokio::test]
    async fn no_sleep_past_overall_timeout_after_throttle() -> anyhow::Result<()> {
        // This test simulates a server responding with a transient error. There
        // is no immediate backoff. The retry throttler then decides we should
        // backoff again before making another request. This time, the backoff
        // policy wants to sleep for longer than the overall timeout. No sleeps
        // should be performed. The loop should terminate with a `timeout`
        // error.
        let mut seq = mockall::Sequence::new();
        let mut call = MockCall::new();
        let mut throttler = MockRetryThrottler::new();
        let mut retry_policy = MockRetryPolicy::new();
        let mut backoff_policy = MockBackoffPolicy::new();
        let mut sleep = MockSleep::new();

        // Calculate the attempt deadline
        retry_policy
            .expect_remaining_time()
            .once()
            .in_sequence(&mut seq)
            .return_const(Duration::from_millis(100));

        // Simulate a call to the server, responding with a transient error.
        call.expect_call()
            .once()
            .in_sequence(&mut seq)
            .returning(|_| transient());

        // The retry policy says we should retry this error.
        retry_policy
            .expect_on_error()
            .once()
            .in_sequence(&mut seq)
            .returning(|_, _, _, e| RetryResult::Continue(e));

        // The backoff policy returns an instantaneous sleep.
        backoff_policy
            .expect_on_failure()
            .once()
            .in_sequence(&mut seq)
            .return_const(Duration::ZERO);

        // The throttler processes the result of the attempt.
        throttler
            .expect_on_retry_failure()
            .once()
            .in_sequence(&mut seq)
            .return_const(());

        // We recalculate how much time is left in the operation. This is
        // compared against the delay returned by the backoff policy.
        retry_policy
            .expect_remaining_time()
            .once()
            .in_sequence(&mut seq)
            .return_const(Duration::from_millis(100));

        // There is enough time, so we perform the (instantaneous) sleep
        sleep
            .expect_sleep()
            .once()
            .in_sequence(&mut seq)
            .withf(move |got| got == &Duration::ZERO)
            .returning(|_| Box::pin(async {}));

        // In the second attempt, the throttler kicks in. It tells us to backoff
        // before sending this request out.
        throttler
            .expect_throttle_retry_attempt()
            .once()
            .in_sequence(&mut seq)
            .return_const(true);

        // The retry policy decides to continue the retry loop.
        retry_policy
            .expect_on_throttle()
            .once()
            .in_sequence(&mut seq)
            .returning(|_, _, e| ThrottleResult::Continue(e));

        // The backoff policy wants to sleep for longer than the overall timeout.
        backoff_policy
            .expect_on_failure()
            .once()
            .in_sequence(&mut seq)
            .return_const(Duration::from_secs(10));

        // We recalculate how much time is left in the operation. This is
        // compared against the delay returned by the backoff policy.
        retry_policy
            .expect_remaining_time()
            .once()
            .in_sequence(&mut seq)
            .return_const(Duration::from_millis(100));

        // There is not enough time left to sleep, and make another attempt, so
        // the retry loop is terminated.

        let inner = async move |d| call.call(d);
        let backoff = async move |d| sleep.sleep(d).await;
        let response = retry_loop_with_callback(
            inner,
            backoff,
            true,
            to_retry_throttler(throttler),
            to_retry_policy(retry_policy),
            to_backoff_policy(backoff_policy),
            |_, _, _| (),
        )
        .await;
        let err = response.expect_err("retry loop should terminate");
        assert!(err.is_exhausted(), "{err:?}");
        // Confirm that we expose the last seen status from the operation
        let got = err
            .source()
            .and_then(|e| e.downcast_ref::<Error>())
            .and_then(|e| e.status());
        assert_eq!(got, Some(&transient_status()), "{err:?}");
        Ok(())
    }

    fn success() -> Result<String> {
        Ok("success".into())
    }

    fn transient_status() -> Status {
        Status::default()
            .set_code(Code::Unavailable)
            .set_message("try-again")
    }

    fn transient() -> Result<String> {
        Err(Error::service(transient_status()))
    }

    fn numbered_transient(i: usize) -> Result<String> {
        Err(Error::service(transient_status().set_details([
            StatusDetails::DebugInfo(rpc::model::DebugInfo::new().set_detail(format!("count={i}"))),
        ])))
    }

    fn permanent() -> Result<String> {
        let status = Status::default()
            .set_code(Code::PermissionDenied)
            .set_message("uh-oh");
        Err(Error::service(status))
    }

    fn to_retry_throttler(mock: MockRetryThrottler) -> Arc<Mutex<dyn RetryThrottler>> {
        Arc::new(Mutex::new(mock))
    }

    fn to_retry_policy(mock: MockRetryPolicy) -> Arc<dyn RetryPolicy> {
        Arc::new(mock)
    }

    fn to_backoff_policy(mock: MockBackoffPolicy) -> Arc<dyn BackoffPolicy> {
        Arc::new(mock)
    }

    trait Call {
        fn call(&self, d: Option<Duration>) -> Result<String>;
    }

    mockall::mock! {
        Call {}
        impl Call for Call {
            fn call(&self, d: Option<Duration>) -> Result<String>;
        }
    }

    trait Sleep {
        fn sleep(&self, d: Duration) -> impl Future<Output = ()>;
    }

    mockall::mock! {
        Sleep {}
        impl Sleep for Sleep {
            fn sleep(&self, d: Duration) -> impl Future<Output = ()> + Send;
        }
    }

    mockall::mock! {
        #[derive(Debug)]
        RetryPolicy {}
        impl RetryPolicy for RetryPolicy {
            fn on_error(&self, loop_start: std::time::Instant, attempt_count: u32, idempotent: bool, error: Error) -> RetryResult;
            fn on_throttle(&self, loop_start: std::time::Instant, attempt_count: u32, error: Error) -> ThrottleResult;
            fn remaining_time(&self, loop_start: std::time::Instant, attempt_count: u32) -> Option<Duration>;
        }
        impl std::clone::Clone for RetryPolicy {
            fn clone(&self) -> Self;
        }
    }

    mockall::mock! {
        #[derive(Debug)]
        BackoffPolicy {}
        impl BackoffPolicy for BackoffPolicy {
            fn on_failure(&self, loop_start: std::time::Instant, attempt_count: u32) -> Duration;
        }
    }

    mockall::mock! {
        #[derive(Debug)]
        RetryThrottler {}
        impl RetryThrottler for RetryThrottler {
            fn throttle_retry_attempt(&self) -> bool;
            fn on_retry_failure(&mut self, error: &RetryResult);
            fn on_success(&mut self);
        }
    }
}
