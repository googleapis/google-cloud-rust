// Copyright 2026 Google LLC
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

//! RPC retry policies used by the Spanner client.

use crate::request_id_interceptor::update_request_id_attempt;
use crate::retry_delay::extract_retry_delay_from_error;
use google_cloud_gax::error::Error;
use google_cloud_gax::options::RequestOptions as GaxRequestOptions;
use google_cloud_gax::retry_policy::{Aip194Strict, RetryPolicy};
use google_cloud_gax::retry_state::RetryState;
use google_cloud_gax::throttle_result::ThrottleResult;
use std::time::{Duration, Instant};
use tokio::time::sleep;

pub use crate::transaction_retry_policy::{BasicTransactionRetryPolicy, TransactionRetryPolicy};
pub use google_cloud_gax::retry_result::RetryResult;

/// The retry policy the Spanner client applies to RPCs that do not configure
/// their own. It decorates/extends [google_cloud_gax::retry_policy::Aip194Strict].
///
/// Like `Aip194Strict`, this policy retries `UNAVAILABLE` errors and transient
/// failures that occur before the request reaches the service, but only for
/// idempotent requests. In addition — because Spanner allows transport and
/// connection errors to be retried on idempotent operations — it also retries
/// transport/network and I/O errors that `Aip194Strict` would classify as
/// permanent (such as a connection dropped after the request was sent), again
/// only if the request is idempotent.
///
/// The policy places no limit on the number of attempts or the elapsed time.
/// Applications that want to bound the client's default retry behavior can
/// decorate this policy with
/// [RetryPolicyExt][google_cloud_gax::retry_policy::RetryPolicyExt] instead of
/// re-implementing its error classification:
///
/// # Example
/// ```
/// # use std::time::Duration;
/// # use google_cloud_spanner::retry_policy::SpannerRetryPolicy;
/// # use google_cloud_spanner::statement::Statement;
/// # use google_cloud_gax::retry_policy::RetryPolicyExt;
/// let statement = Statement::builder("SELECT * FROM Users")
///     .with_retry_policy(
///         SpannerRetryPolicy::new()
///             .with_attempt_limit(5)
///             .with_time_limit(Duration::from_secs(30)),
///     )
///     .build();
/// ```
#[derive(Clone, Debug)]
pub struct SpannerRetryPolicy {
    inner: Aip194Strict,
}

impl SpannerRetryPolicy {
    /// Creates a new Spanner retry policy.
    pub fn new() -> Self {
        Self {
            inner: Aip194Strict,
        }
    }
}

impl Default for SpannerRetryPolicy {
    fn default() -> Self {
        Self::new()
    }
}

impl RetryPolicy for SpannerRetryPolicy {
    fn on_error(&self, state: &RetryState, error: Error) -> RetryResult {
        // 1. Strict AIP-194 checks (Unavailable, is_transient_and_before_rpc)
        let result = self.inner.on_error(state, error);
        match result {
            // If the strict AIP-194 checks classified the error as permanent (such as a transport
            // error that occurred post-headers), we override it to Continue if the request is idempotent.
            RetryResult::Permanent(error)
                if state.idempotent && (error.is_transport() || error.is_io()) =>
            {
                RetryResult::Continue(error)
            }
            res => res,
        }
    }

    fn on_throttle(&self, state: &RetryState, error: Error) -> ThrottleResult {
        self.inner.on_throttle(state, error)
    }

    fn remaining_time(&self, state: &RetryState) -> Option<Duration> {
        self.inner.remaining_time(state)
    }
}

/// Indicates whether a streaming RPC operation is idempotent and safe to retry.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum StreamIdempotency {
    /// The operation is idempotent and safe to retry on transient errors.
    Idempotent,
    /// The operation is non-idempotent and cannot be safely retried once in-flight.
    NonIdempotent,
}

impl StreamIdempotency {
    /// Returns `true` if the stream operation is idempotent.
    pub(crate) fn is_idempotent(self) -> bool {
        matches!(self, Self::Idempotent)
    }
}

impl From<bool> for StreamIdempotency {
    fn from(safe_to_retry: bool) -> Self {
        if safe_to_retry {
            return Self::Idempotent;
        }
        Self::NonIdempotent
    }
}

/// Evaluates whether a streaming RPC error is retryable according to the configured retry policy.
///
/// Constructs a [`RetryState`] capturing the operation's start time, idempotency, and current attempt count.
/// If retryable, returns `Ok(retryable_error)`. If permanent or retry limits (attempts or elapsed time)
/// have been exhausted, returns `Err(terminal_error)`.
pub(crate) fn check_stream_retry(
    gax_options: &GaxRequestOptions,
    operation_start_time: Instant,
    retry_count: usize,
    idempotency: StreamIdempotency,
    error: Error,
) -> Result<Error, Error> {
    let Some(policy) = gax_options.retry_policy() else {
        return Err(error);
    };

    let attempt_count = (retry_count as u32).saturating_add(1);
    let state = RetryState::new(idempotency.is_idempotent())
        .set_attempt_count(attempt_count)
        .set_start(operation_start_time);

    match policy.on_error(&state, error) {
        RetryResult::Continue(retryable_error) => Ok(retryable_error),
        RetryResult::Permanent(terminal_error) | RetryResult::Exhausted(terminal_error) => {
            Err(terminal_error)
        }
    }
}

/// Evaluates a streaming RPC error against the configured retry policy, applies backoff, and prepares for the next attempt.
///
/// If retryable:
/// 1. Increments `*retry_count`.
/// 2. Bumps the attempt suffix on the request ID header in `gax_options`.
/// 3. Computes the delay (preferring server-recommended `RetryInfo` delay, falling back to configured backoff policy).
/// 4. Sleeps for the delay duration (if non-zero and remaining retry time is sufficient).
/// 5. Returns `Ok(())` so the caller can proceed to reconnect/restart the stream.
///
/// If the error is permanent or retry limits have been exhausted, returns `Err(terminal_error)`.
pub(crate) async fn backoff_stream_retry(
    gax_options: &mut GaxRequestOptions,
    operation_start_time: Instant,
    retry_count: &mut usize,
    idempotency: StreamIdempotency,
    error: Error,
) -> Result<(), Error> {
    let retryable_error = check_stream_retry(
        gax_options,
        operation_start_time,
        *retry_count,
        idempotency,
        error,
    )?;

    *retry_count += 1;
    // The request ID header tracks the upcoming RPC attempt count (1-based), which equals
    // `1 + retry_count`. For example, after the first failure (`retry_count == 1`), the next
    // attempt launched by the caller will be attempt 2.
    update_request_id_attempt(gax_options, (*retry_count as u32).saturating_add(1));

    let state = RetryState::new(idempotency.is_idempotent())
        .set_attempt_count(*retry_count as u32)
        .set_start(operation_start_time);

    let delay = extract_retry_delay_from_error(&retryable_error).or_else(|| {
        gax_options
            .backoff_policy()
            .as_deref()
            .map(|policy| policy.on_failure(&state))
    });

    if let Some(delay) = delay
        && !delay.is_zero()
    {
        if let Some(remaining) = gax_options
            .retry_policy()
            .as_deref()
            .and_then(|policy| policy.remaining_time(&state))
            && remaining < delay
        {
            return Err(Error::exhausted(retryable_error));
        }
        sleep(delay).await;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use google_cloud_gax::backoff_policy::BackoffPolicy;
    use google_cloud_gax::error::Error as GaxError;
    use google_cloud_gax::error::rpc::{Code, Status, StatusDetails};
    use google_cloud_gax::options::RequestOptions;
    use google_cloud_gax::retry_policy::RetryPolicyExt;
    use google_cloud_rpc::model::RetryInfo;
    use http::HeaderMap;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::{Arc, Mutex};

    #[test]
    fn test_spanner_retry_policy_idempotent() {
        let policy = SpannerRetryPolicy::new();
        let state = RetryState::new(true); // idempotent = true

        // 1. Service UNAVAILABLE error should be retried (via inner AIP-194)
        let status = Status::default()
            .set_code(Code::Unavailable)
            .set_message("Service Unavailable");
        let err = GaxError::service(status);
        assert!(
            policy.on_error(&state, err).is_continue(),
            "Expected UNAVAILABLE to be retried when idempotent"
        );

        // 2. Service PERMISSION_DENIED error should not be retried
        let status = Status::default()
            .set_code(Code::PermissionDenied)
            .set_message("Denied");
        let err = GaxError::service(status);
        assert!(
            policy.on_error(&state, err).is_permanent(),
            "Expected PERMISSION_DENIED to not be retried"
        );

        // 3. IO/Transport error should be retried when idempotent
        let err = GaxError::transport(
            HeaderMap::new(),
            std::io::Error::new(std::io::ErrorKind::ConnectionReset, "connection closed"),
        );
        assert!(
            policy.on_error(&state, err).is_continue(),
            "Expected transport connection reset to be retried when idempotent"
        );
    }

    #[test]
    fn test_spanner_retry_policy_non_idempotent() {
        let policy = SpannerRetryPolicy::new();
        let state = RetryState::new(false); // idempotent = false

        // 1. Service UNAVAILABLE error should NOT be retried (AIP-194 requires idempotency)
        let status = Status::default()
            .set_code(Code::Unavailable)
            .set_message("Service Unavailable");
        let err = GaxError::service(status);
        assert!(
            policy.on_error(&state, err).is_permanent(),
            "Expected UNAVAILABLE to be permanent when non-idempotent"
        );

        // 2. IO/Transport error should NOT be retried when non-idempotent
        let err = GaxError::transport(
            HeaderMap::new(),
            std::io::Error::new(std::io::ErrorKind::ConnectionReset, "connection closed"),
        );
        assert!(
            policy.on_error(&state, err).is_permanent(),
            "Expected transport connection reset to not be retried when non-idempotent"
        );
    }

    fn unavailable_error() -> Error {
        Error::service(Status::default().set_code(Code::Unavailable))
    }

    fn permission_denied_error() -> Error {
        Error::service(Status::default().set_code(Code::PermissionDenied))
    }

    #[test]
    fn stream_retry_none_policy() {
        let options = RequestOptions::default();
        let error = unavailable_error();
        let result = check_stream_retry(
            &options,
            Instant::now(),
            0,
            StreamIdempotency::Idempotent,
            error,
        );
        assert!(
            result.is_err(),
            "check_stream_retry must fail when no retry policy is configured"
        );
    }

    #[test]
    fn stream_retry_idempotent_unavailable() {
        let mut options = RequestOptions::default();
        options.set_retry_policy(SpannerRetryPolicy::new());
        let result = check_stream_retry(
            &options,
            Instant::now(),
            0,
            StreamIdempotency::Idempotent,
            unavailable_error(),
        );
        assert!(result.is_ok(), "idempotent UNAVAILABLE must be retryable");
    }

    #[test]
    fn stream_retry_non_idempotent_unavailable() {
        let mut options = RequestOptions::default();
        options.set_retry_policy(SpannerRetryPolicy::new());
        let result = check_stream_retry(
            &options,
            Instant::now(),
            0,
            StreamIdempotency::NonIdempotent,
            unavailable_error(),
        );
        assert!(
            result.is_err(),
            "non-idempotent UNAVAILABLE must not be retryable"
        );
    }

    #[test]
    fn stream_retry_idempotent_permission_denied() {
        let mut options = RequestOptions::default();
        options.set_retry_policy(SpannerRetryPolicy::new());
        let result = check_stream_retry(
            &options,
            Instant::now(),
            0,
            StreamIdempotency::Idempotent,
            permission_denied_error(),
        );
        assert!(result.is_err(), "PERMISSION_DENIED must not be retryable");
    }

    #[test]
    fn stream_retry_attempt_limit() {
        let mut options = RequestOptions::default();
        options.set_retry_policy(SpannerRetryPolicy::new().with_attempt_limit(3));
        let now = Instant::now();

        // retry_count 0 -> attempt 1 of 3: allowed
        let result = check_stream_retry(
            &options,
            now,
            0,
            StreamIdempotency::Idempotent,
            unavailable_error(),
        );
        assert!(
            result.is_ok(),
            "attempt 1 should be within attempt limit of 3"
        );

        // retry_count 1 -> attempt 2 of 3: allowed
        let result = check_stream_retry(
            &options,
            now,
            1,
            StreamIdempotency::Idempotent,
            unavailable_error(),
        );
        assert!(
            result.is_ok(),
            "attempt 2 should be within attempt limit of 3"
        );

        // retry_count 2 -> attempt 3 of 3: exhausted
        let result = check_stream_retry(
            &options,
            now,
            2,
            StreamIdempotency::Idempotent,
            unavailable_error(),
        );
        assert!(
            result.is_err(),
            "attempt 3 should exhaust attempt limit of 3"
        );

        // retry_count 5 -> attempt 6: exhausted
        let result = check_stream_retry(
            &options,
            now,
            5,
            StreamIdempotency::Idempotent,
            unavailable_error(),
        );
        assert!(
            result.is_err(),
            "attempt 6 should exhaust attempt limit of 3"
        );
    }

    #[test]
    fn stream_retry_time_limit() {
        let mut options = RequestOptions::default();
        let time_limit = Duration::from_secs(30);
        options.set_retry_policy(SpannerRetryPolicy::new().with_time_limit(time_limit));
        let now = Instant::now();

        // 1. Within time limit
        let result = check_stream_retry(
            &options,
            now,
            0,
            StreamIdempotency::Idempotent,
            unavailable_error(),
        );
        assert!(
            result.is_ok(),
            "request with start time 'now' must be within time limit"
        );

        // 2. Beyond time limit (started 31 seconds ago with 30s limit)
        let past_start = now - Duration::from_secs(31);
        let result = check_stream_retry(
            &options,
            past_start,
            0,
            StreamIdempotency::Idempotent,
            unavailable_error(),
        );
        assert!(
            result.is_err(),
            "request started 31s ago must exhaust 30s time limit"
        );

        // 3. Exactly at/beyond boundary limit
        let boundary_start = now - Duration::from_secs(30);
        let result = check_stream_retry(
            &options,
            boundary_start,
            0,
            StreamIdempotency::Idempotent,
            unavailable_error(),
        );
        assert!(
            result.is_err(),
            "request started exactly 30s ago must exhaust 30s time limit"
        );
    }

    #[test]
    fn stream_retry_combined_attempt_and_time_limit() {
        let mut options = RequestOptions::default();
        let time_limit = Duration::from_secs(30);
        options.set_retry_policy(
            SpannerRetryPolicy::new()
                .with_attempt_limit(3)
                .with_time_limit(time_limit),
        );
        let now = Instant::now();

        // 1. Within attempt and time limit: allowed
        let result = check_stream_retry(
            &options,
            now,
            0,
            StreamIdempotency::Idempotent,
            unavailable_error(),
        );
        assert!(result.is_ok(), "within both limits must be allowed");

        // 2. Attempt count exceeded before time limit: exhausted
        let result = check_stream_retry(
            &options,
            now,
            2,
            StreamIdempotency::Idempotent,
            unavailable_error(),
        );
        assert!(result.is_err(), "attempt limit exhausted must fail");

        // 3. Time limit exceeded before attempt count: exhausted
        let past_start = now - Duration::from_secs(31);
        let result = check_stream_retry(
            &options,
            past_start,
            0,
            StreamIdempotency::Idempotent,
            unavailable_error(),
        );
        assert!(result.is_err(), "time limit exhausted must fail");
    }

    #[test]
    fn stream_retry_transport_and_io_error_handling() {
        use std::io::{Error as IoError, ErrorKind};

        let mut options = RequestOptions::default();
        options.set_retry_policy(SpannerRetryPolicy::new());
        let now = Instant::now();

        let make_io_error =
            || Error::io(IoError::new(ErrorKind::ConnectionReset, "connection reset"));

        // Idempotent stream retry allows transport/io errors
        let result = check_stream_retry(
            &options,
            now,
            0,
            StreamIdempotency::Idempotent,
            make_io_error(),
        );
        assert!(
            result.is_ok(),
            "idempotent stream retry must retry transport/io error"
        );

        // Non-idempotent stream retry rejects transport/io errors
        let result = check_stream_retry(
            &options,
            now,
            0,
            StreamIdempotency::NonIdempotent,
            make_io_error(),
        );
        assert!(
            result.is_err(),
            "non-idempotent stream retry must reject transport/io error"
        );
    }

    #[tokio::test]
    async fn backoff_stream_retry_non_retryable_error_does_not_mutate_state() {
        let mut options = RequestOptions::default();
        options.set_retry_policy(SpannerRetryPolicy::new());
        let mut retry_count = 0;
        let operation_start_time = Instant::now();

        let result = backoff_stream_retry(
            &mut options,
            operation_start_time,
            &mut retry_count,
            StreamIdempotency::Idempotent,
            permission_denied_error(),
        )
        .await;

        assert!(
            result.is_err(),
            "backoff_stream_retry must fail on non-retryable error"
        );
        assert_eq!(
            retry_count, 0,
            "retry_count must not be incremented when retry policy rejects error"
        );
    }

    #[tokio::test]
    async fn backoff_stream_retry_with_server_delay() {
        let mut options = RequestOptions::default();
        options.set_retry_policy(SpannerRetryPolicy::new());
        let mut retry_count = 0;
        let operation_start_time = Instant::now();

        let retry_info = RetryInfo::default().set_retry_delay(wkt::Duration::clamp(0, 1));
        let status = Status::default()
            .set_code(Code::Unavailable)
            .set_details(vec![StatusDetails::RetryInfo(retry_info)]);
        let error = Error::service(status);

        let result = backoff_stream_retry(
            &mut options,
            operation_start_time,
            &mut retry_count,
            StreamIdempotency::Idempotent,
            error,
        )
        .await;

        assert!(
            result.is_ok(),
            "backoff_stream_retry must succeed on retryable error with server delay"
        );
        assert_eq!(
            retry_count, 1,
            "retry_count must be incremented to 1 after first retry backoff"
        );
    }

    #[derive(Debug)]
    struct TestTrackingBackoff {
        observed_attempt: AtomicU32,
        observed_start: Mutex<Option<Instant>>,
    }

    impl BackoffPolicy for TestTrackingBackoff {
        fn on_failure(&self, state: &RetryState) -> Duration {
            self.observed_attempt
                .store(state.attempt_count, Ordering::SeqCst);
            let mut guard = self.observed_start.lock().expect("lock poisoned");
            *guard = Some(state.start);
            Duration::from_nanos(1)
        }
    }

    #[tokio::test]
    async fn backoff_stream_retry_with_backoff_policy() {
        let mut options = RequestOptions::default();
        options.set_retry_policy(SpannerRetryPolicy::new());
        let tracking_backoff = Arc::new(TestTrackingBackoff {
            observed_attempt: AtomicU32::new(0),
            observed_start: Mutex::new(None),
        });
        options.set_backoff_policy(Arc::clone(&tracking_backoff) as Arc<dyn BackoffPolicy>);

        let mut retry_count = 0;
        let operation_start_time = Instant::now();

        let result = backoff_stream_retry(
            &mut options,
            operation_start_time,
            &mut retry_count,
            StreamIdempotency::Idempotent,
            unavailable_error(),
        )
        .await;

        assert!(
            result.is_ok(),
            "backoff_stream_retry must succeed on retryable error with backoff policy"
        );
        assert_eq!(retry_count, 1, "retry_count must be incremented to 1");
        assert_eq!(
            tracking_backoff.observed_attempt.load(Ordering::SeqCst),
            1,
            "backoff policy must observe attempt_count == 1"
        );
        let recorded_start = tracking_backoff
            .observed_start
            .lock()
            .expect("lock poisoned");
        assert_eq!(
            *recorded_start,
            Some(operation_start_time),
            "backoff policy must receive the exact operation_start_time in RetryState"
        );
    }

    #[tokio::test]
    async fn backoff_stream_retry_server_delay_takes_precedence_over_backoff_policy() {
        let mut options = RequestOptions::default();
        options.set_retry_policy(SpannerRetryPolicy::new());
        let tracking_backoff = Arc::new(TestTrackingBackoff {
            observed_attempt: AtomicU32::new(0),
            observed_start: Mutex::new(None),
        });
        options.set_backoff_policy(Arc::clone(&tracking_backoff) as Arc<dyn BackoffPolicy>);

        let mut retry_count = 0;
        let operation_start_time = Instant::now();

        let retry_info = RetryInfo::default().set_retry_delay(wkt::Duration::clamp(0, 1));
        let status = Status::default()
            .set_code(Code::Unavailable)
            .set_details(vec![StatusDetails::RetryInfo(retry_info)]);
        let error = Error::service(status);

        let result = backoff_stream_retry(
            &mut options,
            operation_start_time,
            &mut retry_count,
            StreamIdempotency::Idempotent,
            error,
        )
        .await;

        assert!(
            result.is_ok(),
            "backoff_stream_retry must succeed with server delay"
        );
        assert_eq!(retry_count, 1, "retry_count must be incremented to 1");
        assert_eq!(
            tracking_backoff.observed_attempt.load(Ordering::SeqCst),
            0,
            "custom backoff policy must NOT be called when server RetryInfo is present"
        );
    }

    #[derive(Debug)]
    struct ZeroDelayBackoff;

    impl BackoffPolicy for ZeroDelayBackoff {
        fn on_failure(&self, _state: &RetryState) -> Duration {
            Duration::ZERO
        }
    }

    #[tokio::test]
    async fn backoff_stream_retry_with_zero_delay() {
        let mut options = RequestOptions::default();
        options.set_retry_policy(SpannerRetryPolicy::new());
        options.set_backoff_policy(Arc::new(ZeroDelayBackoff) as Arc<dyn BackoffPolicy>);

        let mut retry_count = 2;
        let operation_start_time = Instant::now();

        let result = backoff_stream_retry(
            &mut options,
            operation_start_time,
            &mut retry_count,
            StreamIdempotency::Idempotent,
            unavailable_error(),
        )
        .await;

        assert!(
            result.is_ok(),
            "backoff_stream_retry must succeed when backoff delay is zero"
        );
        assert_eq!(
            retry_count, 3,
            "retry_count must be incremented from 2 to 3"
        );
    }

    #[tokio::test]
    async fn backoff_stream_retry_without_backoff_policy() {
        let mut options = RequestOptions::default();
        options.set_retry_policy(SpannerRetryPolicy::new());
        // No backoff policy and no server RetryInfo

        let mut retry_count = 0;
        let operation_start_time = Instant::now();

        let result = backoff_stream_retry(
            &mut options,
            operation_start_time,
            &mut retry_count,
            StreamIdempotency::Idempotent,
            unavailable_error(),
        )
        .await;

        assert!(
            result.is_ok(),
            "backoff_stream_retry must succeed even without a configured backoff policy"
        );
        assert_eq!(retry_count, 1, "retry_count must be incremented to 1");
    }

    #[derive(Debug)]
    struct CustomDelayBackoff(Duration);

    impl BackoffPolicy for CustomDelayBackoff {
        fn on_failure(&self, _state: &RetryState) -> Duration {
            self.0
        }
    }

    #[tokio::test]
    async fn backoff_stream_retry_delay_exceeding_remaining_time_fails_fast_without_sleeping() {
        let mut options = RequestOptions::default();
        options.set_retry_policy(
            SpannerRetryPolicy::new().with_time_limit(Duration::from_millis(100)),
        );
        // Backoff asks for 60 seconds of sleep, which far exceeds the remaining ~10ms
        options.set_backoff_policy(
            Arc::new(CustomDelayBackoff(Duration::from_secs(60))) as Arc<dyn BackoffPolicy>
        );

        let mut retry_count = 0;
        let operation_start_time = Instant::now() - Duration::from_millis(90);

        let start_call = Instant::now();
        let result = backoff_stream_retry(
            &mut options,
            operation_start_time,
            &mut retry_count,
            StreamIdempotency::Idempotent,
            unavailable_error(),
        )
        .await;

        let elapsed = start_call.elapsed();
        assert!(
            result.is_err(),
            "backoff_stream_retry must return Err when delay exceeds remaining time"
        );
        assert!(
            elapsed < Duration::from_millis(100),
            "backoff_stream_retry must return immediately without sleeping for the full delay"
        );
        assert_eq!(
            retry_count, 1,
            "retry_count was incremented before evaluating delay"
        );
    }
}
