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

use crate::{Error, Result};
use google_cloud_gax::backoff_policy::BackoffPolicy;
use google_cloud_gax::error::rpc::Code;
use google_cloud_gax::exponential_backoff::{ExponentialBackoff, ExponentialBackoffBuilder};
use google_cloud_gax::retry_result::RetryResult;
use google_cloud_gax::retry_state::RetryState;
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{Instant, sleep};

/// Defines a policy for retrying a transaction when it is aborted by Spanner.
///
/// # Example
/// ```
/// # use google_cloud_spanner::Error;
/// # use google_cloud_spanner::transaction::{RetryResult, TransactionRetryPolicy};
/// # use std::time::Duration;
/// #[derive(Debug)]
/// struct LimitedRetryPolicy {
///     max_attempts: u32,
/// }
///
/// impl TransactionRetryPolicy for LimitedRetryPolicy {
///     fn on_abort(&self, error: Error, attempts: u32, _elapsed: Duration) -> RetryResult {
///         if attempts < self.max_attempts {
///             RetryResult::Continue(error)
///         } else {
///             RetryResult::Exhausted(error)
///         }
///     }
/// }
/// ```
///
/// Spanner can abort any read/write transaction due to lock conflicts or other
/// transient issues. In such cases, the client should retry the complete
/// transaction.
///
/// By default, transaction runners use [`BasicTransactionRetryPolicy`] with no
/// attempt count or timeout limits, retrying aborted transactions until completion
/// or runner timeout.
pub trait TransactionRetryPolicy: Debug + Send + Sync {
    /// Evaluates whether an aborted transaction should be retried.
    ///
    /// * `error` the `Aborted` error that was raised. Note that this policy
    ///   takes ownership of the error and returns it embedded in the retry result.
    /// * `attempts` is the number of attempts already made (1 for the first failure).
    /// * `elapsed` is the total time spent executing the transaction so far.
    fn on_abort(&self, error: Error, attempts: u32, elapsed: Duration) -> RetryResult;
}

impl<T: ?Sized + TransactionRetryPolicy> TransactionRetryPolicy for Box<T> {
    fn on_abort(&self, error: Error, attempts: u32, elapsed: Duration) -> RetryResult {
        (**self).on_abort(error, attempts, elapsed)
    }
}

impl<T: ?Sized + TransactionRetryPolicy> TransactionRetryPolicy for Arc<T> {
    fn on_abort(&self, error: Error, attempts: u32, elapsed: Duration) -> RetryResult {
        (**self).on_abort(error, attempts, elapsed)
    }
}

/// Policy for automatically retrying a transaction when it is aborted based on
/// the number of attempts and total elapsed time.
#[derive(Clone, Debug)]
pub struct BasicTransactionRetryPolicy {
    /// The maximum number of attempts to make. If 0, this field is ignored.
    max_attempts: u32,
    /// The total maximum time to spend retrying. If 0, this field is ignored.
    total_timeout: Duration,
}

impl BasicTransactionRetryPolicy {
    /// Creates a new basic transaction retry policy with no limits.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the maximum number of attempts to make.
    pub fn with_max_attempts(mut self, max_attempts: u32) -> Self {
        self.max_attempts = max_attempts;
        self
    }

    /// Sets the total maximum time to spend retrying.
    pub fn with_total_timeout(mut self, total_timeout: Duration) -> Self {
        self.total_timeout = total_timeout;
        self
    }

    /// Returns the maximum number of attempts configured.
    pub fn max_attempts(&self) -> u32 {
        self.max_attempts
    }

    /// Returns the total maximum time configured.
    pub fn total_timeout(&self) -> Duration {
        self.total_timeout
    }
}

impl Default for BasicTransactionRetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 0,
            total_timeout: Duration::from_secs(0),
        }
    }
}

impl TransactionRetryPolicy for BasicTransactionRetryPolicy {
    fn on_abort(&self, error: Error, attempts: u32, elapsed: Duration) -> RetryResult {
        if self.max_attempts > 0 && attempts >= self.max_attempts {
            return RetryResult::Exhausted(error);
        }
        if self.total_timeout > Duration::from_secs(0) && elapsed > self.total_timeout {
            return RetryResult::Exhausted(error);
        }
        RetryResult::Continue(error)
    }
}

/// Helper method to execute an asynchronous closure, retrying it if the
/// transaction is aborted by the server.
///
/// This is used for operations like Partitioned DML transactions in Cloud Spanner, where
/// the server may abort the transaction due to transient issues, indicating that the client
/// should re-attempt the entire operation.
pub(crate) async fn retry_aborted<T, F, Fut>(
    policy: &dyn TransactionRetryPolicy,
    mut f: F,
    is_emulator: bool,
) -> crate::Result<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = crate::Result<T>>,
{
    let start_time = tokio::time::Instant::now();
    let mut attempts: u32 = 0;

    // This backoff is only used if Spanner does not return a retry delay.
    let backoff = default_retry_backoff();

    loop {
        attempts += 1;
        match f().await {
            Ok(v) => return Ok(v),
            Err(error) => {
                backoff_if_aborted(
                    error,
                    AbortBackoffParams {
                        retry_delay: None,
                        attempts,
                        elapsed: start_time.elapsed(),
                        policy,
                        backoff: &backoff,
                        is_emulator,
                        deadline: None,
                    },
                )
                .await?;
            }
        }
    }
}

pub(crate) fn is_aborted(error: &Error) -> bool {
    error
        .status()
        .is_some_and(|status| status.code == Code::Aborted)
}

pub(crate) use crate::retry_delay::extract_retry_delay_from_error as extract_retry_delay;

pub(crate) fn default_retry_backoff() -> ExponentialBackoff {
    ExponentialBackoffBuilder::new()
        .with_initial_delay(Duration::from_millis(10))
        .with_maximum_delay(Duration::from_secs(1))
        .with_scaling(1.3)
        .build()
        .expect("default retry backoff parameters are valid")
}

pub(crate) fn is_internal_emulator_error(error: &Error) -> bool {
    error.status().is_some_and(|status| {
        status.code == Code::Internal
            && status.message.contains("Schema generation")
            && status
                .message
                .contains("was not registered with the Action Manager")
    })
}

/// Returns true if the error represents an Aborted status or a known emulator internal error.
pub(crate) fn is_aborted_or_emulator_error(error: &Error, is_emulator: bool) -> bool {
    is_aborted(error) || (is_emulator && is_internal_emulator_error(error))
}

/// Parameters for aborted transaction retry backoff.
pub(crate) struct AbortBackoffParams<'a> {
    pub(crate) retry_delay: Option<Duration>,
    pub(crate) attempts: u32,
    pub(crate) elapsed: Duration,
    pub(crate) policy: &'a dyn TransactionRetryPolicy,
    pub(crate) backoff: &'a ExponentialBackoff,
    pub(crate) is_emulator: bool,
    pub(crate) deadline: Option<Instant>,
}

/// Evaluates the error against the retry policy and delays execution if a retry is warranted.
/// Returns Ok(()) after sleeping if a retry should occur, otherwise returns Err with the original error.
pub(crate) async fn backoff_if_aborted(error: Error, params: AbortBackoffParams<'_>) -> Result<()> {
    if !is_aborted_or_emulator_error(&error, params.is_emulator) {
        return Err(error);
    }

    let aborted_error = match params
        .policy
        .on_abort(error, params.attempts, params.elapsed)
    {
        RetryResult::Continue(err) => err,
        RetryResult::Exhausted(err) | RetryResult::Permanent(err) => return Err(err),
    };

    let sleep_duration = params
        .retry_delay
        .or_else(|| extract_retry_delay(&aborted_error))
        .unwrap_or_else(|| {
            params
                .backoff
                .on_failure(&RetryState::new(true).set_attempt_count(params.attempts))
        });

    if let Some(deadline) = params.deadline {
        let now = Instant::now();
        if now >= deadline || deadline.saturating_duration_since(now) < sleep_duration {
            return Err(aborted_error);
        }
    }

    sleep(sleep_duration).await;
    Ok(())
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::Error;
    use gaxi::grpc::tonic::{Code as TonicCode, Status as TonicStatus};
    use google_cloud_gax::error::rpc::{Code, Status};
    use google_cloud_rpc::model::RetryInfo;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};
    use wkt::Any;

    fn create_aborted_error(retry_delay: Option<Duration>) -> Error {
        let mut status = Status::default()
            .set_code(Code::Aborted)
            .set_message("aborted");

        if let Some(delay) = retry_delay {
            let retry_info = RetryInfo::default().set_retry_delay(wkt::Duration::clamp(
                delay.as_secs() as i64,
                delay.subsec_nanos() as i32,
            ));
            status = status.set_details(vec![
                Any::from_msg(&retry_info).expect("serializing RetryInfo to Any must succeed"),
            ]);
        }

        Error::service(status)
    }

    pub(crate) fn create_aborted_status(retry_delay: Duration) -> TonicStatus {
        use crate::retry_delay::{ProtoRetryInfo, RETRY_INFO_TYPE_URL};
        use prost::Message;

        let retry_info = ProtoRetryInfo {
            retry_delay: Some(prost_types::Duration {
                seconds: retry_delay.as_secs() as i64,
                nanos: retry_delay.subsec_nanos() as i32,
            }),
        };

        let mut retry_buf = vec![];
        retry_info
            .encode(&mut retry_buf)
            .expect("encoding retry_info should succeed");

        let status = spanner_grpc_mock::google::rpc::Status {
            code: TonicCode::Aborted as i32,
            message: "test transaction aborted".to_string(),
            details: vec![prost_types::Any {
                type_url: RETRY_INFO_TYPE_URL.to_string(),
                value: retry_buf,
            }],
        };

        let mut buf = vec![];
        status
            .encode(&mut buf)
            .expect("encoding mock status should succeed");

        TonicStatus::with_details(
            TonicCode::Aborted,
            "test transaction aborted",
            bytes::Bytes::from(buf),
        )
    }

    #[test]
    fn auto_traits() {
        use std::fmt::Debug;
        static_assertions::assert_impl_all!(
            BasicTransactionRetryPolicy: Send,
            Sync,
            Unpin,
            Clone,
            Debug,
            Default,
            TransactionRetryPolicy,
        );
        static_assertions::assert_impl_all!(
            Box<dyn TransactionRetryPolicy>: TransactionRetryPolicy,
            Debug,
            Send,
            Sync,
        );
        static_assertions::assert_impl_all!(
            Arc<dyn TransactionRetryPolicy>: TransactionRetryPolicy,
            Debug,
            Send,
            Sync,
        );
    }

    #[test]
    fn box_and_arc_dispatch_on_abort() {
        let boxed: Box<dyn TransactionRetryPolicy> =
            Box::new(BasicTransactionRetryPolicy::new().with_max_attempts(2));
        let error = create_aborted_error(None);
        let result = boxed.on_abort(error, 1, Duration::from_millis(10));
        assert!(
            matches!(result, RetryResult::Continue(_)),
            "Box<dyn TransactionRetryPolicy> should forward on_abort to inner policy"
        );

        let arc: Arc<dyn TransactionRetryPolicy> =
            Arc::new(BasicTransactionRetryPolicy::new().with_max_attempts(2));
        let error = create_aborted_error(None);
        let result = arc.on_abort(error, 1, Duration::from_millis(10));
        assert!(
            matches!(result, RetryResult::Continue(_)),
            "Arc<dyn TransactionRetryPolicy> should forward on_abort to inner policy"
        );
    }

    #[test]
    fn basic_retry_policy_getters() {
        let policy = BasicTransactionRetryPolicy::new()
            .with_max_attempts(3)
            .with_total_timeout(Duration::from_secs(10));
        assert_eq!(policy.max_attempts(), 3);
        assert_eq!(policy.total_timeout(), Duration::from_secs(10));
    }

    #[tokio::test]
    async fn retry_aborted_success_first_try() {
        let policy = BasicTransactionRetryPolicy::default();
        let res = retry_aborted(
            &policy,
            || async { Ok::<i32, Error>(42) },
            /* is_emulator = */ false,
        )
        .await;
        assert_eq!(res.expect("Transaction should succeed cleanly"), 42);
    }

    #[tokio::test]
    async fn retry_aborted_not_aborted_error() {
        let policy = BasicTransactionRetryPolicy::default();
        let res = retry_aborted(
            &policy,
            || async {
                let status = Status::default()
                    .set_code(Code::Unavailable)
                    .set_message("server unavailable");
                Err::<i32, Error>(Error::service(status))
            },
            /* is_emulator = */ false,
        )
        .await;

        let err = res.unwrap_err();
        assert_eq!(
            err.status().expect("Error should contain a status").code,
            Code::Unavailable
        );
    }

    #[tokio::test(start_paused = true)]
    async fn retry_aborted_max_attempts_exceeded() {
        let policy = BasicTransactionRetryPolicy::new()
            .with_max_attempts(2)
            .with_total_timeout(Duration::from_secs(0));
        let attempts = Arc::new(AtomicU32::new(0));

        let res = retry_aborted(
            &policy,
            || {
                let attempts = attempts.clone();
                async move {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    Err::<i32, Error>(create_aborted_error(None))
                }
            },
            /* is_emulator = */ false,
        )
        .await;

        assert!(res.is_err());
        assert_eq!(attempts.load(Ordering::SeqCst), 2); // 1 initial + 1 retry
    }

    #[tokio::test(start_paused = true)]
    async fn retry_aborted_with_retry_info() {
        let policy = BasicTransactionRetryPolicy::default();
        let attempts = Arc::new(AtomicU32::new(0));

        let start = tokio::time::Instant::now();
        let res = retry_aborted(
            &policy,
            || {
                let attempts = attempts.clone();
                async move {
                    let current = attempts.fetch_add(1, Ordering::SeqCst);
                    if current == 0 {
                        Err::<i32, Error>(create_aborted_error(Some(Duration::from_nanos(1))))
                    } else {
                        Ok::<i32, Error>(100)
                    }
                }
            },
            /* is_emulator = */ false,
        )
        .await;
        let elapsed = start.elapsed();

        assert_eq!(res.expect("Transaction should succeed after 1 retry"), 100);
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
        assert!(
            elapsed >= Duration::from_nanos(1),
            "Expected elapsed time to be at least 1ns, but was {:?}",
            elapsed
        );
    }

    #[tokio::test(start_paused = true)]
    async fn retry_aborted_with_default_backoff() {
        let policy = BasicTransactionRetryPolicy::default();
        let attempts = Arc::new(AtomicU32::new(0));

        let res = retry_aborted(
            &policy,
            || {
                let attempts = attempts.clone();
                async move {
                    let current = attempts.fetch_add(1, Ordering::SeqCst);
                    if current == 0 {
                        Err::<i32, Error>(create_aborted_error(None))
                    } else {
                        Ok::<i32, Error>(100)
                    }
                }
            },
            /* is_emulator = */ false,
        )
        .await;

        assert_eq!(
            res.expect("Transaction should succeed using default backoff"),
            100
        );
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn retry_aborted_total_timeout_exceeded() {
        let policy = BasicTransactionRetryPolicy::new()
            .with_max_attempts(0)
            .with_total_timeout(Duration::from_secs(1));
        let attempts = Arc::new(AtomicU32::new(0));

        let res = retry_aborted(
            &policy,
            || {
                let attempts = attempts.clone();
                async move {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    // Return a retry delay of 600ms so that after 2 attempts (1.2s total delay),
                    // we should definitely exceed the 1 second timeout for the 3rd fail check.
                    Err::<i32, Error>(create_aborted_error(Some(Duration::from_millis(600))))
                }
            },
            /* is_emulator = */ false,
        )
        .await;

        assert!(res.is_err());
        assert_eq!(attempts.load(Ordering::SeqCst), 3); // Initial + 2 delays = 1.0s elapsed *before* the 3rd attempt's delay
    }

    #[test]
    fn is_aborted_non_status_error() {
        let err = Error::deser("test internal error");
        assert!(!is_aborted(&err));
    }

    #[test]
    fn extract_retry_delay_no_status() {
        let err = Error::deser("test internal error");
        assert_eq!(extract_retry_delay(&err), None);
    }

    #[test]
    fn extract_retry_delay_no_retry_info() {
        let mut status = Status::default().set_code(Code::Aborted);
        // Put a generic empty 'Any' which is not a RetryInfo
        status = status.set_details(vec![Any::default()]);
        let err = Error::service(status);
        assert_eq!(extract_retry_delay(&err), None);
    }

    #[test]
    fn extract_retry_delay_empty_retry_info() {
        let mut status = Status::default().set_code(Code::Aborted);
        let retry_info = RetryInfo::default(); // no retry_delay set
        status = status.set_details(vec![
            Any::from_msg(&retry_info).expect("serializing RetryInfo to Any must succeed"),
        ]);
        let error = Error::service(status);
        assert_eq!(extract_retry_delay(&error), None);
    }

    #[test]
    fn extract_retry_delay_invalid_delay() {
        let mut status = Status::default().set_code(Code::Aborted);
        let retry_info = RetryInfo::default().set_retry_delay(wkt::Duration::clamp(
            -10, // Invalid negative duration
            0,
        ));
        status = status.set_details(vec![
            Any::from_msg(&retry_info).expect("serializing RetryInfo to Any must succeed"),
        ]);
        let error = Error::service(status);
        assert_eq!(extract_retry_delay(&error), None);
    }

    #[tokio::test(start_paused = true)]
    async fn retry_aborted_with_custom_policy() {
        #[derive(Debug)]
        struct CustomPolicy;
        impl TransactionRetryPolicy for CustomPolicy {
            fn on_abort(&self, error: Error, attempts: u32, _elapsed: Duration) -> RetryResult {
                if attempts < 3 {
                    RetryResult::Continue(error)
                } else {
                    RetryResult::Exhausted(error)
                }
            }
        }

        let policy = CustomPolicy;
        let attempts = Arc::new(AtomicU32::new(0));

        let res = retry_aborted(
            &policy,
            || {
                let attempts = attempts.clone();
                async move {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    Err::<i32, Error>(create_aborted_error(None))
                }
            },
            /* is_emulator = */ false,
        )
        .await;

        assert!(res.is_err());
        assert_eq!(attempts.load(Ordering::SeqCst), 3); // Initial + 2 failures check
    }

    #[tokio::test(start_paused = true)]
    async fn retry_aborted_emulator_internal_schema_error() {
        let policy = BasicTransactionRetryPolicy::default();
        let attempts = Arc::new(AtomicU32::new(0));

        let make_schema_error = || {
            let status = Status::default().set_code(Code::Internal).set_message(
                "INTERNAL: Schema generation 0 was not registered with the Action Manager",
            );
            Error::service(status)
        };

        // If not running on emulator, it should fail immediately (no retry)
        let res = retry_aborted(
            &policy,
            || {
                let attempts = attempts.clone();
                let err = make_schema_error();
                async move {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    Err::<i32, Error>(err)
                }
            },
            /* is_emulator = */ false,
        )
        .await;
        assert!(res.is_err());
        assert_eq!(attempts.load(Ordering::SeqCst), 1);

        // If running on the emulator, it should retry just like aborted error
        attempts.store(0, Ordering::SeqCst);
        let res = retry_aborted(
            &policy,
            || {
                let attempts = attempts.clone();
                let err = make_schema_error();
                async move {
                    let current = attempts.fetch_add(1, Ordering::SeqCst);
                    if current == 0 {
                        Err::<i32, Error>(err)
                    } else {
                        Ok::<i32, Error>(200)
                    }
                }
            },
            /* is_emulator = */ true,
        )
        .await;
        assert_eq!(res.expect("should succeed after retry"), 200);
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn backoff_if_aborted_skips_sleep_when_deadline_passed() {
        let policy = BasicTransactionRetryPolicy::default();
        let backoff = default_retry_backoff();
        let deadline = Instant::now() - Duration::from_millis(100);
        let error = create_aborted_error(Some(Duration::from_millis(500)));

        let start = Instant::now();
        let result = backoff_if_aborted(
            error,
            AbortBackoffParams {
                retry_delay: None,
                attempts: 1,
                elapsed: Duration::from_millis(10),
                policy: &policy,
                backoff: &backoff,
                is_emulator: false,
                deadline: Some(deadline),
            },
        )
        .await;

        assert!(
            result.is_err(),
            "backoff_if_aborted should return Err when deadline has passed"
        );
        let elapsed = start.elapsed();
        assert_eq!(
            elapsed,
            Duration::ZERO,
            "no sleep should occur when deadline has passed"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn backoff_if_aborted_skips_sleep_when_delay_exceeds_remaining_deadline() {
        let policy = BasicTransactionRetryPolicy::default();
        let backoff = default_retry_backoff();
        let deadline = Instant::now() + Duration::from_millis(100);
        let error = create_aborted_error(Some(Duration::from_millis(500)));

        let start = Instant::now();
        let result = backoff_if_aborted(
            error,
            AbortBackoffParams {
                retry_delay: None,
                attempts: 1,
                elapsed: Duration::from_millis(10),
                policy: &policy,
                backoff: &backoff,
                is_emulator: false,
                deadline: Some(deadline),
            },
        )
        .await;

        assert!(
            result.is_err(),
            "backoff_if_aborted should return Err when backoff delay exceeds remaining deadline"
        );
        let elapsed = start.elapsed();
        assert_eq!(
            elapsed,
            Duration::ZERO,
            "no sleep should occur when backoff delay exceeds remaining deadline"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn backoff_if_aborted_sleeps_when_delay_within_deadline() {
        let policy = BasicTransactionRetryPolicy::default();
        let backoff = default_retry_backoff();
        let deadline = Instant::now() + Duration::from_millis(500);
        let error = create_aborted_error(Some(Duration::from_millis(100)));

        let start = Instant::now();
        let result = backoff_if_aborted(
            error,
            AbortBackoffParams {
                retry_delay: None,
                attempts: 1,
                elapsed: Duration::from_millis(10),
                policy: &policy,
                backoff: &backoff,
                is_emulator: false,
                deadline: Some(deadline),
            },
        )
        .await;

        assert!(
            result.is_ok(),
            "backoff_if_aborted should succeed when backoff delay is within deadline"
        );
        let elapsed = start.elapsed();
        assert_eq!(
            elapsed,
            Duration::from_millis(100),
            "expected sleep duration to match retry delay"
        );
    }

    #[test]
    fn is_aborted_or_emulator_error_truth_table() {
        let aborted_status = Status::default().set_code(Code::Aborted);
        let aborted_error = Error::service(aborted_status);
        assert!(
            is_aborted_or_emulator_error(&aborted_error, false),
            "Aborted must match when is_emulator is false"
        );
        assert!(
            is_aborted_or_emulator_error(&aborted_error, true),
            "Aborted must match when is_emulator is true"
        );

        let emulator_schema_status = Status::default()
            .set_code(Code::Internal)
            .set_message("Schema generation error: was not registered with the Action Manager");
        let emulator_schema_error = Error::service(emulator_schema_status);
        assert!(
            is_aborted_or_emulator_error(&emulator_schema_error, true),
            "Internal emulator schema error must match when is_emulator is true"
        );
        assert!(
            !is_aborted_or_emulator_error(&emulator_schema_error, false),
            "Internal emulator schema error must NOT match when is_emulator is false"
        );

        let other_internal_status = Status::default()
            .set_code(Code::Internal)
            .set_message("Other unexpected internal failure");
        let other_internal_error = Error::service(other_internal_status);
        assert!(
            !is_aborted_or_emulator_error(&other_internal_error, true),
            "Generic Internal error must NOT match even on emulator"
        );
        assert!(
            !is_aborted_or_emulator_error(&other_internal_error, false),
            "Generic Internal error must NOT match on production"
        );

        let unavailable_status = Status::default().set_code(Code::Unavailable);
        let unavailable_error = Error::service(unavailable_status);
        assert!(
            !is_aborted_or_emulator_error(&unavailable_error, true),
            "Unavailable error must NOT match as aborted"
        );
        assert!(
            !is_aborted_or_emulator_error(&unavailable_error, false),
            "Unavailable error must NOT match as aborted"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn backoff_if_aborted_prefers_params_retry_delay() {
        let policy = BasicTransactionRetryPolicy::default();
        let backoff = default_retry_backoff();
        // Error contains 50ms retry delay, but params specifies 250ms override.
        let error = create_aborted_error(Some(Duration::from_millis(50)));

        let start = Instant::now();
        let result = backoff_if_aborted(
            error,
            AbortBackoffParams {
                retry_delay: Some(Duration::from_millis(250)),
                attempts: 1,
                elapsed: Duration::from_millis(10),
                policy: &policy,
                backoff: &backoff,
                is_emulator: false,
                deadline: None,
            },
        )
        .await;

        result.expect("backoff_if_aborted should succeed");
        let elapsed = start.elapsed();
        assert_eq!(
            elapsed,
            Duration::from_millis(250),
            "expected sleep duration to match params retry_delay override"
        );
    }
}
