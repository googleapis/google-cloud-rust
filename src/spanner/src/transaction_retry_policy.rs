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

use crate::Error;
use google_cloud_gax::backoff_policy::BackoffPolicy;
use google_cloud_gax::error::rpc::StatusDetails;
use google_cloud_gax::exponential_backoff::{ExponentialBackoff, ExponentialBackoffBuilder};
use google_cloud_gax::retry_result::RetryResult;
use google_cloud_gax::retry_state::RetryState;
use std::time::Duration;

/// Defines a policy for retrying a transaction when it is aborted by Spanner.
///
/// Spanner can abort any read/write transaction due to lock conflicts or other
/// transient issues. In such cases, the client should retry the complete
/// transaction.
pub trait TransactionRetryPolicy: Send + Sync {
    /// Evaluates whether an aborted transaction should be retried.
    ///
    /// * `error` the `Aborted` error that was raised. Note that this policy
    ///   takes ownership of the error and returns it embedded in the retry result.
    /// * `attempts` is the number of attempts already made (1 for the first failure).
    /// * `elapsed` is the total time spent executing the transaction so far.
    fn on_abort(&self, error: Error, attempts: u32, elapsed: Duration) -> RetryResult;
}

/// Policy for automatically retrying a transaction when it is aborted based on
/// the number of attempts and total elapsed time.
#[derive(Clone, Debug)]
pub struct BasicTransactionRetryPolicy {
    /// The maximum number of attempts to make. If 0, this field is ignored.
    pub max_attempts: u32,
    /// The total maximum time to spend retrying. If 0, this field is ignored.
    pub total_timeout: Duration,
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
            Err(e) => {
                backoff_if_aborted(e, attempts, start_time.elapsed(), policy, &backoff).await?;
            }
        }
    }
}

pub(crate) fn is_aborted(err: &crate::Error) -> bool {
    err.status()
        .is_some_and(|s| s.code == google_cloud_gax::error::rpc::Code::Aborted)
}

pub(crate) fn extract_retry_delay(err: &crate::Error) -> Option<Duration> {
    err.status()?.details.iter().find_map(|detail| {
        let StatusDetails::RetryInfo(retry_info) = detail else {
            return None;
        };
        (*retry_info.retry_delay.as_ref()?).try_into().ok()
    })
}

pub(crate) fn default_retry_backoff() -> ExponentialBackoff {
    ExponentialBackoffBuilder::new()
        .with_initial_delay(Duration::from_millis(10))
        .with_maximum_delay(Duration::from_secs(1))
        .with_scaling(1.3)
        .build()
        .unwrap()
}

/// Evaluates the error against the retry policy and delays execution if a retry is warranted.
/// Returns Ok(()) after sleeping if a retry should occur, otherwise returns Err with the original error.
pub(crate) async fn backoff_if_aborted(
    err: crate::Error,
    attempts: u32,
    elapsed: Duration,
    policy: &dyn TransactionRetryPolicy,
    backoff: &ExponentialBackoff,
) -> crate::Result<()> {
    if !is_aborted(&err) {
        return Err(err);
    }

    let e = match policy.on_abort(err, attempts, elapsed) {
        RetryResult::Continue(err) => err,
        RetryResult::Exhausted(err) | RetryResult::Permanent(err) => return Err(err),
    };

    let sleep_duration = extract_retry_delay(&e)
        .unwrap_or_else(|| backoff.on_failure(&RetryState::new(true).set_attempt_count(attempts)));

    tokio::time::sleep(sleep_duration).await;
    Ok(())
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::Error;
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
            status = status.set_details(vec![Any::from_msg(&retry_info).unwrap()]);
        }

        Error::service(status)
    }

    pub(crate) fn create_aborted_status(
        retry_delay: std::time::Duration,
    ) -> gaxi::grpc::tonic::Status {
        use prost::Message;

        #[derive(Clone, PartialEq, prost::Message)]
        struct MockRetryInfo {
            #[prost(message, optional, tag = "1")]
            retry_delay: Option<prost_types::Duration>,
        }

        let retry_info = MockRetryInfo {
            retry_delay: Some(prost_types::Duration {
                seconds: retry_delay.as_secs() as i64,
                nanos: retry_delay.subsec_nanos() as i32,
            }),
        };

        let mut retry_buf = vec![];
        retry_info.encode(&mut retry_buf).unwrap();

        let status = spanner_grpc_mock::google::rpc::Status {
            code: gaxi::grpc::tonic::Code::Aborted as i32,
            message: "test transaction aborted".to_string(),
            details: vec![prost_types::Any {
                type_url: "type.googleapis.com/google.rpc.RetryInfo".to_string(),
                value: retry_buf,
            }],
        };

        let mut buf = vec![];
        status.encode(&mut buf).unwrap();

        gaxi::grpc::tonic::Status::with_details(
            gaxi::grpc::tonic::Code::Aborted,
            "test transaction aborted",
            bytes::Bytes::from(buf),
        )
    }

    #[test]
    fn auto_traits() {
        static_assertions::assert_impl_all!(
            BasicTransactionRetryPolicy: Send,
            Sync,
            Unpin,
            Clone,
            std::fmt::Debug,
            Default,
            TransactionRetryPolicy,
        );
    }

    #[tokio::test]
    async fn retry_aborted_success_first_try() {
        let policy = BasicTransactionRetryPolicy::default();
        let res = retry_aborted(&policy, || async { Ok::<i32, Error>(42) }).await;
        assert_eq!(res.expect("Transaction should succeed cleanly"), 42);
    }

    #[tokio::test]
    async fn retry_aborted_not_aborted_error() {
        let policy = BasicTransactionRetryPolicy::default();
        let res = retry_aborted(&policy, || async {
            let status = Status::default()
                .set_code(Code::Unavailable)
                .set_message("server unavailable");
            Err::<i32, Error>(Error::service(status))
        })
        .await;

        let err = res.unwrap_err();
        assert_eq!(
            err.status().expect("Error should contain a status").code,
            Code::Unavailable
        );
    }

    #[tokio::test(start_paused = true)]
    async fn retry_aborted_max_attempts_exceeded() {
        let policy = BasicTransactionRetryPolicy {
            max_attempts: 2,
            total_timeout: Duration::from_secs(0),
        };
        let attempts = Arc::new(AtomicU32::new(0));

        let res = retry_aborted(&policy, || {
            let attempts = attempts.clone();
            async move {
                attempts.fetch_add(1, Ordering::SeqCst);
                Err::<i32, Error>(create_aborted_error(None))
            }
        })
        .await;

        assert!(res.is_err());
        assert_eq!(attempts.load(Ordering::SeqCst), 2); // 1 initial + 1 retry
    }

    #[tokio::test(start_paused = true)]
    async fn retry_aborted_with_retry_info() {
        let policy = BasicTransactionRetryPolicy::default();
        let attempts = Arc::new(AtomicU32::new(0));

        let start = tokio::time::Instant::now();
        let res = retry_aborted(&policy, || {
            let attempts = attempts.clone();
            async move {
                let current = attempts.fetch_add(1, Ordering::SeqCst);
                if current == 0 {
                    Err::<i32, Error>(create_aborted_error(Some(Duration::from_nanos(1))))
                } else {
                    Ok::<i32, Error>(100)
                }
            }
        })
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

        let res = retry_aborted(&policy, || {
            let attempts = attempts.clone();
            async move {
                let current = attempts.fetch_add(1, Ordering::SeqCst);
                if current == 0 {
                    Err::<i32, Error>(create_aborted_error(None))
                } else {
                    Ok::<i32, Error>(100)
                }
            }
        })
        .await;

        assert_eq!(
            res.expect("Transaction should succeed using default backoff"),
            100
        );
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn retry_aborted_total_timeout_exceeded() {
        let policy = BasicTransactionRetryPolicy {
            max_attempts: 0,
            total_timeout: Duration::from_secs(1),
        };
        let attempts = Arc::new(AtomicU32::new(0));

        let res = retry_aborted(&policy, || {
            let attempts = attempts.clone();
            async move {
                attempts.fetch_add(1, Ordering::SeqCst);
                // Return a retry delay of 600ms so that after 2 attempts (1.2s total delay),
                // we should definitely exceed the 1 second timeout for the 3rd fail check.
                Err::<i32, Error>(create_aborted_error(Some(Duration::from_millis(600))))
            }
        })
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
        status = status.set_details(vec![Any::from_msg(&retry_info).unwrap()]);
        let err = Error::service(status);
        assert_eq!(extract_retry_delay(&err), None);
    }

    #[test]
    fn extract_retry_delay_invalid_delay() {
        let mut status = Status::default().set_code(Code::Aborted);
        let retry_info = RetryInfo::default().set_retry_delay(wkt::Duration::clamp(
            -10, // Invalid negative duration
            0,
        ));
        status = status.set_details(vec![Any::from_msg(&retry_info).unwrap()]);
        let err = Error::service(status);
        assert_eq!(extract_retry_delay(&err), None);
    }

    #[tokio::test(start_paused = true)]
    async fn retry_aborted_with_custom_policy() {
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

        let res = retry_aborted(&policy, || {
            let attempts = attempts.clone();
            async move {
                attempts.fetch_add(1, Ordering::SeqCst);
                Err::<i32, Error>(create_aborted_error(None))
            }
        })
        .await;

        assert!(res.is_err());
        assert_eq!(attempts.load(Ordering::SeqCst), 3); // Initial + 2 failures check
    }
}
