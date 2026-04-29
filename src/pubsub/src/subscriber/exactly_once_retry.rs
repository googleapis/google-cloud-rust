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

use super::handler::AckResult;
use super::leaser::ConfirmedAcks;
use crate::error::AckError;
use google_cloud_gax::backoff_policy::BackoffPolicy;
use google_cloud_gax::error::rpc::{Code, StatusDetails};
use google_cloud_gax::retry_loop_internal::retry_loop;
use google_cloud_gax::retry_policy::RetryPolicy;
use google_cloud_gax::retry_throttler::SharedRetryThrottler;
use http;
use std::collections::{HashMap, HashSet};
use std::ops::AsyncFn;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc::UnboundedSender;

pub(super) fn process_attempt_error(
    ack_ids: Vec<String>,
    shared_err: Arc<crate::Error>,
) -> (HashMap<String, AckResult>, Vec<String>) {
    let (transient_failures, permanent_failures) = extract_failures(&shared_err);

    // If the response lacks specific per ack_id failure info, we treat the
    // response as all sharing the same RPC error.
    if transient_failures.is_empty() && permanent_failures.is_empty() {
        // The error is transient, retry.
        if let Some(status) = shared_err.status() {
            match status.code {
                Code::DeadlineExceeded
                | Code::ResourceExhausted
                | Code::Aborted
                | Code::Internal
                | Code::Unavailable => {
                    return (HashMap::new(), ack_ids);
                }
                _ => {}
            }
        }

        let to_confirm = ack_ids
            .into_iter()
            .map(|id| {
                (
                    id,
                    Err(AckError::Rpc {
                        source: shared_err.clone(),
                    }),
                )
            })
            .collect();
        return (to_confirm, Vec::new());
    }

    // Otherwise, we extract specific failures:
    // - ack_ids with transient failures are to be retried.
    // - ack_ids with permanent failures are resolved with the RPC error.
    // - Unlisted ack_ids are considered successfully acknowledged.
    let mut transient = Vec::new();
    let mut to_confirm = HashMap::new();
    for id in ack_ids {
        if transient_failures.contains(&id) {
            transient.push(id);
        } else if permanent_failures.contains(&id) {
            to_confirm.insert(
                id,
                Err(AckError::Rpc {
                    source: shared_err.clone(),
                }),
            );
        } else {
            to_confirm.insert(id, Ok(()));
        }
    }

    (to_confirm, transient)
}

pub(super) fn extract_failures(e: &crate::Error) -> (HashSet<String>, HashSet<String>) {
    let mut transient = HashSet::new();
    let mut permanent = HashSet::new();
    if let Some(status) = e.status() {
        for detail in &status.details {
            if let StatusDetails::ErrorInfo(info) = detail {
                for (k, v) in &info.metadata {
                    if v.starts_with("TRANSIENT_FAILURE_") {
                        transient.insert(k.clone());
                    } else if v.starts_with("PERMANENT_FAILURE_") {
                        permanent.insert(k.clone());
                    }
                }
            }
        }
    }
    (transient, permanent)
}

/// The exactly-once retry loop.
///
/// The request has N IDs. The server can tell us the result of
/// individual operations in the response metadata.
///
/// If the result for an ack ID is a success or permanent error, we can
/// report it, and remove that ack ID from subsequent attempts of the RPC.
///
/// Results are reported via the channel, as they are known. This lets us
/// keep the retry logic in the leaser, while allowing for partial results
/// to be reported before the entire operation completes.
pub(super) async fn exactly_once_retry_loop<F>(
    ack_ids: Vec<String>,
    inner: F,
    confirmed_tx: UnboundedSender<ConfirmedAcks>,
    retry_throttler: SharedRetryThrottler,
    retry_policy: Arc<dyn RetryPolicy>,
    backoff: Arc<dyn BackoffPolicy>,
) where
    F: AsyncFn(Vec<String>) -> crate::Result<()> + Send + Sync + 'static,
{
    // TODO(#5408): Investigate solutions that avoid using Arc/Mutex.
    let remaining_ids = Arc::new(Mutex::new(ack_ids));
    let last_error = Arc::new(Mutex::new(None));
    let attempt = {
        let remaining_ids = remaining_ids.clone();
        let last_error = last_error.clone();
        let confirmed_tx = confirmed_tx.clone();
        async move |_| {
            let ids = {
                let mut ids_guard = remaining_ids.lock().expect("mutex should not be poisoned");
                std::mem::take(&mut *ids_guard)
            };

            let response = inner(ids.clone()).await;

            let (to_confirm, remaining) = match response {
                Ok(_) => (ids.into_iter().map(|id| (id, Ok(()))).collect(), Vec::new()),
                Err(e) => {
                    let shared_err = Arc::new(e);
                    let (to_confirm, remaining) = process_attempt_error(ids, shared_err.clone());

                    if !remaining.is_empty() {
                        let mut err_guard =
                            last_error.lock().expect("mutex should not be poisoned");
                        *err_guard = Some(shared_err);
                    }

                    (to_confirm, remaining)
                }
            };
            let _ = confirmed_tx.send(to_confirm);
            if remaining.is_empty() {
                Ok(())
            } else {
                let mut ids_guard = remaining_ids.lock().expect("mutex should not be poisoned");
                *ids_guard = remaining;
                // Return a synthetic error to indicate that we should retry.
                Err(crate::Error::transport(http::HeaderMap::new(), "retry me"))
            }
        }
    };

    let sleep = async |d| tokio::time::sleep(d).await;
    let _ = retry_loop(attempt, sleep, true, retry_throttler, retry_policy, backoff).await;

    let final_remaining =
        std::mem::take(&mut *remaining_ids.lock().expect("mutex should not be poisoned"));
    if !final_remaining.is_empty() {
        let err = std::mem::take(&mut *last_error.lock().expect("mutex should not be poisoned"));
        if let Some(shared_err) = err {
            let confirmed_acks = final_remaining
                .into_iter()
                .map(|id| {
                    (
                        id,
                        Err(AckError::Rpc {
                            source: shared_err.clone(),
                        }),
                    )
                })
                .collect();
            let _ = confirmed_tx.send(confirmed_acks);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::lease_state::tests::{sorted, test_id, test_ids};
    use super::*;
    use crate::{Error, Response, Result};
    use google_cloud_gax::backoff_policy::BackoffPolicy;
    use google_cloud_gax::error::rpc::{Code, Status};
    use google_cloud_gax::exponential_backoff::ExponentialBackoff;
    use google_cloud_gax::retry_policy::RetryPolicy;
    use google_cloud_gax::retry_result::RetryResult;
    use google_cloud_gax::retry_state::RetryState;
    use google_cloud_gax::retry_throttler::CircuitBreaker;
    use google_cloud_gax::throttle_result::ThrottleResult;
    use google_cloud_rpc::model::ErrorInfo;
    use mockall::Sequence;
    use std::time::Duration;
    use test_case::test_case;
    use tokio::sync::mpsc::unbounded_channel;

    mockall::mock! {
        #[derive(Debug)]
        pub BackoffPolicy {}
        impl BackoffPolicy for BackoffPolicy {
            fn on_failure(&self, state: &RetryState) -> Duration;
        }
    }

    mockall::mock! {
        pub Rpc {
            async fn call(&self, ids: Vec<String>) -> crate::Result<()>;
        }
    }

    mockall::mock! {
        #[derive(Debug)]
        pub RetryPolicy {}
        impl RetryPolicy for RetryPolicy {
            fn on_error(&self, state: &RetryState, error: Error) -> RetryResult;
            fn on_throttle(&self, state: &RetryState, error: Error) -> ThrottleResult;
            fn remaining_time(&self, state: &RetryState) -> Option<Duration>;
        }
    }

    fn to_retry_policy(m: MockRetryPolicy) -> Arc<dyn RetryPolicy> {
        Arc::new(m)
    }

    fn test_retry_throttler() -> SharedRetryThrottler {
        Arc::new(Mutex::new(
            CircuitBreaker::new(1000, 0, 0).expect("This is a valid configuration"),
        ))
    }

    fn test_retry_policy() -> Arc<dyn RetryPolicy> {
        Arc::new(TestRetryPolicy)
    }

    #[derive(Debug)]
    struct TestRetryPolicy;

    impl RetryPolicy for TestRetryPolicy {
        fn on_error(&self, _state: &RetryState, error: Error) -> RetryResult {
            if error.is_transport() {
                RetryResult::Continue(error)
            } else {
                RetryResult::Permanent(error)
            }
        }
    }

    fn response_with_error_info(infos: Vec<ErrorInfo>) -> Result<Response<()>> {
        Err(Error::service(
            Status::default()
                .set_code(Code::FailedPrecondition)
                .set_message("fail")
                .set_details(infos.into_iter().map(StatusDetails::ErrorInfo)),
        ))
    }

    #[test]
    fn extract_failures() {
        let info = ErrorInfo::new()
            .set_reason("reason")
            .set_domain("domain")
            .set_metadata([
                ("ack_1", "TRANSIENT_FAILURE_UNORDERED_ACK_ID"),
                ("ack_2", "GIBBERISH_IGNORE"),
                ("ack_3", "TRANSIENT_FAILURE_OTHER"),
                ("ack_4", "PERMANENT_FAILURE_INVALID_ACK_ID"),
                ("ack_5", "PERMANENT_FAILURE_OTHER"),
            ]);

        let err = response_with_error_info(vec![info]).unwrap_err();
        let (transient, permanent) = super::extract_failures(&err);

        assert_eq!(
            transient,
            HashSet::from(["ack_1".to_string(), "ack_3".to_string()])
        );
        assert_eq!(
            permanent,
            HashSet::from(["ack_4".to_string(), "ack_5".to_string()])
        );
    }

    #[test]
    fn extract_failures_multiple_error_info() {
        let info1 =
            ErrorInfo::new().set_metadata([("ack_1", "TRANSIENT_FAILURE_UNORDERED_ACK_ID")]);
        let info2 = ErrorInfo::new().set_metadata([("ack_2", "PERMANENT_FAILURE_INVALID_ACK_ID")]);

        let err = response_with_error_info(vec![info1, info2]).unwrap_err();
        let (transient, permanent) = super::extract_failures(&err);

        assert_eq!(transient, HashSet::from(["ack_1".to_string()]));
        assert_eq!(permanent, HashSet::from(["ack_2".to_string()]));
    }

    #[test_case(Code::DeadlineExceeded)]
    #[test_case(Code::ResourceExhausted)]
    #[test_case(Code::Aborted)]
    #[test_case(Code::Internal)]
    #[test_case(Code::Unavailable)]
    #[tokio::test]
    async fn process_attempt_error_retryable_code_without_error_info(
        code: Code,
    ) -> anyhow::Result<()> {
        let err = Arc::new(Error::service(
            Status::default()
                .set_code(code)
                .set_message("retryable error"),
        ));
        let (confirmed_acks, remaining) = process_attempt_error(test_ids(1..3), err);

        assert_eq!(remaining, test_ids(1..3));
        assert!(confirmed_acks.is_empty(), "{confirmed_acks:?}");

        Ok(())
    }

    #[test_case(Code::DeadlineExceeded)]
    #[test_case(Code::ResourceExhausted)]
    #[test_case(Code::Aborted)]
    #[test_case(Code::Internal)]
    #[test_case(Code::Unavailable)]
    #[tokio::test]
    async fn process_attempt_error_retryable_code_with_error_info(
        code: Code,
    ) -> anyhow::Result<()> {
        let info = ErrorInfo::new().set_metadata([
            (test_id(1), "PERMANENT_FAILURE_INVALID_ACK_ID"),
            (test_id(2), "TRANSIENT_FAILURE_OTHER"),
        ]);
        let err = Arc::new(Error::service(
            Status::default()
                .set_code(code)
                .set_message("retryable error")
                .set_details([StatusDetails::ErrorInfo(info)]),
        ));
        let (confirmed_acks, remaining) = process_attempt_error(test_ids(1..4), err.clone());

        assert_eq!(remaining, vec![test_id(2)]);

        let err = AckError::Rpc { source: err };
        let expected = [(test_id(1), Err(err)), (test_id(3), Ok(()))]
            .into_iter()
            .collect();
        assert_eq!(confirmed_acks, expected);

        Ok(())
    }

    #[test_case(Code::Cancelled)]
    #[test_case(Code::Unknown)]
    #[test_case(Code::InvalidArgument)]
    #[test_case(Code::NotFound)]
    #[test_case(Code::AlreadyExists)]
    #[test_case(Code::PermissionDenied)]
    #[test_case(Code::Unauthenticated)]
    #[test_case(Code::FailedPrecondition)]
    #[test_case(Code::OutOfRange)]
    #[test_case(Code::Unimplemented)]
    #[test_case(Code::DataLoss)]
    #[tokio::test]
    async fn process_attempt_error_non_retryable_code_without_error_info(
        code: Code,
    ) -> anyhow::Result<()> {
        let err = Arc::new(Error::service(
            Status::default()
                .set_code(code)
                .set_message("non-retryable error"),
        ));
        let (confirmed_acks, remaining) = process_attempt_error(test_ids(1..3), err);

        assert!(remaining.is_empty(), "{remaining:?}");

        let expected = test_ids(1..3)
            .into_iter()
            .map(|id| {
                let err = AckError::Rpc {
                    source: Arc::new(Error::service(
                        Status::default()
                            .set_code(code)
                            .set_message("non-retryable error"),
                    )),
                };
                (id, Err(err))
            })
            .collect();
        assert_eq!(confirmed_acks, expected);

        Ok(())
    }

    #[test_case(Code::Cancelled)]
    #[test_case(Code::Unknown)]
    #[test_case(Code::InvalidArgument)]
    #[test_case(Code::NotFound)]
    #[test_case(Code::AlreadyExists)]
    #[test_case(Code::PermissionDenied)]
    #[test_case(Code::Unauthenticated)]
    #[test_case(Code::FailedPrecondition)]
    #[test_case(Code::OutOfRange)]
    #[test_case(Code::Unimplemented)]
    #[test_case(Code::DataLoss)]
    #[tokio::test]
    async fn process_attempt_error_non_retryable_code_permanent_failure(
        code: Code,
    ) -> anyhow::Result<()> {
        let info =
            ErrorInfo::new().set_metadata([(test_id(1), "PERMANENT_FAILURE_INVALID_ACK_ID")]);

        let err = Arc::new(Error::service(
            Status::default()
                .set_code(code)
                .set_message("non-retryable error")
                .set_details([StatusDetails::ErrorInfo(info.clone())]),
        ));
        let (confirmed_acks, remaining) = process_attempt_error(test_ids(1..3), err);

        assert!(remaining.is_empty(), "{remaining:?}");

        let err = AckError::Rpc {
            source: Arc::new(Error::service(
                Status::default()
                    .set_code(code)
                    .set_message("non-retryable error")
                    .set_details([StatusDetails::ErrorInfo(info)]),
            )),
        };
        let expected = [(test_id(1), Err(err)), (test_id(2), Ok(()))]
            .into_iter()
            .collect();
        assert_eq!(confirmed_acks, expected);

        Ok(())
    }

    #[test_case(Code::Cancelled)]
    #[test_case(Code::Unknown)]
    #[test_case(Code::InvalidArgument)]
    #[test_case(Code::NotFound)]
    #[test_case(Code::AlreadyExists)]
    #[test_case(Code::PermissionDenied)]
    #[test_case(Code::Unauthenticated)]
    #[test_case(Code::FailedPrecondition)]
    #[test_case(Code::OutOfRange)]
    #[test_case(Code::Unimplemented)]
    #[test_case(Code::DataLoss)]
    #[tokio::test]
    async fn process_attempt_error_non_retryable_code_transient_failure(
        code: Code,
    ) -> anyhow::Result<()> {
        let info = ErrorInfo::new().set_metadata([(test_id(1), "TRANSIENT_FAILURE_OTHER")]);

        let err = Arc::new(Error::service(
            Status::default()
                .set_code(code)
                .set_message("non-retryable error")
                .set_details([StatusDetails::ErrorInfo(info)]),
        ));
        let (confirmed_acks, remaining) = process_attempt_error(test_ids(1..3), err);

        assert_eq!(remaining, test_ids(1..2));

        let expected = [(test_id(2), Ok(()))].into_iter().collect();
        assert_eq!(confirmed_acks, expected);

        Ok(())
    }

    #[tokio::test]
    async fn exactly_once_retry_loop_success() -> anyhow::Result<()> {
        let (confirmed_tx, mut confirmed_rx) = unbounded_channel();

        let mut mock = MockRpc::new();
        mock.expect_call()
            .once()
            .withf(|ids| sorted(ids) == test_ids(0..10))
            .returning(|_| Ok(()));

        let mock = Arc::new(tokio::sync::Mutex::new(mock));
        let inner = async move |ids| mock.lock().await.call(ids).await;

        exactly_once_retry_loop(
            test_ids(0..10),
            inner,
            confirmed_tx,
            test_retry_throttler(),
            test_retry_policy(),
            Arc::new(ExponentialBackoff::default()),
        )
        .await;

        let confirmed_acks = confirmed_rx.recv().await.expect("results were not sent");

        // Verify all ack IDs have a result
        let ack_ids: Vec<_> = confirmed_acks.keys().cloned().collect();
        assert_eq!(sorted(&ack_ids), test_ids(0..10));

        // Verify all acks were ok
        for (ack_id, result) in &confirmed_acks {
            assert!(
                result.is_ok(),
                "Expected success for {ack_id}, got {result:?}"
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn exactly_once_retry_loop_failure() -> anyhow::Result<()> {
        let (confirmed_tx, mut confirmed_rx) = unbounded_channel();

        let mut mock = MockRpc::new();
        mock.expect_call()
            .once()
            .withf(|ids| sorted(ids) == test_ids(0..10))
            .returning(|_| {
                Err(Error::service(
                    Status::default()
                        .set_code(Code::FailedPrecondition)
                        .set_message("fail"),
                ))
            });

        let mock = Arc::new(tokio::sync::Mutex::new(mock));
        let inner = async move |ids| mock.lock().await.call(ids).await;

        exactly_once_retry_loop(
            test_ids(0..10),
            inner,
            confirmed_tx,
            test_retry_throttler(),
            test_retry_policy(),
            Arc::new(ExponentialBackoff::default()),
        )
        .await;

        let confirmed_acks = confirmed_rx.recv().await.expect("results were not sent");

        // Verify all ack IDs have a result
        let ack_ids: Vec<_> = confirmed_acks.keys().cloned().collect();
        assert_eq!(sorted(&ack_ids), test_ids(0..10));

        // Verify all values match the specific error
        for (ack_id, result) in &confirmed_acks {
            match result {
                Err(AckError::Rpc { source, .. }) => {
                    let status = source.status().expect("RPC source should have a status");
                    assert_eq!(status.code, Code::FailedPrecondition);
                    assert_eq!(status.message, "fail");
                }
                _ => panic!("Expected RPC error for {ack_id}, got {result:?}"),
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn exactly_once_retry_loop_partial_transient_failure_retry_success() -> anyhow::Result<()>
    {
        let (confirmed_tx, mut confirmed_rx) = unbounded_channel();

        let mut mock = MockRpc::new();
        let mut seq = Sequence::new();
        mock.expect_call()
            .once()
            .in_sequence(&mut seq)
            .withf(|ids| sorted(ids) == test_ids(1..3))
            .returning(|_| {
                let info = ErrorInfo::new().set_metadata([(test_id(1), "TRANSIENT_FAILURE_OTHER")]);
                response_with_error_info(vec![info]).map(|_| ())
            });
        mock.expect_call()
            .once()
            .in_sequence(&mut seq)
            .withf(|ids| ids == &test_ids(1..2))
            .returning(|_| Ok(()));

        let mock = Arc::new(tokio::sync::Mutex::new(mock));
        let inner = async move |ids| mock.lock().await.call(ids).await;

        let mut mock_backoff = MockBackoffPolicy::new();
        mock_backoff
            .expect_on_failure()
            .times(1)
            .return_const(Duration::ZERO);

        exactly_once_retry_loop(
            test_ids(1..3),
            inner,
            confirmed_tx,
            test_retry_throttler(),
            test_retry_policy(),
            Arc::new(mock_backoff),
        )
        .await;

        let confirmed_acks = confirmed_rx.recv().await.expect("results were not sent");
        let expected = [(test_id(2), Ok(()))].into_iter().collect();
        assert_eq!(confirmed_acks, expected);

        let confirmed_acks_final = confirmed_rx.recv().await.expect("results were not sent");
        let expected_final = [(test_id(1), Ok(()))].into_iter().collect();
        assert_eq!(confirmed_acks_final, expected_final);

        Ok(())
    }

    #[tokio::test]
    async fn exactly_once_retry_loop_partial_transient_failure_retry_failure() -> anyhow::Result<()>
    {
        let (confirmed_tx, mut confirmed_rx) = unbounded_channel();

        let mut mock = MockRpc::new();
        let mut seq = Sequence::new();
        mock.expect_call()
            .once()
            .in_sequence(&mut seq)
            .withf(|ids| sorted(ids) == test_ids(1..3))
            .returning(|_| {
                let info = ErrorInfo::new().set_metadata([(test_id(1), "TRANSIENT_FAILURE_OTHER")]);
                response_with_error_info(vec![info]).map(|_| ())
            });
        mock.expect_call()
            .once()
            .in_sequence(&mut seq)
            .withf(|ids| ids == &test_ids(1..2))
            .returning(|_| {
                Err(crate::Error::service(
                    Status::default()
                        .set_code(Code::FailedPrecondition)
                        .set_message("non-retryable failure"),
                ))
            });

        let mock = Arc::new(tokio::sync::Mutex::new(mock));
        let inner = async move |ids| mock.lock().await.call(ids).await;

        let mut mock_backoff = MockBackoffPolicy::new();
        mock_backoff
            .expect_on_failure()
            .times(1)
            .return_const(Duration::ZERO);

        exactly_once_retry_loop(
            test_ids(1..3),
            inner,
            confirmed_tx,
            test_retry_throttler(),
            test_retry_policy(),
            Arc::new(mock_backoff),
        )
        .await;

        let confirmed_acks = confirmed_rx.recv().await.expect("results were not sent");
        let expected = [(test_id(2), Ok(()))].into_iter().collect();
        assert_eq!(confirmed_acks, expected);

        let confirmed_acks_final = confirmed_rx.recv().await.expect("results were not sent");
        let err = AckError::Rpc {
            source: Arc::new(crate::Error::service(
                Status::default()
                    .set_code(Code::FailedPrecondition)
                    .set_message("non-retryable failure"),
            )),
        };
        let expected_final = [(test_id(1), Err(err))].into_iter().collect();
        assert_eq!(confirmed_acks_final, expected_final);

        Ok(())
    }

    #[tokio::test]
    async fn exactly_once_retry_loop_partial_permanent_failure() -> anyhow::Result<()> {
        let (confirmed_tx, mut confirmed_rx) = unbounded_channel();

        let mut mock = MockRpc::new();
        mock.expect_call()
            .once()
            .withf(|ids| sorted(ids) == test_ids(1..3))
            .returning(|_| {
                let info = ErrorInfo::new()
                    .set_metadata([(test_id(1), "PERMANENT_FAILURE_INVALID_ACK_ID")]);
                response_with_error_info(vec![info]).map(|_| ())
            });

        let mock = Arc::new(tokio::sync::Mutex::new(mock));
        let inner = async move |ids| mock.lock().await.call(ids).await;

        exactly_once_retry_loop(
            test_ids(1..3),
            inner,
            confirmed_tx,
            test_retry_throttler(),
            test_retry_policy(),
            Arc::new(ExponentialBackoff::default()),
        )
        .await;

        let confirmed_acks = confirmed_rx.recv().await.expect("results were not sent");

        let err = AckError::Rpc {
            source: Arc::new(
                response_with_error_info(vec![
                    ErrorInfo::new()
                        .set_metadata([(test_id(1), "PERMANENT_FAILURE_INVALID_ACK_ID")]),
                ])
                .unwrap_err(),
            ),
        };
        let expected = [(test_id(1), Err(err)), (test_id(2), Ok(()))]
            .into_iter()
            .collect();
        assert_eq!(confirmed_acks, expected);

        Ok(())
    }

    #[tokio::test]
    async fn exactly_once_retry_loop_exhausted() -> anyhow::Result<()> {
        let (confirmed_tx, mut confirmed_rx) = unbounded_channel();

        let mut mock = MockRpc::new();
        mock.expect_call()
            .once()
            .withf(|ids| sorted(ids) == test_ids(1..3))
            .returning(|_| {
                let info = ErrorInfo::new().set_metadata([(test_id(1), "TRANSIENT_FAILURE_OTHER")]);
                response_with_error_info(vec![info]).map(|_| ())
            });

        let mock = Arc::new(tokio::sync::Mutex::new(mock));
        let inner = async move |ids| mock.lock().await.call(ids).await;

        let mut retry_policy = MockRetryPolicy::new();
        retry_policy.expect_remaining_time().return_const(None);
        retry_policy
            .expect_on_error()
            .once()
            .returning(|_, e| RetryResult::Exhausted(e));

        exactly_once_retry_loop(
            test_ids(1..3),
            inner,
            confirmed_tx,
            test_retry_throttler(),
            to_retry_policy(retry_policy),
            Arc::new(ExponentialBackoff::default()),
        )
        .await;

        // test id 2 should be confirmed as success
        let confirmed_acks = confirmed_rx.recv().await.expect("results were not sent");
        let expected = [(test_id(2), Ok(()))].into_iter().collect();
        assert_eq!(confirmed_acks, expected);

        // test id 1 should be confirmed as error because retry was exhausted
        let confirmed_acks = confirmed_rx
            .recv()
            .await
            .expect("exhausted retry error were not sent");

        let err = AckError::Rpc {
            source: Arc::new(
                response_with_error_info(vec![
                    ErrorInfo::new().set_metadata([(test_id(1), "TRANSIENT_FAILURE_OTHER")]),
                ])
                .unwrap_err(),
            ),
        };
        let expected = [(test_id(1), Err(err))].into_iter().collect();
        assert_eq!(confirmed_acks, expected);

        Ok(())
    }
}
