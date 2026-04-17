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

use super::handler::AckResult;
use super::retry_policy::StreamRetryPolicy;
use super::retry_policy::{exactly_once_options, rpc_options};
use super::stub::Stub;
use crate::RequestOptions;
use crate::error::AckError;
use crate::model::{AcknowledgeRequest, ModifyAckDeadlineRequest};
use google_cloud_gax::backoff_policy::BackoffPolicy;
use google_cloud_gax::error::rpc::{Code, StatusDetails};
use google_cloud_gax::exponential_backoff::ExponentialBackoff;
use google_cloud_gax::retry_loop_internal::retry_loop;
use google_cloud_gax::retry_policy::RetryPolicy;
use google_cloud_gax::retry_throttler::{CircuitBreaker, SharedRetryThrottler};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc::UnboundedSender;

/// A trait representing leaser actions.
///
/// We stub out the interface, in order to test the lease management.
#[async_trait::async_trait]
pub(super) trait Leaser {
    /// Acknowledge a batch of messages.
    async fn ack(&self, ack_ids: Vec<String>);
    /// Negatively acknowledge a batch of messages.
    async fn nack(&self, ack_ids: Vec<String>);
    /// Extend lease deadlines for a batch of messages.
    async fn extend(&self, ack_ids: Vec<String>);

    /// Acknowledge a batch of messages with exactly-once semantics.
    ///
    /// The caller should spawn a task for this operation, as retries can take
    /// arbitrarily long.
    async fn confirmed_ack(&self, ack_ids: Vec<String>);
    /// Negatively acknowledge a batch of messages and confirm the result.
    async fn confirmed_nack(&self, ack_ids: Vec<String>);
}

/// A map of exactly-once ack IDs to their final result.
pub(super) type ConfirmedAcks = HashMap<String, AckResult>;

pub(super) struct DefaultLeaser<T>
where
    T: Stub + 'static,
{
    inner: Arc<T>,
    confirmed_tx: UnboundedSender<ConfirmedAcks>,
    options: RequestOptions,
    exactly_once_options: RequestOptions,
    subscription: String,
    ack_deadline_seconds: i32,
    backoff: Arc<dyn BackoffPolicy>,
}

impl<T> Clone for DefaultLeaser<T>
where
    T: Stub + 'static,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            confirmed_tx: self.confirmed_tx.clone(),
            options: self.options.clone(),
            exactly_once_options: self.exactly_once_options.clone(),
            subscription: self.subscription.clone(),
            ack_deadline_seconds: self.ack_deadline_seconds,
            backoff: self.backoff.clone(),
        }
    }
}

impl<T> DefaultLeaser<T>
where
    T: Stub + 'static,
{
    pub(super) fn new(
        inner: Arc<T>,
        confirmed_tx: UnboundedSender<ConfirmedAcks>,
        subscription: String,
        ack_deadline_seconds: i32,
        grpc_subchannel_count: usize,
    ) -> Self {
        let eo_options = exactly_once_options();
        let backoff = backoff_policy(&eo_options);
        Self::new_with_backoff(
            inner,
            confirmed_tx,
            subscription,
            ack_deadline_seconds,
            grpc_subchannel_count,
            backoff,
        )
    }

    pub(super) fn new_with_backoff(
        inner: Arc<T>,
        confirmed_tx: UnboundedSender<ConfirmedAcks>,
        subscription: String,
        ack_deadline_seconds: i32,
        grpc_subchannel_count: usize,
        // The default backoff policy is non-deterministic. Exposing the backoff
        // policy in this interface helps us set better test expectations.
        backoff: Arc<dyn BackoffPolicy>,
    ) -> Self {
        DefaultLeaser {
            inner,
            confirmed_tx,
            options: rpc_options(grpc_subchannel_count),
            exactly_once_options: exactly_once_options(),
            subscription,
            ack_deadline_seconds,
            backoff,
        }
    }
}

#[async_trait::async_trait]
impl<T> Leaser for DefaultLeaser<T>
where
    T: Stub + 'static,
{
    async fn ack(&self, ack_ids: Vec<String>) {
        let req = AcknowledgeRequest::new()
            .set_subscription(self.subscription.clone())
            .set_ack_ids(ack_ids);
        let _ = self.inner.acknowledge(req, self.options.clone()).await;
    }

    async fn nack(&self, ack_ids: Vec<String>) {
        let req = ModifyAckDeadlineRequest::new()
            .set_subscription(self.subscription.clone())
            .set_ack_ids(ack_ids)
            .set_ack_deadline_seconds(0);
        let _ = self
            .inner
            .modify_ack_deadline(req, self.options.clone())
            .await;
    }

    async fn extend(&self, ack_ids: Vec<String>) {
        let req = ModifyAckDeadlineRequest::new()
            .set_subscription(self.subscription.clone())
            .set_ack_ids(ack_ids)
            .set_ack_deadline_seconds(self.ack_deadline_seconds);
        let _ = self
            .inner
            .modify_ack_deadline(req, self.options.clone())
            .await;
    }

    /// The exactly-once ack retry loop.
    ///
    /// The request has N ack IDs. The server can tell us the result of
    /// individual acks in the response metadata.
    ///
    /// If the result for an ack ID is a success or permanent error, we can
    /// report it, and remove that ack ID from subsequent attempts of the RPC.
    ///
    /// Results are reported via the channel, as they are known. This lets us
    /// keep the retry logic in the leaser, while allowing for partial results
    /// to be reported before the entire operation completes.
    async fn confirmed_ack(&self, ack_ids: Vec<String>) {
        // TODO(#5408): Investigate solutions that avoid using Arc/Mutex.
        let remaining_ids = Arc::new(Mutex::new(ack_ids));
        let last_error = Arc::new(Mutex::new(None));

        let attempt = {
            let remaining_ids = remaining_ids.clone();
            let last_error = last_error.clone();
            let leaser = self.clone();
            let options = self.options.clone();
            async move |_| {
                let ids = {
                    let mut ids_guard = remaining_ids.lock().expect("mutex should not be poisoned");
                    std::mem::take(&mut *ids_guard)
                };

                let req = AcknowledgeRequest::new()
                    .set_subscription(leaser.subscription.clone())
                    .set_ack_ids(ids.clone());
                let response = leaser.inner.acknowledge(req, options.clone()).await;

                let (to_confirm, remaining) = match response {
                    Ok(_) => (ids.into_iter().map(|id| (id, Ok(()))).collect(), Vec::new()),
                    Err(e) => {
                        let shared_err = Arc::new(e);
                        let (to_confirm, remaining) =
                            process_ack_attempt_error(ids, shared_err.clone());

                        if !remaining.is_empty() {
                            let mut err_guard =
                                last_error.lock().expect("mutex should not be poisoned");
                            *err_guard = Some(shared_err);
                        }

                        (to_confirm, remaining)
                    }
                };
                let _ = leaser.confirmed_tx.send(to_confirm);
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
        // Note: the retry policy is not directly used as attempt explicitly decides
        // the retry logic by returning a synthetic error.
        let _ = retry_loop(
            attempt,
            sleep,
            true,
            retry_throttler(&self.exactly_once_options),
            retry_policy(&self.exactly_once_options),
            self.backoff.clone(),
        )
        .await;

        let final_remaining =
            std::mem::take(&mut *remaining_ids.lock().expect("mutex should not be poisoned"));
        if !final_remaining.is_empty() {
            let err =
                std::mem::take(&mut *last_error.lock().expect("mutex should not be poisoned"));
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
                let _ = self.confirmed_tx.send(confirmed_acks);
            }
        }
    }

    async fn confirmed_nack(&self, ack_ids: Vec<String>) {
        let req = ModifyAckDeadlineRequest::new()
            .set_subscription(self.subscription.clone())
            .set_ack_ids(ack_ids.clone())
            .set_ack_deadline_seconds(0);
        let response = self
            .inner
            .modify_ack_deadline(req, self.options.clone())
            .await;
        let shared_result = response.map(|_| ()).map_err(Arc::new);
        let confirmed_acks = ack_ids
            .into_iter()
            .map(|id| {
                (
                    id,
                    shared_result
                        .clone()
                        .map_err(|source| AckError::Rpc { source }),
                )
            })
            .collect();
        let _ = self.confirmed_tx.send(confirmed_acks);
    }
}

fn retry_policy(options: &RequestOptions) -> Arc<dyn RetryPolicy> {
    options
        .retry_policy()
        .clone()
        .unwrap_or_else(|| Arc::new(StreamRetryPolicy))
}

fn backoff_policy(options: &RequestOptions) -> Arc<dyn BackoffPolicy> {
    options
        .backoff_policy()
        .clone()
        .unwrap_or_else(|| Arc::new(ExponentialBackoff::default()))
}

fn retry_throttler(options: &RequestOptions) -> SharedRetryThrottler {
    options.retry_throttler().clone().unwrap_or_else(|| {
        // Effectively disable throttling. The stub throttles.
        Arc::new(Mutex::new(
            CircuitBreaker::new(1000, 0, 0).expect("This is a valid configuration"),
        ))
    })
}

fn process_ack_attempt_error(
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

fn extract_failures(e: &crate::Error) -> (HashSet<String>, HashSet<String>) {
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

#[cfg(test)]
pub(super) mod tests {
    use super::super::lease_state::tests::{sorted, test_id, test_ids};
    use super::super::retry_policy::tests::verify_policies;
    use super::super::stub::tests::MockStub;
    use super::*;
    use crate::{Error, Response, Result};
    use google_cloud_gax::error::rpc::{Code, Status};
    use google_cloud_gax::retry_state::RetryState;
    use google_cloud_rpc::model::ErrorInfo;
    use mockall::Sequence;
    use std::time::Duration;
    use test_case::test_case;
    use tokio::sync::Mutex;
    use tokio::sync::mpsc::unbounded_channel;

    mockall::mock! {
        #[derive(Debug)]
        pub(in super::super) Leaser {}
        #[async_trait::async_trait]
        impl Leaser for Leaser {
            async fn ack(&self, ack_ids: Vec<String>);
            async fn nack(&self, ack_ids: Vec<String>);
            async fn extend(&self, ack_ids: Vec<String>);
            async fn confirmed_ack(&self, ack_ids: Vec<String>);
            async fn confirmed_nack(&self, ack_ids: Vec<String>);
        }
    }

    #[async_trait::async_trait]
    impl Leaser for Arc<MockLeaser> {
        async fn ack(&self, ack_ids: Vec<String>) {
            MockLeaser::ack(self, ack_ids).await
        }
        async fn nack(&self, ack_ids: Vec<String>) {
            MockLeaser::nack(self, ack_ids).await
        }
        async fn extend(&self, ack_ids: Vec<String>) {
            MockLeaser::extend(self, ack_ids).await
        }
        async fn confirmed_ack(&self, ack_ids: Vec<String>) {
            MockLeaser::confirmed_ack(self, ack_ids).await
        }
        async fn confirmed_nack(&self, ack_ids: Vec<String>) {
            MockLeaser::confirmed_nack(self, ack_ids).await
        }
    }

    #[async_trait::async_trait]
    impl Leaser for Arc<Mutex<MockLeaser>> {
        async fn ack(&self, ack_ids: Vec<String>) {
            self.lock().await.ack(ack_ids).await
        }
        async fn nack(&self, ack_ids: Vec<String>) {
            self.lock().await.nack(ack_ids).await
        }
        async fn extend(&self, ack_ids: Vec<String>) {
            self.lock().await.extend(ack_ids).await
        }
        async fn confirmed_ack(&self, ack_ids: Vec<String>) {
            self.lock().await.confirmed_ack(ack_ids).await
        }
        async fn confirmed_nack(&self, ack_ids: Vec<String>) {
            self.lock().await.confirmed_nack(ack_ids).await
        }
    }

    mockall::mock! {
        #[derive(Debug)]
        pub(in super::super) BackoffPolicy {}
        impl BackoffPolicy for BackoffPolicy {
            fn on_failure(&self, state: &RetryState) -> Duration;
        }
    }

    impl PartialEq for AckError {
        fn eq(&self, other: &Self) -> bool {
            match (self, other) {
                (AckError::LeaseExpired, AckError::LeaseExpired) => true,
                (AckError::ShutdownBeforeAck, AckError::ShutdownBeforeAck) => true,
                (AckError::Rpc { source: s1 }, AckError::Rpc { source: s2 }) => {
                    format!("{:?}", s1) == format!("{:?}", s2)
                }
                (AckError::Shutdown(e1), AckError::Shutdown(e2)) => {
                    format!("{:?}", e1) == format!("{:?}", e2)
                }
                _ => false,
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
    fn clone() {
        let (confirmed_tx, _confirmed_rx) = unbounded_channel();
        let leaser = DefaultLeaser::new(
            Arc::new(MockStub::new()),
            confirmed_tx,
            "projects/my-project/subscriptions/my-subscription".to_string(),
            10,
            1_usize,
        );

        let clone = leaser.clone();
        assert!(Arc::ptr_eq(&leaser.inner, &clone.inner));
        assert!(leaser.confirmed_tx.same_channel(&clone.confirmed_tx));
        assert_eq!(leaser.subscription, clone.subscription);
        assert_eq!(leaser.ack_deadline_seconds, clone.ack_deadline_seconds);
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

    #[tokio::test]
    async fn ack() {
        let (confirmed_tx, _confirmed_rx) = unbounded_channel();
        let mut mock = MockStub::new();
        mock.expect_acknowledge().times(1).return_once(|r, o| {
            assert_eq!(
                r.subscription,
                "projects/my-project/subscriptions/my-subscription"
            );
            assert_eq!(r.ack_ids, test_ids(0..10));
            verify_policies(o, 16);
            Ok(Response::from(()))
        });

        let leaser = DefaultLeaser::new(
            Arc::new(mock),
            confirmed_tx,
            "projects/my-project/subscriptions/my-subscription".to_string(),
            10,
            16_usize,
        );
        leaser.ack(test_ids(0..10)).await;
    }

    #[tokio::test]
    async fn nack() {
        let (confirmed_tx, _confirmed_rx) = unbounded_channel();
        let mut mock = MockStub::new();
        mock.expect_modify_ack_deadline()
            .times(1)
            .return_once(|r, o| {
                assert_eq!(r.ack_deadline_seconds, 0);
                assert_eq!(
                    r.subscription,
                    "projects/my-project/subscriptions/my-subscription"
                );
                assert_eq!(r.ack_ids, test_ids(0..10));
                verify_policies(o, 16);
                Ok(Response::from(()))
            });

        let leaser = DefaultLeaser::new(
            Arc::new(mock),
            confirmed_tx,
            "projects/my-project/subscriptions/my-subscription".to_string(),
            10,
            16_usize,
        );
        leaser.nack(test_ids(0..10)).await;
    }

    #[tokio::test]
    async fn extend() {
        let (confirmed_tx, _confirmed_rx) = unbounded_channel();
        let mut mock = MockStub::new();
        mock.expect_modify_ack_deadline()
            .times(1)
            .return_once(|r, o| {
                assert_eq!(r.ack_deadline_seconds, 10);
                assert_eq!(
                    r.subscription,
                    "projects/my-project/subscriptions/my-subscription"
                );
                assert_eq!(r.ack_ids, test_ids(0..10));
                verify_policies(o, 16);
                Ok(Response::from(()))
            });

        let leaser = DefaultLeaser::new(
            Arc::new(mock),
            confirmed_tx,
            "projects/my-project/subscriptions/my-subscription".to_string(),
            10,
            16_usize,
        );
        leaser.extend(test_ids(0..10)).await;
    }

    #[tokio::test]
    async fn confirmed_ack_success() -> anyhow::Result<()> {
        let (confirmed_tx, mut confirmed_rx) = unbounded_channel();
        let mut mock = MockStub::new();
        mock.expect_acknowledge().times(1).return_once(|r, _| {
            assert_eq!(
                r.subscription,
                "projects/my-project/subscriptions/my-subscription"
            );
            assert_eq!(sorted(&r.ack_ids), test_ids(0..10));
            Ok(Response::from(()))
        });

        let leaser = DefaultLeaser::new(
            Arc::new(mock),
            confirmed_tx,
            "projects/my-project/subscriptions/my-subscription".to_string(),
            10,
            16_usize,
        );
        leaser.confirmed_ack(test_ids(0..10)).await;

        let confirmed_acks = confirmed_rx.recv().await.expect("results were not sent");

        // Verify all ack IDs have a result.
        let ack_ids: Vec<_> = confirmed_acks.keys().cloned().collect();
        assert_eq!(sorted(&ack_ids), test_ids(0..10));

        // Verify all acks were successful.
        for (ack_id, result) in &confirmed_acks {
            assert!(
                result.is_ok(),
                "Expected success for {ack_id}, got {result:?}"
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn confirmed_ack_failure() -> anyhow::Result<()> {
        let (confirmed_tx, mut confirmed_rx) = unbounded_channel();
        let mut mock = MockStub::new();
        mock.expect_acknowledge().times(1).return_once(|r, _| {
            assert_eq!(
                r.subscription,
                "projects/my-project/subscriptions/my-subscription"
            );
            assert_eq!(sorted(&r.ack_ids), test_ids(0..10));
            Err(Error::service(
                Status::default()
                    .set_code(Code::FailedPrecondition)
                    .set_message("fail"),
            ))
        });

        let leaser = DefaultLeaser::new(
            Arc::new(mock),
            confirmed_tx,
            "projects/my-project/subscriptions/my-subscription".to_string(),
            10,
            16_usize,
        );
        leaser.confirmed_ack(test_ids(0..10)).await;

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
    async fn confirmed_ack_partial_transient_failure_retry_failure() -> anyhow::Result<()> {
        let (confirmed_tx, mut confirmed_rx) = unbounded_channel();
        let mut mock = MockStub::new();
        let mut seq = Sequence::new();

        let info = ErrorInfo::new().set_metadata([(test_id(1), "TRANSIENT_FAILURE_OTHER")]);
        let err = response_with_error_info(vec![info.clone()]).unwrap_err();

        mock.expect_acknowledge()
            .times(1)
            .in_sequence(&mut seq)
            .return_once(move |r, _o| {
                assert_eq!(sorted(&r.ack_ids), test_ids(1..3));
                Err(err)
            });

        mock.expect_acknowledge()
            .times(1)
            .in_sequence(&mut seq)
            .return_once(move |r, _o| {
                assert_eq!(r.ack_ids, vec![test_id(1)]);
                Err(crate::Error::service(
                    Status::default()
                        .set_code(Code::FailedPrecondition)
                        .set_message("non-retryable failure"),
                ))
            });

        let mut mock_backoff = MockBackoffPolicy::new();
        mock_backoff
            .expect_on_failure()
            .times(1)
            .return_const(std::time::Duration::ZERO);

        let leaser = DefaultLeaser::new_with_backoff(
            Arc::new(mock),
            confirmed_tx,
            "projects/my-project/subscriptions/my-subscription".to_string(),
            10,
            16_usize,
            Arc::new(mock_backoff),
        );
        leaser.confirmed_ack(test_ids(1..3)).await;

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
    async fn confirmed_ack_partial_transient_failure_retry_success() -> anyhow::Result<()> {
        let (confirmed_tx, mut confirmed_rx) = unbounded_channel();
        let mut mock = MockStub::new();
        let mut seq = Sequence::new();

        let info = ErrorInfo::new().set_metadata([(test_id(1), "TRANSIENT_FAILURE_OTHER")]);
        let err = response_with_error_info(vec![info]).unwrap_err();

        mock.expect_acknowledge()
            .times(1)
            .in_sequence(&mut seq)
            .return_once(move |r, _o| {
                assert_eq!(sorted(&r.ack_ids), test_ids(1..3));
                Err(err)
            });

        mock.expect_acknowledge()
            .times(1)
            .in_sequence(&mut seq)
            .return_once(move |r, _o| {
                assert_eq!(r.ack_ids, vec![test_id(1)]);
                Ok(Response::from(()))
            });

        let mut mock_backoff = MockBackoffPolicy::new();
        mock_backoff
            .expect_on_failure()
            .times(1)
            .return_const(Duration::ZERO);

        let leaser = DefaultLeaser::new_with_backoff(
            Arc::new(mock),
            confirmed_tx,
            "projects/my-project/subscriptions/my-subscription".to_string(),
            10,
            16_usize,
            Arc::new(mock_backoff),
        );
        leaser.confirmed_ack(test_ids(1..3)).await;

        let confirmed_acks = confirmed_rx.recv().await.expect("results were not sent");
        let expected = [(test_id(2), Ok(()))].into_iter().collect();
        assert_eq!(confirmed_acks, expected);

        let confirmed_acks_final = confirmed_rx.recv().await.expect("results were not sent");
        let expected_final = [(test_id(1), Ok(()))].into_iter().collect();
        assert_eq!(confirmed_acks_final, expected_final);

        Ok(())
    }

    #[tokio::test]
    async fn confirmed_ack_partial_permanent_failure() -> anyhow::Result<()> {
        let (confirmed_tx, mut confirmed_rx) = unbounded_channel();
        let mut mock = MockStub::new();

        let info =
            ErrorInfo::new().set_metadata([(test_id(1), "PERMANENT_FAILURE_INVALID_ACK_ID")]);
        let err = response_with_error_info(vec![info.clone()]).unwrap_err();

        mock.expect_acknowledge()
            .times(1)
            .return_once(move |r, _o| {
                assert_eq!(sorted(&r.ack_ids), test_ids(1..3));
                Err(err)
            });

        let leaser = DefaultLeaser::new(
            Arc::new(mock),
            confirmed_tx,
            "projects/my-project/subscriptions/my-subscription".to_string(),
            10,
            16_usize,
        );
        leaser.confirmed_ack(test_ids(1..3)).await;

        let confirmed_acks = confirmed_rx.recv().await.expect("results were not sent");

        let err = AckError::Rpc {
            source: Arc::new(response_with_error_info(vec![info]).unwrap_err()),
        };
        let expected = [(test_id(1), Err(err)), (test_id(2), Ok(()))]
            .into_iter()
            .collect();
        assert_eq!(confirmed_acks, expected);

        Ok(())
    }

    #[test_case(Code::DeadlineExceeded)]
    #[test_case(Code::ResourceExhausted)]
    #[test_case(Code::Aborted)]
    #[test_case(Code::Internal)]
    #[test_case(Code::Unavailable)]
    #[tokio::test]
    async fn process_ack_attempt_error_retryable_code_without_error_info(
        code: Code,
    ) -> anyhow::Result<()> {
        let err = Arc::new(Error::service(
            Status::default()
                .set_code(code)
                .set_message("retryable error"),
        ));
        let (confirmed_acks, remaining) = process_ack_attempt_error(test_ids(1..3), err);

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
    async fn process_ack_attempt_error_retryable_code_with_error_info(
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
        let (confirmed_acks, remaining) = process_ack_attempt_error(test_ids(1..4), err.clone());

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
    async fn process_ack_attempt_error_non_retryable_code_without_error_info(
        code: Code,
    ) -> anyhow::Result<()> {
        let err = Arc::new(Error::service(
            Status::default()
                .set_code(code)
                .set_message("non-retryable error"),
        ));
        let (confirmed_acks, remaining) = process_ack_attempt_error(test_ids(1..3), err);

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
    async fn process_ack_attempt_error_non_retryable_code_permanent_failure(
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
        let (confirmed_acks, remaining) = process_ack_attempt_error(test_ids(1..3), err);

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
    async fn process_ack_attempt_error_non_retryable_code_transient_failure(
        code: Code,
    ) -> anyhow::Result<()> {
        let info = ErrorInfo::new().set_metadata([(test_id(1), "TRANSIENT_FAILURE_OTHER")]);

        let err = Arc::new(Error::service(
            Status::default()
                .set_code(code)
                .set_message("non-retryable error")
                .set_details([StatusDetails::ErrorInfo(info)]),
        ));
        let (confirmed_acks, remaining) = process_ack_attempt_error(test_ids(1..3), err);

        assert_eq!(remaining, test_ids(1..2));

        let expected = [(test_id(2), Ok(()))].into_iter().collect();
        assert_eq!(confirmed_acks, expected);

        Ok(())
    }
}
