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
use super::retry_policy::at_least_once_options;
use super::stub::Stub;
use crate::RequestOptions;
use crate::error::AckError;
use crate::model::{AcknowledgeRequest, ModifyAckDeadlineRequest};
use google_cloud_gax::exponential_backoff::ExponentialBackoff;
use google_cloud_gax::retry_loop_internal::retry_loop;
use google_cloud_gax::retry_policy::NeverRetry;
use google_cloud_gax::retry_throttler::CircuitBreaker;
use std::collections::HashMap;
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
    subscription: String,
    ack_deadline_seconds: i32,
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
            subscription: self.subscription.clone(),
            ack_deadline_seconds: self.ack_deadline_seconds,
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
        DefaultLeaser {
            inner,
            confirmed_tx,
            options: at_least_once_options(grpc_subchannel_count),
            subscription,
            ack_deadline_seconds,
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
        let leaser = self.clone();
        let mut ack_ids = ack_ids;

        let attempt = async move |_| {
            let ids = std::mem::take(&mut ack_ids);
            let ack_ids = leaser.confirmed_ack_attempt(ids).await;
            if ack_ids.is_empty() {
                Ok(())
            } else {
                // Return a synthetic error to indicate that we should retry.
                Err(crate::Error::timeout("retry me"))
            }
        };

        let sleep = async |d| tokio::time::sleep(d).await;
        let _ = retry_loop(
            attempt,
            sleep,
            true,
            retry_throttler(&self.options),
            retry_policy(),
            backoff_policy(),
        )
        .await;
        // TODO(#4804): Process the transient error after the final attempt to
        // propagate it back to the application.
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

fn retry_policy() -> Arc<NeverRetry> {
    // TODO(#4804): Update the retry_policy to retry for the following error
    // codes: [DeadlineExceeded, ResourceExhausted, Aborted, Internal, Unavailable].
    Arc::new(NeverRetry)
}

fn backoff_policy() -> Arc<ExponentialBackoff> {
    Arc::new(ExponentialBackoff::default())
}

fn retry_throttler(
    options: &RequestOptions,
) -> google_cloud_gax::retry_throttler::SharedRetryThrottler {
    options.retry_throttler().clone().unwrap_or_else(|| {
        // Effectively disable throttling. The stub throttles.
        Arc::new(Mutex::new(
            CircuitBreaker::new(1000, 0, 0).expect("This is a valid configuration"),
        ))
    })
}

impl<T> DefaultLeaser<T>
where
    T: Stub + 'static,
{
    async fn confirmed_ack_attempt(&self, ack_ids: Vec<String>) -> Vec<String> {
        let req = AcknowledgeRequest::new()
            .set_subscription(self.subscription.clone())
            .set_ack_ids(ack_ids.clone());
        let response = self.inner.acknowledge(req, self.options.clone()).await;

        let mut has_error_info = false;
        let mut transient_failures = std::collections::HashSet::new();
        let mut permanent_failures = std::collections::HashMap::new();
        if let Err(e) = &response {
            if let Some(status) = e.status() {
                for detail in &status.details {
                    if let google_cloud_gax::error::rpc::StatusDetails::ErrorInfo(info) = detail {
                        has_error_info = true;
                        let (transient, permanent) = Self::extract_failures(info);
                        transient_failures.extend(transient);
                        permanent_failures.extend(permanent);
                    }
                }
            }
        }

        // For transient_failures, we want to retry so do not send the result to
        // confirmed_tx.
        // For other failures, send the result to confirmed_tx.
        let shared_result = response.map(|_| ()).map_err(Arc::new);
        let confirmed_acks = ack_ids
            .into_iter()
            .filter(|id| !transient_failures.contains(id))
            .map(|id| {
                let result = if has_error_info {
                    if permanent_failures.contains_key(&id) {
                        shared_result
                            .clone()
                            .map_err(|source| AckError::Rpc { source })
                    } else {
                        // For responses with error info, successfully processed acknowledgement ids are not
                        // included in the ErrorInfo.
                        Ok(())
                    }
                } else {
                    // No error info in response, so all ack_ids share the same result.
                    shared_result
                        .clone()
                        .map_err(|source| AckError::Rpc { source })
                };
                (id, result)
            })
            .collect::<std::collections::HashMap<_, _>>();
        let _ = self.confirmed_tx.send(confirmed_acks);

        transient_failures.into_iter().collect()
    }

    fn extract_failures(
        info: &google_cloud_rpc::model::ErrorInfo,
    ) -> (
        std::collections::HashSet<String>,
        std::collections::HashMap<String, String>,
    ) {
        let mut transient = std::collections::HashSet::new();
        let mut permanent = std::collections::HashMap::new();
        for (k, v) in &info.metadata {
            if v.starts_with("TRANSIENT_FAILURE_") {
                transient.insert(k.clone());
            } else if v.starts_with("PERMANENT_FAILURE_") {
                permanent.insert(k.clone(), v.clone());
            }
        }
        (transient, permanent)
    }
}

#[cfg(test)]
pub(super) mod tests {
    use super::super::lease_state::tests::{sorted, test_ids};
    use super::super::retry_policy::tests::verify_policies;
    use super::super::stub::tests::MockStub;
    use super::*;
    use crate::{Error, Response};
    use google_cloud_gax::error::rpc::{Code, Status};
    use std::sync::Arc;
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
    fn test_extract_failures() {
        let mut info = google_cloud_rpc::model::ErrorInfo::default();
        info.reason = "reason".to_string();
        info.domain = "domain".to_string();
        info.metadata = [
            (
                "ack_1".to_string(),
                "TRANSIENT_FAILURE_UNORDERED_ACK_ID".to_string(),
            ),
            ("ack_2".to_string(), "GIBBERISH_IGNORE".to_string()),
            ("ack_3".to_string(), "TRANSIENT_FAILURE_OTHER".to_string()),
            (
                "ack_4".to_string(),
                "PERMANENT_FAILURE_INVALID_ACK_ID".to_string(),
            ),
            ("ack_5".to_string(), "PERMANENT_FAILURE_OTHER".to_string()),
        ]
        .into_iter()
        .collect();

        let (transient, permanent) = DefaultLeaser::<MockStub>::extract_failures(&info);
        let expected_transient: std::collections::HashSet<_> =
            vec!["ack_1".to_string(), "ack_3".to_string()]
                .into_iter()
                .collect();
        let expected_permanent: std::collections::HashMap<_, _> = [
            (
                "ack_4".to_string(),
                "PERMANENT_FAILURE_INVALID_ACK_ID".to_string(),
            ),
            ("ack_5".to_string(), "PERMANENT_FAILURE_OTHER".to_string()),
        ]
        .into_iter()
        .collect();

        assert_eq!(transient, expected_transient);
        assert_eq!(permanent, expected_permanent);
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
        mock.expect_acknowledge().times(1).return_once(|r, o| {
            assert_eq!(
                r.subscription,
                "projects/my-project/subscriptions/my-subscription"
            );
            assert_eq!(r.ack_ids, test_ids(0..10));
            verify_policies(o, 16);
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
    async fn confirmed_ack_attempt_success() -> anyhow::Result<()> {
        let (confirmed_tx, mut confirmed_rx) = unbounded_channel();
        let mut mock = MockStub::new();
        mock.expect_acknowledge()
            .times(1)
            .return_once(|_, _| Ok(Response::from(())));

        let leaser = DefaultLeaser::new(
            Arc::new(mock),
            confirmed_tx,
            "projects/my-project/subscriptions/my-subscription".to_string(),
            10,
            1_usize,
        );
        let ack_ids = leaser
            .confirmed_ack_attempt(vec!["ack_1".to_string(), "ack_2".to_string()])
            .await;

        assert!(ack_ids.is_empty());

        let confirmed_acks = confirmed_rx.recv().await.expect("results were not sent");
        assert_eq!(confirmed_acks.len(), 2);
        assert!(confirmed_acks.get("ack_1").unwrap().is_ok());
        assert!(confirmed_acks.get("ack_2").unwrap().is_ok());

        Ok(())
    }

    #[tokio::test]
    async fn confirmed_ack_attempt_permanent_failure() -> anyhow::Result<()> {
        let (confirmed_tx, mut confirmed_rx) = unbounded_channel();
        let mut mock = MockStub::new();

        let mut info = google_cloud_rpc::model::ErrorInfo::default();
        info.metadata = [(
            "ack_1".to_string(),
            "PERMANENT_FAILURE_INVALID_ACK_ID".to_string(),
        )]
        .into_iter()
        .collect();

        mock.expect_acknowledge().times(1).return_once(move |_, _| {
            Err(Error::service(
                Status::default()
                    .set_code(Code::FailedPrecondition)
                    .set_message("fail")
                    .set_details(vec![
                        google_cloud_gax::error::rpc::StatusDetails::ErrorInfo(info),
                    ]),
            ))
        });

        let leaser = DefaultLeaser::new(
            Arc::new(mock),
            confirmed_tx,
            "projects/my-project/subscriptions/my-subscription".to_string(),
            10,
            1_usize,
        );
        let ack_ids = leaser
            .confirmed_ack_attempt(vec!["ack_1".to_string(), "ack_2".to_string()])
            .await;

        assert!(ack_ids.is_empty());

        let confirmed_acks = confirmed_rx.recv().await.expect("results were not sent");
        assert_eq!(confirmed_acks.len(), 2);

        let result_1 = confirmed_acks
            .get("ack_1")
            .expect("ack_1 should be present");
        match result_1 {
            Err(AckError::Rpc { source }) => {
                let status = source.status().expect("RPC source should have a status");
                assert_eq!(status.code, Code::FailedPrecondition);
            }
            _ => panic!("Expected AckError::Rpc, got {:?}", result_1),
        }

        let result_2 = confirmed_acks
            .get("ack_2")
            .expect("ack_2 should be present");
        assert!(
            result_2.is_ok(),
            "Expected Ok for ack_2, got {:?}",
            result_2
        );

        Ok(())
    }

    #[tokio::test]
    async fn confirmed_ack_attempt_transient_failure() -> anyhow::Result<()> {
        let (confirmed_tx, mut confirmed_rx) = unbounded_channel();
        let mut mock = MockStub::new();

        let mut info = google_cloud_rpc::model::ErrorInfo::default();
        info.metadata = [("ack_1".to_string(), "TRANSIENT_FAILURE_OTHER".to_string())]
            .into_iter()
            .collect();

        mock.expect_acknowledge().times(1).return_once(move |_, _| {
            Err(Error::service(
                Status::default()
                    .set_code(Code::FailedPrecondition)
                    .set_message("fail")
                    .set_details(vec![
                        google_cloud_gax::error::rpc::StatusDetails::ErrorInfo(info),
                    ]),
            ))
        });

        let leaser = DefaultLeaser::new(
            Arc::new(mock),
            confirmed_tx,
            "projects/my-project/subscriptions/my-subscription".to_string(),
            10,
            1_usize,
        );
        let ack_ids = leaser
            .confirmed_ack_attempt(vec!["ack_1".to_string(), "ack_2".to_string()])
            .await;

        assert_eq!(ack_ids.len(), 1);
        assert_eq!(ack_ids[0], "ack_1");

        let confirmed_acks = confirmed_rx.recv().await.expect("results were not sent");
        assert_eq!(confirmed_acks.len(), 1); // Only ack_2 should be sent

        let result_2 = confirmed_acks
            .get("ack_2")
            .expect("ack_2 should be present");
        assert!(
            result_2.is_ok(),
            "Expected Ok for ack_2, got {:?}",
            result_2
        );

        Ok(())
    }
}
