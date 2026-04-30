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

use super::exactly_once_retry::exactly_once_retry_loop;
use super::handler::AckResult;
use super::retry_policy::{StreamRetryPolicy, exactly_once_options, rpc_options};
use super::stub::Stub;
use crate::RequestOptions;
use crate::model::{AcknowledgeRequest, ModifyAckDeadlineRequest};
use google_cloud_gax::backoff_policy::BackoffPolicy;
use google_cloud_gax::exponential_backoff::ExponentialBackoff;
use google_cloud_gax::retry_policy::RetryPolicy;
use google_cloud_gax::retry_throttler::{CircuitBreaker, SharedRetryThrottler};
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
    async fn extend(&self, ack_ids: Vec<String>) -> Vec<String>;

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

    async fn extend(&self, ack_ids: Vec<String>) -> Vec<String> {
        let req = ModifyAckDeadlineRequest::new()
            .set_subscription(self.subscription.clone())
            .set_ack_ids(ack_ids.clone())
            .set_ack_deadline_seconds(self.ack_deadline_seconds);
        let response = self
            .inner
            .modify_ack_deadline(req, self.options.clone())
            .await;
        if response.is_ok() {
            ack_ids
        } else {
            Vec::new()
        }
    }

    async fn confirmed_ack(&self, ack_ids: Vec<String>) {
        let leaser = self.clone();
        let options = self.options.clone();
        let inner = async move |ids: Vec<String>| {
            let req = AcknowledgeRequest::new()
                .set_subscription(leaser.subscription.clone())
                .set_ack_ids(ids);
            leaser
                .inner
                .acknowledge(req, options.clone())
                .await
                .map(|_| ())
        };
        exactly_once_retry_loop(
            ack_ids,
            inner,
            self.confirmed_tx.clone(),
            retry_throttler(&self.exactly_once_options),
            retry_policy(&self.exactly_once_options),
            self.backoff.clone(),
        )
        .await;
    }

    async fn confirmed_nack(&self, ack_ids: Vec<String>) {
        let leaser = self.clone();
        let options = self.options.clone();
        let inner = async move |ids: Vec<String>| {
            let req = ModifyAckDeadlineRequest::new()
                .set_subscription(leaser.subscription.clone())
                .set_ack_ids(ids)
                .set_ack_deadline_seconds(0);
            leaser
                .inner
                .modify_ack_deadline(req, options.clone())
                .await
                .map(|_| ())
        };
        // TODO(#4804): update retry policy time limit to max lease extension.
        exactly_once_retry_loop(
            ack_ids,
            inner,
            self.confirmed_tx.clone(),
            retry_throttler(&self.exactly_once_options),
            retry_policy(&self.exactly_once_options),
            self.backoff.clone(),
        )
        .await;
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

#[cfg(test)]
pub(super) mod tests {
    use super::super::lease_state::tests::{sorted, test_ids};
    use super::super::retry_policy::tests::verify_policies;
    use super::super::stub::tests::MockStub;
    use super::*;
    use crate::{Error, Response};
    use google_cloud_gax::error::rpc::{Code, Status};
    use google_cloud_gax::retry_state::RetryState;
    use std::time::Duration;
    use tokio::sync::Mutex;
    use tokio::sync::mpsc::unbounded_channel;

    mockall::mock! {
        #[derive(Debug)]
        pub(in super::super) Leaser {}
        #[async_trait::async_trait]
        impl Leaser for Leaser {
            async fn ack(&self, ack_ids: Vec<String>);
            async fn nack(&self, ack_ids: Vec<String>);
            async fn extend(&self, ack_ids: Vec<String>) -> Vec<String>;
            async fn confirmed_ack(&self, ack_ids: Vec<String>);
            async fn confirmed_nack(&self, ack_ids: Vec<String>);
        }
    }

    mockall::mock! {
        #[derive(Debug)]
        pub BackoffPolicy {}
        impl BackoffPolicy for BackoffPolicy {
            fn on_failure(&self, state: &RetryState) -> Duration;
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
        async fn extend(&self, ack_ids: Vec<String>) -> Vec<String> {
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
        async fn extend(&self, ack_ids: Vec<String>) -> Vec<String> {
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

    #[tokio::test]
    async fn ack() {
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

        let (confirmed_tx, _confirmed_rx) = unbounded_channel();
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

        let (confirmed_tx, _confirmed_rx) = unbounded_channel();
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

        let (confirmed_tx, _confirmed_rx) = unbounded_channel();
        let leaser = DefaultLeaser::new(
            Arc::new(mock),
            confirmed_tx,
            "projects/my-project/subscriptions/my-subscription".to_string(),
            10,
            16_usize,
        );
        let extended = leaser.extend(test_ids(0..10)).await;
        assert_eq!(extended, test_ids(0..10));
    }

    #[tokio::test]
    async fn extend_failure() {
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
                Err(Error::service(Status::default().set_code(Code::Internal)))
            });

        let (confirmed_tx, _confirmed_rx) = unbounded_channel();
        let leaser = DefaultLeaser::new(
            Arc::new(mock),
            confirmed_tx,
            "projects/my-project/subscriptions/my-subscription".to_string(),
            10,
            16_usize,
        );
        let extended = leaser.extend(test_ids(0..10)).await;
        assert!(extended.is_empty(), "{extended:?}");
    }

    #[tokio::test]
    async fn confirmed_ack() -> anyhow::Result<()> {
        let mut mock = MockStub::new();
        mock.expect_acknowledge().times(1).return_once(|r, o| {
            assert_eq!(
                r.subscription,
                "projects/my-project/subscriptions/my-subscription"
            );
            assert_eq!(sorted(&r.ack_ids), test_ids(0..10));
            verify_policies(o, 16);
            Ok(Response::from(()))
        });

        let (confirmed_tx, mut confirmed_rx) = unbounded_channel();
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
    async fn confirmed_ack_retry() -> anyhow::Result<()> {
        let mut mock = MockStub::new();
        let mut seq = mockall::Sequence::new();
        mock.expect_acknowledge()
            .once()
            .in_sequence(&mut seq)
            .return_once(|r, o| {
                assert_eq!(
                    r.subscription,
                    "projects/my-project/subscriptions/my-subscription"
                );
                assert_eq!(sorted(&r.ack_ids), test_ids(0..10));
                verify_policies(o, 16);
                Err(Error::service(
                    Status::default().set_code(Code::Unavailable),
                ))
            });
        mock.expect_acknowledge()
            .once()
            .in_sequence(&mut seq)
            .return_once(|r, o| {
                assert_eq!(
                    r.subscription,
                    "projects/my-project/subscriptions/my-subscription"
                );
                assert_eq!(sorted(&r.ack_ids), test_ids(0..10));
                verify_policies(o, 16);
                Ok(Response::from(()))
            });

        let mut mock_backoff = MockBackoffPolicy::new();
        mock_backoff
            .expect_on_failure()
            .once()
            .return_const(std::time::Duration::ZERO);

        let (confirmed_tx, mut confirmed_rx) = unbounded_channel();
        let leaser = DefaultLeaser::new_with_backoff(
            Arc::new(mock),
            confirmed_tx,
            "projects/my-project/subscriptions/my-subscription".to_string(),
            10,
            16_usize,
            Arc::new(mock_backoff),
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
    async fn confirmed_nack() -> anyhow::Result<()> {
        let mut mock = MockStub::new();
        mock.expect_modify_ack_deadline()
            .times(1)
            .return_once(|r, o| {
                assert_eq!(
                    r.subscription,
                    "projects/my-project/subscriptions/my-subscription"
                );
                assert_eq!(r.ack_deadline_seconds, 0);
                assert_eq!(sorted(&r.ack_ids), test_ids(0..10));
                verify_policies(o, 16);
                Ok(Response::from(()))
            });

        let (confirmed_tx, mut confirmed_rx) = unbounded_channel();
        let leaser = DefaultLeaser::new(
            Arc::new(mock),
            confirmed_tx,
            "projects/my-project/subscriptions/my-subscription".to_string(),
            10,
            16_usize,
        );
        leaser.confirmed_nack(test_ids(0..10)).await;

        let confirmed_nacks = confirmed_rx.recv().await.expect("results were not sent");

        // Verify all ids have a result.
        let ack_ids: Vec<_> = confirmed_nacks.keys().cloned().collect();
        assert_eq!(sorted(&ack_ids), test_ids(0..10));

        // Verify all nacks were successful.
        for (ack_id, result) in &confirmed_nacks {
            assert!(
                result.is_ok(),
                "Expected success for {ack_id}, got {result:?}"
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn confirmed_nack_retry() -> anyhow::Result<()> {
        let mut mock = MockStub::new();
        let mut seq = mockall::Sequence::new();
        mock.expect_modify_ack_deadline()
            .once()
            .in_sequence(&mut seq)
            .return_once(|r, o| {
                assert_eq!(
                    r.subscription,
                    "projects/my-project/subscriptions/my-subscription"
                );
                assert_eq!(r.ack_deadline_seconds, 0);
                assert_eq!(sorted(&r.ack_ids), test_ids(0..10));
                verify_policies(o, 16);
                Err(Error::service(
                    Status::default().set_code(Code::Unavailable),
                ))
            });
        mock.expect_modify_ack_deadline()
            .once()
            .in_sequence(&mut seq)
            .return_once(|r, o| {
                assert_eq!(
                    r.subscription,
                    "projects/my-project/subscriptions/my-subscription"
                );
                assert_eq!(r.ack_deadline_seconds, 0);
                assert_eq!(sorted(&r.ack_ids), test_ids(0..10));
                verify_policies(o, 16);
                Ok(Response::from(()))
            });

        let mut mock_backoff = MockBackoffPolicy::new();
        mock_backoff
            .expect_on_failure()
            .once()
            .return_const(std::time::Duration::ZERO);

        let (confirmed_tx, mut confirmed_rx) = unbounded_channel();
        let leaser = DefaultLeaser::new_with_backoff(
            Arc::new(mock),
            confirmed_tx,
            "projects/my-project/subscriptions/my-subscription".to_string(),
            10,
            16_usize,
            Arc::new(mock_backoff),
        );
        leaser.confirmed_nack(test_ids(0..10)).await;

        let confirmed_nacks = confirmed_rx.recv().await.expect("results were not sent");

        // Verify all ids have a result.
        let ack_ids: Vec<_> = confirmed_nacks.keys().cloned().collect();
        assert_eq!(sorted(&ack_ids), test_ids(0..10));

        // Verify all nacks were successful.
        for (ack_id, result) in &confirmed_nacks {
            assert!(
                result.is_ok(),
                "Expected success for {ack_id}, got {result:?}"
            );
        }
        Ok(())
    }
}
