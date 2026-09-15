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

use google_cloud_gax::options::RequestOptionsBuilder;
use google_cloud_gax::retry_policy::{NeverRetry, RetryPolicyExt};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

use super::token_bucket::TokenBucket;
use crate::error::PublishError;
use crate::generated::gapic_dataplane::client::Publisher as GapicPublisher;
use crate::model::PublishResponse;
use crate::publisher::actor::batch_resolve_publish_futures;

type BatchSenders = Vec<oneshot::Sender<Result<String, PublishError>>>;

/// Shared state for a single publish batch across initial and hedged RPC attempts.
///
/// Only the first attempt to finish (either initial or hedged) resolves the batch's
/// pending `PublishFuture` channels and triggers cancellation of any remaining in-flight
/// attempts for this batch.
pub(crate) struct BatchState {
    pub msgs: Arc<Vec<crate::model::Message>>,
    /// Protected senders for the batch: user response channels and an internal `done_tx`
    /// signal for the actor's inflight `JoinSet`. Taken atomically by the winning attempt.
    pub txs: Mutex<
        Option<(
            BatchSenders,
            tokio::sync::oneshot::Sender<crate::Result<()>>,
        )>,
    >,
    pub client: GapicPublisher,
    pub topic: String,
    pub token_bucket: Arc<TokenBucket>,
    /// Cancellation token shared across all attempts (initial and hedged) for this batch.
    /// Triggered as soon as any attempt succeeds (or the initial attempt fails permanently).
    pub cancel_token: CancellationToken,
}

impl BatchState {
    pub(crate) fn new(
        msgs: Vec<crate::model::Message>,
        txs: BatchSenders,
        client: GapicPublisher,
        topic: String,
        token_bucket: Arc<TokenBucket>,
        done_tx: tokio::sync::oneshot::Sender<crate::Result<()>>,
    ) -> Self {
        Self {
            msgs: Arc::new(msgs),
            txs: Mutex::new(Some((txs, done_tx))),
            client,
            topic,
            token_bucket,
            cancel_token: CancellationToken::new(),
        }
    }

    /// Sends the initial publish attempt with standard retry policy.
    ///
    /// If a hedged attempt finishes first, `cancel_token` is cancelled and drops
    /// the in-flight request, cancelling it.
    pub(crate) async fn send_initial(&self) {
        let request = self
            .client
            .publish()
            .set_topic(self.topic.clone())
            .set_messages((*self.msgs).clone());

        tokio::select! {
            _ = self.cancel_token.cancelled() => {}
            res = request.send() => {
                self.complete(res);
            }
        }
    }

    /// Sends a hedged publish attempt with `NeverRetry`.
    ///
    /// Errors from hedged attempts are ignored so they never fail the batch.
    /// If the hedged attempt succeeds first, it completes the batch and cancels
    /// the slower initial attempt.
    pub(crate) async fn send_hedged_rpc(&self) {
        if self.cancel_token.is_cancelled() {
            return;
        }

        // TODO(#6776): clamp the timeout to the remaining time of the initial request.
        let timeout = Duration::from_secs(10);

        let request = self
            .client
            .publish()
            .set_topic(self.topic.clone())
            .set_messages((*self.msgs).clone())
            .with_retry_policy(NeverRetry.with_time_limit(timeout));

        tokio::select! {
            _ = self.cancel_token.cancelled() => {}
            res = request.send() => {
                if let Ok(resp) = res {
                    self.complete(Ok(resp));
                }
            }
        }
    }

    /// Completes the batch if not already resolved:
    /// - Atomically takes ownership of `txs` (guaranteeing single-winner semantics).
    /// - Cancels all remaining in-flight attempts via `cancel_token`.
    /// - Refills the `TokenBucket` if the response was successful.
    /// - Resolves user `PublishFuture` channels and fires the `done_tx` signal.
    fn complete(&self, resp: crate::Result<PublishResponse>) {
        let mut lock = self.txs.lock().unwrap();
        if let Some((txs, done_tx)) = lock.take() {
            self.cancel_token.cancel();
            if resp.is_ok() {
                self.token_bucket.refill();
            }
            let _ = done_tx.send(batch_resolve_publish_futures(resp, txs));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::Message;
    use google_cloud_test_macros::tokio_test_no_panics;

    mockall::mock! {
        #[derive(Debug)]
        GapicPublisher {}
        impl crate::generated::gapic_dataplane::stub::Publisher for GapicPublisher {
            async fn publish(&self, req: crate::model::PublishRequest, _options: crate::RequestOptions) -> crate::Result<crate::Response<crate::model::PublishResponse>>;
        }
    }

    mockall::mock! {
        #[derive(Debug)]
        GapicPublisherWithFuture {}
        impl crate::generated::gapic_dataplane::stub::Publisher for GapicPublisherWithFuture {
            fn publish(&self, req: crate::model::PublishRequest, _options: google_cloud_gax::options::RequestOptions) -> impl Future<Output=google_cloud_gax::Result<google_cloud_gax::response::Response<crate::model::PublishResponse>>> + Send;
        }
    }

    fn mock_publish_response(msg_id: &str) -> crate::Result<crate::Response<PublishResponse>> {
        Ok(crate::Response::from(
            PublishResponse::new().set_message_ids([msg_id.to_string()]),
        ))
    }

    #[allow(clippy::type_complexity)]
    fn test_batch_state(
        client: GapicPublisher,
        token_bucket: Arc<TokenBucket>,
    ) -> (
        Arc<BatchState>,
        oneshot::Receiver<Result<String, PublishError>>,
        oneshot::Receiver<crate::Result<()>>,
    ) {
        let (tx, rx) = oneshot::channel();
        let (done_tx, done_rx) = oneshot::channel();
        let state = Arc::new(BatchState::new(
            vec![Message::new().set_data("test")],
            vec![tx],
            client,
            "topic".to_string(),
            token_bucket,
            done_tx,
        ));
        (state, rx, done_rx)
    }

    #[tokio_test_no_panics(start_paused = true)]
    async fn test_initial_succeeds_fast() -> anyhow::Result<()> {
        let mut mock = MockGapicPublisher::new();
        mock.expect_publish()
            .return_once(|_, _| mock_publish_response("msg-initial"));

        let client = GapicPublisher::from_stub(mock);
        let token_bucket = Arc::new(TokenBucket::new(10, 0.1));
        let (state, rx, done_rx) = test_batch_state(client, token_bucket);

        state.send_initial().await;
        let msg_id = rx.await??;
        assert_eq!(msg_id, "msg-initial");
        assert!(state.cancel_token.is_cancelled());
        assert!(done_rx.await.is_ok_and(|r| r.is_ok()));

        Ok(())
    }

    #[tokio_test_no_panics(start_paused = true)]
    async fn test_initial_fails_option_1() -> anyhow::Result<()> {
        let mut mock = MockGapicPublisher::new();
        mock.expect_publish().return_once(|_, _| {
            Err(crate::Error::io(std::io::Error::other(
                "fatal network error",
            )))
        });

        let client = GapicPublisher::from_stub(mock);
        let token_bucket = Arc::new(TokenBucket::new(10, 0.1));
        let (state, rx, done_rx) = test_batch_state(client, token_bucket);

        state.send_initial().await;
        let publish_res = rx.await?;
        assert!(publish_res.is_err());
        assert!(state.cancel_token.is_cancelled());
        let done_res = done_rx.await?;
        assert!(done_res.is_err());

        Ok(())
    }

    #[tokio_test_no_panics(start_paused = true)]
    async fn test_cancellation_when_initial_finishes_first() -> anyhow::Result<()> {
        let mut mock = MockGapicPublisherWithFuture::new();

        // Initial succeeds after 50ms
        mock.expect_publish().times(1).returning(|_, _| {
            Box::pin(async {
                tokio::time::sleep(Duration::from_millis(50)).await;
                mock_publish_response("msg-initial")
            })
        });

        let client = GapicPublisher::from_stub(mock);
        let token_bucket = Arc::new(TokenBucket::new(10, 0.1));
        let (state, rx, done_rx) = test_batch_state(client, token_bucket);

        assert!(!state.cancel_token.is_cancelled());
        state.send_initial().await;
        assert!(state.cancel_token.is_cancelled());

        let msg_id = rx.await??;
        assert_eq!(msg_id, "msg-initial");
        assert!(state.cancel_token.is_cancelled());
        assert!(done_rx.await.is_ok_and(|r| r.is_ok()));

        Ok(())
    }

    #[tokio_test_no_panics(start_paused = true)]
    async fn test_cancellation_when_hedged_succeeds_first() -> anyhow::Result<()> {
        let mut mock = MockGapicPublisherWithFuture::new();

        // Initial hangs for 10s
        mock.expect_publish().times(1).returning(|_, _| {
            Box::pin(async {
                tokio::time::sleep(Duration::from_secs(10)).await;
                mock_publish_response("msg-initial")
            })
        });

        // Hedged succeeds after 20ms
        mock.expect_publish().times(1).returning(|_, _| {
            Box::pin(async {
                tokio::time::sleep(Duration::from_millis(20)).await;
                mock_publish_response("msg-hedged")
            })
        });

        let client = GapicPublisher::from_stub(mock);
        let token_bucket = Arc::new(TokenBucket::new(10, 0.1));
        let (state, rx, done_rx) = test_batch_state(client, token_bucket);

        let initial_handle = tokio::spawn({
            let state = state.clone();
            async move {
                state.send_initial().await;
            }
        });

        let hedged_handle = tokio::spawn({
            let state = state.clone();
            async move {
                state.send_hedged_rpc().await;
            }
        });

        let _ = hedged_handle.await;
        // Upon hedged completion, cancel_token was cancelled so initial finishes quickly
        assert!(state.cancel_token.is_cancelled());
        initial_handle.await?;

        let msg_id = rx.await??;
        assert_eq!(msg_id, "msg-hedged");
        assert!(state.cancel_token.is_cancelled());
        assert!(done_rx.await.is_ok_and(|r| r.is_ok()));

        Ok(())
    }

    #[tokio_test_no_panics(start_paused = true)]
    async fn test_hedged_errors_ignored() -> anyhow::Result<()> {
        let mut mock = MockGapicPublisherWithFuture::new();

        // Initial hangs for 10s
        mock.expect_publish().times(1).returning(|_, _| {
            Box::pin(async {
                tokio::time::sleep(Duration::from_secs(10)).await;
                mock_publish_response("msg-initial")
            })
        });

        // Hedged fails immediately
        mock.expect_publish().times(1).returning(|_, _| {
            Box::pin(async {
                Err(crate::Error::io(std::io::Error::other(
                    "fatal network error",
                )))
            })
        });

        let client = GapicPublisher::from_stub(mock);
        let token_bucket = Arc::new(TokenBucket::new(10, 0.1));
        let (state, rx, done_rx) = test_batch_state(client, token_bucket);

        let initial_handle = tokio::spawn({
            let state = state.clone();
            async move {
                state.send_initial().await;
            }
        });

        let hedged_handle = tokio::spawn({
            let state = state.clone();
            async move {
                state.send_hedged_rpc().await;
            }
        });

        let _ = hedged_handle.await;
        // Upon hedged completion, cancel_token should not be cancelled.
        assert!(!state.cancel_token.is_cancelled());
        initial_handle.await?;

        let msg_id = rx.await??;
        assert_eq!(msg_id, "msg-initial");
        assert!(state.cancel_token.is_cancelled());
        assert!(done_rx.await.is_ok_and(|r| r.is_ok()));

        Ok(())
    }
}
