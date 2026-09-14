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
use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinSet;
use tokio::time::Sleep;
use tokio_util::sync::CancellationToken;

use super::actor::batch_resolve_publish_futures;
use super::options::HedgingOptions;
use super::token_bucket::TokenBucket;
use crate::error::PublishError;
use crate::generated::gapic_dataplane::client::Publisher as GapicPublisher;
use crate::model::PublishResponse;

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

/// An entry in the queue of scheduled hedges.
struct HedgeItem {
    state: Arc<BatchState>,
    deadline: tokio::time::Instant,
}

/// Handle held by ConcurrentBatchActor to submit batches to the dedicated scheduler task.
#[derive(Debug, Clone)]
pub(crate) struct HedgingSchedulerHandle {
    pub tx: mpsc::UnboundedSender<Arc<BatchState>>,
    pub token_bucket: Arc<TokenBucket>,
}

impl HedgingSchedulerHandle {
    /// Dispatches a batch to the network and registers it with the hedging scheduler.
    ///
    /// 1. Immediately spawns `send_initial()` on a separate task.
    /// 2. Spawns `done_rx` into the actor's `inflight` set so `flush()`/shutdown waits for the batch to resolve.
    /// 3. Registers `state` with the `HedgingScheduler` to schedule hedged attempts if the initial request is slow.
    pub(crate) fn dispatch(
        &self,
        msgs: Vec<crate::model::Message>,
        txs: Vec<oneshot::Sender<Result<String, PublishError>>>,
        client: GapicPublisher,
        topic: String,
        inflight: &mut JoinSet<crate::Result<()>>,
    ) {
        let (done_tx, done_rx) = oneshot::channel();
        let state = Arc::new(BatchState::new(
            msgs,
            txs,
            client,
            topic,
            self.token_bucket.clone(),
            done_tx,
        ));
        inflight.spawn(async move {
            let res = done_rx.await.map_err(crate::Error::io)?; // forward errors if the oneshot was dropped.
            res
        });
        // Send the initial RPC immediately instead of sending over the channel.
        let state_clone = state.clone();
        tokio::spawn(async move {
            state_clone.send_initial().await;
        });
        let _ = self.tx.send(state);
    }
}

/// Dedicated background task that manages scheduled hedges using a FIFO queue.
///
/// A simple `VecDeque` preserves chronological deadline order because all batches
/// share the same configured hedging `delay`, and rescheduling an expired hedge
/// schedules the next attempt at `now + delay` (which is guaranteed to be in the
/// future of all currently queued entries).
pub(crate) struct HedgingScheduler {
    rx: mpsc::UnboundedReceiver<Arc<BatchState>>,
    queue: VecDeque<HedgeItem>,
    delay: Duration,
    timer: Option<Pin<Box<Sleep>>>,
}

impl HedgingScheduler {
    pub(crate) fn new(rx: mpsc::UnboundedReceiver<Arc<BatchState>>, delay: Duration) -> Self {
        Self {
            rx,
            queue: VecDeque::new(),
            delay,
            timer: None,
        }
    }

    /// Spawns the background scheduler event loop and returns the handle for the actor.
    pub(crate) fn spawn(opts: HedgingOptions) -> HedgingSchedulerHandle {
        let token_bucket = Arc::new(TokenBucket::new(opts.max_tokens, opts.refill_ratio));
        let (tx, rx) = mpsc::unbounded_channel();
        let scheduler = Self::new(rx, opts.delay);
        tokio::spawn(scheduler.run());
        HedgingSchedulerHandle { tx, token_bucket }
    }

    /// Enqueues a new batch state and arms the sleep timer if the queue was previously empty.
    ///
    /// Newly incoming batches are always pushed to the back because their deadline
    /// (`now + delay`) is monotonically non-decreasing relative to previously queued items.
    fn handle_new_batch(&mut self, state: Arc<BatchState>) {
        let deadline = tokio::time::Instant::now() + self.delay;
        self.queue.push_back(HedgeItem { state, deadline });
        if self.timer.is_none() {
            self.timer = Some(Box::pin(tokio::time::sleep_until(deadline)));
        }
    }

    fn handle_expired_hedges(&mut self) {
        let now = tokio::time::Instant::now();
        while let Some(item) = self.queue.front() {
            if item.deadline <= now {
                let item = self.queue.pop_front().unwrap();
                // If the batch has already resolved or cannot acquire a token from the bucket,
                // the hedge is discarded and not rescheduled to avoid sending excessive requests
                // during sustained backend latency or errors.
                if !item.state.cancel_token.is_cancelled() && item.state.token_bucket.try_acquire()
                {
                    let state = item.state.clone();
                    tokio::spawn(async move {
                        state.send_hedged_rpc().await;
                    });
                    // Only schedule the next hedged attempt if this attempt acquired a token.
                    // Rescheduled at `now + delay`, which places it chronologically at the back.
                    self.queue.push_back(HedgeItem {
                        deadline: now + self.delay,
                        state: item.state,
                    });
                }
            } else {
                break;
            }
        }
        self.timer = self
            .queue
            .front()
            .map(|item| Box::pin(tokio::time::sleep_until(item.deadline)));
    }

    pub(crate) async fn run(mut self) {
        loop {
            tokio::select! {
                // Receive new batches to hedge
                item = self.rx.recv() => {
                    match item {
                        Some(state) => self.handle_new_batch(state),
                        None => {
                            // Actor dropped sender.
                            break;
                        }
                    }
                }
                // Earliest scheduled hedge deadline expired
                _ = async { self.timer.as_mut().unwrap().await }, if self.timer.is_some() => {
                    self.handle_expired_hedges();
                }
            }
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
    async fn test_hedged_succeeds_when_initial_hangs() -> anyhow::Result<()> {
        let mut mock = MockGapicPublisherWithFuture::new();

        // Initial hangs indefinitely
        mock.expect_publish().times(1).returning(|_, _| {
            Box::pin(async {
                tokio::time::sleep(Duration::from_secs(60)).await;
                mock_publish_response("msg-initial")
            })
        });

        // Hedged attempt succeeds quickly
        mock.expect_publish()
            .times(1)
            .returning(|_, _| Box::pin(async { mock_publish_response("msg-hedged") }));

        let client = GapicPublisher::from_stub(mock);
        let opts = HedgingOptions {
            delay: Duration::from_millis(100),
            max_tokens: 10,
            refill_ratio: 0.1,
        };
        let scheduler_handle = HedgingScheduler::spawn(opts);
        let token_bucket = scheduler_handle.token_bucket.clone();
        // Refill 10 times (10 * 100 = 1000 units = 1 full token) so a hedge can be acquired
        for _ in 0..10 {
            token_bucket.refill();
        }

        let (state, rx, done_rx) = test_batch_state(client, token_bucket);

        let state_clone = state.clone();
        tokio::spawn(async move {
            let _ = state_clone.send_initial().await;
        });
        scheduler_handle.tx.send(state.clone())?;

        // Advance time by 150ms to trigger hedge
        tokio::time::advance(Duration::from_millis(150)).await;
        tokio::task::yield_now().await;

        let msg_id = rx.await??;
        assert_eq!(msg_id, "msg-hedged");
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
    async fn test_scheduler_shuts_down_promptly_when_batch_done() -> anyhow::Result<()> {
        let mock = MockGapicPublisher::new();
        let client = GapicPublisher::from_stub(mock);
        let token_bucket = Arc::new(TokenBucket::new(10, 0.1));
        let (tx, rx) = mpsc::unbounded_channel();
        let scheduler = HedgingScheduler::new(rx, Duration::from_secs(10));
        let task = tokio::spawn(scheduler.run());

        let (state, _rx, done_rx) = test_batch_state(client, token_bucket);

        // Mark batch done immediately
        state.complete(Ok(
            PublishResponse::new().set_message_ids(["msg".to_string()])
        ));
        assert!(state.cancel_token.is_cancelled());
        assert!(done_rx.await.is_ok_and(|r| r.is_ok()));

        tx.send(state)?;
        // Drop sender to signal shutdown
        drop(tx);

        // Task should finish immediately without sleeping for 10s
        tokio::time::timeout(Duration::from_millis(100), task).await??;

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
    async fn test_cancellation_when_hedged_finishes_first() -> anyhow::Result<()> {
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
    async fn test_multiple_hedged_attempts() -> anyhow::Result<()> {
        let mut mock = MockGapicPublisherWithFuture::new();

        // Initial hangs for 10s
        mock.expect_publish().times(1).returning(|_, _| {
            Box::pin(async {
                tokio::time::sleep(Duration::from_secs(10)).await;
                mock_publish_response("msg-initial")
            })
        });

        // 1st hedged attempt at t = 100ms hangs
        mock.expect_publish().times(1).returning(|_, _| {
            Box::pin(async {
                tokio::time::sleep(Duration::from_secs(10)).await;
                mock_publish_response("msg-hedged-1")
            })
        });

        // 2nd hedged attempt at t = 200ms succeeds immediately
        mock.expect_publish()
            .times(1)
            .returning(|_, _| Box::pin(async { mock_publish_response("msg-hedged-2") }));

        let client = GapicPublisher::from_stub(mock);
        let opts = HedgingOptions {
            delay: Duration::from_millis(100),
            max_tokens: 10,
            refill_ratio: 0.1,
        };
        let scheduler_handle = HedgingScheduler::spawn(opts);
        let token_bucket = scheduler_handle.token_bucket.clone();
        // Refill 20 times (2 tokens) so two hedges can be acquired
        for _ in 0..20 {
            token_bucket.refill();
        }

        let (state, rx, done_rx) = test_batch_state(client, token_bucket);

        let state_clone = state.clone();
        tokio::spawn(async move {
            state_clone.send_initial().await;
        });
        scheduler_handle.tx.send(state.clone())?;

        // Advance time to 250ms to trigger both hedges
        tokio::time::advance(Duration::from_millis(250)).await;
        tokio::task::yield_now().await;

        let msg_id = rx.await??;
        assert_eq!(msg_id, "msg-hedged-2");
        assert!(state.cancel_token.is_cancelled());
        assert!(done_rx.await.is_ok_and(|r| r.is_ok()));

        Ok(())
    }

    #[tokio_test_no_panics(start_paused = true)]
    async fn test_throttled_hedge_discarded_and_initial_completes() -> anyhow::Result<()> {
        let mut mock = MockGapicPublisherWithFuture::new();

        // Initial attempt succeeds after 200ms
        mock.expect_publish().times(1).returning(|_, _| {
            Box::pin(async {
                tokio::time::sleep(Duration::from_millis(200)).await;
                mock_publish_response("msg-initial")
            })
        });

        let client = GapicPublisher::from_stub(mock);
        let opts = HedgingOptions {
            delay: Duration::from_millis(100),
            max_tokens: 10,
            refill_ratio: 0.1,
        };
        let scheduler_handle = HedgingScheduler::spawn(opts);
        let token_bucket = scheduler_handle.token_bucket.clone();
        // Token bucket starts empty (0 tokens)

        let (state, rx, done_rx) = test_batch_state(client, token_bucket);

        let state_clone = state.clone();
        tokio::spawn(async move {
            state_clone.send_initial().await;
        });
        scheduler_handle.tx.send(state.clone())?;

        // At t = 100ms: scheduler ticks, throttled (no tokens), hedge is discarded
        tokio::time::advance(Duration::from_millis(100)).await;
        tokio::task::yield_now().await;
        assert!(!state.cancel_token.is_cancelled());

        // Advance to t = 250ms (initial attempt completes at 200ms)
        tokio::time::advance(Duration::from_millis(150)).await;
        tokio::task::yield_now().await;

        let msg_id = rx.await??;
        assert_eq!(msg_id, "msg-initial");
        assert!(state.cancel_token.is_cancelled());
        assert!(done_rx.await.is_ok_and(|r| r.is_ok()));

        Ok(())
    }

    #[tokio_test_no_panics(start_paused = true)]
    async fn test_failed_hedged_attempt_ignored() -> anyhow::Result<()> {
        let mut mock = MockGapicPublisherWithFuture::new();

        // Initial attempt returns success after 200ms
        mock.expect_publish().times(1).returning(|_, _| {
            Box::pin(async {
                tokio::time::sleep(Duration::from_millis(200)).await;
                mock_publish_response("msg-initial")
            })
        });

        // Hedged attempt at t = 100ms fails immediately
        mock.expect_publish().times(1).returning(|_, _| {
            Box::pin(async { Err(crate::Error::io(std::io::Error::other("hedged rpc failed"))) })
        });

        let client = GapicPublisher::from_stub(mock);
        let opts = HedgingOptions {
            delay: Duration::from_millis(100),
            max_tokens: 10,
            refill_ratio: 0.1,
        };
        let scheduler_handle = HedgingScheduler::spawn(opts);
        let token_bucket = scheduler_handle.token_bucket.clone();
        for _ in 0..10 {
            token_bucket.refill();
        }

        let (state, rx, done_rx) = test_batch_state(client, token_bucket);

        let state_clone = state.clone();
        tokio::spawn(async move {
            state_clone.send_initial().await;
        });
        scheduler_handle.tx.send(state.clone())?;

        // Advance time to 250ms (hedged fails at 100ms, initial succeeds at 200ms)
        tokio::time::advance(Duration::from_millis(250)).await;
        tokio::task::yield_now().await;

        let msg_id = rx.await??;
        assert_eq!(msg_id, "msg-initial");
        assert!(state.cancel_token.is_cancelled());
        assert!(done_rx.await.is_ok_and(|r| r.is_ok()));

        Ok(())
    }
}
