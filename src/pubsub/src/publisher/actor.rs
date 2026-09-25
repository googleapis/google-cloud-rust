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

use super::options::BatchingOptions;
use crate::error::PublishError;
use crate::generated::gapic_dataplane::client::Publisher as GapicPublisher;
use crate::model::{Message, PublishResponse};
use crate::publisher::batch::Batch;
use crate::publisher::hedging::{HedgingScheduler, HedgingSchedulerHandle};
use crate::publisher::options::HedgingOptions;
use std::collections::{HashMap, VecDeque};
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinSet;
use tokio::time::Sleep;
use tokio_util::task::JoinMap;

/// A command sent from the `Publisher` to the background Dispatcher actor.
pub(crate) enum ToDispatcher {
    /// A request to publish a single message.
    Publish(BundledMessage),
    /// A request to flush all outstanding messages.
    Flush(oneshot::Sender<()>),
    /// A request to resume publishing on an ordering key.
    ResumePublish(String),
}

/// A command sent from the Dispatcher to a batch actor.
enum ToBatchActor {
    /// A request to publish a single message.
    Publish(BundledMessage),
    /// A request to flush all outstanding messages.
    Flush(oneshot::Sender<()>),
    /// A request to resume publishing.
    ResumePublish(),
}

/// Object that is passed to the actor tasks over the
/// main channel. This represents a single message and the sender
/// half of the channel to resolve the [PublishFuture].
#[derive(Debug)]
pub(crate) struct BundledMessage {
    pub msg: Message,
    pub tx: oneshot::Sender<std::result::Result<String, crate::error::PublishError>>,
}

/// The Dispatcher runs in a background task and handles all Publisher operations
/// by dispatching it to BatchActors.
#[derive(Debug)]
pub(crate) struct Dispatcher {
    topic_name: String,
    client: GapicPublisher,
    batching_options: BatchingOptions,
    hedging_options: Option<HedgingOptions>,
    total_timeout: Option<std::time::Duration>,
    rx: mpsc::UnboundedReceiver<ToDispatcher>,
}

impl Dispatcher {
    pub(crate) fn new(
        topic_name: String,
        client: GapicPublisher,
        batching_options: BatchingOptions,
        hedging_options: Option<HedgingOptions>,
        total_timeout: Option<std::time::Duration>,
        rx: mpsc::UnboundedReceiver<ToDispatcher>,
    ) -> Self {
        Self {
            topic_name,
            client,
            rx,
            batching_options,
            hedging_options,
            total_timeout,
        }
    }

    fn spawn_actor(&mut self, key: String, tasks: &mut JoinMap<String, ()>) -> BatchActorHandle {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        if key.is_empty() {
            tasks.spawn(
                key,
                ConcurrentBatchActor::new(
                    self.topic_name.clone(),
                    self.client.clone(),
                    self.batching_options.clone(),
                    self.hedging_options.clone(),
                    self.total_timeout,
                    rx,
                )
                .run(),
            );
        } else {
            tasks.spawn(
                key,
                SequentialBatchActor::new(
                    self.topic_name.clone(),
                    self.client.clone(),
                    self.batching_options.clone(),
                    rx,
                )
                .run(),
            );
        }
        BatchActorHandle { sender: tx }
    }

    /// The main loop of the Dispatcher.
    ///
    /// This method continuously handles the following events:
    ///
    /// 1. A Publish command from the `Publisher` is dispatched to the BatchActor
    ///    for its ordering key.
    /// 2. A Flush command from the `Publisher` causes the Dispatcher to flush
    ///    all BatchActors and awaits its completion.
    /// 3. A ResumePublish command from the `Publisher` is dispatched to the BatchActor
    ///    for its ordering key.
    ///
    /// Each batch actor owns its own lazy flush timer (see `ConcurrentBatchActor`
    /// and `SequentialBatchActor`).
    ///
    /// The loop terminates when the `rx` channel is closed, which happens when all
    /// `Publisher` clones have been dropped.
    pub(crate) async fn run(mut self) {
        // A dictionary of ordering key to outstanding publish operations.
        // We batch publish operations on the same ordering key together.
        // Publish without ordering keys are treated as having the key "".
        let mut batch_actors: HashMap<String, BatchActorHandle> = HashMap::new();
        let mut actor_tasks: JoinMap<String, ()> = JoinMap::new();
        loop {
            tokio::select! {
                _ = actor_tasks.join_next(), if !actor_tasks.is_empty() => {
                    // TODO(#4012): Remove batch actors when there are no outstanding operations
                    // on the ordering key.
                    continue;
                }
                // Handle receiving a message from the channel.
                msg = self.rx.recv() => {
                    match msg {
                        Some(ToDispatcher::Publish(msg)) => {
                            let ordering_key = msg.msg.ordering_key.clone();
                            let batch_actor = batch_actors
                                .entry(ordering_key.clone())
                                .or_insert_with(|| self.spawn_actor(ordering_key.clone(), &mut actor_tasks));
                            if batch_actor.sender.send(ToBatchActor::Publish(msg)).is_err() {
                                return; // Stop the dispatcher if a batch actor is dropped.
                            }
                        },
                        Some(ToDispatcher::Flush(tx)) => {
                            let mut flush_set = JoinSet::new();
                            for batch_actor in batch_actors.values() {
                                let (tx, rx) = oneshot::channel();
                                if batch_actor.sender.send(ToBatchActor::Flush(tx)).is_err() {
                                    return; // Stop the dispatcher if a batch actor is dropped.
                                }
                                flush_set.spawn(rx);
                            }
                            tokio::spawn(async move {
                                // Wait on all the flush operations.
                                flush_set.join_all().await;
                                let _ = tx.send(());
                            });
                        },
                        Some(ToDispatcher::ResumePublish(ordering_key)) => {
                            if let Some(batch_actor) = batch_actors.get_mut(&ordering_key) {
                                // Send down the same tx for the BatchActors to directly signal completion
                                // instead of spawning a new task.
                                if batch_actor.sender.send(ToBatchActor::ResumePublish()).is_err() {
                                    return; // Stop the dispatcher if a batch actor is dropped.
                                }
                            }
                        }
                        None => {
                            // Gracefully shutdown since the Publisher has dropped the Sender.
                            // By dropping the BatchActorHandles, they will individually handle the
                            // shutdown procedures.
                            drop(batch_actors);
                            // When we drop actor_tasks, some batch actors may not have started execution
                            // so the batch actor is aborted with messages in its receiving channel.
                            // We wait for the batch actors instead of aborting so that it can
                            // gracefully shutdown.
                            while actor_tasks.join_next().await.is_some() {};
                            break;
                        }
                    }
                }

            }
        }
    }
}

#[derive(Debug)]
struct BatchActorHandle {
    sender: mpsc::UnboundedSender<ToBatchActor>,
}

#[derive(Debug)]
struct BatchActorContext {
    topic: String,
    client: GapicPublisher,
    rx: mpsc::UnboundedReceiver<ToBatchActor>,
    batching_options: BatchingOptions,
}

impl BatchActorContext {
    fn new(
        topic: String,
        client: GapicPublisher,
        batching_options: BatchingOptions,
        rx: mpsc::UnboundedReceiver<ToBatchActor>,
    ) -> Self {
        BatchActorContext {
            topic,
            client,
            rx,
            batching_options,
        }
    }
}

/// A batch actor that sends batches concurrently.
#[derive(Debug)]
struct ConcurrentBatchActor {
    context: BatchActorContext,
    hedging: Option<HedgingSchedulerHandle>,
    total_timeout: Option<std::time::Duration>,
}

impl ConcurrentBatchActor {
    fn new(
        topic: String,
        client: GapicPublisher,
        batching_options: BatchingOptions,
        hedging_options: Option<HedgingOptions>,
        total_timeout: Option<std::time::Duration>,
        rx: mpsc::UnboundedReceiver<ToBatchActor>,
    ) -> Self {
        let hedging = hedging_options.map(HedgingScheduler::spawn);
        ConcurrentBatchActor {
            context: BatchActorContext::new(topic, client, batching_options, rx),
            hedging,
            total_timeout,
        }
    }

    /// The main loop of the concurrent batch actor.
    ///
    /// This method continuously handles the following events:
    ///
    /// 1. A Publish command from the Dispatcher causes the new message to be
    ///    added a pending message queue. If there is enough message to create
    ///    a full batch, then also flush the batch.
    /// 2. A `Flush` command from the Dispatcher causes all the pending messages
    ///    to be flushed concurrently respecting the configured batch size and message
    ///    ordering.
    /// 3. A ResumePublish command from the Dispatcher causes the actor to resume
    ///    publishing.
    ///
    /// The loop terminates when the `rx` channel is closed, which happens when the
    /// Dispatcher drops the Sender.
    async fn run(mut self) {
        let delay = self.context.batching_options.delay_threshold;
        // Lazy timer: armed when the first message enters a batch and dropped
        // (`None`) when a flush drains the batch. Dropping the timer unregisters
        // it from Tokio's timer driver, so idle actors hold zero time-wheel
        // entries and incur zero CPU wakeups at rest.
        let mut timer: Option<Pin<Box<Sleep>>> = None;
        // We have multiple inflight batches concurrently.
        let mut inflight = JoinSet::new();
        let mut batch = Batch::new(
            self.context.topic.len() as u32,
            self.context.batching_options.clone(),
        );
        loop {
            tokio::select! {
                // Remove completed inflight batches.
                _ = inflight.join_next(), if !inflight.is_empty() => {
                    continue;
                }
                // Flush on timer. flush spawns the batch send into `inflight`
                // (reaped by the join_next arm above) and drains the batch, so
                // the timer is dropped afterwards; a later message re-arms it.
                // The timer is wrapped in an async block so the unwrap runs only
                // when the branch is polled (i.e. when timer.is_some()); select!
                // evaluates the branch expression eagerly, so a bare
                // `timer.as_mut().unwrap()` would unwrap None.
                _ = async { timer.as_mut().unwrap().await }, if timer.is_some() => {
                    self.flush(&mut inflight, &mut batch);
                    timer = None;
                }
                msg = self.context.rx.recv() => {
                    match msg {
                        Some(ToBatchActor::Publish(msg)) => {
                            self.add_msg_and_flush(&mut inflight, &mut batch, msg);
                            // add_msg_and_flush may have flushed on a threshold,
                            // leaving the batch empty; disarm if so, otherwise arm.
                            if batch.is_empty() {
                                timer = None;
                            } else if timer.is_none() {
                                timer = Some(Box::pin(tokio::time::sleep(delay)));
                            }
                        },
                        Some(ToBatchActor::Flush(tx)) => {
                            self.flush(&mut inflight, &mut batch);
                            inflight.join_all().await;
                            inflight = JoinSet::new();
                            timer = None;
                            let _ = tx.send(());
                        },
                        Some(ToBatchActor::ResumePublish()) => {
                            // Nothing to resume as we do not pause without ordering key.
                        }
                        None => {
                            // This isn't guaranteed to execute if a user does not .await on the
                            // corresponding PublishFutures.
                            self.flush(&mut inflight, &mut batch);
                            inflight.join_all().await;
                            break;
                        }
                    }
                }
            }
        }
    }

    // Flush the pending batch if it's not empty.
    fn flush(&mut self, inflight: &mut JoinSet<crate::Result<()>>, batch: &mut Batch) {
        if batch.is_empty() {
            return;
        }
        let (msgs, txs) = batch.drain_messages();
        if let Some(hedging) = &self.hedging {
            hedging.dispatch(
                msgs,
                txs,
                self.context.client.clone(),
                self.context.topic.clone(),
                self.total_timeout,
                inflight,
            );
        } else {
            send(
                msgs,
                txs,
                self.context.client.clone(),
                self.context.topic.clone(),
                inflight,
            );
        }
    }

    // Move message to the pending batch respecting batch thresholds
    // and flush the batch if it is full.
    fn add_msg_and_flush(
        &mut self,
        inflight: &mut JoinSet<crate::Result<()>>,
        batch: &mut Batch,
        msg: BundledMessage,
    ) {
        if !batch.can_add(&msg) {
            self.flush(inflight, batch);
        }
        batch.push(msg);
        if batch.at_threshold() {
            self.flush(inflight, batch);
        }
    }
}

/// A batch actor that sends batches sequentially by awaiting on the previous batch.
#[derive(Debug)]
struct SequentialBatchActor {
    context: BatchActorContext,
    pending_msgs: VecDeque<BundledMessage>,
    paused: bool,
}

impl SequentialBatchActor {
    fn new(
        topic: String,
        client: GapicPublisher,
        batching_options: BatchingOptions,
        rx: mpsc::UnboundedReceiver<ToBatchActor>,
    ) -> Self {
        SequentialBatchActor {
            context: BatchActorContext::new(topic, client, batching_options, rx),
            pending_msgs: VecDeque::new(),
            paused: false,
        }
    }

    /// The main loop of the sequential batch actor.
    ///
    /// This method continuously handles the following events:
    ///
    /// 1. A Publish command from the Dispatcher causes the new message to be
    ///    added a pending message queue. If there is enough message to create
    ///    a full batch and there are currently no inflight batch, then also flush
    ///    the batch.
    /// 2. A `Flush` command from the Dispatcher causes all the pending messages
    ///    to be flushed sequentially respecting the configured batch size and message
    ///    ordering.
    /// 3. A ResumePublish command from the Dispatcher causes the actor to resume
    ///    publishing.
    /// 4. A `inflight` batch completion causes the next batch to send if it satisfies
    ///    the configured batch threshold.
    ///
    /// The loop terminates when the `rx` channel is closed, which happens when the
    /// Dispatcher drops the Sender.
    async fn run(mut self) {
        let delay = self.context.batching_options.delay_threshold;
        // Lazy timer: same pattern as ConcurrentBatchActor — armed when the
        // first message arrives in a new batch cycle, disarmed after a flush
        // that drains all pending messages.
        let mut timer: Option<Pin<Box<Sleep>>> = None;
        // While it is possible to use Some(JoinHandle) here as there is at max
        // a single inflight task at any given time, the use of JoinSet
        // simplify the managing the inflight JoinHandle.
        let mut inflight: JoinSet<crate::Result<()>> = JoinSet::new();
        let mut batch = Batch::new(
            self.context.topic.len() as u32,
            self.context.batching_options.clone(),
        );
        loop {
            if self.paused {
                // When paused, we do not need to check inflight as handle_inflight_join()
                // ensures that there are no inflight batch.
                let msg = self.context.rx.recv().await;
                match msg {
                    Some(ToBatchActor::Publish(msg)) => {
                        let _ = msg
                            .tx
                            .send(Err(crate::error::PublishError::OrderingKeyPaused));
                    }
                    Some(ToBatchActor::Flush(tx)) => {
                        // There should be no pending messages and messages in the pending batch as
                        // it was already handled when this was paused.
                        let _ = tx.send(());
                    }
                    Some(ToBatchActor::ResumePublish()) => {
                        self.paused = false;
                    }
                    None => {
                        // There should be no pending messages and messages in the pending batch as
                        // it was already handled when this was paused.
                        break;
                    }
                }
                continue;
            }
            tokio::select! {
                join = inflight.join_next(), if !inflight.is_empty() => {
                    self.handle_inflight_join(join);
                    self.move_to_batch_and_flush(&mut inflight, &mut batch);
                }
                // Flush on timer. flush drains all pending messages, so the
                // timer is dropped afterwards; a later message re-arms it. This
                // mirrors the Flush branch below. The timer is wrapped in an
                // async block so the unwrap runs only when the branch is polled
                // (i.e. when timer.is_some()); select! evaluates the branch
                // expression eagerly, so a bare unwrap would unwrap None.
                _ = async { timer.as_mut().unwrap().await }, if timer.is_some() => {
                    self.flush_all(&mut inflight, &mut batch).await;
                    inflight = JoinSet::new();
                    timer = None;
                }
                msg = self.context.rx.recv() => {
                    match msg {
                        Some(ToBatchActor::Publish(msg)) => {
                            self.pending_msgs.push_back(msg);
                            if inflight.is_empty() {
                                self.move_to_batch_and_flush(&mut inflight, &mut batch);
                            }
                            // move_to_batch_and_flush may have flushed on a
                            // threshold, draining everything; disarm if so, else arm.
                            if self.pending_msgs.is_empty() && batch.is_empty() {
                                timer = None;
                            } else if timer.is_none() {
                                timer = Some(Box::pin(tokio::time::sleep(delay)));
                            }
                        },
                        Some(ToBatchActor::Flush(tx)) => {
                            self.flush_all(&mut inflight, &mut batch).await;
                            inflight = JoinSet::new();
                            timer = None;
                            let _ = tx.send(());
                        },
                        Some(ToBatchActor::ResumePublish()) => {
                            // Nothing to resume as we are not paused.
                        },
                        None => {
                            // This isn't guaranteed to execute if a user does not .await on the
                            // corresponding PublishFutures.
                            self.flush_all(&mut inflight, &mut batch).await;
                            break;
                        }
                    }
                }
            }
        }
    }

    // Flush the pending messages by sending the messages in sequential batches.
    async fn flush_all(&mut self, inflight: &mut JoinSet<crate::Result<()>>, batch: &mut Batch) {
        self.handle_inflight_join(inflight.join_next().await);
        while !self.pending_msgs.is_empty() {
            self.move_to_batch_and_flush(inflight, batch);
            self.handle_inflight_join(inflight.join_next().await);
        }
        // Flush the pending batch even if it does not fill the batch.
        self.flush(inflight, batch);
        self.handle_inflight_join(inflight.join_next().await);
    }

    // Flush the pending batch if it's not empty.
    fn flush(&mut self, inflight: &mut JoinSet<crate::Result<()>>, batch: &mut Batch) {
        if batch.is_empty() {
            return;
        }
        let (msgs, txs) = batch.drain_messages();
        send(
            msgs,
            txs,
            self.context.client.clone(),
            self.context.topic.clone(),
            inflight,
        );
    }

    // Move message to the pending batch respecting batch thresholds
    // and flush the batch if it is full.
    fn move_to_batch_and_flush(
        &mut self,
        inflight: &mut JoinSet<crate::Result<()>>,
        batch: &mut Batch,
    ) {
        let mut should_flush = false;
        while let Some(next) = self.pending_msgs.front() {
            if !batch.can_add(next) && !batch.is_empty() {
                should_flush = true;
                break;
            }
            let publish = self
                .pending_msgs
                .pop_front()
                .expect("front should contain an element");
            batch.push(publish);
            if batch.at_threshold() {
                should_flush = true;
                break;
            }
        }

        if should_flush {
            self.flush(inflight, batch);
        }
    }

    // Pause publish operations.
    fn pause(&mut self) {
        self.paused = true;
        while let Some(publish) = self.pending_msgs.pop_front() {
            // The user may have dropped the handle, so it is ok if this fails.
            let _ = publish.tx.send(Err(PublishError::OrderingKeyPaused));
        }
    }

    fn handle_inflight_join(
        &mut self,
        join_next_option: Option<Result<crate::Result<()>, tokio::task::JoinError>>,
    ) {
        // If there was a JoinError or non-retryable error:
        // 1. We need to pause publishing and send out errors for pending_msgs.
        // 2. The pending batch should have sent out error for its messages.
        // 3. The messages in rx will be handled when they are received.
        if let Some(Err(_) | Ok(Err(_))) = join_next_option {
            self.pause();
        }
    }
}

pub(crate) fn send(
    msgs: Vec<Message>,
    txs: Vec<oneshot::Sender<Result<String, PublishError>>>,
    client: GapicPublisher,
    topic: String,
    inflight: &mut JoinSet<crate::Result<()>>,
) {
    let start_time = wkt::Timestamp::try_from(std::time::SystemTime::now()).ok();
    inflight.spawn(async move {
        let res = client
            .publish()
            .set_topic(topic)
            .set_messages(msgs)
            .set_pubsub_client_telemetry_header(0, start_time)
            .send()
            .await;
        batch_resolve_publish_futures(res, txs)
    });
}

pub(crate) fn batch_resolve_publish_futures(
    resp: crate::Result<PublishResponse>,
    txs: Vec<oneshot::Sender<Result<String, PublishError>>>,
) -> crate::Result<()> {
    match resp {
        Err(e) => {
            // TODO(#4013): To support message ordering retry, we need to correctly handle
            // the send error here with either retry or propagate to the user.
            let e = Arc::new(e);
            for tx in txs {
                // The user may have dropped the handle, so it is ok if this fails.
                let _ = tx.send(Err(PublishError::Rpc(e.clone())));
            }
            Err(crate::Error::io(e))
        }
        Ok(result) => {
            txs.into_iter()
                .zip(result.message_ids)
                .for_each(|(tx, result)| {
                    // The user may have dropped the handle, so it is ok if this fails.
                    let _ = tx.send(Ok(result));
                });
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{ConcurrentBatchActor, SequentialBatchActor};
    use crate::error::PublishError;
    use crate::google::pubsub::v1::pubsub_client_telemetry::Operation;
    use crate::publisher::actor::{BundledMessage, ToBatchActor};
    use crate::publisher::batch::Batch;
    use crate::publisher::constants::{MAX_BYTES, MAX_MESSAGES};
    use crate::publisher::options::BatchingOptions;
    use crate::publisher::publish_telemetry::parse_pubsub_client_telemetry_header;
    use crate::{
        generated::gapic_dataplane::client::Publisher as GapicPublisher,
        model::{Message, PublishResponse},
    };
    use google_cloud_test_macros::tokio_test_no_panics;
    use mockall::Sequence;
    use rand::{RngExt, distr::Alphanumeric};
    use std::collections::VecDeque;
    use std::time::Duration;
    use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};

    static TOPIC: &str = "my-topic";
    const EXPECTED_BATCHES: usize = 5;
    const TIME_PER_BATCH: Duration = Duration::from_secs(10);

    mockall::mock! {
        #[derive(Debug)]
        GapicPublisher {}
        impl crate::generated::gapic_dataplane::stub::Publisher for GapicPublisher {
            async fn publish(&self, req: crate::model::PublishRequest, _options: google_cloud_gax::options::RequestOptions) -> google_cloud_gax::Result<google_cloud_gax::response::Response<crate::model::PublishResponse>>;
        }
    }

    // Similar to GapicPublisher but returns impl Future instead.
    // This is useful for mocking a response with delays/timeouts.
    // See https://github.com/asomers/mockall/issues/189 for more
    // detail on why this is needed.
    // While this can used inplace of GapicPublisher, it makes the
    // normal usage without async closure much more cumbersome.
    mockall::mock! {
        #[derive(Debug)]
        GapicPublisherWithFuture {}
        impl crate::generated::gapic_dataplane::stub::Publisher for GapicPublisherWithFuture {
            fn publish(&self, req: crate::model::PublishRequest, _options: google_cloud_gax::options::RequestOptions) -> impl Future<Output=google_cloud_gax::Result<google_cloud_gax::response::Response<PublishResponse>>> + Send;
        }
    }

    fn publish_ok(
        req: crate::model::PublishRequest,
        _options: crate::RequestOptions,
    ) -> crate::Result<crate::Response<PublishResponse>> {
        let ids = req
            .messages
            .iter()
            .map(|m| String::from_utf8(m.data.to_vec()).unwrap());
        Ok(crate::Response::from(
            PublishResponse::new().set_message_ids(ids),
        ))
    }

    fn track_publish_msg_seq(
        req: &crate::model::PublishRequest,
        msg_seq_tx: UnboundedSender<Message>,
    ) {
        req.messages.iter().for_each(|m| {
            msg_seq_tx
                .send(m.clone())
                .expect("sending should always succeed as the test should not close the channel");
        });
    }

    fn publish_err(
        _req: crate::model::PublishRequest,
        _options: crate::RequestOptions,
    ) -> crate::Result<crate::Response<PublishResponse>> {
        Err(crate::Error::service(
            google_cloud_gax::error::rpc::Status::default()
                .set_code(google_cloud_gax::error::rpc::Code::Unknown)
                .set_message("unknown error has occurred"),
        ))
    }

    fn generate_random_data() -> String {
        rand::rng()
            .sample_iter(&Alphanumeric)
            .take(16)
            .map(char::from)
            .collect()
    }

    // Send ToBatchActor::Publish with random data n times and track the messages in publish_rxs.
    macro_rules! publish_random_data {
        ($publish_rxs:ident, $actor_tx:ident, $n:expr) => {
            for _ in 0..$n {
                let (publish_tx, publish_rx) = tokio::sync::oneshot::channel();
                let msg = generate_random_data();
                let bundle = BundledMessage {
                    msg: Message::new().set_data(msg.clone()),
                    tx: publish_tx,
                };
                $actor_tx.send(ToBatchActor::Publish(bundle))?;
                $publish_rxs.push_back((msg, publish_rx));
            }
        };
    }

    // Verify that the publish data in publish_rxs matches the resolved message.
    macro_rules! assert_publish_data {
        ($publish_rxs:ident) => {
            for (msg, publish_rx) in $publish_rxs {
                assert_eq!(
                    publish_rx.await??,
                    msg,
                    "unexpected message for given handler"
                );
            }
        };
    }

    // Verify that the publish data in publish_rxs the resolved message and the expected sequence.
    macro_rules! assert_publish_data_with_seq {
        ($publish_rxs:ident, $expected_msg_seq_rx:ident) => {
            for (msg, publish_rx) in $publish_rxs {
                assert_eq!(
                    publish_rx.await??,
                    msg,
                    "unexpected message for given handler"
                );
                // Assert that publish message matches the expected message sequence.
                let expected_msg = $expected_msg_seq_rx.try_recv()?.data;
                assert_eq!(msg, expected_msg, "message published out of order");
            }
        };
    }

    // Send ToBatchActor::Publish with random data n times then await and assert the result.
    macro_rules! assert_publish_is_ok {
        ($actor_tx:ident, $n:expr) => {
            let mut publish_rxs = VecDeque::new();
            publish_random_data!(publish_rxs, $actor_tx, $n);
            assert_publish_data!(publish_rxs);
        };
        ($actor_tx:ident, $expected_msg_seq_rx:ident, $n:expr) => {
            let mut publish_rxs = VecDeque::new();
            publish_random_data!(publish_rxs, $actor_tx, $n);
            assert_publish_data_with_seq!(publish_rxs, $expected_msg_seq_rx);
        };
    }

    // Send ToBatchActor::Publish with random data n times then await and assert that the actor is paused.
    macro_rules! assert_actor_is_paused {
        ($actor_tx:ident, $n:expr) => {
            let mut publish_rxs = Vec::new();
            for _ in 0..$n {
                let (publish_tx, publish_rx) = tokio::sync::oneshot::channel();
                // let msg = generate_random_data();
                let bundle = BundledMessage {
                    msg: Message::new().set_data(generate_random_data()),
                    tx: publish_tx,
                };
                $actor_tx.send(ToBatchActor::Publish(bundle))?;
                publish_rxs.push(publish_rx);
            }
            for v in publish_rxs {
                let res = v.await;
                assert!(
                    matches!(res, Ok(Err(PublishError::OrderingKeyPaused))),
                    "{res:?}"
                );
            }
        };
    }

    macro_rules! assert_flush {
        ($actor_tx:ident) => {
            let (flush_tx, flush_rx) = tokio::sync::oneshot::channel();
            $actor_tx.send(ToBatchActor::Flush(flush_tx))?;
            flush_rx.await?;
        };
    }

    #[tokio::test]
    async fn basic() -> anyhow::Result<()> {
        let client = GapicPublisher::from_stub(MockGapicPublisher::new());
        let batching_options = BatchingOptions::default();

        let (_, rx) = tokio::sync::mpsc::unbounded_channel();
        let _ = ConcurrentBatchActor::new(
            TOPIC.to_string(),
            client.clone(),
            batching_options.clone(),
            None,
            None,
            rx,
        );

        let (_, rx) = tokio::sync::mpsc::unbounded_channel();
        let _ = SequentialBatchActor::new("topic".to_string(), client, batching_options, rx);
        Ok(())
    }

    #[tokio_test_no_panics(start_paused = true)]
    async fn concurrent_actor_publish() -> anyhow::Result<()> {
        let mut mock = MockGapicPublisherWithFuture::new();
        mock.expect_publish()
            .withf(|req, _o| req.topic == TOPIC)
            .times(EXPECTED_BATCHES)
            .returning({
                |r, o| {
                    Box::pin(async move {
                        tokio::time::sleep(TIME_PER_BATCH).await;
                        publish_ok(r, o)
                    })
                }
            });
        let (actor_tx, actor_rx) = tokio::sync::mpsc::unbounded_channel();
        tokio::spawn(
            ConcurrentBatchActor::new(
                TOPIC.to_string(),
                GapicPublisher::from_stub(mock),
                BatchingOptions::default().set_message_count_threshold(2_u32),
                None,
                None,
                actor_rx,
            )
            .run(),
        );

        let start = tokio::time::Instant::now();
        assert_publish_is_ok!(actor_tx, 10);
        assert_eq!(
            start.elapsed(),
            TIME_PER_BATCH,
            "all batches should have been concurrently sent and completed by {:?}",
            TIME_PER_BATCH
        );
        Ok(())
    }

    #[tokio_test_no_panics(start_paused = true)]
    async fn sequential_actor_publish() -> anyhow::Result<()> {
        let (msg_seq_tx, mut msg_seq_rx) = unbounded_channel::<Message>();
        let mut mock = MockGapicPublisherWithFuture::new();
        mock.expect_publish()
            .withf(|req, _o| req.topic == TOPIC)
            .times(EXPECTED_BATCHES)
            .returning({
                move |r, o| {
                    Box::pin({
                        let seq_tx = msg_seq_tx.clone();
                        async move {
                            tokio::time::sleep(TIME_PER_BATCH).await;
                            track_publish_msg_seq(&r, seq_tx);
                            publish_ok(r, o)
                        }
                    })
                }
            });
        let (actor_tx, actor_rx) = tokio::sync::mpsc::unbounded_channel();
        tokio::spawn(
            SequentialBatchActor::new(
                TOPIC.to_string(),
                GapicPublisher::from_stub(mock),
                BatchingOptions::default().set_message_count_threshold(2_u32),
                actor_rx,
            )
            .run(),
        );

        let start = tokio::time::Instant::now();
        assert_publish_is_ok!(actor_tx, msg_seq_rx, 10);
        assert_eq!(
            start.elapsed(),
            EXPECTED_BATCHES as u32 * TIME_PER_BATCH,
            "all batches should have been seqentially sent and takes {:?}",
            EXPECTED_BATCHES as u32 * TIME_PER_BATCH
        );
        Ok(())
    }

    #[tokio_test_no_panics(start_paused = true)]
    async fn concurrent_actor_flush() -> anyhow::Result<()> {
        let mut mock = MockGapicPublisherWithFuture::new();
        mock.expect_publish()
            .withf(|req, _o| req.topic == TOPIC)
            .times(EXPECTED_BATCHES)
            .returning({
                |r, o| {
                    Box::pin(async move {
                        tokio::time::sleep(TIME_PER_BATCH).await;
                        publish_ok(r, o)
                    })
                }
            });
        let (actor_tx, actor_rx) = tokio::sync::mpsc::unbounded_channel();
        tokio::spawn(
            ConcurrentBatchActor::new(
                TOPIC.to_string(),
                GapicPublisher::from_stub(mock),
                BatchingOptions::default().set_message_count_threshold(2_u32),
                None,
                None,
                actor_rx,
            )
            .run(),
        );

        // Flush on empty.
        assert_flush!(actor_tx);

        // Publish 10 messages then Flush.
        let start = tokio::time::Instant::now();
        let mut publish_rxs = VecDeque::new();
        publish_random_data!(publish_rxs, actor_tx, 10);
        assert_flush!(actor_tx);
        assert_publish_data!(publish_rxs);
        assert_eq!(
            start.elapsed(),
            TIME_PER_BATCH,
            "all batches should have been concurrently sent and completed by {:?}",
            TIME_PER_BATCH
        );

        Ok(())
    }

    #[tokio_test_no_panics(start_paused = true)]
    async fn sequential_actor_flush() -> anyhow::Result<()> {
        let (msg_seq_tx, mut msg_seq_rx) = unbounded_channel::<Message>();
        let mut mock = MockGapicPublisherWithFuture::new();
        mock.expect_publish()
            .withf(|req, _o| req.topic == TOPIC)
            .times(EXPECTED_BATCHES)
            .returning({
                move |r, o| {
                    Box::pin({
                        let seq_tx = msg_seq_tx.clone();
                        async move {
                            tokio::time::sleep(TIME_PER_BATCH).await;
                            track_publish_msg_seq(&r, seq_tx);
                            publish_ok(r, o)
                        }
                    })
                }
            });
        let (actor_tx, actor_rx) = tokio::sync::mpsc::unbounded_channel();
        tokio::spawn(
            SequentialBatchActor::new(
                TOPIC.to_string(),
                GapicPublisher::from_stub(mock),
                BatchingOptions::default().set_message_count_threshold(2_u32),
                actor_rx,
            )
            .run(),
        );

        // Flush on empty.
        assert_flush!(actor_tx);

        // Publish 10 messages then Flush.
        let start = tokio::time::Instant::now();
        let mut publish_rxs = VecDeque::new();
        publish_random_data!(publish_rxs, actor_tx, 10);
        assert_flush!(actor_tx);
        assert_publish_data_with_seq!(publish_rxs, msg_seq_rx);
        assert_eq!(
            start.elapsed(),
            EXPECTED_BATCHES as u32 * TIME_PER_BATCH,
            "all batches should have been seqentially sent and takes {:?}",
            EXPECTED_BATCHES as u32 * TIME_PER_BATCH
        );

        Ok(())
    }

    #[tokio::test]
    async fn concurrent_actor_resume() -> anyhow::Result<()> {
        let (actor_tx, actor_rx) = tokio::sync::mpsc::unbounded_channel();
        tokio::spawn(
            ConcurrentBatchActor::new(
                TOPIC.to_string(),
                GapicPublisher::from_stub(MockGapicPublisher::new()),
                BatchingOptions::default(),
                None,
                None,
                actor_rx,
            )
            .run(),
        );

        actor_tx.send(ToBatchActor::ResumePublish())?;
        Ok(())
    }

    #[tokio_test_no_panics(start_paused = true)]
    async fn sequential_actor_resume() -> anyhow::Result<()> {
        let (msg_seq_tx, mut msg_seq_rx) = unbounded_channel::<Message>();
        let mut mock = MockGapicPublisherWithFuture::new();
        let mut seq = Sequence::new();
        mock.expect_publish()
            .withf(|req, _o| req.topic == TOPIC)
            .times(EXPECTED_BATCHES)
            .in_sequence(&mut seq)
            .returning(move |r, o| Box::pin(async move { publish_ok(r, o) }));
        mock.expect_publish()
            .withf(|req, _o| req.topic == TOPIC)
            .once()
            .in_sequence(&mut seq)
            .returning(move |r, o| Box::pin(async { publish_err(r, o) }));
        mock.expect_publish()
            .withf(|req, _o| req.topic == TOPIC)
            .times(EXPECTED_BATCHES)
            .in_sequence(&mut seq)
            .returning({
                move |r, o| {
                    Box::pin({
                        let seq_tx = msg_seq_tx.clone();
                        async move {
                            track_publish_msg_seq(&r, seq_tx);
                            publish_ok(r, o)
                        }
                    })
                }
            });

        let (actor_tx, actor_rx) = tokio::sync::mpsc::unbounded_channel();
        tokio::spawn(
            SequentialBatchActor::new(
                TOPIC.to_string(),
                GapicPublisher::from_stub(mock),
                BatchingOptions::default().set_message_count_threshold(1_u32),
                actor_rx,
            )
            .run(),
        );

        // Validate resume when not paused.
        actor_tx.send(ToBatchActor::ResumePublish())?;
        assert_publish_is_ok!(actor_tx, EXPECTED_BATCHES);

        // This message triggers the mock to return publish error and causes the actor to pause.
        let (publish_tx, publish_rx) = tokio::sync::oneshot::channel();
        let bundle = BundledMessage {
            msg: Message::new().set_data(generate_random_data()),
            tx: publish_tx,
        };
        actor_tx.send(ToBatchActor::Publish(bundle))?;
        let got_err = publish_rx.await;
        // TODO(#3689): Validate the error structure when Publisher error structure is better defined.
        assert!(matches!(got_err, Ok(Err(_))), "{got_err:?}");
        assert_actor_is_paused!(actor_tx, 5);

        // Resume then validate that the actor is no longer paused.
        actor_tx.send(ToBatchActor::ResumePublish())?;
        assert_publish_is_ok!(actor_tx, msg_seq_rx, EXPECTED_BATCHES);

        Ok(())
    }

    #[tokio_test_no_panics(start_paused = true)]
    async fn concurrent_actor_batch_message_count_threshold() -> anyhow::Result<()> {
        let mut mock = MockGapicPublisher::new();
        mock.expect_publish()
            .withf(|req, _o| req.topic == TOPIC && req.messages.len() == 10)
            .returning(publish_ok);
        let (actor_tx, actor_rx) = tokio::sync::mpsc::unbounded_channel();
        tokio::spawn(
            ConcurrentBatchActor::new(
                TOPIC.to_string(),
                GapicPublisher::from_stub(mock),
                BatchingOptions::default()
                    .set_message_count_threshold(10_u32)
                    .set_byte_threshold(MAX_BYTES)
                    .set_delay_threshold(std::time::Duration::MAX),
                None,
                None,
                actor_rx,
            )
            .run(),
        );

        assert_publish_is_ok!(actor_tx, 10);

        Ok(())
    }

    #[tokio_test_no_panics(start_paused = true)]
    async fn sequential_actor_batch_message_count_threshold() -> anyhow::Result<()> {
        let mut mock = MockGapicPublisher::new();
        mock.expect_publish()
            .withf(|req, _o| req.topic == TOPIC && req.messages.len() == 10)
            .returning(publish_ok);
        let (actor_tx, actor_rx) = tokio::sync::mpsc::unbounded_channel();
        tokio::spawn(
            SequentialBatchActor::new(
                TOPIC.to_string(),
                GapicPublisher::from_stub(mock),
                BatchingOptions::default()
                    .set_message_count_threshold(10_u32)
                    .set_byte_threshold(MAX_BYTES)
                    .set_delay_threshold(std::time::Duration::MAX),
                actor_rx,
            )
            .run(),
        );

        assert_publish_is_ok!(actor_tx, 10);
        Ok(())
    }

    #[tokio_test_no_panics(start_paused = true)]
    async fn concurrent_actor_byte_count_threshold() -> anyhow::Result<()> {
        let mut mock = MockGapicPublisher::new();
        mock.expect_publish()
            .withf(|req, _o| {
                // Recreate the batch from req to calculate the batch size.
                let mut batch = Batch::new(req.topic.len() as u32, BatchingOptions::default());
                req.messages.iter().for_each(|msg| {
                    let (tx, _rx) = tokio::sync::oneshot::channel();
                    batch.push(BundledMessage {
                        msg: msg.clone(),
                        tx,
                    });
                });
                req.topic == TOPIC && batch.size() <= 25_u32
            })
            .returning(publish_ok);
        let (actor_tx, actor_rx) = tokio::sync::mpsc::unbounded_channel();
        tokio::spawn(
            ConcurrentBatchActor::new(
                TOPIC.to_string(),
                GapicPublisher::from_stub(mock),
                BatchingOptions::default()
                    .set_message_count_threshold(MAX_MESSAGES)
                    .set_byte_threshold(25_u32), // The current test generates 24 byte single message batches.
                None,
                None,
                actor_rx,
            )
            .run(),
        );

        let mut publish_rxs = VecDeque::new();
        publish_random_data!(publish_rxs, actor_tx, 10);
        // We flush here otherwise the last message will await forever since it never exceed the byte threshold.
        assert_flush!(actor_tx);
        assert_publish_data!(publish_rxs);

        Ok(())
    }

    #[tokio_test_no_panics(start_paused = true)]
    async fn sequential_actor_byte_count_threshold() -> anyhow::Result<()> {
        let mut mock = MockGapicPublisher::new();
        mock.expect_publish()
            .withf(|req, _o| {
                // Recreate the batch from req to calculate the batch size.
                let mut batch = Batch::new(req.topic.len() as u32, BatchingOptions::default());
                req.messages.iter().for_each(|msg| {
                    let (tx, _rx) = tokio::sync::oneshot::channel();
                    batch.push(BundledMessage {
                        msg: msg.clone(),
                        tx,
                    });
                });
                req.topic == TOPIC && batch.size() <= 25_u32
            })
            .returning(publish_ok);
        let (actor_tx, actor_rx) = tokio::sync::mpsc::unbounded_channel();
        tokio::spawn(
            SequentialBatchActor::new(
                TOPIC.to_string(),
                GapicPublisher::from_stub(mock),
                BatchingOptions::default()
                    .set_message_count_threshold(MAX_MESSAGES)
                    .set_byte_threshold(25_u32), // The current test generates 24 byte single message batches.
                actor_rx,
            )
            .run(),
        );

        let mut publish_rxs = VecDeque::new();
        publish_random_data!(publish_rxs, actor_tx, 1);
        // We flush here otherwise the last message will await forever since it never exceed the byte threshold.
        assert_flush!(actor_tx);
        assert_publish_data!(publish_rxs);

        Ok(())
    }

    #[tokio_test_no_panics(start_paused = true)]
    async fn sequential_actor_send_large_message() -> anyhow::Result<()> {
        let mut mock = MockGapicPublisher::new();
        mock.expect_publish()
            .withf(|req, _o| {
                // Recreate the batch from req to calculate the batch size.
                let mut batch = Batch::new(req.topic.len() as u32, BatchingOptions::default());
                req.messages.iter().for_each(|msg| {
                    let (tx, _rx) = tokio::sync::oneshot::channel();
                    batch.push(BundledMessage {
                        msg: msg.clone(),
                        tx,
                    });
                });
                req.topic == TOPIC && batch.size() >= 23_u32
            })
            .returning(publish_ok);
        let (actor_tx, actor_rx) = tokio::sync::mpsc::unbounded_channel();
        tokio::spawn(
            SequentialBatchActor::new(
                TOPIC.to_string(),
                GapicPublisher::from_stub(mock),
                BatchingOptions::default()
                    .set_message_count_threshold(MAX_MESSAGES)
                    .set_byte_threshold(1_u32), // The current test generates 24 byte single message batches.
                actor_rx,
            )
            .run(),
        );
        assert_publish_is_ok!(actor_tx, 10);

        Ok(())
    }

    #[tokio_test_no_panics(start_paused = true)]
    async fn concurrent_actor_send_large_message() -> anyhow::Result<()> {
        let mut mock = MockGapicPublisher::new();
        mock.expect_publish()
            .withf(|req, _o| {
                // Recreate the batch from req to calculate the batch size.
                let mut batch = Batch::new(req.topic.len() as u32, BatchingOptions::default());
                req.messages.iter().for_each(|msg| {
                    let (tx, _rx) = tokio::sync::oneshot::channel();
                    batch.push(BundledMessage {
                        msg: msg.clone(),
                        tx,
                    });
                });
                req.topic == TOPIC && batch.size() >= 23_u32
            })
            .returning(publish_ok);
        let (actor_tx, actor_rx) = tokio::sync::mpsc::unbounded_channel();
        tokio::spawn(
            ConcurrentBatchActor::new(
                TOPIC.to_string(),
                GapicPublisher::from_stub(mock),
                BatchingOptions::default()
                    .set_message_count_threshold(MAX_MESSAGES)
                    .set_byte_threshold(1_u32), // The current test generates 24 byte single message batches.
                None,
                None,
                actor_rx,
            )
            .run(),
        );
        assert_publish_is_ok!(actor_tx, 10);

        Ok(())
    }

    #[tokio_test_no_panics(start_paused = true)]
    async fn concurrent_actor_hedging_delay_triggers_hedged_attempt() -> anyhow::Result<()> {
        use crate::publisher::options::HedgingOptions;
        use std::sync::Arc;
        use std::sync::atomic::{AtomicBool, Ordering};

        let hedged_received = Arc::new(AtomicBool::new(false));
        let hedged_received_clone = hedged_received.clone();

        let mut mock = MockGapicPublisherWithFuture::new();
        // 1. Initial 10 fast publishes to fill the token bucket (10 * 0.1 ratio * 1000 = 1000 tokens = 1 whole token).
        mock.expect_publish()
            .times(10)
            .returning(|r, o| Box::pin(async move { publish_ok(r, o) }));

        // 2. Slow attempt 0 (takes 2 seconds)
        mock.expect_publish().once().returning(|r, o| {
            Box::pin(async move {
                tokio::time::sleep(Duration::from_secs(2)).await;
                publish_ok(r, o)
            })
        });

        // 3. Hedged attempt 1 (sent with NeverRetry after 500ms delay, completes quickly)
        mock.expect_publish().once().returning(move |r, o| {
            let flag = hedged_received_clone.clone();
            Box::pin(async move {
                flag.store(true, Ordering::SeqCst);
                publish_ok(r, o)
            })
        });

        let (actor_tx, actor_rx) = tokio::sync::mpsc::unbounded_channel();
        let hedging_options = HedgingOptions::default()
            .set_delay(Duration::from_millis(500))
            .set_max_tokens(50_u32)
            .set_refill_ratio(0.1_f32);

        tokio::spawn(
            ConcurrentBatchActor::new(
                TOPIC.to_string(),
                GapicPublisher::from_stub(mock),
                BatchingOptions::default().set_message_count_threshold(1_u32),
                Some(hedging_options),
                None,
                actor_rx,
            )
            .run(),
        );

        // Fill token bucket by publishing 10 messages
        assert_publish_is_ok!(actor_tx, 10);

        // Now send the 11th message which will trigger hedging after 500ms
        let start = tokio::time::Instant::now();
        let (publish_tx, publish_rx) = tokio::sync::oneshot::channel();
        let bundle = BundledMessage {
            msg: Message::new().set_data("hedged_msg"),
            tx: publish_tx,
        };
        actor_tx.send(ToBatchActor::Publish(bundle))?;
        let res = publish_rx.await??;
        assert_eq!(res, "hedged_msg");
        assert_eq!(start.elapsed(), Duration::from_millis(500));
        assert!(hedged_received.load(Ordering::SeqCst));

        Ok(())
    }

    #[tokio_test_no_panics(start_paused = true)]
    async fn concurrent_actor_hedging_no_tokens_waits_for_attempt0() -> anyhow::Result<()> {
        use crate::publisher::options::HedgingOptions;

        let mut mock = MockGapicPublisherWithFuture::new();
        // Token bucket starts at 0 tokens, so even after 500ms delay, hedging is NOT attempted.
        mock.expect_publish().once().returning(|r, o| {
            Box::pin(async move {
                tokio::time::sleep(Duration::from_secs(2)).await;
                publish_ok(r, o)
            })
        });

        let (actor_tx, actor_rx) = tokio::sync::mpsc::unbounded_channel();
        let hedging_options = HedgingOptions::default()
            .set_delay(Duration::from_millis(500))
            .set_max_tokens(50_u32)
            .set_refill_ratio(0.1_f32);

        tokio::spawn(
            ConcurrentBatchActor::new(
                TOPIC.to_string(),
                GapicPublisher::from_stub(mock),
                BatchingOptions::default().set_message_count_threshold(1_u32),
                Some(hedging_options),
                None,
                actor_rx,
            )
            .run(),
        );

        let start = tokio::time::Instant::now();
        let (publish_tx, publish_rx) = tokio::sync::oneshot::channel();
        let bundle = BundledMessage {
            msg: Message::new().set_data("no_token_msg"),
            tx: publish_tx,
        };
        actor_tx.send(ToBatchActor::Publish(bundle))?;
        let res = publish_rx.await??;
        assert_eq!(res, "no_token_msg");
        assert_eq!(start.elapsed(), Duration::from_secs(2));

        Ok(())
    }

    #[tokio_test_no_panics(start_paused = true)]
    async fn concurrent_actor_hedging_attempt0_fails_fast() -> anyhow::Result<()> {
        use crate::publisher::options::HedgingOptions;

        let mut mock = MockGapicPublisherWithFuture::new();
        // 10 initial publishes to fill token bucket
        mock.expect_publish()
            .times(10)
            .returning(|r, o| Box::pin(async move { publish_ok(r, o) }));

        // Attempt 0 fails after 1 second (exhausting all retries)
        mock.expect_publish().once().returning(|r, o| {
            Box::pin(async move {
                tokio::time::sleep(Duration::from_secs(1)).await;
                publish_err(r, o)
            })
        });

        // Hedged attempt 1 is launched at 500ms, would take 1s to complete (at 1500ms)
        mock.expect_publish().once().returning(|r, o| {
            Box::pin(async move {
                tokio::time::sleep(Duration::from_secs(1)).await;
                publish_ok(r, o)
            })
        });

        let (actor_tx, actor_rx) = tokio::sync::mpsc::unbounded_channel();
        let hedging_options = HedgingOptions::default()
            .set_delay(Duration::from_millis(500))
            .set_max_tokens(50_u32)
            .set_refill_ratio(0.1_f32);

        tokio::spawn(
            ConcurrentBatchActor::new(
                TOPIC.to_string(),
                GapicPublisher::from_stub(mock),
                BatchingOptions::default().set_message_count_threshold(1_u32),
                Some(hedging_options),
                None,
                actor_rx,
            )
            .run(),
        );

        assert_publish_is_ok!(actor_tx, 10);

        let start = tokio::time::Instant::now();
        let (publish_tx, publish_rx) = tokio::sync::oneshot::channel();
        let bundle = BundledMessage {
            msg: Message::new().set_data("fails_fast"),
            tx: publish_tx,
        };
        actor_tx.send(ToBatchActor::Publish(bundle))?;
        let res = publish_rx.await?;
        assert!(res.is_err());
        // Initial attempt failure fails fast at 1s without waiting for hedged attempt at 1.5s
        assert_eq!(start.elapsed(), Duration::from_secs(1));

        Ok(())
    }

    #[tokio_test_no_panics(start_paused = true)]
    async fn concurrent_actor_hedging_attempt1_err_attempt0_ok() -> anyhow::Result<()> {
        use crate::publisher::options::HedgingOptions;

        let mut mock = MockGapicPublisherWithFuture::new();
        // 10 initial publishes to fill token bucket
        mock.expect_publish()
            .times(10)
            .returning(|r, o| Box::pin(async move { publish_ok(r, o) }));

        // Attempt 0 takes 2 seconds and succeeds
        mock.expect_publish().once().returning(|r, o| {
            Box::pin(async move {
                tokio::time::sleep(Duration::from_secs(2)).await;
                publish_ok(r, o)
            })
        });

        // Hedged attempt 1 (launched at 500ms) fails with transient error at 1s
        mock.expect_publish().once().returning(|r, o| {
            Box::pin(async move {
                tokio::time::sleep(Duration::from_millis(500)).await;
                publish_err(r, o)
            })
        });

        let (actor_tx, actor_rx) = tokio::sync::mpsc::unbounded_channel();
        let hedging_options = HedgingOptions::default()
            .set_delay(Duration::from_millis(500))
            .set_max_tokens(50_u32)
            .set_refill_ratio(0.1_f32);

        tokio::spawn(
            ConcurrentBatchActor::new(
                TOPIC.to_string(),
                GapicPublisher::from_stub(mock),
                BatchingOptions::default().set_message_count_threshold(1_u32),
                Some(hedging_options),
                None,
                actor_rx,
            )
            .run(),
        );

        assert_publish_is_ok!(actor_tx, 10);

        let start = tokio::time::Instant::now();
        let (publish_tx, publish_rx) = tokio::sync::oneshot::channel();
        let bundle = BundledMessage {
            msg: Message::new().set_data("attempt0_wins"),
            tx: publish_tx,
        };
        actor_tx.send(ToBatchActor::Publish(bundle))?;
        let res = publish_rx.await??;
        assert_eq!(res, "attempt0_wins");
        assert_eq!(start.elapsed(), Duration::from_secs(2));

        Ok(())
    }

    #[tokio_test_no_panics(start_paused = true)]
    async fn concurrent_actor_hedging_both_fail() -> anyhow::Result<()> {
        use crate::publisher::options::HedgingOptions;

        let mut mock = MockGapicPublisherWithFuture::new();
        // 10 initial publishes to fill token bucket
        mock.expect_publish()
            .times(10)
            .returning(|r, o| Box::pin(async move { publish_ok(r, o) }));

        // Attempt 0 fails after 1 second
        mock.expect_publish().once().returning(|r, o| {
            Box::pin(async move {
                tokio::time::sleep(Duration::from_secs(1)).await;
                publish_err(r, o)
            })
        });

        // Hedged attempt 1 also fails after 1 second (1.5 seconds total)
        mock.expect_publish().once().returning(|r, o| {
            Box::pin(async move {
                tokio::time::sleep(Duration::from_secs(1)).await;
                publish_err(r, o)
            })
        });

        let (actor_tx, actor_rx) = tokio::sync::mpsc::unbounded_channel();
        let hedging_options = HedgingOptions::default()
            .set_delay(Duration::from_millis(500))
            .set_max_tokens(50_u32)
            .set_refill_ratio(0.1_f32);

        tokio::spawn(
            ConcurrentBatchActor::new(
                TOPIC.to_string(),
                GapicPublisher::from_stub(mock),
                BatchingOptions::default().set_message_count_threshold(1_u32),
                Some(hedging_options),
                None,
                actor_rx,
            )
            .run(),
        );

        assert_publish_is_ok!(actor_tx, 10);

        let (publish_tx, publish_rx) = tokio::sync::oneshot::channel();
        let bundle = BundledMessage {
            msg: Message::new().set_data("both_fail"),
            tx: publish_tx,
        };
        actor_tx.send(ToBatchActor::Publish(bundle))?;
        let res = publish_rx.await;
        assert!(matches!(res, Ok(Err(_))), "{res:?}");

        Ok(())
    }

    #[tokio_test_no_panics(start_paused = true)]
    async fn concurrent_actor_hedging_flush() -> anyhow::Result<()> {
        use crate::publisher::options::HedgingOptions;

        let mut mock = MockGapicPublisherWithFuture::new();
        // 10 initial fast publishes to fill token bucket (1 full token)
        mock.expect_publish()
            .times(10)
            .returning(|r, o| Box::pin(async move { publish_ok(r, o) }));

        // Initial attempt of flushed batch hangs for 2s
        mock.expect_publish().once().returning(|r, o| {
            Box::pin(async move {
                tokio::time::sleep(Duration::from_secs(2)).await;
                publish_ok(r, o)
            })
        });

        // Hedged attempt triggers after 500ms delay and succeeds quickly
        mock.expect_publish()
            .once()
            .returning(|r, o| Box::pin(async move { publish_ok(r, o) }));

        let (actor_tx, actor_rx) = tokio::sync::mpsc::unbounded_channel();
        let hedging_options = HedgingOptions::default()
            .set_delay(Duration::from_millis(500))
            .set_max_tokens(50_u32)
            .set_refill_ratio(0.1_f32);

        tokio::spawn(
            ConcurrentBatchActor::new(
                TOPIC.to_string(),
                GapicPublisher::from_stub(mock),
                BatchingOptions::default().set_message_count_threshold(1_u32),
                Some(hedging_options),
                None,
                actor_rx,
            )
            .run(),
        );

        assert_publish_is_ok!(actor_tx, 10);

        let start = tokio::time::Instant::now();
        let (publish_tx, publish_rx) = tokio::sync::oneshot::channel();
        let bundle = BundledMessage {
            msg: Message::new().set_data("flushed_msg"),
            tx: publish_tx,
        };
        actor_tx.send(ToBatchActor::Publish(bundle))?;

        // Explicitly flush the actor
        assert_flush!(actor_tx);

        let res = publish_rx.await??;
        assert_eq!(res, "flushed_msg");
        // Flush waits for the batch to resolve, which happens via hedge at 500ms
        assert_eq!(start.elapsed(), Duration::from_millis(500));

        Ok(())
    }

    #[tokio_test_no_panics(start_paused = true)]
    async fn concurrent_actor_hedging_channel_drop_shutdown() -> anyhow::Result<()> {
        use crate::publisher::options::HedgingOptions;

        let mut mock = MockGapicPublisherWithFuture::new();
        // 10 initial fast publishes to fill token bucket (1 full token)
        mock.expect_publish()
            .times(10)
            .returning(|r, o| Box::pin(async move { publish_ok(r, o) }));

        // Initial attempt hangs for 2s
        mock.expect_publish().once().returning(|r, o| {
            Box::pin(async move {
                tokio::time::sleep(Duration::from_secs(2)).await;
                publish_ok(r, o)
            })
        });

        // Hedged attempt triggers after 500ms and completes quickly
        mock.expect_publish()
            .once()
            .returning(|r, o| Box::pin(async move { publish_ok(r, o) }));

        let (actor_tx, actor_rx) = tokio::sync::mpsc::unbounded_channel();
        let hedging_options = HedgingOptions::default()
            .set_delay(Duration::from_millis(500))
            .set_max_tokens(50_u32)
            .set_refill_ratio(0.1_f32);

        let actor_task = tokio::spawn(
            ConcurrentBatchActor::new(
                TOPIC.to_string(),
                GapicPublisher::from_stub(mock),
                BatchingOptions::default().set_message_count_threshold(1_u32),
                Some(hedging_options),
                None,
                actor_rx,
            )
            .run(),
        );

        assert_publish_is_ok!(actor_tx, 10);

        let start = tokio::time::Instant::now();
        let (publish_tx, publish_rx) = tokio::sync::oneshot::channel();
        let bundle = BundledMessage {
            msg: Message::new().set_data("shutdown_msg"),
            tx: publish_tx,
        };
        actor_tx.send(ToBatchActor::Publish(bundle))?;

        // Drop the actor channel to trigger shutdown (None branch)
        drop(actor_tx);

        // Actor task should wait for the hedged batch to complete (at 500ms) before finishing
        actor_task.await?;

        let res = publish_rx.await??;
        assert_eq!(res, "shutdown_msg");
        assert_eq!(start.elapsed(), Duration::from_millis(500));

        Ok(())
    }

    #[tokio::test]
    async fn batch_resolve_publish_futures_success() {
        let (tx1, rx1) = tokio::sync::oneshot::channel();
        let (tx2, rx2) = tokio::sync::oneshot::channel();
        let resp = Ok(PublishResponse::new()
            .set_message_ids(vec!["msg-id-1".to_string(), "msg-id-2".to_string()]));

        let res = super::batch_resolve_publish_futures(resp, vec![tx1, tx2]);
        assert!(res.is_ok());

        assert_eq!(rx1.await.unwrap().unwrap(), "msg-id-1");
        assert_eq!(rx2.await.unwrap().unwrap(), "msg-id-2");
    }

    #[tokio::test]
    async fn batch_resolve_publish_futures_error() {
        let (tx1, rx1) = tokio::sync::oneshot::channel();
        let (tx2, rx2) = tokio::sync::oneshot::channel();
        let err = crate::Error::service(
            google_cloud_gax::error::rpc::Status::default()
                .set_code(google_cloud_gax::error::rpc::Code::Unavailable)
                .set_message("unavailable"),
        );

        let res = super::batch_resolve_publish_futures(Err(err), vec![tx1, tx2]);
        assert!(res.is_err());

        let res1 = rx1.await.unwrap();
        let res2 = rx2.await.unwrap();
        assert!(matches!(res1, Err(PublishError::Rpc(_))));
        assert!(matches!(res2, Err(PublishError::Rpc(_))));
    }

    #[tokio::test]
    async fn batch_resolve_publish_futures_dropped_receiver() {
        let (tx1, rx1) = tokio::sync::oneshot::channel();
        let (tx2, rx2) = tokio::sync::oneshot::channel();
        drop(rx1); // receiver dropped before batch completes

        let resp = Ok(PublishResponse::new()
            .set_message_ids(vec!["msg-id-1".to_string(), "msg-id-2".to_string()]));

        // Should not panic or fail when one receiver is dropped
        let res = super::batch_resolve_publish_futures(resp, vec![tx1, tx2]);
        assert!(res.is_ok());
        assert_eq!(rx2.await.unwrap().unwrap(), "msg-id-2");

        // Dropped receiver on error path
        let (tx3, rx3) = tokio::sync::oneshot::channel();
        let (tx4, rx4) = tokio::sync::oneshot::channel();
        drop(rx3);

        let err = crate::Error::service(
            google_cloud_gax::error::rpc::Status::default()
                .set_code(google_cloud_gax::error::rpc::Code::Internal)
                .set_message("internal error"),
        );
        let res_err = super::batch_resolve_publish_futures(Err(err), vec![tx3, tx4]);
        assert!(res_err.is_err());
        assert!(matches!(rx4.await.unwrap(), Err(PublishError::Rpc(_))));
    }

    #[tokio::test]
    async fn send_attaches_telemetry_header() {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let mut mock = MockGapicPublisher::new();
        mock.expect_publish()
            .withf(|_req, options| {
                let telemetry = parse_pubsub_client_telemetry_header(options)
                    .expect("telemetry header should be present and valid");
                match telemetry.operation {
                    Some(Operation::PublishOperation(op)) => {
                        op.hedged_attempt_count == 0 && op.publish_start_time.is_some()
                    }
                    _ => false,
                }
            })
            .return_once(|_, _| {
                Ok(crate::Response::from(
                    PublishResponse::new().set_message_ids(["msg-1"]),
                ))
            });

        let client = GapicPublisher::from_stub(mock);
        let mut inflight = tokio::task::JoinSet::new();
        super::send(
            vec![Message::new().set_data("test")],
            vec![tx],
            client,
            "topic".to_string(),
            &mut inflight,
        );

        let _ = inflight.join_next().await;
        assert_eq!(rx.await.unwrap().unwrap(), "msg-1");
    }
}
