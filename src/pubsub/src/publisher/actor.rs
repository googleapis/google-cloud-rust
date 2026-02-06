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
use crate::generated::gapic_dataplane::client::Publisher as GapicPublisher;
use crate::publisher::batch::Batch;
use crate::publisher::constants;
use std::collections::{HashMap, VecDeque};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinSet;

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
pub(crate) enum ToBatchActor {
    /// A request to publish a single message.
    Publish(BundledMessage),
    /// A request to flush all outstanding messages.
    Flush(oneshot::Sender<()>),
    /// A request to resume publishing.
    ResumePublish(),
}

/// Object that is passed to the actor tasks over the
/// main channel. This represents a single message and the sender
/// half of the channel to resolve the [PublishHandle].
#[derive(Debug)]
pub(crate) struct BundledMessage {
    pub msg: crate::model::PubsubMessage,
    pub tx: oneshot::Sender<std::result::Result<String, crate::error::PublishError>>,
}

/// The Dispatcher runs in a background task and handles all Publisher operations
/// by dispatching it to BatchActors.
#[derive(Debug)]
pub(crate) struct Dispatcher {
    topic_name: String,
    client: GapicPublisher,
    #[allow(dead_code)]
    batching_options: BatchingOptions,
    rx: mpsc::UnboundedReceiver<ToDispatcher>,
}

impl Dispatcher {
    pub(crate) fn new(
        topic_name: String,
        client: GapicPublisher,
        batching_options: BatchingOptions,
        rx: mpsc::UnboundedReceiver<ToDispatcher>,
    ) -> Self {
        Self {
            topic_name,
            client,
            rx,
            batching_options,
        }
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
    /// 4. A timer fire causes the Dispatcher to flush all BatchActors.
    ///
    /// The loop terminates when the `rx` channel is closed, which happens when all
    /// `Publisher` clones have been dropped.
    pub(crate) async fn run(mut self) {
        // A dictionary of ordering key to outstanding publish operations.
        // We batch publish operations on the same ordering key together.
        // Publish without ordering keys are treated as having the key "".
        // TODO(#4012): Remove batch actors when there are no outstanding operations on the ordering key.
        let mut batch_actors: HashMap<String, mpsc::UnboundedSender<ToBatchActor>> = HashMap::new();
        let delay = self.batching_options.delay_threshold;

        let timer = tokio::time::sleep(delay);
        // Pin the timer to the stack.
        tokio::pin!(timer);
        loop {
            tokio::select! {
                // Currently, the Dispatcher periodically flushes all batches on a shared timer.
                // If needed, this can be moved into the batch actors such that each are running
                // on a separate timer.
                _ = &mut timer => {
                    for (_, batch_actor) in batch_actors.iter_mut() {
                        let (tx, _) = oneshot::channel();
                        batch_actor
                            .send(ToBatchActor::Flush(tx))
                            .expect(constants::BATCH_ACTOR_SEND_ERROR_MSG);
                    }
                    timer.as_mut().reset(tokio::time::Instant::now() + delay);
                }
                // Handle receiving a message from the channel.
                msg = self.rx.recv() => {
                    match msg {
                        Some(ToDispatcher::Publish(msg)) => {
                            let ordering_key = msg.msg.ordering_key.clone();
                            let batch_actor = batch_actors
                                .entry(ordering_key.clone())
                                .or_insert_with(|| {
                                    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
                                    match ordering_key.as_str() {
                                        "" => {
                                            tokio::spawn(
                                                ConcurrentBatchActor::new(
                                                    self.topic_name.clone(),
                                                    self.client.clone(),
                                                    self.batching_options.clone(),
                                                    rx,
                                                )
                                                .run(),
                                            );
                                        },
                                        _ => {
                                            tokio::spawn(
                                                SequentialBatchActor::new(
                                                    self.topic_name.clone(),
                                                    self.client.clone(),
                                                    self.batching_options.clone(),
                                                    rx,
                                                )
                                                .run(),
                                            );
                                        }
                                    }
                                    tx
                                });
                            batch_actor
                                .send(ToBatchActor::Publish(msg))
                                .expect(constants::BATCH_ACTOR_SEND_ERROR_MSG);
                        },
                        Some(ToDispatcher::Flush(tx)) => {
                            let mut flush_set = JoinSet::new();
                            for (_, batch_actor) in batch_actors.iter_mut() {
                                let (tx, rx) = oneshot::channel();
                                batch_actor
                                    .send(ToBatchActor::Flush(tx))
                                    .expect(constants::BATCH_ACTOR_SEND_ERROR_MSG);
                                flush_set.spawn(rx);
                            }
                            // Wait on all the tasks that exist right now.
                            // TODO(#4505): We could instead tokio::spawn this as well so the
                            // publisher can keep working on additional messages. The Dispatcher
                            // would also need to keep track of any pending flushes, and make sure
                            // all of those resolve as well.
                            flush_set.join_all().await;
                            let _ = tx.send(());
                        },
                        Some(ToDispatcher::ResumePublish(ordering_key)) => {
                            if let Some(batch_actor) = batch_actors.get_mut(&ordering_key) {
                                // Send down the same tx for the BatchActors to directly signal completion
                                // instead of spawning a new task.
                                batch_actor
                                    .send(ToBatchActor::ResumePublish())
                                    .expect(constants::BATCH_ACTOR_SEND_ERROR_MSG);
                            }
                        }
                        None => {
                            // Gracefully shutdown since the Publisher has dropped the Sender.
                            // By dropping the batch actor Senders, they will individually handle the
                            // shutdown procedures.
                            break;
                        }
                    }
                }

            }
        }
    }
}

#[derive(Debug)]
pub(crate) struct BatchActorContext {
    topic: String,
    client: GapicPublisher,
    batching_options: BatchingOptions,
    rx: mpsc::UnboundedReceiver<ToBatchActor>,
    pending_batch: Batch,
}

impl BatchActorContext {
    pub(crate) fn new(
        topic: String,
        client: GapicPublisher,
        batching_options: BatchingOptions,
        rx: mpsc::UnboundedReceiver<ToBatchActor>,
    ) -> Self {
        BatchActorContext {
            pending_batch: Batch::new(topic.len() as u32),
            topic,
            client,
            batching_options,
            rx,
        }
    }

    fn at_batch_threshold(&mut self) -> bool {
        // TODO(#4012): When the message increases the batch size beyond pubsub server acceptable
        // message size (10MB), we should flush existing batch first.
        self.pending_batch.len() as u32 >= self.batching_options.message_count_threshold
            || self.pending_batch.size() >= self.batching_options.byte_threshold
    }
}

/// A batch actor that sends batches concurrently.
#[derive(Debug)]
pub(crate) struct ConcurrentBatchActor {
    context: BatchActorContext,
}

impl ConcurrentBatchActor {
    pub(crate) fn new(
        topic: String,
        client: GapicPublisher,
        batching_options: BatchingOptions,
        rx: mpsc::UnboundedReceiver<ToBatchActor>,
    ) -> Self {
        ConcurrentBatchActor {
            context: BatchActorContext::new(topic, client, batching_options, rx),
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
    pub(crate) async fn run(mut self) {
        // We have multiple inflight batches concurrently.
        let mut inflight = JoinSet::new();
        loop {
            tokio::select! {
                // Remove completed inflight batches.
                _ = inflight.join_next(), if !inflight.is_empty() => {
                    continue;
                }
                msg = self.context.rx.recv() => {
                    match msg {
                        Some(ToBatchActor::Publish(msg)) => {
                            self.move_to_batch(msg);
                            // Flush without awaiting on previous batches.
                            if self.context.at_batch_threshold() {
                                self.flush(&mut inflight);
                            }
                        },
                        Some(ToBatchActor::Flush(tx)) => {
                            self.flush(&mut inflight);
                            inflight.join_all().await;
                            inflight = JoinSet::new();
                            let _ = tx.send(());
                        },
                        Some(ToBatchActor::ResumePublish()) => {
                            // Nothing to resume as we do not pause without ordering key.
                        }
                        None => {
                            // This isn't guaranteed to execute if a user does not .await on the
                            // corresponding PublishHandles.
                            self.flush(&mut inflight);
                            inflight.join_all().await;
                            break;
                        }
                    }
                }
            }
        }
    }

    // Flush the pending batch if it's not empty.
    pub(crate) fn flush(&mut self, inflight: &mut JoinSet<Result<(), gax::error::Error>>) {
        if !self.context.pending_batch.is_empty() {
            self.context.pending_batch.flush(
                self.context.client.clone(),
                self.context.topic.clone(),
                inflight,
            );
        }
    }

    // Move msg to the pending batch.
    pub(crate) fn move_to_batch(&mut self, msg: BundledMessage) {
        self.context.pending_batch.push(msg);
    }
}

/// A batch actor that sends batches sequentially by awaiting on the previous batch.
#[derive(Debug)]
pub(crate) struct SequentialBatchActor {
    context: BatchActorContext,
    pending_msgs: VecDeque<BundledMessage>,
    paused: bool,
}

impl SequentialBatchActor {
    pub(crate) fn new(
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
    pub(crate) async fn run(mut self) {
        // While it is possible to use Some(JoinHandle) here as there is at max
        // a single inflight task at any given time, the use of JoinSet
        // simplify the managing the inflight JoinHandle.
        let mut inflight: JoinSet<Result<(), gax::error::Error>> = JoinSet::new();
        loop {
            if self.paused {
                // When paused, we do not need to check inflight as handle_inflight_join()
                // ensures that there are no inflight batch.
                let msg = self.context.rx.recv().await;
                match msg {
                    Some(ToBatchActor::Publish(msg)) => {
                        let _ = msg
                            .tx
                            .send(Err(crate::error::PublishError::OrderingKeyPaused(())));
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
                    self.move_to_batch();
                    if self.context.at_batch_threshold() {
                        self.context.pending_batch.flush(self.context.client.clone(), self.context.topic.clone(), &mut inflight);
                    }
                }
                msg = self.context.rx.recv() => {
                    match msg {
                        Some(ToBatchActor::Publish(msg)) => {
                            self.pending_msgs.push_back(msg);
                            if inflight.is_empty() {
                                self.move_to_batch();
                                if self.context.at_batch_threshold() {
                                    self.context.pending_batch.flush(self.context.client.clone(), self.context.topic.clone(), &mut inflight);
                                }
                            }
                        },
                        Some(ToBatchActor::Flush(tx)) => {
                            self.flush(inflight).await;
                            inflight = JoinSet::new();
                            let _ = tx.send(());
                        },
                        Some(ToBatchActor::ResumePublish()) => {
                            // Nothing to resume as we are not paused.
                        },
                        None => {
                            // This isn't guaranteed to execute if a user does not .await on the
                            // corresponding PublishHandles.
                            self.flush(inflight).await;
                            break;
                        }
                    }
                }
            }
        }
    }

    // Flush the pending messages by sending the messages in sequential batches.
    async fn flush(&mut self, mut inflight: JoinSet<Result<(), gax::error::Error>>) {
        self.handle_inflight_join(inflight.join_next().await);
        while !self.context.pending_batch.is_empty() || !self.pending_msgs.is_empty() {
            self.move_to_batch();
            self.context.pending_batch.flush(
                self.context.client.clone(),
                self.context.topic.clone(),
                &mut inflight,
            );
            self.handle_inflight_join(inflight.join_next().await);
        }
    }

    // Move pending messages to the pending batch respecting batch thresholds.
    pub(crate) fn move_to_batch(&mut self) {
        while let Some(publish) = self.pending_msgs.pop_front() {
            self.context.pending_batch.push(publish);
            if self.context.at_batch_threshold() {
                break;
            }
        }
    }

    // Pause publish operations.
    pub(crate) fn pause(&mut self) {
        self.paused = true;
        while let Some(publish) = self.pending_msgs.pop_front() {
            // The user may have dropped the handle, so it is ok if this fails.
            let _ = publish
                .tx
                .send(Err(crate::error::PublishError::OrderingKeyPaused(())));
        }
    }

    pub(crate) fn handle_inflight_join(
        &mut self,
        join_next_option: Option<Result<Result<(), gax::error::Error>, tokio::task::JoinError>>,
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

#[cfg(test)]
mod tests {
    use super::{ConcurrentBatchActor, SequentialBatchActor};
    use crate::error::PublishError;
    use crate::publisher::actor::{BundledMessage, ToBatchActor};
    use crate::publisher::options::BatchingOptions;
    use crate::{
        generated::gapic_dataplane::client::Publisher as GapicPublisher,
        model::{PublishResponse, PubsubMessage},
    };
    use mockall::Sequence;
    use rand::{Rng, distr::Alphanumeric};
    use std::collections::HashMap;
    use std::time::Duration;

    static TOPIC: &str = "my-topic";
    const EXPECTED_BATCHES: usize = 5;
    const TIME_PER_BATCH: Duration = Duration::from_secs(10);

    mockall::mock! {
        #[derive(Debug)]
        GapicPublisher {}
        impl crate::generated::gapic_dataplane::stub::Publisher for GapicPublisher {
            async fn publish(&self, req: crate::model::PublishRequest, _options: gax::options::RequestOptions) -> gax::Result<gax::response::Response<crate::model::PublishResponse>>;
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
            fn publish(&self, req: crate::model::PublishRequest, _options: gax::options::RequestOptions) -> impl Future<Output=gax::Result<gax::response::Response<crate::model::PublishResponse>>> + Send;
        }
    }

    fn publish_ok(
        req: crate::model::PublishRequest,
        _options: gax::options::RequestOptions,
    ) -> gax::Result<gax::response::Response<crate::model::PublishResponse>> {
        let ids = req
            .messages
            .iter()
            .map(|m| String::from_utf8(m.data.to_vec()).unwrap());
        Ok(gax::response::Response::from(
            PublishResponse::new().set_message_ids(ids),
        ))
    }

    fn publish_err(
        _req: crate::model::PublishRequest,
        _options: gax::options::RequestOptions,
    ) -> gax::Result<gax::response::Response<crate::model::PublishResponse>> {
        Err(gax::error::Error::service(
            gax::error::rpc::Status::default()
                .set_code(gax::error::rpc::Code::Unknown)
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

    // Send ToBatchActor::Publish with random data n times then await and assert the result.
    macro_rules! assert_publish_is_ok {
        ($actor_tx:ident, $n:expr) => {
            let mut publish_rxs = HashMap::new();
            for _ in 0..$n {
                let (publish_tx, publish_rx) = tokio::sync::oneshot::channel();
                let msg = generate_random_data();
                let bundle = BundledMessage {
                    msg: PubsubMessage::new().set_data(msg.clone()),
                    tx: publish_tx,
                };
                $actor_tx.send(ToBatchActor::Publish(bundle))?;
                publish_rxs.insert(msg, publish_rx);
            }
            for (k, v) in publish_rxs {
                let res = v.await;
                assert!(matches!(res, Ok(Ok(ref msg)) if *msg == k), "got {res:?}, expected {k:?}");
            }
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
                    msg: PubsubMessage::new().set_data(generate_random_data()),
                    tx: publish_tx,
                };
                $actor_tx.send(ToBatchActor::Publish(bundle))?;
                publish_rxs.push(publish_rx);
            }
            for v in publish_rxs {
                let res = v.await;
                assert!(
                    matches!(res, Ok(Err(PublishError::OrderingKeyPaused(())))),
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
            rx,
        );

        let (_, rx) = tokio::sync::mpsc::unbounded_channel();
        let _ = SequentialBatchActor::new("topic".to_string(), client, batching_options, rx);
        Ok(())
    }

    #[tokio::test(start_paused = true)]
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

    #[tokio::test(start_paused = true)]
    async fn sequential_actor_publish() -> anyhow::Result<()> {
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
            SequentialBatchActor::new(
                TOPIC.to_string(),
                GapicPublisher::from_stub(mock),
                BatchingOptions::default().set_message_count_threshold(2_u32),
                actor_rx,
            )
            .run(),
        );

        let start = tokio::time::Instant::now();
        assert_publish_is_ok!(actor_tx, 10);
        assert_eq!(
            start.elapsed(),
            EXPECTED_BATCHES as u32 * TIME_PER_BATCH,
            "all batches should have been seqentially sent and takes {:?}",
            EXPECTED_BATCHES as u32 * TIME_PER_BATCH
        );
        Ok(())
    }

    #[tokio::test(start_paused = true)]
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
                actor_rx,
            )
            .run(),
        );

        // Flush on empty.
        assert_flush!(actor_tx);

        // Publish 10 messages then Flush.
        let start = tokio::time::Instant::now();
        let mut publish_rxs = HashMap::new();
        for _ in 0..10 {
            let (publish_tx, publish_rx) = tokio::sync::oneshot::channel();
            let msg = generate_random_data();
            let bundle = BundledMessage {
                msg: PubsubMessage::new().set_data(msg.clone()),
                tx: publish_tx,
            };
            actor_tx.send(ToBatchActor::Publish(bundle))?;
            publish_rxs.insert(msg, publish_rx);
        }
        assert_flush!(actor_tx);
        for (k, v) in publish_rxs {
            let res = v.await;
            assert!(
                matches!(res, Ok(Ok(ref msg)) if *msg == k),
                "got {res:?}, expected {k:?}"
            );
        }
        assert_eq!(
            start.elapsed(),
            TIME_PER_BATCH,
            "all batches should have been concurrently sent and completed by {:?}",
            TIME_PER_BATCH
        );

        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn sequential_actor_flush() -> anyhow::Result<()> {
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
        let mut publish_rxs = HashMap::new();
        for _ in 0..10 {
            let (publish_tx, publish_rx) = tokio::sync::oneshot::channel();
            let msg = generate_random_data();
            let bundle = BundledMessage {
                msg: PubsubMessage::new().set_data(msg.clone()),
                tx: publish_tx,
            };
            actor_tx.send(ToBatchActor::Publish(bundle))?;
            publish_rxs.insert(msg, publish_rx);
        }
        assert_flush!(actor_tx);
        for (k, v) in publish_rxs {
            let res = v.await;
            assert!(
                matches!(res, Ok(Ok(ref msg)) if *msg == k),
                "got {res:?}, expected {k:?}"
            );
        }
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
                actor_rx,
            )
            .run(),
        );

        actor_tx.send(ToBatchActor::ResumePublish())?;
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn sequential_actor_resume() -> anyhow::Result<()> {
        let mut mock = MockGapicPublisher::new();
        let mut seq = Sequence::new();
        mock.expect_publish()
            .withf(|req, _o| req.topic == TOPIC)
            .times(5)
            .in_sequence(&mut seq)
            .returning(publish_ok);
        mock.expect_publish()
            .withf(|req, _o| req.topic == TOPIC)
            .times(1)
            .in_sequence(&mut seq)
            .returning(publish_err);
        mock.expect_publish()
            .withf(|req, _o| req.topic == TOPIC)
            .times(5)
            .in_sequence(&mut seq)
            .returning(publish_ok);

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
        assert_publish_is_ok!(actor_tx, 5);

        // This message triggers the mock to return publish error and causes the actor to pause.
        let (publish_tx, publish_rx) = tokio::sync::oneshot::channel();
        let bundle = BundledMessage {
            msg: PubsubMessage::new().set_data(generate_random_data()),
            tx: publish_tx,
        };
        actor_tx.send(ToBatchActor::Publish(bundle))?;
        let got_err = publish_rx.await;
        // TODO(#3689): Validate the error structure when Publisher error structure is better defined.
        assert!(matches!(got_err, Ok(Err(_))), "{got_err:?}");
        assert_actor_is_paused!(actor_tx, 5);

        // Resume then validate that the actor is no longer paused.
        actor_tx.send(ToBatchActor::ResumePublish())?;
        assert_publish_is_ok!(actor_tx, 5);

        Ok(())
    }
}
