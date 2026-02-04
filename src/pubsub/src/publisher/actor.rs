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
    pending_msgs: VecDeque<BundledMessage>,
    paused: bool,
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
            pending_msgs: VecDeque::new(),
            paused: false,
        }
    }

    fn at_batch_threshold(&mut self) -> bool {
        // TODO(#4012): When the message increases the batch size beyond pubsub server acceptable
        // message size (10MB), we should flush existing batch first.
        self.pending_batch.len() as u32 >= self.batching_options.message_count_threshold
            || self.pending_batch.size() >= self.batching_options.byte_threshold
    }

    // Move pending messages to the pending batch respecting batch thresholds.
    pub(crate) fn move_to_batch(&mut self) {
        while let Some(publish) = self.pending_msgs.pop_front() {
            self.pending_batch.push(publish);
            if self.at_batch_threshold() {
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
        let mut inflight = JoinSet::new();
        // For messages without an ordering key, we can have multiple inflight batches concurrently.
        loop {
            tokio::select! {
                _ = inflight.join_next(), if !inflight.is_empty() => {
                    continue;
                }
                msg = self.context.rx.recv() => {
                    match msg {
                        Some(ToBatchActor::Publish(msg)) => {
                            self.context.pending_msgs.push_back(msg);
                            self.context.move_to_batch();
                            if self.context.at_batch_threshold() {
                                self.context.pending_batch.flush(self.context.client.clone(), self.context.topic.clone(), &mut inflight);
                            }
                        },
                        Some(ToBatchActor::Flush(tx)) => {
                            // Send all the batches concurrently.
                            self.flush(inflight).await;
                            inflight = JoinSet::new();
                            let _ = tx.send(());
                        },
                        Some(ToBatchActor::ResumePublish()) => {
                            // Nothing to resume as we do not pause without ordering key.
                        }
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

    // Flush the pending batch and pending messages by sending remaining
    // messages in concurrent batches.
    async fn flush(&mut self, mut inflight: JoinSet<Result<(), gax::error::Error>>) {
        while !self.context.pending_batch.is_empty() || !self.context.pending_msgs.is_empty() {
            self.context.move_to_batch();
            self.context.pending_batch.flush(
                self.context.client.clone(),
                self.context.topic.clone(),
                &mut inflight,
            );
        }
        inflight.join_all().await;
    }
}

/// A batch actor that sends batches sequentially by awaiting on the previous batch.
#[derive(Debug)]
pub(crate) struct SequentialBatchActor {
    context: BatchActorContext,
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
            if self.context.paused {
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
                        self.context.paused = false;
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
                    self.context.handle_inflight_join(join);
                    self.context.move_to_batch();
                    if self.context.at_batch_threshold() {
                        self.context.pending_batch.flush(self.context.client.clone(), self.context.topic.clone(), &mut inflight);
                    }
                }
                msg = self.context.rx.recv() => {
                    match msg {
                        Some(ToBatchActor::Publish(msg)) => {
                            self.context.pending_msgs.push_back(msg);
                            if inflight.is_empty() {
                                self.context.move_to_batch();
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
        self.context
            .handle_inflight_join(inflight.join_next().await);
        while !self.context.pending_batch.is_empty() || !self.context.pending_msgs.is_empty() {
            self.context.move_to_batch();
            self.context.pending_batch.flush(
                self.context.client.clone(),
                self.context.topic.clone(),
                &mut inflight,
            );
            self.context
                .handle_inflight_join(inflight.join_next().await);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{ConcurrentBatchActor, SequentialBatchActor};
    use crate::{
        generated::gapic_dataplane::client::Publisher as GapicPublisher,
        publisher::options::BatchingOptions,
    };

    mockall::mock! {
        #[derive(Debug)]
        GapicPublisher {}
        impl crate::generated::gapic_dataplane::stub::Publisher for GapicPublisher {
            async fn publish(&self, req: crate::model::PublishRequest, _options: gax::options::RequestOptions) -> gax::Result<gax::response::Response<crate::model::PublishResponse>>;
        }
    }

    #[tokio::test]
    async fn basic() -> anyhow::Result<()> {
        let client = GapicPublisher::from_stub(MockGapicPublisher::new());
        let batching_options = BatchingOptions::default();

        let (_, rx) = tokio::sync::mpsc::unbounded_channel();
        let _ = ConcurrentBatchActor::new(
            "topic".to_string(),
            client.clone(),
            batching_options.clone(),
            rx,
        );

        let (_, rx) = tokio::sync::mpsc::unbounded_channel();
        let _ = SequentialBatchActor::new("topic".to_string(), client, batching_options, rx);
        Ok(())
    }
}
