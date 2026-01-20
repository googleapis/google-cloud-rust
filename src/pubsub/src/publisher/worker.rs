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
use std::collections::{HashMap, VecDeque};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinSet;

/// A command sent from the `Publisher` to the background `Worker`.
pub(crate) enum ToWorker {
    /// A request to publish a single message.
    Publish(BundledMessage),
    /// A request to flush all outstanding messages.
    Flush(oneshot::Sender<()>),
    // TODO(#4015): Add a resume function to allow resume Publishing on a ordering key after a
    // failure.
}

/// A command sent from the `Worker` to the background `batch` worker.
pub(crate) enum ToBatchWorker {
    /// A request to publish a single message.
    Publish(BundledMessage),
    /// A request to flush all outstanding messages.
    Flush(oneshot::Sender<()>),
    // TODO(#4015): Add a resume function to allow resume Publishing on a ordering key after a
    // failure.
}

/// Object that is passed to the worker task over the
/// main channel. This represents a single message and the sender
/// half of the channel to resolve the [PublishHandle].
#[derive(Debug)]
pub(crate) struct BundledMessage {
    pub msg: crate::model::PubsubMessage,
    pub tx: oneshot::Sender<std::result::Result<String, crate::error::PublishError>>,
}

/// The worker is spawned in a background task and handles
/// batching and publishing all messages that are sent to the publisher.
#[derive(Debug)]
pub(crate) struct Worker {
    topic_name: String,
    client: GapicPublisher,
    #[allow(dead_code)]
    batching_options: BatchingOptions,
    rx: mpsc::UnboundedReceiver<ToWorker>,
}

impl Worker {
    pub(crate) fn new(
        topic_name: String,
        client: GapicPublisher,
        batching_options: BatchingOptions,
        rx: mpsc::UnboundedReceiver<ToWorker>,
    ) -> Self {
        Self {
            topic_name,
            client,
            rx,
            batching_options,
        }
    }

    /// The main loop of the background worker.
    ///
    /// This method concurrently handles four main events:
    ///
    /// 1. Publish command from the `Publisher` is demultiplexed to the batch worker
    ///    for its ordering key.
    /// 2. A Flush command from the `Publisher` causes all batch workers to flush
    ///    all pending messages.
    /// 2. A timer fire causes the Worker to flush all pending batches.
    ///
    /// The loop terminates when the `rx` channel is closed, which happens when all
    /// `Publisher` clones have been dropped.
    pub(crate) async fn run(mut self) {
        // A dictionary of ordering key to outstanding publish operations.
        // We batch publish operations on the same ordering key together.
        // Publish without ordering keys are treated as having the key "".
        // TODO(#4012): Remove batch workers when there are no outstanding operations on the ordering key.
        let mut batch_workers: HashMap<String, mpsc::UnboundedSender<ToBatchWorker>> =
            HashMap::new();
        let delay = self.batching_options.delay_threshold;
        let batch_worker_error_msg = "Batch worker should not close the channel";

        let timer = tokio::time::sleep(delay);
        // Pin the timer to the stack.
        tokio::pin!(timer);
        loop {
            tokio::select! {
                // Currently, the batch worker periodically flush on a shared timer. If needed,
                // this can be moved into the batch worker such that each are running on a
                // separate timer.
                _ = &mut timer => {
                    for (_, batch_worker) in batch_workers.iter_mut() {
                        let (tx, _) = oneshot::channel();
                        batch_worker
                            .send(ToBatchWorker::Flush(tx))
                            .expect(batch_worker_error_msg);
                    }
                    timer.as_mut().reset(tokio::time::Instant::now() + delay);
                }
                // Handle receiving a message from the channel.
                msg = self.rx.recv() => {
                    match msg {
                        Some(ToWorker::Publish(msg)) => {
                            let ordering_key = msg.msg.ordering_key.clone();
                            let batch_worker =
                                batch_workers
                                    .entry(ordering_key.clone())
                                    .or_insert({
                                        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
                                        let batch_worker = BatchWorker::new(
                                                        self.topic_name.clone(),
                                                        ordering_key,
                                                        self.client.clone(),
                                                        self.batching_options.clone(),
                                                        rx,
                                                );
                                        tokio::spawn(batch_worker.run());
                                        tx
                                });
                            batch_worker
                                .send(ToBatchWorker::Publish(msg))
                                .expect(batch_worker_error_msg);
                        },
                        Some(ToWorker::Flush(tx)) => {
                            let mut flush_set = JoinSet::new();
                            for (_, batch_worker) in batch_workers.iter_mut() {
                                let (tx, rx) = oneshot::channel();
                                batch_worker
                                    .send(ToBatchWorker::Flush(tx))
                                    .expect(batch_worker_error_msg);
                                flush_set.spawn(rx);
                            }
                            // Wait on all the tasks that exist right now.
                            // We could instead tokio::spawn this as well so the publisher
                            // can keep working on additional messages. The worker would
                            // also need to keep track of any pending flushes, and make sure
                            // all of those resolve as well.
                            flush_set.join_all().await;
                            let _ = tx.send(());
                        },
                        None => {
                            // The sender has been dropped send batch and stop running.
                            // This isn't guaranteed to execute if a user does not .await on the
                            // corresponding PublishHandles for the batch and the program ends.
                            let mut flush_set = JoinSet::new();
                            for (_, batch_worker) in batch_workers.iter_mut() {
                                let (tx, rx) = oneshot::channel();
                                batch_worker
                                    .send(ToBatchWorker::Flush(tx))
                                    .expect(batch_worker_error_msg);
                                flush_set.spawn(rx);
                            }
                            flush_set.join_all().await;
                            break;
                        }
                    }
                }

            }
        }
    }
}

/// A background worker that continuously handles Publisher commands for a specific ordering key.
#[derive(Debug)]
pub(crate) struct BatchWorker {
    topic: String,
    ordering_key: String,
    client: GapicPublisher,
    batching_options: BatchingOptions,
    rx: mpsc::UnboundedReceiver<ToBatchWorker>,
    pending_batch: Batch,
    pending_msgs: VecDeque<BundledMessage>,
    paused: bool,
}

impl BatchWorker {
    pub(crate) fn new(
        topic: String,
        ordering_key: String,
        client: GapicPublisher,
        batching_options: BatchingOptions,
        rx: mpsc::UnboundedReceiver<ToBatchWorker>,
    ) -> Self {
        BatchWorker {
            pending_batch: Batch::new(topic.len() as u32),
            topic,
            ordering_key,
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

    /// The main loop of the batch worker.
    ///
    /// This method concurrently handles the following events:
    ///
    /// 1. A Publish command from the `Worker` causes the new message to be
    ///    added a pending message queue. If there is enough message to create
    ///    a full batch, then also flush the batch.
    /// 2. A `Flush` command from the `Publisher` causes all the pending messages
    ///    to be flushed respecting the configured batch size and message ordering.
    /// 4. A `inflight` batch completion causes the next batch to send if it satisfies
    ///    the configured batch threshold.
    ///
    /// The loop terminates when the `rx` channel is closed, which happens when the
    /// `Worker` drops the Sender.
    pub(crate) async fn run(mut self) {
        if self.ordering_key.is_empty() {
            self.run_without_ordering_key().await;
        } else {
            self.run_with_ordering_key().await;
        }
    }

    async fn run_without_ordering_key(&mut self) {
        let mut inflight = JoinSet::new();
        // For messages without an ordering key, we can have multiple inflight batches concurrently.
        loop {
            tokio::select! {
                _ = inflight.join_next(), if !inflight.is_empty() => {
                    continue;
                }
                msg = self.rx.recv() => {
                    match msg {
                        Some(ToBatchWorker::Publish(msg)) => {
                            self.pending_msgs.push_back(msg);
                            self.move_to_batch();
                            if self.at_batch_threshold() {
                                self.pending_batch.flush(self.client.clone(), self.topic.clone(), &mut inflight);
                            }
                        },
                        Some(ToBatchWorker::Flush(tx)) => {
                            // Send all the batches concurrently.
                            while !self.pending_batch.is_empty() || !self.pending_msgs.is_empty() {
                                self.move_to_batch();
                                self.pending_batch.flush(self.client.clone(), self.topic.clone(), &mut inflight);
                            }
                            inflight.join_all().await;
                            inflight = JoinSet::new();
                            let _ = tx.send(());
                        },
                        None => {
                            // TODO(#4012): Add shutdown procedure for BatchWorker.
                            break;
                        }
                    }
                }
            }
        }
    }

    pub(crate) fn handle_inflight_join(
        &mut self,
        join_next_option: Option<Result<Result<(), gax::error::Error>, tokio::task::JoinError>>,
    ) {
        match join_next_option {
            Some(join_next_result) => {
                match join_next_result {
                    Ok(inflight_result) => {
                        match inflight_result {
                            Ok(_) => {}
                            Err(_) => {
                                // There was a non-retryable error:
                                // 1. We need to pause publishing and send out errors for pending_msgs.
                                // 2. The pending batch should have sent out error for its messages.
                                // 3. The messages in rx will be handled when they are received.
                                self.pause();
                            }
                        }
                    }
                    Err(_) => {
                        // JoinError.
                        // This is unexpected and we should pause the publisher.
                        self.pause();
                    }
                }
            }
            None => {}
        }
    }

    async fn run_with_ordering_key(&mut self) {
        // While it is possible to use Some(JoinHandle) here as there is at max
        // a single inflight task at any given time, the use of JoinSet
        // simplify the managing the inflight JoinHandle.
        let mut inflight: JoinSet<Result<(), gax::error::Error>> = JoinSet::new();
        loop {
            tokio::select! {
                join = inflight.join_next(), if !inflight.is_empty() => {
                    self.handle_inflight_join(join);
                    self.move_to_batch();
                    if self.at_batch_threshold() {
                        self.pending_batch.flush(self.client.clone(), self.topic.clone(), &mut inflight);
                    }
                }
                msg = self.rx.recv() => {
                    match msg {
                        Some(ToBatchWorker::Publish(msg)) => {
                            if self.paused {
                                // The user may have dropped the handle, so it is ok if this fails.
                                let _ = msg.tx.send(Err(crate::error::PublishError::OrderingKeyPaused(())));
                                continue;
                            }
                            self.pending_msgs.push_back(msg);
                            if inflight.is_empty() {
                                self.move_to_batch();
                                if self.at_batch_threshold() {
                                    self.pending_batch.flush(self.client.clone(), self.topic.clone(), &mut inflight);
                                }
                            }
                        },
                        Some(ToBatchWorker::Flush(tx)) => {
                            // Send batches sequentially.
                            self.handle_inflight_join(inflight.join_next().await);
                            while !self.pending_batch.is_empty() || !self.pending_msgs.is_empty() {
                                self.move_to_batch();
                                self.pending_batch.flush(self.client.clone(), self.topic.clone(), &mut inflight);
                                self.handle_inflight_join(inflight.join_next().await);
                            }
                            let _ = tx.send(());
                        },
                        None => {
                            // TODO(#4012): Add shutdown procedure for BatchWorker.
                            break;
                        }
                    }
                }
            }
        }
    }
}
