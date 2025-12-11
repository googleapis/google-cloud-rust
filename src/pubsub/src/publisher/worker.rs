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
use futures::StreamExt as _;
use futures::stream::FuturesUnordered;
use std::collections::HashMap;
use tokio::sync::{mpsc, oneshot};

/// A command sent from the `Publisher` to the background `Worker`.
pub(crate) enum ToWorker {
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
    pub tx: oneshot::Sender<crate::Result<String>>,
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
    /// 1. Messages from the `Publisher` are received from the `rx` channel
    ///    and added to the current `batch`.
    /// 2. A timer is armed when the first message is added to a batch.
    ///    If that timer fires, the batch is sent.
    /// 3. A `Flush` command from the `Publisher` causes the current batch to be
    ///    sent immediately, and all in-flight send tasks to be awaited.
    /// 4. The `inflight` set is continuously polled to remove `JoinHandle`s for
    ///    send tasks that have completed, preventing the set from growing indefinitely.
    ///
    /// The loop terminates when the `rx` channel is closed, which happens when all
    /// `Publisher` clones have been dropped.
    pub(crate) async fn run(mut self) {
        // A dictionary of ordering key to outstanding publish operations.
        // We batch publish operations on the same ordering key together.
        // Publish without ordering keys are treated as having the key "".
        // TODO(#4012): Remove pending_batches entries when there are no outstanding publish.
        let mut pending_batches: HashMap<String, OutstandingPublishes> = HashMap::new();
        let delay = self.batching_options.delay_threshold;
        let message_limit = self.batching_options.message_count_threshold;
        let byte_threshold = self.batching_options.byte_threshold;
        let mut inflight = FuturesUnordered::new();

        let timer = tokio::time::sleep(delay);
        // Pin the timer to the stack.
        tokio::pin!(timer);
        loop {
            tokio::select! {
                // Remove finished futures from the inflight messages.
                _ = inflight.next(), if !inflight.is_empty() => {},
                // Handle timer events.
                _ = &mut timer => {
                    for (_, outstanding) in pending_batches.iter_mut() {
                        if !outstanding.pending_batch.is_empty() {
                            outstanding.pending_batch.flush(self.client.clone(), self.topic_name.clone(), &mut inflight);
                        }
                    }
                    timer.as_mut().reset(tokio::time::Instant::now() + delay);
                }
                // Handle receiving a message from the channel.
                msg = self.rx.recv() => {
                    match msg {
                        Some(ToWorker::Publish(msg)) => {
                            let ordering_key = msg.msg.ordering_key.clone();
                            let outstanding_publishes = pending_batches.entry(ordering_key).or_insert(OutstandingPublishes::new(&self.topic_name));
                            outstanding_publishes.pending_batch.push(msg);
                            if outstanding_publishes.pending_batch.len() as u32 >= message_limit || outstanding_publishes.pending_batch.size() >=  byte_threshold {
                                outstanding_publishes.pending_batch.flush(self.client.clone(), self.topic_name.clone(), &mut inflight);
                            }
                        },
                        Some(ToWorker::Flush(tx)) => {
                            for (_, outstanding) in pending_batches.iter_mut() {
                                // TODO(#4012): To guarantee ordering, we should wait for the
                                // inflight batch to complete so that messages are publish in order.
                                if !outstanding.pending_batch.is_empty() {
                                    outstanding.pending_batch.flush(self.client.clone(), self.topic_name.clone(), &mut inflight);
                                }

                                // Wait on all the tasks that exist right now.
                                // We could instead tokio::spawn this as well so the publisher
                                // can keep working on additional messages. The worker would
                                // also need to keep track of any pending flushes, and make sure
                                // all of those resolve as well.
                                let mut flushing = std::mem::take(&mut inflight);
                                while flushing.next().await.is_some() {}
                            }
                            let _ = tx.send(());
                        },
                        None => {
                            // The sender has been dropped send batch and stop running.
                            // This isn't guaranteed to execute if a user does not .await on the
                            // corresponding PublishHandles for the batch and the program ends.
                            for (_, outstanding) in pending_batches.iter_mut() {
                                outstanding.pending_batch.flush(self.client.clone(), self.topic_name.clone(), &mut inflight);
                            }
                            break;
                        }
                    }
                }

            }
        }
    }
}

#[derive(Debug, Default)]
pub(crate) struct OutstandingPublishes {
    pub(crate) pending_batch: Batch,
    // TODO(#4012): Track pending messages as within key message ordering is
    // not currently respected during a failure.
}

impl OutstandingPublishes {
    pub(crate) fn new(topic: &str) -> Self {
        OutstandingPublishes {
            pending_batch: Batch::new(topic),
        }
    }
}
