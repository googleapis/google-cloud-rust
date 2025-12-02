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
use tokio::sync::{mpsc, oneshot};

/// A command sent from the `Publisher` to the background `Worker`.
pub(crate) enum ToWorker {
    /// A request to publish a single message.
    Publish(BundledMessage),
    /// A request to flush all outstanding messages.
    Flush(oneshot::Sender<()>),
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
        let mut batch = Batch::new(&self.topic_name);
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
                // This branch will only be checked when there is a non-empty batch,
                // so this will not fire continuously.
                _ = &mut timer, if !batch.is_empty() => {
                    batch.flush(self.client.clone(), self.topic_name.clone(), &mut inflight);
                }
                // Handle receiving a message from the channel.
                msg = self.rx.recv() => {
                    match msg {
                        Some(ToWorker::Publish(msg)) => {
                            // Reset the timer if this is the first message to be added to the batch.
                            if batch.is_empty() {
                                timer.as_mut().reset(tokio::time::Instant::now() + delay);
                            }
                            batch.push(msg);
                            if batch.len() as u32 >= message_limit || batch.size() >=  byte_threshold {
                                batch.flush(self.client.clone(), self.topic_name.clone(), &mut inflight);
                            }
                        },
                        Some(ToWorker::Flush(tx)) => {
                            batch.flush(self.client.clone(), self.topic_name.clone(), &mut inflight);
                            // Wait on all the tasks that exist right now.
                            // We could instead tokio::spawn this as well so the publisher
                            // can keep working on additional messages. The worker would
                            // also need to keep track of any pending flushes, and make sure
                            // all of those resolve as well.
                            let mut flushing = std::mem::take(&mut inflight);
                            while flushing.next().await.is_some() {}
                            let _ = tx.send(());
                        },
                        None => {
                            // The sender has been dropped send batch and stop running.
                            // This isn't guaranteed to execute if a user does not .await on the
                            // corresponding PublishHandles for the batch and the program ends.
                            batch.flush(self.client.clone(), self.topic_name.clone(), &mut inflight);
                            break;
                        }
                    }
                }

            }
        }
    }
}
