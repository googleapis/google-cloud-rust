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
/// half of the channel to resolve the [PublishFuture].
#[derive(Debug)]
pub(crate) struct BundledMessage {
    pub msg: crate::model::Message,
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
                    for (_, batch_actor) in batch_actors.iter() {
                        let (tx, _) = oneshot::channel();
                        if batch_actor.send(ToBatchActor::Flush(tx)).is_err() {
                            return; // Stop the dispatcher if a batch actor is dropped.
                        }
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
                            if batch_actor.send(ToBatchActor::Publish(msg)).is_err() {
                                return; // Stop the dispatcher if a batch actor is dropped.
                            }
                        },
                        Some(ToDispatcher::Flush(tx)) => {
                            let mut flush_set = JoinSet::new();
                            for (_, batch_actor) in batch_actors.iter() {
                                let (tx, rx) = oneshot::channel();
                                if batch_actor.send(ToBatchActor::Flush(tx)).is_err() {
                                    return; // Stop the dispatcher if a batch actor is dropped.
                                }
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
                                if batch_actor.send(ToBatchActor::ResumePublish()).is_err() {
                                    return; // Stop the dispatcher if a batch actor is dropped.
                                }
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
    rx: mpsc::UnboundedReceiver<ToBatchActor>,
    batching_options: BatchingOptions,
}

impl BatchActorContext {
    pub(crate) fn new(
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
                msg = self.context.rx.recv() => {
                    match msg {
                        Some(ToBatchActor::Publish(msg)) => {
                            self.add_msg_and_flush(&mut inflight, &mut batch, msg);
                        },
                        Some(ToBatchActor::Flush(tx)) => {
                            self.flush(&mut inflight, &mut batch);
                            inflight.join_all().await;
                            inflight = JoinSet::new();
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
    pub(crate) fn flush(&mut self, inflight: &mut JoinSet<crate::Result<()>>, batch: &mut Batch) {
        if !batch.is_empty() {
            batch.flush(
                self.context.client.clone(),
                self.context.topic.clone(),
                inflight,
            );
        }
    }

    // Move message to the pending batch respecting batch thresholds
    // and flush the batch if it is full.
    pub(crate) fn add_msg_and_flush(
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
                    self.move_to_batch_and_flush(&mut inflight, &mut batch);
                }
                msg = self.context.rx.recv() => {
                    match msg {
                        Some(ToBatchActor::Publish(msg)) => {
                            self.pending_msgs.push_back(msg);
                            if inflight.is_empty() {
                                self.move_to_batch_and_flush(&mut inflight, &mut batch);
                            }
                        },
                        Some(ToBatchActor::Flush(tx)) => {
                            self.flush(&mut inflight, &mut batch).await;
                            inflight = JoinSet::new();
                            let _ = tx.send(());
                        },
                        Some(ToBatchActor::ResumePublish()) => {
                            // Nothing to resume as we are not paused.
                        },
                        None => {
                            // This isn't guaranteed to execute if a user does not .await on the
                            // corresponding PublishFutures.
                            self.flush(&mut inflight, &mut batch).await;
                            break;
                        }
                    }
                }
            }
        }
    }

    // Flush the pending messages by sending the messages in sequential batches.
    async fn flush(&mut self, inflight: &mut JoinSet<crate::Result<()>>, batch: &mut Batch) {
        self.handle_inflight_join(inflight.join_next().await);
        while !self.pending_msgs.is_empty() {
            self.move_to_batch_and_flush(inflight, batch);
            self.handle_inflight_join(inflight.join_next().await);
        }
        // Flush the pending batch even if it does not fill the batch.
        if !batch.is_empty() {
            batch.flush(
                self.context.client.clone(),
                self.context.topic.clone(),
                inflight,
            );
        }
        self.handle_inflight_join(inflight.join_next().await);
    }

    // Move message to the pending batch respecting batch thresholds
    // and flush the batch if it is full.
    pub(crate) fn move_to_batch_and_flush(
        &mut self,
        inflight: &mut JoinSet<crate::Result<()>>,
        batch: &mut Batch,
    ) {
        let mut should_flush = false;
        while let Some(next) = self.pending_msgs.front() {
            if !batch.can_add(next) {
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
            batch.flush(
                self.context.client.clone(),
                self.context.topic.clone(),
                inflight,
            );
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

#[cfg(test)]
mod tests {
    use super::{ConcurrentBatchActor, SequentialBatchActor};
    use crate::error::PublishError;
    use crate::publisher::actor::{BundledMessage, ToBatchActor};
    use crate::publisher::batch::Batch;
    use crate::publisher::constants::{MAX_BYTES, MAX_MESSAGES};
    use crate::publisher::options::BatchingOptions;
    use crate::{
        generated::gapic_dataplane::client::Publisher as GapicPublisher,
        model::{Message, PublishResponse},
    };
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
            fn publish(&self, req: crate::model::PublishRequest, _options: google_cloud_gax::options::RequestOptions) -> impl Future<Output=google_cloud_gax::Result<google_cloud_gax::response::Response<crate::model::PublishResponse>>> + Send;
        }
    }

    fn publish_ok(
        req: crate::model::PublishRequest,
        _options: crate::RequestOptions,
    ) -> crate::Result<crate::Response<crate::model::PublishResponse>> {
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
    ) -> crate::Result<crate::Response<crate::model::PublishResponse>> {
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

    #[tokio::test(start_paused = true)]
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
                actor_rx,
            )
            .run(),
        );

        actor_tx.send(ToBatchActor::ResumePublish())?;
        Ok(())
    }

    #[tokio::test(start_paused = true)]
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

    #[tokio::test(start_paused = true)]
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
                actor_rx,
            )
            .run(),
        );

        assert_publish_is_ok!(actor_tx, 10);

        Ok(())
    }

    #[tokio::test(start_paused = true)]
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

    #[tokio::test(start_paused = true)]
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

    #[tokio::test(start_paused = true)]
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
}
