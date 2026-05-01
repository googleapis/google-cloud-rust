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

use super::handler::Action;
use super::lease_state::{LeaseEvent, LeaseOptions, LeaseState, NewMessage};
use super::leaser::{ConfirmedAcks, Leaser};
use super::shutdown_behavior::ShutdownBehavior;
#[cfg(test)]
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::mpsc::{UnboundedReceiver, WeakUnboundedSender, unbounded_channel};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// A convenience struct that groups the components of the lease loop.
pub(super) struct LeaseLoop {
    /// A handle to the task running the lease loop.
    pub(super) handle: JoinHandle<()>,
    /// For sending messages from the stream to the lease loop.
    pub(super) message_tx: WeakUnboundedSender<NewMessage>,
    /// For sending acks/nacks from the application to the lease loop.
    pub(super) ack_tx: WeakUnboundedSender<Action>,
    /// A token that can signal shutdown of the lease loop.
    pub(super) cancel: CancellationToken,
}

impl LeaseLoop {
    pub(super) fn new<L>(
        leaser: L,
        mut confirmed_rx: UnboundedReceiver<ConfirmedAcks>,
        mut eo_extend_rx: UnboundedReceiver<ConfirmedAcks>,
        options: LeaseOptions,
    ) -> Self
    where
        L: Leaser + Clone + Send + 'static,
    {
        let (message_tx, mut message_rx) = unbounded_channel::<NewMessage>();
        let (ack_tx, mut ack_rx) = unbounded_channel();

        let weak_message_tx = message_tx.downgrade();
        let weak_ack_tx = ack_tx.downgrade();

        let shutdown_guard = match options.shutdown_behavior {
            // If the subscriber is configured to wait for processing, we do not
            // want to break out of the lease loop when the stream drops its
            // message sender. We want to continue extending leases for these
            // messages as needed. So we hold a clone of the message sender.
            ShutdownBehavior::WaitForProcessing => Some(message_tx.clone()),
            ShutdownBehavior::NackImmediately => None,
        };

        let cancel = CancellationToken::new();
        let cancel_clone = cancel.clone();
        tokio::spawn(async move {
            // Hold the strong senders for the channels, dropping them when an
            // application signals a shutdown. This lets us begin the shutdown
            // procedure without requiring the application to `drop(stream)` or
            // call `stream.next()`.
            cancel_clone.cancelled().await;
            drop(message_tx);
            drop(ack_tx);
        });

        let mut state = LeaseState::new(leaser, options);

        let handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    // We use `biased` to make sure ack IDs from the stream are
                    // added to lease management before acks/nacks for them are
                    // processed.
                    //
                    // If these channels can race, we might accept an
                    // application's acknowledgement, and then immediately
                    // after, accept that message under lease management.
                    biased;
                    event = state.next_event() => {
                        match event {
                            LeaseEvent::Flush => state.flush(),
                            LeaseEvent::Extend => state.extend(),
                            LeaseEvent::ExtendCompleted(ack_ids) => {
                                state.update_last_extension(ack_ids);
                            }

                        }
                    },
                    message = message_rx.recv() => {
                        match message {
                            None => break shutdown(state, ack_rx).await,
                            Some(m) => state.add(m.ack_id, m.lease_info),
                        }
                    },
                    action = ack_rx.recv() => {
                        match action {
                            None => break state.shutdown().await,
                            Some(a) => state.process(a),
                        }
                    },
                    confirmed_acks = confirmed_rx.recv() => {
                        match confirmed_acks {
                            None => break,
                            Some(results) => state.confirm(results),
                        }
                    },
                    extend_results = eo_extend_rx.recv() => {
                        match extend_results {
                            None => break,
                            Some(r) => {
                                // TODO(#4804): Emit a log when there is an error.
                                let extended: Vec<String> = r
                                    .into_iter()
                                    .filter_map(|(id, res)| res.ok().map(|_| id))
                                    .collect();
                                state.update_last_extension_eo(extended);
                            }
                        }
                    },
                }
            }
            drop(shutdown_guard);
        });
        LeaseLoop {
            handle,
            message_tx: weak_message_tx,
            ack_tx: weak_ack_tx,
            cancel,
        }
    }
}

// Shuts down lease management.
//
// Processes any acks from the application that we already know about and
// triggers a shutdown of the lease state.
async fn shutdown<L>(mut state: LeaseState<L>, mut ack_rx: UnboundedReceiver<Action>)
where
    L: Leaser + Clone + Send + 'static,
{
    while let Ok(r) = ack_rx.try_recv() {
        if let Action::Ack(ack_id) = r {
            state.process(Action::Ack(ack_id));
        }
    }
    state.shutdown().await;
}

#[cfg(test)]
mod tests {
    use super::super::lease_state::tests::{
        at_least_once_info, exactly_once_info, sorted, test_id, test_ids,
    };
    use super::super::leaser::tests::MockLeaser;
    use super::*;
    use crate::subscriber::lease_state::{ExactlyOnceInfo, LeaseInfo};
    use google_cloud_test_macros::tokio_test_no_panics;
    use std::collections::HashMap;
    use std::sync::Arc;
    use test_case::test_case;
    use tokio::sync::Mutex;
    use tokio::sync::oneshot::channel;
    use tokio::time::{Duration, Instant};

    fn test_message(id: i32) -> NewMessage {
        NewMessage {
            ack_id: test_id(id),
            lease_info: at_least_once_info(),
        }
    }

    impl LeaseLoop {
        #[track_caller]
        fn strong_ack_tx(&self) -> UnboundedSender<Action> {
            self.ack_tx.upgrade().expect("shutdown has not begun")
        }

        #[track_caller]
        fn strong_message_tx(&self) -> UnboundedSender<NewMessage> {
            self.message_tx.upgrade().expect("shutdown has not begun")
        }
    }

    #[tokio_test_no_panics(start_paused = true)]
    async fn basic_exactly_once() -> anyhow::Result<()> {
        const FLUSH_START: Duration = Duration::from_millis(200);

        let mock = Arc::new(Mutex::new(MockLeaser::new()));

        let (_confirmed_tx, confirmed_rx) = unbounded_channel();
        let (_eo_extend_tx, eo_extend_rx) = unbounded_channel();
        let options = LeaseOptions {
            flush_start: FLUSH_START,
            // effectively disable extensions to simplify this test.
            extend_start: Duration::from_secs(900),
            ..Default::default()
        };
        let lease_loop = LeaseLoop::new(mock.clone(), confirmed_rx, eo_extend_rx, options);
        // Yield execution, so tokio can actually start the lease loop.
        tokio::task::yield_now().await;

        // Seed the lease loop with some messages
        for i in 0..30 {
            lease_loop.strong_message_tx().send(NewMessage {
                ack_id: test_id(i),
                lease_info: exactly_once_info(),
            })?;
        }

        // Ack 10 messages
        for i in 0..10 {
            lease_loop
                .strong_ack_tx()
                .send(Action::ExactlyOnceAck(test_id(i)))?;
        }

        // Nack 10 messages
        for i in 10..20 {
            lease_loop
                .strong_ack_tx()
                .send(Action::ExactlyOnceNack(test_id(i)))?;
        }

        mock.lock()
            .await
            .expect_confirmed_ack()
            .times(1)
            .withf(|v| sorted(v) == test_ids(0..10))
            .returning(|_| ());
        mock.lock()
            .await
            .expect_confirmed_nack()
            .times(1)
            .withf(|v| sorted(v) == test_ids(10..20))
            .returning(|_| ());
        tokio::time::advance(FLUSH_START).await;

        // Yield the current task, so tokio can execute the flush().
        tokio::task::yield_now().await;

        Ok(())
    }

    #[tokio_test_no_panics(start_paused = true)]
    async fn confirmed_ack() -> anyhow::Result<()> {
        const FLUSH_START: Duration = Duration::from_millis(200);

        let mock = Arc::new(Mutex::new(MockLeaser::new()));

        let (confirmed_tx, confirmed_rx) = unbounded_channel();
        let (_eo_extend_tx, eo_extend_rx) = unbounded_channel();
        let options = LeaseOptions {
            flush_start: FLUSH_START,
            extend_start: Duration::from_secs(900),
            ..Default::default()
        };
        let lease_loop = LeaseLoop::new(mock.clone(), confirmed_rx, eo_extend_rx, options);
        // Yield execution, so tokio can actually start the lease loop.
        tokio::task::yield_now().await;

        // Add a message and confirm ack.
        let (result_tx, mut result_rx) = channel();
        lease_loop.strong_message_tx().send(NewMessage {
            ack_id: test_id(0),
            lease_info: LeaseInfo::ExactlyOnce(ExactlyOnceInfo::new(result_tx)),
        })?;
        lease_loop
            .strong_ack_tx()
            .send(Action::ExactlyOnceAck(test_id(0)))?;
        let mut ack_results = HashMap::new();
        ack_results.insert(test_id(0), Ok(()));
        confirmed_tx.send(ack_results)?;

        mock.lock()
            .await
            .expect_confirmed_ack()
            .times(1)
            .withf(|v| *v == vec![test_id(0)])
            .returning(|_| ());
        tokio::time::advance(FLUSH_START).await;

        // Yield the current task, so tokio can execute the flush().
        tokio::task::yield_now().await;

        // Validate confirmed ack was processed.
        result_rx.try_recv()??;

        Ok(())
    }

    #[tokio_test_no_panics(start_paused = true)]
    async fn flush_acks_nacks_on_interval() -> anyhow::Result<()> {
        const FLUSH_PERIOD: Duration = Duration::from_secs(1);
        const FLUSH_START: Duration = Duration::from_millis(200);

        let mock = Arc::new(Mutex::new(MockLeaser::new()));

        let (_confirmed_tx, confirmed_rx) = unbounded_channel();
        let (_eo_extend_tx, eo_extend_rx) = unbounded_channel();
        let options = LeaseOptions {
            flush_period: FLUSH_PERIOD,
            flush_start: FLUSH_START,
            // effectively disable extensions to simplify this test.
            extend_start: Duration::from_secs(900),
            ..Default::default()
        };
        let lease_loop = LeaseLoop::new(mock.clone(), confirmed_rx, eo_extend_rx, options);
        // Yield execution, so tokio can actually start the lease loop.
        tokio::task::yield_now().await;

        // Seed the lease loop with some messages
        for i in 0..30 {
            lease_loop.strong_message_tx().send(test_message(i))?;
        }

        // Ack 10 messages
        for i in 0..10 {
            lease_loop.strong_ack_tx().send(Action::Ack(test_id(i)))?;
        }

        // Confirm initial state
        mock.lock().await.checkpoint();

        // Advance to and validate the first flush
        {
            mock.lock()
                .await
                .expect_ack()
                .times(1)
                .withf(|v| sorted(v) == test_ids(0..10))
                .returning(move |_| ());
            tokio::time::advance(FLUSH_START).await;

            // Yield the current task, so tokio can execute the flush().
            tokio::task::yield_now().await;
            mock.lock().await.checkpoint();
        }

        // Nack 10 messages
        for i in 10..20 {
            lease_loop.strong_ack_tx().send(Action::Nack(test_id(i)))?;
        }

        // Advance to and validate the second flush
        {
            mock.lock()
                .await
                .expect_nack()
                .times(1)
                .withf(|v| sorted(v) == test_ids(10..20))
                .returning(|_| ());
            tokio::time::advance(FLUSH_PERIOD).await;

            // Yield the current task, so tokio can execute the flush().
            tokio::task::yield_now().await;
            mock.lock().await.checkpoint();
        }

        // Ack 5 messages
        for i in 20..25 {
            lease_loop.strong_ack_tx().send(Action::Ack(test_id(i)))?;
        }
        // Nack 5 messages
        for i in 25..30 {
            lease_loop.strong_ack_tx().send(Action::Nack(test_id(i)))?;
        }

        // Advance to the third flush
        {
            mock.lock()
                .await
                .expect_ack()
                .times(1)
                .withf(|v| sorted(v) == test_ids(20..25))
                .returning(move |_| ());
            mock.lock()
                .await
                .expect_nack()
                .times(1)
                .withf(|v| sorted(v) == test_ids(25..30))
                .returning(|_| ());
            tokio::time::advance(FLUSH_PERIOD).await;

            // Yield the current task, so tokio can execute the flush().
            tokio::task::yield_now().await;
            mock.lock().await.checkpoint();
        }

        Ok(())
    }

    #[tokio_test_no_panics(start_paused = true)]
    async fn extend_interval() -> anyhow::Result<()> {
        const EXTEND_PERIOD: Duration = Duration::from_secs(1);
        const EXTEND_START: Duration = Duration::from_millis(200);

        let mock = Arc::new(Mutex::new(MockLeaser::new()));

        let (_confirmed_tx, confirmed_rx) = unbounded_channel();
        let (eo_extend_tx, eo_extend_rx) = unbounded_channel();
        let options = LeaseOptions {
            // effectively disable ack/nack flushes to simplify this test.
            flush_start: Duration::from_secs(900),
            extend_period: EXTEND_PERIOD,
            extend_start: EXTEND_START,
            // max_lease_extension is set to 7 seconds as the test advances
            // extend_period twice and the buffer is 5 seconds.
            max_lease_extension: Duration::from_secs(7),
            ..Default::default()
        };
        let lease_loop = LeaseLoop::new(mock.clone(), confirmed_rx, eo_extend_rx, options);
        // Yield execution, so tokio can actually start the lease loop.
        tokio::task::yield_now().await;

        // Seed the lease loop with some messages
        for i in 0..30 {
            lease_loop.strong_message_tx().send(test_message(i))?;
        }

        // Seed the lease loop with some exactly-once messages
        for i in 30..60 {
            lease_loop.strong_message_tx().send(NewMessage {
                ack_id: test_id(i),
                lease_info: exactly_once_info(),
            })?;
        }

        // Confirm initial state
        mock.lock().await.checkpoint();

        // Advance to and validate the first extension
        {
            mock.lock()
                .await
                .expect_extend()
                .times(1)
                .withf(|v| sorted(v) == test_ids(0..30))
                .returning(move |ack_ids| ack_ids);

            mock.lock()
                .await
                .expect_eo_extend()
                .times(1)
                .withf(|v| sorted(v) == test_ids(30..60))
                .return_once({
                    let tx = eo_extend_tx.clone();
                    move |ack_ids| {
                        let mut results = HashMap::new();
                        for id in ack_ids {
                            results.insert(id, Ok(()));
                        }
                        let _ = tx.send(results);
                    }
                });

            tokio::time::advance(EXTEND_START).await;

            // Yield the current task, so tokio can execute the flush().
            tokio::task::yield_now().await;
            mock.lock().await.checkpoint();
        }

        // Ack 10 messages
        for i in 0..10 {
            lease_loop.strong_ack_tx().send(Action::Ack(test_id(i)))?;
        }

        // Advance to and validate the second extension period (should be skipped)
        {
            mock.lock().await.expect_extend().times(0);
            tokio::time::advance(EXTEND_PERIOD).await;

            // Yield the current task, so tokio can execute the flush().
            tokio::task::yield_now().await;
            mock.lock().await.checkpoint();
        }

        // Advance to and validate the third extension (should be extended)
        {
            mock.lock()
                .await
                .expect_extend()
                .times(1)
                .withf(|v| sorted(v) == test_ids(10..30))
                .returning(|ack_ids| ack_ids);

            mock.lock()
                .await
                .expect_eo_extend()
                .times(1)
                .withf(|v| sorted(v) == test_ids(30..60))
                .return_once({
                    let tx = eo_extend_tx.clone();
                    move |ack_ids| {
                        let mut results = HashMap::new();
                        for id in ack_ids {
                            results.insert(id, Ok(()));
                        }
                        let _ = tx.send(results);
                    }
                });

            tokio::time::advance(EXTEND_PERIOD).await;

            // Yield the current task, so tokio can execute the flush().
            tokio::task::yield_now().await;
            mock.lock().await.checkpoint();
        }

        Ok(())
    }

    #[tokio_test_no_panics(start_paused = true)]
    async fn drop_does_not_wait_for_pending_operations() -> anyhow::Result<()> {
        let start = Instant::now();
        let mock = MockLeaser::new();
        let (_confirmed_tx, confirmed_rx) = unbounded_channel();
        let (_eo_extend_tx, eo_extend_rx) = unbounded_channel();
        let lease_loop = LeaseLoop::new(
            Arc::new(mock),
            confirmed_rx,
            eo_extend_rx,
            LeaseOptions::default(),
        );
        // Yield execution, so tokio can actually start the lease loop.
        tokio::task::yield_now().await;

        // Seed the lease loop with some messages
        for i in 0..30 {
            lease_loop.strong_message_tx().send(test_message(i))?;
        }

        // Ack 10 messages
        for i in 0..10 {
            lease_loop.strong_ack_tx().send(Action::Ack(test_id(i)))?;
        }

        // Drop the lease_loop.
        drop(lease_loop);

        // Verify no time has passed.
        assert_eq!(start.elapsed(), Duration::ZERO);

        Ok(())
    }

    #[tokio_test_no_panics(start_paused = true)]
    async fn shutdown_nack_immediately() -> anyhow::Result<()> {
        let mock = Arc::new(Mutex::new(MockLeaser::new()));
        mock.lock()
            .await
            .expect_ack()
            .times(1)
            .withf(|v| sorted(v) == test_ids(0..10))
            .returning(|_| ());
        mock.lock()
            .await
            .expect_nack()
            .times(1)
            .withf(|v| sorted(v) == test_ids(10..30))
            .returning(|_| ());

        let (_confirmed_tx, confirmed_rx) = unbounded_channel();
        let (_eo_extend_tx, eo_extend_rx) = unbounded_channel();
        let options = LeaseOptions {
            shutdown_behavior: ShutdownBehavior::NackImmediately,
            ..Default::default()
        };
        let lease_loop = LeaseLoop::new(mock, confirmed_rx, eo_extend_rx, options);

        // Seed the lease loop with some messages
        for i in 0..30 {
            lease_loop.strong_message_tx().send(test_message(i))?;
        }

        // Ack 10 messages
        for i in 0..10 {
            lease_loop.strong_ack_tx().send(Action::Ack(test_id(i)))?;
        }

        // Nack 10 messages
        for i in 10..20 {
            lease_loop.strong_ack_tx().send(Action::Nack(test_id(i)))?;
        }

        // Shutdown the lease_loop.
        lease_loop.cancel.cancel();
        tokio::task::yield_now().await;
        lease_loop.handle.await?;

        Ok(())
    }

    #[tokio_test_no_panics(start_paused = true)]
    async fn shutdown_wait_for_processing() -> anyhow::Result<()> {
        let mock = Arc::new(Mutex::new(MockLeaser::new()));
        mock.lock()
            .await
            .expect_ack()
            .times(1)
            .withf(|v| sorted(v) == test_ids(0..20))
            .returning(|_| ());

        let (_confirmed_tx, confirmed_rx) = unbounded_channel();
        let (_eo_extend_tx, eo_extend_rx) = unbounded_channel();
        let options = LeaseOptions {
            shutdown_behavior: ShutdownBehavior::WaitForProcessing,
            ..Default::default()
        };
        let lease_loop = LeaseLoop::new(mock, confirmed_rx, eo_extend_rx, options);
        let ack_tx = lease_loop.strong_ack_tx();

        // Seed the lease loop with some messages
        for i in 0..20 {
            lease_loop.strong_message_tx().send(test_message(i))?;
        }

        // Ack 10 messages
        for i in 0..10 {
            ack_tx.send(Action::Ack(test_id(i)))?;
        }

        // Signal and await a shutdown of the lease_loop.
        lease_loop.cancel.cancel();
        // Yield execution to the lease loop. If it shuts down now while
        // `ack_tx` is still in scope, the test will fail.
        tokio::task::yield_now().await;

        // Simulate the application using the rest of its handlers.
        for i in 10..20 {
            ack_tx.send(Action::Ack(test_id(i)))?;
        }
        drop(ack_tx);

        lease_loop.handle.await?;

        Ok(())
    }

    #[test_case(ShutdownBehavior::WaitForProcessing)]
    #[test_case(ShutdownBehavior::NackImmediately)]
    #[tokio_test_no_panics(start_paused = true)]
    async fn shutdown_waits_for_flush(shutdown_behavior: ShutdownBehavior) -> anyhow::Result<()> {
        const EXPECTED_SLEEP: Duration = Duration::from_millis(100);

        let start = Instant::now();

        #[derive(Clone)]
        struct FakeLeaser;
        #[async_trait::async_trait]
        impl Leaser for FakeLeaser {
            async fn ack(&self, mut ack_ids: Vec<String>) {
                ack_ids.sort();
                assert_eq!(ack_ids, test_ids(0..10));
                tokio::time::sleep(EXPECTED_SLEEP).await;
            }
            async fn nack(&self, mut ack_ids: Vec<String>) {
                ack_ids.sort();
                assert_eq!(ack_ids, test_ids(10..30));
            }
            async fn extend(&self, ack_ids: Vec<String>) -> Vec<String> {
                ack_ids
            }
            async fn confirmed_ack(&self, _ack_ids: Vec<String>) {}
            async fn confirmed_nack(&self, _ack_ids: Vec<String>) {}
            async fn eo_extend(&self, _ack_ids: Vec<String>) {}
        }
        let (_confirmed_tx, confirmed_rx) = unbounded_channel();
        let (_eo_extend_tx, eo_extend_rx) = unbounded_channel();
        let options = LeaseOptions {
            shutdown_behavior,
            ..Default::default()
        };
        let lease_loop = LeaseLoop::new(FakeLeaser, confirmed_rx, eo_extend_rx, options);

        // Seed the lease loop with some messages
        for i in 0..30 {
            lease_loop.strong_message_tx().send(test_message(i))?;
        }

        // Ack 10 messages
        for i in 0..10 {
            lease_loop.strong_ack_tx().send(Action::Ack(test_id(i)))?;
        }

        // Shutdown the lease_loop.
        lease_loop.cancel.cancel();
        lease_loop.handle.await?;

        // Verify that we flushed the acks immediately, and waited for them to
        // complete.
        assert_eq!(start.elapsed(), EXPECTED_SLEEP);

        Ok(())
    }

    #[tokio_test_no_panics(start_paused = true)]
    async fn no_add_and_ack_race() -> anyhow::Result<()> {
        // This test validates the use of `biased` in the select statement.
        //
        // Specifically, we want incoming messages to be processed by the event
        // loop before any incoming acks/nacks.
        //
        // If these channels can race, we might accept an application's
        // acknowledgement, and then immediately after, accept that message under
        // lease management.

        for _ in 0..1000 {
            // Run this test enough times to trigger a race, if one existed.

            let mock = Arc::new(Mutex::new(MockLeaser::new()));
            let (_confirmed_tx, confirmed_rx) = unbounded_channel();
            let (_eo_extend_tx, eo_extend_rx) = unbounded_channel();
            let options = LeaseOptions {
                flush_start: Duration::from_millis(100),
                extend_start: Duration::from_millis(200),
                ..Default::default()
            };
            let lease_loop = LeaseLoop::new(mock.clone(), confirmed_rx, eo_extend_rx, options);
            // Yield execution, so tokio can actually start the lease loop.
            tokio::task::yield_now().await;

            // Seed the lease loop with a message
            lease_loop.strong_message_tx().send(test_message(1))?;
            // Immediately ack the message
            lease_loop.strong_ack_tx().send(Action::Ack(test_id(1)))?;

            // Advance to and validate the first flush
            {
                mock.lock()
                    .await
                    .expect_ack()
                    .times(1)
                    .withf(|v| *v == vec![test_id(1)])
                    .returning(|_| ());
                tokio::time::advance(Duration::from_millis(100)).await;

                // Yield the current task, so tokio can execute the flush().
                tokio::task::yield_now().await;
                mock.lock().await.checkpoint();
            }

            // Confirm that no messages are under lease management.
            {
                mock.lock().await.expect_extend().times(0);
                tokio::time::advance(Duration::from_millis(100)).await;

                // Yield the current task, so tokio can execute the flush().
                tokio::task::yield_now().await;
                mock.lock().await.checkpoint();
            }
        }
        Ok(())
    }

    #[tokio_test_no_panics(start_paused = true)]
    async fn eo_extend_result_updates_lease_state() -> anyhow::Result<()> {
        const EXTEND_START: Duration = Duration::from_millis(200);
        const EXTEND_PERIOD: Duration = Duration::from_secs(1);

        let mock = Arc::new(Mutex::new(MockLeaser::new()));

        let (_confirmed_tx, confirmed_rx) = unbounded_channel();
        let (eo_extend_tx, eo_extend_rx) = unbounded_channel();
        let options = LeaseOptions {
            // effectively disable ack/nack flushes to simplify this test.
            flush_start: Duration::from_secs(900),
            extend_period: EXTEND_PERIOD,
            extend_start: EXTEND_START,
            max_lease_extension: Duration::from_secs(10),
            ..Default::default()
        };
        let lease_loop = LeaseLoop::new(mock.clone(), confirmed_rx, eo_extend_rx, options);
        tokio::task::yield_now().await;

        // Seed with an exactly-once message.
        lease_loop.strong_message_tx().send(NewMessage {
            ack_id: test_id(0),
            lease_info: exactly_once_info(),
        })?;

        mock.lock()
            .await
            .expect_eo_extend()
            .times(1)
            .withf(|v| *v == vec![test_id(0)])
            .return_once({
                let tx = eo_extend_tx.clone();
                move |ack_ids| {
                    let mut results = HashMap::new();
                    for id in ack_ids {
                        results.insert(id, Ok(()));
                    }
                    let _ = tx.send(results);
                }
            });

        tokio::time::advance(EXTEND_START).await;
        tokio::task::yield_now().await;
        mock.lock().await.checkpoint();

        tokio::task::yield_now().await; // Let the loop process the extend result.

        // Since the lease has been successfully extended, it should not be extended again.
        mock.lock().await.expect_eo_extend().times(0);

        tokio::time::advance(EXTEND_PERIOD).await;
        tokio::task::yield_now().await;
        mock.lock().await.checkpoint();

        Ok(())
    }
}
