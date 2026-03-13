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
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tokio::task::JoinHandle;

/// A convenience struct that groups the components of the lease loop.
pub(super) struct LeaseLoop {
    /// A handle to the task running the lease loop.
    pub(super) handle: JoinHandle<()>,
    /// For sending messages from the stream to the lease loop.
    pub(super) message_tx: UnboundedSender<NewMessage>,
    /// For sending acks/nacks from the application to the lease loop.
    pub(super) ack_tx: UnboundedSender<Action>,
}

impl LeaseLoop {
    pub(super) fn new<L>(
        leaser: L,
        mut confirmed_rx: UnboundedReceiver<ConfirmedAcks>,
        options: LeaseOptions,
    ) -> Self
    where
        L: Leaser + Clone + Send + 'static,
    {
        let (message_tx, mut message_rx) = unbounded_channel::<NewMessage>();
        let (ack_tx, mut ack_rx) = unbounded_channel();
        let mut state = LeaseState::new(leaser, options);

        let handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    event = state.next_event() => {
                        match event {
                            LeaseEvent::Flush => state.flush().await,
                            LeaseEvent::Extend => state.extend().await,
                        }
                    },
                    message = message_rx.recv() => {
                        match message {
                            None => break shutdown(state, ack_rx).await,
                            Some(m) => state.add(m.ack_id, m.lease_info),
                        }
                    },
                    ack_id = ack_rx.recv() => {
                        match ack_id {
                            None => break,
                            Some(Action::Ack(ack_id)) => state.process(Action::Ack(ack_id)),
                            Some(Action::Nack(ack_id)) => state.process(Action::Nack(ack_id)),
                            Some(Action::ExactlyOnceAck(ack_id)) => state.process(Action::ExactlyOnceAck(ack_id)),
                            Some(Action::ExactlyOnceNack(ack_id)) => state.process(Action::ExactlyOnceNack(ack_id)),
                        }
                    },
                    confirmed_acks = confirmed_rx.recv() => {
                        match confirmed_acks {
                            None => break,
                            Some(results) => state.confirm(results),
                        }
                    },
                }
            }
        });
        LeaseLoop {
            handle,
            message_tx,
            ack_tx,
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
        // Do nothing for Action::ExactlyOnceAck, the state returns
        // NACK_SHUTDOWN_ERROR.
        // TODO(#4869) - also update shutdown behavior here if needed.
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
    use google_cloud_test_macros::tokio_test_no_panics;
    use std::collections::HashMap;
    use std::sync::Arc;
    use test_case::test_case;
    use tokio::sync::Mutex;
    use tokio::time::{Duration, Instant};

    fn test_at_least_once_message(id: i32) -> NewMessage {
        NewMessage {
            ack_id: test_id(id),
            lease_info: at_least_once_info(),
        }
    }

    fn test_exactly_once_message(id: i32) -> NewMessage {
        NewMessage {
            ack_id: test_id(id),
            lease_info: exactly_once_info(),
        }
    }

    fn test_at_least_once_ack(id: i32) -> Action {
        Action::Ack(test_id(id))
    }

    fn test_at_least_once_nack(id: i32) -> Action {
        Action::Nack(test_id(id))
    }

    fn test_exactly_once_ack(id: i32) -> Action {
        Action::ExactlyOnceAck(test_id(id))
    }

    fn test_exactly_once_nack(id: i32) -> Action {
        Action::ExactlyOnceNack(test_id(id))
    }

    #[tokio_test_no_panics(start_paused = true)]
    async fn flush_acks_nacks_on_interval_at_least_once() -> anyhow::Result<()> {
        const FLUSH_PERIOD: Duration = Duration::from_secs(1);
        const FLUSH_START: Duration = Duration::from_millis(200);

        let mock = Arc::new(Mutex::new(MockLeaser::new()));

        let (_confirmed_tx, confirmed_rx) = unbounded_channel();
        let options = LeaseOptions {
            flush_period: FLUSH_PERIOD,
            flush_start: FLUSH_START,
            // effectively disable extensions to simplify this test.
            extend_start: Duration::from_secs(900),
            ..Default::default()
        };
        let lease_loop = LeaseLoop::new(mock.clone(), confirmed_rx, options);
        // Yield execution, so tokio can actually start the lease loop.
        tokio::task::yield_now().await;

        // Seed the lease loop with some messages
        for i in 0..30 {
            lease_loop.message_tx.send(test_at_least_once_message(i))?;
        }

        // Ack 10 messages
        for i in 0..10 {
            lease_loop.ack_tx.send(test_at_least_once_ack(i))?;
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
            lease_loop.ack_tx.send(test_at_least_once_nack(i))?;
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
            lease_loop.ack_tx.send(test_at_least_once_ack(i))?;
        }
        // Nack 5 messages
        for i in 25..30 {
            lease_loop.ack_tx.send(test_at_least_once_nack(i))?;
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
    async fn flush_acks_nacks_on_interval_exactly_once() -> anyhow::Result<()> {
        const FLUSH_PERIOD: Duration = Duration::from_secs(1);
        const FLUSH_START: Duration = Duration::from_millis(200);

        let mock = Arc::new(Mutex::new(MockLeaser::new()));

        let (_confirmed_tx, confirmed_rx) = unbounded_channel();
        let options = LeaseOptions {
            flush_period: FLUSH_PERIOD,
            flush_start: FLUSH_START,
            // effectively disable extensions to simplify this test.
            extend_start: Duration::from_secs(900),
            ..Default::default()
        };
        let lease_loop = LeaseLoop::new(mock.clone(), confirmed_rx, options);
        // Yield execution, so tokio can actually start the lease loop.
        tokio::task::yield_now().await;

        // Seed the lease loop with some messages
        for i in 0..30 {
            lease_loop.message_tx.send(test_exactly_once_message(i))?;
        }

        // Ack 10 messages
        for i in 0..10 {
            lease_loop.ack_tx.send(test_exactly_once_ack(i))?;
        }

        // Confirm initial state
        mock.lock().await.checkpoint();

        // Advance to and validate the first flush
        {
            mock.lock()
                .await
                .expect_confirmed_ack()
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
            lease_loop.ack_tx.send(test_exactly_once_nack(i))?;
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
            lease_loop.ack_tx.send(test_exactly_once_ack(i))?;
        }
        // Nack 5 messages
        for i in 25..30 {
            lease_loop.ack_tx.send(test_exactly_once_nack(i))?;
        }

        // Advance to the third flush
        {
            mock.lock()
                .await
                .expect_confirmed_ack()
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
    async fn deadline_interval_at_least_once() -> anyhow::Result<()> {
        const EXTEND_PERIOD: Duration = Duration::from_secs(1);
        const EXTEND_START: Duration = Duration::from_millis(200);

        let mock = Arc::new(Mutex::new(MockLeaser::new()));

        let (_confirmed_tx, confirmed_rx) = unbounded_channel();
        let options = LeaseOptions {
            // effectively disable ack/nack flushes to simplify this test.
            flush_start: Duration::from_secs(900),
            extend_period: EXTEND_PERIOD,
            extend_start: EXTEND_START,
            ..Default::default()
        };
        let lease_loop = LeaseLoop::new(mock.clone(), confirmed_rx, options);
        // Yield execution, so tokio can actually start the lease loop.
        tokio::task::yield_now().await;

        // Seed the lease loop with some messages
        for i in 0..30 {
            lease_loop.message_tx.send(test_at_least_once_message(i))?;
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
                .returning(move |_| ());
            tokio::time::advance(EXTEND_START).await;

            // Yield the current task, so tokio can execute the flush().
            tokio::task::yield_now().await;
            mock.lock().await.checkpoint();
        }

        // Ack 10 messages
        for i in 0..10 {
            lease_loop.ack_tx.send(test_at_least_once_ack(i))?;
        }

        // Advance to and validate the second extension
        {
            mock.lock()
                .await
                .expect_extend()
                .times(1)
                .withf(|v| sorted(v) == test_ids(10..30))
                .returning(|_| ());
            tokio::time::advance(EXTEND_PERIOD).await;

            // Yield the current task, so tokio can execute the flush().
            tokio::task::yield_now().await;
            mock.lock().await.checkpoint();
        }

        Ok(())
    }

    #[tokio_test_no_panics(start_paused = true)]
    async fn deadline_interval_exactly_once() -> anyhow::Result<()> {
        const EXTEND_PERIOD: Duration = Duration::from_secs(1);
        const EXTEND_START: Duration = Duration::from_millis(200);

        let mock = Arc::new(Mutex::new(MockLeaser::new()));

        let (confirmed_tx, confirmed_rx) = unbounded_channel();
        let options = LeaseOptions {
            // effectively disable ack/nack flushes to simplify this test.
            flush_start: Duration::from_secs(900),
            extend_period: EXTEND_PERIOD,
            extend_start: EXTEND_START,
            ..Default::default()
        };
        let lease_loop = LeaseLoop::new(mock.clone(), confirmed_rx, options);
        // Yield execution, so tokio can actually start the lease loop.
        tokio::task::yield_now().await;

        // Seed the lease loop with some messages
        for i in 0..30 {
            lease_loop.message_tx.send(test_exactly_once_message(i))?;
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
                .returning(move |_| ());
            tokio::time::advance(EXTEND_START).await;

            // Yield the current task, so tokio can execute the flush().
            tokio::task::yield_now().await;
            mock.lock().await.checkpoint();
        }

        // Ack 10 messages. We should continue to extend these leases
        // as they are not yet confirmed.
        for i in 0..10 {
            lease_loop.ack_tx.send(test_exactly_once_ack(i))?;
        }

        {
            mock.lock()
                .await
                .expect_extend()
                .times(1)
                .withf(|v| sorted(v) == test_ids(0..30))
                .returning(|_| ());
            tokio::time::advance(EXTEND_PERIOD).await;

            // Yield the current task, so tokio can execute the flush().
            tokio::task::yield_now().await;
            mock.lock().await.checkpoint();
        }

        // Confirm 5 messages. We expect these to not be extended.
        let mut confirms = HashMap::new();
        for i in 0..5 {
            confirms.insert(test_id(i), Ok(()));
        }
        confirmed_tx.send(confirms)?;

        {
            mock.lock()
                .await
                .expect_extend()
                .times(1)
                .withf(|v| sorted(v) == test_ids(5..30))
                .returning(|_| ());
            tokio::time::advance(EXTEND_PERIOD).await;

            // Yield the current task, so tokio can execute the flush().
            tokio::task::yield_now().await;
            mock.lock().await.checkpoint();
        }

        Ok(())
    }

    #[test_case(super::test_at_least_once_message, super::test_at_least_once_ack)]
    #[test_case(super::test_exactly_once_message, super::test_exactly_once_ack)]
    #[tokio_test_no_panics(start_paused = true)]
    async fn drop_does_not_wait_for_pending_operations(
        msg_factory: fn(i32) -> NewMessage,
        action_factory: fn(i32) -> Action,
    ) -> anyhow::Result<()> {
        let start = Instant::now();
        let mock = MockLeaser::new();
        let (_confirmed_tx, confirmed_rx) = unbounded_channel();
        let lease_loop = LeaseLoop::new(Arc::new(mock), confirmed_rx, LeaseOptions::default());
        // Yield execution, so tokio can actually start the lease loop.
        tokio::task::yield_now().await;

        // Seed the lease loop with some messages
        for i in 0..30 {
            lease_loop.message_tx.send(msg_factory(i))?;
        }

        // Ack 10 messages
        for i in 0..10 {
            lease_loop.ack_tx.send(action_factory(i))?;
        }

        // Drop the lease_loop.
        drop(lease_loop);

        // Verify no time has passed.
        assert_eq!(start.elapsed(), Duration::ZERO);

        Ok(())
    }

    #[tokio_test_no_panics(start_paused = true)]
    async fn close_at_least_once_waits_for_ack() -> anyhow::Result<()> {
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
            async fn extend(&self, _ack_ids: Vec<String>) {}
            async fn confirmed_ack(&self, _ack_ids: Vec<String>) {}
        }

        let (_confirmed_tx, confirmed_rx) = unbounded_channel();
        let lease_loop = LeaseLoop::new(FakeLeaser, confirmed_rx, LeaseOptions::default());

        // Seed the lease loop with some messages
        for i in 0..30 {
            lease_loop.message_tx.send(test_at_least_once_message(i))?;
        }

        // Ack 10 messages
        for i in 0..10 {
            lease_loop.ack_tx.send(test_at_least_once_ack(i))?;
        }

        // Shutdown the lease_loop.
        drop(lease_loop.message_tx);
        lease_loop.handle.await?;

        // Verify that we flushed the acks immediately, and waited for them to
        // complete.
        assert_eq!(start.elapsed(), EXPECTED_SLEEP);

        Ok(())
    }

    #[tokio_test_no_panics(start_paused = true)]
    async fn close_exactly_once_no_confirmed_ack() -> anyhow::Result<()> {
        const EXPECTED_SLEEP: Duration = Duration::from_millis(100);

        let start = Instant::now();

        #[derive(Clone)]
        struct FakeLeaser;
        #[async_trait::async_trait]
        impl Leaser for FakeLeaser {
            async fn ack(&self, mut _ack_ids: Vec<String>) {}
            async fn nack(&self, mut ack_ids: Vec<String>) {
                ack_ids.sort();
                assert_eq!(ack_ids, test_ids(0..30));
            }
            async fn extend(&self, _ack_ids: Vec<String>) {}
            async fn confirmed_ack(&self, _ack_ids: Vec<String>) {
                tokio::time::sleep(EXPECTED_SLEEP).await;
            }
        }

        let (_confirmed_tx, confirmed_rx) = unbounded_channel();
        let lease_loop = LeaseLoop::new(FakeLeaser, confirmed_rx, LeaseOptions::default());

        // Seed the lease loop with some messages
        for i in 0..30 {
            lease_loop.message_tx.send(test_exactly_once_message(i))?;
        }

        // Ack 10 messages
        for i in 0..10 {
            lease_loop.ack_tx.send(test_exactly_once_ack(i))?;
        }

        // Shutdown the lease_loop.
        drop(lease_loop.message_tx);
        lease_loop.handle.await?;

        // Verify that confirmed ack has not been sent.
        assert_eq!(start.elapsed(), Duration::ZERO);

        Ok(())
    }

    #[tokio_test_no_panics(start_paused = true)]
    async fn no_add_and_ack_race_at_least_once() -> anyhow::Result<()> {
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
            let options = LeaseOptions {
                flush_start: Duration::from_millis(100),
                extend_start: Duration::from_millis(200),
                ..Default::default()
            };
            let lease_loop = LeaseLoop::new(mock.clone(), confirmed_rx, options);
            // Yield execution, so tokio can actually start the lease loop.
            tokio::task::yield_now().await;

            // Seed the lease loop with a message
            lease_loop.message_tx.send(test_at_least_once_message(1))?;
            // Immediately ack the message
            lease_loop.ack_tx.send(test_at_least_once_ack(1))?;

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
    async fn no_add_and_ack_race_exactly_once() -> anyhow::Result<()> {
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
            let (confirmed_tx, confirmed_rx) = unbounded_channel();
            let options = LeaseOptions {
                flush_start: Duration::from_millis(100),
                extend_start: Duration::from_millis(200),
                ..Default::default()
            };
            let lease_loop = LeaseLoop::new(mock.clone(), confirmed_rx, options);
            // Yield execution, so tokio can actually start the lease loop.
            tokio::task::yield_now().await;

            // Seed the lease loop with a message
            lease_loop.message_tx.send(test_exactly_once_message(1))?;
            // Immediately ack the message
            lease_loop.ack_tx.send(test_exactly_once_ack(1))?;
            let mut ack_results = HashMap::new();
            ack_results.insert(test_id(1), Ok(()));
            confirmed_tx.send(ack_results)?;

            // Advance to and validate the first flush
            {
                mock.lock()
                    .await
                    .expect_confirmed_ack()
                    .times(1)
                    .withf(|v| *v == vec![test_id(1)])
                    .returning(|_| ());
                tokio::time::advance(Duration::from_millis(100)).await;

                // Yield the current task, so tokio can execute the flush().
                tokio::task::yield_now().await;
                mock.lock().await.checkpoint();
            }

            // Validate that no messages are under lease management.
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
}
