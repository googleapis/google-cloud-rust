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

use super::handler::AckResult;
use super::lease_state::{LeaseEvent, LeaseOptions, LeaseState};
use super::leaser::Leaser;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tokio::task::JoinHandle;

/// A convenience struct that groups the components of the lease loop.
pub(super) struct LeaseLoop {
    /// A handle to the task running the lease loop.
    pub(super) handle: JoinHandle<()>,
    /// For sending messages from the stream to the lease loop.
    pub(super) message_tx: UnboundedSender<String>,
    /// For sending acks/nacks from the application to the lease loop.
    pub(super) ack_tx: UnboundedSender<AckResult>,
}

impl LeaseLoop {
    pub(super) fn new<L>(leaser: L, options: LeaseOptions) -> Self
    where
        L: Leaser + Clone + Send + 'static,
    {
        let (message_tx, mut message_rx) = unbounded_channel();
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
                            Some(ack_id) => state.add(ack_id),
                        }
                    },
                    ack_id = ack_rx.recv() => {
                        match ack_id {
                            None => break,
                            Some(AckResult::Ack(ack_id)) => state.ack(ack_id),
                            Some(AckResult::Nack(ack_id)) => state.nack(ack_id),
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
async fn shutdown<L>(mut state: LeaseState<L>, mut ack_rx: UnboundedReceiver<AckResult>)
where
    L: Leaser + Clone + Send + 'static,
{
    while let Ok(r) = ack_rx.try_recv() {
        if let AckResult::Ack(ack_id) = r {
            state.ack(ack_id);
        }
    }
    state.shutdown().await;
}

#[cfg(test)]
mod tests {
    use super::super::lease_state::tests::{sorted, test_id, test_ids};
    use super::super::leaser::tests::MockLeaser;
    use super::*;
    use std::sync::Arc;
    use tokio::sync::Mutex;
    use tokio::time::{Duration, Instant};

    #[tokio::test(start_paused = true)]
    async fn flush_acks_nacks_on_interval() -> anyhow::Result<()> {
        const FLUSH_PERIOD: Duration = Duration::from_secs(1);
        const FLUSH_START: Duration = Duration::from_millis(200);

        let mock = Arc::new(Mutex::new(MockLeaser::new()));

        let options = LeaseOptions {
            flush_period: FLUSH_PERIOD,
            flush_start: FLUSH_START,
            // effectively disable extensions to simplify this test.
            extend_start: Duration::from_secs(900),
            ..Default::default()
        };
        let lease_loop = LeaseLoop::new(mock.clone(), options);
        // Yield execution, so tokio can actually start the lease loop.
        tokio::task::yield_now().await;

        // Seed the lease loop with some messages
        for i in 0..30 {
            lease_loop.message_tx.send(test_id(i))?;
        }

        // Ack 10 messages
        for i in 0..10 {
            lease_loop.ack_tx.send(AckResult::Ack(test_id(i)))?;
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
            lease_loop.ack_tx.send(AckResult::Nack(test_id(i)))?;
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
            lease_loop.ack_tx.send(AckResult::Ack(test_id(i)))?;
        }
        // Nack 5 messages
        for i in 25..30 {
            lease_loop.ack_tx.send(AckResult::Nack(test_id(i)))?;
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

    #[tokio::test(start_paused = true)]
    async fn deadline_interval() -> anyhow::Result<()> {
        const EXTEND_PERIOD: Duration = Duration::from_secs(1);
        const EXTEND_START: Duration = Duration::from_millis(200);

        let mock = Arc::new(Mutex::new(MockLeaser::new()));

        let options = LeaseOptions {
            // effectively disable ack/nack flushes to simplify this test.
            flush_start: Duration::from_secs(900),
            extend_period: EXTEND_PERIOD,
            extend_start: EXTEND_START,
            ..Default::default()
        };
        let lease_loop = LeaseLoop::new(mock.clone(), options);
        // Yield execution, so tokio can actually start the lease loop.
        tokio::task::yield_now().await;

        // Seed the lease loop with some messages
        for i in 0..30 {
            lease_loop.message_tx.send(test_id(i))?;
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
            lease_loop.ack_tx.send(AckResult::Ack(test_id(i)))?;
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

    #[tokio::test(start_paused = true)]
    async fn drop_does_not_wait_for_pending_operations() -> anyhow::Result<()> {
        let start = Instant::now();
        let mock = MockLeaser::new();
        let lease_loop = LeaseLoop::new(Arc::new(mock), LeaseOptions::default());
        // Yield execution, so tokio can actually start the lease loop.
        tokio::task::yield_now().await;

        // Seed the lease loop with some messages
        for i in 0..30 {
            lease_loop.message_tx.send(test_id(i))?;
        }

        // Ack 10 messages
        for i in 0..10 {
            lease_loop.ack_tx.send(AckResult::Ack(test_id(i)))?;
        }

        // Drop the lease_loop.
        drop(lease_loop);

        // Verify no time has passed.
        assert_eq!(start.elapsed(), Duration::ZERO);

        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn close_waits_for_flush() -> anyhow::Result<()> {
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
        }

        let lease_loop = LeaseLoop::new(FakeLeaser, LeaseOptions::default());

        // Seed the lease loop with some messages
        for i in 0..30 {
            lease_loop.message_tx.send(test_id(i))?;
        }

        // Ack 10 messages
        for i in 0..10 {
            lease_loop.ack_tx.send(AckResult::Ack(test_id(i)))?;
        }

        // Shutdown the lease_loop.
        drop(lease_loop.message_tx);
        lease_loop.handle.await?;

        // Verify that we flushed the acks immediately, and waited for them to
        // complete.
        assert_eq!(start.elapsed(), EXPECTED_SLEEP);

        Ok(())
    }

    #[tokio::test(start_paused = true)]
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
            let options = LeaseOptions {
                flush_start: Duration::from_millis(100),
                extend_start: Duration::from_millis(200),
                ..Default::default()
            };
            let lease_loop = LeaseLoop::new(mock.clone(), options);
            // Yield execution, so tokio can actually start the lease loop.
            tokio::task::yield_now().await;

            // Seed the lease loop with a message
            lease_loop.message_tx.send(test_id(1))?;
            // Immediately ack the message
            lease_loop.ack_tx.send(AckResult::Ack(test_id(1)))?;

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
}
