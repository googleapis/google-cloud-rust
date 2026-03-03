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

mod at_least_once;
mod exactly_once;

use super::handler::Action;
use super::leaser::Leaser;
use at_least_once::Leases;
// Use a `tokio::time::Instant` to facilitate time-based unit testing.
use tokio::time::{Duration, Instant, Interval, interval_at};

// An ack ID is less than 200 bytes. The limit for a request is 512kB. It should
// be safe to fit 2500 Ack IDs in a single RPC.
//
// https://docs.cloud.google.com/pubsub/quotas
const MAX_IDS_PER_RPC: usize = 2500;

pub(super) struct LeaseOptions {
    /// How often we flush acks/nacks
    pub(super) flush_period: Duration,
    /// How long we wait for the initial flush
    pub(super) flush_start: Duration,
    /// How often we extend deadlines for messages under lease
    pub(super) extend_period: Duration,
    /// How long we wait for the initial extensions
    pub(super) extend_start: Duration,
    /// How long messages can be kept under lease. A message's lease can be
    /// extended as long as `max_lease_extension` has not elapsed.
    pub(super) max_lease_extension: Duration,
}

impl Default for LeaseOptions {
    fn default() -> Self {
        LeaseOptions {
            flush_period: Duration::from_millis(100),
            flush_start: Duration::from_millis(100),
            extend_period: Duration::from_secs(3),
            extend_start: Duration::from_millis(500),
            max_lease_extension: Duration::from_secs(600),
        }
    }
}

pub(super) struct NewMessage {
    pub(super) ack_id: String,
    pub(super) lease_info: LeaseInfo,
}

#[derive(Debug)]
pub(super) enum LeaseInfo {
    AtLeastOnce(Instant),
    // TODO(#3964) - support exactly once delivery
}

#[derive(Debug)]
pub(super) struct LeaseState<L>
where
    L: Leaser + Clone,
{
    // Ack IDs with at-least-once semantics under lease management.
    leases: Leases,
    // TODO(#3964) - support exactly once acks

    // The leaser, which performs lease operations - (acks, nacks, lease
    // extensions).
    leaser: L,

    // A timer for flushing acks/nacks
    flush_interval: Interval,
    // A timer for extending leases
    extend_interval: Interval,
    // How long messages can be kept under lease
    max_lease_extension: Duration,
}

/// Actions taken by the `LeaseState` in the lease loop.
#[derive(Debug, PartialEq)]
pub(super) enum LeaseEvent {
    /// Flush acks/nacks
    Flush,
    /// Extend leases
    Extend,
}

impl<L> LeaseState<L>
where
    L: Leaser + Clone,
{
    pub(super) fn new(leaser: L, options: LeaseOptions) -> Self {
        let flush_interval =
            interval_at(Instant::now() + options.flush_start, options.flush_period);
        let extend_interval =
            interval_at(Instant::now() + options.extend_start, options.extend_period);
        Self {
            leases: Leases::default(),
            leaser,
            flush_interval,
            extend_interval,
            max_lease_extension: options.max_lease_extension,
        }
    }

    /// A future that fires when it is time to either:
    /// - flush acks/nacks
    /// - extend leases
    ///
    /// We need to centralize event handling because the lease loop can only
    /// hold one mutable reference to `LeaseState` within its `select!`
    /// statement.
    pub(super) async fn next_event(&mut self) -> LeaseEvent {
        if self.leases.needs_flush() {
            return LeaseEvent::Flush;
        }

        tokio::select! {
            _ = self.flush_interval.tick() => LeaseEvent::Flush,
            _ = self.extend_interval.tick() => LeaseEvent::Extend,
        }
    }

    /// Accept a new ack ID under lease management
    pub(super) fn add(&mut self, ack_id: String, info: LeaseInfo) {
        match info {
            LeaseInfo::AtLeastOnce(i) => {
                self.leases.add(ack_id, i);
            }
        }
    }

    // TODO(#3964) - delete, in favor of process.
    pub(super) fn ack(&mut self, ack_id: String) {
        self.process(Action::Ack(ack_id));
    }

    // TODO(#3964) - delete, in favor of process.
    pub(super) fn nack(&mut self, ack_id: String) {
        self.process(Action::Nack(ack_id));
    }

    /// Process an action from the application.
    pub(super) fn process(&mut self, action: Action) {
        match action {
            Action::Ack(ack_id) => self.leases.ack(ack_id),
            Action::Nack(ack_id) => self.leases.nack(ack_id),
            // TODO(#3964) - process exactly-once acks/nacks in the lease state
            _ => unreachable!("we do not return exactly-once handlers yet."),
        }
    }

    /// Flush pending acks/nacks
    pub(super) async fn flush(&mut self) {
        let (to_ack, to_nack) = self.leases.flush();

        // TODO(#3975) - await these concurrently.
        if !to_ack.is_empty() {
            self.leaser.ack(to_ack).await;
        }
        if !to_nack.is_empty() {
            self.leaser.nack(to_nack).await;
        }
    }

    /// Extends leases for messages under lease management
    ///
    /// Drops messages whose lease deadline cannot be extended any further.
    pub(super) async fn extend(&mut self) {
        let batches = self.leases.retain(self.max_lease_extension);
        for ack_ids in batches {
            // TODO(#3975) - send RPCs concurrently
            self.leaser.extend(ack_ids).await;
        }
    }

    /// Shutdown the leaser
    ///
    /// This flushes all pending acks and nacks all other messages.
    pub(super) async fn shutdown(mut self) {
        // TODO(#4869) - support `WaitForProcessing` shutdown behavior.
        self.leases.evict();

        // TODO(#3975) - await these concurrently.
        let (to_ack, to_nack) = self.leases.flush();
        if !to_ack.is_empty() {
            self.leaser.ack(to_ack).await;
        }

        // TODO(#4847) - this nack needs to be broken into batches.
        if !to_nack.is_empty() {
            self.leaser.nack(to_nack).await;
        }
    }
}

#[cfg(test)]
pub(super) mod tests {
    use super::super::leaser::tests::MockLeaser;
    use super::*;
    use std::collections::HashSet;
    use std::sync::Arc;
    use tokio::sync::mpsc::unbounded_channel;
    use tokio::time::interval;

    // Cover the constant, converting it to an integer for convenience.
    const MAX_IDS_PER_RPC: i32 = super::MAX_IDS_PER_RPC as i32;

    // Any valid `Interval` will do.
    fn test_interval() -> Interval {
        interval(Duration::from_secs(1))
    }

    fn test_duration() -> Duration {
        Duration::from_secs(123)
    }

    #[derive(Debug)]
    pub(super) struct TestLeases {
        pub(super) under_lease: Vec<String>,
        pub(super) to_ack: Vec<String>,
        pub(super) to_nack: Vec<String>,
    }

    pub(in super::super) fn test_id(v: i32) -> String {
        format!("{v:05}")
    }

    pub(in super::super) fn test_ids(range: std::ops::Range<i32>) -> Vec<String> {
        range.map(test_id).collect()
    }

    pub(in super::super) fn sorted(v: &[String]) -> Vec<String> {
        let mut s = v.to_owned();
        s.sort();
        s
    }

    pub(in super::super) fn test_info() -> LeaseInfo {
        LeaseInfo::AtLeastOnce(Instant::now())
    }

    #[tokio::test(start_paused = true)]
    async fn basic_add_ack_nack() {
        let mock = MockLeaser::new();
        let mut state = LeaseState::new(Arc::new(mock), LeaseOptions::default());
        assert_eq!(
            TestLeases {
                under_lease: Vec::new(),
                to_ack: Vec::new(),
                to_nack: Vec::new(),
            },
            state.leases
        );

        state.add(test_id(1), test_info());
        assert_eq!(
            TestLeases {
                under_lease: vec![test_id(1)],
                to_ack: Vec::new(),
                to_nack: Vec::new(),
            },
            state.leases
        );

        state.add(test_id(2), test_info());
        assert_eq!(
            TestLeases {
                under_lease: vec![test_id(1), test_id(2)],
                to_ack: Vec::new(),
                to_nack: Vec::new(),
            },
            state.leases
        );

        state.add(test_id(3), test_info());
        assert_eq!(
            TestLeases {
                under_lease: vec![test_id(1), test_id(2), test_id(3)],
                to_ack: Vec::new(),
                to_nack: Vec::new(),
            },
            state.leases
        );

        state.ack(test_id(1));
        assert_eq!(
            TestLeases {
                under_lease: vec![test_id(2), test_id(3)],
                to_ack: vec![test_id(1)],
                to_nack: Vec::new(),
            },
            state.leases
        );

        state.nack(test_id(2));
        assert_eq!(
            TestLeases {
                under_lease: vec![test_id(3)],
                to_ack: vec![test_id(1)],
                to_nack: vec![test_id(2)],
            },
            state.leases
        );

        state.add(test_id(4), test_info());
        assert_eq!(
            TestLeases {
                under_lease: vec![test_id(3), test_id(4)],
                to_ack: vec![test_id(1)],
                to_nack: vec![test_id(2)],
            },
            state.leases
        );

        state.ack(test_id(4));
        assert_eq!(
            TestLeases {
                under_lease: vec![test_id(3)],
                to_ack: vec![test_id(1), test_id(4)],
                to_nack: vec![test_id(2)],
            },
            state.leases
        );

        state.nack(test_id(3));
        assert_eq!(
            TestLeases {
                under_lease: Vec::new(),
                to_ack: vec![test_id(1), test_id(4)],
                to_nack: vec![test_id(2), test_id(3)],
            },
            state.leases
        );
    }

    #[tokio::test]
    async fn leaser_noops() {
        let mock = MockLeaser::new();
        // Note that there are no calls expected into the leaser, as there are
        // no messages under lease management.
        let mut state = LeaseState::new(Arc::new(mock), LeaseOptions::default());
        state.extend().await;
        state.flush().await;
        state.shutdown().await;
    }

    #[tokio::test(start_paused = true)]
    async fn flush() {
        let mut mock = MockLeaser::new();
        mock.expect_ack()
            .times(1)
            .withf(|v| sorted(v) == test_ids(0..10))
            .returning(|_| ());
        mock.expect_nack()
            .times(1)
            .withf(|v| sorted(v) == test_ids(10..20))
            .returning(|_| ());

        let mut state = LeaseState::new(Arc::new(mock), LeaseOptions::default());
        for i in 0..100 {
            state.add(test_id(i), test_info());
        }
        for i in 0..10 {
            state.ack(test_id(i));
        }
        for i in 10..20 {
            state.nack(test_id(i));
        }
        assert_eq!(
            TestLeases {
                under_lease: test_ids(20..100),
                to_ack: test_ids(0..10),
                to_nack: test_ids(10..20),
            },
            state.leases
        );

        state.flush().await;
        assert_eq!(
            TestLeases {
                under_lease: test_ids(20..100),
                to_ack: Vec::new(),
                to_nack: Vec::new(),
            },
            state.leases
        );
    }

    #[tokio::test(start_paused = true)]
    async fn extend() {
        let mut seq = mockall::Sequence::new();
        let mut mock = MockLeaser::new();
        mock.expect_extend()
            .times(1)
            .in_sequence(&mut seq)
            .withf(|v| sorted(v) == test_ids(0..10))
            .returning(|_| ());
        mock.expect_extend()
            .times(1)
            .in_sequence(&mut seq)
            .withf(|v| sorted(v) == test_ids(0..20))
            .returning(|_| ());
        mock.expect_extend()
            .times(1)
            .in_sequence(&mut seq)
            .withf(|v| sorted(v) == test_ids(5..20))
            .returning(|_| ());
        mock.expect_extend()
            .times(1)
            .in_sequence(&mut seq)
            .withf(|v| sorted(v) == test_ids(10..20))
            .returning(|_| ());

        let mut state = LeaseState::new(Arc::new(mock), LeaseOptions::default());

        // Accept 10 messages. These are now under lease management.
        for i in 0..10 {
            state.add(test_id(i), test_info());
        }
        state.extend().await;

        // Accept another 10 messages. These are now under lease management.
        for i in 10..20 {
            state.add(test_id(i), test_info());
        }
        state.extend().await;

        // Ack the first 5 messages. We should not extend these leases.
        for i in 0..5 {
            state.ack(test_id(i));
        }
        state.extend().await;

        // Nack the next 5 messages. We should not extend these leases.
        for i in 5..10 {
            state.nack(test_id(i));
        }
        state.extend().await;
    }

    #[tokio::test]
    async fn shutdown() {
        let mut mock = MockLeaser::new();
        mock.expect_ack()
            .times(1)
            .withf(|v| sorted(v) == test_ids(0..10))
            .returning(|_| ());
        mock.expect_nack()
            .times(1)
            .withf(|v| sorted(v) == test_ids(10..30))
            .returning(|_| ());

        let mut state = LeaseState::new(Arc::new(mock), LeaseOptions::default());
        for i in 0..30 {
            state.add(test_id(i), test_info());
        }
        for i in 0..10 {
            state.ack(test_id(i));
        }
        for i in 10..20 {
            state.nack(test_id(i));
        }
        state.shutdown().await;
    }

    #[tokio::test(start_paused = true)]
    async fn ack_out_of_lease_included() {
        let mock = MockLeaser::new();
        let mut state = LeaseState::new(Arc::new(mock), LeaseOptions::default());
        assert_eq!(
            TestLeases {
                under_lease: Vec::new(),
                to_ack: Vec::new(),
                to_nack: Vec::new(),
            },
            state.leases
        );

        state.ack(test_id(1));
        assert_eq!(
            TestLeases {
                under_lease: Vec::new(),
                to_ack: vec![test_id(1)],
                to_nack: Vec::new(),
            },
            state.leases
        );
    }

    #[tokio::test(start_paused = true)]
    async fn nack_out_of_lease_ignored() {
        let mock = MockLeaser::new();
        let mut state = LeaseState::new(Arc::new(mock), LeaseOptions::default());
        assert_eq!(
            TestLeases {
                under_lease: Vec::new(),
                to_ack: Vec::new(),
                to_nack: Vec::new(),
            },
            state.leases
        );

        state.nack(test_id(1));
        assert_eq!(
            TestLeases {
                under_lease: Vec::new(),
                to_ack: Vec::new(),
                to_nack: Vec::new(),
            },
            state.leases
        );
    }

    #[tokio::test(start_paused = true)]
    async fn lease_events_timing() {
        let start = Instant::now();

        // We expect flushes at time t=[1, 3, 5, 7, ...]
        const FLUSH_START: Duration = Duration::from_secs(1);
        const FLUSH_PERIOD: Duration = Duration::from_secs(2);

        // We expect extensions at time t=[2, 6, 10, 14, ...]
        const EXTEND_START: Duration = Duration::from_secs(2);
        const EXTEND_PERIOD: Duration = Duration::from_secs(4);

        let mock = MockLeaser::new();
        let options = LeaseOptions {
            flush_start: FLUSH_START,
            flush_period: FLUSH_PERIOD,
            extend_start: EXTEND_START,
            extend_period: EXTEND_PERIOD,
            ..Default::default()
        };
        let mut state = LeaseState::new(Arc::new(mock), options);

        assert_eq!(state.next_event().await, LeaseEvent::Flush);
        assert_eq!(start.elapsed(), Duration::from_secs(1));

        assert_eq!(state.next_event().await, LeaseEvent::Extend);
        assert_eq!(start.elapsed(), Duration::from_secs(2));

        assert_eq!(state.next_event().await, LeaseEvent::Flush);
        assert_eq!(start.elapsed(), Duration::from_secs(3));

        assert_eq!(state.next_event().await, LeaseEvent::Flush);
        assert_eq!(start.elapsed(), Duration::from_secs(5));

        assert_eq!(state.next_event().await, LeaseEvent::Extend);
        assert_eq!(start.elapsed(), Duration::from_secs(6));

        assert_eq!(state.next_event().await, LeaseEvent::Flush);
        assert_eq!(start.elapsed(), Duration::from_secs(7));
    }

    #[tokio::test(start_paused = true)]
    async fn limit_size_of_ack_batch() {
        let start = Instant::now();

        const FLUSH_START: Duration = Duration::from_secs(1);

        let mut mock = MockLeaser::new();
        mock.expect_ack()
            .times(1)
            .withf(|v| sorted(v) == test_ids(0..MAX_IDS_PER_RPC))
            .returning(|_| ());
        let options = LeaseOptions {
            flush_start: FLUSH_START,
            flush_period: Duration::from_secs(100),
            extend_start: Duration::from_secs(100),
            extend_period: Duration::from_secs(100),
            ..Default::default()
        };
        let mut state = LeaseState::new(Arc::new(mock), options);

        for i in 0..MAX_IDS_PER_RPC {
            state.add(test_id(i), test_info());
            state.ack(test_id(i));
        }
        // With 2500 pending acks, the batch is full. We should flush it now.
        assert_eq!(state.next_event().await, LeaseEvent::Flush);
        assert_eq!(start.elapsed(), Duration::ZERO);
        state.flush().await;

        // With 1000 pending acks, the batch is not full. The next event should
        // occur on the interval timer.
        for i in MAX_IDS_PER_RPC..MAX_IDS_PER_RPC + 1000 {
            state.add(test_id(i), test_info());
            state.ack(test_id(i));
        }
        assert_eq!(state.next_event().await, LeaseEvent::Flush);
        assert_eq!(start.elapsed(), FLUSH_START);
    }

    #[tokio::test(start_paused = true)]
    async fn limit_size_of_nack_batch() {
        let start = Instant::now();

        const FLUSH_START: Duration = Duration::from_secs(1);

        let mut mock = MockLeaser::new();
        mock.expect_nack()
            .times(1)
            .withf(|v| sorted(v) == test_ids(0..MAX_IDS_PER_RPC))
            .returning(|_| ());
        let options = LeaseOptions {
            flush_start: FLUSH_START,
            flush_period: Duration::from_secs(100),
            extend_start: Duration::from_secs(100),
            extend_period: Duration::from_secs(100),
            ..Default::default()
        };
        let mut state = LeaseState::new(Arc::new(mock), options);

        for i in 0..MAX_IDS_PER_RPC {
            state.add(test_id(i), test_info());
            state.nack(test_id(i));
        }
        // With 2500 pending nacks, the batch is full. We should flush it now.
        assert_eq!(state.next_event().await, LeaseEvent::Flush);
        assert_eq!(start.elapsed(), Duration::ZERO);
        state.flush().await;

        // With 1000 pending nacks, the batch is not full. The next event should
        // occur on the interval timer.
        for i in MAX_IDS_PER_RPC..MAX_IDS_PER_RPC + 1000 {
            state.add(test_id(i), test_info());
            state.nack(test_id(i));
        }
        assert_eq!(state.next_event().await, LeaseEvent::Flush);
        assert_eq!(start.elapsed(), FLUSH_START);
    }

    #[tokio::test(start_paused = true)]
    async fn ack_and_nack_batches_are_independent() {
        let start = Instant::now();

        const FLUSH_START: Duration = Duration::from_secs(1);

        let mock = MockLeaser::new();
        let options = LeaseOptions {
            flush_start: FLUSH_START,
            flush_period: Duration::from_secs(100),
            extend_start: Duration::from_secs(100),
            extend_period: Duration::from_secs(100),
            ..Default::default()
        };
        let mut state = LeaseState::new(Arc::new(mock), options);

        let over_half_full = MAX_IDS_PER_RPC / 2 + 100;
        for i in 0..over_half_full {
            state.add(test_id(i), test_info());
            state.ack(test_id(i));

            state.add(test_id(over_half_full + i), test_info());
            state.nack(test_id(over_half_full + i));
        }

        // While there are more than `MAX_IDS_PER_RPC` total messages under
        // lease management, neither the ack batch nor the nack batch are full.
        // The next event should occur on the interval timer.
        assert_eq!(state.next_event().await, LeaseEvent::Flush);
        assert_eq!(start.elapsed(), FLUSH_START);
    }

    #[tokio::test(start_paused = true)]
    async fn limit_size_of_extends() -> anyhow::Result<()> {
        const NUM_BATCHES: i32 = 5;

        // We use this channel to surface ack_ids from the mock expectation.
        let (ack_id_tx, mut ack_id_rx) = unbounded_channel();

        let mut mock = MockLeaser::new();
        mock.expect_extend()
            .times(NUM_BATCHES as usize)
            .returning(move |ack_ids| {
                ack_id_tx
                    .send(ack_ids)
                    .expect("sending on channel always succeeds");
            });
        let mut state = LeaseState::new(Arc::new(mock), LeaseOptions::default());

        let mut want = HashSet::new();
        for i in 0..NUM_BATCHES * MAX_IDS_PER_RPC {
            state.add(test_id(i), test_info());

            // All ack IDs should be extended.
            want.insert(test_id(i));
        }
        state.extend().await;

        let mut got = HashSet::new();
        for i in 0..NUM_BATCHES {
            let Some(ack_ids) = ack_id_rx.recv().await else {
                anyhow::bail!("expected batch {i}/{NUM_BATCHES}");
            };
            assert_eq!(ack_ids.len(), MAX_IDS_PER_RPC as usize);
            for ack_id in ack_ids {
                got.insert(ack_id);
            }
        }
        assert!(
            ack_id_rx.is_empty(),
            "There should be exactly {NUM_BATCHES} batches"
        );

        // Make sure all ack IDs were extended.
        assert_eq!(got, want);
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn message_expiration() -> anyhow::Result<()> {
        const MAX_LEASE_EXTENSION: Duration = Duration::from_secs(300);
        const DELTA: Duration = Duration::from_secs(1);

        let mut seq = mockall::Sequence::new();
        let mut mock = MockLeaser::new();
        mock.expect_extend()
            .times(1)
            .in_sequence(&mut seq)
            .withf(|v| sorted(v) == test_ids(0..20))
            .returning(|_| ());
        mock.expect_extend()
            .times(1)
            .in_sequence(&mut seq)
            .withf(|v| sorted(v) == test_ids(10..20))
            .returning(|_| ());

        let options = LeaseOptions {
            max_lease_extension: MAX_LEASE_EXTENSION,
            ..Default::default()
        };
        let mut state = LeaseState::new(Arc::new(mock), options);

        // Add 10 messages under lease management
        for i in 0..10 {
            state.add(test_id(i), test_info());
        }

        // Add 10 more messages under lease management, a little later.
        tokio::time::advance(DELTA * 2).await;
        for i in 10..20 {
            state.add(test_id(i), test_info());
        }
        state.extend().await;

        // Advance the time past the expiration of the original 10 messages.
        tokio::time::advance(MAX_LEASE_EXTENSION - DELTA).await;
        state.extend().await;

        // Advance the time past the expiration of the subsequent 10 messages.
        tokio::time::advance(DELTA * 2).await;
        state.extend().await;

        Ok(())
    }
}
