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

use super::leaser::Leaser;
use std::collections::HashMap;
// Use a `tokio::time::Instant` to facilitate time-based unit testing.
use tokio::time::{Duration, Instant, Interval, interval_at};

// An ack ID is less than 200 bytes. The limit for a request is 512kB. It should
// be safe to fit 2500 Ack IDs in a single RPC.
//
// https://docs.cloud.google.com/pubsub/quotas
const ACK_IDS_PER_RPC: usize = 2500;

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
            flush_period: Duration::from_secs(1),
            flush_start: Duration::from_secs(1),
            extend_period: Duration::from_secs(3),
            extend_start: Duration::from_millis(500),
            max_lease_extension: Duration::from_secs(600),
        }
    }
}

#[derive(Debug)]
pub(super) struct LeaseState<L>
where
    L: Leaser,
{
    // A map of ack IDs to the time they were first received.
    under_lease: HashMap<String, Instant>,
    to_ack: Vec<String>,
    to_nack: Vec<String>,
    // TODO(#3964) - support exactly once acks
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
    L: Leaser,
{
    pub(super) fn new(leaser: L, options: LeaseOptions) -> Self {
        let flush_interval =
            interval_at(Instant::now() + options.flush_start, options.flush_period);
        let extend_interval =
            interval_at(Instant::now() + options.extend_start, options.extend_period);
        Self {
            under_lease: HashMap::new(),
            to_ack: Vec::new(),
            to_nack: Vec::new(),
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
        if self.to_ack.len() >= ACK_IDS_PER_RPC || self.to_nack.len() >= ACK_IDS_PER_RPC {
            // This is an OR because `Acknowledge` and `ModifyAckDeadline` are
            // separate RPCs, with separate limits.
            return LeaseEvent::Flush;
        }

        tokio::select! {
            _ = self.flush_interval.tick() => LeaseEvent::Flush,
            _ = self.extend_interval.tick() => LeaseEvent::Extend,
        }
    }

    /// Accept a new ack ID under lease management
    pub(super) fn add(&mut self, ack_id: String) {
        self.under_lease.insert(ack_id, Instant::now());
    }

    /// Process an ack from the application
    pub(super) fn ack(&mut self, ack_id: String) {
        self.under_lease.remove(&ack_id);
        // Unconditionally add the ack ID to the next ack batch. It doesn't hurt
        // to optimistically add it, even if its lease has expired.
        self.to_ack.push(ack_id);
    }

    /// Process a nack from the application
    pub(super) fn nack(&mut self, ack_id: String) {
        if self.under_lease.remove(&ack_id).is_some() {
            // Only add the ack ID to the nack batch if the message is under our
            // lease. If the message's lease has already expired, we do not need
            // to take any additional action.
            self.to_nack.push(ack_id);
        }
    }

    /// Flush pending acks/nacks
    pub(super) async fn flush(&mut self) {
        let to_ack = std::mem::take(&mut self.to_ack);
        let to_nack = std::mem::take(&mut self.to_nack);

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
        let now = Instant::now();
        let mut batches = Vec::new();
        let mut batch = Vec::new();
        self.under_lease.retain(|ack_id, receive_time| {
            // Note that using `HashMap::retain` allows us to iterate over the
            // map and conditionally drop elements in one pass.

            if *receive_time + self.max_lease_extension < now {
                // Drop messages that have been held for too long.
                false
            } else {
                // Extend leases for all other messages.
                batch.push(ack_id.clone());
                if batch.len() == ACK_IDS_PER_RPC {
                    // Flush the batch when it is full.
                    batches.push(std::mem::take(&mut batch));
                }
                true
            }
        });
        if !batch.is_empty() {
            batches.push(batch);
        }
        for ack_ids in batches {
            // TODO(#3975) - send RPCs concurrently
            self.leaser.extend(ack_ids).await;
        }
    }

    /// Shutdown the leaser
    ///
    /// This flushes all pending acks and nacks all other messages.
    pub(super) async fn shutdown(self) {
        // TODO(#3975) - await these concurrently.
        let to_ack = self.to_ack;
        if !to_ack.is_empty() {
            self.leaser.ack(to_ack).await;
        }

        let mut to_nack = self.to_nack;
        to_nack.extend(self.under_lease.into_keys());
        if !to_nack.is_empty() {
            self.leaser.nack(to_nack).await;
        }
    }
}

impl<L> PartialEq for LeaseState<L>
where
    L: Leaser,
{
    fn eq(&self, other: &Self) -> bool {
        self.under_lease == other.under_lease
            && self.to_ack == other.to_ack
            && self.to_nack == other.to_nack
    }
}

#[cfg(test)]
pub(super) mod tests {
    use super::super::leaser::tests::MockLeaser;
    use super::*;
    use std::collections::HashSet;
    use tokio::sync::mpsc::unbounded_channel;
    use tokio::time::interval;

    // Cover the constant, converting it to an integer for convenience.
    const ACK_IDS_PER_RPC: i32 = super::ACK_IDS_PER_RPC as i32;

    // Any valid `Interval` will do.
    fn test_interval() -> Interval {
        interval(Duration::from_secs(1))
    }

    fn test_duration() -> Duration {
        Duration::from_secs(123)
    }

    fn make_state<L, A, N>(under_lease: L, to_ack: A, to_nack: N) -> LeaseState<MockLeaser>
    where
        L: IntoIterator<Item = &'static str>,
        A: IntoIterator<Item = &'static str>,
        N: IntoIterator<Item = &'static str>,
    {
        LeaseState {
            under_lease: under_lease
                .into_iter()
                .map(|s| (s.to_string(), Instant::now()))
                .collect(),
            to_ack: to_ack.into_iter().map(|s| s.to_string()).collect(),
            to_nack: to_nack.into_iter().map(|s| s.to_string()).collect(),
            leaser: MockLeaser::new(),
            flush_interval: test_interval(),
            extend_interval: test_interval(),
            max_lease_extension: test_duration(),
        }
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

    #[tokio::test(start_paused = true)]
    async fn basic_add_ack_nack() {
        let mock = MockLeaser::new();
        let mut state = LeaseState::new(mock, LeaseOptions::default());
        assert_eq!(state, make_state([], [], []));

        state.add("1".to_string());
        assert_eq!(state, make_state(["1"], [], []));

        state.add("2".to_string());
        assert_eq!(state, make_state(["1", "2"], [], []));

        state.add("3".to_string());
        assert_eq!(state, make_state(["1", "2", "3"], [], []));

        state.ack("1".to_string());
        assert_eq!(state, make_state(["2", "3"], ["1"], []));

        state.nack("2".to_string());
        assert_eq!(state, make_state(["3"], ["1"], ["2"]));

        state.add("4".to_string());
        assert_eq!(state, make_state(["3", "4"], ["1"], ["2"]));

        state.ack("4".to_string());
        assert_eq!(state, make_state(["3"], ["1", "4"], ["2"]));

        state.nack("3".to_string());
        assert_eq!(state, make_state([], ["1", "4"], ["2", "3"]));
    }

    #[tokio::test]
    async fn leaser_noops() {
        let mock = MockLeaser::new();
        // Note that there are no calls expected into the leaser, as there are
        // no messages under lease management.
        let mut state = LeaseState::new(mock, LeaseOptions::default());
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

        let mut state = LeaseState::new(mock, LeaseOptions::default());
        for i in 0..100 {
            state.add(test_id(i));
        }
        for i in 0..10 {
            state.ack(test_id(i));
        }
        for i in 10..20 {
            state.nack(test_id(i));
        }
        let expected = LeaseState {
            under_lease: test_ids(20..100)
                .into_iter()
                .map(|s| (s, Instant::now()))
                .collect(),
            to_ack: test_ids(0..10),
            to_nack: test_ids(10..20),
            leaser: MockLeaser::new(),
            flush_interval: test_interval(),
            extend_interval: test_interval(),
            max_lease_extension: test_duration(),
        };
        assert_eq!(state, expected);

        state.flush().await;
        let expected = LeaseState {
            under_lease: test_ids(20..100)
                .into_iter()
                .map(|s| (s, Instant::now()))
                .collect(),
            to_ack: Vec::new(),
            to_nack: Vec::new(),
            leaser: MockLeaser::new(),
            flush_interval: test_interval(),
            extend_interval: test_interval(),
            max_lease_extension: test_duration(),
        };
        assert_eq!(state, expected);
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

        let mut state = LeaseState::new(mock, LeaseOptions::default());

        // Accept 10 messages. These are now under lease management.
        for i in 0..10 {
            state.add(test_id(i));
        }
        state.extend().await;

        // Accept another 10 messages. These are now under lease management.
        for i in 10..20 {
            state.add(test_id(i));
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

        let mut state = LeaseState::new(mock, LeaseOptions::default());
        for i in 0..30 {
            state.add(test_id(i));
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
        let mut state = LeaseState::new(mock, LeaseOptions::default());
        assert_eq!(state, make_state([], [], []));

        state.ack("1".to_string());
        assert_eq!(state, make_state([], ["1"], []));
    }

    #[tokio::test(start_paused = true)]
    async fn nack_out_of_lease_ignored() {
        let mock = MockLeaser::new();
        let mut state = LeaseState::new(mock, LeaseOptions::default());
        assert_eq!(state, make_state([], [], []));

        state.nack("1".to_string());
        assert_eq!(state, make_state([], [], []));
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
        let mut state = LeaseState::new(mock, options);

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
            .withf(|v| sorted(v) == test_ids(0..ACK_IDS_PER_RPC))
            .returning(|_| ());
        let options = LeaseOptions {
            flush_start: FLUSH_START,
            flush_period: Duration::from_secs(100),
            extend_start: Duration::from_secs(100),
            extend_period: Duration::from_secs(100),
            ..Default::default()
        };
        let mut state = LeaseState::new(mock, options);

        for i in 0..ACK_IDS_PER_RPC {
            state.add(test_id(i));
            state.ack(test_id(i));
        }
        // With 2500 pending acks, the batch is full. We should flush it now.
        assert_eq!(state.next_event().await, LeaseEvent::Flush);
        assert_eq!(start.elapsed(), Duration::ZERO);
        state.flush().await;

        // With 1000 pending acks, the batch is not full. The next event should
        // occur on the interval timer.
        for i in ACK_IDS_PER_RPC..ACK_IDS_PER_RPC + 1000 {
            state.add(test_id(i));
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
            .withf(|v| sorted(v) == test_ids(0..ACK_IDS_PER_RPC))
            .returning(|_| ());
        let options = LeaseOptions {
            flush_start: FLUSH_START,
            flush_period: Duration::from_secs(100),
            extend_start: Duration::from_secs(100),
            extend_period: Duration::from_secs(100),
            ..Default::default()
        };
        let mut state = LeaseState::new(mock, options);

        for i in 0..ACK_IDS_PER_RPC {
            state.add(test_id(i));
            state.nack(test_id(i));
        }
        // With 2500 pending nacks, the batch is full. We should flush it now.
        assert_eq!(state.next_event().await, LeaseEvent::Flush);
        assert_eq!(start.elapsed(), Duration::ZERO);
        state.flush().await;

        // With 1000 pending nacks, the batch is not full. The next event should
        // occur on the interval timer.
        for i in ACK_IDS_PER_RPC..ACK_IDS_PER_RPC + 1000 {
            state.add(test_id(i));
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
        let mut state = LeaseState::new(mock, options);

        let over_half_full = ACK_IDS_PER_RPC / 2 + 100;
        for i in 0..over_half_full {
            state.add(test_id(i));
            state.ack(test_id(i));

            state.add(test_id(over_half_full + i));
            state.nack(test_id(over_half_full + i));
        }

        // While there are more than `ACK_IDS_PER_RPC` total messages under
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
        let mut state = LeaseState::new(mock, LeaseOptions::default());

        let mut want = HashSet::new();
        for i in 0..NUM_BATCHES * ACK_IDS_PER_RPC {
            state.add(test_id(i));

            // All ack IDs should be extended.
            want.insert(test_id(i));
        }
        state.extend().await;

        let mut got = HashSet::new();
        for i in 0..NUM_BATCHES {
            let Some(ack_ids) = ack_id_rx.recv().await else {
                anyhow::bail!("expected batch {i}/{NUM_BATCHES}");
            };
            assert_eq!(ack_ids.len(), ACK_IDS_PER_RPC as usize);
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
        let mut state = LeaseState::new(mock, options);

        // Add 10 messages under lease management
        for i in 0..10 {
            state.add(test_id(i));
        }

        // Add 10 more messages under lease management, a little later.
        tokio::time::advance(DELTA * 2).await;
        for i in 10..20 {
            state.add(test_id(i));
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
