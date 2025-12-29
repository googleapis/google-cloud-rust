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
use std::collections::HashSet;
use tokio::time::{Duration, Instant, Interval, interval_at};

pub(crate) struct LeaseOptions {
    /// How often we flush acks/nacks
    pub(crate) flush_period: Duration,
    /// How long we wait for the initial flush
    pub(crate) flush_start: Duration,
    /// How often we extend deadlines for messages under lease
    pub(crate) extend_period: Duration,
    /// How long we wait for the initial extensions
    pub(crate) extend_start: Duration,
}

impl Default for LeaseOptions {
    fn default() -> Self {
        LeaseOptions {
            flush_period: Duration::from_secs(1),
            flush_start: Duration::from_secs(1),
            extend_period: Duration::from_secs(3),
            extend_start: Duration::from_millis(500),
        }
    }
}

#[derive(Debug)]
pub(crate) struct LeaseState<L>
where
    L: Leaser,
{
    // TODO(#3957) - support message expiry
    under_lease: HashSet<String>,
    to_ack: Vec<String>,
    to_nack: Vec<String>,
    // TODO(#3964) - support exactly once acks
    leaser: L,

    // A timer for flushing acks/nacks
    flush_interval: Interval,
    // A timer for extending leases
    extend_interval: Interval,
}

/// Actions taken by the `LeaseState` in the lease loop.
#[derive(Debug, PartialEq)]
pub(crate) enum LeaseEvent {
    /// Flush acks/nacks
    Flush,
    /// Extend leases
    Extend,
}

impl<L> LeaseState<L>
where
    L: Leaser,
{
    pub(crate) fn new(leaser: L, options: LeaseOptions) -> Self {
        let flush_interval =
            interval_at(Instant::now() + options.flush_start, options.flush_period);
        let extend_interval =
            interval_at(Instant::now() + options.extend_start, options.extend_period);
        Self {
            under_lease: HashSet::new(),
            to_ack: Vec::new(),
            to_nack: Vec::new(),
            leaser,
            flush_interval,
            extend_interval,
        }
    }

    /// A future that fires when it is time to either:
    /// - flush acks/nacks
    /// - extend leases
    ///
    /// We need to centralize event handling because the lease loop can only
    /// hold one mutable reference to `LeaseState` within its `select!`
    /// statement.
    pub(crate) async fn next_event(&mut self) -> LeaseEvent {
        // TODO(#3972) - flush on size if an `Acknowledge` or
        // `ModifyAckDeadline` RPC is full.

        tokio::select! {
            _ = self.flush_interval.tick() => LeaseEvent::Flush,
            _ = self.extend_interval.tick() => LeaseEvent::Extend,
        }
    }

    /// Accept a new ack ID under lease management
    pub(crate) fn add(&mut self, ack_id: String) {
        self.under_lease.insert(ack_id);
    }

    /// Process an ack from the application
    pub(crate) fn ack(&mut self, ack_id: String) {
        self.under_lease.remove(&ack_id);
        // Unconditionally add the ack ID to the next ack batch. It doesn't hurt
        // to optimistically add it, even if its lease has expired.
        self.to_ack.push(ack_id);
    }

    /// Process a nack from the application
    pub(crate) fn nack(&mut self, ack_id: String) {
        if self.under_lease.remove(&ack_id) {
            // Only add the ack ID to the nack batch if the message is under our
            // lease. If the message's lease has already expired, we do not need
            // to take any additional action.
            self.to_nack.push(ack_id);
        }
    }

    /// Flush pending acks/nacks
    pub(crate) async fn flush(&mut self) {
        let to_ack = std::mem::take(&mut self.to_ack);
        let to_nack = std::mem::take(&mut self.to_nack);
        // TODO(#3975) - await these concurrently.
        self.leaser.ack(to_ack).await;
        self.leaser.nack(to_nack).await;
    }

    /// Extends leases for messages under lease management
    ///
    /// Drops messages whose lease deadline cannot be extended any further.
    pub(crate) async fn extend(&mut self) {
        // TODO(#3957) - drop expired messages
        let under_lease: Vec<String> = self.under_lease.iter().cloned().collect();
        self.leaser.extend(under_lease).await;
    }

    /// Shutdown the leaser
    ///
    /// This flushes all pending acks and nacks all other messages.
    pub(crate) async fn shutdown(self) {
        let mut to_nack = self.to_nack;
        to_nack.extend(self.under_lease.into_iter());
        // TODO(#3975) - await these concurrently.
        self.leaser.ack(self.to_ack).await;
        self.leaser.nack(to_nack).await;
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
pub(crate) mod tests {
    use super::super::leaser::tests::MockLeaser;
    use super::*;
    use tokio::time::interval;

    // Any valid `Interval` will do.
    fn test_interval() -> Interval {
        interval(Duration::from_secs(1))
    }

    fn make_state<L, A, N>(under_lease: L, to_ack: A, to_nack: N) -> LeaseState<MockLeaser>
    where
        L: IntoIterator<Item = &'static str>,
        A: IntoIterator<Item = &'static str>,
        N: IntoIterator<Item = &'static str>,
    {
        LeaseState {
            under_lease: under_lease.into_iter().map(|s| s.to_string()).collect(),
            to_ack: to_ack.into_iter().map(|s| s.to_string()).collect(),
            to_nack: to_nack.into_iter().map(|s| s.to_string()).collect(),
            leaser: MockLeaser::new(),
            flush_interval: test_interval(),
            extend_interval: test_interval(),
        }
    }

    pub(crate) fn test_id(v: i32) -> String {
        format!("{v:03}")
    }

    pub(crate) fn test_ids(range: std::ops::Range<i32>) -> Vec<String> {
        range.map(test_id).collect()
    }

    pub(crate) fn sorted(v: &[String]) -> Vec<String> {
        let mut s = v.to_owned();
        s.sort();
        s
    }

    #[tokio::test]
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
            under_lease: test_ids(20..100).into_iter().collect(),
            to_ack: test_ids(0..10),
            to_nack: test_ids(10..20),
            leaser: MockLeaser::new(),
            flush_interval: test_interval(),
            extend_interval: test_interval(),
        };
        assert_eq!(state, expected);

        state.flush().await;
        let expected = LeaseState {
            under_lease: test_ids(20..100).into_iter().collect(),
            to_ack: Vec::new(),
            to_nack: Vec::new(),
            leaser: MockLeaser::new(),
            flush_interval: test_interval(),
            extend_interval: test_interval(),
        };
        assert_eq!(state, expected);
    }

    #[tokio::test]
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

    #[tokio::test]
    async fn ack_out_of_lease_included() {
        let mock = MockLeaser::new();
        let mut state = LeaseState::new(mock, LeaseOptions::default());
        assert_eq!(state, make_state([], [], []));

        state.ack("1".to_string());
        assert_eq!(state, make_state([], ["1"], []));
    }

    #[tokio::test]
    async fn nack_out_of_lease_ignored() {
        let mock = MockLeaser::new();
        let mut state = LeaseState::new(mock, LeaseOptions::default());
        assert_eq!(state, make_state([], [], []));

        state.nack("1".to_string());
        assert_eq!(state, make_state([], [], []));
    }

    #[tokio::test(start_paused = true)]
    async fn lease_events() {
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
}
