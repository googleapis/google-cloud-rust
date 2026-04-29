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

pub(super) use exactly_once::NACK_SHUTDOWN_ERROR;

use super::handler::AckResult;
use super::handler::Action;
use super::leaser::ConfirmedAcks;
use super::leaser::Leaser;
use super::shutdown_behavior::ShutdownBehavior;
use at_least_once::Leases;
use exactly_once::Leases as EoLeases;
use tokio::sync::oneshot::Sender;
// Use a `tokio::time::Instant` to facilitate time-based unit testing.
use tokio::task::JoinSet;
use tokio::time::{Duration, Instant, Interval, interval_at};
use tokio_util::task::TaskTracker;

// Request sizes are limited to 512kB. 500 bytes is a conservative upper limit
// on the size of an ack ID. We can safely fit 1000 ack IDs into a request.
//
// https://docs.cloud.google.com/pubsub/quotas
const MAX_IDS_PER_RPC: usize = 1000;

// How often we extend deadlines for messages under lease
const EXTEND_PERIOD: Duration = Duration::from_secs(3);

/// The buffer applied to the lease extension period to account for network
/// latency and processing time.
const EXTEND_BUFFER: Duration = Duration::from_secs(2);

// Helper function to chunk ack ids into chunks of MAX_IDS_PER_RPC.
fn batch(ack_ids: Vec<String>) -> Vec<Vec<String>> {
    ack_ids
        .chunks(MAX_IDS_PER_RPC)
        .map(|c| c.to_vec())
        .collect()
}

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
    /// extended as long as `max_lease` has not elapsed.
    pub(super) max_lease: Duration,
    /// How long a message's lease can be extended by.
    pub(super) max_lease_extension: Duration,
    /// The shutdown behavior of the lease loop
    pub(super) shutdown_behavior: ShutdownBehavior,
}

impl Default for LeaseOptions {
    fn default() -> Self {
        LeaseOptions {
            flush_period: Duration::from_millis(100),
            flush_start: Duration::from_millis(100),
            extend_period: EXTEND_PERIOD,
            extend_start: Duration::from_millis(500),
            max_lease: Duration::from_secs(600),
            max_lease_extension: Duration::from_secs(60),
            shutdown_behavior: ShutdownBehavior::WaitForProcessing,
        }
    }
}

#[derive(Debug)]
pub(super) struct NewMessage {
    pub(super) ack_id: String,
    pub(super) lease_info: LeaseInfo,
}

#[derive(Debug)]
pub(super) enum LeaseInfo {
    AtLeastOnce(AtLeastOnceInfo),
    ExactlyOnce(ExactlyOnceInfo),
}

#[derive(Debug)]
pub(super) struct AtLeastOnceInfo {
    receive_time: Instant,
    last_extension: Option<Instant>,
}

impl AtLeastOnceInfo {
    pub(super) fn new() -> Self {
        AtLeastOnceInfo {
            receive_time: Instant::now(),
            last_extension: None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(super) enum MessageStatus {
    Leased,
    /// We are currently trying to ack this message.
    ///
    /// We need to continue to extend these leases because the exactly-once
    /// confirmed ack retry loop can take arbitrarily long.
    ///
    /// The client will not expire leases in this state. The server will
    /// report if a lease has expired. We do not want to mask a success with
    /// a `LeaseExpired` error.
    Acking,
    /// We are currently trying to nack this message.
    ///
    /// We keep it in `under_lease` to hold onto `result_tx` until the nack is confirmed,
    /// but we do not want to extend its lease while we wait.
    Nacking,
}

#[derive(Debug)]
pub(super) struct ExactlyOnceInfo {
    receive_time: Instant,
    result_tx: Sender<AckResult>,
    status: MessageStatus,
    last_extension: Option<Instant>,
}

impl ExactlyOnceInfo {
    pub(super) fn new(result_tx: Sender<AckResult>) -> Self {
        ExactlyOnceInfo {
            receive_time: Instant::now(),
            result_tx,
            status: MessageStatus::Leased,
            last_extension: None,
        }
    }
}

#[derive(Debug)]
pub(super) struct LeaseState<L>
where
    L: Leaser + Clone + Send + 'static,
{
    // Ack IDs with at-least-once semantics under lease management.
    leases: Leases,
    // Ack IDs with exactly-once semantics under lease management.
    eo_leases: EoLeases,
    // The leaser, which performs lease operations - (acks, nacks, lease
    // extensions).
    leaser: L,

    // A timer for flushing acks/nacks
    flush_interval: Interval,
    // A timer for extending leases
    extend_interval: Interval,
    // How long messages can be kept under lease
    max_lease: Duration,
    // How long a message's lease can be extended by
    max_lease_extension: Duration,

    // In flight acks and nacks.
    pending_acks_nacks: TaskTracker,

    // In flight lease extension operations.
    //
    // These are held separate from pending acks/nacks because we do not need to
    // await them on shutdown.
    pending_extends: JoinSet<Vec<String>>,
    eo_pending_extends: JoinSet<Vec<String>>,
}

/// Actions taken by the `LeaseState` in the lease loop.
#[derive(Debug, PartialEq)]
pub(super) enum LeaseEvent {
    /// Flush acks/nacks
    Flush,
    /// Extend leases
    Extend,
    /// Pending extensions completed
    ExtendCompleted(Vec<String>),
    /// Pending exactly-once extensions completed
    ExtendCompletedEO(Vec<String>),
}

impl<L> LeaseState<L>
where
    L: Leaser + Clone + Send + 'static,
{
    pub(super) fn new(leaser: L, options: LeaseOptions) -> Self {
        let flush_interval =
            interval_at(Instant::now() + options.flush_start, options.flush_period);
        let extend_interval =
            interval_at(Instant::now() + options.extend_start, options.extend_period);
        Self {
            leases: Leases::default(),
            eo_leases: EoLeases::default(),
            leaser,
            flush_interval,
            extend_interval,
            max_lease: options.max_lease,
            max_lease_extension: options.max_lease_extension,
            pending_acks_nacks: TaskTracker::new(),
            pending_extends: JoinSet::new(),
            eo_pending_extends: JoinSet::new(),
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
        if self.leases.needs_flush() || self.eo_leases.needs_flush() {
            return LeaseEvent::Flush;
        }

        loop {
            tokio::select! {
                _ = self.flush_interval.tick() => return LeaseEvent::Flush,
                _ = self.extend_interval.tick() => return LeaseEvent::Extend,
                res = self.pending_extends.join_next(), if !self.pending_extends.is_empty() => {
                    if let Some(Ok(ack_ids)) = res {
                        return LeaseEvent::ExtendCompleted(ack_ids);
                    } else {
                        // swallow the JoinError.
                        continue;
                    }
                }
                res = self.eo_pending_extends.join_next(), if !self.eo_pending_extends.is_empty() => {
                    if let Some(Ok(ack_ids)) = res {
                        return LeaseEvent::ExtendCompletedEO(ack_ids);
                    } else {
                        // swallow the JoinError.
                        continue;
                    }
                }
            }
        }
    }

    /// Accept a new ack ID under lease management
    pub(super) fn add(&mut self, ack_id: String, info: LeaseInfo) {
        match info {
            LeaseInfo::AtLeastOnce(i) => {
                self.leases.add(ack_id, i);
            }
            LeaseInfo::ExactlyOnce(i) => {
                self.eo_leases.add(ack_id, i);
            }
        }
    }

    /// Process an action from the application.
    pub(super) fn process(&mut self, action: Action) {
        match action {
            Action::Ack(id) => self.leases.ack(id),
            Action::Nack(id) => self.leases.nack(id),
            Action::ExactlyOnceAck(id) => self.eo_leases.ack(id),
            Action::ExactlyOnceNack(id) => self.eo_leases.nack(id),
        }
    }

    /// Process ack results from the server.
    pub(super) fn confirm(&mut self, results: ConfirmedAcks) {
        for (ack_id, result) in results {
            self.eo_leases.confirm(ack_id, result);
        }
    }

    /// Updates the `last_extension` timestamp for the given at-least-once ack IDs
    /// with the completion time of a successful extension RPC.
    pub(super) fn update_last_extension(&mut self, ack_ids: Vec<String>) {
        self.leases.update_last_extension(&ack_ids);
    }

    /// Updates the `last_extension` timestamp for the given exactly-once ack IDs
    /// with the completion time of a successful extension RPC.
    pub(super) fn update_last_extension_eo(&mut self, ack_ids: Vec<String>) {
        self.eo_leases.update_last_extension(&ack_ids);
    }

    /// Flush pending acks/nacks
    pub(super) fn flush(&mut self) {
        let (to_ack, to_nack) = self.leases.drain();
        if !to_ack.is_empty() {
            let leaser = self.leaser.clone();
            self.pending_acks_nacks
                .spawn(async move { leaser.ack(to_ack).await });
        }
        if !to_nack.is_empty() {
            let leaser = self.leaser.clone();
            self.pending_acks_nacks
                .spawn(async move { leaser.nack(to_nack).await });
        }

        let (to_ack, to_nack) = self.eo_leases.drain();
        if !to_ack.is_empty() {
            let leaser = self.leaser.clone();
            self.pending_acks_nacks
                .spawn(async move { leaser.confirmed_ack(to_ack).await });
        }
        if !to_nack.is_empty() {
            let leaser = self.leaser.clone();
            self.pending_acks_nacks
                .spawn(async move { leaser.confirmed_nack(to_nack).await });
        }
    }

    /// Extends leases for messages under lease management
    ///
    /// Drops messages whose lease deadline cannot be extended any further.
    pub(super) fn extend(&mut self) {
        let batches = self.leases.retain(self.max_lease, self.max_lease_extension);
        for ack_ids in batches {
            let leaser = self.leaser.clone();
            self.pending_extends
                .spawn(async move { leaser.extend(ack_ids).await });
        }

        let batches = self
            .eo_leases
            .retain(self.max_lease, self.max_lease_extension);
        for ack_ids in batches {
            let leaser = self.leaser.clone();
            self.eo_pending_extends
                .spawn(async move { leaser.extend(ack_ids).await });
        }
    }

    /// Shutdown the leaser
    ///
    /// This flushes all pending acks and nacks all other messages.
    pub(super) async fn shutdown(self) {
        // Note that if `WaitForProcessing` was selected by the application,
        // there are no messages under lease. They have all been processed.
        let (to_ack, to_nack) = self.leases.evict_and_drain();
        if !to_ack.is_empty() {
            let leaser = self.leaser.clone();
            self.pending_acks_nacks
                .spawn(async move { leaser.ack(to_ack).await });
        }
        for to_nack in to_nack {
            let leaser = self.leaser.clone();
            self.pending_acks_nacks
                .spawn(async move { leaser.nack(to_nack).await });
        }

        // TODO(#5109) - evicting exactly-once leases is ok, but not ideal.
        // Currently, evict returns NACK_SHUTDOWN_ERROR for all exactly once
        // leases. This includes the to_ack leases. Specifically,
        // the leases that have been acknowledged by the application but not yet
        // flushed. Therefore, we do not need to flush those leases.
        let (_, to_nack) = self.eo_leases.evict_and_drain();
        for to_nack in to_nack {
            let leaser = self.leaser.clone();
            self.pending_acks_nacks
                .spawn(async move { leaser.confirmed_nack(to_nack).await });
        }

        // Wait for pending acks/nacks to complete.
        self.pending_acks_nacks.close();
        self.pending_acks_nacks.wait().await;

        // Wait for pending lease extensions to complete. This is not useful in
        // practice, because we are nacking all the messages, but it simplifies
        // our tests.
        #[cfg(test)]
        {
            self.pending_extends.join_all().await;
            self.eo_pending_extends.join_all().await;
        }
    }
}

#[cfg(test)]
pub(super) mod tests {
    use super::super::leaser::tests::MockLeaser;
    use super::Action::{Ack, ExactlyOnceAck, ExactlyOnceNack, Nack};
    use super::*;
    use std::collections::HashMap;
    use std::collections::HashSet;
    use std::sync::Arc;
    use test_case::test_case;
    use tokio::sync::mpsc::unbounded_channel;
    use tokio::sync::oneshot::channel;

    // Cover the constant, converting it to an integer for convenience.
    const MAX_IDS_PER_RPC: i32 = super::MAX_IDS_PER_RPC as i32;

    #[derive(Debug)]
    pub(super) struct TestLeases {
        pub(super) under_lease: Vec<String>,
        pub(super) to_ack: Vec<String>,
        pub(super) to_nack: Vec<String>,
    }

    #[derive(Debug)]
    pub(super) struct Batches {
        pub(super) counts: Vec<i32>,
        pub(super) ack_ids: Vec<String>,
    }

    impl Batches {
        pub(super) fn flatten(to_nack: Vec<Vec<String>>) -> Self {
            let counts = to_nack.iter().map(|v| v.len() as i32).collect();
            let ack_ids = to_nack.into_iter().flatten().collect();
            Self { counts, ack_ids }
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

    pub(in super::super) fn at_least_once_info() -> LeaseInfo {
        LeaseInfo::AtLeastOnce(AtLeastOnceInfo::new())
    }

    pub(in super::super) fn exactly_once_info() -> LeaseInfo {
        let (result_tx, _result_rx) = channel();
        LeaseInfo::ExactlyOnce(ExactlyOnceInfo::new(result_tx))
    }

    async fn extend_and_await<L>(state: &mut LeaseState<L>)
    where
        L: Leaser + Clone + Send + 'static,
    {
        state.extend();
        let pending_extends = std::mem::take(&mut state.pending_extends);
        let _ = pending_extends.join_all().await;
        let eo_pending_extends = std::mem::take(&mut state.eo_pending_extends);
        let _ = eo_pending_extends.join_all().await;
    }

    #[tokio::test(start_paused = true)]
    async fn update_last_extension() {
        let mock = MockLeaser::new();
        let mut state = LeaseState::new(Arc::new(mock), LeaseOptions::default());

        // Test at-least-once
        state.add(test_id(1), at_least_once_info());
        state.update_last_extension(test_ids(1..2));

        // Verify no extension immediately
        let batches = state
            .leases
            .retain(Duration::from_secs(600), Duration::from_secs(60));
        assert!(
            batches.is_empty(),
            "lease should not be extended since time has not advanced"
        );

        // Advance time past the extension buffer (60s max_lease_extension)
        tokio::time::advance(Duration::from_secs(61)).await;
        let batches = state
            .leases
            .retain(Duration::from_secs(600), Duration::from_secs(60));
        let flattened = Batches::flatten(batches);
        assert_eq!(flattened.ack_ids, test_ids(1..2));

        // Test exactly-once
        state.add(test_id(2), exactly_once_info());
        state.update_last_extension_eo(test_ids(2..3));

        // Verify no extension immediately
        let batches = state
            .eo_leases
            .retain(Duration::from_secs(600), Duration::from_secs(60));
        assert!(
            batches.is_empty(),
            "lease should not be extended since time has not advanced"
        );

        // Advance time past the extension buffer
        tokio::time::advance(Duration::from_secs(61)).await;
        let batches = state
            .eo_leases
            .retain(Duration::from_secs(600), Duration::from_secs(60));
        let flattened = Batches::flatten(batches);
        assert_eq!(flattened.ack_ids, test_ids(2..3));
    }

    async fn flush_and_await<L>(state: &mut LeaseState<L>)
    where
        L: Leaser + Clone + Send + 'static,
    {
        state.flush();
        let pending_acks_nacks = std::mem::take(&mut state.pending_acks_nacks);
        pending_acks_nacks.close();
        pending_acks_nacks.wait().await;
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

        state.add(test_id(1), at_least_once_info());
        assert_eq!(
            TestLeases {
                under_lease: vec![test_id(1)],
                to_ack: Vec::new(),
                to_nack: Vec::new(),
            },
            state.leases
        );

        state.add(test_id(2), at_least_once_info());
        assert_eq!(
            TestLeases {
                under_lease: vec![test_id(1), test_id(2)],
                to_ack: Vec::new(),
                to_nack: Vec::new(),
            },
            state.leases
        );

        state.add(test_id(3), at_least_once_info());
        assert_eq!(
            TestLeases {
                under_lease: vec![test_id(1), test_id(2), test_id(3)],
                to_ack: Vec::new(),
                to_nack: Vec::new(),
            },
            state.leases
        );

        state.process(Ack(test_id(1)));
        assert_eq!(
            TestLeases {
                under_lease: vec![test_id(2), test_id(3)],
                to_ack: vec![test_id(1)],
                to_nack: Vec::new(),
            },
            state.leases
        );

        state.process(Nack(test_id(2)));
        assert_eq!(
            TestLeases {
                under_lease: vec![test_id(3)],
                to_ack: vec![test_id(1)],
                to_nack: vec![test_id(2)],
            },
            state.leases
        );

        state.add(test_id(4), at_least_once_info());
        assert_eq!(
            TestLeases {
                under_lease: vec![test_id(3), test_id(4)],
                to_ack: vec![test_id(1)],
                to_nack: vec![test_id(2)],
            },
            state.leases
        );

        state.process(Ack(test_id(4)));
        assert_eq!(
            TestLeases {
                under_lease: vec![test_id(3)],
                to_ack: vec![test_id(1), test_id(4)],
                to_nack: vec![test_id(2)],
            },
            state.leases
        );

        state.process(Nack(test_id(3)));
        assert_eq!(
            TestLeases {
                under_lease: Vec::new(),
                to_ack: vec![test_id(1), test_id(4)],
                to_nack: vec![test_id(2), test_id(3)],
            },
            state.leases
        );
    }

    #[tokio::test(start_paused = true)]
    async fn basic_add_confirmed_ack_nack() {
        let mock = MockLeaser::new();
        let mut state = LeaseState::new(Arc::new(mock), LeaseOptions::default());
        assert_eq!(
            TestLeases {
                under_lease: Vec::new(),
                to_ack: Vec::new(),
                to_nack: Vec::new(),
            },
            state.eo_leases
        );

        state.add(test_id(1), exactly_once_info());
        assert_eq!(
            TestLeases {
                under_lease: vec![test_id(1)],
                to_ack: Vec::new(),
                to_nack: Vec::new(),
            },
            state.eo_leases
        );

        state.add(test_id(2), exactly_once_info());
        assert_eq!(
            TestLeases {
                under_lease: vec![test_id(1), test_id(2)],
                to_ack: Vec::new(),
                to_nack: Vec::new(),
            },
            state.eo_leases
        );

        state.add(test_id(3), exactly_once_info());
        assert_eq!(
            TestLeases {
                under_lease: vec![test_id(1), test_id(2), test_id(3)],
                to_ack: Vec::new(),
                to_nack: Vec::new(),
            },
            state.eo_leases
        );

        state.process(ExactlyOnceAck(test_id(1)));
        assert_eq!(
            TestLeases {
                under_lease: vec![test_id(1), test_id(2), test_id(3)],
                to_ack: vec![test_id(1)],
                to_nack: Vec::new(),
            },
            state.eo_leases
        );

        state.process(ExactlyOnceNack(test_id(2)));
        assert_eq!(
            TestLeases {
                under_lease: vec![test_id(1), test_id(2), test_id(3)],
                to_ack: vec![test_id(1)],
                to_nack: vec![test_id(2)],
            },
            state.eo_leases
        );

        state.add(test_id(4), exactly_once_info());
        assert_eq!(
            TestLeases {
                under_lease: vec![test_id(1), test_id(2), test_id(3), test_id(4)],
                to_ack: vec![test_id(1)],
                to_nack: vec![test_id(2)],
            },
            state.eo_leases
        );

        state.process(ExactlyOnceAck(test_id(4)));
        assert_eq!(
            TestLeases {
                under_lease: vec![test_id(1), test_id(2), test_id(3), test_id(4)],
                to_ack: vec![test_id(1), test_id(4)],
                to_nack: vec![test_id(2)],
            },
            state.eo_leases
        );

        state.process(ExactlyOnceNack(test_id(3)));
        assert_eq!(
            TestLeases {
                under_lease: vec![test_id(1), test_id(2), test_id(3), test_id(4)],
                to_ack: vec![test_id(1), test_id(4)],
                to_nack: vec![test_id(2), test_id(3)],
            },
            state.eo_leases
        );
    }

    #[tokio::test]
    async fn leaser_noops() {
        let mock = MockLeaser::new();
        // Note that there are no calls expected into the leaser, as there are
        // no messages under lease management.
        let mut state = LeaseState::new(Arc::new(mock), LeaseOptions::default());
        extend_and_await(&mut state).await;
        state.flush();
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
        mock.expect_confirmed_ack()
            .times(1)
            .withf(|v| sorted(v) == test_ids(110..120))
            .returning(|_| ());
        mock.expect_confirmed_nack()
            .times(1)
            .withf(|v| sorted(v) == test_ids(100..110))
            .returning(|_| ());
        let mut state = LeaseState::new(Arc::new(mock), LeaseOptions::default());
        for i in 0..100 {
            state.add(test_id(i), at_least_once_info());
        }
        for i in 0..10 {
            state.process(Ack(test_id(i)));
        }
        for i in 10..20 {
            state.process(Nack(test_id(i)));
        }
        for i in 100..200 {
            state.add(test_id(i), exactly_once_info());
        }
        for i in 100..110 {
            state.process(ExactlyOnceNack(test_id(i)));
        }
        for i in 110..120 {
            state.process(ExactlyOnceAck(test_id(i)));
        }
        assert_eq!(
            TestLeases {
                under_lease: test_ids(20..100),
                to_ack: test_ids(0..10),
                to_nack: test_ids(10..20),
            },
            state.leases
        );
        assert_eq!(
            TestLeases {
                under_lease: test_ids(100..200),
                to_ack: test_ids(110..120),
                to_nack: test_ids(100..110),
            },
            state.eo_leases
        );

        flush_and_await(&mut state).await;
        assert_eq!(
            TestLeases {
                under_lease: test_ids(20..100),
                to_ack: Vec::new(),
                to_nack: Vec::new(),
            },
            state.leases
        );
        assert_eq!(
            TestLeases {
                under_lease: test_ids(100..200),
                to_ack: Vec::new(),
                to_nack: Vec::new(),
            },
            state.eo_leases
        );

        let mut ack_results = HashMap::new();
        for i in 110..115 {
            ack_results.insert(test_id(i), Ok(()));
        }
        state.confirm(ack_results);
        let mut expected_under_lease = test_ids(100..110);
        expected_under_lease.extend(test_ids(115..200));
        assert_eq!(
            TestLeases {
                under_lease: expected_under_lease,
                to_ack: Vec::new(),
                to_nack: Vec::new(),
            },
            state.eo_leases
        );
    }

    #[tokio::test(start_paused = true)]
    async fn pending_acks_nacks_size_management() {
        let mut mock = MockLeaser::new();
        mock.expect_ack()
            .times(1)
            .withf(|v| *v == vec![test_id(1)])
            .returning(|_| ());
        mock.expect_nack()
            .times(1)
            .withf(|v| *v == vec![test_id(2)])
            .returning(|_| ());

        let mut state = LeaseState::new(Arc::new(mock), LeaseOptions::default());

        state.add(test_id(1), at_least_once_info());
        state.process(Ack(test_id(1)));

        state.flush();
        // Yield execution so the ack attempt can execute.
        tokio::task::yield_now().await;
        assert!(
            state.pending_acks_nacks.is_empty(),
            "The ack task should have completed. We should not hold onto it."
        );

        state.add(test_id(2), at_least_once_info());
        state.process(Nack(test_id(2)));

        state.flush();
        // Yield execution so the nack attempt can execute.
        tokio::task::yield_now().await;
        assert!(
            state.pending_acks_nacks.is_empty(),
            "The nack task should have completed. We should not hold onto it."
        );
    }

    #[tokio::test(start_paused = true)]
    async fn extend_at_least_once() {
        let mut seq = mockall::Sequence::new();
        let mut mock = MockLeaser::new();
        mock.expect_extend()
            .times(1)
            .in_sequence(&mut seq)
            .withf(|v| sorted(v) == test_ids(0..10))
            .returning(|ack_ids| ack_ids);
        mock.expect_extend()
            .times(1)
            .in_sequence(&mut seq)
            .withf(|v| sorted(v) == test_ids(0..20))
            .returning(|ack_ids| ack_ids);
        mock.expect_extend()
            .times(1)
            .in_sequence(&mut seq)
            .withf(|v| sorted(v) == test_ids(5..20))
            .returning(|ack_ids| ack_ids);
        mock.expect_extend()
            .times(1)
            .in_sequence(&mut seq)
            .withf(|v| sorted(v) == test_ids(10..20))
            .returning(|ack_ids| ack_ids);

        let options = LeaseOptions {
            max_lease_extension: Duration::ZERO,
            ..Default::default()
        };
        let mut state = LeaseState::new(Arc::new(mock), options);

        // Add 10 messages. These are now under lease management.
        for i in 0..10 {
            state.add(test_id(i), at_least_once_info());
        }
        extend_and_await(&mut state).await;

        // Add another 10 messages. These are now under lease management.
        for i in 10..20 {
            state.add(test_id(i), at_least_once_info());
        }
        extend_and_await(&mut state).await;

        // Ack the first 5 messages. We should not extend these leases.
        for i in 0..5 {
            state.process(Ack(test_id(i)));
        }
        extend_and_await(&mut state).await;

        // Nack the next 5 messages. We should not extend these leases.
        for i in 5..10 {
            state.process(Nack(test_id(i)));
        }
        extend_and_await(&mut state).await;
    }

    #[tokio::test(start_paused = true)]
    async fn extend_exactly_once() {
        let mut seq = mockall::Sequence::new();
        let mut mock = MockLeaser::new();
        mock.expect_extend()
            .times(1)
            .in_sequence(&mut seq)
            .withf(|v| sorted(v) == test_ids(0..10))
            .returning(|ack_ids| ack_ids);
        mock.expect_extend()
            .times(2)
            .in_sequence(&mut seq)
            .withf(|v| sorted(v) == test_ids(0..20))
            .returning(|ack_ids| ack_ids);
        mock.expect_confirmed_ack()
            .times(1)
            .in_sequence(&mut seq)
            .withf(|v| sorted(v) == test_ids(0..5))
            .returning(|_| ());
        mock.expect_extend()
            .times(1)
            .in_sequence(&mut seq)
            .withf(|v| sorted(v) == test_ids(5..20))
            .returning(|ack_ids| ack_ids);
        mock.expect_extend()
            .times(1)
            .in_sequence(&mut seq)
            .withf(|v| sorted(v) == test_ids(5..20))
            .returning(|ack_ids| ack_ids);

        let options = LeaseOptions {
            max_lease_extension: Duration::ZERO,
            ..Default::default()
        };
        let mut state = LeaseState::new(Arc::new(mock), options);

        // Add 10 messages. These are now under lease management.
        for i in 0..10 {
            state.add(test_id(i), exactly_once_info());
        }
        extend_and_await(&mut state).await;

        // Add another 10 messages. These are now under lease management.
        for i in 10..20 {
            state.add(test_id(i), exactly_once_info());
        }
        extend_and_await(&mut state).await;

        // Ack the first 5 messages. We should continue to extend these leases
        // as they are not yet confirmed.
        let mut ack_results = HashMap::new();
        for i in 0..5 {
            state.process(ExactlyOnceAck(test_id(i)));
            ack_results.insert(test_id(i), Ok(()));
        }
        extend_and_await(&mut state).await;

        // Flush the acks and confirm.
        flush_and_await(&mut state).await;
        state.confirm(ack_results);

        // We should not extend the confirmed acks.
        extend_and_await(&mut state).await;

        // Nack the next 5 messages. We should not extend these leases.
        for i in 0..5 {
            state.process(ExactlyOnceNack(test_id(i)));
        }
        extend_and_await(&mut state).await;
    }

    #[tokio::test(start_paused = true)]
    async fn pending_extends_size_management() {
        let mut mock = MockLeaser::new();
        mock.expect_extend()
            .times(1)
            .withf(|v| *v == vec![test_id(1)])
            .returning(|ack_ids| ack_ids);
        mock.expect_extend()
            .times(1)
            .withf(|v| *v == vec![test_id(2)])
            .returning(|ack_ids| ack_ids);

        let options = LeaseOptions {
            max_lease_extension: Duration::ZERO,
            ..Default::default()
        };
        let mut state = LeaseState::new(Arc::new(mock), options);

        state.add(test_id(1), at_least_once_info());
        state.add(test_id(2), exactly_once_info());
        state.extend();

        let mut events = Vec::new();
        events.push(state.next_event().await);
        events.push(state.next_event().await);

        assert!(events.contains(&LeaseEvent::ExtendCompleted(test_ids(1..2))));
        assert!(events.contains(&LeaseEvent::ExtendCompletedEO(test_ids(2..3))));

        assert_eq!(
            state.pending_extends.len(),
            0,
            "Completed at-least-once extensions should be cleaned up"
        );
        assert_eq!(
            state.eo_pending_extends.len(),
            0,
            "Completed exactly-once extensions should be cleaned up"
        );
    }

    #[tokio::test]
    async fn shutdown() {
        let mut mock = MockLeaser::new();
        // For exactly once, the current behavior is to Nack everything that has not yet
        // been confirmed.
        mock.expect_ack()
            .times(1)
            .withf(|v| sorted(v) == test_ids(0..10))
            .returning(|_| ());
        mock.expect_nack()
            .times(1)
            .withf(|v| sorted(v) == test_ids(10..30))
            .returning(|_| ());
        mock.expect_confirmed_nack()
            .times(1)
            .withf(|v| sorted(v) == test_ids(30..60))
            .returning(|_| ());

        let mut state = LeaseState::new(Arc::new(mock), LeaseOptions::default());
        for i in 0..30 {
            state.add(test_id(i), at_least_once_info());
        }
        for i in 0..10 {
            state.process(Ack(test_id(i)));
        }
        for i in 10..20 {
            state.process(Nack(test_id(i)));
        }
        for i in 30..60 {
            state.add(test_id(i), exactly_once_info());
        }
        for i in 30..40 {
            state.process(ExactlyOnceAck(test_id(i)));
        }
        for i in 40..50 {
            state.process(ExactlyOnceNack(test_id(i)));
        }
        state.shutdown().await;
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
    async fn limit_size_of_ack_batch_at_least_once() {
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
            state.add(test_id(i), at_least_once_info());
            state.process(Ack(test_id(i)));
        }
        // With MAX_IDS_PER_RPC pending acks, the batch is full. We should flush it now.
        assert_eq!(state.next_event().await, LeaseEvent::Flush);
        assert_eq!(start.elapsed(), Duration::ZERO);
        flush_and_await(&mut state).await;

        // With the batch is not full. The next event should occur on the interval timer.
        for i in MAX_IDS_PER_RPC..(2 * MAX_IDS_PER_RPC - 1) {
            state.add(test_id(i), at_least_once_info());
            state.process(Ack(test_id(i)));
        }
        assert_eq!(state.next_event().await, LeaseEvent::Flush);
        assert_eq!(start.elapsed(), FLUSH_START);
    }

    #[tokio::test(start_paused = true)]
    async fn limit_size_of_ack_batch_exactly_once() {
        let start = Instant::now();

        const FLUSH_START: Duration = Duration::from_secs(1);

        let mut mock = MockLeaser::new();
        mock.expect_confirmed_ack()
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

        // With MAX_IDS_PER_RPC pending confirmed acks, the batch is full. We should flush it now.
        for i in 0..MAX_IDS_PER_RPC {
            state.add(test_id(i), exactly_once_info());
            state.process(ExactlyOnceAck(test_id(i)));
        }
        assert_eq!(state.next_event().await, LeaseEvent::Flush);
        assert_eq!(start.elapsed(), Duration::ZERO);
        flush_and_await(&mut state).await;

        // With the batch is not full. The next event should occur on the interval timer.
        for i in MAX_IDS_PER_RPC..(2 * MAX_IDS_PER_RPC - 1) {
            state.add(test_id(i), exactly_once_info());
            state.process(ExactlyOnceAck(test_id(i)));
        }
        assert_eq!(state.next_event().await, LeaseEvent::Flush);
        assert_eq!(start.elapsed(), FLUSH_START);
    }

    #[tokio::test(start_paused = true)]
    async fn limit_size_of_nack_batch_at_least_once() {
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
            state.add(test_id(i), at_least_once_info());
            state.process(Nack(test_id(i)));
        }
        // With MAX_IDS_PER_RPC pending nacks, the batch is full. We should flush it now.
        assert_eq!(state.next_event().await, LeaseEvent::Flush);
        assert_eq!(start.elapsed(), Duration::ZERO);
        flush_and_await(&mut state).await;

        // With the batch is not full. The next event should occur on the interval timer.
        for i in MAX_IDS_PER_RPC..(2 * MAX_IDS_PER_RPC - 1) {
            state.add(test_id(i), at_least_once_info());
            state.process(Nack(test_id(i)));
        }

        assert_eq!(state.next_event().await, LeaseEvent::Flush);
        assert_eq!(start.elapsed(), FLUSH_START);
    }

    #[tokio::test(start_paused = true)]
    async fn limit_size_of_nack_batch_exactly_once() {
        let start = Instant::now();

        const FLUSH_START: Duration = Duration::from_secs(1);

        let mut mock = MockLeaser::new();
        mock.expect_confirmed_nack()
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
            state.add(test_id(i), exactly_once_info());
            state.process(ExactlyOnceNack(test_id(i)));
        }
        // With MAX_IDS_PER_RPC pending nacks for exactly once leases, the batch is full. We should flush it now.
        assert_eq!(state.next_event().await, LeaseEvent::Flush);
        assert_eq!(start.elapsed(), Duration::ZERO);
        flush_and_await(&mut state).await;

        // With the batch is not full. The next event should occur on the interval timer.
        for i in MAX_IDS_PER_RPC..(2 * MAX_IDS_PER_RPC - 1) {
            state.add(test_id(i), exactly_once_info());
            state.process(ExactlyOnceNack(test_id(i)));
        }
        assert_eq!(state.next_event().await, LeaseEvent::Flush);
        assert_eq!(start.elapsed(), FLUSH_START);
    }

    #[tokio::test(start_paused = true)]
    async fn ack_and_nack_batches_are_independent_at_least_once() {
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
            state.add(test_id(i), at_least_once_info());
            state.process(Ack(test_id(i)));

            state.add(test_id(over_half_full + i), at_least_once_info());
            state.process(Nack(test_id(over_half_full + i)));
        }

        // While there are more than `MAX_IDS_PER_RPC` total messages under
        // lease management, neither the ack batch nor the nack batch are full.
        // The next event should occur on the interval timer.
        assert_eq!(state.next_event().await, LeaseEvent::Flush);
        assert_eq!(start.elapsed(), FLUSH_START);
    }

    #[tokio::test(start_paused = true)]
    async fn ack_and_nack_batches_are_independent_exactly_once() {
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
            state.add(test_id(i), exactly_once_info());
            state.process(ExactlyOnceAck(test_id(i)));

            state.add(test_id(over_half_full + i), exactly_once_info());
            state.process(ExactlyOnceNack(test_id(over_half_full + i)));
        }

        // While there are more than `MAX_IDS_PER_RPC` total messages under
        // lease management, neither the ack batch nor the nack batch are full.
        // The next event should occur on the interval timer.
        assert_eq!(state.next_event().await, LeaseEvent::Flush);
        assert_eq!(start.elapsed(), FLUSH_START);
    }

    #[test_case(super::at_least_once_info)]
    #[test_case(super::exactly_once_info)]
    #[tokio::test(start_paused = true)]
    async fn limit_size_of_extends(lease_info_factory: fn() -> LeaseInfo) -> anyhow::Result<()> {
        const NUM_BATCHES: i32 = 5;

        // We use this channel to surface ack_ids from the mock expectation.
        let (ack_id_tx, mut ack_id_rx) = unbounded_channel();

        let mut mock = MockLeaser::new();
        mock.expect_extend()
            .times(NUM_BATCHES as usize)
            .returning(move |ack_ids| {
                ack_id_tx
                    .send(ack_ids.clone())
                    .expect("sending on channel always succeeds");
                ack_ids
            });
        let mut state = LeaseState::new(Arc::new(mock), LeaseOptions::default());

        let mut want = HashSet::new();
        for i in 0..NUM_BATCHES * MAX_IDS_PER_RPC {
            state.add(test_id(i), lease_info_factory());

            // All ack IDs should be extended.
            want.insert(test_id(i));
        }
        extend_and_await(&mut state).await;

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

    #[test_case(super::at_least_once_info)]
    #[test_case(super::exactly_once_info)]
    #[tokio::test(start_paused = true)]
    async fn message_expiration(lease_info_factory: fn() -> LeaseInfo) -> anyhow::Result<()> {
        const MAX_LEASE: Duration = Duration::from_secs(300);
        const DELTA: Duration = Duration::from_secs(1);

        let mut seq = mockall::Sequence::new();
        let mut mock = MockLeaser::new();
        mock.expect_extend()
            .times(1)
            .in_sequence(&mut seq)
            .withf(|v| sorted(v) == test_ids(0..20))
            .returning(|ack_ids| ack_ids);
        mock.expect_extend()
            .times(1)
            .in_sequence(&mut seq)
            .withf(|v| sorted(v) == test_ids(10..20))
            .returning(|ack_ids| ack_ids);

        let options = LeaseOptions {
            max_lease: MAX_LEASE,
            ..Default::default()
        };
        let mut state = LeaseState::new(Arc::new(mock), options);

        // Add 10 messages under lease management
        for i in 0..10 {
            state.add(test_id(i), lease_info_factory());
        }

        // Add 10 more messages under lease management, a little later.
        tokio::time::advance(DELTA * 2).await;
        for i in 10..20 {
            state.add(test_id(i), lease_info_factory());
        }
        extend_and_await(&mut state).await;

        // Advance the time past the expiration of the original 10 messages.
        tokio::time::advance(MAX_LEASE - DELTA).await;
        extend_and_await(&mut state).await;

        // Advance the time past the expiration of the subsequent 10 messages.
        tokio::time::advance(DELTA * 2).await;
        extend_and_await(&mut state).await;

        Ok(())
    }
}
