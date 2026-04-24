// Copyright 2026 Google LLC
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

use super::super::handler::AckResult;
use super::{EXTEND_BUFFER, EXTEND_PERIOD, ExactlyOnceInfo, MAX_IDS_PER_RPC, MessageStatus};
use crate::error::AckError;
use std::collections::HashMap;
// Use a `tokio::time::Instant` to facilitate time-based unit testing.
use tokio::time::{Duration, Instant};

pub(crate) const NACK_SHUTDOWN_ERROR: &str = "subscriber is configured to `NackImmediately` on shutdown. To avoid this error, consider setting the shutdown behavior to `WaitForProcessing`.";

/// Leases for messages with exactly-once delivery semantics.
#[derive(Debug, Default)]
pub struct Leases {
    /// Ack IDs that are under lease management.
    under_lease: HashMap<String, ExactlyOnceInfo>,
    /// Ack IDs we need to acknowledge.
    to_ack: Vec<String>,
    /// Ack IDs we need to nack.
    to_nack: Vec<String>,
}

impl Leases {
    /// Accept a new ack ID under lease management
    pub fn add(&mut self, ack_id: String, info: ExactlyOnceInfo) {
        self.under_lease.insert(ack_id, info);
    }

    /// Forward the result of an ack to the application
    pub fn confirm(&mut self, ack_id: String, result: AckResult) {
        let Some(info) = self.under_lease.remove(&ack_id) else {
            // We already reported an error for this message, either because
            // it's lease expired, or because the server reported a failure in
            // an attempt to extend its lease.
            return;
        };
        let _ = info.result_tx.send(result);
    }

    /// Process an ack from the application
    pub fn ack(&mut self, ack_id: String) {
        let Some(info) = self.under_lease.get_mut(&ack_id) else {
            // We already reported an error for this message, either because
            // it's lease expired, or because the server reported a failure in
            // an attempt to extend its lease.
            return;
        };
        info.status = MessageStatus::Acking;
        self.to_ack.push(ack_id);
    }

    /// Process a nack from the application
    pub fn nack(&mut self, ack_id: String) {
        let Some(info) = self.under_lease.get_mut(&ack_id) else {
            return;
        };
        info.status = MessageStatus::Nacking;
        self.to_nack.push(ack_id);
    }

    /// If true, an ack or nack batch is full. We need to flush it.
    pub fn needs_flush(&self) -> bool {
        // This is an OR because `Acknowledge` and `ModifyAckDeadline` are
        // separate RPCs, with separate limits.
        self.to_ack.len() >= MAX_IDS_PER_RPC || self.to_nack.len() >= MAX_IDS_PER_RPC
    }

    /// Drain the pending (acks, nacks) for the lease state to flush.
    pub fn drain(&mut self) -> (Vec<String>, Vec<String>) {
        (
            std::mem::take(&mut self.to_ack),
            std::mem::take(&mut self.to_nack),
        )
    }

    /// Updates the `last_extension` timestamp for the given ack IDs with the
    /// completion time of a successful extension RPC.
    pub fn update_last_extension(&mut self, ack_ids: &[String]) {
        let now = Instant::now();
        for id in ack_ids {
            if let Some(info) = self.under_lease.get_mut(id) {
                info.last_extension = Some(now);
            }
        }
    }

    /// Returns batches of ack IDs to extend.
    ///
    /// Drops messages whose lease deadline cannot be extended any further.
    pub fn retain(
        &mut self,
        max_lease: Duration,
        max_lease_extension: Duration,
    ) -> Vec<Vec<String>> {
        let now = Instant::now();

        // We want to extract some values from `HashMap`, leaving the rest
        // unchanged.
        // - `extract_if()` is not available as our MSRV is 1.87 and that appears
        //   in 1.88.
        // - `retain` does not work because we need a *value* of `info` to
        //   change `tx` and that only gives us a `&mut ExactlyOnceInfo`.
        // - using `Option<Sender>` would complicate the rest of the code.
        //
        // We believe the iterations are most of the problem.

        let mut expired = Vec::new();
        let remaining = self
            .under_lease
            .iter_mut()
            .filter_map(|(id, info)| match info.status {
                MessageStatus::Nacking => None,
                MessageStatus::Acking => {
                    if info.last_extension.is_some_and(|i| {
                        i + max_lease_extension > now + EXTEND_PERIOD + EXTEND_BUFFER
                    }) {
                        // The lease is still valid for a while. No need to extend.
                        None
                    } else {
                        // Continue to extend messages being acked.
                        Some(id.clone())
                    }
                }
                MessageStatus::Leased => {
                    if info.receive_time + max_lease < now {
                        // Drop messages that have been held for too long.
                        expired.push(id.clone());
                        None
                    } else if info.last_extension.is_some_and(|i| {
                        i + max_lease_extension > now + EXTEND_PERIOD + EXTEND_BUFFER
                    }) {
                        // The lease is still valid for a while. No need to extend.
                        None
                    } else {
                        // Extend leases for all other messages
                        Some(id.clone())
                    }
                }
            })
            .collect::<Vec<_>>()
            .chunks(MAX_IDS_PER_RPC)
            .map(|c| c.to_vec())
            .collect::<Vec<_>>();

        expired
            .into_iter()
            .filter_map(|id| self.under_lease.remove_entry(&id))
            .for_each(|(_id, info)| {
                let _ = info.result_tx.send(Err(AckError::LeaseExpired));
            });
        remaining
    }

    /// Nacks all messages under lease management that have not been acked by
    /// the application and drains all messages from the lease state.
    ///
    /// Called during shutdown, if configured to `NackImmediately`.
    pub fn evict_and_drain(self) -> (Vec<String>, Vec<Vec<String>>) {
        let to_nack = self
            .under_lease
            .into_iter()
            .map(|(ack_id, info)| {
                let _ = info
                    .result_tx
                    .send(Err(AckError::Shutdown(NACK_SHUTDOWN_ERROR.into())));
                ack_id
            })
            .collect();
        (self.to_ack, super::batch(to_nack))
    }
}

#[cfg(test)]
impl PartialEq<Leases> for super::tests::TestLeases {
    fn eq(&self, leases: &Leases) -> bool {
        let under_lease = {
            let mut v: Vec<String> = leases.under_lease.keys().cloned().collect();
            v.sort();
            v
        };
        let to_ack = {
            let mut v = leases.to_ack.clone();
            v.sort();
            v
        };
        let to_nack = {
            let mut v = leases.to_nack.clone();
            v.sort();
            v
        };
        self.under_lease == under_lease && self.to_ack == to_ack && self.to_nack == to_nack
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::{Batches, TestLeases, sorted, test_id, test_ids};
    use super::*;
    use std::collections::HashSet;
    use tokio::sync::oneshot::channel;

    // Cover the constant, converting it to an integer for convenience.
    const MAX_IDS_PER_RPC: i32 = super::MAX_IDS_PER_RPC as i32;

    impl Leases {
        fn last_extension(&self, id: &str) -> Option<Instant> {
            self.under_lease
                .get(id)
                .expect("test id should be under lease")
                .last_extension
        }
    }

    fn test_info() -> ExactlyOnceInfo {
        let (result_tx, _result_rx) = channel();
        ExactlyOnceInfo {
            receive_time: Instant::now(),
            result_tx,
            status: MessageStatus::Leased,
            last_extension: None,
        }
    }

    #[test]
    fn basic_add_ack_nack() {
        let mut leases = Leases::default();
        assert_eq!(
            TestLeases {
                under_lease: Vec::new(),
                to_ack: Vec::new(),
                to_nack: Vec::new(),
            },
            leases
        );

        leases.add(test_id(1), test_info());
        assert_eq!(
            TestLeases {
                under_lease: vec![test_id(1)],
                to_ack: Vec::new(),
                to_nack: Vec::new(),
            },
            leases
        );

        leases.add(test_id(2), test_info());
        assert_eq!(
            TestLeases {
                under_lease: vec![test_id(1), test_id(2)],
                to_ack: Vec::new(),
                to_nack: Vec::new(),
            },
            leases
        );

        leases.add(test_id(3), test_info());
        assert_eq!(
            TestLeases {
                under_lease: vec![test_id(1), test_id(2), test_id(3)],
                to_ack: Vec::new(),
                to_nack: Vec::new(),
            },
            leases
        );

        leases.ack(test_id(1));
        assert_eq!(
            TestLeases {
                under_lease: vec![test_id(1), test_id(2), test_id(3)],
                to_ack: vec![test_id(1)],
                to_nack: Vec::new(),
            },
            leases
        );

        leases.nack(test_id(2));
        assert_eq!(
            TestLeases {
                under_lease: vec![test_id(1), test_id(2), test_id(3)],
                to_ack: vec![test_id(1)],
                to_nack: vec![test_id(2)],
            },
            leases
        );

        leases.add(test_id(4), test_info());
        assert_eq!(
            TestLeases {
                under_lease: vec![test_id(1), test_id(2), test_id(3), test_id(4)],
                to_ack: vec![test_id(1)],
                to_nack: vec![test_id(2)],
            },
            leases
        );

        leases.ack(test_id(4));
        assert_eq!(
            TestLeases {
                under_lease: vec![test_id(1), test_id(2), test_id(3), test_id(4)],
                to_ack: vec![test_id(1), test_id(4)],
                to_nack: vec![test_id(2)],
            },
            leases
        );

        leases.nack(test_id(3));
        assert_eq!(
            TestLeases {
                under_lease: vec![test_id(1), test_id(2), test_id(3), test_id(4)],
                to_ack: vec![test_id(1), test_id(4)],
                to_nack: vec![test_id(2), test_id(3)],
            },
            leases
        );
    }

    #[test]
    fn mark_pending_acks() {
        let mut leases = Leases::default();
        leases.add(test_id(1), test_info());

        let ack_id = leases
            .under_lease
            .get(&test_id(1))
            .expect("ack ID should be under lease");
        assert_eq!(ack_id.status, MessageStatus::Leased);

        leases.ack(test_id(1));
        let ack_id = leases
            .under_lease
            .get(&test_id(1))
            .expect("ack ID should be under lease");
        assert_eq!(ack_id.status, MessageStatus::Acking);
    }

    #[tokio::test]
    async fn confirm() -> anyhow::Result<()> {
        let mut leases = Leases::default();

        let (result_tx, result_rx) = channel();
        leases.add(
            test_id(1),
            ExactlyOnceInfo {
                receive_time: Instant::now() - Duration::from_secs(3),
                result_tx,
                status: MessageStatus::Leased,
                last_extension: None,
            },
        );
        assert_eq!(
            TestLeases {
                under_lease: vec![test_id(1)],
                to_ack: Vec::new(),
                to_nack: Vec::new(),
            },
            leases
        );

        leases.confirm(test_id(1), Ok(()));
        result_rx.await??;
        assert_eq!(
            TestLeases {
                under_lease: Vec::new(),
                to_ack: Vec::new(),
                to_nack: Vec::new(),
            },
            leases
        );

        Ok(())
    }

    #[test]
    fn ack_out_of_lease_ignored() {
        let mut leases = Leases::default();
        assert_eq!(
            TestLeases {
                under_lease: Vec::new(),
                to_ack: Vec::new(),
                to_nack: Vec::new(),
            },
            leases
        );

        leases.ack(test_id(1));
        assert_eq!(
            TestLeases {
                under_lease: Vec::new(),
                to_ack: Vec::new(),
                to_nack: Vec::new(),
            },
            leases
        );
    }

    #[test]
    fn nack_out_of_lease_ignored() {
        let mut leases = Leases::default();
        assert_eq!(
            TestLeases {
                under_lease: Vec::new(),
                to_ack: Vec::new(),
                to_nack: Vec::new(),
            },
            leases
        );

        leases.nack(test_id(1));
        assert_eq!(
            TestLeases {
                under_lease: Vec::new(),
                to_ack: Vec::new(),
                to_nack: Vec::new(),
            },
            leases
        );
    }

    #[test]
    fn confirm_out_of_lease() {
        let mut leases = Leases::default();
        assert_eq!(
            TestLeases {
                under_lease: Vec::new(),
                to_ack: Vec::new(),
                to_nack: Vec::new(),
            },
            leases
        );

        leases.confirm(test_id(1), Ok(()));
        assert_eq!(
            TestLeases {
                under_lease: Vec::new(),
                to_ack: Vec::new(),
                to_nack: Vec::new(),
            },
            leases
        );
    }

    #[test]
    fn drain() {
        let mut leases = Leases::default();
        for i in 0..100 {
            leases.add(test_id(i), test_info());
        }
        for i in 0..10 {
            leases.nack(test_id(i));
        }
        for i in 10..20 {
            leases.ack(test_id(i));
        }
        assert_eq!(
            TestLeases {
                under_lease: test_ids(0..100),
                to_ack: test_ids(10..20),
                to_nack: test_ids(0..10),
            },
            leases
        );

        let (to_ack, to_nack) = leases.drain();
        assert_eq!(to_ack, test_ids(10..20));
        assert_eq!(to_nack, test_ids(0..10));

        assert_eq!(
            TestLeases {
                under_lease: test_ids(0..100),
                to_ack: Vec::new(),
                to_nack: Vec::new(),
            },
            leases
        );
    }

    #[tokio::test(start_paused = true)]
    async fn update_last_extension() {
        let mut leases = Leases::default();
        let now = Instant::now();

        leases.add(test_id(1), test_info());
        leases.add(test_id(2), test_info());

        assert_eq!(leases.last_extension(&test_id(1)), None);
        assert_eq!(leases.last_extension(&test_id(2)), None);

        leases.update_last_extension(&[test_id(1)]);
        assert_eq!(leases.last_extension(&test_id(1)), Some(now));
        assert_eq!(leases.last_extension(&test_id(2)), None);

        tokio::time::advance(Duration::from_secs(5)).await;
        let later = Instant::now();
        leases.update_last_extension(&[test_id(1), test_id(2)]);
        assert_eq!(leases.last_extension(&test_id(1)), Some(later));
        assert_eq!(leases.last_extension(&test_id(2)), Some(later));

        // Test with non-existent ID
        leases.update_last_extension(&[test_id(3)]);
        // Should not have side effects.
        assert_eq!(leases.last_extension(&test_id(1)), Some(later));
        assert_eq!(leases.last_extension(&test_id(2)), Some(later));
    }

    #[test]
    fn needs_flush_ack() {
        let mut leases = Leases::default();

        for i in 0..100 {
            leases.add(test_id(i), test_info());
            leases.ack(test_id(i));
        }
        // With 100 pending acks, the batch is not full.
        assert!(!leases.needs_flush());

        for i in 100..MAX_IDS_PER_RPC {
            leases.add(test_id(i), test_info());
            leases.ack(test_id(i));
        }
        // With `MAX_IDS_PER_RPC` pending acks, the batch is full. We should
        // flush it now.
        assert!(leases.needs_flush());
    }

    #[test]
    fn needs_flush_nack() {
        let mut leases = Leases::default();

        for i in 0..100 {
            leases.add(test_id(i), test_info());
            leases.nack(test_id(i));
        }
        // With 100 pending nacks, the batch is not full.
        assert!(!leases.needs_flush());

        for i in 100..MAX_IDS_PER_RPC {
            leases.add(test_id(i), test_info());
            leases.nack(test_id(i));
        }
        // With `MAX_IDS_PER_RPC` pending nacks, the batch is full. We should
        // flush it now.
        assert!(leases.needs_flush());
    }

    #[test]
    fn ack_and_nack_batches_are_independent() {
        let mut leases = Leases::default();

        let over_half_full = MAX_IDS_PER_RPC / 2 + 100;
        for i in 0..over_half_full {
            leases.add(test_id(i), test_info());
            leases.ack(test_id(i));

            leases.add(test_id(over_half_full + i), test_info());
            leases.nack(test_id(over_half_full + i));
        }

        // While there are more than `MAX_IDS_PER_RPC` total messages under
        // lease management, neither the ack batch nor the nack batch are full.
        assert!(!leases.needs_flush());
    }

    #[test]
    fn batching() -> anyhow::Result<()> {
        const NUM_BATCHES: i32 = 5;

        let mut leases = Leases::default();

        let mut want = HashSet::new();
        for i in 0..NUM_BATCHES * MAX_IDS_PER_RPC {
            leases.add(test_id(i), test_info());
            want.insert(test_id(i));
        }

        let batches = leases.retain(Duration::from_secs(1), Duration::ZERO);
        assert_eq!(batches.len(), NUM_BATCHES as usize);

        let mut got = HashSet::new();
        for batch in batches {
            assert_eq!(batch.len(), MAX_IDS_PER_RPC as usize);
            got.extend(batch);
        }

        // Make sure all ack IDs are included.
        assert_eq!(got, want);

        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn message_expiration() -> anyhow::Result<()> {
        let mut leases = Leases::default();

        let (result_tx, result_rx1) = channel();
        leases.add(
            test_id(1),
            ExactlyOnceInfo {
                receive_time: Instant::now() - Duration::from_secs(3),
                result_tx,
                status: MessageStatus::Leased,
                last_extension: None,
            },
        );

        let (result_tx, result_rx2) = channel();
        leases.add(
            test_id(2),
            ExactlyOnceInfo {
                receive_time: Instant::now() - Duration::from_secs(1),
                result_tx,
                status: MessageStatus::Leased,
                last_extension: None,
            },
        );

        // No messages expired.
        let mut batches = leases.retain(Duration::from_secs(4), Duration::ZERO);
        for batch in &mut batches {
            batch.sort();
        }
        assert_eq!(batches, vec![vec![test_id(1), test_id(2)]]);
        assert_eq!(
            TestLeases {
                under_lease: vec![test_id(1), test_id(2)],
                to_ack: Vec::new(),
                to_nack: Vec::new(),
            },
            leases
        );

        // Message 1 expires.
        let batches = leases.retain(Duration::from_secs(2), Duration::ZERO);
        assert_eq!(batches, vec![vec![test_id(2)]]);
        assert_eq!(
            TestLeases {
                under_lease: vec![test_id(2)],
                to_ack: Vec::new(),
                to_nack: Vec::new(),
            },
            leases
        );
        let err = result_rx1.await?.expect_err("error should be returned");
        assert!(matches!(err, AckError::LeaseExpired), "{err:?}");

        // Message 2 expires.
        let batches = leases.retain(Duration::ZERO, Duration::ZERO);
        assert!(batches.is_empty(), "{batches:?}");
        assert_eq!(
            TestLeases {
                under_lease: Vec::new(),
                to_ack: Vec::new(),
                to_nack: Vec::new(),
            },
            leases
        );
        let err = result_rx2.await?.expect_err("error should be returned");
        assert!(matches!(err, AckError::LeaseExpired), "{err:?}");

        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn pending_acks_do_not_expire() -> anyhow::Result<()> {
        let mut leases = Leases::default();

        let (result_tx, result_rx1) = channel();
        leases.add(
            test_id(1),
            ExactlyOnceInfo {
                receive_time: Instant::now() - Duration::from_secs(1),
                result_tx,
                status: MessageStatus::Acking,
                last_extension: None,
            },
        );

        let (result_tx, result_rx2) = channel();
        leases.add(
            test_id(2),
            ExactlyOnceInfo {
                receive_time: Instant::now() - Duration::from_secs(1),
                result_tx,
                status: MessageStatus::Leased,
                last_extension: None,
            },
        );

        let batches = leases.retain(Duration::ZERO, Duration::ZERO);
        assert_eq!(batches, vec![vec![test_id(1)]]);
        assert_eq!(
            TestLeases {
                under_lease: vec![test_id(1)],
                to_ack: Vec::new(),
                to_nack: Vec::new(),
            },
            leases
        );
        let err = result_rx2.await?.expect_err("error should be returned");
        assert!(matches!(err, AckError::LeaseExpired), "{err:?}");

        assert!(result_rx1.is_empty(), "{result_rx1:?}");

        Ok(())
    }

    #[tokio::test]
    async fn nacking_messages_are_not_extended() -> anyhow::Result<()> {
        let mut leases = Leases::default();

        let (result_tx, result_rx1) = channel();
        leases.add(
            test_id(1),
            ExactlyOnceInfo {
                receive_time: Instant::now() - Duration::from_secs(1),
                result_tx,
                status: MessageStatus::Leased,
                last_extension: None,
            },
        );
        leases.nack(test_id(1));

        let (result_tx, result_rx2) = channel();
        leases.add(
            test_id(2),
            ExactlyOnceInfo {
                receive_time: Instant::now() - Duration::from_secs(1),
                result_tx,
                status: MessageStatus::Leased,
                last_extension: None,
            },
        );

        let batches = leases.retain(Duration::ZERO, Duration::ZERO);
        assert!(batches.is_empty(), "{batches:?}");

        assert_eq!(
            TestLeases {
                under_lease: vec![test_id(1)],
                to_ack: Vec::new(),
                to_nack: vec![test_id(1)],
            },
            leases
        );

        let err = result_rx2.await?.expect_err("error should be returned");
        assert!(matches!(err, AckError::LeaseExpired), "{err:?}");

        assert!(result_rx1.is_empty(), "{result_rx1:?}");

        Ok(())
    }

    #[tokio::test]
    async fn evict() -> anyhow::Result<()> {
        let mut leases = Leases::default();

        let (result_tx, result_rx1) = channel();
        leases.add(
            test_id(1),
            ExactlyOnceInfo {
                receive_time: Instant::now(),
                result_tx,
                // Even pending acks will be evicted, and satisfied with
                // `Shutdown` errors.
                status: MessageStatus::Acking,
                last_extension: None,
            },
        );
        let (result_tx, result_rx2) = channel();
        leases.add(
            test_id(2),
            ExactlyOnceInfo {
                receive_time: Instant::now(),
                result_tx,
                status: MessageStatus::Leased,
                last_extension: None,
            },
        );
        let (result_tx, result_rx3) = channel();
        leases.add(
            test_id(3),
            ExactlyOnceInfo {
                receive_time: Instant::now(),
                result_tx,
                status: MessageStatus::Leased,
                last_extension: None,
            },
        );
        leases.nack(test_id(3));
        assert_eq!(
            leases
                .under_lease
                .get(&test_id(3))
                .expect("nack is under lease")
                .status,
            MessageStatus::Nacking
        );
        assert_eq!(
            TestLeases {
                under_lease: vec![test_id(1), test_id(2), test_id(3)],
                to_ack: Vec::new(),
                to_nack: vec![test_id(3)],
            },
            leases
        );

        let (to_ack, to_nack) = leases.evict_and_drain();
        assert!(to_ack.is_empty(), "{to_ack:?}");

        let to_nack = Batches::flatten(to_nack);
        assert_eq!(sorted(&to_nack.ack_ids), test_ids(1..4));

        let err = result_rx1.await?.expect_err("error should be returned");
        assert!(matches!(err, AckError::Shutdown(_)), "{err:?}");
        let err = result_rx2.await?.expect_err("error should be returned");
        assert!(matches!(err, AckError::Shutdown(_)), "{err:?}");
        let err = result_rx3.await?.expect_err("error should be returned");
        assert!(matches!(err, AckError::Shutdown(_)), "{err:?}");

        Ok(())
    }

    #[tokio::test]
    async fn evict_overflow_batches() -> anyhow::Result<()> {
        let mut leases = Leases::default();

        // Add MAX_IDS_PER_RPC + 10 messages under lease management. Nack some and evict.
        for i in 0..MAX_IDS_PER_RPC + 20 {
            leases.add(test_id(i), test_info());
            if i % 2 == 0 {
                leases.nack(test_id(i));
            }
        }
        let (to_ack, to_nack) = leases.evict_and_drain();
        assert!(to_ack.is_empty(), "{to_ack:?}");

        let to_nack = Batches::flatten(to_nack);
        assert_eq!(to_nack.counts, vec![MAX_IDS_PER_RPC, 20]);
        assert_eq!(sorted(&to_nack.ack_ids), test_ids(0..MAX_IDS_PER_RPC + 20));

        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn necessary_extensions() -> anyhow::Result<()> {
        const MAX_LEASE: Duration = Duration::from_secs(900);
        const MAX_LEASE_EXTENSION: Duration = Duration::from_secs(10);

        let mut leases = Leases::default();
        leases.add(test_id(0), test_info());

        // Add an acked message.
        leases.add(test_id(1), test_info());
        leases.ack(test_id(1));

        // We should always send a receipt lease extension upon receiving a
        // message.
        let batches = leases.retain(MAX_LEASE, MAX_LEASE_EXTENSION);
        leases.update_last_extension(&test_ids(0..2));
        let flattened = Batches::flatten(batches);
        assert_eq!(sorted(&flattened.ack_ids), test_ids(0..2));
        assert_eq!(
            TestLeases {
                under_lease: test_ids(0..2),
                to_ack: vec![test_id(1)],
                to_nack: Vec::new(),
            },
            leases
        );

        // The clock has not advanced, and we just sent out an extension. We
        // should not send another extension.
        let batches = leases.retain(MAX_LEASE, MAX_LEASE_EXTENSION);
        assert!(batches.is_empty(), "{batches:?}");
        assert_eq!(
            TestLeases {
                under_lease: test_ids(0..2),
                to_ack: vec![test_id(1)],
                to_nack: Vec::new(),
            },
            leases
        );

        // Advance the time close to the expiration of the initial lease.
        tokio::time::advance(MAX_LEASE_EXTENSION - Duration::from_secs(1)).await;

        // We need to extend the lease again.
        let batches = leases.retain(MAX_LEASE, MAX_LEASE_EXTENSION);
        leases.update_last_extension(&test_ids(0..2));
        let flattened = Batches::flatten(batches);
        assert_eq!(sorted(&flattened.ack_ids), test_ids(0..2));
        assert_eq!(
            TestLeases {
                under_lease: test_ids(0..2),
                to_ack: vec![test_id(1)],
                to_nack: Vec::new(),
            },
            leases
        );

        Ok(())
    }
}
