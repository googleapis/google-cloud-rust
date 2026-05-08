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

use super::{AtLeastOnceInfo, EXTEND_BUFFER, EXTEND_PERIOD, MAX_IDS_PER_RPC};
use std::collections::HashMap;
// Use a `tokio::time::Instant` to facilitate time-based unit testing.
use tokio::time::{Duration, Instant};

/// Leases for messages with at-least-once delivery semantics.
#[derive(Debug, Default)]
pub struct Leases {
    /// Ack IDs that are under lease management. The `Instant` denotes the time
    /// they were received.
    under_lease: HashMap<String, AtLeastOnceInfo>,
    /// Ack IDs we need to acknowledge.
    to_ack: Vec<String>,
    /// Ack IDs we need to nack.
    to_nack: Vec<String>,
}

impl Leases {
    /// Accept a new ack ID under lease management
    pub fn add(&mut self, ack_id: String, info: AtLeastOnceInfo) {
        self.under_lease.insert(ack_id, info);
    }

    /// Process an ack from the application
    pub fn ack(&mut self, ack_id: String) {
        self.under_lease.remove(&ack_id);
        // Unconditionally add the ack ID to the next ack batch. It doesn't hurt
        // to optimistically add it, even if its lease has expired.
        self.to_ack.push(ack_id);
    }

    /// Process a nack from the application
    pub fn nack(&mut self, ack_id: String) {
        if self.under_lease.remove(&ack_id).is_some() {
            // Only add the ack ID to the nack batch if the message is under our
            // lease. If the message's lease has already expired, we do not need
            // to take any additional action.
            self.to_nack.push(ack_id);
        }
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
        let mut batches = Vec::new();
        let mut batch = Vec::new();
        self.under_lease.retain(|ack_id, info| {
            // Note that using `HashMap::retain` allows us to iterate over the
            // map and conditionally drop elements in one pass.

            if info.receive_time + max_lease < now {
                // Drop messages that have been held for too long.
                false
            } else if info
                .last_extension
                .is_some_and(|i| i + max_lease_extension > now + EXTEND_PERIOD + EXTEND_BUFFER)
            {
                // The current lease is valid for a while. Retain the message,
                // but do not extend its leases.
                true
            } else {
                // Extend leases for all other messages.
                batch.push(ack_id.clone());
                if batch.len() == MAX_IDS_PER_RPC {
                    // Flush the batch when it is full.
                    batches.push(std::mem::take(&mut batch));
                }
                true
            }
        });
        if !batch.is_empty() {
            batches.push(batch);
        }
        batches
    }

    /// Nacks all messages under lease management that have not been acked by
    /// the application and drains all messages from the lease state.
    ///
    /// Called during shutdown, if configured to `NackImmediately`.
    pub fn evict_and_drain(mut self) -> (Vec<String>, Vec<Vec<String>>) {
        self.to_nack.extend(self.under_lease.into_keys());
        (self.to_ack, super::batch(self.to_nack))
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

        leases.add(test_id(1), AtLeastOnceInfo::new());
        assert_eq!(
            TestLeases {
                under_lease: vec![test_id(1)],
                to_ack: Vec::new(),
                to_nack: Vec::new(),
            },
            leases
        );

        leases.add(test_id(2), AtLeastOnceInfo::new());
        assert_eq!(
            TestLeases {
                under_lease: vec![test_id(1), test_id(2)],
                to_ack: Vec::new(),
                to_nack: Vec::new(),
            },
            leases
        );

        leases.add(test_id(3), AtLeastOnceInfo::new());
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
                under_lease: vec![test_id(2), test_id(3)],
                to_ack: vec![test_id(1)],
                to_nack: Vec::new(),
            },
            leases
        );

        leases.nack(test_id(2));
        assert_eq!(
            TestLeases {
                under_lease: vec![test_id(3)],
                to_ack: vec![test_id(1)],
                to_nack: vec![test_id(2)],
            },
            leases
        );

        leases.add(test_id(4), AtLeastOnceInfo::new());
        assert_eq!(
            TestLeases {
                under_lease: vec![test_id(3), test_id(4)],
                to_ack: vec![test_id(1)],
                to_nack: vec![test_id(2)],
            },
            leases
        );

        leases.ack(test_id(4));
        assert_eq!(
            TestLeases {
                under_lease: vec![test_id(3)],
                to_ack: vec![test_id(1), test_id(4)],
                to_nack: vec![test_id(2)],
            },
            leases
        );

        leases.nack(test_id(3));
        assert_eq!(
            TestLeases {
                under_lease: Vec::new(),
                to_ack: vec![test_id(1), test_id(4)],
                to_nack: vec![test_id(2), test_id(3)],
            },
            leases
        );
    }

    #[test]
    fn drain() {
        let mut leases = Leases::default();
        for i in 0..100 {
            leases.add(test_id(i), AtLeastOnceInfo::new());
        }
        for i in 0..10 {
            leases.ack(test_id(i));
        }
        for i in 10..20 {
            leases.nack(test_id(i));
        }
        assert_eq!(
            TestLeases {
                under_lease: test_ids(20..100),
                to_ack: test_ids(0..10),
                to_nack: test_ids(10..20),
            },
            leases
        );

        let (to_ack, to_nack) = leases.drain();
        assert_eq!(to_ack, test_ids(0..10));
        assert_eq!(to_nack, test_ids(10..20));

        assert_eq!(
            TestLeases {
                under_lease: test_ids(20..100),
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

        leases.add(test_id(1), AtLeastOnceInfo::new());
        leases.add(test_id(2), AtLeastOnceInfo::new());

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
    fn evict() {
        let mut leases = Leases::default();
        for i in 0..30 {
            leases.add(test_id(i), AtLeastOnceInfo::new());
        }
        for i in 0..10 {
            leases.ack(test_id(i));
        }
        for i in 10..20 {
            leases.nack(test_id(i));
        }
        assert_eq!(
            TestLeases {
                under_lease: test_ids(20..30),
                to_ack: test_ids(0..10),
                to_nack: test_ids(10..20),
            },
            leases
        );

        let (to_ack, to_nack) = leases.evict_and_drain();
        assert_eq!(sorted(&to_ack), test_ids(0..10));

        let to_nack = Batches::flatten(to_nack);
        assert_eq!(sorted(&to_nack.ack_ids), test_ids(10..30));
    }

    #[test]
    fn evict_overflow_batches() {
        let mut leases = Leases::default();
        for i in 0..MAX_IDS_PER_RPC * 3 {
            leases.add(test_id(i), AtLeastOnceInfo::new());
        }
        for i in 0..10 {
            leases.ack(test_id(i));
        }
        for i in 10..MAX_IDS_PER_RPC {
            leases.nack(test_id(i));
        }
        assert_eq!(
            TestLeases {
                under_lease: test_ids(MAX_IDS_PER_RPC..MAX_IDS_PER_RPC * 3),
                to_ack: test_ids(0..10),
                to_nack: test_ids(10..MAX_IDS_PER_RPC),
            },
            leases
        );

        let (to_ack, to_nack) = leases.evict_and_drain();
        assert_eq!(sorted(&to_ack), test_ids(0..10));

        let to_nack = Batches::flatten(to_nack);
        assert_eq!(
            to_nack.counts,
            vec![MAX_IDS_PER_RPC, MAX_IDS_PER_RPC, MAX_IDS_PER_RPC - 10]
        );
        assert_eq!(sorted(&to_nack.ack_ids), test_ids(10..MAX_IDS_PER_RPC * 3));
    }

    #[test]
    fn ack_out_of_lease_included() {
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
                to_ack: vec![test_id(1)],
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
    fn needs_flush_ack() {
        let mut leases = Leases::default();

        for i in 0..100 {
            leases.add(test_id(i), AtLeastOnceInfo::new());
            leases.ack(test_id(i));
        }
        // With 100 pending acks, the batch is not full.
        assert!(!leases.needs_flush());

        for i in 100..MAX_IDS_PER_RPC {
            leases.add(test_id(i), AtLeastOnceInfo::new());
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
            leases.add(test_id(i), AtLeastOnceInfo::new());
            leases.nack(test_id(i));
        }
        // With 100 pending nacks, the batch is not full.
        assert!(!leases.needs_flush());

        for i in 100..MAX_IDS_PER_RPC {
            leases.add(test_id(i), AtLeastOnceInfo::new());
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
            leases.add(test_id(i), AtLeastOnceInfo::new());
            leases.ack(test_id(i));

            leases.add(test_id(over_half_full + i), AtLeastOnceInfo::new());
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
            leases.add(test_id(i), AtLeastOnceInfo::new());
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
        const MAX_LEASE: Duration = Duration::from_secs(300);
        const DELTA: Duration = Duration::from_secs(1);

        let mut leases = Leases::default();

        // Add 10 messages under lease management
        for i in 0..10 {
            leases.add(test_id(i), AtLeastOnceInfo::new());
        }

        // Add 10 more messages under lease management, a little later.
        tokio::time::advance(DELTA * 2).await;
        for i in 10..20 {
            leases.add(test_id(i), AtLeastOnceInfo::new());
        }
        let batches = leases.retain(MAX_LEASE, Duration::ZERO);
        assert_eq!(batches.len(), 1);
        assert_eq!(sorted(&batches[0]), test_ids(0..20));
        assert_eq!(
            TestLeases {
                under_lease: test_ids(0..20),
                to_ack: Vec::new(),
                to_nack: Vec::new(),
            },
            leases
        );

        // Advance the time past the expiration of the original 10 messages.
        tokio::time::advance(MAX_LEASE - DELTA).await;
        let batches = leases.retain(MAX_LEASE, Duration::ZERO);
        assert_eq!(batches.len(), 1);
        assert_eq!(sorted(&batches[0]), test_ids(10..20));
        assert_eq!(
            TestLeases {
                under_lease: test_ids(10..20),
                to_ack: Vec::new(),
                to_nack: Vec::new(),
            },
            leases
        );

        // Advance the time past the expiration of the subsequent 10 messages.
        tokio::time::advance(DELTA * 2).await;
        let batches = leases.retain(MAX_LEASE, Duration::ZERO);
        assert!(batches.is_empty(), "{}", batches.len());
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

    #[tokio::test(start_paused = true)]
    async fn necessary_extensions() -> anyhow::Result<()> {
        const MAX_LEASE: Duration = Duration::from_secs(900);
        const MAX_LEASE_EXTENSION: Duration = Duration::from_secs(10);

        let mut leases = Leases::default();
        leases.add(test_id(0), AtLeastOnceInfo::new());

        // We should always send a receipt lease extension upon receiving a
        // message.
        let batches = leases.retain(MAX_LEASE, MAX_LEASE_EXTENSION);
        leases.update_last_extension(&test_ids(0..1));
        assert_eq!(batches, vec![vec![test_id(0)]]);
        assert_eq!(
            TestLeases {
                under_lease: test_ids(0..1),
                to_ack: Vec::new(),
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
                under_lease: test_ids(0..1),
                to_ack: Vec::new(),
                to_nack: Vec::new(),
            },
            leases
        );

        // Advance the time close to the expiration of the initial lease.
        tokio::time::advance(MAX_LEASE_EXTENSION - Duration::from_secs(1)).await;

        // We need to extend the lease again.
        let batches = leases.retain(MAX_LEASE, MAX_LEASE_EXTENSION);
        leases.update_last_extension(&test_ids(0..1));
        assert_eq!(batches, vec![vec![test_id(0)]]);
        assert_eq!(
            TestLeases {
                under_lease: test_ids(0..1),
                to_ack: Vec::new(),
                to_nack: Vec::new(),
            },
            leases
        );

        Ok(())
    }
}
