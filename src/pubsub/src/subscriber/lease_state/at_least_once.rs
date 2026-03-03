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

use super::ACK_IDS_PER_RPC;
use std::collections::HashMap;
// Use a `tokio::time::Instant` to facilitate time-based unit testing.
use tokio::time::{Duration, Instant};

/// Leases for messages with at-least-once delivery semantics.
#[derive(Debug, Default)]
pub struct Leases {
    /// Ack IDs that are under lease management. The `Instant` denotes the time
    /// they were received.
    under_lease: HashMap<String, Instant>,
    /// Ack IDs we need to acknowledge.
    to_ack: Vec<String>,
    /// Ack IDs we need to nack.
    to_nack: Vec<String>,
}

impl Leases {
    /// Accept a new ack ID under lease management
    pub fn add(&mut self, ack_id: String, receive_time: Instant) {
        self.under_lease.insert(ack_id, receive_time);
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
        self.to_ack.len() >= ACK_IDS_PER_RPC || self.to_nack.len() >= ACK_IDS_PER_RPC
    }

    /// Returns a pair of pending (acks, nacks) to flush.
    pub fn flush(&mut self) -> (Vec<String>, Vec<String>) {
        (
            std::mem::take(&mut self.to_ack),
            std::mem::take(&mut self.to_nack),
        )
    }

    /// Returns batches of ack IDs to extend.
    ///
    /// Drops messages whose lease deadline cannot be extended any further.
    pub fn retain(&mut self, max_lease_extension: Duration) -> Vec<Vec<String>> {
        let now = Instant::now();
        let mut batches = Vec::new();
        let mut batch = Vec::new();
        self.under_lease.retain(|ack_id, receive_time| {
            // Note that using `HashMap::retain` allows us to iterate over the
            // map and conditionally drop elements in one pass.

            if *receive_time + max_lease_extension < now {
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
        batches
    }

    /// Nacks all messages under lease management that have not been acked by
    /// the application.
    ///
    /// Called during shutdown, if configured to `NackImmediately`.
    pub fn evict(&mut self) {
        let under_lease = std::mem::take(&mut self.under_lease);
        self.to_nack.extend(under_lease.into_keys());
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
    use super::super::tests::{TestLeases, sorted, test_id, test_ids};
    use super::*;
    use std::collections::HashSet;

    // Cover the constant, converting it to an integer for convenience.
    const ACK_IDS_PER_RPC: i32 = super::ACK_IDS_PER_RPC as i32;

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

        leases.add(test_id(1), Instant::now());
        assert_eq!(
            TestLeases {
                under_lease: vec![test_id(1)],
                to_ack: Vec::new(),
                to_nack: Vec::new(),
            },
            leases
        );

        leases.add(test_id(2), Instant::now());
        assert_eq!(
            TestLeases {
                under_lease: vec![test_id(1), test_id(2)],
                to_ack: Vec::new(),
                to_nack: Vec::new(),
            },
            leases
        );

        leases.add(test_id(3), Instant::now());
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

        leases.add(test_id(4), Instant::now());
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
    fn flush() {
        let mut leases = Leases::default();
        for i in 0..100 {
            leases.add(test_id(i), Instant::now());
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

        let (to_ack, to_nack) = leases.flush();
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

    #[test]
    fn evict() {
        let mut leases = Leases::default();
        for i in 0..30 {
            leases.add(test_id(i), Instant::now());
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

        leases.evict();
        assert_eq!(
            TestLeases {
                under_lease: Vec::new(),
                to_ack: test_ids(0..10),
                to_nack: test_ids(10..30),
            },
            leases
        );
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

        for i in 0..1000 {
            leases.add(test_id(i), Instant::now());
            leases.ack(test_id(i));
        }
        // With 1000 pending acks, the batch is not full.
        assert!(!leases.needs_flush());

        for i in 1000..ACK_IDS_PER_RPC {
            leases.add(test_id(i), Instant::now());
            leases.ack(test_id(i));
        }
        // With 2500 pending acks, the batch is full. We should flush it now.
        assert!(leases.needs_flush());
    }

    #[test]
    fn needs_flush_nack() {
        let mut leases = Leases::default();

        for i in 0..1000 {
            leases.add(test_id(i), Instant::now());
            leases.nack(test_id(i));
        }
        // With 1000 pending nacks, the batch is not full.
        assert!(!leases.needs_flush());

        for i in 1000..ACK_IDS_PER_RPC {
            leases.add(test_id(i), Instant::now());
            leases.nack(test_id(i));
        }
        // With 2500 pending nacks, the batch is full. We should flush it now.
        assert!(leases.needs_flush());
    }

    #[test]
    fn ack_and_nack_batches_are_independent() {
        let mut leases = Leases::default();

        let over_half_full = ACK_IDS_PER_RPC / 2 + 100;
        for i in 0..over_half_full {
            leases.add(test_id(i), Instant::now());
            leases.ack(test_id(i));

            leases.add(test_id(over_half_full + i), Instant::now());
            leases.nack(test_id(over_half_full + i));
        }

        // While there are more than `ACK_IDS_PER_RPC` total messages under
        // lease management, neither the ack batch nor the nack batch are full.
        // The next event should occur on the interval timer.
        assert!(!leases.needs_flush());
    }

    #[test]
    fn batching() -> anyhow::Result<()> {
        const NUM_BATCHES: i32 = 5;

        let mut leases = Leases::default();

        let mut want = HashSet::new();
        for i in 0..NUM_BATCHES * ACK_IDS_PER_RPC {
            leases.add(test_id(i), Instant::now());
            want.insert(test_id(i));
        }

        let batches = leases.retain(Duration::from_secs(1));
        assert_eq!(batches.len(), NUM_BATCHES as usize);

        let mut got = HashSet::new();
        for batch in batches {
            assert_eq!(batch.len(), ACK_IDS_PER_RPC as usize);
            got.extend(batch.into_iter());
        }

        // Make sure all ack IDs are included.
        assert_eq!(got, want);
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn message_expiration() -> anyhow::Result<()> {
        const MAX_LEASE_EXTENSION: Duration = Duration::from_secs(300);
        const DELTA: Duration = Duration::from_secs(1);

        let mut leases = Leases::default();

        // Add 10 messages under lease management
        for i in 0..10 {
            leases.add(test_id(i), Instant::now());
        }

        // Add 10 more messages under lease management, a little later.
        tokio::time::advance(DELTA * 2).await;
        for i in 10..20 {
            leases.add(test_id(i), Instant::now());
        }
        let batches = leases.retain(MAX_LEASE_EXTENSION);
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
        tokio::time::advance(MAX_LEASE_EXTENSION - DELTA).await;
        let batches = leases.retain(MAX_LEASE_EXTENSION);
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
        let batches = leases.retain(MAX_LEASE_EXTENSION);
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
}
