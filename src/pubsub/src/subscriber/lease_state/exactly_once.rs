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
use super::ExactlyOnceInfo;
use super::MAX_IDS_PER_RPC;
use crate::error::AckError;
use std::collections::HashMap;
// Use a `tokio::time::Instant` to facilitate time-based unit testing.
use tokio::time::{Duration, Instant};

const NACK_SHUTDOWN_ERROR: &str = "subscriber is configured to `NackImmediately` on shutdown. To avoid this error, consider setting the shutdown behavior to `WaitForProcessing`.";

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
        let Some(ExactlyOnceInfo { pending, .. }) = self.under_lease.get_mut(&ack_id) else {
            // We already reported an error for this message, either because
            // it's lease expired, or because the server reported a failure in
            // an attempt to extend its lease.
            return;
        };
        *pending = true;
        self.to_ack.push(ack_id);
    }

    /// Process a nack from the application
    pub fn nack(&mut self, ack_id: String) {
        if self.under_lease.remove(&ack_id).is_some() {
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

    /// Returns batches of ack IDs to extend.
    ///
    /// Drops messages whose lease deadline cannot be extended any further.
    pub fn retain(&mut self, max_lease_extension: Duration) -> Vec<Vec<String>> {
        let now = Instant::now();

        // We want to extract some values from `HashMap`, leaving the rest
        // unchanged.
        // - `extract_if()` is not available as our MSRV is 1.86 and that appears
        //   in 1.88.
        // - `retain` does not work because we need a *value* of `info` to
        //   change `tx` and that only gives us a `&mut ExactlyOnceInfo`.
        // - using `Option<Sender>` would complicate the rest of the code.
        //
        // We believe the iterations are most of the problem.

        let mut expired = Vec::new();
        let remaining = self
            .under_lease
            .iter()
            .filter_map(|(id, info)| {
                if !info.pending && info.receive_time + max_lease_extension < now {
                    expired.push(id.clone());
                    None
                } else {
                    Some(id.clone())
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
    pub fn evict_and_drain(&mut self) -> (Vec<String>, Vec<Vec<String>>) {
        let under_lease = std::mem::take(&mut self.under_lease);
        for (ack_id, info) in under_lease {
            let _ = info
                .result_tx
                .send(Err(AckError::Shutdown(NACK_SHUTDOWN_ERROR.into())));
            self.to_nack.push(ack_id);
        }
        super::batch_drained(self.drain())
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
    use super::super::tests::{TestLeases, test_id, test_ids};
    use super::*;
    use std::collections::HashSet;
    use tokio::sync::oneshot::channel;

    // Cover the constant, converting it to an integer for convenience.
    const MAX_IDS_PER_RPC: i32 = super::MAX_IDS_PER_RPC as i32;

    fn test_info() -> ExactlyOnceInfo {
        let (result_tx, _result_rx) = channel();
        ExactlyOnceInfo {
            receive_time: Instant::now(),
            result_tx,
            pending: false,
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
                under_lease: vec![test_id(1), test_id(3)],
                to_ack: vec![test_id(1)],
                to_nack: vec![test_id(2)],
            },
            leases
        );

        leases.add(test_id(4), test_info());
        assert_eq!(
            TestLeases {
                under_lease: vec![test_id(1), test_id(3), test_id(4)],
                to_ack: vec![test_id(1)],
                to_nack: vec![test_id(2)],
            },
            leases
        );

        leases.ack(test_id(4));
        assert_eq!(
            TestLeases {
                under_lease: vec![test_id(1), test_id(3), test_id(4)],
                to_ack: vec![test_id(1), test_id(4)],
                to_nack: vec![test_id(2)],
            },
            leases
        );

        leases.nack(test_id(3));
        assert_eq!(
            TestLeases {
                under_lease: vec![test_id(1), test_id(4)],
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
        assert!(!ack_id.pending, "{ack_id:?}");

        leases.ack(test_id(1));
        let ack_id = leases
            .under_lease
            .get(&test_id(1))
            .expect("ack ID should be under lease");
        assert!(ack_id.pending, "{ack_id:?}");
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
                pending: false,
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
                under_lease: test_ids(10..100),
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
                under_lease: test_ids(10..100),
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
            leases.add(test_id(i), test_info());
            leases.ack(test_id(i));
        }
        // With 1000 pending acks, the batch is not full.
        assert!(!leases.needs_flush());

        for i in 1000..MAX_IDS_PER_RPC {
            leases.add(test_id(i), test_info());
            leases.ack(test_id(i));
        }
        // With 2500 pending acks, the batch is full. We should flush it now.
        assert!(leases.needs_flush());
    }

    #[test]
    fn needs_flush_nack() {
        let mut leases = Leases::default();

        for i in 0..1000 {
            leases.add(test_id(i), test_info());
            leases.nack(test_id(i));
        }
        // With 1000 pending nacks, the batch is not full.
        assert!(!leases.needs_flush());

        for i in 1000..MAX_IDS_PER_RPC {
            leases.add(test_id(i), test_info());
            leases.nack(test_id(i));
        }
        // With 2500 pending nacks, the batch is full. We should flush it now.
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

        let batches = leases.retain(Duration::from_secs(1));
        assert_eq!(batches.len(), NUM_BATCHES as usize);

        let mut got = HashSet::new();
        for batch in batches {
            assert_eq!(batch.len(), MAX_IDS_PER_RPC as usize);
            got.extend(batch.into_iter());
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
                pending: false,
            },
        );

        let (result_tx, result_rx2) = channel();
        leases.add(
            test_id(2),
            ExactlyOnceInfo {
                receive_time: Instant::now() - Duration::from_secs(1),
                result_tx,
                pending: false,
            },
        );

        // No messages expired.
        let mut batches = leases.retain(Duration::from_secs(4));
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
        let batches = leases.retain(Duration::from_secs(2));
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
        let batches = leases.retain(Duration::ZERO);
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
                pending: true,
            },
        );

        let (result_tx, result_rx2) = channel();
        leases.add(
            test_id(2),
            ExactlyOnceInfo {
                receive_time: Instant::now() - Duration::from_secs(1),
                result_tx,
                pending: false,
            },
        );

        let batches = leases.retain(Duration::ZERO);
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
                pending: true,
            },
        );
        let (result_tx, result_rx2) = channel();
        leases.add(
            test_id(2),
            ExactlyOnceInfo {
                receive_time: Instant::now(),
                result_tx,
                pending: false,
            },
        );
        leases.add(test_id(3), test_info());
        leases.nack(test_id(3));
        assert_eq!(
            TestLeases {
                under_lease: vec![test_id(1), test_id(2)],
                to_ack: Vec::new(),
                to_nack: vec![test_id(3)],
            },
            leases
        );

        let (to_ack, to_nack) = leases.evict_and_drain();

        assert_eq!(to_nack.len(), 1);
        assert_eq!(
            TestLeases {
                under_lease: Vec::new(),
                to_ack: Vec::new(),
                to_nack: test_ids(1..4),
            },
            Leases {
                to_ack,
                to_nack: to_nack.into_iter().flatten().collect(),
                ..Default::default()
            }
        );

        assert_eq!(
            TestLeases {
                under_lease: Vec::new(),
                to_ack: Vec::new(),
                to_nack: Vec::new(),
            },
            leases
        );
        let err = result_rx1.await?.expect_err("error should be returned");
        assert!(matches!(err, AckError::Shutdown(_)), "{err:?}");
        let err = result_rx2.await?.expect_err("error should be returned");
        assert!(matches!(err, AckError::Shutdown(_)), "{err:?}");

        Ok(())
    }

    #[tokio::test]
    async fn evict_overflow_batches() -> anyhow::Result<()> {
        let mut leases = Leases::default();

        let (result_tx, result_rx1) = channel();
        leases.add(
            test_id(1),
            ExactlyOnceInfo {
                receive_time: Instant::now(),
                result_tx,
                // Even pending acks will be evicted, and satisfied with
                // `Shutdown` errors.
                pending: true,
            },
        );
        let (result_tx, result_rx2) = channel();
        leases.add(
            test_id(2),
            ExactlyOnceInfo {
                receive_time: Instant::now(),
                result_tx,
                pending: false,
            },
        );

        // Add MAX_IDS_PER_RPC + 10 messages under lease management. Nack some and evict.
        for i in 3..MAX_IDS_PER_RPC + 20 {
            leases.add(test_id(i), test_info());
            if i % 2 == 0 {
                leases.nack(test_id(i));
            }
        }
        let (to_ack, to_nack) = leases.evict_and_drain();
        assert_eq!(to_nack.len(), 2);
        assert_eq!(to_nack[0].len(), MAX_IDS_PER_RPC as usize);
        assert_eq!(to_nack[1].len(), 19);

        assert_eq!(
            TestLeases {
                under_lease: Vec::new(),
                to_ack: Vec::new(),
                to_nack: test_ids(1..MAX_IDS_PER_RPC + 20),
            },
            Leases {
                to_ack,
                to_nack: to_nack.into_iter().flatten().collect(),
                ..Default::default()
            }
        );

        assert_eq!(
            TestLeases {
                under_lease: Vec::new(),
                to_ack: Vec::new(),
                to_nack: Vec::new(),
            },
            leases
        );
        let err = result_rx1.await?.expect_err("error should be returned");
        assert!(matches!(err, AckError::Shutdown(_)), "{err:?}");
        let err = result_rx2.await?.expect_err("error should be returned");
        assert!(matches!(err, AckError::Shutdown(_)), "{err:?}");

        Ok(())
    }
}
