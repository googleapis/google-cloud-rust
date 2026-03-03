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
use std::collections::HashMap;
use tokio::sync::oneshot::Sender;
// Use a `tokio::time::Instant` to facilitate time-based unit testing.
use tokio::time::Instant;

#[derive(Debug)]
pub(super) struct ExactlyOnceInfo {
    receive_time: Instant,
    result_tx: Sender<AckResult>,
    // If true, we are currently trying to ack this message.
    //
    // We need to continue to extend these leases because the exactly-once
    // confirmed ack retry loop can take arbitrarily long.
    //
    // The client will not expire leases in this state. The server will
    // report if a lease has expired. We do not want to mask a success with
    // a `LeaseExpired` error.
    pending: bool,
}

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
    use super::super::tests::{TestLeases, test_id};
    use super::*;
    use tokio::sync::oneshot::channel;

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
}
