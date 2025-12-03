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

use std::collections::HashSet;

#[derive(Debug, PartialEq)]
struct LeaseState {
    // TODO(#3957) - support message expiry
    under_lease: HashSet<String>,
    to_ack: Vec<String>,
    to_nack: Vec<String>,
    // TODO(#3964) - support exactly once acks
}

impl LeaseState {
    fn new() -> Self {
        Self {
            under_lease: HashSet::new(),
            to_ack: Vec::new(),
            to_nack: Vec::new(),
        }
    }

    /// Accept a new ack ID under lease management
    fn add(&mut self, ack_id: String) {
        self.under_lease.insert(ack_id);
    }

    /// Process an ack from the application
    fn ack(&mut self, ack_id: String) {
        self.under_lease.remove(&ack_id);
        // Unconditionally add the ack ID to the next ack batch. It doesn't hurt
        // to optimistically add it, even if its lease has expired.
        self.to_ack.push(ack_id);
    }

    /// Process a nack from the application
    fn nack(&mut self, ack_id: String) {
        if self.under_lease.remove(&ack_id) {
            // Only add the ack ID to the nack batch if the message is under our
            // lease. If the message's lease has already expired, we do not need
            // to take any additional action.
            self.to_nack.push(ack_id);
        }
    }

    /// Flush pending acks/nacks and extend leases
    async fn flush(&mut self) {
        let _to_ack = std::mem::take(&mut self.to_ack);
        let _to_nack = std::mem::take(&mut self.to_nack);
        let _under_lease: Vec<String> = self.under_lease.iter().cloned().collect();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_state<L, A, N>(under_lease: L, to_ack: A, to_nack: N) -> LeaseState
    where
        L: IntoIterator<Item = &'static str>,
        A: IntoIterator<Item = &'static str>,
        N: IntoIterator<Item = &'static str>,
    {
        LeaseState {
            under_lease: under_lease.into_iter().map(|s| s.to_string()).collect(),
            to_ack: to_ack.into_iter().map(|s| s.to_string()).collect(),
            to_nack: to_nack.into_iter().map(|s| s.to_string()).collect(),
        }
    }

    #[test]
    fn basic_add_ack_nack() {
        let mut state = LeaseState::new();
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
        let mut state = LeaseState::new();
        for i in 0..100 {
            state.add(format!("{i}"));
        }
        for i in 0..10 {
            state.ack(format!("{i}"));
        }
        for i in 10..20 {
            state.nack(format!("{i}"));
        }
        let expected = LeaseState {
            under_lease: (20..100).map(|i| format!("{i}")).collect(),
            to_ack: (0..10).map(|i| format!("{i}")).collect(),
            to_nack: (10..20).map(|i| format!("{i}")).collect(),
        };
        assert_eq!(state, expected);

        state.flush().await;
        let expected = LeaseState {
            under_lease: (20..100).map(|i| format!("{i}")).collect(),
            to_ack: Vec::new(),
            to_nack: Vec::new(),
        };
        assert_eq!(state, expected);
    }

    #[test]
    fn ack_out_of_lease_included() {
        let mut state = LeaseState::new();
        assert_eq!(state, make_state([], [], []));

        state.ack("1".to_string());
        assert_eq!(state, make_state([], ["1"], []));
    }

    #[test]
    fn nack_out_of_lease_ignored() {
        let mut state = LeaseState::new();
        assert_eq!(state, make_state([], [], []));

        state.nack("1".to_string());
        assert_eq!(state, make_state([], [], []));
    }
}
