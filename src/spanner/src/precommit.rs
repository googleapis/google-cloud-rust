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

use std::sync::{Arc, RwLock};

#[derive(Clone, Debug)]
pub(crate) enum PrecommitTokenTracker {
    NoOp,
    Track(Arc<RwLock<Option<crate::model::MultiplexedSessionPrecommitToken>>>),
}

impl PrecommitTokenTracker {
    /// Creates a pre-commit token tracker for read-write transactions.
    pub(crate) fn new() -> Self {
        Self::Track(Arc::new(RwLock::new(None)))
    }

    /// Creates a no-op tracker for read-only transactions.
    pub(crate) fn new_noop() -> Self {
        Self::NoOp
    }

    /// Updates the tracker with an optional precommit token from a response.
    pub(crate) fn update(&self, token: Option<crate::model::MultiplexedSessionPrecommitToken>) {
        if let (Some(token), Self::Track(tracker)) = (token, self) {
            let mut guard = tracker.write().unwrap();
            if guard.as_ref().is_none_or(|c| c.seq_num < token.seq_num) {
                *guard = Some(token);
            }
        }
    }

    /// Returns the highest sequenced precommit token.
    pub(crate) fn get(&self) -> Option<crate::model::MultiplexedSessionPrecommitToken> {
        let Self::Track(tracker) = self else {
            return None;
        };
        tracker.read().unwrap().clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::MultiplexedSessionPrecommitToken;

    #[test]
    fn auto_traits() {
        static_assertions::assert_impl_all!(PrecommitTokenTracker: Send, Sync, std::fmt::Debug);
    }

    #[test]
    fn test_noop_tracker() {
        let tracker = PrecommitTokenTracker::new_noop();
        assert!(
            tracker.get().is_none(),
            "NoOp tracker should not return a token"
        );

        tracker.update(Some(MultiplexedSessionPrecommitToken::new().set_seq_num(1)));
        assert!(
            tracker.get().is_none(),
            "NoOp tracker should ignore updates"
        );
    }

    #[test]
    fn test_tracker_update_highest_seq() {
        let tracker = PrecommitTokenTracker::new();
        assert!(tracker.get().is_none(), "Tracker should initially be empty");

        let token1 = MultiplexedSessionPrecommitToken::new()
            .set_precommit_token(bytes::Bytes::from("token1"))
            .set_seq_num(1);
        tracker.update(Some(token1));

        let retrieved = tracker.get().expect("expected token to be tracked");
        assert_eq!(retrieved.precommit_token, "token1");
        assert_eq!(retrieved.seq_num, 1);

        // Update with lower sequence number, should not modify state
        let token0 = MultiplexedSessionPrecommitToken::new()
            .set_precommit_token(bytes::Bytes::from("token0"))
            .set_seq_num(0);
        tracker.update(Some(token0));

        let retrieved = tracker.get().expect("expected token 1 to be retained");
        assert_eq!(retrieved.precommit_token, "token1");
        assert_eq!(retrieved.seq_num, 1);

        // Update with higher sequence number, should modify state
        let token2 = MultiplexedSessionPrecommitToken::new()
            .set_precommit_token(bytes::Bytes::from("token2"))
            .set_seq_num(2);
        tracker.update(Some(token2));

        let retrieved = tracker.get().expect("expected token 2 to be tracked");
        assert_eq!(retrieved.precommit_token, "token2");
        assert_eq!(retrieved.seq_num, 2);

        // Update with None, should gracefully escape and do nothing to state
        tracker.update(None::<MultiplexedSessionPrecommitToken>);
        let retrieved = tracker.get().expect("expected token 2 to be unmodified");
        assert_eq!(retrieved.precommit_token, "token2");
        assert_eq!(retrieved.seq_num, 2);
    }
}
