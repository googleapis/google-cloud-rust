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

use crate::model::MultiplexedSessionPrecommitToken;
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

#[derive(Clone, Debug)]
pub(crate) enum PrecommitTokenTracker {
    NoOp,
    Track(Arc<RwLock<Option<MultiplexedSessionPrecommitToken>>>),
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

    /// Acquires a shared read lock on the tracked precommit token, recovering if the lock was poisoned.
    fn read_tracker(
        &self,
    ) -> Option<RwLockReadGuard<'_, Option<MultiplexedSessionPrecommitToken>>> {
        let Self::Track(tracker) = self else {
            return None;
        };
        Some(match tracker.read() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        })
    }

    /// Acquires an exclusive write lock on the tracked precommit token, recovering if the lock was poisoned.
    fn write_tracker(
        &self,
    ) -> Option<RwLockWriteGuard<'_, Option<MultiplexedSessionPrecommitToken>>> {
        let Self::Track(tracker) = self else {
            return None;
        };
        Some(match tracker.write() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        })
    }

    /// Updates the tracker with an optional precommit token from a response.
    pub(crate) fn update(&self, token: Option<MultiplexedSessionPrecommitToken>) {
        let Some(token) = token else {
            return;
        };
        let Some(mut guard) = self.write_tracker() else {
            return;
        };
        if guard
            .as_ref()
            .is_none_or(|current| current.seq_num < token.seq_num)
        {
            *guard = Some(token);
        }
    }

    /// Returns the highest sequenced precommit token.
    pub(crate) fn get(&self) -> Option<MultiplexedSessionPrecommitToken> {
        let guard = self.read_tracker()?;
        (*guard).clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::panic::{AssertUnwindSafe, catch_unwind};

    #[test]
    fn auto_traits() {
        static_assertions::assert_impl_all!(PrecommitTokenTracker: Send, Sync, std::fmt::Debug);
    }

    #[test]
    fn noop_tracker() {
        let tracker = PrecommitTokenTracker::new_noop();
        assert!(
            tracker.get().is_none(),
            "NoOp tracker must not return a token"
        );
        assert!(
            tracker.read_tracker().is_none(),
            "read_tracker must return None on NoOp variant"
        );
        assert!(
            tracker.write_tracker().is_none(),
            "write_tracker must return None on NoOp variant"
        );

        tracker.update(Some(MultiplexedSessionPrecommitToken::new().set_seq_num(1)));
        assert!(tracker.get().is_none(), "NoOp tracker must ignore updates");
    }

    #[test]
    fn tracker_update_highest_seq() {
        let tracker = PrecommitTokenTracker::new();
        assert!(tracker.get().is_none(), "Tracker must initially be empty");

        let token1 = MultiplexedSessionPrecommitToken::new()
            .set_precommit_token(bytes::Bytes::from("token1"))
            .set_seq_num(1);
        tracker.update(Some(token1));

        let retrieved = tracker.get().expect("expected token 1 to be tracked");
        assert_eq!(
            retrieved.precommit_token, "token1",
            "tracked token bytes must match"
        );
        assert_eq!(retrieved.seq_num, 1, "tracked sequence number must match");

        // Update with lower sequence number, should not modify state
        let token0 = MultiplexedSessionPrecommitToken::new()
            .set_precommit_token(bytes::Bytes::from("token0"))
            .set_seq_num(0);
        tracker.update(Some(token0));

        let retrieved = tracker.get().expect("expected token 1 to be retained");
        assert_eq!(
            retrieved.precommit_token, "token1",
            "tracked token bytes must match"
        );
        assert_eq!(retrieved.seq_num, 1, "tracked sequence number must match");

        // Update with higher sequence number, should modify state
        let token2 = MultiplexedSessionPrecommitToken::new()
            .set_precommit_token(bytes::Bytes::from("token2"))
            .set_seq_num(2);
        tracker.update(Some(token2));

        let retrieved = tracker.get().expect("expected token 2 to be tracked");
        assert_eq!(
            retrieved.precommit_token, "token2",
            "tracked token bytes must match"
        );
        assert_eq!(retrieved.seq_num, 2, "tracked sequence number must match");

        // Update with equal sequence number and different bytes, should not modify state
        let token_same_seq = MultiplexedSessionPrecommitToken::new()
            .set_precommit_token(bytes::Bytes::from("token2_duplicate"))
            .set_seq_num(2);
        tracker.update(Some(token_same_seq));

        let retrieved = tracker.get().expect("expected token 2 to be unmodified");
        assert_eq!(
            retrieved.precommit_token, "token2",
            "duplicate sequence number must not overwrite existing token"
        );
        assert_eq!(retrieved.seq_num, 2, "tracked sequence number must match");

        // Update with None, should gracefully escape and do nothing to state
        tracker.update(None);
        let retrieved = tracker.get().expect("expected token 2 to be unmodified");
        assert_eq!(
            retrieved.precommit_token, "token2",
            "tracked token bytes must match"
        );
        assert_eq!(retrieved.seq_num, 2, "tracked sequence number must match");
    }

    #[test]
    fn tracker_recovers_from_poisoned_write_lock() {
        let tracker = PrecommitTokenTracker::new();

        let initial_token = MultiplexedSessionPrecommitToken::new()
            .set_precommit_token(bytes::Bytes::from("initial_token"))
            .set_seq_num(1);
        tracker.update(Some(initial_token));

        // Intentionally poison the RwLock by panicking while holding an exclusive write lock.
        let PrecommitTokenTracker::Track(inner) = &tracker else {
            panic!("expected Track variant");
        };

        let panic_result = catch_unwind(AssertUnwindSafe(|| {
            let _guard = inner.write().expect("lock should acquire write lock");
            panic!("deliberately poisoning write lock for test");
        }));
        assert!(panic_result.is_err(), "catch_unwind must capture panic");
        assert!(
            inner.is_poisoned(),
            "RwLock must be poisoned after write lock panic"
        );

        // Verify that get() recovers seamlessly via into_inner() without panicking.
        let retrieved = tracker
            .get()
            .expect("get must recover from poisoned lock and return existing token");
        assert_eq!(
            retrieved.precommit_token, "initial_token",
            "token bytes must match initial token"
        );
        assert_eq!(
            retrieved.seq_num, 1,
            "sequence number must match initial token"
        );

        // Verify that update() with a higher sequence number recovers and modifies state.
        let higher_token = MultiplexedSessionPrecommitToken::new()
            .set_precommit_token(bytes::Bytes::from("higher_token"))
            .set_seq_num(2);
        tracker.update(Some(higher_token));

        let retrieved = tracker
            .get()
            .expect("get must return updated token after poisoned lock write");
        assert_eq!(
            retrieved.precommit_token, "higher_token",
            "token bytes must match higher token"
        );
        assert_eq!(
            retrieved.seq_num, 2,
            "sequence number must match higher token"
        );

        // Verify that update() with a lower sequence number is safely ignored under poisoned lock.
        let lower_token = MultiplexedSessionPrecommitToken::new()
            .set_precommit_token(bytes::Bytes::from("lower_token"))
            .set_seq_num(0);
        tracker.update(Some(lower_token));

        let retrieved = tracker
            .get()
            .expect("get must return unmodified token after lower token update");
        assert_eq!(
            retrieved.precommit_token, "higher_token",
            "token bytes must remain higher token"
        );
        assert_eq!(
            retrieved.seq_num, 2,
            "sequence number must remain higher token"
        );

        // Verify that update() with an equal sequence number is safely ignored under poisoned lock.
        let equal_token = MultiplexedSessionPrecommitToken::new()
            .set_precommit_token(bytes::Bytes::from("equal_token"))
            .set_seq_num(2);
        tracker.update(Some(equal_token));

        let retrieved = tracker
            .get()
            .expect("get must return unmodified token after equal token update");
        assert_eq!(
            retrieved.precommit_token, "higher_token",
            "token bytes must remain higher token"
        );
        assert_eq!(
            retrieved.seq_num, 2,
            "sequence number must remain higher token"
        );
        // Verify that read_tracker() and write_tracker() return valid guards despite poisoned lock.
        assert!(
            tracker.read_tracker().is_some(),
            "read_tracker must return valid guard on poisoned lock"
        );
        assert!(
            tracker.write_tracker().is_some(),
            "write_tracker must return valid guard on poisoned lock"
        );
    }

    #[test]
    fn tracker_recovers_when_poisoned_while_empty() {
        let tracker = PrecommitTokenTracker::new();

        let PrecommitTokenTracker::Track(inner) = &tracker else {
            panic!("expected Track variant");
        };

        let panic_result = catch_unwind(AssertUnwindSafe(|| {
            let _guard = inner.write().expect("lock should acquire write lock");
            panic!("deliberately poisoning empty tracker lock");
        }));
        assert!(panic_result.is_err(), "catch_unwind must capture panic");
        assert!(
            inner.is_poisoned(),
            "RwLock must be poisoned after write lock panic"
        );

        // get() should return None without panicking
        assert!(
            tracker.get().is_none(),
            "get must return None on empty tracker despite poisoned lock"
        );

        // update() should populate the empty tracker despite poisoned lock
        let token = MultiplexedSessionPrecommitToken::new()
            .set_precommit_token(bytes::Bytes::from("token_after_poison"))
            .set_seq_num(10);
        tracker.update(Some(token));

        let retrieved = tracker
            .get()
            .expect("get must return token populated after lock was poisoned");
        assert_eq!(
            retrieved.precommit_token, "token_after_poison",
            "token bytes must match"
        );
        assert_eq!(retrieved.seq_num, 10, "sequence number must match");
    }

    #[test]
    fn tracker_recovers_across_clones() {
        let tracker = PrecommitTokenTracker::new();
        let tracker_clone = tracker.clone();

        let PrecommitTokenTracker::Track(inner) = &tracker else {
            panic!("expected Track variant");
        };

        let panic_result = catch_unwind(AssertUnwindSafe(|| {
            let _guard = inner.write().expect("lock should acquire write lock");
            panic!("deliberately poisoning lock via original handle");
        }));
        assert!(panic_result.is_err(), "catch_unwind must capture panic");
        assert!(inner.is_poisoned(), "RwLock must be poisoned");

        // Verify that cloned tracker handle recovers seamlessly on both update and get
        let token = MultiplexedSessionPrecommitToken::new()
            .set_precommit_token(bytes::Bytes::from("shared_token"))
            .set_seq_num(3);
        tracker_clone.update(Some(token));

        let retrieved = tracker_clone
            .get()
            .expect("cloned tracker must recover from poisoned lock");
        assert_eq!(
            retrieved.precommit_token, "shared_token",
            "token bytes must match"
        );
        assert_eq!(retrieved.seq_num, 3, "sequence number must match");

        // Verify original handle also observes the updated state
        let original_retrieved = tracker
            .get()
            .expect("original tracker must recover from poisoned lock");
        assert_eq!(
            original_retrieved.precommit_token, "shared_token",
            "token bytes must match"
        );
        assert_eq!(original_retrieved.seq_num, 3, "sequence number must match");
    }
}
