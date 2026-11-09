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

use crate::channel_pool::TransactionAffinity;
use crate::database_client::DatabaseClient;
use crate::error::internal_error;
use crate::model::commit_response::MultiplexedSessionRetry;
use crate::model::{
    CommitRequest, CommitResponse, MultiplexedSessionPrecommitToken, RequestOptions,
};
use crate::write_only_transaction::create_commit_request;
use bytes::Bytes;
use google_cloud_gax::options::RequestOptions as GaxRequestOptions;
use std::sync::{Arc, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::Duration;

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

    /// Acquires a shared read lock on the tracked precommit token, recovering from lock poisoning via `into_inner()`.
    ///
    /// # Poison Recovery Rationale
    /// `tracker` protects an `Option<MultiplexedSessionPrecommitToken>`. State transitions are atomic
    /// replacements (`*guard = Some(token)`); the underlying data is always memory-safe and structurally
    /// sound in Rust even if a previous reader/writer thread panicked. Recovering the guard via
    /// `into_inner()` prevents an isolated panic in an application RPC, streaming query, or commit step
    /// from permanently disabling precommit token tracking and causing cascading transaction failures.
    fn read_tracker(
        &self,
    ) -> Option<RwLockReadGuard<'_, Option<MultiplexedSessionPrecommitToken>>> {
        let Self::Track(tracker) = self else {
            return None;
        };
        Some(tracker.read().unwrap_or_else(PoisonError::into_inner))
    }

    /// Acquires an exclusive write lock on the tracked precommit token, recovering from lock poisoning via `into_inner()`.
    ///
    /// # Poison Recovery Rationale
    /// See [`read_tracker`](Self::read_tracker).
    fn write_tracker(
        &self,
    ) -> Option<RwLockWriteGuard<'_, Option<MultiplexedSessionPrecommitToken>>> {
        let Self::Track(tracker) = self else {
            return None;
        };
        Some(tracker.write().unwrap_or_else(PoisonError::into_inner))
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

/// Constructs a commit request for retrying after receiving a precommit token.
///
/// Mutations are explicitly omitted from the parameter list to guarantee by construction
/// that mutations are never re-sent on a precommit token retry.
pub(crate) fn create_retry_commit_request(
    session_name: String,
    transaction_id: Bytes,
    precommit_token: Option<MultiplexedSessionPrecommitToken>,
    request_options: Option<RequestOptions>,
    max_commit_delay: Option<Duration>,
    return_commit_stats: bool,
) -> CommitRequest {
    create_commit_request(
        session_name,
        transaction_id,
        Vec::new(), // Mutations are never sent on retry
        precommit_token,
        request_options,
        max_commit_delay,
        return_commit_stats,
    )
}

/// Executes a commit request with support for a single precommit-token retry if Spanner
/// requests one via `MultiplexedSessionRetry::PrecommitToken`.
///
/// If Spanner returns a precommit token directive, the retry request is constructed using
/// the provided `build_retry_request` closure and dispatched with the updated token.
///
/// If the retry attempt unexpectedly returns another precommit token (or any unrecognized
/// retry directive), this function fails fast with `Code::Internal`, ensuring uncommitted
/// transactions are never reported as successful and are not mistaken for `Aborted` errors.
pub(crate) async fn commit_with_precommit_retry(
    client: &DatabaseClient,
    commit_request: CommitRequest,
    commit_gax_options: GaxRequestOptions,
    affinity: Option<&TransactionAffinity>,
    precommit_token_tracker: &PrecommitTokenTracker,
    inspect_err: impl Fn(&crate::Error),
    build_retry_request: impl FnOnce(Option<MultiplexedSessionPrecommitToken>) -> CommitRequest,
) -> Result<CommitResponse, crate::Error> {
    let response = client
        .commit(commit_request, commit_gax_options.clone(), affinity)
        .await
        .inspect_err(&inspect_err)?;

    let Some(retry_directive) = response.multiplexed_session_retry else {
        return Ok(response);
    };

    let MultiplexedSessionRetry::PrecommitToken(new_precommit_token) = retry_directive;

    // Update the tracker with the new token returned by Spanner
    precommit_token_tracker.update(Some(*new_precommit_token));
    let retry_request = build_retry_request(precommit_token_tracker.get());
    let transaction_id = retry_request.transaction_id().cloned();

    let retry_response = client
        .commit(retry_request, commit_gax_options, affinity)
        .await
        .inspect_err(&inspect_err)?;

    if retry_response.multiplexed_session_retry.is_some() {
        client.clear_transaction_affinity_routing(transaction_id.as_deref());
        return Err(internal_error(
            "commit failed: received an unexpected subsequent precommit token on multiplexed session commit retry",
        ));
    }

    Ok(retry_response)
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

    #[test]
    fn retry_commit_request_omits_mutations_and_preserves_token() {
        let token = MultiplexedSessionPrecommitToken::new()
            .set_precommit_token(Bytes::from("retry_precommit_token"))
            .set_seq_num(42);
        let request_options = RequestOptions::default().set_request_tag("tag1");

        let request = create_retry_commit_request(
            "projects/p/instances/i/databases/d/sessions/s1".to_string(),
            Bytes::from_static(b"tx_id_123"),
            Some(token.clone()),
            Some(request_options.clone()),
            Some(Duration::from_millis(500)),
            true,
        );

        assert_eq!(
            request.session, "projects/p/instances/i/databases/d/sessions/s1",
            "session name must match"
        );
        assert_eq!(
            request.transaction_id(),
            Some(&Bytes::from_static(b"tx_id_123")),
            "transaction id must match"
        );
        assert!(
            request.mutations.is_empty(),
            "mutations must always be empty in retry commit request"
        );
        assert_eq!(
            request.precommit_token,
            Some(token),
            "precommit token must match"
        );
        assert_eq!(
            request.request_options,
            Some(request_options),
            "request options must match"
        );
        use wkt::Duration as WktDuration;

        assert_eq!(
            request.max_commit_delay,
            Some(WktDuration::clamp(0, 500_000_000)),
            "max commit delay must match"
        );
        assert!(
            request.return_commit_stats,
            "return commit stats must be true"
        );
    }
}
