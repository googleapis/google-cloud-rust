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

//! Endpoint overload cooldown and request-scoped endpoint exclusion list.
//!
//! When routing requests to specific Spanner tablet nodes, the client uses two complementary
//! mechanisms to avoid sending traffic to unhealthy or overloaded endpoints:
//!
//! 1. [`EndpointCooldownTracker`]: A global, probabilistic cooldown tracker that puts an endpoint
//!    address on temporary backoff (with exponential backoff and jitter in `[cooldown / 2, cooldown]`)
//!    when it returns a `RESOURCE_EXHAUSTED` error.
//! 2. [`EndpointExclusionList`]: A deterministic, request-scoped exclusion set of endpoints that
//!    have already been attempted during an individual RPC retry loop.

// TODO(location-aware-routing): Remove allow(dead_code) once integrated into LocationRouter.
#![allow(dead_code)]

use google_cloud_gax::error::rpc::Code;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::RwLock;
use std::time::Duration;
use std::time::Instant;

/// Default initial cooldown duration when an endpoint first returns `RESOURCE_EXHAUSTED`.
const DEFAULT_INITIAL_COOLDOWN: Duration = Duration::from_secs(5);

/// Default maximum cooldown duration cap for repeatedly failing endpoints.
const DEFAULT_MAX_COOLDOWN: Duration = Duration::from_secs(60);

/// Default time window after which consecutive failure counts are reset if no further failures occur.
const DEFAULT_RESET_AFTER: Duration = Duration::from_secs(600);

/// Internal state recorded for an endpoint currently on overload cooldown.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct EndpointCooldownState {
    /// Number of consecutive `RESOURCE_EXHAUSTED` failures recorded within the reset window.
    consecutive_failures: usize,
    /// Absolute time until which this endpoint is considered on cooldown.
    cooldown_until: Instant,
    /// Timestamp of the most recent failure.
    last_failure_at: Instant,
}

/// Tracks endpoint-scoped overload cooldowns triggered by `RESOURCE_EXHAUSTED` gRPC errors.
///
/// When a routed Spanner tablet endpoint returns `RESOURCE_EXHAUSTED`, it is placed on a
/// probabilistic cooldown with exponential backoff and jitter in `[cooldown / 2, cooldown]`,
/// preventing the client from routing new traffic to an overloaded tablet node.
#[derive(Debug)]
pub(crate) struct EndpointCooldownTracker {
    initial_cooldown: Duration,
    max_cooldown: Duration,
    reset_after: Duration,
    state: RwLock<HashMap<String, EndpointCooldownState>>,
}

impl Default for EndpointCooldownTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl EndpointCooldownTracker {
    /// Creates a new `EndpointCooldownTracker` with default durations:
    /// - 5 seconds initial cooldown
    /// - 60 seconds maximum cooldown cap
    /// - 10 minutes reset window
    pub(crate) fn new() -> Self {
        Self::with_options(
            DEFAULT_INITIAL_COOLDOWN,
            DEFAULT_MAX_COOLDOWN,
            DEFAULT_RESET_AFTER,
        )
    }

    /// Creates a new `EndpointCooldownTracker` with customizable durations.
    pub(crate) fn with_options(
        initial_cooldown: Duration,
        max_cooldown: Duration,
        reset_after: Duration,
    ) -> Self {
        Self {
            initial_cooldown,
            max_cooldown,
            reset_after,
            state: RwLock::new(HashMap::new()),
        }
    }

    /// Returns `true` if the given endpoint address is currently on active cooldown.
    pub(crate) fn is_cooling_down(&self, endpoint: &str) -> bool {
        self.is_cooling_down_at(endpoint, Instant::now())
    }

    /// Checks if the endpoint is cooling down relative to a specific reference timestamp.
    pub(crate) fn is_cooling_down_at(&self, endpoint: &str, now: Instant) -> bool {
        let guard = self
            .state
            .read()
            .expect("EndpointCooldownTracker read lock poisoned");
        guard
            .get(endpoint)
            .is_some_and(|entry| now < entry.cooldown_until)
    }

    /// Records an RPC failure for the endpoint.
    ///
    /// If the `status_code` is [`Code::ResourceExhausted`], places the endpoint on cooldown
    /// and returns `Some(cooldown_duration)`. For any other error code, does nothing and returns `None`.
    pub(crate) fn record_error(&self, endpoint: &str, status_code: Code) -> Option<Duration> {
        if status_code != Code::ResourceExhausted {
            return None;
        }
        Some(self.record_failure(endpoint))
    }

    /// Records a `RESOURCE_EXHAUSTED` failure for the endpoint, applying exponential backoff
    /// and jitter in `[cooldown / 2, cooldown]` to calculate the new cooldown period.
    pub(crate) fn record_failure(&self, endpoint: &str) -> Duration {
        self.record_failure_at(endpoint, Instant::now())
    }

    /// Records a failure at a specific reference timestamp and returns the applied cooldown duration.
    pub(crate) fn record_failure_at(&self, endpoint: &str, now: Instant) -> Duration {
        let mut guard = self
            .state
            .write()
            .expect("EndpointCooldownTracker write lock poisoned");

        let consecutive_failures = match guard.get(endpoint) {
            Some(entry)
                if now.saturating_duration_since(entry.last_failure_at) < self.reset_after =>
            {
                entry.consecutive_failures.saturating_add(1)
            }
            _ => 1,
        };

        // Double base cooldown for each failure after the first: 5s -> 10s -> 20s -> 40s -> 60s (capped).
        let shift = consecutive_failures.saturating_sub(1).min(31) as u32;
        let base_cooldown = self.initial_cooldown.saturating_mul(1u32 << shift);
        let capped_cooldown = base_cooldown.min(self.max_cooldown);

        // Apply half-to-full jitter duration in [capped_cooldown / 2, capped_cooldown].
        let millis = u64::try_from(capped_cooldown.as_millis()).unwrap_or(u64::MAX);
        let jittered_millis = if millis == 0 {
            0
        } else {
            let floor_millis = (millis / 2).max(1);
            rand::random_range(floor_millis..=millis)
        };
        let jittered_cooldown = Duration::from_millis(jittered_millis);

        let new_state = EndpointCooldownState {
            consecutive_failures,
            cooldown_until: now
                .checked_add(jittered_cooldown)
                .or_else(|| now.checked_add(Duration::from_secs(86400 * 30)))
                .unwrap_or(now),
            last_failure_at: now,
        };

        match guard.get_mut(endpoint) {
            Some(existing) => {
                *existing = new_state;
            }
            None => {
                guard.insert(endpoint.to_string(), new_state);
            }
        }

        jittered_cooldown
    }

    /// Removes entries from the tracker that have expired their cooldown and reset window.
    pub(crate) fn clear_expired(&self) {
        self.clear_expired_at(Instant::now());
    }

    /// Removes expired entries relative to the specified timestamp.
    pub(crate) fn clear_expired_at(&self, now: Instant) {
        {
            let guard = self
                .state
                .read()
                .expect("EndpointCooldownTracker read lock poisoned");
            let has_expired = guard.values().any(|entry| {
                now >= entry.cooldown_until
                    && now.saturating_duration_since(entry.last_failure_at) >= self.reset_after
            });
            if !has_expired {
                return;
            }
        }

        let mut guard = self
            .state
            .write()
            .expect("EndpointCooldownTracker write lock poisoned");
        guard.retain(|_, entry| {
            now < entry.cooldown_until
                || now.saturating_duration_since(entry.last_failure_at) < self.reset_after
        });
    }

    /// Clears all tracked endpoint cooldowns.
    pub(crate) fn clear(&self) {
        let mut guard = self
            .state
            .write()
            .expect("EndpointCooldownTracker write lock poisoned");
        guard.clear();
    }

    /// Returns the number of endpoints currently tracked in the state map.
    pub(crate) fn len(&self) -> usize {
        let guard = self
            .state
            .read()
            .expect("EndpointCooldownTracker read lock poisoned");
        guard.len()
    }

    /// Returns `true` if no endpoints are currently tracked.
    pub(crate) fn is_empty(&self) -> bool {
        let guard = self
            .state
            .read()
            .expect("EndpointCooldownTracker read lock poisoned");
        guard.is_empty()
    }
}

/// A request-scoped exclusion list of Spanner tablet endpoints.
///
/// Because [`EndpointCooldownTracker`] applies jitter in `[cooldown / 2, cooldown]`,
/// global cooldown alone does not guarantee that an endpoint cannot expire during long retries.
/// `EndpointExclusionList` provides a deterministic guarantee for an individual
/// RPC retry chain by recording every endpoint address attempted during that request.
#[derive(Debug, Default, Clone)]
pub(crate) struct EndpointExclusionList {
    excluded: HashSet<String>,
}

impl EndpointExclusionList {
    /// Creates a new empty request-scoped endpoint exclusion list.
    pub(crate) fn new() -> Self {
        Self {
            excluded: HashSet::new(),
        }
    }

    /// Excludes the given endpoint address from subsequent retry attempts in this request.
    pub(crate) fn exclude(&mut self, endpoint: impl Into<String>) {
        self.excluded.insert(endpoint.into());
    }

    /// Returns `true` if the endpoint address is currently in this exclusion list.
    pub(crate) fn is_excluded(&self, endpoint: &str) -> bool {
        self.excluded.contains(endpoint)
    }

    /// Returns `true` if the endpoint is either in this request-scoped exclusion list
    /// or is currently cooling down in the global `cooldown_tracker`.
    ///
    /// Checks the in-memory request exclusion set first and short-circuits without acquiring
    /// locks on the global cooldown tracker if the endpoint is already excluded.
    pub(crate) fn is_excluded_or_cooling_down(
        &self,
        endpoint: &str,
        cooldown_tracker: &EndpointCooldownTracker,
    ) -> bool {
        if self.is_excluded(endpoint) {
            return true;
        }
        cooldown_tracker.is_cooling_down(endpoint)
    }

    /// Clears all excluded endpoints from this list.
    pub(crate) fn clear(&mut self) {
        self.excluded.clear();
    }

    /// Returns the number of excluded endpoints.
    pub(crate) fn len(&self) -> usize {
        self.excluded.len()
    }

    /// Returns `true` if no endpoints are excluded.
    pub(crate) fn is_empty(&self) -> bool {
        self.excluded.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn traits() {
        static_assertions::assert_impl_all!(EndpointCooldownTracker: Send, Sync, std::fmt::Debug);
        static_assertions::assert_impl_all!(
            EndpointExclusionList: Send,
            Sync,
            std::fmt::Debug,
            Clone
        );
    }

    #[test]
    fn cooldown_tracker_default_construction() {
        let tracker = EndpointCooldownTracker::new();
        assert!(tracker.is_empty(), "tracker should be empty initially");
        assert_eq!(tracker.len(), 0, "tracker length should be 0");
        assert!(
            !tracker.is_cooling_down("10.0.0.1:15000"),
            "untracked endpoint should not be cooling down"
        );
    }

    #[test]
    fn cooldown_tracker_record_failure_increases_consecutive_failures() {
        let now = Instant::now();
        let tracker = EndpointCooldownTracker::with_options(
            Duration::from_millis(100),
            Duration::from_millis(1000),
            Duration::from_secs(60),
        );

        let duration1 = tracker.record_failure_at("ep-1", now);
        assert!(
            duration1 <= Duration::from_millis(100),
            "duration1 {duration1:?} <= 100ms"
        );
        assert!(
            duration1 >= Duration::from_millis(50),
            "duration1 {duration1:?} >= 50ms (jitter floor)"
        );

        let duration2 = tracker.record_failure_at("ep-1", now + Duration::from_millis(10));
        assert!(
            duration2 <= Duration::from_millis(200),
            "duration2 {duration2:?} <= 200ms"
        );
        assert!(
            duration2 >= Duration::from_millis(100),
            "duration2 {duration2:?} >= 100ms (jitter floor)"
        );

        let duration3 = tracker.record_failure_at("ep-1", now + Duration::from_millis(20));
        assert!(
            duration3 <= Duration::from_millis(400),
            "duration3 {duration3:?} <= 400ms"
        );
        assert!(
            duration3 >= Duration::from_millis(200),
            "duration3 {duration3:?} >= 200ms (jitter floor)"
        );
        assert_eq!(tracker.len(), 1, "should have 1 tracked endpoint");
    }

    #[test]
    fn cooldown_tracker_exponential_backoff_and_max_cap() {
        let now = Instant::now();
        let tracker = EndpointCooldownTracker::with_options(
            Duration::from_millis(100),
            Duration::from_millis(500),
            Duration::from_secs(60),
        );

        // Failures 1..10 should eventually cap at max_cooldown (500ms).
        for i in 0..10 {
            let duration = tracker.record_failure_at("ep-cap", now + Duration::from_millis(i));
            assert!(
                duration <= Duration::from_millis(500),
                "duration {duration:?} exceeded max cap 500ms on attempt {i}"
            );
            if i >= 3 {
                assert!(
                    duration >= Duration::from_millis(250),
                    "duration {duration:?} under capped floor 250ms on attempt {i}"
                );
            }
        }
    }

    #[test]
    fn cooldown_tracker_reset_after_window() {
        let now = Instant::now();
        let reset_after = Duration::from_secs(10);
        let tracker = EndpointCooldownTracker::with_options(
            Duration::from_secs(5),
            Duration::from_secs(60),
            reset_after,
        );

        // First failure
        tracker.record_failure_at("ep-reset", now);

        // Second failure after reset window -> consecutive_failures resets to 1, cap at initial 5s.
        let duration =
            tracker.record_failure_at("ep-reset", now + reset_after + Duration::from_millis(1));
        assert!(
            duration <= Duration::from_secs(5),
            "after reset window, backoff should reset to base cooldown <= 5s"
        );
        assert!(
            duration >= Duration::from_millis(2500),
            "after reset window, backoff should reset to base cooldown >= 2.5s"
        );
    }

    #[test]
    fn cooldown_tracker_only_triggers_on_resource_exhausted() {
        let tracker = EndpointCooldownTracker::new();

        assert_eq!(
            tracker.record_error("ep-1", Code::Unavailable),
            None,
            "UNAVAILABLE should not trigger cooldown"
        );
        assert_eq!(
            tracker.record_error("ep-1", Code::Internal),
            None,
            "INTERNAL should not trigger cooldown"
        );
        assert_eq!(
            tracker.record_error("ep-1", Code::DeadlineExceeded),
            None,
            "DEADLINE_EXCEEDED should not trigger cooldown"
        );
        assert!(tracker.is_empty(), "tracker should remain empty");

        let res = tracker.record_error("ep-1", Code::ResourceExhausted);
        assert!(res.is_some(), "RESOURCE_EXHAUSTED should trigger cooldown");
        assert_eq!(tracker.len(), 1, "tracker should have 1 entry");
    }

    #[test]
    fn cooldown_tracker_clear_expired_removes_old_entries() {
        let now = Instant::now();
        let tracker = EndpointCooldownTracker::with_options(
            Duration::from_millis(10),
            Duration::from_millis(50),
            Duration::from_millis(100),
        );

        tracker.record_failure_at("ep-old", now);
        assert_eq!(tracker.len(), 1, "tracker should have 1 entry");

        // Advance time past both cooldown_until and reset_after
        let future = now + Duration::from_millis(200);
        tracker.clear_expired_at(future);
        assert!(
            tracker.is_empty(),
            "expired entry should be cleaned up by clear_expired_at"
        );
    }

    #[test]
    fn cooldown_tracker_clear_removes_all_entries() {
        let tracker = EndpointCooldownTracker::new();
        tracker.record_failure("ep-1");
        tracker.record_failure("ep-2");
        assert_eq!(tracker.len(), 2, "tracker should have 2 entries");

        tracker.clear();
        assert!(tracker.is_empty(), "tracker should be empty after clear");
    }

    #[test]
    fn cooldown_tracker_is_cooling_down_at_time() {
        let now = Instant::now();
        let tracker = EndpointCooldownTracker::with_options(
            Duration::from_secs(5),
            Duration::from_secs(5),
            Duration::from_secs(60),
        );

        tracker.record_failure_at("ep-time", now);

        // At time of failure, is_cooling_down_at should be true (minimum cooldown is 2.5s).
        assert!(
            tracker.is_cooling_down_at("ep-time", now),
            "endpoint should be cooling down at time of failure"
        );

        // At now + 10s (past max 5s cooldown), is_cooling_down_at should be false.
        assert!(
            !tracker.is_cooling_down_at("ep-time", now + Duration::from_secs(10)),
            "endpoint should not be cooling down after cooldown expires"
        );
    }

    #[test]
    fn cooldown_tracker_concurrent_access() {
        let tracker = EndpointCooldownTracker::new();

        thread::scope(|s| {
            for i in 0..10 {
                let t = &tracker;
                s.spawn(move || {
                    let ep = format!("ep-{}", i % 3);
                    t.record_failure(&ep);
                    let _ = t.is_cooling_down(&ep);
                    let _ = t.len();
                });
            }
        });

        assert!(
            tracker.len() <= 3,
            "tracker length should be at most 3 distinct endpoints"
        );
    }

    #[test]
    fn exclusion_list_basic_operations() {
        let mut list = EndpointExclusionList::new();
        assert!(list.is_empty(), "exclusion list should be empty initially");
        assert_eq!(list.len(), 0, "exclusion list length should be 0");

        list.exclude("10.0.0.1:15000");
        assert!(!list.is_empty(), "exclusion list should not be empty");
        assert_eq!(list.len(), 1, "exclusion list length should be 1");
        assert!(
            list.is_excluded("10.0.0.1:15000"),
            "10.0.0.1:15000 should be excluded"
        );
        assert!(
            !list.is_excluded("10.0.0.2:15000"),
            "10.0.0.2:15000 should not be excluded"
        );

        list.exclude("10.0.0.2:15000");
        assert_eq!(list.len(), 2, "exclusion list length should be 2");

        list.clear();
        assert!(
            list.is_empty(),
            "exclusion list should be empty after clear"
        );
        assert!(
            !list.is_excluded("10.0.0.1:15000"),
            "cleared list should not contain excluded endpoints"
        );
    }

    #[test]
    fn exclusion_list_is_excluded_or_cooling_down_short_circuit() {
        let now = Instant::now();
        let tracker = EndpointCooldownTracker::with_options(
            Duration::from_secs(10),
            Duration::from_secs(10),
            Duration::from_secs(60),
        );
        let mut list = EndpointExclusionList::new();

        // 1. Neither excluded nor on cooldown
        assert!(
            !list.is_excluded_or_cooling_down("ep-1", &tracker),
            "ep-1 is neither excluded nor on cooldown"
        );

        // 2. Only in request exclusion list
        list.exclude("ep-2");
        assert!(
            list.is_excluded_or_cooling_down("ep-2", &tracker),
            "ep-2 is in request exclusion list"
        );

        // 3. Only in global cooldown tracker
        tracker.record_failure_at("ep-3", now);
        assert!(
            tracker.is_cooling_down_at("ep-3", now),
            "ep-3 must be on cooldown at now"
        );
        assert!(
            list.is_excluded_or_cooling_down("ep-3", &tracker),
            "ep-3 must be recognized as cooling down"
        );
    }

    #[test]
    fn exclusion_list_clear() {
        let mut list = EndpointExclusionList::new();
        list.exclude("ep-a");
        list.exclude("ep-b");
        assert_eq!(list.len(), 2, "should have 2 items");

        list.clear();
        assert_eq!(list.len(), 0, "should have 0 items after clear");
        assert!(!list.is_excluded("ep-a"), "ep-a should not be excluded");
    }

    #[test]
    fn cooldown_tracker_and_exclusion_list_default_traits() {
        let tracker = EndpointCooldownTracker::default();
        assert!(tracker.is_empty(), "default tracker should be empty");

        let list = EndpointExclusionList::default();
        assert!(list.is_empty(), "default exclusion list should be empty");
    }

    #[test]
    fn cooldown_tracker_zero_duration() {
        let tracker = EndpointCooldownTracker::with_options(
            Duration::ZERO,
            Duration::ZERO,
            Duration::from_secs(60),
        );
        let now = Instant::now();
        let duration = tracker.record_failure_at("ep-zero", now);
        assert_eq!(duration, Duration::ZERO, "cooldown duration should be zero");
        assert!(
            !tracker.is_cooling_down_at("ep-zero", now + Duration::from_millis(1)),
            "should not be cooling down past now"
        );
    }

    #[test]
    fn cooldown_tracker_clear_expired_selective() {
        let now = Instant::now();
        let tracker = EndpointCooldownTracker::with_options(
            Duration::from_millis(50),
            Duration::from_millis(50),
            Duration::from_millis(100),
        );

        tracker.record_failure_at("ep-old", now);
        tracker.record_failure_at("ep-new", now + Duration::from_millis(200));
        assert_eq!(tracker.len(), 2, "tracker should have 2 entries");

        // At now + 150ms, ep-old is expired (>100ms reset window and >50ms cooldown), but ep-new is in the future.
        tracker.clear_expired_at(now + Duration::from_millis(150));
        assert_eq!(tracker.len(), 1, "tracker should have 1 entry left");
        assert!(
            !tracker.is_cooling_down_at("ep-old", now + Duration::from_millis(150)),
            "ep-old should no longer be cooling down"
        );
    }

    #[test]
    fn cooldown_tracker_clear_expired_no_op_when_none_expired() {
        let now = Instant::now();
        let tracker = EndpointCooldownTracker::with_options(
            Duration::from_secs(10),
            Duration::from_secs(10),
            Duration::from_secs(60),
        );

        tracker.record_failure_at("ep-1", now);
        tracker.clear_expired_at(now);
        assert_eq!(tracker.len(), 1, "tracker should retain non-expired entry");
    }

    #[test]
    fn cooldown_tracker_atomic_consecutive_failures_no_lost_updates() {
        let tracker = EndpointCooldownTracker::new();
        let num_threads = 10;
        let iterations_per_thread = 100;
        let total_expected = num_threads * iterations_per_thread;
        let barrier = std::sync::Barrier::new(num_threads);

        thread::scope(|s| {
            for _ in 0..num_threads {
                let t = &tracker;
                let b = &barrier;
                s.spawn(move || {
                    b.wait();
                    for _ in 0..iterations_per_thread {
                        t.record_failure("ep-atomic");
                    }
                });
            }
        });

        let failures = tracker
            .state
            .read()
            .expect("read lock poisoned")
            .get("ep-atomic")
            .map_or(0, |entry| entry.consecutive_failures);

        assert_eq!(
            failures, total_expected,
            "all concurrent failures must be recorded atomically without lost updates"
        );
    }

    #[test]
    fn cooldown_tracker_large_duration_no_wrapping_cast() {
        let tracker = EndpointCooldownTracker::with_options(
            Duration::MAX,
            Duration::MAX,
            Duration::from_secs(600),
        );

        let now = Instant::now();
        let duration = tracker.record_failure_at("ep-huge", now);
        assert!(
            duration <= Duration::from_millis(u64::MAX),
            "duration should not overflow u64 millis"
        );
        assert_eq!(tracker.len(), 1, "tracker should have 1 entry");
    }
}
