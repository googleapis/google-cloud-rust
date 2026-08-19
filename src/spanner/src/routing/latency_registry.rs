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

//! Exponentially Weighted Moving Average (EWMA) latency tracking and replica latency registry.
//!
//! Provides time-decayed latency tracking per endpoint address and split group scope
//! to enable latency-aware replica selection for location-aware routing.

// TODO(location-aware-routing): Remove allow(dead_code) once integrated into LocationRouter and KeyRangeCache.
#![allow(dead_code)]

use std::borrow::Borrow;
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;
use std::hash::Hasher;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::RwLock;
use std::time::Duration;
use std::time::Instant;

/// Default error penalty duration applied when an endpoint encounters a failure: 10 seconds.
const DEFAULT_ERROR_PENALTY: Duration = Duration::from_secs(10);

/// Default fallback round-trip time (RTT) for unmeasured endpoints: 10 milliseconds.
const DEFAULT_RTT: Duration = Duration::from_millis(10);

/// Default penalty value for untracked endpoints with active in-flight requests: 1 second (1,000,000 microseconds).
const DEFAULT_PENALTY_VALUE: f64 = 1_000_000.0;

/// Default time-decay window ($\tau$) for EWMA latency: 10 seconds.
const DEFAULT_DECAY_DURATION: Duration = Duration::from_secs(10);

/// Registry managing process-local EWMA latency scores across Spanner split replicas and endpoints.
///
/// Tracks round-trip latency per `(database_scope, group_uid, endpoint_address)` tuple, allowing
/// the replica selector to choose the lowest-latency healthy replica for a given database partition or split.
#[derive(Debug)]
pub(crate) struct LatencyRegistry {
    trackers: RwLock<HashMap<LatencyKey, Arc<EwmaLatencyTracker>>>,
    decay_duration: Duration,
    error_penalty: Duration,
    default_rtt: Duration,
}

impl Default for LatencyRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl LatencyRegistry {
    /// Creates a new `LatencyRegistry` with default settings:
    /// - 10-second EWMA decay window
    /// - 10-second error penalty
    /// - 10-millisecond default RTT
    pub(crate) fn new() -> Self {
        Self::with_options(DEFAULT_DECAY_DURATION, DEFAULT_ERROR_PENALTY, DEFAULT_RTT)
    }

    /// Creates a new `LatencyRegistry` with custom configuration parameters.
    pub(crate) fn with_options(
        decay_duration: Duration,
        error_penalty: Duration,
        default_rtt: Duration,
    ) -> Self {
        Self {
            trackers: RwLock::new(HashMap::new()),
            decay_duration,
            error_penalty,
            default_rtt,
        }
    }

    /// Returns whether a latency score has been recorded for the specified latency key.
    /// Performs a zero-allocation borrowed lookup without holding the global read lock during inner mutex queries.
    pub(crate) fn has_score(
        &self,
        database_scope: Option<&str>,
        group_uid: u64,
        endpoint_address: &str,
    ) -> bool {
        if group_uid == 0 || endpoint_address.is_empty() {
            return false;
        }

        let lookup = LatencyKeyRef {
            database_scope,
            group_uid,
            endpoint_address,
        };

        let tracker = {
            let trackers = self
                .trackers
                .read()
                .expect("LatencyRegistry trackers read lock poisoned");
            trackers.get(&lookup as &dyn LatencyLookup).map(Arc::clone)
        };

        tracker.is_some_and(|tracker| tracker.is_initialized())
    }

    /// Computes the replica selection cost for an endpoint given its active in-flight request count.
    /// Performs a zero-allocation borrowed lookup without holding the global read lock during inner mutex queries.
    ///
    /// The selection cost is calculated as follows:
    /// 1. If an initialized score exists: `score * (active_requests + 1.0)`
    /// 2. If the endpoint is unmeasured but has in-flight requests: `DEFAULT_PENALTY_VALUE + active_requests`
    ///    to steer traffic away from burdened unknown endpoints.
    /// 3. If the endpoint is unmeasured and idle: `default_rtt_micros`.
    pub(crate) fn get_selection_cost(
        &self,
        database_scope: Option<&str>,
        group_uid: u64,
        active_requests: usize,
        endpoint_address: &str,
    ) -> f64 {
        if group_uid == 0 || endpoint_address.is_empty() {
            return f64::MAX;
        }

        let lookup = LatencyKeyRef {
            database_scope,
            group_uid,
            endpoint_address,
        };

        let active_multiplier = active_requests as f64 + 1.0;

        // Fast path: inspect the tracker under a scoped read lock, releasing the registry lock before query.
        let tracker = {
            let trackers = self
                .trackers
                .read()
                .expect("LatencyRegistry trackers read lock poisoned");
            trackers.get(&lookup as &dyn LatencyLookup).map(Arc::clone)
        };

        if let Some(tracker) = tracker
            && let Some(score) = tracker.score()
        {
            return score * active_multiplier;
        }

        // If the endpoint has never been measured but already has active in-flight requests,
        // penalize it heavily so the replica selector prefers unburdened or measured endpoints.
        if active_requests > 0 {
            return DEFAULT_PENALTY_VALUE + (active_requests as f64);
        }

        // If the endpoint is unmeasured and idle (active_requests == 0), return default RTT in microseconds.
        self.default_rtt.as_micros() as f64
    }

    /// Records an observed round-trip latency sample at the current timestamp.
    pub(crate) fn record_latency(
        &self,
        database_scope: Option<&str>,
        group_uid: u64,
        endpoint_address: &str,
        latency: Duration,
    ) {
        self.record_latency_at(
            database_scope,
            group_uid,
            endpoint_address,
            latency,
            Instant::now(),
        );
    }

    /// Records an observed round-trip latency sample at a specific timestamp.
    pub(crate) fn record_latency_at(
        &self,
        database_scope: Option<&str>,
        group_uid: u64,
        endpoint_address: &str,
        latency: Duration,
        now: Instant,
    ) {
        if group_uid == 0 || endpoint_address.is_empty() {
            return;
        }

        let tracker = self.get_or_create_tracker(database_scope, group_uid, endpoint_address);
        tracker.update_at(latency, now);
    }

    /// Records an RPC error penalty using the default penalty duration (10 seconds) at the current timestamp.
    ///
    /// The penalty is recorded as an inflated latency sample, causing the EWMA score to spike
    /// and temporarily disincentivizing the replica selector from routing traffic to this endpoint.
    pub(crate) fn record_error(
        &self,
        database_scope: Option<&str>,
        group_uid: u64,
        endpoint_address: &str,
    ) {
        self.record_error_with_penalty(
            database_scope,
            group_uid,
            endpoint_address,
            self.error_penalty,
            Instant::now(),
        );
    }

    /// Records an RPC error penalty with a custom penalty duration and reference timestamp.
    ///
    /// The penalty is recorded as an inflated latency sample, causing the EWMA score to spike
    /// and temporarily disincentivizing the replica selector from routing traffic to this endpoint.
    pub(crate) fn record_error_with_penalty(
        &self,
        database_scope: Option<&str>,
        group_uid: u64,
        endpoint_address: &str,
        penalty: Duration,
        now: Instant,
    ) {
        if group_uid == 0 || endpoint_address.is_empty() {
            return;
        }

        let tracker = self.get_or_create_tracker(database_scope, group_uid, endpoint_address);
        tracker.record_error_at(penalty, now);
    }

    /// Clears all tracked endpoint latency scores.
    pub(crate) fn clear(&self) {
        let mut trackers = self
            .trackers
            .write()
            .expect("LatencyRegistry trackers write lock poisoned");
        trackers.clear();
    }

    /// Returns the number of currently tracked latency keys.
    pub(crate) fn len(&self) -> usize {
        let trackers = self
            .trackers
            .read()
            .expect("LatencyRegistry trackers read lock poisoned");
        trackers.len()
    }

    /// Returns whether the registry contains zero tracked latency keys.
    pub(crate) fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn get_or_create_tracker(
        &self,
        database_scope: Option<&str>,
        group_uid: u64,
        endpoint_address: &str,
    ) -> Arc<EwmaLatencyTracker> {
        let lookup = LatencyKeyRef {
            database_scope,
            group_uid,
            endpoint_address,
        };

        // Fast path: shared read lock with zero heap allocation
        {
            let trackers = self
                .trackers
                .read()
                .expect("LatencyRegistry trackers read lock poisoned");
            if let Some(tracker) = trackers.get(&lookup as &dyn LatencyLookup) {
                return Arc::clone(tracker);
            }
        }

        // Slow path: exclusive write lock; allocate owned key strings only upon missing insertion
        let mut trackers = self
            .trackers
            .write()
            .expect("LatencyRegistry trackers write lock poisoned");

        if let Some(tracker) = trackers.get(&lookup as &dyn LatencyLookup) {
            return Arc::clone(tracker);
        }

        let owned_key = LatencyKey {
            database_scope: database_scope.map(ToString::to_string),
            group_uid,
            endpoint_address: endpoint_address.to_string(),
        };

        let new_tracker = Arc::new(EwmaLatencyTracker::with_decay_duration(self.decay_duration));
        trackers.insert(owned_key, Arc::clone(&new_tracker));
        new_tracker
    }
}

/// Zero-allocation borrowed lookup trait for [`LatencyKey`].
///
/// Enables querying `HashMap<LatencyKey, ...>` with stack-borrowed slices (`LatencyKeyRef`)
/// via `Borrow<dyn LatencyLookup>`, completely eliminating heap string allocations on read queries.
trait LatencyLookup {
    fn database_scope(&self) -> Option<&str>;
    fn group_uid(&self) -> u64;
    fn endpoint_address(&self) -> &str;
}

/// Composite owned key identifying a replica latency tracking scope.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct LatencyKey {
    database_scope: Option<String>,
    group_uid: u64,
    endpoint_address: String,
}

impl LatencyLookup for LatencyKey {
    fn database_scope(&self) -> Option<&str> {
        self.database_scope.as_deref()
    }

    fn group_uid(&self) -> u64 {
        self.group_uid
    }

    fn endpoint_address(&self) -> &str {
        &self.endpoint_address
    }
}

/// Zero-allocation borrowed reference for [`LatencyKey`] lookups.
struct LatencyKeyRef<'a> {
    database_scope: Option<&'a str>,
    group_uid: u64,
    endpoint_address: &'a str,
}

impl LatencyLookup for LatencyKeyRef<'_> {
    fn database_scope(&self) -> Option<&str> {
        self.database_scope
    }

    fn group_uid(&self) -> u64 {
        self.group_uid
    }

    fn endpoint_address(&self) -> &str {
        self.endpoint_address
    }
}

impl<'a> Borrow<dyn LatencyLookup + 'a> for LatencyKey {
    fn borrow(&self) -> &(dyn LatencyLookup + 'a) {
        self
    }
}

impl Hash for dyn LatencyLookup + '_ {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.database_scope().hash(state);
        self.group_uid().hash(state);
        self.endpoint_address().hash(state);
    }
}

impl PartialEq for dyn LatencyLookup + '_ {
    fn eq(&self, other: &Self) -> bool {
        self.database_scope() == other.database_scope()
            && self.group_uid() == other.group_uid()
            && self.endpoint_address() == other.endpoint_address()
    }
}

impl Eq for dyn LatencyLookup + '_ {}

#[derive(Debug)]
struct EwmaState {
    score_microseconds: f64,
    last_updated_at: Instant,
}

/// Exponentially Weighted Moving Average (EWMA) latency tracker.
///
/// Supports time-decayed weighting where the smoothing factor $\alpha(\Delta t)$ decays
/// exponentially with the elapsed time since the last sample:
///
/// $$\alpha(\Delta t) = 1 - e^{-\Delta t / \tau}$$
/// $$S_{i+1} = \alpha(\Delta t) \cdot \text{latency} + (1 - \alpha(\Delta t)) \cdot S_i$$
///
/// Where $\tau$ is the decay time constant (default 10 seconds).
#[derive(Debug)]
pub(crate) struct EwmaLatencyTracker {
    fixed_alpha: Option<f64>,
    tau_nanoseconds: f64,
    state: Mutex<Option<EwmaState>>,
}

impl Default for EwmaLatencyTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl EwmaLatencyTracker {
    /// Creates a new time-decayed `EwmaLatencyTracker` with the default 10-second decay window.
    pub(crate) fn new() -> Self {
        Self::with_decay_duration(DEFAULT_DECAY_DURATION)
    }

    /// Creates a new time-decayed `EwmaLatencyTracker` with a custom decay duration.
    pub(crate) fn with_decay_duration(decay_duration: Duration) -> Self {
        let effective_decay = if decay_duration.is_zero() {
            DEFAULT_DECAY_DURATION
        } else {
            decay_duration
        };

        Self {
            fixed_alpha: None,
            tau_nanoseconds: effective_decay.as_nanos().max(1) as f64,
            state: Mutex::new(None),
        }
    }

    /// Creates a new fixed-alpha `EwmaLatencyTracker` where smoothing $\alpha \in (0.0, 1.0]$
    /// remains constant regardless of elapsed time.
    pub(crate) fn with_fixed_alpha(alpha: f64) -> Self {
        let clamped_alpha = alpha.clamp(f64::MIN_POSITIVE, 1.0);
        Self {
            fixed_alpha: Some(clamped_alpha),
            tau_nanoseconds: 0.0,
            state: Mutex::new(None),
        }
    }

    /// Returns the current latency score in microseconds.
    ///
    /// If no samples have been recorded yet, returns `f64::MAX`.
    pub(crate) fn get_score(&self) -> f64 {
        self.score().unwrap_or(f64::MAX)
    }

    /// Returns the current latency score in microseconds, or `None` if uninitialized.
    pub(crate) fn score(&self) -> Option<f64> {
        let guard = self
            .state
            .lock()
            .expect("EwmaLatencyTracker state mutex poisoned");

        guard.as_ref().map(|state| state.score_microseconds)
    }

    /// Returns whether at least one latency sample or error penalty has been recorded.
    pub(crate) fn is_initialized(&self) -> bool {
        let guard = self
            .state
            .lock()
            .expect("EwmaLatencyTracker state mutex poisoned");
        guard.is_some()
    }

    /// Records an observed round-trip latency sample at the current timestamp.
    pub(crate) fn update(&self, latency: Duration) {
        self.update_at(latency, Instant::now());
    }

    /// Records an observed round-trip latency sample at a specific timestamp.
    pub(crate) fn update_at(&self, latency: Duration, now: Instant) {
        let latency_micros = latency.as_micros() as f64;
        let mut guard = self
            .state
            .lock()
            .expect("EwmaLatencyTracker state mutex poisoned");

        let Some(ref mut state) = *guard else {
            *guard = Some(EwmaState {
                score_microseconds: latency_micros,
                last_updated_at: now,
            });
            return;
        };

        let alpha = match self.fixed_alpha {
            Some(fixed) => fixed,
            None => self.calculate_time_based_alpha(state.last_updated_at, now),
        };

        state.score_microseconds =
            alpha * latency_micros + (1.0 - alpha) * state.score_microseconds;
        state.last_updated_at = state.last_updated_at.max(now);
    }

    /// Records an error penalty using the default 10-second penalty duration.
    pub(crate) fn record_error(&self) {
        self.record_error_at(DEFAULT_ERROR_PENALTY, Instant::now());
    }

    /// Records an error penalty with a specific penalty duration at the given timestamp.
    pub(crate) fn record_error_at(&self, penalty: Duration, now: Instant) {
        self.update_at(penalty, now);
    }

    fn calculate_time_based_alpha(&self, last_updated_at: Instant, now: Instant) -> f64 {
        if now <= last_updated_at {
            // Over infinitesimally small or concurrent intervals, decay approaches 0.
            return 0.0;
        }

        let delta_nanoseconds = now.saturating_duration_since(last_updated_at).as_nanos() as f64;
        let ratio = delta_nanoseconds / self.tau_nanoseconds;
        let alpha = 1.0 - (-ratio).exp();
        alpha.clamp(0.0, 1.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Barrier;
    use std::thread;

    #[test]
    fn traits() {
        static_assertions::assert_impl_all!(LatencyRegistry: Send, Sync, Debug);
        static_assertions::assert_impl_all!(EwmaLatencyTracker: Send, Sync, Debug);
        static_assertions::assert_impl_all!(LatencyKey: Send, Sync, Debug, Clone, PartialEq, Eq);
    }

    #[test]
    fn tracker_uninitialized_returns_max() {
        let tracker = EwmaLatencyTracker::new();
        assert!(
            !tracker.is_initialized(),
            "new tracker must not be initialized"
        );
        assert_eq!(
            tracker.get_score(),
            f64::MAX,
            "uninitialized tracker score must be f64::MAX"
        );
        assert_eq!(
            tracker.score(),
            None,
            "uninitialized tracker score() must return None"
        );
    }

    #[test]
    fn tracker_first_sample_initializes_score() {
        let tracker = EwmaLatencyTracker::new();
        let now = Instant::now();
        tracker.update_at(Duration::from_millis(50), now);

        assert!(
            tracker.is_initialized(),
            "tracker must be initialized after update"
        );
        assert_eq!(
            tracker.get_score(),
            50_000.0,
            "first sample must directly set initial score in microseconds"
        );
        assert_eq!(
            tracker.score(),
            Some(50_000.0),
            "score() must return initialized score in microseconds"
        );
    }

    #[test]
    fn tracker_default_and_custom_options() {
        let default_tracker = EwmaLatencyTracker::default();
        assert!(!default_tracker.is_initialized());

        let custom_tracker = EwmaLatencyTracker::with_decay_duration(Duration::ZERO);
        assert_eq!(
            custom_tracker.tau_nanoseconds,
            DEFAULT_DECAY_DURATION.as_nanos() as f64
        );
    }

    #[test]
    fn tracker_fixed_alpha_updates_and_clamping() {
        let tracker = EwmaLatencyTracker::with_fixed_alpha(0.5);
        let now = Instant::now();

        tracker.update_at(Duration::from_millis(100), now);
        assert_eq!(tracker.get_score(), 100_000.0, "initial score is 100ms");

        // Score = 0.5 * 200ms + 0.5 * 100ms = 150ms
        tracker.update_at(Duration::from_millis(200), now);
        assert_eq!(
            tracker.get_score(),
            150_000.0,
            "score with alpha 0.5 must be 150ms"
        );

        // Score = 0.5 * 50ms + 0.5 * 150ms = 100ms
        tracker.update_at(Duration::from_millis(50), now);
        assert_eq!(
            tracker.get_score(),
            100_000.0,
            "score with alpha 0.5 must be 100ms"
        );

        // Clamping check: alpha > 1.0 clamped to 1.0
        let clamped_tracker = EwmaLatencyTracker::with_fixed_alpha(1.5);
        assert_eq!(clamped_tracker.fixed_alpha, Some(1.0));
    }

    #[test]
    fn tracker_time_based_alpha_decay() {
        let tracker = EwmaLatencyTracker::with_decay_duration(Duration::from_secs(10));
        let start = Instant::now();

        tracker.update_at(Duration::from_millis(100), start);
        assert_eq!(tracker.get_score(), 100_000.0);

        // Sample arriving 10 seconds later (delta = tau = 10s):
        // alpha = 1 - e^(-1) = 1 - 0.36787944117 = 0.63212055882
        // New latency = 200_000
        // Expected score = 0.63212055882 * 200_000 + (1 - 0.63212055882) * 100_000 = 163_212.05588
        let ten_seconds_later = start + Duration::from_secs(10);
        tracker.update_at(Duration::from_millis(200), ten_seconds_later);

        let score = tracker.get_score();
        let expected_alpha = 1.0 - (-1.0_f64).exp();
        let expected_score = expected_alpha * 200_000.0 + (1.0 - expected_alpha) * 100_000.0;
        assert!(
            (score - expected_score).abs() < 1e-3,
            "time-decayed score must match decay formula: got {score}, expected {expected_score}"
        );
    }

    #[test]
    fn tracker_concurrent_or_past_samples_preserve_moving_average() {
        let tracker = EwmaLatencyTracker::with_decay_duration(Duration::from_secs(10));
        let start = Instant::now();

        tracker.update_at(Duration::from_millis(100), start);

        // Sample with past or equal timestamp has alpha = 0.0, preserving moving average
        let past = start
            .checked_sub(Duration::from_secs(1))
            .expect("past timestamp");
        tracker.update_at(Duration::from_millis(40), past);

        assert_eq!(
            tracker.get_score(),
            100_000.0,
            "sample with past timestamp must preserve score without erratic jump"
        );
    }

    #[test]
    fn tracker_error_penalty() {
        let tracker = EwmaLatencyTracker::with_fixed_alpha(1.0);
        tracker.record_error();

        assert_eq!(
            tracker.get_score(),
            10_000_000.0,
            "default error penalty must record 10 seconds in microseconds"
        );

        // Convenience update without timestamp
        tracker.update(Duration::from_millis(250));
        assert_eq!(tracker.get_score(), 250_000.0);
    }

    #[test]
    fn registry_invalid_key_returns_defaults() {
        let registry = LatencyRegistry::new();

        // group_uid = 0 is invalid
        assert!(!registry.has_score(Some("db"), 0, "10.0.0.1:15000"));
        assert_eq!(
            registry.get_selection_cost(Some("db"), 0, 0, "10.0.0.1:15000"),
            f64::MAX
        );

        // empty address is invalid
        assert!(!registry.has_score(Some("db"), 100, ""));
        assert_eq!(
            registry.get_selection_cost(Some("db"), 100, 0, ""),
            f64::MAX
        );
    }

    #[test]
    fn registry_untracked_endpoint_selection_costs() {
        let registry = LatencyRegistry::with_options(
            Duration::from_secs(15),
            Duration::from_secs(5),
            Duration::from_millis(10),
        );

        // Untracked endpoint with 0 active requests returns default RTT (10ms = 10,000us)
        let cost_idle = registry.get_selection_cost(Some("db1"), 1, 0, "10.0.0.1:15000");
        assert_eq!(
            cost_idle, 10_000.0,
            "cost for idle untracked endpoint must be default RTT"
        );

        // Untracked endpoint with 2 active requests returns DEFAULT_PENALTY_VALUE + active_requests
        let cost_busy = registry.get_selection_cost(Some("db1"), 1, 2, "10.0.0.1:15000");
        assert_eq!(
            cost_busy,
            DEFAULT_PENALTY_VALUE + 2.0,
            "cost for busy untracked endpoint must include penalty"
        );
    }

    #[test]
    fn registry_recorded_latency_cost_calculation() {
        let registry = LatencyRegistry::new();
        let now = Instant::now();

        // Record 50ms latency (50,000us)
        registry.record_latency_at(
            Some("db1"),
            42,
            "10.0.0.1:15000",
            Duration::from_millis(50),
            now,
        );

        assert!(
            registry.has_score(Some("db1"), 42, "10.0.0.1:15000"),
            "registry must have score"
        );
        assert_eq!(registry.len(), 1, "registry must track 1 key");

        // Selection cost with 0 active requests = 50,000 * 1.0 = 50,000
        let cost_idle = registry.get_selection_cost(Some("db1"), 42, 0, "10.0.0.1:15000");
        assert_eq!(
            cost_idle, 50_000.0,
            "cost with 0 active requests is score * 1.0"
        );

        // Selection cost with 3 active requests = 50,000 * (3 + 1.0) = 200,000
        let cost_busy = registry.get_selection_cost(Some("db1"), 42, 3, "10.0.0.1:15000");
        assert_eq!(
            cost_busy, 200_000.0,
            "cost with 3 active requests is score * 4.0"
        );

        // Test convenience record_latency (uses Instant::now())
        registry.record_latency(Some("db1"), 42, "10.0.0.1:15000", Duration::from_millis(60));
    }

    #[test]
    fn registry_error_recording_and_clearing() {
        let registry = LatencyRegistry::default();
        let now = Instant::now();

        registry.record_error_with_penalty(
            None, // Global scope
            100,
            "10.0.0.2:15000",
            Duration::from_secs(5),
            now,
        );

        assert!(registry.has_score(None, 100, "10.0.0.2:15000"));
        let cost = registry.get_selection_cost(None, 100, 0, "10.0.0.2:15000");
        assert_eq!(
            cost, 5_000_000.0,
            "cost must reflect recorded 5s error penalty"
        );

        // Convenience record_error
        registry.record_error(None, 100, "10.0.0.2:15000");

        // Clear registry
        registry.clear();
        assert_eq!(registry.len(), 0, "registry must be empty after clear");
        assert!(registry.is_empty(), "is_empty must return true after clear");
        assert!(!registry.has_score(None, 100, "10.0.0.2:15000"));

        // Invalid key in record_latency_at / record_error is a harmless no-op
        registry.record_latency_at(None, 0, "", Duration::from_millis(10), now);
        registry.record_error_with_penalty(None, 0, "", Duration::from_secs(1), now);
    }

    #[test]
    fn registry_scope_differentiation() {
        let registry = LatencyRegistry::new();
        let now = Instant::now();

        // Record for database 1
        registry.record_latency_at(
            Some("database-1"),
            10,
            "10.0.0.1:15000",
            Duration::from_millis(20),
            now,
        );

        // Record for database 2 on same endpoint and group
        registry.record_latency_at(
            Some("database-2"),
            10,
            "10.0.0.1:15000",
            Duration::from_millis(80),
            now,
        );

        assert_eq!(registry.len(), 2, "must track 2 separate keys");

        let cost_db1 = registry.get_selection_cost(Some("database-1"), 10, 0, "10.0.0.1:15000");
        let cost_db2 = registry.get_selection_cost(Some("database-2"), 10, 0, "10.0.0.1:15000");

        assert_eq!(cost_db1, 20_000.0, "database-1 score must be 20ms");
        assert_eq!(cost_db2, 80_000.0, "database-2 score must be 80ms");
    }

    #[test]
    fn concurrent_tracker_and_registry_access() {
        let registry = Arc::new(LatencyRegistry::new());
        let num_threads = 8;
        let iterations = 500;
        let barrier = Arc::new(Barrier::new(num_threads));

        thread::scope(|scope| {
            for thread_index in 0..num_threads {
                let registry_clone = Arc::clone(&registry);
                let barrier_clone = Arc::clone(&barrier);

                scope.spawn(move || {
                    barrier_clone.wait();

                    for iteration in 0..iterations {
                        let endpoint = format!("10.0.0.{}:15000", (thread_index + iteration) % 5);
                        let latency = Duration::from_millis(
                            ((thread_index * 10 + iteration) % 100 + 1) as u64,
                        );

                        registry_clone.record_latency(Some("test-db"), 1, &endpoint, latency);

                        let cost = registry_clone.get_selection_cost(
                            Some("test-db"),
                            1,
                            iteration % 3,
                            &endpoint,
                        );
                        assert!(cost > 0.0, "selection cost must be positive");
                        assert!(
                            registry_clone.has_score(Some("test-db"), 1, &endpoint),
                            "score must exist"
                        );
                    }
                });
            }
        });

        assert_eq!(
            registry.len(),
            5,
            "registry must have tracked exactly 5 endpoints"
        );
    }
}
