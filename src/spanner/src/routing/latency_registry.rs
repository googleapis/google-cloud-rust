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
use std::sync::Mutex;
use std::sync::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};
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

/// Default maximum number of tracked endpoint latency entries: 100,000.
pub(crate) const DEFAULT_MAX_TRACKERS: usize = 100_000;

/// Default idle time after which an unaccessed tracker expires: 10 minutes.
pub(crate) const DEFAULT_EXPIRE_AFTER_ACCESS: Duration = Duration::from_secs(10 * 60);

/// Default interval between background opportunistic cleanup sweeps: 1 minute.
pub(crate) const DEFAULT_CLEANUP_INTERVAL: Duration = Duration::from_secs(60);

/// Number of candidate entries sampled for eviction when the registry exceeds capacity.
const EVICTION_SAMPLE_SIZE: usize = 8;

/// Default upper bound on initial pre-allocated capacity for hash maps.
const DEFAULT_INITIAL_CAPACITY_BOUND: usize = 256;

/// Default interval below which consecutive access touches are elided: 1 second (1,000 milliseconds).
const TOUCH_THROTTLE_MILLIS: u64 = 1_000;

/// Registry managing process-local EWMA latency scores across Spanner split replicas and endpoints.
///
/// Tracks round-trip latency per `(database_scope, group_uid, endpoint_address)` tuple, allowing
/// the replica selector to choose the lowest-latency healthy replica for a given database partition or split.
///
/// Enforces bounded memory capacity (default 100,000 trackers) and time-to-idle expiration
/// (default 10 minutes).
#[derive(Debug)]
pub(crate) struct LatencyRegistry {
    trackers: RwLock<HashMap<LatencyKey, RegistryEntry>>,
    decay_duration: Duration,
    error_penalty: Duration,
    default_rtt: Duration,
    max_trackers: usize,
    expire_after_access: Duration,
    cleanup_interval: Duration,
    epoch: Instant,
    last_cleanup_millis: AtomicU64,
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
    /// - 100,000 maximum tracked endpoints
    /// - 10-minute idle tracker expiration
    /// - 1-minute periodic cleanup interval
    pub(crate) fn new() -> Self {
        Self::with_capacity(DEFAULT_MAX_TRACKERS)
    }

    /// Creates a new `LatencyRegistry` with the specified maximum capacity.
    ///
    /// Pre-allocates initial capacity bounded to at most `min(max_trackers, DEFAULT_INITIAL_CAPACITY_BOUND)`
    /// to prevent excessive startup memory consumption.
    pub(crate) fn with_capacity(max_trackers: usize) -> Self {
        Self::with_initial_capacity(
            max_trackers.min(DEFAULT_INITIAL_CAPACITY_BOUND),
            max_trackers,
        )
    }

    /// Creates a new `LatencyRegistry` with an explicit initial pre-allocated capacity
    /// and a maximum entry eviction limit.
    pub(crate) fn with_initial_capacity(initial_capacity: usize, max_trackers: usize) -> Self {
        Self::with_all_options(
            DEFAULT_DECAY_DURATION,
            DEFAULT_ERROR_PENALTY,
            DEFAULT_RTT,
            initial_capacity,
            max_trackers,
            DEFAULT_EXPIRE_AFTER_ACCESS,
            DEFAULT_CLEANUP_INTERVAL,
        )
    }

    /// Creates a new `LatencyRegistry` with custom EWMA decay duration, error penalty, and default RTT,
    /// using default bounds for capacity and expiration.
    pub(crate) fn with_options(
        decay_duration: Duration,
        error_penalty: Duration,
        default_rtt: Duration,
    ) -> Self {
        Self::with_all_options(
            decay_duration,
            error_penalty,
            default_rtt,
            DEFAULT_MAX_TRACKERS.min(DEFAULT_INITIAL_CAPACITY_BOUND),
            DEFAULT_MAX_TRACKERS,
            DEFAULT_EXPIRE_AFTER_ACCESS,
            DEFAULT_CLEANUP_INTERVAL,
        )
    }

    /// Creates a new `LatencyRegistry` with full configuration over all parameters.
    pub(crate) fn with_all_options(
        decay_duration: Duration,
        error_penalty: Duration,
        default_rtt: Duration,
        initial_capacity: usize,
        max_trackers: usize,
        expire_after_access: Duration,
        cleanup_interval: Duration,
    ) -> Self {
        let initial_capacity = initial_capacity.min(max_trackers);
        Self {
            trackers: RwLock::new(HashMap::with_capacity(initial_capacity)),
            decay_duration,
            error_penalty,
            default_rtt,
            max_trackers,
            expire_after_access,
            cleanup_interval,
            epoch: Instant::now(),
            last_cleanup_millis: AtomicU64::new(0),
        }
    }

    /// Returns the maximum capacity of the latency registry.
    pub(crate) fn max_trackers(&self) -> usize {
        self.max_trackers
    }

    /// Returns the configured idle expiration duration.
    pub(crate) fn expire_after_access(&self) -> Duration {
        self.expire_after_access
    }

    /// Returns the configured periodic cleanup sweep interval.
    pub(crate) fn cleanup_interval(&self) -> Duration {
        self.cleanup_interval
    }

    /// Returns whether latency tracking is disabled (`max_trackers == 0`).
    pub(crate) fn is_tracking_disabled(&self) -> bool {
        self.max_trackers == 0
    }

    /// Returns whether a latency score has been recorded for the specified latency key at the current timestamp.
    pub(crate) fn has_score(
        &self,
        database_scope: Option<&str>,
        group_uid: u64,
        endpoint_address: &str,
    ) -> bool {
        self.has_score_at(database_scope, group_uid, endpoint_address, Instant::now())
    }

    /// Returns whether a latency score has been recorded for the specified latency key at the given timestamp.
    /// Performs a zero-allocation borrowed lookup under a shared read lock.
    pub(crate) fn has_score_at(
        &self,
        database_scope: Option<&str>,
        group_uid: u64,
        endpoint_address: &str,
        now: Instant,
    ) -> bool {
        if self.is_tracking_disabled() || group_uid == 0 || endpoint_address.is_empty() {
            return false;
        }

        let database_scope = database_scope.filter(|scope| !scope.is_empty());
        let lookup = LatencyKeyRef {
            database_scope,
            group_uid,
            endpoint_address,
        };

        let now_millis = self.instant_to_millis(now);
        let expire_after_millis = self.expire_after_access.as_millis() as u64;

        let trackers = self
            .trackers
            .read()
            .expect("LatencyRegistry trackers read lock poisoned");

        let Some(entry) = trackers.get(&lookup as &dyn LatencyLookup) else {
            return false;
        };

        if entry.is_expired(now_millis, expire_after_millis) {
            return false;
        }

        entry.touch(now_millis);
        entry.tracker.is_initialized()
    }

    /// Computes the replica selection cost for an endpoint given its active in-flight request count.
    /// Performs a zero-allocation borrowed lookup without allocating memory on the hot path.
    ///
    /// The selection cost is calculated as follows:
    /// 1. If an initialized score exists: `score * (active_requests + 1.0)`
    /// 2. If the endpoint is unmeasured or expired but has in-flight requests: `DEFAULT_PENALTY_VALUE + active_requests`
    ///    to steer traffic away from burdened unknown endpoints.
    /// 3. If the endpoint is unmeasured or expired and idle: `default_rtt_micros`.
    pub(crate) fn selection_cost(
        &self,
        database_scope: Option<&str>,
        group_uid: u64,
        active_requests: usize,
        endpoint_address: &str,
    ) -> f64 {
        self.selection_cost_at(
            database_scope,
            group_uid,
            active_requests,
            endpoint_address,
            Instant::now(),
        )
    }

    /// Alias for [`selection_cost`](Self::selection_cost) matching Spanner router conventions.
    pub(crate) fn get_selection_cost(
        &self,
        database_scope: Option<&str>,
        group_uid: u64,
        active_requests: usize,
        endpoint_address: &str,
    ) -> f64 {
        self.selection_cost(database_scope, group_uid, active_requests, endpoint_address)
    }

    /// Computes the replica selection cost for an endpoint at the given timestamp.
    pub(crate) fn selection_cost_at(
        &self,
        database_scope: Option<&str>,
        group_uid: u64,
        active_requests: usize,
        endpoint_address: &str,
        now: Instant,
    ) -> f64 {
        if group_uid == 0 || endpoint_address.is_empty() {
            return f64::MAX;
        }

        // When latency tracking is disabled, selection cost scales purely with active in-flight
        // requests against the baseline default RTT, providing smooth least-connections balancing.
        if self.is_tracking_disabled() {
            return self.default_rtt.as_micros() as f64 * ((active_requests as f64) + 1.0);
        }

        let database_scope = database_scope.filter(|scope| !scope.is_empty());
        let lookup = LatencyKeyRef {
            database_scope,
            group_uid,
            endpoint_address,
        };

        let now_millis = self.instant_to_millis(now);
        let expire_after_millis = self.expire_after_access.as_millis() as u64;
        let active_multiplier = active_requests as f64 + 1.0;

        {
            let trackers = self
                .trackers
                .read()
                .expect("LatencyRegistry trackers read lock poisoned");

            if let Some(entry) = trackers.get(&lookup as &dyn LatencyLookup)
                && !entry.is_expired(now_millis, expire_after_millis)
            {
                entry.touch(now_millis);
                if let Some(score) = entry.tracker.score() {
                    return score * active_multiplier;
                }
            }
        }

        // If the endpoint has never been measured or has expired, but already has active in-flight requests,
        // penalize it heavily (DEFAULT_PENALTY_VALUE + active_requests) so the replica selector prefers
        // other unburdened or measured endpoints. This prevents a thundering herd / traffic stampede where
        // multiple concurrent requests all route to an unmeasured or potentially slow/unresponsive endpoint
        // before the first probe measurement completes. An idle unmeasured endpoint (active_requests == 0)
        // receives the default RTT to allow exactly one initial probe request to measure it.
        if active_requests > 0 {
            return DEFAULT_PENALTY_VALUE + (active_requests as f64);
        }

        // If the endpoint is unmeasured/expired and idle (active_requests == 0), return default RTT in microseconds.
        self.default_rtt.as_micros() as f64
    }

    /// Alias for [`selection_cost_at`](Self::selection_cost_at) matching Spanner router conventions.
    pub(crate) fn get_selection_cost_at(
        &self,
        database_scope: Option<&str>,
        group_uid: u64,
        active_requests: usize,
        endpoint_address: &str,
        now: Instant,
    ) -> f64 {
        self.selection_cost_at(
            database_scope,
            group_uid,
            active_requests,
            endpoint_address,
            now,
        )
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
        if self.is_tracking_disabled() || group_uid == 0 || endpoint_address.is_empty() {
            return;
        }

        let now_millis = self.instant_to_millis(now);
        self.update_tracker(
            database_scope,
            group_uid,
            endpoint_address,
            now_millis,
            |tracker| tracker.update_at(latency, now),
        );
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
        if self.is_tracking_disabled() || group_uid == 0 || endpoint_address.is_empty() {
            return;
        }

        let now_millis = self.instant_to_millis(now);
        self.update_tracker(
            database_scope,
            group_uid,
            endpoint_address,
            now_millis,
            |tracker| tracker.record_error_at(penalty, now),
        );
    }

    /// Clears all tracked endpoint latency scores and resets lifecycle state.
    pub(crate) fn clear(&self) {
        self.clear_at(Instant::now());
    }

    /// Clears all tracked endpoint latency scores and resets lifecycle state at the given timestamp.
    pub(crate) fn clear_at(&self, now: Instant) {
        let mut trackers = self
            .trackers
            .write()
            .expect("LatencyRegistry trackers write lock poisoned");
        trackers.clear();
        self.last_cleanup_millis
            .store(self.instant_to_millis(now), Ordering::Release);
    }

    /// Explicitly prunes all expired entries from the registry at the given timestamp.
    pub(crate) fn prune_expired(&self, now: Instant) {
        let now_millis = self.instant_to_millis(now);
        let expire_after_millis = self.expire_after_access.as_millis() as u64;
        let mut trackers = self
            .trackers
            .write()
            .expect("LatencyRegistry trackers write lock poisoned");
        trackers.retain(|_, entry| !entry.is_expired(now_millis, expire_after_millis));
        self.last_cleanup_millis
            .store(now_millis, Ordering::Release);
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

    fn instant_to_millis(&self, instant: Instant) -> u64 {
        instant
            .checked_duration_since(self.epoch)
            .map_or(0, |duration| {
                duration.as_millis().min(u64::MAX as u128) as u64
            })
    }

    fn evict_one_candidate(
        trackers: &mut HashMap<LatencyKey, RegistryEntry>,
        now_millis: u64,
        expire_after_millis: u64,
    ) -> bool {
        let mut best_key: Option<&LatencyKey> = None;
        let mut oldest_access = u64::MAX;

        for (candidate_key, entry) in trackers.iter().take(EVICTION_SAMPLE_SIZE) {
            if entry.is_expired(now_millis, expire_after_millis) {
                best_key = Some(candidate_key);
                break;
            }
            let access = entry.last_access_millis.load(Ordering::Acquire);
            if best_key.is_none() || access < oldest_access {
                oldest_access = access;
                best_key = Some(candidate_key);
            }
        }

        let Some(victim_key) = best_key.cloned() else {
            return false;
        };

        trackers.remove(&victim_key);
        true
    }

    fn evict_sample_locked(
        trackers: &mut HashMap<LatencyKey, RegistryEntry>,
        max_trackers: usize,
        now_millis: u64,
        expire_after_millis: u64,
    ) {
        while trackers.len() >= max_trackers {
            if !Self::evict_one_candidate(trackers, now_millis, expire_after_millis) {
                break;
            }
        }
    }

    fn try_claim_cleanup(&self, now_millis: u64) -> bool {
        if self.cleanup_interval.is_zero() || self.expire_after_access.is_zero() {
            return false;
        }
        let last_cleanup = self.last_cleanup_millis.load(Ordering::Acquire);
        let cleanup_interval_millis = self.cleanup_interval.as_millis() as u64;
        if now_millis.saturating_sub(last_cleanup) < cleanup_interval_millis {
            return false;
        }
        self.last_cleanup_millis
            .compare_exchange(
                last_cleanup,
                now_millis,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }

    fn try_update_fast_path(
        &self,
        lookup: &LatencyKeyRef,
        now_millis: u64,
        expire_after_millis: u64,
        update: &impl Fn(&EwmaLatencyTracker),
    ) -> bool {
        let trackers = self
            .trackers
            .read()
            .expect("LatencyRegistry trackers read lock poisoned");
        let Some(entry) = trackers.get(lookup as &dyn LatencyLookup) else {
            return false;
        };
        if entry.is_expired(now_millis, expire_after_millis) {
            return false;
        }
        entry.touch(now_millis);
        update(&entry.tracker);
        true
    }

    fn update_or_insert_entry<F>(
        &self,
        trackers: &mut HashMap<LatencyKey, RegistryEntry>,
        lookup: &LatencyKeyRef,
        now_millis: u64,
        expire_after_millis: u64,
        update: F,
    ) where
        F: FnOnce(&EwmaLatencyTracker),
    {
        if let Some(entry) = trackers.get_mut(lookup as &dyn LatencyLookup) {
            if !entry.is_expired(now_millis, expire_after_millis) {
                entry.touch(now_millis);
                update(&entry.tracker);
                return;
            }
            entry.tracker = EwmaLatencyTracker::with_decay_duration(self.decay_duration);
            entry
                .last_access_millis
                .store(now_millis, Ordering::Release);
            update(&entry.tracker);
            return;
        }

        Self::evict_sample_locked(trackers, self.max_trackers, now_millis, expire_after_millis);

        let tracker = EwmaLatencyTracker::with_decay_duration(self.decay_duration);
        update(&tracker);
        trackers.insert(
            LatencyKey::from(lookup),
            RegistryEntry::new(tracker, now_millis),
        );
    }

    fn update_tracker<F>(
        &self,
        database_scope: Option<&str>,
        group_uid: u64,
        endpoint_address: &str,
        now_millis: u64,
        update: F,
    ) where
        F: Fn(&EwmaLatencyTracker),
    {
        let database_scope = database_scope.filter(|scope| !scope.is_empty());
        let lookup = LatencyKeyRef {
            database_scope,
            group_uid,
            endpoint_address,
        };
        let expire_after_millis = self.expire_after_access.as_millis() as u64;

        let should_cleanup = self.try_claim_cleanup(now_millis);
        if !should_cleanup
            && self.try_update_fast_path(&lookup, now_millis, expire_after_millis, &update)
        {
            return;
        }

        let mut trackers = self
            .trackers
            .write()
            .expect("LatencyRegistry trackers write lock poisoned");

        if should_cleanup {
            trackers.retain(|_, entry| !entry.is_expired(now_millis, expire_after_millis));
        }

        self.update_or_insert_entry(
            &mut trackers,
            &lookup,
            now_millis,
            expire_after_millis,
            update,
        );
    }
}

/// An entry within [`LatencyRegistry`], pairing the latency tracker with an atomic monotonic access timestamp.
#[derive(Debug)]
struct RegistryEntry {
    tracker: EwmaLatencyTracker,
    last_access_millis: AtomicU64,
}

impl RegistryEntry {
    fn new(tracker: EwmaLatencyTracker, access_millis: u64) -> Self {
        Self {
            tracker,
            last_access_millis: AtomicU64::new(access_millis),
        }
    }

    fn touch(&self, access_millis: u64) {
        let last_access = self.last_access_millis.load(Ordering::Relaxed);
        if access_millis.saturating_sub(last_access) >= TOUCH_THROTTLE_MILLIS {
            self.last_access_millis
                .fetch_max(access_millis, Ordering::Release);
        }
    }

    fn is_expired(&self, now_millis: u64, expire_after_millis: u64) -> bool {
        if expire_after_millis == 0 {
            return false;
        }
        let last_access = self.last_access_millis.load(Ordering::Acquire);
        now_millis.saturating_sub(last_access) >= expire_after_millis
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

impl From<&LatencyKeyRef<'_>> for LatencyKey {
    fn from(lookup: &LatencyKeyRef<'_>) -> Self {
        Self {
            database_scope: lookup.database_scope.map(str::to_string),
            group_uid: lookup.group_uid,
            endpoint_address: lookup.endpoint_address.to_string(),
        }
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

/// Sentinel bit pattern indicating that no latency sample or error penalty has been recorded.
///
/// In IEEE 754, `u64::MAX` corresponds to a NaN with all exponent and mantissa bits set.
/// Non-negative microsecond latency values never produce this bit representation.
const UNINITIALIZED_SCORE_BITS: u64 = u64::MAX;

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
    score_bits: AtomicU64,
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
            score_bits: AtomicU64::new(UNINITIALIZED_SCORE_BITS),
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
            score_bits: AtomicU64::new(UNINITIALIZED_SCORE_BITS),
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
        let bits = self.score_bits.load(Ordering::Acquire);
        if bits == UNINITIALIZED_SCORE_BITS {
            return None;
        }
        Some(f64::from_bits(bits))
    }

    /// Returns whether at least one latency sample or error penalty has been recorded.
    pub(crate) fn is_initialized(&self) -> bool {
        self.score_bits.load(Ordering::Acquire) != UNINITIALIZED_SCORE_BITS
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

        let new_score = match *guard {
            None => {
                *guard = Some(EwmaState {
                    score_microseconds: latency_micros,
                    last_updated_at: now,
                });
                latency_micros
            }
            Some(ref mut state) => {
                let alpha = match self.fixed_alpha {
                    Some(fixed) => fixed,
                    None => self.calculate_time_based_alpha(state.last_updated_at, now),
                };

                let updated = alpha * latency_micros + (1.0 - alpha) * state.score_microseconds;
                state.score_microseconds = updated;
                state.last_updated_at = state.last_updated_at.max(now);
                updated
            }
        };

        self.score_bits
            .store(new_score.to_bits(), Ordering::Release);
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
            // If no time has elapsed (now == last_updated_at) or a sample arrived out-of-order
            // from the past (now < last_updated_at), return alpha = 0.0 to ignore the sample
            // and preserve the existing moving average without score distortion.
            return 0.0;
        }

        let delta_nanoseconds = now.saturating_duration_since(last_updated_at).as_nanos() as f64;
        let ratio = delta_nanoseconds / self.tau_nanoseconds;
        // Use -exp_m1(-ratio) to prevent catastrophic floating-point cancellation for small delta_t:
        // 1 - exp(-x) == -(exp(-x) - 1) == -(-x).exp_m1()
        (-(-ratio).exp_m1()).clamp(0.0, 1.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use static_assertions::assert_impl_all;
    use std::sync::Arc;
    use std::sync::Barrier;
    use std::thread;

    #[test]
    fn traits() {
        assert_impl_all!(LatencyRegistry: Send, Sync, Debug);
        assert_impl_all!(RegistryEntry: Send, Sync, Debug);
        assert_impl_all!(EwmaLatencyTracker: Send, Sync, Debug);
        assert_impl_all!(LatencyKey: Send, Sync, Debug, Clone, PartialEq, Eq);
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
        assert!(
            !default_tracker.is_initialized(),
            "default tracker must be uninitialized"
        );

        let custom_tracker = EwmaLatencyTracker::with_decay_duration(Duration::ZERO);
        assert_eq!(
            custom_tracker.tau_nanoseconds,
            DEFAULT_DECAY_DURATION.as_nanos() as f64,
            "custom tracker with zero decay duration must fall back to default decay duration"
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

        // Clamping check: alpha > 1.0 clamped to 1.0, alpha <= 0.0 clamped to f64::MIN_POSITIVE
        let clamped_tracker = EwmaLatencyTracker::with_fixed_alpha(1.5);
        assert_eq!(
            clamped_tracker.fixed_alpha,
            Some(1.0),
            "fixed alpha above 1.0 must clamp to 1.0"
        );

        let clamped_zero = EwmaLatencyTracker::with_fixed_alpha(0.0);
        assert_eq!(
            clamped_zero.fixed_alpha,
            Some(f64::MIN_POSITIVE),
            "zero fixed alpha must clamp to MIN_POSITIVE"
        );

        let clamped_negative = EwmaLatencyTracker::with_fixed_alpha(-0.5);
        assert_eq!(
            clamped_negative.fixed_alpha,
            Some(f64::MIN_POSITIVE),
            "negative fixed alpha must clamp to MIN_POSITIVE"
        );
    }

    #[test]
    fn tracker_time_based_alpha_decay() {
        let tracker = EwmaLatencyTracker::with_decay_duration(Duration::from_secs(10));
        let start = Instant::now();

        tracker.update_at(Duration::from_millis(100), start);
        assert_eq!(
            tracker.get_score(),
            100_000.0,
            "initial score must be 100ms"
        );

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
    fn tracker_past_sample_preserves_moving_average() {
        let tracker = EwmaLatencyTracker::with_decay_duration(Duration::from_secs(10));
        let start = Instant::now();

        tracker.update_at(Duration::from_millis(100), start);
        assert_eq!(
            tracker.get_score(),
            100_000.0,
            "initial score must be 100ms"
        );

        // Sample with concurrent timestamp (now == last_updated_at) uses alpha = 0.0
        // (zero time elapsed), so the existing moving average is preserved without change.
        tracker.update_at(Duration::from_millis(60), start);
        assert_eq!(
            tracker.get_score(),
            100_000.0,
            "sample with identical timestamp must preserve historical moving average"
        );

        // Sample with past timestamp (now < last_updated_at) uses alpha = 0.0 to prevent
        // an out-of-order stale sample from corrupting the accumulated moving average.
        let past = start
            .checked_sub(Duration::from_secs(1))
            .expect("valid past timestamp");
        tracker.update_at(Duration::from_millis(40), past);

        assert_eq!(
            tracker.get_score(),
            100_000.0,
            "sample with past timestamp must preserve historical moving average"
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
        assert_eq!(
            tracker.get_score(),
            250_000.0,
            "score must be updated to 250ms"
        );
    }

    #[test]
    fn registry_invalid_key_returns_defaults() {
        let registry = LatencyRegistry::new();

        // group_uid = 0 is invalid
        assert!(
            !registry.has_score(Some("db"), 0, "10.0.0.1:15000"),
            "group_uid 0 must have no score"
        );
        assert_eq!(
            registry.get_selection_cost(Some("db"), 0, 0, "10.0.0.1:15000"),
            f64::MAX,
            "group_uid 0 selection cost must be MAX"
        );

        // empty address is invalid
        assert!(
            !registry.has_score(Some("db"), 100, ""),
            "empty address must have no score"
        );
        assert_eq!(
            registry.get_selection_cost(Some("db"), 100, 0, ""),
            f64::MAX,
            "empty address selection cost must be MAX"
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

        assert!(
            registry.has_score(None, 100, "10.0.0.2:15000"),
            "entry must have score after record_error"
        );
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
        assert!(
            !registry.has_score(None, 100, "10.0.0.2:15000"),
            "entry must have no score after clear"
        );

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

    #[test]
    fn trackers_expire_after_access_window() {
        let registry = LatencyRegistry::new();
        let start_time = Instant::now();
        let database_scope = Some("projects/p/instances/i/databases/d");
        let endpoint_address = "server-a:1234";

        registry.record_latency_at(
            database_scope,
            101,
            endpoint_address,
            Duration::from_millis(5),
            start_time,
        );

        assert!(
            registry.has_score_at(database_scope, 101, endpoint_address, start_time),
            "score must exist immediately after recording"
        );

        // Advance virtual time beyond the 10-minute expiry window
        let expired_time = start_time + DEFAULT_EXPIRE_AFTER_ACCESS + Duration::from_millis(1);

        assert!(
            !registry.has_score_at(database_scope, 101, endpoint_address, expired_time),
            "tracker must expire after 10-minute access window"
        );

        // Selection cost for expired idle endpoint must fall back to default RTT
        let selection_cost_idle =
            registry.get_selection_cost_at(database_scope, 101, 0, endpoint_address, expired_time);
        assert_eq!(
            selection_cost_idle,
            DEFAULT_RTT.as_micros() as f64,
            "selection cost for expired idle endpoint must be default RTT"
        );

        // Selection cost for expired busy endpoint must include penalty
        let selection_cost_busy =
            registry.get_selection_cost_at(database_scope, 101, 2, endpoint_address, expired_time);
        assert_eq!(
            selection_cost_busy,
            DEFAULT_PENALTY_VALUE + 2.0,
            "selection cost for expired busy endpoint must be penalty value plus active requests"
        );
    }

    #[test]
    fn access_keeps_tracker_alive_within_expiry_window() {
        let registry = LatencyRegistry::new();
        let start_time = Instant::now();
        let database_scope = Some("projects/p/instances/i/databases/d");
        let endpoint_address = "server-b:1234";

        registry.record_latency_at(
            database_scope,
            202,
            endpoint_address,
            Duration::from_millis(7),
            start_time,
        );

        // Advance by half the expiration duration (5 minutes) and touch via cost lookup
        let intermediate_time = start_time + DEFAULT_EXPIRE_AFTER_ACCESS / 2;
        let cost = registry.get_selection_cost_at(
            database_scope,
            202,
            0,
            endpoint_address,
            intermediate_time,
        );
        assert!(
            cost > 0.0,
            "intermediate selection cost must be positive and non-zero"
        );

        // Advance another half expiration duration (total 10 minutes from start, but only 5 from last access)
        let total_ten_minutes = start_time + DEFAULT_EXPIRE_AFTER_ACCESS;
        assert!(
            registry.has_score_at(database_scope, 202, endpoint_address, total_ten_minutes),
            "tracker must remain alive because access at 5 minutes refreshed the expiration window"
        );

        // Advance past the refreshed expiration window (10 minutes after the access at total_ten_minutes)
        let fully_expired_time =
            total_ten_minutes + DEFAULT_EXPIRE_AFTER_ACCESS + Duration::from_millis(1);
        assert!(
            !registry.has_score_at(database_scope, 202, endpoint_address, fully_expired_time),
            "tracker must expire after 10 minutes from last access"
        );
    }

    #[test]
    fn capacity_overflow_evicts_oldest_or_expired() {
        // Create registry with max_trackers = 3
        let max_trackers = 3;
        let registry = LatencyRegistry::with_capacity(max_trackers);
        let start_time = Instant::now();
        let database_scope = Some("test-database");

        // Insert 3 entries with spaced access times
        registry.record_latency_at(
            database_scope,
            1,
            "10.0.0.1:15000",
            Duration::from_millis(10),
            start_time,
        );
        registry.record_latency_at(
            database_scope,
            2,
            "10.0.0.2:15000",
            Duration::from_millis(20),
            start_time + Duration::from_secs(1),
        );
        registry.record_latency_at(
            database_scope,
            3,
            "10.0.0.3:15000",
            Duration::from_millis(30),
            start_time + Duration::from_secs(2),
        );

        assert_eq!(
            registry.len(),
            3,
            "registry must hold exactly 3 entries initially"
        );

        // Insert a 4th entry at start_time + 3s
        registry.record_latency_at(
            database_scope,
            4,
            "10.0.0.4:15000",
            Duration::from_millis(40),
            start_time + Duration::from_secs(3),
        );

        assert!(
            registry.len() <= max_trackers,
            "registry size must not exceed configured capacity"
        );
        assert!(
            registry.has_score_at(
                database_scope,
                4,
                "10.0.0.4:15000",
                start_time + Duration::from_secs(3)
            ),
            "newly inserted entry must exist"
        );
    }

    #[test]
    fn periodic_cleanup_sweeps_expired_entries() {
        let max_trackers = 100;
        let expire_after_access = Duration::from_secs(5);
        let cleanup_interval = Duration::from_secs(1);
        let registry = LatencyRegistry::with_all_options(
            DEFAULT_DECAY_DURATION,
            DEFAULT_ERROR_PENALTY,
            DEFAULT_RTT,
            max_trackers,
            max_trackers,
            expire_after_access,
            cleanup_interval,
        );

        let start_time = Instant::now();
        let database_scope = Some("test-database");

        // Insert 3 entries
        registry.record_latency_at(
            database_scope,
            1,
            "10.0.0.1:15000",
            Duration::from_millis(10),
            start_time,
        );
        registry.record_latency_at(
            database_scope,
            2,
            "10.0.0.2:15000",
            Duration::from_millis(20),
            start_time,
        );
        registry.record_latency_at(
            database_scope,
            3,
            "10.0.0.3:15000",
            Duration::from_millis(30),
            start_time,
        );

        assert_eq!(registry.len(), 3, "must have 3 entries initially");

        // Advance virtual time by 10 seconds (both expiration and cleanup interval elapsed)
        let sweep_time = start_time + Duration::from_secs(10);

        // Recording latency for a new endpoint triggers opportunistic cleanup on the mutation path
        registry.record_latency_at(
            database_scope,
            4,
            "10.0.0.4:15000",
            Duration::from_millis(40),
            sweep_time,
        );

        assert_eq!(
            registry.len(),
            1,
            "all expired entries must be pruned by opportunistic cleanup sweep on mutation"
        );
        assert!(
            registry.has_score_at(database_scope, 4, "10.0.0.4:15000", sweep_time),
            "newly inserted entry must be present"
        );
    }

    #[test]
    fn prune_expired_explicitly_sweeps_expired_entries() {
        let max_trackers = 100;
        let expire_after_access = Duration::from_secs(5);
        let cleanup_interval = Duration::from_secs(1);
        let registry = LatencyRegistry::with_all_options(
            DEFAULT_DECAY_DURATION,
            DEFAULT_ERROR_PENALTY,
            DEFAULT_RTT,
            max_trackers,
            max_trackers,
            expire_after_access,
            cleanup_interval,
        );

        let start_time = Instant::now();
        let database_scope = Some("test-database");

        registry.record_latency_at(
            database_scope,
            1,
            "10.0.0.1:15000",
            Duration::from_millis(10),
            start_time,
        );
        assert_eq!(registry.len(), 1, "must have 1 entry initially");

        let sweep_time = start_time + Duration::from_secs(10);
        registry.prune_expired(sweep_time);
        assert_eq!(
            registry.len(),
            0,
            "all expired entries must be pruned by prune_expired"
        );
    }

    #[test]
    fn evict_sample_prefers_expired_entries() {
        let max_trackers = 2;
        let expire_after_access = Duration::from_secs(10);
        let cleanup_interval = Duration::from_secs(3600);
        let registry = LatencyRegistry::with_all_options(
            DEFAULT_DECAY_DURATION,
            DEFAULT_ERROR_PENALTY,
            DEFAULT_RTT,
            max_trackers,
            max_trackers,
            expire_after_access,
            cleanup_interval,
        );

        let start_time = Instant::now();
        let database_scope = Some("test-database");

        // Insert entry 1 at t0
        registry.record_latency_at(
            database_scope,
            1,
            "10.0.0.1:15000",
            Duration::from_millis(10),
            start_time,
        );

        // Insert entry 2 at t0 + 5s
        registry.record_latency_at(
            database_scope,
            2,
            "10.0.0.2:15000",
            Duration::from_millis(20),
            start_time + Duration::from_secs(5),
        );

        assert_eq!(registry.len(), 2, "must hold 2 entries");

        // At t0 + 12s, entry 1 is expired (12s > 10s), while entry 2 is alive (12s - 5s = 7s < 10s)
        let insert_time = start_time + Duration::from_secs(12);

        // Insert entry 3, forcing eviction of 1 of the 2 entries.
        // The eviction sample must identify entry 1 as expired and evict it instead of the active entry 2.
        registry.record_latency_at(
            database_scope,
            3,
            "10.0.0.3:15000",
            Duration::from_millis(30),
            insert_time,
        );

        assert_eq!(registry.len(), 2, "registry must not exceed capacity");
        assert!(
            registry.has_score_at(database_scope, 3, "10.0.0.3:15000", insert_time),
            "newly inserted entry must be present"
        );
        assert!(
            registry.has_score_at(database_scope, 2, "10.0.0.2:15000", insert_time),
            "active entry must be preserved"
        );
        assert!(
            !registry.has_score_at(database_scope, 1, "10.0.0.1:15000", insert_time),
            "expired entry must have been preferred for eviction"
        );
    }

    #[test]
    fn expired_entry_reactivated_on_mutation() {
        let registry = LatencyRegistry::new();
        let start_time = Instant::now();
        let database_scope = Some("projects/p/instances/i/databases/d");
        let endpoint_address = "server-c:1234";

        registry.record_latency_at(
            database_scope,
            303,
            endpoint_address,
            Duration::from_millis(10),
            start_time,
        );

        // Advance time past expiration window
        let expired_time = start_time + DEFAULT_EXPIRE_AFTER_ACCESS + Duration::from_millis(1);
        assert!(
            !registry.has_score_at(database_scope, 303, endpoint_address, expired_time),
            "entry must be considered expired before reactivation"
        );

        // Mutating the expired entry reactivates it, updating its touched timestamp
        registry.record_latency_at(
            database_scope,
            303,
            endpoint_address,
            Duration::from_millis(25),
            expired_time,
        );

        assert!(
            registry.has_score_at(database_scope, 303, endpoint_address, expired_time),
            "reactivated entry must have valid score at mutation time"
        );
        assert_eq!(registry.len(), 1, "entry count must remain 1");
    }

    #[test]
    fn constructor_capacity_boundary_matrix() {
        // 1. Initial capacity larger than max capacity clamps to max capacity
        let registry_clamped = LatencyRegistry::with_initial_capacity(100, 10);
        assert_eq!(
            registry_clamped.max_trackers(),
            10,
            "max trackers must match configured upper bound"
        );

        // 2. Zero max capacity
        let registry_zero_capacity = LatencyRegistry::with_capacity(0);
        assert_eq!(
            registry_zero_capacity.max_trackers(),
            0,
            "max trackers must be zero"
        );
        assert_eq!(registry_zero_capacity.len(), 0, "initial len must be zero");
        registry_zero_capacity.record_latency(
            Some("test-database"),
            1,
            "10.0.0.1:15000",
            Duration::from_millis(10),
        );
        assert_eq!(
            registry_zero_capacity.len(),
            0,
            "zero capacity registry must not store entries"
        );

        // 3. Zero initial capacity with positive max capacity
        let registry_zero_initial = LatencyRegistry::with_initial_capacity(0, 50);
        assert_eq!(
            registry_zero_initial.max_trackers(),
            50,
            "max trackers must be 50"
        );

        // 4. Extreme initial capacity (usize::MAX) must clamp safely without panicking or OOM
        let registry_extreme = LatencyRegistry::with_initial_capacity(usize::MAX, 100);
        assert_eq!(
            registry_extreme.max_trackers(),
            100,
            "extreme initial capacity must clamp to max_trackers"
        );

        // 5. Pre-allocation threshold verification (DEFAULT_MAX_TRACKERS is bounded to DEFAULT_INITIAL_CAPACITY_BOUND)
        let registry_default = LatencyRegistry::new();
        assert_eq!(
            registry_default.max_trackers(),
            DEFAULT_MAX_TRACKERS,
            "default max trackers must be 100,000"
        );
        assert_eq!(
            registry_default.expire_after_access(),
            DEFAULT_EXPIRE_AFTER_ACCESS,
            "default expire after access must be 10 minutes"
        );
        assert_eq!(
            registry_default.cleanup_interval(),
            DEFAULT_CLEANUP_INTERVAL,
            "default cleanup interval must be 1 minute"
        );

        // 6. Non-default capacity with with_capacity
        let registry_custom_capacity = LatencyRegistry::with_capacity(500);
        assert_eq!(
            registry_custom_capacity.max_trackers(),
            500,
            "max trackers must match custom capacity"
        );

        // 7. Initial capacity with zero max trackers clamps to 0
        let registry_initial_zero_max = LatencyRegistry::with_initial_capacity(50, 0);
        assert_eq!(
            registry_initial_zero_max.max_trackers(),
            0,
            "max trackers must clamp to 0"
        );
    }

    #[test]
    fn clear_resets_all_fields() {
        let registry = LatencyRegistry::new();
        let now = Instant::now();

        registry.record_latency_at(
            Some("database"),
            42,
            "10.0.0.1:15000",
            Duration::from_millis(15),
            now,
        );
        assert_eq!(
            registry.len(),
            1,
            "registry must contain 1 entry before clear"
        );
        assert!(
            !registry.is_empty(),
            "registry must not be empty before clear"
        );

        let clear_time = now + Duration::from_secs(30);
        registry.clear_at(clear_time);

        assert_eq!(registry.len(), 0, "registry len must be 0 after clear");
        assert!(
            registry.is_empty(),
            "registry is_empty must be true after clear"
        );
        assert_eq!(
            registry.last_cleanup_millis.load(Ordering::Acquire),
            30_000,
            "last_cleanup_millis must match clear timestamp"
        );

        // Also verify that calling clear() without arguments updates last_cleanup_millis
        registry.clear();
        assert!(
            registry.last_cleanup_millis.load(Ordering::Acquire) < u64::MAX,
            "last_cleanup_millis must be a valid timestamp after clear"
        );
    }

    #[test]
    fn scope_normalization_empty_and_none() {
        let registry = LatencyRegistry::new();
        let now = Instant::now();

        // Record with Some("")
        registry.record_latency_at(
            Some(""),
            10,
            "10.0.0.1:15000",
            Duration::from_millis(20),
            now,
        );

        // Look up with None - must find the entry recorded with empty scope
        assert!(
            registry.has_score_at(None, 10, "10.0.0.1:15000", now),
            "lookup with None must match entry recorded with empty scope"
        );
        assert_eq!(
            registry.selection_cost_at(None, 10, 0, "10.0.0.1:15000", now),
            20_000.0,
            "selection cost lookup with None must match entry recorded with empty scope"
        );

        // Record with None on the same endpoint and group - updates existing entry in place
        registry.record_latency_at(None, 10, "10.0.0.1:15000", Duration::from_millis(40), now);
        assert_eq!(
            registry.len(),
            1,
            "empty string scope and None must map to the same key"
        );
    }

    #[test]
    fn idiomatic_method_aliases_selection_cost() {
        let registry = LatencyRegistry::new();
        registry.record_latency(Some("db"), 1, "10.0.0.1:15000", Duration::from_millis(25));

        // Exercise selection_cost (without get_ prefix)
        let cost = registry.selection_cost(Some("db"), 1, 0, "10.0.0.1:15000");
        assert_eq!(
            cost, 25_000.0,
            "selection_cost must match recorded EWMA score"
        );

        let cost_at =
            registry.selection_cost_at(Some("db"), 1, 0, "10.0.0.1:15000", Instant::now());
        assert_eq!(
            cost_at, 25_000.0,
            "selection_cost_at must match recorded EWMA score"
        );
    }

    #[test]
    fn disabled_interval_permutations() {
        // cleanup_interval = ZERO, expire_after_access = ZERO (expiration and background cleanup disabled)
        let registry = LatencyRegistry::with_all_options(
            DEFAULT_DECAY_DURATION,
            DEFAULT_ERROR_PENALTY,
            DEFAULT_RTT,
            100,
            100,
            Duration::ZERO,
            Duration::ZERO,
        );

        let start_time = Instant::now();
        registry.record_latency_at(
            Some("db"),
            1,
            "10.0.0.1:15000",
            Duration::from_millis(10),
            start_time,
        );

        // 100 days later: entry must never expire because expire_after_access is ZERO
        let future_time = start_time + Duration::from_secs(100 * 24 * 3600);
        assert!(
            registry.has_score_at(Some("db"), 1, "10.0.0.1:15000", future_time),
            "entry must never expire when expire_after_access is ZERO"
        );

        // prune_expired with expire_after_access == ZERO is a safe no-op
        registry.prune_expired(future_time);
        assert_eq!(
            registry.len(),
            1,
            "prune_expired must not remove entries when expire_after_access is ZERO"
        );
    }

    #[test]
    fn zero_capacity_behavior() {
        let registry = LatencyRegistry::with_capacity(0);
        let now = Instant::now();

        assert!(
            !registry.has_score_at(Some("db"), 1, "10.0.0.1:15000", now),
            "zero-capacity registry must not report having a score"
        );
        assert_eq!(
            registry.selection_cost_at(Some("db"), 1, 0, "10.0.0.1:15000", now),
            DEFAULT_RTT.as_micros() as f64,
            "zero-capacity selection cost with 0 active requests must equal default RTT"
        );
        assert_eq!(
            registry.selection_cost_at(Some("db"), 1, 2, "10.0.0.1:15000", now),
            (DEFAULT_RTT.as_micros() as f64) * 3.0,
            "zero-capacity selection cost with active requests must scale default RTT proportionally"
        );
    }

    #[test]
    fn is_tracking_disabled_accessor() {
        let registry_enabled = LatencyRegistry::with_capacity(10);
        assert!(
            !registry_enabled.is_tracking_disabled(),
            "registry with positive capacity must not be disabled"
        );

        let registry_disabled = LatencyRegistry::with_capacity(0);
        assert!(
            registry_disabled.is_tracking_disabled(),
            "registry with zero capacity must report tracking disabled"
        );
    }

    #[test]
    fn touch_throttling_elides_frequent_updates() {
        let registry = LatencyRegistry::new();
        let initial_time = Instant::now();
        registry.record_latency_at(
            Some("db"),
            1,
            "10.0.0.1:15000",
            Duration::from_millis(10),
            initial_time,
        );

        // Access 500ms later (below TOUCH_THROTTLE_MILLIS = 1,000ms): touch must be throttled
        let short_delay_time = initial_time + Duration::from_millis(500);
        let _ = registry.selection_cost_at(Some("db"), 1, 0, "10.0.0.1:15000", short_delay_time);

        let trackers = registry
            .trackers
            .read()
            .expect("LatencyRegistry trackers read lock must not be poisoned");
        let lookup = LatencyKeyRef {
            database_scope: Some("db"),
            group_uid: 1,
            endpoint_address: "10.0.0.1:15000",
        };
        let entry = trackers
            .get(&lookup as &dyn LatencyLookup)
            .expect("entry must exist in trackers");
        assert_eq!(
            entry.last_access_millis.load(Ordering::Acquire),
            registry.instant_to_millis(initial_time),
            "access within 500ms must be throttled and preserve initial_time"
        );
        drop(trackers);

        // Access 1,500ms later (exceeds TOUCH_THROTTLE_MILLIS): touch must advance
        let long_delay_time = initial_time + Duration::from_millis(1500);
        let _ = registry.selection_cost_at(Some("db"), 1, 0, "10.0.0.1:15000", long_delay_time);

        let trackers = registry
            .trackers
            .read()
            .expect("LatencyRegistry trackers read lock must not be poisoned");
        let entry = trackers
            .get(&lookup as &dyn LatencyLookup)
            .expect("entry must exist in trackers");
        assert_eq!(
            entry.last_access_millis.load(Ordering::Acquire),
            registry.instant_to_millis(long_delay_time),
            "access after 1,500ms must update last_access_millis to long_delay_time"
        );
    }

    #[test]
    fn past_timestamps_clamp_gracefully() {
        let registry = LatencyRegistry::new();
        // Timestamp 10 seconds before registry initialization
        let past_time = Instant::now()
            .checked_sub(Duration::from_secs(10))
            .expect("valid past timestamp");

        registry.record_latency_at(
            Some("db"),
            1,
            "10.0.0.1:15000",
            Duration::from_millis(20),
            past_time,
        );
        assert!(
            registry.has_score_at(Some("db"), 1, "10.0.0.1:15000", past_time),
            "sample with past timestamp must be recorded safely"
        );
    }

    #[test]
    fn multiple_consecutive_evictions_succeed() {
        let registry = LatencyRegistry::with_capacity(3);
        let now = Instant::now();

        // Insert 10 entries into a capacity-3 registry
        for index in 1..=10 {
            registry.record_latency_at(
                Some("db"),
                index,
                &format!("10.0.0.{index}:15000"),
                Duration::from_millis(10),
                now + Duration::from_secs(index),
            );
        }

        assert_eq!(
            registry.len(),
            3,
            "registry must stay bounded at capacity 3 despite 10 sequential insertions"
        );
    }

    #[test]
    fn uninitialized_tracker_in_registry_falls_back_to_untracked_cost() {
        let registry = LatencyRegistry::new();
        let key = LatencyKey {
            database_scope: None,
            group_uid: 1,
            endpoint_address: "server-uninit:1234".to_string(),
        };
        registry
            .trackers
            .write()
            .expect("LatencyRegistry trackers write lock poisoned")
            .insert(key, RegistryEntry::new(EwmaLatencyTracker::new(), 0));

        let cost_idle =
            registry.selection_cost_at(None, 1, 0, "server-uninit:1234", Instant::now());
        assert_eq!(
            cost_idle,
            DEFAULT_RTT.as_micros() as f64,
            "uninitialized tracker with 0 active requests must fall back to default RTT"
        );

        let cost_active =
            registry.selection_cost_at(None, 1, 2, "server-uninit:1234", Instant::now());
        assert_eq!(
            cost_active,
            DEFAULT_PENALTY_VALUE + 2.0,
            "uninitialized tracker with active requests must fall back to default unmeasured penalty"
        );
    }

    #[test]
    fn evict_sample_locked_zero_capacity_evicts_all_and_breaks() {
        let mut trackers = HashMap::new();
        let key = LatencyKey {
            database_scope: None,
            group_uid: 1,
            endpoint_address: "server-evict:1234".to_string(),
        };
        trackers.insert(key, RegistryEntry::new(EwmaLatencyTracker::new(), 100));

        LatencyRegistry::evict_sample_locked(&mut trackers, 0, 1000, 5000);
        assert!(
            trackers.is_empty(),
            "evict_sample_locked with 0 max_trackers must evict all entries"
        );

        // Calling on an already empty map breaks immediately without panic
        LatencyRegistry::evict_sample_locked(&mut trackers, 0, 1000, 5000);
        assert!(
            trackers.is_empty(),
            "evict_sample_locked on empty map must break safely"
        );
    }

    #[test]
    fn evict_one_candidate_empty_map_returns_false() {
        let mut trackers = HashMap::new();
        let evicted = LatencyRegistry::evict_one_candidate(&mut trackers, 1000, 5000);
        assert!(
            !evicted,
            "evict_one_candidate on empty map must return false"
        );
    }

    #[test]
    fn expired_entry_reactivated_in_place_without_periodic_cleanup() {
        // Configure expire_after_access = 5s, cleanup_interval = 60s.
        // Entries expire well before the 60s periodic cleanup interval elapses.
        let registry = LatencyRegistry::with_all_options(
            Duration::from_secs(10),
            DEFAULT_ERROR_PENALTY,
            DEFAULT_RTT,
            100,
            100,
            Duration::from_secs(5),
            Duration::from_secs(60),
        );
        let start_time = Instant::now();
        let database_scope = Some("projects/p/instances/i/databases/d");
        let endpoint_address = "server-reactivate:1234";

        // Initial measurement at start_time
        registry.record_latency_at(
            database_scope,
            100,
            endpoint_address,
            Duration::from_millis(50),
            start_time,
        );

        // At start_time + 8s, the entry is expired (> 5s expiration window),
        // but the 60s cleanup interval has not elapsed, so periodic cleanup does not run.
        let reactivate_time = start_time + Duration::from_secs(8);
        assert!(
            !registry.has_score_at(database_scope, 100, endpoint_address, reactivate_time),
            "entry must be considered expired prior to reactivation"
        );

        // Reactivating via mutation should detect expiration in try_update_fast_path,
        // fall back to update_or_insert_entry, and reactivate the entry in place.
        registry.record_latency_at(
            database_scope,
            100,
            endpoint_address,
            Duration::from_millis(20),
            reactivate_time,
        );

        assert!(
            registry.has_score_at(database_scope, 100, endpoint_address, reactivate_time),
            "entry must be reactivated with valid score"
        );
        assert_eq!(
            registry.get_selection_cost_at(
                database_scope,
                100,
                0,
                endpoint_address,
                reactivate_time
            ),
            20_000.0,
            "reactivated tracker must reflect the new measurement"
        );
    }
}
