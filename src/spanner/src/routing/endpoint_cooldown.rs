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
use rand::random_range;
use std::collections::{HashMap, HashSet};
use std::sync::RwLock;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

/// Default initial cooldown duration when an endpoint first returns `RESOURCE_EXHAUSTED`.
const DEFAULT_INITIAL_COOLDOWN: Duration = Duration::from_secs(10);

/// Default maximum cooldown duration cap for repeatedly failing endpoints.
const DEFAULT_MAX_COOLDOWN: Duration = Duration::from_secs(60);

/// Default time window after which consecutive failure counts are reset if no further failures occur.
const DEFAULT_RESET_AFTER: Duration = Duration::from_secs(600);

/// Minimum cooldown floor applied when a server-recommended retry delay is present.
const DEFAULT_MIN_HINTED_COOLDOWN: Duration = Duration::from_millis(100);

/// Maximum client-side floor cap for hinted overload backoff.
const DEFAULT_MAX_HINTED_CLIENT_FLOOR: Duration = Duration::from_secs(2);

/// Maximum jitter cap applied to hinted overload backoff.
const DEFAULT_MAX_HINTED_JITTER: Duration = Duration::from_millis(500);

/// Number of consecutive successful RPCs required to decrement the failure tier.
const DEFAULT_SUCCESSES_PER_REPAIR: usize = 3;

/// Maximum failure tier cap for hinted escalation.
const DEFAULT_MAX_FAILURE_TIER: usize = 6;

/// Maximum fallback cooldown duration (10 minutes) used to cap arithmetic overflow when computing cooldown deadlines.
/// This matches `DEFAULT_RESET_AFTER` so an endpoint is never blacklisted longer than the failure reset window.
const MAX_OVERFLOW_FALLBACK_COOLDOWN: Duration = DEFAULT_RESET_AFTER;

/// Tracks endpoint-scoped overload and availability cooldowns triggered by `RESOURCE_EXHAUSTED`
/// and `UNAVAILABLE` gRPC errors.
///
/// When a routed Spanner tablet endpoint returns `RESOURCE_EXHAUSTED` or `UNAVAILABLE`, it is
/// placed on a probabilistic cooldown with exponential backoff and jitter, preventing the client
/// from routing new traffic to an overloaded or unreachable tablet node.
///
/// # Fallback Routing Note
/// If all routed tablet replicas for a key range are cooling down or unavailable, the client
/// falls back directly to the default gateway connection, ensuring that traffic continues
/// to make progress without blocking on overloaded endpoints.
#[derive(Debug)]
pub(crate) struct EndpointCooldownTracker {
    initial_cooldown: Duration,
    max_cooldown: Duration,
    reset_after: Duration,
    tracked_entry_count: AtomicUsize,
    state: RwLock<HashMap<String, EndpointCooldownState>>,
}

impl Default for EndpointCooldownTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl EndpointCooldownTracker {
    /// Creates a new `EndpointCooldownTracker` with default durations:
    /// - 10 seconds initial cooldown
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
    ///
    /// `max_cooldown` is defensively clamped to be at least `initial_cooldown` to guard against
    /// inverted configuration parameters.
    pub(crate) fn with_options(
        initial_cooldown: Duration,
        max_cooldown: Duration,
        reset_after: Duration,
    ) -> Self {
        Self {
            initial_cooldown,
            max_cooldown: max_cooldown.max(initial_cooldown),
            reset_after,
            tracked_entry_count: AtomicUsize::new(0),
            state: RwLock::new(HashMap::new()),
        }
    }

    /// Returns `true` if the given endpoint address is currently on active cooldown.
    pub(crate) fn is_cooling_down(&self, endpoint: &str) -> bool {
        self.is_cooling_down_at(endpoint, Instant::now())
    }

    /// Checks if the endpoint is cooling down relative to a specific reference timestamp.
    ///
    /// If an endpoint entry has passed its cooldown duration and remained idle beyond
    /// the reset window (`reset_after`), or has been repaired to 0 failures and passed its
    /// cooldown, it is pruned from the map to restore the zero-lock fast path.
    pub(crate) fn is_cooling_down_at(&self, endpoint: &str, now: Instant) -> bool {
        if endpoint.is_empty() || self.tracked_entry_count.load(Ordering::Acquire) == 0 {
            return false;
        }

        {
            let guard = self
                .state
                .read()
                .expect("EndpointCooldownTracker read lock poisoned");
            let Some(entry) = guard.get(endpoint) else {
                return false;
            };
            if entry.is_cooling_down(now) {
                return true;
            }
            if !entry.is_idle(now, self.reset_after) {
                return false;
            }
        }

        let mut guard = self
            .state
            .write()
            .expect("EndpointCooldownTracker write lock poisoned");
        if let Some(entry) = guard.get(endpoint) {
            // Re-validate cooling down status under write lock to eliminate TOCTOU race
            // if a failure was recorded concurrently between dropping read lock and acquiring write lock.
            if entry.is_cooling_down(now) {
                return true;
            }
            if entry.is_idle(now, self.reset_after) {
                guard.remove(endpoint);
                self.tracked_entry_count
                    .store(guard.len(), Ordering::Release);
            }
        }

        false
    }

    /// Records an RPC failure for the endpoint, optionally incorporating a server-recommended retry delay.
    ///
    /// If the `status_code` is [`Code::ResourceExhausted`], places the endpoint on cooldown
    /// in the overload escalation lane (using hinted or unhinted backoff) and returns `Some(cooldown_duration)`.
    /// If the `status_code` is [`Code::Unavailable`], places the endpoint on cooldown
    /// in the availability escalation lane (using unhinted exponential backoff with 10s initial cooldown)
    /// and returns `Some(cooldown_duration)`.
    /// Overload and availability failures use separate escalation counters to ensure short load-shed
    /// hints do not weaken transport protection, and transport failures do not inflate overload tiers.
    /// For any other error code, does nothing and returns `None`.
    pub(crate) fn record_error_with_delay(
        &self,
        endpoint: &str,
        status_code: Code,
        server_retry_delay: Option<Duration>,
    ) -> Option<Duration> {
        self.record_error_with_delay_at(endpoint, status_code, server_retry_delay, Instant::now())
    }

    /// Records an RPC failure for the endpoint at a specific reference timestamp,
    /// optionally incorporating a server-recommended retry delay.
    pub(crate) fn record_error_with_delay_at(
        &self,
        endpoint: &str,
        status_code: Code,
        server_retry_delay: Option<Duration>,
        now: Instant,
    ) -> Option<Duration> {
        if endpoint.is_empty() {
            return None;
        }
        match status_code {
            Code::ResourceExhausted | Code::Unavailable => {
                Some(self.record_failure_internal(endpoint, status_code, server_retry_delay, now))
            }
            _ => None,
        }
    }

    /// Records an RPC failure for the endpoint without a server retry delay hint.
    pub(crate) fn record_error(&self, endpoint: &str, status_code: Code) -> Option<Duration> {
        self.record_error_with_delay(endpoint, status_code, None)
    }

    /// Records an overload failure for the endpoint at the current timestamp, applying an optional
    /// server-recommended retry delay hint.
    pub(crate) fn record_failure_with_delay(
        &self,
        endpoint: &str,
        server_retry_delay: Option<Duration>,
    ) -> Duration {
        self.record_failure_with_delay_at(endpoint, server_retry_delay, Instant::now())
    }

    /// Records an overload failure for the endpoint at the current timestamp without a server delay hint.
    pub(crate) fn record_failure(&self, endpoint: &str) -> Duration {
        self.record_failure_with_delay(endpoint, None)
    }

    /// Records an overload failure for the endpoint at a specific reference timestamp with an
    /// optional server retry delay hint, returning the applied cooldown duration.
    pub(crate) fn record_failure_with_delay_at(
        &self,
        endpoint: &str,
        server_retry_delay: Option<Duration>,
        now: Instant,
    ) -> Duration {
        self.record_failure_internal(endpoint, Code::ResourceExhausted, server_retry_delay, now)
    }

    /// Records an overload failure at a specific reference timestamp without a server delay hint.
    pub(crate) fn record_failure_at(&self, endpoint: &str, now: Instant) -> Duration {
        self.record_failure_with_delay_at(endpoint, None, now)
    }

    /// Records a transport availability failure (`UNAVAILABLE`) at the current timestamp,
    /// returning the applied cooldown duration.
    pub(crate) fn record_unavailable_failure(&self, endpoint: &str) -> Duration {
        self.record_unavailable_failure_at(endpoint, Instant::now())
    }

    /// Records a transport availability failure (`UNAVAILABLE`) at a specific reference timestamp,
    /// returning the applied cooldown duration.
    pub(crate) fn record_unavailable_failure_at(&self, endpoint: &str, now: Instant) -> Duration {
        self.record_failure_internal(endpoint, Code::Unavailable, None, now)
    }

    fn apply_failure(
        &self,
        state: &mut EndpointCooldownState,
        status_code: Code,
        server_retry_delay: Option<Duration>,
        now: Instant,
    ) -> Duration {
        match status_code {
            Code::ResourceExhausted => {
                let failures = state.overload.next_failure_tier(now, self.reset_after);
                let cooldown = match server_retry_delay {
                    Some(delay) => Self::calculate_hinted_cooldown(failures, delay),
                    None => self.calculate_unhinted_cooldown(failures),
                };
                state
                    .overload
                    .record_failure(failures, cooldown, now, self.reset_after);
            }
            Code::Unavailable => {
                let failures = state.unavailable.next_failure_tier(now, self.reset_after);
                let cooldown = self.calculate_unhinted_cooldown(failures);
                state
                    .unavailable
                    .record_failure(failures, cooldown, now, self.reset_after);
            }
            _ => return Duration::ZERO,
        }
        state.successes_toward_repair = 0;

        let effective_cooldown_until = state
            .overload
            .cooldown_until
            .max(state.unavailable.cooldown_until);
        effective_cooldown_until.saturating_duration_since(now)
    }

    fn record_failure_internal(
        &self,
        endpoint: &str,
        status_code: Code,
        server_retry_delay: Option<Duration>,
        now: Instant,
    ) -> Duration {
        if endpoint.is_empty() {
            return Duration::ZERO;
        }

        let mut guard = self
            .state
            .write()
            .expect("EndpointCooldownTracker write lock poisoned");

        // `guard` is the tracker's internal map of endpoints with recorded failures (not the cluster's endpoints).
        // - `Some(existing)`: The endpoint already has a failure entry in this tracker from a prior failure.
        //   We update the existing entry in-place under the write lock to avoid re-allocation and re-hashing.
        if let Some(existing) = guard.get_mut(endpoint) {
            let mut state = if existing.is_idle(now, self.reset_after) {
                EndpointCooldownState::empty(now)
            } else {
                *existing
            };
            let applied_cooldown =
                self.apply_failure(&mut state, status_code, server_retry_delay, now);
            if applied_cooldown != Duration::ZERO {
                *existing = state;
            }
            return applied_cooldown;
        }

        // - `None`: This endpoint has experienced its first recorded failure (no entry exists yet in the
        //   cooldown tracker). We initialize a fresh failure state and insert it into the map.
        let mut state = EndpointCooldownState::empty(now);
        let applied_cooldown = self.apply_failure(&mut state, status_code, server_retry_delay, now);
        if applied_cooldown != Duration::ZERO {
            guard.insert(endpoint.to_string(), state);
            self.tracked_entry_count
                .store(guard.len(), Ordering::Release);
        }
        applied_cooldown
    }

    /// Records a successful RPC completion to the specified endpoint, advancing repair progress.
    ///
    /// Every 3 consecutive successful RPCs decrements both failure tiers by 1.
    /// When both failure tiers reach 0 and the endpoint is not cooling down, its state entry is pruned.
    pub(crate) fn record_success(&self, endpoint: &str) {
        self.record_success_at(endpoint, Instant::now());
    }

    /// Records a successful RPC completion at a specific reference timestamp.
    pub(crate) fn record_success_at(&self, endpoint: &str, now: Instant) {
        if endpoint.is_empty() || self.tracked_entry_count.load(Ordering::Acquire) == 0 {
            return;
        }

        // Fast path: avoid acquiring an exclusive write lock for healthy endpoints not in the tracker.
        {
            let guard = self
                .state
                .read()
                .expect("EndpointCooldownTracker read lock poisoned");
            if !guard.contains_key(endpoint) {
                return;
            }
        }

        let mut guard = self
            .state
            .write()
            .expect("EndpointCooldownTracker write lock poisoned");

        let Some(entry) = guard.get_mut(endpoint) else {
            return;
        };

        // If idle beyond reset window or already fully repaired and past cooldown, prune immediately.
        if entry.is_idle(now, self.reset_after) {
            guard.remove(endpoint);
            self.tracked_entry_count
                .store(guard.len(), Ordering::Release);
            return;
        }

        if entry.record_success() && entry.is_idle(now, self.reset_after) {
            guard.remove(endpoint);
            self.tracked_entry_count
                .store(guard.len(), Ordering::Release);
        }
    }

    /// Calculates a hinted cooldown when a server-recommended retry delay is present.
    ///
    /// The calculation follows the Spanner client location-aware routing overload specification:
    /// 1. Server Floor: Respects the server's delay hint, bounded by a minimum floor (100ms)
    ///    so even zero or sub-millisecond hints enforce a brief backoff.
    /// 2. Client Escalation Floor: Applies progressive exponential backoff (`100ms * 2^(failures - 1)`),
    ///    capped at 2 seconds (`DEFAULT_MAX_HINTED_CLIENT_FLOOR`), ensuring that repeated overload
    ///    failures escalate client backoff even if the server keeps returning small hints.
    /// 3. Additive Jitter: Adds randomized jitter between 0 and 25% of the base delay (`base / 4`),
    ///    capped at 500ms (`DEFAULT_MAX_HINTED_JITTER`). Jitter is strictly additive (`base + jitter`)
    ///    to guarantee that the client never retries before the server's requested delay, while
    ///    desynchronizing concurrent client retries to prevent thundering-herd spikes on the server.
    fn calculate_hinted_cooldown(
        consecutive_failures: usize,
        server_retry_delay: Duration,
    ) -> Duration {
        let server_floor = server_retry_delay.max(DEFAULT_MIN_HINTED_COOLDOWN);
        let shift = consecutive_failures
            .saturating_sub(1)
            .min(DEFAULT_MAX_FAILURE_TIER) as u32;
        let client_floor = DEFAULT_MIN_HINTED_COOLDOWN
            .saturating_mul(1u32 << shift)
            .min(DEFAULT_MAX_HINTED_CLIENT_FLOOR);
        let base = server_floor.max(client_floor);

        // Additive jitter: up to 25% of base (base / 4), capped at DEFAULT_MAX_HINTED_JITTER (500ms).
        let jitter_limit = (base / 4).min(DEFAULT_MAX_HINTED_JITTER);
        let jitter_limit_millis = u64::try_from(jitter_limit.as_millis()).unwrap_or(u64::MAX);
        if jitter_limit_millis == 0 {
            return base;
        }
        let jitter_millis = random_range(0..=jitter_limit_millis);
        base.saturating_add(Duration::from_millis(jitter_millis))
    }

    /// Calculates an unhinted cooldown using exponential backoff with jitter in `[cooldown / 2, cooldown]`.
    ///
    /// Logic:
    /// 1. Exponential Backoff: Scales `initial_cooldown` (default 10s) by `2^(consecutive_failures - 1)`.
    ///    The shift is capped at 31 to prevent integer overflow in `1u32 << shift`.
    /// 2. Cooldown Cap: Caps the base duration at `max_cooldown` (default 60s).
    /// 3. Half-Jitter Floor: Jitters uniformly in `[capped_cooldown / 2, capped_cooldown]`.
    ///    Unlike full jitter in `[0, capped_cooldown]`, the half-jitter lower bound prevents
    ///    unlucky retries from selecting a near-zero backoff on heavily overloaded endpoints.
    fn calculate_unhinted_cooldown(&self, consecutive_failures: usize) -> Duration {
        let shift = consecutive_failures.saturating_sub(1).min(31) as u32;
        let base_cooldown = self.initial_cooldown.saturating_mul(1u32 << shift);
        let capped_cooldown = base_cooldown.min(self.max_cooldown);

        let millis = u64::try_from(capped_cooldown.as_millis()).unwrap_or(u64::MAX);
        if millis == 0 {
            return Duration::ZERO;
        }
        let floor_millis = (millis / 2).max(1);
        let jittered_millis = random_range(floor_millis..=millis);
        Duration::from_millis(jittered_millis)
    }

    /// Removes entries from the tracker that have expired their cooldown and reset window.
    pub(crate) fn clear_expired(&self) {
        self.clear_expired_at(Instant::now());
    }

    /// Removes expired entries relative to the specified timestamp.
    pub(crate) fn clear_expired_at(&self, now: Instant) {
        if self.tracked_entry_count.load(Ordering::Acquire) == 0 {
            return;
        }
        {
            let guard = self
                .state
                .read()
                .expect("EndpointCooldownTracker read lock poisoned");
            let has_expired = guard
                .values()
                .any(|entry| entry.is_idle(now, self.reset_after));
            if !has_expired {
                return;
            }
        }

        let mut guard = self
            .state
            .write()
            .expect("EndpointCooldownTracker write lock poisoned");
        guard.retain(|_, entry| !entry.is_idle(now, self.reset_after));
        self.tracked_entry_count
            .store(guard.len(), Ordering::Release);
    }

    /// Clears all tracked endpoint cooldowns.
    pub(crate) fn clear(&self) {
        let mut guard = self
            .state
            .write()
            .expect("EndpointCooldownTracker write lock poisoned");
        guard.clear();
        self.tracked_entry_count.store(0, Ordering::Release);
    }

    /// Returns the number of endpoints currently tracked in the state map.
    pub(crate) fn len(&self) -> usize {
        self.tracked_entry_count.load(Ordering::Acquire)
    }

    /// Returns `true` if no endpoints are currently tracked.
    pub(crate) fn is_empty(&self) -> bool {
        self.tracked_entry_count.load(Ordering::Acquire) == 0
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

    /// Creates a new empty request-scoped endpoint exclusion list with the specified initial capacity.
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self {
            excluded: HashSet::with_capacity(capacity),
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

/// Internal state for a single failure escalation lane (overload or availability).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FailureLane {
    /// Number of consecutive failures recorded within the reset window.
    failures: usize,
    /// Absolute time until which this endpoint is cooling down in this lane.
    cooldown_until: Instant,
    /// Timestamp of the most recent failure in this lane.
    last_failure_at: Option<Instant>,
}

impl FailureLane {
    /// Creates a fresh empty lane with cooldown deadline initialized to `now`.
    fn empty(now: Instant) -> Self {
        Self {
            failures: 0,
            cooldown_until: now,
            last_failure_at: None,
        }
    }

    /// Returns the next failure tier for this lane based on elapsed time since `last_failure_at`.
    #[inline]
    fn next_failure_tier(&self, now: Instant, reset_after: Duration) -> usize {
        next_failure_tier(self.failures, self.last_failure_at, now, reset_after)
    }

    /// Records a failure in this lane, updating consecutive failures, monotonic deadline, and timestamp.
    fn record_failure(
        &mut self,
        failures: usize,
        cooldown: Duration,
        now: Instant,
        reset_after: Duration,
    ) {
        let new_deadline = compute_cooldown_deadline(now, cooldown);
        let previous_deadline = match self.last_failure_at {
            Some(timestamp) if now.saturating_duration_since(timestamp) < reset_after => {
                self.cooldown_until
            }
            _ => now,
        };
        self.failures = failures;
        self.cooldown_until = previous_deadline.max(new_deadline);
        self.last_failure_at = Some(
            self.last_failure_at
                .map_or(now, |previous| previous.max(now)),
        );
    }

    /// Returns `true` if this lane is currently cooling down relative to `now`.
    #[inline]
    fn is_cooling_down(&self, now: Instant) -> bool {
        now < self.cooldown_until
    }

    /// Returns `true` if this lane is idle: cooldown deadline has passed and either failures are 0
    /// or inactivity exceeds `reset_after`.
    #[inline]
    fn is_idle(&self, now: Instant, reset_after: Duration) -> bool {
        if self.is_cooling_down(now) {
            return false;
        }
        self.failures == 0
            || self
                .last_failure_at
                .is_none_or(|timestamp| now.saturating_duration_since(timestamp) >= reset_after)
    }

    /// Decrements consecutive failures by 1 toward failure tier repair.
    #[inline]
    fn repair(&mut self) {
        self.failures = self.failures.saturating_sub(1);
    }
}

/// Internal state recorded for an endpoint currently on overload or availability cooldown.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct EndpointCooldownState {
    /// Overload failure escalation lane (`RESOURCE_EXHAUSTED`).
    overload: FailureLane,
    /// Transport availability failure escalation lane (`UNAVAILABLE`).
    unavailable: FailureLane,
    /// Number of consecutive successful RPCs recorded toward failure tier repair.
    successes_toward_repair: usize,
}

impl EndpointCooldownState {
    /// Creates a fresh empty state with timestamps initialized to `now`.
    fn empty(now: Instant) -> Self {
        Self {
            overload: FailureLane::empty(now),
            unavailable: FailureLane::empty(now),
            successes_toward_repair: 0,
        }
    }

    /// Returns `true` if this endpoint is currently cooling down relative to `now`.
    #[inline]
    fn is_cooling_down(&self, now: Instant) -> bool {
        self.overload.is_cooling_down(now) || self.unavailable.is_cooling_down(now)
    }

    /// Returns `true` if this entry is idle and eligible for pruning: both cooldown deadlines
    /// have passed, and both failure lanes are idle.
    #[inline]
    fn is_idle(&self, now: Instant, reset_after: Duration) -> bool {
        self.overload.is_idle(now, reset_after) && self.unavailable.is_idle(now, reset_after)
    }

    /// Records a successful RPC toward repairing failure tiers for an endpoint.
    /// Returns `true` if a repair step was triggered.
    #[inline]
    fn record_success(&mut self) -> bool {
        self.successes_toward_repair = self.successes_toward_repair.saturating_add(1);
        if self.successes_toward_repair >= DEFAULT_SUCCESSES_PER_REPAIR {
            self.overload.repair();
            self.unavailable.repair();
            self.successes_toward_repair = 0;
            true
        } else {
            false
        }
    }
}

fn next_failure_tier(
    previous_failures: usize,
    last_failure_at: Option<Instant>,
    now: Instant,
    reset_after: Duration,
) -> usize {
    match last_failure_at {
        Some(timestamp) if now.saturating_duration_since(timestamp) < reset_after => {
            previous_failures
                .saturating_add(1)
                .min(DEFAULT_MAX_FAILURE_TIER)
        }
        _ => 1,
    }
}

fn compute_cooldown_deadline(now: Instant, cooldown: Duration) -> Instant {
    now.checked_add(cooldown)
        .or_else(|| now.checked_add(MAX_OVERFLOW_FALLBACK_COOLDOWN))
        .unwrap_or(now)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fmt::Debug;
    use std::sync::Barrier;
    use std::thread;

    impl EndpointCooldownTracker {
        fn consecutive_failures(&self, endpoint: &str) -> usize {
            let guard = self
                .state
                .read()
                .expect("EndpointCooldownTracker read lock poisoned");
            guard.get(endpoint).map_or(0, |entry| {
                entry.overload.failures.max(entry.unavailable.failures)
            })
        }

        fn overload_failures(&self, endpoint: &str) -> usize {
            let guard = self
                .state
                .read()
                .expect("EndpointCooldownTracker read lock poisoned");
            guard
                .get(endpoint)
                .map_or(0, |entry| entry.overload.failures)
        }

        fn unavailable_failures(&self, endpoint: &str) -> usize {
            let guard = self
                .state
                .read()
                .expect("EndpointCooldownTracker read lock poisoned");
            guard
                .get(endpoint)
                .map_or(0, |entry| entry.unavailable.failures)
        }

        fn successes_toward_repair(&self, endpoint: &str) -> usize {
            let guard = self
                .state
                .read()
                .expect("EndpointCooldownTracker read lock poisoned");
            guard
                .get(endpoint)
                .map_or(0, |entry| entry.successes_toward_repair)
        }
    }

    #[test]
    fn traits() {
        static_assertions::assert_impl_all!(EndpointCooldownTracker: Send, Sync, Debug);
        static_assertions::assert_impl_all!(
            EndpointExclusionList: Send,
            Sync,
            Debug,
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
        for failure_index in 0..10 {
            let duration =
                tracker.record_failure_at("ep-cap", now + Duration::from_millis(failure_index));
            assert!(
                duration <= Duration::from_millis(500),
                "duration {duration:?} exceeded max cap 500ms on attempt {failure_index}"
            );
            if failure_index >= 3 {
                assert!(
                    duration >= Duration::from_millis(250),
                    "duration {duration:?} under capped floor 250ms on attempt {failure_index}"
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
    fn cooldown_tracker_triggers_on_resource_exhausted_and_unavailable() {
        let tracker = EndpointCooldownTracker::new();

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
        assert_eq!(
            tracker.record_error("ep-1", Code::NotFound),
            None,
            "NOT_FOUND should not trigger cooldown"
        );
        assert!(tracker.is_empty(), "tracker should remain empty");

        let unavailable_cooldown = tracker.record_error("ep-1", Code::Unavailable);
        assert!(
            unavailable_cooldown.is_some(),
            "UNAVAILABLE must trigger cooldown"
        );
        assert_eq!(
            tracker.len(),
            1,
            "tracker should have 1 entry after UNAVAILABLE"
        );

        let exhausted_cooldown = tracker.record_error("ep-2", Code::ResourceExhausted);
        assert!(
            exhausted_cooldown.is_some(),
            "RESOURCE_EXHAUSTED must trigger cooldown"
        );
        assert_eq!(
            tracker.len(),
            2,
            "tracker should have 2 entries after RESOURCE_EXHAUSTED"
        );
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

        thread::scope(|scope| {
            for thread_index in 0..10 {
                let tracker_reference = &tracker;
                scope.spawn(move || {
                    let endpoint = format!("endpoint-{}", thread_index % 3);
                    tracker_reference.record_failure(&endpoint);
                    let _ = tracker_reference.is_cooling_down(&endpoint);
                    let _ = tracker_reference.len();
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
    fn exclusion_list_with_capacity() {
        let mut list = EndpointExclusionList::with_capacity(8);
        assert!(
            list.is_empty(),
            "newly allocated exclusion list should be empty"
        );
        assert_eq!(list.len(), 0, "exclusion list length should be 0");

        list.exclude("10.0.0.1:15000");
        assert_eq!(list.len(), 1, "exclusion list length should be 1");
        assert!(
            list.is_excluded("10.0.0.1:15000"),
            "excluded endpoint must be present"
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
    fn cooldown_tracker_with_options_defensive_clamping() {
        let tracker = EndpointCooldownTracker::with_options(
            Duration::from_secs(10),
            Duration::from_secs(2), // Smaller than initial_cooldown
            Duration::from_secs(60),
        );
        let now = Instant::now();
        let cooldown = tracker.record_failure_at("ep-clamped", now);
        assert!(
            cooldown >= Duration::from_secs(5) && cooldown <= Duration::from_secs(10),
            "cooldown {cooldown:?} must respect half-jitter bounds [5s, 10s] with max_cooldown clamped to initial_cooldown"
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
    fn cooldown_tracker_clear_expired_prunes_repaired_entry_before_reset_window() {
        let now = Instant::now();
        let tracker = EndpointCooldownTracker::with_options(
            Duration::from_millis(200),
            Duration::from_millis(200),
            Duration::from_secs(600),
        );

        tracker.record_failure_at("ep-repair", now);
        assert_eq!(tracker.len(), 1, "tracker should have 1 entry");

        // Record 3 successes while still within cooldown window (< 100ms half-jitter floor) to repair failure tier to 0
        tracker.record_success_at("ep-repair", now + Duration::from_millis(10));
        tracker.record_success_at("ep-repair", now + Duration::from_millis(20));
        tracker.record_success_at("ep-repair", now + Duration::from_millis(30));

        // Entry is repaired (consecutive_failures == 0), but still cooling down at 30ms (< 100ms..=200ms)
        assert_eq!(tracker.len(), 1, "entry remains tracked while cooling down");

        // At now + 300ms, cooldown has elapsed (<= 200ms) but reset window (600s) has not.
        // clear_expired_at must prune the fully repaired entry immediately.
        tracker.clear_expired_at(now + Duration::from_millis(300));
        assert!(
            tracker.is_empty(),
            "repaired entry past cooldown should be pruned by clear_expired_at before reset window"
        );
    }

    #[test]
    fn cooldown_tracker_concurrent_failures_track_all_endpoints() {
        let tracker = EndpointCooldownTracker::new();
        let num_threads = 10;
        let iterations_per_thread = 50;
        let barrier = Barrier::new(num_threads);

        thread::scope(|scope| {
            for thread_index in 0..num_threads {
                let tracker_reference = &tracker;
                let barrier_reference = &barrier;
                scope.spawn(move || {
                    barrier_reference.wait();
                    let endpoint = format!("endpoint-atomic-{thread_index}");
                    for _ in 0..iterations_per_thread {
                        tracker_reference.record_failure(&endpoint);
                    }
                });
            }
        });

        assert_eq!(
            tracker.len(),
            num_threads,
            "all concurrent endpoints must be tracked in state"
        );
        for thread_index in 0..num_threads {
            let endpoint = format!("endpoint-atomic-{thread_index}");
            assert_eq!(
                tracker.consecutive_failures(&endpoint),
                DEFAULT_MAX_FAILURE_TIER,
                "each endpoint must reach max failure tier 6"
            );
        }
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

    #[test]
    fn hinted_resource_exhausted_honors_server_delay() {
        let now = Instant::now();
        let tracker = EndpointCooldownTracker::new();
        let server_retry_delay = Duration::from_millis(500);

        // Server delay is 500ms > initial client floor (100ms).
        // Base is 500ms. Jitter is in [0, min(125ms, 500ms)] = [0, 125ms].
        // Cooldown duration must be in [500ms, 625ms].
        let duration =
            tracker.record_failure_with_delay_at("endpoint-hinted", Some(server_retry_delay), now);

        assert!(
            duration >= Duration::from_millis(500),
            "hinted cooldown duration {duration:?} must be >= server floor 500ms"
        );
        assert!(
            duration <= Duration::from_millis(625),
            "hinted cooldown duration {duration:?} must be <= base + jitter limit (625ms)"
        );
        assert!(
            tracker.is_cooling_down_at("endpoint-hinted", now),
            "endpoint must be cooling down immediately after hinted failure"
        );
        assert!(
            tracker.is_cooling_down_at("endpoint-hinted", now + Duration::from_millis(499)),
            "endpoint must still be cooling down at 499ms"
        );
        assert!(
            !tracker.is_cooling_down_at("endpoint-hinted", now + Duration::from_millis(700)),
            "endpoint must not be cooling down after cooldown expires"
        );
    }

    #[test]
    fn zero_retry_delay_uses_minimum_hinted_cooldown() {
        let now = Instant::now();
        let tracker = EndpointCooldownTracker::new();

        // When server retry delay is 0, minimum hinted floor 100ms applies.
        // Base is 100ms. Jitter is in [0, 25ms].
        // Cooldown duration must be in [100ms, 125ms].
        let duration =
            tracker.record_failure_with_delay_at("endpoint-zero-hint", Some(Duration::ZERO), now);

        assert!(
            duration >= Duration::from_millis(100),
            "zero hint must apply minimum floor 100ms, got {duration:?}"
        );
        assert!(
            duration <= Duration::from_millis(125),
            "zero hint cooldown duration {duration:?} must not exceed 125ms"
        );
    }

    #[test]
    fn hinted_resource_exhausted_client_escalation_cap() {
        let now = Instant::now();
        let tracker = EndpointCooldownTracker::new();
        let small_server_delay = Duration::from_millis(50);

        // Tier 1: client_floor = 100ms, jitter in [0, 25ms], duration in [100ms, 125ms]
        let duration1 = tracker.record_failure_with_delay_at(
            "endpoint-escalate",
            Some(small_server_delay),
            now,
        );
        assert!(
            duration1 >= Duration::from_millis(100) && duration1 <= Duration::from_millis(125),
            "tier 1 cooldown {duration1:?} expected in [100ms, 125ms]"
        );
        assert_eq!(
            tracker.consecutive_failures("endpoint-escalate"),
            1,
            "consecutive failures should be 1"
        );

        // Tier 2: client_floor = 200ms, jitter in [0, 50ms], duration in [200ms, 250ms]
        let duration2 = tracker.record_failure_with_delay_at(
            "endpoint-escalate",
            Some(small_server_delay),
            now + Duration::from_millis(10),
        );
        assert!(
            duration2 >= Duration::from_millis(200) && duration2 <= Duration::from_millis(250),
            "tier 2 cooldown {duration2:?} expected in [200ms, 250ms]"
        );
        assert_eq!(
            tracker.consecutive_failures("endpoint-escalate"),
            2,
            "consecutive failures should be 2"
        );

        // Escalate up to tier 10 (which caps at tier 6 / 2s client floor).
        for failure_index in 3..=10 {
            let duration = tracker.record_failure_with_delay_at(
                "endpoint-escalate",
                Some(small_server_delay),
                now + Duration::from_millis(failure_index * 10),
            );
            assert!(
                duration <= Duration::from_millis(2500),
                "hinted cooldown {duration:?} at attempt {failure_index} must not exceed 2s floor + 500ms max jitter"
            );
            if failure_index >= 6 {
                assert!(
                    duration >= Duration::from_millis(2000),
                    "hinted cooldown {duration:?} at attempt {failure_index} must meet 2s client floor"
                );
            }
        }
        assert_eq!(
            tracker.consecutive_failures("endpoint-escalate"),
            DEFAULT_MAX_FAILURE_TIER,
            "consecutive failures should cap at tier 6"
        );
    }

    #[test]
    fn unhinted_resource_exhausted_keeps_long_backoff() {
        let now = Instant::now();
        let tracker = EndpointCooldownTracker::new();

        // Without server delay hint, initial cooldown is 10s with jitter in [5s, 10s].
        let duration = tracker.record_failure_with_delay_at("endpoint-unhinted", None, now);
        assert!(
            duration >= Duration::from_secs(5),
            "unhinted cooldown {duration:?} must be >= 5s jitter floor"
        );
        assert!(
            duration <= Duration::from_secs(10),
            "unhinted cooldown {duration:?} must be <= 10s base cooldown"
        );
    }

    #[test]
    fn record_success_repairs_failure_tier() {
        let now = Instant::now();
        let tracker = EndpointCooldownTracker::new();

        // Record 3 consecutive failures.
        tracker.record_failure_at("endpoint-repair", now);
        tracker.record_failure_at("endpoint-repair", now + Duration::from_millis(10));
        tracker.record_failure_at("endpoint-repair", now + Duration::from_millis(20));

        assert_eq!(
            tracker.consecutive_failures("endpoint-repair"),
            3,
            "consecutive failures should be 3"
        );
        assert_eq!(
            tracker.successes_toward_repair("endpoint-repair"),
            0,
            "successes toward repair should be 0"
        );

        // First 2 successes advance repair progress without decrementing tier yet.
        tracker.record_success_at("endpoint-repair", now + Duration::from_millis(30));
        assert_eq!(
            tracker.consecutive_failures("endpoint-repair"),
            3,
            "failures should still be 3 after 1 success"
        );
        assert_eq!(
            tracker.successes_toward_repair("endpoint-repair"),
            1,
            "successes toward repair should be 1"
        );

        tracker.record_success_at("endpoint-repair", now + Duration::from_millis(40));
        assert_eq!(
            tracker.consecutive_failures("endpoint-repair"),
            3,
            "failures should still be 3 after 2 successes"
        );
        assert_eq!(
            tracker.successes_toward_repair("endpoint-repair"),
            2,
            "successes toward repair should be 2"
        );

        // 3rd success decrements failure tier from 3 to 2 and resets repair counter.
        tracker.record_success_at("endpoint-repair", now + Duration::from_millis(50));
        assert_eq!(
            tracker.consecutive_failures("endpoint-repair"),
            2,
            "failures should decrement to 2 after 3 successes"
        );
        assert_eq!(
            tracker.successes_toward_repair("endpoint-repair"),
            0,
            "successes toward repair should reset to 0"
        );

        // 3 more successes decrement failure tier from 2 to 1.
        for index in 1..=3 {
            tracker.record_success_at(
                "endpoint-repair",
                now + Duration::from_millis(50 + index * 10),
            );
        }
        assert_eq!(
            tracker.consecutive_failures("endpoint-repair"),
            1,
            "failures should decrement to 1 after 6 total successes"
        );

        // 3 more successes decrement failure tier to 0. When past cooldown, entry is removed.
        let future = now + Duration::from_secs(100);
        for index in 1..=3 {
            tracker.record_success_at(
                "endpoint-repair",
                future + Duration::from_millis(index * 10),
            );
        }
        assert_eq!(
            tracker.consecutive_failures("endpoint-repair"),
            0,
            "failures should be 0 and entry pruned"
        );
        assert!(
            tracker.is_empty(),
            "tracker should be empty after endpoint fully repaired and expired"
        );
    }

    #[test]
    fn failure_resets_success_repair_progress() {
        let now = Instant::now();
        let tracker = EndpointCooldownTracker::new();

        tracker.record_failure_at("endpoint-reset-progress", now);
        tracker.record_success_at("endpoint-reset-progress", now + Duration::from_millis(10));
        tracker.record_success_at("endpoint-reset-progress", now + Duration::from_millis(20));

        assert_eq!(
            tracker.successes_toward_repair("endpoint-reset-progress"),
            2,
            "successes toward repair should be 2"
        );

        // An intervening failure resets successes_toward_repair back to 0.
        tracker.record_failure_at("endpoint-reset-progress", now + Duration::from_millis(30));
        assert_eq!(
            tracker.consecutive_failures("endpoint-reset-progress"),
            2,
            "consecutive failures should increment to 2"
        );
        assert_eq!(
            tracker.successes_toward_repair("endpoint-reset-progress"),
            0,
            "successes toward repair should reset to 0 on failure"
        );
    }

    #[test]
    fn subsequent_failure_never_shortens_active_cooldown() {
        let now = Instant::now();
        let tracker = EndpointCooldownTracker::with_options(
            Duration::from_secs(10),
            Duration::from_secs(10),
            Duration::from_secs(60),
        );

        // First failure sets cooldown_until to at least now + 5s (half jitter floor).
        tracker.record_failure_at("endpoint-monotonic", now);
        assert!(
            tracker.is_cooling_down_at("endpoint-monotonic", now + Duration::from_secs(4)),
            "endpoint must be cooling down at +4s"
        );

        // Second failure with a short 100ms server hint at now + 1s.
        // The resulting cooldown (100ms-125ms) would finish around now + 1.125s,
        // but it must NOT shorten the existing deadline at now + 5s.
        let returned_duration = tracker.record_failure_with_delay_at(
            "endpoint-monotonic",
            Some(Duration::from_millis(100)),
            now + Duration::from_secs(1),
        );
        assert!(
            returned_duration >= Duration::from_secs(4),
            "returned duration {returned_duration:?} must reflect the remaining active monotonic cooldown (>= 4s)"
        );

        assert!(
            tracker.is_cooling_down_at("endpoint-monotonic", now + Duration::from_secs(4)),
            "endpoint must still be cooling down at +4s; cooldown deadline must be monotonic"
        );
    }

    #[test]
    fn empty_endpoint_address_is_ignored() {
        let now = Instant::now();
        let tracker = EndpointCooldownTracker::new();

        assert_eq!(
            tracker.record_error("", Code::ResourceExhausted),
            None,
            "empty address record_error for ResourceExhausted should return None"
        );
        assert_eq!(
            tracker.record_error("", Code::Unavailable),
            None,
            "empty address record_error for Unavailable should return None"
        );
        assert_eq!(
            tracker.record_error_with_delay(
                "",
                Code::ResourceExhausted,
                Some(Duration::from_secs(5))
            ),
            None,
            "empty address record_error_with_delay should return None"
        );
        assert_eq!(
            tracker.record_error_with_delay_at(
                "",
                Code::ResourceExhausted,
                Some(Duration::from_secs(5)),
                now
            ),
            None,
            "empty address record_error_with_delay_at should return None"
        );
        assert_eq!(
            tracker.record_failure(""),
            Duration::ZERO,
            "empty address failure should return zero duration"
        );
        assert_eq!(
            tracker.record_failure_with_delay("", Some(Duration::from_secs(5))),
            Duration::ZERO,
            "empty address failure with delay should return zero duration"
        );
        assert_eq!(
            tracker.record_failure_at("", now),
            Duration::ZERO,
            "empty address failure at now should return zero duration"
        );
        tracker.record_success("");
        tracker.record_success_at("", now);

        assert!(
            !tracker.is_cooling_down(""),
            "empty address should never be reported as cooling down"
        );
        assert!(
            !tracker.is_cooling_down_at("", now),
            "empty address at now should never be cooling down"
        );
        assert!(tracker.is_empty(), "tracker must remain empty");
    }

    #[test]
    fn is_cooling_down_prunes_idle_entries_past_reset_window() {
        let now = Instant::now();
        let tracker = EndpointCooldownTracker::with_options(
            Duration::from_secs(10),
            Duration::from_secs(10),
            Duration::from_secs(60),
        );

        tracker.record_failure_at("ep-idle", now);
        assert_eq!(tracker.len(), 1, "tracker should have 1 entry");

        // At now + 15s: cooldown (<=10s) expired, but within 60s reset window.
        let within_reset = now + Duration::from_secs(15);
        assert!(
            !tracker.is_cooling_down_at("ep-idle", within_reset),
            "cooldown should have expired"
        );
        assert_eq!(
            tracker.len(),
            1,
            "entry must be retained while within reset window"
        );

        // At now + 65s: past both cooldown and 60s reset window.
        let past_reset = now + Duration::from_secs(65);
        assert!(
            !tracker.is_cooling_down_at("ep-idle", past_reset),
            "cooldown expired"
        );
        assert!(
            tracker.is_empty(),
            "idle entry must be pruned on is_cooling_down check past reset window"
        );
    }

    #[test]
    fn reset_after_window_does_not_carry_over_stale_deadline() {
        let now = Instant::now();
        let reset_after = Duration::from_secs(60);
        let tracker = EndpointCooldownTracker::with_options(
            Duration::from_secs(10),
            Duration::from_secs(10),
            reset_after,
        );

        // First failure establishes an initial cooldown.
        tracker.record_failure_at("ep-stale", now);

        // Failure occurs well after the reset window with a small 100ms server hint.
        let long_after = now + reset_after + Duration::from_secs(10);
        let duration = tracker.record_failure_with_delay_at(
            "ep-stale",
            Some(Duration::from_millis(100)),
            long_after,
        );

        assert!(
            duration <= Duration::from_millis(125),
            "after reset window, old cooldown deadline must not be carried over; expected <= 125ms, got {duration:?}"
        );
        assert_eq!(
            tracker.consecutive_failures("ep-stale"),
            1,
            "failure count should reset to 1 after reset window"
        );
    }

    #[test]
    fn record_success_prunes_fully_repaired_entry_after_cooldown() {
        let now = Instant::now();
        let tracker = EndpointCooldownTracker::new();

        // 1 failure
        tracker.record_failure_at("ep-repair-prune", now);

        // 3 successes repair the failure tier to 0 while cooldown is still active.
        for index in 1..=3 {
            tracker.record_success_at("ep-repair-prune", now + Duration::from_millis(index * 10));
        }
        assert_eq!(
            tracker.consecutive_failures("ep-repair-prune"),
            0,
            "failure tier repaired to 0"
        );
        assert_eq!(tracker.len(), 1, "entry retained while cooldown is active");

        // Once cooldown expires (e.g. +20s), the next success should prune the entry immediately.
        let after_cooldown = now + Duration::from_secs(20);
        tracker.record_success_at("ep-repair-prune", after_cooldown);
        assert!(
            tracker.is_empty(),
            "entry must be pruned on next success once repaired to 0 and past cooldown"
        );
    }

    #[test]
    fn cooldown_tracker_unavailable_ignores_subsecond_delay_hint() {
        let tracker = EndpointCooldownTracker::new();
        let delay_hint = Duration::from_millis(300);
        let cooldown = tracker
            .record_error_with_delay("ep-unavail-hinted", Code::Unavailable, Some(delay_hint))
            .expect("Unavailable error should trigger cooldown");

        // UNAVAILABLE uses the unhinted exponential backoff lane (5s..=10s initial backoff with half-jitter)
        // and ignores any sub-second retry hints, preventing short load-shed hints from weakening protection
        // for an unavailable endpoint.
        assert!(
            cooldown >= Duration::from_secs(5) && cooldown <= Duration::from_secs(10),
            "cooldown for UNAVAILABLE must ignore subsecond hint and use unhinted lane 5s..=10s; got {cooldown:?}"
        );
        assert!(
            tracker.is_cooling_down("ep-unavail-hinted"),
            "endpoint must be cooling down"
        );
    }

    #[test]
    fn cooldown_tracker_resource_exhausted_with_server_retry_delay_honors_hint() {
        let tracker = EndpointCooldownTracker::new();
        let delay_hint = Duration::from_millis(300);
        let cooldown = tracker
            .record_error_with_delay(
                "ep-exhausted-hinted",
                Code::ResourceExhausted,
                Some(delay_hint),
            )
            .expect("ResourceExhausted error with retry delay should trigger cooldown");

        assert!(
            cooldown >= delay_hint && cooldown <= Duration::from_millis(400),
            "cooldown for RESOURCE_EXHAUSTED with delay hint must honor hint; expected 300..=375ms, got {cooldown:?}"
        );
        assert!(
            tracker.is_cooling_down("ep-exhausted-hinted"),
            "endpoint must be cooling down"
        );
    }

    #[test]
    fn cooldown_tracker_convenience_methods_using_current_time() {
        let tracker = EndpointCooldownTracker::new();
        let endpoint = "ep-convenience";

        assert!(
            !tracker.is_cooling_down(endpoint),
            "untracked endpoint must not be cooling down"
        );

        let delay = tracker.record_failure_with_delay(endpoint, Some(Duration::from_millis(150)));
        assert!(
            delay >= Duration::from_millis(150),
            "applied delay should be at least server hint"
        );
        assert!(
            tracker.is_cooling_down(endpoint),
            "endpoint must be cooling down after failure recorded"
        );

        // Record a success at the current time
        tracker.record_success(endpoint);

        // Clear expired relative to now (cooldown is active, so entry is retained)
        tracker.clear_expired();
        assert_eq!(
            tracker.len(),
            1,
            "entry should be retained while cooling down"
        );

        // Clear all
        tracker.clear();
        assert!(tracker.is_empty(), "tracker should be empty after clear()");
    }

    #[test]
    fn dual_failure_lanes_separate_overload_from_unavailable_escalation() {
        let tracker = EndpointCooldownTracker::new();
        let endpoint = "ep-dual-lanes";
        let base_now = Instant::now();

        // 1. Record 6 consecutive RESOURCE_EXHAUSTED errors with 50ms delay hints.
        // In the overload lane, this escalates overload_failures to 6 (the max tier).
        let delay_hint = Duration::from_millis(50);
        for index in 0..6 {
            let timestamp = base_now + Duration::from_millis(index * 100);
            let cooldown = tracker.record_error_with_delay_at(
                endpoint,
                Code::ResourceExhausted,
                Some(delay_hint),
                timestamp,
            );
            assert!(
                cooldown.is_some(),
                "RESOURCE_EXHAUSTED failure {index} should trigger a cooldown"
            );
        }

        assert_eq!(
            tracker.overload_failures(endpoint),
            6,
            "overload lane should escalate to max failure tier 6"
        );
        assert_eq!(
            tracker.unavailable_failures(endpoint),
            0,
            "unavailable lane must remain at 0 failures despite overload errors"
        );

        // 2. Now simulate a transport failure (UNAVAILABLE).
        // If failure tiers were unified, this would apply Tier 6 (30s..=60s).
        // With dual failure lanes, this is the 1st unavailable failure,
        // so it must use Tier 1 backoff (5s..=10s unhinted backoff).
        let unavailable_timestamp = base_now + Duration::from_secs(1);
        let unavail_cooldown = tracker
            .record_error_with_delay_at(endpoint, Code::Unavailable, None, unavailable_timestamp)
            .expect("UNAVAILABLE failure should trigger cooldown");

        assert_eq!(
            tracker.unavailable_failures(endpoint),
            1,
            "unavailable lane must record exactly 1 failure"
        );
        assert_eq!(
            tracker.overload_failures(endpoint),
            6,
            "overload lane must retain its failure tier"
        );
        assert!(
            unavail_cooldown >= Duration::from_secs(5)
                && unavail_cooldown <= Duration::from_secs(10),
            "first UNAVAILABLE failure must use Tier 1 backoff (5s..=10s), but got {unavail_cooldown:?}"
        );

        // 3. Monotonicity: a subsequent short-hinted overload error must not shorten
        // an active multi-second transport cooldown.
        let sub_second_now = unavailable_timestamp + Duration::from_millis(500);
        let effective_cooldown = tracker
            .record_error_with_delay_at(
                endpoint,
                Code::ResourceExhausted,
                Some(Duration::from_millis(50)),
                sub_second_now,
            )
            .expect("RESOURCE_EXHAUSTED failure should be recorded");

        // The remaining transport cooldown from unavailable_timestamp is at least 5s - 500ms = 4.5s.
        assert!(
            effective_cooldown >= Duration::from_millis(4500),
            "effective cooldown must respect active transport deadline and not be truncated by short hint: got {effective_cooldown:?}"
        );

        // 4. Test repair: 3 consecutive successes decrement BOTH lanes.
        let repair_now = unavailable_timestamp + Duration::from_secs(15);
        tracker.record_success_at(endpoint, repair_now);
        tracker.record_success_at(endpoint, repair_now);
        assert_eq!(
            tracker.overload_failures(endpoint),
            6,
            "failures should not decrement before 3 consecutive successes"
        );
        assert_eq!(
            tracker.unavailable_failures(endpoint),
            1,
            "failures should not decrement before 3 consecutive successes"
        );

        tracker.record_success_at(endpoint, repair_now);
        assert_eq!(
            tracker.overload_failures(endpoint),
            5,
            "3 consecutive successes should decrement overload failures by 1"
        );
        assert_eq!(
            tracker.unavailable_failures(endpoint),
            0,
            "3 consecutive successes should decrement unavailable failures to 0"
        );
    }

    #[test]
    fn cooldown_tracker_record_unavailable_failure_convenience() {
        let tracker = EndpointCooldownTracker::new();
        let endpoint = "ep-unavail-conv";
        let cooldown = tracker.record_unavailable_failure(endpoint);
        assert!(
            cooldown >= Duration::from_secs(5) && cooldown <= Duration::from_secs(10),
            "initial unavailable failure must apply 5s..=10s cooldown, got {cooldown:?}"
        );
        assert_eq!(
            tracker.unavailable_failures(endpoint),
            1,
            "unavailable lane should record 1 failure"
        );
        assert_eq!(
            tracker.overload_failures(endpoint),
            0,
            "overload lane should have 0 failures"
        );
    }

    #[test]
    fn out_of_order_failure_timestamps_preserve_monotonic_timestamp_and_deadline() {
        let tracker = EndpointCooldownTracker::new();
        let endpoint = "ep-out-of-order";
        let base_now = Instant::now();

        // 1. Record a failure at a later timestamp (base_now + 2s)
        let later_timestamp = base_now + Duration::from_secs(2);
        tracker.record_error_with_delay_at(
            endpoint,
            Code::ResourceExhausted,
            Some(Duration::from_millis(200)),
            later_timestamp,
        );

        let guard = tracker
            .state
            .read()
            .expect("EndpointCooldownTracker read lock poisoned");
        let initial_entry = guard
            .get(endpoint)
            .copied()
            .expect("entry must exist after recording failure");
        drop(guard);

        assert_eq!(
            initial_entry.overload.last_failure_at,
            Some(later_timestamp),
            "last failure timestamp should be later_timestamp"
        );

        // 2. Record an out-of-order failure with an earlier timestamp (base_now + 1s)
        let earlier_timestamp = base_now + Duration::from_secs(1);
        tracker.record_error_with_delay_at(
            endpoint,
            Code::ResourceExhausted,
            Some(Duration::from_millis(200)),
            earlier_timestamp,
        );

        let guard = tracker
            .state
            .read()
            .expect("EndpointCooldownTracker read lock poisoned");
        let updated_entry = guard
            .get(endpoint)
            .copied()
            .expect("entry must exist after recording failure");
        drop(guard);

        // The timestamp must NOT have regressed to earlier_timestamp!
        assert_eq!(
            updated_entry.overload.last_failure_at,
            Some(later_timestamp),
            "failure timestamp must advance monotonically and never regress to an earlier timestamp"
        );
        assert!(
            updated_entry.overload.cooldown_until >= initial_entry.overload.cooldown_until,
            "deadline must not regress"
        );
    }
}
