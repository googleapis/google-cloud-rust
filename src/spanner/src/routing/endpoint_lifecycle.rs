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

//! Endpoint lifecycle management for location-aware routing.
//!
//! Tracks server node connections, manages background health checking and channel warmup,
//! evicts stale or persistently failing endpoints, and shuts down idle channels that have
//! received no real application traffic within the configured timeout window.

use crate::routing::connection_cache::ConnectionCache;
use gaxi::options::ClientConfig;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::RwLock;
use std::sync::Weak;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use std::time::Instant;
use tokio::runtime::Handle;
use tokio::task::JoinHandle;
use tokio::time::{MissedTickBehavior, interval};
use tracing::debug;

/// Default probe interval: 60 seconds.
const DEFAULT_PROBE_INTERVAL: Duration = Duration::from_secs(60);

/// Default idle eviction threshold: 30 minutes without real traffic.
const DEFAULT_IDLE_EVICTION_DURATION: Duration = Duration::from_secs(1800);

/// Throttle threshold between traffic timestamp updates for an endpoint to reduce write-lock contention.
const TRAFFIC_RECORD_THROTTLE: Duration = Duration::from_secs(10);

/// Maximum observed `TRANSIENT_FAILURE` probes before evicting an endpoint.
const MAX_TRANSIENT_FAILURE_COUNT: usize = 3;

/// Manages the lifecycle of location-aware routing endpoints.
///
/// Responsible for:
/// 1. Creating and tracking lifecycle states for active replica tablet server addresses.
/// 2. Tracking real application traffic per endpoint to refresh idle timers.
/// 3. Periodically probing endpoint health and evicting endpoints that stay in `TRANSIENT_FAILURE`
///    for 3 consecutive probe cycles.
/// 4. Evicting idle endpoints that have not received real traffic for 30 minutes.
/// 5. Promptly evicting stale endpoints that are no longer referenced by any active tablet.
#[derive(Debug)]
pub(crate) struct EndpointLifecycleManager {
    connection_cache: Arc<ConnectionCache>,
    client_config: Option<Arc<ClientConfig>>,
    state: Arc<RwLock<LifecycleManagerState>>,
    transient_failure_count: Arc<AtomicUsize>,
    probe_interval: Duration,
    idle_eviction_duration: Duration,
    default_endpoint_address: String,
}

impl EndpointLifecycleManager {
    /// Creates a new `EndpointLifecycleManager` with default probe and idle eviction settings.
    pub(crate) fn new(connection_cache: Arc<ConnectionCache>) -> Self {
        Self::with_options(
            connection_cache,
            DEFAULT_PROBE_INTERVAL,
            DEFAULT_IDLE_EVICTION_DURATION,
        )
    }

    /// Creates a new `EndpointLifecycleManager` with a [`ClientConfig`] for background connection
    /// recreation and pre-warming.
    pub(crate) fn with_client_config(
        connection_cache: Arc<ConnectionCache>,
        client_config: ClientConfig,
    ) -> Self {
        let mut manager = Self::new(connection_cache);
        manager.client_config = Some(Arc::new(client_config));
        manager
    }

    /// Creates a new `EndpointLifecycleManager` with custom probe interval and idle eviction duration.
    pub(crate) fn with_options(
        connection_cache: Arc<ConnectionCache>,
        probe_interval: Duration,
        idle_eviction_duration: Duration,
    ) -> Self {
        let default_endpoint_address = connection_cache.default_connection().address().to_string();
        Self {
            connection_cache,
            client_config: None,
            state: Arc::new(RwLock::new(LifecycleManagerState::default())),
            transient_failure_count: Arc::new(AtomicUsize::new(0)),
            probe_interval,
            idle_eviction_duration,
            default_endpoint_address,
        }
    }

    /// Starts the background periodic health probing and idle eviction maintenance task if running
    /// within a Tokio asynchronous runtime.
    ///
    /// Uses a [`std::sync::Weak`] reference so the background worker task terminates automatically
    /// when all owning [`Arc<EndpointLifecycleManager>`] handles are dropped, preventing task leaks.
    pub(crate) fn start_maintenance(self: &Arc<Self>) {
        let Ok(handle) = Handle::try_current() else {
            return;
        };
        let weak = Arc::downgrade(self);
        let probe_interval = self.probe_interval;
        let task = handle.spawn(Self::run_maintenance_loop(weak, probe_interval));
        let mut state = self
            .state
            .write()
            .expect("EndpointLifecycleManager state write lock poisoned");
        if let Some(old) = state.maintenance_task.replace(task) {
            old.abort();
        }
    }

    /// Background maintenance loop that periodically probes endpoints and performs idle eviction.
    async fn run_maintenance_loop(weak: Weak<Self>, probe_interval: Duration) {
        const IDLE_EVICTION_INTERVAL_TICKS: usize = 5;
        let mut interval = interval(probe_interval);
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
        // The first tick finishes immediately; consume it to wait a full interval.
        interval.tick().await;
        let mut tick_count = 0usize;
        loop {
            interval.tick().await;
            let Some(manager) = weak.upgrade() else {
                break;
            };
            manager.probe_all_endpoints();
            tick_count += 1;
            if tick_count >= IDLE_EVICTION_INTERVAL_TICKS {
                tick_count = 0;
                manager.check_idle_eviction();
            }
        }
    }

    /// Stops the background maintenance task if currently running.
    pub(crate) fn stop_maintenance(&self) {
        let mut state = self
            .state
            .write()
            .expect("EndpointLifecycleManager state write lock poisoned");
        if let Some(handle) = state.maintenance_task.take() {
            handle.abort();
        }
    }

    /// Records that real application traffic was routed to the specified endpoint at the current timestamp.
    pub(crate) fn record_real_traffic(&self, address: &str) {
        self.record_real_traffic_at(address, Instant::now());
    }

    /// Records that real application traffic was routed to the specified endpoint at a specific timestamp.
    pub(crate) fn record_real_traffic_at(&self, address: &str, now: Instant) {
        if address.is_empty() || address == self.default_endpoint_address {
            return;
        }

        // Fast path: Check under shared read lock without contending for the exclusive write lock.
        // If traffic was already recorded within the throttle window, return immediately.
        {
            let state = self
                .state
                .read()
                .expect("EndpointLifecycleManager state read lock poisoned");

            let Some(endpoint_state) = state.endpoints.get(address) else {
                return;
            };

            if now.saturating_duration_since(endpoint_state.last_real_traffic_at)
                < TRAFFIC_RECORD_THROTTLE
            {
                return;
            }
        }

        // Slow path: Acquire exclusive write lock to update the timestamp.
        let mut state = self
            .state
            .write()
            .expect("EndpointLifecycleManager state write lock poisoned");

        // Double-Checked Locking: Re-check the throttle condition under the write lock.
        // Multiple concurrent requests may have passed the read-lock check simultaneously
        // (thundering herd); only the first one to acquire the write lock should update the timestamp.
        if let Some(endpoint_state) = state.endpoints.get_mut(address) {
            if now.saturating_duration_since(endpoint_state.last_real_traffic_at)
                < TRAFFIC_RECORD_THROTTLE
            {
                return;
            }
            endpoint_state.last_real_traffic_at = endpoint_state.last_real_traffic_at.max(now);
        }
    }

    /// Updates the active server addresses for a given source identifier (e.g. database ID),
    /// registering new endpoints and immediately evicting stale endpoints no longer referenced anywhere.
    ///
    /// Returns the list of newly registered endpoint addresses that were not previously tracked.
    pub(crate) fn update_active_addresses(
        &self,
        source_key: &str,
        active_addresses: HashSet<String>,
    ) -> Vec<String> {
        self.update_active_addresses_at(source_key, active_addresses, Instant::now())
    }

    /// Updates active server addresses with a specific reference timestamp.
    pub(crate) fn update_active_addresses_at(
        &self,
        source_key: &str,
        active_addresses: HashSet<String>,
        now: Instant,
    ) -> Vec<String> {
        if source_key.is_empty() {
            return Vec::new();
        }

        let (newly_registered, stale_addresses) = {
            let mut state = self
                .state
                .write()
                .expect("EndpointLifecycleManager state write lock poisoned");

            let newly_registered = state.register_new_endpoints(
                &active_addresses,
                &self.default_endpoint_address,
                now,
            );

            state
                .active_addresses_per_source
                .insert(source_key.to_string(), active_addresses);

            let stale = state.prune_stale_endpoints();
            self.transient_failure_count
                .store(state.transient_failure_evicted.len(), Ordering::Release);
            (newly_registered, stale)
        };

        for address in &stale_addresses {
            self.evict_connection(address, EvictionReason::Stale);
        }

        newly_registered
    }

    /// Unregisters all active addresses for a source identifier and evicts any endpoints that
    /// are no longer referenced by any remaining source.
    pub(crate) fn unregister_source(&self, source_key: &str) -> Vec<String> {
        if source_key.is_empty() {
            return Vec::new();
        }

        let stale_addresses = {
            let mut state = self
                .state
                .write()
                .expect("EndpointLifecycleManager state write lock poisoned");

            if state
                .active_addresses_per_source
                .remove(source_key)
                .is_none()
            {
                return Vec::new();
            }

            let stale = state.prune_stale_endpoints();
            self.transient_failure_count
                .store(state.transient_failure_evicted.len(), Ordering::Release);
            stale
        };

        for address in &stale_addresses {
            self.evict_connection(address, EvictionReason::Stale);
        }

        stale_addresses
    }

    /// Requests that an idle-evicted or untracked endpoint be recreated and tracked again.
    ///
    /// If the address is still present in at least one registered source's active addresses,
    /// it is re-inserted into tracked endpoints with `last_real_traffic_at = now`, starts background
    /// connection warmup, and returns `true`.
    ///
    /// If the address was previously evicted due to `TRANSIENT_FAILURE`, it remains marked as
    /// evicted until background probing observes the connection in `READY` state.
    ///
    /// If the address is not in any registered source's active set, or is already tracked, returns `false`.
    pub(crate) fn request_endpoint_recreation(&self, address: &str) -> bool {
        self.request_endpoint_recreation_at(address, Instant::now())
    }

    /// Requests endpoint recreation at a specific reference timestamp.
    pub(crate) fn request_endpoint_recreation_at(&self, address: &str, now: Instant) -> bool {
        if address.is_empty() || address == self.default_endpoint_address {
            return false;
        }

        // Fast-path read check: avoid exclusive write lock if endpoint is already tracked.
        {
            let state = self
                .state
                .read()
                .expect("EndpointLifecycleManager state read lock poisoned");
            if state.endpoints.contains_key(address) {
                return false;
            }
        }

        let mut state = self
            .state
            .write()
            .expect("EndpointLifecycleManager state write lock poisoned");

        // Double check after acquiring write lock.
        if state.endpoints.contains_key(address) {
            return false;
        }

        // Verify the address is still active in at least one registered source.
        let is_still_active = state
            .active_addresses_per_source
            .values()
            .any(|addresses| addresses.contains(address));

        if !is_still_active {
            return false;
        }

        let address_string = address.to_string();
        state.endpoints.insert(
            address_string.clone(),
            EndpointLifecycleState::new(address_string, now),
        );

        // DO NOT remove from transient_failure_evicted here.
        // If the endpoint was evicted due to consecutive transient failures, it must remain
        // marked as transient_failure_evicted until a probe observes the connection in READY state.

        drop(state);

        self.spawn_connection_task(address);

        true
    }

    /// Evicts an endpoint connection from the cache and emits a debug log.
    fn evict_connection(&self, address: &str, reason: EvictionReason) {
        debug!(
            endpoint = %address,
            ?reason,
            "Evicted endpoint from lifecycle tracking"
        );
        self.connection_cache.evict(address);
    }

    /// Spawns an asynchronous background task to establish a connection in the cache
    /// for a recreated or pre-warmed endpoint if a [`ClientConfig`] and Tokio runtime are present.
    fn spawn_connection_task(&self, address: &str) -> Option<JoinHandle<()>> {
        let config = self.client_config.as_ref()?;
        let handle = Handle::try_current().ok()?;
        let connection_cache = Arc::clone(&self.connection_cache);
        let config = Arc::clone(config);
        let address_string = address.to_string();
        let weak_state = Arc::downgrade(&self.state);
        let transient_failure_count = Arc::clone(&self.transient_failure_count);
        Some(handle.spawn(Self::connect_and_record(
            connection_cache,
            config,
            address_string,
            weak_state,
            transient_failure_count,
        )))
    }

    /// Asynchronous background worker to establish a connection in the cache and record its health.
    async fn connect_and_record(
        connection_cache: Arc<ConnectionCache>,
        config: Arc<ClientConfig>,
        address_string: String,
        weak_state: Weak<RwLock<LifecycleManagerState>>,
        transient_failure_count: Arc<AtomicUsize>,
    ) {
        let connection = match connection_cache.get(&address_string, &config).await {
            Ok(connection) => connection,
            Err(err) => {
                debug!(
                    ?err,
                    address = %address_string,
                    "Endpoint background connection attempt failed"
                );
                return;
            }
        };

        let is_still_tracked = if let Some(state) = weak_state.upgrade() {
            if let Ok(mut lifecycle_state) = state.write() {
                if connection.is_healthy() {
                    lifecycle_state.record_probe_healthy(&address_string, Instant::now());
                    transient_failure_count.store(
                        lifecycle_state.transient_failure_evicted.len(),
                        Ordering::Release,
                    );
                }
                lifecycle_state.endpoints.contains_key(&address_string)
            } else {
                false
            }
        } else {
            false
        };

        if !is_still_tracked {
            debug!(
                address = %address_string,
                "Endpoint was evicted during connection establishment, cleaning up"
            );
            connection_cache.evict(&address_string);
        }
    }

    /// Probes all currently tracked endpoints at the current timestamp and returns
    /// the list of endpoints that were evicted due to persistent failures.
    pub(crate) fn probe_all_endpoints(&self) -> Vec<(String, EvictionReason)> {
        self.probe_all_endpoints_at(Instant::now())
    }

    /// Probes all currently tracked endpoints at the given reference timestamp and returns
    /// the list of endpoints that were evicted due to persistent failures.
    pub(crate) fn probe_all_endpoints_at(&self, now: Instant) -> Vec<(String, EvictionReason)> {
        // Step 1: Read all tracked endpoint addresses under a shared read lock.
        let addresses: Vec<String> = {
            let state = self
                .state
                .read()
                .expect("EndpointLifecycleManager state read lock poisoned");
            state.endpoints.keys().cloned().collect()
        };

        // Step 2: Inspect connection health outside the write lock.
        let mut healthy_endpoints = Vec::new();
        let mut transient_failure_endpoints = Vec::new();
        let mut missing_endpoints = Vec::new();
        for address in addresses {
            if let Some(connection) = self.connection_cache.get_if_present(&address) {
                if connection.is_healthy() {
                    healthy_endpoints.push(address);
                } else if connection.is_transient_failure() {
                    transient_failure_endpoints.push(address);
                }
            } else {
                missing_endpoints.push(address);
            }
        }

        // Step 3: Batch-update lifecycle state under a single exclusive write lock if there are updates.
        let evicted = if !healthy_endpoints.is_empty() || !transient_failure_endpoints.is_empty() {
            let mut state = self
                .state
                .write()
                .expect("EndpointLifecycleManager state write lock poisoned");

            let evicted =
                state.apply_probe_results(&healthy_endpoints, &transient_failure_endpoints, now);

            self.transient_failure_count
                .store(state.transient_failure_evicted.len(), Ordering::Release);

            evicted
        } else {
            Vec::new()
        };

        // Step 4: Evict failed connections from cache after releasing the write lock.
        for (address, reason) in &evicted {
            self.evict_connection(address, *reason);
        }

        // Step 5: Retry establishing connections for tracked endpoints that are missing from cache.
        for address in missing_endpoints {
            self.spawn_connection_task(&address);
        }

        evicted
    }

    /// Scans all tracked endpoints and evicts any that have had no real application traffic
    /// for longer than [`Self::idle_eviction_duration`] at the current timestamp.
    ///
    /// Returns the list of evicted endpoint addresses.
    pub(crate) fn check_idle_eviction(&self) -> Vec<String> {
        self.check_idle_eviction_at(Instant::now())
    }

    /// Scans all tracked endpoints and evicts any that have had no real application traffic
    /// for longer than [`Self::idle_eviction_duration`].
    ///
    /// Returns the list of evicted endpoint addresses.
    pub(crate) fn check_idle_eviction_at(&self, now: Instant) -> Vec<String> {
        let idle_duration = self.idle_eviction_duration;
        let default_address = &self.default_endpoint_address;

        // Fast path: check under shared read lock if any endpoint is idle before taking write lock.
        {
            let state = self
                .state
                .read()
                .expect("EndpointLifecycleManager state read lock poisoned");
            let has_idle = state.endpoints.iter().any(|(address, endpoint_state)| {
                address != default_address && endpoint_state.is_idle_at(now, idle_duration)
            });
            if !has_idle {
                return Vec::new();
            }
        }

        let evicted_addresses = {
            let mut state = self
                .state
                .write()
                .expect("EndpointLifecycleManager state write lock poisoned");

            let mut evicted = Vec::new();
            state.endpoints.retain(|address, endpoint_state| {
                let is_idle =
                    address != default_address && endpoint_state.is_idle_at(now, idle_duration);
                if is_idle {
                    evicted.push(address.clone());
                }
                !is_idle
            });

            evicted
        };

        for address in &evicted_addresses {
            self.evict_connection(address, EvictionReason::Idle);
        }

        evicted_addresses
    }

    /// Returns whether the specified endpoint is currently marked as evicted due to `TRANSIENT_FAILURE`.
    pub(crate) fn is_transient_failure_evicted(&self, address: &str) -> bool {
        if !self.has_transient_failure_evictions() {
            return false;
        }
        let state = self
            .state
            .read()
            .expect("EndpointLifecycleManager state read lock poisoned");
        state.transient_failure_evicted.contains(address)
    }

    /// Checks if an address is marked as evicted due to `TRANSIENT_FAILURE`, and if it is not
    /// yet tracked for recreation/probing in `endpoints`, requests recreation.
    ///
    /// Returns `true` if the endpoint is currently marked as transient failure evicted.
    pub(crate) fn check_transient_failure_evicted_and_request_recreation(
        &self,
        address: &str,
    ) -> bool {
        if !self.has_transient_failure_evictions() {
            return false;
        }
        let needs_recreation = {
            let state = self
                .state
                .read()
                .expect("EndpointLifecycleManager state read lock poisoned");
            if !state.transient_failure_evicted.contains(address) {
                return false;
            }
            !state.endpoints.contains_key(address)
        };

        if needs_recreation {
            self.request_endpoint_recreation(address);
        }

        true
    }

    /// Returns whether any tracked endpoint is currently marked as evicted due to `TRANSIENT_FAILURE`.
    pub(crate) fn has_transient_failure_evictions(&self) -> bool {
        self.transient_failure_count.load(Ordering::Acquire) > 0
    }

    /// Pre-warms server connections in background tasks for the given addresses if not already cached.
    pub(crate) fn prewarm_endpoints(&self, addresses: &[String]) {
        for address in addresses {
            if address.is_empty() || self.connection_cache.get_if_present(address).is_some() {
                continue;
            }
            self.spawn_connection_task(address);
        }
    }
}

impl Drop for EndpointLifecycleManager {
    fn drop(&mut self) {
        if let Ok(mut state) = self.state.write() {
            for address in state.endpoints.keys() {
                self.evict_connection(address, EvictionReason::Shutdown);
            }
            state.endpoints.clear();
            state.transient_failure_evicted.clear();
            state.active_addresses_per_source.clear();
            self.transient_failure_count.store(0, Ordering::Release);
            if let Some(handle) = state.maintenance_task.take() {
                handle.abort();
            }
        }
    }
}

/// Internal consolidated state for [`EndpointLifecycleManager`].
#[derive(Debug, Default)]
struct LifecycleManagerState {
    endpoints: HashMap<String, EndpointLifecycleState>,
    transient_failure_evicted: HashSet<String>,
    active_addresses_per_source: HashMap<String, HashSet<String>>,
    maintenance_task: Option<JoinHandle<()>>,
}

impl LifecycleManagerState {
    /// Applies probe health results for tracked endpoints, recording updates and returning evicted endpoints.
    fn apply_probe_results(
        &mut self,
        healthy_endpoints: &[String],
        transient_failure_endpoints: &[String],
        now: Instant,
    ) -> Vec<(String, EvictionReason)> {
        for address in healthy_endpoints {
            self.record_probe_healthy(address, now);
        }

        let mut evicted = Vec::new();
        for address in transient_failure_endpoints {
            if let Some(reason) = self.record_probe_transient_failure(address, now) {
                evicted.push((address.clone(), reason));
            }
        }
        evicted
    }

    /// Updates internal state when a probe observes a healthy (`READY`) connection.
    fn record_probe_healthy(&mut self, address: &str, now: Instant) {
        if let Some(endpoint_state) = self.endpoints.get_mut(address) {
            endpoint_state.last_probe_at = Some(now);
            endpoint_state.last_ready_at = Some(now);
            endpoint_state.consecutive_transient_failures = 0;
        }
        self.transient_failure_evicted.remove(address);
    }

    /// Updates internal state when a probe observes a connection in `TRANSIENT_FAILURE`,
    /// evicting it if it reaches `MAX_TRANSIENT_FAILURE_COUNT`.
    fn record_probe_transient_failure(
        &mut self,
        address: &str,
        now: Instant,
    ) -> Option<EvictionReason> {
        let endpoint_state = self.endpoints.get_mut(address)?;
        endpoint_state.last_probe_at = Some(now);
        endpoint_state.consecutive_transient_failures = endpoint_state
            .consecutive_transient_failures
            .saturating_add(1);

        if endpoint_state.consecutive_transient_failures < MAX_TRANSIENT_FAILURE_COUNT {
            return None;
        }

        if let Some(removed) = self.endpoints.remove(address) {
            self.transient_failure_evicted.insert(removed.address);
        }

        Some(EvictionReason::TransientFailure)
    }

    /// Registers newly active addresses and clears stale transient failure markers.
    fn register_new_endpoints(
        &mut self,
        active_addresses: &HashSet<String>,
        default_address: &str,
        now: Instant,
    ) -> Vec<String> {
        let mut newly_registered = Vec::new();
        for address in active_addresses {
            if address.is_empty() || address == default_address {
                continue;
            }

            if !self.endpoints.contains_key(address) {
                let address_string = address.clone();
                self.endpoints.insert(
                    address_string.clone(),
                    EndpointLifecycleState::new(address_string.clone(), now),
                );
                newly_registered.push(address_string);
            }
        }
        newly_registered
    }

    /// Prunes failure markers and endpoints that are no longer active in any registered source.
    fn prune_stale_endpoints(&mut self) -> Vec<String> {
        let active_sources = &self.active_addresses_per_source;
        let is_active = |address: &str| {
            active_sources
                .values()
                .any(|addresses| addresses.contains(address))
        };

        self.transient_failure_evicted
            .retain(|address| is_active(address));

        let mut stale = Vec::new();
        self.endpoints.retain(|address, _| {
            let active = is_active(address);
            if !active {
                stale.push(address.clone());
            }
            active
        });

        stale
    }
}

/// The reason an endpoint was evicted from lifecycle management and connection cache.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EvictionReason {
    /// Evicted after exceeding the maximum consecutive `TRANSIENT_FAILURE` probe count.
    TransientFailure,
    /// Evicted after remaining idle without real traffic past the eviction threshold.
    Idle,
    /// Evicted because the endpoint is no longer referenced by any active tablet in routing table.
    Stale,
    /// Evicted during manager shutdown or explicit clear.
    Shutdown,
}

/// Lifecycle tracking state for an individual routed server endpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EndpointLifecycleState {
    /// Server network address in `"host:port"` format.
    pub(crate) address: String,
    /// Timestamp when this endpoint was last probed for health.
    pub(crate) last_probe_at: Option<Instant>,
    /// Timestamp when real (non-probe) application traffic was last routed to this endpoint.
    pub(crate) last_real_traffic_at: Instant,
    /// Timestamp when this endpoint was last observed in the `READY` state.
    pub(crate) last_ready_at: Option<Instant>,
    /// Number of consecutive probe observations where the endpoint was in `TRANSIENT_FAILURE`.
    pub(crate) consecutive_transient_failures: usize,
}

impl EndpointLifecycleState {
    /// Creates a new `EndpointLifecycleState` for the given address initialized at the given timestamp.
    fn new(address: String, now: Instant) -> Self {
        Self {
            address,
            last_probe_at: None,
            last_real_traffic_at: now,
            last_ready_at: None,
            consecutive_transient_failures: 0,
        }
    }

    /// Returns whether this endpoint has been idle without real traffic for longer than `timeout`.
    fn is_idle_at(&self, now: Instant, timeout: Duration) -> bool {
        now.saturating_duration_since(self.last_real_traffic_at) > timeout
    }
}

// =========================================================================
// Test-Only Inspection & State Control APIs
//
// The following methods are compiled only during tests (`#[cfg(test)]`).
// They provide inspection access and deterministic state control for unit
// and mock tests across the crate (including `database_client::tests` and
// `location_router::tests`) without exposing test-only methods in production.
// =========================================================================
#[cfg(test)]
impl EndpointLifecycleManager {
    /// Returns `true` if the background maintenance task is currently active.
    pub(crate) fn is_maintenance_active(&self) -> bool {
        let state = self
            .state
            .read()
            .expect("EndpointLifecycleManager state read lock poisoned");
        state
            .maintenance_task
            .as_ref()
            .is_some_and(|handle| !handle.is_finished())
    }

    /// Returns the configured probe interval.
    pub(crate) fn probe_interval(&self) -> Duration {
        self.probe_interval
    }

    /// Returns a reference to the [`ClientConfig`] if configured.
    pub(crate) fn client_config(&self) -> Option<&ClientConfig> {
        self.client_config.as_deref()
    }

    /// Returns the configured idle eviction duration.
    pub(crate) fn idle_eviction_duration(&self) -> Duration {
        self.idle_eviction_duration
    }

    /// Returns the default fallback endpoint address.
    pub(crate) fn default_endpoint_address(&self) -> &str {
        &self.default_endpoint_address
    }

    /// Probes an individual endpoint address at the given reference timestamp.
    ///
    /// If the endpoint is in `TRANSIENT_FAILURE`, increments its consecutive failure count.
    /// If it reaches `MAX_TRANSIENT_FAILURE_COUNT`, it is evicted from cache and lifecycle tracking.
    pub(crate) fn probe_endpoint_at(&self, address: &str, now: Instant) -> Option<EvictionReason> {
        if address.is_empty() || address == self.default_endpoint_address {
            return None;
        }

        let connection = self.connection_cache.get_if_present(address)?;
        if connection.is_healthy() {
            let mut state = self
                .state
                .write()
                .expect("EndpointLifecycleManager state write lock poisoned");
            state.record_probe_healthy(address, now);
            self.transient_failure_count
                .store(state.transient_failure_evicted.len(), Ordering::Release);
            return None;
        }

        if connection.is_transient_failure() {
            let mut state = self
                .state
                .write()
                .expect("EndpointLifecycleManager state write lock poisoned");
            let reason = state.record_probe_transient_failure(address, now);
            self.transient_failure_count
                .store(state.transient_failure_evicted.len(), Ordering::Release);
            if let Some(reason) = reason {
                drop(state);
                self.evict_connection(address, reason);
            }
            return reason;
        }

        None
    }

    /// Returns the number of currently tracked endpoints.
    pub(crate) fn len(&self) -> usize {
        let state = self
            .state
            .read()
            .expect("EndpointLifecycleManager state read lock poisoned");
        state.endpoints.len()
    }

    /// Returns whether there are no tracked endpoints.
    pub(crate) fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns a copy of the lifecycle state for the given address, if tracked.
    pub(crate) fn get_endpoint_state(&self, address: &str) -> Option<EndpointLifecycleState> {
        let state = self
            .state
            .read()
            .expect("EndpointLifecycleManager state read lock poisoned");
        state.endpoints.get(address).cloned()
    }

    /// Clears all tracked endpoint states and failure markers.
    pub(crate) fn clear(&self) {
        let mut state = self
            .state
            .write()
            .expect("EndpointLifecycleManager state write lock poisoned");
        for address in state.endpoints.keys() {
            self.evict_connection(address, EvictionReason::Shutdown);
        }
        state.endpoints.clear();
        state.transient_failure_evicted.clear();
        state.active_addresses_per_source.clear();
        self.transient_failure_count.store(0, Ordering::Release);
    }

    /// Returns whether the specified endpoint address is currently tracked in the lifecycle manager.
    pub(crate) fn is_managed(&self, address: &str) -> bool {
        let state = self
            .state
            .read()
            .expect("EndpointLifecycleManager state read lock poisoned");
        state.endpoints.contains_key(address)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::Channel;
    use crate::generated::gapic_dataplane::stub::Spanner as SpannerStub;
    use crate::routing::server_connection::ServerConnection;
    use gaxi::options::ClientConfig;
    use std::fmt::Debug;
    use std::sync::Barrier;
    use std::thread;
    use tokio::runtime::Builder;
    use tokio::task::yield_now;

    #[test]
    fn traits() {
        static_assertions::assert_impl_all!(
            EndpointLifecycleManager: Send,
            Sync,
            Debug
        );
        static_assertions::assert_impl_all!(
            EndpointLifecycleState: Send,
            Sync,
            Debug,
            Clone,
            PartialEq,
            Eq
        );
        static_assertions::assert_impl_all!(
            EvictionReason: Send,
            Sync,
            Debug,
            Clone,
            Copy,
            PartialEq,
            Eq
        );
    }

    #[derive(Debug)]
    struct DummyStub;
    impl SpannerStub for DummyStub {}

    fn create_test_connection(address: &str) -> ServerConnection {
        let channel = Channel::new_for_test(DummyStub);
        ServerConnection::new(address.to_string(), channel)
    }

    fn make_test_manager() -> (EndpointLifecycleManager, Arc<ConnectionCache>) {
        let default_connection = create_test_connection("spanner.googleapis.com:443");
        let connection_cache = Arc::new(ConnectionCache::new(default_connection));
        let manager = EndpointLifecycleManager::new(Arc::clone(&connection_cache));
        (manager, connection_cache)
    }

    /// Helper to wait deterministically for a background connection task to establish a connection in the cache.
    async fn wait_for_connection_in_cache(cache: &ConnectionCache, address: &str) {
        let start = Instant::now();
        let timeout = Duration::from_secs(3);
        while start.elapsed() < timeout {
            if cache.get_if_present(address).is_some() {
                return;
            }
            yield_now().await;
        }
        panic!(
            "timed out after {:?} waiting for connection {} in cache",
            timeout, address
        );
    }

    #[test]
    fn lifecycle_manager_initial_state() {
        let (manager, _cache) = make_test_manager();
        assert!(manager.is_empty(), "manager should be initially empty");
        assert_eq!(manager.len(), 0, "manager len should be 0");
        assert_eq!(
            manager.default_endpoint_address(),
            "spanner.googleapis.com:443",
            "default address should match connection cache default"
        );
        assert_eq!(
            manager.probe_interval(),
            DEFAULT_PROBE_INTERVAL,
            "default probe interval should match constant"
        );
        assert_eq!(
            manager.idle_eviction_duration(),
            DEFAULT_IDLE_EVICTION_DURATION,
            "default idle eviction duration should match constant"
        );
    }

    #[test]
    fn lifecycle_manager_with_options() {
        let default_connection = create_test_connection("spanner.googleapis.com:443");
        let connection_cache = Arc::new(ConnectionCache::new(default_connection));
        let custom_probe = Duration::from_secs(15);
        let custom_idle = Duration::from_secs(300);
        let manager = EndpointLifecycleManager::with_options(
            Arc::clone(&connection_cache),
            custom_probe,
            custom_idle,
        );

        assert_eq!(
            manager.probe_interval(),
            custom_probe,
            "custom probe interval should be applied"
        );
        assert_eq!(
            manager.idle_eviction_duration(),
            custom_idle,
            "custom idle eviction duration should be applied"
        );
    }

    #[test]
    fn lifecycle_manager_update_active_addresses_registers_and_evicts_stale() {
        let (manager, cache) = make_test_manager();
        let now = Instant::now();

        // Populate ConnectionCache with mock connections
        {
            let configuration = ClientConfig::default();
            let runtime = Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("runtime build failed");
            runtime.block_on(async {
                let _ = cache.get("10.0.0.1:15000", &configuration).await;
                let _ = cache.get("10.0.0.2:15000", &configuration).await;
                let _ = cache.get("10.0.0.3:15000", &configuration).await;
            });
        }
        assert_eq!(cache.len(), 4, "default + 3 connections");

        // 1. Initial update with endpoint1 and endpoint2
        let mut active1 = HashSet::new();
        active1.insert("10.0.0.1:15000".to_string());
        active1.insert("10.0.0.2:15000".to_string());

        let newly_created = manager.update_active_addresses_at("database-1", active1, now);
        assert_eq!(
            newly_created.len(),
            2,
            "both addresses should be newly registered"
        );
        assert_eq!(manager.len(), 2, "tracked endpoint count should be 2");

        let state1 = manager
            .get_endpoint_state("10.0.0.1:15000")
            .expect("endpoint1 state should exist");
        assert_eq!(
            state1.address, "10.0.0.1:15000",
            "state1 address must match"
        );
        assert_eq!(
            state1.last_real_traffic_at, now,
            "state1 last_real_traffic_at must match now"
        );
        assert_eq!(
            state1.last_probe_at, None,
            "state1 last_probe_at must be None initially"
        );
        assert_eq!(
            state1.consecutive_transient_failures, 0,
            "state1 failures must be 0"
        );

        // 2. Migration: endpoint1 is replaced by endpoint3 in routing update
        let mut active2 = HashSet::new();
        active2.insert("10.0.0.2:15000".to_string());
        active2.insert("10.0.0.3:15000".to_string());

        let newly_created2 = manager.update_active_addresses_at(
            "database-1",
            active2,
            now + Duration::from_secs(10),
        );
        assert_eq!(
            newly_created2,
            vec!["10.0.0.3:15000".to_string()],
            "only endpoint3 should be newly created"
        );
        assert_eq!(
            manager.len(),
            2,
            "endpoint1 should be evicted and endpoint3 added"
        );
        assert!(
            manager.get_endpoint_state("10.0.0.1:15000").is_none(),
            "endpoint1 must be evicted from lifecycle manager"
        );
        assert!(
            cache.get_if_present("10.0.0.1:15000").is_none(),
            "endpoint1 must be evicted from ConnectionCache"
        );
        assert!(
            cache.get_if_present("10.0.0.2:15000").is_some(),
            "endpoint2 must remain in ConnectionCache"
        );
        assert!(
            cache.get_if_present("10.0.0.3:15000").is_some(),
            "endpoint3 must remain in ConnectionCache"
        );
    }

    #[test]
    fn lifecycle_manager_re_registration_retains_transient_failure_marker_until_healthy_probe() {
        let (manager, cache) = make_test_manager();
        let now = Instant::now();

        let mut active = HashSet::new();
        active.insert("10.0.0.1:15000".to_string());
        manager.update_active_addresses_at("database-1", active.clone(), now);

        let connection = {
            let runtime = Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("runtime build failed");
            runtime.block_on(async {
                cache
                    .get("10.0.0.1:15000", &ClientConfig::default())
                    .await
                    .expect("connection create failed")
            })
        };

        // Trigger 3 transient failures to evict
        connection.set_transient_failure();
        manager.probe_endpoint_at("10.0.0.1:15000", now + Duration::from_secs(60));
        manager.probe_endpoint_at("10.0.0.1:15000", now + Duration::from_secs(120));
        let eviction_reason =
            manager.probe_endpoint_at("10.0.0.1:15000", now + Duration::from_secs(180));

        assert_eq!(
            eviction_reason,
            Some(EvictionReason::TransientFailure),
            "endpoint must be evicted due to transient failures"
        );
        assert!(
            manager.is_transient_failure_evicted("10.0.0.1:15000"),
            "transient failure marker must be set"
        );
        assert_eq!(manager.len(), 0, "endpoints map should be empty");

        // Next routing table update arrives with the same address -> re-registration must retain failure marker
        let newly_registered = manager.update_active_addresses_at(
            "database-1",
            active,
            now + Duration::from_secs(200),
        );

        assert_eq!(
            newly_registered,
            vec!["10.0.0.1:15000".to_string()],
            "endpoint must be re-registered"
        );
        assert_eq!(
            manager.len(),
            1,
            "endpoint must be tracked in lifecycle manager"
        );
        assert!(
            manager.is_transient_failure_evicted("10.0.0.1:15000"),
            "transient failure marker must be retained on re-registration until healthy probe"
        );

        // Re-populate connection in cache and mark ready
        let connection = {
            let runtime = Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("runtime build failed");
            runtime.block_on(async {
                cache
                    .get("10.0.0.1:15000", &ClientConfig::default())
                    .await
                    .expect("connection create failed")
            })
        };
        connection.set_ready();
        manager.probe_endpoint_at("10.0.0.1:15000", now + Duration::from_secs(260));
        assert!(
            !manager.is_transient_failure_evicted("10.0.0.1:15000"),
            "transient failure marker must be cleared after healthy probe"
        );
    }

    #[test]
    fn lifecycle_manager_update_active_addresses_empty_or_default_ignored() {
        let (manager, _cache) = make_test_manager();
        let now = Instant::now();

        // Empty source key -> returns empty vec without modifying state
        let mut active = HashSet::new();
        active.insert("10.0.0.1:15000".to_string());
        let result = manager.update_active_addresses_at("", active, now);
        assert!(result.is_empty(), "empty source key should return empty");
        assert_eq!(manager.len(), 0, "manager should remain empty");

        // Default endpoint address and empty string in active set should be ignored
        let mut active_with_default = HashSet::new();
        active_with_default.insert("".to_string());
        active_with_default.insert("spanner.googleapis.com:443".to_string());
        active_with_default.insert("10.0.0.1:15000".to_string());

        let newly_created = manager.update_active_addresses("database-1", active_with_default);
        assert_eq!(
            newly_created,
            vec!["10.0.0.1:15000".to_string()],
            "only non-default, non-empty address should be registered"
        );
        assert_eq!(
            manager.len(),
            1,
            "manager should contain exactly 1 registered endpoint"
        );
    }

    #[test]
    fn lifecycle_manager_record_real_traffic_refreshes_timestamp() {
        let (manager, _cache) = make_test_manager();
        let start = Instant::now();

        let mut active = HashSet::new();
        active.insert("10.0.0.1:15000".to_string());
        manager.update_active_addresses_at("database-1", active, start);

        let later = start + Duration::from_secs(600);
        manager.record_real_traffic_at("10.0.0.1:15000", later);

        let state = manager
            .get_endpoint_state("10.0.0.1:15000")
            .expect("state should exist");
        assert_eq!(
            state.last_real_traffic_at, later,
            "real traffic should update last_real_traffic_at"
        );

        // Test convenience record_real_traffic method (uses Instant::now())
        manager.record_real_traffic("10.0.0.1:15000");

        // Recording on default endpoint, empty string, or non-existent endpoint should be a harmless no-op
        manager.record_real_traffic_at("spanner.googleapis.com:443", later);
        manager.record_real_traffic_at("", later);
        manager.record_real_traffic_at("non.existent:15000", later);
    }

    #[test]
    fn lifecycle_manager_record_real_traffic_monotonic_timestamp() {
        let (manager, _cache) = make_test_manager();
        let start = Instant::now();

        let mut active = HashSet::new();
        active.insert("10.0.0.1:15000".to_string());
        manager.update_active_addresses_at("database-1", active, start);

        let later = start + Duration::from_secs(600);
        manager.record_real_traffic_at("10.0.0.1:15000", later);

        // Attempt to record traffic with an older timestamp must not regress last_real_traffic_at
        let older = start + Duration::from_secs(300);
        manager.record_real_traffic_at("10.0.0.1:15000", older);

        let state = manager
            .get_endpoint_state("10.0.0.1:15000")
            .expect("state exists");
        assert_eq!(
            state.last_real_traffic_at, later,
            "older timestamp must not regress last_real_traffic_at"
        );
    }

    #[test]
    fn lifecycle_manager_record_real_traffic_throttling() {
        let (manager, _cache) = make_test_manager();
        let start = Instant::now();

        let mut active = HashSet::new();
        active.insert("10.0.0.1:15000".to_string());
        manager.update_active_addresses_at("database-1", active, start);

        // Within throttle window (5s < 10s throttle): timestamp should remain unchanged at `start`
        let within_throttle = start + Duration::from_secs(5);
        manager.record_real_traffic_at("10.0.0.1:15000", within_throttle);
        let state = manager
            .get_endpoint_state("10.0.0.1:15000")
            .expect("state exists");
        assert_eq!(
            state.last_real_traffic_at, start,
            "traffic within throttle window should not update last_real_traffic_at"
        );

        // Past throttle window (11s > 10s throttle): timestamp must update to `past_throttle`
        let past_throttle = start + Duration::from_secs(11);
        manager.record_real_traffic_at("10.0.0.1:15000", past_throttle);
        let state = manager
            .get_endpoint_state("10.0.0.1:15000")
            .expect("state exists");
        assert_eq!(
            state.last_real_traffic_at, past_throttle,
            "traffic past throttle window must update last_real_traffic_at"
        );
    }

    #[test]
    fn lifecycle_manager_idle_eviction() {
        let (manager, cache) = make_test_manager();
        let start = Instant::now();

        let mut active = HashSet::new();
        active.insert("10.0.0.1:15000".to_string());
        active.insert("10.0.0.2:15000".to_string());
        manager.update_active_addresses_at("database-1", active, start);

        // endpoint1 receives traffic at start + 15 minutes (900s)
        manager.record_real_traffic_at("10.0.0.1:15000", start + Duration::from_secs(900));

        // When checking before timeout (e.g. at 500s), nothing is evicted
        let empty_evicted = manager.check_idle_eviction_at(start + Duration::from_secs(500));
        assert!(empty_evicted.is_empty(), "no endpoints should be idle yet");

        // endpoint2 receives no further traffic.
        // At start + 31 minutes (1860s):
        // endpoint1 idle duration = 1860 - 900 = 960s (< 1800s idle eviction duration) -> not evicted
        // endpoint2 idle duration = 1860 - 0 = 1860s (> 1800s idle eviction duration) -> evicted
        let check_time = start + Duration::from_secs(1860);
        let evicted = manager.check_idle_eviction_at(check_time);

        assert_eq!(
            evicted,
            vec!["10.0.0.2:15000".to_string()],
            "only endpoint2 should be idle evicted"
        );
        assert_eq!(
            manager.len(),
            1,
            "only endpoint1 should remain tracked in manager"
        );
        assert!(
            manager.get_endpoint_state("10.0.0.1:15000").is_some(),
            "endpoint1 state should remain in manager"
        );
        assert!(
            manager.get_endpoint_state("10.0.0.2:15000").is_none(),
            "endpoint2 state should be removed from manager"
        );
        assert!(
            cache.get_if_present("10.0.0.2:15000").is_none(),
            "endpoint2 should be evicted from ConnectionCache"
        );
    }

    #[test]
    fn lifecycle_manager_request_endpoint_recreation() {
        let (manager, _cache) = make_test_manager();
        let start = Instant::now();

        let mut active = HashSet::new();
        active.insert("10.0.0.1:15000".to_string());
        active.insert("10.0.0.2:15000".to_string());
        manager.update_active_addresses_at("database-1", active, start);

        // Idle-evict endpoint1 and endpoint2 at start + 31 minutes
        let check_time = start + Duration::from_secs(1860);
        let evicted = manager.check_idle_eviction_at(check_time);
        assert_eq!(evicted.len(), 2, "both endpoints should be idle evicted");
        assert_eq!(manager.len(), 0, "manager should have 0 tracked endpoints");

        // Recreate endpoint1 at later timestamp
        let later = start + Duration::from_secs(2000);
        let recreated = manager.request_endpoint_recreation_at("10.0.0.1:15000", later);
        assert!(recreated, "recreation of active address must return true");
        assert_eq!(manager.len(), 1, "endpoint1 should now be tracked");

        let state = manager
            .get_endpoint_state("10.0.0.1:15000")
            .expect("state should exist");
        assert_eq!(
            state.address, "10.0.0.1:15000",
            "recreated endpoint address must match"
        );
        assert_eq!(
            state.last_real_traffic_at, later,
            "recreated endpoint must be initialized with recreation timestamp"
        );

        // Recreating an already tracked endpoint returns false
        assert!(
            !manager.request_endpoint_recreation("10.0.0.1:15000"),
            "recreating already tracked endpoint must return false"
        );

        // Recreating an address not in any registered source's active set returns false
        assert!(
            !manager.request_endpoint_recreation("10.0.0.99:15000"),
            "recreating inactive address must return false"
        );

        // Recreating default or empty address returns false
        assert!(
            !manager.request_endpoint_recreation("spanner.googleapis.com:443"),
            "recreating default endpoint must return false"
        );
        assert!(
            !manager.request_endpoint_recreation(""),
            "recreating empty address must return false"
        );
    }

    #[tokio::test]
    async fn lifecycle_manager_request_endpoint_recreation_with_client_config_warms_cache() {
        let default_connection = create_test_connection("spanner.googleapis.com:443");
        let cache = Arc::new(ConnectionCache::new(default_connection));
        let manager = EndpointLifecycleManager::with_client_config(
            Arc::clone(&cache),
            ClientConfig::default(),
        );
        let start = Instant::now();

        let mut active = HashSet::new();
        active.insert("10.0.0.1:15000".to_string());
        manager.update_active_addresses_at("database-1", active, start);

        // Idle-evict endpoint1
        let check_time = start + Duration::from_secs(1860);
        let evicted = manager.check_idle_eviction_at(check_time);
        assert_eq!(evicted.len(), 1, "endpoint1 should be idle evicted");
        assert!(
            cache.get_if_present("10.0.0.1:15000").is_none(),
            "endpoint1 should be absent from cache after idle eviction"
        );

        // Recreate endpoint1 - should trigger background connection warmup
        let later = start + Duration::from_secs(2000);
        let recreated = manager.request_endpoint_recreation_at("10.0.0.1:15000", later);
        assert!(recreated, "recreation of active address must return true");

        // Wait deterministically for spawned reconnection task to establish connection in cache
        wait_for_connection_in_cache(&cache, "10.0.0.1:15000").await;
        assert!(
            cache.get_if_present("10.0.0.1:15000").is_some(),
            "connection must be established in connection cache after recreation"
        );
    }

    #[test]
    fn lifecycle_manager_transient_failure_recreation_retains_marker_until_probe_healthy() {
        let (manager, cache) = make_test_manager();
        let now = Instant::now();

        let mut active = HashSet::new();
        active.insert("10.0.0.1:15000".to_string());
        manager.update_active_addresses_at("database-1", active, now);

        let connection = {
            let runtime = Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("runtime build failed");
            runtime.block_on(async {
                cache
                    .get("10.0.0.1:15000", &ClientConfig::default())
                    .await
                    .expect("cache get failed")
            })
        };

        // Trigger 3 consecutive transient failures to evict the endpoint
        connection.set_transient_failure();
        manager.probe_endpoint_at("10.0.0.1:15000", now + Duration::from_secs(60));
        manager.probe_endpoint_at("10.0.0.1:15000", now + Duration::from_secs(120));
        let eviction_reason =
            manager.probe_endpoint_at("10.0.0.1:15000", now + Duration::from_secs(180));
        assert_eq!(
            eviction_reason,
            Some(EvictionReason::TransientFailure),
            "third transient failure probe must trigger eviction"
        );
        assert!(
            manager.is_transient_failure_evicted("10.0.0.1:15000"),
            "endpoint must be marked as transient_failure_evicted"
        );
        assert_eq!(
            manager.len(),
            0,
            "evicted endpoint must be removed from tracked endpoints"
        );

        // Recreate the endpoint: should re-track but retain transient_failure_evicted marker
        let recreation_result = manager.request_endpoint_recreation("10.0.0.1:15000");
        assert!(
            recreation_result,
            "recreation must return true for active address"
        );
        assert_eq!(
            manager.len(),
            1,
            "recreated endpoint must be added back to tracked endpoints"
        );
        assert!(
            manager.is_transient_failure_evicted("10.0.0.1:15000"),
            "recreated endpoint must retain transient_failure_evicted until proven healthy"
        );

        // Put connection back into cache and mark ready
        connection.set_ready();
        let runtime = Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime build failed");
        runtime.block_on(async {
            cache
                .get("10.0.0.1:15000", &ClientConfig::default())
                .await
                .expect("cache get failed");
        });

        // Probe sees READY: clears transient failure eviction marker
        let probe_result =
            manager.probe_endpoint_at("10.0.0.1:15000", now + Duration::from_secs(240));
        assert_eq!(probe_result, None, "healthy probe must succeed");
        assert!(
            !manager.is_transient_failure_evicted("10.0.0.1:15000"),
            "transient failure eviction marker must be cleared after observing READY"
        );
    }

    #[tokio::test]
    async fn lifecycle_manager_probe_retries_missing_endpoints() {
        let default_connection = create_test_connection("spanner.googleapis.com:443");
        let cache = Arc::new(ConnectionCache::new(default_connection));
        let manager = EndpointLifecycleManager::with_client_config(
            Arc::clone(&cache),
            ClientConfig::default(),
        );
        let start = Instant::now();

        let mut active = HashSet::new();
        active.insert("10.0.0.1:15000".to_string());
        manager.update_active_addresses_at("database-1", active, start);

        // Manually evict from connection cache to simulate failed warmup or connection drop
        cache.evict("10.0.0.1:15000");
        assert!(
            cache.get_if_present("10.0.0.1:15000").is_none(),
            "connection should be missing from cache"
        );

        // Probe runs: notices tracked endpoint missing from cache, triggers reconnection
        manager.probe_all_endpoints_at(start + Duration::from_secs(60));

        // Wait deterministically for spawned warmup task to recover connection in cache
        wait_for_connection_in_cache(&cache, "10.0.0.1:15000").await;
        assert!(
            cache.get_if_present("10.0.0.1:15000").is_some(),
            "probe must retry and recover missing connection in cache"
        );
    }

    #[test]
    fn lifecycle_manager_probe_consecutive_transient_failures_evicts_endpoint() {
        let (manager, cache) = make_test_manager();
        let now = Instant::now();

        let mut active = HashSet::new();
        active.insert("10.0.0.1:15000".to_string());
        manager.update_active_addresses_at("database-1", active, now);

        let connection = {
            let runtime = Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("runtime build failed");
            runtime.block_on(async {
                cache
                    .get("10.0.0.1:15000", &ClientConfig::default())
                    .await
                    .expect("connection create failed")
            })
        };

        // Mark connection as in TRANSIENT_FAILURE
        connection.set_transient_failure();

        // Probe 1: failure count = 1 -> not evicted
        let result1 = manager.probe_endpoint_at("10.0.0.1:15000", now + Duration::from_secs(60));
        assert_eq!(result1, None, "probe 1 should not evict");
        assert_eq!(
            manager
                .get_endpoint_state("10.0.0.1:15000")
                .expect("state exists")
                .consecutive_transient_failures,
            1
        );
        assert!(
            !manager.is_transient_failure_evicted("10.0.0.1:15000"),
            "failure marker should not be set after 1 probe"
        );

        // Probe 2: failure count = 2 -> not evicted
        let result2 = manager.probe_endpoint_at("10.0.0.1:15000", now + Duration::from_secs(120));
        assert_eq!(result2, None, "probe 2 should not evict");
        assert_eq!(
            manager
                .get_endpoint_state("10.0.0.1:15000")
                .expect("state exists")
                .consecutive_transient_failures,
            2
        );

        // Probe 3: failure count = 3 (MAX) -> evicted with TransientFailure reason
        let result3 = manager.probe_endpoint_at("10.0.0.1:15000", now + Duration::from_secs(180));
        assert_eq!(
            result3,
            Some(EvictionReason::TransientFailure),
            "probe 3 must trigger eviction"
        );
        assert!(
            manager.get_endpoint_state("10.0.0.1:15000").is_none(),
            "endpoint must be removed from manager"
        );
        assert!(
            cache.get_if_present("10.0.0.1:15000").is_none(),
            "endpoint must be evicted from connection cache"
        );
        assert!(
            manager.is_transient_failure_evicted("10.0.0.1:15000"),
            "transient failure marker must be set"
        );
    }

    #[test]
    fn lifecycle_manager_probe_healthy_resets_failure_counter() {
        let (manager, cache) = make_test_manager();
        let now = Instant::now();

        let mut active = HashSet::new();
        active.insert("10.0.0.1:15000".to_string());
        manager.update_active_addresses_at("database-1", active, now);

        let connection = {
            let runtime = Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("runtime build failed");
            runtime.block_on(async {
                cache
                    .get("10.0.0.1:15000", &ClientConfig::default())
                    .await
                    .expect("connection create failed")
            })
        };

        // 2 consecutive failures
        connection.set_transient_failure();
        manager.probe_endpoint_at("10.0.0.1:15000", now + Duration::from_secs(60));
        manager.probe_endpoint_at("10.0.0.1:15000", now + Duration::from_secs(120));
        assert_eq!(
            manager
                .get_endpoint_state("10.0.0.1:15000")
                .expect("state exists")
                .consecutive_transient_failures,
            2
        );

        // Channel recovers to READY
        connection.set_ready();
        let result = manager.probe_endpoint_at("10.0.0.1:15000", now + Duration::from_secs(180));
        assert_eq!(result, None, "probe should succeed");

        let state = manager
            .get_endpoint_state("10.0.0.1:15000")
            .expect("state exists");
        assert_eq!(
            state.consecutive_transient_failures, 0,
            "failure count must reset to 0 on READY"
        );
        assert_eq!(
            state.last_ready_at,
            Some(now + Duration::from_secs(180)),
            "last_ready_at must be recorded"
        );
        assert!(
            !manager.is_transient_failure_evicted("10.0.0.1:15000"),
            "transient failure marker should be cleared"
        );
    }

    #[test]
    fn lifecycle_manager_probe_unconnected_or_default_returns_none() {
        let (manager, _cache) = make_test_manager();
        let now = Instant::now();

        // Probing default endpoint returns None
        assert_eq!(
            manager.probe_endpoint_at("spanner.googleapis.com:443", now),
            None,
            "probing default endpoint must return None"
        );

        // Probing empty address returns None
        assert_eq!(
            manager.probe_endpoint_at("", now),
            None,
            "probing empty address must return None"
        );

        // Probing an endpoint not present in connection cache returns None
        assert_eq!(
            manager.probe_endpoint_at("10.0.0.99:15000", now),
            None,
            "probing untracked address must return None"
        );
    }

    #[test]
    fn lifecycle_manager_probe_unhealthy_state_ignored() {
        let (manager, cache) = make_test_manager();
        let now = Instant::now();

        let mut active = HashSet::new();
        active.insert("10.0.0.1:15000".to_string());
        manager.update_active_addresses_at("database-1", active, now);

        let connection = {
            let runtime = Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("runtime build failed");
            runtime.block_on(async {
                cache
                    .get("10.0.0.1:15000", &ClientConfig::default())
                    .await
                    .expect("connection create failed")
            })
        };

        connection.set_unhealthy();
        let result = manager.probe_endpoint_at("10.0.0.1:15000", now);
        assert_eq!(
            result, None,
            "STATE_UNHEALTHY should return None without incrementing transient failure"
        );
        assert_eq!(
            manager
                .get_endpoint_state("10.0.0.1:15000")
                .expect("state exists")
                .consecutive_transient_failures,
            0
        );
    }

    #[test]
    fn lifecycle_manager_unregister_source_cleans_up() {
        let (manager, _cache) = make_test_manager();
        let now = Instant::now();

        // Unregistering empty source key -> returns empty vec
        assert!(
            manager.unregister_source("").is_empty(),
            "unregistering empty source key should return empty vec"
        );

        // Unregistering non-existent source key -> returns empty vec
        assert!(
            manager.unregister_source("non-existent").is_empty(),
            "unregistering unknown source key should return empty vec"
        );

        let mut database1_addresses = HashSet::new();
        database1_addresses.insert("10.0.0.1:15000".to_string());
        database1_addresses.insert("10.0.0.2:15000".to_string());
        manager.update_active_addresses_at("database-1", database1_addresses, now);

        let mut database2_addresses = HashSet::new();
        database2_addresses.insert("10.0.0.2:15000".to_string());
        database2_addresses.insert("10.0.0.3:15000".to_string());
        manager.update_active_addresses_at("database-2", database2_addresses, now);

        assert_eq!(manager.len(), 3, "total 3 distinct endpoints");

        // Unregister database-1 -> endpoint1 is no longer in any source and should be evicted; endpoint2 is still in database-2.
        let evicted = manager.unregister_source("database-1");
        assert_eq!(
            evicted,
            vec!["10.0.0.1:15000".to_string()],
            "endpoint1 should be evicted"
        );
        assert_eq!(
            manager.len(),
            2,
            "endpoint2 and endpoint3 should remain tracked"
        );
        assert!(
            manager.get_endpoint_state("10.0.0.1:15000").is_none(),
            "endpoint1 should not be in manager"
        );
        assert!(
            manager.get_endpoint_state("10.0.0.2:15000").is_some(),
            "endpoint2 should remain in manager"
        );
        assert!(
            manager.get_endpoint_state("10.0.0.3:15000").is_some(),
            "endpoint3 should remain in manager"
        );
    }

    #[test]
    fn lifecycle_manager_probe_all_endpoints() {
        let (manager, cache) = make_test_manager();
        let now = Instant::now();

        let mut addresses = HashSet::new();
        addresses.insert("10.0.0.1:15000".to_string());
        addresses.insert("10.0.0.2:15000".to_string());
        manager.update_active_addresses_at("database-1", addresses, now);

        let (connection1, connection2) = {
            let runtime = Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("runtime build failed");
            runtime.block_on(async {
                let connection1 = cache
                    .get("10.0.0.1:15000", &ClientConfig::default())
                    .await
                    .expect("connection1 create failed");
                let connection2 = cache
                    .get("10.0.0.2:15000", &ClientConfig::default())
                    .await
                    .expect("connection2 create failed");
                (connection1, connection2)
            })
        };

        connection1.set_transient_failure();
        connection2.set_ready();

        // 3 probe ticks
        manager.probe_all_endpoints_at(now + Duration::from_secs(60));
        manager.probe_all_endpoints_at(now + Duration::from_secs(120));
        let evicted = manager.probe_all_endpoints_at(now + Duration::from_secs(180));

        assert_eq!(
            evicted,
            vec![(
                "10.0.0.1:15000".to_string(),
                EvictionReason::TransientFailure
            )],
            "endpoint1 should be evicted due to transient failure"
        );
        assert_eq!(manager.len(), 1, "only endpoint2 should remain in manager");
        assert!(
            manager.get_endpoint_state("10.0.0.2:15000").is_some(),
            "endpoint2 should remain tracked"
        );
    }

    #[test]
    fn lifecycle_manager_clear() {
        let (manager, _cache) = make_test_manager();
        let mut addresses = HashSet::new();
        addresses.insert("10.0.0.1:15000".to_string());
        manager.update_active_addresses("database-1", addresses);
        assert_eq!(manager.len(), 1, "manager should contain 1 endpoint");

        manager.clear();
        assert_eq!(manager.len(), 0, "manager len should be 0 after clear");
        assert!(manager.is_empty(), "manager should be empty after clear");
    }

    #[test]
    fn lifecycle_state_and_reason_debug_clone_equality() {
        let now = Instant::now();
        let state1 = EndpointLifecycleState::new("10.0.0.1:15000".to_string(), now);
        let state2 = state1.clone();
        assert_eq!(state1, state2, "cloned state must equal original state");
        assert!(
            format!("{state1:?}").contains("10.0.0.1:15000"),
            "debug output should contain endpoint address"
        );

        let reason = EvictionReason::Idle;
        let reason_clone = reason;
        assert_eq!(
            reason, reason_clone,
            "cloned reason must equal original reason"
        );
        assert_eq!(
            format!("{reason:?}"),
            "Idle",
            "debug format for Idle reason should match"
        );
        assert_eq!(
            format!("{:?}", EvictionReason::Stale),
            "Stale",
            "debug format for Stale reason should match"
        );
        assert_eq!(
            format!("{:?}", EvictionReason::Shutdown),
            "Shutdown",
            "debug format for Shutdown reason should match"
        );
    }

    #[test]
    fn lifecycle_manager_concurrent_traffic_and_probes() {
        let (manager, _cache) = make_test_manager();
        let manager = Arc::new(manager);
        let num_threads = 8;
        let iterations = 100;
        let barrier = Arc::new(Barrier::new(num_threads));

        let mut addresses = HashSet::new();
        for i in 0..10 {
            addresses.insert(format!("10.0.0.{}:15000", i));
        }
        manager.update_active_addresses("database-1", addresses);

        thread::scope(|scope| {
            for thread_index in 0..num_threads {
                let manager_clone = Arc::clone(&manager);
                let barrier_clone = Arc::clone(&barrier);
                scope.spawn(move || {
                    barrier_clone.wait();
                    for iteration in 0..iterations {
                        let endpoint = format!("10.0.0.{}:15000", (thread_index + iteration) % 10);
                        manager_clone.record_real_traffic(&endpoint);
                        let _ = manager_clone.get_endpoint_state(&endpoint);
                        let _ = manager_clone.len();
                    }
                });
            }
        });

        assert_eq!(
            manager.len(),
            10,
            "all 10 endpoints should still be tracked"
        );
    }

    #[tokio::test]
    async fn lifecycle_manager_maintenance_task_lifecycle_and_helpers() {
        let (manager, _cache) = make_test_manager();
        let manager = Arc::new(manager);

        assert!(
            !manager.has_transient_failure_evictions(),
            "new manager should have no transient failure evictions"
        );
        assert!(
            !manager.is_maintenance_active(),
            "maintenance task should not be active initially"
        );

        manager.start_maintenance();
        assert!(
            manager.is_maintenance_active(),
            "maintenance task should be active after start_maintenance"
        );

        // Idempotent restart
        manager.start_maintenance();
        assert!(
            manager.is_maintenance_active(),
            "maintenance task should remain active after second start_maintenance"
        );

        manager.stop_maintenance();
        assert!(
            !manager.is_maintenance_active(),
            "maintenance task should be inactive after stop_maintenance"
        );

        // Test convenience methods probe_all_endpoints and check_idle_eviction
        let mut addresses = HashSet::new();
        addresses.insert("10.0.0.1:15000".to_string());
        manager.update_active_addresses("database-1", addresses);

        let evicted_probes = manager.probe_all_endpoints();
        assert!(
            evicted_probes.is_empty(),
            "no endpoints should be evicted when no failures"
        );

        let evicted_idle = manager.check_idle_eviction();
        assert!(
            evicted_idle.is_empty(),
            "no endpoints should be evicted when not idle"
        );
    }

    #[test]
    fn lifecycle_manager_drop_evicts_connections_from_cache() {
        let (manager, cache) = make_test_manager();
        let now = Instant::now();

        let mut active = HashSet::new();
        active.insert("10.0.0.1:15000".to_string());
        manager.update_active_addresses_at("database-1", active, now);

        let runtime = Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime build failed");
        runtime.block_on(async {
            cache
                .get("10.0.0.1:15000", &ClientConfig::default())
                .await
                .expect("connection create failed");
        });

        assert!(
            cache.get_if_present("10.0.0.1:15000").is_some(),
            "connection must be in cache before drop"
        );

        drop(manager);

        assert!(
            cache.get_if_present("10.0.0.1:15000").is_none(),
            "connection must be evicted from cache on manager drop"
        );
    }

    #[tokio::test]
    async fn lifecycle_manager_reconnection_task_cleans_up_if_evicted_during_connect() {
        let default_connection = create_test_connection("spanner.googleapis.com:443");
        let cache = Arc::new(ConnectionCache::new(default_connection));
        let manager = EndpointLifecycleManager::with_client_config(
            Arc::clone(&cache),
            ClientConfig::default(),
        );

        let mut active = HashSet::new();
        active.insert("10.0.0.1:15000".to_string());
        manager.update_active_addresses("database-1", active);

        // Manually evict from connection cache to simulate cold endpoint
        cache.evict("10.0.0.1:15000");

        // Spawn reconnection
        let connection_task = manager.spawn_connection_task("10.0.0.1:15000");

        // Unregister source before connection task completes
        manager.unregister_source("database-1");
        assert!(
            !manager.is_managed("10.0.0.1:15000"),
            "endpoint must not be managed after unregistering source"
        );

        // Await connection task completion to verify post-completion cleanup
        if let Some(task) = connection_task {
            task.await.expect("connection task failed");
        }

        assert!(
            cache.get_if_present("10.0.0.1:15000").is_none(),
            "channel must be evicted from cache if endpoint was evicted before connection finished"
        );
    }

    #[tokio::test]
    async fn lifecycle_manager_reconnection_task_cleans_up_if_manager_dropped_during_connect() {
        let default_connection = create_test_connection("spanner.googleapis.com:443");
        let cache = Arc::new(ConnectionCache::new(default_connection));
        let manager = EndpointLifecycleManager::with_client_config(
            Arc::clone(&cache),
            ClientConfig::default(),
        );

        let mut active = HashSet::new();
        active.insert("10.0.0.1:15000".to_string());
        manager.update_active_addresses("database-1", active);

        // Manually evict from connection cache to simulate cold endpoint
        cache.evict("10.0.0.1:15000");

        // Spawn reconnection
        let connection_task = manager.spawn_connection_task("10.0.0.1:15000");

        // Drop manager while reconnection task is in flight
        drop(manager);

        // Await connection task completion to verify post-drop cleanup
        if let Some(task) = connection_task {
            task.await.expect("connection task failed");
        }

        assert!(
            cache.get_if_present("10.0.0.1:15000").is_none(),
            "channel must be evicted from cache if manager was dropped before connection finished"
        );
    }
}
