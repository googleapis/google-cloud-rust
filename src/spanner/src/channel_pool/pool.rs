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

//! Unified channel pool engine, Power of Two Least Busy (P2C) selection, and affinity routing.
//!
//! Provides `ChannelPool`, which unifies both static (fixed-size) and dynamically scaling channel
//! pool configurations under a single API for the Spanner client.

use crate::channel_pool::affinity::{ChannelTarget, TransactionAffinity};
use crate::channel_pool::config::{
    ChannelPoolConfig, DynamicChannelPoolConfig, MAX_SUPPORTED_CHANNELS, StaticChannelPoolConfig,
};
use crate::channel_pool::entry::{ActiveRpcGuard, ChannelEntry, ChannelLease};
use crate::channel_pool::scaler::{scale_down_monitor_loop, scale_up_worker_loop};
use crate::client::Channel;
use crate::routing::power_of_two_selector::PowerOfTwoSelector;
use gaxi::options::ClientConfig;
use std::fmt::{Debug, Formatter, Result as FmtResult};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::{Duration, Instant};
use tokio::spawn;
use tokio::sync::Notify;
use tokio::sync::watch::{Sender as WatchSender, channel as watch_channel};

/// Unified channel pool managing gRPC channels for the Spanner client.
///
/// Supports both fixed-size static pools and dynamically scaling pools with identical caller interfaces.
#[derive(Clone)]
pub(crate) struct ChannelPool {
    pub(crate) inner: Arc<ChannelPoolInner>,
}

impl Debug for ChannelPool {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> FmtResult {
        formatter
            .debug_struct("ChannelPool")
            .field("config", &self.inner.config)
            .field("active_channels", &self.active_channel_count())
            .field("draining_channels", &self.draining_channel_count())
            .finish()
    }
}

impl ChannelPool {
    /// Initializes active channel entries and monotonic ID allocator from input channels.
    fn initialize_entries(channels: Vec<Channel>) -> (Vec<Arc<ChannelEntry>>, AtomicU64) {
        let mut active = Vec::with_capacity(channels.len());
        for (index, channel) in channels.into_iter().enumerate() {
            let id = (index + 1) as u64;
            let logical_channel_id = index + 1;
            active.push(Arc::new(ChannelEntry::new(id, logical_channel_id, channel)));
        }
        let next_entry_id = AtomicU64::new((active.len() + 1) as u64);
        (active, next_entry_id)
    }

    /// Creates a static channel pool from pre-initialized channels.
    pub(crate) fn new_static(
        channels: Vec<Channel>,
        config: StaticChannelPoolConfig,
        client_config: ClientConfig,
    ) -> Self {
        let (active, next_entry_id) = Self::initialize_entries(channels);
        let (shutdown_sender, _shutdown_receiver) = watch_channel(());

        let inner = Arc::new(ChannelPoolInner {
            config: ChannelPoolConfig::Static(config),
            client_config,
            active_entries: RwLock::new(active),
            draining_entries: RwLock::new(Vec::new()),
            next_entry_id,
            scale_up_notify: Arc::new(Notify::new()),
            scale_up_requested: AtomicBool::new(false),
            shutdown_sender,
            last_scale_up_time: Mutex::new(None),
            consecutive_low_load_checks: AtomicUsize::new(0),
            prime_session: RwLock::new(None),
            selector: PowerOfTwoSelector::new(),
        });

        Self { inner }
    }

    /// Creates a dynamic channel pool and spawns background scale-up and scale-down tasks.
    pub(crate) fn new_dynamic(
        initial_channels: Vec<Channel>,
        config: DynamicChannelPoolConfig,
        client_config: ClientConfig,
    ) -> Self {
        let scale_down_interval = config.scale_down_check_interval;
        let (active, next_entry_id) = Self::initialize_entries(initial_channels);
        let (shutdown_sender, shutdown_receiver) = watch_channel(());

        let inner = Arc::new(ChannelPoolInner {
            config: ChannelPoolConfig::Dynamic(config),
            client_config,
            active_entries: RwLock::new(active),
            draining_entries: RwLock::new(Vec::new()),
            next_entry_id,
            scale_up_notify: Arc::new(Notify::new()),
            scale_up_requested: AtomicBool::new(false),
            shutdown_sender,
            last_scale_up_time: Mutex::new(None),
            consecutive_low_load_checks: AtomicUsize::new(0),
            prime_session: RwLock::new(None),
            selector: PowerOfTwoSelector::new(),
        });

        // Spawn background scale-up worker using a weak handle and persistent shutdown receiver.
        let weak_up = Arc::downgrade(&inner);
        let receiver_up = shutdown_receiver.clone();
        spawn(async move {
            scale_up_worker_loop(weak_up, receiver_up).await;
        });

        // Spawn background scale-down monitor using a weak handle and persistent shutdown receiver.
        let weak_down = Arc::downgrade(&inner);
        let receiver_down = shutdown_receiver;
        spawn(async move {
            scale_down_monitor_loop(weak_down, receiver_down, scale_down_interval).await;
        });

        Self { inner }
    }

    /// Wraps an entry in an `ActiveRpcGuard` and signals scale-up if high load is detected.
    fn make_lease(&self, entry: Arc<ChannelEntry>) -> ChannelLease {
        let guard = self.inner.make_guard(entry);

        // Signal scale-up if high load threshold is exceeded on dynamic pools.
        // Works consistently across both standalone RPCs and pinned affinity transactions.
        if self
            .inner
            .config
            .dynamic_config()
            .is_some_and(|dynamic_config| {
                guard.entry.effective_pick_load() as f64 > dynamic_config.max_rpc_per_channel
            })
        {
            self.inner.scale_up_requested.store(true, Ordering::Release);
            self.inner.scale_up_notify.notify_one();
        }

        ChannelLease::new(guard)
    }

    /// Selects an active channel using Power of Two Least Busy (P2C) selection.
    pub(crate) fn pick_channel(&self) -> Option<ChannelLease> {
        let active_guard = self.inner.read_active_entries();

        self.pick_from_slice(&active_guard)
    }

    /// Leases a channel from the pool based on the specified routing target.
    pub(crate) fn pick_channel_for_target(
        &self,
        target: &ChannelTarget<'_>,
    ) -> Option<ChannelLease> {
        match target {
            ChannelTarget::Affinity(affinity) => self.resolve_affinity(affinity),
            ChannelTarget::Any => self.pick_channel(),
        }
    }

    /// Resolves an affinity handle to a leased channel.
    ///
    /// # Transaction Affinity Routing Invariants
    ///
    /// 1. **Hard Stickiness (Read/Write Transactions)**:
    ///    - Multi-statement Read/Write transactions in Spanner are owned by a SpanFE,
    ///      and all statements within the transaction must reach the same SpanFE.
    ///    - If the pinned channel transitions to `ChannelState::Draining` during scale-down, R/W
    ///      transactions continue using the draining channel until transaction completion or until
    ///      Spanner's 10-second idle server abort timeout elapses.
    ///
    /// 2. **Soft Stickiness (Read-Only Transactions)**:
    ///    - Multi-use Read-Only transactions do not hold server locks and can execute across any
    ///      SpanFE. They prefer soft stickiness for cache and connection warmth.
    ///    - If their pinned channel begins draining, Read-Only transactions do not hold up draining;
    ///      they seamlessly switch to a fresh channel in `active_entries`.
    pub(crate) fn resolve_affinity(&self, affinity: &TransactionAffinity) -> Option<ChannelLease> {
        let current_id = affinity.pinned_entry_id();

        let active_guard = self.inner.read_active_entries();

        if active_guard.is_empty() {
            return None;
        }

        if let Some(id) = current_id {
            // 1. Fast-path: Check active_entries for pinned channel (using monotonic internal id)
            if let Some(entry) = active_guard
                .iter()
                .find(|entry| entry.id == id && entry.is_active())
            {
                let lease = self.make_lease(Arc::clone(entry));
                if affinity.is_read_write() {
                    affinity.ensure_rw_guard(&lease);
                }
                return Some(lease);
            }

            // 2. Draining-path: Only Read/Write transactions (hard stickiness) preserve draining affinity.
            // Read-Only transactions (soft stickiness) bypass draining channels and pick a fresh active channel.
            if affinity.is_read_write()
                && let Some(entry) = self.find_draining_entry(id)
            {
                let lease = self.make_lease(entry);
                affinity.ensure_rw_guard(&lease);
                return Some(lease);
            }
        }

        // 3. Selection: Select a fresh channel from active_entries (unpinned or soft stickiness fallback).
        let lease = self.pick_from_slice(&active_guard)?;
        let expected_id = current_id.unwrap_or(0);

        // Atomically attempt to pin this channel. If another concurrent thread pinned first,
        // use the winning channel to ensure all concurrent statements route to the same SpanFE.
        let final_lease = match affinity.compare_and_set_entry_id(expected_id, lease.entry_id()) {
            Ok(()) => lease,
            Err(winner_id) => self.resolve_cas_conflict(affinity, lease, &active_guard, winner_id),
        };

        if affinity.is_read_write() {
            affinity.ensure_rw_guard(&final_lease);
        }

        Some(final_lease)
    }

    /// Resolves a compare-and-swap (CAS) conflict when two or more concurrent tasks attempt to
    /// pin or re-pin channel affinity for the same transaction simultaneously.
    ///
    /// # Background and Motivation
    ///
    /// When concurrent tasks execute statements within an unpinned transaction, each task independently
    /// selects an active channel candidate from the pool and attempts to atomically record its choice
    /// via CAS on the shared [`TransactionAffinity`] handle. The first task to succeed wins and establishes
    /// the transaction's channel pin.
    ///
    /// Any losing task arrives here with the winning channel ID (`winner_id`). To guarantee that all
    /// statements within the transaction route to the same Spanner frontend (SpanFE) server (preserving
    /// in-memory lock state and avoiding transaction aborted errors), the losing task discards its
    /// independently selected lease and adopts the winning channel.
    ///
    /// # Handling Stale or Closed Winner Channels
    ///
    /// If the winning channel is no longer active in `active_candidates`:
    /// 1. **Read/Write transactions (hard stickiness)**: Checks if the winning channel transitioned to
    ///    `Draining`. If so, hard affinity requires following that channel to avoid aborting in-flight work.
    /// 2. **Unusable winner**: If the winner is closed (or is draining for Read-Only transactions which use
    ///    soft stickiness), it cannot be used. In this case, this task attempts to re-pin affinity to its
    ///    own active `lease` via CAS.
    /// 3. **Retry on contention**: If re-pinning encounters another concurrent CAS race, the loop retries
    ///    with the new winning entry ID.
    pub(crate) fn resolve_cas_conflict(
        &self,
        affinity: &TransactionAffinity,
        lease: ChannelLease,
        active_candidates: &[Arc<ChannelEntry>],
        mut winner_id: u64,
    ) -> ChannelLease {
        loop {
            // 1. If the winning channel is still active in the candidate set, adopt it.
            if let Some(winner_entry) = active_candidates
                .iter()
                .find(|entry| entry.id == winner_id && entry.is_active())
            {
                return self.make_lease(Arc::clone(winner_entry));
            }

            // 2. Read/Write transactions preserve draining channels under hard stickiness.
            if affinity.is_read_write()
                && let Some(entry) = self.find_draining_entry(winner_id)
            {
                return self.make_lease(entry);
            }

            // 3. The winner channel is unusable (closed, or draining for Read-Only soft stickiness).
            // Attempt to re-pin affinity to our active lease.
            match affinity.compare_and_set_entry_id(winner_id, lease.entry_id()) {
                Ok(()) => return lease,
                Err(new_winner_id) => winner_id = new_winner_id,
            }
        }
    }

    /// Finds a channel entry in the draining pool by ID, if present and still draining.
    fn find_draining_entry(&self, entry_id: u64) -> Option<Arc<ChannelEntry>> {
        let draining_guard = self.inner.read_draining_entries();
        draining_guard
            .iter()
            .find(|entry| entry.id == entry_id && entry.is_draining())
            .cloned()
    }

    fn pick_from_slice(&self, candidates: &[Arc<ChannelEntry>]) -> Option<ChannelLease> {
        if candidates.is_empty() {
            return None;
        }

        // Score candidates by effective load (in-flight + error penalty).
        // On a tie in load, PowerOfTwoSelector breaks ties uniformly at random between the sampled
        // candidates, distributing traffic and warmth across all channels and preventing the
        // "hot-channel trap" under sequential traffic patterns.
        let selected_index = self
            .inner
            .selector
            .select_index(candidates, |entry| entry.effective_pick_load())?;

        let entry = Arc::clone(&candidates[selected_index]);
        Some(self.make_lease(entry))
    }

    /// Sets the multiplexed session name used for scale-up channel priming and signals the worker.
    pub(crate) fn set_prime_session(&self, session_name: String) {
        {
            let mut prime = self.inner.write_prime_session();
            *prime = Some(session_name);
        }
        {
            let mut last_scale = self.inner.lock_last_scale_up_time();
            *last_scale = None;
        }
        self.inner.scale_up_notify.notify_one();
    }

    /// Returns the total number of active channels in the pool.
    pub(crate) fn active_channel_count(&self) -> usize {
        self.inner.read_active_entries().len()
    }

    /// Returns the total number of draining channels in the pool.
    pub(crate) fn draining_channel_count(&self) -> usize {
        self.inner.read_draining_entries().len()
    }

    /// Returns a clone of the first active channel in the pool, if present.
    ///
    /// # Warning
    ///
    /// This method is intended strictly for internal metadata setup (e.g. configuring
    /// fallback gateway connection endpoints during client initialization).
    ///
    /// **Never** use this method for routing or executing queries or RPCs. Doing so would
    /// bypass load balancing and cause traffic to herd onto the first channel.
    /// Use [`ChannelPool::pick_channel`] for P2C load-balanced channel selection, or
    /// [`ChannelPool::resolve_affinity`] for operations requiring transaction affinity.
    pub(crate) fn default_channel(&self) -> Option<Channel> {
        let active_guard = self.inner.read_active_entries();
        active_guard.first().map(|entry| entry.channel.clone())
    }
}

/// Internal state of the `ChannelPool`.
pub(crate) struct ChannelPoolInner {
    pub(crate) config: ChannelPoolConfig,
    pub(crate) client_config: ClientConfig,
    pub(crate) active_entries: RwLock<Vec<Arc<ChannelEntry>>>,
    pub(crate) draining_entries: RwLock<Vec<Arc<ChannelEntry>>>,
    pub(crate) next_entry_id: AtomicU64,
    pub(crate) scale_up_notify: Arc<Notify>,
    pub(crate) scale_up_requested: AtomicBool,
    #[allow(dead_code)]
    // Retained for RAII drop signaling; read in scaler unit tests via subscribe()
    pub(crate) shutdown_sender: WatchSender<()>,
    pub(crate) last_scale_up_time: Mutex<Option<Instant>>,
    pub(crate) consecutive_low_load_checks: AtomicUsize,
    pub(crate) prime_session: RwLock<Option<String>>,
    pub(crate) selector: PowerOfTwoSelector,
}

impl Drop for ChannelPoolInner {
    fn drop(&mut self) {
        // Wake up any background worker awaiting scale-up notification.
        // Dropping shutdown_sender automatically and persistently notifies all shutdown receivers.
        self.scale_up_notify.notify_waiters();
    }
}

impl ChannelPoolInner {
    /// Acquires a shared read lock on active channel entries, recovering from lock poisoning via `into_inner()`.
    ///
    /// # Poison Recovery Rationale
    /// `active_entries` stores reference-counted `Arc<ChannelEntry>` handles. The underlying `Vec`
    /// remains memory-safe and structurally valid in Rust even if a previous reader/writer thread panicked.
    /// Recovering the guard via `into_inner()` prevents an isolated panic in a caller request or
    /// background scaler task from permanently disabling the channel pool and taking down all client RPCs.
    pub(crate) fn read_active_entries(&self) -> RwLockReadGuard<'_, Vec<Arc<ChannelEntry>>> {
        match self.active_entries.read() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    /// Acquires an exclusive write lock on active channel entries, recovering from lock poisoning via `into_inner()`.
    ///
    /// # Poison Recovery Rationale
    /// See [`read_active_entries`](Self::read_active_entries).
    pub(crate) fn write_active_entries(&self) -> RwLockWriteGuard<'_, Vec<Arc<ChannelEntry>>> {
        match self.active_entries.write() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    /// Acquires a shared read lock on draining channel entries, recovering from lock poisoning via `into_inner()`.
    ///
    /// # Poison Recovery Rationale
    /// `draining_entries` stores reference-counted `Arc<ChannelEntry>` handles being drained. Recovering
    /// via `into_inner()` ensures that draining channel sweeping and affinity lookups continue operating
    /// normally even if an earlier task panicked.
    pub(crate) fn read_draining_entries(&self) -> RwLockReadGuard<'_, Vec<Arc<ChannelEntry>>> {
        match self.draining_entries.read() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    /// Acquires an exclusive write lock on draining channel entries, recovering from lock poisoning via `into_inner()`.
    ///
    /// # Poison Recovery Rationale
    /// See [`read_draining_entries`](Self::read_draining_entries).
    pub(crate) fn write_draining_entries(&self) -> RwLockWriteGuard<'_, Vec<Arc<ChannelEntry>>> {
        match self.draining_entries.write() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    /// Acquires a shared read lock on the prime session name, recovering from lock poisoning via `into_inner()`.
    ///
    /// # Poison Recovery Rationale
    /// `prime_session` holds an `Option<String>` used to prime newly established channels. Recovering
    /// via `into_inner()` ensures that new channels can still read the prime session name even if an earlier
    /// task panicked.
    pub(crate) fn read_prime_session(&self) -> RwLockReadGuard<'_, Option<String>> {
        match self.prime_session.read() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    /// Acquires an exclusive write lock on the prime session name, recovering from lock poisoning via `into_inner()`.
    ///
    /// # Poison Recovery Rationale
    /// See [`read_prime_session`](Self::read_prime_session).
    pub(crate) fn write_prime_session(&self) -> RwLockWriteGuard<'_, Option<String>> {
        match self.prime_session.write() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    /// Acquires an exclusive mutex lock on the last scale-up time, recovering from lock poisoning via `into_inner()`.
    ///
    /// # Poison Recovery Rationale
    /// `last_scale_up_time` protects an `Option<Instant>` cooldown timestamp. The value is always structurally
    /// valid in memory; recovering via `into_inner()` allows scaling cooldown evaluation to proceed safely.
    pub(crate) fn lock_last_scale_up_time(&self) -> MutexGuard<'_, Option<Instant>> {
        match self.last_scale_up_time.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    pub(crate) fn make_guard(&self, entry: Arc<ChannelEntry>) -> ActiveRpcGuard {
        let (step, duration, max_penalty) = match &self.config {
            ChannelPoolConfig::Dynamic(dynamic_config) => (
                dynamic_config.error_penalty_step,
                dynamic_config.error_penalty_duration,
                dynamic_config.error_penalty_max(),
            ),
            ChannelPoolConfig::Static(_) => (0, Duration::ZERO, 0),
        };
        ActiveRpcGuard::new(entry, step, duration, max_penalty)
    }

    /// Finds the lowest available slot number (1..=MAX_SUPPORTED_CHANNELS) not marked occupied in `occupied_slots`.
    ///
    /// Searches 1..=max_channels first; if lower slots are occupied by draining channels,
    /// falls back to temporary higher slots up to MAX_SUPPORTED_CHANNELS to prevent ID collisions.
    pub(crate) fn allocate_slot(occupied_slots: &[bool], max_channels: usize) -> usize {
        (1..=MAX_SUPPORTED_CHANNELS)
            .find(|&slot| !occupied_slots.get(slot).copied().unwrap_or(false))
            .unwrap_or(max_channels)
    }
}

#[cfg(test)]
impl ChannelPool {
    pub(crate) fn config(&self) -> &ChannelPoolConfig {
        &self.inner.config
    }

    pub(crate) fn has_prime_session(&self) -> bool {
        self.inner.read_prime_session().is_some()
    }

    pub(crate) fn prime_session_name(&self) -> Option<String> {
        self.inner.read_prime_session().clone()
    }

    pub(crate) fn active_entries(&self) -> Vec<Arc<ChannelEntry>> {
        self.inner.read_active_entries().clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Response;
    use crate::Result;
    use crate::channel_pool::config::MAX_SUPPORTED_CHANNELS;
    use crate::channel_pool::entry::ChannelState;
    use crate::generated::gapic_dataplane::stub::Spanner as SpannerStub;
    use crate::model::{CreateSessionRequest, Session};
    use google_cloud_gax::error::rpc::Code;
    use google_cloud_gax::options::RequestOptions;
    use std::collections::HashSet;
    use std::fmt::Debug;
    use std::future::{Future, ready};
    use std::panic::{AssertUnwindSafe, catch_unwind};
    use std::sync::atomic::Ordering;
    use tokio::task::JoinSet;

    #[derive(Debug, Default)]
    struct MockSpannerStub;

    impl SpannerStub for MockSpannerStub {
        fn create_session(
            &self,
            _req: CreateSessionRequest,
            _options: RequestOptions,
        ) -> impl Future<Output = Result<Response<Session>>> + Send {
            ready(Ok(Response::from(Session::default())))
        }
    }

    fn create_mock_channel() -> Channel {
        Channel::new_for_test(MockSpannerStub)
    }

    #[test]
    fn traits() {
        static_assertions::assert_impl_all!(ChannelPool: Clone, Debug, Send, Sync);
        static_assertions::assert_impl_all!(ChannelPoolInner: Send, Sync);
    }

    impl ChannelPool {
        fn total_in_flight_rpcs(&self) -> u32 {
            let active_guard = self.inner.read_active_entries();
            active_guard.iter().map(|entry| entry.in_flight()).sum()
        }

        fn clear_prime_session(&self) {
            *self.inner.write_prime_session() = None;
        }
    }

    #[test]
    fn p2c_selection_avoids_loaded_channels_and_distributes_traffic() {
        let client_config = ClientConfig::default();
        let channels = vec![
            create_mock_channel(),
            create_mock_channel(),
            create_mock_channel(),
        ];
        let pool = ChannelPool::new_static(
            channels,
            StaticChannelPoolConfig { num_channels: 3 },
            client_config,
        );

        let lease1 = pool.pick_channel().expect("channel pick should succeed");
        assert!(
            (1..=3).contains(&lease1.channel_id),
            "logical channel ID must be in range 1..=3"
        );

        // Effective load comparison: Channel 1 has high load (10 in flight)
        {
            let active = pool.inner.read_active_entries();
            active[0].in_flight_rpcs.store(10, Ordering::Relaxed);
            active[1].in_flight_rpcs.store(1, Ordering::Relaxed);
            active[2].in_flight_rpcs.store(1, Ordering::Relaxed);
        }

        let selected = pool.pick_channel().expect("pick should succeed");
        assert_ne!(selected.entry_id(), 1, "P2C must avoid loaded channel 1");

        // Uniform distribution check: With all channels at equal 0 load,
        // multiple sequential picks should distribute across different channels.
        {
            let active = pool.inner.read_active_entries();
            active[0].in_flight_rpcs.store(0, Ordering::Relaxed);
            active[1].in_flight_rpcs.store(0, Ordering::Relaxed);
            active[2].in_flight_rpcs.store(0, Ordering::Relaxed);
        }

        let mut picked_ids = HashSet::new();
        for _ in 0..100 {
            if let Some(lease) = pool.pick_channel() {
                picked_ids.insert(lease.entry_id());
            }
        }
        assert_eq!(
            picked_ids.len(),
            3,
            "P2C must distribute traffic across all channels under equal load without hot-channel trapping"
        );
    }

    #[test]
    fn affinity_resolution_and_reset() {
        let client_config = ClientConfig::default();
        let channels = vec![create_mock_channel(), create_mock_channel()];
        let pool = ChannelPool::new_static(
            channels,
            StaticChannelPoolConfig { num_channels: 2 },
            client_config,
        );

        let affinity = TransactionAffinity::new_read_write();
        let lease1 = pool
            .resolve_affinity(&affinity)
            .expect("first resolve succeeds");
        let first_id = lease1.entry_id();
        assert_eq!(
            affinity.pinned_entry_id(),
            Some(first_id),
            "pinned entry ID must match first lease ID"
        );

        // Second resolve reuses pinned channel
        let lease2 = pool
            .resolve_affinity(&affinity)
            .expect("second resolve succeeds");
        assert_eq!(
            lease2.entry_id(),
            first_id,
            "second resolve must reuse pinned entry ID"
        );

        // Reset unpins the affinity handle
        affinity.reset();
        assert_eq!(
            affinity.pinned_entry_id(),
            None,
            "pinned entry ID must be None after reset"
        );
        let lease3 = pool
            .resolve_affinity(&affinity)
            .expect("resolve after reset succeeds");
        assert_eq!(
            affinity.pinned_entry_id(),
            Some(lease3.entry_id()),
            "pinned entry ID must update to new lease ID after reset"
        );
    }

    #[test]
    fn affinity_resolution_preserves_draining_channel_for_read_write() {
        let client_config = ClientConfig::default();
        let channel_1 = Arc::new(ChannelEntry::new(1, 1, create_mock_channel()));
        let channel_2 = Arc::new(ChannelEntry::new(2, 2, create_mock_channel()));

        let pool = ChannelPool::new_static(
            vec![create_mock_channel()],
            StaticChannelPoolConfig { num_channels: 1 },
            client_config,
        );

        // Setup: channel_1 is active, channel_2 is draining
        channel_2.set_state(ChannelState::Draining);
        *pool.inner.write_active_entries() = vec![Arc::clone(&channel_1)];
        *pool.inner.write_draining_entries() = vec![Arc::clone(&channel_2)];

        // Read/Write transaction requires hard stickiness
        let affinity = TransactionAffinity::new_read_write();
        affinity
            .compare_and_set_entry_id(0, 2)
            .expect("pin affinity"); // Pinned to channel 2 (which is draining)

        let lease = pool
            .resolve_affinity(&affinity)
            .expect("must resolve to draining channel 2 for R/W");
        assert_eq!(
            lease.entry_id(),
            2,
            "Must preserve affinity to draining channel for Read/Write transactions"
        );
        assert_eq!(
            channel_2.active_rw_count(),
            1,
            "Draining channel must have active_rw_count = 1 while Read/Write affinity holds guard"
        );
        assert!(
            affinity.has_rw_guard(),
            "Read/Write affinity must hold rw_guard"
        );

        // Releasing the guard decrements active_rw_count to 0 and transitions the
        // affinity handle to the terminal Released state so late calls cannot
        // re-acquire a guard on the draining channel.
        affinity.release_rw_guard();
        assert_eq!(
            channel_2.active_rw_count(),
            0,
            "active_rw_count must decrement to 0 after release_rw_guard"
        );
        assert!(
            !affinity.has_rw_guard(),
            "affinity must not hold rw_guard after release_rw_guard"
        );

        let _late_lease = pool
            .resolve_affinity(&affinity)
            .expect("late resolution on released affinity");
        assert_eq!(
            channel_2.active_rw_count(),
            0,
            "Late resolution on a released affinity must not re-acquire active_rw_count on draining channel 2"
        );
        assert!(
            !affinity.has_rw_guard(),
            "Released affinity must not re-acquire rw_guard"
        );

        // A new transaction attempt uses a fresh TransactionAffinity and selects active channel 1.
        let retry_affinity = TransactionAffinity::new_read_write();
        let next_lease = pool
            .resolve_affinity(&retry_affinity)
            .expect("new attempt resolution must select active channel 1");
        assert_eq!(
            next_lease.entry_id(),
            1,
            "New transaction attempt must select active channel 1 instead of draining channel 2"
        );
        assert_eq!(
            channel_2.active_rw_count(),
            0,
            "Draining channel 2 active_rw_count must remain 0"
        );
        assert_eq!(
            channel_1.active_rw_count(),
            1,
            "Active channel 1 active_rw_count must be 1"
        );
    }

    #[test]
    fn affinity_resolution_soft_stickiness_sheds_draining_channel_for_read_only() {
        let client_config = ClientConfig::default();
        let channel_1 = Arc::new(ChannelEntry::new(1, 1, create_mock_channel()));
        let channel_2 = Arc::new(ChannelEntry::new(2, 2, create_mock_channel()));

        let pool = ChannelPool::new_static(
            vec![create_mock_channel()],
            StaticChannelPoolConfig { num_channels: 1 },
            client_config,
        );

        // Setup: channel_1 is active, channel_2 is draining
        channel_2.set_state(ChannelState::Draining);
        *pool.inner.write_active_entries() = vec![Arc::clone(&channel_1)];
        *pool.inner.write_draining_entries() = vec![Arc::clone(&channel_2)];

        // Simulate a Read-Only transaction that was previously pinned to channel 2,
        // which has now transitioned to Draining during a scale-down event.
        let read_only_affinity = TransactionAffinity::new_read_only();
        read_only_affinity.set_entry_id(2);

        let lease = pool
            .resolve_affinity(&read_only_affinity)
            .expect("must resolve to active channel for Read-Only");
        assert_eq!(
            lease.entry_id(),
            1,
            "Read-Only transaction must switch away from draining channel to active channel 1"
        );
        assert_eq!(
            read_only_affinity.pinned_entry_id(),
            Some(1),
            "Read-Only affinity pin must update to active channel 1"
        );
    }

    #[test]
    fn logical_slot_allocation_and_recycling() {
        // When slots 1, 2, 3 are occupied and max is 4, next slot must be 4
        let mut occupied = [false; MAX_SUPPORTED_CHANNELS + 1];
        occupied[1] = true;
        occupied[2] = true;
        occupied[3] = true;

        let slot = ChannelPoolInner::allocate_slot(&occupied, 4);
        assert_eq!(slot, 4, "Should allocate unused slot 4");

        // When slot 2 is freed
        let mut occupied_recycled = [false; MAX_SUPPORTED_CHANNELS + 1];
        occupied_recycled[1] = true;
        occupied_recycled[3] = true;

        let recycled_slot = ChannelPoolInner::allocate_slot(&occupied_recycled, 4);
        assert_eq!(recycled_slot, 2, "Should recycle lowest available slot 2");

        // When slots 1..=4 are all occupied (e.g. 2 active + 2 draining), should allocate slot 5
        let mut occupied_full = [false; MAX_SUPPORTED_CHANNELS + 1];
        occupied_full[1] = true;
        occupied_full[2] = true;
        occupied_full[3] = true;
        occupied_full[4] = true;

        let overflow_slot = ChannelPoolInner::allocate_slot(&occupied_full, 4);
        assert_eq!(
            overflow_slot, 5,
            "Should allocate slot 5 to avoid collision with draining channels"
        );
    }

    #[tokio::test]
    async fn scale_up_trigger_and_session_registration() {
        let client_config = ClientConfig::default();
        let channels = vec![create_mock_channel(), create_mock_channel()];
        let pool = ChannelPool::new_dynamic(
            channels,
            DynamicChannelPoolConfig {
                initial_channels: 2,
                min_channels: 2,
                max_channels: 10,
                max_rpc_per_channel: 5.0,
                ..Default::default()
            },
            client_config,
        );

        assert!(
            !pool.has_prime_session(),
            "Initial prime session should be None"
        );
        pool.set_prime_session("projects/p/instances/i/databases/d/sessions/s123".to_string());
        assert!(
            pool.has_prime_session(),
            "Prime session must be registered after set_prime_session"
        );

        // Under high load, pick_channel triggers scale-up notification
        {
            let active = pool.inner.read_active_entries();
            active[0].in_flight_rpcs.store(10, Ordering::Relaxed);
            active[1].in_flight_rpcs.store(10, Ordering::Relaxed);
        }

        let _lease = pool.pick_channel().expect("pick succeeds");

        pool.clear_prime_session();
        assert!(
            !pool.has_prime_session(),
            "Prime session must be None after clearing"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_resolve_affinity_pins_same_channel_entry() {
        let channel_1 = Arc::new(ChannelEntry::new(1, 1, create_mock_channel()));
        let channel_2 = Arc::new(ChannelEntry::new(2, 2, create_mock_channel()));
        let channel_3 = Arc::new(ChannelEntry::new(3, 3, create_mock_channel()));

        let pool = ChannelPool::new_static(
            vec![
                create_mock_channel(),
                create_mock_channel(),
                create_mock_channel(),
            ],
            StaticChannelPoolConfig { num_channels: 3 },
            ClientConfig::default(),
        );
        *pool.inner.write_active_entries() = vec![channel_1, channel_2, channel_3];

        let affinity = Arc::new(TransactionAffinity::new_read_write());
        let mut join_set = JoinSet::new();

        for _ in 0..10 {
            let pool_clone = pool.clone();
            let affinity_clone = Arc::clone(&affinity);
            join_set.spawn(async move {
                let lease = pool_clone
                    .resolve_affinity(&affinity_clone)
                    .expect("resolve_affinity must succeed");
                lease.entry_id()
            });
        }

        let mut resolved_ids = Vec::new();
        while let Some(join_result) = join_set.join_next().await {
            resolved_ids.push(join_result.expect("task must succeed"));
        }

        assert_eq!(resolved_ids.len(), 10, "All 10 tasks must complete");
        let first_id = resolved_ids[0];
        for (index, id) in resolved_ids.iter().enumerate() {
            assert_eq!(
                *id, first_id,
                "Concurrent task {index} resolved to entry {id}, but must match first pinned entry {first_id}"
            );
        }
    }

    #[test]
    fn channel_pool_accounting_and_empty_edge_cases() {
        let client_config = ClientConfig::default();
        let channels = vec![create_mock_channel(), create_mock_channel()];
        let pool = ChannelPool::new_static(
            channels,
            StaticChannelPoolConfig { num_channels: 2 },
            client_config.clone(),
        );

        assert_eq!(
            pool.active_channel_count(),
            2,
            "Active channel count must be 2"
        );
        assert_eq!(
            pool.draining_channel_count(),
            0,
            "Draining channel count must be 0"
        );
        assert_eq!(
            pool.total_in_flight_rpcs(),
            0,
            "Initial in-flight RPCs must be 0"
        );

        {
            let active = pool.inner.read_active_entries();
            active[0].in_flight_rpcs.store(3, Ordering::Relaxed);
            active[1].in_flight_rpcs.store(2, Ordering::Relaxed);
        }
        assert_eq!(
            pool.total_in_flight_rpcs(),
            5,
            "Total in-flight RPCs must sum to 5"
        );

        // Empty pool edge cases
        let empty_pool = ChannelPool::new_static(
            Vec::new(),
            StaticChannelPoolConfig { num_channels: 0 },
            client_config,
        );
        assert_eq!(
            empty_pool.active_channel_count(),
            0,
            "empty pool active channel count must be 0"
        );
        assert!(
            empty_pool.pick_channel().is_none(),
            "Pick channel on empty pool must return None"
        );

        let affinity = TransactionAffinity::new_read_write();
        assert!(
            empty_pool.resolve_affinity(&affinity).is_none(),
            "Resolve affinity on empty pool must return None"
        );
    }

    #[test]
    fn affinity_resolution_edge_cases_and_fallbacks() {
        let client_config = ClientConfig::default();
        let channel_1 = Arc::new(ChannelEntry::new(1, 1, create_mock_channel()));
        let channel_2 = Arc::new(ChannelEntry::new(2, 2, create_mock_channel()));
        channel_2.set_state(ChannelState::Closed);

        let pool = ChannelPool::new_static(
            vec![create_mock_channel()],
            StaticChannelPoolConfig { num_channels: 1 },
            client_config,
        );

        *pool.inner.write_active_entries() = vec![Arc::clone(&channel_1)];
        *pool.inner.write_draining_entries() = vec![Arc::clone(&channel_2)];

        // 1. Simulate a Read/Write transaction previously pinned to channel 2,
        // which has now transitioned to Closed after an idle timeout.
        let rw_affinity = TransactionAffinity::new_read_write();
        rw_affinity.set_entry_id(2);
        let lease = pool
            .resolve_affinity(&rw_affinity)
            .expect("must fallback to active channel when draining channel is closed");
        assert_eq!(lease.entry_id(), 1, "Must fallback to active channel 1");
        assert_eq!(
            rw_affinity.pinned_entry_id(),
            Some(1),
            "Affinity pin must be updated to active channel 1"
        );

        // 2. Simulate affinity pinned to a stale / non-existent channel ID -> must fallback to active channel
        let non_existent_affinity = TransactionAffinity::new_read_write();
        non_existent_affinity.set_entry_id(999);
        let lease_fallback = pool
            .resolve_affinity(&non_existent_affinity)
            .expect("must fallback to active channel for unknown channel ID");
        assert_eq!(
            lease_fallback.entry_id(),
            1,
            "must fallback to active channel 1 for unknown entry ID"
        );
        assert_eq!(
            non_existent_affinity.pinned_entry_id(),
            Some(1),
            "affinity pin must update to active channel 1"
        );
    }

    #[test]
    fn resolve_cas_conflict_winner_in_active_candidates() {
        let client_config = ClientConfig::default();
        let channel_1 = Arc::new(ChannelEntry::new(1, 1, create_mock_channel()));
        let channel_2 = Arc::new(ChannelEntry::new(2, 2, create_mock_channel()));

        let pool = ChannelPool::new_static(
            vec![create_mock_channel(), create_mock_channel()],
            StaticChannelPoolConfig { num_channels: 2 },
            client_config,
        );
        let active = vec![Arc::clone(&channel_1), Arc::clone(&channel_2)];
        *pool.inner.write_active_entries() = active.clone();

        let affinity = TransactionAffinity::new_read_write();
        let my_lease = pool.make_lease(Arc::clone(&channel_1));

        let resolved_lease = pool.resolve_cas_conflict(&affinity, my_lease, &active, 2);
        assert_eq!(
            resolved_lease.entry_id(),
            2,
            "Resolved lease must match winner channel entry ID 2"
        );
    }

    #[test]
    fn resolve_cas_conflict_read_write_winner_in_draining_entries() {
        let client_config = ClientConfig::default();
        let channel_1 = Arc::new(ChannelEntry::new(1, 1, create_mock_channel()));
        let channel_2 = Arc::new(ChannelEntry::new(2, 2, create_mock_channel()));
        channel_2.set_state(ChannelState::Draining);

        let pool = ChannelPool::new_static(
            vec![create_mock_channel()],
            StaticChannelPoolConfig { num_channels: 1 },
            client_config,
        );
        let active = vec![Arc::clone(&channel_1)];
        *pool.inner.write_active_entries() = active.clone();
        *pool.inner.write_draining_entries() = vec![Arc::clone(&channel_2)];

        let affinity = TransactionAffinity::new_read_write();
        let my_lease = pool.make_lease(Arc::clone(&channel_1));

        let resolved_lease = pool.resolve_cas_conflict(&affinity, my_lease, &active, 2);
        assert_eq!(
            resolved_lease.entry_id(),
            2,
            "ReadWrite affinity must return lease for draining winner channel 2"
        );
    }

    #[test]
    fn resolve_cas_conflict_read_write_winner_closed_or_unknown() {
        let client_config = ClientConfig::default();
        let channel_1 = Arc::new(ChannelEntry::new(1, 1, create_mock_channel()));
        let channel_2 = Arc::new(ChannelEntry::new(2, 2, create_mock_channel()));
        channel_2.set_state(ChannelState::Closed);

        let pool = ChannelPool::new_static(
            vec![create_mock_channel()],
            StaticChannelPoolConfig { num_channels: 1 },
            client_config,
        );
        let active = vec![Arc::clone(&channel_1)];
        *pool.inner.write_active_entries() = active.clone();
        *pool.inner.write_draining_entries() = vec![Arc::clone(&channel_2)];

        let affinity = TransactionAffinity::new_read_write();
        affinity
            .compare_and_set_entry_id(0, 2)
            .expect("pin affinity to 2");

        let my_lease = pool.make_lease(Arc::clone(&channel_1));

        let resolved_lease = pool.resolve_cas_conflict(&affinity, my_lease, &active, 2);
        assert_eq!(
            resolved_lease.entry_id(),
            1,
            "Must fallback to active lease 1 when winner channel is closed"
        );
        assert_eq!(
            affinity.pinned_entry_id(),
            Some(1),
            "Affinity must be re-pinned to active channel 1"
        );
    }

    #[test]
    fn resolve_cas_conflict_read_only_winner_not_active() {
        let client_config = ClientConfig::default();
        let channel_1 = Arc::new(ChannelEntry::new(1, 1, create_mock_channel()));
        let channel_2 = Arc::new(ChannelEntry::new(2, 2, create_mock_channel()));
        channel_2.set_state(ChannelState::Draining);

        let pool = ChannelPool::new_static(
            vec![create_mock_channel()],
            StaticChannelPoolConfig { num_channels: 1 },
            client_config,
        );
        let active = vec![Arc::clone(&channel_1)];
        *pool.inner.write_active_entries() = active.clone();
        *pool.inner.write_draining_entries() = vec![Arc::clone(&channel_2)];

        let affinity = TransactionAffinity::new_read_only();
        affinity
            .compare_and_set_entry_id(0, 2)
            .expect("pin affinity to 2");

        let my_lease = pool.make_lease(Arc::clone(&channel_1));

        let resolved_lease = pool.resolve_cas_conflict(&affinity, my_lease, &active, 2);
        assert_eq!(
            resolved_lease.entry_id(),
            1,
            "ReadOnly affinity must fallback to active lease 1 when winner is not active"
        );
        assert_eq!(
            affinity.pinned_entry_id(),
            Some(1),
            "Affinity must be re-pinned to active channel 1"
        );
    }

    #[test]
    fn resolve_cas_conflict_cas_failure_loops_and_resolves_with_new_winner() {
        let client_config = ClientConfig::default();
        let channel_1 = Arc::new(ChannelEntry::new(1, 1, create_mock_channel()));
        let channel_2 = Arc::new(ChannelEntry::new(2, 2, create_mock_channel()));
        channel_2.set_state(ChannelState::Closed);
        let channel_3 = Arc::new(ChannelEntry::new(3, 3, create_mock_channel()));

        let pool = ChannelPool::new_static(
            vec![create_mock_channel(), create_mock_channel()],
            StaticChannelPoolConfig { num_channels: 2 },
            client_config,
        );
        let active = vec![Arc::clone(&channel_1), Arc::clone(&channel_3)];
        *pool.inner.write_active_entries() = active.clone();
        *pool.inner.write_draining_entries() = vec![Arc::clone(&channel_2)];

        let affinity = TransactionAffinity::new_read_write();
        // Another thread already updated affinity from 2 to 3
        affinity
            .compare_and_set_entry_id(0, 3)
            .expect("pin affinity to 3");

        let my_lease = pool.make_lease(Arc::clone(&channel_1));

        // When this thread tries to resolve conflict with winner_id 2 (which is closed),
        // CAS to re-pin to 1 fails because affinity is 3. The method must loop and resolve with 3!
        let resolved_lease = pool.resolve_cas_conflict(&affinity, my_lease, &active, 2);
        assert_eq!(
            resolved_lease.entry_id(),
            3,
            "Must loop and adopt winning channel 3 when CAS fails during re-pinning"
        );
        assert_eq!(
            affinity.pinned_entry_id(),
            Some(3),
            "Affinity must remain pinned to channel 3"
        );
    }

    #[test]
    fn make_guard_configurations() {
        let channel = create_mock_channel();
        let entry = Arc::new(ChannelEntry::new(1, 1, channel));

        // Static configuration: no error penalty applied
        let static_inner = ChannelPoolInner {
            config: ChannelPoolConfig::Static(StaticChannelPoolConfig::default()),
            client_config: ClientConfig::default(),
            active_entries: RwLock::new(vec![Arc::clone(&entry)]),
            draining_entries: RwLock::new(Vec::new()),
            next_entry_id: AtomicU64::new(2),
            scale_up_notify: Arc::new(Notify::new()),
            scale_up_requested: AtomicBool::new(false),
            shutdown_sender: watch_channel(()).0,
            last_scale_up_time: Mutex::new(None),
            consecutive_low_load_checks: AtomicUsize::new(0),
            prime_session: RwLock::new(None),
            selector: PowerOfTwoSelector::new(),
        };
        let guard = static_inner.make_guard(Arc::clone(&entry));
        guard.record_error_code(Code::Unavailable);
        assert_eq!(
            entry.current_penalty(),
            0,
            "Static pool guard must not accumulate error penalty"
        );
        drop(guard);

        // Dynamic configuration: error penalty applies
        let dynamic_inner = ChannelPoolInner {
            config: ChannelPoolConfig::Dynamic(DynamicChannelPoolConfig {
                error_penalty_step: 7,
                error_penalty_duration: Duration::from_secs(10),
                max_rpc_per_channel: 30.0,
                ..Default::default()
            }),
            client_config: ClientConfig::default(),
            active_entries: RwLock::new(vec![Arc::clone(&entry)]),
            draining_entries: RwLock::new(Vec::new()),
            next_entry_id: AtomicU64::new(2),
            scale_up_notify: Arc::new(Notify::new()),
            scale_up_requested: AtomicBool::new(false),
            shutdown_sender: watch_channel(()).0,
            last_scale_up_time: Mutex::new(None),
            consecutive_low_load_checks: AtomicUsize::new(0),
            prime_session: RwLock::new(None),
            selector: PowerOfTwoSelector::new(),
        };
        let dynamic_guard = dynamic_inner.make_guard(Arc::clone(&entry));
        dynamic_guard.record_error_code(Code::Unavailable);
        assert_eq!(
            entry.current_penalty(),
            7,
            "Dynamic pool guard must accumulate configured error penalty step of 7"
        );
        drop(dynamic_guard);
    }

    #[test]
    fn channel_pool_helpers() {
        let channels = vec![
            create_mock_channel(),
            create_mock_channel(),
            create_mock_channel(),
        ];
        let pool = ChannelPool::new_static(
            channels,
            StaticChannelPoolConfig { num_channels: 3 },
            ClientConfig::default(),
        );

        assert_eq!(
            pool.active_channel_count(),
            3,
            "active_channel_count must match 3"
        );
        assert!(
            pool.default_channel().is_some(),
            "default_channel must return the first channel"
        );
        assert!(
            matches!(pool.config(), ChannelPoolConfig::Static(_)),
            "pool.config() must return configured static pool config"
        );
        assert_eq!(
            pool.active_entries().len(),
            3,
            "pool.active_entries() must return active channel entries"
        );

        let empty_pool = ChannelPool::new_static(
            Vec::new(),
            StaticChannelPoolConfig { num_channels: 0 },
            ClientConfig::default(),
        );
        assert!(
            empty_pool.default_channel().is_none(),
            "default_channel must return None on empty pool"
        );
    }

    #[test]
    fn channel_pool_debug_formatting() {
        let channels = vec![create_mock_channel(), create_mock_channel()];
        let pool = ChannelPool::new_static(
            channels,
            StaticChannelPoolConfig { num_channels: 2 },
            ClientConfig::default(),
        );

        let debug_output = format!("{pool:?}");
        assert!(
            debug_output.contains("ChannelPool"),
            "debug output should contain struct name: {debug_output}"
        );
        assert!(
            debug_output.contains("active_channels: 2"),
            "debug output should contain active channel count: {debug_output}"
        );
        assert!(
            debug_output.contains("draining_channels: 0"),
            "debug output should contain draining channel count: {debug_output}"
        );
        assert!(
            debug_output.contains("Static"),
            "debug output should contain config details: {debug_output}"
        );
    }

    #[test]
    fn affinity_resolution_read_only_happy_path() {
        let client_config = ClientConfig::default();
        let channels = vec![create_mock_channel(), create_mock_channel()];
        let pool = ChannelPool::new_static(
            channels,
            StaticChannelPoolConfig { num_channels: 2 },
            client_config,
        );

        let affinity = TransactionAffinity::new_read_only();
        let lease1 = pool
            .resolve_affinity(&affinity)
            .expect("unpinned read-only resolve succeeds");
        assert_eq!(
            affinity.pinned_entry_id(),
            Some(lease1.entry_id()),
            "read-only affinity must pin to first lease"
        );
        assert!(
            !affinity.has_rw_guard(),
            "read-only affinity must not hold rw_guard"
        );

        let lease2 = pool
            .resolve_affinity(&affinity)
            .expect("pinned read-only resolve succeeds");
        assert_eq!(
            lease2.entry_id(),
            lease1.entry_id(),
            "read-only affinity must reuse pinned channel on fast path"
        );
        assert!(
            !affinity.has_rw_guard(),
            "read-only affinity must not hold rw_guard on fast path"
        );
    }

    #[test]
    fn resolve_affinity_cas_conflict_resolution() {
        let client_config = ClientConfig::default();
        let channel_1 = Arc::new(ChannelEntry::new(1, 1, create_mock_channel()));
        let channel_2 = Arc::new(ChannelEntry::new(2, 2, create_mock_channel()));

        let pool = ChannelPool::new_static(
            vec![create_mock_channel(), create_mock_channel()],
            StaticChannelPoolConfig { num_channels: 2 },
            client_config,
        );
        let active = vec![Arc::clone(&channel_1), Arc::clone(&channel_2)];
        *pool.inner.write_active_entries() = active;

        let affinity = TransactionAffinity::new_read_write();
        // Simulate affinity having an unknown stale ID (e.g. 999)
        affinity.set_entry_id(999);

        // Another concurrent thread successfully updates affinity to channel 2
        affinity
            .compare_and_set_entry_id(999, 2)
            .expect("pin affinity to 2");

        // When resolve_affinity attempts compare_and_set_entry_id(999, lease_id),
        // it encounters CAS Err(2) and triggers resolve_cas_conflict internally
        let lease = pool
            .resolve_affinity(&affinity)
            .expect("must resolve affinity via CAS conflict resolution");
        assert_eq!(
            lease.entry_id(),
            2,
            "must adopt winning channel 2 on CAS conflict"
        );
    }

    #[tokio::test]
    async fn mock_stub_create_session() {
        let channel = create_mock_channel();
        let result = channel.inner.create_session().send().await;
        assert!(result.is_ok(), "mock session create must succeed");
    }
    #[test]
    fn empty_pool_pick_channel_for_target() {
        let client_config = ClientConfig::default();
        let pool = ChannelPool::new_static(
            vec![],
            StaticChannelPoolConfig { num_channels: 0 },
            client_config,
        );

        assert!(
            pool.pick_channel().is_none(),
            "pick_channel on empty pool must return None"
        );
        assert!(
            pool.pick_channel_for_target(&ChannelTarget::Any).is_none(),
            "pick_channel_for_target on empty pool must return None"
        );
        let affinity = TransactionAffinity::new_read_write();
        assert!(
            pool.pick_channel_for_target(&ChannelTarget::Affinity(&affinity))
                .is_none(),
            "pick_channel_for_target with affinity on empty pool must return None"
        );
    }

    #[test]
    fn pick_channel_for_target_variants() {
        let client_config = ClientConfig::default();
        let channel = Arc::new(ChannelEntry::new(10, 3, create_mock_channel()));
        let pool = ChannelPool::new_static(
            vec![],
            StaticChannelPoolConfig { num_channels: 0 },
            client_config,
        );
        pool.inner.write_active_entries().push(Arc::clone(&channel));

        // 1. ChannelTarget::Any leases an active channel
        let any_lease = pool.pick_channel_for_target(&ChannelTarget::Any);
        assert!(
            any_lease.is_some(),
            "pick_channel_for_target on ChannelTarget::Any must return a lease"
        );
        assert_eq!(
            any_lease.expect("lease must be present").channel_id,
            3,
            "Leased channel for ChannelTarget::Any must have channel_id 3"
        );

        // 2. ChannelTarget::Affinity with unpinned affinity leases and pins the channel
        let affinity = TransactionAffinity::new_read_write();
        let affinity_lease = pool.pick_channel_for_target(&ChannelTarget::Affinity(&affinity));
        assert!(
            affinity_lease.is_some(),
            "pick_channel_for_target on ChannelTarget::Affinity must return a lease"
        );
        assert_eq!(
            affinity.pinned_entry_id(),
            Some(10),
            "Unpinned affinity must be pinned to channel entry 10 upon lease"
        );
        assert_eq!(
            affinity_lease.expect("lease must be present").channel_id,
            3,
            "Leased channel for affinity target must have channel_id 3"
        );
    }

    #[tokio::test]
    async fn pick_channel_triggers_scale_up_request_on_saturation() {
        let client_config = ClientConfig::default();
        let channels = vec![create_mock_channel()];
        let pool = ChannelPool::new_dynamic(
            channels,
            DynamicChannelPoolConfig {
                initial_channels: 1,
                min_channels: 1,
                max_channels: 4,
                // Setting max_rpc_per_channel to 0.5 ensures 1 in-flight RPC triggers scale-up
                max_rpc_per_channel: 0.5,
                ..Default::default()
            },
            client_config,
        );

        assert!(
            !pool.inner.scale_up_requested.load(Ordering::Acquire),
            "scale_up_requested must be false initially"
        );

        let lease = pool
            .pick_channel()
            .expect("pick_channel on non-empty pool must succeed");
        assert!(
            pool.inner.scale_up_requested.load(Ordering::Acquire),
            "scale_up_requested must be true after picking saturated channel"
        );

        drop(lease);
    }

    #[test]
    fn channel_pool_recovers_from_poisoned_active_entries_lock() {
        let client_config = ClientConfig::default();
        let channels = vec![create_mock_channel(), create_mock_channel()];
        let pool = ChannelPool::new_static(
            channels,
            StaticChannelPoolConfig { num_channels: 2 },
            client_config,
        );

        // Intentionally poison active_entries by panicking while holding an exclusive write lock.
        let panic_result = catch_unwind(AssertUnwindSafe(|| {
            let _guard = pool
                .inner
                .active_entries
                .write()
                .expect("lock active_entries");
            panic!("deliberately poisoning active_entries lock for test");
        }));
        assert!(panic_result.is_err(), "catch_unwind must capture panic");
        assert!(
            pool.inner.active_entries.is_poisoned(),
            "active_entries lock must be poisoned"
        );

        // Verify that operations accessing active_entries recover seamlessly via into_inner().
        assert_eq!(
            pool.active_channel_count(),
            2,
            "active_channel_count must return 2 despite poisoned lock"
        );
        assert_eq!(
            pool.active_entries().len(),
            2,
            "active_entries must return active channels despite poisoned lock"
        );
        let lease = pool
            .pick_channel()
            .expect("pick_channel must succeed despite poisoned lock");
        assert!(
            (1..=2).contains(&lease.channel_id),
            "leased channel ID must be in valid range"
        );
        assert!(
            pool.default_channel().is_some(),
            "default_channel must return a channel despite poisoned lock"
        );

        let affinity = TransactionAffinity::new_read_write();
        let affinity_lease = pool
            .resolve_affinity(&affinity)
            .expect("resolve_affinity must succeed despite poisoned lock");
        assert!(
            (1..=2).contains(&affinity_lease.channel_id),
            "affinity leased channel ID must be in valid range"
        );
        let fast_affinity_lease = pool
            .resolve_affinity(&affinity)
            .expect("resolve_affinity fast path must succeed despite poisoned lock");
        assert_eq!(
            fast_affinity_lease.channel_id, affinity_lease.channel_id,
            "resolve_affinity fast path must return same channel"
        );

        // Read and write accessors must also return valid guards.
        assert_eq!(
            pool.inner.read_active_entries().len(),
            2,
            "read_active_entries must return valid guard"
        );
        // Also verify read-lock poisoning: a panic while holding a read guard poisons the RwLock.
        let read_panic_result = catch_unwind(AssertUnwindSafe(|| {
            let _read_guard = pool.inner.read_active_entries();
            panic!("deliberately poisoning active_entries via read lock");
        }));
        assert!(
            read_panic_result.is_err(),
            "catch_unwind must capture read panic"
        );
        assert!(
            pool.inner.active_entries.is_poisoned(),
            "active_entries lock must remain poisoned after read panic"
        );

        // Subsequent write and read operations must still recover seamlessly.
        assert_eq!(
            pool.inner.write_active_entries().len(),
            2,
            "write_active_entries must recover from read-lock poisoning"
        );
        assert_eq!(
            pool.inner.read_active_entries().len(),
            2,
            "read_active_entries must recover from read-lock poisoning"
        );
    }

    #[test]
    fn channel_pool_recovers_from_poisoned_draining_entries_lock() {
        let client_config = ClientConfig::default();
        let channel_1 = Arc::new(ChannelEntry::new(1, 1, create_mock_channel()));
        let channel_2 = Arc::new(ChannelEntry::new(2, 2, create_mock_channel()));
        channel_2.set_state(ChannelState::Draining);

        let pool = ChannelPool::new_static(
            vec![create_mock_channel()],
            StaticChannelPoolConfig { num_channels: 1 },
            client_config,
        );
        *pool.inner.write_active_entries() = vec![Arc::clone(&channel_1)];
        *pool.inner.write_draining_entries() = vec![Arc::clone(&channel_2)];

        // Intentionally poison draining_entries by panicking while holding an exclusive write lock.
        let panic_result = catch_unwind(AssertUnwindSafe(|| {
            let _guard = pool
                .inner
                .draining_entries
                .write()
                .expect("lock draining_entries");
            panic!("deliberately poisoning draining_entries lock for test");
        }));
        assert!(panic_result.is_err(), "catch_unwind must capture panic");
        assert!(
            pool.inner.draining_entries.is_poisoned(),
            "draining_entries lock must be poisoned"
        );

        // Verify operations accessing draining_entries recover seamlessly via into_inner().
        assert_eq!(
            pool.draining_channel_count(),
            1,
            "draining_channel_count must return 1 despite poisoned lock"
        );
        assert!(
            pool.find_draining_entry(2).is_some(),
            "find_draining_entry must find draining channel 2 despite poisoned lock"
        );

        let rw_affinity = TransactionAffinity::new_read_write();
        rw_affinity
            .compare_and_set_entry_id(0, 2)
            .expect("pin affinity to 2");
        let lease = pool
            .resolve_affinity(&rw_affinity)
            .expect("resolve_affinity must resolve to draining channel despite poisoned lock");
        assert_eq!(
            lease.entry_id(),
            2,
            "must preserve draining channel affinity for R/W"
        );

        let active_candidates = vec![Arc::clone(&channel_1)];
        let conflict_lease = pool.resolve_cas_conflict(
            &rw_affinity,
            pool.make_lease(Arc::clone(&channel_1)),
            &active_candidates,
            2,
        );
        assert_eq!(
            conflict_lease.entry_id(),
            2,
            "resolve_cas_conflict must recover draining channel despite poisoned lock"
        );

        assert_eq!(
            pool.inner.read_draining_entries().len(),
            1,
            "read_draining_entries must return valid guard"
        );
        assert_eq!(
            pool.inner.write_draining_entries().len(),
            1,
            "write_draining_entries must return valid guard"
        );
    }

    #[test]
    fn channel_pool_recovers_from_poisoned_prime_session_lock() {
        let client_config = ClientConfig::default();
        let pool = ChannelPool::new_static(
            vec![create_mock_channel()],
            StaticChannelPoolConfig { num_channels: 1 },
            client_config,
        );

        // Intentionally poison prime_session by panicking while holding an exclusive write lock.
        let panic_result = catch_unwind(AssertUnwindSafe(|| {
            let _guard = pool
                .inner
                .prime_session
                .write()
                .expect("lock prime_session");
            panic!("deliberately poisoning prime_session lock for test");
        }));
        assert!(panic_result.is_err(), "catch_unwind must capture panic");
        assert!(
            pool.inner.prime_session.is_poisoned(),
            "prime_session lock must be poisoned"
        );

        // Operations must recover and allow reading and updating the prime session.
        assert!(
            !pool.has_prime_session(),
            "has_prime_session must return false when None despite poisoned lock"
        );
        assert_eq!(
            pool.prime_session_name(),
            None,
            "prime_session_name must return None despite poisoned lock"
        );

        pool.set_prime_session("projects/p/instances/i/databases/d/sessions/recovered".to_string());
        assert!(
            pool.has_prime_session(),
            "has_prime_session must return true after set_prime_session"
        );
        assert_eq!(
            pool.prime_session_name(),
            Some("projects/p/instances/i/databases/d/sessions/recovered".to_string()),
            "prime_session_name must return updated session name"
        );
    }

    #[test]
    fn channel_pool_recovers_from_poisoned_last_scale_up_time_lock() {
        let client_config = ClientConfig::default();
        let pool = ChannelPool::new_static(
            vec![create_mock_channel()],
            StaticChannelPoolConfig { num_channels: 1 },
            client_config,
        );

        // Set last_scale_up_time to Some(now)
        *pool.inner.lock_last_scale_up_time() = Some(Instant::now());

        // Intentionally poison last_scale_up_time by panicking while holding the lock.
        let panic_result = catch_unwind(AssertUnwindSafe(|| {
            let _guard = pool
                .inner
                .last_scale_up_time
                .lock()
                .expect("lock last_scale_up_time");
            panic!("deliberately poisoning last_scale_up_time lock for test");
        }));
        assert!(panic_result.is_err(), "catch_unwind must capture panic");
        assert!(
            pool.inner.last_scale_up_time.is_poisoned(),
            "last_scale_up_time lock must be poisoned"
        );

        // Operations accessing last_scale_up_time must recover via into_inner().
        assert!(
            pool.inner.lock_last_scale_up_time().is_some(),
            "lock_last_scale_up_time must recover previous timestamp"
        );

        // set_prime_session resets last_scale_up_time to None; must succeed without panic.
        pool.set_prime_session("projects/p/instances/i/databases/d/sessions/s1".to_string());
        assert!(
            pool.inner.lock_last_scale_up_time().is_none(),
            "set_prime_session must reset last_scale_up_time to None despite previous poisoning"
        );
    }
}
