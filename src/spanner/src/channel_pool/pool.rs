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

use crate::channel_pool::affinity::TransactionAffinity;
use crate::channel_pool::config::{
    ChannelPoolConfig, DynamicChannelPoolConfig, MAX_SUPPORTED_CHANNELS, StaticChannelPoolConfig,
};
use crate::channel_pool::entry::{ActiveRpcGuard, ChannelEntry, ChannelLease};
use crate::channel_pool::scaler::{scale_down_monitor_loop, scale_up_worker_loop};
use crate::client::Channel;
use crate::routing::power_of_two_selector::PowerOfTwoSelector;
use gaxi::options::ClientConfig;
use std::sync::atomic::{AtomicU64, AtomicUsize};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};
use tokio::spawn;
use tokio::sync::Notify;
use tokio::sync::watch::{
    Receiver as WatchReceiver, Sender as WatchSender, channel as watch_channel,
};

/// Unified channel pool managing gRPC channels for the Spanner client.
///
/// Supports both fixed-size static pools and dynamically scaling pools with identical caller interfaces.
#[derive(Clone)]
pub(crate) struct ChannelPool {
    pub(crate) inner: Arc<ChannelPoolInner>,
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
            self.inner.scale_up_notify.notify_one();
        }

        ChannelLease::new(guard)
    }

    /// Selects an active channel using Power of Two Least Busy (P2C) selection.
    pub(crate) fn pick_channel(&self) -> Option<ChannelLease> {
        let active_guard = self.inner.active_entries.read().expect("lock poisoned");

        self.pick_from_slice(&active_guard)
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

        let active_guard = self.inner.active_entries.read().expect("lock poisoned");

        if active_guard.is_empty() {
            return None;
        }

        if let Some(id) = current_id {
            // 1. Fast-path: Check active_entries for pinned channel (using monotonic internal id)
            if let Some(entry) = active_guard
                .iter()
                .find(|entry| entry.id == id && entry.is_active())
            {
                return Some(self.make_lease(Arc::clone(entry)));
            }

            // 2. Draining-path: Only Read/Write transactions (hard stickiness) preserve draining affinity.
            // Read-Only transactions (soft stickiness) bypass draining channels and pick a fresh active channel.
            if affinity.is_read_write() {
                let draining_guard = self.inner.draining_entries.read().expect("lock poisoned");
                if let Some(entry) = draining_guard
                    .iter()
                    .find(|entry| entry.id == id && !entry.is_closed())
                {
                    return Some(self.make_lease(Arc::clone(entry)));
                }
            }
        }

        // 3. Selection: Select a fresh channel from active_entries (unpinned or soft stickiness fallback).
        let lease = self.pick_from_slice(&active_guard)?;
        let expected_id = current_id.unwrap_or(0);

        // Atomically attempt to pin this channel. If another concurrent thread pinned first,
        // use the winning channel to ensure all concurrent statements route to the same SpanFE.
        match affinity.compare_and_set_entry_id(expected_id, lease.entry_id()) {
            Ok(()) => Some(lease),
            Err(winner_id) => {
                if let Some(winner_entry) = active_guard
                    .iter()
                    .find(|entry| entry.id == winner_id && entry.is_active())
                {
                    return Some(self.make_lease(Arc::clone(winner_entry)));
                }
                if affinity.is_read_write() {
                    let draining_guard = self.inner.draining_entries.read().expect("lock poisoned");
                    if let Some(entry) = draining_guard
                        .iter()
                        .find(|entry| entry.id == winner_id && !entry.is_closed())
                    {
                        return Some(self.make_lease(Arc::clone(entry)));
                    }
                }
                Some(lease)
            }
        }
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
            let mut prime = self.inner.prime_session.write().expect("lock poisoned");
            *prime = Some(session_name);
        }
        {
            let mut last_scale = self.inner.last_scale_up_time.lock().expect("lock poisoned");
            *last_scale = None;
        }
        self.inner.scale_up_notify.notify_one();
    }

    /// Clears the cached prime session name.
    pub(crate) fn clear_prime_session(&self) {
        let mut prime = self.inner.prime_session.write().expect("lock poisoned");
        *prime = None;
    }

    /// Checks if a valid multiplexed session name is currently registered.
    pub(crate) fn has_prime_session(&self) -> bool {
        self.inner
            .prime_session
            .read()
            .expect("lock poisoned")
            .is_some()
    }

    /// Returns the total number of active channels in the pool.
    pub(crate) fn active_channel_count(&self) -> usize {
        self.inner
            .active_entries
            .read()
            .expect("lock poisoned")
            .len()
    }

    /// Returns the total number of draining channels in the pool.
    pub(crate) fn draining_channel_count(&self) -> usize {
        self.inner
            .draining_entries
            .read()
            .expect("lock poisoned")
            .len()
    }

    /// Returns the total count of in-flight RPCs across all active channels.
    pub(crate) fn total_in_flight_rpcs(&self) -> u32 {
        let active_guard = self.inner.active_entries.read().expect("lock poisoned");
        active_guard.iter().map(|entry| entry.in_flight()).sum()
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
        static_assertions::assert_impl_all!(ChannelPool: Clone, Send, Sync);
        static_assertions::assert_impl_all!(ChannelPoolInner: Send, Sync);
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
            (1..=3).contains(&lease1.logical_channel_id()),
            "logical channel ID must be in range 1..=3"
        );

        // Effective load comparison: Channel 1 has high load (10 in flight)
        {
            let active = pool.inner.active_entries.read().expect("lock poisoned");
            active[0].in_flight_rpcs.store(10, Ordering::Relaxed);
            active[1].in_flight_rpcs.store(1, Ordering::Relaxed);
            active[2].in_flight_rpcs.store(1, Ordering::Relaxed);
        }

        let selected = pool.pick_channel().expect("pick should succeed");
        assert_ne!(selected.entry_id(), 1, "P2C must avoid loaded channel 1");

        // Uniform distribution check: With all channels at equal 0 load,
        // multiple sequential picks should distribute across different channels.
        {
            let active = pool.inner.active_entries.read().expect("lock poisoned");
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
        *pool.inner.active_entries.write().expect("lock") = vec![Arc::clone(&channel_1)];
        *pool.inner.draining_entries.write().expect("lock") = vec![Arc::clone(&channel_2)];

        // Read/Write transaction requires hard stickiness
        let affinity = TransactionAffinity::new_read_write();
        affinity.set_entry_id(2); // Pinned to channel 2 (which is draining)

        let lease = pool
            .resolve_affinity(&affinity)
            .expect("must resolve to draining channel 2 for R/W");
        assert_eq!(
            lease.entry_id(),
            2,
            "Must preserve affinity to draining channel for Read/Write transactions"
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
        *pool.inner.active_entries.write().expect("lock") = vec![Arc::clone(&channel_1)];
        *pool.inner.draining_entries.write().expect("lock") = vec![Arc::clone(&channel_2)];

        // Read-Only transaction uses soft stickiness
        let read_only_affinity = TransactionAffinity::new_read_only();
        read_only_affinity.set_entry_id(2); // Was pinned to channel 2 (now draining)

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
            let active = pool.inner.active_entries.read().expect("lock poisoned");
            active[0].in_flight_rpcs.store(10, Ordering::Relaxed);
            active[1].in_flight_rpcs.store(10, Ordering::Relaxed);
        }

        let _lease = pool.pick_channel().expect("pick succeeds");

        pool.clear_prime_session();
        assert!(
            !pool.has_prime_session(),
            "Prime session must be None after clear_prime_session"
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
        *pool.inner.active_entries.write().expect("lock") = vec![channel_1, channel_2, channel_3];

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
            let active = pool.inner.active_entries.read().expect("lock poisoned");
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

        *pool.inner.active_entries.write().expect("lock") = vec![Arc::clone(&channel_1)];
        *pool.inner.draining_entries.write().expect("lock") = vec![Arc::clone(&channel_2)];

        // 1. ReadWrite affinity pinned to a Closed draining channel -> must fallback to active channel
        let rw_affinity = TransactionAffinity::new_read_write();
        rw_affinity.set_entry_id(2); // Channel 2 is closed
        let lease = pool
            .resolve_affinity(&rw_affinity)
            .expect("must fallback to active channel when draining channel is closed");
        assert_eq!(lease.entry_id(), 1, "Must fallback to active channel 1");
        assert_eq!(
            rw_affinity.pinned_entry_id(),
            Some(1),
            "Affinity pin must be updated to active channel 1"
        );

        // 2. Affinity pinned to a non-existent channel ID -> must fallback to active channel
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
}
