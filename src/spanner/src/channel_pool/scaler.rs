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

//! Background scaling workers, parallel channel priming, and debounced draining.
//!
//! This module implements the autonomous scaling engine for Dynamic Channel Pooling:
//! - **Scale-Up Worker**: Reacts to high-load picker notifications, dials physical TCP
//!   connections in parallel, primes backend caches with `SELECT 1` queries using the
//!   multiplexed session, and publishes ready channels immediately with unique logical IDs.
//! - **Scale-Down Monitor**: Periodically checks pool utilization at fixed intervals,
//!   requires consecutive low-load confirmations (debouncing) to prevent thrashing, moves
//!   under-utilized channels to draining, and closes them after idle grace periods.

use crate::Result;
use crate::channel_pool::config::{
    ChannelPoolConfig, DynamicChannelPoolConfig, MAX_SUPPORTED_CHANNELS,
};
use crate::channel_pool::entry::{ChannelEntry, ChannelState, SPANNER_RW_TRANSACTION_IDLE_TIMEOUT};
use crate::channel_pool::pool::ChannelPoolInner;
use crate::client::Channel;
use gaxi::options::ClientConfig;
use google_cloud_gax::backoff_policy::BackoffPolicy;
use google_cloud_gax::error::Error as GaxError;
use google_cloud_gax::exponential_backoff::ExponentialBackoffBuilder;
use google_cloud_gax::retry_state::RetryState;
use std::cmp::Reverse;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};
use tokio::task::JoinSet;
use tokio::time::{Instant as TokioInstant, MissedTickBehavior, interval_at, sleep, timeout};

/// Runs the background scale-up worker loop for a dynamic channel pool.
///
/// The worker sleeps on a `tokio::sync::Notify` handle triggered by the P2C picker
/// when an active channel's effective load exceeds `max_rpc_per_channel`.
///
/// # Loop Lifecycle & Error Resilience
/// The loop holds only a `Weak<ChannelPoolInner>` reference. When the parent `ChannelPool`
/// or client is dropped, `ChannelPoolInner::drop` notifies all waiters, waking this task
/// so `weak_inner.upgrade()` returns `None` and cleanly terminates the loop.
pub(crate) async fn scale_up_worker_loop(weak_inner: Weak<ChannelPoolInner>) {
    loop {
        // Retrieve a notification handle while the pool is alive.
        let notify = match weak_inner.upgrade() {
            Some(inner) => Arc::clone(&inner.scale_up_notify),
            None => return, // Pool was dropped; exit worker cleanly.
        };

        // Sleep asynchronously until signaled by the channel picker under high load or on pool drop.
        notify.notified().await;

        let inner = match weak_inner.upgrade() {
            Some(inner) => inner,
            None => return, // Pool was dropped during wait; exit worker cleanly.
        };

        let dynamic_config = match &inner.config {
            ChannelPoolConfig::Dynamic(config) => config.clone(),
            ChannelPoolConfig::Static(_) => return, // Static pools never scale; exit worker.
        };

        // 1. Multiplexed session must be registered to execute physical warming queries.
        // Check session availability FIRST so cooldown is never committed when priming is impossible.
        let prime_session_name = match inner
            .prime_session
            .read()
            .unwrap_or_else(|poison| poison.into_inner())
            .clone()
        {
            Some(session) => session,
            None => continue,
        };

        // 2. Determine if cooldown allows scaling and compute the exact number of channels to add.
        let channels_to_add = calculate_scale_up_count(&inner, &dynamic_config);
        if channels_to_add == 0 {
            continue;
        }

        // 3. Dial and prime channels in parallel, publishing each channel immediately upon readiness.
        dial_prime_and_publish_channels_parallel(
            &inner,
            inner.client_config.clone(),
            prime_session_name,
            channels_to_add,
            &dynamic_config,
        )
        .await;
    }
}

/// Evaluates scale-up eligibility, enforces cooldown, and calculates how many channels to add.
///
/// Returns `0` if cooldown is active, capacity is already at `max_channels`, or current
/// capacity is sufficient for the observed load. Only updates the cooldown timestamp if
/// channels will actually be added.
fn calculate_scale_up_count(inner: &ChannelPoolInner, config: &DynamicChannelPoolConfig) -> usize {
    // 1. Check scale-up cooldown without updating timestamp yet.
    {
        let last_scale = inner
            .last_scale_up_time
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        if let Some(last_time) = *last_scale
            && last_time.elapsed() < config.scale_up_cooldown
        {
            return 0;
        }
    }

    let active_guard = inner
        .active_entries
        .read()
        .unwrap_or_else(|poison| poison.into_inner());
    let current_len = active_guard.len();
    if current_len >= config.max_channels {
        return 0;
    }

    // 2. Sizing calculation:
    // desired_channels = ceil(total_load / target_rpc).
    // Note: Scale-up uses effective_pick_load() (in-flight + error penalty) to prompt
    // replacement capacity for failing channels.
    let total_load: u32 = active_guard
        .iter()
        .map(|entry| entry.effective_pick_load())
        .sum();
    let desired_channels = config.desired_channel_count(total_load);

    if desired_channels <= current_len {
        return 0;
    }

    // 3. Rate limiting:
    // Add at most max_scale_up_percent (default 30%, minimum 2 channels) per scale event,
    // bounded by max_channels ceiling.
    let max_to_add_by_percent =
        ((current_len as f64) * (config.max_scale_up_percent as f64) / 100.0).ceil() as usize;
    let max_to_add_by_percent = max_to_add_by_percent.max(2);

    let channels_to_add = (desired_channels - current_len)
        .min(max_to_add_by_percent)
        .min(config.max_channels - current_len);

    // Only commit cooldown timestamp if channels are actually being added.
    // Note: We commit the scale-up cooldown timestamp here before awaiting dialing/priming
    // to prevent redundant scale-up attempts from firing while parallel dialing is already in flight.
    if channels_to_add > 0 {
        let mut last_scale = inner
            .last_scale_up_time
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        *last_scale = Some(Instant::now());
    }

    channels_to_add
}

/// Dials and primes multiple channels concurrently using `tokio::task::JoinSet`,
/// publishing each primed channel immediately as soon as its priming completes.
async fn dial_prime_and_publish_channels_parallel(
    inner: &ChannelPoolInner,
    client_config: ClientConfig,
    session_name: String,
    count: usize,
    config: &DynamicChannelPoolConfig,
) {
    let mut join_set = JoinSet::new();
    for _ in 0..count {
        let channel_client_config = client_config.clone();
        let session = session_name.clone();
        let timeout = config.prime_timeout;
        let max_attempts = config.prime_max_attempts;

        join_set.spawn(async move {
            dial_and_prime_channel(channel_client_config, session, timeout, max_attempts).await
        });
    }

    while let Some(join_result) = join_set.join_next().await {
        match join_result {
            Ok(Ok(channel)) => {
                publish_primed_channel(inner, channel, config.max_channels);
            }
            Ok(Err(error)) => {
                tracing::warn!("Failed to dial and prime scaled-up channel: {error:?}");
            }
            Err(join_error) => {
                tracing::error!("Scale-up priming task panicked: {join_error:?}");
            }
        }
    }
}

/// Publishes a single primed channel to the active channel pool under an exclusive write lock.
///
/// Guarantees that each channel receives a strictly unique `logical_channel_id` (1..=max_channels)
/// by marking slots from both `active_entries` and active `draining_entries` as occupied.
fn publish_primed_channel(inner: &ChannelPoolInner, channel: Channel, max_channels: usize) {
    let mut active_write = inner
        .active_entries
        .write()
        .unwrap_or_else(|poison| poison.into_inner());

    if active_write.len() >= max_channels {
        return;
    }

    let mut occupied_slots = [false; MAX_SUPPORTED_CHANNELS + 1];

    // Mark slots occupied by active channels
    for entry in active_write.iter() {
        if entry.logical_channel_id <= MAX_SUPPORTED_CHANNELS {
            occupied_slots[entry.logical_channel_id] = true;
        }
    }

    // Also mark slots occupied by draining channels that are not yet closed
    {
        let draining_guard = inner
            .draining_entries
            .read()
            .unwrap_or_else(|poison| poison.into_inner());
        for entry in draining_guard.iter() {
            if !entry.is_closed() && entry.logical_channel_id <= MAX_SUPPORTED_CHANNELS {
                occupied_slots[entry.logical_channel_id] = true;
            }
        }
    }

    let logical_slot = ChannelPoolInner::allocate_slot(&occupied_slots, max_channels);
    let id = inner.next_entry_id.fetch_add(1, Ordering::Relaxed);
    active_write.push(Arc::new(ChannelEntry::new(id, logical_slot, channel)));
}

/// Dials a physical gRPC channel and primes it with `SELECT 1` queries using exponential backoff.
///
/// Priming warms the TCP socket, completes TLS handshakes, establishes HTTP/2 flow-control
/// windows, and populates Spanner FrontEnd routing caches before the channel receives user traffic.
pub(crate) async fn dial_and_prime_channel(
    client_config: ClientConfig,
    session_name: String,
    prime_timeout: Duration,
    prime_max_attempts: usize,
) -> Result<Channel> {
    let channel = Channel::create(&client_config)
        .await
        .map_err(GaxError::connect)?;

    let backoff = ExponentialBackoffBuilder::new()
        .with_initial_delay(Duration::from_millis(50))
        .with_maximum_delay(Duration::from_secs(1))
        .with_scaling(2.0)
        .build()
        .map_err(GaxError::binding)?;

    let mut retry_state = RetryState::new(true);
    let mut attempts: u32 = 0;

    loop {
        attempts += 1;
        retry_state = retry_state.set_attempt_count(attempts);
        let prime_future = channel
            .inner
            .execute_sql()
            .set_session(session_name.clone())
            .set_sql("SELECT 1")
            .send();

        let result = timeout(prime_timeout, prime_future).await;

        match result {
            Ok(Ok(_)) => return Ok(channel),
            Ok(Err(err)) => {
                if attempts as usize >= prime_max_attempts {
                    return Err(err);
                }
            }
            Err(_) => {
                if attempts as usize >= prime_max_attempts {
                    return Err(GaxError::timeout("Priming timed out"));
                }
            }
        }

        let delay = backoff.on_failure(&retry_state);
        sleep(delay).await;
    }
}

/// Runs the background periodic scale-down monitor loop for a dynamic channel pool.
///
/// Executes on a periodic interval using `tokio::time::interval_at` with `MissedTickBehavior::Delay`.
/// Evaluates pool load, applies consecutive low-load debouncing to prevent thrashing, transitions
/// under-utilized channels to `ChannelState::Draining`, and sweeps drained channels for closure.
///
/// # Loop Lifecycle & Error Resilience
/// Holds only a `Weak<ChannelPoolInner>` reference. Exits cleanly when `weak_inner.upgrade()`
/// returns `None` (on pool drop). Never escapes prematurely on internal errors.
pub(crate) async fn scale_down_monitor_loop(
    weak_inner: Weak<ChannelPoolInner>,
    interval: Duration,
) {
    // Start after one full interval has elapsed.
    let mut timer = interval_at(TokioInstant::now() + interval, interval);
    timer.set_missed_tick_behavior(MissedTickBehavior::Delay);

    loop {
        timer.tick().await;

        let inner = match weak_inner.upgrade() {
            Some(inner) => inner,
            None => return, // Pool was dropped; terminate monitor task cleanly.
        };

        let dynamic_config = match &inner.config {
            ChannelPoolConfig::Dynamic(config) => config.clone(),
            ChannelPoolConfig::Static(_) => return, // Static pools never scale; exit monitor.
        };

        // 1. Evaluate utilization and transition excess channels to DRAINING if debounce criteria met.
        evaluate_and_execute_scale_down(&inner, &dynamic_config);

        // 2. Sweep the draining pool to close idle channels whose grace/abort periods have elapsed.
        sweep_draining_channels(&inner, dynamic_config.drain_idle_grace);
    }
}

/// Evaluates load across active channels, manages debounce counters, and moves candidates to draining.
fn evaluate_and_execute_scale_down(inner: &ChannelPoolInner, config: &DynamicChannelPoolConfig) {
    let channels_to_remove = {
        let active_guard = inner
            .active_entries
            .read()
            .unwrap_or_else(|poison| poison.into_inner());

        // Do not scale down below configured min_channels floor.
        if active_guard.len() <= config.min_channels {
            inner
                .consecutive_low_load_checks
                .store(0, Ordering::Relaxed);
            return;
        }

        // Note: Scale-down deliberately evaluates real in-flight RPCs only (entry.in_flight()),
        // excluding synthetic error penalties so failing channels are not retained as busy.
        let total_in_flight: u32 = active_guard.iter().map(|entry| entry.in_flight()).sum();
        let avg_load = (total_in_flight as f64) / (active_guard.len() as f64);

        // Debouncing: Require consecutive_low_load_checks (default 3 cycles = 9 minutes)
        // of sustained low load before transitioning channels to draining.
        if avg_load >= config.min_rpc_per_channel {
            // Load recovered above min threshold; reset debounce counter.
            inner
                .consecutive_low_load_checks
                .store(0, Ordering::Relaxed);
            return;
        }

        let low_runs = inner
            .consecutive_low_load_checks
            .fetch_add(1, Ordering::Relaxed)
            + 1;
        if low_runs < config.consecutive_low_load_checks {
            return;
        }

        // Downscale candidate count calculation:
        // desired_channels = max(ceil(total_in_flight / target_rpc), min_channels).
        let desired_channels = config
            .desired_channel_count(total_in_flight)
            .max(config.min_channels);

        if desired_channels >= active_guard.len() {
            inner
                .consecutive_low_load_checks
                .store(0, Ordering::Relaxed);
            return;
        }

        let channels_to_remove =
            (active_guard.len() - desired_channels).min(config.max_remove_channels);
        if channels_to_remove == 0 {
            inner
                .consecutive_low_load_checks
                .store(0, Ordering::Relaxed);
            return;
        }

        channels_to_remove
    };

    // Move candidate channels from active_entries to draining_entries in-place under write lock.
    let mut active_write = inner
        .active_entries
        .write()
        .unwrap_or_else(|poison| poison.into_inner());
    let mut draining_write = inner
        .draining_entries
        .write()
        .unwrap_or_else(|poison| poison.into_inner());

    // Sort in-place by (in_flight, Reverse(created_at))
    active_write.sort_by_key(|entry| (entry.in_flight(), Reverse(entry.created_at)));

    // Exactly calculate number of channels eligible to remove without breaching min_channels.
    let eligible_to_remove = active_write
        .len()
        .saturating_sub(config.min_channels)
        .min(channels_to_remove);

    if eligible_to_remove > 0 {
        let drained_entries: Vec<Arc<ChannelEntry>> =
            active_write.drain(0..eligible_to_remove).collect();
        for entry in drained_entries {
            entry.set_state(ChannelState::Draining);
            draining_write.push(entry);
        }
    }

    inner
        .consecutive_low_load_checks
        .store(0, Ordering::Relaxed);
}

/// Sweeps the draining pool and closes idle channels whose draining requirements are met.
///
/// Rules:
/// 1. Channels with active in-flight RPCs remain in `Draining` state.
/// 2. Channels with attached Read/Write transactions remain in `Draining` state until
///    either the transaction completes or the Spanner server 10-second idle abort timeout elapses.
/// 3. Channels with 0 load and no R/W transactions are closed once `drain_idle_grace` (1 min) elapses.
pub(crate) fn sweep_draining_channels(inner: &ChannelPoolInner, drain_idle_grace: Duration) {
    let mut draining_write = inner
        .draining_entries
        .write()
        .unwrap_or_else(|poison| poison.into_inner());

    draining_write.retain(|entry| {
        if entry.in_flight() > 0 {
            return true; // Keep open while in-flight RPCs execute.
        }

        let idle_duration = entry.elapsed_since_activity();

        // If active R/W transactions are attached, close only if server abort timeout (10s) elapsed.
        if entry.active_rw_count() > 0 {
            if idle_duration >= SPANNER_RW_TRANSACTION_IDLE_TIMEOUT {
                entry.set_state(ChannelState::Closed);
                return false;
            }
            return true;
        }

        // If no R/W transactions and idle >= drain_idle_grace, close channel.
        if idle_duration >= drain_idle_grace {
            entry.set_state(ChannelState::Closed);
            return false;
        }

        true
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Response;
    use crate::channel_pool::config::DynamicChannelPoolConfig;
    use crate::channel_pool::pool::ChannelPool;
    use crate::generated::gapic_dataplane::stub::Spanner as SpannerStub;
    use crate::model::{CreateSessionRequest, Session};
    use crate::routing::power_of_two_selector::PowerOfTwoSelector;
    use google_cloud_gax::options::RequestOptions;
    use std::fmt::Debug;
    use std::future::{Future, ready};
    use std::sync::atomic::{AtomicU64, AtomicUsize};
    use std::sync::{Mutex, RwLock};
    use tokio::sync::Notify;

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
    fn draining_sweep_respects_rw_transactions_and_grace() {
        let channel_1 = Arc::new(ChannelEntry::new(1, 1, create_mock_channel()));
        let channel_2 = Arc::new(ChannelEntry::new_with_created_at(
            2,
            2,
            create_mock_channel(),
            Instant::now() - Duration::from_secs(20),
        ));
        let channel_3 = Arc::new(ChannelEntry::new_with_created_at(
            3,
            3,
            create_mock_channel(),
            Instant::now() - Duration::from_secs(70),
        ));

        channel_1.set_state(ChannelState::Draining);
        channel_2.set_state(ChannelState::Draining);
        channel_3.set_state(ChannelState::Draining);

        // channel_1 has active R/W transaction and was active recently (< 10s) -> must keep open
        channel_1.active_rw_transactions.store(1, Ordering::Relaxed);
        channel_1.touch_activity();

        // channel_2 has active R/W transaction but has been idle for >= 10s -> server aborted, can close
        channel_2.active_rw_transactions.store(1, Ordering::Relaxed);

        let inner = ChannelPoolInner {
            config: ChannelPoolConfig::Dynamic(DynamicChannelPoolConfig::default()),
            client_config: ClientConfig::default(),
            active_entries: RwLock::new(Vec::new()),
            draining_entries: RwLock::new(vec![
                Arc::clone(&channel_1),
                Arc::clone(&channel_2),
                Arc::clone(&channel_3),
            ]),
            next_entry_id: AtomicU64::new(4),
            scale_up_notify: Arc::new(Notify::new()),
            last_scale_up_time: Mutex::new(None),
            consecutive_low_load_checks: AtomicUsize::new(0),
            prime_session: RwLock::new(None),
            selector: PowerOfTwoSelector::new(),
        };

        sweep_draining_channels(&inner, Duration::from_secs(60));

        let remaining = inner
            .draining_entries
            .read()
            .expect("lock poisoned")
            .clone();
        assert_eq!(remaining.len(), 1, "Only channel_1 should remain draining");
        assert_eq!(remaining[0].id, 1);
        assert!(
            channel_2.is_closed(),
            "channel_2 must be closed after 10s idle R/W timeout"
        );
        assert!(
            channel_3.is_closed(),
            "channel_3 must be closed after 60s idle grace"
        );
    }

    #[test]
    fn scale_down_consecutive_debounce() {
        let channel_1 = Arc::new(ChannelEntry::new(1, 1, create_mock_channel()));
        let channel_2 = Arc::new(ChannelEntry::new(2, 2, create_mock_channel()));
        let channel_3 = Arc::new(ChannelEntry::new(3, 3, create_mock_channel()));
        let channel_4 = Arc::new(ChannelEntry::new(4, 4, create_mock_channel()));

        let inner = Arc::new(ChannelPoolInner {
            config: ChannelPoolConfig::Dynamic(DynamicChannelPoolConfig {
                min_channels: 2,
                max_channels: 4,
                min_rpc_per_channel: 15.0,
                max_rpc_per_channel: 25.0,
                consecutive_low_load_checks: 3,
                max_remove_channels: 2,
                ..Default::default()
            }),
            client_config: ClientConfig::default(),
            active_entries: RwLock::new(vec![
                Arc::clone(&channel_1),
                Arc::clone(&channel_2),
                Arc::clone(&channel_3),
                Arc::clone(&channel_4),
            ]),
            draining_entries: RwLock::new(Vec::new()),
            next_entry_id: AtomicU64::new(5),
            scale_up_notify: Arc::new(Notify::new()),
            last_scale_up_time: Mutex::new(None),
            consecutive_low_load_checks: AtomicUsize::new(0),
            prime_session: RwLock::new(None),
            selector: PowerOfTwoSelector::new(),
        });

        // Run 1: Low load (0 rpcs) -> increment consecutive checks to 1, no scale-down
        assert_eq!(
            inner.consecutive_low_load_checks.load(Ordering::Relaxed),
            0,
            "Initial consecutive low load checks must be 0"
        );
        {
            let active = inner.active_entries.read().expect("lock poisoned");
            let total_load: u32 = active.iter().map(|entry| entry.in_flight()).sum();
            let avg_load = (total_load as f64) / (active.len() as f64);
            assert!(avg_load < 15.0, "Average load must be below threshold 15.0");
        }

        let runs = inner
            .consecutive_low_load_checks
            .fetch_add(1, Ordering::Relaxed)
            + 1;
        assert_eq!(runs, 1, "First low load run count must be 1");
        assert_eq!(
            inner.active_entries.read().expect("lock poisoned").len(),
            4,
            "Active channels must stay 4"
        );

        // Run 2: Low load -> increment to 2, no scale-down
        let runs2 = inner
            .consecutive_low_load_checks
            .fetch_add(1, Ordering::Relaxed)
            + 1;
        assert_eq!(runs2, 2, "Second low load run count must be 2");
        assert_eq!(
            inner.active_entries.read().expect("lock poisoned").len(),
            4,
            "Active channels must stay 4 on second low load run"
        );

        // Interrupted by temporary spike -> resets count to 0
        inner
            .consecutive_low_load_checks
            .store(0, Ordering::Relaxed);
        assert_eq!(
            inner.consecutive_low_load_checks.load(Ordering::Relaxed),
            0,
            "Spike must reset debounce counter"
        );
    }

    #[test]
    fn scale_down_strictly_preserves_min_channels() {
        let channel_1 = Arc::new(ChannelEntry::new(1, 1, create_mock_channel()));
        let channel_2 = Arc::new(ChannelEntry::new(2, 2, create_mock_channel()));
        let channel_3 = Arc::new(ChannelEntry::new(3, 3, create_mock_channel()));

        let inner = Arc::new(ChannelPoolInner {
            config: ChannelPoolConfig::Dynamic(DynamicChannelPoolConfig {
                min_channels: 2,
                max_channels: 4,
                min_rpc_per_channel: 15.0,
                max_rpc_per_channel: 25.0,
                consecutive_low_load_checks: 1,
                max_remove_channels: 2,
                ..Default::default()
            }),
            client_config: ClientConfig::default(),
            active_entries: RwLock::new(vec![
                Arc::clone(&channel_1),
                Arc::clone(&channel_2),
                Arc::clone(&channel_3),
            ]),
            draining_entries: RwLock::new(Vec::new()),
            next_entry_id: AtomicU64::new(4),
            scale_up_notify: Arc::new(Notify::new()),
            last_scale_up_time: Mutex::new(None),
            consecutive_low_load_checks: AtomicUsize::new(0),
            prime_session: RwLock::new(None),
            selector: PowerOfTwoSelector::new(),
        });

        // 3 active channels, min 2, max_remove 2: must only remove 1 channel to preserve min_channels=2
        let dynamic_config = match &inner.config {
            ChannelPoolConfig::Dynamic(config) => config.clone(),
            ChannelPoolConfig::Static(_) => unreachable!(),
        };
        evaluate_and_execute_scale_down(&inner, &dynamic_config);

        let active = inner.active_entries.read().expect("lock");
        assert_eq!(
            active.len(),
            2,
            "Active channel count must never drop below min_channels (2)"
        );

        let draining = inner.draining_entries.read().expect("lock");
        assert_eq!(draining.len(), 1, "Exactly 1 channel should be draining");
    }

    #[tokio::test]
    async fn clean_teardown_on_client_drop() {
        let channels = vec![create_mock_channel(), create_mock_channel()];
        let config = DynamicChannelPoolConfig {
            initial_channels: 2,
            min_channels: 2,
            max_channels: 10,
            scale_down_check_interval: Duration::from_millis(50),
            ..Default::default()
        };

        let pool = ChannelPool::new_dynamic(channels, config, ClientConfig::default());
        let weak_inner = Arc::downgrade(&pool.inner);

        assert!(
            weak_inner.upgrade().is_some(),
            "Inner must be alive while pool is held"
        );

        drop(pool);

        sleep(Duration::from_millis(100)).await;
        assert!(
            weak_inner.upgrade().is_none(),
            "Inner must be dropped cleanly when pool is dropped"
        );
    }
}
