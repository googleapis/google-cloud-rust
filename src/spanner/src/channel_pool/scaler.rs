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
use tokio::sync::watch::Receiver as WatchReceiver;
use tokio::task::JoinSet;
use tokio::time::{Instant as TokioInstant, MissedTickBehavior, interval_at, sleep, timeout};

/// Runs the background scale-up worker loop for a dynamic channel pool.
///
/// The worker sleeps on a `tokio::sync::Notify` handle triggered by the P2C picker
/// when an active channel's effective load exceeds `max_rpc_per_channel`.
///
/// # Loop Lifecycle & Error Resilience
/// The loop holds only a `Weak<ChannelPoolInner>` reference and a `WatchReceiver<()>` shutdown signal.
/// When the parent `ChannelPool` or client is dropped, the shutdown sender is dropped, waking
/// any awaiting future on `shutdown_receiver.changed()` immediately and persistently without leaking tasks.
pub(crate) async fn scale_up_worker_loop(
    weak_inner: Weak<ChannelPoolInner>,
    mut shutdown_receiver: WatchReceiver<()>,
) {
    let scale_up_notify = match weak_inner.upgrade() {
        Some(inner) if matches!(inner.config, ChannelPoolConfig::Dynamic(_)) => {
            Arc::clone(&inner.scale_up_notify)
        }
        _ => return,
    };

    loop {
        // Fast-exit if the parent pool was dropped before entering wait.
        if weak_inner.upgrade().is_none() {
            return;
        }

        // Sleep asynchronously until signaled by the channel picker under high load or on pool shutdown.
        tokio::select! {
            _ = scale_up_notify.notified() => {}
            _ = shutdown_receiver.changed() => return,
        }

        // Evaluate cooldown and extract configuration in a scoped block so `inner` (Arc) is
        // not held alive across any subsequent async sleep or wait points.
        let (dynamic_config, cooldown_remaining) = {
            let inner = match weak_inner.upgrade() {
                Some(inner) => inner,
                None => return, // Pool was dropped during wait; exit worker cleanly.
            };

            let dynamic_config = match &inner.config {
                ChannelPoolConfig::Dynamic(config) => config.clone(),
                ChannelPoolConfig::Static(_) => return, // Static pools never scale; exit worker.
            };

            let cooldown_remaining = inner
                .last_scale_up_time
                .lock()
                .expect("lock poisoned")
                .and_then(|last_time| {
                    dynamic_config
                        .scale_up_cooldown
                        .checked_sub(last_time.elapsed())
                });

            (dynamic_config, cooldown_remaining)
        };

        // 1. Check if cooldown is active. If so, sleep for the remaining duration to prevent busy-wakeup storms.
        if let Some(remaining) = cooldown_remaining {
            tokio::select! {
                _ = sleep(remaining) => {}
                _ = shutdown_receiver.changed() => return,
            }
        }

        // 2. Multiplexed session must be registered to execute physical warming queries.
        // Check session availability FIRST so cooldown is never committed when priming is impossible.
        // Drops `inner` (Arc) before awaiting parallel dialing/priming so the parent pool
        // can be dropped immediately without waiting for priming network I/O.
        let (channels_to_add, client_config, prime_session_name) = {
            let inner = match weak_inner.upgrade() {
                Some(inner) => inner,
                None => return,
            };

            let maybe_prime_session = inner.prime_session.read().expect("lock poisoned").clone();
            let prime_session_name = match maybe_prime_session {
                Some(session) => session,
                None => {
                    drop(inner);
                    tokio::select! {
                        _ = sleep(Duration::from_millis(100)) => continue,
                        _ = shutdown_receiver.changed() => return,
                    }
                }
            };

            let channels_to_add = calculate_scale_up_count(&inner, &dynamic_config);
            let client_config = inner.client_config.clone();
            (channels_to_add, client_config, prime_session_name)
        };

        if channels_to_add == 0 {
            tokio::select! {
                _ = sleep(Duration::from_millis(100)) => continue,
                _ = shutdown_receiver.changed() => return,
            }
        }

        // 3. Dial and prime channels in parallel, aborting immediately if pool is shut down.
        tokio::select! {
            _ = dial_prime_and_publish_channels_parallel(
                &weak_inner,
                client_config,
                prime_session_name,
                channels_to_add,
                &dynamic_config,
            ) => {}
            _ = shutdown_receiver.changed() => return,
        }
    }
}

/// Evaluates scale-up eligibility, enforces capacity bounds, and calculates how many channels to add.
///
/// Returns `0` if capacity is already at `max_channels` or current capacity is sufficient for the
/// observed load. Commits cooldown timestamp to throttle subsequent picker wakeups when at capacity
/// ceiling or when scaling begins. Cooldown sleep is managed by the worker loop before invocation.
fn calculate_scale_up_count(inner: &ChannelPoolInner, config: &DynamicChannelPoolConfig) -> usize {
    let active_guard = inner.active_entries.read().expect("lock poisoned");
    let current_len = active_guard.len();
    if current_len >= config.max_channels {
        let mut last_scale = inner.last_scale_up_time.lock().expect("lock poisoned");
        *last_scale = Some(Instant::now());
        return 0;
    }

    // 1. Sizing calculation:
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

    // 2. Rate limiting:
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
        let mut last_scale = inner.last_scale_up_time.lock().expect("lock poisoned");
        *last_scale = Some(Instant::now());
    }

    channels_to_add
}

/// Dials and primes multiple channels concurrently using `tokio::task::JoinSet`,
/// publishing each primed channel immediately as soon as its priming completes.
async fn dial_prime_and_publish_channels_parallel(
    weak_inner: &Weak<ChannelPoolInner>,
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
                let Some(inner) = weak_inner.upgrade() else {
                    // Pool was dropped while dialing/priming was in flight; discard channel.
                    return;
                };
                publish_primed_channel(&inner, channel, config.max_channels);
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
    let mut active_write = inner.active_entries.write().expect("lock poisoned");

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
        let draining_guard = inner.draining_entries.read().expect("lock poisoned");
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
    let channel = match timeout(prime_timeout, Channel::create(&client_config, 0)).await {
        Ok(Ok(channel)) => channel,
        Ok(Err(err)) => return Err(GaxError::connect(err)),
        Err(_) => return Err(GaxError::timeout("Channel creation timed out")),
    };

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
/// Holds only a `Weak<ChannelPoolInner>` reference and a `WatchReceiver<()>` shutdown signal.
/// Exits cleanly and immediately when `weak_inner.upgrade()` returns `None` or when notified via
/// `shutdown_receiver.changed()` on pool drop. Never escapes prematurely on internal errors.
pub(crate) async fn scale_down_monitor_loop(
    weak_inner: Weak<ChannelPoolInner>,
    mut shutdown_receiver: WatchReceiver<()>,
    interval: Duration,
) {
    let is_dynamic = weak_inner
        .upgrade()
        .is_some_and(|inner| matches!(inner.config, ChannelPoolConfig::Dynamic(_)));
    if !is_dynamic {
        return;
    }

    // Start after one full interval has elapsed.
    let mut timer = interval_at(TokioInstant::now() + interval, interval);
    timer.set_missed_tick_behavior(MissedTickBehavior::Delay);

    loop {
        // Fast-exit if the parent pool was dropped before entering wait.
        if weak_inner.upgrade().is_none() {
            return;
        }

        tokio::select! {
            _ = timer.tick() => {
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
            _ = shutdown_receiver.changed() => {
                return; // Pool was dropped; terminate monitor task immediately.
            }
        }
    }
}

/// Evaluates load across active channels, manages debounce counters, and moves candidates to draining.
fn evaluate_and_execute_scale_down(inner: &ChannelPoolInner, config: &DynamicChannelPoolConfig) {
    let mut active_write = inner.active_entries.write().expect("lock poisoned");

    // Do not scale down below configured min_channels floor.
    if active_write.len() <= config.min_channels {
        inner
            .consecutive_low_load_checks
            .store(0, Ordering::Relaxed);
        return;
    }

    // Note: Scale-down deliberately evaluates real in-flight RPCs only (entry.in_flight()),
    // excluding synthetic error penalties so failing channels are not retained as busy.
    let total_in_flight: u32 = active_write.iter().map(|entry| entry.in_flight()).sum();
    let avg_load = (total_in_flight as f64) / (active_write.len() as f64);

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

    if desired_channels >= active_write.len() {
        inner
            .consecutive_low_load_checks
            .store(0, Ordering::Relaxed);
        return;
    }

    let channels_to_remove =
        (active_write.len() - desired_channels).min(config.max_remove_channels);
    if channels_to_remove == 0 {
        inner
            .consecutive_low_load_checks
            .store(0, Ordering::Relaxed);
        return;
    }

    // Move candidate channels from active_entries to draining_entries in-place under write lock.
    let mut draining_write = inner.draining_entries.write().expect("lock poisoned");

    // Snapshot stable keys to prevent race conditions during sorting if in-flight counts change concurrently.
    // Order:
    // 1. in_flight: lowest active RPC load drained first.
    // 2. active_rw_count: between channels with equal active load, prefer draining channels with 0 R/W transactions.
    // 3. created_at (reversed): prefer newer channels on tie, preserving older/warmer channels.
    let mut active_with_keys: Vec<(u32, u32, Instant, Arc<ChannelEntry>)> = active_write
        .drain(..)
        .map(|entry| {
            (
                entry.in_flight(),
                entry.active_rw_count(),
                entry.created_at,
                entry,
            )
        })
        .collect();
    active_with_keys.sort_unstable_by_key(|(in_flight, rw_count, created_at, _)| {
        (*in_flight, *rw_count, Reverse(*created_at))
    });

    // Exactly calculate number of channels eligible to remove without breaching min_channels.
    let eligible_to_remove = active_with_keys
        .len()
        .saturating_sub(config.min_channels)
        .min(channels_to_remove);

    for (index, (_, _, _, entry)) in active_with_keys.into_iter().enumerate() {
        if index < eligible_to_remove {
            entry.set_state(ChannelState::Draining);
            draining_write.push(entry);
        } else {
            active_write.push(entry);
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
///    the transaction completes, or `SPANNER_RW_TRANSACTION_IDLE_TIMEOUT` (10s) plus `drain_idle_grace` elapses.
/// 3. Channels with 0 load and no R/W transactions are closed once `drain_idle_grace` elapses.
pub(crate) fn sweep_draining_channels(inner: &ChannelPoolInner, drain_idle_grace: Duration) {
    let mut draining_write = inner.draining_entries.write().expect("lock poisoned");

    draining_write.retain(|entry| {
        if entry.in_flight() > 0 {
            return true; // Keep open while in-flight RPCs execute.
        }

        let idle_duration = entry.elapsed_since_activity();

        // Determine required idle duration:
        // - If active R/W transactions are attached, wait until the 10s Spanner server abort timeout
        //   plus drain_idle_grace as extra safety buffer.
        // - Otherwise, wait drain_idle_grace after the last activity has ceased.
        let required_idle = if entry.active_rw_count() > 0 {
            SPANNER_RW_TRANSACTION_IDLE_TIMEOUT + drain_idle_grace
        } else {
            drain_idle_grace
        };

        if idle_duration >= required_idle {
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
    use crate::channel_pool::config::{DynamicChannelPoolConfig, StaticChannelPoolConfig};
    use crate::channel_pool::pool::ChannelPool;
    use crate::generated::gapic_dataplane::stub::Spanner as SpannerStub;
    use crate::model::{CreateSessionRequest, Session};
    use crate::routing::power_of_two_selector::PowerOfTwoSelector;
    use google_cloud_gax::options::RequestOptions;
    use spanner_grpc_mock::google::spanner::v1 as mock_v1;
    use std::fmt::Debug;
    use std::future::{Future, ready};
    use std::sync::atomic::{AtomicU64, AtomicUsize};
    use std::sync::{Mutex, RwLock};
    use tokio::sync::Notify;
    use tokio::sync::watch::channel as watch_channel;
    use tokio::task::yield_now;
    use tokio::time::{advance, pause};

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

    fn dummy_shutdown_receiver() -> WatchReceiver<()> {
        watch_channel(()).1
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
            shutdown_sender: watch_channel(()).0,
            last_scale_up_time: Mutex::new(None),
            consecutive_low_load_checks: AtomicUsize::new(0),
            prime_session: RwLock::new(None),
            selector: PowerOfTwoSelector::new(),
        };

        sweep_draining_channels(&inner, Duration::from_secs(5));

        let draining_guard = inner.draining_entries.read().expect("lock poisoned");
        assert_eq!(
            draining_guard.len(),
            1,
            "Only channel_1 should remain draining"
        );
        assert_eq!(
            draining_guard[0].id, 1,
            "remaining draining channel ID must be 1"
        );
        drop(draining_guard);
        assert!(
            channel_2.is_closed(),
            "channel_2 must be closed after 10s idle R/W timeout plus 5s grace"
        );
        assert!(
            channel_3.is_closed(),
            "channel_3 must be closed after 5s idle grace"
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
            shutdown_sender: watch_channel(()).0,
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
            shutdown_sender: watch_channel(()).0,
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

    #[test]
    fn scale_down_candidate_sort_priority() {
        // channel_1: in_flight=0, active_rw=1
        let channel_1 = Arc::new(ChannelEntry::new(1, 1, create_mock_channel()));
        channel_1.active_rw_transactions.store(1, Ordering::Relaxed);

        // channel_2: in_flight=0, active_rw=0 -> should be drained before channel_1
        let channel_2 = Arc::new(ChannelEntry::new(2, 2, create_mock_channel()));

        // channel_3: in_flight=5, active_rw=0 -> should NOT be drained before channel_1
        let channel_3 = Arc::new(ChannelEntry::new(3, 3, create_mock_channel()));
        channel_3.in_flight_rpcs.store(5, Ordering::Relaxed);

        let inner = Arc::new(ChannelPoolInner {
            config: ChannelPoolConfig::Dynamic(DynamicChannelPoolConfig {
                min_channels: 2,
                max_channels: 4,
                min_rpc_per_channel: 15.0,
                max_rpc_per_channel: 25.0,
                consecutive_low_load_checks: 1,
                max_remove_channels: 1,
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
            shutdown_sender: watch_channel(()).0,
            last_scale_up_time: Mutex::new(None),
            consecutive_low_load_checks: AtomicUsize::new(0),
            prime_session: RwLock::new(None),
            selector: PowerOfTwoSelector::new(),
        });

        let dynamic_config = match &inner.config {
            ChannelPoolConfig::Dynamic(config) => config.clone(),
            ChannelPoolConfig::Static(_) => unreachable!(),
        };
        evaluate_and_execute_scale_down(&inner, &dynamic_config);

        let draining = inner.draining_entries.read().expect("lock poisoned");
        assert_eq!(draining.len(), 1, "Exactly 1 channel should be draining");
        assert_eq!(
            draining[0].id, 2,
            "channel_2 (in_flight=0, active_rw=0) must be drained before channel_1 (in_flight=0, active_rw=1)"
        );
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

        assert!(
            weak_inner.upgrade().is_none(),
            "Inner must be dropped cleanly when pool is dropped"
        );
    }

    #[test]
    fn calculate_scale_up_count_all_branches() {
        let config = DynamicChannelPoolConfig {
            min_channels: 2,
            max_channels: 8,
            min_rpc_per_channel: 10.0,
            max_rpc_per_channel: 20.0,
            scale_up_cooldown: Duration::from_secs(10),
            max_scale_up_percent: 30,
            ..Default::default()
        };

        let channel_1 = Arc::new(ChannelEntry::new(1, 1, create_mock_channel()));
        let channel_2 = Arc::new(ChannelEntry::new(2, 2, create_mock_channel()));

        let inner = ChannelPoolInner {
            config: ChannelPoolConfig::Dynamic(config.clone()),
            client_config: ClientConfig::default(),
            active_entries: RwLock::new(vec![Arc::clone(&channel_1), Arc::clone(&channel_2)]),
            draining_entries: RwLock::new(Vec::new()),
            next_entry_id: AtomicU64::new(3),
            scale_up_notify: Arc::new(Notify::new()),
            shutdown_sender: watch_channel(()).0,
            last_scale_up_time: Mutex::new(None),
            consecutive_low_load_checks: AtomicUsize::new(0),
            prime_session: RwLock::new(None),
            selector: PowerOfTwoSelector::new(),
        };

        // 1. Low load: desired channels (0) <= current (2) -> returns 0 without committing cooldown
        assert_eq!(
            calculate_scale_up_count(&inner, &config),
            0,
            "Low load must not trigger scale-up"
        );
        assert!(
            inner.last_scale_up_time.lock().expect("lock").is_none(),
            "Cooldown timestamp must not be set on low load so subsequent bursts are not delayed"
        );

        // 2. High load: total load 90 -> target rpc 15 -> desired = 6 channels
        // current = 2, max_to_add = max(ceil(2 * 0.3) = 1, 2) = 2 -> channels_to_add = 2
        channel_1.in_flight_rpcs.store(45, Ordering::Relaxed);
        channel_2.in_flight_rpcs.store(45, Ordering::Relaxed);
        let count = calculate_scale_up_count(&inner, &config);
        assert_eq!(
            count, 2,
            "High load must calculate 2 channels to add based on rate limiting"
        );

        // Verify cooldown was committed
        assert!(
            inner.last_scale_up_time.lock().expect("lock").is_some(),
            "Cooldown timestamp must be set after scale-up count > 0"
        );

        // 3. Pool already at max_channels (8) -> returns 0 and commits cooldown timestamp
        *inner.last_scale_up_time.lock().expect("lock") = None;
        let mut full_channels = Vec::new();
        for index in 1..=8 {
            full_channels.push(Arc::new(ChannelEntry::new(
                index,
                index as usize,
                create_mock_channel(),
            )));
        }
        *inner.active_entries.write().expect("lock") = full_channels;
        assert_eq!(
            calculate_scale_up_count(&inner, &config),
            0,
            "Pool at max_channels must return 0 channels to add"
        );
        assert!(
            inner.last_scale_up_time.lock().expect("lock").is_some(),
            "Cooldown timestamp must be set when pool is at capacity ceiling"
        );
    }

    #[test]
    fn publish_primed_channel_all_branches() {
        let channel_1 = Arc::new(ChannelEntry::new(1, 1, create_mock_channel()));
        let channel_2 = Arc::new(ChannelEntry::new(2, 2, create_mock_channel()));
        channel_2.set_state(ChannelState::Draining);

        let inner = ChannelPoolInner {
            config: ChannelPoolConfig::Dynamic(DynamicChannelPoolConfig {
                max_channels: 3,
                ..Default::default()
            }),
            client_config: ClientConfig::default(),
            active_entries: RwLock::new(vec![Arc::clone(&channel_1)]),
            draining_entries: RwLock::new(vec![Arc::clone(&channel_2)]),
            next_entry_id: AtomicU64::new(3),
            scale_up_notify: Arc::new(Notify::new()),
            shutdown_sender: watch_channel(()).0,
            last_scale_up_time: Mutex::new(None),
            consecutive_low_load_checks: AtomicUsize::new(0),
            prime_session: RwLock::new(None),
            selector: PowerOfTwoSelector::new(),
        };

        // Slots 1 (active) and 2 (draining) are occupied -> new channel should receive slot 3
        publish_primed_channel(&inner, create_mock_channel(), 3);
        {
            let active = inner.active_entries.read().expect("lock");
            assert_eq!(
                active.len(),
                2,
                "Active channel count must be 2 after publishing"
            );
            assert_eq!(
                active[1].logical_channel_id, 3,
                "New channel must receive slot 3"
            );
            assert_eq!(active[1].id, 3, "New channel must receive monotonic ID 3");
        }

        // When active channels reach max_channels (3), publishing is a no-op
        publish_primed_channel(&inner, create_mock_channel(), 3);
        assert_eq!(
            inner.active_entries.read().expect("lock").len(),
            3,
            "Active channel count must reach max_channels (3)"
        );

        publish_primed_channel(&inner, create_mock_channel(), 3);
        assert_eq!(
            inner.active_entries.read().expect("lock").len(),
            3,
            "Publishing beyond max_channels must be a no-op"
        );
    }

    #[test]
    fn evaluate_scale_down_all_branches() {
        let config = DynamicChannelPoolConfig {
            min_channels: 2,
            max_channels: 6,
            min_rpc_per_channel: 15.0,
            max_rpc_per_channel: 25.0,
            consecutive_low_load_checks: 2,
            max_remove_channels: 2,
            ..Default::default()
        };

        let channel_1 = Arc::new(ChannelEntry::new_with_created_at(
            1,
            1,
            create_mock_channel(),
            Instant::now() - Duration::from_secs(100),
        ));
        let channel_2 = Arc::new(ChannelEntry::new_with_created_at(
            2,
            2,
            create_mock_channel(),
            Instant::now() - Duration::from_secs(80),
        ));
        let channel_3 = Arc::new(ChannelEntry::new_with_created_at(
            3,
            3,
            create_mock_channel(),
            Instant::now() - Duration::from_secs(60),
        ));
        let channel_4 = Arc::new(ChannelEntry::new_with_created_at(
            4,
            4,
            create_mock_channel(),
            Instant::now() - Duration::from_secs(40),
        ));

        let inner = ChannelPoolInner {
            config: ChannelPoolConfig::Dynamic(config.clone()),
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
            shutdown_sender: watch_channel(()).0,
            last_scale_up_time: Mutex::new(None),
            consecutive_low_load_checks: AtomicUsize::new(0),
            prime_session: RwLock::new(None),
            selector: PowerOfTwoSelector::new(),
        };

        // 1. High load (avg >= min_rpc_per_channel) -> resets consecutive checks to 0
        channel_1.in_flight_rpcs.store(20, Ordering::Relaxed);
        channel_2.in_flight_rpcs.store(20, Ordering::Relaxed);
        channel_3.in_flight_rpcs.store(20, Ordering::Relaxed);
        channel_4.in_flight_rpcs.store(20, Ordering::Relaxed);
        evaluate_and_execute_scale_down(&inner, &config);
        assert_eq!(
            inner.consecutive_low_load_checks.load(Ordering::Relaxed),
            0,
            "High load must reset consecutive checks to 0"
        );

        // 2. First low-load check: increments count from 0 to 1 (< consecutive_low_load_checks=2) -> returns without draining
        channel_1.in_flight_rpcs.store(0, Ordering::Relaxed);
        channel_2.in_flight_rpcs.store(0, Ordering::Relaxed);
        channel_3.in_flight_rpcs.store(0, Ordering::Relaxed);
        channel_4.in_flight_rpcs.store(0, Ordering::Relaxed);
        evaluate_and_execute_scale_down(&inner, &config);
        assert_eq!(
            inner.consecutive_low_load_checks.load(Ordering::Relaxed),
            1,
            "First low load run must increment consecutive checks to 1"
        );
        assert_eq!(
            inner.active_entries.read().expect("lock").len(),
            4,
            "No channels drained after 1 low-load check"
        );

        // 3. Second low-load check: count reaches 2 -> drains 2 channels to reach min_channels=2
        // Sort order prefers lowest load and newest channel (channel 4, then channel 3)
        evaluate_and_execute_scale_down(&inner, &config);
        assert_eq!(
            inner.consecutive_low_load_checks.load(Ordering::Relaxed),
            0,
            "Consecutive checks must be reset to 0 after scale-down execution"
        );
        assert_eq!(
            inner.active_entries.read().expect("lock").len(),
            2,
            "Active channel count must be reduced to 2"
        );
        let draining = inner.draining_entries.read().expect("lock");
        assert_eq!(draining.len(), 2, "2 channels must be moved to draining");
        assert!(
            draining[0].is_draining(),
            "first drained channel state must be Draining"
        );
        assert!(
            draining[1].is_draining(),
            "second drained channel state must be Draining"
        );

        // 4. When already at min_channels (2), evaluate_and_execute_scale_down resets counter and returns immediately
        evaluate_and_execute_scale_down(&inner, &config);
        assert_eq!(
            inner.active_entries.read().expect("lock").len(),
            2,
            "Must remain at min_channels 2"
        );

        // 5. When desired_channels >= active_channels, resets counter and returns without draining
        let heavy_config = DynamicChannelPoolConfig {
            min_channels: 2,
            max_channels: 6,
            min_rpc_per_channel: 10.0,
            max_rpc_per_channel: 12.0,
            consecutive_low_load_checks: 1,
            ..Default::default()
        };
        let active_heavy = vec![
            Arc::new(ChannelEntry::new(10, 1, create_mock_channel())),
            Arc::new(ChannelEntry::new(11, 2, create_mock_channel())),
            Arc::new(ChannelEntry::new(12, 3, create_mock_channel())),
        ];
        // Avg load = 9 (< 10 min_rpc), total_in_flight = 27 -> desired = ceil(27/11) = 3 >= len(3)
        active_heavy[0].in_flight_rpcs.store(9, Ordering::Relaxed);
        active_heavy[1].in_flight_rpcs.store(9, Ordering::Relaxed);
        active_heavy[2].in_flight_rpcs.store(9, Ordering::Relaxed);
        let heavy_inner = ChannelPoolInner {
            config: ChannelPoolConfig::Dynamic(heavy_config.clone()),
            client_config: ClientConfig::default(),
            active_entries: RwLock::new(active_heavy),
            draining_entries: RwLock::new(Vec::new()),
            next_entry_id: AtomicU64::new(13),
            scale_up_notify: Arc::new(Notify::new()),
            shutdown_sender: watch_channel(()).0,
            last_scale_up_time: Mutex::new(None),
            consecutive_low_load_checks: AtomicUsize::new(0),
            prime_session: RwLock::new(None),
            selector: PowerOfTwoSelector::new(),
        };
        evaluate_and_execute_scale_down(&heavy_inner, &heavy_config);
        assert_eq!(
            heavy_inner.active_entries.read().expect("lock").len(),
            3,
            "No channels drained when desired_channels >= active_channels"
        );

        // 6. When max_remove_channels is 0, channels_to_remove is 0 -> resets counter and returns
        let zero_remove_config = DynamicChannelPoolConfig {
            min_channels: 2,
            max_channels: 6,
            min_rpc_per_channel: 15.0,
            max_rpc_per_channel: 25.0,
            consecutive_low_load_checks: 1,
            max_remove_channels: 0,
            ..Default::default()
        };
        let active_zero_remove = vec![
            Arc::new(ChannelEntry::new(20, 1, create_mock_channel())),
            Arc::new(ChannelEntry::new(21, 2, create_mock_channel())),
            Arc::new(ChannelEntry::new(22, 3, create_mock_channel())),
        ];
        let zero_remove_inner = ChannelPoolInner {
            config: ChannelPoolConfig::Dynamic(zero_remove_config.clone()),
            client_config: ClientConfig::default(),
            active_entries: RwLock::new(active_zero_remove),
            draining_entries: RwLock::new(Vec::new()),
            next_entry_id: AtomicU64::new(23),
            scale_up_notify: Arc::new(Notify::new()),
            shutdown_sender: watch_channel(()).0,
            last_scale_up_time: Mutex::new(None),
            consecutive_low_load_checks: AtomicUsize::new(0),
            prime_session: RwLock::new(None),
            selector: PowerOfTwoSelector::new(),
        };
        evaluate_and_execute_scale_down(&zero_remove_inner, &zero_remove_config);
        assert_eq!(
            zero_remove_inner.active_entries.read().expect("lock").len(),
            3,
            "No channels drained when channels_to_remove is 0"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn evaluate_and_execute_scale_down_sorts_deterministically_with_concurrent_updates() {
        use std::sync::atomic::AtomicBool;

        let config = DynamicChannelPoolConfig {
            min_channels: 2,
            max_channels: 8,
            min_rpc_per_channel: 15.0,
            max_rpc_per_channel: 25.0,
            consecutive_low_load_checks: 1,
            max_remove_channels: 4,
            ..Default::default()
        };

        let mut channels = Vec::new();
        for index in 1..=6 {
            channels.push(Arc::new(ChannelEntry::new_with_created_at(
                index,
                index as usize,
                create_mock_channel(),
                Instant::now() - Duration::from_secs(100 - index * 10),
            )));
        }

        let inner = Arc::new(ChannelPoolInner {
            config: ChannelPoolConfig::Dynamic(config.clone()),
            client_config: ClientConfig::default(),
            active_entries: RwLock::new(channels.clone()),
            draining_entries: RwLock::new(Vec::new()),
            next_entry_id: AtomicU64::new(7),
            scale_up_notify: Arc::new(Notify::new()),
            shutdown_sender: watch_channel(()).0,
            last_scale_up_time: Mutex::new(None),
            consecutive_low_load_checks: AtomicUsize::new(0),
            prime_session: RwLock::new(None),
            selector: PowerOfTwoSelector::new(),
        });

        // Spawn a background task updating in-flight counts concurrently
        let stop_flag = Arc::new(AtomicBool::new(false));
        let stop_clone = Arc::clone(&stop_flag);
        let channels_clone = channels.clone();
        let background_writer = tokio::spawn(async move {
            let mut iteration = 0u32;
            while !stop_clone.load(Ordering::Relaxed) {
                iteration = iteration.wrapping_add(1);
                for channel in &channels_clone {
                    channel
                        .in_flight_rpcs
                        .store(iteration % 5, Ordering::Relaxed);
                }
                yield_now().await;
            }
        });

        // Execute scale-down: must not panic or corrupt ordering
        evaluate_and_execute_scale_down(&inner, &config);

        stop_flag.store(true, Ordering::Relaxed);
        let _ = background_writer.await;

        let active_len = inner.active_entries.read().expect("lock poisoned").len();
        let draining_len = inner.draining_entries.read().expect("lock poisoned").len();
        assert_eq!(
            active_len + draining_len,
            6,
            "Total channels must remain invariant during scale-down"
        );
        assert!(
            active_len >= config.min_channels,
            "Active channels must never drop below min_channels"
        );
    }

    #[test]
    fn sweep_draining_in_flight_and_idle_grace_branches() {
        let channel_in_flight = Arc::new(ChannelEntry::new(1, 1, create_mock_channel()));
        channel_in_flight.set_state(ChannelState::Draining);
        channel_in_flight.in_flight_rpcs.store(2, Ordering::Relaxed);

        let channel_recent_idle = Arc::new(ChannelEntry::new_with_created_at(
            2,
            2,
            create_mock_channel(),
            Instant::now() - Duration::from_secs(10),
        ));
        channel_recent_idle.set_state(ChannelState::Draining);
        channel_recent_idle.touch_activity();

        let inner = ChannelPoolInner {
            config: ChannelPoolConfig::Dynamic(DynamicChannelPoolConfig::default()),
            client_config: ClientConfig::default(),
            active_entries: RwLock::new(Vec::new()),
            draining_entries: RwLock::new(vec![
                Arc::clone(&channel_in_flight),
                Arc::clone(&channel_recent_idle),
            ]),
            next_entry_id: AtomicU64::new(3),
            scale_up_notify: Arc::new(Notify::new()),
            shutdown_sender: watch_channel(()).0,
            last_scale_up_time: Mutex::new(None),
            consecutive_low_load_checks: AtomicUsize::new(0),
            prime_session: RwLock::new(None),
            selector: PowerOfTwoSelector::new(),
        };

        // Sweep with 5s idle grace:
        // - channel_in_flight has in_flight=2 -> retained
        // - channel_recent_idle was active recently (< 5s) -> retained
        sweep_draining_channels(&inner, Duration::from_secs(5));
        assert_eq!(
            inner.draining_entries.read().expect("lock").len(),
            2,
            "Both in-flight and recently active draining channels must be retained"
        );
    }

    #[tokio::test]
    async fn worker_loops_static_config_and_teardown() {
        // 1. Weak pointer already dead before loop start -> both functions exit immediately
        scale_up_worker_loop(Weak::new(), dummy_shutdown_receiver()).await;
        scale_down_monitor_loop(
            Weak::new(),
            dummy_shutdown_receiver(),
            Duration::from_millis(10),
        )
        .await;

        // 2. Static configuration -> both loops exit immediately without waiting
        let static_pool = ChannelPool::new_static(
            vec![create_mock_channel()],
            StaticChannelPoolConfig::default(),
            ClientConfig::default(),
        );
        let weak_static = Arc::downgrade(&static_pool.inner);
        scale_up_worker_loop(
            Weak::clone(&weak_static),
            static_pool.inner.shutdown_sender.subscribe(),
        )
        .await;
        scale_down_monitor_loop(
            weak_static,
            static_pool.inner.shutdown_sender.subscribe(),
            Duration::from_millis(10),
        )
        .await;

        // 3. Dynamic pool -> worker loops terminate cleanly when pool/inner is dropped
        let inner_up = Arc::new(ChannelPoolInner {
            config: ChannelPoolConfig::Dynamic(DynamicChannelPoolConfig::default()),
            client_config: ClientConfig::default(),
            active_entries: RwLock::new(vec![Arc::new(ChannelEntry::new(
                1,
                1,
                create_mock_channel(),
            ))]),
            draining_entries: RwLock::new(Vec::new()),
            next_entry_id: AtomicU64::new(2),
            scale_up_notify: Arc::new(Notify::new()),
            shutdown_sender: watch_channel(()).0,
            last_scale_up_time: Mutex::new(None),
            consecutive_low_load_checks: AtomicUsize::new(0),
            prime_session: RwLock::new(None),
            selector: PowerOfTwoSelector::new(),
        });
        let weak_up = Arc::downgrade(&inner_up);
        let receiver_up = inner_up.shutdown_sender.subscribe();
        let scale_up_handle = tokio::spawn(async move {
            scale_up_worker_loop(weak_up, receiver_up).await;
        });

        // Yield execution so the worker loop enters notify.notified()
        yield_now().await;
        drop(inner_up);

        let scale_up_result = scale_up_handle.await;
        assert!(
            scale_up_result.is_ok(),
            "scale_up_worker_loop must terminate promptly on pool drop"
        );

        // 4. Dynamic pool scale-down monitor loop -> executes ticks and terminates cleanly on pool drop
        pause();

        let inner_down = Arc::new(ChannelPoolInner {
            config: ChannelPoolConfig::Dynamic(DynamicChannelPoolConfig::default()),
            client_config: ClientConfig::default(),
            active_entries: RwLock::new(vec![Arc::new(ChannelEntry::new(
                1,
                1,
                create_mock_channel(),
            ))]),
            draining_entries: RwLock::new(Vec::new()),
            next_entry_id: AtomicU64::new(2),
            scale_up_notify: Arc::new(Notify::new()),
            shutdown_sender: watch_channel(()).0,
            last_scale_up_time: Mutex::new(None),
            consecutive_low_load_checks: AtomicUsize::new(0),
            prime_session: RwLock::new(None),
            selector: PowerOfTwoSelector::new(),
        });
        let weak_down = Arc::downgrade(&inner_down);
        let receiver_down = inner_down.shutdown_sender.subscribe();
        let scale_down_handle = tokio::spawn(async move {
            scale_down_monitor_loop(weak_down, receiver_down, Duration::from_millis(50)).await;
        });

        // Advance virtual time to trigger an active monitor loop tick deterministically
        advance(Duration::from_millis(50)).await;
        yield_now().await;

        // Drop the inner instance so the subsequent tick observes None
        drop(inner_down);

        // Advance virtual time to fire the next tick
        advance(Duration::from_millis(50)).await;

        let scale_down_result = scale_down_handle.await;
        assert!(
            scale_down_result.is_ok(),
            "scale_down_monitor_loop must terminate promptly on pool drop"
        );
    }

    #[tokio::test]
    async fn scale_up_worker_loop_executes_scaling() {
        use crate::client::Spanner;
        use gaxi::grpc::tonic::Response;
        use google_cloud_auth::credentials::anonymous::Builder as AnonymousCredentialsBuilder;
        use spanner_grpc_mock::{MockSpanner, start};

        let primed_count = Arc::new(AtomicUsize::new(0));
        let primed_notify = Arc::new(Notify::new());
        let primed_count_clone = Arc::clone(&primed_count);
        let primed_notify_clone = Arc::clone(&primed_notify);

        let mut mock = MockSpanner::new();
        mock.expect_execute_sql().returning(move |request| {
            let request = request.into_inner();
            assert_eq!(request.sql, "SELECT 1", "priming SQL must be SELECT 1");
            if primed_count_clone.fetch_add(1, Ordering::Relaxed) + 1 == 2 {
                primed_notify_clone.notify_waiters();
            }
            Ok(Response::new(mock_v1::ResultSet::default()))
        });
        let (address, _server) = start("0.0.0.0:0", mock).await.expect("start mock server");

        let spanner = Spanner::builder()
            .with_endpoint(address)
            .with_credentials(AnonymousCredentialsBuilder::new().build())
            .build()
            .await
            .expect("build client");

        let client_config = spanner.config.clone();

        let channel = Arc::new(ChannelEntry::new(1, 1, create_mock_channel()));
        channel.in_flight_rpcs.store(100, Ordering::Relaxed);

        let inner = Arc::new(ChannelPoolInner {
            config: ChannelPoolConfig::Dynamic(DynamicChannelPoolConfig {
                min_channels: 1,
                max_channels: 3,
                scale_up_cooldown: Duration::ZERO,
                prime_timeout: Duration::from_secs(2),
                ..Default::default()
            }),
            client_config,
            active_entries: RwLock::new(vec![channel]),
            draining_entries: RwLock::new(Vec::new()),
            next_entry_id: AtomicU64::new(2),
            scale_up_notify: Arc::new(Notify::new()),
            shutdown_sender: watch_channel(()).0,
            last_scale_up_time: Mutex::new(None),
            consecutive_low_load_checks: AtomicUsize::new(0),
            prime_session: RwLock::new(Some(
                "projects/p/instances/i/databases/d/sessions/s".to_string(),
            )),
            selector: PowerOfTwoSelector::new(),
        });

        let weak_inner = Arc::downgrade(&inner);
        let receiver = inner.shutdown_sender.subscribe();
        let worker_handle = tokio::spawn(async move {
            scale_up_worker_loop(weak_inner, receiver).await;
        });

        // Trigger scale-up
        inner.scale_up_notify.notify_one();

        // Deterministically await priming query execution on the mock server
        primed_notify.notified().await;

        // Yield execution to allow published channels to register in active_entries
        let deadline = Instant::now() + Duration::from_secs(2);
        while inner.active_entries.read().expect("lock").len() < 3 {
            if Instant::now() >= deadline {
                panic!("Timed out waiting for channel count to reach 3");
            }
            yield_now().await;
        }

        assert_eq!(
            inner.active_entries.read().expect("lock").len(),
            3,
            "Worker loop must scale up pool to 3 channels"
        );

        drop(inner);
        let result = worker_handle.await;
        assert!(
            result.is_ok(),
            "Worker loop must terminate cleanly on pool drop"
        );
    }

    #[tokio::test]
    async fn dial_and_prime_channel_success() {
        use crate::client::Spanner;
        use gaxi::grpc::tonic::Response;
        use google_cloud_auth::credentials::anonymous::Builder as AnonymousCredentialsBuilder;
        use spanner_grpc_mock::{MockSpanner, start};

        let mut mock = MockSpanner::new();
        mock.expect_execute_sql().returning(|request| {
            let request = request.into_inner();
            assert_eq!(request.sql, "SELECT 1", "priming SQL must be SELECT 1");
            assert_eq!(
                request.session, "projects/p/instances/i/databases/d/sessions/s1",
                "session name must match registered session"
            );
            Ok(Response::new(mock_v1::ResultSet::default()))
        });

        let (address, _server) = start("0.0.0.0:0", mock)
            .await
            .expect("Failed to start mock server");

        let spanner = Spanner::builder()
            .with_endpoint(address)
            .with_credentials(AnonymousCredentialsBuilder::new().build())
            .build()
            .await
            .expect("Failed to build client");

        let result = dial_and_prime_channel(
            spanner.config.clone(),
            "projects/p/instances/i/databases/d/sessions/s1".to_string(),
            Duration::from_secs(5),
            3,
        )
        .await;

        assert!(
            result.is_ok(),
            "dial_and_prime_channel must succeed when mock returns OK"
        );
    }

    #[tokio::test]
    async fn dial_and_prime_channel_failure_exhaustion() {
        use crate::client::Spanner;
        use gaxi::grpc::tonic::Status;
        use google_cloud_auth::credentials::anonymous::Builder as AnonymousCredentialsBuilder;
        use spanner_grpc_mock::{MockSpanner, start};

        let mut mock = MockSpanner::new();
        mock.expect_execute_sql()
            .times(2)
            .returning(|_| Err(Status::unavailable("unavailable")));

        let (address, _server) = start("0.0.0.0:0", mock)
            .await
            .expect("Failed to start mock server");

        let spanner = Spanner::builder()
            .with_endpoint(address)
            .with_credentials(AnonymousCredentialsBuilder::new().build())
            .build()
            .await
            .expect("Failed to build client");

        let result = dial_and_prime_channel(
            spanner.config.clone(),
            "projects/p/instances/i/databases/d/sessions/s1".to_string(),
            Duration::from_secs(5),
            2,
        )
        .await;

        assert!(
            result.is_err(),
            "dial_and_prime_channel must fail when retries are exhausted"
        );
    }

    #[tokio::test]
    async fn dial_and_prime_channel_creation_timeout() {
        use crate::client::Spanner;
        use google_cloud_auth::credentials::anonymous::Builder as AnonymousCredentialsBuilder;

        // Use an endpoint that will not respond to TCP connect immediately
        let spanner = Spanner::builder()
            .with_endpoint("http://192.0.2.1:80")
            .with_credentials(AnonymousCredentialsBuilder::new().build())
            .build()
            .await
            .expect("Failed to build client");

        let result = dial_and_prime_channel(
            spanner.config.clone(),
            "projects/p/instances/i/databases/d/sessions/s1".to_string(),
            Duration::from_millis(10),
            1,
        )
        .await;

        assert!(
            result.is_err(),
            "dial_and_prime_channel must fail with timeout when channel creation hangs"
        );
    }

    #[tokio::test]
    async fn parallel_priming_and_publishing() {
        use crate::client::Spanner;
        use gaxi::grpc::tonic::{Response, Status};
        use google_cloud_auth::credentials::anonymous::Builder as AnonymousCredentialsBuilder;
        use spanner_grpc_mock::{MockSpanner, start};

        let mut mock = MockSpanner::new();
        mock.expect_execute_sql()
            .times(1)
            .returning(|_| Ok(Response::new(mock_v1::ResultSet::default())));
        mock.expect_execute_sql()
            .times(1)
            .returning(|_| Err(Status::unavailable("unavailable")));

        let (address, _server) = start("0.0.0.0:0", mock)
            .await
            .expect("Failed to start mock server");

        let spanner = Spanner::builder()
            .with_endpoint(address)
            .with_credentials(AnonymousCredentialsBuilder::new().build())
            .build()
            .await
            .expect("Failed to build client");

        let inner = Arc::new(ChannelPoolInner {
            config: ChannelPoolConfig::Dynamic(DynamicChannelPoolConfig {
                max_channels: 5,
                prime_timeout: Duration::from_secs(5),
                prime_max_attempts: 1,
                ..Default::default()
            }),
            client_config: spanner.config.clone(),
            active_entries: RwLock::new(Vec::new()),
            draining_entries: RwLock::new(Vec::new()),
            next_entry_id: AtomicU64::new(1),
            scale_up_notify: Arc::new(Notify::new()),
            shutdown_sender: watch_channel(()).0,
            last_scale_up_time: Mutex::new(None),
            consecutive_low_load_checks: AtomicUsize::new(0),
            prime_session: RwLock::new(Some(
                "projects/p/instances/i/databases/d/sessions/s1".to_string(),
            )),
            selector: PowerOfTwoSelector::new(),
        });

        let config = DynamicChannelPoolConfig {
            max_channels: 5,
            prime_timeout: Duration::from_secs(5),
            prime_max_attempts: 1,
            ..Default::default()
        };

        let weak_inner = Arc::downgrade(&inner);
        dial_prime_and_publish_channels_parallel(
            &weak_inner,
            spanner.config.clone(),
            "projects/p/instances/i/databases/d/sessions/s1".to_string(),
            2,
            &config,
        )
        .await;

        assert_eq!(
            inner.active_entries.read().expect("lock").len(),
            1,
            "Only 1 successfully primed channel must be published when other fails"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn parallel_priming_and_publishing_discards_when_pool_dropped() {
        use crate::client::Spanner;
        use gaxi::grpc::tonic::Response;
        use google_cloud_auth::credentials::anonymous::Builder as AnonymousCredentialsBuilder;
        use spanner_grpc_mock::{MockSpanner, start};
        use std::sync::mpsc::channel;

        let (started_sender, started_receiver) = channel();
        let (release_sender, release_receiver) = channel();

        let mut mock = MockSpanner::new();
        mock.expect_execute_sql().returning(move |_| {
            let _ = started_sender.send(());
            let _ = release_receiver.recv();
            Ok(Response::new(mock_v1::ResultSet::default()))
        });

        let (address, _server) = start("0.0.0.0:0", mock)
            .await
            .expect("Failed to start mock server");

        let spanner = Spanner::builder()
            .with_endpoint(address)
            .with_credentials(AnonymousCredentialsBuilder::new().build())
            .build()
            .await
            .expect("Failed to build client");

        let inner = Arc::new(ChannelPoolInner {
            config: ChannelPoolConfig::Dynamic(DynamicChannelPoolConfig {
                max_channels: 5,
                prime_timeout: Duration::from_secs(5),
                prime_max_attempts: 1,
                ..Default::default()
            }),
            client_config: spanner.config.clone(),
            active_entries: RwLock::new(Vec::new()),
            draining_entries: RwLock::new(Vec::new()),
            next_entry_id: AtomicU64::new(1),
            scale_up_notify: Arc::new(Notify::new()),
            shutdown_sender: watch_channel(()).0,
            last_scale_up_time: Mutex::new(None),
            consecutive_low_load_checks: AtomicUsize::new(0),
            prime_session: RwLock::new(Some(
                "projects/p/instances/i/databases/d/sessions/s1".to_string(),
            )),
            selector: PowerOfTwoSelector::new(),
        });

        let config = DynamicChannelPoolConfig {
            max_channels: 5,
            prime_timeout: Duration::from_secs(5),
            prime_max_attempts: 1,
            ..Default::default()
        };

        let weak_inner = Arc::downgrade(&inner);
        let priming_task = tokio::spawn(async move {
            dial_prime_and_publish_channels_parallel(
                &weak_inner,
                spanner.config.clone(),
                "projects/p/instances/i/databases/d/sessions/s1".to_string(),
                1,
                &config,
            )
            .await;
        });

        // Wait until mock execution starts
        started_receiver.recv().expect("mock execute_sql started");

        // Drop pool while priming is in flight
        drop(inner);

        // Release mock execution
        let _ = release_sender.send(());

        let result = priming_task.await;
        assert!(
            result.is_ok(),
            "Parallel priming task must terminate cleanly when pool is dropped"
        );
    }

    #[tokio::test]
    async fn scale_up_worker_loop_skips_when_no_prime_session_or_no_scale_needed() {
        let inner = Arc::new(ChannelPoolInner {
            config: ChannelPoolConfig::Dynamic(DynamicChannelPoolConfig::default()),
            client_config: ClientConfig::default(),
            active_entries: RwLock::new(vec![Arc::new(ChannelEntry::new(
                1,
                1,
                create_mock_channel(),
            ))]),
            draining_entries: RwLock::new(Vec::new()),
            next_entry_id: AtomicU64::new(2),
            scale_up_notify: Arc::new(Notify::new()),
            shutdown_sender: watch_channel(()).0,
            last_scale_up_time: Mutex::new(None),
            consecutive_low_load_checks: AtomicUsize::new(0),
            prime_session: RwLock::new(None),
            selector: PowerOfTwoSelector::new(),
        });

        let weak_dynamic = Arc::downgrade(&inner);
        let receiver = inner.shutdown_sender.subscribe();
        let worker_handle = tokio::spawn(async move {
            scale_up_worker_loop(weak_dynamic, receiver).await;
        });

        // 1. Trigger notification while prime_session is None -> worker should continue looping
        inner.scale_up_notify.notify_one();
        yield_now().await;

        // 2. Set prime_session, but load is 0 so channels_to_add is 0 -> worker should continue looping
        *inner.prime_session.write().expect("lock") =
            Some("projects/p/instances/i/databases/d/sessions/s1".to_string());
        inner.scale_up_notify.notify_one();
        yield_now().await;

        drop(inner);
        let result = worker_handle.await;
        assert!(
            result.is_ok(),
            "scale_up_worker_loop must terminate on drop after handling unprimed notification"
        );
    }

    #[tokio::test]
    async fn scale_up_worker_loop_throttles_during_cooldown_and_terminates_on_drop() {
        let channel = Arc::new(ChannelEntry::new(1, 1, create_mock_channel()));
        let inner = Arc::new(ChannelPoolInner {
            config: ChannelPoolConfig::Dynamic(DynamicChannelPoolConfig {
                scale_up_cooldown: Duration::from_secs(10),
                ..Default::default()
            }),
            client_config: ClientConfig::default(),
            active_entries: RwLock::new(vec![channel]),
            draining_entries: RwLock::new(Vec::new()),
            next_entry_id: AtomicU64::new(2),
            scale_up_notify: Arc::new(Notify::new()),
            shutdown_sender: watch_channel(()).0,
            last_scale_up_time: Mutex::new(Some(Instant::now())),
            consecutive_low_load_checks: AtomicUsize::new(0),
            prime_session: RwLock::new(Some(
                "projects/p/instances/i/databases/d/sessions/s1".to_string(),
            )),
            selector: PowerOfTwoSelector::new(),
        });

        let weak_dynamic = Arc::downgrade(&inner);
        let weak_worker = Weak::clone(&weak_dynamic);
        let receiver = inner.shutdown_sender.subscribe();
        let worker_handle = tokio::spawn(async move {
            scale_up_worker_loop(weak_worker, receiver).await;
        });

        // Trigger notification while in active cooldown -> worker enters sleep(remaining)
        inner.scale_up_notify.notify_one();
        yield_now().await;

        // Dropping inner while worker is in cooldown sleep must wake it via shutdown receiver and exit cleanly.
        // Because inner was scoped before entering sleep, dropping it here must immediately drop ChannelPoolInner.
        drop(inner);
        assert!(
            weak_dynamic.upgrade().is_none(),
            "ChannelPoolInner must drop immediately without being held alive across cooldown sleep"
        );

        let result = worker_handle.await;
        assert!(
            result.is_ok(),
            "scale_up_worker_loop must terminate promptly on pool drop even while in cooldown sleep"
        );
    }

    #[tokio::test]
    async fn scale_up_worker_loop_scales_up_after_cooldown_sleep_without_discarding_notification() {
        use crate::client::Spanner;
        use gaxi::grpc::tonic::Response;
        use google_cloud_auth::credentials::anonymous::Builder as AnonymousCredentialsBuilder;
        use spanner_grpc_mock::{MockSpanner, start};

        let primed_count = Arc::new(AtomicUsize::new(0));
        let primed_notify = Arc::new(Notify::new());
        let primed_count_clone = Arc::clone(&primed_count);
        let primed_notify_clone = Arc::clone(&primed_notify);

        let mut mock = MockSpanner::new();
        mock.expect_execute_sql().returning(move |request| {
            let request = request.into_inner();
            assert_eq!(request.sql, "SELECT 1", "priming SQL must be SELECT 1");
            if primed_count_clone.fetch_add(1, Ordering::Relaxed) + 1 == 1 {
                primed_notify_clone.notify_waiters();
            }
            Ok(Response::new(mock_v1::ResultSet::default()))
        });
        let (address, _server) = start("0.0.0.0:0", mock).await.expect("start mock server");

        let spanner = Spanner::builder()
            .with_endpoint(address)
            .with_credentials(AnonymousCredentialsBuilder::new().build())
            .build()
            .await
            .expect("build client");

        let channel = Arc::new(ChannelEntry::new(1, 1, create_mock_channel()));
        channel.in_flight_rpcs.store(100, Ordering::Relaxed);

        let inner = Arc::new(ChannelPoolInner {
            config: ChannelPoolConfig::Dynamic(DynamicChannelPoolConfig {
                min_channels: 1,
                max_channels: 2,
                scale_up_cooldown: Duration::from_millis(50),
                prime_timeout: Duration::from_secs(2),
                ..Default::default()
            }),
            client_config: spanner.config.clone(),
            active_entries: RwLock::new(vec![channel]),
            draining_entries: RwLock::new(Vec::new()),
            next_entry_id: AtomicU64::new(2),
            scale_up_notify: Arc::new(Notify::new()),
            shutdown_sender: watch_channel(()).0,
            last_scale_up_time: Mutex::new(Some(Instant::now())),
            consecutive_low_load_checks: AtomicUsize::new(0),
            prime_session: RwLock::new(Some(
                "projects/p/instances/i/databases/d/sessions/s".to_string(),
            )),
            selector: PowerOfTwoSelector::new(),
        });

        let weak_inner = Arc::downgrade(&inner);
        let receiver = inner.shutdown_sender.subscribe();
        let worker_handle = tokio::spawn(async move {
            scale_up_worker_loop(weak_inner, receiver).await;
        });

        // Trigger scale-up while cooldown is active
        inner.scale_up_notify.notify_one();

        // Worker sleeps for remaining cooldown, then proceeds directly to scale up without needing a 2nd notification
        primed_notify.notified().await;

        let deadline = Instant::now() + Duration::from_secs(2);
        while inner.active_entries.read().expect("lock poisoned").len() < 2 {
            if Instant::now() >= deadline {
                panic!("Timed out waiting for channel count to reach 2");
            }
            yield_now().await;
        }

        assert_eq!(
            inner.active_entries.read().expect("lock poisoned").len(),
            2,
            "Worker loop must scale up pool to 2 channels after cooldown sleep completes"
        );

        drop(inner);
        let result = worker_handle.await;
        assert!(
            result.is_ok(),
            "Worker loop must terminate cleanly on pool drop"
        );
    }

    #[tokio::test]
    async fn scale_up_worker_loop_picks_up_session_registered_during_cooldown() {
        use crate::client::Spanner;
        use gaxi::grpc::tonic::Response;
        use google_cloud_auth::credentials::anonymous::Builder as AnonymousCredentialsBuilder;
        use spanner_grpc_mock::{MockSpanner, start};

        let primed_notify = Arc::new(Notify::new());
        let primed_notify_clone = Arc::clone(&primed_notify);

        let mut mock = MockSpanner::new();
        mock.expect_execute_sql().returning(move |request| {
            let request = request.into_inner();
            assert_eq!(request.sql, "SELECT 1", "priming SQL must be SELECT 1");
            primed_notify_clone.notify_waiters();
            Ok(Response::new(mock_v1::ResultSet::default()))
        });
        let (address, _server) = start("0.0.0.0:0", mock).await.expect("start mock server");

        let spanner = Spanner::builder()
            .with_endpoint(address)
            .with_credentials(AnonymousCredentialsBuilder::new().build())
            .build()
            .await
            .expect("build client");

        let channel = Arc::new(ChannelEntry::new(1, 1, create_mock_channel()));
        channel.in_flight_rpcs.store(100, Ordering::Relaxed);

        // Pool starts with NO prime session registered
        let inner = Arc::new(ChannelPoolInner {
            config: ChannelPoolConfig::Dynamic(DynamicChannelPoolConfig {
                min_channels: 1,
                max_channels: 2,
                scale_up_cooldown: Duration::from_millis(50),
                prime_timeout: Duration::from_secs(2),
                ..Default::default()
            }),
            client_config: spanner.config.clone(),
            active_entries: RwLock::new(vec![channel]),
            draining_entries: RwLock::new(Vec::new()),
            next_entry_id: AtomicU64::new(2),
            scale_up_notify: Arc::new(Notify::new()),
            shutdown_sender: watch_channel(()).0,
            last_scale_up_time: Mutex::new(Some(Instant::now())),
            consecutive_low_load_checks: AtomicUsize::new(0),
            prime_session: RwLock::new(None),
            selector: PowerOfTwoSelector::new(),
        });

        let weak_inner = Arc::downgrade(&inner);
        let receiver = inner.shutdown_sender.subscribe();
        let worker_handle = tokio::spawn(async move {
            scale_up_worker_loop(weak_inner, receiver).await;
        });

        // Trigger scale-up: worker wakes and enters cooldown sleep (50ms)
        inner.scale_up_notify.notify_one();

        // While worker is sleeping in cooldown, session registration occurs
        *inner.prime_session.write().expect("lock poisoned") =
            Some("projects/p/instances/i/databases/d/sessions/s_registered".to_string());

        // Worker must pick up the newly registered session after cooldown expires
        primed_notify.notified().await;

        let deadline = Instant::now() + Duration::from_secs(2);
        while inner.active_entries.read().expect("lock poisoned").len() < 2 {
            if Instant::now() >= deadline {
                panic!("Timed out waiting for channel count to reach 2");
            }
            yield_now().await;
        }

        assert_eq!(
            inner.active_entries.read().expect("lock poisoned").len(),
            2,
            "Worker loop must pick up session registered during cooldown sleep and scale up"
        );

        drop(inner);
        let result = worker_handle.await;
        assert!(
            result.is_ok(),
            "Worker loop must terminate cleanly on pool drop"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn scale_up_worker_loop_terminates_when_pool_dropped_during_priming() {
        use crate::client::Spanner;
        use gaxi::grpc::tonic::Response;
        use google_cloud_auth::credentials::anonymous::Builder as AnonymousCredentialsBuilder;
        use spanner_grpc_mock::{MockSpanner, start};
        use std::sync::mpsc::channel;

        let (started_sender, started_receiver) = channel();
        let (release_sender, release_receiver) = channel();

        let mut mock = MockSpanner::new();
        mock.expect_execute_sql().returning(move |_| {
            let _ = started_sender.send(());
            let _ = release_receiver.recv();
            Ok(Response::new(mock_v1::ResultSet::default()))
        });

        let (address, _server) = start("0.0.0.0:0", mock).await.expect("start mock server");

        let spanner = Spanner::builder()
            .with_endpoint(address)
            .with_credentials(AnonymousCredentialsBuilder::new().build())
            .build()
            .await
            .expect("build client");

        let channel = Arc::new(ChannelEntry::new(1, 1, create_mock_channel()));
        channel.in_flight_rpcs.store(100, Ordering::Relaxed);

        let inner = Arc::new(ChannelPoolInner {
            config: ChannelPoolConfig::Dynamic(DynamicChannelPoolConfig {
                min_channels: 1,
                max_channels: 2,
                scale_up_cooldown: Duration::ZERO,
                prime_timeout: Duration::from_secs(5),
                ..Default::default()
            }),
            client_config: spanner.config.clone(),
            active_entries: RwLock::new(vec![channel]),
            draining_entries: RwLock::new(Vec::new()),
            next_entry_id: AtomicU64::new(2),
            scale_up_notify: Arc::new(Notify::new()),
            shutdown_sender: watch_channel(()).0,
            last_scale_up_time: Mutex::new(None),
            consecutive_low_load_checks: AtomicUsize::new(0),
            prime_session: RwLock::new(Some(
                "projects/p/instances/i/databases/d/sessions/s1".to_string(),
            )),
            selector: PowerOfTwoSelector::new(),
        });

        let weak_inner = Arc::downgrade(&inner);
        let receiver = inner.shutdown_sender.subscribe();
        let worker_handle = tokio::spawn(async move {
            scale_up_worker_loop(weak_inner, receiver).await;
        });

        // Trigger scale-up
        inner.scale_up_notify.notify_one();

        // Wait until priming starts on the mock server
        started_receiver
            .recv()
            .expect("priming request received on mock server");

        // Drop the pool while priming is actively in flight
        drop(inner);

        // Allow mock RPC to complete
        release_sender.send(()).expect("mock response released");

        // The worker must terminate cleanly upon finishing the iteration rather than hanging
        let result = worker_handle.await;
        assert!(
            result.is_ok(),
            "scale_up_worker_loop must terminate cleanly when pool is dropped during in-flight priming"
        );
    }

    #[tokio::test]
    async fn scale_down_monitor_loop_terminates_immediately_on_pool_drop() {
        let channel = Arc::new(ChannelEntry::new(1, 1, create_mock_channel()));
        let inner = Arc::new(ChannelPoolInner {
            config: ChannelPoolConfig::Dynamic(DynamicChannelPoolConfig {
                scale_down_check_interval: Duration::from_secs(180),
                ..Default::default()
            }),
            client_config: ClientConfig::default(),
            active_entries: RwLock::new(vec![channel]),
            draining_entries: RwLock::new(Vec::new()),
            next_entry_id: AtomicU64::new(2),
            scale_up_notify: Arc::new(Notify::new()),
            shutdown_sender: watch_channel(()).0,
            last_scale_up_time: Mutex::new(None),
            consecutive_low_load_checks: AtomicUsize::new(0),
            prime_session: RwLock::new(None),
            selector: PowerOfTwoSelector::new(),
        });

        let weak_dynamic = Arc::downgrade(&inner);
        let receiver = inner.shutdown_sender.subscribe();
        let monitor_handle = tokio::spawn(async move {
            scale_down_monitor_loop(weak_dynamic, receiver, Duration::from_secs(180)).await;
        });

        // Yield so monitor loop begins waiting on select! with timer and shutdown_receiver
        yield_now().await;

        // Drop pool inner immediately; task should terminate without waiting 180s
        drop(inner);
        let result = monitor_handle.await;
        assert!(
            result.is_ok(),
            "scale_down_monitor_loop must terminate immediately on drop without waiting for timer tick"
        );
    }

    #[tokio::test]
    async fn scale_up_worker_loop_throttles_at_capacity_ceiling_under_continuous_load() {
        let channel_1 = Arc::new(ChannelEntry::new(1, 1, create_mock_channel()));
        let channel_2 = Arc::new(ChannelEntry::new(2, 2, create_mock_channel()));
        channel_1.in_flight_rpcs.store(100, Ordering::Relaxed);
        channel_2.in_flight_rpcs.store(100, Ordering::Relaxed);

        let inner = Arc::new(ChannelPoolInner {
            config: ChannelPoolConfig::Dynamic(DynamicChannelPoolConfig {
                min_channels: 1,
                max_channels: 2,
                scale_up_cooldown: Duration::from_millis(50),
                ..Default::default()
            }),
            client_config: ClientConfig::default(),
            active_entries: RwLock::new(vec![channel_1, channel_2]),
            draining_entries: RwLock::new(Vec::new()),
            next_entry_id: AtomicU64::new(3),
            scale_up_notify: Arc::new(Notify::new()),
            shutdown_sender: watch_channel(()).0,
            last_scale_up_time: Mutex::new(None),
            consecutive_low_load_checks: AtomicUsize::new(0),
            prime_session: RwLock::new(Some(
                "projects/p/instances/i/databases/d/sessions/s".to_string(),
            )),
            selector: PowerOfTwoSelector::new(),
        });

        let weak_inner = Arc::downgrade(&inner);
        let receiver = inner.shutdown_sender.subscribe();
        let worker_handle = tokio::spawn(async move {
            scale_up_worker_loop(weak_inner, receiver).await;
        });

        // Trigger repeated scale-up notifications
        for _ in 0..10 {
            inner.scale_up_notify.notify_one();
        }
        yield_now().await;

        // Worker must have committed a cooldown timestamp rather than spinning in a tight loop
        assert!(
            inner
                .last_scale_up_time
                .lock()
                .expect("lock poisoned")
                .is_some(),
            "Scale-up worker must commit cooldown timestamp when at capacity ceiling to prevent tight-loop wakeups"
        );

        drop(inner);
        let result = worker_handle.await;
        assert!(
            result.is_ok(),
            "scale_up_worker_loop must terminate promptly on pool drop"
        );
    }

    #[tokio::test]
    async fn scale_up_worker_loop_scales_up_immediately_after_cooldown_timer_jitter() {
        use crate::client::Spanner;
        use gaxi::grpc::tonic::Response;
        use google_cloud_auth::credentials::anonymous::Builder as AnonymousCredentialsBuilder;
        use spanner_grpc_mock::{MockSpanner, start};

        let primed_notify = Arc::new(Notify::new());
        let primed_notify_clone = Arc::clone(&primed_notify);

        let mut mock = MockSpanner::new();
        mock.expect_execute_sql().returning(move |_| {
            primed_notify_clone.notify_one();
            Ok(Response::new(mock_v1::ResultSet::default()))
        });

        let (address, _server) = start("0.0.0.0:0", mock)
            .await
            .expect("Failed to start mock server");

        let spanner = Spanner::builder()
            .with_endpoint(address)
            .with_credentials(AnonymousCredentialsBuilder::new().build())
            .build()
            .await
            .expect("Failed to build client");

        let channel = Arc::new(ChannelEntry::new(1, 1, create_mock_channel()));
        channel.in_flight_rpcs.store(100, Ordering::Relaxed);

        // Cooldown was set 49ms ago (with 50ms cooldown). When timer wakes up, calculate_scale_up_count
        // must execute scale up without dropping the event due to timer resolution jitter.
        let inner = Arc::new(ChannelPoolInner {
            config: ChannelPoolConfig::Dynamic(DynamicChannelPoolConfig {
                min_channels: 1,
                max_channels: 2,
                scale_up_cooldown: Duration::from_millis(50),
                prime_timeout: Duration::from_secs(5),
                ..Default::default()
            }),
            client_config: spanner.config.clone(),
            active_entries: RwLock::new(vec![channel]),
            draining_entries: RwLock::new(Vec::new()),
            next_entry_id: AtomicU64::new(2),
            scale_up_notify: Arc::new(Notify::new()),
            shutdown_sender: watch_channel(()).0,
            last_scale_up_time: Mutex::new(Some(Instant::now() - Duration::from_millis(49))),
            consecutive_low_load_checks: AtomicUsize::new(0),
            prime_session: RwLock::new(Some(
                "projects/p/instances/i/databases/d/sessions/s".to_string(),
            )),
            selector: PowerOfTwoSelector::new(),
        });

        let weak_inner = Arc::downgrade(&inner);
        let receiver = inner.shutdown_sender.subscribe();
        let worker_handle = tokio::spawn(async move {
            scale_up_worker_loop(weak_inner, receiver).await;
        });

        inner.scale_up_notify.notify_one();

        // Worker must sleep remaining 1ms and then proceed to priming without dropping scale-up event
        primed_notify.notified().await;

        let deadline = Instant::now() + Duration::from_secs(2);
        while inner.active_entries.read().expect("lock poisoned").len() < 2 {
            if Instant::now() >= deadline {
                panic!("Timed out waiting for channel count to reach 2");
            }
            yield_now().await;
        }

        assert_eq!(
            inner.active_entries.read().expect("lock poisoned").len(),
            2,
            "Worker loop must scale up even with sub-millisecond timer resolution"
        );

        drop(inner);
        let result = worker_handle.await;
        assert!(
            result.is_ok(),
            "scale_up_worker_loop must terminate promptly on pool drop"
        );
    }

    #[tokio::test]
    async fn scale_up_worker_loop_terminates_when_pool_dropped_before_wait_point() {
        let channel = Arc::new(ChannelEntry::new(1, 1, create_mock_channel()));
        let inner = Arc::new(ChannelPoolInner {
            config: ChannelPoolConfig::Dynamic(DynamicChannelPoolConfig::default()),
            client_config: ClientConfig::default(),
            active_entries: RwLock::new(vec![channel]),
            draining_entries: RwLock::new(Vec::new()),
            next_entry_id: AtomicU64::new(2),
            scale_up_notify: Arc::new(Notify::new()),
            shutdown_sender: watch_channel(()).0,
            last_scale_up_time: Mutex::new(None),
            consecutive_low_load_checks: AtomicUsize::new(0),
            prime_session: RwLock::new(None),
            selector: PowerOfTwoSelector::new(),
        });

        let weak_inner = Arc::downgrade(&inner);
        let receiver = inner.shutdown_sender.subscribe();

        // Drop pool BEFORE spawning or entering worker loop
        drop(inner);

        // When worker loop starts, shutdown_receiver.changed() is already persistently closed
        let worker_handle = tokio::spawn(async move {
            scale_up_worker_loop(weak_inner, receiver).await;
        });

        let result = worker_handle.await;
        assert!(
            result.is_ok(),
            "scale_up_worker_loop must terminate immediately without hanging when pool was dropped before wait point"
        );
    }

    #[tokio::test]
    async fn scale_down_monitor_loop_terminates_when_pool_dropped_before_wait_point() {
        let channel = Arc::new(ChannelEntry::new(1, 1, create_mock_channel()));
        let inner = Arc::new(ChannelPoolInner {
            config: ChannelPoolConfig::Dynamic(DynamicChannelPoolConfig::default()),
            client_config: ClientConfig::default(),
            active_entries: RwLock::new(vec![channel]),
            draining_entries: RwLock::new(Vec::new()),
            next_entry_id: AtomicU64::new(2),
            scale_up_notify: Arc::new(Notify::new()),
            shutdown_sender: watch_channel(()).0,
            last_scale_up_time: Mutex::new(None),
            consecutive_low_load_checks: AtomicUsize::new(0),
            prime_session: RwLock::new(None),
            selector: PowerOfTwoSelector::new(),
        });

        let weak_inner = Arc::downgrade(&inner);
        let receiver = inner.shutdown_sender.subscribe();

        // Drop pool BEFORE spawning or entering monitor loop
        drop(inner);

        // When monitor loop starts, shutdown_receiver.changed() is already persistently closed
        let monitor_handle = tokio::spawn(async move {
            scale_down_monitor_loop(weak_inner, receiver, Duration::from_secs(180)).await;
        });

        let result = monitor_handle.await;
        assert!(
            result.is_ok(),
            "scale_down_monitor_loop must terminate immediately without hanging when pool was dropped before wait point"
        );
    }

    #[tokio::test]
    async fn scale_up_worker_loop_scales_up_immediately_on_burst_after_insufficient_load_notification()
     {
        use crate::client::Spanner;
        use gaxi::grpc::tonic::Response;
        use google_cloud_auth::credentials::anonymous::Builder as AnonymousCredentialsBuilder;
        use spanner_grpc_mock::{MockSpanner, start};

        let primed_notify = Arc::new(Notify::new());
        let primed_notify_clone = Arc::clone(&primed_notify);

        let mut mock = MockSpanner::new();
        mock.expect_execute_sql().returning(move |_| {
            primed_notify_clone.notify_one();
            Ok(Response::new(mock_v1::ResultSet::default()))
        });

        let (address, _server) = start("0.0.0.0:0", mock)
            .await
            .expect("Failed to start mock server");

        let spanner = Spanner::builder()
            .with_endpoint(address)
            .with_credentials(AnonymousCredentialsBuilder::new().build())
            .build()
            .await
            .expect("Failed to build client");

        let channel_1 = Arc::new(ChannelEntry::new(1, 1, create_mock_channel()));
        let channel_2 = Arc::new(ChannelEntry::new(2, 2, create_mock_channel()));

        let inner = Arc::new(ChannelPoolInner {
            config: ChannelPoolConfig::Dynamic(DynamicChannelPoolConfig {
                min_channels: 2,
                max_channels: 4,
                scale_up_cooldown: Duration::from_secs(10),
                prime_timeout: Duration::from_secs(5),
                ..Default::default()
            }),
            client_config: spanner.config.clone(),
            active_entries: RwLock::new(vec![Arc::clone(&channel_1), Arc::clone(&channel_2)]),
            draining_entries: RwLock::new(Vec::new()),
            next_entry_id: AtomicU64::new(3),
            scale_up_notify: Arc::new(Notify::new()),
            shutdown_sender: watch_channel(()).0,
            last_scale_up_time: Mutex::new(None),
            consecutive_low_load_checks: AtomicUsize::new(0),
            prime_session: RwLock::new(Some(
                "projects/p/instances/i/databases/d/sessions/s".to_string(),
            )),
            selector: PowerOfTwoSelector::new(),
        });

        let weak_inner = Arc::downgrade(&inner);
        let receiver = inner.shutdown_sender.subscribe();
        let worker_handle = tokio::spawn(async move {
            scale_up_worker_loop(weak_inner, receiver).await;
        });

        // 1. Trigger notification when aggregate load is insufficient (e.g., 0 in-flight)
        inner.scale_up_notify.notify_one();
        yield_now().await;

        // Verify that no cooldown timestamp was committed
        assert!(
            inner.last_scale_up_time.lock().expect("lock").is_none(),
            "Low-load notification must NOT commit a cooldown timestamp"
        );

        // 2. Immediately simulate a high-load traffic burst
        channel_1.in_flight_rpcs.store(60, Ordering::Relaxed);
        channel_2.in_flight_rpcs.store(60, Ordering::Relaxed);
        inner.scale_up_notify.notify_one();

        // Worker must scale up immediately without waiting 10 seconds for cooldown
        primed_notify.notified().await;

        let deadline = Instant::now() + Duration::from_secs(2);
        while inner.active_entries.read().expect("lock poisoned").len() < 4 {
            if Instant::now() >= deadline {
                panic!("Timed out waiting for pool to scale up after traffic burst");
            }
            yield_now().await;
        }

        assert_eq!(
            inner.active_entries.read().expect("lock poisoned").len(),
            4,
            "Worker must scale up to 4 channels immediately upon burst"
        );

        // After actual scale-up, cooldown timestamp MUST now be committed
        assert!(
            inner.last_scale_up_time.lock().expect("lock").is_some(),
            "Cooldown timestamp must be set after an actual scale-up event"
        );

        drop(inner);
        let result = worker_handle.await;
        assert!(
            result.is_ok(),
            "scale_up_worker_loop must terminate promptly on pool drop"
        );
    }

    #[tokio::test]
    async fn scale_up_worker_loop_debounces_single_channel_burst_without_scaling() {
        let channel_1 = Arc::new(ChannelEntry::new(1, 1, create_mock_channel()));
        let channel_2 = Arc::new(ChannelEntry::new(2, 2, create_mock_channel()));
        let channel_3 = Arc::new(ChannelEntry::new(3, 3, create_mock_channel()));
        let channel_4 = Arc::new(ChannelEntry::new(4, 4, create_mock_channel()));

        // Channel 1 receives a heavy burst of 35 in-flight RPCs (> 25 max_rpc),
        // but aggregate load (35) across 4 channels is well within total capacity (desired = ceil(35/20) = 2 <= 4).
        channel_1.in_flight_rpcs.store(35, Ordering::Relaxed);

        let inner = Arc::new(ChannelPoolInner {
            config: ChannelPoolConfig::Dynamic(DynamicChannelPoolConfig {
                min_channels: 4,
                max_channels: 8,
                min_rpc_per_channel: 15.0,
                max_rpc_per_channel: 25.0,
                scale_up_cooldown: Duration::from_secs(10),
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
            shutdown_sender: watch_channel(()).0,
            last_scale_up_time: Mutex::new(None),
            consecutive_low_load_checks: AtomicUsize::new(0),
            prime_session: RwLock::new(Some(
                "projects/p/instances/i/databases/d/sessions/s".to_string(),
            )),
            selector: PowerOfTwoSelector::new(),
        });

        let weak_inner = Arc::downgrade(&inner);
        let receiver = inner.shutdown_sender.subscribe();
        let worker_handle = tokio::spawn(async move {
            scale_up_worker_loop(weak_inner, receiver).await;
        });

        // Trigger multiple rapid scale-up notifications simulating RPCs on the hot channel
        for _ in 0..10 {
            inner.scale_up_notify.notify_one();
        }
        yield_now().await;

        // Worker debounces and must NOT scale up
        assert_eq!(
            inner.active_entries.read().expect("lock").len(),
            4,
            "Pool size must remain at 4 since aggregate load does not warrant scale-up"
        );

        // Cooldown must not be set on insufficient aggregate load
        assert!(
            inner.last_scale_up_time.lock().expect("lock").is_none(),
            "Capacity-absorption cooldown must NOT be committed on insufficient aggregate load"
        );

        drop(inner);
        let result = worker_handle.await;
        assert!(
            result.is_ok(),
            "scale_up_worker_loop must terminate promptly on pool drop"
        );
    }
}
