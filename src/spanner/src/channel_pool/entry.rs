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

//! Channel entry lifecycle, atomic accounting, and RAII drop guards.

use crate::client::Channel;
use google_cloud_gax::error::rpc::Code;
use std::result::Result;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicU32, AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Spanner server automatically aborts idle Read/Write transactions after 10 seconds.
pub(crate) const SPANNER_RW_TRANSACTION_IDLE_TIMEOUT: Duration = Duration::from_secs(10);

/// Lifecycle state of an individual gRPC channel in the pool.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ChannelState {
    /// Actively serving new traffic and eligible for P2C picker selection.
    Active = 0,
    /// Draining in-flight RPCs and active transactions; closed to new traffic.
    Draining = 1,
    /// Draining completed; physical connection is closed.
    Closed = 2,
}

/// Managed entry representing a single physical gRPC channel in the pool.
#[derive(Debug)]
pub(crate) struct ChannelEntry {
    /// Monotonically increasing unique internal ID for transaction affinity pinning.
    pub(crate) id: u64,
    /// Logical 1-based channel slot (1..=max_channels) passed to `x-goog-spanner-request-id`.
    pub(crate) logical_channel_id: usize,
    /// Physical gRPC channel instance.
    pub(crate) channel: Channel,
    /// Count of active RPCs currently executing over the wire.
    pub(crate) in_flight_rpcs: AtomicU32,
    /// Count of active Read/Write transactions pinned to this channel.
    pub(crate) active_rw_transactions: AtomicU32,
    /// Accumulated synthetic error penalty load.
    pub(crate) penalty_load: AtomicU32,
    /// Expiry nanoseconds offset from `created_at` (0 if no active penalty).
    pub(crate) penalty_expiry_nanos: AtomicU64,
    /// Lifecycle state of the channel.
    pub(crate) state: AtomicU8,
    /// Creation instant of the channel entry used as a monotonic reference baseline.
    pub(crate) created_at: Instant,
    /// Elapsed nanoseconds from `created_at` corresponding to the most recent RPC activity.
    pub(crate) last_activity_nanos: AtomicU64,
}

impl ChannelEntry {
    /// Creates a new `ChannelEntry`.
    pub(crate) fn new(id: u64, logical_channel_id: usize, channel: Channel) -> Self {
        Self::new_with_created_at(id, logical_channel_id, channel, Instant::now())
    }

    /// Creates a new `ChannelEntry` with a specific `created_at` baseline instant (useful in tests).
    pub(crate) fn new_with_created_at(
        id: u64,
        logical_channel_id: usize,
        channel: Channel,
        created_at: Instant,
    ) -> Self {
        Self {
            id,
            logical_channel_id,
            channel,
            in_flight_rpcs: AtomicU32::new(0),
            active_rw_transactions: AtomicU32::new(0),
            penalty_load: AtomicU32::new(0),
            penalty_expiry_nanos: AtomicU64::new(0),
            state: AtomicU8::new(ChannelState::Active as u8),
            created_at,
            last_activity_nanos: AtomicU64::new(0),
        }
    }

    /// Updates the last activity timestamp to the current instant in a lock-free manner.
    pub(crate) fn touch_activity(&self) {
        let elapsed = self.created_at.elapsed().as_nanos() as u64;
        self.last_activity_nanos
            .fetch_max(elapsed, Ordering::Relaxed);
    }

    /// Returns the raw activity timestamp in nanoseconds from `created_at` for warmth comparisons.
    pub(crate) fn last_activity_nanos(&self) -> u64 {
        self.last_activity_nanos.load(Ordering::Relaxed)
    }

    /// Returns the current number of in-flight RPCs on this channel.
    pub(crate) fn in_flight(&self) -> u32 {
        self.in_flight_rpcs.load(Ordering::Relaxed)
    }

    /// Returns the current number of active Read/Write transactions pinned to this channel.
    pub(crate) fn active_rw_count(&self) -> u32 {
        self.active_rw_transactions.load(Ordering::Relaxed)
    }

    /// Returns the active synthetic error penalty load, or 0 if expired (lock-free).
    pub(crate) fn current_penalty(&self) -> u32 {
        let expiry = self.penalty_expiry_nanos.load(Ordering::Acquire);
        if expiry == 0 {
            return 0;
        }
        if (self.created_at.elapsed().as_nanos() as u64) >= expiry {
            return 0;
        }
        self.penalty_load.load(Ordering::Relaxed)
    }

    /// Returns the effective picker load evaluated by P2C (in-flight load + active error penalty).
    pub(crate) fn effective_pick_load(&self) -> u32 {
        self.in_flight() + self.current_penalty()
    }

    /// Applies a sliding penalty when qualifying transport-level errors occur (lock-free).
    pub(crate) fn apply_error_penalty(
        &self,
        code: Code,
        step: u32,
        duration: Duration,
        max_penalty: u32,
    ) {
        if code != Code::Unavailable && code != Code::ResourceExhausted {
            return;
        }
        let current_load = self.current_penalty();
        let new_load = (current_load + step).min(max_penalty);
        let now_nanos = self.created_at.elapsed().as_nanos() as u64;
        self.penalty_load.store(new_load, Ordering::Relaxed);
        self.penalty_expiry_nanos.store(
            now_nanos.saturating_add(duration.as_nanos() as u64),
            Ordering::Release,
        );
    }

    /// Returns the duration elapsed since the channel's last recorded activity.
    pub(crate) fn elapsed_since_activity(&self) -> Duration {
        let last = self.last_activity_nanos.load(Ordering::Relaxed);
        let now = self.created_at.elapsed().as_nanos() as u64;
        Duration::from_nanos(now.saturating_sub(last))
    }

    /// Returns the current lifecycle state of the channel.
    pub(crate) fn state(&self) -> ChannelState {
        match self.state.load(Ordering::Relaxed) {
            0 => ChannelState::Active,
            1 => ChannelState::Draining,
            _ => ChannelState::Closed,
        }
    }

    /// Sets the channel's lifecycle state.
    pub(crate) fn set_state(&self, state: ChannelState) {
        self.state.store(state as u8, Ordering::Relaxed);
    }

    /// Checks if the channel entry is currently in the `Active` state.
    pub(crate) fn is_active(&self) -> bool {
        self.state() == ChannelState::Active
    }

    /// Checks if the channel entry is currently in the `Draining` state.
    pub(crate) fn is_draining(&self) -> bool {
        self.state() == ChannelState::Draining
    }

    /// Checks if the channel entry is currently in the `Closed` state.
    pub(crate) fn is_closed(&self) -> bool {
        self.state() == ChannelState::Closed
    }
}

/// RAII guard that tracks an active in-flight RPC on a channel entry.
///
/// Decrements `in_flight_rpcs` and updates the channel's activity timestamp upon drop.
#[must_use = "if unused the in-flight RPC count will decrement immediately"]
pub(crate) struct ActiveRpcGuard {
    pub(crate) entry: Arc<ChannelEntry>,
    penalty_step: u32,
    penalty_duration: Duration,
    penalty_max: u32,
}

impl ActiveRpcGuard {
    /// Creates a new `ActiveRpcGuard` for the given channel entry.
    pub(crate) fn new(
        entry: Arc<ChannelEntry>,
        penalty_step: u32,
        penalty_duration: Duration,
        penalty_max: u32,
    ) -> Self {
        entry.in_flight_rpcs.fetch_add(1, Ordering::Relaxed);
        entry.touch_activity();
        Self {
            entry,
            penalty_step,
            penalty_duration,
            penalty_max,
        }
    }

    /// Records the result of an RPC call and applies an error penalty if a qualifying error occurred.
    pub(crate) fn record_result<T, E>(
        &self,
        result: &Result<T, E>,
        extract_code: impl Fn(&E) -> Option<Code>,
    ) {
        if let Err(err) = result
            && let Some(code) = extract_code(err)
        {
            self.entry.apply_error_penalty(
                code,
                self.penalty_step,
                self.penalty_duration,
                self.penalty_max,
            );
        }
    }

    /// Records a specific `Code` error directly.
    pub(crate) fn record_error_code(&self, code: Code) {
        self.entry.apply_error_penalty(
            code,
            self.penalty_step,
            self.penalty_duration,
            self.penalty_max,
        );
    }
}

impl Drop for ActiveRpcGuard {
    fn drop(&mut self) {
        self.entry.in_flight_rpcs.fetch_sub(1, Ordering::Relaxed);
        self.entry.touch_activity();
    }
}

/// RAII token held by an active Read/Write transaction to prevent premature channel closure during draining.
pub(crate) struct RwTransactionAffinityGuard {
    entry: Arc<ChannelEntry>,
}

impl RwTransactionAffinityGuard {
    /// Creates a new `RwTransactionAffinityGuard`.
    pub(crate) fn new(entry: Arc<ChannelEntry>) -> Self {
        entry.active_rw_transactions.fetch_add(1, Ordering::Relaxed);
        Self { entry }
    }
}

impl Drop for RwTransactionAffinityGuard {
    fn drop(&mut self) {
        self.entry
            .active_rw_transactions
            .fetch_sub(1, Ordering::Relaxed);
        self.entry.touch_activity();
    }
}

/// A short-lived, caller-held leased channel reference returned by channel selection.
///
/// Bundles:
/// 1. A borrowed reference to the physical `Channel` for executing the gRPC RPC.
/// 2. The unique `logical_channel_id` (1..=max_channels) to attach to `x-goog-spanner-request-id`.
/// 3. The monotonic `entry_id` used for transaction affinity pinning.
/// 4. An embedded `ActiveRpcGuard` that automatically decrements in-flight accounting and
///    records transport error penalties on drop.
#[must_use = "if unused the leased channel's in-flight RPC count will decrement immediately"]
pub(crate) struct ChannelLease {
    pub(crate) guard: ActiveRpcGuard,
}

impl ChannelLease {
    /// Creates a new `ChannelLease` wrapping an active RPC guard.
    pub(crate) fn new(guard: ActiveRpcGuard) -> Self {
        Self { guard }
    }

    /// Returns a reference to the physical `Channel`.
    pub(crate) fn channel(&self) -> &Channel {
        &self.guard.entry.channel
    }

    /// Returns the logical 1-based channel slot (1..=max_channels) for request ID tagging.
    pub(crate) fn logical_channel_id(&self) -> usize {
        self.guard.entry.logical_channel_id
    }

    /// Returns the unique monotonic internal entry ID.
    pub(crate) fn entry_id(&self) -> u64 {
        self.guard.entry.id
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Response;
    use crate::generated::gapic_dataplane::stub::Spanner as SpannerStub;
    use crate::model::{CreateSessionRequest, Session};
    use google_cloud_gax::error::rpc::Status;
    use google_cloud_gax::options::RequestOptions;
    use std::fmt::Debug;
    use std::future::{Future, ready};
    use std::result::Result;

    #[derive(Debug, Default)]
    struct MockSpannerStub;

    impl SpannerStub for MockSpannerStub {
        fn create_session(
            &self,
            _req: CreateSessionRequest,
            _options: RequestOptions,
        ) -> impl Future<Output = crate::Result<Response<Session>>> + Send {
            ready(Ok(Response::from(Session::default())))
        }
    }

    fn create_mock_channel() -> Channel {
        Channel::new_for_test(MockSpannerStub)
    }

    #[test]
    fn traits() {
        static_assertions::assert_impl_all!(ChannelEntry: Debug, Send, Sync);
        static_assertions::assert_impl_all!(ActiveRpcGuard: Send, Sync);
        static_assertions::assert_impl_all!(RwTransactionAffinityGuard: Send, Sync);
        static_assertions::assert_impl_all!(ChannelLease: Send, Sync);
        static_assertions::assert_impl_all!(
            ChannelState: Clone,
            Copy,
            Debug,
            PartialEq,
            Eq,
            Send,
            Sync
        );
    }

    #[test]
    fn error_penalty_allowlist_and_sliding_expiry() {
        let channel = create_mock_channel();
        let entry = ChannelEntry::new(1, 1, channel);

        // Non-qualifying errors should not apply penalty
        entry.apply_error_penalty(Code::Aborted, 5, Duration::from_secs(5), 25);
        assert_eq!(entry.current_penalty(), 0, "Aborted must not apply penalty");

        entry.apply_error_penalty(Code::InvalidArgument, 5, Duration::from_secs(5), 25);
        assert_eq!(
            entry.current_penalty(),
            0,
            "InvalidArgument must not apply penalty"
        );

        entry.apply_error_penalty(Code::NotFound, 5, Duration::from_secs(5), 25);
        assert_eq!(
            entry.current_penalty(),
            0,
            "NotFound must not apply penalty"
        );

        // Qualifying error: Unavailable
        entry.apply_error_penalty(Code::Unavailable, 5, Duration::from_secs(5), 25);
        assert_eq!(
            entry.current_penalty(),
            5,
            "Unavailable must add +5 penalty"
        );

        // Consecutive qualifying error: ResourceExhausted extends and increments
        entry.apply_error_penalty(Code::ResourceExhausted, 5, Duration::from_secs(5), 25);
        assert_eq!(
            entry.current_penalty(),
            10,
            "ResourceExhausted must add +5 penalty"
        );

        // Penalty capping at 25
        for _ in 0..5 {
            entry.apply_error_penalty(Code::Unavailable, 5, Duration::from_secs(5), 25);
        }
        assert_eq!(entry.current_penalty(), 25, "Penalty must cap at 25");

        // Simulated expiry using atomic nanoseconds offset
        entry.penalty_expiry_nanos.store(1, Ordering::Relaxed);
        assert_eq!(entry.current_penalty(), 0, "Expired penalty must return 0");
    }

    #[test]
    fn active_rpc_guard_drop_accounting() {
        let channel = create_mock_channel();
        let entry = Arc::new(ChannelEntry::new(1, 1, channel));
        assert_eq!(
            entry.in_flight(),
            0,
            "Initial in-flight RPC count must be 0"
        );

        {
            let guard = ActiveRpcGuard::new(Arc::clone(&entry), 5, Duration::from_secs(5), 25);
            assert_eq!(
                entry.in_flight(),
                1,
                "In-flight count must increment on guard creation"
            );

            // Record error result
            let status = Status::default()
                .set_code(Code::Unavailable)
                .set_message("server unavailable");
            let err_result: Result<(), Status> = Err(status);
            guard.record_result(&err_result, |status| Some(status.code));
            assert_eq!(
                entry.current_penalty(),
                5,
                "Unavailable error must record penalty of 5"
            );
        }

        assert_eq!(
            entry.in_flight(),
            0,
            "In-flight count must decrement on guard drop"
        );
    }

    #[test]
    fn rw_transaction_affinity_guard_drop_accounting() {
        let channel = create_mock_channel();
        let entry = Arc::new(ChannelEntry::new(1, 1, channel));
        assert_eq!(
            entry.active_rw_count(),
            0,
            "Initial active R/W transaction count must be 0"
        );

        {
            let _guard = RwTransactionAffinityGuard::new(Arc::clone(&entry));
            assert_eq!(
                entry.active_rw_count(),
                1,
                "Active R/W count must increment on guard creation"
            );
        }

        assert_eq!(
            entry.active_rw_count(),
            0,
            "Active R/W count must decrement on guard drop"
        );
    }
}
