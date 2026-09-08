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
    /// Packed representation of synthetic error penalty load and expiry:
    /// - Bits 48..63 (16 bits): penalty load
    /// - Bits 0..47  (48 bits): expiry timestamp in milliseconds from `created_at`
    pub(crate) penalty_state: AtomicU64,
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
        mut channel: Channel,
        created_at: Instant,
    ) -> Self {
        channel.channel_id = logical_channel_id;
        Self {
            id,
            logical_channel_id,
            channel,
            in_flight_rpcs: AtomicU32::new(0),
            active_rw_transactions: AtomicU32::new(0),
            penalty_state: AtomicU64::new(0),
            state: AtomicU8::new(ChannelState::Active as u8),
            created_at,
            last_activity_nanos: AtomicU64::new(0),
        }
    }

    pub(crate) fn decode_penalty_state(packed: u64) -> (u32, u64) {
        let penalty_load = (packed >> 48) as u32;
        let expiry_millis = packed & 0x0000_FFFF_FFFF_FFFF;
        (penalty_load, expiry_millis)
    }

    pub(crate) fn encode_penalty_state(penalty_load: u32, expiry_millis: u64) -> u64 {
        let load_bits = u64::from(u16::try_from(penalty_load).unwrap_or(u16::MAX)) << 48;
        let expiry_bits = expiry_millis.min(0x0000_FFFF_FFFF_FFFF);
        load_bits | expiry_bits
    }

    /// Updates the last activity timestamp to the current instant in a lock-free manner.
    pub(crate) fn touch_activity(&self) {
        let elapsed = u64::try_from(self.created_at.elapsed().as_nanos()).unwrap_or(u64::MAX);
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
        let packed = self.penalty_state.load(Ordering::Acquire);
        if packed == 0 {
            return 0;
        }
        let (penalty_load, expiry_millis) = Self::decode_penalty_state(packed);
        if penalty_load == 0 || expiry_millis == 0 {
            return 0;
        }
        let now_millis = u64::try_from(self.created_at.elapsed().as_millis()).unwrap_or(u64::MAX);
        if now_millis >= expiry_millis {
            return 0;
        }
        penalty_load
    }

    /// Returns the effective picker load evaluated by P2C (in-flight load + active error penalty).
    pub(crate) fn effective_pick_load(&self) -> u32 {
        self.in_flight().saturating_add(self.current_penalty())
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
        let duration_millis = u64::try_from(duration.as_millis()).unwrap_or(u64::MAX);

        let _ = self
            .penalty_state
            .fetch_update(Ordering::Release, Ordering::Acquire, |packed| {
                let now_millis =
                    u64::try_from(self.created_at.elapsed().as_millis()).unwrap_or(u64::MAX);
                let (current_load, expiry_millis) = Self::decode_penalty_state(packed);
                let active_load = if now_millis < expiry_millis {
                    current_load
                } else {
                    0
                };
                let new_load = active_load.saturating_add(step).min(max_penalty);
                let new_expiry_millis = now_millis.saturating_add(duration_millis);
                Some(Self::encode_penalty_state(new_load, new_expiry_millis))
            });
    }

    /// Returns the duration elapsed since the channel's last recorded activity.
    pub(crate) fn elapsed_since_activity(&self) -> Duration {
        let last = self.last_activity_nanos.load(Ordering::Relaxed);
        let now = u64::try_from(self.created_at.elapsed().as_nanos()).unwrap_or(u64::MAX);
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
#[derive(Debug)]
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
        if let Some(code) = result.as_ref().err().and_then(extract_code) {
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
#[derive(Debug)]
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
#[derive(Debug)]
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

    /// Creates an RAII guard that pins this channel entry for an active Read/Write transaction.
    pub(crate) fn rw_affinity_guard(&self) -> RwTransactionAffinityGuard {
        RwTransactionAffinityGuard::new(Arc::clone(&self.guard.entry))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Response;
    use crate::Result as SpannerResult;
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
        ) -> impl Future<Output = SpannerResult<Response<Session>>> + Send {
            ready(Ok(Response::from(Session::default())))
        }
    }

    fn create_mock_channel() -> Channel {
        Channel::new_for_test(MockSpannerStub)
    }

    #[test]
    fn traits() {
        static_assertions::assert_impl_all!(ChannelEntry: Debug, Send, Sync);
        static_assertions::assert_impl_all!(ActiveRpcGuard: Debug, Send, Sync);
        static_assertions::assert_impl_all!(RwTransactionAffinityGuard: Debug, Send, Sync);
        static_assertions::assert_impl_all!(ChannelLease: Debug, Send, Sync);
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
        let entry = ChannelEntry::new_with_created_at(
            1,
            1,
            channel,
            Instant::now() - Duration::from_secs(10),
        );

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

        // Extremely large duration (e.g. Duration::MAX) does not truncate and stays active
        entry.apply_error_penalty(Code::Unavailable, 5, Duration::MAX, 25);
        assert_eq!(
            entry.current_penalty(),
            25,
            "Duration::MAX must not cause truncation and must keep penalty active"
        );

        // Simulated expiry using encoded expired timestamp
        entry
            .penalty_state
            .store(ChannelEntry::encode_penalty_state(10, 1), Ordering::Relaxed);
        assert_eq!(entry.current_penalty(), 0, "Expired penalty must return 0");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_error_penalty_updates_do_not_lose_increments() {
        let channel = create_mock_channel();
        let entry = Arc::new(ChannelEntry::new(1, 1, channel));

        let mut handles = Vec::new();
        for _ in 0..10 {
            let entry_clone = Arc::clone(&entry);
            handles.push(tokio::spawn(async move {
                entry_clone.apply_error_penalty(Code::Unavailable, 1, Duration::from_secs(60), 100);
            }));
        }

        for handle in handles {
            handle.await.expect("task completed successfully");
        }

        assert_eq!(
            entry.current_penalty(),
            10,
            "All 10 concurrent penalty increments must be recorded atomically"
        );
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

    #[test]
    fn channel_entry_lifecycle_and_activity_tracking() {
        let channel = create_mock_channel();
        let entry = ChannelEntry::new(10, 2, channel);

        assert_eq!(entry.id, 10, "id must match constructor arg");
        assert_eq!(
            entry.logical_channel_id, 2,
            "logical_channel_id must match constructor arg"
        );
        assert!(entry.is_active(), "New channel entry must start Active");
        assert!(
            !entry.is_draining(),
            "New channel entry must not be Draining"
        );
        assert!(!entry.is_closed(), "New channel entry must not be Closed");
        assert_eq!(
            entry.state(),
            ChannelState::Active,
            "entry must be in Active state"
        );

        entry.set_state(ChannelState::Draining);
        assert!(
            !entry.is_active(),
            "Entry must not be Active after set_state(Draining)"
        );
        assert!(
            entry.is_draining(),
            "Entry must be Draining after set_state(Draining)"
        );
        assert!(
            !entry.is_closed(),
            "Entry must not be Closed after set_state(Draining)"
        );
        assert_eq!(
            entry.state(),
            ChannelState::Draining,
            "entry must be in Draining state"
        );

        entry.set_state(ChannelState::Closed);
        assert!(
            !entry.is_active(),
            "Entry must not be Active after set_state(Closed)"
        );
        assert!(
            !entry.is_draining(),
            "Entry must not be Draining after set_state(Closed)"
        );
        assert!(
            entry.is_closed(),
            "Entry must be Closed after set_state(Closed)"
        );
        assert_eq!(
            entry.state(),
            ChannelState::Closed,
            "entry must be in Closed state"
        );

        // Activity timestamps
        let initial_activity = entry.last_activity_nanos();
        assert_eq!(initial_activity, 0, "Initial last_activity_nanos must be 0");

        entry.touch_activity();
        let updated_activity = entry.last_activity_nanos();
        assert!(
            updated_activity > 0,
            "touch_activity() must set last_activity_nanos > 0"
        );
        let elapsed = entry.elapsed_since_activity();
        assert!(
            elapsed <= Duration::from_secs(1),
            "Elapsed since recent activity must be very small"
        );

        // Effective pick load combines in_flight + penalty
        assert_eq!(
            entry.effective_pick_load(),
            0,
            "Effective load with 0 in-flight and 0 penalty must be 0"
        );
        entry.in_flight_rpcs.store(3, Ordering::Relaxed);
        assert_eq!(
            entry.effective_pick_load(),
            3,
            "Effective load with 3 in-flight and 0 penalty must be 3"
        );
        entry.apply_error_penalty(Code::Unavailable, 5, Duration::from_secs(5), 25);
        assert_eq!(
            entry.effective_pick_load(),
            8,
            "Effective load with 3 in-flight and 5 penalty must be 8"
        );
    }

    #[test]
    fn active_rpc_guard_record_variations() {
        let channel = create_mock_channel();
        let entry = Arc::new(ChannelEntry::new(1, 1, channel));
        let guard = ActiveRpcGuard::new(Arc::clone(&entry), 5, Duration::from_secs(5), 25);

        // Record Ok result -> no penalty
        let ok_result: Result<&str, Status> = Ok("success");
        guard.record_result(&ok_result, |status| Some(status.code));
        assert_eq!(
            entry.current_penalty(),
            0,
            "Ok result must not apply error penalty"
        );

        // Record Err where extractor returns None -> no penalty
        let custom_err: Result<(), &str> = Err("custom error");
        guard.record_result(&custom_err, |_| None);
        assert_eq!(
            entry.current_penalty(),
            0,
            "Error with None extracted Code must not apply error penalty"
        );

        // Record direct error code
        guard.record_error_code(Code::ResourceExhausted);
        assert_eq!(
            entry.current_penalty(),
            5,
            "record_error_code(ResourceExhausted) must apply error penalty of 5"
        );
    }

    #[test]
    fn channel_lease_accessors() {
        let channel = create_mock_channel();
        let entry = Arc::new(ChannelEntry::new(42, 3, channel));
        let guard = ActiveRpcGuard::new(Arc::clone(&entry), 0, Duration::ZERO, 0);
        let lease = ChannelLease::new(guard);

        assert_eq!(
            lease.entry_id(),
            42,
            "entry_id() must return entry's internal id 42"
        );
        assert_eq!(
            lease.logical_channel_id(),
            3,
            "logical_channel_id() must return entry's logical id 3"
        );
        let _channel = lease.channel();

        // rw_affinity_guard helper creates an RAII guard incrementing active_rw_transactions
        let rw_guard = lease.rw_affinity_guard();
        assert_eq!(
            entry.active_rw_count(),
            1,
            "rw_affinity_guard() must increment active_rw_transactions"
        );
        drop(rw_guard);
        assert_eq!(
            entry.active_rw_count(),
            0,
            "dropping RwTransactionAffinityGuard must decrement active_rw_transactions"
        );
    }
}
