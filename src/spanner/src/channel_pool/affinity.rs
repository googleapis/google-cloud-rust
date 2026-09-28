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

//! Transaction channel affinity management for Spanner.
//!
//! Provides caller-owned handles to pin multi-statement transactions to the same physical
//! channel and support both hard affinity (Read/Write transactions) and soft affinity (Read-Only transactions).

use crate::channel_pool::entry::{ChannelLease, RwTransactionAffinityGuard};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

/// Caller-owned handle managing channel affinity across multi-statement transactions.
#[derive(Debug)]
pub(crate) struct TransactionAffinity {
    entry_id: AtomicU64,
    kind: AffinityKind,
    rw_guard: Mutex<Option<RwTransactionAffinityGuard>>,
}

impl Default for TransactionAffinity {
    fn default() -> Self {
        Self::new_read_write()
    }
}

impl TransactionAffinity {
    /// Creates a new, unpinned `TransactionAffinity` handle for Read/Write transactions (hard stickiness).
    pub(crate) fn new_read_write() -> Self {
        Self {
            entry_id: AtomicU64::new(0),
            kind: AffinityKind::ReadWrite,
            rw_guard: Mutex::new(None),
        }
    }

    /// Creates a new, unpinned `TransactionAffinity` handle for Read-Only transactions (soft stickiness).
    pub(crate) fn new_read_only() -> Self {
        Self {
            entry_id: AtomicU64::new(0),
            kind: AffinityKind::ReadOnly,
            rw_guard: Mutex::new(None),
        }
    }

    /// Returns `true` if this handle requires hard stickiness (Read/Write transactions).
    pub(crate) fn is_read_write(&self) -> bool {
        self.kind == AffinityKind::ReadWrite
    }

    /// Returns `true` if this handle represents soft stickiness (Read-Only transactions).
    #[cfg(test)]
    pub(crate) fn is_read_only(&self) -> bool {
        self.kind == AffinityKind::ReadOnly
    }

    /// Returns the pinned monotonic channel entry ID, or `None` if unpinned.
    pub(crate) fn pinned_entry_id(&self) -> Option<u64> {
        let id = self.entry_id.load(Ordering::Acquire);
        (id != 0).then_some(id)
    }

    /// Atomically sets the pinned channel entry ID if matching `current`.
    ///
    /// Returns `Ok(())` if this caller won the pin, or `Err(winner_id)` containing
    /// the winning pinned entry ID if another thread pinned concurrently.
    pub(crate) fn compare_and_set_entry_id(&self, current: u64, new: u64) -> Result<(), u64> {
        self.entry_id
            .compare_exchange(current, new, Ordering::AcqRel, Ordering::Acquire)
            .map(|_| ())
    }

    /// Ensures that an `RwTransactionAffinityGuard` is attached for the leased channel entry.
    ///
    /// If an existing guard already protects `lease.entry_id()`, this is a no-op that avoids
    /// allocating a new guard or mutating active transaction atomic counters.
    pub(crate) fn ensure_rw_guard(&self, lease: &ChannelLease) {
        let mut slot = self
            .rw_guard
            .lock()
            .expect("affinity rw_guard lock poisoned");
        if let Some(existing) = slot.as_ref()
            && existing.entry_id() == lease.entry_id()
        {
            return;
        }
        *slot = Some(lease.rw_affinity_guard());
    }

    /// Releases the active Read/Write transaction guard on the channel entry,
    /// allowing draining channels to close once the transaction completes.
    pub(crate) fn release_rw_guard(&self) {
        let mut slot = self
            .rw_guard
            .lock()
            .expect("affinity rw_guard lock poisoned");
        *slot = None;
    }
}

/// Stickiness kind for transaction channel affinity.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum AffinityKind {
    /// Read/Write transactions require hard stickiness,
    /// even if the channel has transitioned to draining.
    #[default]
    ReadWrite,
    /// Read-Only transactions prefer soft stickiness, but seamlessly
    /// switch to a fresh active channel if their pinned channel begins draining.
    ReadOnly,
}

/// Routing target for channel selection: either an unpinned channel or a transaction affinity handle.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) enum ChannelTarget<'a> {
    /// Leases any active channel via Power-of-Two-Choices (P2C) load balancing.
    #[default]
    Any,
    /// Pins or resolves request dispatch to the channel associated with the given transaction affinity handle.
    Affinity(&'a TransactionAffinity),
}

impl<'a> From<&'a TransactionAffinity> for ChannelTarget<'a> {
    fn from(affinity: &'a TransactionAffinity) -> Self {
        Self::Affinity(affinity)
    }
}

impl<'a> From<&'a Arc<TransactionAffinity>> for ChannelTarget<'a> {
    fn from(affinity: &'a Arc<TransactionAffinity>) -> Self {
        Self::Affinity(affinity)
    }
}

impl<'a> From<Option<&'a TransactionAffinity>> for ChannelTarget<'a> {
    fn from(affinity: Option<&'a TransactionAffinity>) -> Self {
        match affinity {
            Some(affinity) => Self::Affinity(affinity),
            None => Self::Any,
        }
    }
}

impl<'a> From<&'a Option<Arc<TransactionAffinity>>> for ChannelTarget<'a> {
    fn from(affinity: &'a Option<Arc<TransactionAffinity>>) -> Self {
        match affinity {
            Some(affinity) => Self::Affinity(affinity),
            None => Self::Any,
        }
    }
}

impl From<()> for ChannelTarget<'_> {
    fn from(_: ()) -> Self {
        Self::Any
    }
}

#[cfg(test)]
impl TransactionAffinity {
    pub(crate) fn set_entry_id(&self, entry_id: u64) {
        debug_assert_ne!(entry_id, 0, "entry_id must be non-zero");
        self.entry_id.store(entry_id, Ordering::Release);
    }

    pub(crate) fn reset(&self) {
        self.entry_id.store(0, Ordering::Release);
        let mut slot = self
            .rw_guard
            .lock()
            .expect("affinity rw_guard lock poisoned");
        *slot = None;
    }

    pub(crate) fn has_rw_guard(&self) -> bool {
        self.rw_guard
            .lock()
            .expect("affinity rw_guard lock poisoned")
            .is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::channel_pool::entry::{ActiveRpcGuard, ChannelEntry};
    use crate::client::Channel;
    use crate::generated::gapic_dataplane::stub::Spanner as SpannerStub;
    use std::fmt::Debug;
    use std::ptr;
    use std::sync::Arc;
    use std::time::Duration;

    #[derive(Debug)]
    struct DummyStub;
    impl SpannerStub for DummyStub {}

    #[test]
    fn traits() {
        static_assertions::assert_impl_all!(TransactionAffinity: Debug, Send, Sync);
        static_assertions::assert_impl_all!(AffinityKind: Clone, Copy, Debug, PartialEq, Eq, Send, Sync);
        static_assertions::assert_impl_all!(ChannelTarget<'_>: Clone, Copy, Debug, Send, Sync);
    }

    impl TransactionAffinity {
        fn attach_rw_guard(&self, guard: RwTransactionAffinityGuard) {
            let mut slot = self
                .rw_guard
                .lock()
                .expect("affinity rw_guard lock poisoned");
            match slot.as_ref() {
                Some(existing) if existing.entry_id() == guard.entry_id() => {}
                _ => *slot = Some(guard),
            }
        }
    }

    #[test]
    fn transaction_affinity_pin_and_reset() {
        assert_eq!(
            AffinityKind::default(),
            AffinityKind::ReadWrite,
            "Default AffinityKind must be ReadWrite"
        );
        let default_affinity = TransactionAffinity::default();
        assert!(
            default_affinity.is_read_write(),
            "Default affinity must be ReadWrite"
        );

        let affinity = TransactionAffinity::new_read_write();
        assert!(
            affinity.is_read_write(),
            "Default affinity must be ReadWrite"
        );
        assert!(
            !affinity.is_read_only(),
            "ReadWrite affinity is not ReadOnly"
        );
        assert_eq!(
            affinity.pinned_entry_id(),
            None,
            "Initial pinned entry ID must be None"
        );

        affinity.set_entry_id(42);
        assert_eq!(
            affinity.pinned_entry_id(),
            Some(42),
            "Pinned entry ID must be 42 after set_entry_id"
        );

        affinity.set_entry_id(99);
        assert_eq!(
            affinity.pinned_entry_id(),
            Some(99),
            "Pinned entry ID must be updated to 99"
        );

        let cas_failure = affinity.compare_and_set_entry_id(0, 100);
        assert_eq!(
            cas_failure,
            Err(99),
            "CAS with non-matching current value must fail and return existing winner ID"
        );

        affinity.reset();
        assert_eq!(
            affinity.pinned_entry_id(),
            None,
            "Pinned entry ID must be None after reset"
        );

        let cas_success = affinity.compare_and_set_entry_id(0, 100);
        assert_eq!(cas_success, Ok(()), "CAS on unpinned affinity must succeed");
        assert_eq!(
            affinity.pinned_entry_id(),
            Some(100),
            "Pinned entry ID must be 100 after successful CAS"
        );

        let read_only_affinity = TransactionAffinity::new_read_only();
        assert!(
            read_only_affinity.is_read_only(),
            "new_read_only must set ReadOnly kind"
        );
        assert!(
            !read_only_affinity.is_read_write(),
            "ReadOnly affinity is not ReadWrite"
        );
    }

    #[test]
    fn attach_rw_guard_repinning_replaces_old_guard() {
        let channel1 = Channel::new_for_test(DummyStub);
        let channel2 = Channel::new_for_test(DummyStub);
        let entry1 = Arc::new(ChannelEntry::new(1, 10, channel1));
        let entry2 = Arc::new(ChannelEntry::new(2, 20, channel2));

        assert_eq!(
            entry1.active_rw_count(),
            0,
            "entry1 initial active_rw_count must be 0"
        );
        assert_eq!(
            entry2.active_rw_count(),
            0,
            "entry2 initial active_rw_count must be 0"
        );

        let affinity = TransactionAffinity::new_read_write();
        assert!(
            !affinity.has_rw_guard(),
            "affinity must not have rw guard initially"
        );

        // First attach for entry 1
        affinity.attach_rw_guard(RwTransactionAffinityGuard::new(Arc::clone(&entry1)));
        assert!(affinity.has_rw_guard(), "guard must be attached");
        assert_eq!(
            entry1.active_rw_count(),
            1,
            "entry1 active_rw_count must be 1"
        );
        assert_eq!(
            entry2.active_rw_count(),
            0,
            "entry2 active_rw_count must be 0"
        );

        // Duplicate attach for entry 1 (same entry id) must not increment count
        affinity.attach_rw_guard(RwTransactionAffinityGuard::new(Arc::clone(&entry1)));
        assert_eq!(
            entry1.active_rw_count(),
            1,
            "duplicate attach for entry1 must keep active_rw_count at 1"
        );

        // Repinning attach for entry 2: old guard on entry 1 is dropped and replaced with guard on entry 2
        affinity.attach_rw_guard(RwTransactionAffinityGuard::new(Arc::clone(&entry2)));
        assert_eq!(
            entry1.active_rw_count(),
            0,
            "entry1 active_rw_count must drop to 0 after repinning"
        );
        assert_eq!(
            entry2.active_rw_count(),
            1,
            "entry2 active_rw_count must be 1 after repinning"
        );

        // Reset drops any attached guard
        affinity.reset();
        assert!(
            !affinity.has_rw_guard(),
            "has_rw_guard must be false after reset"
        );
        assert_eq!(
            entry2.active_rw_count(),
            0,
            "entry2 active_rw_count must drop to 0 after reset"
        );
    }

    #[test]
    fn ensure_rw_guard_attaches_and_is_noop_on_same_entry() {
        let channel1 = Channel::new_for_test(DummyStub);
        let channel2 = Channel::new_for_test(DummyStub);
        let entry1 = Arc::new(ChannelEntry::new(1, 1, channel1));
        let entry2 = Arc::new(ChannelEntry::new(2, 2, channel2));

        let affinity = TransactionAffinity::new_read_write();
        let lease1 = ChannelLease::new(ActiveRpcGuard::new(
            Arc::clone(&entry1),
            0,
            Duration::ZERO,
            0,
        ));
        affinity.ensure_rw_guard(&lease1);
        assert!(affinity.has_rw_guard(), "guard must be attached");
        assert_eq!(entry1.active_rw_count(), 1, "entry1 count must be 1");

        // Calling ensure_rw_guard again for the same entry does not increment count
        affinity.ensure_rw_guard(&lease1);
        assert_eq!(entry1.active_rw_count(), 1, "entry1 count must remain 1");

        // Calling ensure_rw_guard with lease2 repins and drops old guard
        let lease2 = ChannelLease::new(ActiveRpcGuard::new(
            Arc::clone(&entry2),
            0,
            Duration::ZERO,
            0,
        ));
        affinity.ensure_rw_guard(&lease2);
        assert_eq!(entry1.active_rw_count(), 0, "entry1 count must drop to 0");
        assert_eq!(entry2.active_rw_count(), 1, "entry2 count must be 1");
    }

    #[test]
    fn release_rw_guard_drops_guard_and_decrements_count() {
        let channel = Channel::new_for_test(DummyStub);
        let entry = Arc::new(ChannelEntry::new(1, 1, channel));
        let affinity = TransactionAffinity::new_read_write();
        let lease = ChannelLease::new(ActiveRpcGuard::new(
            Arc::clone(&entry),
            0,
            Duration::ZERO,
            0,
        ));

        affinity.ensure_rw_guard(&lease);
        assert!(affinity.has_rw_guard(), "guard must be attached");
        assert_eq!(entry.active_rw_count(), 1, "entry count must be 1");

        affinity.release_rw_guard();
        assert!(!affinity.has_rw_guard(), "guard must be released");
        assert_eq!(
            entry.active_rw_count(),
            0,
            "entry count must drop to 0 after release_rw_guard"
        );

        // Repeated release must be an idempotent no-op
        affinity.release_rw_guard();
        assert!(!affinity.has_rw_guard(), "guard must remain released");
        assert_eq!(
            entry.active_rw_count(),
            0,
            "entry count must remain 0 on repeated release"
        );
    }

    #[test]
    fn channel_target_from_conversions() {
        let affinity = TransactionAffinity::new_read_write();
        let target_from_ref = ChannelTarget::from(&affinity);
        assert!(
            matches!(target_from_ref, ChannelTarget::Affinity(target_affinity) if ptr::eq(target_affinity, &affinity)),
            "target_from_ref affinity must match original reference"
        );

        let arc_affinity = Arc::new(TransactionAffinity::new_read_only());
        let target_from_arc = ChannelTarget::from(&arc_affinity);
        assert!(
            matches!(target_from_arc, ChannelTarget::Affinity(target_affinity) if ptr::eq(target_affinity, &*arc_affinity)),
            "target_from_arc affinity must match pointer to arc inner"
        );

        let target_from_some = ChannelTarget::from(Some(&affinity));
        assert!(
            matches!(target_from_some, ChannelTarget::Affinity(target_affinity) if ptr::eq(target_affinity, &affinity)),
            "target_from_some affinity must match original reference"
        );

        let target_from_none = ChannelTarget::from(None);
        assert!(
            matches!(target_from_none, ChannelTarget::Any),
            "target_from_none must be ChannelTarget::Any"
        );

        let opt_arc: Option<Arc<TransactionAffinity>> = Some(Arc::clone(&arc_affinity));
        let target_from_opt_arc = ChannelTarget::from(&opt_arc);
        assert!(
            matches!(target_from_opt_arc, ChannelTarget::Affinity(target_affinity) if ptr::eq(target_affinity, &*arc_affinity)),
            "target_from_opt_arc must be ChannelTarget::Affinity"
        );

        let opt_arc_none: Option<Arc<TransactionAffinity>> = None;
        let target_from_opt_arc_none = ChannelTarget::from(&opt_arc_none);
        assert!(
            matches!(target_from_opt_arc_none, ChannelTarget::Any),
            "target_from_opt_arc_none must be ChannelTarget::Any"
        );

        let target_from_unit = ChannelTarget::from(());
        assert!(
            matches!(target_from_unit, ChannelTarget::Any),
            "target_from_unit must be ChannelTarget::Any"
        );

        assert!(
            matches!(ChannelTarget::default(), ChannelTarget::Any),
            "ChannelTarget default must be ChannelTarget::Any"
        );
    }
}
