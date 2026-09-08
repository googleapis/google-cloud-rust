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

use std::sync::atomic::{AtomicU64, Ordering};

/// Caller-owned handle managing channel affinity across multi-statement transactions.
#[derive(Debug)]
pub(crate) struct TransactionAffinity {
    entry_id: AtomicU64,
    kind: AffinityKind,
}

impl Default for TransactionAffinity {
    fn default() -> Self {
        Self::new_read_write()
    }
}

impl TransactionAffinity {
    /// Creates a new, unpinned `TransactionAffinity` handle for Read/Write transactions (hard stickiness).
    pub(crate) fn new() -> Self {
        Self::new_read_write()
    }

    /// Creates a new, unpinned `TransactionAffinity` handle for Read/Write transactions (hard stickiness).
    pub(crate) fn new_read_write() -> Self {
        Self {
            entry_id: AtomicU64::new(0),
            kind: AffinityKind::ReadWrite,
        }
    }

    /// Creates a new, unpinned `TransactionAffinity` handle for Read-Only transactions (soft stickiness).
    pub(crate) fn new_read_only() -> Self {
        Self {
            entry_id: AtomicU64::new(0),
            kind: AffinityKind::ReadOnly,
        }
    }

    /// Returns `true` if this handle requires hard stickiness (Read/Write transactions).
    pub(crate) fn is_read_write(&self) -> bool {
        self.kind == AffinityKind::ReadWrite
    }

    /// Returns `true` if this handle uses soft stickiness (Read-Only transactions).
    pub(crate) fn is_read_only(&self) -> bool {
        self.kind == AffinityKind::ReadOnly
    }

    /// Returns the pinned monotonic channel entry ID, or `None` if unpinned.
    pub(crate) fn pinned_entry_id(&self) -> Option<u64> {
        let id = self.entry_id.load(Ordering::Acquire);
        (id != 0).then_some(id)
    }

    /// Sets the pinned channel entry ID.
    pub(crate) fn set_entry_id(&self, entry_id: u64) {
        debug_assert_ne!(entry_id, 0, "entry_id must be non-zero");
        self.entry_id.store(entry_id, Ordering::Release);
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

    /// Clears the pinned channel entry ID, making this handle unpinned.
    pub(crate) fn reset(&self) {
        self.entry_id.store(0, Ordering::Release);
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::fmt::Debug;

    #[test]
    fn traits() {
        static_assertions::assert_impl_all!(TransactionAffinity: Debug, Send, Sync);
        static_assertions::assert_impl_all!(AffinityKind: Clone, Copy, Debug, PartialEq, Eq, Send, Sync);
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
        let new_affinity = TransactionAffinity::new();
        assert!(
            new_affinity.is_read_write(),
            "TransactionAffinity::new must be ReadWrite"
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
}
