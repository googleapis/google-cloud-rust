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

//! Generic bounded cache implementing the CLOCK (Second-Chance) replacement algorithm.
//!
//! Provides a bounded key-value store with lock-free atomic reference bit updates on read,
//! and $O(1)$ amortized second-chance eviction on write when reaching configured capacity.

use std::borrow::Borrow;
use std::collections::{HashMap, VecDeque};
use std::fmt::{self, Debug, Formatter};
use std::hash::Hash;
use std::mem::replace;
use std::sync::atomic::{AtomicBool, Ordering};

/// Default upper bound on initial startup hash map allocation for bounded caches.
pub(crate) const DEFAULT_INITIAL_CAPACITY_BOUND: usize = 256;

/// A bounded cache store implementing the CLOCK (Second-Chance) page replacement algorithm.
///
/// Designed to be embedded within outer synchronization primitives (e.g. `RwLock<ClockStore<K, V>>`),
/// where concurrent readers (`&self`) can update cache reference bits lock-free via atomic flags (`AtomicBool`),
/// while writers (`&mut self`) perform mutations and evictions.
///
/// Features:
/// - $O(1)$ lock-free read reference bit updates (`Ordering::Relaxed`).
/// - $O(1)$ amortized eviction on write when exceeding capacity.
/// - In-place replacement semantics with reference bit refresh.
/// - Zero heap allocations on hit.
#[derive(Debug)]
pub(crate) struct ClockStore<K, V> {
    entries: HashMap<K, ClockEntry<V>>,
    order: VecDeque<K>,
    capacity: usize,
}

impl<K: Eq + Hash, V> ClockStore<K, V> {
    /// Creates a new [`ClockStore`] with the given maximum entry capacity.
    ///
    /// Pre-allocates initial capacity bounded to at most `min(capacity, DEFAULT_INITIAL_CAPACITY_BOUND)`
    /// to avoid excessive memory usage on startup when configuring large cache limits.
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self::with_initial_capacity(capacity.min(DEFAULT_INITIAL_CAPACITY_BOUND), capacity)
    }

    /// Creates a new [`ClockStore`] with an explicit initial pre-allocated capacity
    /// and a maximum capacity eviction limit.
    ///
    /// The initial pre-allocation is bounded by `max_capacity` (`min(initial_capacity, max_capacity)`).
    pub(crate) fn with_initial_capacity(initial_capacity: usize, max_capacity: usize) -> Self {
        let initial_capacity = initial_capacity.min(max_capacity);
        Self {
            entries: HashMap::with_capacity(initial_capacity),
            order: VecDeque::with_capacity(initial_capacity),
            capacity: max_capacity,
        }
    }

    /// Returns a cloned value for `key` if present, marking the entry as recently referenced.
    ///
    /// For reference-counted values (e.g. `Arc<T>`), cloning is an inexpensive $O(1)$ atomic increment
    /// on the strong reference count, allowing callers to release any outer synchronization lock immediately.
    pub(crate) fn get<Q>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: ?Sized + Hash + Eq,
        V: Clone,
    {
        let entry = self.entries.get(key)?;
        if !entry.referenced.load(Ordering::Relaxed) {
            entry.referenced.store(true, Ordering::Relaxed);
        }
        Some(entry.value.clone())
    }

    /// Returns a reference to the value for `key` if present, marking the entry as recently referenced.
    ///
    /// Performs zero heap allocations or clones, allowing in-place inspection under outer read guards.
    #[allow(dead_code)]
    pub(crate) fn get_ref<Q>(&self, key: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: ?Sized + Hash + Eq,
    {
        let entry = self.entries.get(key)?;
        if !entry.referenced.load(Ordering::Relaxed) {
            entry.referenced.store(true, Ordering::Relaxed);
        }
        Some(&entry.value)
    }

    /// Inserts a key-value pair into the store.
    ///
    /// Returns `Some(previous_value)` if `key` already existed in the store.
    pub(crate) fn insert(&mut self, key: K, value: V) -> Option<V>
    where
        K: Clone,
    {
        if self.capacity == 0 {
            return None;
        }

        if let Some(entry) = self.entries.get_mut(&key) {
            let previous_value = replace(&mut entry.value, value);
            // Overwriting an existing entry marks it as recently referenced, granting it a
            // second chance in CLOCK eviction so freshly updated entries are not prematurely evicted.
            entry.referenced.store(true, Ordering::Relaxed);
            return Some(previous_value);
        }

        // Evict from existing entries to make room BEFORE inserting the new entry.
        self.evict_for_insert();

        let order_key = key.clone();
        self.entries.insert(
            key,
            ClockEntry {
                value,
                referenced: AtomicBool::new(false),
            },
        );
        self.order.push_back(order_key);
        None
    }

    /// Clears all entries from the store while preserving configured capacity.
    #[allow(dead_code)]
    pub(crate) fn clear(&mut self) {
        self.entries.clear();
        self.order.clear();
    }

    /// Returns the number of entries currently stored in the cache.
    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns `true` if the store contains no entries.
    #[allow(dead_code)]
    pub(crate) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Returns the maximum capacity of the store.
    #[allow(dead_code)]
    pub(crate) fn capacity(&self) -> usize {
        self.capacity
    }

    /// Empties the store and returns the inner entries map, allowing deallocation
    /// of elements outside of lock guards. Preserves the configured capacity.
    pub(crate) fn take_all(&mut self) -> HashMap<K, ClockEntry<V>> {
        self.order.clear();
        let initial_capacity = self.capacity.min(DEFAULT_INITIAL_CAPACITY_BOUND);
        replace(&mut self.entries, HashMap::with_capacity(initial_capacity))
    }

    /// Evicts an unreferenced entry when `entries.len() >= capacity` using the CLOCK (Second-Chance) algorithm.
    fn evict_for_insert(&mut self) {
        if self.capacity == 0 {
            return;
        }
        while self.entries.len() >= self.capacity {
            let Some(candidate_key) = self.order.pop_front() else {
                break;
            };
            let Some(entry) = self.entries.get(&candidate_key) else {
                // Defensive check against queue desynchronization.
                continue;
            };
            if entry.referenced.swap(false, Ordering::Relaxed) {
                // Entry was referenced since last inspection: grant a second chance.
                self.order.push_back(candidate_key);
                continue;
            }
            // Entry was not referenced: evict from cache.
            self.entries.remove(&candidate_key);
            break;
        }
    }
}

/// An entry within [`ClockStore`], pairing a cached value with an atomic reference flag
/// for scan-resistant CLOCK (Second-Chance) cache eviction.
pub(crate) struct ClockEntry<V> {
    pub(crate) value: V,
    pub(crate) referenced: AtomicBool,
}

impl<V: Debug> Debug for ClockEntry<V> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ClockEntry")
            .field("value", &self.value)
            .field("referenced", &self.referenced.load(Ordering::Relaxed))
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn traits() {
        static_assertions::assert_impl_all!(ClockStore<String, String>: Send, Sync, Debug);
        static_assertions::assert_impl_all!(ClockEntry<String>: Send, Sync, Debug);
    }

    #[test]
    fn clock_store_generic_types_and_eviction() {
        let mut store = ClockStore::<String, i32>::with_capacity(2);
        assert_eq!(store.capacity(), 2, "capacity must match configured value");
        assert!(store.is_empty(), "new store must be empty");
        assert_eq!(store.len(), 0, "new store must have length 0");

        assert_eq!(
            store.insert("item_a".to_string(), 100),
            None,
            "new insert must return None"
        );
        assert_eq!(
            store.insert("item_b".to_string(), 200),
            None,
            "new insert must return None"
        );
        assert_eq!(store.len(), 2, "length must be 2 after two inserts");
        assert!(!store.is_empty(), "store must not be empty after inserts");

        // Access item_a to mark referenced
        assert_eq!(
            store.get("item_a"),
            Some(100),
            "get must return cached value"
        );

        // Insert item_c (evicts item_b because item_a got a second chance)
        assert_eq!(
            store.insert("item_c".to_string(), 300),
            None,
            "new insert must return None"
        );
        assert_eq!(store.len(), 2, "length must remain bounded at capacity");

        assert_eq!(
            store.get("item_a"),
            Some(100),
            "item_a must survive due to second chance"
        );
        assert_eq!(
            store.get("item_b"),
            None,
            "item_b must be evicted as unreferenced"
        );
        assert_eq!(
            store.get("item_c"),
            Some(300),
            "newly inserted item_c must be present"
        );
    }

    #[test]
    fn clock_store_insert_when_all_entries_referenced_evicts_oldest_not_new() {
        let mut store = ClockStore::<u64, String>::with_capacity(2);
        assert_eq!(
            store.insert(1, "one".to_string()),
            None,
            "insert 1 must succeed"
        );
        assert_eq!(
            store.insert(2, "two".to_string()),
            None,
            "insert 2 must succeed"
        );

        // Reference both existing entries
        assert_eq!(store.get(&1), Some("one".to_string()), "get 1 must succeed");
        assert_eq!(store.get(&2), Some("two".to_string()), "get 2 must succeed");

        // Inserting 3 when all entries are referenced must evict oldest entry 1 (after second chance cycle)
        // and must NOT evict newly inserted entry 3
        assert_eq!(
            store.insert(3, "three".to_string()),
            None,
            "insert 3 into full cache must succeed"
        );
        assert_eq!(
            store.get(&3),
            Some("three".to_string()),
            "newly inserted entry 3 must NOT be evicted on insertion"
        );
        assert_eq!(
            store.get(&1),
            None,
            "oldest entry 1 must be evicted after receiving second chance"
        );
        assert_eq!(
            store.get(&2),
            Some("two".to_string()),
            "entry 2 must survive in cache"
        );
    }

    #[test]
    fn clock_store_insert_evict_and_reinsert_lifecycle() {
        let mut store = ClockStore::<u64, String>::with_capacity(2);
        // 1. Insert key 1 and 2 (cache at capacity 2)
        assert_eq!(
            store.insert(1, "one_v1".to_string()),
            None,
            "initial insert 1 must succeed"
        );
        assert_eq!(
            store.insert(2, "two".to_string()),
            None,
            "initial insert 2 must succeed"
        );
        assert_eq!(store.len(), 2, "cache length must be 2");

        // 2. Insert key 3: key 1 is unreferenced and evicted
        assert_eq!(
            store.insert(3, "three".to_string()),
            None,
            "insert 3 into full cache must succeed"
        );
        assert_eq!(store.len(), 2, "cache length must remain 2");
        assert_eq!(store.get(&1), None, "key 1 must be evicted");

        // 3. Re-insert key 1 with new value: key 2 is unreferenced and evicted
        assert_eq!(
            store.insert(1, "one_v2".to_string()),
            None,
            "re-inserting key 1 must succeed"
        );
        assert_eq!(store.len(), 2, "cache length must remain 2");
        assert_eq!(store.get(&2), None, "key 2 must be evicted");
        assert_eq!(
            store.get(&1),
            Some("one_v2".to_string()),
            "key 1 must be active with new value"
        );

        // 4. Key 1 is now referenced (from step 3 get). Insert key 4: key 3 is evicted, key 1 survives
        assert_eq!(
            store.insert(4, "four".to_string()),
            None,
            "insert 4 into full cache must succeed"
        );
        assert_eq!(store.len(), 2, "cache length must remain 2");
        assert_eq!(store.get(&3), None, "key 3 must be evicted");
        assert_eq!(
            store.get(&1),
            Some("one_v2".to_string()),
            "key 1 must survive via second chance"
        );
        assert_eq!(
            store.get(&4),
            Some("four".to_string()),
            "key 4 must be present"
        );
    }

    #[test]
    fn clock_store_zero_capacity() {
        let mut store = ClockStore::<u64, String>::with_capacity(0);
        assert_eq!(store.capacity(), 0, "capacity must be 0");
        assert_eq!(
            store.insert(1, "val".to_string()),
            None,
            "insert into zero capacity cache must return None"
        );
        assert_eq!(store.len(), 0, "length must remain 0");
        assert!(store.is_empty(), "store must be empty");
        assert_eq!(
            store.get(&1),
            None,
            "get from zero capacity cache must return None"
        );
    }

    #[test]
    fn clock_store_zero_initial_and_max_capacity() {
        let mut store = ClockStore::<u64, String>::with_initial_capacity(0, 0);
        assert_eq!(store.capacity(), 0, "capacity must be 0");
        assert_eq!(
            store.insert(1, "val".to_string()),
            None,
            "insert into zero capacity cache must return None"
        );
        assert_eq!(store.len(), 0, "length must remain 0");
        assert!(store.is_empty(), "store must be empty");
        assert_eq!(
            store.get(&1),
            None,
            "get from zero capacity cache must return None"
        );
    }

    #[test]
    fn clock_store_overwrite_returns_previous_value_and_refreshes_referenced() {
        let mut store = ClockStore::<u64, String>::with_capacity(2);
        assert_eq!(
            store.insert(1, "initial".to_string()),
            None,
            "initial insert must return None"
        );
        assert_eq!(
            store.insert(1, "updated".to_string()),
            Some("initial".to_string()),
            "overwrite must return the displaced previous value"
        );
        assert_eq!(store.len(), 1, "length must be 1 after overwrite");

        // Overwritten entry should have referenced = true, giving it a second chance
        assert_eq!(
            store.insert(2, "two".to_string()),
            None,
            "insert 2 must return None"
        );
        // Insert 3 -> 2 should be evicted because 1 got a second chance from overwrite
        assert_eq!(
            store.insert(3, "three".to_string()),
            None,
            "insert 3 must return None"
        );
        assert_eq!(
            store.get(&1),
            Some("updated".to_string()),
            "overwritten entry 1 must survive due to refreshed reference bit"
        );
        assert_eq!(
            store.get(&2),
            None,
            "entry 2 must be evicted as unreferenced"
        );
        assert_eq!(
            store.get(&3),
            Some("three".to_string()),
            "entry 3 must be present"
        );
    }

    #[test]
    fn clock_store_clear_empties_entries_and_preserves_capacity() {
        let mut store = ClockStore::<u64, String>::with_capacity(3);
        store.insert(1, "one".to_string());
        store.insert(2, "two".to_string());
        assert_eq!(store.len(), 2, "length must be 2 after two inserts");

        store.clear();
        assert!(store.is_empty(), "store must be empty after clear");
        assert_eq!(store.len(), 0, "length must be 0 after clear");
        assert_eq!(
            store.capacity(),
            3,
            "capacity must be preserved after clear"
        );
        assert_eq!(store.get(&1), None, "get on cleared key must return None");
        assert_eq!(store.get(&2), None, "get on cleared key must return None");
    }

    #[test]
    fn clock_store_get_ref_marks_referenced() {
        let mut store = ClockStore::<u64, String>::with_capacity(2);
        store.insert(1, "one".to_string());
        store.insert(2, "two".to_string());

        assert_eq!(store.get_ref(&999), None, "get_ref miss returns None");

        // Use get_ref on 1 to mark referenced
        assert_eq!(
            store.get_ref(&1).map(String::as_str),
            Some("one"),
            "get_ref on key 1 must return reference to value"
        );

        // Insert 3 -> 2 evicted, 1 survives
        assert_eq!(
            store.insert(3, "three".to_string()),
            None,
            "insert 3 must return None"
        );
        assert_eq!(
            store.get(&1),
            Some("one".to_string()),
            "entry 1 must survive due to get_ref reference bit update"
        );
        assert_eq!(
            store.get(&2),
            None,
            "entry 2 must be evicted as unreferenced"
        );
        assert_eq!(
            store.get(&3),
            Some("three".to_string()),
            "entry 3 must be present"
        );
    }

    #[test]
    fn clock_store_take_all_empties_entries_and_preserves_capacity() {
        let mut store = ClockStore::<u64, String>::with_capacity(5);
        store.insert(1, "one".to_string());
        store.insert(2, "two".to_string());
        assert_eq!(store.len(), 2, "length must be 2 after two inserts");

        let extracted = store.take_all();
        assert_eq!(extracted.len(), 2, "extracted map must contain 2 entries");
        assert!(
            extracted.contains_key(&1),
            "extracted map must contain key 1"
        );
        assert!(
            extracted.contains_key(&2),
            "extracted map must contain key 2"
        );

        assert!(store.is_empty(), "store must be empty after take_all");
        assert_eq!(store.len(), 0, "length must be 0 after take_all");
        assert_eq!(
            store.capacity(),
            5,
            "capacity must be preserved after take_all"
        );
        assert!(
            store.entries.capacity() >= store.capacity().min(DEFAULT_INITIAL_CAPACITY_BOUND),
            "entries map must preserve pre-allocated capacity after take_all"
        );
    }

    #[test]
    fn clock_store_debug_formatting() {
        let mut store = ClockStore::<u64, i32>::with_capacity(2);
        store.insert(42, 100);
        let entry = store
            .entries
            .get(&42)
            .expect("entry 42 must exist in entries");
        let entry_debug = format!("{entry:?}");
        assert!(
            entry_debug.contains("ClockEntry"),
            "debug format must contain ClockEntry name"
        );
        assert!(
            entry_debug.contains("value: 100"),
            "debug format must contain value"
        );

        let store_debug = format!("{store:?}");
        assert!(
            store_debug.contains("ClockStore"),
            "debug format must contain ClockStore name"
        );
        assert!(
            store_debug.contains("capacity: 2"),
            "debug format must contain capacity"
        );
    }

    #[test]
    fn clock_store_initial_capacity_larger_than_max_capacity_evicts_at_max() {
        let mut store = ClockStore::<u64, String>::with_initial_capacity(100, 2);
        assert_eq!(store.capacity(), 2, "capacity must match max_capacity");
        assert!(store.is_empty(), "new store must be empty");

        assert_eq!(
            store.insert(1, "one".to_string()),
            None,
            "insert 1 must return None"
        );
        assert_eq!(
            store.insert(2, "two".to_string()),
            None,
            "insert 2 must return None"
        );
        assert_eq!(store.len(), 2, "length must be 2 after two inserts");

        // Third insert must trigger eviction at max_capacity (2), not initial_capacity (100)
        assert_eq!(
            store.insert(3, "three".to_string()),
            None,
            "insert 3 must return None"
        );
        assert_eq!(store.len(), 2, "length must remain bounded at capacity 2");
        assert_eq!(store.get(&1), None, "key 1 must be evicted at capacity 2");
        assert_eq!(
            store.get(&2),
            Some("two".to_string()),
            "key 2 must be present"
        );
        assert_eq!(
            store.get(&3),
            Some("three".to_string()),
            "key 3 must be present"
        );
    }

    #[test]
    fn clock_store_zero_max_capacity_with_positive_initial_capacity() {
        let mut store = ClockStore::<u64, String>::with_initial_capacity(50, 0);
        assert_eq!(store.capacity(), 0, "capacity must be 0");
        assert!(store.is_empty(), "store must be empty");
        assert_eq!(store.len(), 0, "length must be 0");

        assert_eq!(
            store.insert(1, "one".to_string()),
            None,
            "insert into 0 max capacity must return None"
        );
        assert_eq!(store.len(), 0, "length must remain 0");
        assert!(store.is_empty(), "store must remain empty");
        assert_eq!(store.get(&1), None, "get must return None");
    }

    #[test]
    fn clock_store_zero_initial_capacity_with_positive_max_capacity() {
        let mut store = ClockStore::<u64, String>::with_initial_capacity(0, 3);
        assert_eq!(store.capacity(), 3, "capacity must be 3");
        assert!(store.is_empty(), "store must be empty");

        store.insert(1, "one".to_string());
        store.insert(2, "two".to_string());
        store.insert(3, "three".to_string());
        assert_eq!(store.len(), 3, "length must be 3 after three inserts");

        // Fourth insert triggers eviction
        assert_eq!(
            store.insert(4, "four".to_string()),
            None,
            "insert 4 must return None"
        );
        assert_eq!(store.len(), 3, "length must remain bounded at capacity 3");
        assert_eq!(store.get(&1), None, "key 1 must be evicted");
        assert_eq!(
            store.get(&2),
            Some("two".to_string()),
            "key 2 must be present"
        );
        assert_eq!(
            store.get(&3),
            Some("three".to_string()),
            "key 3 must be present"
        );
        assert_eq!(
            store.get(&4),
            Some("four".to_string()),
            "key 4 must be present"
        );
    }

    #[test]
    fn clock_store_with_capacity_bounds_initial_allocation_below_and_above_threshold() {
        // Below 256 threshold: initial allocation is min(2, 256) = 2
        let mut store_small = ClockStore::<u64, String>::with_capacity(2);
        assert_eq!(store_small.capacity(), 2, "capacity must be 2");
        store_small.insert(1, "one".to_string());
        store_small.insert(2, "two".to_string());
        store_small.insert(3, "three".to_string());
        assert_eq!(store_small.len(), 2, "length must be 2");
        assert_eq!(store_small.get(&1), None, "key 1 must be evicted");

        // Above 256 threshold: initial allocation is min(300, 256) = 256, max capacity is 300
        let mut store_large = ClockStore::<usize, usize>::with_capacity(300);
        assert_eq!(store_large.capacity(), 300, "capacity must be 300");
        for i in 0..300 {
            store_large.insert(i, i * 10);
        }
        assert_eq!(store_large.len(), 300, "length must be 300");
        // 301st insert triggers eviction at max capacity 300
        assert_eq!(
            store_large.insert(300, 3000),
            None,
            "insert 300 must succeed and return None"
        );
        assert_eq!(store_large.len(), 300, "length must remain 300");
        assert_eq!(
            store_large.get(&0),
            None,
            "oldest entry 0 must be evicted at capacity 300"
        );
        assert_eq!(
            store_large.get(&300),
            Some(3000),
            "newly inserted entry 300 must be present"
        );
    }

    #[test]
    fn clock_store_huge_initial_capacity_clamped_to_max_capacity() {
        // usize::MAX initial capacity must be safely clamped to max_capacity (2) without OOM
        let mut store = ClockStore::<u64, String>::with_initial_capacity(usize::MAX, 2);
        assert_eq!(store.capacity(), 2, "capacity must be 2");
        store.insert(1, "one".to_string());
        store.insert(2, "two".to_string());
        store.insert(3, "three".to_string());
        assert_eq!(store.len(), 2, "length must be 2");
        assert_eq!(store.get(&1), None, "key 1 must be evicted");
        assert_eq!(
            store.get(&2),
            Some("two".to_string()),
            "key 2 must be present"
        );
        assert_eq!(
            store.get(&3),
            Some("three".to_string()),
            "key 3 must be present"
        );
    }

    #[test]
    fn clock_store_get_and_get_ref_reference_flag_transitions() {
        let mut store = ClockStore::<u64, String>::with_capacity(3);
        store.insert(1, "one".to_string());
        store.insert(2, "two".to_string());

        // Newly inserted entries have referenced set to false initially
        let entry_1 = store.entries.get(&1).expect("entry 1 must exist in store");
        assert!(
            !entry_1.referenced.load(Ordering::Relaxed),
            "newly inserted entry 1 must have referenced = false"
        );

        let entry_2 = store.entries.get(&2).expect("entry 2 must exist in store");
        assert!(
            !entry_2.referenced.load(Ordering::Relaxed),
            "newly inserted entry 2 must have referenced = false"
        );

        // First get(&1) transitions referenced from false to true
        assert_eq!(
            store.get(&1),
            Some("one".to_string()),
            "get(&1) must return cached value"
        );
        assert!(
            entry_1.referenced.load(Ordering::Relaxed),
            "entry 1 referenced bit must be true after get"
        );

        // Subsequent get(&1) when already true preserves true and avoids unnecessary write
        assert_eq!(
            store.get(&1),
            Some("one".to_string()),
            "second get(&1) must return cached value"
        );
        assert!(
            entry_1.referenced.load(Ordering::Relaxed),
            "entry 1 referenced bit must remain true on subsequent get"
        );

        // First get_ref(&2) transitions referenced from false to true
        assert_eq!(
            store.get_ref(&2).map(String::as_str),
            Some("two"),
            "get_ref(&2) must return cached reference"
        );
        assert!(
            entry_2.referenced.load(Ordering::Relaxed),
            "entry 2 referenced bit must be true after get_ref"
        );

        // Subsequent get_ref(&2) when already true preserves true
        assert_eq!(
            store.get_ref(&2).map(String::as_str),
            Some("two"),
            "second get_ref(&2) must return cached reference"
        );
        assert!(
            entry_2.referenced.load(Ordering::Relaxed),
            "entry 2 referenced bit must remain true on subsequent get_ref"
        );
    }
}
