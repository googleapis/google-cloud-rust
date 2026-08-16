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

//! In-memory interval cache (`KeyRangeCache`) for Spanner location-aware routing.
//!
//! Stores table key range split boundaries and maps them to tablet replica groups.
//! Provides interval lookups for point keys and key ranges under `CoveringSplit`
//! and `PickRandom` routing modes.

#![allow(dead_code)]

use crate::model::{CacheUpdate, Group, Range, Tablet};
use bytes::Bytes;
use std::collections::{BTreeMap, HashMap};
use std::ops::Bound;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

/// Determines how to handle ranges that span multiple splits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RangeMode {
    /// Consider it a cache miss if the whole range is not in a single split.
    CoveringSplit,
    /// If the range spans multiple splits, pick a random split when possible.
    PickRandom,
}

/// An in-memory cache representation of a Spanner Paxos group.
#[derive(Debug, Clone)]
pub(crate) struct CachedGroup {
    pub group_uid: u64,
    pub generation: Bytes,
    pub tablets: Vec<Tablet>,
    pub leader_index: i32,
}

impl CachedGroup {
    pub(crate) fn from_proto(proto: &Group) -> Self {
        Self {
            group_uid: proto.group_uid,
            generation: proto.generation.clone(),
            tablets: proto.tablets.clone(),
            leader_index: proto.leader_index,
        }
    }

    pub(crate) fn update_from_proto(&mut self, proto: &Group) {
        if proto.generation >= self.generation {
            self.generation = proto.generation.clone();
            self.tablets = proto.tablets.clone();
            self.leader_index = proto.leader_index;
        }
    }
}

/// An in-memory cache representation of a Spanner split key range.
#[derive(Debug)]
pub(crate) struct CachedRange {
    pub start_key: Bytes,
    pub limit_key: Bytes,
    pub group_uid: u64,
    pub split_id: u64,
    pub generation: Bytes,
    pub last_access: AtomicU64,
}

impl CachedRange {
    pub(crate) fn new(
        start_key: Bytes,
        limit_key: Bytes,
        group_uid: u64,
        split_id: u64,
        generation: Bytes,
        last_access: u64,
    ) -> Self {
        Self {
            start_key,
            limit_key,
            group_uid,
            split_id,
            generation,
            last_access: AtomicU64::new(last_access),
        }
    }
}

#[derive(Default)]
struct CacheState {
    /// Maps `start_key` (start of range, inclusive) to the cached split range.
    ranges: BTreeMap<Bytes, Arc<CachedRange>>,
    /// Maps `group_uid` to the cached replica group.
    groups: HashMap<u64, Arc<CachedGroup>>,
}

/// Thread-safe in-memory key range cache mapping split boundaries to replica groups.
pub(crate) struct KeyRangeCache {
    state: RwLock<CacheState>,
    access_counter: AtomicU64,
    min_cache_entries_for_random_pick: AtomicUsize,
}

impl KeyRangeCache {
    /// Creates a new empty key range cache.
    pub(crate) fn new() -> Self {
        Self {
            state: RwLock::new(CacheState::default()),
            access_counter: AtomicU64::new(0),
            min_cache_entries_for_random_pick: AtomicUsize::new(1000),
        }
    }

    /// Returns the current logical access time counter value and increments it.
    ///
    /// Note: `fetch_add` returns the previous value, so adding `1` yields the new value
    /// after the increment (matching Java's `AtomicLong.incrementAndGet()`).
    pub(crate) fn access_time_now(&self) -> u64 {
        self.access_counter.fetch_add(1, Ordering::Relaxed) + 1
    }

    /// Returns `true` if the cache has no stored ranges.
    pub(crate) fn is_empty(&self) -> bool {
        self.state
            .read()
            .expect("lock cache state for is_empty")
            .ranges
            .is_empty()
    }

    /// Returns the number of cached split ranges.
    pub(crate) fn len(&self) -> usize {
        self.state
            .read()
            .expect("lock cache state for len")
            .ranges
            .len()
    }

    /// Clears all cached ranges and groups.
    pub(crate) fn clear(&self) {
        let mut state = self.state.write().expect("lock cache state for clear");
        state.ranges.clear();
        state.groups.clear();
    }

    /// Returns the cached group for the given group UID, if present.
    pub(crate) fn get_group(&self, group_uid: u64) -> Option<Arc<CachedGroup>> {
        self.state
            .read()
            .expect("lock cache state for get_group")
            .groups
            .get(&group_uid)
            .cloned()
    }

    /// Applies updates from a Spanner `CacheUpdate` message.
    pub(crate) fn add_ranges(&self, cache_update: &CacheUpdate) {
        if cache_update.group.is_empty() && cache_update.range.is_empty() {
            return;
        }
        let mut state = self.state.write().expect("lock cache state for add_ranges");

        for group_in in &cache_update.group {
            if let Some(existing) = state.groups.get_mut(&group_in.group_uid) {
                if group_in.generation >= existing.generation {
                    let mut updated = (**existing).clone();
                    updated.update_from_proto(group_in);
                    *existing = Arc::new(updated);
                }
            } else {
                state.groups.insert(
                    group_in.group_uid,
                    Arc::new(CachedGroup::from_proto(group_in)),
                );
            }
        }

        for range_in in &cache_update.range {
            self.replace_range_if_newer_locked(&mut state, range_in);
        }
    }

    fn replace_range_if_newer_locked(&self, state: &mut CacheState, range_in: &Range) {
        let start_key_query = if let Some((_, first_existing)) = state
            .ranges
            .range::<[u8], _>((
                Bound::Unbounded,
                Bound::Included(range_in.start_key.as_ref()),
            ))
            .next_back()
        {
            if first_existing.limit_key.is_empty()
                || first_existing.limit_key.as_ref() > range_in.start_key.as_ref()
            {
                first_existing.start_key.as_ref()
            } else {
                range_in.start_key.as_ref()
            }
        } else {
            range_in.start_key.as_ref()
        };

        let limit_bound = if range_in.limit_key.is_empty() {
            Bound::Unbounded
        } else {
            Bound::Excluded(range_in.limit_key.as_ref())
        };

        // Step 1: Check generation against existing overlapping ranges without allocating a Vec.
        let mut has_overlap = false;
        for (_, existing) in state
            .ranges
            .range::<[u8], _>((Bound::Included(start_key_query), limit_bound))
        {
            let overlaps = existing.limit_key.is_empty()
                || existing.limit_key.as_ref() > range_in.start_key.as_ref();
            if !overlaps {
                continue;
            }
            has_overlap = true;
            if range_in.generation < existing.generation
                || (range_in.generation == existing.generation
                    && range_in.start_key == existing.start_key
                    && range_in.limit_key == existing.limit_key)
            {
                return;
            }
        }

        // Step 2: If there is no overlap, insert directly with zero intermediate allocations.
        if !has_overlap {
            let start_key = range_in.start_key.clone();
            let new_range = Arc::new(CachedRange::new(
                start_key.clone(),
                range_in.limit_key.clone(),
                range_in.group_uid,
                range_in.split_id,
                range_in.generation.clone(),
                self.access_time_now(),
            ));
            state.ranges.insert(start_key, new_range);
            return;
        }

        // Step 3: Overlapping older ranges must be removed or split.
        let mut overlapping = Vec::new();
        for (_, existing) in state
            .ranges
            .range::<[u8], _>((Bound::Included(start_key_query), limit_bound))
        {
            let overlaps = existing.limit_key.is_empty()
                || existing.limit_key.as_ref() > range_in.start_key.as_ref();
            if overlaps {
                overlapping.push(existing.clone());
            }
        }

        for existing in &overlapping {
            state.ranges.remove(&existing.start_key);
        }

        let first = &overlapping[0];
        if first.start_key < range_in.start_key {
            let start_key = first.start_key.clone();
            let head = Arc::new(CachedRange::new(
                start_key.clone(),
                range_in.start_key.clone(),
                first.group_uid,
                first.split_id,
                first.generation.clone(),
                first.last_access.load(Ordering::Relaxed),
            ));
            state.ranges.insert(start_key, head);
        }

        let last = overlapping.last().expect("overlapping is not empty");
        if !range_in.limit_key.is_empty()
            && (last.limit_key.is_empty() || last.limit_key > range_in.limit_key)
        {
            let start_key = range_in.limit_key.clone();
            let tail = Arc::new(CachedRange::new(
                start_key.clone(),
                last.limit_key.clone(),
                last.group_uid,
                last.split_id,
                last.generation.clone(),
                last.last_access.load(Ordering::Relaxed),
            ));
            state.ranges.insert(start_key, tail);
        }

        let start_key = range_in.start_key.clone();
        let new_range = Arc::new(CachedRange::new(
            start_key.clone(),
            range_in.limit_key.clone(),
            range_in.group_uid,
            range_in.split_id,
            range_in.generation.clone(),
            self.access_time_now(),
        ));
        state.ranges.insert(start_key, new_range);
    }

    /// Finds a cached range covering the specified key or range.
    ///
    /// Uses zero-allocation slice borrowing (`Bound::Excluded(key)`) to query the B-tree map.
    pub(crate) fn find_range(
        &self,
        key: &[u8],
        limit: &[u8],
        mode: RangeMode,
    ) -> Option<Arc<CachedRange>> {
        let state = self.state.read().expect("lock cache state for find_range");
        let first_range_opt = state
            .ranges
            .range::<[u8], _>((Bound::Unbounded, Bound::Included(key)))
            .next_back()
            .map(|(_, r)| r);

        if let Some(first_range) = first_range_opt {
            let in_range = first_range.limit_key.is_empty() || key < first_range.limit_key.as_ref();

            if limit.is_empty() {
                if in_range {
                    first_range
                        .last_access
                        .store(self.access_time_now(), Ordering::Relaxed);
                    return Some(first_range.clone());
                }
                return None;
            }

            let limit_in_range =
                first_range.limit_key.is_empty() || limit <= first_range.limit_key.as_ref();
            if in_range && limit_in_range {
                first_range
                    .last_access
                    .store(self.access_time_now(), Ordering::Relaxed);
                return Some(first_range.clone());
            }
        }

        if limit.is_empty() || mode == RangeMode::CoveringSplit {
            return None;
        }

        let mut total = 0usize;
        let mut found_gap = false;
        let mut sampled: Option<&Arc<CachedRange>> = None;
        let mut last_limit: &[u8] = key;

        let scan_start = match first_range_opt {
            Some(first_range)
                if first_range.limit_key.is_empty() || key < first_range.limit_key.as_ref() =>
            {
                total = 1;
                sampled = Some(first_range);
                last_limit = first_range.limit_key.as_ref();
                Bound::Excluded(first_range.start_key.as_ref())
            }
            Some(first_range) => {
                found_gap = true;
                Bound::Excluded(first_range.start_key.as_ref())
            }
            None => {
                found_gap = true;
                Bound::Included(key)
            }
        };

        for (_, current) in state
            .ranges
            .range::<[u8], _>((scan_start, Bound::Unbounded))
        {
            if current.start_key.as_ref() >= limit {
                break;
            }
            if last_limit != current.start_key.as_ref() {
                found_gap = true;
            }
            total += 1;
            if total == 1 || rand::random_range(0..total) == 0 {
                sampled = Some(current);
            }
            last_limit = current.limit_key.as_ref();
            if current.limit_key.is_empty()
                || last_limit >= limit
                || total
                    >= self
                        .min_cache_entries_for_random_pick
                        .load(Ordering::Relaxed)
            {
                break;
            }
        }

        if !last_limit.is_empty() && last_limit < limit {
            found_gap = true;
        }

        if let Some(sampled_range) = sampled
            && (!found_gap
                || total
                    >= self
                        .min_cache_entries_for_random_pick
                        .load(Ordering::Relaxed))
        {
            sampled_range
                .last_access
                .store(self.access_time_now(), Ordering::Relaxed);
            return Some(sampled_range.clone());
        }

        None
    }

    /// Selects an appropriate tablet replica from the cached range's group.
    pub(crate) fn select_tablet(&self, range: &CachedRange, prefer_leader: bool) -> Option<Tablet> {
        let state = self
            .state
            .read()
            .expect("lock cache state for select_tablet");
        let group = state.groups.get(&range.group_uid)?;
        if group.tablets.is_empty() {
            return None;
        }
        if prefer_leader
            && group.leader_index >= 0
            && (group.leader_index as usize) < group.tablets.len()
        {
            let leader = &group.tablets[group.leader_index as usize];
            if !leader.skip && !leader.server_address.is_empty() {
                return Some(leader.clone());
            }
        }
        let mut best_tablet = None;
        let mut best_distance = u32::MAX;
        let mut count = 0;
        for t in &group.tablets {
            if !t.skip && !t.server_address.is_empty() {
                if t.distance < best_distance {
                    best_tablet = Some(t);
                    best_distance = t.distance;
                    count = 1;
                } else if t.distance == best_distance {
                    count += 1;
                    if rand::random_range(0..count) == 0 {
                        best_tablet = Some(t);
                    }
                }
            }
        }
        best_tablet.cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_range(
        start: &'static str,
        limit: &'static str,
        group_uid: u64,
        generation: &'static str,
    ) -> Range {
        Range {
            start_key: Bytes::from_static(start.as_bytes()),
            limit_key: Bytes::from_static(limit.as_bytes()),
            group_uid,
            split_id: group_uid,
            generation: Bytes::from_static(generation.as_bytes()),
            _unknown_fields: Default::default(),
        }
    }

    fn make_group(group_uid: u64, generation: &'static str, leader_index: i32) -> Group {
        Group {
            group_uid,
            generation: Bytes::from_static(generation.as_bytes()),
            tablets: vec![
                Tablet {
                    tablet_uid: 1,
                    server_address: "localhost:8001".to_string(),
                    location: "us-central1".to_string(),
                    role: crate::model::tablet::Role::ReadWrite,
                    incarnation: Bytes::from_static(b"1"),
                    distance: 0,
                    skip: false,
                    _unknown_fields: Default::default(),
                },
                Tablet {
                    tablet_uid: 2,
                    server_address: "localhost:8002".to_string(),
                    location: "us-central1".to_string(),
                    role: crate::model::tablet::Role::ReadWrite,
                    incarnation: Bytes::from_static(b"1"),
                    distance: 1,
                    skip: false,
                    _unknown_fields: Default::default(),
                },
            ],
            leader_index,
            _unknown_fields: Default::default(),
        }
    }

    #[test]
    fn lookup_empty_cache_returns_none() {
        let cache = KeyRangeCache::new();
        assert!(cache.is_empty());
        assert_eq!(cache.len(), 0);
        assert!(
            cache
                .find_range(b"a", b"", RangeMode::CoveringSplit)
                .is_none()
        );
        assert!(
            cache
                .find_range(b"a", b"z", RangeMode::CoveringSplit)
                .is_none()
        );
    }

    #[test]
    fn lookup_before_first_range_returns_none() {
        let cache = KeyRangeCache::new();
        let update = CacheUpdate {
            database_id: 1,
            range: vec![make_range("m", "z", 1, "1")],
            group: vec![make_group(1, "1", 0)],
            key_recipes: None,
            _unknown_fields: Default::default(),
        };
        cache.add_ranges(&update);

        assert!(
            cache
                .find_range(b"a", b"", RangeMode::CoveringSplit)
                .is_none()
        );
        assert!(
            cache
                .find_range(b"a", b"f", RangeMode::CoveringSplit)
                .is_none()
        );
    }

    #[test]
    fn lookup_after_last_range_returns_none() {
        let cache = KeyRangeCache::new();
        let update = CacheUpdate {
            database_id: 1,
            range: vec![make_range("a", "m", 1, "1")],
            group: vec![make_group(1, "1", 0)],
            key_recipes: None,
            _unknown_fields: Default::default(),
        };
        cache.add_ranges(&update);

        assert!(
            cache
                .find_range(b"z", b"", RangeMode::CoveringSplit)
                .is_none()
        );
        assert!(
            cache
                .find_range(b"p", b"z", RangeMode::CoveringSplit)
                .is_none()
        );
    }

    #[test]
    fn lookup_gap_between_ranges_returns_none() {
        let cache = KeyRangeCache::new();
        let update = CacheUpdate {
            database_id: 1,
            range: vec![make_range("a", "f", 1, "1"), make_range("m", "z", 2, "1")],
            group: vec![make_group(1, "1", 0), make_group(2, "1", 0)],
            key_recipes: None,
            _unknown_fields: Default::default(),
        };
        cache.add_ranges(&update);

        assert!(
            cache
                .find_range(b"g", b"", RangeMode::CoveringSplit)
                .is_none()
        );
        assert!(
            cache
                .find_range(b"g", b"k", RangeMode::CoveringSplit)
                .is_none()
        );
    }

    #[test]
    fn point_lookup_at_start_key_returns_range() {
        let cache = KeyRangeCache::new();
        let update = CacheUpdate {
            database_id: 1,
            range: vec![make_range("a", "z", 1, "1")],
            group: vec![make_group(1, "1", 0)],
            key_recipes: None,
            _unknown_fields: Default::default(),
        };
        cache.add_ranges(&update);

        let hit = cache
            .find_range(b"a", b"", RangeMode::CoveringSplit)
            .expect("start key is inclusive");
        assert_eq!(hit.split_id, 1);
    }

    #[test]
    fn point_lookup_at_limit_key_returns_none() {
        let cache = KeyRangeCache::new();
        let update = CacheUpdate {
            database_id: 1,
            range: vec![make_range("a", "z", 1, "1")],
            group: vec![make_group(1, "1", 0)],
            key_recipes: None,
            _unknown_fields: Default::default(),
        };
        cache.add_ranges(&update);

        assert!(
            cache
                .find_range(b"z", b"", RangeMode::CoveringSplit)
                .is_none(),
            "limit key is exclusive"
        );
    }

    #[test]
    fn point_lookup_in_middle_returns_range() {
        let cache = KeyRangeCache::new();
        let update = CacheUpdate {
            database_id: 1,
            range: vec![make_range("a", "z", 1, "1")],
            group: vec![make_group(1, "1", 0)],
            key_recipes: None,
            _unknown_fields: Default::default(),
        };
        cache.add_ranges(&update);

        let hit = cache
            .find_range(b"m", b"", RangeMode::CoveringSplit)
            .expect("m should be inside [a, z)");
        assert_eq!(hit.split_id, 1);
    }

    #[test]
    fn covering_split_exact_match_returns_range() {
        let cache = KeyRangeCache::new();
        let update = CacheUpdate {
            database_id: 1,
            range: vec![make_range("a", "z", 1, "1")],
            group: vec![make_group(1, "1", 0)],
            key_recipes: None,
            _unknown_fields: Default::default(),
        };
        cache.add_ranges(&update);

        let hit = cache
            .find_range(b"a", b"z", RangeMode::CoveringSplit)
            .expect("exact range match should return split");
        assert_eq!(hit.split_id, 1);
    }

    #[test]
    fn covering_split_subrange_returns_range() {
        let cache = KeyRangeCache::new();
        let update = CacheUpdate {
            database_id: 1,
            range: vec![make_range("a", "z", 1, "1")],
            group: vec![make_group(1, "1", 0)],
            key_recipes: None,
            _unknown_fields: Default::default(),
        };
        cache.add_ranges(&update);

        let hit = cache
            .find_range(b"c", b"k", RangeMode::CoveringSplit)
            .expect("subrange inside split should succeed");
        assert_eq!(hit.split_id, 1);
    }

    #[test]
    fn covering_split_spanning_multiple_ranges_returns_none() {
        let cache = KeyRangeCache::new();
        let update = CacheUpdate {
            database_id: 1,
            range: vec![make_range("a", "m", 1, "1"), make_range("m", "z", 2, "1")],
            group: vec![make_group(1, "1", 0), make_group(2, "1", 0)],
            key_recipes: None,
            _unknown_fields: Default::default(),
        };
        cache.add_ranges(&update);

        assert!(
            cache
                .find_range(b"c", b"p", RangeMode::CoveringSplit)
                .is_none(),
            "range spanning two splits should return None in CoveringSplit mode"
        );
    }

    #[test]
    fn pick_random_single_range_returns_range() {
        let cache = KeyRangeCache::new();
        let update = CacheUpdate {
            database_id: 1,
            range: vec![make_range("a", "z", 1, "1")],
            group: vec![make_group(1, "1", 0)],
            key_recipes: None,
            _unknown_fields: Default::default(),
        };
        cache.add_ranges(&update);

        let hit = cache
            .find_range(b"c", b"k", RangeMode::PickRandom)
            .expect("single range match in PickRandom");
        assert_eq!(hit.split_id, 1);
    }

    #[test]
    fn pick_random_spanning_multiple_contiguous_ranges_returns_one() {
        let cache = KeyRangeCache::new();
        let update = CacheUpdate {
            database_id: 1,
            range: vec![make_range("a", "m", 1, "1"), make_range("m", "z", 2, "1")],
            group: vec![make_group(1, "1", 0), make_group(2, "1", 0)],
            key_recipes: None,
            _unknown_fields: Default::default(),
        };
        cache.add_ranges(&update);

        let hit = cache
            .find_range(b"c", b"p", RangeMode::PickRandom)
            .expect("contiguous multi-split should sample one split in PickRandom");
        assert!(hit.split_id == 1 || hit.split_id == 2);
    }

    #[test]
    fn pick_random_with_gap_returns_none() {
        let cache = KeyRangeCache::new();
        let update = CacheUpdate {
            database_id: 1,
            range: vec![make_range("a", "f", 1, "1"), make_range("g", "z", 2, "1")],
            group: vec![make_group(1, "1", 0), make_group(2, "1", 0)],
            key_recipes: None,
            _unknown_fields: Default::default(),
        };
        cache.add_ranges(&update);

        assert!(
            cache
                .find_range(b"c", b"p", RangeMode::PickRandom)
                .is_none(),
            "gap between [a, f) and [g, z) should return None in PickRandom"
        );
    }

    #[test]
    fn newer_generation_replaces_entire_range() {
        let cache = KeyRangeCache::new();
        let update1 = CacheUpdate {
            database_id: 1,
            range: vec![make_range("a", "z", 1, "1")],
            group: vec![make_group(1, "1", 0)],
            key_recipes: None,
            _unknown_fields: Default::default(),
        };
        cache.add_ranges(&update1);

        let update2 = CacheUpdate {
            database_id: 1,
            range: vec![make_range("a", "z", 2, "2")],
            group: vec![make_group(2, "2", 0)],
            key_recipes: None,
            _unknown_fields: Default::default(),
        };
        cache.add_ranges(&update2);

        assert_eq!(cache.len(), 1);
        let hit = cache
            .find_range(b"m", b"", RangeMode::CoveringSplit)
            .expect("newer range");
        assert_eq!(hit.split_id, 2);
    }

    #[test]
    fn older_generation_is_ignored() {
        let cache = KeyRangeCache::new();
        let update1 = CacheUpdate {
            database_id: 1,
            range: vec![make_range("a", "z", 1, "2")],
            group: vec![make_group(1, "2", 0)],
            key_recipes: None,
            _unknown_fields: Default::default(),
        };
        cache.add_ranges(&update1);

        let update2 = CacheUpdate {
            database_id: 1,
            range: vec![make_range("a", "z", 2, "1")],
            group: vec![make_group(2, "1", 0)],
            key_recipes: None,
            _unknown_fields: Default::default(),
        };
        cache.add_ranges(&update2);

        assert_eq!(cache.len(), 1);
        let hit = cache
            .find_range(b"m", b"", RangeMode::CoveringSplit)
            .expect("original newer range should remain");
        assert_eq!(hit.split_id, 1);
    }

    #[test]
    fn equal_generation_is_ignored() {
        let cache = KeyRangeCache::new();
        let update1 = CacheUpdate {
            database_id: 1,
            range: vec![make_range("a", "z", 1, "1")],
            group: vec![make_group(1, "1", 0)],
            key_recipes: None,
            _unknown_fields: Default::default(),
        };
        cache.add_ranges(&update1);

        let update2 = CacheUpdate {
            database_id: 1,
            range: vec![make_range("a", "z", 1, "1")],
            group: vec![make_group(1, "1", 0)],
            key_recipes: None,
            _unknown_fields: Default::default(),
        };
        cache.add_ranges(&update2);

        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn newer_generation_splitting_head_and_tail() {
        let cache = KeyRangeCache::new();
        let update1 = CacheUpdate {
            database_id: 1,
            range: vec![make_range("a", "z", 1, "1")],
            group: vec![make_group(1, "1", 0)],
            key_recipes: None,
            _unknown_fields: Default::default(),
        };
        cache.add_ranges(&update1);

        let update2 = CacheUpdate {
            database_id: 1,
            range: vec![make_range("g", "m", 2, "2")],
            group: vec![make_group(2, "2", 0)],
            key_recipes: None,
            _unknown_fields: Default::default(),
        };
        cache.add_ranges(&update2);

        assert_eq!(
            cache.len(),
            3,
            "range [a, z) should be split into [a, g), [g, m), and [m, z)"
        );

        let left = cache
            .find_range(b"c", b"", RangeMode::CoveringSplit)
            .expect("left head");
        assert_eq!(left.split_id, 1);
        assert_eq!(left.limit_key.as_ref(), b"g");

        let mid = cache
            .find_range(b"j", b"", RangeMode::CoveringSplit)
            .expect("middle newer range");
        assert_eq!(mid.split_id, 2);

        let right = cache
            .find_range(b"t", b"", RangeMode::CoveringSplit)
            .expect("right tail");
        assert_eq!(right.split_id, 1);
        assert_eq!(right.start_key.as_ref(), b"m");
    }

    #[test]
    fn newer_generation_overwriting_start_of_range() {
        let cache = KeyRangeCache::new();
        let update1 = CacheUpdate {
            database_id: 1,
            range: vec![make_range("a", "z", 1, "1")],
            group: vec![make_group(1, "1", 0)],
            key_recipes: None,
            _unknown_fields: Default::default(),
        };
        cache.add_ranges(&update1);

        let update2 = CacheUpdate {
            database_id: 1,
            range: vec![make_range("a", "m", 2, "2")],
            group: vec![make_group(2, "2", 0)],
            key_recipes: None,
            _unknown_fields: Default::default(),
        };
        cache.add_ranges(&update2);

        assert_eq!(cache.len(), 2);
        let left = cache
            .find_range(b"c", b"", RangeMode::CoveringSplit)
            .expect("new left range");
        assert_eq!(left.split_id, 2);

        let right = cache
            .find_range(b"t", b"", RangeMode::CoveringSplit)
            .expect("remaining tail");
        assert_eq!(right.split_id, 1);
        assert_eq!(right.start_key.as_ref(), b"m");
    }

    #[test]
    fn newer_generation_overwriting_end_of_range() {
        let cache = KeyRangeCache::new();
        let update1 = CacheUpdate {
            database_id: 1,
            range: vec![make_range("a", "z", 1, "1")],
            group: vec![make_group(1, "1", 0)],
            key_recipes: None,
            _unknown_fields: Default::default(),
        };
        cache.add_ranges(&update1);

        let update2 = CacheUpdate {
            database_id: 1,
            range: vec![make_range("m", "z", 2, "2")],
            group: vec![make_group(2, "2", 0)],
            key_recipes: None,
            _unknown_fields: Default::default(),
        };
        cache.add_ranges(&update2);

        assert_eq!(cache.len(), 2);
        let left = cache
            .find_range(b"c", b"", RangeMode::CoveringSplit)
            .expect("remaining head");
        assert_eq!(left.split_id, 1);
        assert_eq!(left.limit_key.as_ref(), b"m");

        let right = cache
            .find_range(b"t", b"", RangeMode::CoveringSplit)
            .expect("new right range");
        assert_eq!(right.split_id, 2);
    }

    #[test]
    fn select_tablet_prefers_leader_when_requested() {
        let cache = KeyRangeCache::new();
        let update = CacheUpdate {
            database_id: 1,
            range: vec![make_range("a", "z", 1, "1")],
            group: vec![make_group(1, "1", 1)], // leader_index = 1
            key_recipes: None,
            _unknown_fields: Default::default(),
        };
        cache.add_ranges(&update);

        let hit = cache
            .find_range(b"m", b"", RangeMode::CoveringSplit)
            .expect("range hit");
        let tablet = cache
            .select_tablet(&hit, true)
            .expect("should select leader");
        assert_eq!(tablet.tablet_uid, 2);
    }

    #[test]
    fn select_tablet_falls_back_when_leader_skipped_or_empty() {
        let cache = KeyRangeCache::new();
        let mut group = make_group(1, "1", 1);
        group.tablets[1].skip = true; // Mark leader as skipped

        let update = CacheUpdate {
            database_id: 1,
            range: vec![make_range("a", "z", 1, "1")],
            group: vec![group],
            key_recipes: None,
            _unknown_fields: Default::default(),
        };
        cache.add_ranges(&update);

        let hit = cache
            .find_range(b"m", b"", RangeMode::CoveringSplit)
            .expect("range hit");
        let tablet = cache
            .select_tablet(&hit, true)
            .expect("should fall back to non-leader tablet");
        assert_eq!(tablet.tablet_uid, 1);
    }

    #[test]
    fn select_tablet_prefers_distance_less_than_five() {
        let cache = KeyRangeCache::new();
        let mut group = make_group(1, "1", -1);
        group.tablets[0].distance = 10; // Far replica
        group.tablets[1].distance = 2; // Close replica

        let update = CacheUpdate {
            database_id: 1,
            range: vec![make_range("a", "z", 1, "1")],
            group: vec![group],
            key_recipes: None,
            _unknown_fields: Default::default(),
        };
        cache.add_ranges(&update);

        let hit = cache
            .find_range(b"m", b"", RangeMode::CoveringSplit)
            .expect("range hit");
        let tablet = cache
            .select_tablet(&hit, false)
            .expect("should select close replica");
        assert_eq!(tablet.tablet_uid, 2);
    }

    #[test]
    fn select_tablet_prefers_minimum_distance() {
        let cache = KeyRangeCache::new();
        let mut group = make_group(1, "1", -1);
        group.tablets.push(Tablet {
            tablet_uid: 3,
            server_address: "localhost:8003".to_string(),
            location: "us-central1".to_string(),
            role: crate::model::tablet::Role::ReadWrite,
            incarnation: Bytes::from_static(b"1"),
            distance: 10,
            skip: false,
            _unknown_fields: Default::default(),
        });
        group.tablets[0].distance = 4; // same region, different zone
        group.tablets[1].distance = 0; // same zone
        group.tablets[2].distance = 10; // remote region

        let update = CacheUpdate {
            database_id: 1,
            range: vec![make_range("a", "z", 1, "1")],
            group: vec![group],
            key_recipes: None,
            _unknown_fields: Default::default(),
        };
        cache.add_ranges(&update);

        let hit = cache
            .find_range(b"m", b"", RangeMode::CoveringSplit)
            .expect("range hit");
        let tablet = cache
            .select_tablet(&hit, false)
            .expect("should select minimum distance replica");
        assert_eq!(
            tablet.tablet_uid, 2,
            "tablet uid 2 has distance 0, preferring it over distance 4 and 10"
        );
    }

    #[test]
    fn select_tablet_returns_none_for_empty_group() {
        let cache = KeyRangeCache::new();
        let mut group = make_group(1, "1", -1);
        group.tablets.clear();

        let update = CacheUpdate {
            database_id: 1,
            range: vec![make_range("a", "z", 1, "1")],
            group: vec![group],
            key_recipes: None,
            _unknown_fields: Default::default(),
        };
        cache.add_ranges(&update);

        let hit = cache
            .find_range(b"m", b"", RangeMode::CoveringSplit)
            .expect("range hit");
        assert!(cache.select_tablet(&hit, true).is_none());
    }

    #[test]
    fn clear_removes_all_ranges_and_groups() {
        let cache = KeyRangeCache::new();
        let update = CacheUpdate {
            database_id: 1,
            range: vec![make_range("a", "z", 1, "1")],
            group: vec![make_group(1, "1", 0)],
            key_recipes: None,
            _unknown_fields: Default::default(),
        };
        cache.add_ranges(&update);
        assert_eq!(cache.len(), 1);

        cache.clear();
        assert!(cache.is_empty());
        assert_eq!(cache.len(), 0);
        assert!(cache.get_group(1).is_none());
    }

    #[test]
    fn access_time_updates_on_lookup() {
        let cache = KeyRangeCache::new();
        let update = CacheUpdate {
            database_id: 1,
            range: vec![make_range("a", "z", 1, "1")],
            group: vec![make_group(1, "1", 0)],
            key_recipes: None,
            _unknown_fields: Default::default(),
        };
        cache.add_ranges(&update);

        let hit1 = cache
            .find_range(b"m", b"", RangeMode::CoveringSplit)
            .expect("hit 1");
        let t1 = hit1.last_access.load(Ordering::Relaxed);

        let hit2 = cache
            .find_range(b"m", b"", RangeMode::CoveringSplit)
            .expect("hit 2");
        let t2 = hit2.last_access.load(Ordering::Relaxed);

        assert!(t2 > t1, "access time should increment on each lookup");
    }

    #[test]
    fn unbounded_right_split_lookup() {
        let cache = KeyRangeCache::new();
        let update = CacheUpdate {
            database_id: 1,
            range: vec![make_range("m", "", 1, "1")], // right-unbounded (+inf)
            group: vec![make_group(1, "1", 0)],
            key_recipes: None,
            _unknown_fields: Default::default(),
        };
        cache.add_ranges(&update);

        assert!(
            cache
                .find_range(b"a", b"", RangeMode::CoveringSplit)
                .is_none()
        );
        assert!(
            cache
                .find_range(b"m", b"", RangeMode::CoveringSplit)
                .is_some()
        );
        assert!(
            cache
                .find_range(b"z", b"", RangeMode::CoveringSplit)
                .is_some()
        );
        assert!(
            cache
                .find_range(b"z\x00", b"", RangeMode::CoveringSplit)
                .is_some()
        );
    }

    #[test]
    fn unbounded_left_split_lookup() {
        let cache = KeyRangeCache::new();
        let update = CacheUpdate {
            database_id: 1,
            range: vec![make_range("", "m", 1, "1")], // left-unbounded (-inf)
            group: vec![make_group(1, "1", 0)],
            key_recipes: None,
            _unknown_fields: Default::default(),
        };
        cache.add_ranges(&update);

        assert!(
            cache
                .find_range(b"", b"", RangeMode::CoveringSplit)
                .is_some()
        );
        assert!(
            cache
                .find_range(b"\x00", b"", RangeMode::CoveringSplit)
                .is_some()
        );
        assert!(
            cache
                .find_range(b"a", b"", RangeMode::CoveringSplit)
                .is_some()
        );
        assert!(
            cache
                .find_range(b"m", b"", RangeMode::CoveringSplit)
                .is_none()
        );
    }

    #[test]
    fn full_table_unbounded_split_lookup() {
        let cache = KeyRangeCache::new();
        let update = CacheUpdate {
            database_id: 1,
            range: vec![make_range("", "", 1, "1")], // entire table (-inf .. +inf)
            group: vec![make_group(1, "1", 0)],
            key_recipes: None,
            _unknown_fields: Default::default(),
        };
        cache.add_ranges(&update);

        assert!(
            cache
                .find_range(b"", b"", RangeMode::CoveringSplit)
                .is_some()
        );
        assert!(
            cache
                .find_range(b"a", b"", RangeMode::CoveringSplit)
                .is_some()
        );
        assert!(
            cache
                .find_range(b"z", b"", RangeMode::CoveringSplit)
                .is_some()
        );
    }

    #[test]
    fn newer_generation_replaces_unbounded_split() {
        let cache = KeyRangeCache::new();
        let update1 = CacheUpdate {
            database_id: 1,
            range: vec![make_range("", "", 1, "1")],
            group: vec![make_group(1, "1", 0)],
            key_recipes: None,
            _unknown_fields: Default::default(),
        };
        cache.add_ranges(&update1);

        // Replace entire unbounded split with a newer generation subrange
        let update2 = CacheUpdate {
            database_id: 1,
            range: vec![make_range("f", "m", 2, "2")],
            group: vec![make_group(2, "2", 0)],
            key_recipes: None,
            _unknown_fields: Default::default(),
        };
        cache.add_ranges(&update2);

        // Check head (-inf .. f), middle (f .. m), tail (m .. +inf)
        let hit_head = cache
            .find_range(b"a", b"", RangeMode::CoveringSplit)
            .expect("head");
        assert_eq!(hit_head.group_uid, 1);
        assert_eq!(hit_head.limit_key, Bytes::from_static(b"f"));

        let hit_mid = cache
            .find_range(b"g", b"", RangeMode::CoveringSplit)
            .expect("mid");
        assert_eq!(hit_mid.group_uid, 2);

        let hit_tail = cache
            .find_range(b"z", b"", RangeMode::CoveringSplit)
            .expect("tail");
        assert_eq!(hit_tail.group_uid, 1);
        assert_eq!(hit_tail.start_key, Bytes::from_static(b"m"));
        assert_eq!(hit_tail.limit_key, Bytes::from_static(b""));
    }

    #[test]
    fn pick_random_with_no_initial_overlap_never_samples_preceding_range() {
        let cache = KeyRangeCache::new();
        // Set min entries to 1 so that random pick returns even with a gap
        cache
            .min_cache_entries_for_random_pick
            .store(1, Ordering::Relaxed);

        let update = CacheUpdate {
            database_id: 1,
            range: vec![
                make_range("a", "c", 1, "1"), // preceding range not covering "d"
                make_range("e", "g", 2, "1"), // range after gap
            ],
            group: vec![make_group(1, "1", 0), make_group(2, "1", 0)],
            key_recipes: None,
            _unknown_fields: Default::default(),
        };
        cache.add_ranges(&update);

        let hit = cache.find_range(b"d", b"g", RangeMode::PickRandom);
        assert!(hit.is_some());
        assert_eq!(
            hit.unwrap().group_uid,
            2,
            "must never sample preceding non-overlapping range"
        );
    }

    #[test]
    fn pick_random_with_no_preceding_range_finds_subsequent_range_after_gap() {
        let cache = KeyRangeCache::new();
        cache
            .min_cache_entries_for_random_pick
            .store(1, Ordering::Relaxed);

        let update = CacheUpdate {
            database_id: 1,
            range: vec![make_range("m", "z", 1, "1")],
            group: vec![make_group(1, "1", 0)],
            key_recipes: None,
            _unknown_fields: Default::default(),
        };
        cache.add_ranges(&update);

        let hit = cache.find_range(b"a", b"p", RangeMode::PickRandom);
        assert!(hit.is_some());
        assert_eq!(hit.unwrap().group_uid, 1);
    }

    #[test]
    fn equal_generation_with_different_bounds_overwrites_overlapping_portion() {
        let cache = KeyRangeCache::new();
        let update1 = CacheUpdate {
            database_id: 1,
            range: vec![make_range("a", "z", 1, "1")],
            group: vec![make_group(1, "1", 0)],
            key_recipes: None,
            _unknown_fields: Default::default(),
        };
        cache.add_ranges(&update1);

        // Same generation "1", but subrange "m" .. "p" with a new group
        let update2 = CacheUpdate {
            database_id: 1,
            range: vec![make_range("m", "p", 2, "1")],
            group: vec![make_group(2, "1", 0)],
            key_recipes: None,
            _unknown_fields: Default::default(),
        };
        cache.add_ranges(&update2);

        let hit_mid = cache
            .find_range(b"n", b"", RangeMode::CoveringSplit)
            .expect("mid");
        assert_eq!(hit_mid.group_uid, 2);
    }

    #[test]
    fn newer_generation_spanning_multiple_ranges_splits_head_and_tail() {
        let cache = KeyRangeCache::new();
        let update1 = CacheUpdate {
            database_id: 1,
            range: vec![
                make_range("a", "d", 1, "1"),
                make_range("d", "g", 2, "1"),
                make_range("g", "j", 3, "1"),
            ],
            group: vec![
                make_group(1, "1", 0),
                make_group(2, "1", 0),
                make_group(3, "1", 0),
            ],
            key_recipes: None,
            _unknown_fields: Default::default(),
        };
        cache.add_ranges(&update1);

        let update2 = CacheUpdate {
            database_id: 1,
            range: vec![make_range("b", "i", 4, "2")],
            group: vec![make_group(4, "2", 0)],
            key_recipes: None,
            _unknown_fields: Default::default(),
        };
        cache.add_ranges(&update2);

        assert_eq!(cache.len(), 3);
        let head = cache
            .find_range(b"a", b"", RangeMode::CoveringSplit)
            .expect("head");
        assert_eq!(head.group_uid, 1);
        assert_eq!(head.limit_key, Bytes::from_static(b"b"));

        let mid = cache
            .find_range(b"e", b"", RangeMode::CoveringSplit)
            .expect("mid");
        assert_eq!(mid.group_uid, 4);

        let tail = cache
            .find_range(b"i", b"", RangeMode::CoveringSplit)
            .expect("tail");
        assert_eq!(tail.group_uid, 3);
        assert_eq!(tail.start_key, Bytes::from_static(b"i"));
        assert_eq!(tail.limit_key, Bytes::from_static(b"j"));

        assert!(
            cache
                .find_range(b"d", b"", RangeMode::CoveringSplit)
                .unwrap()
                .group_uid
                == 4
        );
    }

    #[test]
    fn replace_range_log_n_start_key_query_cases() {
        let cache = KeyRangeCache::new();

        // Case 3: Empty cache insert ["m" .. "p")
        let update1 = CacheUpdate {
            database_id: 1,
            range: vec![make_range("m", "p", 1, "1")],
            group: vec![make_group(1, "1", 0)],
            key_recipes: None,
            _unknown_fields: Default::default(),
        };
        cache.add_ranges(&update1);
        assert_eq!(cache.len(), 1);

        // Case 3 (non-empty): Insert ["a" .. "c") before ["m" .. "p")
        let update2 = CacheUpdate {
            database_id: 1,
            range: vec![make_range("a", "c", 2, "1")],
            group: vec![make_group(2, "1", 0)],
            key_recipes: None,
            _unknown_fields: Default::default(),
        };
        cache.add_ranges(&update2);
        assert_eq!(cache.len(), 2);

        // Case 2: Insert adjacent ["c" .. "e") right after ["a" .. "c")
        let update3 = CacheUpdate {
            database_id: 1,
            range: vec![make_range("c", "e", 3, "1")],
            group: vec![make_group(3, "1", 0)],
            key_recipes: None,
            _unknown_fields: Default::default(),
        };
        cache.add_ranges(&update3);
        assert_eq!(cache.len(), 3);

        // Case 1: Insert ["d" .. "n") with generation 2 overlapping both ["c".."e") and ["m".."p")
        let update4 = CacheUpdate {
            database_id: 1,
            range: vec![make_range("d", "n", 4, "2")],
            group: vec![make_group(4, "2", 0)],
            key_recipes: None,
            _unknown_fields: Default::default(),
        };
        cache.add_ranges(&update4);

        // Check resulting splits:
        // 1. ["a" .. "c") - group 2
        // 2. ["c" .. "d") - group 3 (split head of ["c" .. "e"))
        // 3. ["d" .. "n") - group 4 (new range)
        // 4. ["n" .. "p") - group 1 (split tail of ["m" .. "p"))
        assert_eq!(cache.len(), 4);

        let r1 = cache
            .find_range(b"a", b"", RangeMode::CoveringSplit)
            .unwrap();
        assert_eq!(r1.group_uid, 2);
        assert_eq!(r1.limit_key, Bytes::from_static(b"c"));

        let r2 = cache
            .find_range(b"c", b"", RangeMode::CoveringSplit)
            .unwrap();
        assert_eq!(r2.group_uid, 3);
        assert_eq!(r2.limit_key, Bytes::from_static(b"d"));

        let r3 = cache
            .find_range(b"d", b"", RangeMode::CoveringSplit)
            .unwrap();
        assert_eq!(r3.group_uid, 4);
        assert_eq!(r3.limit_key, Bytes::from_static(b"n"));

        let r4 = cache
            .find_range(b"n", b"", RangeMode::CoveringSplit)
            .unwrap();
        assert_eq!(r4.group_uid, 1);
        assert_eq!(r4.limit_key, Bytes::from_static(b"p"));
    }
}
