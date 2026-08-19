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
use std::collections::hash_map::Entry;
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

/// Maximum distance for a replica to be considered local (within the same region or metro).
pub(crate) const MAX_LOCAL_REPLICA_DISTANCE: u32 = 5;

/// An immutable, in-memory cache representation of a Spanner Paxos group.
///
/// # Thread-Safety & Immutability
/// This struct is completely immutable once constructed via [`CachedGroup::from_proto`].
/// Precomputed fields (`local_leader_index` and `eligible_replica_indices`) are evaluated once
/// during ingestion and remain fixed for the lifetime of the struct.
///
/// The cache manages group updates via a Read-Copy-Update (RCU) pattern wrapped in `Arc<CachedGroup>`:
/// background updates atomically replace the `Arc` pointer in the cache map under a write lock,
/// while concurrent in-flight requests safely read their immutable snapshot without locks or data races.
#[derive(Debug, Clone)]
pub(crate) struct CachedGroup {
    pub group_uid: u64,
    pub generation: Bytes,
    pub tablets: Vec<Tablet>,
    /// 0-based index into `tablets` representing the designated Paxos leader, or `None` if no leader is designated.
    ///
    /// In Spanner metadata protos, a negative `leader_index` (typically `-1`) denotes that no leader is designated
    /// or that leader routing is unknown/unspecified.
    pub leader_index: Option<usize>,
    /// Precomputed index into `tablets` of the local leader (if designated, routable, and distance <= 5).
    pub local_leader_index: Option<usize>,
    /// Precomputed indices into `tablets` for candidate replicas in the lowest available distance tier.
    pub eligible_replica_indices: Vec<usize>,
}

impl CachedGroup {
    pub(crate) fn from_proto(proto: Group) -> Self {
        let leader_index = Self::parse_leader_index(proto.leader_index, proto.tablets.len());
        let local_leader_index = Self::compute_local_leader_index(&proto.tablets, leader_index);
        let eligible_replica_indices = Self::compute_eligible_replica_indices(&proto.tablets);

        Self {
            group_uid: proto.group_uid,
            generation: proto.generation,
            tablets: proto.tablets,
            leader_index,
            local_leader_index,
            eligible_replica_indices,
        }
    }

    /// Returns `true` if this group has a designated leader index within valid bounds.
    pub(crate) fn has_leader(&self) -> bool {
        self.leader_index.is_some()
    }

    /// Returns a reference to the leader tablet if designated, non-skipped, and with a non-empty server address.
    pub(crate) fn leader(&self) -> Option<&Tablet> {
        let candidate = &self.tablets[self.leader_index?];
        if !Self::is_routable(candidate) {
            return None;
        }
        Some(candidate)
    }

    /// Returns a reference to the leader tablet if designated, routable, and local
    /// (`distance <= MAX_LOCAL_REPLICA_DISTANCE`).
    pub(crate) fn local_leader(&self) -> Option<&Tablet> {
        let index = self.local_leader_index?;
        Some(&self.tablets[index])
    }

    /// Returns candidate replica references in the lowest locality tier matching the minimum distance.
    ///
    /// If `prefer_leader` is `true` and a valid leader is present, returns a single-element
    /// vector containing a reference to that leader (even if remote, to avoid forwarding hops).
    ///
    /// Otherwise, returns references to the precomputed candidate replicas in the lowest available distance tier.
    pub(crate) fn eligible_tablets(&self, prefer_leader: bool) -> Vec<&Tablet> {
        if prefer_leader && let Some(leader) = self.leader() {
            return vec![leader];
        }

        self.eligible_replica_indices
            .iter()
            .map(|&index| &self.tablets[index])
            .collect()
    }

    /// Parses the raw protobuf leader index, returning `None` if negative or out of bounds.
    fn parse_leader_index(leader_index: i32, tablets_count: usize) -> Option<usize> {
        if leader_index < 0 {
            return None;
        }
        let index = leader_index as usize;
        if index >= tablets_count {
            return None;
        }
        Some(index)
    }

    /// Precomputes the local leader index if the designated leader is routable and local.
    fn compute_local_leader_index(
        tablets: &[Tablet],
        leader_index: Option<usize>,
    ) -> Option<usize> {
        let index = leader_index?;
        let candidate = &tablets[index];
        if !Self::is_routable(candidate) || !Self::is_local_distance(candidate.distance) {
            return None;
        }
        Some(index)
    }

    /// Precomputes indices into `tablets` for candidate replicas in the lowest available distance tier.
    fn compute_eligible_replica_indices(tablets: &[Tablet]) -> Vec<usize> {
        let mut candidates = Vec::new();
        let mut minimum_distance = u32::MAX;
        let mut local_tier_active = false;

        for (index, tablet) in tablets.iter().enumerate() {
            if !Self::is_routable(tablet) {
                continue;
            }

            let is_local = Self::is_local_distance(tablet.distance);

            // When encountering a local replica for the first time, switch to local tier and clear remote candidates.
            if is_local && !local_tier_active {
                local_tier_active = true;
                minimum_distance = u32::MAX;
                candidates.clear();
            }

            // If a local replica was already found, ignore all remote replicas.
            if !is_local && local_tier_active {
                continue;
            }

            Self::collect_minimum_distance_index(
                &mut candidates,
                &mut minimum_distance,
                index,
                tablet.distance,
            );
        }

        candidates
    }

    /// Returns `true` if the tablet is non-skipped and has a non-empty server address.
    fn is_routable(tablet: &Tablet) -> bool {
        !tablet.skip && !tablet.server_address.is_empty()
    }

    /// Returns `true` if the distance metric is within the local region/metro threshold.
    fn is_local_distance(distance: u32) -> bool {
        distance <= MAX_LOCAL_REPLICA_DISTANCE
    }

    /// Appends the tablet index if distance matches current minimum, or resets the list
    /// if it establishes a strictly lower minimum distance.
    fn collect_minimum_distance_index(
        candidates: &mut Vec<usize>,
        minimum_distance: &mut u32,
        index: usize,
        distance: u32,
    ) {
        if distance < *minimum_distance {
            *minimum_distance = distance;
            candidates.clear();
            candidates.push(index);
            return;
        }
        if distance == *minimum_distance {
            candidates.push(index);
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
            .map(Arc::clone)
    }

    /// Applies updates from a Spanner `CacheUpdate` message.
    pub(crate) fn add_ranges(&self, cache_update: &CacheUpdate) {
        if cache_update.group.is_empty() && cache_update.range.is_empty() {
            return;
        }
        let mut state = self.state.write().expect("lock cache state for add_ranges");

        for group_in in &cache_update.group {
            match state.groups.entry(group_in.group_uid) {
                Entry::Occupied(mut entry) if group_in.generation >= entry.get().generation => {
                    entry.insert(Arc::new(CachedGroup::from_proto(group_in.clone())));
                }
                Entry::Vacant(entry) => {
                    entry.insert(Arc::new(CachedGroup::from_proto(group_in.clone())));
                }
                _ => {}
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
                overlapping.push(Arc::clone(existing));
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
                    return Some(Arc::clone(first_range));
                }
                return None;
            }

            let limit_in_range =
                first_range.limit_key.is_empty() || limit <= first_range.limit_key.as_ref();
            if in_range && limit_in_range {
                first_range
                    .last_access
                    .store(self.access_time_now(), Ordering::Relaxed);
                return Some(Arc::clone(first_range));
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
            return Some(Arc::clone(sampled_range));
        }

        None
    }

    /// Returns all eligible candidate tablets in the lowest distance tier for the split range.
    ///
    /// If `prefer_leader` is `true` and a valid leader is present, returns a single-element
    /// vector containing that leader. Otherwise, returns candidate replicas in the lowest available
    /// distance tier.
    pub(crate) fn get_eligible_tablets(
        &self,
        range: &CachedRange,
        prefer_leader: bool,
    ) -> Option<Vec<Tablet>> {
        let group = self.get_group(range.group_uid)?;
        let eligible = group.eligible_tablets(prefer_leader);
        if eligible.is_empty() {
            return None;
        }
        Some(eligible.into_iter().cloned().collect())
    }

    /// Selects an appropriate tablet replica from the cached range's group.
    ///
    /// If `prefer_leader` is `true` and a valid leader is present, selects that leader.
    /// Otherwise, selects uniformly among candidate replicas within the lowest available distance tier.
    pub(crate) fn select_tablet(&self, range: &CachedRange, prefer_leader: bool) -> Option<Tablet> {
        let group = self.get_group(range.group_uid)?;

        if prefer_leader && let Some(leader) = group.leader() {
            return Some(leader.clone());
        }

        let indices = &group.eligible_replica_indices;
        if indices.is_empty() {
            return None;
        }
        if indices.len() == 1 {
            return Some(group.tablets[indices[0]].clone());
        }
        let selected_index = rand::random_range(0..indices.len());
        Some(group.tablets[indices[selected_index]].clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::tablet::Role;
    use static_assertions::assert_impl_all;
    use std::collections::HashSet;
    use std::fmt::Debug;

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
                    role: Role::ReadWrite,
                    incarnation: Bytes::from_static(b"1"),
                    distance: 0,
                    skip: false,
                    _unknown_fields: Default::default(),
                },
                Tablet {
                    tablet_uid: 2,
                    server_address: "localhost:8002".to_string(),
                    location: "us-central1".to_string(),
                    role: Role::ReadWrite,
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
            role: Role::ReadWrite,
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
            hit.expect("hit").group_uid,
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
        assert_eq!(hit.expect("hit").group_uid, 1);
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

        assert_eq!(
            cache
                .find_range(b"d", b"", RangeMode::CoveringSplit)
                .expect("range")
                .group_uid,
            4
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

        let range_a = cache
            .find_range(b"a", b"", RangeMode::CoveringSplit)
            .expect("find range starting at 'a'");
        assert_eq!(range_a.group_uid, 2);
        assert_eq!(range_a.limit_key, Bytes::from_static(b"c"));

        let range_c = cache
            .find_range(b"c", b"", RangeMode::CoveringSplit)
            .expect("find range starting at 'c'");
        assert_eq!(range_c.group_uid, 3);
        assert_eq!(range_c.limit_key, Bytes::from_static(b"d"));

        let range_d = cache
            .find_range(b"d", b"", RangeMode::CoveringSplit)
            .expect("find range starting at 'd'");
        assert_eq!(range_d.group_uid, 4);
        assert_eq!(range_d.limit_key, Bytes::from_static(b"n"));

        let range_n = cache
            .find_range(b"n", b"", RangeMode::CoveringSplit)
            .expect("find range starting at 'n'");
        assert_eq!(range_n.group_uid, 1);
        assert_eq!(range_n.limit_key, Bytes::from_static(b"p"));
    }

    #[test]
    fn traits() {
        assert_impl_all!(KeyRangeCache: Send, Sync);
        assert_impl_all!(CachedGroup: Send, Sync, Debug, Clone);
        assert_impl_all!(CachedRange: Send, Sync, Debug);
    }

    #[test]
    fn cached_group_leader_accessors() {
        // 1. Group with no leader (leader_index = -1)
        let group_no_leader = CachedGroup::from_proto(make_group(1, "1", -1));
        assert!(!group_no_leader.has_leader());
        assert!(group_no_leader.leader().is_none());
        assert!(group_no_leader.local_leader().is_none());

        // 2. Group with valid local leader (leader_index = 0, distance = 0)
        let group_local_leader = CachedGroup::from_proto(make_group(1, "1", 0));
        assert!(group_local_leader.has_leader());
        assert_eq!(
            group_local_leader
                .leader()
                .expect("should find leader")
                .tablet_uid,
            1
        );
        assert_eq!(
            group_local_leader
                .local_leader()
                .expect("should find local leader")
                .tablet_uid,
            1
        );

        // 3. Group with remote leader (leader_index = 0, distance = 10)
        let mut proto_remote = make_group(1, "1", 0);
        proto_remote.tablets[0].distance = 10;
        let group_remote_leader = CachedGroup::from_proto(proto_remote);
        assert!(group_remote_leader.has_leader());
        assert_eq!(
            group_remote_leader
                .leader()
                .expect("should find leader")
                .tablet_uid,
            1
        );
        assert!(
            group_remote_leader.local_leader().is_none(),
            "remote leader with distance 10 must not be considered a local leader"
        );

        // 4. Group with skipped leader
        let mut proto_skipped = make_group(1, "1", 0);
        proto_skipped.tablets[0].skip = true;
        let group_skipped_leader = CachedGroup::from_proto(proto_skipped);
        assert!(group_skipped_leader.has_leader());
        assert!(group_skipped_leader.leader().is_none());
        assert!(group_skipped_leader.local_leader().is_none());

        // 5. Group with empty address leader
        let mut proto_empty_address = make_group(1, "1", 0);
        proto_empty_address.tablets[0].server_address.clear();
        let group_empty_address_leader = CachedGroup::from_proto(proto_empty_address);
        assert!(group_empty_address_leader.has_leader());
        assert!(group_empty_address_leader.leader().is_none());
        assert!(group_empty_address_leader.local_leader().is_none());
    }

    #[test]
    fn cached_group_eligible_tablets_partitions_local_and_remote() {
        let mut proto = make_group(1, "1", -1);
        proto.tablets.push(Tablet {
            tablet_uid: 3,
            server_address: "localhost:8003".to_string(),
            location: "us-central1".to_string(),
            role: Role::ReadWrite,
            incarnation: Bytes::from_static(b"1"),
            distance: 10,
            skip: false,
            _unknown_fields: Default::default(),
        });
        proto.tablets[0].distance = 4; // Local tier
        proto.tablets[1].distance = 2; // Local tier (minimum)
        proto.tablets[2].distance = 10; // Remote tier

        let group = CachedGroup::from_proto(proto.clone());
        let eligible = group.eligible_tablets(false);
        assert_eq!(
            eligible.len(),
            1,
            "should select single minimum distance local replica"
        );
        assert_eq!(eligible[0].tablet_uid, 2);

        // When multiple replicas tie at the minimum local distance
        let mut proto_tied = proto;
        proto_tied.tablets[0].distance = 2; // tied with tablet 2 at distance 2
        let group_tied = CachedGroup::from_proto(proto_tied);
        let eligible_tied = group_tied.eligible_tablets(false);
        assert_eq!(
            eligible_tied.len(),
            2,
            "both local candidates at distance 2 should be eligible"
        );

        // When only remote replicas exist (all distance > 5)
        let mut proto_remote_only = make_group(1, "1", -1);
        proto_remote_only.tablets[0].distance = 15;
        proto_remote_only.tablets[1].distance = 10; // lowest remote distance
        let group_remote_only = CachedGroup::from_proto(proto_remote_only);
        let eligible_remote = group_remote_only.eligible_tablets(false);
        assert_eq!(
            eligible_remote.len(),
            1,
            "should select lowest remote distance replica"
        );
        assert_eq!(eligible_remote[0].tablet_uid, 2);
    }

    #[test]
    fn get_group_and_get_eligible_tablets() {
        let cache = KeyRangeCache::new();
        let update = CacheUpdate {
            database_id: 1,
            range: vec![make_range("a", "z", 100, "1")],
            group: vec![make_group(100, "1", 0)],
            key_recipes: None,
            _unknown_fields: Default::default(),
        };
        cache.add_ranges(&update);

        let group = cache.get_group(100);
        assert!(group.is_some(), "group 100 should be cached");
        assert_eq!(group.expect("group").group_uid, 100);

        assert!(
            cache.get_group(999).is_none(),
            "non-existent group should return None"
        );

        let range = cache
            .find_range(b"m", b"", RangeMode::CoveringSplit)
            .expect("range hit");

        let eligible_leader = cache.get_eligible_tablets(&range, true);
        assert!(eligible_leader.is_some());
        assert_eq!(eligible_leader.expect("eligible leader").len(), 1);

        let eligible_replicas = cache.get_eligible_tablets(&range, false);
        assert!(eligible_replicas.is_some());
        assert_eq!(
            eligible_replicas.expect("eligible replicas")[0].tablet_uid,
            1
        );
    }

    #[test]
    fn select_tablet_prefers_leader_even_when_remote() {
        let cache = KeyRangeCache::new();
        let mut group = make_group(1, "1", 0);
        // Leader (tablet 1) is remote in Europe (distance = 20)
        group.tablets[0].distance = 20;
        // Replica (tablet 2) is local in US (distance = 1)
        group.tablets[1].distance = 1;

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

        // When prefer_leader = true, the leader (even if remote at distance 20) is selected
        // to avoid forwarding hops for write transactions.
        let selected_leader = cache
            .select_tablet(&hit, true)
            .expect("should select remote leader");
        assert_eq!(
            selected_leader.tablet_uid, 1,
            "must prefer leader (UID 1) when prefer_leader is true"
        );

        // When prefer_leader = false, the local replica tier (distance 1) is selected.
        let selected_replica = cache
            .select_tablet(&hit, false)
            .expect("should select local replica");
        assert_eq!(
            selected_replica.tablet_uid, 2,
            "must prefer local replica (UID 2) when prefer_leader is false"
        );
    }

    #[test]
    fn parse_leader_index_out_of_bounds() {
        // leader_index = 5 when tablets count = 2
        let group_out_of_bounds = CachedGroup::from_proto(make_group(1, "1", 5));
        assert!(!group_out_of_bounds.has_leader());
        assert!(group_out_of_bounds.leader().is_none());
        assert!(group_out_of_bounds.local_leader().is_none());
    }

    #[test]
    fn eligible_tablets_prefer_leader_returns_leader_even_when_remote() {
        let mut proto = make_group(1, "1", 0);
        proto.tablets[0].distance = 20; // Remote leader
        proto.tablets[1].distance = 1; // Local replica
        let group = CachedGroup::from_proto(proto);

        // When prefer_leader is true, eligible_tablets returns the leader directly
        let eligible = group.eligible_tablets(true);
        assert_eq!(eligible.len(), 1);
        assert_eq!(eligible[0].tablet_uid, 1);

        // When prefer_leader is false, eligible_tablets returns the local replica
        let eligible_read = group.eligible_tablets(false);
        assert_eq!(eligible_read.len(), 1);
        assert_eq!(eligible_read[0].tablet_uid, 2);
    }

    #[test]
    fn compute_eligible_replica_indices_clears_prior_remote_when_local_discovered() {
        let mut proto = make_group(1, "1", -1);
        proto.tablets.clear();
        proto.tablets.push(Tablet {
            tablet_uid: 1,
            server_address: "localhost:8001".to_string(),
            location: "us-central1".to_string(),
            role: Role::ReadWrite,
            incarnation: Bytes::from_static(b"1"),
            distance: 10, // Remote seen first
            skip: false,
            _unknown_fields: Default::default(),
        });
        proto.tablets.push(Tablet {
            tablet_uid: 2,
            server_address: "localhost:8002".to_string(),
            location: "us-central1".to_string(),
            role: Role::ReadWrite,
            incarnation: Bytes::from_static(b"1"),
            distance: 3, // Local seen second (should clear remote)
            skip: false,
            _unknown_fields: Default::default(),
        });
        proto.tablets.push(Tablet {
            tablet_uid: 3,
            server_address: "localhost:8003".to_string(),
            location: "us-central1".to_string(),
            role: Role::ReadWrite,
            incarnation: Bytes::from_static(b"1"),
            distance: 3, // Local tied with tablet 2
            skip: false,
            _unknown_fields: Default::default(),
        });
        proto.tablets.push(Tablet {
            tablet_uid: 4,
            server_address: "localhost:8004".to_string(),
            location: "us-central1".to_string(),
            role: Role::ReadWrite,
            incarnation: Bytes::from_static(b"1"),
            distance: 1, // Strictly lower local distance (should clear tablets 2 and 3)
            skip: false,
            _unknown_fields: Default::default(),
        });
        proto.tablets.push(Tablet {
            tablet_uid: 5,
            server_address: "localhost:8005".to_string(),
            location: "us-central1".to_string(),
            role: Role::ReadWrite,
            incarnation: Bytes::from_static(b"1"),
            distance: 1, // Skipped replica with distance 1 (must be ignored)
            skip: true,
            _unknown_fields: Default::default(),
        });
        proto.tablets.push(Tablet {
            tablet_uid: 6,
            server_address: "".to_string(), // Empty address (must be ignored)
            location: "us-central1".to_string(),
            role: Role::ReadWrite,
            incarnation: Bytes::from_static(b"1"),
            distance: 1,
            skip: false,
            _unknown_fields: Default::default(),
        });

        let group = CachedGroup::from_proto(proto);
        let eligible = group.eligible_tablets(false);
        assert_eq!(eligible.len(), 1, "only tablet 4 should be eligible");
        assert_eq!(eligible[0].tablet_uid, 4);
    }

    #[test]
    fn get_eligible_tablets_and_select_tablet_missing_group_and_empty_tablets() {
        let cache = KeyRangeCache::new();
        let range = CachedRange::new(
            Bytes::from_static(b"a"),
            Bytes::from_static(b"z"),
            999, // Non-existent group UID
            1,
            Bytes::from_static(b"1"),
            0,
        );

        assert!(cache.get_eligible_tablets(&range, true).is_none());
        assert!(cache.get_eligible_tablets(&range, false).is_none());
        assert!(cache.select_tablet(&range, true).is_none());
        assert!(cache.select_tablet(&range, false).is_none());

        // Group exists but has zero routable tablets
        let mut group = make_group(100, "1", -1);
        group.tablets[0].skip = true;
        group.tablets[1].server_address.clear();
        let update = CacheUpdate {
            database_id: 1,
            range: vec![make_range("a", "z", 100, "1")],
            group: vec![group],
            key_recipes: None,
            _unknown_fields: Default::default(),
        };
        cache.add_ranges(&update);

        let hit = cache
            .find_range(b"m", b"", RangeMode::CoveringSplit)
            .expect("range hit");
        assert!(cache.get_eligible_tablets(&hit, true).is_none());
        assert!(cache.get_eligible_tablets(&hit, false).is_none());
        assert!(cache.select_tablet(&hit, true).is_none());
        assert!(cache.select_tablet(&hit, false).is_none());
    }

    #[test]
    fn select_tablet_random_sampling_over_tied_replicas() {
        let cache = KeyRangeCache::new();
        let mut group = make_group(1, "1", -1);
        // Add 3 replicas all with same distance
        group.tablets[0].distance = 1;
        group.tablets[1].distance = 1;
        group.tablets.push(Tablet {
            tablet_uid: 3,
            server_address: "localhost:8003".to_string(),
            location: "us-central1".to_string(),
            role: Role::ReadWrite,
            incarnation: Bytes::from_static(b"1"),
            distance: 1,
            skip: false,
            _unknown_fields: Default::default(),
        });

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

        let eligible = cache
            .get_eligible_tablets(&hit, false)
            .expect("eligible replicas");
        assert_eq!(eligible.len(), 3);
        let candidate_uids: HashSet<u64> =
            eligible.iter().map(|tablet| tablet.tablet_uid).collect();
        assert_eq!(candidate_uids, HashSet::from([1, 2, 3]));

        let selected = cache
            .select_tablet(&hit, false)
            .expect("should select tablet");
        assert!(
            candidate_uids.contains(&selected.tablet_uid),
            "selected tablet UID {} must be among candidate pool {:?}",
            selected.tablet_uid,
            candidate_uids
        );
    }
}
