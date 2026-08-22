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

//! Thread-safe cache for Spanner key recipes.
//!
//! Stores table, index, and query key recipes returned by Spanner in [`ResultSetMetadata`](crate::model::ResultSetMetadata)
//! so that subsequent read and query RPCs can encode binary routing keys locally without relying on the
//! Spanner Frontend (SpanFE) proxy to resolve tablet shard boundaries.

// TODO(#6236): Remove dead_code allowance once request routing interceptors utilize KeyRecipeCache in subsequent PRs.
#![allow(dead_code)]

use crate::model::key_recipe::Target;
use crate::model::{KeyRecipe, RecipeList};
use std::collections::hash_map::Entry;
use std::collections::{HashMap, VecDeque};
use std::fmt::{self, Debug, Formatter};
use std::mem::take;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

/// Default maximum number of SQL query recipes cached simultaneously.
///
/// Prevents unbounded memory growth in applications executing many dynamic,
/// parameter-varying, or ad-hoc SQL queries.
pub(crate) const DEFAULT_QUERY_RECIPE_CACHE_CAPACITY: usize = 2_000;

/// A concurrent, thread-safe cache for Spanner key recipes.
///
/// Backed by [`RwLock`] around separate hash maps for tables, indexes, and queries, enabling
/// zero-allocation `&str` lookups, non-blocking concurrent reads across Tokio tasks, and
/// scan-resistant CLOCK (Second-Chance) eviction for query recipes.
#[derive(Default)]
pub(crate) struct KeyRecipeCache {
    store: RwLock<RecipeStore>,
}

impl Debug for KeyRecipeCache {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        let count = self.len();
        formatter
            .debug_struct("KeyRecipeCache")
            .field("entry_count", &count)
            .finish_non_exhaustive()
    }
}

impl KeyRecipeCache {
    /// Creates a new, empty [`KeyRecipeCache`] with the default query recipe capacity (`2,000`).
    pub(crate) fn new() -> Self {
        Self {
            store: RwLock::new(RecipeStore::default()),
        }
    }

    /// Creates a new, empty [`KeyRecipeCache`] with the specified query recipe capacity limit.
    pub(crate) fn with_query_capacity(query_capacity: usize) -> Self {
        Self {
            store: RwLock::new(RecipeStore::with_query_capacity(query_capacity)),
        }
    }

    fn read_store(&self) -> RwLockReadGuard<'_, RecipeStore> {
        self.store
            .read()
            .expect("key recipe cache read lock poisoned")
    }

    fn write_store(&self) -> RwLockWriteGuard<'_, RecipeStore> {
        self.store
            .write()
            .expect("key recipe cache write lock poisoned")
    }

    /// Returns the cached recipe for a given database table name (`Read` RPC), if present.
    ///
    /// Accepts a `&str` slice with zero heap allocations on lookup.
    /// Returning `Option<Arc<KeyRecipe>>` performs an inexpensive atomic reference count increment
    /// (`Arc::clone`), which is necessary to release the underlying [`RwLock`] read guard immediately.
    ///
    /// Note on naming: Intentionally uses the `get_` prefix because fetching an item by key from a cache
    /// is a lookup operation, consistent with `HashMap::get`, `ConnectionCache::get_if_present`, and
    /// `KeyRangeCache::get_group`.
    pub(crate) fn get_table_recipe(&self, table_name: &str) -> Option<Arc<KeyRecipe>> {
        self.read_store().tables.get(table_name).map(Arc::clone)
    }

    /// Returns the cached recipe for a given database index name (`Read` RPC on an index), if present.
    ///
    /// Accepts a `&str` slice with zero heap allocations on lookup.
    ///
    /// Note on naming: Intentionally uses the `get_` prefix because fetching an item by key from a cache
    /// is a lookup operation, consistent with `HashMap::get`, `ConnectionCache::get_if_present`, and
    /// `KeyRangeCache::get_group`.
    pub(crate) fn get_index_recipe(&self, index_name: &str) -> Option<Arc<KeyRecipe>> {
        self.read_store().indexes.get(index_name).map(Arc::clone)
    }

    /// Returns the cached recipe for a given SQL query operation UID, if present.
    ///
    /// Marks the entry as recently referenced using a lock-free atomic store (`Ordering::Relaxed`),
    /// granting it a second chance during CLOCK cache eviction without acquiring an exclusive write lock.
    ///
    /// Note on naming: Intentionally uses the `get_` prefix because fetching an item by key from a cache
    /// is a lookup operation, consistent with `HashMap::get`, `ConnectionCache::get_if_present`, and
    /// `KeyRangeCache::get_group`.
    pub(crate) fn get_query_recipe(&self, operation_uid: u64) -> Option<Arc<KeyRecipe>> {
        let guard = self.read_store();
        let entry = guard.queries.get(&operation_uid)?;
        entry.referenced.store(true, Ordering::Relaxed);
        Some(Arc::clone(&entry.recipe))
    }

    /// Inserts a [`KeyRecipe`] into the cache.
    ///
    /// # Concurrency Optimization
    /// Clones `recipe.target` (`String::clone` for table/index names) and wraps `recipe`
    /// in an [`Arc`] before acquiring the write lock (`self.store.write()`). This ensures that all
    /// heap allocations occur outside the critical section, reducing lock hold duration to a pure
    /// $O(1)$ hashmap insertion.
    ///
    /// The lock guard is explicitly dropped before any displaced previous recipe is deallocated,
    /// ensuring heap deallocations also occur outside the critical section.
    ///
    /// Returns `true` if the recipe contained a target and was stored in the cache;
    /// returns `false` if `recipe.target` was `None`.
    pub(crate) fn insert(&self, recipe: KeyRecipe) -> bool {
        let Some(target) = recipe.target.clone() else {
            return false;
        };
        let recipe_arc = Arc::new(recipe);
        let mut guard = self.write_store();
        let _old = match target {
            Target::TableName(name) => guard.tables.insert(name, recipe_arc),
            Target::IndexName(name) => guard.indexes.insert(name, recipe_arc),
            Target::OperationUid(operation_uid) => guard.insert_query(operation_uid, recipe_arc),
        };
        // Explicitly drop the lock guard before `_old` is dropped so that if an existing
        // recipe with reference count 1 was overwritten, its heap deallocation occurs
        // outside the critical section.
        drop(guard);
        true
    }

    /// Ingests a slice of [`KeyRecipe`]s into the cache in a single batch,
    /// acquiring the write lock only once.
    pub(crate) fn insert_batch(&self, recipes: &[KeyRecipe]) {
        if recipes.is_empty() {
            return;
        }
        // Prepare target and Arc outside the write lock to minimize lock hold duration.
        let mut prepared = Vec::with_capacity(recipes.len());
        for recipe in recipes {
            if let Some(target) = recipe.target.clone() {
                prepared.push((target, Arc::new(recipe.clone())));
            }
        }
        if prepared.is_empty() {
            return;
        }
        let mut guard = self.write_store();
        for (target, recipe_arc) in prepared {
            match target {
                Target::TableName(name) => {
                    guard.tables.insert(name, recipe_arc);
                }
                Target::IndexName(name) => {
                    guard.indexes.insert(name, recipe_arc);
                }
                Target::OperationUid(operation_uid) => {
                    guard.insert_query(operation_uid, recipe_arc);
                }
            }
        }
    }

    /// Ingests all recipes from a [`RecipeList`] returned in [`CacheUpdate`](crate::model::CacheUpdate).
    ///
    /// Consumes the [`RecipeList`] by value to move recipes directly into internal storage
    /// without cloning.
    pub(crate) fn update_from_recipe_list(&self, recipe_list: RecipeList) {
        if recipe_list.recipe.is_empty() {
            return;
        }
        let mut prepared = Vec::with_capacity(recipe_list.recipe.len());
        for recipe in recipe_list.recipe {
            if let Some(target) = recipe.target.clone() {
                prepared.push((target, Arc::new(recipe)));
            }
        }
        if prepared.is_empty() {
            return;
        }
        let mut guard = self.write_store();
        for (target, recipe_arc) in prepared {
            match target {
                Target::TableName(name) => {
                    guard.tables.insert(name, recipe_arc);
                }
                Target::IndexName(name) => {
                    guard.indexes.insert(name, recipe_arc);
                }
                Target::OperationUid(operation_uid) => {
                    guard.insert_query(operation_uid, recipe_arc);
                }
            }
        }
    }

    /// Clears all entries from the cache while preserving configured capacity limits.
    pub(crate) fn clear(&self) {
        let (old_tables, old_indexes, old_queries) = {
            let mut guard = self.write_store();
            let old_tables = take(&mut guard.tables);
            let old_indexes = take(&mut guard.indexes);
            let old_queries = take(&mut guard.queries);
            guard.query_order.clear();
            (old_tables, old_indexes, old_queries)
        };
        // Drop old collections (and cached Arc<KeyRecipe> entries) outside the write lock.
        drop(old_tables);
        drop(old_indexes);
        drop(old_queries);
    }

    /// Returns the total number of recipes stored in the cache.
    pub(crate) fn len(&self) -> usize {
        self.read_store().len()
    }

    /// Returns `true` if the cache is empty.
    pub(crate) fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Entry for a cached query recipe, pairing the recipe pointer with an atomic reference flag
/// for CLOCK (Second-Chance) cache eviction.
struct QueryRecipeEntry {
    recipe: Arc<KeyRecipe>,
    referenced: AtomicBool,
}

/// Internal storage for key recipes, separated by target type to allow zero-allocation
/// `&str` lookups for tables and indexes, and bounded CLOCK (Second-Chance) storage for query recipes.
struct RecipeStore {
    tables: HashMap<String, Arc<KeyRecipe>>,
    indexes: HashMap<String, Arc<KeyRecipe>>,
    queries: HashMap<u64, QueryRecipeEntry>,
    query_order: VecDeque<u64>,
    query_capacity: usize,
}

impl Default for RecipeStore {
    fn default() -> Self {
        Self::with_query_capacity(DEFAULT_QUERY_RECIPE_CACHE_CAPACITY)
    }
}

impl RecipeStore {
    fn with_query_capacity(query_capacity: usize) -> Self {
        Self {
            tables: HashMap::new(),
            indexes: HashMap::new(),
            queries: HashMap::new(),
            query_order: VecDeque::new(),
            query_capacity,
        }
    }

    fn len(&self) -> usize {
        self.tables.len() + self.indexes.len() + self.queries.len()
    }

    fn insert_query(
        &mut self,
        operation_uid: u64,
        recipe: Arc<KeyRecipe>,
    ) -> Option<Arc<KeyRecipe>> {
        if self.query_capacity == 0 {
            return None;
        }
        match self.queries.entry(operation_uid) {
            Entry::Occupied(mut occupied) => {
                let entry = occupied.get_mut();
                let previous_recipe = Arc::clone(&entry.recipe);
                entry.recipe = recipe;
                // Overwriting an existing entry marks it as recently referenced, granting it a
                // second chance in CLOCK eviction so freshly updated recipes are not prematurely evicted.
                entry.referenced.store(true, Ordering::Relaxed);
                Some(previous_recipe)
            }
            Entry::Vacant(vacant) => {
                vacant.insert(QueryRecipeEntry {
                    recipe,
                    referenced: AtomicBool::new(false),
                });
                self.query_order.push_back(operation_uid);
                self.evict_excess_queries();
                None
            }
        }
    }

    /// Evicts excess query recipes beyond `query_capacity` using the CLOCK (Second-Chance) algorithm.
    fn evict_excess_queries(&mut self) {
        while self.queries.len() > self.query_capacity {
            let Some(candidate_uid) = self.query_order.pop_front() else {
                break;
            };
            let Some(entry) = self.queries.get(&candidate_uid) else {
                // Stale queue entry (already removed/overwritten): discard and continue.
                continue;
            };
            if entry.referenced.swap(false, Ordering::Relaxed) {
                // Entry was referenced since last inspection: grant a second chance.
                self.query_order.push_back(candidate_uid);
            } else {
                // Entry was not referenced: evict from cache.
                self.queries.remove(&candidate_uid);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn key_recipe_cache_implements_send_sync_debug() {
        static_assertions::assert_impl_all!(KeyRecipeCache: Send, Sync, Debug);
    }

    #[test]
    fn cache_new_is_empty() {
        let cache = KeyRecipeCache::new();
        assert!(cache.is_empty(), "new cache must be empty");
        assert_eq!(cache.len(), 0, "new cache length must be zero");
    }

    #[test]
    fn insert_without_target_returns_false() {
        let cache = KeyRecipeCache::new();
        let recipe = KeyRecipe::new();
        assert!(
            !cache.insert(recipe),
            "inserting recipe without target must return false"
        );
        assert!(cache.is_empty(), "cache must remain empty");
    }

    #[test]
    fn insert_and_get_table_recipe() {
        let cache = KeyRecipeCache::new();
        let recipe = KeyRecipe::new().set_table_name("Users");
        assert!(cache.insert(recipe), "insert must return true");
        assert_eq!(cache.len(), 1, "cache length must be 1");

        let cached = cache
            .get_table_recipe("Users")
            .expect("recipe should be cached");
        assert_eq!(cached.table_name().expect("table name present"), "Users");
    }

    #[test]
    fn insert_and_get_index_recipe() {
        let cache = KeyRecipeCache::new();
        let recipe = KeyRecipe::new().set_index_name("UsersByEmail");
        assert!(cache.insert(recipe), "insert must return true");
        assert_eq!(cache.len(), 1, "cache length must be 1");

        let cached = cache
            .get_index_recipe("UsersByEmail")
            .expect("recipe should be cached");
        assert_eq!(
            cached.index_name().expect("index name present"),
            "UsersByEmail"
        );
    }

    #[test]
    fn insert_and_get_query_recipe() {
        let cache = KeyRecipeCache::new();
        let recipe = KeyRecipe::new().set_target(Target::from_operation_uid(12345u64));
        assert!(cache.insert(recipe), "insert must return true");
        assert_eq!(cache.len(), 1, "cache length must be 1");

        let cached = cache
            .get_query_recipe(12345)
            .expect("recipe should be cached");
        assert_eq!(
            cached.operation_uid().expect("operation uid present"),
            &12345
        );
    }

    #[test]
    fn get_non_existent_recipes_returns_none() {
        let cache = KeyRecipeCache::new();
        assert!(cache.get_table_recipe("NonExistent").is_none());
        assert!(cache.get_index_recipe("NonExistent").is_none());
        assert!(cache.get_query_recipe(9999).is_none());
    }

    #[test]
    fn insert_overwrites_existing_recipe() {
        let cache = KeyRecipeCache::new();
        let first_recipe = KeyRecipe::new().set_table_name("Orders");
        let second_recipe = KeyRecipe::new().set_table_name("Orders");
        assert!(cache.insert(first_recipe), "first insert must return true");
        assert!(
            cache.insert(second_recipe),
            "second insert must return true"
        );
        assert_eq!(cache.len(), 1, "cache length must remain 1 after overwrite");
    }

    #[test]
    fn clear_removes_all_recipes() {
        let cache = KeyRecipeCache::new();
        assert!(cache.insert(KeyRecipe::new().set_table_name("Users")));
        assert!(cache.insert(KeyRecipe::new().set_index_name("IndexA")));
        assert_eq!(cache.len(), 2, "cache length must be 2");

        cache.clear();
        assert!(cache.is_empty(), "cache must be empty after clear");
        assert_eq!(cache.len(), 0, "cache length must be zero after clear");
    }

    #[test]
    fn concurrent_read_write_access() {
        let cache = Arc::new(KeyRecipeCache::new());
        let mut handles = Vec::new();

        for i in 0..10 {
            let cache_clone = Arc::clone(&cache);
            handles.push(thread::spawn(move || {
                let table_name = format!("Table_{i}");
                cache_clone.insert(KeyRecipe::new().set_table_name(&table_name));
                let found = cache_clone.get_table_recipe(&table_name);
                assert!(
                    found.is_some(),
                    "thread must observe its own inserted recipe"
                );
            }));
        }

        for handle in handles {
            handle.join().expect("thread should finish cleanly");
        }

        assert_eq!(cache.len(), 10, "all 10 concurrent inserts must be stored");
    }

    #[test]
    fn cache_debug_formatting() {
        let cache = KeyRecipeCache::new();
        assert!(
            format!("{cache:?}").contains("entry_count: 0"),
            "debug format must show zero entry count for empty cache"
        );
        cache.insert(KeyRecipe::new().set_table_name("Users"));
        assert!(
            format!("{cache:?}").contains("entry_count: 1"),
            "debug format must show updated entry count"
        );
    }

    #[test]
    fn insert_batch_empty_or_no_targets() {
        let cache = KeyRecipeCache::new();
        cache.insert_batch(&[]);
        assert!(
            cache.is_empty(),
            "cache must remain empty after empty batch"
        );

        let untargeted_recipe = KeyRecipe::new();
        cache.insert_batch(&[untargeted_recipe]);
        assert!(
            cache.is_empty(),
            "cache must remain empty when batch contains only untargeted recipes"
        );
    }

    #[test]
    fn insert_batch_all_target_types() {
        let cache = KeyRecipeCache::new();
        let table_recipe = KeyRecipe::new().set_table_name("Albums");
        let index_recipe = KeyRecipe::new().set_index_name("AlbumsByArtist");
        let query_recipe = KeyRecipe::new().set_operation_uid(42u64);
        let untargeted_recipe = KeyRecipe::new();

        cache.insert_batch(&[table_recipe, index_recipe, query_recipe, untargeted_recipe]);

        assert_eq!(
            cache.len(),
            3,
            "cache length must be 3 for the 3 targeted recipes"
        );
        assert!(
            cache.get_table_recipe("Albums").is_some(),
            "table recipe must be retrieved"
        );
        assert!(
            cache.get_index_recipe("AlbumsByArtist").is_some(),
            "index recipe must be retrieved"
        );
        assert!(
            cache.get_query_recipe(42).is_some(),
            "query recipe must be retrieved"
        );
    }

    #[test]
    fn update_from_recipe_list_inserts_all_recipes() {
        let cache = KeyRecipeCache::new();
        let recipe_list = RecipeList::new().set_recipe(vec![
            KeyRecipe::new().set_table_name("Albums"),
            KeyRecipe::new().set_index_name("AlbumsBySinger"),
            KeyRecipe::new().set_operation_uid(12345u64),
        ]);

        cache.update_from_recipe_list(recipe_list);
        assert_eq!(cache.len(), 3, "all 3 recipes in list should be inserted");
        assert!(
            cache.get_table_recipe("Albums").is_some(),
            "table recipe must be present"
        );
        assert!(
            cache.get_index_recipe("AlbumsBySinger").is_some(),
            "index recipe must be present"
        );
        assert!(
            cache.get_query_recipe(12345u64).is_some(),
            "query recipe must be present"
        );
    }

    #[test]
    fn query_recipe_cache_is_bounded_on_insert() {
        let cache = KeyRecipeCache::with_query_capacity(3);

        // Insert 5 queries without accessing them (unreferenced)
        for i in 1..=5 {
            let recipe = KeyRecipe::new().set_operation_uid(i as u64);
            assert!(cache.insert(recipe), "insert must succeed");
        }

        // Must cap at 3 entries
        assert_eq!(
            cache.len(),
            3,
            "cache length must not exceed configured capacity 3"
        );

        // Unreferenced oldest entries 1 and 2 must have been evicted
        assert!(
            cache.get_query_recipe(1).is_none(),
            "query 1 must be evicted"
        );
        assert!(
            cache.get_query_recipe(2).is_none(),
            "query 2 must be evicted"
        );

        // Latest entries 3, 4, 5 must remain cached
        assert!(
            cache.get_query_recipe(3).is_some(),
            "query 3 must remain cached"
        );
        assert!(
            cache.get_query_recipe(4).is_some(),
            "query 4 must remain cached"
        );
        assert!(
            cache.get_query_recipe(5).is_some(),
            "query 5 must remain cached"
        );
    }

    #[test]
    fn query_recipe_cache_is_bounded_on_insert_batch() {
        let cache = KeyRecipeCache::with_query_capacity(2);
        let batch = vec![
            KeyRecipe::new().set_operation_uid(100u64),
            KeyRecipe::new().set_operation_uid(200u64),
            KeyRecipe::new().set_operation_uid(300u64),
            KeyRecipe::new().set_table_name("UnboundedTable"),
        ];

        cache.insert_batch(&batch);

        // Table recipe is stored, but queries are bounded to 2 (100 is evicted, 200 and 300 remain)
        assert_eq!(
            cache.len(),
            3,
            "cache must have 1 table and 2 query recipes"
        );
        assert!(
            cache.get_table_recipe("UnboundedTable").is_some(),
            "table recipe must not be subject to query capacity limits"
        );
        assert!(
            cache.get_query_recipe(100).is_none(),
            "query 100 must be evicted"
        );
        assert!(
            cache.get_query_recipe(200).is_some(),
            "query 200 must remain cached"
        );
        assert!(
            cache.get_query_recipe(300).is_some(),
            "query 300 must remain cached"
        );
    }

    #[test]
    fn query_recipe_cache_overwrite_does_not_evict() {
        let cache = KeyRecipeCache::with_query_capacity(2);
        assert!(cache.insert(KeyRecipe::new().set_operation_uid(1u64)));
        assert!(cache.insert(KeyRecipe::new().set_operation_uid(2u64)));
        assert_eq!(cache.len(), 2);

        // Overwrite query 1
        assert!(cache.insert(KeyRecipe::new().set_operation_uid(1u64)));
        assert_eq!(
            cache.len(),
            2,
            "overwriting existing query must not change count"
        );

        // Both queries 1 and 2 must still be present
        assert!(cache.get_query_recipe(1).is_some());
        assert!(cache.get_query_recipe(2).is_some());
    }

    #[test]
    fn query_recipe_cache_overwrite_marks_referenced_for_second_chance() {
        let cache = KeyRecipeCache::with_query_capacity(2);

        // Insert query 1 (head of FIFO queue) and query 2 (tail of FIFO queue) without reading them
        assert!(cache.insert(KeyRecipe::new().set_operation_uid(1u64)));
        assert!(cache.insert(KeyRecipe::new().set_operation_uid(2u64)));

        // Overwrite query 1 with an updated recipe (without calling get_query_recipe)
        let updated_recipe = KeyRecipe::new()
            .set_operation_uid(1u64)
            .set_part(vec![crate::model::key_recipe::Part::new()]);
        assert!(cache.insert(updated_recipe));

        // Insert query 3 to trigger eviction under capacity limit of 2
        assert!(cache.insert(KeyRecipe::new().set_operation_uid(3u64)));

        assert_eq!(cache.len(), 2, "cache length must remain at capacity 2");

        // Query 1 was freshly overwritten, so it must have received a second chance and survived
        let cached_query_1 = cache
            .get_query_recipe(1)
            .expect("overwritten query 1 must survive eviction via second chance");
        assert_eq!(
            cached_query_1.part.len(),
            1,
            "updated parts must be present"
        );

        // Query 2 was older and unreferenced, so it must have been evicted
        assert!(
            cache.get_query_recipe(2).is_none(),
            "unreferenced query 2 must be evicted"
        );

        // Query 3 must be cached
        assert!(
            cache.get_query_recipe(3).is_some(),
            "new query 3 must be cached"
        );
    }

    #[test]
    fn query_recipe_cache_zero_capacity() {
        let cache = KeyRecipeCache::with_query_capacity(0);
        let query_recipe = KeyRecipe::new().set_operation_uid(1u64);
        assert!(
            cache.insert(query_recipe),
            "insert returns true for valid target"
        );
        assert_eq!(
            cache.len(),
            0,
            "cache with zero query capacity must store 0 query recipes"
        );
        assert!(cache.get_query_recipe(1).is_none());

        // Tables and indexes can still be stored
        assert!(cache.insert(KeyRecipe::new().set_table_name("Users")));
        assert_eq!(cache.len(), 1);
        assert!(cache.get_table_recipe("Users").is_some());
    }

    #[test]
    fn clear_preserves_query_capacity() {
        let cache = KeyRecipeCache::with_query_capacity(2);
        assert!(cache.insert(KeyRecipe::new().set_operation_uid(1u64)));
        assert!(cache.insert(KeyRecipe::new().set_operation_uid(2u64)));
        assert_eq!(cache.len(), 2);

        cache.clear();
        assert!(cache.is_empty());

        // Insert 3 new queries, capacity limit of 2 must still be enforced
        assert!(cache.insert(KeyRecipe::new().set_operation_uid(10u64)));
        assert!(cache.insert(KeyRecipe::new().set_operation_uid(20u64)));
        assert!(cache.insert(KeyRecipe::new().set_operation_uid(30u64)));

        assert_eq!(
            cache.len(),
            2,
            "capacity limit of 2 must be preserved after clear"
        );
        assert!(cache.get_query_recipe(10).is_none());
        assert!(cache.get_query_recipe(20).is_some());
        assert!(cache.get_query_recipe(30).is_some());
    }

    #[test]
    fn query_recipe_cache_clock_second_chance_eviction() {
        let cache = KeyRecipeCache::with_query_capacity(2);

        // Insert query 1 and query 2
        assert!(cache.insert(KeyRecipe::new().set_operation_uid(1u64)));
        assert!(cache.insert(KeyRecipe::new().set_operation_uid(2u64)));

        // Read query 1 (marks query 1 as referenced)
        let query_1 = cache.get_query_recipe(1);
        assert!(query_1.is_some(), "query 1 must be present");

        // Query 2 is NOT accessed (referenced = false)

        // Insert query 3 (triggers eviction)
        assert!(cache.insert(KeyRecipe::new().set_operation_uid(3u64)));

        // Cache must still hold at most 2 queries
        assert_eq!(cache.len(), 2, "cache length must remain at capacity 2");

        // Query 1 received a second chance, so it must still be cached
        assert!(
            cache.get_query_recipe(1).is_some(),
            "query 1 received a second chance and must remain cached"
        );

        // Query 2 was not referenced, so it must have been evicted
        assert!(
            cache.get_query_recipe(2).is_none(),
            "query 2 was not referenced and must be evicted"
        );

        // Query 3 must be cached
        assert!(
            cache.get_query_recipe(3).is_some(),
            "newly inserted query 3 must be cached"
        );
    }

    #[test]
    fn query_recipe_cache_scan_resistance_against_ad_hoc_burst() {
        let cache = KeyRecipeCache::with_query_capacity(3);

        // Insert hot queries 1 and 2
        assert!(cache.insert(KeyRecipe::new().set_operation_uid(1u64)));
        assert!(cache.insert(KeyRecipe::new().set_operation_uid(2u64)));

        // Repeatedly hit hot queries 1 and 2
        assert!(cache.get_query_recipe(1).is_some());
        assert!(cache.get_query_recipe(2).is_some());

        // Simulate a burst of 10 ad-hoc / one-off queries (100..110)
        for i in 100u64..110u64 {
            assert!(cache.insert(KeyRecipe::new().set_operation_uid(i)));
            // Note: Ad-hoc queries are never read, so their referenced bit remains false
            // Keep refreshing hot queries 1 and 2 during the workload
            assert!(cache.get_query_recipe(1).is_some());
            assert!(cache.get_query_recipe(2).is_some());
        }

        // Cache must not exceed capacity 3
        assert_eq!(cache.len(), 3, "cache must be capped at 3 entries");

        // Hot queries 1 and 2 must survive the ad-hoc scan
        assert!(
            cache.get_query_recipe(1).is_some(),
            "hot query 1 must survive ad-hoc query flood"
        );
        assert!(
            cache.get_query_recipe(2).is_some(),
            "hot query 2 must survive ad-hoc query flood"
        );

        // Only the latest ad-hoc query (109) should occupy the remaining slot
        assert!(
            cache.get_query_recipe(109).is_some(),
            "latest ad-hoc query 109 should be present"
        );
        assert!(
            cache.get_query_recipe(100).is_none(),
            "earlier ad-hoc query 100 must have been evicted"
        );
    }
}
