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
use crate::routing::clock_cache::{ClockEntry, ClockStore};
use bytes::Bytes;
use std::collections::HashMap;
use std::fmt::{self, Debug, Formatter};
use std::mem::take;
use std::sync::atomic::{AtomicU64, Ordering};
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
pub(crate) struct KeyRecipeCache {
    store: RwLock<RecipeStore>,
    next_operation_uid: AtomicU64,
}

impl Default for KeyRecipeCache {
    fn default() -> Self {
        Self::new()
    }
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
        Self::with_query_capacity(DEFAULT_QUERY_RECIPE_CACHE_CAPACITY)
    }

    /// Creates a new, empty [`KeyRecipeCache`] with the specified query recipe capacity limit.
    pub(crate) fn with_query_capacity(query_capacity: usize) -> Self {
        Self {
            store: RwLock::new(RecipeStore::with_query_capacity(query_capacity)),
            next_operation_uid: AtomicU64::new(1),
        }
    }

    /// Generates and returns a monotonically increasing operation UID for SQL query operations and prepared operations.
    pub(crate) fn next_operation_uid(&self) -> u64 {
        self.next_operation_uid.fetch_add(1, Ordering::Relaxed)
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
        self.read_store().queries.get(&operation_uid)
    }

    /// Inserts a [`KeyRecipe`] into the cache.
    ///
    /// # Concurrency Optimization
    /// Clones `recipe.target` (`String::clone` for table/index names) and wraps `recipe`
    /// in an [`Arc`] before acquiring the write lock (`self.store.write()`). This ensures that all
    /// heap allocations occur outside the critical section, reducing lock hold duration to a pure
    /// $O(1)$ hashmap insertion.
    ///
    /// The lock guard is explicitly dropped before any displaced overwritten recipe is deallocated,
    /// ensuring heap deallocations for overwritten entries occur outside the critical section.
    /// (Evicted entries from bounded query store capacity limits are dropped on removal under the write guard,
    /// which is an $O(1)$ atomic reference counter decrement).
    ///
    /// Returns `true` if the recipe contained a target and was stored in the cache;
    /// returns `false` if `recipe.target` was `None`.
    pub(crate) fn insert(&self, recipe: KeyRecipe) -> bool {
        let Some(target) = recipe.target.clone() else {
            return false;
        };
        let recipe_arc = Arc::new(recipe);
        let mut guard = self.write_store();
        let _previous_recipe = match target {
            Target::TableName(name) => guard.tables.insert(name, recipe_arc),
            Target::IndexName(name) => guard.indexes.insert(name, recipe_arc),
            Target::OperationUid(operation_uid) => guard.queries.insert(operation_uid, recipe_arc),
        };
        // Explicitly drop the lock guard before `_previous_recipe` is dropped so that if an existing
        // recipe with reference count 1 was overwritten, its heap deallocation occurs
        // outside the critical section.
        drop(guard);
        true
    }

    /// Ingests an iterator of [`KeyRecipe`]s into the cache in a single batch,
    /// acquiring the write lock only once.
    pub(crate) fn insert_batch<I>(&self, recipes: I)
    where
        I: IntoIterator<Item = KeyRecipe>,
    {
        let iterator = recipes.into_iter();
        let (lower_bound, _) = iterator.size_hint();
        // Prepare target and Arc outside the write lock to minimize lock hold duration.
        let mut prepared = Vec::with_capacity(lower_bound);
        for recipe in iterator {
            if let Some(target) = recipe.target.clone() {
                prepared.push((target, Arc::new(recipe)));
            }
        }
        if prepared.is_empty() {
            return;
        }
        let mut displaced_recipes = Vec::new();
        let mut guard = self.write_store();
        for (target, recipe_arc) in prepared {
            let previous_recipe = match target {
                Target::TableName(name) => guard.tables.insert(name, recipe_arc),
                Target::IndexName(name) => guard.indexes.insert(name, recipe_arc),
                Target::OperationUid(operation_uid) => {
                    guard.queries.insert(operation_uid, recipe_arc)
                }
            };
            if let Some(displaced) = previous_recipe {
                displaced_recipes.push(displaced);
            }
        }
        drop(guard);
        drop(displaced_recipes);
    }

    /// Ingests all recipes and schema generation from a [`RecipeList`] returned in [`CacheUpdate`](crate::model::CacheUpdate).
    ///
    /// # Schema Generation Invalidation & Ordering:
    /// - If `incoming.schema_generation < current.schema_generation`: drops the stale update immediately.
    /// - If `incoming.schema_generation > current.schema_generation`: updates schema generation and invalidates
    ///   all previously cached table, index, and query recipes from the older schema version.
    /// - If `incoming.schema_generation == current.schema_generation` (or initial generation): merges incoming recipes.
    pub(crate) fn update_from_recipe_list(&self, recipe_list: RecipeList) {
        let incoming_generation = recipe_list.schema_generation;
        if recipe_list.recipe.is_empty() && incoming_generation.is_empty() {
            return;
        }

        // Prepare targets and Arcs outside the write lock to minimize lock hold duration.
        let mut prepared = Vec::with_capacity(recipe_list.recipe.len());
        for recipe in recipe_list.recipe {
            if let Some(target) = recipe.target.clone() {
                prepared.push((target, Arc::new(recipe)));
            }
        }

        let mut guard = self.write_store();
        let _dropped_entries = match (!incoming_generation.is_empty(), &guard.schema_generation) {
            (true, Some(current_generation)) if incoming_generation < *current_generation => {
                // Stale update: drop entirely without modifying existing cache state.
                return;
            }
            (true, Some(current_generation)) if incoming_generation > *current_generation => {
                // Newer generation: invalidate all existing cached recipes and query recipes.
                Some(guard.invalidate_all(Some(incoming_generation)))
            }
            (true, None) => {
                // First schema generation observed: record it.
                guard.schema_generation = Some(incoming_generation);
                None
            }
            _ => None,
        };

        let mut displaced_recipes = Vec::new();
        for (target, recipe_arc) in prepared {
            let previous_recipe = match target {
                Target::TableName(name) => guard.tables.insert(name, recipe_arc),
                Target::IndexName(name) => guard.indexes.insert(name, recipe_arc),
                Target::OperationUid(operation_uid) => {
                    guard.queries.insert(operation_uid, recipe_arc)
                }
            };
            if let Some(displaced) = previous_recipe {
                displaced_recipes.push(displaced);
            }
        }

        // Release write lock before deallocating any old invalidated recipe collections or displaced recipes.
        drop(guard);
        drop(_dropped_entries);
        drop(displaced_recipes);
    }

    /// Returns the schema generation of the most recently ingested [`RecipeList`], if any.
    ///
    /// # Performance
    /// Cloning the returned [`Bytes`] handle is an $O(1)$ atomic reference counter increment on
    /// the shared underlying buffer without copying memory, allowing zero-copy sharing across
    /// the read lock boundary.
    pub(crate) fn schema_generation(&self) -> Option<Bytes> {
        self.read_store().schema_generation.clone()
    }

    /// Clears all entries from the cache while preserving configured capacity limits.
    ///
    /// Note: Intentionally retains `next_operation_uid` monotonically increasing to ensure
    /// operation UIDs remain globally unique across the client lifecycle, preventing collisions
    /// with in-flight asynchronous queries.
    pub(crate) fn clear(&self) {
        let old_entries = {
            let mut guard = self.write_store();
            guard.invalidate_all(None)
        };
        // Drop old collections (and cached Arc<KeyRecipe> entries) outside the write lock.
        drop(old_entries);
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

/// Container for invalidated cache collections, returned by [`RecipeStore::invalidate_all`]
/// to allow dropping memory allocations outside the write lock.
struct InvalidatedEntries {
    tables: HashMap<String, Arc<KeyRecipe>>,
    indexes: HashMap<String, Arc<KeyRecipe>>,
    queries: HashMap<u64, ClockEntry<Arc<KeyRecipe>>>,
}

/// Internal storage for key recipes, separated by target type to allow zero-allocation
/// `&str` lookups for tables and indexes, and bounded CLOCK (Second-Chance) storage for query recipes.
struct RecipeStore {
    tables: HashMap<String, Arc<KeyRecipe>>,
    indexes: HashMap<String, Arc<KeyRecipe>>,
    queries: ClockStore<u64, Arc<KeyRecipe>>,
    schema_generation: Option<Bytes>,
}

impl RecipeStore {
    fn with_query_capacity(query_capacity: usize) -> Self {
        Self {
            tables: HashMap::new(),
            indexes: HashMap::new(),
            queries: ClockStore::with_capacity(query_capacity),
            schema_generation: None,
        }
    }

    fn len(&self) -> usize {
        self.tables.len() + self.indexes.len() + self.queries.len()
    }

    /// Invalidates all cached tables, indexes, and query recipes, and transitions to the new schema generation.
    ///
    /// Returns the previous collections using [`take`] so deallocation can occur outside the write lock.
    fn invalidate_all(&mut self, new_schema_generation: Option<Bytes>) -> InvalidatedEntries {
        let tables = take(&mut self.tables);
        let indexes = take(&mut self.indexes);
        let queries = self.queries.take_all();
        self.schema_generation = new_schema_generation;
        InvalidatedEntries {
            tables,
            indexes,
            queries,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::key_recipe::Part;
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
        cache.insert_batch(Vec::new());
        assert!(
            cache.is_empty(),
            "cache must remain empty after empty batch"
        );

        let untargeted_recipe = KeyRecipe::new();
        cache.insert_batch(vec![untargeted_recipe]);
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

        cache.insert_batch(vec![
            table_recipe,
            index_recipe,
            query_recipe,
            untargeted_recipe,
        ]);

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

        cache.insert_batch(batch);

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
        assert!(
            cache.insert(KeyRecipe::new().set_operation_uid(1u64)),
            "insert query 1 must succeed"
        );
        assert!(
            cache.insert(KeyRecipe::new().set_operation_uid(2u64)),
            "insert query 2 must succeed"
        );
        assert_eq!(cache.len(), 2, "length must be 2 after two inserts");

        // Overwrite query 1
        assert!(
            cache.insert(KeyRecipe::new().set_operation_uid(1u64)),
            "overwrite query 1 must succeed"
        );
        assert_eq!(
            cache.len(),
            2,
            "overwriting existing query must not change count"
        );

        // Both queries 1 and 2 must still be present
        assert!(
            cache.get_query_recipe(1).is_some(),
            "query 1 must be present"
        );
        assert!(
            cache.get_query_recipe(2).is_some(),
            "query 2 must be present"
        );
    }

    #[test]
    fn query_recipe_cache_overwrite_marks_referenced_for_second_chance() {
        let cache = KeyRecipeCache::with_query_capacity(2);

        // Insert query 1 (head of FIFO queue) and query 2 (tail of FIFO queue) without reading them
        assert!(
            cache.insert(KeyRecipe::new().set_operation_uid(1u64)),
            "insert query 1 must succeed"
        );
        assert!(
            cache.insert(KeyRecipe::new().set_operation_uid(2u64)),
            "insert query 2 must succeed"
        );

        // Overwrite query 1 with an updated recipe (without calling get_query_recipe)
        let updated_recipe = KeyRecipe::new()
            .set_operation_uid(1u64)
            .set_part(vec![Part::new()]);
        assert!(
            cache.insert(updated_recipe),
            "overwrite query 1 must succeed"
        );

        // Insert query 3 to trigger eviction under capacity limit of 2
        assert!(
            cache.insert(KeyRecipe::new().set_operation_uid(3u64)),
            "insert query 3 must succeed"
        );

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
        assert!(
            cache.get_query_recipe(1).is_none(),
            "query 1 must not be stored"
        );

        // Tables and indexes can still be stored
        assert!(
            cache.insert(KeyRecipe::new().set_table_name("Users")),
            "insert table recipe must succeed"
        );
        assert_eq!(cache.len(), 1, "table recipe must be stored in cache");
        assert!(
            cache.get_table_recipe("Users").is_some(),
            "table recipe must be retrieved"
        );
    }

    #[test]
    fn clear_preserves_query_capacity() {
        let cache = KeyRecipeCache::with_query_capacity(2);
        assert!(
            cache.insert(KeyRecipe::new().set_operation_uid(1u64)),
            "insert query 1 must succeed"
        );
        assert!(
            cache.insert(KeyRecipe::new().set_operation_uid(2u64)),
            "insert query 2 must succeed"
        );
        assert_eq!(cache.len(), 2, "cache length must be 2");

        cache.clear();
        assert!(cache.is_empty(), "cache must be empty after clear");

        // Insert 3 new queries, capacity limit of 2 must still be enforced
        assert!(
            cache.insert(KeyRecipe::new().set_operation_uid(10u64)),
            "insert query 10 must succeed"
        );
        assert!(
            cache.insert(KeyRecipe::new().set_operation_uid(20u64)),
            "insert query 20 must succeed"
        );
        assert!(
            cache.insert(KeyRecipe::new().set_operation_uid(30u64)),
            "insert query 30 must succeed"
        );

        assert_eq!(
            cache.len(),
            2,
            "capacity limit of 2 must be preserved after clear"
        );
        assert!(
            cache.get_query_recipe(10).is_none(),
            "query 10 must be evicted"
        );
        assert!(
            cache.get_query_recipe(20).is_some(),
            "query 20 must be present"
        );
        assert!(
            cache.get_query_recipe(30).is_some(),
            "query 30 must be present"
        );
    }

    #[test]
    fn query_recipe_cache_clock_second_chance_eviction() {
        let cache = KeyRecipeCache::with_query_capacity(2);

        // Insert query 1 and query 2
        assert!(
            cache.insert(KeyRecipe::new().set_operation_uid(1u64)),
            "insert query 1 must succeed"
        );
        assert!(
            cache.insert(KeyRecipe::new().set_operation_uid(2u64)),
            "insert query 2 must succeed"
        );

        // Read query 1 (marks query 1 as referenced)
        let query_1 = cache.get_query_recipe(1);
        assert!(query_1.is_some(), "query 1 must be present");

        // Query 2 is NOT accessed (referenced = false)

        // Insert query 3 (triggers eviction)
        assert!(
            cache.insert(KeyRecipe::new().set_operation_uid(3u64)),
            "insert query 3 must succeed"
        );

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
        assert!(
            cache.insert(KeyRecipe::new().set_operation_uid(1u64)),
            "insert query 1 must succeed"
        );
        assert!(
            cache.insert(KeyRecipe::new().set_operation_uid(2u64)),
            "insert query 2 must succeed"
        );

        // Repeatedly hit hot queries 1 and 2
        assert!(cache.get_query_recipe(1).is_some(), "query 1 must be hit");
        assert!(cache.get_query_recipe(2).is_some(), "query 2 must be hit");

        // Simulate a burst of 10 ad-hoc / one-off queries (100..110)
        for i in 100u64..110u64 {
            assert!(
                cache.insert(KeyRecipe::new().set_operation_uid(i)),
                "insert ad-hoc query must succeed"
            );
            // Note: Ad-hoc queries are never read, so their referenced bit remains false
            // Keep refreshing hot queries 1 and 2 during the workload
            assert!(
                cache.get_query_recipe(1).is_some(),
                "query 1 must remain hit"
            );
            assert!(
                cache.get_query_recipe(2).is_some(),
                "query 2 must remain hit"
            );
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

    #[test]
    fn key_recipe_cache_stores_and_clears_schema_generation() {
        let cache = KeyRecipeCache::new();
        assert!(
            cache.schema_generation().is_none(),
            "initial schema_generation must be None"
        );

        let initial_generation = Bytes::from_static(b"gen-12345");
        let recipe_list = RecipeList::new()
            .set_schema_generation(initial_generation.clone())
            .set_recipe(vec![
                KeyRecipe::new()
                    .set_table_name("Users")
                    .set_operation_uid(10u64),
            ]);

        cache.update_from_recipe_list(recipe_list);
        assert_eq!(
            cache.schema_generation(),
            Some(initial_generation),
            "schema_generation must match ingested RecipeList"
        );
        assert_eq!(cache.len(), 1, "table recipe must be cached");

        cache.clear();
        assert!(
            cache.schema_generation().is_none(),
            "schema_generation must be None after clear()"
        );
        assert!(cache.is_empty(), "cache must be empty after clear()");
    }

    #[test]
    fn update_from_recipe_list_ignores_stale_schema_generation() {
        let cache = KeyRecipeCache::new();

        // 1. Initial schema generation v2 with table Users
        let initial_list = RecipeList::new()
            .set_schema_generation(Bytes::from_static(b"v2"))
            .set_recipe(vec![KeyRecipe::new().set_table_name("Users")]);
        cache.update_from_recipe_list(initial_list);

        assert_eq!(
            cache.schema_generation(),
            Some(Bytes::from_static(b"v2")),
            "schema generation must be v2"
        );
        assert!(
            cache.get_table_recipe("Users").is_some(),
            "Users table recipe must be present"
        );

        // 2. Incoming stale schema generation v1 with table OldTable
        let stale_list = RecipeList::new()
            .set_schema_generation(Bytes::from_static(b"v1"))
            .set_recipe(vec![KeyRecipe::new().set_table_name("OldTable")]);
        cache.update_from_recipe_list(stale_list);

        // Stale update must be ignored: generation stays v2, OldTable is NOT added
        assert_eq!(
            cache.schema_generation(),
            Some(Bytes::from_static(b"v2")),
            "stale update must not overwrite current schema generation"
        );
        assert!(
            cache.get_table_recipe("Users").is_some(),
            "Users table recipe must remain present"
        );
        assert!(
            cache.get_table_recipe("OldTable").is_none(),
            "stale recipes must be dropped"
        );
    }

    #[test]
    fn update_from_recipe_list_invalidates_cache_on_newer_schema_generation() {
        let cache = KeyRecipeCache::new();

        // 1. Initial schema generation v1 with Users table and a cached query
        let v1_list = RecipeList::new()
            .set_schema_generation(Bytes::from_static(b"v1"))
            .set_recipe(vec![
                KeyRecipe::new().set_table_name("Users"),
                KeyRecipe::new().set_operation_uid(42u64),
            ]);
        cache.update_from_recipe_list(v1_list);

        assert_eq!(cache.len(), 2, "cache must have 2 entries for v1");
        assert!(
            cache.get_table_recipe("Users").is_some(),
            "Users must exist in v1"
        );
        assert!(
            cache.get_query_recipe(42).is_some(),
            "Query 42 must exist in v1"
        );

        // 2. Schema bump to v2 with Orders table
        let v2_list = RecipeList::new()
            .set_schema_generation(Bytes::from_static(b"v2"))
            .set_recipe(vec![KeyRecipe::new().set_table_name("Orders")]);
        cache.update_from_recipe_list(v2_list);

        // Cache must be invalidated: Users and Query 42 are cleared, only Orders remains
        assert_eq!(
            cache.schema_generation(),
            Some(Bytes::from_static(b"v2")),
            "schema generation must advance to v2"
        );
        assert_eq!(cache.len(), 1, "cache must contain only v2 recipes");
        assert!(
            cache.get_table_recipe("Orders").is_some(),
            "Orders must be present in v2"
        );
        assert!(
            cache.get_table_recipe("Users").is_none(),
            "Users recipe from v1 must be invalidated"
        );
        assert!(
            cache.get_query_recipe(42).is_none(),
            "Query 42 recipe from v1 must be invalidated"
        );
    }

    #[test]
    fn update_from_recipe_list_merges_recipes_on_matching_generation() {
        let cache = KeyRecipeCache::new();

        // 1. Ingest initial recipe for schema generation v1
        let list1 = RecipeList::new()
            .set_schema_generation(Bytes::from_static(b"v1"))
            .set_recipe(vec![KeyRecipe::new().set_table_name("Users")]);
        cache.update_from_recipe_list(list1);

        // 2. Ingest additional recipe for same schema generation v1
        let list2 = RecipeList::new()
            .set_schema_generation(Bytes::from_static(b"v1"))
            .set_recipe(vec![KeyRecipe::new().set_table_name("Accounts")]);
        cache.update_from_recipe_list(list2);

        // Both recipes must be present
        assert_eq!(
            cache.schema_generation(),
            Some(Bytes::from_static(b"v1")),
            "schema generation must remain v1"
        );
        assert_eq!(cache.len(), 2, "both recipes must be retained");
        assert!(
            cache.get_table_recipe("Users").is_some(),
            "Users table recipe must be present"
        );
        assert!(
            cache.get_table_recipe("Accounts").is_some(),
            "Accounts table recipe must be present"
        );
    }

    #[test]
    fn update_from_recipe_list_overwrites_existing_recipes_and_displaces_old_entries() {
        let cache = KeyRecipeCache::new();

        // 1. Ingest initial recipes for schema generation v1
        let initial_list = RecipeList::new()
            .set_schema_generation(Bytes::from_static(b"v1"))
            .set_recipe(vec![
                KeyRecipe::new()
                    .set_table_name("Users")
                    .set_part(vec![Part::new().set_tag(10u32)]),
                KeyRecipe::new()
                    .set_index_name("UsersByEmail")
                    .set_part(vec![Part::new().set_tag(20u32)]),
                KeyRecipe::new()
                    .set_operation_uid(42u64)
                    .set_part(vec![Part::new().set_tag(30u32)]),
            ]);
        cache.update_from_recipe_list(initial_list);

        // Retrieve handles to the original Arc<KeyRecipe> instances
        let old_table_recipe = cache
            .get_table_recipe("Users")
            .expect("initial Users recipe must be present");
        let old_index_recipe = cache
            .get_index_recipe("UsersByEmail")
            .expect("initial UsersByEmail recipe must be present");
        let old_query_recipe = cache
            .get_query_recipe(42u64)
            .expect("initial query 42 recipe must be present");

        assert_eq!(
            Arc::strong_count(&old_table_recipe),
            2,
            "old table recipe strong count must be 2 (local variable + cache storage)"
        );
        assert_eq!(
            Arc::strong_count(&old_index_recipe),
            2,
            "old index recipe strong count must be 2 (local variable + cache storage)"
        );
        assert_eq!(
            Arc::strong_count(&old_query_recipe),
            2,
            "old query recipe strong count must be 2 (local variable + cache storage)"
        );

        // 2. Ingest updated recipes for the same schema generation v1
        let updated_list = RecipeList::new()
            .set_schema_generation(Bytes::from_static(b"v1"))
            .set_recipe(vec![
                KeyRecipe::new()
                    .set_table_name("Users")
                    .set_part(vec![Part::new().set_tag(100u32)]),
                KeyRecipe::new()
                    .set_index_name("UsersByEmail")
                    .set_part(vec![Part::new().set_tag(200u32)]),
                KeyRecipe::new()
                    .set_operation_uid(42u64)
                    .set_part(vec![Part::new().set_tag(300u32)]),
            ]);
        cache.update_from_recipe_list(updated_list);

        // 3. Verify old Arc handles were displaced and released from cache (strong count drops to 1)
        assert_eq!(
            Arc::strong_count(&old_table_recipe),
            1,
            "cache must release old table recipe Arc upon overwrite"
        );
        assert_eq!(
            Arc::strong_count(&old_index_recipe),
            1,
            "cache must release old index recipe Arc upon overwrite"
        );
        assert_eq!(
            Arc::strong_count(&old_query_recipe),
            1,
            "cache must release old query recipe Arc upon overwrite"
        );

        // 4. Verify cache returns the updated recipes with distinct pointers and new part tags
        let new_table_recipe = cache
            .get_table_recipe("Users")
            .expect("updated Users recipe must be present");
        let new_index_recipe = cache
            .get_index_recipe("UsersByEmail")
            .expect("updated UsersByEmail recipe must be present");
        let new_query_recipe = cache
            .get_query_recipe(42u64)
            .expect("updated query 42 recipe must be present");

        assert!(
            !Arc::ptr_eq(&old_table_recipe, &new_table_recipe),
            "new table recipe must be a distinct Arc allocation from the old one"
        );
        assert!(
            !Arc::ptr_eq(&old_index_recipe, &new_index_recipe),
            "new index recipe must be a distinct Arc allocation from the old one"
        );
        assert!(
            !Arc::ptr_eq(&old_query_recipe, &new_query_recipe),
            "new query recipe must be a distinct Arc allocation from the old one"
        );

        assert_eq!(
            new_table_recipe.part.first().map(|part| part.tag),
            Some(100),
            "updated table recipe part tag must match list2"
        );
        assert_eq!(
            new_index_recipe.part.first().map(|part| part.tag),
            Some(200),
            "updated index recipe part tag must match list2"
        );
        assert_eq!(
            new_query_recipe.part.first().map(|part| part.tag),
            Some(300),
            "updated query recipe part tag must match list2"
        );

        assert_eq!(cache.len(), 3, "total cached recipe count must remain 3");
    }

    #[test]
    fn key_recipe_cache_next_operation_uid_increments_monotonically() {
        let cache = KeyRecipeCache::new();
        assert_eq!(cache.next_operation_uid(), 1, "first UID must be 1");
        assert_eq!(cache.next_operation_uid(), 2, "second UID must be 2");
        assert_eq!(cache.next_operation_uid(), 3, "third UID must be 3");
    }

    #[test]
    fn key_recipe_cache_next_operation_uid_concurrent_access() {
        let cache = Arc::new(KeyRecipeCache::new());
        let mut handles = Vec::new();
        for _ in 0..10 {
            let cache_clone = Arc::clone(&cache);
            handles.push(thread::spawn(move || {
                let mut uids = Vec::with_capacity(100);
                for _ in 0..100 {
                    uids.push(cache_clone.next_operation_uid());
                }
                uids
            }));
        }

        let mut all_uids = Vec::new();
        for handle in handles {
            let uids = handle.join().expect("thread should join cleanly");
            all_uids.extend(uids);
        }

        assert_eq!(all_uids.len(), 1000, "must collect 1000 total UIDs");
        all_uids.sort_unstable();
        all_uids.dedup();
        assert_eq!(
            all_uids.len(),
            1000,
            "all 1000 generated UIDs must be unique and monotonic"
        );
        assert_eq!(all_uids.first(), Some(&1), "first UID generated must be 1");
        assert_eq!(
            all_uids.last(),
            Some(&1000),
            "last UID generated must be 1000"
        );
    }

    #[test]
    fn key_recipe_cache_default_is_empty() {
        let cache = KeyRecipeCache::default();
        assert!(cache.is_empty(), "default cache must be empty");
        assert_eq!(cache.len(), 0, "default cache length must be zero");
    }

    #[test]
    fn update_from_recipe_list_empty_list_noops() {
        let cache = KeyRecipeCache::new();
        cache.update_from_recipe_list(RecipeList::new());
        assert!(
            cache.is_empty(),
            "cache must remain empty after empty RecipeList update"
        );
        assert!(
            cache.schema_generation().is_none(),
            "schema generation must remain None"
        );
    }

    #[test]
    fn update_from_recipe_list_skips_untargeted_recipes() {
        let cache = KeyRecipeCache::new();
        let recipe_list = RecipeList::new()
            .set_schema_generation(Bytes::from_static(b"v1"))
            .set_recipe(vec![KeyRecipe::new()]);
        cache.update_from_recipe_list(recipe_list);
        assert!(
            cache.is_empty(),
            "cache must remain empty when RecipeList contains only untargeted recipes"
        );
        assert_eq!(
            cache.schema_generation(),
            Some(Bytes::from_static(b"v1")),
            "schema generation must still be recorded"
        );
    }
}
