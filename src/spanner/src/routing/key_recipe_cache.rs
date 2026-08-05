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

// TODO(#6236): Remove dead_code allowance once KeyRecipe and KeyRecipeCache are integrated into DatabaseClient.
#![allow(dead_code)]

use crate::model::KeyRecipe;
use crate::model::key_recipe::Target;
use std::collections::HashMap;
use std::fmt::{self, Debug, Formatter};
use std::sync::{Arc, RwLock};

/// Internal storage for key recipes, separated by target type to allow zero-allocation
/// `&str` lookups for tables and indexes.
#[derive(Default)]
struct RecipeStore {
    tables: HashMap<String, Arc<KeyRecipe>>,
    indexes: HashMap<String, Arc<KeyRecipe>>,
    queries: HashMap<u64, Arc<KeyRecipe>>,
}

impl RecipeStore {
    fn clear(&mut self) {
        self.tables.clear();
        self.indexes.clear();
        self.queries.clear();
    }

    fn len(&self) -> usize {
        self.tables.len() + self.indexes.len() + self.queries.len()
    }
}

/// A concurrent, thread-safe cache for Spanner key recipes.
///
/// Backed by [`RwLock`] around separate hash maps for tables, indexes, and queries, enabling
/// zero-allocation `&str` lookups and non-blocking concurrent reads across Tokio tasks.
#[derive(Clone, Default)]
pub(crate) struct KeyRecipeCache {
    store: Arc<RwLock<RecipeStore>>,
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
    /// Creates a new, empty [`KeyRecipeCache`].
    pub(crate) fn new() -> Self {
        Self {
            store: Arc::new(RwLock::new(RecipeStore::default())),
        }
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
        let guard = self
            .store
            .read()
            .expect("key recipe cache read lock poisoned");
        guard.tables.get(table_name).cloned()
    }

    /// Returns the cached recipe for a given database index name (`Read` RPC on an index), if present.
    ///
    /// Accepts a `&str` slice with zero heap allocations on lookup.
    ///
    /// Note on naming: Intentionally uses the `get_` prefix because fetching an item by key from a cache
    /// is a lookup operation, consistent with `HashMap::get`, `ConnectionCache::get_if_present`, and
    /// `KeyRangeCache::get_group`.
    pub(crate) fn get_index_recipe(&self, index_name: &str) -> Option<Arc<KeyRecipe>> {
        let guard = self
            .store
            .read()
            .expect("key recipe cache read lock poisoned");
        guard.indexes.get(index_name).cloned()
    }

    /// Returns the cached recipe for a given SQL query operation UID, if present.
    ///
    /// Note on naming: Intentionally uses the `get_` prefix because fetching an item by key from a cache
    /// is a lookup operation, consistent with `HashMap::get`, `ConnectionCache::get_if_present`, and
    /// `KeyRangeCache::get_group`.
    pub(crate) fn get_query_recipe(&self, operation_uid: u64) -> Option<Arc<KeyRecipe>> {
        let guard = self
            .store
            .read()
            .expect("key recipe cache read lock poisoned");
        guard.queries.get(&operation_uid).cloned()
    }

    /// Inserts a [`KeyRecipe`] into the cache.
    ///
    /// # Concurrency Optimization
    /// Intentionally clones `recipe.target` (`String::clone` for table/index names) and wraps `recipe`
    /// in an `Arc` before acquiring the write lock (`self.store.write()`). This ensures that all heap
    /// allocations occur outside the critical section, reducing lock hold duration to a pure $O(1)$
    /// hashmap insertion.
    ///
    /// Returns `true` if the recipe contained a target and was stored in the cache;
    /// returns `false` if `recipe.target` was `None`.
    pub(crate) fn insert(&self, recipe: KeyRecipe) -> bool {
        let target_proto = match recipe.target.as_ref() {
            Some(target) => target,
            None => return false,
        };
        let target_clone = target_proto.clone();
        let recipe_arc = Arc::new(recipe);
        let mut guard = self
            .store
            .write()
            .expect("key recipe cache write lock poisoned");
        match target_clone {
            Target::TableName(name) => {
                guard.tables.insert(name, recipe_arc);
            }
            Target::IndexName(name) => {
                guard.indexes.insert(name, recipe_arc);
            }
            Target::OperationUid(uid) => {
                guard.queries.insert(uid, recipe_arc);
            }
        }
        true
    }

    /// Clears all entries from the cache.
    pub(crate) fn clear(&self) {
        let mut guard = self
            .store
            .write()
            .expect("key recipe cache write lock poisoned");
        guard.clear();
    }

    /// Returns the total number of recipes stored in the cache.
    pub(crate) fn len(&self) -> usize {
        let guard = self
            .store
            .read()
            .expect("key recipe cache read lock poisoned");
        guard.len()
    }

    /// Returns `true` if the cache is empty.
    pub(crate) fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn key_recipe_cache_implements_send_sync_debug_clone() {
        static_assertions::assert_impl_all!(KeyRecipeCache: Send, Sync, Debug, Clone);
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
        let cache = KeyRecipeCache::new();
        let mut handles = Vec::new();

        for i in 0..10 {
            let cache_clone = cache.clone();
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
}
