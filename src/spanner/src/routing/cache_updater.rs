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

//! Cache updater for location-aware routing.
//!
//! Responsible for ingesting [`CacheUpdate`] messages received from Spanner servers
//! (either piggybacked on RPC responses or streamed via `FetchCacheUpdate`) and applying
//! them to the in-memory routing table ([`KeyRangeCache`]) and server connection pool
//! ([`ConnectionCache`]).

// TODO(#6236): Remove dead_code allowance once CacheUpdater is integrated into LocationRouter and DatabaseClient.
#![allow(dead_code)]

use crate::model::CacheUpdate;
use crate::routing::connection_cache::ConnectionCache;
use crate::routing::key_range_cache::KeyRangeCache;
use crate::routing::key_recipe_cache::KeyRecipeCache;
use gaxi::options::ClientConfig;
use std::fmt::{self, Debug, Formatter};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use tokio::runtime::Handle;

/// Orchestrates updates to the location-aware routing caches.
///
/// `CacheUpdater` coordinates between wire-format [`CacheUpdate`] protobuf payloads and the
/// client's in-memory [`KeyRangeCache`], [`KeyRecipeCache`], and [`ConnectionCache`].
pub(crate) struct CacheUpdater {
    key_range_cache: Arc<KeyRangeCache>,
    key_recipe_cache: Arc<KeyRecipeCache>,
    connection_cache: Arc<ConnectionCache>,
    client_config: Arc<ClientConfig>,
    database_id: AtomicU64,
    update_lock: RwLock<()>,
}

impl Debug for CacheUpdater {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("CacheUpdater")
            .field("connection_cache", &self.connection_cache)
            .field("client_config", &self.client_config)
            .field("database_id", &self.database_id.load(Ordering::Acquire))
            .finish_non_exhaustive()
    }
}

impl CacheUpdater {
    /// Creates a new `CacheUpdater` wrapping the provided caches and client configuration.
    pub(crate) fn new(
        key_range_cache: Arc<KeyRangeCache>,
        key_recipe_cache: Arc<KeyRecipeCache>,
        connection_cache: Arc<ConnectionCache>,
        client_config: ClientConfig,
    ) -> Self {
        Self {
            key_range_cache,
            key_recipe_cache,
            connection_cache,
            client_config: Arc::new(client_config),
            database_id: AtomicU64::new(0),
            update_lock: RwLock::new(()),
        }
    }

    /// Returns a reference to the underlying [`KeyRangeCache`].
    pub(crate) fn key_range_cache(&self) -> &Arc<KeyRangeCache> {
        &self.key_range_cache
    }

    /// Returns a reference to the underlying [`KeyRecipeCache`].
    pub(crate) fn key_recipe_cache(&self) -> &Arc<KeyRecipeCache> {
        &self.key_recipe_cache
    }

    /// Returns the current active database ID recorded by the cache updater.
    pub(crate) fn database_id(&self) -> u64 {
        self.database_id.load(Ordering::Acquire)
    }

    /// Returns a reference to the underlying [`ConnectionCache`].
    pub(crate) fn connection_cache(&self) -> &Arc<ConnectionCache> {
        &self.connection_cache
    }

    /// Returns a reference to the underlying [`ClientConfig`].
    pub(crate) fn client_config(&self) -> &ClientConfig {
        &self.client_config
    }

    /// Ingests a [`CacheUpdate`] payload, updating the routing table, key recipe cache,
    /// and asynchronously pre-warming server connections for newly discovered tablet endpoints.
    ///
    /// Synchronizes concurrent incremental updates under a shared read lock and coordinates
    /// database ID transitions/cache invalidations under an exclusive write lock.
    pub(crate) fn process_cache_update(&self, cache_update: CacheUpdate) {
        let update_database_id = cache_update.database_id;

        // If the update specifies a database ID that differs from the active one,
        // or on initial startup, acquire an exclusive write lock to transition the ID
        // and safely clear stale caches.
        if update_database_id != 0 {
            let current_id = self.database_id.load(Ordering::Acquire);
            if current_id != update_database_id {
                let _write_guard = self.update_lock.write().expect("poisoned update lock");
                let current_id = self.database_id.load(Ordering::Acquire);
                if current_id != update_database_id {
                    if current_id != 0 && update_database_id < current_id {
                        // Stale update from an older database generation: abort ingestion.
                        return;
                    }
                    if current_id != 0 {
                        self.key_range_cache.clear();
                        self.key_recipe_cache.clear();
                    }
                    self.database_id
                        .store(update_database_id, Ordering::Release);
                    self.ingest_cache_payload(cache_update);
                    return;
                }
            }
        }

        // Shared read path: Multiple threads can concurrently ingest incremental updates for the current active database.
        // The shared read lock prevents cache updates from racing with an exclusive cache invalidation / database ID switch.
        let _read_guard = self.update_lock.read().expect("poisoned update lock");
        if update_database_id != 0 && update_database_id < self.database_id.load(Ordering::Acquire)
        {
            // A database ID switch occurred before acquiring the read lock; abort stale update.
            return;
        }

        self.ingest_cache_payload(cache_update);
    }

    /// Ingests recipes, ranges, and pre-warms endpoints for an accepted [`CacheUpdate`].
    fn ingest_cache_payload(&self, mut cache_update: CacheUpdate) {
        if let Some(key_recipes) = cache_update.key_recipes.take() {
            self.key_recipe_cache.update_from_recipe_list(key_recipes);
        }

        if !cache_update.group.is_empty() || !cache_update.range.is_empty() {
            // Apply tablet ranges and group metadata to the key range cache.
            self.key_range_cache.add_ranges(&cache_update);
        }

        // Pre-warm server connections for any newly discovered tablet addresses.
        self.prewarm_server_connections(&cache_update);
    }

    /// Identifies new server addresses in `cache_update` and spawns asynchronous background tasks
    /// to establish connections in the connection cache without blocking foreground RPCs.
    fn prewarm_server_connections(&self, cache_update: &CacheUpdate) {
        // In production, the Spanner client always runs inside a Tokio async runtime. We check
        // `Handle::try_current()` first to prevent any work in synchronous unit tests where no
        // runtime is active; in production, `handle.spawn` always executes.
        let Ok(handle) = Handle::try_current() else {
            return;
        };

        // A single Spanner paxos group typically advertises 3-4 replica server addresses
        // (1 leader + read-only/read-write replicas). Pre-allocating capacity 4 avoids heap
        // reallocations for the vast majority of CacheUpdate payloads.
        let mut new_addresses: Vec<&str> = Vec::with_capacity(4);
        for group in &cache_update.group {
            for tablet in &group.tablets {
                let address = tablet.server_address.as_str();
                if !address.is_empty()
                    && !new_addresses.contains(&address)
                    && self.connection_cache.get_if_present(address).is_none()
                {
                    new_addresses.push(address);
                }
            }
        }

        for address in new_addresses {
            let connection_cache = Arc::clone(&self.connection_cache);
            let config = Arc::clone(&self.client_config);
            let address_string = address.to_string();
            handle.spawn(async move {
                // Calling `get` asynchronously initializes the server connection in the cache
                // if it does not already exist, ensuring foreground RPCs don't incur connection
                // handshake latency.
                if let Err(err) = connection_cache.get(&address_string, &config).await {
                    tracing::warn!(
                        ?err,
                        address = %address_string,
                        "Failed to pre-warm connection to Spanner server"
                    );
                }
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::Channel;
    use crate::model::key_recipe::Part;
    use crate::model::{Group, KeyRecipe, Range, RecipeList, Tablet};
    use crate::routing::server_connection::ServerConnection;
    use std::time::{Duration, Instant};

    #[test]
    fn cache_updater_implements_send_sync_debug() {
        static_assertions::assert_impl_all!(CacheUpdater: Send, Sync, Debug);
    }

    #[derive(Debug)]
    struct DummyStub;
    impl crate::generated::gapic_dataplane::stub::Spanner for DummyStub {}

    fn create_test_connection(address: &str) -> ServerConnection {
        let channel = Channel::new_for_test(DummyStub);
        ServerConnection::new(address.to_string(), channel)
    }

    fn make_test_updater() -> CacheUpdater {
        let default_connection = create_test_connection("spanner.googleapis.com:443");
        let connection_cache = Arc::new(ConnectionCache::new(default_connection));
        let key_range_cache = Arc::new(KeyRangeCache::new());
        let key_recipe_cache = Arc::new(KeyRecipeCache::new());
        let client_config = ClientConfig::default();
        CacheUpdater::new(
            key_range_cache,
            key_recipe_cache,
            connection_cache,
            client_config,
        )
    }

    /// Helper to wait deterministically for background connection pre-warming tasks to complete
    /// without arbitrary timer sleeps.
    async fn wait_for_connections(updater: &CacheUpdater, expected_count: usize) {
        let start = Instant::now();
        let timeout = Duration::from_secs(2);
        while start.elapsed() < timeout {
            if updater.connection_cache().len() >= expected_count {
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!(
            "timed out after {:?} waiting for connections: expected {}, got {}",
            timeout,
            expected_count,
            updater.connection_cache().len()
        );
    }

    #[test]
    fn cache_updater_new_and_accessors() {
        let updater = make_test_updater();
        assert!(updater.key_range_cache().is_empty());
        assert_eq!(updater.connection_cache().len(), 1);
    }

    #[test]
    fn cache_updater_key_range_cache_and_config_accessors() {
        let updater = make_test_updater();
        assert_eq!(updater.key_range_cache().len(), 0);
        assert!(updater.client_config().endpoint.is_none());
    }

    #[test]
    fn cache_updater_process_empty_update() {
        let updater = make_test_updater();
        let update = CacheUpdate::default();
        updater.process_cache_update(update);
        assert!(updater.key_range_cache().is_empty());
        assert_eq!(updater.connection_cache().len(), 1);
    }

    #[test]
    fn cache_updater_debug_formatting() {
        let updater = make_test_updater();
        let debug_str = format!("{:?}", updater);
        assert!(debug_str.contains("CacheUpdater"));
        assert!(debug_str.contains("connection_cache"));
        assert!(debug_str.contains("client_config"));
    }

    #[test]
    fn cache_updater_process_update_populates_key_range_cache() {
        let updater = make_test_updater();
        let group = Group::new().set_group_uid(100u64).set_tablets(vec![
            Tablet::default()
                .set_tablet_uid(10u64)
                .set_server_address("10.0.0.1:15000"),
        ]);
        let range = Range::new()
            .set_group_uid(100u64)
            .set_start_key(vec![0x01])
            .set_limit_key(vec![0x09]);
        let update = CacheUpdate::new()
            .set_database_id(1u64)
            .set_group(vec![group])
            .set_range(vec![range]);

        updater.process_cache_update(update);
        assert_eq!(updater.key_range_cache().len(), 1);
        assert!(updater.key_range_cache().get_group(100).is_some());
    }

    #[test]
    fn cache_updater_ignores_empty_server_address() {
        let updater = make_test_updater();
        let group = Group::new().set_group_uid(100u64).set_tablets(vec![
            Tablet::default()
                .set_tablet_uid(10u64)
                .set_server_address(""),
        ]);
        let range = Range::new()
            .set_group_uid(100u64)
            .set_start_key(vec![0x01])
            .set_limit_key(vec![0x09]);
        let update = CacheUpdate::new()
            .set_database_id(1u64)
            .set_group(vec![group])
            .set_range(vec![range]);

        updater.process_cache_update(update);
        assert_eq!(updater.connection_cache().len(), 1);
    }

    #[test]
    fn cache_updater_prewarm_with_no_tablets_returns_early() {
        let updater = make_test_updater();
        let group = Group::new().set_group_uid(100u64);
        let range = Range::new()
            .set_group_uid(100u64)
            .set_start_key(vec![0x01])
            .set_limit_key(vec![0x09]);
        let update = CacheUpdate::new()
            .set_database_id(1u64)
            .set_group(vec![group])
            .set_range(vec![range]);

        updater.process_cache_update(update);
        assert_eq!(updater.key_range_cache().len(), 1);
        assert_eq!(updater.connection_cache().len(), 1);
    }

    #[tokio::test]
    async fn cache_updater_prewarms_connections() {
        let updater = make_test_updater();
        assert!(
            updater
                .connection_cache()
                .get_if_present("10.0.0.1:15000")
                .is_none()
        );

        let group = Group::new().set_group_uid(100u64).set_tablets(vec![
            Tablet::default()
                .set_tablet_uid(10u64)
                .set_server_address("10.0.0.1:15000"),
        ]);
        let range = Range::new()
            .set_group_uid(100u64)
            .set_start_key(vec![0x01])
            .set_limit_key(vec![0x09]);
        let update = CacheUpdate::new()
            .set_database_id(1u64)
            .set_group(vec![group])
            .set_range(vec![range]);

        updater.process_cache_update(update);

        wait_for_connections(&updater, 2).await;

        assert!(
            updater
                .connection_cache()
                .get_if_present("10.0.0.1:15000")
                .is_some()
        );
    }

    #[tokio::test]
    async fn cache_updater_deduplicates_addresses() {
        let updater = make_test_updater();

        let tablet_a = Tablet::default()
            .set_tablet_uid(10u64)
            .set_server_address("10.0.0.1:15000");
        let tablet_b = Tablet::default()
            .set_tablet_uid(11u64)
            .set_server_address("10.0.0.1:15000");
        let group = Group::new()
            .set_group_uid(100u64)
            .set_tablets(vec![tablet_a, tablet_b]);
        let range = Range::new()
            .set_group_uid(100u64)
            .set_start_key(vec![0x01])
            .set_limit_key(vec![0x09]);
        let update = CacheUpdate::new()
            .set_database_id(1u64)
            .set_group(vec![group])
            .set_range(vec![range]);

        updater.process_cache_update(update);

        wait_for_connections(&updater, 2).await;

        assert_eq!(updater.connection_cache().len(), 2);
    }

    #[tokio::test]
    async fn cache_updater_does_not_overwrite_existing_connection() {
        let updater = make_test_updater();
        let _ = updater
            .connection_cache()
            .get("10.0.0.1:15000", updater.client_config())
            .await
            .expect("should initialize connection");

        assert_eq!(updater.connection_cache().len(), 2);

        let group = Group::new().set_group_uid(100u64).set_tablets(vec![
            Tablet::default()
                .set_tablet_uid(10u64)
                .set_server_address("10.0.0.1:15000"),
        ]);
        let range = Range::new()
            .set_group_uid(100u64)
            .set_start_key(vec![0x01])
            .set_limit_key(vec![0x09]);
        let update = CacheUpdate::new()
            .set_database_id(1u64)
            .set_group(vec![group])
            .set_range(vec![range]);

        updater.process_cache_update(update);

        wait_for_connections(&updater, 2).await;

        let connection_after = updater
            .connection_cache()
            .get_if_present("10.0.0.1:15000")
            .expect("connection should remain");
        assert_eq!(connection_after.address(), "10.0.0.1:15000");
        assert_eq!(updater.connection_cache().len(), 2);
    }

    #[tokio::test]
    async fn cache_updater_prewarms_multiple_distinct_addresses() {
        let updater = make_test_updater();

        let tablet_a = Tablet::default()
            .set_tablet_uid(10u64)
            .set_server_address("10.0.0.1:15000");
        let tablet_b = Tablet::default()
            .set_tablet_uid(11u64)
            .set_server_address("10.0.0.2:15000");
        let group = Group::new()
            .set_group_uid(100u64)
            .set_tablets(vec![tablet_a, tablet_b]);
        let range = Range::new()
            .set_group_uid(100u64)
            .set_start_key(vec![0x01])
            .set_limit_key(vec![0x09]);
        let update = CacheUpdate::new()
            .set_database_id(1u64)
            .set_group(vec![group])
            .set_range(vec![range]);

        updater.process_cache_update(update);

        wait_for_connections(&updater, 3).await;

        assert_eq!(updater.connection_cache().len(), 3);
        assert!(
            updater
                .connection_cache()
                .get_if_present("10.0.0.1:15000")
                .is_some()
        );
        assert!(
            updater
                .connection_cache()
                .get_if_present("10.0.0.2:15000")
                .is_some()
        );
    }

    #[tokio::test]
    async fn cache_updater_prewarms_only_uncached_addresses_in_mixed_update() {
        let updater = make_test_updater();
        let _ = updater
            .connection_cache()
            .get("10.0.0.1:15000", updater.client_config())
            .await
            .expect("should initialize connection");

        assert_eq!(updater.connection_cache().len(), 2);

        let tablet_a = Tablet::default()
            .set_tablet_uid(10u64)
            .set_server_address("10.0.0.1:15000");
        let tablet_b = Tablet::default()
            .set_tablet_uid(11u64)
            .set_server_address("10.0.0.2:15000");
        let group = Group::new()
            .set_group_uid(100u64)
            .set_tablets(vec![tablet_a, tablet_b]);
        let range = Range::new()
            .set_group_uid(100u64)
            .set_start_key(vec![0x01])
            .set_limit_key(vec![0x09]);
        let update = CacheUpdate::new()
            .set_database_id(1u64)
            .set_group(vec![group])
            .set_range(vec![range]);

        updater.process_cache_update(update);

        wait_for_connections(&updater, 3).await;

        assert_eq!(updater.connection_cache().len(), 3);
        assert!(
            updater
                .connection_cache()
                .get_if_present("10.0.0.1:15000")
                .is_some()
        );
        assert!(
            updater
                .connection_cache()
                .get_if_present("10.0.0.2:15000")
                .is_some()
        );
    }

    #[test]
    fn cache_updater_process_update_replaces_older_generation() {
        let updater = make_test_updater();
        let group_old = Group::new()
            .set_group_uid(100u64)
            .set_generation(vec![0x01])
            .set_tablets(vec![
                Tablet::default()
                    .set_tablet_uid(10u64)
                    .set_server_address("10.0.0.1:15000"),
            ]);
        let range = Range::new()
            .set_group_uid(100u64)
            .set_start_key(vec![0x01])
            .set_limit_key(vec![0x09]);
        let update_old = CacheUpdate::new()
            .set_database_id(1u64)
            .set_group(vec![group_old])
            .set_range(vec![range.clone()]);

        updater.process_cache_update(update_old);
        assert_eq!(
            updater
                .key_range_cache()
                .get_group(100)
                .expect("should exist")
                .generation,
            vec![0x01]
        );

        let group_new = Group::new()
            .set_group_uid(100u64)
            .set_generation(vec![0x02])
            .set_tablets(vec![
                Tablet::default()
                    .set_tablet_uid(11u64)
                    .set_server_address("10.0.0.2:15000"),
            ]);
        let update_new = CacheUpdate::new()
            .set_database_id(1u64)
            .set_group(vec![group_new])
            .set_range(vec![range]);

        updater.process_cache_update(update_new);
        assert_eq!(
            updater
                .key_range_cache()
                .get_group(100)
                .expect("should exist")
                .generation,
            vec![0x02]
        );
    }

    #[test]
    fn test_database_id_change_clears_range_and_recipe_caches() {
        let updater = make_test_updater();
        let recipe_cache = Arc::clone(updater.key_recipe_cache());

        let initial_recipe = KeyRecipe::new()
            .set_table_name("Users")
            .set_part(vec![Part::new().set_tag(1u32)]);
        let initial_group = Group::new().set_group_uid(10u64).set_generation(vec![0x01]);
        let initial_range = Range::new()
            .set_group_uid(10u64)
            .set_start_key(vec![0x01])
            .set_limit_key(vec![0x09]);

        let initial_update = CacheUpdate::new()
            .set_database_id(100u64)
            .set_key_recipes(RecipeList::new().set_recipe(vec![initial_recipe]))
            .set_group(vec![initial_group])
            .set_range(vec![initial_range]);

        updater.process_cache_update(initial_update);
        assert_eq!(updater.database_id(), 100);
        assert_eq!(updater.key_range_cache().len(), 1);
        assert!(recipe_cache.get_table_recipe("Users").is_some());

        // Ingest an update with a different database_id (e.g. database dropped and recreated)
        let new_update = CacheUpdate::new().set_database_id(200u64);
        updater.process_cache_update(new_update);

        assert_eq!(updater.database_id(), 200);
        assert_eq!(
            updater.key_range_cache().len(),
            0,
            "key range cache must be cleared when database_id changes"
        );
        assert!(
            recipe_cache.get_table_recipe("Users").is_none(),
            "key recipe cache must be cleared when database_id changes"
        );
    }

    #[test]
    fn test_initial_database_id_does_not_clear_cache() {
        let updater = make_test_updater();
        let recipe_cache = Arc::clone(updater.key_recipe_cache());

        // Pre-populate recipe cache before any database_id is recorded
        let pre_recipe = KeyRecipe::new()
            .set_table_name("PrePopulated")
            .set_part(vec![Part::new().set_tag(2u32)]);
        recipe_cache.insert(pre_recipe);

        assert_eq!(updater.database_id(), 0);
        assert!(recipe_cache.get_table_recipe("PrePopulated").is_some());

        // First update establishing initial database_id must not wipe the pre-populated entries
        let first_update = CacheUpdate::new().set_database_id(100u64);
        updater.process_cache_update(first_update);

        assert_eq!(updater.database_id(), 100);
        assert!(
            recipe_cache.get_table_recipe("PrePopulated").is_some(),
            "initial transition from database_id 0 to 100 must preserve existing cache entries"
        );
    }

    #[test]
    fn test_key_recipe_cache_ingestion() {
        let updater = make_test_updater();
        let recipe_cache = Arc::clone(updater.key_recipe_cache());

        let recipe = KeyRecipe::new()
            .set_table_name("Albums")
            .set_part(vec![Part::new().set_tag(50u32)]);

        let update = CacheUpdate::new()
            .set_database_id(1u64)
            .set_key_recipes(RecipeList::new().set_recipe(vec![recipe]));

        updater.process_cache_update(update);
        assert!(
            recipe_cache.get_table_recipe("Albums").is_some(),
            "KeyRecipeCache must receive recipes from CacheUpdate"
        );
    }

    #[test]
    fn stale_database_id_update_is_ignored_and_does_not_regress_state() {
        let updater = make_test_updater();
        let recipe_cache = Arc::clone(updater.key_recipe_cache());

        // Step 1: Establish active database ID 200 with range and recipe
        let recipe_200 = KeyRecipe::new()
            .set_table_name("ActiveTable")
            .set_part(vec![Part::new().set_tag(10u32)]);
        let range_200 = Range::new()
            .set_group_uid(200u64)
            .set_start_key(vec![0x10])
            .set_limit_key(vec![0x20]);
        let update_200 = CacheUpdate::new()
            .set_database_id(200u64)
            .set_key_recipes(RecipeList::new().set_recipe(vec![recipe_200]))
            .set_range(vec![range_200]);

        updater.process_cache_update(update_200);
        assert_eq!(updater.database_id(), 200);
        assert_eq!(updater.key_range_cache().len(), 1);
        assert!(recipe_cache.get_table_recipe("ActiveTable").is_some());

        // Step 2: Attempt to ingest a stale update with an older database ID 100
        let stale_recipe = KeyRecipe::new()
            .set_table_name("StaleTable")
            .set_part(vec![Part::new().set_tag(99u32)]);
        let stale_range = Range::new()
            .set_group_uid(100u64)
            .set_start_key(vec![0x01])
            .set_limit_key(vec![0x09]);
        let stale_update = CacheUpdate::new()
            .set_database_id(100u64)
            .set_key_recipes(RecipeList::new().set_recipe(vec![stale_recipe]))
            .set_range(vec![stale_range]);

        updater.process_cache_update(stale_update);

        // Database ID must remain 200, and active ranges/recipes must not be cleared or corrupted
        assert_eq!(
            updater.database_id(),
            200,
            "stale update with older database_id must not regress active database ID"
        );
        assert_eq!(
            updater.key_range_cache().len(),
            1,
            "active ranges must not be wiped by stale update"
        );
        assert!(
            recipe_cache.get_table_recipe("ActiveTable").is_some(),
            "active recipes must be preserved"
        );
        assert!(
            recipe_cache.get_table_recipe("StaleTable").is_none(),
            "stale recipes must be rejected"
        );
    }

    #[test]
    fn concurrent_incremental_updates_under_shared_read_lock_do_not_clear_cache() {
        let updater = make_test_updater();
        let recipe_cache = Arc::clone(updater.key_recipe_cache());

        // Initial update establishing database ID 300
        let initial_update = CacheUpdate::new().set_database_id(300u64).set_range(vec![
            Range::new()
                .set_group_uid(301u64)
                .set_start_key(vec![0x01])
                .set_limit_key(vec![0x10]),
        ]);
        updater.process_cache_update(initial_update);
        assert_eq!(updater.database_id(), 300);
        assert_eq!(updater.key_range_cache().len(), 1);

        // Incremental update 1 with same database ID 300
        let incremental_1 = CacheUpdate::new().set_database_id(300u64).set_range(vec![
            Range::new()
                .set_group_uid(302u64)
                .set_start_key(vec![0x10])
                .set_limit_key(vec![0x20]),
        ]);
        updater.process_cache_update(incremental_1);
        assert_eq!(updater.key_range_cache().len(), 2);

        // Incremental update 2 with database ID 0 (unspecified)
        let incremental_2 =
            CacheUpdate::new().set_key_recipes(RecipeList::new().set_recipe(vec![KeyRecipe::new()
                .set_table_name("IncTable")
                .set_part(vec![Part::new().set_tag(5u32)])]));
        updater.process_cache_update(incremental_2);

        assert_eq!(updater.database_id(), 300);
        assert_eq!(
            updater.key_range_cache().len(),
            2,
            "existing ranges must not be cleared during incremental updates"
        );
        assert!(
            recipe_cache.get_table_recipe("IncTable").is_some(),
            "recipes from incremental updates must be present"
        );
    }
}
