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

//! Location router for Spanner client requests.
//!
//! Responsible for resolving the target [`ServerConnection`] for a given request context by:
//! 1. Checking transaction affinity to keep operations within a multi-use transaction pinned to the
//!    same Spanner server node.
//! 2. Evaluating key ranges against [`KeyRangeCache`] to select an eligible tablet replica.
//! 3. Bypassing node endpoints currently marked on cooldown in [`EndpointCooldownTracker`].
//! 4. Falling back cleanly to the default fallback connection when cache misses occur.

// TODO(#6236): Remove dead_code allowance once LocationRouter is integrated into DatabaseClient.
#![allow(dead_code)]

use crate::model::Tablet;
use crate::routing::connection_cache::ConnectionCache;
use crate::routing::endpoint_cooldown::EndpointCooldownTracker;
use crate::routing::key_range_cache::{CachedGroup, CachedRange, KeyRangeCache};
use crate::routing::latency_registry::LatencyRegistry;
use crate::routing::power_of_two_selector::PowerOfTwoSelector;
use crate::routing::server_connection::ServerConnection;
use std::collections::HashMap;
use std::fmt::{self, Debug, Formatter};
use std::sync::{Arc, RwLock};
use std::time::Duration;

/// Context parameters used by [`LocationRouter`] to determine the target server connection.
#[derive(Clone, Debug, Default)]
pub(crate) struct RoutingContext<'a> {
    /// Optional identifier of the Spanner transaction associated with the request.
    pub transaction_id: Option<&'a [u8]>,
    /// Optional binary storage specification key used for tablet range lookup.
    pub routing_key: Option<&'a [u8]>,
    /// Whether the request requires a leader replica (e.g. read-write transactions or commits).
    pub prefer_leader: bool,
    /// Whether to check and record server affinity for this transaction (true for Read-Write
    /// transactions; false for Read-Only transactions which route strictly by key range).
    pub use_transaction_affinity: bool,
}

/// Unbounded transaction affinity map.
///
/// Entries are explicitly cleaned up by calling [`LocationRouter::clear_transaction_affinity`]
/// when a transaction completes (`Commit`, `Rollback`, or drop).
#[derive(Debug, Default)]
struct AffinityTracker {
    entries: HashMap<Vec<u8>, Arc<str>>,
}

impl AffinityTracker {
    fn new() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }

    fn get(&self, transaction_id: &[u8]) -> Option<Arc<str>> {
        if transaction_id.is_empty() {
            return None;
        }
        self.entries.get(transaction_id).cloned()
    }

    fn insert(&mut self, transaction_id: &[u8], address: &str) {
        if transaction_id.is_empty() {
            return;
        }
        if let Some(entry) = self.entries.get_mut(transaction_id) {
            if entry.as_ref() != address {
                *entry = Arc::from(address);
            }
            return;
        }
        self.entries
            .insert(transaction_id.to_vec(), Arc::from(address));
    }

    fn remove(&mut self, transaction_id: &[u8]) -> Option<Arc<str>> {
        if transaction_id.is_empty() {
            return None;
        }
        self.entries.remove(transaction_id)
    }

    fn len(&self) -> usize {
        self.entries.len()
    }
}

/// Routes Spanner requests to specific server node connections using location metadata,
/// latency scores, and transaction affinity.
#[derive(Clone)]
pub(crate) struct LocationRouter {
    database_scope: String,
    key_range_cache: Arc<KeyRangeCache>,
    connection_cache: Arc<ConnectionCache>,
    cooldown_tracker: Arc<EndpointCooldownTracker>,
    latency_registry: Arc<LatencyRegistry>,
    replica_selector: PowerOfTwoSelector,
    affinity_tracker: Arc<RwLock<AffinityTracker>>,
}

impl Debug for LocationRouter {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("LocationRouter")
            .field("database_scope", &self.database_scope)
            .field("connection_cache", &self.connection_cache)
            .field("cooldown_tracker", &self.cooldown_tracker)
            .field("latency_registry", &self.latency_registry)
            .finish_non_exhaustive()
    }
}

impl LocationRouter {
    /// Creates a new `LocationRouter`.
    pub(crate) fn new(
        database_scope: String,
        key_range_cache: Arc<KeyRangeCache>,
        connection_cache: Arc<ConnectionCache>,
        cooldown_tracker: Arc<EndpointCooldownTracker>,
        latency_registry: Arc<LatencyRegistry>,
    ) -> Self {
        debug_assert!(
            !database_scope.is_empty(),
            "database scope must not be empty"
        );
        Self {
            database_scope,
            key_range_cache,
            connection_cache,
            cooldown_tracker,
            latency_registry,
            replica_selector: PowerOfTwoSelector::new(),
            affinity_tracker: Arc::new(RwLock::new(AffinityTracker::new())),
        }
    }

    /// Returns the database scope configured for this router.
    pub(crate) fn database_scope(&self) -> &str {
        &self.database_scope
    }

    /// Returns a reference to the underlying [`KeyRangeCache`].
    pub(crate) fn key_range_cache(&self) -> &Arc<KeyRangeCache> {
        &self.key_range_cache
    }

    /// Returns a reference to the underlying [`ConnectionCache`].
    pub(crate) fn connection_cache(&self) -> &Arc<ConnectionCache> {
        &self.connection_cache
    }

    /// Returns a reference to the underlying [`EndpointCooldownTracker`].
    pub(crate) fn cooldown_tracker(&self) -> &Arc<EndpointCooldownTracker> {
        &self.cooldown_tracker
    }

    /// Returns a reference to the underlying [`LatencyRegistry`].
    pub(crate) fn latency_registry(&self) -> &LatencyRegistry {
        &self.latency_registry
    }

    /// Records an observed round-trip latency sample for an endpoint address within a paxos group.
    pub(crate) fn record_latency(&self, group_uid: u64, server_address: &str, latency: Duration) {
        self.latency_registry.record_latency(
            Some(&self.database_scope),
            group_uid,
            server_address,
            latency,
        );
    }

    /// Records an RPC error penalty for an endpoint address within a paxos group.
    pub(crate) fn record_error(&self, group_uid: u64, server_address: &str) {
        self.latency_registry
            .record_error(Some(&self.database_scope), group_uid, server_address);
    }

    /// Resolves the optimal [`ServerConnection`] for the provided request routing context.
    ///
    /// 1. Checks transaction affinity for an existing session address.
    /// 2. If no affinity match is found, looks up the routing key in the key range cache.
    /// 3. Selects a healthy tablet replica, using P2C latency-aware selection among follower replicas.
    /// 4. Skips endpoints marked on cooldown.
    /// 5. Falls back to the default server connection if no cached node connection is available.
    pub(crate) fn resolve_connection(&self, context: &RoutingContext<'_>) -> ServerConnection {
        // Step 1: Check existing transaction affinity.
        if context.use_transaction_affinity
            && let Some(transaction_id) = context.transaction_id
            && let Some(affinity_address) = self.get_transaction_affinity(transaction_id)
            && !self.cooldown_tracker.is_cooling_down(&affinity_address)
            && let Some(connection) = self.connection_cache.get_if_present(&affinity_address)
        {
            return connection;
        }

        // Step 2: Query key range cache and select healthy replica.
        if let Some(routing_key) = context.routing_key
            && let Some(range) = self.key_range_cache.find_key(routing_key)
            && let Some((tablet, connection)) =
                self.select_healthy_replica(&range, context.prefer_leader)
        {
            // Bind transaction affinity if enabled and a transaction ID is present.
            if context.use_transaction_affinity
                && let Some(transaction_id) = context.transaction_id
            {
                self.record_transaction_affinity(transaction_id, &tablet.server_address);
            }
            return connection;
        }

        // Step 3: Fall back cleanly to the default fallback connection.
        self.connection_cache.default_connection().clone()
    }

    /// Selects an eligible, non-cooling-down tablet replica and its pre-warmed connection for the given cached range,
    /// using P2C replica selection weighted by EWMA latency and active in-flight requests.
    fn select_healthy_replica(
        &self,
        range: &CachedRange,
        prefer_leader: bool,
    ) -> Option<(Tablet, ServerConnection)> {
        let group = self.key_range_cache.get_group(range.group_uid)?;
        if prefer_leader {
            return self.select_healthy_leader(&group);
        }
        self.select_healthy_follower(&group, range.group_uid)
    }

    /// Selects the local leader tablet if healthy and routable.
    fn select_healthy_leader(&self, group: &CachedGroup) -> Option<(Tablet, ServerConnection)> {
        let leader = group.local_leader()?;
        let connection = self.get_routable_connection(leader)?;
        Some((leader.clone(), connection))
    }

    /// Selects a follower replica using P2C replica selection weighted by latency and in-flight load.
    fn select_healthy_follower(
        &self,
        group: &CachedGroup,
        group_uid: u64,
    ) -> Option<(Tablet, ServerConnection)> {
        const MAX_ROUTABLE_CANDIDATES: usize = 8;
        // Filter routable replicas in the lowest distance tier into a fixed-size stack buffer
        // to eliminate heap allocation on the hot request path while ensuring P2C samples from
        // the active healthy candidate set.
        let mut routable: [Option<(&Tablet, ServerConnection)>; MAX_ROUTABLE_CANDIDATES] =
            [const { None }; MAX_ROUTABLE_CANDIDATES];
        let mut routable_count = 0;
        for &index in &group.eligible_replica_indices {
            if routable_count >= routable.len() {
                break;
            }
            let tablet = &group.tablets[index];
            if let Some(connection) = self.get_routable_connection(tablet) {
                routable[routable_count] = Some((tablet, connection));
                routable_count += 1;
            }
        }

        if routable_count == 0 {
            return None;
        }
        if routable_count == 1 {
            return routable[0]
                .take()
                .map(|(tablet, connection)| (tablet.clone(), connection));
        }

        self.select_p2c_winner(&mut routable, routable_count, group_uid)
    }

    /// Compares sampled candidates from the routable stack buffer using P2C and returns the winner.
    fn select_p2c_winner(
        &self,
        routable: &mut [Option<(&Tablet, ServerConnection)>],
        routable_count: usize,
        group_uid: u64,
    ) -> Option<(Tablet, ServerConnection)> {
        let (first_index, second_index) = self.replica_selector.sample_two_distinct(routable_count);

        let (first_tablet, first_connection) = routable.get(first_index)?.as_ref()?;
        let (second_tablet, second_connection) = routable.get(second_index)?.as_ref()?;

        let first_cost = self.latency_registry.get_selection_cost(
            Some(&self.database_scope),
            group_uid,
            first_connection.active_request_count(),
            &first_tablet.server_address,
        );
        let second_cost = self.latency_registry.get_selection_cost(
            Some(&self.database_scope),
            group_uid,
            second_connection.active_request_count(),
            &second_tablet.server_address,
        );

        let selected_index = if first_cost <= second_cost {
            first_index
        } else {
            second_index
        };

        let (selected_tablet, selected_connection) = routable.get_mut(selected_index)?.take()?;
        Some((selected_tablet.clone(), selected_connection))
    }

    /// Returns the pre-warmed, healthy [`ServerConnection`] for a tablet if it has a non-empty
    /// server address, is not currently on cooldown, and is ready in the connection cache.
    fn get_routable_connection(&self, tablet: &Tablet) -> Option<ServerConnection> {
        if tablet.server_address.is_empty()
            || self
                .cooldown_tracker
                .is_cooling_down(&tablet.server_address)
        {
            return None;
        }
        self.connection_cache
            .get_if_present(&tablet.server_address)
            .filter(ServerConnection::is_healthy)
    }

    /// Records affinity mapping `transaction_id` to `address` if not already present.
    pub(crate) fn record_transaction_affinity(&self, transaction_id: &[u8], address: &str) {
        if transaction_id.is_empty() {
            return;
        }
        let mut tracker = self
            .affinity_tracker
            .write()
            .expect("affinity tracker lock poisoned");
        tracker.insert(transaction_id, address);
    }

    /// Returns the address pinned to `transaction_id`, if present.
    pub(crate) fn get_transaction_affinity(&self, transaction_id: &[u8]) -> Option<Arc<str>> {
        if transaction_id.is_empty() {
            return None;
        }
        let tracker = self
            .affinity_tracker
            .read()
            .expect("affinity tracker lock poisoned");
        tracker.get(transaction_id)
    }

    /// Clears any recorded affinity for `transaction_id`.
    pub(crate) fn clear_transaction_affinity(&self, transaction_id: &[u8]) {
        if transaction_id.is_empty() {
            return;
        }
        let mut tracker = self
            .affinity_tracker
            .write()
            .expect("affinity tracker lock poisoned");
        let _ = tracker.remove(transaction_id);
    }

    /// Returns the number of active transaction affinity entries.
    pub(crate) fn affinity_count(&self) -> usize {
        let tracker = self
            .affinity_tracker
            .read()
            .expect("affinity tracker lock poisoned");
        tracker.len()
    }

    /// Helper to record an overload failure cooldown for an endpoint address.
    pub(crate) fn record_failure(&self, address: &str) {
        self.cooldown_tracker.record_failure(address);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::Channel;
    use crate::generated::gapic_dataplane::stub::Spanner as SpannerStub;
    use crate::model::{CacheUpdate, Group, Range, Tablet};
    use crate::routing::server_connection::ServerConnection;
    use gaxi::options::ClientConfig;

    #[test]
    fn location_router_implements_send_sync_debug_clone() {
        static_assertions::assert_impl_all!(LocationRouter: Send, Sync, Debug, Clone);
    }

    #[derive(Debug)]
    struct DummyStub;
    impl SpannerStub for DummyStub {}

    fn create_test_connection(address: &str) -> ServerConnection {
        let channel = Channel::new_for_test(DummyStub);
        ServerConnection::new(address.to_string(), channel)
    }

    fn make_test_router() -> LocationRouter {
        let default_connection = create_test_connection("spanner.googleapis.com:443");
        let connection_cache = Arc::new(ConnectionCache::new(default_connection));
        let key_range_cache = Arc::new(KeyRangeCache::new());
        let cooldown_tracker = Arc::new(EndpointCooldownTracker::new());
        let latency_registry = Arc::new(LatencyRegistry::new());
        LocationRouter::new(
            "projects/test-project/instances/test-instance/databases/test-database".to_string(),
            key_range_cache,
            connection_cache,
            cooldown_tracker,
            latency_registry,
        )
    }

    async fn populate_test_routing_table(
        router: &LocationRouter,
        address: &str,
        start_key: Vec<u8>,
        limit_key: Vec<u8>,
    ) {
        let group = Group::new().set_group_uid(100u64).set_tablets(vec![
            Tablet::default()
                .set_tablet_uid(10u64)
                .set_server_address(address),
        ]);
        let range = Range::new()
            .set_group_uid(100u64)
            .set_start_key(start_key)
            .set_limit_key(limit_key);
        let update = CacheUpdate::new()
            .set_database_id(1u64)
            .set_group(vec![group])
            .set_range(vec![range]);

        router.key_range_cache().add_ranges(&update);

        let _ = router
            .connection_cache()
            .get(address, &ClientConfig::default())
            .await
            .expect("should initialize connection");
    }

    #[test]
    fn location_router_new_and_accessors() {
        let router = make_test_router();
        assert!(router.key_range_cache().is_empty());
        assert_eq!(router.connection_cache().len(), 1);
        assert_eq!(router.affinity_count(), 0);
    }

    #[test]
    fn location_router_resolve_empty_cache_returns_default_connection() {
        let router = make_test_router();
        let context = RoutingContext::default();
        let connection = router.resolve_connection(&context);
        assert_eq!(connection.address(), "spanner.googleapis.com:443");
    }

    #[tokio::test]
    async fn location_router_resolve_routing_key_returns_cached_connection() {
        let router = make_test_router();
        populate_test_routing_table(&router, "10.0.0.1:15000", vec![0x01], vec![0x09]).await;

        let key = vec![0x05];
        let context = RoutingContext {
            transaction_id: None,
            routing_key: Some(&key),
            prefer_leader: false,
            use_transaction_affinity: false,
        };
        let connection = router.resolve_connection(&context);
        assert_eq!(connection.address(), "10.0.0.1:15000");
    }

    #[tokio::test]
    async fn location_router_resolve_records_and_uses_transaction_affinity() {
        let router = make_test_router();
        populate_test_routing_table(&router, "10.0.0.1:15000", vec![0x01], vec![0x09]).await;

        let transaction_id = b"tx1";
        let key = vec![0x05];
        let context = RoutingContext {
            transaction_id: Some(transaction_id),
            routing_key: Some(&key),
            prefer_leader: false,
            use_transaction_affinity: true,
        };

        // First resolve: discovers address via key and binds affinity.
        let connection_first = router.resolve_connection(&context);
        assert_eq!(connection_first.address(), "10.0.0.1:15000");
        assert_eq!(
            router.get_transaction_affinity(transaction_id).as_deref(),
            Some("10.0.0.1:15000")
        );

        // Clear key range cache so key lookup would miss.
        router.key_range_cache().clear();

        // Second resolve: uses transaction affinity directly.
        let context_affinity = RoutingContext {
            transaction_id: Some(transaction_id),
            routing_key: None,
            prefer_leader: false,
            use_transaction_affinity: true,
        };
        let connection_second = router.resolve_connection(&context_affinity);
        assert_eq!(connection_second.address(), "10.0.0.1:15000");
    }

    #[tokio::test]
    async fn location_router_read_only_transaction_bypasses_affinity() {
        let router = make_test_router();
        populate_test_routing_table(&router, "10.0.0.1:15000", vec![0x01], vec![0x09]).await;

        let transaction_id = b"tx_readonly";
        let key = vec![0x05];
        let context = RoutingContext {
            transaction_id: Some(transaction_id),
            routing_key: Some(&key),
            prefer_leader: false,
            use_transaction_affinity: false,
        };

        // Resolves connection via key range, but should NOT record affinity because use_transaction_affinity = false.
        let connection = router.resolve_connection(&context);
        assert_eq!(connection.address(), "10.0.0.1:15000");
        assert_eq!(router.get_transaction_affinity(transaction_id), None);
    }

    #[tokio::test]
    async fn location_router_skips_cooldown_endpoint() {
        let router = make_test_router();
        populate_test_routing_table(&router, "10.0.0.1:15000", vec![0x01], vec![0x09]).await;

        router.record_failure("10.0.0.1:15000");

        let key = vec![0x05];
        let context = RoutingContext {
            transaction_id: None,
            routing_key: Some(&key),
            prefer_leader: false,
            use_transaction_affinity: false,
        };

        let connection = router.resolve_connection(&context);
        // Falls back to default because target is on cooldown.
        assert_eq!(connection.address(), "spanner.googleapis.com:443");
    }

    #[tokio::test]
    async fn location_router_skips_cooldown_affinity_endpoint() {
        let router = make_test_router();
        populate_test_routing_table(&router, "10.0.0.1:15000", vec![0x01], vec![0x09]).await;

        router.record_transaction_affinity(b"tx1", "10.0.0.1:15000");
        router.record_failure("10.0.0.1:15000");

        let context = RoutingContext {
            transaction_id: Some(b"tx1"),
            routing_key: None,
            prefer_leader: false,
            use_transaction_affinity: true,
        };

        let connection = router.resolve_connection(&context);
        // Bypasses cooldown affinity and returns default fallback.
        assert_eq!(connection.address(), "spanner.googleapis.com:443");
    }

    #[test]
    fn location_router_clear_transaction_affinity() {
        let router = make_test_router();
        router.record_transaction_affinity(b"tx1", "10.0.0.1:15000");
        assert_eq!(router.affinity_count(), 1);

        router.clear_transaction_affinity(b"tx1");
        assert_eq!(router.affinity_count(), 0);
        assert_eq!(router.get_transaction_affinity(b"tx1"), None);
    }

    #[test]
    fn location_router_ignores_empty_transaction_id() {
        let router = make_test_router();
        router.record_transaction_affinity(&[], "10.0.0.1:15000");
        assert_eq!(router.affinity_count(), 0);
        assert_eq!(router.get_transaction_affinity(&[]), None);
        router.clear_transaction_affinity(&[]);
        assert_eq!(router.affinity_count(), 0);
    }

    #[test]
    fn location_router_affinity_explicit_cleanup() {
        let router = make_test_router();

        router.record_transaction_affinity(b"tx1", "10.0.0.1:15000");
        router.record_transaction_affinity(b"tx2", "10.0.0.2:15000");
        router.record_transaction_affinity(b"tx3", "10.0.0.3:15000");

        assert_eq!(router.affinity_count(), 3);
        assert_eq!(
            router.get_transaction_affinity(b"tx1").as_deref(),
            Some("10.0.0.1:15000")
        );
        assert_eq!(
            router.get_transaction_affinity(b"tx2").as_deref(),
            Some("10.0.0.2:15000")
        );

        router.clear_transaction_affinity(b"tx1");
        assert_eq!(router.affinity_count(), 2);
        assert_eq!(router.get_transaction_affinity(b"tx1"), None);
    }

    #[tokio::test]
    async fn location_router_prefer_leader_routing() {
        let router = make_test_router();

        let tablet_leader = Tablet::default()
            .set_tablet_uid(10u64)
            .set_server_address("10.0.0.1:15000");
        let tablet_replica = Tablet::default()
            .set_tablet_uid(11u64)
            .set_server_address("10.0.0.2:15000");
        let group = Group::new()
            .set_group_uid(100u64)
            .set_leader_index(0)
            .set_tablets(vec![tablet_leader, tablet_replica]);
        let range = Range::new()
            .set_group_uid(100u64)
            .set_start_key(vec![0x01])
            .set_limit_key(vec![0x09]);
        let update = CacheUpdate::new()
            .set_database_id(1u64)
            .set_group(vec![group])
            .set_range(vec![range]);

        router.key_range_cache().add_ranges(&update);

        let _ = router
            .connection_cache()
            .get("10.0.0.1:15000", &ClientConfig::default())
            .await
            .expect("should initialize connection");

        let key = vec![0x05];
        let context = RoutingContext {
            transaction_id: None,
            routing_key: Some(&key),
            prefer_leader: true,
            use_transaction_affinity: false,
        };
        let connection = router.resolve_connection(&context);
        assert_eq!(connection.address(), "10.0.0.1:15000");
    }

    #[test]
    fn location_router_debug_formatting() {
        let router = make_test_router();
        let debug_str = format!("{:?}", router);
        assert!(debug_str.contains("LocationRouter"));
        assert!(debug_str.contains("connection_cache"));
        assert!(debug_str.contains("cooldown_tracker"));
    }

    #[test]
    fn location_router_record_cooldown_helper() {
        let router = make_test_router();
        router.record_failure("10.0.0.1:15000");
        assert!(router.cooldown_tracker().is_cooling_down("10.0.0.1:15000"));
    }

    #[tokio::test]
    async fn select_replica_picks_lower_latency_follower() {
        let router = make_test_router();

        let tablet_fast = Tablet::default()
            .set_tablet_uid(10u64)
            .set_server_address("10.0.0.1:15000")
            .set_distance(0u32);
        let tablet_slow = Tablet::default()
            .set_tablet_uid(11u64)
            .set_server_address("10.0.0.2:15000")
            .set_distance(0u32);

        let group = Group::new()
            .set_group_uid(100u64)
            .set_tablets(vec![tablet_fast, tablet_slow]);
        let range = Range::new()
            .set_group_uid(100u64)
            .set_start_key(vec![0x01])
            .set_limit_key(vec![0x09]);
        let update = CacheUpdate::new()
            .set_database_id(1u64)
            .set_group(vec![group])
            .set_range(vec![range]);

        router.key_range_cache().add_ranges(&update);

        let _ = router
            .connection_cache()
            .get("10.0.0.1:15000", &ClientConfig::default())
            .await
            .expect("initialize fast connection");
        let _ = router
            .connection_cache()
            .get("10.0.0.2:15000", &ClientConfig::default())
            .await
            .expect("initialize slow connection");

        // Record 5ms latency for fast node and 100ms latency for slow node
        router.record_latency(100, "10.0.0.1:15000", Duration::from_millis(5));
        router.record_latency(100, "10.0.0.2:15000", Duration::from_millis(100));

        let key = vec![0x05];
        let context = RoutingContext {
            transaction_id: None,
            routing_key: Some(&key),
            prefer_leader: false,
            use_transaction_affinity: false,
        };

        // P2C sampling 2 candidates without replacement will compare fast (5ms) vs slow (100ms)
        // and must pick the fast node.
        for _ in 0..10 {
            let connection = router.resolve_connection(&context);
            assert_eq!(
                connection.address(),
                "10.0.0.1:15000",
                "P2C must choose the lower-latency replica"
            );
        }
    }

    #[tokio::test]
    async fn select_replica_weights_active_inflight_requests() {
        let router = make_test_router();

        let tablet_busy = Tablet::default()
            .set_tablet_uid(10u64)
            .set_server_address("10.0.0.1:15000")
            .set_distance(0u32);
        let tablet_idle = Tablet::default()
            .set_tablet_uid(11u64)
            .set_server_address("10.0.0.2:15000")
            .set_distance(0u32);

        let group = Group::new()
            .set_group_uid(100u64)
            .set_tablets(vec![tablet_busy, tablet_idle]);
        let range = Range::new()
            .set_group_uid(100u64)
            .set_start_key(vec![0x01])
            .set_limit_key(vec![0x09]);
        let update = CacheUpdate::new()
            .set_database_id(1u64)
            .set_group(vec![group])
            .set_range(vec![range]);

        router.key_range_cache().add_ranges(&update);

        let connection_busy = router
            .connection_cache()
            .get("10.0.0.1:15000", &ClientConfig::default())
            .await
            .expect("initialize busy connection");
        let _ = router
            .connection_cache()
            .get("10.0.0.2:15000", &ClientConfig::default())
            .await
            .expect("initialize idle connection");

        // Both nodes have equal baseline latency (10ms)
        router.record_latency(100, "10.0.0.1:15000", Duration::from_millis(10));
        router.record_latency(100, "10.0.0.2:15000", Duration::from_millis(10));

        // Simulate 5 active inflight requests on node 1
        for _ in 0..5 {
            connection_busy.increment_active_requests();
        }

        let key = vec![0x05];
        let context = RoutingContext {
            transaction_id: None,
            routing_key: Some(&key),
            prefer_leader: false,
            use_transaction_affinity: false,
        };

        // P2C compares cost: node 1 (10ms * 6 = 60ms) vs node 2 (10ms * 1 = 10ms), picking node 2
        for _ in 0..10 {
            let connection = router.resolve_connection(&context);
            assert_eq!(
                connection.address(),
                "10.0.0.2:15000",
                "P2C must choose the unburdened replica with fewer active requests"
            );
        }
    }

    #[tokio::test]
    async fn select_replica_error_penalty_steers_traffic_away() {
        let router = make_test_router();

        let tablet_failing = Tablet::default()
            .set_tablet_uid(10u64)
            .set_server_address("10.0.0.1:15000")
            .set_distance(0u32);
        let tablet_healthy = Tablet::default()
            .set_tablet_uid(11u64)
            .set_server_address("10.0.0.2:15000")
            .set_distance(0u32);

        let group = Group::new()
            .set_group_uid(100u64)
            .set_tablets(vec![tablet_failing, tablet_healthy]);
        let range = Range::new()
            .set_group_uid(100u64)
            .set_start_key(vec![0x01])
            .set_limit_key(vec![0x09]);
        let update = CacheUpdate::new()
            .set_database_id(1u64)
            .set_group(vec![group])
            .set_range(vec![range]);

        router.key_range_cache().add_ranges(&update);

        let _ = router
            .connection_cache()
            .get("10.0.0.1:15000", &ClientConfig::default())
            .await
            .expect("initialize connection 1");
        let _ = router
            .connection_cache()
            .get("10.0.0.2:15000", &ClientConfig::default())
            .await
            .expect("initialize connection 2");

        // Both nodes have equal initial latency (10ms)
        router.record_latency(100, "10.0.0.1:15000", Duration::from_millis(10));
        router.record_latency(100, "10.0.0.2:15000", Duration::from_millis(10));

        // Record error on node 1
        router.record_error(100, "10.0.0.1:15000");

        let key = vec![0x05];
        let context = RoutingContext {
            transaction_id: None,
            routing_key: Some(&key),
            prefer_leader: false,
            use_transaction_affinity: false,
        };

        for _ in 0..10 {
            let connection = router.resolve_connection(&context);
            assert_eq!(
                connection.address(),
                "10.0.0.2:15000",
                "P2C must steer traffic away from penalized node"
            );
        }
    }

    #[tokio::test]
    async fn select_healthy_tablet_falls_back_to_p2c_followers_when_leader_on_cooldown() {
        let router = make_test_router();

        let tablet_leader = Tablet::default()
            .set_tablet_uid(10u64)
            .set_server_address("10.0.0.1:15000")
            .set_distance(0u32);
        let tablet_follower_fast = Tablet::default()
            .set_tablet_uid(11u64)
            .set_server_address("10.0.0.2:15000")
            .set_distance(0u32);
        let tablet_follower_slow = Tablet::default()
            .set_tablet_uid(12u64)
            .set_server_address("10.0.0.3:15000")
            .set_distance(0u32);

        let group = Group::new()
            .set_group_uid(100u64)
            .set_leader_index(0)
            .set_tablets(vec![
                tablet_leader,
                tablet_follower_fast,
                tablet_follower_slow,
            ]);
        let range = Range::new()
            .set_group_uid(100u64)
            .set_start_key(vec![0x01])
            .set_limit_key(vec![0x09]);
        let update = CacheUpdate::new()
            .set_database_id(1u64)
            .set_group(vec![group])
            .set_range(vec![range]);

        router.key_range_cache().add_ranges(&update);

        let _ = router
            .connection_cache()
            .get("10.0.0.1:15000", &ClientConfig::default())
            .await
            .expect("init leader");
        let _ = router
            .connection_cache()
            .get("10.0.0.2:15000", &ClientConfig::default())
            .await
            .expect("init fast follower");
        let _ = router
            .connection_cache()
            .get("10.0.0.3:15000", &ClientConfig::default())
            .await
            .expect("init slow follower");

        // Record latencies for followers
        router.record_latency(100, "10.0.0.2:15000", Duration::from_millis(5));
        router.record_latency(100, "10.0.0.3:15000", Duration::from_millis(50));

        // Place leader on cooldown
        router.record_failure("10.0.0.1:15000");

        let key = vec![0x05];
        let context_write = RoutingContext {
            transaction_id: None,
            routing_key: Some(&key),
            prefer_leader: true,
            use_transaction_affinity: false,
        };

        // Leader is cooling down: prefer_leader write operations must fall back to the gateway
        let connection_write = router.resolve_connection(&context_write);
        assert_eq!(
            connection_write.address(),
            "spanner.googleapis.com:443",
            "write requiring leader must fall back to gateway when leader is on cooldown"
        );

        let context_read = RoutingContext {
            transaction_id: None,
            routing_key: Some(&key),
            prefer_leader: false,
            use_transaction_affinity: false,
        };

        // Read query (prefer_leader: false) evaluates healthy followers with P2C, selecting fast follower
        let connection_read = router.resolve_connection(&context_read);
        assert_eq!(
            connection_read.address(),
            "10.0.0.2:15000",
            "read query must select lower-latency follower with P2C"
        );
    }

    #[tokio::test]
    async fn select_healthy_tablet_skips_unwarmed_follower_replica() {
        let router = make_test_router();

        let tablet_connected = Tablet::default()
            .set_tablet_uid(10u64)
            .set_server_address("10.0.0.1:15000")
            .set_distance(0u32);
        let tablet_unwarmed = Tablet::default()
            .set_tablet_uid(11u64)
            .set_server_address("10.0.0.2:15000")
            .set_distance(0u32);

        let group = Group::new()
            .set_group_uid(100u64)
            .set_tablets(vec![tablet_connected, tablet_unwarmed]);
        let range = Range::new()
            .set_group_uid(100u64)
            .set_start_key(vec![0x01])
            .set_limit_key(vec![0x09]);
        let update = CacheUpdate::new()
            .set_database_id(1u64)
            .set_group(vec![group])
            .set_range(vec![range]);

        router.key_range_cache().add_ranges(&update);

        // Only pre-warm tablet 10.0.0.1; tablet 10.0.0.2 is NOT in connection_cache
        let _ = router
            .connection_cache()
            .get("10.0.0.1:15000", &ClientConfig::default())
            .await
            .expect("init connected node");

        let key = vec![0x05];
        let context = RoutingContext {
            transaction_id: None,
            routing_key: Some(&key),
            prefer_leader: false,
            use_transaction_affinity: false,
        };

        // Routing must pick the ready/warmed replica (10.0.0.1) and NOT pick 10.0.0.2 (which would fall back to gateway)
        for _ in 0..10 {
            let connection = router.resolve_connection(&context);
            assert_eq!(
                connection.address(),
                "10.0.0.1:15000",
                "must route to the only pre-warmed healthy replica"
            );
        }
    }

    #[tokio::test]
    async fn select_healthy_replica_returns_gateway_when_group_missing() {
        let router = make_test_router();

        // Create range pointing to group UID 999 which is not added to the cache
        let range = Range::new()
            .set_group_uid(999u64)
            .set_start_key(vec![0x01])
            .set_limit_key(vec![0x09]);
        let update = CacheUpdate::new()
            .set_database_id(1u64)
            .set_range(vec![range]);

        router.key_range_cache().add_ranges(&update);

        let key = vec![0x05];
        let context = RoutingContext {
            transaction_id: None,
            routing_key: Some(&key),
            prefer_leader: false,
            use_transaction_affinity: false,
        };

        let connection = router.resolve_connection(&context);
        assert_eq!(
            connection.address(),
            "spanner.googleapis.com:443",
            "must fall back to default gateway when group is missing from cache"
        );
    }

    #[tokio::test]
    async fn select_healthy_leader_falls_back_to_gateway_when_group_has_no_leader() {
        let router = make_test_router();

        // Create a group without setting leader_index
        let tablet = Tablet::default()
            .set_tablet_uid(10u64)
            .set_server_address("10.0.0.1:15000")
            .set_distance(0u32);

        let group = Group::new()
            .set_group_uid(100u64)
            .set_leader_index(-1)
            .set_tablets(vec![tablet]);
        let range = Range::new()
            .set_group_uid(100u64)
            .set_start_key(vec![0x01])
            .set_limit_key(vec![0x09]);
        let update = CacheUpdate::new()
            .set_database_id(1u64)
            .set_group(vec![group])
            .set_range(vec![range]);

        router.key_range_cache().add_ranges(&update);

        let _ = router
            .connection_cache()
            .get("10.0.0.1:15000", &ClientConfig::default())
            .await
            .expect("init connection");

        let key = vec![0x05];
        let context = RoutingContext {
            transaction_id: None,
            routing_key: Some(&key),
            prefer_leader: true,
            use_transaction_affinity: false,
        };

        let connection = router.resolve_connection(&context);
        assert_eq!(
            connection.address(),
            "spanner.googleapis.com:443",
            "must fall back to default gateway when leader is requested but no leader exists"
        );
    }

    #[tokio::test]
    async fn select_healthy_follower_falls_back_to_gateway_when_no_replicas_routable() {
        let router = make_test_router();

        let tablet1 = Tablet::default()
            .set_tablet_uid(10u64)
            .set_server_address("10.0.0.1:15000")
            .set_distance(0u32);
        let tablet2 = Tablet::default()
            .set_tablet_uid(11u64)
            .set_server_address("10.0.0.2:15000")
            .set_distance(0u32);

        let group = Group::new()
            .set_group_uid(100u64)
            .set_tablets(vec![tablet1, tablet2]);
        let range = Range::new()
            .set_group_uid(100u64)
            .set_start_key(vec![0x01])
            .set_limit_key(vec![0x09]);
        let update = CacheUpdate::new()
            .set_database_id(1u64)
            .set_group(vec![group])
            .set_range(vec![range]);

        router.key_range_cache().add_ranges(&update);

        // Neither node is pre-warmed in the connection cache
        let key = vec![0x05];
        let context = RoutingContext {
            transaction_id: None,
            routing_key: Some(&key),
            prefer_leader: false,
            use_transaction_affinity: false,
        };

        let connection = router.resolve_connection(&context);
        assert_eq!(
            connection.address(),
            "spanner.googleapis.com:443",
            "must fall back to gateway when no candidate replicas are routable"
        );
    }

    #[tokio::test]
    async fn select_healthy_follower_routes_among_multiple_p2c_candidates() {
        let router = make_test_router();

        let mut tablets = Vec::new();
        for index in 0..4 {
            let address = format!("10.0.0.{}:15000", index + 1);
            let tablet = Tablet::default()
                .set_tablet_uid(10u64 + index as u64)
                .set_server_address(address.clone())
                .set_distance(0u32);
            tablets.push(tablet);

            let _ = router
                .connection_cache()
                .get(&address, &ClientConfig::default())
                .await
                .expect("initialize candidate connection");
        }

        let group = Group::new().set_group_uid(100u64).set_tablets(tablets);
        let range = Range::new()
            .set_group_uid(100u64)
            .set_start_key(vec![0x01])
            .set_limit_key(vec![0x09]);
        let update = CacheUpdate::new()
            .set_database_id(1u64)
            .set_group(vec![group])
            .set_range(vec![range]);

        router.key_range_cache().add_ranges(&update);

        // Record latencies: node 1 is fastest (5ms), node 4 is slowest (200ms)
        router.record_latency(100, "10.0.0.1:15000", Duration::from_millis(5));
        router.record_latency(100, "10.0.0.2:15000", Duration::from_millis(20));
        router.record_latency(100, "10.0.0.3:15000", Duration::from_millis(50));
        router.record_latency(100, "10.0.0.4:15000", Duration::from_millis(200));

        let key = vec![0x05];
        let context = RoutingContext {
            transaction_id: None,
            routing_key: Some(&key),
            prefer_leader: false,
            use_transaction_affinity: false,
        };

        for _ in 0..20 {
            let connection = router.resolve_connection(&context);
            assert!(
                connection.address() != "spanner.googleapis.com:443",
                "must route to one of the healthy candidates"
            );
        }
    }

    #[tokio::test]
    async fn select_healthy_follower_caps_at_stack_buffer_limit() {
        let router = make_test_router();

        // Create 10 replicas (exceeding the stack buffer size of 8)
        let mut tablets = Vec::new();
        for index in 0..10 {
            let address = format!("10.0.0.{}:15000", index + 1);
            let tablet = Tablet::default()
                .set_tablet_uid(10u64 + index as u64)
                .set_server_address(address.clone())
                .set_distance(0u32);
            tablets.push(tablet);

            let _ = router
                .connection_cache()
                .get(&address, &ClientConfig::default())
                .await
                .expect("initialize candidate connection");
        }

        let group = Group::new().set_group_uid(100u64).set_tablets(tablets);
        let range = Range::new()
            .set_group_uid(100u64)
            .set_start_key(vec![0x01])
            .set_limit_key(vec![0x09]);
        let update = CacheUpdate::new()
            .set_database_id(1u64)
            .set_group(vec![group])
            .set_range(vec![range]);

        router.key_range_cache().add_ranges(&update);

        let key = vec![0x05];
        let context = RoutingContext {
            transaction_id: None,
            routing_key: Some(&key),
            prefer_leader: false,
            use_transaction_affinity: false,
        };

        let connection = router.resolve_connection(&context);
        assert!(
            connection.address().starts_with("10.0.0."),
            "must successfully route even when candidate count exceeds stack buffer capacity"
        );
    }
}
