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

use crate::model::directed_read_options::Replicas;
use crate::model::routing_hint::SkippedTablet;
use crate::model::{DirectedReadOptions, RoutingHint, Tablet};
use crate::routing::connection_cache::ConnectionCache;
use crate::routing::directed_read::matches_replicas;
use crate::routing::endpoint_cooldown::EndpointCooldownTracker;
use crate::routing::endpoint_lifecycle::EndpointLifecycleManager;
use crate::routing::key_range_cache::{CachedGroup, CachedRange, KeyRangeCache, RangeMode};
use crate::routing::latency_registry::LatencyRegistry;
use crate::routing::power_of_two_selector::PowerOfTwoSelector;
use crate::routing::server_connection::ServerConnection;
use bytes::Bytes;
use google_cloud_gax::error::rpc::Code;
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

/// The outcome of location-aware route resolution, containing the target [`ServerConnection`]
/// and an optional [`RoutingHint`] constructed in the exact same resolution pass.
#[derive(Clone, Debug)]
pub(crate) struct ResolvedRoute {
    pub(crate) connection: ServerConnection,
    pub(crate) routing_hint: Option<RoutingHint>,
}

/// Parameters used to construct a [`RoutingHint`].
struct RoutingHintParams<'a> {
    database_id: u64,
    schema_generation: Option<Bytes>,
    operation_uid: u64,
    client_location: Option<&'a str>,
    directed_read_options: Option<&'a DirectedReadOptions>,
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
    endpoint_lifecycle_manager: Arc<EndpointLifecycleManager>,
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
        endpoint_lifecycle_manager: Arc<EndpointLifecycleManager>,
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
            endpoint_lifecycle_manager,
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

    /// Returns a reference to the underlying [`EndpointLifecycleManager`].
    pub(crate) fn endpoint_lifecycle_manager(&self) -> &EndpointLifecycleManager {
        &self.endpoint_lifecycle_manager
    }

    /// Returns a reference to the underlying [`EndpointCooldownTracker`].
    pub(crate) fn cooldown_tracker(&self) -> &EndpointCooldownTracker {
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

    /// Resolves the optimal [`ResolvedRoute`] (containing both [`ServerConnection`] and optional [`RoutingHint`])
    /// in a single atomic resolution pass.
    ///
    /// # Resolution & Tablet Selection Guarantees:
    /// 1. **Single-Pass Tablet Selection:** Ensures that the replica tablet selected for direct gRPC
    ///    transport connection matches the `tablet_uid` stamped into the [`RoutingHint`], eliminating
    ///    sampling divergence and TOCTOU races.
    /// 2. **Transaction Affinity:** Checks existing session affinity mapping first. If valid and not cooling down,
    ///    routes to the affinity endpoint.
    /// 3. **Key Range Lookup & Cooldown Filtering:** Evaluates covering ranges from [`KeyRangeCache`], selects
    ///    an eligible leader or follower replica, and falls back to the default gateway connection if all
    ///    replicas are skipped, unroutable, or cooling down.
    /// 4. **Skipped Tablet Collection:** Records all skipped, unroutable (empty `server_address`), or cooling
    ///    down tablets into `skipped_tablet_uid`.
    pub(crate) fn resolve_route(
        &self,
        context: &RoutingContext<'_>,
        directed_read_options: Option<&DirectedReadOptions>,
        database_id: u64,
        schema_generation: Option<Bytes>,
        operation_uid: u64,
        client_location: Option<&str>,
    ) -> ResolvedRoute {
        // Step 1: Check existing transaction affinity.
        let affinity_connection = self.resolve_affinity_connection(context);

        // Step 2: Query key range cache for covering range and group.
        let range_and_group = self.find_range_and_group(context.routing_key);
        let selected_tablet = range_and_group.as_ref().and_then(|(range, group)| {
            let index = self.select_healthy_tablet(
                range,
                group,
                context.prefer_leader,
                directed_read_options,
            )?;
            group.tablets.get(index)
        });

        // Step 3: Resolve ServerConnection (affinity -> direct tablet -> default gateway).
        let connection =
            self.resolve_target_connection(context, affinity_connection, selected_tablet);

        // Step 4: Construct RoutingHint in the exact same pass if database_id is active.
        let routing_hint = self.build_routing_hint(
            RoutingHintParams {
                database_id,
                schema_generation,
                operation_uid,
                client_location,
                directed_read_options,
            },
            range_and_group.as_ref(),
            selected_tablet,
        );

        ResolvedRoute {
            connection,
            routing_hint,
        }
    }

    /// Checks existing session affinity mapping for `context.transaction_id`.
    ///
    /// Returns the cached connection if valid and not currently on cooldown.
    fn resolve_affinity_connection(
        &self,
        context: &RoutingContext<'_>,
    ) -> Option<ServerConnection> {
        if !context.use_transaction_affinity {
            return None;
        }
        let transaction_id = context.transaction_id?;
        let affinity_address = self.get_transaction_affinity(transaction_id)?;
        if self.cooldown_tracker.is_cooling_down(&affinity_address) {
            return None;
        }
        self.connection_cache
            .get_if_present(&affinity_address)
            .filter(ServerConnection::is_healthy)
    }

    /// Looks up the covering range and group from [`KeyRangeCache`] if a non-empty routing key is present.
    fn find_range_and_group(
        &self,
        routing_key: Option<&[u8]>,
    ) -> Option<(Arc<CachedRange>, Arc<CachedGroup>)> {
        let key = routing_key.filter(|key| !key.is_empty())?;
        let range = self
            .key_range_cache
            .find_range(key, &[], RangeMode::CoveringSplit)?;
        let group = self.key_range_cache.get_group(range.group_uid)?;
        Some((range, group))
    }

    /// Selects the 0-based index into `group.tablets` of an eligible, non-cooling-down tablet replica
    /// for the given cached range and group.
    ///
    /// Replica priority:
    /// 1. If `prefer_leader` is true and no directed read options are specified, tries selecting the local leader if routable.
    ///    - If leader is pre-warmed, routes directly to the leader.
    ///    - If leader is unwarmed, checks if an eligible follower is pre-warmed and falls back to that follower.
    ///    - If no follower is pre-warmed either, preserves the preferred leader (routed via gateway).
    /// 2. If leader is not available, not routable, cooling down, or if directed read options are specified
    ///    (or if `prefer_leader` is false), selects an eligible follower replica index using P2C replica selection
    ///    weighted by latency and in-flight load among candidates matching `directed_read_options`.
    /// 3. Returns `None` if all candidate replicas are cooling down or unroutable.
    fn select_healthy_tablet(
        &self,
        range: &CachedRange,
        group: &CachedGroup,
        prefer_leader: bool,
        directed_read_options: Option<&DirectedReadOptions>,
    ) -> Option<usize> {
        let has_directed_read_options = directed_read_options
            .and_then(|options| options.replicas.as_ref())
            .is_some();

        if prefer_leader
            && !has_directed_read_options
            && let Some((leader_index, maybe_connection)) = self.select_healthy_leader(group)
        {
            if maybe_connection.is_some() {
                return Some(leader_index);
            }
            // Leader is routable but unwarmed. Check if any eligible follower is already warmed.
            if let Some(follower_index) =
                self.select_healthy_follower(group, range.group_uid, directed_read_options)
                && let Some(tablet) = group.tablets.get(follower_index)
                && let Some(Some(_)) = self.resolve_candidate_connection(tablet)
            {
                return Some(follower_index);
            }
            // No warmed follower available; stick with the preferred leader (routed via gateway).
            return Some(leader_index);
        }

        self.select_healthy_follower(group, range.group_uid, directed_read_options)
    }

    /// Evaluates a tablet's routability and retrieves its warmed connection if present.
    ///
    /// - Returns `None` if the tablet is unroutable (empty address, cooling down, or in transient failure).
    /// - Returns `Some(Some(connection))` if the tablet is routable and pre-warmed.
    /// - Returns `Some(None)` if the tablet is routable but currently unwarmed.
    fn resolve_candidate_connection(&self, tablet: &Tablet) -> Option<Option<ServerConnection>> {
        if tablet.server_address.is_empty()
            || self
                .cooldown_tracker
                .is_cooling_down(&tablet.server_address)
            || self
                .endpoint_lifecycle_manager
                .check_transient_failure_evicted_and_request_recreation(&tablet.server_address)
        {
            return None;
        }

        let connection = self.connection_cache.get_if_present(&tablet.server_address);
        match connection {
            Some(connection) if connection.is_transient_failure() => None,
            Some(connection) if connection.is_healthy() => Some(Some(connection)),
            Some(_) => Some(None),
            None => {
                self.endpoint_lifecycle_manager
                    .request_endpoint_recreation(&tablet.server_address);
                Some(None)
            }
        }
    }

    /// Returns `true` if the tablet has a non-empty server address, is not currently on cooldown,
    /// is not in transient failure, and is not evicted due to transient failure.
    fn is_tablet_routable(&self, tablet: &Tablet) -> bool {
        if tablet.server_address.is_empty()
            || self
                .cooldown_tracker
                .is_cooling_down(&tablet.server_address)
            || self
                .endpoint_lifecycle_manager
                .is_transient_failure_evicted(&tablet.server_address)
        {
            return false;
        }

        !self
            .connection_cache
            .get_if_present(&tablet.server_address)
            .is_some_and(|connection| connection.is_transient_failure())
    }

    /// Resolves the [`ServerConnection`] according to priority:
    /// 1. Existing transaction affinity connection.
    /// 2. Direct healthy tablet replica connection.
    /// 3. Default gateway connection fallback.
    fn resolve_target_connection(
        &self,
        context: &RoutingContext<'_>,
        affinity_connection: Option<ServerConnection>,
        selected_tablet: Option<&Tablet>,
    ) -> ServerConnection {
        if let Some(connection) = affinity_connection {
            self.endpoint_lifecycle_manager
                .record_real_traffic(connection.address());
            return connection;
        }

        if let Some(tablet) = selected_tablet
            && let Some(connection) = self.get_routable_connection(tablet)
        {
            self.endpoint_lifecycle_manager
                .record_real_traffic(&tablet.server_address);
            if context.use_transaction_affinity
                && let Some(transaction_id) = context.transaction_id
            {
                self.record_transaction_affinity(transaction_id, &tablet.server_address);
            }
            return connection;
        }

        self.connection_cache.default_connection().clone()
    }

    /// Selects the local leader tablet index into `group.tablets` along with its pre-warmed connection status
    /// if designated and routable.
    fn select_healthy_leader(
        &self,
        group: &CachedGroup,
    ) -> Option<(usize, Option<ServerConnection>)> {
        let leader_index = group.local_leader_index?;
        let leader = group.tablets.get(leader_index)?;
        let maybe_connection = self.resolve_candidate_connection(leader)?;
        Some((leader_index, maybe_connection))
    }

    /// Selects a follower replica index into `group.tablets` using P2C replica selection weighted by latency and in-flight load.
    ///
    /// If `directed_read_options` specifies replica selectors, filters candidate tablets to matching,
    /// routable replicas in the lowest available distance tier.
    fn select_healthy_follower(
        &self,
        group: &CachedGroup,
        group_uid: u64,
        directed_read_options: Option<&DirectedReadOptions>,
    ) -> Option<usize> {
        const MAX_ROUTABLE_CANDIDATES: usize = 8;
        let mut routable: [Option<(usize, Option<ServerConnection>)>; MAX_ROUTABLE_CANDIDATES] =
            [const { None }; MAX_ROUTABLE_CANDIDATES];

        let (mut routable_count, warmed_count) = match directed_read_options
            .and_then(|options| options.replicas.as_ref())
        {
            Some(replicas) => self.collect_directed_read_candidates(group, replicas, &mut routable),
            None => self.collect_default_candidates(group, &mut routable),
        };

        if routable_count == 0 {
            return None;
        }

        // If at least one candidate is pre-warmed, filter down to only the pre-warmed candidates
        // so P2C never selects an unwarmed replica over a warmed one.
        if warmed_count > 0 && warmed_count < routable_count {
            let mut warmed_only: [Option<(usize, Option<ServerConnection>)>;
                MAX_ROUTABLE_CANDIDATES] = [const { None }; MAX_ROUTABLE_CANDIDATES];
            let mut new_warmed_count = 0;
            for slot in routable[..routable_count].iter_mut() {
                if let Some((tablet_index, Some(connection))) = slot.take() {
                    warmed_only[new_warmed_count] = Some((tablet_index, Some(connection)));
                    new_warmed_count += 1;
                }
            }
            routable = warmed_only;
            routable_count = new_warmed_count;
        }

        if routable_count == 1 {
            return routable[0]
                .take()
                .map(|(tablet_index, _maybe_connection)| tablet_index);
        }

        self.select_p2c_winner(group, &mut routable, routable_count, group_uid)
    }

    /// Collects candidate follower replica indices matching directed read options into the stack buffer.
    ///
    /// Evaluates tablets in a single pass:
    /// 1. Filters by directed read selectors and verifies routability (non-empty address, not cooling down, not in transient failure).
    /// 2. Tracks the lowest distance tier among matching routable replicas. If a tablet with a lower
    ///    distance is encountered, previous higher-distance candidates are discarded.
    /// 3. Populates candidate indices and pre-warmed connection status directly into `routable`.
    fn collect_directed_read_candidates(
        &self,
        group: &CachedGroup,
        replicas: &Replicas,
        routable: &mut [Option<(usize, Option<ServerConnection>)>],
    ) -> (usize, usize) {
        let mut routable_count = 0;
        let mut warmed_count = 0;
        let mut minimum_distance = u32::MAX;

        for (index, tablet) in group.tablets.iter().enumerate() {
            if tablet.skip || !matches_replicas(tablet, replicas) {
                continue;
            }

            let Some(maybe_connection) = self.resolve_candidate_connection(tablet) else {
                continue;
            };

            if tablet.distance > minimum_distance {
                // Skip replicas that belong to a farther distance tier.
                continue;
            }

            if tablet.distance < minimum_distance {
                // Found a closer matching distance tier: discard previously collected candidates.
                minimum_distance = tablet.distance;
                routable[..routable_count].fill(None);
                routable_count = 0;
                warmed_count = 0;
            }

            if routable_count < routable.len() {
                if maybe_connection.is_some() {
                    warmed_count += 1;
                }
                routable[routable_count] = Some((index, maybe_connection));
                routable_count += 1;
            }
        }

        (routable_count, warmed_count)
    }

    /// Collects candidate follower replica indices from precomputed group eligible replica indices into the stack buffer.
    fn collect_default_candidates(
        &self,
        group: &CachedGroup,
        routable: &mut [Option<(usize, Option<ServerConnection>)>],
    ) -> (usize, usize) {
        let mut routable_count = 0;
        let mut warmed_count = 0;

        for &index in &group.eligible_replica_indices {
            if routable_count >= routable.len() {
                break;
            }
            if let Some(tablet) = group.tablets.get(index)
                && let Some(maybe_connection) = self.resolve_candidate_connection(tablet)
            {
                if maybe_connection.is_some() {
                    warmed_count += 1;
                }
                routable[routable_count] = Some((index, maybe_connection));
                routable_count += 1;
            }
        }

        (routable_count, warmed_count)
    }

    /// Compares sampled candidates from the routable stack buffer using P2C and returns the winning tablet index into `group.tablets`.
    fn select_p2c_winner(
        &self,
        group: &CachedGroup,
        routable: &mut [Option<(usize, Option<ServerConnection>)>],
        routable_count: usize,
        group_uid: u64,
    ) -> Option<usize> {
        let (first_slot, second_slot) = self.replica_selector.sample_two_distinct(routable_count);
        let (first_index, first_connection) = routable.get(first_slot)?.as_ref()?;
        let (second_index, second_connection) = routable.get(second_slot)?.as_ref()?;

        let first_tablet = group.tablets.get(*first_index)?;
        let second_tablet = group.tablets.get(*second_index)?;

        let first_active_requests = first_connection
            .as_ref()
            .map_or(0, ServerConnection::active_request_count);
        let second_active_requests = second_connection
            .as_ref()
            .map_or(0, ServerConnection::active_request_count);

        let first_cost = self.latency_registry.get_selection_cost(
            Some(&self.database_scope),
            group_uid,
            first_active_requests,
            &first_tablet.server_address,
        );
        let second_cost = self.latency_registry.get_selection_cost(
            Some(&self.database_scope),
            group_uid,
            second_active_requests,
            &second_tablet.server_address,
        );

        let selected_slot = if first_cost <= second_cost {
            first_slot
        } else {
            second_slot
        };

        let (selected_index, _connection) = routable.get_mut(selected_slot)?.take()?;
        Some(selected_index)
    }

    /// Returns the pre-warmed, healthy [`ServerConnection`] for a tablet if it has a non-empty
    /// server address, is not currently on cooldown, and is ready in the connection cache.
    fn get_routable_connection(&self, tablet: &Tablet) -> Option<ServerConnection> {
        if !self.is_tablet_routable(tablet) {
            return None;
        }
        self.connection_cache
            .get_if_present(&tablet.server_address)
            .filter(ServerConnection::is_healthy)
    }

    /// Gathers all skipped, unroutable (empty `server_address`), cooling-down, or transient-failure tablets
    /// matching directed read options, excluding the currently selected tablet.
    fn collect_skipped_tablets(
        &self,
        group: &CachedGroup,
        selected_tablet_uid: Option<u64>,
        directed_read_options: Option<&DirectedReadOptions>,
    ) -> Vec<SkippedTablet> {
        let has_skipped_or_empty = group
            .tablets
            .iter()
            .any(|tablet| tablet.skip || tablet.server_address.is_empty());
        if !has_skipped_or_empty
            && self.cooldown_tracker.is_empty()
            && !self
                .endpoint_lifecycle_manager
                .has_transient_failure_evictions()
            && !group.tablets.iter().any(|tablet| {
                self.connection_cache
                    .get_if_present(&tablet.server_address)
                    .is_some_and(|connection| connection.is_transient_failure())
            })
        {
            return Vec::new();
        }

        let active_replicas = directed_read_options.and_then(|options| options.replicas.as_ref());

        group
            .tablets
            .iter()
            .filter(|tablet| {
                (tablet.skip || !self.is_tablet_routable(tablet))
                    && selected_tablet_uid != Some(tablet.tablet_uid)
                    && active_replicas.is_none_or(|replicas| matches_replicas(tablet, replicas))
            })
            .map(|tablet| {
                SkippedTablet::new()
                    .set_tablet_uid(tablet.tablet_uid)
                    .set_incarnation(tablet.incarnation.clone())
            })
            .collect()
    }

    /// Constructs a [`RoutingHint`] if `database_id != 0` and a covering range exists.
    fn build_routing_hint(
        &self,
        params: RoutingHintParams<'_>,
        range_and_group: Option<&(Arc<CachedRange>, Arc<CachedGroup>)>,
        selected_tablet: Option<&Tablet>,
    ) -> Option<RoutingHint> {
        if params.database_id == 0 {
            return None;
        }
        let (range, group) = range_and_group?;
        let selected_uid = selected_tablet.map(|tablet| tablet.tablet_uid);
        let tablet_uid = selected_uid.unwrap_or(0);
        let skipped_tablet_uid =
            self.collect_skipped_tablets(group, selected_uid, params.directed_read_options);

        let mut hint = RoutingHint::new()
            .set_operation_uid(params.operation_uid)
            .set_database_id(params.database_id)
            .set_key(range.start_key.clone())
            .set_limit_key(range.limit_key.clone())
            .set_group_uid(range.group_uid)
            .set_split_id(range.split_id)
            .set_tablet_uid(tablet_uid)
            .set_skipped_tablet_uid(skipped_tablet_uid);

        if let Some(schema_generation) = params.schema_generation {
            hint = hint.set_schema_generation(schema_generation);
        }

        if let Some(location) = params.client_location
            && !location.is_empty()
        {
            hint = hint.set_client_location(location);
        }

        Some(hint)
    }

    /// Resolves the optimal [`ServerConnection`] for the provided request routing context.
    pub(crate) fn resolve_connection(&self, context: &RoutingContext<'_>) -> ServerConnection {
        self.resolve_route(context, None, 0, None, 0, None)
            .connection
    }

    /// Generates a [`RoutingHint`] based on the provided routing context, active database ID,
    /// and schema generation version.
    ///
    /// Returns `None` if:
    /// - `database_id` is 0 (uninitialized or unknown server epoch).
    /// - `context.routing_key` is `None` or empty.
    /// - No covering range is found in [`KeyRangeCache`].
    pub(crate) fn create_routing_hint(
        &self,
        context: &RoutingContext<'_>,
        database_id: u64,
        schema_generation: Option<Bytes>,
        operation_uid: u64,
        client_location: Option<&str>,
    ) -> Option<RoutingHint> {
        self.resolve_route(
            context,
            None,
            database_id,
            schema_generation,
            operation_uid,
            client_location,
        )
        .routing_hint
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

    /// Returns `true` if the given address matches the default fallback gateway connection.
    pub(crate) fn is_default_endpoint(&self, address: &str) -> bool {
        self.connection_cache.is_default_address(address)
    }

    /// Helper to record an overload failure cooldown for an endpoint address.
    ///
    /// Skips placing the default fallback gateway on cooldown to ensure fallback routing remains viable.
    pub(crate) fn record_failure(&self, address: &str) -> Duration {
        self.record_failure_with_delay(address, None)
    }

    /// Helper to record an error with an optional server-recommended retry delay.
    ///
    /// Places the endpoint on cooldown if the error code is `RESOURCE_EXHAUSTED` or `UNAVAILABLE`.
    /// Skips placing the default fallback gateway on cooldown.
    pub(crate) fn record_cooldown_error_with_delay(
        &self,
        address: &str,
        status_code: Code,
        server_retry_delay: Option<Duration>,
    ) -> Option<Duration> {
        if self.is_default_endpoint(address) {
            return None;
        }
        self.cooldown_tracker
            .record_error_with_delay(address, status_code, server_retry_delay)
    }

    /// Helper to record an RPC failure without a server delay hint.
    ///
    /// Skips placing the default fallback gateway on cooldown.
    pub(crate) fn record_cooldown_error(
        &self,
        address: &str,
        status_code: Code,
    ) -> Option<Duration> {
        self.record_cooldown_error_with_delay(address, status_code, None)
    }

    /// Helper to record a failure with an optional server retry delay hint.
    ///
    /// Skips placing the default fallback gateway on cooldown.
    pub(crate) fn record_failure_with_delay(
        &self,
        address: &str,
        server_retry_delay: Option<Duration>,
    ) -> Duration {
        self.record_cooldown_error_with_delay(address, Code::ResourceExhausted, server_retry_delay)
            .unwrap_or(Duration::ZERO)
    }

    /// Helper to record a successful RPC completion, advancing failure tier repair for the endpoint.
    ///
    /// Skips recording for the default fallback gateway.
    pub(crate) fn record_success(&self, address: &str) {
        if self.is_default_endpoint(address) {
            return;
        }
        self.cooldown_tracker.record_success(address);
    }
}

#[cfg(test)]
mod golden_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::Channel;
    use crate::generated::gapic_dataplane::stub::Spanner as SpannerStub;
    use crate::model::directed_read_options::replica_selection::Type as ReplicaType;
    use crate::model::directed_read_options::{
        ExcludeReplicas, IncludeReplicas, ReplicaSelection, Replicas,
    };
    use crate::model::tablet::Role;
    use crate::model::{CacheUpdate, Group, Range, Tablet};
    use crate::routing::server_connection::ServerConnection;
    use gaxi::options::ClientConfig;
    use std::collections::HashSet;
    use std::time::{Duration, Instant};

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
        let endpoint_lifecycle_manager =
            Arc::new(EndpointLifecycleManager::new(Arc::clone(&connection_cache)));
        let cooldown_tracker = Arc::new(EndpointCooldownTracker::new());
        let latency_registry = Arc::new(LatencyRegistry::new());
        LocationRouter::new(
            "projects/test-project/instances/test-instance/databases/test-database".to_string(),
            key_range_cache,
            connection_cache,
            endpoint_lifecycle_manager,
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
        assert!(
            router.key_range_cache().is_empty(),
            "key range cache should initially be empty"
        );
        assert_eq!(
            router.connection_cache().len(),
            1,
            "connection cache should have default connection"
        );
        assert_eq!(
            router.affinity_count(),
            0,
            "affinity count should initially be 0"
        );
        assert_eq!(
            router.endpoint_lifecycle_manager().len(),
            0,
            "lifecycle manager should have no tracked endpoints"
        );
    }

    #[test]
    fn location_router_resolve_empty_cache_returns_default_connection() {
        let router = make_test_router();
        let context = RoutingContext::default();
        let connection = router.resolve_connection(&context);
        assert_eq!(
            connection.address(),
            "spanner.googleapis.com:443",
            "empty cache should route to default gateway"
        );
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
        assert_eq!(
            connection.address(),
            "10.0.0.1:15000",
            "should route to cached tablet connection"
        );
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
        assert_eq!(
            connection_first.address(),
            "10.0.0.1:15000",
            "first resolve should route to tablet"
        );
        assert_eq!(
            router.get_transaction_affinity(transaction_id).as_deref(),
            Some("10.0.0.1:15000"),
            "transaction affinity should be recorded"
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
        assert_eq!(
            connection_second.address(),
            "10.0.0.1:15000",
            "second resolve should use affinity"
        );
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
        assert_eq!(
            connection.address(),
            "10.0.0.1:15000",
            "should route to tablet without affinity"
        );
        assert_eq!(
            router.get_transaction_affinity(transaction_id),
            None,
            "read only query should not record affinity"
        );
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
        assert_eq!(
            connection.address(),
            "spanner.googleapis.com:443",
            "cooldown target should fall back to default gateway"
        );
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
        assert_eq!(
            connection.address(),
            "spanner.googleapis.com:443",
            "cooldown affinity target should fall back to default gateway"
        );
    }

    #[test]
    fn location_router_clear_transaction_affinity() {
        let router = make_test_router();
        router.record_transaction_affinity(b"tx1", "10.0.0.1:15000");
        assert_eq!(
            router.affinity_count(),
            1,
            "affinity count should be 1 after recording"
        );

        router.clear_transaction_affinity(b"tx1");
        assert_eq!(
            router.affinity_count(),
            0,
            "affinity count should be 0 after clearing"
        );
        assert_eq!(
            router.get_transaction_affinity(b"tx1"),
            None,
            "cleared transaction affinity should be None"
        );
    }

    #[test]
    fn location_router_ignores_empty_transaction_id() {
        let router = make_test_router();
        router.record_transaction_affinity(&[], "10.0.0.1:15000");
        assert_eq!(
            router.affinity_count(),
            0,
            "empty transaction id should not record affinity"
        );
        assert_eq!(
            router.get_transaction_affinity(&[]),
            None,
            "empty transaction id should return None"
        );
        router.clear_transaction_affinity(&[]);
        assert_eq!(router.affinity_count(), 0, "affinity count should remain 0");
    }

    #[test]
    fn location_router_affinity_explicit_cleanup() {
        let router = make_test_router();

        router.record_transaction_affinity(b"tx1", "10.0.0.1:15000");
        router.record_transaction_affinity(b"tx2", "10.0.0.2:15000");
        router.record_transaction_affinity(b"tx3", "10.0.0.3:15000");

        assert_eq!(router.affinity_count(), 3, "affinity count should be 3");
        assert_eq!(
            router.get_transaction_affinity(b"tx1").as_deref(),
            Some("10.0.0.1:15000"),
            "tx1 affinity address should match"
        );
        assert_eq!(
            router.get_transaction_affinity(b"tx2").as_deref(),
            Some("10.0.0.2:15000"),
            "tx2 affinity address should match"
        );

        router.clear_transaction_affinity(b"tx1");
        assert_eq!(
            router.affinity_count(),
            2,
            "affinity count should be 2 after clearing tx1"
        );
        assert_eq!(
            router.get_transaction_affinity(b"tx1"),
            None,
            "tx1 affinity should be None"
        );
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
        assert_eq!(
            connection.address(),
            "10.0.0.1:15000",
            "should route to leader connection"
        );
    }

    #[test]
    fn location_router_debug_formatting() {
        let router = make_test_router();
        let debug_str = format!("{:?}", router);
        assert!(
            debug_str.contains("LocationRouter"),
            "debug string should contain LocationRouter"
        );
        assert!(
            debug_str.contains("connection_cache"),
            "debug string should contain connection_cache"
        );
        assert!(
            debug_str.contains("cooldown_tracker"),
            "debug string should contain cooldown_tracker"
        );
    }

    #[test]
    fn location_router_record_cooldown_helper() {
        let router = make_test_router();
        router.record_failure("10.0.0.1:15000");
        assert!(
            router.cooldown_tracker().is_cooling_down("10.0.0.1:15000"),
            "endpoint must be cooling down after failure recorded"
        );
    }

    #[test]
    fn location_router_record_failure_skips_default_connection() {
        let router = make_test_router();
        let default_address = router.connection_cache.default_connection().address();

        assert!(
            router.is_default_endpoint(default_address),
            "is_default_endpoint must return true for default connection"
        );
        assert!(
            !router.is_default_endpoint("10.0.0.1:15000"),
            "is_default_endpoint must return false for tablet connection"
        );

        router.record_failure(default_address);
        assert!(
            !router.cooldown_tracker().is_cooling_down(default_address),
            "default connection must never be placed on cooldown"
        );

        assert_eq!(
            router.record_cooldown_error(default_address, Code::ResourceExhausted),
            None,
            "record_cooldown_error on default connection must return None"
        );
        assert_eq!(
            router.record_cooldown_error_with_delay(
                default_address,
                Code::Unavailable,
                Some(Duration::from_secs(5)),
            ),
            None,
            "record_cooldown_error_with_delay on default connection must return None"
        );
        assert_eq!(
            router.record_failure_with_delay(default_address, Some(Duration::from_secs(5))),
            Duration::ZERO,
            "record_failure_with_delay on default connection must return Duration::ZERO"
        );
        router.record_success(default_address);
        assert!(
            !router.cooldown_tracker().is_cooling_down(default_address),
            "default connection must remain not cooling down"
        );
    }

    #[test]
    fn location_router_record_error_helpers_delegate_for_tablets() {
        let router = make_test_router();
        let tablet_address = "10.0.0.2:15000";

        let cooldown = router.record_cooldown_error(tablet_address, Code::Unavailable);
        assert!(
            cooldown.is_some(),
            "record_cooldown_error must place tablet on cooldown for UNAVAILABLE"
        );
        assert!(
            router.cooldown_tracker().is_cooling_down(tablet_address),
            "tablet must be cooling down"
        );

        router.record_success(tablet_address);

        let tablet_address_2 = "10.0.0.3:15000";
        let hinted_cooldown = router.record_cooldown_error_with_delay(
            tablet_address_2,
            Code::ResourceExhausted,
            Some(Duration::from_millis(500)),
        );
        assert!(
            hinted_cooldown.is_some(),
            "record_cooldown_error_with_delay must place tablet on cooldown"
        );
        assert!(
            router.cooldown_tracker().is_cooling_down(tablet_address_2),
            "tablet must be cooling down after hinted error"
        );

        let failure_cooldown =
            router.record_failure_with_delay(tablet_address_2, Some(Duration::from_millis(600)));
        assert!(
            failure_cooldown >= Duration::from_millis(500),
            "cooldown must be at least hinted duration"
        );
    }

    #[test]
    fn create_routing_hint_returns_none_when_uninitialized() {
        let router = make_test_router();
        let key = vec![0x05];
        let context = RoutingContext {
            transaction_id: None,
            routing_key: Some(&key),
            prefer_leader: false,
            use_transaction_affinity: false,
        };

        // database_id == 0 -> None
        let hint = router.create_routing_hint(&context, 0, None, 1, None);
        assert!(
            hint.is_none(),
            "create_routing_hint must return None when database_id is 0"
        );

        // routing_key is None -> None
        let empty_context = RoutingContext::default();
        let hint = router.create_routing_hint(&empty_context, 100, None, 1, None);
        assert!(
            hint.is_none(),
            "create_routing_hint must return None when routing_key is None"
        );

        // empty routing_key slice -> None
        let empty_key_context = RoutingContext {
            routing_key: Some(&[]),
            ..Default::default()
        };
        let hint = router.create_routing_hint(&empty_key_context, 100, None, 1, None);
        assert!(
            hint.is_none(),
            "create_routing_hint must return None when routing_key is empty"
        );

        // cache miss -> None
        let hint = router.create_routing_hint(&context, 100, None, 1, None);
        assert!(
            hint.is_none(),
            "create_routing_hint must return None on key range cache miss"
        );
    }

    #[test]
    fn create_routing_hint_populates_all_fields_on_cache_hit() {
        let router = make_test_router();

        let tablet1 = Tablet::default()
            .set_tablet_uid(101u64)
            .set_server_address("10.0.0.1:15000")
            .set_incarnation(Bytes::from_static(b"inc-1"));
        let tablet2 = Tablet::default()
            .set_tablet_uid(102u64)
            .set_server_address("10.0.0.2:15000")
            .set_incarnation(Bytes::from_static(b"inc-2"));
        let group = Group::new()
            .set_group_uid(500u64)
            .set_leader_index(0)
            .set_tablets(vec![tablet1, tablet2]);
        let range = Range::new()
            .set_group_uid(500u64)
            .set_split_id(700u64)
            .set_start_key(vec![0x01, 0x00])
            .set_limit_key(vec![0x09, 0x00]);
        let update = CacheUpdate::new()
            .set_database_id(42u64)
            .set_group(vec![group])
            .set_range(vec![range]);

        router.key_range_cache().add_ranges(&update);

        let key = vec![0x05, 0x00];
        let context = RoutingContext {
            transaction_id: None,
            routing_key: Some(&key),
            prefer_leader: true,
            use_transaction_affinity: false,
        };

        let schema_generation = Bytes::from_static(b"schema-v1");
        let hint = router
            .create_routing_hint(
                &context,
                42,
                Some(schema_generation.clone()),
                100,
                Some("us-central1"),
            )
            .expect("routing hint must be generated on cache hit");

        assert_eq!(hint.operation_uid, 100, "operation_uid must match input");
        assert_eq!(hint.database_id, 42, "database_id must match");
        assert_eq!(
            hint.schema_generation, schema_generation,
            "schema_generation must match"
        );
        assert_eq!(
            hint.key.as_ref(),
            &[0x01, 0x00],
            "key must match range start_key"
        );
        assert_eq!(
            hint.limit_key.as_ref(),
            &[0x09, 0x00],
            "limit_key must match range limit_key"
        );
        assert_eq!(hint.group_uid, 500, "group_uid must match");
        assert_eq!(hint.split_id, 700, "split_id must match");
        assert_eq!(hint.tablet_uid, 101, "tablet_uid must match leader tablet");
        assert_eq!(
            hint.client_location, "us-central1",
            "client_location must match"
        );
        assert!(
            hint.skipped_tablet_uid.is_empty(),
            "no skipped tablets when all healthy"
        );
    }

    #[test]
    fn create_routing_hint_records_skipped_and_cooling_down_tablets() {
        let router = make_test_router();

        let tablet_healthy = Tablet::default()
            .set_tablet_uid(201u64)
            .set_server_address("10.0.0.1:15000")
            .set_incarnation(Bytes::from_static(b"inc-healthy"));
        let tablet_skipped = Tablet::default()
            .set_tablet_uid(202u64)
            .set_server_address("10.0.0.2:15000")
            .set_skip(true)
            .set_incarnation(Bytes::from_static(b"inc-skip"));
        let tablet_cooldown = Tablet::default()
            .set_tablet_uid(203u64)
            .set_server_address("10.0.0.3:15000")
            .set_incarnation(Bytes::from_static(b"inc-cool"));

        let group = Group::new()
            .set_group_uid(800u64)
            .set_leader_index(0)
            .set_tablets(vec![tablet_healthy, tablet_skipped, tablet_cooldown]);
        let range = Range::new()
            .set_group_uid(800u64)
            .set_split_id(900u64)
            .set_start_key(vec![0x10])
            .set_limit_key(vec![0x20]);
        let update = CacheUpdate::new()
            .set_database_id(99u64)
            .set_group(vec![group])
            .set_range(vec![range]);

        router.key_range_cache().add_ranges(&update);
        router.record_failure("10.0.0.3:15000");

        let key = vec![0x15];
        let context = RoutingContext {
            transaction_id: None,
            routing_key: Some(&key),
            prefer_leader: true,
            use_transaction_affinity: false,
        };

        let hint = router
            .create_routing_hint(&context, 99, None, 200, None)
            .expect("routing hint generated");

        assert_eq!(hint.tablet_uid, 201, "healthy leader must be selected");
        assert_eq!(
            hint.skipped_tablet_uid.len(),
            2,
            "both skipped and cooling down tablets must be reported in skipped_tablet_uid"
        );

        let skipped_uids: Vec<u64> = hint
            .skipped_tablet_uid
            .iter()
            .map(|skipped_tablet| skipped_tablet.tablet_uid)
            .collect();
        assert!(
            skipped_uids.contains(&202),
            "skipped tablet 202 must be included in skipped_tablet_uid"
        );
        assert!(
            skipped_uids.contains(&203),
            "cooling down tablet 203 must be included in skipped_tablet_uid"
        );
    }

    #[test]
    fn create_routing_hint_when_all_tablets_on_cooldown() {
        let router = make_test_router();

        let tablet1 = Tablet::default()
            .set_tablet_uid(301u64)
            .set_server_address("10.0.0.1:15000")
            .set_incarnation(Bytes::from_static(b"inc-1"));
        let tablet2 = Tablet::default()
            .set_tablet_uid(302u64)
            .set_server_address("10.0.0.2:15000")
            .set_incarnation(Bytes::from_static(b"inc-2"));

        let group = Group::new()
            .set_group_uid(1000u64)
            .set_leader_index(0)
            .set_tablets(vec![tablet1, tablet2]);
        let range = Range::new()
            .set_group_uid(1000u64)
            .set_split_id(1100u64)
            .set_start_key(vec![0x30])
            .set_limit_key(vec![0x40]);
        let update = CacheUpdate::new()
            .set_database_id(55u64)
            .set_group(vec![group])
            .set_range(vec![range]);

        router.key_range_cache().add_ranges(&update);
        router.record_failure("10.0.0.1:15000");
        router.record_failure("10.0.0.2:15000");

        let key = vec![0x35];
        let context = RoutingContext {
            transaction_id: None,
            routing_key: Some(&key),
            prefer_leader: false,
            use_transaction_affinity: false,
        };

        let hint = router
            .create_routing_hint(&context, 55, None, 300, None)
            .expect("routing hint must be generated even when all tablets are on cooldown");

        // When all tablets are cooling down, select_tablet returns None -> tablet_uid = 0
        assert_eq!(
            hint.tablet_uid, 0,
            "tablet_uid must be 0 when no eligible tablet is available"
        );
        assert_eq!(
            hint.skipped_tablet_uid.len(),
            2,
            "all cooling down tablets must be reported in skipped_tablet_uid"
        );
    }

    #[test]
    fn create_routing_hint_includes_empty_server_address_tablets_in_skipped() {
        let router = make_test_router();

        let tablet_healthy = Tablet::default()
            .set_tablet_uid(401u64)
            .set_server_address("10.0.0.1:15000")
            .set_incarnation(Bytes::from_static(b"inc-1"));
        let tablet_empty_addr = Tablet::default()
            .set_tablet_uid(402u64)
            .set_server_address("") // empty address
            .set_incarnation(Bytes::from_static(b"inc-empty"));

        let group = Group::new()
            .set_group_uid(1200u64)
            .set_leader_index(0)
            .set_tablets(vec![tablet_healthy, tablet_empty_addr]);
        let range = Range::new()
            .set_group_uid(1200u64)
            .set_split_id(1300u64)
            .set_start_key(vec![0x50])
            .set_limit_key(vec![0x60]);
        let update = CacheUpdate::new()
            .set_database_id(66u64)
            .set_group(vec![group])
            .set_range(vec![range]);

        router.key_range_cache().add_ranges(&update);

        let key = vec![0x55];
        let context = RoutingContext {
            transaction_id: None,
            routing_key: Some(&key),
            prefer_leader: true,
            use_transaction_affinity: false,
        };

        let hint = router
            .create_routing_hint(&context, 66, None, 400, None)
            .expect("routing hint generated");

        assert_eq!(hint.tablet_uid, 401, "healthy leader tablet must be chosen");
        assert_eq!(
            hint.skipped_tablet_uid.len(),
            1,
            "empty address tablet must be recorded as skipped"
        );
        assert_eq!(
            hint.skipped_tablet_uid[0].tablet_uid, 402,
            "tablet with empty server_address must be in skipped_tablet_uid"
        );
    }

    #[tokio::test]
    async fn resolve_route_unifies_connection_and_routing_hint() {
        let router = make_test_router();
        populate_test_routing_table(&router, "10.0.0.1:15000", vec![0x70], vec![0x80]).await;

        let key = vec![0x75];
        let context = RoutingContext {
            transaction_id: None,
            routing_key: Some(&key),
            prefer_leader: true,
            use_transaction_affinity: false,
        };

        let route = router.resolve_route(&context, None, 77, None, 500, Some("us-central1"));
        assert_eq!(
            route.connection.address(),
            "10.0.0.1:15000",
            "connection must match tablet server address"
        );

        let hint = route
            .routing_hint
            .expect("routing hint must be present in resolved route");
        assert_eq!(
            hint.tablet_uid, 10,
            "routing hint tablet_uid must match resolved connection"
        );
        assert_eq!(hint.operation_uid, 500, "operation_uid must match");
        assert_eq!(hint.database_id, 77, "database_id must match");
    }

    #[tokio::test]
    async fn resolve_route_falls_back_to_healthy_follower_when_leader_on_cooldown() {
        let router = make_test_router();

        let leader_tablet = Tablet::default()
            .set_tablet_uid(501u64)
            .set_server_address("10.0.0.1:15000")
            .set_distance(0u32)
            .set_incarnation(Bytes::from_static(b"inc-leader"));
        let follower_tablet = Tablet::default()
            .set_tablet_uid(502u64)
            .set_server_address("10.0.0.2:15000")
            .set_distance(0u32)
            .set_incarnation(Bytes::from_static(b"inc-follower"));

        let group = Group::new()
            .set_group_uid(1500u64)
            .set_leader_index(0)
            .set_tablets(vec![leader_tablet, follower_tablet]);
        let range = Range::new()
            .set_group_uid(1500u64)
            .set_split_id(1600u64)
            .set_start_key(vec![0x80])
            .set_limit_key(vec![0x90]);
        let update = CacheUpdate::new()
            .set_database_id(88u64)
            .set_group(vec![group])
            .set_range(vec![range]);

        router.key_range_cache().add_ranges(&update);
        let _ = router
            .connection_cache()
            .get("10.0.0.2:15000", &ClientConfig::default())
            .await
            .expect("should initialize connection");

        // Put leader on cooldown
        router.record_failure("10.0.0.1:15000");

        let key = vec![0x85];
        let context = RoutingContext {
            transaction_id: None,
            routing_key: Some(&key),
            prefer_leader: true,
            use_transaction_affinity: false,
        };

        let route = router.resolve_route(&context, None, 88, None, 600, None);
        assert_eq!(
            route.connection.address(),
            "10.0.0.2:15000",
            "must fall back to healthy follower connection when leader is on cooldown"
        );

        let hint = route
            .routing_hint
            .expect("routing hint must be present in resolved route");
        assert_eq!(
            hint.tablet_uid, 502,
            "routing hint must target the healthy follower tablet"
        );
        assert_eq!(
            hint.skipped_tablet_uid.len(),
            1,
            "cooling down leader must be recorded in skipped_tablet_uid"
        );
        assert_eq!(
            hint.skipped_tablet_uid[0].tablet_uid, 501,
            "skipped tablet must be the cooling down leader"
        );
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

        // Leader is cooling down: prefer_leader request falls back to healthy follower replica with P2C
        let connection_write = router.resolve_connection(&context_write);
        assert_eq!(
            connection_write.address(),
            "10.0.0.2:15000",
            "request preferring leader must fall back to fast follower when leader is on cooldown"
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
    async fn select_healthy_leader_falls_back_to_gateway_when_group_has_no_leader_and_no_routable_followers()
     {
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
        router.record_failure("10.0.0.1:15000");

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
            "must fall back to default gateway when leader is requested but no leader exists and followers are cooling down"
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

    #[tokio::test]
    async fn location_router_directed_read_include_replicas_selects_matching_replica() {
        let router = make_test_router();

        let tablet_central = Tablet::default()
            .set_tablet_uid(101u64)
            .set_server_address("10.0.0.1:15000")
            .set_location("us-central1")
            .set_role(Role::ReadOnly)
            .set_distance(1u32);
        let tablet_east = Tablet::default()
            .set_tablet_uid(102u64)
            .set_server_address("10.0.0.2:15000")
            .set_location("us-east1")
            .set_role(Role::ReadOnly)
            .set_distance(1u32);

        let group = Group::new()
            .set_group_uid(200u64)
            .set_tablets(vec![tablet_central, tablet_east]);
        let range = Range::new()
            .set_group_uid(200u64)
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
            .expect("initialize central connection");
        let _ = router
            .connection_cache()
            .get("10.0.0.2:15000", &ClientConfig::default())
            .await
            .expect("initialize east connection");

        let directed_read_options = DirectedReadOptions {
            replicas: Some(Replicas::IncludeReplicas(Box::new(IncludeReplicas {
                replica_selections: vec![ReplicaSelection {
                    location: "us-east1".to_string(),
                    r#type: ReplicaType::ReadOnly,
                    ..Default::default()
                }],
                auto_failover_disabled: false,
                ..Default::default()
            }))),
            ..Default::default()
        };

        let key = vec![0x05];
        let context = RoutingContext {
            transaction_id: None,
            routing_key: Some(&key),
            prefer_leader: false,
            use_transaction_affinity: false,
        };

        let route = router.resolve_route(&context, Some(&directed_read_options), 1, None, 10, None);
        assert_eq!(
            route.connection.address(),
            "10.0.0.2:15000",
            "directed read targeting us-east1 must route to the east replica"
        );
        let hint = route
            .routing_hint
            .expect("routing hint must be generated on cache hit");
        assert_eq!(
            hint.tablet_uid, 102,
            "routing hint must record the matched east tablet UID"
        );
    }

    #[tokio::test]
    async fn location_router_directed_read_exclude_replicas_filters_excluded() {
        let router = make_test_router();

        let tablet_central = Tablet::default()
            .set_tablet_uid(201u64)
            .set_server_address("10.0.0.1:15000")
            .set_location("us-central1")
            .set_role(Role::ReadOnly)
            .set_distance(1u32);
        let tablet_east = Tablet::default()
            .set_tablet_uid(202u64)
            .set_server_address("10.0.0.2:15000")
            .set_location("us-east1")
            .set_role(Role::ReadOnly)
            .set_distance(1u32);

        let group = Group::new()
            .set_group_uid(300u64)
            .set_tablets(vec![tablet_central, tablet_east]);
        let range = Range::new()
            .set_group_uid(300u64)
            .set_start_key(vec![0x01])
            .set_limit_key(vec![0x09]);
        let update = CacheUpdate::new()
            .set_database_id(2u64)
            .set_group(vec![group])
            .set_range(vec![range]);

        router.key_range_cache().add_ranges(&update);

        let _ = router
            .connection_cache()
            .get("10.0.0.1:15000", &ClientConfig::default())
            .await
            .expect("initialize central connection");
        let _ = router
            .connection_cache()
            .get("10.0.0.2:15000", &ClientConfig::default())
            .await
            .expect("initialize east connection");

        let directed_read_options = DirectedReadOptions {
            replicas: Some(Replicas::ExcludeReplicas(Box::new(ExcludeReplicas {
                replica_selections: vec![ReplicaSelection {
                    location: "us-central1".to_string(),
                    ..Default::default()
                }],
                ..Default::default()
            }))),
            ..Default::default()
        };

        let key = vec![0x05];
        let context = RoutingContext {
            transaction_id: None,
            routing_key: Some(&key),
            prefer_leader: false,
            use_transaction_affinity: false,
        };

        let route = router.resolve_route(&context, Some(&directed_read_options), 2, None, 20, None);
        assert_eq!(
            route.connection.address(),
            "10.0.0.2:15000",
            "directed read excluding us-central1 must route to the east replica"
        );
        let hint = route
            .routing_hint
            .expect("routing hint must be generated on cache hit");
        assert_eq!(
            hint.tablet_uid, 202,
            "routing hint must record the matched east tablet UID"
        );
    }

    #[tokio::test]
    async fn location_router_directed_read_selects_lowest_distance_among_matching() {
        let router = make_test_router();

        let tablet_europe_1 = Tablet::default()
            .set_tablet_uid(301u64)
            .set_server_address("10.0.0.1:15000")
            .set_location("europe-west1")
            .set_role(Role::ReadOnly)
            .set_distance(10u32);
        let tablet_europe_4 = Tablet::default()
            .set_tablet_uid(302u64)
            .set_server_address("10.0.0.2:15000")
            .set_location("europe-west4")
            .set_role(Role::ReadOnly)
            .set_distance(20u32);
        let tablet_us_central = Tablet::default()
            .set_tablet_uid(303u64)
            .set_server_address("10.0.0.3:15000")
            .set_location("us-central1")
            .set_role(Role::ReadOnly)
            .set_distance(1u32);

        let group = Group::new().set_group_uid(400u64).set_tablets(vec![
            tablet_europe_1,
            tablet_europe_4,
            tablet_us_central,
        ]);
        let range = Range::new()
            .set_group_uid(400u64)
            .set_start_key(vec![0x01])
            .set_limit_key(vec![0x09]);
        let update = CacheUpdate::new()
            .set_database_id(3u64)
            .set_group(vec![group])
            .set_range(vec![range]);

        router.key_range_cache().add_ranges(&update);

        let _ = router
            .connection_cache()
            .get("10.0.0.1:15000", &ClientConfig::default())
            .await
            .expect("initialize europe-west1 connection");
        let _ = router
            .connection_cache()
            .get("10.0.0.2:15000", &ClientConfig::default())
            .await
            .expect("initialize europe-west4 connection");
        let _ = router
            .connection_cache()
            .get("10.0.0.3:15000", &ClientConfig::default())
            .await
            .expect("initialize us-central1 connection");

        let directed_read_options = DirectedReadOptions {
            replicas: Some(Replicas::IncludeReplicas(Box::new(IncludeReplicas {
                replica_selections: vec![
                    ReplicaSelection {
                        location: "europe-west1".to_string(),
                        ..Default::default()
                    },
                    ReplicaSelection {
                        location: "europe-west4".to_string(),
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }))),
            ..Default::default()
        };

        let key = vec![0x05];
        let context = RoutingContext {
            transaction_id: None,
            routing_key: Some(&key),
            prefer_leader: false,
            use_transaction_affinity: false,
        };

        let route = router.resolve_route(&context, Some(&directed_read_options), 3, None, 30, None);
        assert_eq!(
            route.connection.address(),
            "10.0.0.1:15000",
            "directed read must pick europe-west1 (distance 10) over europe-west4 (distance 20)"
        );
        let hint = route
            .routing_hint
            .expect("routing hint must be generated on cache hit");
        assert_eq!(
            hint.tablet_uid, 301,
            "routing hint must record the closest matched tablet UID"
        );
    }

    #[tokio::test]
    async fn location_router_directed_read_selects_leader_when_matching_read_write() {
        let router = make_test_router();

        let leader_tablet = Tablet::default()
            .set_tablet_uid(401u64)
            .set_server_address("10.0.0.1:15000")
            .set_location("us-central1")
            .set_role(Role::ReadWrite)
            .set_distance(1u32);
        let follower_tablet = Tablet::default()
            .set_tablet_uid(402u64)
            .set_server_address("10.0.0.2:15000")
            .set_location("us-central1")
            .set_role(Role::ReadOnly)
            .set_distance(1u32);

        let group = Group::new()
            .set_group_uid(500u64)
            .set_leader_index(0)
            .set_tablets(vec![leader_tablet, follower_tablet]);
        let range = Range::new()
            .set_group_uid(500u64)
            .set_start_key(vec![0x01])
            .set_limit_key(vec![0x09]);
        let update = CacheUpdate::new()
            .set_database_id(4u64)
            .set_group(vec![group])
            .set_range(vec![range]);

        router.key_range_cache().add_ranges(&update);

        let _ = router
            .connection_cache()
            .get("10.0.0.1:15000", &ClientConfig::default())
            .await
            .expect("initialize leader connection");
        let _ = router
            .connection_cache()
            .get("10.0.0.2:15000", &ClientConfig::default())
            .await
            .expect("initialize follower connection");

        let directed_read_options = DirectedReadOptions {
            replicas: Some(Replicas::IncludeReplicas(Box::new(IncludeReplicas {
                replica_selections: vec![ReplicaSelection {
                    location: "us-central1".to_string(),
                    r#type: ReplicaType::ReadWrite,
                    ..Default::default()
                }],
                ..Default::default()
            }))),
            ..Default::default()
        };

        let key = vec![0x05];
        let context = RoutingContext {
            transaction_id: None,
            routing_key: Some(&key),
            prefer_leader: true,
            use_transaction_affinity: false,
        };

        let route = router.resolve_route(&context, Some(&directed_read_options), 4, None, 40, None);
        assert_eq!(
            route.connection.address(),
            "10.0.0.1:15000",
            "leader must be selected when directed read options specifically request ReadWrite replica"
        );
        let hint = route
            .routing_hint
            .expect("routing hint must be generated on cache hit");
        assert_eq!(
            hint.tablet_uid, 401,
            "routing hint must record the leader tablet UID"
        );
    }

    #[tokio::test]
    async fn location_router_directed_read_rejects_leader_when_read_only_specified() {
        let router = make_test_router();

        let leader_tablet = Tablet::default()
            .set_tablet_uid(501u64)
            .set_server_address("10.0.0.1:15000")
            .set_location("us-central1")
            .set_role(Role::ReadWrite)
            .set_distance(1u32);
        let follower_tablet = Tablet::default()
            .set_tablet_uid(502u64)
            .set_server_address("10.0.0.2:15000")
            .set_location("us-central1")
            .set_role(Role::ReadOnly)
            .set_distance(1u32);

        let group = Group::new()
            .set_group_uid(600u64)
            .set_leader_index(0)
            .set_tablets(vec![leader_tablet, follower_tablet]);
        let range = Range::new()
            .set_group_uid(600u64)
            .set_start_key(vec![0x01])
            .set_limit_key(vec![0x09]);
        let update = CacheUpdate::new()
            .set_database_id(5u64)
            .set_group(vec![group])
            .set_range(vec![range]);

        router.key_range_cache().add_ranges(&update);

        let _ = router
            .connection_cache()
            .get("10.0.0.1:15000", &ClientConfig::default())
            .await
            .expect("initialize leader connection");
        let _ = router
            .connection_cache()
            .get("10.0.0.2:15000", &ClientConfig::default())
            .await
            .expect("initialize follower connection");

        // DirectedReadOptions specifies ReadOnly: leader is ReadWrite, so leader is rejected
        let directed_read_options = DirectedReadOptions {
            replicas: Some(Replicas::IncludeReplicas(Box::new(IncludeReplicas {
                replica_selections: vec![ReplicaSelection {
                    location: "us-central1".to_string(),
                    r#type: ReplicaType::ReadOnly,
                    ..Default::default()
                }],
                ..Default::default()
            }))),
            ..Default::default()
        };

        let key = vec![0x05];
        let context = RoutingContext {
            transaction_id: None,
            routing_key: Some(&key),
            prefer_leader: true,
            use_transaction_affinity: false,
        };

        let route = router.resolve_route(&context, Some(&directed_read_options), 5, None, 50, None);
        assert_eq!(
            route.connection.address(),
            "10.0.0.2:15000",
            "ReadWrite leader must be rejected when options require ReadOnly; must select matching follower"
        );
        let hint = route
            .routing_hint
            .expect("routing hint must be generated on cache hit");
        assert_eq!(
            hint.tablet_uid, 502,
            "routing hint must record the follower tablet UID"
        );
    }

    #[tokio::test]
    async fn location_router_directed_read_exclude_replicas_applies_locality_rule_for_leader() {
        let router = make_test_router();

        // Designated leader is in europe-west1 (distance 10, remote)
        let remote_leader = Tablet::default()
            .set_tablet_uid(551u64)
            .set_server_address("10.0.0.1:15000")
            .set_location("europe-west1")
            .set_role(Role::ReadWrite)
            .set_distance(10u32);
        // Follower is in us-central1 (distance 1, local)
        let local_follower = Tablet::default()
            .set_tablet_uid(552u64)
            .set_server_address("10.0.0.2:15000")
            .set_location("us-central1")
            .set_role(Role::ReadOnly)
            .set_distance(1u32);

        let group = Group::new()
            .set_group_uid(650u64)
            .set_leader_index(0)
            .set_tablets(vec![remote_leader, local_follower]);
        let range = Range::new()
            .set_group_uid(650u64)
            .set_start_key(vec![0x01])
            .set_limit_key(vec![0x09]);
        let update = CacheUpdate::new()
            .set_database_id(55u64)
            .set_group(vec![group])
            .set_range(vec![range]);

        router.key_range_cache().add_ranges(&update);

        let _ = router
            .connection_cache()
            .get("10.0.0.1:15000", &ClientConfig::default())
            .await
            .expect("initialize remote leader connection");
        let _ = router
            .connection_cache()
            .get("10.0.0.2:15000", &ClientConfig::default())
            .await
            .expect("initialize local follower connection");

        // ExcludeReplicas excludes us-east1.
        // Even though remote leader is not excluded, locality rules apply:
        // remote leader is NOT local, so it must not be selected. Local follower must be selected instead!
        let directed_read_options = DirectedReadOptions {
            replicas: Some(Replicas::ExcludeReplicas(Box::new(ExcludeReplicas {
                replica_selections: vec![ReplicaSelection {
                    location: "us-east1".to_string(),
                    ..Default::default()
                }],
                ..Default::default()
            }))),
            ..Default::default()
        };

        let key = vec![0x05];
        let context = RoutingContext {
            transaction_id: None,
            routing_key: Some(&key),
            prefer_leader: true,
            use_transaction_affinity: false,
        };

        let route =
            router.resolve_route(&context, Some(&directed_read_options), 55, None, 55, None);
        assert_eq!(
            route.connection.address(),
            "10.0.0.2:15000",
            "ExcludeReplicas must preserve locality rules: remote leader must not be selected over local follower"
        );
        let hint = route
            .routing_hint
            .expect("routing hint must be generated on cache hit");
        assert_eq!(
            hint.tablet_uid, 552,
            "routing hint must record the local follower tablet UID"
        );
    }

    #[tokio::test]
    async fn location_router_directed_read_no_matching_replicas_falls_back_to_gateway() {
        let router = make_test_router();

        let tablet_central = Tablet::default()
            .set_tablet_uid(601u64)
            .set_server_address("10.0.0.1:15000")
            .set_location("us-central1")
            .set_role(Role::ReadOnly)
            .set_distance(1u32);

        let group = Group::new()
            .set_group_uid(700u64)
            .set_tablets(vec![tablet_central]);
        let range = Range::new()
            .set_group_uid(700u64)
            .set_start_key(vec![0x01])
            .set_limit_key(vec![0x09]);
        let update = CacheUpdate::new()
            .set_database_id(6u64)
            .set_group(vec![group])
            .set_range(vec![range]);

        router.key_range_cache().add_ranges(&update);

        let _ = router
            .connection_cache()
            .get("10.0.0.1:15000", &ClientConfig::default())
            .await
            .expect("initialize central connection");

        // Requested location asia-east1 does not exist in the cached group
        let directed_read_options = DirectedReadOptions {
            replicas: Some(Replicas::IncludeReplicas(Box::new(IncludeReplicas {
                replica_selections: vec![ReplicaSelection {
                    location: "asia-east1".to_string(),
                    ..Default::default()
                }],
                ..Default::default()
            }))),
            ..Default::default()
        };

        let key = vec![0x05];
        let context = RoutingContext {
            transaction_id: None,
            routing_key: Some(&key),
            prefer_leader: false,
            use_transaction_affinity: false,
        };

        let route = router.resolve_route(&context, Some(&directed_read_options), 6, None, 60, None);
        assert_eq!(
            route.connection.address(),
            "spanner.googleapis.com:443",
            "must fall back to default gateway when no replicas match directed read options"
        );
        let hint = route
            .routing_hint
            .expect("routing hint must be generated on cache hit");
        assert_eq!(
            hint.tablet_uid, 0,
            "tablet_uid must be 0 when falling back to gateway"
        );
    }

    #[tokio::test]
    async fn location_router_directed_read_cooling_down_replica_fails_over_to_next_tier() {
        let router = make_test_router();

        let tablet_europe_1 = Tablet::default()
            .set_tablet_uid(701u64)
            .set_server_address("10.0.0.1:15000")
            .set_location("europe-west1")
            .set_role(Role::ReadOnly)
            .set_distance(10u32);
        let tablet_europe_4 = Tablet::default()
            .set_tablet_uid(702u64)
            .set_server_address("10.0.0.2:15000")
            .set_location("europe-west4")
            .set_role(Role::ReadOnly)
            .set_distance(20u32);

        let group = Group::new()
            .set_group_uid(800u64)
            .set_tablets(vec![tablet_europe_1, tablet_europe_4]);
        let range = Range::new()
            .set_group_uid(800u64)
            .set_start_key(vec![0x01])
            .set_limit_key(vec![0x09]);
        let update = CacheUpdate::new()
            .set_database_id(7u64)
            .set_group(vec![group])
            .set_range(vec![range]);

        router.key_range_cache().add_ranges(&update);

        let _ = router
            .connection_cache()
            .get("10.0.0.1:15000", &ClientConfig::default())
            .await
            .expect("initialize europe-west1 connection");
        let _ = router
            .connection_cache()
            .get("10.0.0.2:15000", &ClientConfig::default())
            .await
            .expect("initialize europe-west4 connection");

        let directed_read_options = DirectedReadOptions {
            replicas: Some(Replicas::IncludeReplicas(Box::new(IncludeReplicas {
                replica_selections: vec![
                    ReplicaSelection {
                        location: "europe-west1".to_string(),
                        ..Default::default()
                    },
                    ReplicaSelection {
                        location: "europe-west4".to_string(),
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }))),
            ..Default::default()
        };

        let key = vec![0x05];
        let context = RoutingContext {
            transaction_id: None,
            routing_key: Some(&key),
            prefer_leader: false,
            use_transaction_affinity: false,
        };

        // Place europe-west1 on cooldown
        router.record_failure("10.0.0.1:15000");

        let route = router.resolve_route(&context, Some(&directed_read_options), 7, None, 70, None);
        assert_eq!(
            route.connection.address(),
            "10.0.0.2:15000",
            "must fail over to europe-west4 (distance 20) when europe-west1 (distance 10) is cooling down"
        );
        let hint = route
            .routing_hint
            .expect("routing hint must be generated on cache hit");
        assert_eq!(
            hint.tablet_uid, 702,
            "routing hint must record the failover tablet UID"
        );
        assert!(
            hint.skipped_tablet_uid
                .iter()
                .any(|skipped| skipped.tablet_uid == 701),
            "cooling down tablet 701 must be recorded in skipped_tablet_uid"
        );

        // Place europe-west4 on cooldown as well
        router.record_failure("10.0.0.2:15000");

        let route_fallback =
            router.resolve_route(&context, Some(&directed_read_options), 7, None, 71, None);
        assert_eq!(
            route_fallback.connection.address(),
            "spanner.googleapis.com:443",
            "must fall back to default gateway when all matching replicas are on cooldown"
        );
    }

    #[tokio::test]
    async fn location_router_directed_read_prioritizes_prewarmed_connection() {
        let router = make_test_router();

        let tablet_warmed = Tablet::default()
            .set_tablet_uid(801u64)
            .set_server_address("10.0.0.1:15000")
            .set_location("us-east1")
            .set_role(Role::ReadOnly)
            .set_distance(1u32);
        let tablet_unwarmed = Tablet::default()
            .set_tablet_uid(802u64)
            .set_server_address("10.0.0.2:15000")
            .set_location("us-east1")
            .set_role(Role::ReadOnly)
            .set_distance(1u32);

        let group = Group::new()
            .set_group_uid(900u64)
            .set_tablets(vec![tablet_warmed, tablet_unwarmed]);
        let range = Range::new()
            .set_group_uid(900u64)
            .set_start_key(vec![0x01])
            .set_limit_key(vec![0x09]);
        let update = CacheUpdate::new()
            .set_database_id(8u64)
            .set_group(vec![group])
            .set_range(vec![range]);

        router.key_range_cache().add_ranges(&update);

        // Only pre-warm tablet 801
        let _ = router
            .connection_cache()
            .get("10.0.0.1:15000", &ClientConfig::default())
            .await
            .expect("initialize warmed connection");

        let directed_read_options = DirectedReadOptions {
            replicas: Some(Replicas::IncludeReplicas(Box::new(IncludeReplicas {
                replica_selections: vec![ReplicaSelection {
                    location: "us-east1".to_string(),
                    ..Default::default()
                }],
                ..Default::default()
            }))),
            ..Default::default()
        };

        let key = vec![0x05];
        let context = RoutingContext {
            transaction_id: None,
            routing_key: Some(&key),
            prefer_leader: false,
            use_transaction_affinity: false,
        };

        for _ in 0..10 {
            let route =
                router.resolve_route(&context, Some(&directed_read_options), 8, None, 80, None);
            assert_eq!(
                route.connection.address(),
                "10.0.0.1:15000",
                "must prioritize the pre-warmed matching replica"
            );
            assert_eq!(
                route.routing_hint.as_ref().map(|hint| hint.tablet_uid),
                Some(801),
                "routing hint must record the pre-warmed tablet UID"
            );
        }
    }

    #[tokio::test]
    async fn location_router_directed_read_does_not_prefer_leader_over_matching_followers() {
        let router = make_test_router();

        // Designated leader is in us-central1 (distance 10, ReadWrite)
        let remote_leader = Tablet::default()
            .set_tablet_uid(1001u64)
            .set_server_address("10.0.0.1:15000")
            .set_location("us-central1")
            .set_role(Role::ReadWrite)
            .set_distance(10u32);
        // Follower is in us-east1 (distance 1, ReadOnly)
        let local_follower = Tablet::default()
            .set_tablet_uid(1002u64)
            .set_server_address("10.0.0.2:15000")
            .set_location("us-east1")
            .set_role(Role::ReadOnly)
            .set_distance(1u32);

        let group = Group::new()
            .set_group_uid(1100u64)
            .set_leader_index(0)
            .set_tablets(vec![remote_leader, local_follower]);
        let range = Range::new()
            .set_group_uid(1100u64)
            .set_start_key(vec![0x01])
            .set_limit_key(vec![0x09]);
        let update = CacheUpdate::new()
            .set_database_id(10u64)
            .set_group(vec![group])
            .set_range(vec![range]);

        router.key_range_cache().add_ranges(&update);

        let _ = router
            .connection_cache()
            .get("10.0.0.1:15000", &ClientConfig::default())
            .await
            .expect("initialize remote leader connection");
        let _ = router
            .connection_cache()
            .get("10.0.0.2:15000", &ClientConfig::default())
            .await
            .expect("initialize local follower connection");

        // Directed read includes both us-central1 and us-east1 without specifying replica type.
        // Even with prefer_leader: true, leader preference is disabled when directed read options are present.
        // The router must select the closer us-east1 replica (distance 1) rather than the leader (distance 10).
        let directed_read_options = DirectedReadOptions {
            replicas: Some(Replicas::IncludeReplicas(Box::new(IncludeReplicas {
                replica_selections: vec![
                    ReplicaSelection {
                        location: "us-central1".to_string(),
                        ..Default::default()
                    },
                    ReplicaSelection {
                        location: "us-east1".to_string(),
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }))),
            ..Default::default()
        };

        let key = vec![0x05];
        let context = RoutingContext {
            transaction_id: None,
            routing_key: Some(&key),
            prefer_leader: true,
            use_transaction_affinity: false,
        };

        let route =
            router.resolve_route(&context, Some(&directed_read_options), 10, None, 100, None);
        assert_eq!(
            route.connection.address(),
            "10.0.0.2:15000",
            "directed read must disable leader preference and pick closest replica matching options"
        );
        let hint = route
            .routing_hint
            .expect("routing hint must be generated on cache hit");
        assert_eq!(
            hint.tablet_uid, 1002,
            "routing hint must record the local follower tablet UID"
        );
    }

    #[tokio::test]
    async fn location_router_directed_read_skipped_tablets_only_includes_matching() {
        let router = make_test_router();

        // Tablet in us-east1 (healthy)
        let tablet_east_healthy = Tablet::default()
            .set_tablet_uid(1101u64)
            .set_server_address("10.0.0.1:15000")
            .set_location("us-east1")
            .set_role(Role::ReadOnly)
            .set_distance(1u32);
        // Tablet in us-east1 (on cooldown)
        let tablet_east_cooling = Tablet::default()
            .set_tablet_uid(1102u64)
            .set_server_address("10.0.0.2:15000")
            .set_location("us-east1")
            .set_role(Role::ReadOnly)
            .set_distance(1u32);
        // Tablet in europe-west1 (on cooldown, does NOT match directed read)
        let tablet_europe_cooling = Tablet::default()
            .set_tablet_uid(1103u64)
            .set_server_address("10.0.0.3:15000")
            .set_location("europe-west1")
            .set_role(Role::ReadOnly)
            .set_distance(10u32);

        let group = Group::new().set_group_uid(1200u64).set_tablets(vec![
            tablet_east_healthy,
            tablet_east_cooling,
            tablet_europe_cooling,
        ]);
        let range = Range::new()
            .set_group_uid(1200u64)
            .set_start_key(vec![0x01])
            .set_limit_key(vec![0x09]);
        let update = CacheUpdate::new()
            .set_database_id(11u64)
            .set_group(vec![group])
            .set_range(vec![range]);

        router.key_range_cache().add_ranges(&update);

        let _ = router
            .connection_cache()
            .get("10.0.0.1:15000", &ClientConfig::default())
            .await
            .expect("initialize east healthy connection");
        let _ = router
            .connection_cache()
            .get("10.0.0.2:15000", &ClientConfig::default())
            .await
            .expect("initialize east cooling connection");
        let _ = router
            .connection_cache()
            .get("10.0.0.3:15000", &ClientConfig::default())
            .await
            .expect("initialize europe cooling connection");

        // Mark both 10.0.0.2 and 10.0.0.3 as cooling down
        router.record_failure("10.0.0.2:15000");
        router.record_failure("10.0.0.3:15000");

        let directed_read_options = DirectedReadOptions {
            replicas: Some(Replicas::IncludeReplicas(Box::new(IncludeReplicas {
                replica_selections: vec![ReplicaSelection {
                    location: "us-east1".to_string(),
                    ..Default::default()
                }],
                ..Default::default()
            }))),
            ..Default::default()
        };

        let key = vec![0x05];
        let context = RoutingContext {
            transaction_id: None,
            routing_key: Some(&key),
            prefer_leader: false,
            use_transaction_affinity: false,
        };

        let route =
            router.resolve_route(&context, Some(&directed_read_options), 11, None, 110, None);
        assert_eq!(
            route.connection.address(),
            "10.0.0.1:15000",
            "must route to healthy us-east1 replica"
        );
        let hint = route
            .routing_hint
            .expect("routing hint must be generated on cache hit");
        assert_eq!(
            hint.tablet_uid, 1101,
            "routing hint must record healthy tablet UID"
        );

        // Skipped tablets must ONLY include matching tablet 1102, and MUST NOT include 1103 (europe-west1)
        let skipped_uids: Vec<u64> = hint
            .skipped_tablet_uid
            .iter()
            .map(|skipped_tablet| skipped_tablet.tablet_uid)
            .collect();
        assert!(
            skipped_uids.contains(&1102),
            "matching cooling-down tablet 1102 must be in skipped_tablet_uid"
        );
        assert!(
            !skipped_uids.contains(&1103),
            "non-matching cooling-down tablet 1103 must NOT be in skipped_tablet_uid"
        );
    }

    #[tokio::test]
    async fn location_router_directed_read_clears_stale_buffer_on_lower_distance() {
        let router = make_test_router();

        // Farther replica first in group (distance 20)
        let tablet_farther = Tablet::default()
            .set_tablet_uid(1201u64)
            .set_server_address("10.0.0.1:15000")
            .set_location("us-east1")
            .set_role(Role::ReadOnly)
            .set_distance(20u32);
        // Closer replica second in group (distance 1)
        let tablet_closer = Tablet::default()
            .set_tablet_uid(1202u64)
            .set_server_address("10.0.0.2:15000")
            .set_location("us-east1")
            .set_role(Role::ReadOnly)
            .set_distance(1u32);

        let group = Group::new()
            .set_group_uid(1300u64)
            .set_tablets(vec![tablet_farther, tablet_closer]);
        let range = Range::new()
            .set_group_uid(1300u64)
            .set_start_key(vec![0x01])
            .set_limit_key(vec![0x09]);
        let update = CacheUpdate::new()
            .set_database_id(12u64)
            .set_group(vec![group])
            .set_range(vec![range]);

        router.key_range_cache().add_ranges(&update);

        let _ = router
            .connection_cache()
            .get("10.0.0.1:15000", &ClientConfig::default())
            .await
            .expect("initialize farther connection");
        let _ = router
            .connection_cache()
            .get("10.0.0.2:15000", &ClientConfig::default())
            .await
            .expect("initialize closer connection");

        let directed_read_options = DirectedReadOptions {
            replicas: Some(Replicas::IncludeReplicas(Box::new(IncludeReplicas {
                replica_selections: vec![ReplicaSelection {
                    location: "us-east1".to_string(),
                    ..Default::default()
                }],
                ..Default::default()
            }))),
            ..Default::default()
        };

        let key = vec![0x05];
        let context = RoutingContext {
            transaction_id: None,
            routing_key: Some(&key),
            prefer_leader: false,
            use_transaction_affinity: false,
        };

        let route =
            router.resolve_route(&context, Some(&directed_read_options), 12, None, 120, None);
        assert_eq!(
            route.connection.address(),
            "10.0.0.2:15000",
            "must discard farther candidate and select closer candidate"
        );
        let hint = route
            .routing_hint
            .expect("routing hint must be generated on cache hit");
        assert_eq!(
            hint.tablet_uid, 1202,
            "routing hint must record the closer tablet UID"
        );
    }

    #[tokio::test]
    async fn location_router_directed_read_empty_include_replicas_falls_back_to_gateway() {
        let router = make_test_router();

        let tablet = Tablet::default()
            .set_tablet_uid(1301u64)
            .set_server_address("10.0.0.1:15000")
            .set_location("us-central1")
            .set_role(Role::ReadOnly)
            .set_distance(1u32);

        let group = Group::new()
            .set_group_uid(1400u64)
            .set_tablets(vec![tablet]);
        let range = Range::new()
            .set_group_uid(1400u64)
            .set_start_key(vec![0x01])
            .set_limit_key(vec![0x09]);
        let update = CacheUpdate::new()
            .set_database_id(13u64)
            .set_group(vec![group])
            .set_range(vec![range]);

        router.key_range_cache().add_ranges(&update);

        // IncludeReplicas with empty selections matches no replicas
        let directed_read_options = DirectedReadOptions {
            replicas: Some(Replicas::IncludeReplicas(Box::new(IncludeReplicas {
                replica_selections: vec![],
                ..Default::default()
            }))),
            ..Default::default()
        };

        let key = vec![0x05];
        let context = RoutingContext {
            transaction_id: None,
            routing_key: Some(&key),
            prefer_leader: false,
            use_transaction_affinity: false,
        };

        let route =
            router.resolve_route(&context, Some(&directed_read_options), 13, None, 130, None);
        assert_eq!(
            route.connection.address(),
            "spanner.googleapis.com:443",
            "empty IncludeReplicas must fall back to default gateway"
        );
    }

    #[tokio::test]
    async fn location_router_directed_read_empty_exclude_replicas_all_eligible() {
        let router = make_test_router();

        let tablet_closer = Tablet::default()
            .set_tablet_uid(1401u64)
            .set_server_address("10.0.0.1:15000")
            .set_location("us-central1")
            .set_role(Role::ReadOnly)
            .set_distance(1u32);
        let tablet_farther = Tablet::default()
            .set_tablet_uid(1402u64)
            .set_server_address("10.0.0.2:15000")
            .set_location("europe-west1")
            .set_role(Role::ReadOnly)
            .set_distance(10u32);

        let group = Group::new()
            .set_group_uid(1500u64)
            .set_tablets(vec![tablet_closer, tablet_farther]);
        let range = Range::new()
            .set_group_uid(1500u64)
            .set_start_key(vec![0x01])
            .set_limit_key(vec![0x09]);
        let update = CacheUpdate::new()
            .set_database_id(14u64)
            .set_group(vec![group])
            .set_range(vec![range]);

        router.key_range_cache().add_ranges(&update);

        let _ = router
            .connection_cache()
            .get("10.0.0.1:15000", &ClientConfig::default())
            .await
            .expect("initialize closer connection");
        let _ = router
            .connection_cache()
            .get("10.0.0.2:15000", &ClientConfig::default())
            .await
            .expect("initialize farther connection");

        // ExcludeReplicas with empty selections excludes nothing: all replicas remain eligible
        let directed_read_options = DirectedReadOptions {
            replicas: Some(Replicas::ExcludeReplicas(Box::new(ExcludeReplicas {
                replica_selections: vec![],
                ..Default::default()
            }))),
            ..Default::default()
        };

        let key = vec![0x05];
        let context = RoutingContext {
            transaction_id: None,
            routing_key: Some(&key),
            prefer_leader: false,
            use_transaction_affinity: false,
        };

        let route =
            router.resolve_route(&context, Some(&directed_read_options), 14, None, 140, None);
        assert_eq!(
            route.connection.address(),
            "10.0.0.1:15000",
            "empty ExcludeReplicas must leave all replicas eligible and select lowest distance"
        );
        let hint = route
            .routing_hint
            .expect("routing hint must be generated on cache hit");
        assert_eq!(
            hint.tablet_uid, 1401,
            "routing hint must record the closest tablet UID"
        );
    }

    #[tokio::test]
    async fn location_router_directed_read_none_replicas_falls_back_to_default() {
        let router = make_test_router();

        let leader_tablet = Tablet::default()
            .set_tablet_uid(1501u64)
            .set_server_address("10.0.0.1:15000")
            .set_location("us-central1")
            .set_role(Role::ReadWrite)
            .set_distance(1u32);
        let follower_tablet = Tablet::default()
            .set_tablet_uid(1502u64)
            .set_server_address("10.0.0.2:15000")
            .set_location("us-central1")
            .set_role(Role::ReadOnly)
            .set_distance(1u32);

        let group = Group::new()
            .set_group_uid(1600u64)
            .set_leader_index(0)
            .set_tablets(vec![leader_tablet, follower_tablet]);
        let range = Range::new()
            .set_group_uid(1600u64)
            .set_start_key(vec![0x01])
            .set_limit_key(vec![0x09]);
        let update = CacheUpdate::new()
            .set_database_id(15u64)
            .set_group(vec![group])
            .set_range(vec![range]);

        router.key_range_cache().add_ranges(&update);

        let _ = router
            .connection_cache()
            .get("10.0.0.1:15000", &ClientConfig::default())
            .await
            .expect("initialize leader connection");
        let _ = router
            .connection_cache()
            .get("10.0.0.2:15000", &ClientConfig::default())
            .await
            .expect("initialize follower connection");

        // DirectedReadOptions with replicas: None must fall back to standard routing behavior
        let directed_read_options = DirectedReadOptions {
            replicas: None,
            ..Default::default()
        };

        let key = vec![0x05];
        let context = RoutingContext {
            transaction_id: None,
            routing_key: Some(&key),
            prefer_leader: true,
            use_transaction_affinity: false,
        };

        let route =
            router.resolve_route(&context, Some(&directed_read_options), 15, None, 150, None);
        assert_eq!(
            route.connection.address(),
            "10.0.0.1:15000",
            "DirectedReadOptions with replicas: None must fall back to standard routing and prefer local leader"
        );
        let hint = route
            .routing_hint
            .expect("routing hint must be generated on cache hit");
        assert_eq!(
            hint.tablet_uid, 1501,
            "routing hint must record the leader tablet UID"
        );
    }

    #[tokio::test]
    async fn location_router_regular_read_includes_remote_skipped_tablets() {
        let router = make_test_router();

        // Local healthy replica (distance 1)
        let tablet_local = Tablet::default()
            .set_tablet_uid(1601u64)
            .set_server_address("10.0.0.1:15000")
            .set_location("us-central1")
            .set_role(Role::ReadOnly)
            .set_distance(1u32);
        // Remote cooling-down replica (distance 10 > MAX_LOCAL_REPLICA_DISTANCE)
        let tablet_remote_cooling_down = Tablet::default()
            .set_tablet_uid(1602u64)
            .set_server_address("10.0.0.2:15000")
            .set_location("europe-west1")
            .set_role(Role::ReadOnly)
            .set_distance(10u32);

        let group = Group::new()
            .set_group_uid(1600u64)
            .set_tablets(vec![tablet_local, tablet_remote_cooling_down]);
        let range = Range::new()
            .set_group_uid(1600u64)
            .set_start_key(vec![0x01])
            .set_limit_key(vec![0x09]);
        let update = CacheUpdate::new()
            .set_database_id(16u64)
            .set_group(vec![group])
            .set_range(vec![range]);

        router.key_range_cache().add_ranges(&update);

        let _ = router
            .connection_cache()
            .get("10.0.0.1:15000", &ClientConfig::default())
            .await
            .expect("initialize local connection");

        // Place remote tablet on cooldown
        router.record_failure("10.0.0.2:15000");

        let key = vec![0x05];
        let context = RoutingContext {
            transaction_id: None,
            routing_key: Some(&key),
            prefer_leader: false,
            use_transaction_affinity: false,
        };

        // Normal read without directed_read_options
        let route = router.resolve_route(&context, None, 16, None, 160, None);
        assert_eq!(
            route.connection.address(),
            "10.0.0.1:15000",
            "regular read must route to healthy local replica"
        );
        let hint = route
            .routing_hint
            .expect("routing hint must be generated on cache hit");
        assert_eq!(
            hint.tablet_uid, 1601,
            "routing hint must record healthy local tablet UID"
        );

        // Remote cooling-down tablet (distance 10) must be recorded in skipped_tablet_uid
        let skipped_uids: Vec<u64> = hint
            .skipped_tablet_uid
            .iter()
            .map(|skipped_tablet| skipped_tablet.tablet_uid)
            .collect();
        assert!(
            skipped_uids.contains(&1602),
            "remote cooling-down tablet 1602 (distance 10) must be included in skipped_tablet_uid for regular reads"
        );

        // Also verify when DirectedReadOptions is present but has replicas: None
        let empty_options = DirectedReadOptions::default();
        let route_empty = router.resolve_route(&context, Some(&empty_options), 16, None, 161, None);
        let hint_empty = route_empty
            .routing_hint
            .expect("routing hint must be generated on cache hit");
        let skipped_empty_uids: Vec<u64> = hint_empty
            .skipped_tablet_uid
            .iter()
            .map(|skipped_tablet| skipped_tablet.tablet_uid)
            .collect();
        assert!(
            skipped_empty_uids.contains(&1602),
            "remote cooling-down tablet 1602 must be included when DirectedReadOptions has no replicas set"
        );
    }

    #[tokio::test]
    async fn location_router_records_real_traffic_on_routed_connection() {
        let router = make_test_router();
        populate_test_routing_table(&router, "10.0.0.1:15000", vec![0x01], vec![0x09]).await;

        let past = Instant::now() - Duration::from_secs(20);
        let mut addresses = HashSet::new();
        addresses.insert("10.0.0.1:15000".to_string());
        router
            .endpoint_lifecycle_manager()
            .update_active_addresses_at(router.database_scope(), addresses, past);

        let initial_state = router
            .endpoint_lifecycle_manager()
            .get_endpoint_state("10.0.0.1:15000")
            .expect("endpoint state must exist");
        let initial_traffic_time = initial_state.last_real_traffic_at;

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
            "10.0.0.1:15000",
            "resolved connection should match tablet address"
        );

        let updated_state = router
            .endpoint_lifecycle_manager()
            .get_endpoint_state("10.0.0.1:15000")
            .expect("endpoint state must exist");
        assert!(
            updated_state.last_real_traffic_at > initial_traffic_time,
            "real traffic timestamp must be updated after routing"
        );
    }

    #[tokio::test]
    async fn location_router_bypasses_transient_failure_evicted_tablet() {
        let router = make_test_router();
        populate_test_routing_table(&router, "10.0.0.1:15000", vec![0x01], vec![0x09]).await;

        let mut addresses = HashSet::new();
        addresses.insert("10.0.0.1:15000".to_string());
        router
            .endpoint_lifecycle_manager()
            .update_active_addresses(router.database_scope(), addresses);

        let connection = router
            .connection_cache()
            .get_if_present("10.0.0.1:15000")
            .expect("connection must be present");
        connection.set_transient_failure();

        let now = Instant::now();
        router
            .endpoint_lifecycle_manager()
            .probe_all_endpoints_at(now + Duration::from_secs(60));
        router
            .endpoint_lifecycle_manager()
            .probe_all_endpoints_at(now + Duration::from_secs(120));
        let evicted = router
            .endpoint_lifecycle_manager()
            .probe_all_endpoints_at(now + Duration::from_secs(180));
        assert_eq!(
            evicted.len(),
            1,
            "endpoint must be evicted due to transient failure"
        );
        assert!(
            router
                .endpoint_lifecycle_manager()
                .is_transient_failure_evicted("10.0.0.1:15000"),
            "endpoint must be marked as transient failure evicted"
        );

        let key = vec![0x05];
        let context = RoutingContext {
            transaction_id: None,
            routing_key: Some(&key),
            prefer_leader: false,
            use_transaction_affinity: false,
        };

        let route = router.resolve_route(&context, None, 100, None, 1, None);
        assert_eq!(
            route.connection.address(),
            "spanner.googleapis.com:443",
            "must fall back to default connection when tablet is evicted"
        );

        let hint = route.routing_hint.expect("hint must be present");
        assert_eq!(
            hint.tablet_uid, 0,
            "no tablet should be selected when all are unroutable"
        );
        assert_eq!(
            hint.skipped_tablet_uid.len(),
            1,
            "transient failure evicted tablet must be recorded in skipped_tablet_uid"
        );
        assert_eq!(
            hint.skipped_tablet_uid[0].tablet_uid, 10,
            "skipped tablet uid must match evicted tablet"
        );
    }

    #[tokio::test]
    async fn location_router_requests_endpoint_recreation_when_connection_absent() {
        let router = make_test_router();

        let mut addresses = HashSet::new();
        addresses.insert("10.0.0.1:15000".to_string());
        router
            .endpoint_lifecycle_manager()
            .update_active_addresses(router.database_scope(), addresses);

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
        router.key_range_cache().add_ranges(&update);

        let now = Instant::now();
        router
            .endpoint_lifecycle_manager()
            .check_idle_eviction_at(now + Duration::from_secs(4000));
        assert!(
            router
                .endpoint_lifecycle_manager()
                .get_endpoint_state("10.0.0.1:15000")
                .is_none(),
            "endpoint must be idle evicted"
        );

        let key = vec![0x05];
        let context = RoutingContext {
            transaction_id: None,
            routing_key: Some(&key),
            prefer_leader: false,
            use_transaction_affinity: false,
        };

        let _ = router.resolve_route(&context, None, 100, None, 1, None);

        assert!(
            router
                .endpoint_lifecycle_manager()
                .get_endpoint_state("10.0.0.1:15000")
                .is_some(),
            "endpoint must be recreated in lifecycle manager when routed"
        );
    }

    #[tokio::test]
    async fn select_healthy_tablet_falls_back_to_p2c_followers_when_leader_is_unwarmed() {
        let router = make_test_router();

        let tablet_leader = Tablet::default()
            .set_tablet_uid(10u64)
            .set_server_address("10.0.0.1:15000")
            .set_distance(0u32);
        let tablet_follower = Tablet::default()
            .set_tablet_uid(11u64)
            .set_server_address("10.0.0.2:15000")
            .set_distance(0u32);

        let group = Group::new()
            .set_group_uid(100u64)
            .set_leader_index(0)
            .set_tablets(vec![tablet_leader, tablet_follower]);
        let range = Range::new()
            .set_group_uid(100u64)
            .set_start_key(vec![0x01])
            .set_limit_key(vec![0x09]);
        let update = CacheUpdate::new()
            .set_database_id(1u64)
            .set_group(vec![group])
            .set_range(vec![range]);

        router.key_range_cache().add_ranges(&update);

        // Pre-warm ONLY the follower connection; leader remains unwarmed
        let _ = router
            .connection_cache()
            .get("10.0.0.2:15000", &ClientConfig::default())
            .await
            .expect("init follower");

        let key = vec![0x05];
        let context_prefer_leader = RoutingContext {
            transaction_id: None,
            routing_key: Some(&key),
            prefer_leader: true,
            use_transaction_affinity: false,
        };

        let connection = router.resolve_connection(&context_prefer_leader);
        assert_eq!(
            connection.address(),
            "10.0.0.2:15000",
            "request preferring leader must fall back to warmed follower when leader is unwarmed"
        );
    }

    #[tokio::test]
    async fn location_router_transient_failure_connection_marked_unroutable_and_skipped() {
        let router = make_test_router();

        let tablet_leader = Tablet::default()
            .set_tablet_uid(10u64)
            .set_server_address("10.0.0.1:15000")
            .set_distance(0u32);
        let tablet_follower = Tablet::default()
            .set_tablet_uid(11u64)
            .set_server_address("10.0.0.2:15000")
            .set_distance(0u32);

        let group = Group::new()
            .set_group_uid(100u64)
            .set_leader_index(0)
            .set_tablets(vec![tablet_leader, tablet_follower]);
        let range = Range::new()
            .set_group_uid(100u64)
            .set_start_key(vec![0x01])
            .set_limit_key(vec![0x09]);
        let update = CacheUpdate::new()
            .set_database_id(1u64)
            .set_group(vec![group])
            .set_range(vec![range]);

        router.key_range_cache().add_ranges(&update);

        let leader_connection = router
            .connection_cache()
            .get("10.0.0.1:15000", &ClientConfig::default())
            .await
            .expect("init leader");
        let _ = router
            .connection_cache()
            .get("10.0.0.2:15000", &ClientConfig::default())
            .await
            .expect("init follower");

        // Mark the active leader connection as TRANSIENT_FAILURE
        leader_connection.set_transient_failure();

        let key = vec![0x05];
        let context = RoutingContext {
            transaction_id: None,
            routing_key: Some(&key),
            prefer_leader: true,
            use_transaction_affinity: false,
        };

        let route = router.resolve_route(&context, None, 100, None, 1, None);
        assert_eq!(
            route.connection.address(),
            "10.0.0.2:15000",
            "must steer traffic to healthy follower when leader connection is in TRANSIENT_FAILURE"
        );

        let hint = route.routing_hint.expect("routing hint must be present");
        assert_eq!(hint.tablet_uid, 11, "follower tablet must be selected");
        let skipped_uids: Vec<u64> = hint
            .skipped_tablet_uid
            .iter()
            .map(|skipped| skipped.tablet_uid)
            .collect();
        assert!(
            skipped_uids.contains(&10),
            "leader tablet UID 10 must be included in skipped_tablet_uid due to TRANSIENT_FAILURE"
        );
    }

    #[tokio::test]
    async fn location_router_unhealthy_candidate_connection_treated_as_unwarmed() {
        let router = make_test_router();

        let tablet_leader = Tablet::default()
            .set_tablet_uid(10u64)
            .set_server_address("10.0.0.1:15000")
            .set_distance(0u32);
        let tablet_follower = Tablet::default()
            .set_tablet_uid(11u64)
            .set_server_address("10.0.0.2:15000")
            .set_distance(0u32);

        let group = Group::new()
            .set_group_uid(100u64)
            .set_leader_index(0)
            .set_tablets(vec![tablet_leader, tablet_follower]);
        let range = Range::new()
            .set_group_uid(100u64)
            .set_start_key(vec![0x01])
            .set_limit_key(vec![0x09]);
        let update = CacheUpdate::new()
            .set_database_id(1u64)
            .set_group(vec![group])
            .set_range(vec![range]);

        router.key_range_cache().add_ranges(&update);

        let leader_connection = router
            .connection_cache()
            .get("10.0.0.1:15000", &ClientConfig::default())
            .await
            .expect("init leader");
        let _ = router
            .connection_cache()
            .get("10.0.0.2:15000", &ClientConfig::default())
            .await
            .expect("init follower");

        // Mark leader connection unhealthy (neither READY nor TRANSIENT_FAILURE)
        leader_connection.set_unhealthy();

        let key = vec![0x05];
        let context_prefer_leader = RoutingContext {
            transaction_id: None,
            routing_key: Some(&key),
            prefer_leader: true,
            use_transaction_affinity: false,
        };

        // When leader is unhealthy (classified as unwarmed), route must fall back to healthy follower
        let route = router.resolve_route(&context_prefer_leader, None, 100, None, 1, None);
        assert_eq!(
            route.connection.address(),
            "10.0.0.2:15000",
            "request preferring leader must fall back to healthy follower when leader connection is unhealthy"
        );
    }
}
