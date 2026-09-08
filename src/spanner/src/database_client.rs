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

use crate::batch_read_only_transaction::BatchReadOnlyTransactionBuilder;
use crate::batch_write_transaction::BatchWriteTransactionBuilder;
use crate::client::Spanner;
use crate::model::transaction_options::Mode;
use crate::model::transaction_options::read_only::TimestampBound;
use crate::model::transaction_selector::Selector;
use crate::model::{
    BatchWriteRequest, BeginTransactionRequest, CacheUpdate, CommitRequest, CommitResponse,
    DirectedReadOptions, ExecuteBatchDmlRequest, ExecuteBatchDmlResponse, ExecuteSqlRequest,
    PartitionQueryRequest, PartitionReadRequest, PartitionResponse, ReadRequest, ResultSet,
    RollbackRequest, RoutingHint, Transaction, TransactionOptions, TransactionSelector,
};
use crate::mutation::Mutation;
use crate::observability::Observability;
use crate::omni::{InstanceType, format_database_name};
use crate::partitioned_dml_transaction::PartitionedDmlTransactionBuilder;
use crate::read_only_transaction::{
    MultiUseReadOnlyTransactionBuilder, SingleUseReadOnlyTransactionBuilder,
};
use crate::routing::cache_subscriber::CacheSubscriber;
use crate::routing::cache_updater::CacheUpdater;
use crate::routing::connection_cache::ConnectionCache;
use crate::routing::endpoint_cooldown::EndpointCooldownTracker;
use crate::routing::endpoint_lifecycle::EndpointLifecycleManager;
use crate::routing::key_extractor::{
    extract_execute_sql_request_routing, extract_mutation_routing_key,
    extract_proto_partition_read_request_routing_key, extract_proto_read_request_routing,
};
use crate::routing::key_range_cache::KeyRangeCache;
use crate::routing::key_recipe_cache::KeyRecipeCache;
use crate::routing::latency_registry::LatencyRegistry;
use crate::routing::location_router::{LocationRouter, RoutingContext};
use crate::routing::server_connection::ServerConnection;
use crate::server_streaming::builder::{BatchWrite, ExecuteStreamingSql, StreamingRead};
use crate::session_maintainer::ManagedSessionMaintainer;
use crate::transaction_runner::TransactionRunnerBuilder;
use crate::write_only_transaction::WriteOnlyTransactionBuilder;
use crate::{RequestOptions, Result};
use bytes::Bytes;
use std::env;
use std::sync::Arc;
use std::time::Duration;

/// A client for interacting with a specific Spanner database.
///
/// `DatabaseClient` provides methods to execute transactions and queries.
/// # Example
/// ```
/// # use google_cloud_spanner::client::Spanner;
/// # async fn sample() -> anyhow::Result<()> {
///     let spanner = Spanner::builder().build().await?;
///     let database_client = spanner
///         .database_client("projects/my-project/instances/my-instance/databases/my-db")
///         .build()
///         .await?;
///     # Ok(())
/// # }
/// ```
///
/// It holds a single multiplexed session for the database.
///
/// A `DatabaseClient` is intended to be a long-lived object, and normally an
/// application will have a single `DatabaseClient` per database. The client is
/// thread-safe and should be reused for all operations on the database.
///
/// Cloning a `DatabaseClient` is cheap, as it shares the underlying session and channel.
#[derive(Clone, Debug)]
pub struct DatabaseClient {
    spanner: Spanner,
    pub(crate) session_maintainer: Arc<ManagedSessionMaintainer>,
    pub(crate) leader_aware_routing_enabled: bool,
    #[allow(dead_code)] // TODO: Used by request routing interceptors in subsequent PRs
    pub(crate) location_routing: Option<Arc<LocationRoutingState>>,
    pub(crate) o11y: Arc<Observability>,
}

macro_rules! define_db_rpc {
    (
        $method:ident,
        $expect_method:ident,
        $request_type:ty,
        $response_type:ty,
        $pre_route:path,
        $post_hook:path
    ) => {
        pub(crate) async fn $method(
            &self,
            mut request: $request_type,
            options: RequestOptions,
            channel_hint: usize,
        ) -> Result<$response_type> {
            let (connection, routing_context) = $pre_route(self, &mut request);
            let channel = match &connection {
                Some(connection) => connection.channel(),
                None => self.spanner.get_channel(channel_hint),
            };
            let result = self
                .spanner
                .$method(request, options, channel, &self.o11y)
                .await;
            $post_hook(self, routing_context, connection.as_ref(), &result);
            let response = result?;
            response.observe(self);
            Ok(response)
        }
    };
}

macro_rules! define_db_streaming_rpc {
    ($method:ident, $expect_method:ident, $request_type:ty, $builder_type:ty) => {
        pub(crate) fn $method(
            &self,
            request: $request_type,
            options: RequestOptions,
            channel_hint: usize,
        ) -> $builder_type {
            let channel = self.spanner.get_channel(channel_hint);
            self.spanner.$method(request, options, channel)
        }
    };
    ($method:ident, $expect_method:ident, $request_type:ty, $builder_type:ty, $extract_key:expr) => {
        pub(crate) fn $method(
            &self,
            mut request: $request_type,
            options: RequestOptions,
            channel_hint: usize,
        ) -> $builder_type {
            // Step 1: When location-aware routing is disabled (standard Cloud Spanner),
            // `self.location_routing` is `None` so `$extract_key` is skipped immediately.
            // When enabled (Spanner Omni), extract the operation UID and binary routing key.
            let (operation_uid, routing_key) = match &self.location_routing {
                Some(routing) => $extract_key(routing, &request),
                None => (UNASSIGNED_OPERATION_UID, None),
            };

            // Step 2: Resolve the optimal server connection and attach the routing hint (or bootstrap hint).
            let connection = self.route_and_attach_hint(
                request.transaction.as_ref(),
                request.directed_read_options.as_ref(),
                operation_uid,
                routing_key.as_deref(),
                &mut request.routing_hint,
            );

            // Step 4: Select the gRPC channel:
            // - If location-aware routing resolved a direct node connection (`Some(connection)`), use `connection.channel()`.
            // - Otherwise (location routing disabled, unkeyed query/read, or cold cache), fall back to round-robin
            //   load-balancing across the client's channel pool via `self.spanner.get_channel(channel_hint)`.
            //   This fallback is a fast O(1) slice index without any heap allocation, cloning, or lock acquisition.
            let channel = match &connection {
                Some(connection) => connection.channel(),
                None => self.spanner.get_channel(channel_hint),
            };
            self.spanner.$method(request, options, channel)
        }
    };
}

macro_rules! for_all_unary_db_rpcs {
    ($macro:ident) => {
        $macro!(
            begin_transaction,
            expect_begin_transaction,
            BeginTransactionRequest,
            Transaction,
            DatabaseClient::pre_route_begin_transaction,
            DatabaseClient::post_route_begin_transaction
        );
        $macro!(
            commit,
            expect_commit,
            CommitRequest,
            CommitResponse,
            DatabaseClient::pre_route_commit,
            DatabaseClient::post_route_commit
        );
        $macro!(
            execute_batch_dml,
            expect_execute_batch_dml,
            ExecuteBatchDmlRequest,
            ExecuteBatchDmlResponse,
            DatabaseClient::pre_route_execute_batch_dml,
            DatabaseClient::post_route_execute_batch_dml
        );
        $macro!(
            execute_sql,
            expect_execute_sql,
            ExecuteSqlRequest,
            ResultSet,
            DatabaseClient::pre_route_execute_sql,
            DatabaseClient::post_route_execute_sql
        );
        $macro!(
            rollback,
            expect_rollback,
            RollbackRequest,
            (),
            DatabaseClient::pre_route_rollback,
            DatabaseClient::post_route_rollback
        );
        $macro!(
            partition_query,
            expect_partition_query,
            PartitionQueryRequest,
            PartitionResponse,
            DatabaseClient::pre_route_partition_query,
            DatabaseClient::post_route_noop
        );
        $macro!(
            partition_read,
            expect_partition_read,
            PartitionReadRequest,
            PartitionResponse,
            DatabaseClient::pre_route_partition_read,
            DatabaseClient::post_route_noop
        );
    };
}

macro_rules! for_all_streaming_db_rpcs {
    ($macro:ident) => {
        $macro!(
            execute_streaming_sql,
            expect_execute_streaming_sql,
            ExecuteSqlRequest,
            ExecuteStreamingSql,
            |routing: &LocationRoutingState, request: &ExecuteSqlRequest| {
                extract_execute_sql_request_routing(&routing.key_recipe_cache, request)
            }
        );
        $macro!(
            streaming_read,
            expect_streaming_read,
            ReadRequest,
            StreamingRead,
            |routing: &LocationRoutingState, request: &ReadRequest| {
                extract_proto_read_request_routing(&routing.key_recipe_cache, request)
            }
        );
        $macro!(
            batch_write,
            expect_batch_write,
            BatchWriteRequest,
            BatchWrite
        );
    };
}

impl DatabaseClient {
    pub(crate) fn is_emulator(&self) -> bool {
        self.spanner.is_emulator()
    }

    pub(crate) fn next_channel_hint(&self) -> usize {
        self.spanner.next_channel_hint()
    }

    pub(crate) fn attach_request_id(
        &self,
        options: RequestOptions,
        channel_hint: usize,
    ) -> RequestOptions {
        let channel = self.spanner.get_channel(channel_hint);
        self.spanner.attach_request_id(options, channel)
    }

    for_all_unary_db_rpcs!(define_db_rpc);

    /// Resolves the optimal [`ServerConnection`] for a request if location-aware routing is enabled.
    ///
    /// # Performance & Routing Flow:
    /// - **Location routing disabled (Standard Cloud Spanner default)**: Returns `None` immediately on
    ///   the fast path (`self.location_routing.as_ref()?`) without extracting keys, checking transactions,
    ///   or acquiring any locks.
    /// - **Location routing enabled (Spanner Omni)**:
    ///   - If neither a `transaction_id` nor a `routing_key` is present (e.g. unkeyed reads or queries),
    ///     returns `None` early to allow standard round-robin channel pooling across channels 1..=4.
    ///   - If a routing key or transaction affinity matches an active endpoint in cache, returns `Some(connection)`
    ///     pointing directly to the target node.
    #[allow(dead_code)] // TODO(#6236): Used by request routing in subsequent PRs
    pub(crate) fn resolve_routing_connection(
        &self,
        context: &RoutingContext,
    ) -> Option<ServerConnection> {
        let routing = self.location_routing.as_ref()?;
        if context.transaction_id.is_none() && context.routing_key.is_none() {
            return None;
        }
        Some(routing.location_router.resolve_connection(context))
    }

    for_all_streaming_db_rpcs!(define_db_streaming_rpc);

    /// Returns a builder for a single-use read-only transaction.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::Spanner;
    /// # use google_cloud_spanner::statement::Statement;
    /// # async fn run(spanner: Spanner) -> Result<(), google_cloud_spanner::Error> {
    /// let db_client = spanner.database_client("projects/p/instances/i/databases/d").build().await?;
    /// let tx = db_client.single_use().build();
    /// let stmt = Statement::builder("SELECT * FROM users WHERE id = @id")
    ///     .add_param("id", &42)
    ///     .build();
    /// let mut rs = tx.execute_query(stmt).await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// A single-use read-only transaction is optimized for the case where only a single
    /// read or query is needed. This is more efficient than using a read-only transaction
    /// for a single read or query.
    pub fn single_use(&self) -> SingleUseReadOnlyTransactionBuilder {
        SingleUseReadOnlyTransactionBuilder::new(self.clone())
    }

    /// Returns a builder for a multi-use read-only transaction.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::Spanner;
    /// # use google_cloud_spanner::statement::Statement;
    /// # async fn run(spanner: Spanner) -> Result<(), google_cloud_spanner::Error> {
    /// let db_client = spanner.database_client("projects/p/instances/i/databases/d").build().await?;
    /// let tx = db_client.read_only_transaction().build().await?;
    /// let stmt = Statement::builder("SELECT * FROM users WHERE id = @id")
    ///     .add_param("id", &42)
    ///     .build();
    /// let mut rs = tx.execute_query(stmt).await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// A read-only transaction can be used to execute multiple reads or queries.
    /// These transactions guarantee data consistency across multiple read operations,
    /// but don't permit data modifications. Read-only transactions do not take locks.
    pub fn read_only_transaction(&self) -> MultiUseReadOnlyTransactionBuilder {
        MultiUseReadOnlyTransactionBuilder::new(self.clone())
    }

    /// Returns a builder for a batch read-only transaction.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::Spanner;
    /// # use google_cloud_spanner::statement::Statement;
    /// # async fn build(spanner: Spanner) -> Result<(), google_cloud_spanner::Error> {
    /// let db_client = spanner.database_client("projects/p/instances/i/databases/d").build().await?;
    /// let transaction = db_client.batch_read_only_transaction().build().await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// A batch read-only transaction is similar to a read-only transaction, but it allows for partitioning
    /// a read or query request. Run tasks in parallel over the partitions to execute a large read or query.
    pub fn batch_read_only_transaction(&self) -> BatchReadOnlyTransactionBuilder {
        BatchReadOnlyTransactionBuilder::new(self.clone())
    }

    /// Returns a builder for a partitioned DML transaction.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::Spanner;
    /// # use google_cloud_spanner::statement::Statement;
    /// # async fn run(spanner: Spanner) -> Result<(), google_cloud_spanner::Error> {
    /// let db_client = spanner.database_client("projects/p/instances/i/databases/d").build().await?;
    /// let transaction = db_client.partitioned_dml_transaction().build().await?;
    /// let statement = Statement::builder("UPDATE users SET active = true WHERE TRUE").build();
    /// let modified_rows = transaction.execute_update(statement).await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// Partitioned DML is used to execute a single DML statement that may modify a large number
    /// of rows. The execution of the statement will automatically be partitioned into smaller
    /// transactions by Spanner, which may execute in parallel.
    ///
    /// See also: <https://docs.cloud.google.com/spanner/docs/dml-partitioned>
    pub fn partitioned_dml_transaction(&self) -> PartitionedDmlTransactionBuilder {
        PartitionedDmlTransactionBuilder::new(self.clone())
    }

    /// Returns a builder for a read-write transaction runner.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::Spanner;
    /// # use google_cloud_spanner::statement::Statement;
    /// # async fn build(spanner: Spanner) -> Result<(), google_cloud_spanner::Error> {
    /// let db_client = spanner.database_client("projects/p/instances/i/databases/d").build().await?;
    /// let runner = db_client.read_write_transaction().build().await?;
    /// let result = runner.run(async |transaction| {
    ///     let statement = Statement::builder("UPDATE users SET active = true WHERE id = 1").build();
    ///     transaction.execute_update(statement).await?;
    ///     Ok(())
    /// }).await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// Read-write transactions can be used to execute multiple queries and updates
    /// atomically. If the transaction is aborted by Spanner, the `run` method will
    /// automatically retry the transaction.
    pub fn read_write_transaction(&self) -> TransactionRunnerBuilder {
        TransactionRunnerBuilder::new(self.clone())
    }

    /// Returns a builder for a write-only transaction.
    ///
    /// # Example
    /// ```rust
    /// # use google_cloud_spanner::client::Spanner;
    /// # use google_cloud_spanner::mutation::Mutation;
    /// # async fn test_doc() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = Spanner::builder().build().await?;
    /// let db = client.database_client("projects/p/instances/i/databases/d").build().await?;
    ///
    /// let mutation = Mutation::new_insert_builder("Users")
    ///     .set("UserId").to(&1)
    ///     .set("UserName").to(&"Alice")
    ///     .build();
    ///
    /// let response = db.write_only_transaction()
    ///     .set_transaction_tag("my-tag")
    ///     .build()
    ///     .write(vec![mutation])
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// A write-only transaction is used to execute blind writes using mutations.
    pub fn write_only_transaction(&self) -> WriteOnlyTransactionBuilder {
        WriteOnlyTransactionBuilder::new(self.clone())
    }

    /// Returns a builder for a batch write transaction.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::Spanner;
    /// # use google_cloud_spanner::mutation::Mutation;
    /// # use google_cloud_spanner::mutation::MutationGroup;
    /// # use google_cloud_gax::error::rpc::Code;
    /// # async fn sample() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = Spanner::builder().build().await?;
    /// let db = client.database_client("projects/p/instances/i/databases/d").build().await?;
    ///
    /// let mutation1a = Mutation::new_insert_builder("Users")
    ///     .set("UserId").to(&1)
    ///     .build();
    /// let mutation1b = Mutation::new_insert_builder("UserRoles")
    ///     .set("UserId").to(&1)
    ///     .set("Role").to(&"Admin")
    ///     .build();
    /// let group1 = MutationGroup::new(vec![mutation1a, mutation1b]);
    ///
    /// let mutation2 = Mutation::new_insert_builder("Users")
    ///     .set("UserId").to(&2)
    ///     .build();
    /// let group2 = MutationGroup::new(vec![mutation2]);
    ///
    /// let transaction = db.batch_write_transaction().build();
    /// let mut stream = transaction.execute_streaming(vec![group1, group2]).await?;
    ///
    /// while let Some(response) = stream.next().await {
    ///     let response = response?;
    ///     if let Some(status) = response.status.as_ref().filter(|s| s.code != Code::Ok as i32) {
    ///         eprintln!("Error applying groups {:?}: {}", response.indexes, status.message);
    ///     } else {
    ///         println!("Applied groups: {:?}", response.indexes);
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// A batch write transaction is used to execute non-atomic writes using mutations.
    /// Related mutations should be placed in a group. For example, two mutations inserting
    /// rows with the same primary key prefix in both parent and child tables are related.
    /// All mutations within a group are applied atomically, but the entire batch is not
    /// guaranteed to be atomic.
    pub fn batch_write_transaction(&self) -> BatchWriteTransactionBuilder {
        BatchWriteTransactionBuilder::new(self.clone())
    }

    pub(crate) fn session_name(&self) -> String {
        self.session_maintainer.session_name()
    }

    /// Returns a reference to the [`LocationRouter`] if location-aware routing is enabled.
    #[allow(dead_code)] // TODO: Used by request routing interceptors in subsequent PRs
    pub(crate) fn location_router(&self) -> Option<&Arc<LocationRouter>> {
        self.location_routing
            .as_ref()
            .map(|routing| &routing.location_router)
    }

    /// Returns a reference to the [`CacheUpdater`] if location-aware routing is enabled.
    #[allow(dead_code)] // TODO: Used by request routing interceptors in subsequent PRs
    pub(crate) fn cache_updater(&self) -> Option<&Arc<CacheUpdater>> {
        self.location_routing
            .as_ref()
            .map(|routing| &routing.cache_updater)
    }

    /// Returns a reference to the [`KeyRecipeCache`] if location-aware routing is enabled.
    #[allow(dead_code)] // TODO: Used by request routing interceptors in subsequent PRs
    pub(crate) fn key_recipe_cache(&self) -> Option<&Arc<KeyRecipeCache>> {
        self.location_routing
            .as_ref()
            .map(|routing| &routing.key_recipe_cache)
    }

    /// Returns whether location-aware routing is enabled.
    #[allow(dead_code)] // TODO: Used by request routing interceptors in subsequent PRs
    pub(crate) fn is_location_aware_routing_enabled(&self) -> bool {
        self.location_routing.is_some()
    }

    /// Returns a reference to the [`LatencyRegistry`] used by location-aware routing, if enabled.
    #[allow(dead_code)] // TODO: Used for latency recording in subsequent PRs
    pub(crate) fn latency_registry(&self) -> Option<&LatencyRegistry> {
        self.location_routing
            .as_ref()
            .map(|routing| routing.location_router.latency_registry())
    }

    /// Records an observed round-trip latency sample for an endpoint address within a paxos group.
    #[allow(dead_code)] // TODO: Used for latency recording in subsequent PRs
    pub(crate) fn record_latency(&self, group_uid: u64, server_address: &str, latency: Duration) {
        let Some(routing) = &self.location_routing else {
            return;
        };
        routing
            .location_router
            .record_latency(group_uid, server_address, latency);
    }

    /// Records an RPC error penalty for an endpoint address within a paxos group.
    #[allow(dead_code)] // TODO: Used for error penalty recording in subsequent PRs
    pub(crate) fn record_routing_error(&self, group_uid: u64, server_address: &str) {
        let Some(routing) = &self.location_routing else {
            return;
        };
        routing
            .location_router
            .record_error(group_uid, server_address);
    }

    /// Returns the database ID assigned by the server for location-aware routing, if known.
    #[allow(dead_code)] // TODO(#6236): Used by request routing in subsequent PRs
    pub(crate) fn database_id(&self) -> Option<u64> {
        self.location_routing
            .as_ref()
            .map(|routing| routing.cache_updater.database_id())
    }

    /// Returns a reference to the background [`CacheSubscriber`] if location-aware routing is enabled.
    #[allow(dead_code)] // TODO: Used for lifecycle inspection in subsequent PRs
    pub(crate) fn cache_subscriber(&self) -> Option<&CacheSubscriber> {
        self.location_routing
            .as_ref()
            .map(|routing| &routing.cache_subscriber)
    }
    /// Returns a reference to the [`EndpointLifecycleManager`] if location-aware routing is enabled.
    #[allow(dead_code)] // TODO: Used for lifecycle inspection in subsequent PRs
    pub(crate) fn endpoint_lifecycle_manager(&self) -> Option<&EndpointLifecycleManager> {
        self.location_routing
            .as_ref()
            .map(|routing| &*routing.endpoint_lifecycle_manager)
    }

    /// Resolves the optimal [`ServerConnection`] and [`RoutingHint`] in a single pass for a request.
    fn resolve_request_route(
        &self,
        transaction: Option<&TransactionSelector>,
        directed_read_options: Option<&DirectedReadOptions>,
        operation_uid: u64,
        routing_key: Option<&[u8]>,
    ) -> (Option<ServerConnection>, Option<RoutingHint>) {
        let Some(routing) = &self.location_routing else {
            return (None, None);
        };
        let context = routing_context_from_selector(transaction, routing_key);
        // Fast path: if neither transaction affinity, a routing key, nor a prepared operation UID
        // is available, fall back to the default channel pool.
        if context.transaction_id.is_none()
            && context.routing_key.is_none()
            && operation_uid == UNASSIGNED_OPERATION_UID
        {
            return (None, None);
        }
        let database_id = routing.cache_updater.database_id();
        let schema_generation = routing.key_recipe_cache.schema_generation();
        let resolved = routing.location_router.resolve_route(
            &context,
            directed_read_options,
            database_id,
            schema_generation,
            operation_uid,
            None,
        );
        let connection = if context.transaction_id.is_some() || context.routing_key.is_some() {
            Some(resolved.connection)
        } else {
            None
        };
        (connection, resolved.routing_hint)
    }

    /// Resolves the optimal route and attaches either the tablet-routed [`RoutingHint`] or the cold-start
    /// bootstrap [`RoutingHint`] to the request's `routing_hint` field.
    fn route_and_attach_hint(
        &self,
        transaction: Option<&TransactionSelector>,
        directed_read_options: Option<&DirectedReadOptions>,
        operation_uid: u64,
        routing_key: Option<&[u8]>,
        routing_hint: &mut Option<RoutingHint>,
    ) -> Option<ServerConnection> {
        let (connection, resolved_hint) = self.resolve_request_route(
            transaction,
            directed_read_options,
            operation_uid,
            routing_key,
        );

        if let Some(hint) = resolved_hint {
            *routing_hint = Some(hint);
        } else if operation_uid > UNASSIGNED_OPERATION_UID
            && let Some(routing) = &self.location_routing
        {
            // Cold-start query recipe discovery:
            // When executing a query shape for the first time, no matching `KeyRecipe` is cached yet,
            // so no routing key can be extracted and `routing_hint` is `None`.
            //
            // We attach a bootstrap `RoutingHint` containing `operation_uid` (plus `database_id` and
            // `schema_generation` if known). The Spanner server includes the query `KeyRecipe` in the
            // stream response (`PartialResultSet.cache_update`), allowing subsequent executions of this
            // query shape to resolve routing keys and route directly to tablet replicas.
            let database_id = routing.cache_updater.database_id();
            let mut hint = RoutingHint::new().set_operation_uid(operation_uid);
            if database_id != 0 {
                hint = hint.set_database_id(database_id);
            }
            if let Some(schema_generation) = routing.key_recipe_cache.schema_generation() {
                hint = hint.set_schema_generation(schema_generation);
            }
            *routing_hint = Some(hint);
        }

        connection
    }

    /// Observes an incoming [`CacheUpdate`], updating routing ranges, pre-warming connections, and caching key recipes.
    pub(crate) fn observe_cache_update(&self, cache_update: Option<CacheUpdate>) {
        let (Some(routing), Some(cache_update)) = (&self.location_routing, cache_update) else {
            return;
        };
        routing.cache_updater.process_cache_update(cache_update);
    }

    /// Intercepts unary [`BeginTransactionRequest`] calls to resolve leader routing and attach [`RoutingHint`].
    ///
    /// When location-aware routing is active and the request carries a `mutation_key`, extracts the routing
    /// key from [`KeyRecipeCache`] and resolves the target connection and covering [`RoutingHint`] in a single
    /// pass. Attaches the resolved [`RoutingHint`] to `request.routing_hint`.
    ///
    /// When `request.mutation_key` is omitted or unresolvable, returns `(None, is_read_write)` so that
    /// [`Self::post_route_begin_transaction`] binds transaction affinity to the default gateway connection
    /// for read-write transactions.
    fn pre_route_begin_transaction(
        &self,
        request: &mut BeginTransactionRequest,
    ) -> (Option<ServerConnection>, bool) {
        let Some(routing) = &self.location_routing else {
            return (None, false);
        };
        let options = request.options.as_ref();
        let is_read_write = is_read_write_options(options);
        let routing_key = request.mutation_key.as_ref().and_then(|mutation_key| {
            extract_mutation_routing_key(&routing.key_recipe_cache, mutation_key)
        });
        let Some(routing_key) = routing_key else {
            return (None, is_read_write);
        };
        let prefer_leader = prefer_leader_from_options(options);
        let context = RoutingContext {
            routing_key: Some(&routing_key),
            prefer_leader,
            ..Default::default()
        };
        let database_id = routing.cache_updater.database_id();
        let schema_generation = routing.key_recipe_cache.schema_generation();
        let resolved = routing.location_router.resolve_route(
            &context,
            None,
            database_id,
            schema_generation,
            UNASSIGNED_OPERATION_UID,
            None,
        );
        if let Some(hint) = resolved.routing_hint {
            request.routing_hint = Some(hint);
        }
        (Some(resolved.connection), is_read_write)
    }

    fn post_route_begin_transaction(
        &self,
        is_read_write: bool,
        connection: Option<&ServerConnection>,
        result: &Result<Transaction>,
    ) {
        self.record_transaction_affinity_routing(
            is_read_write,
            result
                .as_ref()
                .ok()
                .map(|transaction| transaction.id.as_ref()),
            connection,
        );
    }

    /// Intercepts unary [`CommitRequest`] calls to resolve leader or affinity routing and attach [`RoutingHint`].
    ///
    /// When location-aware routing is active, selects candidate mutation key from `request.mutations` via
    /// [`Mutation::select_mutation_key_ref`] and resolves route and covering [`RoutingHint`]. If `request.transaction_id`
    /// has active transaction affinity, routes to the affinity connection while attaching the mutation [`RoutingHint`]
    /// to `request.routing_hint`. If neither affinity nor a routing key is present, returns `None` connection so
    /// that the request falls back to round-robin over the client's channel pool.
    fn pre_route_commit(
        &self,
        request: &mut CommitRequest,
    ) -> (Option<ServerConnection>, Option<Bytes>) {
        let Some(routing) = &self.location_routing else {
            return (None, None);
        };
        let transaction_id = request
            .transaction_id()
            .filter(|id| !id.is_empty())
            .cloned();
        let routing_key = Mutation::select_mutation_key_ref(&request.mutations)
            .and_then(|mutation| extract_mutation_routing_key(&routing.key_recipe_cache, mutation));

        let has_affinity = transaction_id
            .as_deref()
            .and_then(|id| routing.location_router.get_transaction_affinity(id))
            .is_some();

        // If neither active affinity nor a routing key exists, fall back immediately to channel pool round-robin.
        if !has_affinity && routing_key.is_none() {
            return (None, transaction_id);
        }

        let context = RoutingContext {
            transaction_id: transaction_id.as_deref(),
            routing_key: routing_key.as_deref(),
            prefer_leader: true,
            use_transaction_affinity: has_affinity,
        };
        let database_id = routing.cache_updater.database_id();
        let schema_generation = routing.key_recipe_cache.schema_generation();
        let resolved = routing.location_router.resolve_route(
            &context,
            None,
            database_id,
            schema_generation,
            UNASSIGNED_OPERATION_UID,
            None,
        );
        if let Some(hint) = resolved.routing_hint {
            request.routing_hint = Some(hint);
        }
        (Some(resolved.connection), transaction_id)
    }

    fn post_route_commit(
        &self,
        transaction_id: Option<Bytes>,
        _connection: Option<&ServerConnection>,
        _result: &Result<CommitResponse>,
    ) {
        self.clear_transaction_affinity_routing(transaction_id.as_deref());
    }

    fn pre_route_execute_batch_dml(
        &self,
        request: &mut ExecuteBatchDmlRequest,
    ) -> (Option<ServerConnection>, bool) {
        self.pre_route_transaction_selector(request.transaction.as_ref())
    }

    fn post_route_execute_batch_dml(
        &self,
        is_read_write_begin: bool,
        connection: Option<&ServerConnection>,
        result: &Result<ExecuteBatchDmlResponse>,
    ) {
        let transaction_id = result
            .as_ref()
            .ok()
            .and_then(|response| response.result_sets.first())
            .and_then(|result_set| result_set.metadata.as_ref())
            .and_then(|metadata| metadata.transaction.as_ref())
            .map(|transaction| transaction.id.as_ref());
        self.record_transaction_affinity_routing(is_read_write_begin, transaction_id, connection);
    }

    /// Intercepts unary [`ExecuteSqlRequest`] calls to resolve node routing before dispatch.
    ///
    /// Prepares the query in [`KeyRecipeCache`] to assign or retrieve an operation UID and extract
    /// any cached routing key, then delegates to [`DatabaseClient::route_and_attach_hint`] to resolve
    /// an existing transaction affinity or direct replica connection and attach the [`RoutingHint`].
    fn pre_route_execute_sql(
        &self,
        request: &mut ExecuteSqlRequest,
    ) -> (Option<ServerConnection>, bool) {
        let is_read_write_begin = is_read_write_begin(request.transaction.as_ref());
        let (operation_uid, routing_key) = match &self.location_routing {
            Some(routing) => {
                extract_execute_sql_request_routing(&routing.key_recipe_cache, request)
            }
            None => (UNASSIGNED_OPERATION_UID, None),
        };
        let connection = self.route_and_attach_hint(
            request.transaction.as_ref(),
            request.directed_read_options.as_ref(),
            operation_uid,
            routing_key.as_deref(),
            &mut request.routing_hint,
        );
        (connection, is_read_write_begin)
    }

    fn post_route_execute_sql(
        &self,
        is_read_write_begin: bool,
        connection: Option<&ServerConnection>,
        result: &Result<ResultSet>,
    ) {
        let transaction_id = result
            .as_ref()
            .ok()
            .and_then(|result_set| result_set.metadata.as_ref())
            .and_then(|metadata| metadata.transaction.as_ref())
            .map(|transaction| transaction.id.as_ref());
        self.record_transaction_affinity_routing(is_read_write_begin, transaction_id, connection);
    }

    fn pre_route_rollback(
        &self,
        request: &mut RollbackRequest,
    ) -> (Option<ServerConnection>, Option<Bytes>) {
        let Some(_routing) = &self.location_routing else {
            return (None, None);
        };
        let transaction_id =
            (!request.transaction_id.is_empty()).then(|| request.transaction_id.clone());
        let context = RoutingContext {
            transaction_id: transaction_id.as_deref(),
            routing_key: None,
            prefer_leader: true,
            use_transaction_affinity: transaction_id.is_some(),
        };
        let connection = self.resolve_routing_connection(&context);
        (connection, transaction_id)
    }

    fn post_route_rollback(
        &self,
        transaction_id: Option<Bytes>,
        _connection: Option<&ServerConnection>,
        _result: &Result<()>,
    ) {
        self.clear_transaction_affinity_routing(transaction_id.as_deref());
    }

    fn pre_route_partition_query(
        &self,
        request: &mut PartitionQueryRequest,
    ) -> (Option<ServerConnection>, ()) {
        (
            self.pre_route_transaction_selector(request.transaction.as_ref())
                .0,
            (),
        )
    }

    fn pre_route_partition_read(
        &self,
        request: &mut PartitionReadRequest,
    ) -> (Option<ServerConnection>, ()) {
        let Some(routing) = &self.location_routing else {
            return (None, ());
        };
        let routing_key =
            extract_proto_partition_read_request_routing_key(&routing.key_recipe_cache, request);
        let context =
            routing_context_from_selector(request.transaction.as_ref(), routing_key.as_deref());
        (self.resolve_routing_connection(&context), ())
    }

    fn post_route_noop<T>(
        &self,
        _context: (),
        _connection: Option<&ServerConnection>,
        _result: &Result<T>,
    ) {
    }

    fn pre_route_transaction_selector(
        &self,
        transaction: Option<&TransactionSelector>,
    ) -> (Option<ServerConnection>, bool) {
        let Some(_routing) = &self.location_routing else {
            return (None, false);
        };
        let context = routing_context_from_selector(transaction, None);
        let connection = self.resolve_routing_connection(&context);
        let is_read_write_begin = is_read_write_begin(transaction);
        (connection, is_read_write_begin)
    }

    fn record_transaction_affinity_routing(
        &self,
        should_record: bool,
        transaction_id: Option<&[u8]>,
        connection: Option<&ServerConnection>,
    ) {
        if !should_record {
            return;
        }
        let Some(transaction_id) = transaction_id.filter(|id| !id.is_empty()) else {
            return;
        };
        let Some(routing) = &self.location_routing else {
            return;
        };
        let address = match connection {
            Some(connection) => connection.address(),
            None => routing
                .location_router
                .connection_cache()
                .default_connection()
                .address(),
        };
        routing
            .location_router
            .record_transaction_affinity(transaction_id, address);
    }

    fn clear_transaction_affinity_routing(&self, transaction_id: Option<&[u8]>) {
        let Some(transaction_id) = transaction_id.filter(|id| !id.is_empty()) else {
            return;
        };
        let Some(routing) = &self.location_routing else {
            return;
        };
        routing
            .location_router
            .clear_transaction_affinity(transaction_id);
    }
}

fn routing_context_from_selector<'a>(
    transaction: Option<&'a TransactionSelector>,
    routing_key: Option<&'a [u8]>,
) -> RoutingContext<'a> {
    let transaction_id = extract_transaction_id(transaction);
    let prefer_leader = prefer_leader_from_selector(transaction);
    RoutingContext {
        transaction_id,
        routing_key,
        prefer_leader,
        use_transaction_affinity: transaction_id.is_some(),
    }
}

fn extract_transaction_id(transaction: Option<&TransactionSelector>) -> Option<&[u8]> {
    match transaction.and_then(|t| t.selector.as_ref()) {
        Some(Selector::Id(id)) => Some(id.as_ref()),
        _ => None,
    }
}

fn prefer_leader_from_selector(selector: Option<&TransactionSelector>) -> bool {
    let Some(selector) = selector else {
        return true;
    };
    match &selector.selector {
        Some(Selector::Begin(options)) => prefer_leader_from_options(Some(options)),
        Some(Selector::SingleUse(options)) => prefer_leader_from_options(Some(options)),
        _ => true,
    }
}

fn prefer_leader_from_options(options: Option<&TransactionOptions>) -> bool {
    let Some(options) = options else {
        return true;
    };
    match &options.mode {
        Some(Mode::ReadOnly(read_only)) => match &read_only.timestamp_bound {
            Some(TimestampBound::Strong(strong)) => *strong,
            Some(_) => false,
            None => true,
        },
        _ => true,
    }
}

fn is_read_write_options(options: Option<&TransactionOptions>) -> bool {
    let Some(options) = options else {
        return false;
    };
    matches!(&options.mode, Some(Mode::ReadWrite(_)))
}

fn is_read_write_begin(selector: Option<&TransactionSelector>) -> bool {
    let Some(selector) = selector else {
        return false;
    };
    match &selector.selector {
        Some(Selector::Begin(options)) => is_read_write_options(Some(options)),
        _ => false,
    }
}
/// A builder for [DatabaseClient].
pub struct DatabaseClientBuilder {
    spanner: Spanner,
    database_name: String,
    database_role: Option<String>,
    options: Option<RequestOptions>,
    leader_aware_routing_enabled: bool,
    location_aware_routing_enabled: Option<bool>,
}

impl DatabaseClientBuilder {
    pub(crate) fn new(spanner: Spanner, database_name: String) -> Self {
        Self {
            spanner,
            database_name,
            database_role: None,
            options: None,
            leader_aware_routing_enabled: true,
            location_aware_routing_enabled: None,
        }
    }

    /// Sets the database role for the client.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::Spanner;
    /// # async fn sample() -> anyhow::Result<()> {
    ///     let spanner = Spanner::builder().build().await?;
    ///     let database_client = spanner
    ///         .database_client("projects/my-project/instances/my-instance/databases/my-db")
    ///         .with_database_role("my-role")
    ///         .build()
    ///         .await?;
    ///     # Ok(())
    /// # }
    /// ```
    ///
    /// Database roles are used for Fine-Grained Access Control (FGAC).
    /// You can assign a database role to a session, and that role determines the permissions for that session.
    /// For more information, see [Access with FGAC](https://docs.cloud.google.com/spanner/docs/access-with-fgac).
    pub fn with_database_role(mut self, role: impl Into<String>) -> Self {
        self.database_role = Some(role.into());
        self
    }

    /// Sets the request options that will be used when creating the multiplexed
    /// session for the client.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::Spanner;
    /// # use google_cloud_gax::options::RequestOptions;
    /// # use std::time::Duration;
    /// # async fn sample() -> anyhow::Result<()> {
    ///     let spanner = Spanner::builder().build().await?;
    ///     let mut options = RequestOptions::default();
    ///     options.set_attempt_timeout(Duration::from_secs(60));
    ///     let database_client = spanner
    ///         .database_client("projects/my-project/instances/my-instance/databases/my-db")
    ///         .with_request_options(options)
    ///         .build()
    ///         .await?;
    ///     # Ok(())
    /// # }
    /// ```
    pub fn with_request_options(mut self, options: crate::RequestOptions) -> Self {
        self.options = Some(options);
        self
    }

    /// Sets whether Leader-Aware Routing (LAR) is enabled for read/write transactions.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::Spanner;
    /// # async fn sample() -> anyhow::Result<()> {
    ///     let spanner = Spanner::builder().build().await?;
    ///     let database_client = spanner
    ///         .database_client("projects/my-project/instances/my-instance/databases/my-db")
    ///         .with_leader_aware_routing(true)
    ///         .build()
    ///         .await?;
    ///     # Ok(())
    /// # }
    /// ```
    ///
    /// When LAR is enabled, modifying operations (Read-Write, Write-Only, and Partitioned DML
    /// transactions) automatically route requests directly to the Spanner leader replica. This
    /// eliminates internal forwarding hops between replicas and reduces overall transaction latency.
    ///
    /// Enabled by default.
    ///
    /// See also: <https://docs.cloud.google.com/spanner/docs/leader-aware-routing>
    pub fn with_leader_aware_routing(mut self, enabled: bool) -> Self {
        self.leader_aware_routing_enabled = enabled;
        self
    }

    #[cfg(test)]
    pub(crate) fn with_location_aware_routing(mut self, enabled: bool) -> Self {
        self.location_aware_routing_enabled = Some(enabled);
        self
    }

    /// Builds the [DatabaseClient] and creates a single multiplexed session that
    /// will be used for all operations on the database.
    pub async fn build(self) -> crate::Result<DatabaseClient> {
        let is_omni = self.spanner.instance_type() == InstanceType::Omni;
        let database_name = if is_omni {
            format_database_name(&self.database_name)
        } else {
            self.database_name
        };

        let o11y = Arc::new(
            Observability::init(
                &self.spanner.config,
                self.spanner.instance_type(),
                &database_name,
                self.spanner.is_emulator(),
            )
            .await,
        );
        // TODO: Enable location-aware routing by default for Omni instances once fully stabilized.
        let location_aware_routing_enabled = self
            .location_aware_routing_enabled
            .or_else(|| {
                env::var("GOOGLE_SPANNER_EXPERIMENTAL_LOCATION_API")
                    .ok()
                    .and_then(|v| v.parse::<bool>().ok())
            })
            .unwrap_or(false);

        let session_maintainer = ManagedSessionMaintainer::create_and_start_maintenance(
            self.spanner.clone(),
            database_name,
            self.database_role.unwrap_or_default(),
            self.options.unwrap_or_default(),
            Arc::clone(&o11y),
        )
        .await?;

        let location_routing = location_aware_routing_enabled.then(|| {
            Arc::new(LocationRoutingState::new(
                session_maintainer.database_name.clone(),
                &self.spanner,
            ))
        });

        Ok(DatabaseClient {
            spanner: self.spanner,
            session_maintainer,
            leader_aware_routing_enabled: self.leader_aware_routing_enabled,
            location_routing,
            o11y,
        })
    }
}

#[derive(Debug)]
pub(crate) struct LocationRoutingState {
    pub(crate) location_router: Arc<LocationRouter>,
    pub(crate) cache_updater: Arc<CacheUpdater>,
    pub(crate) key_recipe_cache: Arc<KeyRecipeCache>,
    pub(crate) cache_subscriber: CacheSubscriber,
    pub(crate) endpoint_lifecycle_manager: Arc<EndpointLifecycleManager>,
}

const DEFAULT_ENDPOINT: &str = "spanner.googleapis.com:443";
/// Sentinel value representing an unassigned or absent operation UID.
const UNASSIGNED_OPERATION_UID: u64 = 0;

impl LocationRoutingState {
    fn new(database_name: String, spanner: &Spanner) -> Self {
        let default_endpoint = spanner
            .config
            .endpoint
            .as_deref()
            .unwrap_or(DEFAULT_ENDPOINT)
            .to_string();

        let default_channel = spanner
            .channels
            .first()
            .cloned()
            .expect("Spanner client must have at least one channel");

        let default_connection = ServerConnection::new(default_endpoint, default_channel);
        let connection_cache = Arc::new(ConnectionCache::new(default_connection));
        let endpoint_lifecycle_manager = Arc::new(EndpointLifecycleManager::with_client_config(
            Arc::clone(&connection_cache),
            spanner.config.clone(),
        ));
        let key_range_cache = Arc::new(KeyRangeCache::new());
        let key_recipe_cache = Arc::new(KeyRecipeCache::new());
        let cooldown_tracker = Arc::new(EndpointCooldownTracker::new());
        let latency_registry = Arc::new(LatencyRegistry::new());
        let location_router = Arc::new(LocationRouter::new(
            database_name.clone(),
            Arc::clone(&key_range_cache),
            Arc::clone(&connection_cache),
            Arc::clone(&endpoint_lifecycle_manager),
            cooldown_tracker,
            latency_registry,
        ));
        let cache_updater = Arc::new(CacheUpdater::new(
            database_name.clone(),
            key_range_cache,
            Arc::clone(&key_recipe_cache),
            connection_cache,
            Arc::clone(&endpoint_lifecycle_manager),
            spanner.config.clone(),
        ));
        let cache_subscriber =
            CacheSubscriber::start(database_name, spanner.clone(), Arc::clone(&cache_updater));
        endpoint_lifecycle_manager.start_maintenance();

        Self {
            location_router,
            cache_updater,
            key_recipe_cache,
            cache_subscriber,
            endpoint_lifecycle_manager,
        }
    }
}

impl Drop for LocationRoutingState {
    fn drop(&mut self) {
        self.cache_subscriber.stop();
        self.endpoint_lifecycle_manager.stop_maintenance();
    }
}

trait ObserveResponse {
    fn observe(&self, client: &DatabaseClient);
}

impl ObserveResponse for Transaction {
    fn observe(&self, client: &DatabaseClient) {
        client.observe_cache_update(self.cache_update.clone());
    }
}

impl ObserveResponse for CommitResponse {
    fn observe(&self, client: &DatabaseClient) {
        client.observe_cache_update(self.cache_update.clone());
    }
}

impl ObserveResponse for ExecuteBatchDmlResponse {
    fn observe(&self, client: &DatabaseClient) {
        for result_set in &self.result_sets {
            result_set.observe(client);
        }
    }
}

impl ObserveResponse for ResultSet {
    fn observe(&self, client: &DatabaseClient) {
        client.observe_cache_update(self.cache_update.clone());
    }
}

impl ObserveResponse for () {
    fn observe(&self, _client: &DatabaseClient) {}
}

impl ObserveResponse for PartitionResponse {
    fn observe(&self, _client: &DatabaseClient) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::SpannerBuilderExt;
    use crate::error::internal_error;
    use crate::model::key_recipe::Part;
    use crate::model::key_recipe::part::{NullOrder, Order};
    use crate::model::mutation::Operation;
    use crate::model::tablet::Role;
    use crate::model::transaction_options::{PartitionedDml, ReadOnly, ReadWrite};
    use crate::model::{
        CacheUpdate, CommitResponse, Group, KeyRecipe, KeySet, Range, RecipeList, Tablet,
        TransactionOptions, Type, TypeCode,
    };
    use crate::mutation::Mutation;
    use crate::result_set::tests::adapt;
    use crate::routing::key_extractor::extract_proto_read_request_routing_key;
    use crate::routing::key_range_cache::RangeMode;
    use crate::statement::Statement;
    use bytes::Bytes;
    use gaxi::grpc::tonic::Response;
    use gaxi::options::ClientConfig;
    use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
    use google_cloud_test_macros::tokio_test_no_panics;
    use mockall::Sequence;
    use spanner_grpc_mock::google::spanner::v1 as mock_v1;
    use spanner_grpc_mock::{MockSpanner, start};
    use std::sync::Mutex;
    use tokio::sync::mpsc;

    fn create_test_mock() -> MockSpanner {
        let mut mock = MockSpanner::new();
        mock.expect_create_session().returning(|req| {
            let req = req.into_inner();
            let session = req.session.expect("session present in request");
            Ok(Response::new(mock_v1::Session {
                name: "projects/p/instances/i/databases/d/sessions/s1".to_string(),
                multiplexed: session.multiplexed,
                ..Default::default()
            }))
        });
        mock.expect_fetch_cache_update().returning(|_| {
            let (_sender, receiver) = mpsc::channel(1);
            Ok(Response::from(receiver))
        });
        mock
    }

    #[test]
    fn test_auto_traits() {
        use static_assertions::assert_impl_all;
        assert_impl_all!(DatabaseClient: Send, Sync, Clone, std::fmt::Debug);
    }

    #[tokio_test_no_panics]
    async fn test_database_client_builder() {
        let mut mock = MockSpanner::new();
        mock.expect_create_session().once().returning(|req| {
            let req = req.into_inner();
            let session = req.session.unwrap();
            assert!(session.multiplexed);
            assert_eq!(session.creator_role, "test-role");

            Ok(gaxi::grpc::tonic::Response::new(
                spanner_grpc_mock::google::spanner::v1::Session {
                    name: "projects/test-project/instances/test-instance/databases/test-db/sessions/123".to_string(),
                    multiplexed: true,
                    creator_role: "test-role".to_string(),
                    ..Default::default()
                },
            ))
        });

        let (address, _server) = start("0.0.0.0:0", mock)
            .await
            .expect("Failed to start mock server");
        let spanner = Spanner::builder()
            .with_endpoint(address)
            .with_credentials(Anonymous::new().build())
            .build()
            .await
            .expect("Failed to build client");

        let db_client = spanner
            .database_client("projects/test-project/instances/test-instance/databases/test-db")
            .with_database_role("test-role")
            .build()
            .await
            .expect("Failed to create DatabaseClient");

        let session = db_client
            .session_maintainer
            .session
            .read()
            .expect("failed to read session")
            .session
            .clone();
        assert_eq!(
            session.name,
            "projects/test-project/instances/test-instance/databases/test-db/sessions/123"
        );
        assert!(session.multiplexed);
        assert_eq!(session.creator_role, "test-role");
    }

    #[tokio_test_no_panics]
    async fn test_database_client_builder_with_options() {
        let mut mock = MockSpanner::new();
        let mut seq = Sequence::new();
        mock.expect_create_session()
            .once()
            .in_sequence(&mut seq)
            .returning(|_| Err(gaxi::grpc::tonic::Status::unavailable("unavailable")));
        mock.expect_create_session()
            .once()
            .in_sequence(&mut seq)
            .returning(|req| {
                let req = req.into_inner();
                let session = req.session.unwrap();
                assert!(session.multiplexed);
                Ok(gaxi::grpc::tonic::Response::new(
                    spanner_grpc_mock::google::spanner::v1::Session {
                        name: "projects/test-project/instances/test-instance/databases/test-db/sessions/123".to_string(),
                        multiplexed: true,
                        ..Default::default()
                    },
                ))
            });

        let (address, _server) = start("0.0.0.0:0", mock)
            .await
            .expect("Failed to start mock server");
        let spanner = Spanner::builder()
            .with_endpoint(address)
            .with_credentials(Anonymous::new().build())
            .build()
            .await
            .expect("Failed to build client");

        let mut options = crate::RequestOptions::default();
        options.set_retry_policy(google_cloud_gax::retry_policy::Aip194Strict);
        options.set_idempotency(true);

        let db_client = spanner
            .database_client("projects/test-project/instances/test-instance/databases/test-db")
            .with_request_options(options)
            .build()
            .await
            .expect("Failed to create DatabaseClient");

        let session = db_client
            .session_maintainer
            .session
            .read()
            .expect("failed to read session")
            .session
            .clone();
        assert_eq!(
            session.name,
            "projects/test-project/instances/test-instance/databases/test-db/sessions/123"
        );
    }

    #[tokio_test_no_panics]
    async fn test_database_client_builder_error() {
        let mut mock = MockSpanner::new();
        mock.expect_create_session().once().returning(|_| {
            Err(gaxi::grpc::tonic::Status::permission_denied(
                "permission denied",
            ))
        });

        let (address, _server) = start("0.0.0.0:0", mock)
            .await
            .expect("Failed to start mock server");
        let spanner = Spanner::builder()
            .with_endpoint(address)
            .with_credentials(Anonymous::new().build())
            .build()
            .await
            .expect("Failed to build client");

        let result = spanner
            .database_client("projects/test-project/instances/test-instance/databases/test-db")
            .build()
            .await;

        match result {
            Ok(_) => panic!("Client creation should have failed"),
            Err(e) => assert_eq!(
                e.status().map(|s| s.code),
                Some(google_cloud_gax::error::rpc::Code::PermissionDenied)
            ),
        }
    }

    #[tokio_test_no_panics]
    async fn database_client_builder_with_location_aware_routing_flag() {
        let mock = create_test_mock();

        let (address, _server) = start("0.0.0.0:0", mock)
            .await
            .expect("Failed to start mock server");
        let spanner_cloud = Spanner::builder()
            .with_endpoint(address.clone())
            .with_credentials(Anonymous::new().build())
            .build()
            .await
            .expect("Failed to build client");

        let db_client_default = spanner_cloud
            .database_client("projects/p/instances/i/databases/d")
            .build()
            .await
            .expect("default build should succeed");
        assert!(
            !db_client_default.is_location_aware_routing_enabled(),
            "location-aware routing should be disabled by default on standard Spanner"
        );

        let spanner_omni = Spanner::builder()
            .with_endpoint(address)
            .with_instance_type(InstanceType::Omni)
            .with_credentials(Anonymous::new().build())
            .build()
            .await
            .expect("Failed to build client");

        let db_client_omni_default = spanner_omni
            .database_client("projects/p/instances/i/databases/d")
            .build()
            .await
            .expect("omni default build should succeed");
        assert!(
            !db_client_omni_default.is_location_aware_routing_enabled(),
            "location-aware routing should be disabled by default for Omni instances"
        );

        let db_client_enabled = spanner_omni
            .database_client("projects/p/instances/i/databases/d")
            .with_location_aware_routing(true)
            .build()
            .await
            .expect("build with enabled location-aware routing should succeed");
        assert!(
            db_client_enabled.is_location_aware_routing_enabled(),
            "location-aware routing should be enabled when explicitly configured"
        );
    }

    #[tokio_test_no_panics]
    async fn database_client_observe_cache_update_database_id_switch_clears_caches() {
        let mock = create_test_mock();

        let (address, _server) = start("0.0.0.0:0", mock)
            .await
            .expect("Failed to start mock server");
        let spanner = Spanner::builder()
            .with_endpoint(address)
            .with_instance_type(InstanceType::Omni)
            .with_credentials(Anonymous::new().build())
            .build()
            .await
            .expect("Failed to build client");

        let db_client = spanner
            .database_client("projects/p/instances/i/databases/d")
            .with_location_aware_routing(true)
            .build()
            .await
            .expect("build should succeed");

        let recipe_list =
            RecipeList::new().set_recipe(vec![KeyRecipe::new().set_table_name("Users")]);
        let cache_update_db1 = CacheUpdate::new()
            .set_database_id(1u64)
            .set_key_recipes(recipe_list)
            .set_group(vec![
                Group::new()
                    .set_group_uid(100u64)
                    .set_leader_index(0)
                    .set_tablets(vec![
                        Tablet::new().set_server_address("node-100.spanner.internal:15000"),
                    ]),
            ])
            .set_range(vec![
                Range::new()
                    .set_group_uid(100u64)
                    .set_start_key(b"a".to_vec())
                    .set_limit_key(b"z".to_vec()),
            ]);

        db_client.observe_cache_update(Some(cache_update_db1));
        assert_eq!(db_client.database_id(), Some(1));
        assert!(
            db_client
                .key_recipe_cache()
                .expect("recipe cache present")
                .get_table_recipe("Users")
                .is_some(),
            "Users table recipe should be present for db1"
        );
        assert!(
            db_client
                .location_router()
                .expect("router present")
                .key_range_cache()
                .find_range(b"m", &[], RangeMode::CoveringSplit)
                .is_some(),
            "range covering 'm' should be present for db1"
        );

        let cache_update_db2 = CacheUpdate::new()
            .set_database_id(2u64)
            .set_group(vec![
                Group::new()
                    .set_group_uid(200u64)
                    .set_leader_index(0)
                    .set_tablets(vec![
                        Tablet::new().set_server_address("node-200.spanner.internal:15000"),
                    ]),
            ])
            .set_range(vec![
                Range::new()
                    .set_group_uid(200u64)
                    .set_start_key(b"0".to_vec())
                    .set_limit_key(b"9".to_vec()),
            ]);

        db_client.observe_cache_update(Some(cache_update_db2));
        assert_eq!(db_client.database_id(), Some(2));
        assert!(
            db_client
                .key_recipe_cache()
                .expect("recipe cache present")
                .get_table_recipe("Users")
                .is_none(),
            "Old recipes should be cleared on database_id switch"
        );
        assert!(
            db_client
                .location_router()
                .expect("router present")
                .key_range_cache()
                .find_range(b"m", &[], RangeMode::CoveringSplit)
                .is_none(),
            "Old ranges should be cleared on database_id switch"
        );
        assert!(
            db_client
                .location_router()
                .expect("router present")
                .key_range_cache()
                .find_range(b"5", &[], RangeMode::CoveringSplit)
                .is_some(),
            "New ranges for db2 should be present"
        );
    }

    #[tokio_test_no_panics]
    async fn database_client_observe_metadata_populates_key_recipe_cache() {
        let mock = create_test_mock();

        let (address, _server) = start("0.0.0.0:0", mock)
            .await
            .expect("Failed to start mock server");
        let spanner = Spanner::builder()
            .with_endpoint(address)
            .with_instance_type(InstanceType::Omni)
            .with_credentials(Anonymous::new().build())
            .build()
            .await
            .expect("Failed to build client");

        let db_client = spanner
            .database_client("projects/p/instances/i/databases/d")
            .with_location_aware_routing(true)
            .build()
            .await
            .expect("build should succeed");

        let recipe_list = RecipeList::new().set_recipe(vec![
            KeyRecipe::new().set_table_name("Users"),
            KeyRecipe::new().set_index_name("UsersByEmail"),
        ]);
        let cache_update = CacheUpdate::new().set_key_recipes(recipe_list);

        db_client.observe_cache_update(Some(cache_update));

        let recipe_cache = db_client
            .key_recipe_cache()
            .expect("recipe cache should be present");
        assert!(
            recipe_cache.get_table_recipe("Users").is_some(),
            "Users table recipe should be cached after observing cache update"
        );
        assert!(
            recipe_cache.get_index_recipe("UsersByEmail").is_some(),
            "UsersByEmail index recipe should be cached after observing cache update"
        );
    }

    #[tokio_test_no_panics]
    async fn database_client_observe_cache_update_populates_key_range_cache() {
        let mock = create_test_mock();

        let (address, _server) = start("0.0.0.0:0", mock)
            .await
            .expect("Failed to start mock server");
        let spanner = Spanner::builder()
            .with_endpoint(address)
            .with_instance_type(InstanceType::Omni)
            .with_credentials(Anonymous::new().build())
            .build()
            .await
            .expect("Failed to build client");

        let db_client = spanner
            .database_client("projects/p/instances/i/databases/d")
            .with_location_aware_routing(true)
            .build()
            .await
            .expect("build should succeed");

        let cache_update = CacheUpdate::new()
            .set_group(vec![
                Group::new()
                    .set_group_uid(1u64)
                    .set_leader_index(0)
                    .set_tablets(vec![
                        Tablet::new().set_server_address("node-1.spanner.internal:15000"),
                    ]),
            ])
            .set_range(vec![
                Range::new()
                    .set_group_uid(1u64)
                    .set_start_key(b"a".to_vec())
                    .set_limit_key(b"z".to_vec()),
            ]);

        db_client.observe_cache_update(Some(cache_update));

        let router = db_client
            .location_router()
            .expect("location router should be present");
        let found_range = router
            .key_range_cache()
            .find_range(b"m", &[], RangeMode::CoveringSplit);
        assert!(
            found_range.is_some(),
            "key range cache should find range covering 'm'"
        );
        let range = found_range.expect("range present");
        assert_eq!(range.group_uid, 1);
    }

    #[tokio_test_no_panics]
    async fn database_client_observe_commit_response_populates_key_range_cache() {
        let mock = create_test_mock();

        let (address, _server) = start("0.0.0.0:0", mock)
            .await
            .expect("Failed to start mock server");
        let spanner = Spanner::builder()
            .with_endpoint(address)
            .with_instance_type(InstanceType::Omni)
            .with_credentials(Anonymous::new().build())
            .build()
            .await
            .expect("Failed to build client");

        let db_client = spanner
            .database_client("projects/p/instances/i/databases/d")
            .with_location_aware_routing(true)
            .build()
            .await
            .expect("build should succeed");

        let cache_update = CacheUpdate::new()
            .set_group(vec![
                Group::new()
                    .set_group_uid(2u64)
                    .set_leader_index(0)
                    .set_tablets(vec![
                        Tablet::new().set_server_address("node-2.spanner.internal:15000"),
                    ]),
            ])
            .set_range(vec![
                Range::new()
                    .set_group_uid(2u64)
                    .set_start_key(b"0".to_vec())
                    .set_limit_key(b"9".to_vec()),
            ]);

        let commit_response = CommitResponse::new().set_cache_update(cache_update);
        commit_response.observe(&db_client);

        let router = db_client
            .location_router()
            .expect("location router should be present");
        let found_range = router
            .key_range_cache()
            .find_range(b"5", &[], RangeMode::CoveringSplit);
        assert!(
            found_range.is_some(),
            "key range cache should find range covering '5'"
        );
        let range = found_range.expect("range present");
        assert_eq!(range.group_uid, 2);
    }

    #[tokio_test_no_panics]
    async fn database_client_observe_cache_update_concurrent_access() {
        let mock = create_test_mock();

        let (address, _server) = start("0.0.0.0:0", mock)
            .await
            .expect("Failed to start mock server");
        let spanner = Spanner::builder()
            .with_endpoint(address)
            .with_instance_type(InstanceType::Omni)
            .with_credentials(Anonymous::new().build())
            .build()
            .await
            .expect("Failed to build client");

        let db_client = Arc::new(
            spanner
                .database_client("projects/p/instances/i/databases/d")
                .with_location_aware_routing(true)
                .build()
                .await
                .expect("build should succeed"),
        );

        let mut handles = Vec::new();
        for index in 0..10 {
            let client = Arc::clone(&db_client);
            let handle = tokio::spawn(async move {
                let cache_update = CacheUpdate::new()
                    .set_database_id((index + 1) as u64)
                    .set_key_recipes(RecipeList::new().set_recipe(vec![
                        KeyRecipe::new().set_table_name(format!("Table_{index}")),
                    ]));
                client.observe_cache_update(Some(cache_update));
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.await.expect("task completed successfully");
        }

        assert!(
            db_client.database_id().is_some(),
            "database_id should be set after concurrent updates"
        );
    }

    #[tokio_test_no_panics]
    async fn database_client_clone_shares_location_routing_state() {
        let mock = create_test_mock();

        let (address, _server) = start("0.0.0.0:0", mock)
            .await
            .expect("Failed to start mock server");
        let spanner = Spanner::builder()
            .with_endpoint(address)
            .with_instance_type(InstanceType::Omni)
            .with_credentials(Anonymous::new().build())
            .build()
            .await
            .expect("Failed to build client");

        let original_client = spanner
            .database_client("projects/p/instances/i/databases/d")
            .with_location_aware_routing(true)
            .build()
            .await
            .expect("build should succeed");

        assert!(original_client.cache_updater().is_some());
        assert!(original_client.location_router().is_some());
        assert!(original_client.key_recipe_cache().is_some());
        assert_eq!(original_client.database_id(), Some(0));

        let cloned_client = original_client.clone();

        let recipe_list =
            RecipeList::new().set_recipe(vec![KeyRecipe::new().set_table_name("SharedTable")]);
        let cache_update = CacheUpdate::new()
            .set_database_id(999u64)
            .set_key_recipes(recipe_list);

        // Observe on the clone
        cloned_client.observe_cache_update(Some(cache_update));

        // Original client must reflect the update through shared Arc<LocationRoutingState>
        assert_eq!(original_client.database_id(), Some(999));
        assert!(
            original_client
                .key_recipe_cache()
                .expect("recipe cache present")
                .get_table_recipe("SharedTable")
                .is_some(),
            "original client must see recipe cached via cloned client"
        );
    }

    #[tokio_test_no_panics]
    async fn database_client_accessors_and_observe_when_disabled() {
        let mock = create_test_mock();

        let (address, _server) = start("0.0.0.0:0", mock)
            .await
            .expect("Failed to start mock server");
        let spanner = Spanner::builder()
            .with_endpoint(address)
            .with_credentials(Anonymous::new().build())
            .build()
            .await
            .expect("Failed to build client");

        let db_client = spanner
            .database_client("projects/p/instances/i/databases/d")
            .build()
            .await
            .expect("build should succeed");

        assert!(!db_client.is_location_aware_routing_enabled());
        assert!(db_client.location_router().is_none());
        assert!(db_client.cache_updater().is_none());
        assert!(db_client.key_recipe_cache().is_none());
        assert_eq!(db_client.database_id(), None);

        // Observing None or Some update when disabled must be safe no-op
        db_client.observe_cache_update(None);
        let update = CacheUpdate::new().set_database_id(123u64);
        db_client.observe_cache_update(Some(update));
        assert_eq!(db_client.database_id(), None);
    }

    #[tokio_test_no_panics]
    async fn database_client_observe_execute_batch_dml_response() {
        let mock = create_test_mock();

        let (address, _server) = start("0.0.0.0:0", mock)
            .await
            .expect("Failed to start mock server");
        let spanner = Spanner::builder()
            .with_endpoint(address)
            .with_instance_type(InstanceType::Omni)
            .with_credentials(Anonymous::new().build())
            .build()
            .await
            .expect("Failed to build client");

        let db_client = spanner
            .database_client("projects/p/instances/i/databases/d")
            .with_location_aware_routing(true)
            .build()
            .await
            .expect("build should succeed");

        let cache_update1 = CacheUpdate::new().set_key_recipes(
            RecipeList::new().set_recipe(vec![KeyRecipe::new().set_table_name("BatchTable1")]),
        );
        let cache_update2 = CacheUpdate::new().set_key_recipes(
            RecipeList::new().set_recipe(vec![KeyRecipe::new().set_table_name("BatchTable2")]),
        );

        let result_set1 = crate::model::ResultSet::new().set_cache_update(cache_update1);
        let result_set2 = crate::model::ResultSet::new().set_cache_update(cache_update2);

        let batch_dml_response =
            ExecuteBatchDmlResponse::new().set_result_sets(vec![result_set1, result_set2]);

        batch_dml_response.observe(&db_client);

        let recipe_cache = db_client.key_recipe_cache().expect("recipe cache present");
        assert!(
            recipe_cache.get_table_recipe("BatchTable1").is_some(),
            "first result set cache update should be observed"
        );
        assert!(
            recipe_cache.get_table_recipe("BatchTable2").is_some(),
            "second result set cache update should be observed"
        );

        // Verify no-op ObserveResponse implementations
        ().observe(&db_client);
        PartitionResponse::new().observe(&db_client);
    }

    #[tokio_test_no_panics]
    async fn database_client_observe_cache_update_stale_database_id_aborts_ingestion() {
        let mock = create_test_mock();

        let (address, _server) = start("0.0.0.0:0", mock)
            .await
            .expect("Failed to start mock server");
        let spanner = Spanner::builder()
            .with_endpoint(address)
            .with_instance_type(InstanceType::Omni)
            .with_credentials(Anonymous::new().build())
            .build()
            .await
            .expect("Failed to build client");

        let db_client = spanner
            .database_client("projects/p/instances/i/databases/d")
            .with_location_aware_routing(true)
            .build()
            .await
            .expect("build should succeed");

        // 1. Ingest newer database_id = 200
        let cache_update_new = CacheUpdate::new().set_database_id(200u64).set_key_recipes(
            RecipeList::new().set_recipe(vec![KeyRecipe::new().set_table_name("NewTable")]),
        );
        db_client.observe_cache_update(Some(cache_update_new));
        assert_eq!(db_client.database_id(), Some(200));
        assert!(
            db_client
                .key_recipe_cache()
                .expect("recipe cache present")
                .get_table_recipe("NewTable")
                .is_some()
        );

        // 2. Ingest stale database_id = 100 from an older RPC
        let cache_update_stale = CacheUpdate::new().set_database_id(100u64).set_key_recipes(
            RecipeList::new().set_recipe(vec![KeyRecipe::new().set_table_name("StaleTable")]),
        );
        db_client.observe_cache_update(Some(cache_update_stale));

        // Active database ID must remain 200 and stale table recipe must NOT be ingested
        assert_eq!(db_client.database_id(), Some(200));
        assert!(
            db_client
                .key_recipe_cache()
                .expect("recipe cache present")
                .get_table_recipe("StaleTable")
                .is_none(),
            "stale update must be aborted and must not pollute cache"
        );
    }

    #[tokio_test_no_panics]
    async fn database_client_observe_cache_update_concurrent_id_switch() {
        let mock = create_test_mock();

        let (address, _server) = start("0.0.0.0:0", mock)
            .await
            .expect("Failed to start mock server");
        let spanner = Spanner::builder()
            .with_endpoint(address)
            .with_instance_type(InstanceType::Omni)
            .with_credentials(Anonymous::new().build())
            .build()
            .await
            .expect("Failed to build client");

        let db_client = Arc::new(
            spanner
                .database_client("projects/p/instances/i/databases/d")
                .with_location_aware_routing(true)
                .build()
                .await
                .expect("build should succeed"),
        );

        // Initial setup for database 1
        let initial_update = CacheUpdate::new().set_database_id(1u64).set_key_recipes(
            RecipeList::new().set_recipe(vec![KeyRecipe::new().set_table_name("InitialTable")]),
        );
        db_client.observe_cache_update(Some(initial_update));

        let mut handles = Vec::new();
        // Spawn concurrent incremental updates for database 1
        for index in 0..20 {
            let client = Arc::clone(&db_client);
            let handle = tokio::spawn(async move {
                let cache_update = CacheUpdate::new().set_database_id(1u64).set_key_recipes(
                    RecipeList::new().set_recipe(vec![
                        KeyRecipe::new().set_table_name(format!("OldTable_{index}")),
                    ]),
                );
                client.observe_cache_update(Some(cache_update));
            });
            handles.push(handle);
        }

        // Spawn a concurrent database ID transition to database 2
        let client_switch = Arc::clone(&db_client);
        let switch_handle = tokio::spawn(async move {
            let cache_update = CacheUpdate::new().set_database_id(2u64).set_key_recipes(
                RecipeList::new().set_recipe(vec![KeyRecipe::new().set_table_name("NewTable_2")]),
            );
            client_switch.observe_cache_update(Some(cache_update));
        });
        handles.push(switch_handle);

        for handle in handles {
            handle.await.expect("task completed successfully");
        }

        assert_eq!(db_client.database_id(), Some(2));
        let recipe_cache = db_client.key_recipe_cache().expect("recipe cache present");
        assert!(
            recipe_cache.get_table_recipe("NewTable_2").is_some(),
            "new database recipes must be preserved"
        );
    }

    #[tokio_test_no_panics]
    async fn resolve_read_channel_cloud_spanner_uses_default_channel() {
        let mock = create_test_mock();

        let (address, _server) = start("0.0.0.0:0", mock)
            .await
            .expect("Failed to start mock server");
        let spanner = Spanner::builder()
            .with_endpoint(address)
            .with_instance_type(InstanceType::Cloud)
            .with_credentials(Anonymous::new().build())
            .build()
            .await
            .expect("Failed to build client");

        let db_client = spanner
            .database_client("projects/p/instances/i/databases/d")
            .build()
            .await
            .expect("build should succeed");

        assert!(!db_client.is_location_aware_routing_enabled());
        let context = RoutingContext {
            routing_key: Some(b"Users.id=1".as_slice()),
            ..Default::default()
        };
        let connection = db_client.resolve_routing_connection(&context);
        assert!(connection.is_none());
    }

    #[tokio_test_no_panics]
    async fn resolve_routing_connection_omni_cold_start_and_cache_hit_and_cooldown() {
        let mock = create_test_mock();

        let (address, _server) = start("0.0.0.0:0", mock)
            .await
            .expect("Failed to start mock server");
        let spanner = Spanner::builder()
            .with_endpoint(address)
            .with_instance_type(InstanceType::Omni)
            .with_credentials(Anonymous::new().build())
            .build()
            .await
            .expect("Failed to build client");

        let db_client = spanner
            .database_client("projects/p/instances/i/databases/d")
            .with_location_aware_routing(true)
            .build()
            .await
            .expect("build should succeed");

        assert!(db_client.is_location_aware_routing_enabled());

        let mut key_set = KeySet::new();
        key_set
            .keys
            .push(vec![serde_json::Value::String("m".to_string())]);
        let read_request = ReadRequest::new().set_table("Users").set_key_set(key_set);

        // 1. Cold start: empty cache yields no routing key, so resolve returns None (preserving channel pool round-robin)
        let routing_key = db_client.location_routing.as_ref().and_then(|routing| {
            extract_proto_read_request_routing_key(&routing.key_recipe_cache, &read_request)
        });
        assert!(routing_key.is_none());
        let cold_start_context = routing_context_from_selector(None, routing_key.as_deref());
        let cold_start_connection = db_client.resolve_routing_connection(&cold_start_context);
        assert!(
            cold_start_connection.is_none(),
            "Cold start with unpopulated cache must resolve to None"
        );

        // 2. Populate cache with a split covering "a" to "z" on node address
        let node_address = "node-1.spanner.internal:15000";
        let recipe = KeyRecipe::new().set_table_name("Users").set_part(vec![
            Part::new().set_tag(50020u32),
            Part::new()
                .set_tag(1u32)
                .set_identifier("id")
                .set_type(Type::new().set_code(TypeCode::String))
                .set_order(Order::Ascending)
                .set_null_order(NullOrder::NotNull),
        ]);
        let cache_update = CacheUpdate::new()
            .set_database_id(1u64)
            .set_key_recipes(RecipeList::new().set_recipe(vec![recipe]))
            .set_group(vec![
                Group::new()
                    .set_group_uid(100u64)
                    .set_leader_index(0)
                    .set_tablets(vec![Tablet::new().set_server_address(node_address)]),
            ])
            .set_range(vec![
                Range::new()
                    .set_group_uid(100u64)
                    .set_start_key(b"".to_vec())
                    .set_limit_key(b"\xff".to_vec()),
            ]);
        db_client.observe_cache_update(Some(cache_update));

        let router = db_client.location_router().expect("router present");
        let _ = router
            .connection_cache()
            .get(node_address, &ClientConfig::default())
            .await
            .expect("should initialize connection");

        // 3. Cache hit: returns direct node connection
        let routing_key = db_client.location_routing.as_ref().and_then(|routing| {
            extract_proto_read_request_routing_key(&routing.key_recipe_cache, &read_request)
        });
        let hit_context = routing_context_from_selector(None, routing_key.as_deref());
        let hit_connection = db_client
            .resolve_routing_connection(&hit_context)
            .expect("hit connection present");
        assert_eq!(hit_connection.address(), node_address);

        // 4. Mark node on cooldown: falls back to default connection
        router.cooldown_tracker().record_failure(node_address);
        let fallback_connection = db_client
            .resolve_routing_connection(&hit_context)
            .expect("fallback connection present");
        assert_ne!(fallback_connection.address(), node_address);
    }

    #[test]
    fn extract_transaction_id_cases() {
        assert_eq!(extract_transaction_id(None), None);

        let selector_none = TransactionSelector::new();
        assert_eq!(extract_transaction_id(Some(&selector_none)), None);

        let selector_single = TransactionSelector::new().set_single_use(TransactionOptions::new());
        assert_eq!(extract_transaction_id(Some(&selector_single)), None);

        let selector_id = TransactionSelector::new().set_id(bytes::Bytes::from_static(b"txn-123"));
        assert_eq!(
            extract_transaction_id(Some(&selector_id)),
            Some(b"txn-123".as_slice())
        );
    }

    #[test]
    fn is_read_write_begin_cases() {
        assert!(!is_read_write_begin(None));

        let selector_id = TransactionSelector::new().set_id(bytes::Bytes::from_static(b"tx-1"));
        assert!(!is_read_write_begin(Some(&selector_id)));

        let selector_single = TransactionSelector::new()
            .set_single_use(TransactionOptions::new().set_read_write(ReadWrite::default()));
        assert!(!is_read_write_begin(Some(&selector_single)));

        let selector_begin_ro = TransactionSelector::new()
            .set_begin(TransactionOptions::new().set_read_only(ReadOnly::default()));
        assert!(!is_read_write_begin(Some(&selector_begin_ro)));

        let selector_begin_pdml = TransactionSelector::new()
            .set_begin(TransactionOptions::new().set_partitioned_dml(PartitionedDml::default()));
        assert!(!is_read_write_begin(Some(&selector_begin_pdml)));

        let selector_begin_rw = TransactionSelector::new()
            .set_begin(TransactionOptions::new().set_read_write(ReadWrite::default()));
        assert!(is_read_write_begin(Some(&selector_begin_rw)));
    }

    #[test]
    fn prefer_leader_from_selector_cases() {
        assert!(prefer_leader_from_selector(None));

        let selector_empty = TransactionSelector::new();
        assert!(prefer_leader_from_selector(Some(&selector_empty)));

        let selector_id = TransactionSelector::new().set_id(bytes::Bytes::from_static(b"tx-1"));
        assert!(prefer_leader_from_selector(Some(&selector_id)));

        // ReadOnly with default options (no timestamp bound) defaults to strong -> prefers leader
        let ro_default = ReadOnly::new();
        let selector_ro_default = TransactionSelector::new()
            .set_single_use(TransactionOptions::new().set_read_only(ro_default));
        assert!(prefer_leader_from_selector(Some(&selector_ro_default)));

        // Strong read prefers leader
        let ro_strong = ReadOnly::new().set_strong(true);
        let selector_ro_strong = TransactionSelector::new()
            .set_single_use(TransactionOptions::new().set_read_only(ro_strong));
        assert!(prefer_leader_from_selector(Some(&selector_ro_strong)));

        let ro_strong_false = ReadOnly::new().set_strong(false);
        let selector_ro_strong_false = TransactionSelector::new()
            .set_single_use(TransactionOptions::new().set_read_only(ro_strong_false));
        assert!(!prefer_leader_from_selector(Some(
            &selector_ro_strong_false
        )));

        // Exact staleness read does not prefer leader
        let ro_exact_staleness = ReadOnly::new().set_exact_staleness(wkt::Duration::clamp(10, 0));
        let selector_ro_exact_staleness = TransactionSelector::new()
            .set_single_use(TransactionOptions::new().set_read_only(ro_exact_staleness));
        assert!(!prefer_leader_from_selector(Some(
            &selector_ro_exact_staleness
        )));

        // Max staleness read does not prefer leader
        let ro_max_staleness = ReadOnly::new().set_max_staleness(wkt::Duration::clamp(10, 0));
        let selector_ro_max_staleness = TransactionSelector::new()
            .set_single_use(TransactionOptions::new().set_read_only(ro_max_staleness));
        assert!(!prefer_leader_from_selector(Some(
            &selector_ro_max_staleness
        )));

        // Min read timestamp does not prefer leader
        let ro_min_timestamp =
            ReadOnly::new().set_min_read_timestamp(wkt::Timestamp::clamp(100, 0));
        let selector_ro_min_timestamp = TransactionSelector::new()
            .set_single_use(TransactionOptions::new().set_read_only(ro_min_timestamp));
        assert!(!prefer_leader_from_selector(Some(
            &selector_ro_min_timestamp
        )));

        // Read timestamp does not prefer leader
        let ro_read_timestamp = ReadOnly::new().set_read_timestamp(wkt::Timestamp::clamp(100, 0));
        let selector_ro_read_timestamp = TransactionSelector::new()
            .set_single_use(TransactionOptions::new().set_read_only(ro_read_timestamp));
        assert!(!prefer_leader_from_selector(Some(
            &selector_ro_read_timestamp
        )));

        // ReadWrite and PartitionedDml prefer leader
        let selector_rw = TransactionSelector::new()
            .set_begin(TransactionOptions::new().set_read_write(ReadWrite::default()));
        assert!(prefer_leader_from_selector(Some(&selector_rw)));

        let selector_pdml = TransactionSelector::new()
            .set_begin(TransactionOptions::new().set_partitioned_dml(PartitionedDml::default()));
        assert!(prefer_leader_from_selector(Some(&selector_pdml)));

        // Empty options (mode: None) prefers leader
        let selector_empty_options =
            TransactionSelector::new().set_begin(TransactionOptions::new());
        assert!(prefer_leader_from_selector(Some(&selector_empty_options)));
    }

    #[test]
    fn routing_context_from_selector_cases() {
        // None selector
        let context_none = routing_context_from_selector(None, Some(b"key1"));
        assert_eq!(context_none.transaction_id, None);
        assert_eq!(context_none.routing_key, Some(b"key1".as_slice()));
        assert!(context_none.prefer_leader);
        assert!(!context_none.use_transaction_affinity);

        // SingleUse selector
        let selector_single = TransactionSelector::new()
            .set_single_use(TransactionOptions::new().set_read_write(ReadWrite::default()));
        let context_single = routing_context_from_selector(Some(&selector_single), Some(b"key2"));
        assert_eq!(context_single.transaction_id, None);
        assert!(!context_single.use_transaction_affinity);

        // Id selector
        let selector_id = TransactionSelector::new().set_id(bytes::Bytes::from_static(b"tx-123"));
        let context_id = routing_context_from_selector(Some(&selector_id), Some(b"key3"));
        assert_eq!(context_id.transaction_id, Some(b"tx-123".as_slice()));
        assert!(context_id.use_transaction_affinity);

        // Begin ReadWrite selector: no transaction_id yet, so use_transaction_affinity is false
        let selector_begin_rw = TransactionSelector::new()
            .set_begin(TransactionOptions::new().set_read_write(ReadWrite::default()));
        let context_begin_rw = routing_context_from_selector(Some(&selector_begin_rw), None);
        assert_eq!(context_begin_rw.transaction_id, None);
        assert!(!context_begin_rw.use_transaction_affinity);

        // Begin ReadOnly selector: no transaction_id yet, so use_transaction_affinity is false
        let selector_begin_ro = TransactionSelector::new()
            .set_begin(TransactionOptions::new().set_read_only(ReadOnly::default()));
        let context_begin_ro = routing_context_from_selector(Some(&selector_begin_ro), None);
        assert_eq!(context_begin_ro.transaction_id, None);
        assert!(!context_begin_ro.use_transaction_affinity);
    }

    #[tokio_test_no_panics]
    async fn resolve_routing_connection_omni_without_routing_info_returns_none() {
        let mock = create_test_mock();

        let (address, _server) = start("0.0.0.0:0", mock)
            .await
            .expect("Failed to start mock server");
        let spanner = Spanner::builder()
            .with_endpoint(address)
            .with_instance_type(InstanceType::Omni)
            .with_credentials(Anonymous::new().build())
            .build()
            .await
            .expect("Failed to build client");

        let db_client = spanner
            .database_client("projects/p/instances/i/databases/d")
            .with_location_aware_routing(true)
            .build()
            .await
            .expect("build should succeed");

        assert!(db_client.is_location_aware_routing_enabled());

        // 1. Neither transaction_id nor routing_key: must return None to preserve channel pool round-robin
        assert!(
            db_client
                .resolve_routing_connection(&RoutingContext::default())
                .is_none(),
            "Requests with neither transaction_id nor routing_key must resolve to None"
        );

        // 2. Transaction selector without an explicit ID and no routing key: must return None
        let selector_none = TransactionSelector::new();
        let context_none = routing_context_from_selector(Some(&selector_none), None);
        assert!(
            db_client
                .resolve_routing_connection(&context_none)
                .is_none(),
            "Requests with empty transaction selector must resolve to None"
        );

        let selector_single = TransactionSelector::new().set_single_use(TransactionOptions::new());
        let context_single = routing_context_from_selector(Some(&selector_single), None);
        assert!(
            db_client
                .resolve_routing_connection(&context_single)
                .is_none(),
            "Requests with single_use transaction selector must resolve to None"
        );
    }

    #[tokio_test_no_panics]
    async fn resolve_routing_connection_uses_transaction_affinity_when_transaction_id_present() {
        let mock = create_test_mock();

        let (address, _server) = start("0.0.0.0:0", mock)
            .await
            .expect("Failed to start mock server");
        let spanner = Spanner::builder()
            .with_endpoint(address)
            .with_instance_type(InstanceType::Omni)
            .with_credentials(Anonymous::new().build())
            .build()
            .await
            .expect("Failed to build client");

        let db_client = spanner
            .database_client("projects/p/instances/i/databases/d")
            .with_location_aware_routing(true)
            .build()
            .await
            .expect("build should succeed");

        // 1. Populate cache with node-1
        let node_address = "node-1.spanner.internal:15000";
        let cache_update = CacheUpdate::new()
            .set_database_id(1u64)
            .set_group(vec![
                Group::new()
                    .set_group_uid(100u64)
                    .set_leader_index(0)
                    .set_tablets(vec![Tablet::new().set_server_address(node_address)]),
            ])
            .set_range(vec![
                Range::new()
                    .set_group_uid(100u64)
                    .set_start_key(b"".to_vec())
                    .set_limit_key(b"\xff".to_vec()),
            ]);
        db_client.observe_cache_update(Some(cache_update));

        let router = db_client.location_router().expect("router present");
        let _ = router
            .connection_cache()
            .get(node_address, &ClientConfig::default())
            .await
            .expect("should initialize connection");

        let transaction_id = b"tx-rw-affinity-1";
        let selector = TransactionSelector::new().set_id(bytes::Bytes::from_static(transaction_id));
        let routing_key = b"Users.id=10";

        // 2. Initial request with transaction_id and routing_key:
        //    Resolves to node-1 AND records transaction affinity in LocationRouter.
        let initial_context =
            routing_context_from_selector(Some(&selector), Some(routing_key.as_slice()));
        let initial_connection = db_client
            .resolve_routing_connection(&initial_context)
            .expect("initial connection resolved");
        assert_eq!(initial_connection.address(), node_address);
        assert_eq!(
            router.get_transaction_affinity(transaction_id).as_deref(),
            Some(node_address),
            "Transaction affinity must be recorded after initial request"
        );

        // 3. Clear key range cache so any key lookup would miss.
        router.key_range_cache().clear();

        // 4. Subsequent request with transaction_id but NO routing_key (e.g. unkeyed query):
        //    Must resolve to node-1 via transaction affinity.
        let unkeyed_context = routing_context_from_selector(Some(&selector), None);
        let unkeyed_connection = db_client
            .resolve_routing_connection(&unkeyed_context)
            .expect("unkeyed request with transaction affinity must resolve to connection");
        assert_eq!(
            unkeyed_connection.address(),
            node_address,
            "Unkeyed query with active transaction_id must route to affinity node"
        );

        // 5. Subsequent request with transaction_id and a different routing key:
        //    Must STILL resolve to node-1 via transaction affinity.
        let different_key_context =
            routing_context_from_selector(Some(&selector), Some(b"Orders.id=99".as_slice()));
        let different_key_connection = db_client
            .resolve_routing_connection(&different_key_context)
            .expect("different key request with transaction affinity must resolve");
        assert_eq!(
            different_key_connection.address(),
            node_address,
            "Query with active transaction_id must prioritize affinity node over key"
        );

        // 6. Explicitly clear affinity (simulating Commit or Rollback).
        router.clear_transaction_affinity(transaction_id);
        assert_eq!(
            router.get_transaction_affinity(transaction_id),
            None,
            "Affinity must be cleared"
        );

        // 7. Request with transaction_id after affinity is cleared and with no routing_key:
        //    Must resolve to fallback connection.
        let post_cleanup_connection = db_client
            .resolve_routing_connection(&unkeyed_context)
            .expect("post-cleanup resolution fallback");
        assert_ne!(
            post_cleanup_connection.address(),
            node_address,
            "After affinity is cleared, request with no routing key must fall back"
        );
    }

    #[tokio_test_no_panics]
    async fn resolve_routing_connection_does_not_record_affinity_without_transaction_id() {
        let mock = create_test_mock();

        let (address, _server) = start("0.0.0.0:0", mock)
            .await
            .expect("Failed to start mock server");
        let spanner = Spanner::builder()
            .with_endpoint(address)
            .with_instance_type(InstanceType::Omni)
            .with_credentials(Anonymous::new().build())
            .build()
            .await
            .expect("Failed to build client");

        let db_client = spanner
            .database_client("projects/p/instances/i/databases/d")
            .with_location_aware_routing(true)
            .build()
            .await
            .expect("build should succeed");

        let node_address = "node-1.spanner.internal:15000";
        let cache_update = CacheUpdate::new()
            .set_database_id(1u64)
            .set_group(vec![
                Group::new()
                    .set_group_uid(100u64)
                    .set_leader_index(0)
                    .set_tablets(vec![Tablet::new().set_server_address(node_address)]),
            ])
            .set_range(vec![
                Range::new()
                    .set_group_uid(100u64)
                    .set_start_key(b"".to_vec())
                    .set_limit_key(b"\xff".to_vec()),
            ]);
        db_client.observe_cache_update(Some(cache_update));

        let router = db_client.location_router().expect("router present");
        let _ = router
            .connection_cache()
            .get(node_address, &ClientConfig::default())
            .await
            .expect("should initialize connection");

        let routing_key = b"Users.id=10";

        // 1. None transaction selector with routing key: routes to node-1 without recording affinity
        let context_none = routing_context_from_selector(None, Some(routing_key.as_slice()));
        let connection_none = db_client
            .resolve_routing_connection(&context_none)
            .expect("keyed request resolves to connection");
        assert_eq!(connection_none.address(), node_address);
        assert_eq!(
            router.affinity_count(),
            0,
            "Must not record transaction affinity when transaction is None"
        );

        // 2. Single-use transaction selector with routing key: routes to node-1 without recording affinity
        let selector_single = TransactionSelector::new().set_single_use(TransactionOptions::new());
        let context_single =
            routing_context_from_selector(Some(&selector_single), Some(routing_key.as_slice()));
        let connection_single = db_client
            .resolve_routing_connection(&context_single)
            .expect("single-use keyed request resolves to connection");
        assert_eq!(connection_single.address(), node_address);
        assert_eq!(
            router.affinity_count(),
            0,
            "Must not record transaction affinity for single_use transaction"
        );
    }

    #[tokio_test_no_panics]
    async fn channel_pool_round_robin_for_all_rpcs_when_location_routing_disabled() {
        use std::sync::Mutex;

        let captured_requests = Arc::new(Mutex::new(Vec::new()));
        let mut mock = create_test_mock();

        // 1. Map unary methods to default mock responses
        macro_rules! mock_unary_response {
            (begin_transaction) => {
                spanner_grpc_mock::google::spanner::v1::Transaction::default()
            };
            (commit) => {
                spanner_grpc_mock::google::spanner::v1::CommitResponse::default()
            };
            (execute_batch_dml) => {
                spanner_grpc_mock::google::spanner::v1::ExecuteBatchDmlResponse::default()
            };
            (execute_sql) => {
                spanner_grpc_mock::google::spanner::v1::ResultSet::default()
            };
            (rollback) => {
                ()
            };
            (partition_query) => {
                spanner_grpc_mock::google::spanner::v1::PartitionResponse::default()
            };
            (partition_read) => {
                spanner_grpc_mock::google::spanner::v1::PartitionResponse::default()
            };
        }

        // Set up mock expectations for all unary RPCs via macro
        macro_rules! setup_unary_mock {
            ($method:ident, $expect_method:ident, $request_type:ident, $response_type:ty $(, $extra:expr)*) => {
                let captured_clone = Arc::clone(&captured_requests);
                mock.$expect_method().returning(move |req| {
                    if let Some(id) = req.metadata().get("x-goog-spanner-request-id") {
                        captured_clone
                            .lock()
                            .expect("lock should succeed")
                            .push((stringify!($method), id.to_str().expect("ascii").to_string()));
                    }
                    Ok(gaxi::grpc::tonic::Response::new(mock_unary_response!(
                        $method
                    )))
                });
            };
        }
        for_all_unary_db_rpcs!(setup_unary_mock);

        // Set up mock expectations for all streaming RPCs via macro
        macro_rules! setup_streaming_mock {
            ($method:ident, $expect_method:ident, $request_type:ident, $builder_type:ident $(, $extract_key:expr)?) => {
                let captured_clone = Arc::clone(&captured_requests);
                mock.$expect_method().returning(move |req| {
                    if let Some(id) = req.metadata().get("x-goog-spanner-request-id") {
                        captured_clone
                            .lock()
                            .expect("lock should succeed")
                            .push((stringify!($method), id.to_str().expect("ascii").to_string()));
                    }
                    Ok(gaxi::grpc::tonic::Response::from(adapt([])))
                });
            };
        }
        for_all_streaming_db_rpcs!(setup_streaming_mock);

        let (address, _server) = start("0.0.0.0:0", mock)
            .await
            .expect("Failed to start mock server");
        let spanner = Spanner::builder()
            .with_endpoint(address)
            .with_instance_type(InstanceType::Cloud)
            .with_credentials(Anonymous::new().build())
            .build()
            .await
            .expect("Failed to build client");

        let db_client = spanner
            .database_client("projects/p/instances/i/databases/d")
            .build()
            .await
            .expect("build should succeed");

        assert!(!db_client.is_location_aware_routing_enabled());

        // Verify round-robin channel distribution 1..=4 for all mapped RPCs across 4 hints
        for channel_hint in 0..4 {
            let expected_channel_id = format!(".{}.", channel_hint + 1);

            macro_rules! call_unary_rpc {
                ($method:ident, $expect_method:ident, $request_type:ident, $response_type:ty $(, $extra:expr)*) => {
                    let _ = db_client
                        .$method(
                            $request_type::default(),
                            RequestOptions::default(),
                            channel_hint,
                        )
                        .await;
                };
            }
            for_all_unary_db_rpcs!(call_unary_rpc);

            macro_rules! call_streaming_rpc {
                ($method:ident, $expect_method:ident, $request_type:ident, $builder_type:ident $(, $extract_key:expr)?) => {
                    let _ = db_client
                        .$method(
                            $request_type::default(),
                            RequestOptions::default(),
                            channel_hint,
                        )
                        .send()
                        .await;
                };
            }
            for_all_streaming_db_rpcs!(call_streaming_rpc);

            let calls = captured_requests.lock().expect("lock").clone();
            captured_requests.lock().expect("lock").clear();
            assert_eq!(
                calls.len(),
                10,
                "each RPC method must be called once per hint"
            );
            for (rpc_name, request_id) in calls {
                assert!(
                    request_id.contains(&expected_channel_id),
                    "RPC {rpc_name} with channel_hint {channel_hint} must use channel ID {expected_channel_id}, got {request_id}"
                );
            }
        }
    }

    #[tokio_test_no_panics]
    async fn streaming_rpcs_round_robin_when_location_routing_enabled_without_routing_key() {
        use std::sync::Mutex;

        let captured_requests = Arc::new(Mutex::new(Vec::new()));
        let mut mock = create_test_mock();

        let captured = Arc::clone(&captured_requests);
        mock.expect_execute_streaming_sql().returning(move |req| {
            if let Some(id) = req.metadata().get("x-goog-spanner-request-id") {
                captured.lock().expect("lock").push((
                    "execute_streaming_sql",
                    id.to_str().expect("ascii").to_string(),
                ));
            }
            Ok(gaxi::grpc::tonic::Response::from(adapt([])))
        });

        let captured = Arc::clone(&captured_requests);
        mock.expect_streaming_read().returning(move |req| {
            if let Some(id) = req.metadata().get("x-goog-spanner-request-id") {
                captured
                    .lock()
                    .expect("lock")
                    .push(("streaming_read", id.to_str().expect("ascii").to_string()));
            }
            Ok(gaxi::grpc::tonic::Response::from(adapt([])))
        });

        let (address, _server) = start("0.0.0.0:0", mock)
            .await
            .expect("Failed to start mock server");
        let spanner = Spanner::builder()
            .with_endpoint(address)
            .with_instance_type(InstanceType::Omni)
            .with_credentials(Anonymous::new().build())
            .build()
            .await
            .expect("Failed to build client");

        let db_client = spanner
            .database_client("projects/p/instances/i/databases/d")
            .with_location_aware_routing(true)
            .build()
            .await
            .expect("build should succeed");

        assert!(db_client.is_location_aware_routing_enabled());

        for channel_hint in 0..4 {
            let expected_channel_id = format!(".{}.", channel_hint + 1);

            // execute_streaming_sql (no routing key)
            let _ = db_client
                .execute_streaming_sql(
                    ExecuteSqlRequest::default(),
                    RequestOptions::default(),
                    channel_hint,
                )
                .send()
                .await;

            // streaming_read with KeySet::all() (no routing key)
            let mut key_set = KeySet::new();
            key_set.all = true;
            let read_request = ReadRequest::new().set_table("Users").set_key_set(key_set);
            let _ = db_client
                .streaming_read(read_request, RequestOptions::default(), channel_hint)
                .send()
                .await;

            let calls = captured_requests.lock().expect("lock").clone();
            captured_requests.lock().expect("lock").clear();
            assert_eq!(calls.len(), 2);
            for (rpc_name, request_id) in calls {
                assert!(
                    request_id.contains(&expected_channel_id),
                    "Even when location routing is enabled, {rpc_name} without routing key must round-robin onto channel {expected_channel_id}, got {request_id}"
                );
            }
        }
    }

    #[tokio_test_no_panics]
    async fn execute_streaming_sql_attaches_operation_uid_on_cold_start_and_routes_to_tablet_on_cache_hit()
     {
        use std::sync::Mutex;

        let captured_gateway_requests = Arc::new(Mutex::new(Vec::new()));
        let mut mock_gateway = create_test_mock();

        let captured_gateway = Arc::clone(&captured_gateway_requests);
        mock_gateway
            .expect_execute_streaming_sql()
            .returning(move |request| {
                captured_gateway
                    .lock()
                    .expect("lock captured gateway requests")
                    .push(request.into_inner());
                Ok(Response::from(adapt([])))
            });

        let (gateway_address, _gateway_server) = start("127.0.0.1:0", mock_gateway)
            .await
            .expect("start mock gateway");

        let captured_tablet_requests = Arc::new(Mutex::new(Vec::new()));
        let mut mock_tablet = create_test_mock();
        let captured_tablet = Arc::clone(&captured_tablet_requests);
        mock_tablet
            .expect_execute_streaming_sql()
            .returning(move |request| {
                captured_tablet
                    .lock()
                    .expect("lock captured tablet requests")
                    .push(request.into_inner());
                Ok(Response::from(adapt([])))
            });

        let (tablet_address, _tablet_server) = start("127.0.0.1:0", mock_tablet)
            .await
            .expect("start mock tablet");

        let spanner = Spanner::builder()
            .with_endpoint(gateway_address)
            .with_instance_type(InstanceType::Omni)
            .with_credentials(Anonymous::new().build())
            .build()
            .await
            .expect("build spanner client");

        let database_client = spanner
            .database_client("projects/p/instances/i/databases/d")
            .with_location_aware_routing(true)
            .build()
            .await
            .expect("build database client");

        let statement = Statement::builder("SELECT * FROM Accounts WHERE account_id = @id")
            .add_param("id", 12345i64)
            .build();
        let request = statement.clone().into_request();

        // 1. Cold start: routes to gateway, attaches discovery routing hint with operation_uid
        let _ = database_client
            .execute_streaming_sql(request.clone(), RequestOptions::default(), 0)
            .send()
            .await;

        let gateway_requests = captured_gateway_requests
            .lock()
            .expect("lock gateway requests")
            .clone();
        assert_eq!(
            gateway_requests.len(),
            1,
            "cold-start query must be dispatched to the gateway"
        );
        let cold_request = &gateway_requests[0];
        let cold_hint = cold_request
            .routing_hint
            .as_ref()
            .expect("cold-start query must attach bootstrap routing hint");
        assert_ne!(
            cold_hint.operation_uid, 0,
            "bootstrap hint must contain assigned operation UID"
        );
        let allocated_operation_uid = cold_hint.operation_uid;

        // 2. Pre-warm tablet connection in connection cache
        let router = database_client
            .location_router()
            .expect("location router present");
        let client_config = database_client
            .cache_updater()
            .expect("cache updater present")
            .client_config();
        let _ = router
            .connection_cache()
            .get(&tablet_address, client_config)
            .await
            .expect("pre-warm tablet connection");

        // 3. Ingest CacheUpdate with recipe for allocated_operation_uid and range mapping to tablet
        let cache_update = CacheUpdate::new()
            .set_database_id(8888u64)
            .set_key_recipes(
                RecipeList::new()
                    .set_schema_generation(Bytes::from_static(b"v1"))
                    .set_recipe(vec![
                        KeyRecipe::new()
                            .set_operation_uid(allocated_operation_uid)
                            .set_part(vec![
                                Part::new().set_tag(10u32),
                                Part::new()
                                    .set_identifier("id")
                                    .set_order(Order::Ascending)
                                    .set_null_order(NullOrder::NullsFirst)
                                    .set_type(Type::default().set_code(TypeCode::Int64)),
                            ]),
                    ]),
            )
            .set_group(vec![Group::new().set_group_uid(7001u64).set_tablets(vec![
                        Tablet::new()
                            .set_tablet_uid(7001u64)
                            .set_server_address(tablet_address)
                            .set_role(Role::ReadOnly)
                            .set_distance(0u32),
                    ])])
            .set_range(vec![
                Range::new()
                    .set_group_uid(7001u64)
                    .set_start_key(vec![0x01])
                    .set_limit_key(vec![0xff, 0xff]),
            ]);

        database_client.observe_cache_update(Some(cache_update));

        // 4. Cache hit: routes directly to tablet mock with full routing hint
        let _ = database_client
            .execute_streaming_sql(request, RequestOptions::default(), 0)
            .send()
            .await;

        let tablet_requests = captured_tablet_requests
            .lock()
            .expect("lock tablet requests")
            .clone();
        assert_eq!(
            tablet_requests.len(),
            1,
            "keyed query after recipe and range caching must route directly to tablet"
        );
        let tablet_request = &tablet_requests[0];
        let tablet_hint = tablet_request
            .routing_hint
            .as_ref()
            .expect("tablet-routed query must attach routing hint");
        assert_eq!(
            tablet_hint.operation_uid, allocated_operation_uid,
            "tablet hint operation UID must match assigned UID"
        );
        assert_eq!(
            tablet_hint.tablet_uid, 7001,
            "tablet hint tablet UID must match target tablet"
        );
        assert_eq!(
            tablet_hint.database_id, 8888,
            "tablet hint database ID must match updated database ID"
        );
        assert!(
            !tablet_hint.key.is_empty(),
            "tablet hint must contain encoded routing key"
        );
    }

    #[tokio_test_no_panics]
    async fn streaming_read_routes_to_gateway_on_cold_start_and_tablet_on_cache_hit() {
        use std::sync::Mutex;

        let captured_gateway_requests = Arc::new(Mutex::new(Vec::new()));
        let mut mock_gateway = create_test_mock();

        let captured_gateway = Arc::clone(&captured_gateway_requests);
        mock_gateway
            .expect_streaming_read()
            .returning(move |request| {
                captured_gateway
                    .lock()
                    .expect("lock captured gateway requests")
                    .push(request.into_inner());
                Ok(Response::from(adapt([])))
            });

        let (gateway_address, _gateway_server) = start("127.0.0.1:0", mock_gateway)
            .await
            .expect("start mock gateway");

        let captured_tablet_requests = Arc::new(Mutex::new(Vec::new()));
        let mut mock_tablet = create_test_mock();
        let captured_tablet = Arc::clone(&captured_tablet_requests);
        mock_tablet
            .expect_streaming_read()
            .returning(move |request| {
                captured_tablet
                    .lock()
                    .expect("lock captured tablet requests")
                    .push(request.into_inner());
                Ok(Response::from(adapt([])))
            });

        let (tablet_address, _tablet_server) = start("127.0.0.1:0", mock_tablet)
            .await
            .expect("start mock tablet");

        let spanner = Spanner::builder()
            .with_endpoint(gateway_address)
            .with_instance_type(InstanceType::Omni)
            .with_credentials(Anonymous::new().build())
            .build()
            .await
            .expect("build spanner client");

        let database_client = spanner
            .database_client("projects/p/instances/i/databases/d")
            .with_location_aware_routing(true)
            .build()
            .await
            .expect("build database client");

        let mut key_set = KeySet::new();
        key_set
            .keys
            .push(vec![serde_json::Value::String("user123".to_string())]);
        let read_request = ReadRequest::new()
            .set_table("Users")
            .set_columns(vec!["name".to_string()])
            .set_key_set(key_set);

        // 1. Cold start: without cached recipe or range, routes to gateway and attaches bootstrap routing hint
        let _ = database_client
            .streaming_read(read_request.clone(), RequestOptions::default(), 0)
            .send()
            .await;

        let gateway_requests = captured_gateway_requests
            .lock()
            .expect("lock gateway requests")
            .clone();
        assert_eq!(
            gateway_requests.len(),
            1,
            "cold-start table read must be dispatched to the gateway"
        );
        let cold_request = &gateway_requests[0];
        let cold_hint = cold_request
            .routing_hint
            .as_ref()
            .expect("cold-start read must attach RoutingHint with assigned operation_uid");
        assert_ne!(
            cold_hint.operation_uid, UNASSIGNED_OPERATION_UID,
            "cold-start read must assign dynamic operation UID"
        );

        // 2. Pre-warm tablet connection in connection cache
        let router = database_client
            .location_router()
            .expect("location router present");
        let client_config = database_client
            .cache_updater()
            .expect("cache updater present")
            .client_config();
        let _ = router
            .connection_cache()
            .get(&tablet_address, client_config)
            .await
            .expect("pre-warm tablet connection");

        // 3. Ingest CacheUpdate with table recipe and covering range pointing to tablet
        let cache_update = CacheUpdate::new()
            .set_database_id(9999u64)
            .set_key_recipes(
                RecipeList::new()
                    .set_schema_generation(Bytes::from_static(b"v1"))
                    .set_recipe(vec![KeyRecipe::new().set_table_name("Users").set_part(
                        vec![
                                Part::new().set_tag(50020u32),
                                Part::new()
                                    .set_tag(1u32)
                                    .set_identifier("id")
                                    .set_type(Type::new().set_code(TypeCode::String))
                                    .set_order(Order::Ascending)
                                    .set_null_order(NullOrder::NotNull),
                            ],
                    )]),
            )
            .set_group(vec![Group::new().set_group_uid(8001u64).set_tablets(vec![
                Tablet::new()
                    .set_tablet_uid(8001u64)
                    .set_server_address(tablet_address)
                    .set_role(Role::ReadOnly)
                    .set_distance(0u32),
            ])])
            .set_range(vec![
                Range::new()
                    .set_group_uid(8001u64)
                    .set_start_key(vec![0x00])
                    .set_limit_key(vec![0xff]),
            ]);

        database_client.observe_cache_update(Some(cache_update));

        // 4. Cache hit: routes directly to tablet mock with full routing hint
        let _ = database_client
            .streaming_read(read_request, RequestOptions::default(), 0)
            .send()
            .await;

        let tablet_requests = captured_tablet_requests
            .lock()
            .expect("lock tablet requests")
            .clone();
        assert_eq!(
            tablet_requests.len(),
            1,
            "keyed table read after recipe and range caching must route directly to tablet"
        );
        let tablet_request = &tablet_requests[0];
        let tablet_hint = tablet_request
            .routing_hint
            .as_ref()
            .expect("tablet-routed read must attach routing hint");
        assert_eq!(
            tablet_hint.operation_uid, cold_hint.operation_uid,
            "tablet hint operation UID must match assigned UID"
        );
        assert_eq!(
            tablet_hint.tablet_uid, 8001,
            "tablet hint tablet UID must match target tablet"
        );
        assert_eq!(
            tablet_hint.database_id, 9999,
            "tablet hint database ID must match updated database ID"
        );
        assert!(
            !tablet_hint.key.is_empty(),
            "tablet hint must contain encoded routing key"
        );
    }

    #[tokio_test_no_panics]
    async fn database_client_latency_recording_and_error_tracking() {
        let mock = create_test_mock();

        let (address, _server) = start("0.0.0.0:0", mock)
            .await
            .expect("Failed to start mock server");
        let spanner = Spanner::builder()
            .with_endpoint(address)
            .with_credentials(Anonymous::new().build())
            .build()
            .await
            .expect("build spanner");

        let database_name = "projects/test-p/instances/test-i/databases/test-d";
        let database_client = spanner
            .database_client(database_name)
            .with_location_aware_routing(true)
            .build()
            .await
            .expect("build database_client");

        let latency_registry = database_client
            .latency_registry()
            .expect("latency registry must be present when location routing is enabled");

        // Record latency and error
        database_client.record_latency(100, "10.0.0.1:15000", Duration::from_millis(25));
        database_client.record_routing_error(100, "10.0.0.2:15000");

        // Verify the score is attributed under the database scope
        let cost_scoped =
            latency_registry.get_selection_cost(Some(database_name), 100, 0, "10.0.0.1:15000");
        assert!(
            cost_scoped > 0.0,
            "measured score must be greater than zero"
        );

        // Also verify location router holds the correct database scope
        let router = database_client
            .location_router()
            .expect("location router present");
        assert_eq!(router.database_scope(), database_name);
    }

    #[tokio_test_no_panics]
    async fn database_client_latency_recording_when_disabled_is_noop() {
        let mock = create_test_mock();

        let (address, _server) = start("0.0.0.0:0", mock)
            .await
            .expect("Failed to start mock server");
        let spanner = Spanner::builder()
            .with_endpoint(address)
            .with_credentials(Anonymous::new().build())
            .build()
            .await
            .expect("build spanner");

        let database_client = spanner
            .database_client("projects/p/instances/i/databases/d")
            .with_location_aware_routing(false)
            .build()
            .await
            .expect("build database_client");

        assert!(database_client.latency_registry().is_none());
        assert!(database_client.location_router().is_none());

        // Calling record_latency and record_routing_error when disabled must be a clean no-op
        database_client.record_latency(100, "10.0.0.1:15000", Duration::from_millis(50));
        database_client.record_routing_error(100, "10.0.0.1:15000");
    }

    #[tokio_test_no_panics]
    async fn cache_subscriber_lifecycle_when_location_routing_disabled() {
        let mock = create_test_mock();

        let (address, _server) = start("0.0.0.0:0", mock)
            .await
            .expect("Failed to start mock server");
        let spanner = Spanner::builder()
            .with_endpoint(address)
            .with_instance_type(InstanceType::Omni)
            .with_credentials(Anonymous::new().build())
            .build()
            .await
            .expect("Failed to build client");

        let database_client = spanner
            .database_client("projects/p/instances/i/databases/d")
            .build()
            .await
            .expect("build should succeed");

        assert!(
            database_client.cache_subscriber().is_none(),
            "cache subscriber should not be initialized when location-aware routing is disabled"
        );
        assert!(
            database_client.endpoint_lifecycle_manager().is_none(),
            "endpoint lifecycle manager should not be initialized when location-aware routing is disabled"
        );
    }

    #[tokio_test_no_panics]
    async fn cache_subscriber_lifecycle_when_location_routing_enabled() {
        let (attempt_sender, mut attempt_receiver) = mpsc::channel(4);
        let mut mock = MockSpanner::new();
        mock.expect_create_session().returning(|req| {
            let req = req.into_inner();
            let session = req.session.expect("session present in request");
            Ok(Response::new(mock_v1::Session {
                name: "projects/p/instances/i/databases/d/sessions/s1".to_string(),
                multiplexed: session.multiplexed,
                ..Default::default()
            }))
        });

        let update = mock_v1::CacheUpdate {
            database_id: 12345,
            group: vec![mock_v1::Group {
                group_uid: 99,
                leader_index: 0,
                tablets: vec![mock_v1::Tablet {
                    server_address: "node-99.spanner.internal:15000".to_string(),
                    location: "us-central1".to_string(),
                    role: 1, // ReadWrite
                    distance: 0,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            range: vec![mock_v1::Range {
                group_uid: 99,
                start_key: b"a".to_vec(),
                limit_key: b"z".to_vec(),
                generation: vec![1],
                ..Default::default()
            }],
            ..Default::default()
        };

        mock.expect_fetch_cache_update().returning(move |_| {
            let _ = attempt_sender.try_send(());
            let (stream_sender, stream_receiver) = mpsc::channel(4);
            let _ = stream_sender.try_send(Ok(update.clone()));
            Ok(Response::from(stream_receiver))
        });

        let (address, _server) = start("0.0.0.0:0", mock)
            .await
            .expect("Failed to start mock server");
        let spanner = Spanner::builder()
            .with_endpoint(address)
            .with_instance_type(InstanceType::Omni)
            .with_credentials(Anonymous::new().build())
            .build()
            .await
            .expect("Failed to build client");

        let database_client = spanner
            .database_client("projects/p/instances/i/databases/d")
            .with_location_aware_routing(true)
            .build()
            .await
            .expect("build should succeed");

        assert!(
            database_client.cache_subscriber().is_some(),
            "cache subscriber must be initialized when location-aware routing is enabled"
        );
        assert!(
            database_client.endpoint_lifecycle_manager().is_some(),
            "endpoint lifecycle manager must be initialized when location-aware routing is enabled"
        );
        assert!(
            database_client
                .endpoint_lifecycle_manager()
                .expect("lifecycle manager must exist")
                .is_maintenance_active(),
            "maintenance task must be active while client is alive"
        );
        let lifecycle_manager = database_client
            .location_routing
            .as_ref()
            .map(|routing| Arc::clone(&routing.endpoint_lifecycle_manager))
            .expect("lifecycle manager must exist");

        // Deterministically wait for the initial connection and subsequent reconnection attempt,
        // which guarantees that the first stream's CacheUpdate was completely ingested into KeyRangeCache.
        attempt_receiver
            .recv()
            .await
            .expect("initial subscriber stream request should arrive");
        attempt_receiver
            .recv()
            .await
            .expect("reconnection attempt should arrive after first stream finishes");

        let location_router = database_client
            .location_router()
            .expect("location router must be present");
        let found_range = location_router
            .key_range_cache()
            .find_range(b"m", &[], RangeMode::CoveringSplit)
            .expect("range should be populated by background cache subscriber");
        assert_eq!(
            found_range.group_uid, 99,
            "streamed range must map to group 99"
        );
        assert_eq!(
            database_client.database_id(),
            Some(12345),
            "streamed database id must be updated"
        );

        drop(database_client);
        assert!(
            !lifecycle_manager.is_maintenance_active(),
            "maintenance task must be stopped when client is dropped"
        );
    }

    #[tokio_test_no_panics]
    async fn streaming_read_attaches_routing_hint_when_location_routing_active() {
        use crate::model::key_recipe::Part;
        use crate::model::key_recipe::part::{NullOrder, Order};
        use bytes::Bytes;
        use std::sync::Mutex;

        let captured_hints = Arc::new(Mutex::new(Vec::new()));
        let mut mock = create_test_mock();

        let captured = Arc::clone(&captured_hints);
        mock.expect_streaming_read().returning(move |request| {
            let request = request.into_inner();
            captured.lock().expect("lock").push(request.routing_hint);
            Ok(Response::from(adapt([])))
        });

        let (address, _server) = start("0.0.0.0:0", mock)
            .await
            .expect("Failed to start mock server");
        let spanner = Spanner::builder()
            .with_endpoint(address)
            .with_instance_type(InstanceType::Omni)
            .with_credentials(Anonymous::new().build())
            .build()
            .await
            .expect("Failed to build client");

        let database_client = spanner
            .database_client("projects/p/instances/i/databases/d")
            .with_location_aware_routing(true)
            .build()
            .await
            .expect("build should succeed");
        // 1. Initial streaming read without cache update -> routing_hint is None (cold cache, db_id = 0)
        let mut key_set = KeySet::new();
        key_set
            .keys
            .push(vec![serde_json::Value::String("user123".to_string())]);
        let read_request = ReadRequest::new()
            .set_table("Users")
            .set_columns(vec!["name".to_string()])
            .set_key_set(key_set.clone());

        let _ = database_client
            .streaming_read(read_request.clone(), RequestOptions::default(), 0)
            .send()
            .await;

        let hints = captured_hints.lock().expect("lock").clone();
        captured_hints.lock().expect("lock").clear();
        assert_eq!(
            hints.len(),
            1,
            "exactly one initial request must be captured"
        );
        let cold_hint = hints[0]
            .as_ref()
            .expect("cold-start read must attach RoutingHint with operation_uid");
        assert_eq!(
            cold_hint.operation_uid, 1,
            "cold-start read must assign operation UID 1"
        );

        // 2. Ingest CacheUpdate with database_id, recipe list, and key range
        let recipe = KeyRecipe::new().set_table_name("Users").set_part(vec![
            Part::new().set_tag(50020u32),
            Part::new()
                .set_tag(1u32)
                .set_identifier("id")
                .set_type(Type::new().set_code(TypeCode::String))
                .set_order(Order::Ascending)
                .set_null_order(NullOrder::NotNull),
        ]);
        let recipe_list = RecipeList::new()
            .set_schema_generation(Bytes::from_static(b"schema-v1"))
            .set_recipe(vec![recipe]);

        let tablet = Tablet::default()
            .set_tablet_uid(301u64)
            .set_server_address("10.0.0.1:15000")
            .set_incarnation(Bytes::from_static(b"inc-1"));
        let group = Group::new()
            .set_group_uid(400u64)
            .set_leader_index(0)
            .set_tablets(vec![tablet]);
        let range = Range::new()
            .set_group_uid(400u64)
            .set_split_id(500u64)
            .set_start_key(vec![0x00])
            .set_limit_key(vec![0xff]);

        let cache_update = CacheUpdate::new()
            .set_database_id(999u64)
            .set_key_recipes(recipe_list)
            .set_group(vec![group])
            .set_range(vec![range]);

        database_client.observe_cache_update(Some(cache_update));

        // 3. Subsequent streaming read for table Users -> RoutingHint must be populated and attached
        let _ = database_client
            .streaming_read(read_request, RequestOptions::default(), 0)
            .send()
            .await;

        let hints = captured_hints.lock().expect("lock").clone();
        assert_eq!(
            hints.len(),
            1,
            "exactly one subsequent request must be captured"
        );
        let hint = hints[0].as_ref().expect("RoutingHint must be attached");

        assert_eq!(
            database_client.database_id(),
            Some(999),
            "database_id must match active database"
        );
        assert_eq!(
            hint.operation_uid, 1,
            "operation_uid matches prepared read UID"
        );
        assert_eq!(hint.database_id, 999, "database_id must match CacheUpdate");
        assert_eq!(
            &hint.schema_generation[..],
            b"schema-v1",
            "schema_generation must match RecipeList"
        );
        assert_eq!(hint.group_uid, 400, "group_uid must match CacheUpdate");
        assert_eq!(hint.split_id, 500, "split_id must match CacheUpdate");
        assert_eq!(
            hint.tablet_uid, 301,
            "tablet_uid must match selected tablet"
        );
        assert_eq!(&hint.key[..], &[0x00], "key must match range start_key");
        assert_eq!(
            &hint.limit_key[..],
            &[0xff],
            "limit_key must match range limit_key"
        );
    }

    #[test]
    fn routing_context_prefer_leader_classification() {
        use crate::model::transaction_options::{PartitionedDml, ReadOnly, ReadWrite};
        use crate::model::{TransactionOptions, TransactionSelector};
        use bytes::Bytes;

        // None selector defaults to prefer_leader = true
        let context_none = routing_context_from_selector(None, None);
        assert!(
            context_none.prefer_leader,
            "None selector must prefer leader"
        );
        assert!(
            context_none.transaction_id.is_none(),
            "transaction_id must be None"
        );
        assert!(
            !context_none.use_transaction_affinity,
            "use_transaction_affinity must be false without transaction_id"
        );

        // Transaction ID selector sets affinity and defaults to prefer_leader = true
        let id_selector = TransactionSelector::new().set_id(Bytes::from_static(b"tx-123"));
        let context_id = routing_context_from_selector(Some(&id_selector), None);
        assert!(
            context_id.prefer_leader,
            "Transaction ID selector must prefer leader"
        );
        assert_eq!(
            context_id.transaction_id,
            Some(b"tx-123".as_slice()),
            "transaction_id must match Bytes"
        );
        assert!(
            context_id.use_transaction_affinity,
            "use_transaction_affinity must be true when transaction_id is present"
        );

        // ReadWrite transaction prefers leader
        let rw_options = TransactionOptions::new().set_read_write(ReadWrite::default());
        let rw_selector = TransactionSelector::new().set_begin(rw_options);
        let context_rw = routing_context_from_selector(Some(&rw_selector), None);
        assert!(
            context_rw.prefer_leader,
            "ReadWrite transaction must prefer leader"
        );

        // PartitionedDML transaction prefers leader
        let pdml_options = TransactionOptions::new().set_partitioned_dml(PartitionedDml::default());
        let pdml_selector = TransactionSelector::new().set_begin(pdml_options);
        let context_pdml = routing_context_from_selector(Some(&pdml_selector), None);
        assert!(
            context_pdml.prefer_leader,
            "PartitionedDML transaction must prefer leader"
        );

        // Read-only with default options (no timestamp bound) defaults to strong -> prefers leader
        let ro_default = TransactionOptions::new().set_read_only(ReadOnly::default());
        let selector_ro_default = TransactionSelector::new().set_single_use(ro_default);
        let context_ro_default = routing_context_from_selector(Some(&selector_ro_default), None);
        assert!(
            context_ro_default.prefer_leader,
            "ReadOnly with default options must prefer leader"
        );

        // Read-only with Strong(true) timestamp bound -> prefer_leader = true
        let strong_true_ro =
            TransactionOptions::new().set_read_only(ReadOnly::new().set_strong(true));
        let selector_strong_true = TransactionSelector::new().set_single_use(strong_true_ro);
        let context_strong_true = routing_context_from_selector(Some(&selector_strong_true), None);
        assert!(
            context_strong_true.prefer_leader,
            "Strong(true) read must prefer leader"
        );

        // Read-only with Strong(false) timestamp bound -> prefer_leader = false
        let strong_false_ro =
            TransactionOptions::new().set_read_only(ReadOnly::new().set_strong(false));
        let selector_strong_false = TransactionSelector::new().set_single_use(strong_false_ro);
        let context_strong_false =
            routing_context_from_selector(Some(&selector_strong_false), None);
        assert!(
            !context_strong_false.prefer_leader,
            "Strong(false) read must route to follower"
        );

        // Read-only with ExactStaleness timestamp bound -> prefer_leader = false
        let exact_staleness_ro = TransactionOptions::new()
            .set_read_only(ReadOnly::new().set_exact_staleness(wkt::Duration::clamp(10, 0)));
        let selector_exact = TransactionSelector::new().set_begin(exact_staleness_ro);
        let context_exact = routing_context_from_selector(Some(&selector_exact), None);
        assert!(
            !context_exact.prefer_leader,
            "ExactStaleness read must route to follower"
        );

        // Read-only with MaxStaleness timestamp bound -> prefer_leader = false
        let max_staleness_ro = TransactionOptions::new()
            .set_read_only(ReadOnly::new().set_max_staleness(wkt::Duration::clamp(10, 0)));
        let selector_max = TransactionSelector::new().set_single_use(max_staleness_ro);
        let context_max = routing_context_from_selector(Some(&selector_max), None);
        assert!(
            !context_max.prefer_leader,
            "MaxStaleness read must route to follower"
        );

        // Read-only with ReadTimestamp timestamp bound -> prefer_leader = false
        let read_timestamp_ro = TransactionOptions::new()
            .set_read_only(ReadOnly::new().set_read_timestamp(wkt::Timestamp::clamp(100, 0)));
        let selector_read_ts = TransactionSelector::new().set_single_use(read_timestamp_ro);
        let context_read_ts = routing_context_from_selector(Some(&selector_read_ts), None);
        assert!(
            !context_read_ts.prefer_leader,
            "ReadTimestamp read must route to follower"
        );

        // Read-only with MinReadTimestamp timestamp bound -> prefer_leader = false
        let min_read_timestamp_ro = TransactionOptions::new()
            .set_read_only(ReadOnly::new().set_min_read_timestamp(wkt::Timestamp::clamp(100, 0)));
        let selector_min_ts = TransactionSelector::new().set_single_use(min_read_timestamp_ro);
        let context_min_ts = routing_context_from_selector(Some(&selector_min_ts), None);
        assert!(
            !context_min_ts.prefer_leader,
            "MinReadTimestamp read must route to follower"
        );
    }

    #[tokio_test_no_panics]
    async fn pre_route_begin_transaction_attaches_routing_hint_when_location_routing_active() {
        let mock = create_test_mock();

        let (address, _server) = start("0.0.0.0:0", mock)
            .await
            .expect("Failed to start mock server");
        let spanner = Spanner::builder()
            .with_endpoint(address)
            .with_instance_type(InstanceType::Omni)
            .with_credentials(Anonymous::new().build())
            .build()
            .await
            .expect("Failed to build client");

        let database_client = spanner
            .database_client("projects/p/instances/i/databases/d")
            .with_location_aware_routing(true)
            .build()
            .await
            .expect("build should succeed");

        let node_address = "node-1.spanner.internal:15000";
        let follower_address = "node-follower.spanner.internal:15000";
        let recipe = KeyRecipe::new().set_table_name("Users").set_part(vec![
            Part::new().set_tag(50020u32),
            Part::new()
                .set_tag(1u32)
                .set_identifier("id")
                .set_type(Type::new().set_code(TypeCode::String))
                .set_order(Order::Ascending)
                .set_null_order(NullOrder::NotNull),
        ]);
        let cache_update = CacheUpdate::new()
            .set_database_id(42u64)
            .set_key_recipes(
                RecipeList::new()
                    .set_schema_generation(Bytes::from_static(b"schema-v1"))
                    .set_recipe(vec![recipe.clone()]),
            )
            .set_group(vec![
                Group::new()
                    .set_group_uid(100u64)
                    .set_leader_index(0)
                    .set_tablets(vec![
                        Tablet::new()
                            .set_tablet_uid(1001u64)
                            .set_server_address(node_address)
                            .set_distance(1u32)
                            .set_role(Role::ReadWrite),
                        Tablet::new()
                            .set_tablet_uid(1002u64)
                            .set_server_address(follower_address)
                            .set_distance(0u32)
                            .set_role(Role::ReadOnly),
                    ]),
            ])
            .set_range(vec![
                Range::new()
                    .set_group_uid(100u64)
                    .set_split_id(500u64)
                    .set_start_key(b"".to_vec())
                    .set_limit_key(b"\xff".to_vec()),
            ]);
        database_client.observe_cache_update(Some(cache_update));

        let router = database_client
            .location_router()
            .expect("router must be present");
        let client_config = database_client
            .cache_updater()
            .expect("cache updater must be present")
            .client_config();
        let _ = router
            .connection_cache()
            .get(node_address, client_config)
            .await
            .expect("should initialize connection for leader");
        let _ = router
            .connection_cache()
            .get(follower_address, client_config)
            .await
            .expect("should initialize connection for follower");

        let user_mutation = Mutation::new_insert_builder("Users")
            .set("id")
            .to("user123")
            .build();

        // 1. Keyed begin transaction with read-write options: resolves leader connection and attaches RoutingHint
        let mut read_write_begin_request = BeginTransactionRequest::new()
            .set_options(TransactionOptions::new().set_read_write(ReadWrite::new()))
            .set_mutation_key(user_mutation.clone().build_proto());
        let (resolved_connection, is_read_write) =
            database_client.pre_route_begin_transaction(&mut read_write_begin_request);
        assert!(
            is_read_write,
            "read-write options must result in is_read_write = true"
        );
        let connection = resolved_connection.expect("connection must be resolved for keyed begin");
        assert_eq!(
            connection.address(),
            node_address,
            "connection must point to the leader tablet"
        );
        let routing_hint = read_write_begin_request
            .routing_hint
            .expect("routing_hint must be attached to begin transaction request");
        assert_eq!(
            routing_hint.database_id, 42,
            "routing_hint database_id must match cache"
        );
        assert_eq!(
            routing_hint.schema_generation.as_ref(),
            b"schema-v1",
            "routing_hint schema_generation must match cache"
        );
        assert_eq!(
            routing_hint.tablet_uid, 1001,
            "routing_hint tablet_uid must match leader tablet"
        );
        assert_eq!(
            routing_hint.group_uid, 100,
            "routing_hint group_uid must match group"
        );
        assert_eq!(
            routing_hint.split_id, 500,
            "routing_hint split_id must match split"
        );
        assert_eq!(
            routing_hint.key.as_ref(),
            b"",
            "routing_hint key must match range start"
        );
        assert_eq!(
            routing_hint.limit_key.as_ref(),
            b"\xff",
            "routing_hint limit_key must match range limit"
        );

        // 2a. Keyed begin transaction with strong read-only options: prefers leader and attaches RoutingHint
        let mut strong_read_only_begin_request = BeginTransactionRequest::new()
            .set_options(TransactionOptions::new().set_read_only(ReadOnly::new().set_strong(true)))
            .set_mutation_key(user_mutation.clone().build_proto());
        let (strong_connection, strong_is_read_write) =
            database_client.pre_route_begin_transaction(&mut strong_read_only_begin_request);
        assert!(
            !strong_is_read_write,
            "strong read-only options must result in is_read_write = false"
        );
        let connection =
            strong_connection.expect("strong read-only keyed begin must resolve connection");
        assert_eq!(
            connection.address(),
            node_address,
            "strong read-only keyed begin must route to leader tablet"
        );
        let strong_hint = strong_read_only_begin_request
            .routing_hint
            .expect("strong read-only keyed begin must attach routing hint");
        assert_eq!(
            strong_hint.tablet_uid, 1001,
            "strong read-only routing hint must target leader tablet"
        );

        // 2b. Keyed begin transaction with stale read-only options (prefer_leader = false): routes to follower replica
        let mut stale_read_only_begin_request =
            BeginTransactionRequest::new()
                .set_options(TransactionOptions::new().set_read_only(
                    ReadOnly::new().set_exact_staleness(wkt::Duration::clamp(10, 0)),
                ))
                .set_mutation_key(user_mutation.clone().build_proto());
        let (stale_connection, stale_is_read_write) =
            database_client.pre_route_begin_transaction(&mut stale_read_only_begin_request);
        assert!(
            !stale_is_read_write,
            "stale read-only options must result in is_read_write = false"
        );
        let connection =
            stale_connection.expect("stale read-only keyed begin must resolve connection");
        assert_eq!(
            connection.address(),
            follower_address,
            "stale read-only keyed begin must route to follower replica"
        );
        let stale_hint = stale_read_only_begin_request
            .routing_hint
            .expect("stale read-only keyed begin must attach routing hint");
        assert_eq!(
            stale_hint.tablet_uid, 1002,
            "stale read-only routing hint must target follower tablet"
        );

        // 3. Unkeyed begin transaction (mutation_key is None): returns None connection, leaves hint None,
        // and returns is_read_write = true so affinity is recorded to the default gateway connection.
        let mut unkeyed_begin_request = BeginTransactionRequest::new()
            .set_options(TransactionOptions::new().set_read_write(ReadWrite::new()));
        let (unkeyed_connection, unkeyed_is_read_write) =
            database_client.pre_route_begin_transaction(&mut unkeyed_begin_request);
        assert!(
            unkeyed_is_read_write,
            "unkeyed read-write begin must return is_read_write = true to preserve affinity"
        );
        assert!(
            unkeyed_connection.is_none(),
            "unkeyed begin must return None connection"
        );
        assert!(
            unkeyed_begin_request.routing_hint.is_none(),
            "unkeyed begin must not attach routing hint"
        );

        // 4. Keyed begin transaction with uncached table: returns None connection, leaves hint None,
        // and returns is_read_write = true so affinity is recorded to the default gateway connection.
        let unknown_mutation = Mutation::new_insert_builder("UnknownTable")
            .set("id")
            .to("unknown_key")
            .build();
        let mut unknown_begin_request = BeginTransactionRequest::new()
            .set_options(TransactionOptions::new().set_read_write(ReadWrite::new()))
            .set_mutation_key(unknown_mutation.build_proto());
        let (unknown_connection, unknown_is_read_write) =
            database_client.pre_route_begin_transaction(&mut unknown_begin_request);
        assert!(
            unknown_is_read_write,
            "uncached table read-write begin must return is_read_write = true to preserve affinity"
        );
        assert!(
            unknown_connection.is_none(),
            "uncached table begin must return None connection"
        );
        assert!(
            unknown_begin_request.routing_hint.is_none(),
            "uncached table begin must not attach routing hint"
        );

        // 5. Unkeyed read-only begin transaction: returns None connection, leaves hint None,
        // and returns is_read_write = false as read-only transactions do not use affinity.
        let mut unkeyed_read_only_begin_request = BeginTransactionRequest::new()
            .set_options(TransactionOptions::new().set_read_only(ReadOnly::new().set_strong(true)));
        let (unkeyed_read_only_connection, unkeyed_read_only_is_read_write) =
            database_client.pre_route_begin_transaction(&mut unkeyed_read_only_begin_request);
        assert!(
            !unkeyed_read_only_is_read_write,
            "unkeyed read-only begin must return is_read_write = false"
        );
        assert!(
            unkeyed_read_only_connection.is_none(),
            "unkeyed read-only begin must return None connection"
        );
        assert!(
            unkeyed_read_only_begin_request.routing_hint.is_none(),
            "unkeyed read-only begin must not attach routing hint"
        );

        // 6. Begin transaction with mutation_key but omitted options: defaults to prefer_leader = true and is_read_write = false
        let mut omitted_options_begin_request =
            BeginTransactionRequest::new().set_mutation_key(user_mutation.clone().build_proto());
        let (omitted_options_connection, omitted_options_is_read_write) =
            database_client.pre_route_begin_transaction(&mut omitted_options_begin_request);
        assert!(
            !omitted_options_is_read_write,
            "omitted options begin must return is_read_write = false"
        );
        let connection = omitted_options_connection
            .expect("leader connection must be resolved when options are omitted");
        assert_eq!(
            connection.address(),
            node_address,
            "omitted options begin must route to leader tablet"
        );
        let omitted_options_hint = omitted_options_begin_request
            .routing_hint
            .expect("routing_hint must be attached when options are omitted");
        assert_eq!(
            omitted_options_hint.tablet_uid, 1001,
            "routing_hint must match leader tablet"
        );

        // 7. Keyed begin transaction when database_id == 0: resolves connection but leaves routing_hint None
        let zero_database_id_client = spanner
            .database_client("projects/p/instances/i/databases/d")
            .with_location_aware_routing(true)
            .build()
            .await
            .expect("build should succeed");
        let zero_database_id_cache_update = CacheUpdate::new()
            .set_database_id(0u64)
            .set_key_recipes(
                RecipeList::new()
                    .set_schema_generation(Bytes::from_static(b"schema-v1"))
                    .set_recipe(vec![recipe]),
            )
            .set_group(vec![
                Group::new()
                    .set_group_uid(100u64)
                    .set_leader_index(0)
                    .set_tablets(vec![
                        Tablet::new()
                            .set_tablet_uid(1001u64)
                            .set_server_address(node_address)
                            .set_role(Role::ReadWrite),
                    ]),
            ])
            .set_range(vec![
                Range::new()
                    .set_group_uid(100u64)
                    .set_split_id(500u64)
                    .set_start_key(b"".to_vec())
                    .set_limit_key(b"\xff".to_vec()),
            ]);
        zero_database_id_client.observe_cache_update(Some(zero_database_id_cache_update));
        let router_zero = zero_database_id_client
            .location_router()
            .expect("router must be present");
        let client_config_zero = zero_database_id_client
            .cache_updater()
            .expect("cache updater must be present")
            .client_config();
        let _ = router_zero
            .connection_cache()
            .get(node_address, client_config_zero)
            .await
            .expect("should initialize connection");
        let mut zero_database_id_begin_request = BeginTransactionRequest::new()
            .set_options(TransactionOptions::new().set_read_write(ReadWrite::new()))
            .set_mutation_key(user_mutation.clone().build_proto());
        let (zero_database_id_connection, zero_database_id_is_read_write) = zero_database_id_client
            .pre_route_begin_transaction(&mut zero_database_id_begin_request);
        assert!(
            zero_database_id_is_read_write,
            "read-write options must result in is_read_write = true even when database_id is 0"
        );
        let connection = zero_database_id_connection
            .expect("connection must still be resolved to leader tablet when database_id is 0");
        assert_eq!(
            connection.address(),
            node_address,
            "zero database_id begin must route to leader tablet"
        );
        assert!(
            zero_database_id_begin_request.routing_hint.is_none(),
            "routing_hint must NOT be attached when database_id is 0"
        );

        // 8. Begin transaction with location-aware routing disabled: returns None connection and leaves hint None
        let disabled_client = spanner
            .database_client("projects/p/instances/i/databases/d")
            .with_location_aware_routing(false)
            .build()
            .await
            .expect("build should succeed");
        let mut disabled_begin_request = BeginTransactionRequest::new()
            .set_options(TransactionOptions::new().set_read_write(ReadWrite::new()))
            .set_mutation_key(user_mutation.build_proto());
        let (disabled_connection, disabled_is_read_write) =
            disabled_client.pre_route_begin_transaction(&mut disabled_begin_request);
        assert!(
            !disabled_is_read_write,
            "disabled location routing must return is_read_write = false"
        );
        assert!(
            disabled_connection.is_none(),
            "disabled location routing must return None connection"
        );
        assert!(
            disabled_begin_request.routing_hint.is_none(),
            "disabled location routing must not attach routing hint"
        );
    }

    #[tokio_test_no_panics]
    async fn pre_route_commit_attaches_routing_hint_when_location_routing_active() {
        let mock = create_test_mock();

        let (address, _server) = start("0.0.0.0:0", mock)
            .await
            .expect("Failed to start mock server");
        let spanner = Spanner::builder()
            .with_endpoint(address)
            .with_instance_type(InstanceType::Omni)
            .with_credentials(Anonymous::new().build())
            .build()
            .await
            .expect("Failed to build client");

        let database_client = spanner
            .database_client("projects/p/instances/i/databases/d")
            .with_location_aware_routing(true)
            .build()
            .await
            .expect("build should succeed");

        let node_address = "node-1.spanner.internal:15000";
        let node_address_2 = "node-2.spanner.internal:15000";
        let recipe = KeyRecipe::new().set_table_name("Users").set_part(vec![
            Part::new().set_tag(5u32),
            Part::new()
                .set_tag(1u32)
                .set_identifier("id")
                .set_type(Type::new().set_code(TypeCode::String))
                .set_order(Order::Ascending)
                .set_null_order(NullOrder::NotNull),
        ]);
        let recipe_accounts = KeyRecipe::new().set_table_name("Accounts").set_part(vec![
            Part::new().set_tag(10u32),
            Part::new()
                .set_tag(1u32)
                .set_identifier("account_id")
                .set_type(Type::new().set_code(TypeCode::String))
                .set_order(Order::Ascending)
                .set_null_order(NullOrder::NotNull),
        ]);
        let cache_update = CacheUpdate::new()
            .set_database_id(42u64)
            .set_key_recipes(
                RecipeList::new()
                    .set_schema_generation(Bytes::from_static(b"schema-v1"))
                    .set_recipe(vec![recipe.clone(), recipe_accounts]),
            )
            .set_group(vec![
                Group::new()
                    .set_group_uid(100u64)
                    .set_leader_index(0)
                    .set_tablets(vec![
                        Tablet::new()
                            .set_tablet_uid(1001u64)
                            .set_server_address(node_address)
                            .set_role(Role::ReadWrite),
                    ]),
                Group::new()
                    .set_group_uid(200u64)
                    .set_leader_index(0)
                    .set_tablets(vec![
                        Tablet::new()
                            .set_tablet_uid(2001u64)
                            .set_server_address(node_address_2)
                            .set_role(Role::ReadWrite),
                    ]),
            ])
            .set_range(vec![
                Range::new()
                    .set_group_uid(100u64)
                    .set_split_id(500u64)
                    .set_start_key(vec![0x00])
                    .set_limit_key(vec![0x10]),
                Range::new()
                    .set_group_uid(200u64)
                    .set_split_id(600u64)
                    .set_start_key(vec![0x10])
                    .set_limit_key(vec![0x30]),
            ]);
        database_client.observe_cache_update(Some(cache_update));

        let router = database_client
            .location_router()
            .expect("router must be present");
        let client_config = database_client
            .cache_updater()
            .expect("cache updater must be present")
            .client_config();
        let _ = router
            .connection_cache()
            .get(node_address, client_config)
            .await
            .expect("should initialize connection for node 1");
        let _ = router
            .connection_cache()
            .get(node_address_2, client_config)
            .await
            .expect("should initialize connection for node 2");

        let user_mutation = Mutation::new_insert_builder("Users")
            .set("id")
            .to("user123")
            .build();

        // 1. Single-use commit with mutations: resolves leader connection and attaches RoutingHint
        let mut single_use_commit_request = CommitRequest::new()
            .set_single_use_transaction(TransactionOptions::new().set_read_write(ReadWrite::new()))
            .set_mutations(vec![user_mutation.clone().build_proto()]);
        let (single_use_connection, single_use_transaction_id) =
            database_client.pre_route_commit(&mut single_use_commit_request);
        assert!(
            single_use_transaction_id.is_none(),
            "single-use commit must have no transaction ID"
        );
        let connection =
            single_use_connection.expect("connection must be resolved for single-use commit");
        assert_eq!(
            connection.address(),
            node_address,
            "single-use commit connection must point to the leader tablet"
        );
        let routing_hint = single_use_commit_request
            .routing_hint
            .expect("routing_hint must be attached to single-use commit request");
        assert_eq!(
            routing_hint.database_id, 42,
            "routing_hint database_id must match cache"
        );
        assert_eq!(
            routing_hint.schema_generation.as_ref(),
            b"schema-v1",
            "routing_hint schema_generation must match cache"
        );
        assert_eq!(
            routing_hint.tablet_uid, 1001,
            "routing_hint tablet_uid must match leader tablet"
        );

        // 2. Read-write commit with transaction affinity AND mutations:
        //    Routes to affinity connection AND attaches RoutingHint derived from mutations.
        let active_transaction_id = Bytes::from_static(b"rw-tx-42");
        router.record_transaction_affinity(&active_transaction_id, node_address);

        let mut read_write_commit_request = CommitRequest::new()
            .set_transaction_id(active_transaction_id.clone())
            .set_mutations(vec![user_mutation.clone().build_proto()]);
        let (read_write_connection, read_write_transaction_id) =
            database_client.pre_route_commit(&mut read_write_commit_request);
        assert_eq!(
            read_write_transaction_id,
            Some(active_transaction_id),
            "read-write commit must return transaction ID"
        );
        let connection = read_write_connection
            .expect("affinity connection must be resolved for read-write commit");
        assert_eq!(
            connection.address(),
            node_address,
            "read-write commit must route to affinity address"
        );
        let read_write_routing_hint = read_write_commit_request
            .routing_hint
            .expect("routing_hint must be attached to commit request with mutations");
        assert_eq!(
            read_write_routing_hint.database_id, 42,
            "routing_hint database_id must match cache"
        );
        assert_eq!(
            read_write_routing_hint.tablet_uid, 1001,
            "routing_hint tablet_uid must match leader tablet"
        );

        // 3. Read-write commit with transaction affinity and NO mutations (DML-only transaction):
        //    Routes to affinity connection and does NOT attach RoutingHint.
        let dml_transaction_id = Bytes::from_static(b"dml-tx-99");
        router.record_transaction_affinity(&dml_transaction_id, node_address);

        let mut dml_commit_request =
            CommitRequest::new().set_transaction_id(dml_transaction_id.clone());
        let (dml_connection, dml_transaction_id_result) =
            database_client.pre_route_commit(&mut dml_commit_request);
        assert_eq!(
            dml_transaction_id_result,
            Some(dml_transaction_id),
            "dml-only commit must return transaction ID"
        );
        let connection =
            dml_connection.expect("affinity connection must be resolved for dml commit");
        assert_eq!(
            connection.address(),
            node_address,
            "dml commit must route to affinity address"
        );
        assert!(
            dml_commit_request.routing_hint.is_none(),
            "dml-only commit without mutations must not attach routing hint"
        );

        // 4. Multi-mutation commit (mix of Insert and Update):
        //    select_mutation_key prefers non-insert Update, routing to Accounts leader on node 2.
        let account_update = Mutation::new_update_builder("Accounts")
            .set("account_id")
            .to("acc123")
            .build();
        let mut mixed_commit_request = CommitRequest::new().set_mutations(vec![
            user_mutation.clone().build_proto(),
            account_update.clone().build_proto(),
        ]);
        let (mixed_connection, mixed_transaction_id) =
            database_client.pre_route_commit(&mut mixed_commit_request);
        assert!(
            mixed_transaction_id.is_none(),
            "single-use mixed commit must have no transaction ID"
        );
        let connection = mixed_connection.expect("connection must be resolved for mixed commit");
        assert_eq!(
            connection.address(),
            node_address_2,
            "mixed commit must prefer non-insert Update and route to Accounts leader"
        );
        let mixed_hint = mixed_commit_request
            .routing_hint
            .expect("routing_hint must be attached to mixed commit request");
        assert_eq!(
            mixed_hint.tablet_uid, 2001,
            "routing_hint must be derived from preferred Update mutation"
        );

        // 5. Multi-mutation commit (only Inserts with different row counts):
        //    select_mutation_key prefers largest insert (Accounts with 2 rows over Users with 1 row).
        let mut multi_row_insert = Mutation::new_insert_builder("Accounts")
            .set("account_id")
            .to("acc456")
            .build()
            .build_proto();
        if let Some(Operation::Insert(ref mut write)) = multi_row_insert.operation {
            let first_row = write.values[0].clone();
            write.values.push(first_row);
        }
        let mut multi_insert_commit_request = CommitRequest::new()
            .set_mutations(vec![user_mutation.clone().build_proto(), multi_row_insert]);
        let (multi_insert_connection, multi_insert_transaction_id) =
            database_client.pre_route_commit(&mut multi_insert_commit_request);
        assert!(
            multi_insert_transaction_id.is_none(),
            "multi-insert commit must have no transaction ID"
        );
        let connection =
            multi_insert_connection.expect("connection must be resolved for multi-insert commit");
        assert_eq!(
            connection.address(),
            node_address_2,
            "multi-insert commit must prefer largest insert and route to Accounts leader"
        );
        let multi_insert_hint = multi_insert_commit_request
            .routing_hint
            .expect("routing_hint must be attached to multi-insert commit request");
        assert_eq!(
            multi_insert_hint.tablet_uid, 2001,
            "routing_hint must be derived from largest insert mutation"
        );

        // 6. Single-use commit with uncached table: returns None connection and leaves hint None
        let unknown_mutation = Mutation::new_insert_builder("UnknownTable")
            .set("id")
            .to("unknown_key")
            .build();
        let mut unknown_commit_request =
            CommitRequest::new().set_mutations(vec![unknown_mutation.build_proto()]);
        let (unknown_connection, unknown_transaction_id) =
            database_client.pre_route_commit(&mut unknown_commit_request);
        assert!(
            unknown_connection.is_none(),
            "uncached table commit must return None connection"
        );
        assert!(
            unknown_transaction_id.is_none(),
            "uncached table commit must have no transaction ID"
        );
        assert!(
            unknown_commit_request.routing_hint.is_none(),
            "uncached table commit must not attach routing hint"
        );

        // 7. Empty commit (no mutations, no transaction_id): returns None connection and leaves hint None
        let mut empty_commit_request = CommitRequest::new();
        let (empty_connection, empty_transaction_id) =
            database_client.pre_route_commit(&mut empty_commit_request);
        assert!(
            empty_connection.is_none(),
            "empty commit must return None connection"
        );
        assert!(
            empty_transaction_id.is_none(),
            "empty commit must have no transaction ID"
        );
        assert!(
            empty_commit_request.routing_hint.is_none(),
            "empty commit must not attach routing hint"
        );

        // 8. Commit affinity precedence: transaction affinity overrides mutation target connection,
        //    while routing_hint is derived from the mutation key.
        let affinity_precedence_transaction_id = Bytes::from_static(b"affinity-precedence-tx");
        router.record_transaction_affinity(&affinity_precedence_transaction_id, node_address);
        let mut precedence_commit_request = CommitRequest::new()
            .set_transaction_id(affinity_precedence_transaction_id.clone())
            .set_mutations(vec![account_update.clone().build_proto()]);
        let (precedence_connection, precedence_transaction_id) =
            database_client.pre_route_commit(&mut precedence_commit_request);
        assert_eq!(
            precedence_transaction_id,
            Some(affinity_precedence_transaction_id),
            "commit must return transaction ID"
        );
        let connection = precedence_connection.expect("affinity connection must be resolved");
        assert_eq!(
            connection.address(),
            node_address,
            "commit connection must route to affinity address (node 1), overriding mutation target (node 2)"
        );
        let precedence_hint = precedence_commit_request
            .routing_hint
            .expect("routing_hint must be attached based on mutation");
        assert_eq!(
            precedence_hint.tablet_uid, 2001,
            "routing_hint must still target mutation leader tablet (Accounts on node 2)"
        );

        // 9. Commit with transaction_id but without affinity: routes to mutation leader and attaches routing hint.
        let unbound_transaction_id = Bytes::from_static(b"unbound-tx-with-mutations");
        let mut unbound_commit_request = CommitRequest::new()
            .set_transaction_id(unbound_transaction_id.clone())
            .set_mutations(vec![account_update.build_proto()]);
        let (unbound_connection, unbound_transaction_id_result) =
            database_client.pre_route_commit(&mut unbound_commit_request);
        assert_eq!(
            unbound_transaction_id_result,
            Some(unbound_transaction_id),
            "commit must return transaction ID for cleanup"
        );
        let connection = unbound_connection
            .expect("mutation leader connection must be resolved when no affinity is present");
        assert_eq!(
            connection.address(),
            node_address_2,
            "commit without affinity must route to mutation leader tablet (node 2)"
        );
        let unbound_hint = unbound_commit_request
            .routing_hint
            .expect("routing_hint must be attached based on mutation");
        assert_eq!(
            unbound_hint.tablet_uid, 2001,
            "routing_hint must target mutation leader tablet"
        );

        // 10. Commit with transaction_id without affinity and without mutations (e.g. DML-only transaction with expired affinity):
        //     returns None connection to fall back to round-robin over the channel pool.
        let unbound_empty_transaction_id = Bytes::from_static(b"unbound-tx-no-mutations");
        let mut unbound_empty_commit_request =
            CommitRequest::new().set_transaction_id(unbound_empty_transaction_id.clone());
        let (unbound_empty_connection, unbound_empty_transaction_id_result) =
            database_client.pre_route_commit(&mut unbound_empty_commit_request);
        assert_eq!(
            unbound_empty_transaction_id_result,
            Some(unbound_empty_transaction_id),
            "commit must return transaction ID for cleanup"
        );
        assert!(
            unbound_empty_connection.is_none(),
            "commit without affinity and without mutations must return None connection to fall back to channel pool round-robin"
        );
        assert!(
            unbound_empty_commit_request.routing_hint.is_none(),
            "commit without affinity and without mutations must not attach routing hint"
        );

        // 11. Commit with mutations when database_id == 0: resolves connection but leaves routing_hint None
        let zero_database_id_client = spanner
            .database_client("projects/p/instances/i/databases/d")
            .with_location_aware_routing(true)
            .build()
            .await
            .expect("build should succeed");
        let zero_database_id_cache_update = CacheUpdate::new()
            .set_database_id(0u64)
            .set_key_recipes(
                RecipeList::new()
                    .set_schema_generation(Bytes::from_static(b"schema-v1"))
                    .set_recipe(vec![recipe]),
            )
            .set_group(vec![
                Group::new()
                    .set_group_uid(100u64)
                    .set_leader_index(0)
                    .set_tablets(vec![
                        Tablet::new()
                            .set_tablet_uid(1001u64)
                            .set_server_address(node_address)
                            .set_role(Role::ReadWrite),
                    ]),
            ])
            .set_range(vec![
                Range::new()
                    .set_group_uid(100u64)
                    .set_split_id(500u64)
                    .set_start_key(vec![0x00])
                    .set_limit_key(vec![0x10]),
            ]);
        zero_database_id_client.observe_cache_update(Some(zero_database_id_cache_update));
        let router_zero = zero_database_id_client
            .location_router()
            .expect("router must be present");
        let client_config_zero = zero_database_id_client
            .cache_updater()
            .expect("cache updater must be present")
            .client_config();
        let _ = router_zero
            .connection_cache()
            .get(node_address, client_config_zero)
            .await
            .expect("should initialize connection");
        let mut zero_database_id_commit_request =
            CommitRequest::new().set_mutations(vec![user_mutation.clone().build_proto()]);
        let (zero_database_id_connection, zero_database_id_transaction_id) =
            zero_database_id_client.pre_route_commit(&mut zero_database_id_commit_request);
        assert!(
            zero_database_id_transaction_id.is_none(),
            "single-use commit must have no transaction ID"
        );
        let connection = zero_database_id_connection
            .expect("connection must still be resolved to leader tablet when database_id is 0");
        assert_eq!(
            connection.address(),
            node_address,
            "zero database_id commit must route to leader tablet"
        );
        assert!(
            zero_database_id_commit_request.routing_hint.is_none(),
            "routing_hint must NOT be attached when database_id is 0"
        );

        // 12. Commit with empty Bytes transaction_id (treated as unbound commit with mutations):
        //     filters out empty transaction_id, resolves connection to mutation leader, and attaches RoutingHint.
        let mut empty_bytes_commit_request = CommitRequest::new()
            .set_transaction_id(Bytes::new())
            .set_mutations(vec![user_mutation.clone().build_proto()]);
        let (empty_bytes_connection, empty_bytes_transaction_id) =
            database_client.pre_route_commit(&mut empty_bytes_commit_request);
        assert!(
            empty_bytes_transaction_id.is_none(),
            "empty transaction ID must be filtered out to None"
        );
        let connection = empty_bytes_connection
            .expect("leader connection must be resolved for empty transaction_id commit");
        assert_eq!(
            connection.address(),
            node_address,
            "empty transaction_id commit must route to mutation leader tablet"
        );
        let empty_bytes_hint = empty_bytes_commit_request
            .routing_hint
            .expect("routing_hint must be attached based on mutation");
        assert_eq!(
            empty_bytes_hint.tablet_uid, 1001,
            "routing_hint must match leader tablet"
        );

        // 13. Commit with location-aware routing disabled: returns None connection and leaves hint None
        let disabled_client = spanner
            .database_client("projects/p/instances/i/databases/d")
            .with_location_aware_routing(false)
            .build()
            .await
            .expect("build should succeed");
        let mut disabled_commit_request =
            CommitRequest::new().set_mutations(vec![user_mutation.build_proto()]);
        let (disabled_connection, disabled_transaction_id) =
            disabled_client.pre_route_commit(&mut disabled_commit_request);
        assert!(
            disabled_connection.is_none(),
            "disabled location routing must return None connection"
        );
        assert!(
            disabled_transaction_id.is_none(),
            "disabled location routing must have no transaction ID"
        );
        assert!(
            disabled_commit_request.routing_hint.is_none(),
            "disabled location routing must not attach routing hint"
        );
    }

    #[tokio_test_no_panics]
    async fn begin_transaction_and_commit_unary_rpcs_attach_routing_hint_end_to_end() {
        let captured_begin_hints = Arc::new(Mutex::new(Vec::new()));
        let captured_commit_hints = Arc::new(Mutex::new(Vec::new()));

        let mut mock = create_test_mock();

        let mut sequence = Sequence::new();
        let captured_begin_first = Arc::clone(&captured_begin_hints);
        let captured_begin_second = Arc::clone(&captured_begin_hints);
        mock.expect_begin_transaction()
            .times(1)
            .in_sequence(&mut sequence)
            .returning(move |request| {
                let request = request.into_inner();
                captured_begin_first
                    .lock()
                    .expect("lock captured begin hints")
                    .push(request.routing_hint);
                Ok(Response::new(mock_v1::Transaction {
                    id: b"tx-e2e-1".to_vec(),
                    ..Default::default()
                }))
            });
        mock.expect_begin_transaction()
            .times(1)
            .in_sequence(&mut sequence)
            .returning(move |request| {
                let request = request.into_inner();
                captured_begin_second
                    .lock()
                    .expect("lock captured begin hints")
                    .push(request.routing_hint);
                Ok(Response::new(mock_v1::Transaction {
                    id: b"tx-e2e-2".to_vec(),
                    ..Default::default()
                }))
            });

        let captured_commit = Arc::clone(&captured_commit_hints);
        mock.expect_commit().returning(move |request| {
            let request = request.into_inner();
            captured_commit
                .lock()
                .expect("lock captured commit hints")
                .push(request.routing_hint);
            Ok(Response::new(mock_v1::CommitResponse::default()))
        });

        let (address, _server) = start("0.0.0.0:0", mock)
            .await
            .expect("Failed to start mock server");
        let spanner = Spanner::builder()
            .with_endpoint(address.clone())
            .with_instance_type(InstanceType::Omni)
            .with_credentials(Anonymous::new().build())
            .build()
            .await
            .expect("Failed to build client");

        let database_client = spanner
            .database_client("projects/p/instances/i/databases/d")
            .with_location_aware_routing(true)
            .build()
            .await
            .expect("build should succeed");

        // Ingest CacheUpdate pointing to the mock server address
        let recipe = KeyRecipe::new().set_table_name("Users").set_part(vec![
            Part::new().set_tag(50020u32),
            Part::new()
                .set_tag(1u32)
                .set_identifier("id")
                .set_type(Type::new().set_code(TypeCode::String))
                .set_order(Order::Ascending)
                .set_null_order(NullOrder::NotNull),
        ]);
        let cache_update = CacheUpdate::new()
            .set_database_id(42u64)
            .set_key_recipes(
                RecipeList::new()
                    .set_schema_generation(Bytes::from_static(b"schema-v1"))
                    .set_recipe(vec![recipe]),
            )
            .set_group(vec![
                Group::new()
                    .set_group_uid(100u64)
                    .set_leader_index(0)
                    .set_tablets(vec![
                        Tablet::new()
                            .set_tablet_uid(1001u64)
                            .set_server_address(address.clone())
                            .set_role(Role::ReadWrite),
                    ]),
            ])
            .set_range(vec![
                Range::new()
                    .set_group_uid(100u64)
                    .set_split_id(500u64)
                    .set_start_key(b"".to_vec())
                    .set_limit_key(b"\xff".to_vec()),
            ]);
        database_client.observe_cache_update(Some(cache_update));

        let router = database_client
            .location_router()
            .expect("router must be present");
        let client_config = database_client
            .cache_updater()
            .expect("cache updater must be present")
            .client_config();
        let _ = router
            .connection_cache()
            .get(&address, client_config)
            .await
            .expect("should initialize connection");

        let user_mutation = Mutation::new_insert_builder("Users")
            .set("id")
            .to("user123")
            .build();

        // 1. Call begin_transaction: verify RoutingHint is attached in dispatched RPC
        let begin_request = BeginTransactionRequest::new()
            .set_session("projects/p/instances/i/databases/d/sessions/s1")
            .set_options(TransactionOptions::new().set_read_write(ReadWrite::new()))
            .set_mutation_key(user_mutation.clone().build_proto());
        let _ = database_client
            .begin_transaction(begin_request, RequestOptions::default(), 0)
            .await
            .expect("begin_transaction must succeed");

        let begin_hints = captured_begin_hints
            .lock()
            .expect("lock captured begin hints")
            .clone();
        assert_eq!(
            begin_hints.len(),
            1,
            "exactly one begin_transaction call must be captured"
        );
        let begin_hint = begin_hints[0]
            .as_ref()
            .expect("dispatched BeginTransactionRequest must have routing_hint populated");
        assert_eq!(
            begin_hint.database_id, 42,
            "dispatched begin hint database_id must match cache"
        );
        assert_eq!(
            begin_hint.tablet_uid, 1001,
            "dispatched begin hint tablet_uid must match leader tablet"
        );
        assert_eq!(
            router.get_transaction_affinity(b"tx-e2e-1").as_deref(),
            Some(address.as_str()),
            "transaction affinity must be recorded to the leader address after begin_transaction"
        );

        // 2. Call commit: verify RoutingHint is attached in dispatched RPC
        let commit_request = CommitRequest::new()
            .set_session("projects/p/instances/i/databases/d/sessions/s1")
            .set_transaction_id(Bytes::from_static(b"tx-e2e-1"))
            .set_mutations(vec![user_mutation.clone().build_proto()]);
        let _ = database_client
            .commit(commit_request, RequestOptions::default(), 0)
            .await
            .expect("commit must succeed");

        let commit_hints = captured_commit_hints
            .lock()
            .expect("lock captured commit hints")
            .clone();
        assert_eq!(
            commit_hints.len(),
            1,
            "exactly one commit call must be captured"
        );
        let commit_hint = commit_hints[0]
            .as_ref()
            .expect("dispatched CommitRequest must have routing_hint populated");
        assert_eq!(
            commit_hint.database_id, 42,
            "dispatched commit hint database_id must match cache"
        );
        assert_eq!(
            commit_hint.tablet_uid, 1001,
            "dispatched commit hint tablet_uid must match leader tablet"
        );
        assert!(
            router.get_transaction_affinity(b"tx-e2e-1").is_none(),
            "transaction affinity must be cleared after commit"
        );

        // 3. Call single-use commit: verify RoutingHint is attached in dispatched RPC
        let single_use_commit_request = CommitRequest::new()
            .set_session("projects/p/instances/i/databases/d/sessions/s1")
            .set_single_use_transaction(TransactionOptions::new().set_read_write(ReadWrite::new()))
            .set_mutations(vec![user_mutation.clone().build_proto()]);
        let _ = database_client
            .commit(single_use_commit_request, RequestOptions::default(), 0)
            .await
            .expect("single-use commit must succeed");

        let commit_hints_after_single_use = captured_commit_hints
            .lock()
            .expect("lock captured commit hints")
            .clone();
        assert_eq!(
            commit_hints_after_single_use.len(),
            2,
            "exactly two commit calls must be captured"
        );
        let single_use_hint = commit_hints_after_single_use[1]
            .as_ref()
            .expect("dispatched single-use CommitRequest must have routing_hint populated");
        assert_eq!(
            single_use_hint.database_id, 42,
            "dispatched single-use commit hint database_id must match cache"
        );
        assert_eq!(
            single_use_hint.tablet_uid, 1001,
            "dispatched single-use commit hint tablet_uid must match leader tablet"
        );

        // 4. Call unkeyed begin_transaction: verify no RoutingHint is attached, and affinity is recorded to default connection
        let unkeyed_begin_request = BeginTransactionRequest::new()
            .set_session("projects/p/instances/i/databases/d/sessions/s1")
            .set_options(TransactionOptions::new().set_read_write(ReadWrite::new()));
        let unkeyed_response = database_client
            .begin_transaction(unkeyed_begin_request, RequestOptions::default(), 0)
            .await
            .expect("unkeyed begin_transaction must succeed");

        let begin_hints_after_unkeyed = captured_begin_hints
            .lock()
            .expect("lock captured begin hints")
            .clone();
        assert_eq!(
            begin_hints_after_unkeyed.len(),
            2,
            "exactly two begin_transaction calls must be captured"
        );
        assert!(
            begin_hints_after_unkeyed[1].is_none(),
            "unkeyed BeginTransactionRequest must not have routing_hint populated"
        );
        let default_address = router
            .connection_cache()
            .default_connection()
            .address()
            .to_string();
        assert_eq!(
            router
                .get_transaction_affinity(&unkeyed_response.id)
                .as_deref(),
            Some(default_address.as_str()),
            "unkeyed read-write begin must record affinity to default gateway connection"
        );
    }

    #[tokio_test_no_panics]
    async fn post_route_begin_transaction_records_affinity() {
        let mock = create_test_mock();
        let (address, _server) = start("0.0.0.0:0", mock)
            .await
            .expect("Failed to start mock server");
        let spanner = Spanner::builder()
            .with_endpoint(address)
            .with_instance_type(InstanceType::Omni)
            .with_credentials(Anonymous::new().build())
            .build()
            .await
            .expect("Failed to build client");

        let database_client = spanner
            .database_client("projects/p/instances/i/databases/d")
            .with_location_aware_routing(true)
            .build()
            .await
            .expect("build should succeed");

        let router = database_client
            .location_router()
            .expect("router must be present");
        let client_config = database_client
            .cache_updater()
            .expect("cache updater must be present")
            .client_config();
        let explicit_connection = router
            .connection_cache()
            .get("explicit.node:15000", client_config)
            .await
            .expect("should initialize connection");

        // 1. Read-write transaction with explicit connection: records affinity to explicit connection address
        let success_read_write =
            Ok(Transaction::new().set_id(Bytes::from_static(b"tx-rw-explicit")));
        database_client.post_route_begin_transaction(
            true,
            Some(&explicit_connection),
            &success_read_write,
        );
        assert_eq!(
            router
                .get_transaction_affinity(b"tx-rw-explicit")
                .as_deref(),
            Some("explicit.node:15000"),
            "affinity must be recorded to explicit connection address"
        );

        // 2. Read-write transaction with None connection: records affinity to default connection address
        let default_address = router
            .connection_cache()
            .default_connection()
            .address()
            .to_string();
        let success_read_write_default =
            Ok(Transaction::new().set_id(Bytes::from_static(b"tx-rw-default")));
        database_client.post_route_begin_transaction(true, None, &success_read_write_default);
        assert_eq!(
            router.get_transaction_affinity(b"tx-rw-default").as_deref(),
            Some(default_address.as_str()),
            "affinity must be recorded to default connection address when connection is None"
        );

        // 3. Read-only transaction (is_read_write = false): does NOT record affinity
        let success_read_only = Ok(Transaction::new().set_id(Bytes::from_static(b"tx-ro")));
        database_client.post_route_begin_transaction(
            false,
            Some(&explicit_connection),
            &success_read_only,
        );
        assert!(
            router.get_transaction_affinity(b"tx-ro").is_none(),
            "read-only transactions must not record affinity"
        );

        // 4. Failed read-write transaction (Err): does NOT record affinity
        let failed_read_write: Result<Transaction> = Err(internal_error("RPC error"));
        database_client.post_route_begin_transaction(
            true,
            Some(&explicit_connection),
            &failed_read_write,
        );
        assert!(
            router.get_transaction_affinity(b"tx-rw-failed").is_none(),
            "failed transactions must not record affinity"
        );

        // 5. Read-write transaction with empty ID: does NOT record affinity
        let empty_id_read_write = Ok(Transaction::new().set_id(Bytes::new()));
        database_client.post_route_begin_transaction(
            true,
            Some(&explicit_connection),
            &empty_id_read_write,
        );
        assert!(
            router.get_transaction_affinity(b"").is_none(),
            "empty transaction ID must not record affinity"
        );
    }

    #[tokio_test_no_panics]
    async fn post_route_commit_clears_affinity() {
        let mock = create_test_mock();
        let (address, _server) = start("0.0.0.0:0", mock)
            .await
            .expect("Failed to start mock server");
        let spanner = Spanner::builder()
            .with_endpoint(address)
            .with_instance_type(InstanceType::Omni)
            .with_credentials(Anonymous::new().build())
            .build()
            .await
            .expect("Failed to build client");

        let database_client = spanner
            .database_client("projects/p/instances/i/databases/d")
            .with_location_aware_routing(true)
            .build()
            .await
            .expect("build should succeed");

        let router = database_client
            .location_router()
            .expect("router must be present");

        // 1. Clear active affinity on commit success
        let active_transaction_id = Bytes::from_static(b"committed-tx-1");
        router.record_transaction_affinity(&active_transaction_id, "some-node:15000");
        assert!(
            router
                .get_transaction_affinity(&active_transaction_id)
                .is_some(),
            "affinity must be pre-recorded"
        );

        let success_commit = Ok(CommitResponse::new());
        database_client.post_route_commit(
            Some(active_transaction_id.clone()),
            None,
            &success_commit,
        );
        assert!(
            router
                .get_transaction_affinity(&active_transaction_id)
                .is_none(),
            "affinity must be cleared after successful commit"
        );

        // 2. Clear active affinity even on commit error
        let failed_commit_transaction_id = Bytes::from_static(b"committed-tx-2");
        router.record_transaction_affinity(&failed_commit_transaction_id, "some-node:15000");
        let error_commit: Result<CommitResponse> = Err(internal_error("commit aborted"));
        database_client.post_route_commit(
            Some(failed_commit_transaction_id.clone()),
            None,
            &error_commit,
        );
        assert!(
            router
                .get_transaction_affinity(&failed_commit_transaction_id)
                .is_none(),
            "affinity must be cleared even after commit failure"
        );

        // 3. No-op when transaction ID is None or empty
        database_client.post_route_commit(None, None, &success_commit);
        database_client.post_route_commit(Some(Bytes::new()), None, &success_commit);
    }
}
