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
use crate::model::transaction_selector::Selector;
use crate::model::{
    BatchWriteRequest, BeginTransactionRequest, CacheUpdate, CommitRequest, CommitResponse,
    ExecuteBatchDmlRequest, ExecuteBatchDmlResponse, ExecuteSqlRequest, PartitionQueryRequest,
    PartitionReadRequest, PartitionResponse, ReadRequest, ResultSet, RollbackRequest, Transaction,
    TransactionSelector,
};
use crate::observability::Observability;
use crate::omni::{InstanceType, format_database_name};
use crate::partitioned_dml_transaction::PartitionedDmlTransactionBuilder;
use crate::read_only_transaction::{
    MultiUseReadOnlyTransactionBuilder, SingleUseReadOnlyTransactionBuilder,
};
use crate::routing::cache_updater::CacheUpdater;
use crate::routing::connection_cache::ConnectionCache;
use crate::routing::endpoint_cooldown::EndpointCooldownTracker;
use crate::routing::key_extractor::extract_proto_read_request_routing_key;
use crate::routing::key_range_cache::KeyRangeCache;
use crate::routing::key_recipe_cache::KeyRecipeCache;
use crate::routing::location_router::{LocationRouter, RoutingContext};
use crate::routing::server_connection::ServerConnection;
use crate::server_streaming::builder::{BatchWrite, ExecuteStreamingSql, StreamingRead};
use crate::session_maintainer::ManagedSessionMaintainer;
use crate::transaction_runner::TransactionRunnerBuilder;
use crate::write_only_transaction::WriteOnlyTransactionBuilder;
use crate::{RequestOptions, Result};
use std::env;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

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
    ($method:ident, $expect_method:ident, $request_type:ty, $response_type:ty) => {
        pub(crate) async fn $method(
            &self,
            request: $request_type,
            options: RequestOptions,
            channel_hint: usize,
        ) -> Result<$response_type> {
            let channel = self.spanner.get_channel(channel_hint);
            let response = self
                .spanner
                .$method(request, options, channel, &self.o11y)
                .await?;
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
            request: $request_type,
            options: RequestOptions,
            channel_hint: usize,
        ) -> $builder_type {
            // Step 1: When location-aware routing is disabled (standard Cloud Spanner),
            // `self.location_routing` is `None` so `$extract_key` is skipped immediately.
            // When enabled (Spanner Omni), extract the binary routing key from the request if present.
            let routing_key = self
                .location_routing
                .as_ref()
                .and_then(|routing| $extract_key(routing, &request));

            // Step 2: Resolve the optimal server connection if location-aware routing is enabled.
            // - If disabled (standard Cloud Spanner), returns `None` immediately on the fast path.
            // - If enabled but both `transaction_id` and `routing_key` are `None`, returns `None` early.
            // - If a target tablet replica or affinity connection is found, returns `Some(connection)`.
            let connection = self
                .resolve_routing_connection(request.transaction.as_ref(), routing_key.as_deref());

            // Step 3: Select the gRPC channel:
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
            Transaction
        );
        $macro!(commit, expect_commit, CommitRequest, CommitResponse);
        $macro!(
            execute_batch_dml,
            expect_execute_batch_dml,
            ExecuteBatchDmlRequest,
            ExecuteBatchDmlResponse
        );
        $macro!(
            execute_sql,
            expect_execute_sql,
            ExecuteSqlRequest,
            ResultSet
        );
        $macro!(rollback, expect_rollback, RollbackRequest, ());
        $macro!(
            partition_query,
            expect_partition_query,
            PartitionQueryRequest,
            PartitionResponse
        );
        $macro!(
            partition_read,
            expect_partition_read,
            PartitionReadRequest,
            PartitionResponse
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
            |_routing, _request| None::<Vec<u8>>
        );
        $macro!(
            streaming_read,
            expect_streaming_read,
            ReadRequest,
            StreamingRead,
            |routing: &LocationRoutingState, request: &ReadRequest| {
                extract_proto_read_request_routing_key(&routing.key_recipe_cache, request)
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
    pub(crate) fn resolve_routing_connection(
        &self,
        transaction: Option<&TransactionSelector>,
        routing_key: Option<&[u8]>,
    ) -> Option<ServerConnection> {
        // Fast path: When location routing is disabled (standard Cloud Spanner), exit immediately.
        let routing = self.location_routing.as_ref()?;

        // Fast path: No routing metadata available to route the request (preserves channel pooling).
        let transaction_id = extract_transaction_id(transaction);
        if transaction_id.is_none() && routing_key.is_none() {
            return None;
        }

        let routing_context = RoutingContext {
            transaction_id,
            routing_key,
            prefer_leader: false,
            use_transaction_affinity: transaction_id.is_some(),
        };
        Some(routing.location_router.resolve_connection(&routing_context))
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

    /// Returns the database ID assigned by the server for location-aware routing, if known.
    #[allow(dead_code)] // TODO: Used when constructing RoutingHint on requests in subsequent PRs
    pub(crate) fn database_id(&self) -> Option<u64> {
        self.location_routing
            .as_ref()
            .map(|routing| routing.database_id.load(Ordering::Acquire))
    }

    /// Observes an incoming [`CacheUpdate`], updating routing ranges, pre-warming connections, and caching key recipes.
    pub(crate) fn observe_cache_update(&self, cache_update: Option<CacheUpdate>) {
        let (Some(routing), Some(cache_update)) = (&self.location_routing, cache_update) else {
            return;
        };
        let update_database_id = cache_update.database_id;

        // If the update specifies a database ID that differs from the active one,
        // or on initial startup, acquire an exclusive write lock to transition the ID
        // and safely clear stale caches.
        if update_database_id != 0 {
            let current_id = routing.database_id.load(Ordering::Acquire);
            if current_id != update_database_id {
                let _write_guard = routing.update_lock.write().expect("poisoned update lock");
                let current_id = routing.database_id.load(Ordering::Acquire);
                if current_id != update_database_id {
                    if current_id != 0 && update_database_id < current_id {
                        // Stale update from an older database generation: abort ingestion.
                        return;
                    }
                    routing.cache_updater.process_cache_update(&cache_update);
                    routing
                        .database_id
                        .store(routing.cache_updater.database_id(), Ordering::Release);
                    return;
                }
            }
        }

        // Shared read path: Multiple threads can concurrently ingest incremental updates for the current active database.
        // The shared read lock prevents cache updates from racing with an exclusive cache invalidation / database ID switch.
        let _read_guard = routing.update_lock.read().expect("poisoned update lock");
        if update_database_id != 0
            && update_database_id < routing.database_id.load(Ordering::Acquire)
        {
            // A database ID switch occurred before acquiring the read lock; abort stale update.
            return;
        }

        routing.cache_updater.process_cache_update(&cache_update);
        if update_database_id != 0 {
            routing
                .database_id
                .store(routing.cache_updater.database_id(), Ordering::Release);
        }
    }
}

fn extract_transaction_id(transaction: Option<&TransactionSelector>) -> Option<&[u8]> {
    match transaction?.selector.as_ref()? {
        Selector::Id(bytes) => Some(bytes.as_ref()),
        _ => None,
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
        let session_maintainer = ManagedSessionMaintainer::create_and_start_maintenance(
            self.spanner.clone(),
            database_name,
            self.database_role.unwrap_or_default(),
            self.options.unwrap_or_default(),
            Arc::clone(&o11y),
        )
        .await?;

        // TODO: Enable location-aware routing by default for Omni instances once fully stabilized.
        let location_aware_routing_enabled = self
            .location_aware_routing_enabled
            .or_else(|| {
                env::var("GOOGLE_SPANNER_EXPERIMENTAL_LOCATION_API")
                    .ok()
                    .and_then(|v| v.parse::<bool>().ok())
            })
            .unwrap_or(false);

        let location_routing = location_aware_routing_enabled
            .then(|| Arc::new(LocationRoutingState::new(&self.spanner)));

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
    pub(crate) database_id: AtomicU64,
    pub(crate) update_lock: RwLock<()>,
}

const DEFAULT_ENDPOINT: &str = "spanner.googleapis.com:443";

impl LocationRoutingState {
    fn new(spanner: &Spanner) -> Self {
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
        let key_range_cache = Arc::new(KeyRangeCache::new());
        let key_recipe_cache = Arc::new(KeyRecipeCache::new());
        let cooldown_tracker = Arc::new(EndpointCooldownTracker::new());
        let location_router = Arc::new(LocationRouter::new(
            Arc::clone(&key_range_cache),
            Arc::clone(&connection_cache),
            cooldown_tracker,
        ));
        let cache_updater = Arc::new(CacheUpdater::new(
            key_range_cache,
            Arc::clone(&key_recipe_cache),
            connection_cache,
            spanner.config.clone(),
        ));
        let database_id = AtomicU64::new(0);
        let update_lock = RwLock::new(());

        Self {
            location_router,
            cache_updater,
            key_recipe_cache,
            database_id,
            update_lock,
        }
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
    use crate::model::key_recipe::Part;
    use crate::model::key_recipe::part::{NullOrder, Order};
    use crate::model::{
        CacheUpdate, CommitResponse, Group, KeyRecipe, KeySet, Range, RecipeList, Tablet,
        TransactionOptions, Type, TypeCode,
    };
    use crate::result_set::tests::adapt;
    use crate::routing::key_range_cache::RangeMode;
    use gaxi::options::ClientConfig;
    use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
    use google_cloud_test_macros::tokio_test_no_panics;
    use spanner_grpc_mock::{MockSpanner, start};

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
        let mut seq = mockall::Sequence::new();
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
        let mut mock = MockSpanner::new();
        mock.expect_create_session().returning(|req| {
            let req = req.into_inner();
            let session = req.session.expect("session present in request");
            Ok(gaxi::grpc::tonic::Response::new(
                spanner_grpc_mock::google::spanner::v1::Session {
                    name: "projects/p/instances/i/databases/d/sessions/s1".to_string(),
                    multiplexed: session.multiplexed,
                    ..Default::default()
                },
            ))
        });

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
        let mut mock = MockSpanner::new();
        mock.expect_create_session().returning(|req| {
            let req = req.into_inner();
            let session = req.session.expect("session present in request");
            Ok(gaxi::grpc::tonic::Response::new(
                spanner_grpc_mock::google::spanner::v1::Session {
                    name: "projects/p/instances/i/databases/d/sessions/s1".to_string(),
                    multiplexed: session.multiplexed,
                    ..Default::default()
                },
            ))
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
        let mut mock = MockSpanner::new();
        mock.expect_create_session().returning(|req| {
            let req = req.into_inner();
            let session = req.session.expect("session present in request");
            Ok(gaxi::grpc::tonic::Response::new(
                spanner_grpc_mock::google::spanner::v1::Session {
                    name: "projects/p/instances/i/databases/d/sessions/s1".to_string(),
                    multiplexed: session.multiplexed,
                    ..Default::default()
                },
            ))
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
        let mut mock = MockSpanner::new();
        mock.expect_create_session().returning(|req| {
            let req = req.into_inner();
            let session = req.session.expect("session present in request");
            Ok(gaxi::grpc::tonic::Response::new(
                spanner_grpc_mock::google::spanner::v1::Session {
                    name: "projects/p/instances/i/databases/d/sessions/s1".to_string(),
                    multiplexed: session.multiplexed,
                    ..Default::default()
                },
            ))
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
        let mut mock = MockSpanner::new();
        mock.expect_create_session().returning(|req| {
            let req = req.into_inner();
            let session = req.session.expect("session present in request");
            Ok(gaxi::grpc::tonic::Response::new(
                spanner_grpc_mock::google::spanner::v1::Session {
                    name: "projects/p/instances/i/databases/d/sessions/s1".to_string(),
                    multiplexed: session.multiplexed,
                    ..Default::default()
                },
            ))
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
        let mut mock = MockSpanner::new();
        mock.expect_create_session().returning(|req| {
            let req = req.into_inner();
            let session = req.session.expect("session present in request");
            Ok(gaxi::grpc::tonic::Response::new(
                spanner_grpc_mock::google::spanner::v1::Session {
                    name: "projects/p/instances/i/databases/d/sessions/s1".to_string(),
                    multiplexed: session.multiplexed,
                    ..Default::default()
                },
            ))
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
        let mut mock = MockSpanner::new();
        mock.expect_create_session().returning(|req| {
            let req = req.into_inner();
            let session = req.session.expect("session present in request");
            Ok(gaxi::grpc::tonic::Response::new(
                spanner_grpc_mock::google::spanner::v1::Session {
                    name: "projects/p/instances/i/databases/d/sessions/s1".to_string(),
                    multiplexed: session.multiplexed,
                    ..Default::default()
                },
            ))
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
        let mut mock = MockSpanner::new();
        mock.expect_create_session().returning(|req| {
            let req = req.into_inner();
            let session = req.session.expect("session present in request");
            Ok(gaxi::grpc::tonic::Response::new(
                spanner_grpc_mock::google::spanner::v1::Session {
                    name: "projects/p/instances/i/databases/d/sessions/s1".to_string(),
                    multiplexed: session.multiplexed,
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
        let mut mock = MockSpanner::new();
        mock.expect_create_session().returning(|req| {
            let req = req.into_inner();
            let session = req.session.expect("session present in request");
            Ok(gaxi::grpc::tonic::Response::new(
                spanner_grpc_mock::google::spanner::v1::Session {
                    name: "projects/p/instances/i/databases/d/sessions/s1".to_string(),
                    multiplexed: session.multiplexed,
                    ..Default::default()
                },
            ))
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
        let mut mock = MockSpanner::new();
        mock.expect_create_session().returning(|req| {
            let req = req.into_inner();
            let session = req.session.expect("session present in request");
            Ok(gaxi::grpc::tonic::Response::new(
                spanner_grpc_mock::google::spanner::v1::Session {
                    name: "projects/p/instances/i/databases/d/sessions/s1".to_string(),
                    multiplexed: session.multiplexed,
                    ..Default::default()
                },
            ))
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
        let mut mock = MockSpanner::new();
        mock.expect_create_session().returning(|req| {
            let req = req.into_inner();
            let session = req.session.expect("session present in request");
            Ok(gaxi::grpc::tonic::Response::new(
                spanner_grpc_mock::google::spanner::v1::Session {
                    name: "projects/p/instances/i/databases/d/sessions/s1".to_string(),
                    multiplexed: session.multiplexed,
                    ..Default::default()
                },
            ))
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
        let mut mock = MockSpanner::new();
        mock.expect_create_session().returning(|req| {
            let req = req.into_inner();
            let session = req.session.expect("session present in request");
            Ok(gaxi::grpc::tonic::Response::new(
                spanner_grpc_mock::google::spanner::v1::Session {
                    name: "projects/p/instances/i/databases/d/sessions/s1".to_string(),
                    multiplexed: session.multiplexed,
                    ..Default::default()
                },
            ))
        });

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
        let connection = db_client.resolve_routing_connection(None, Some(b"Users.id=1".as_slice()));
        assert!(connection.is_none());
    }

    #[tokio_test_no_panics]
    async fn resolve_routing_connection_omni_cold_start_and_cache_hit_and_cooldown() {
        let mut mock = MockSpanner::new();
        mock.expect_create_session().returning(|req| {
            let req = req.into_inner();
            let session = req.session.expect("session present in request");
            Ok(gaxi::grpc::tonic::Response::new(
                spanner_grpc_mock::google::spanner::v1::Session {
                    name: "projects/p/instances/i/databases/d/sessions/s1".to_string(),
                    multiplexed: session.multiplexed,
                    ..Default::default()
                },
            ))
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
        let cold_start_connection =
            db_client.resolve_routing_connection(None, routing_key.as_deref());
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
        let hit_connection = db_client
            .resolve_routing_connection(None, routing_key.as_deref())
            .expect("hit connection present");
        assert_eq!(hit_connection.address(), node_address);

        // 4. Mark node on cooldown: falls back to default connection
        router.cooldown_tracker().record_failure(node_address);
        let fallback_connection = db_client
            .resolve_routing_connection(None, routing_key.as_deref())
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

    #[tokio_test_no_panics]
    async fn resolve_routing_connection_omni_without_routing_info_returns_none() {
        let mut mock = MockSpanner::new();
        mock.expect_create_session().returning(|req| {
            let req = req.into_inner();
            let session = req.session.expect("session present in request");
            Ok(gaxi::grpc::tonic::Response::new(
                spanner_grpc_mock::google::spanner::v1::Session {
                    name: "projects/p/instances/i/databases/d/sessions/s1".to_string(),
                    multiplexed: session.multiplexed,
                    ..Default::default()
                },
            ))
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

        // 1. Neither transaction_id nor routing_key: must return None to preserve channel pool round-robin
        assert!(
            db_client.resolve_routing_connection(None, None).is_none(),
            "Requests with neither transaction_id nor routing_key must resolve to None"
        );

        // 2. Transaction selector without an explicit ID and no routing key: must return None
        let selector_none = TransactionSelector::new();
        assert!(
            db_client
                .resolve_routing_connection(Some(&selector_none), None)
                .is_none(),
            "Requests with empty transaction selector must resolve to None"
        );

        let selector_single = TransactionSelector::new().set_single_use(TransactionOptions::new());
        assert!(
            db_client
                .resolve_routing_connection(Some(&selector_single), None)
                .is_none(),
            "Requests with single_use transaction selector must resolve to None"
        );
    }

    #[tokio_test_no_panics]
    async fn resolve_routing_connection_uses_transaction_affinity_when_transaction_id_present() {
        let mut mock = MockSpanner::new();
        mock.expect_create_session().returning(|req| {
            let req = req.into_inner();
            let session = req.session.expect("session present in request");
            Ok(gaxi::grpc::tonic::Response::new(
                spanner_grpc_mock::google::spanner::v1::Session {
                    name: "projects/p/instances/i/databases/d/sessions/s1".to_string(),
                    multiplexed: session.multiplexed,
                    ..Default::default()
                },
            ))
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
        let initial_connection = db_client
            .resolve_routing_connection(Some(&selector), Some(routing_key.as_slice()))
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
        let unkeyed_connection = db_client
            .resolve_routing_connection(Some(&selector), None)
            .expect("unkeyed request with transaction affinity must resolve to connection");
        assert_eq!(
            unkeyed_connection.address(),
            node_address,
            "Unkeyed query with active transaction_id must route to affinity node"
        );

        // 5. Subsequent request with transaction_id and a different routing key:
        //    Must STILL resolve to node-1 via transaction affinity.
        let different_key_connection = db_client
            .resolve_routing_connection(Some(&selector), Some(b"Orders.id=99".as_slice()))
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
            .resolve_routing_connection(Some(&selector), None)
            .expect("post-cleanup resolution fallback");
        assert_ne!(
            post_cleanup_connection.address(),
            node_address,
            "After affinity is cleared, request with no routing key must fall back"
        );
    }

    #[tokio_test_no_panics]
    async fn resolve_routing_connection_does_not_record_affinity_without_transaction_id() {
        let mut mock = MockSpanner::new();
        mock.expect_create_session().returning(|req| {
            let req = req.into_inner();
            let session = req.session.expect("session present in request");
            Ok(gaxi::grpc::tonic::Response::new(
                spanner_grpc_mock::google::spanner::v1::Session {
                    name: "projects/p/instances/i/databases/d/sessions/s1".to_string(),
                    multiplexed: session.multiplexed,
                    ..Default::default()
                },
            ))
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
        let connection_none = db_client
            .resolve_routing_connection(None, Some(routing_key.as_slice()))
            .expect("keyed request resolves to connection");
        assert_eq!(connection_none.address(), node_address);
        assert_eq!(
            router.affinity_count(),
            0,
            "Must not record transaction affinity when transaction is None"
        );

        // 2. Single-use transaction selector with routing key: routes to node-1 without recording affinity
        let selector_single = TransactionSelector::new().set_single_use(TransactionOptions::new());
        let connection_single = db_client
            .resolve_routing_connection(Some(&selector_single), Some(routing_key.as_slice()))
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
        let mut mock = MockSpanner::new();
        mock.expect_create_session().returning(|req| {
            let req = req.into_inner();
            let session = req.session.expect("session present in request");
            Ok(gaxi::grpc::tonic::Response::new(
                spanner_grpc_mock::google::spanner::v1::Session {
                    name: "projects/p/instances/i/databases/d/sessions/s1".to_string(),
                    multiplexed: session.multiplexed,
                    ..Default::default()
                },
            ))
        });

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
            ($method:ident, $expect_method:ident, $request_type:ident, $response_type:ty) => {
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
                ($method:ident, $expect_method:ident, $request_type:ident, $response_type:ty) => {
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
        let mut mock = MockSpanner::new();
        mock.expect_create_session().returning(|req| {
            let req = req.into_inner();
            let session = req.session.expect("session present in request");
            Ok(gaxi::grpc::tonic::Response::new(
                spanner_grpc_mock::google::spanner::v1::Session {
                    name: "projects/p/instances/i/databases/d/sessions/s1".to_string(),
                    multiplexed: session.multiplexed,
                    ..Default::default()
                },
            ))
        });

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
}
