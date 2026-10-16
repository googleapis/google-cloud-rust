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

use crate::Error;
use crate::RequestOptions;
use crate::Result;
use crate::batch::BatchDml;
use crate::batch_dml::process_response;
use crate::channel_pool::{ChannelTarget, TransactionAffinity};
use crate::client::amend_request_options_for_lar;
use crate::database_client::DatabaseClient;
use crate::error::internal_error;
use crate::model::CommitRequest;
use crate::model::CommitResponse;
use crate::model::ExecuteBatchDmlRequest;
use crate::model::ExecuteBatchDmlResponse;
use crate::model::MultiplexedSessionPrecommitToken;
use crate::model::Mutation as ProtoMutation;
use crate::model::ResultSet as ProtoResultSet;
use crate::model::RollbackRequest;
use crate::model::TransactionOptions;
use crate::model::TransactionSelector;
use crate::model::commit_response::MultiplexedSessionRetry;
use crate::model::execute_batch_dml_request::Statement as ExecuteBatchDmlStatement;
use crate::model::request_options::Priority;
use crate::model::result_set_stats::RowCount;
use crate::model::transaction_options::IsolationLevel;
use crate::model::transaction_options::Mode;
use crate::model::transaction_options::ReadWrite;
use crate::model::transaction_options::read_write::ReadLockMode;
use crate::model::transaction_selector::Selector;
use crate::mutation::Mutation;
use crate::precommit::PrecommitTokenTracker;
use crate::read_only_transaction::{
    AbortedInfo, AbortedState, BeginTransactionOption, LazyTransactionStartGuard, ReadContext,
    ReadContextTransactionSelector, TransactionState, execute_begin_transaction,
};
use crate::result_set::ResultSet;
use crate::retry_policy::SpannerRetryPolicy;
use crate::statement::Statement;
use crate::write_only_transaction::create_commit_request;
use bytes::Bytes;
use google_cloud_gax::error::Error as GaxError;
use google_cloud_gax::error::rpc::{Code, Status};
use google_cloud_gax::options::RequestOptions as GaxRequestOptions;
use google_cloud_gax::retry_policy::RetryPolicy;
use google_cloud_gax::retry_result::RetryResult;
use google_cloud_gax::retry_state::RetryState;
use google_cloud_gax::throttle_result::ThrottleResult;
use std::cmp::min;
use std::mem::take;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;
use tokio::runtime::Handle;
use tokio::time::Instant;
use tracing::debug;

/// A builder for [ReadWriteTransaction].
#[derive(Clone, Debug)]
pub(crate) struct ReadWriteTransactionBuilder {
    pub(crate) client: DatabaseClient,
    options: TransactionOptions,
    transaction_tag: Option<String>,
    max_commit_delay: Option<Duration>,
    pub(crate) session_name: String,
    return_commit_stats: bool,
    commit_priority: Priority,
    begin_transaction_option: BeginTransactionOption,
    begin_gax_options: Option<crate::RequestOptions>,
    commit_gax_options: Option<crate::RequestOptions>,
    rollback_gax_options: Option<RequestOptions>,
    affinity: Option<Arc<TransactionAffinity>>,
}

impl ReadWriteTransactionBuilder {
    pub(crate) fn new(client: DatabaseClient) -> Self {
        let session_name = client.session_name();
        Self {
            client,
            options: TransactionOptions::default().set_read_write(ReadWrite::default()),
            transaction_tag: None,
            max_commit_delay: None,
            session_name,
            return_commit_stats: false,
            commit_priority: Priority::Unspecified,
            begin_transaction_option: BeginTransactionOption::InlineBegin,
            begin_gax_options: None,
            commit_gax_options: None,
            rollback_gax_options: None,
            affinity: None,
        }
    }

    pub(crate) fn set_isolation_level(mut self, isolation_level: IsolationLevel) -> Self {
        self.options = self.options.set_isolation_level(isolation_level);
        self
    }

    pub(crate) fn set_read_lock_mode(mut self, read_lock_mode: ReadLockMode) -> Self {
        if let Some(Mode::ReadWrite(rw)) = self.options.mode.take() {
            self.options = self
                .options
                .set_read_write(rw.set_read_lock_mode(read_lock_mode));
        }
        self
    }

    pub(crate) fn set_previous_transaction_id(mut self, id: Bytes) -> Self {
        if let Some(Mode::ReadWrite(read_write)) = &mut self.options.mode {
            read_write.multiplexed_session_previous_transaction_id = id;
        }
        self
    }

    pub(crate) fn set_transaction_tag(mut self, tag: impl Into<String>) -> Self {
        self.transaction_tag = Some(tag.into());
        self
    }

    pub(crate) fn set_commit_priority(mut self, priority: Priority) -> Self {
        self.commit_priority = priority;
        self
    }

    pub(crate) fn set_max_commit_delay(mut self, delay: Duration) -> Self {
        self.max_commit_delay = Some(delay);
        self
    }

    pub(crate) fn set_exclude_txn_from_change_streams(mut self, exclude: bool) -> Self {
        self.options = self.options.set_exclude_txn_from_change_streams(exclude);
        self
    }

    pub(crate) fn set_return_commit_stats(mut self, return_stats: bool) -> Self {
        self.return_commit_stats = return_stats;
        self
    }

    pub(crate) fn set_begin_transaction_option(mut self, option: BeginTransactionOption) -> Self {
        self.begin_transaction_option = option;
        self
    }

    pub(crate) fn with_begin_transaction_request_options(
        mut self,
        options: Option<crate::RequestOptions>,
    ) -> Self {
        self.begin_gax_options = options;
        self
    }

    pub(crate) fn with_commit_request_options(
        mut self,
        options: Option<crate::RequestOptions>,
    ) -> Self {
        self.commit_gax_options = options;
        self
    }

    pub(crate) fn with_rollback_request_options(mut self, options: Option<RequestOptions>) -> Self {
        self.rollback_gax_options = options;
        self
    }

    async fn begin<'a>(
        &self,
        session_name: String,
        channel_target: impl Into<ChannelTarget<'a>>,
        request_options: RequestOptions,
        precommit_token_tracker: &PrecommitTokenTracker,
    ) -> Result<ReadContextTransactionSelector> {
        let response = execute_begin_transaction(
            &self.client,
            session_name,
            self.options.clone(),
            self.transaction_tag.clone(),
            channel_target,
            request_options,
            None,
        )
        .await?;

        precommit_token_tracker.update(response.precommit_token);

        Ok(ReadContextTransactionSelector::Fixed(
            TransactionSelector::default().set_id(response.id),
            None,
        ))
    }

    pub(crate) async fn build(mut self, deadline: Option<Instant>) -> Result<ReadWriteTransaction> {
        let affinity = Some(
            self.affinity
                .take()
                .unwrap_or_else(|| Arc::new(TransactionAffinity::new_read_write())),
        );
        let precommit_token_tracker = PrecommitTokenTracker::new();
        let transaction_selector = match self.begin_transaction_option {
            BeginTransactionOption::ExplicitBegin => {
                let mut options = self.begin_gax_options.clone().unwrap_or_default();
                amend_gax_options(
                    self.client.leader_aware_routing_enabled,
                    deadline,
                    &mut options,
                );

                self.begin(
                    self.session_name.clone(),
                    &affinity,
                    options,
                    &precommit_token_tracker,
                )
                .await?
            }
            BeginTransactionOption::InlineBegin => ReadContextTransactionSelector::Lazy(Arc::new(
                Mutex::new(TransactionState::NotStarted(self.options)),
            )),
        };

        let tokio_handle = Handle::try_current().ok();
        let drop_guard = Arc::new(ReadWriteTransactionDropGuard::new(
            self.client.clone(),
            self.session_name.clone(),
            transaction_selector.clone(),
            affinity.as_ref().map(Arc::clone),
            self.rollback_gax_options,
            tokio_handle,
        ));

        Ok(ReadWriteTransaction {
            context: ReadContext {
                session_name: self.session_name,
                client: self.client,
                transaction_selector,
                precommit_token_tracker,
                transaction_tag: self.transaction_tag,
                begin_transaction_request_options: None,
                affinity,
                aborted_state: AbortedState::new(),
            },
            seqno: Arc::new(AtomicI64::new(1)),
            max_commit_delay: self.max_commit_delay,
            return_commit_stats: self.return_commit_stats,
            deadline,
            commit_priority: self.commit_priority,
            mutations: Arc::new(Mutex::new(Vec::new())),
            begin_gax_options: self.begin_gax_options,
            commit_gax_options: self.commit_gax_options,
            drop_guard,
        })
    }

    pub(crate) fn with_affinity(mut self, affinity: Arc<TransactionAffinity>) -> Self {
        self.affinity = Some(affinity);
        self
    }
}

trait CheckServiceError {
    fn check_service_error(&self) -> Option<Error>;
}

impl CheckServiceError for ProtoResultSet {
    fn check_service_error(&self) -> Option<Error> {
        None
    }
}

/// Normalizes responses from `ExecuteBatchDml`.
/// Spanner returns batch DML errors (both when the initial statement fails and when a subsequent
/// statement in the batch fails) inside `Ok(ExecuteBatchDmlResponse)` via the `status` field.
impl CheckServiceError for ExecuteBatchDmlResponse {
    fn check_service_error(&self) -> Option<Error> {
        if let Some(status) = &self.status
            && status.code != Code::Ok as i32
        {
            let rpc_status = Status::default()
                .set_code(status.code)
                .set_message(status.message.clone())
                .set_details(status.details.clone());
            return Some(Error::service(rpc_status));
        }
        None
    }
}

/// Helper macro to execute a DML or BatchDML RPC with retry logic if the
/// request included a BeginTransaction option.
macro_rules! execute_with_retry {
    ($self:expr, $request:ident, $gax_options:expr, $rpc_method:ident, $extract_id:expr) => {{
        if let Some(info) = $self.aborted_info() {
            return Err(info.to_error());
        }
        let is_starting = matches!(
            $request
                .transaction
                .as_ref()
                .and_then(|t| t.selector.as_ref()),
            Some(Selector::Begin(_))
        );

        let mut guard = LazyTransactionStartGuard::new(
            is_starting.then(|| $self.context.transaction_selector.clone()),
        );

        let response_result = $self
            .context
            .client
            .$rpc_method(
                $request.clone(),
                $gax_options.clone(),
                $self.context.affinity(),
            )
            .await;

        let response = match response_result {
            Ok(mut response) => {
                $self
                    .context
                    .precommit_token_tracker
                    .update(response.precommit_token.take());
                if is_starting {
                    guard.disarm();
                    let service_error = response.check_service_error();
                    let transaction_id = $extract_id(&response);

                    if let Some(ref error) = service_error {
                        $self.context.record_if_aborted(error);
                    }

                    match (transaction_id, service_error) {
                        (Some(id), _) => {
                            $self.context.transaction_selector.update(id, None)?;
                        }
                        (None, Some(error)) => {
                            $self
                                .context
                                .transaction_selector
                                .set_failed(&error, $self.context.client.is_emulator());
                        }
                        (None, None) => {
                            let error =
                                internal_error("Transaction ID was not returned by Spanner");
                            $self
                                .context
                                .transaction_selector
                                .set_failed(&error, $self.context.client.is_emulator());
                            return Err(error);
                        }
                    }
                }
                response
            }
            Err(error) => {
                $self.context.record_if_aborted(&error);
                if is_starting {
                    guard.disarm();
                    $self
                        .context
                        .transaction_selector
                        .set_failed(&error, $self.context.client.is_emulator());
                }
                return Err(error);
            }
        };

        response
    }};
}

/// RAII drop guard for read-write transactions.
///
/// When a [`ReadWriteTransaction`] is dropped without being committed or rolled back
/// (for example, due to async cancellation via `tokio::time::timeout`, task abortion,
/// early return, or panic), this guard releases held resources:
/// 1. Synchronously releases client-side channel pool active RW guard accounting, allowing
///    draining channels to close while preserving the pinned channel for routing.
/// 2. Cleans up the transaction ID entry in [`LocationRouter`]'s affinity table.
/// 3. Spawns a best-effort background task on the Tokio runtime to issue a [`RollbackRequest`]
///    to Cloud Spanner, releasing server-side pessimistic locks immediately instead of holding
///    them until Spanner's lock wait timeout expires (10 seconds).
///
/// # Two Kinds of Affinity in Cloud Spanner Client
/// The Spanner client utilizes the term "affinity" in two completely distinct mechanisms
/// with different scopes, purposes, and lifecycles:
///
/// 1. **Channel Pool Affinity** ([`TransactionAffinity`]):
///    - **Scope**: Local client-side gRPC connection pool ([`ChannelPool`][crate::channel_pool::ChannelPool]).
///    - **Purpose**: Pins all gRPC requests of a transaction to the exact same physical HTTP/2 connection.
///    - **Lifecycle**: Managed by `RwTransactionAffinityGuard`. Automatically decremented when the guard
///      drops, or explicitly via `release_rw_guard()` on commit/rollback. In
///      [`TransactionRunner`][crate::transaction_runner::TransactionRunner], each retry attempt
///      creates a fresh channel pool affinity handle to allow retries to avoid degraded channels.
///
/// 2. **Location-Aware Routing Affinity** ([`LocationRouter`]):
///    - **Scope**: Remote Spanner backend server topology.
///    - **Purpose**: Maps a Spanner transaction ID ([`bytes::Bytes`]) to a specific remote Spanner tablet
///      endpoint ([`ServerConnection`][crate::routing::connection_cache::ServerConnection]), ensuring all
///      subsequent statements route directly to the tablet leader rather than re-resolving key ranges.
///    - **Lifecycle**: Recorded when the transaction begins on a specific tablet. Cleared when the transaction
///      completes (commit, rollback, abort, or drop). Each retry attempt gets a new transaction ID from Spanner,
///      so location routing affinity is strictly scoped to a single transaction ID.
#[derive(Debug)]
struct ReadWriteTransactionDropGuard {
    client: DatabaseClient,
    session_name: String,
    selector: ReadContextTransactionSelector,
    affinity: Option<Arc<TransactionAffinity>>,
    rollback_gax_options: Option<RequestOptions>,
    tokio_handle: Option<Handle>,
    disarmed: AtomicBool,
}

impl ReadWriteTransactionDropGuard {
    fn new(
        client: DatabaseClient,
        session_name: String,
        selector: ReadContextTransactionSelector,
        affinity: Option<Arc<TransactionAffinity>>,
        rollback_gax_options: Option<RequestOptions>,
        tokio_handle: Option<Handle>,
    ) -> Self {
        Self {
            client,
            session_name,
            selector,
            affinity,
            rollback_gax_options,
            tokio_handle,
            disarmed: AtomicBool::new(false),
        }
    }

    fn disarm(&self) {
        self.disarmed.store(true, Ordering::Release);
    }
}

struct AffinityCleanupGuard {
    client: DatabaseClient,
    transaction_id: Bytes,
}

impl Drop for AffinityCleanupGuard {
    fn drop(&mut self) {
        self.client
            .clear_transaction_affinity_routing(Some(self.transaction_id.as_ref()));
    }
}

impl Drop for ReadWriteTransactionDropGuard {
    fn drop(&mut self) {
        // Synchronously release client-side channel pool active RW guard accounting,
        // allowing draining channels to close while preserving the pinned entry ID for routing.
        // This runs unconditionally, even if the transaction never established a transaction ID
        // (for example, if the initial statement failed or was cancelled before returning an ID).
        if let Some(affinity) = &self.affinity {
            affinity.release_rw_guard();
        }

        let Ok(Some(transaction_id)) = self.selector.get_id_no_wait() else {
            return;
        };

        if *self.disarmed.get_mut() {
            // Rollback was disarmed (e.g. committed, explicitly rolled back, or aborted).
            // Location router affinity is cleaned up here if not already cleared.
            self.client
                .clear_transaction_affinity_routing(Some(transaction_id.as_ref()));
            return;
        }

        // We only reach this point if the transaction was not committed,
        // rolled back, or aborted.
        let handle = self
            .tokio_handle
            .take()
            .or_else(|| Handle::try_current().ok());
        let Some(handle) = handle else {
            self.client
                .clear_transaction_affinity_routing(Some(transaction_id.as_ref()));
            return;
        };

        let client = self.client.clone();
        let session_name = take(&mut self.session_name);
        let affinity = self.affinity.take();
        let mut gax_options = self.rollback_gax_options.take().unwrap_or_default();
        amend_gax_options_for_rollback(client.leader_aware_routing_enabled, &mut gax_options);

        let cleanup_guard = AffinityCleanupGuard {
            client: client.clone(),
            transaction_id,
        };

        let _ = catch_unwind(AssertUnwindSafe(move || {
            handle.spawn(async move {
                let _cleanup_guard = cleanup_guard;
                let request = RollbackRequest::default()
                    .set_session(session_name)
                    .set_transaction_id(_cleanup_guard.transaction_id.clone());
                // When location-aware routing is enabled, client.rollback calls pre_route_rollback
                // (which routes to the tablet holding the locks using the transaction affinity)
                // and post_route_rollback (which clears the transaction affinity upon completion).
                // Channel pool affinity ensures routing over the same pinned channel.
                let target = affinity.as_deref();
                if let Err(error) = client.rollback(request, gax_options, target).await {
                    debug!(%error, "Background rollback on drop failed");
                }
            });
        }));
    }
}

/// A read-write transaction.
#[derive(Clone, Debug)]
pub struct ReadWriteTransaction {
    pub(crate) context: ReadContext,
    pub(crate) deadline: Option<Instant>,
    seqno: Arc<AtomicI64>,
    max_commit_delay: Option<Duration>,
    return_commit_stats: bool,
    commit_priority: Priority,
    mutations: Arc<Mutex<Vec<ProtoMutation>>>,
    begin_gax_options: Option<crate::RequestOptions>,
    commit_gax_options: Option<crate::RequestOptions>,
    drop_guard: Arc<ReadWriteTransactionDropGuard>,
}

impl ReadWriteTransaction {
    /// Buffers one or more mutations to be applied when the transaction commits.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::Spanner;
    /// # use google_cloud_spanner::mutation::Mutation;
    /// # async fn sample(spanner: Spanner) -> Result<(), google_cloud_spanner::Error> {
    /// let db_client = spanner.database_client("projects/p/instances/i/databases/d").build().await?;
    /// let runner = db_client.read_write_transaction().build();
    /// runner.run(async |tx| {
    ///     let mutation = Mutation::new_insert_builder("users")
    ///         .set("id").to(&1)
    ///         .build();
    ///     tx.buffer([mutation])?;
    ///     Ok(())
    /// }).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn buffer<I>(&self, mutations: I) -> crate::Result<()>
    where
        I: IntoIterator<Item = Mutation>,
    {
        let mut guard = self.lock_mutations();
        for mutation in mutations {
            guard.push(mutation.build_proto());
        }
        Ok(())
    }

    /// Executes a query using this transaction.
    pub async fn execute_query<T: Into<Statement>>(
        &self,
        statement: T,
    ) -> crate::Result<ResultSet> {
        let stmt = statement.into();
        let mut gax_options = stmt.gax_options().clone();
        self.amend_gax_options(&mut gax_options);
        let stmt = stmt.with_gax_options(gax_options);
        let seqno = self.seqno.fetch_add(1, Ordering::SeqCst);
        self.context.execute_query(stmt, Some(seqno)).await
    }

    /// Reads rows from the database using key lookups and scans, as a simple key/value style alternative to execute_query.
    pub async fn execute_read<T: Into<crate::read::ReadRequest>>(
        &self,
        read: T,
    ) -> crate::Result<ResultSet> {
        let mut req = read.into();
        self.amend_gax_options(&mut req.gax_options);
        self.context.execute_read(req).await
    }

    /// Executes an update using this transaction.
    pub async fn execute_update<T: Into<Statement>>(&self, statement: T) -> crate::Result<i64> {
        if let Some(info) = self.aborted_info() {
            return Err(info.to_error());
        }
        let statement = statement.into();
        let mut gax_options = statement.gax_options().clone();
        self.amend_gax_options(&mut gax_options);
        let seqno = self.seqno.fetch_add(1, Ordering::SeqCst);
        let selector = self.context.selector().await?;
        let mut request = statement
            .into_request()
            .set_session(self.context.session_name.clone())
            .set_transaction(selector)
            .set_seqno(seqno);
        request.request_options = self.context.amend_request_options(request.request_options);

        let response = execute_with_retry!(
            self,
            request,
            gax_options,
            execute_sql,
            |response: &crate::model::ResultSet| {
                response
                    .metadata
                    .as_ref()
                    .and_then(|md| md.transaction.as_ref())
                    .map(|t| t.id.clone())
                    .filter(|id| !id.is_empty())
            }
        );

        let stats = response
            .stats
            .ok_or_else(|| internal_error("No stats returned"))?;
        match stats.row_count {
            Some(RowCount::RowCountExact(c)) => Ok(c),
            _ => Err(internal_error(
                "ExecuteSql returned an invalid or missing row count type for a read/write transaction",
            )),
        }
    }

    /// Executes a batch of DML statements using this transaction.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::Spanner;
    /// # use google_cloud_spanner::statement::Statement;
    /// # use google_cloud_spanner::batch::BatchDml;
    /// # async fn build(spanner: Spanner) -> Result<(), google_cloud_spanner::Error> {
    /// let db_client = spanner.database_client("projects/p/instances/i/databases/d").build().await?;
    /// let runner = db_client.read_write_transaction().build();
    /// let result = runner.run(async |transaction| {
    ///     let statement1 = Statement::builder("UPDATE users SET active = true WHERE id = @id")
    ///         .add_param("id", &1)
    ///         .build();
    ///     let statement2 = Statement::builder("UPDATE users SET active = true WHERE id = @id")
    ///         .add_param("id", &2)
    ///         .build();
    ///     let batch = BatchDml::builder()
    ///         .add_statement(statement1)
    ///         .add_statement(statement2)
    ///         .build();
    ///     let update_counts = transaction.execute_batch_update(batch).await?;
    ///     Ok(())
    /// }).await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// If a `BatchDml` request fails halfway through execution, `execute_batch_update` will return a
    /// `BatchUpdateError` indicating exactly which statements succeeded (and their respective update counts)
    /// before the batch execution failed.
    ///
    /// # Error Handling Example
    /// ```
    /// # use google_cloud_spanner::client::Spanner;
    /// # use google_cloud_spanner::statement::Statement;
    /// # use google_cloud_spanner::batch::BatchDml;
    /// # use google_cloud_spanner::error::BatchUpdateError;
    /// # async fn build(spanner: Spanner) -> Result<(), google_cloud_spanner::Error> {
    /// # let db_client = spanner.database_client("projects/p/instances/i/databases/d").build().await?;
    /// # let runner = db_client.read_write_transaction().build();
    /// # let result = runner.run(async |transaction| {
    /// let statement1 = Statement::builder("UPDATE users SET active = true WHERE id = 1").build();
    /// let statement2 = Statement::builder("UPDATE non_existent_table SET active = true WHERE id = 2").build();
    ///
    /// let batch = BatchDml::builder()
    ///     .add_statement(statement1)
    ///     .add_statement(statement2)
    ///     .build();
    ///
    /// match transaction.execute_batch_update(batch).await {
    ///     Ok(update_counts) => {
    ///         println!("All statements succeeded. Update counts: {:?}", update_counts);
    ///     }
    ///     Err(e) => {
    ///         if let Some(batch_error) = BatchUpdateError::extract(&e) {
    ///             println!("Batch execution failed. Successful update counts: {:?}", batch_error.update_counts);
    ///         } else {
    ///             println!("RPC failed or internal error occurred: {}", e);
    ///         }
    ///     }
    /// }
    /// # Ok(())
    /// # }).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn execute_batch_update<T: Into<BatchDml>>(
        &self,
        batch: T,
    ) -> crate::Result<Vec<i64>> {
        if let Some(info) = self.aborted_info() {
            return Err(info.to_error());
        }
        let mut batch = batch.into();
        self.amend_gax_options(&mut batch.gax_options);
        let seqno = self.seqno.fetch_add(1, Ordering::SeqCst);

        let statements: Vec<ExecuteBatchDmlStatement> = batch
            .statements
            .into_iter()
            .map(|stmt: crate::statement::Statement| stmt.into_batch_statement())
            .collect();

        let selector = self.context.selector().await?;

        let request = ExecuteBatchDmlRequest::default()
            .set_session(self.context.session_name.clone())
            .set_transaction(selector)
            .set_seqno(seqno)
            .set_statements(statements)
            .set_or_clear_request_options(self.context.amend_request_options(batch.request_options))
            .set_last_statements(batch.last_statements);

        let response = execute_with_retry!(
            self,
            request,
            batch.gax_options,
            execute_batch_dml,
            |response: &crate::model::ExecuteBatchDmlResponse| {
                response
                    .result_sets
                    .first()
                    .and_then(|rs| rs.metadata.as_ref())
                    .and_then(|md| md.transaction.as_ref())
                    .map(|t| t.id.clone())
                    .filter(|id| !id.is_empty())
            }
        );

        process_response(response).inspect_err(|error| {
            self.context.record_if_aborted(error);
        })
    }

    pub(crate) async fn begin_explicitly_if_not_started(
        &self,
        mutation_key: Option<crate::model::Mutation>,
    ) -> crate::Result<bool> {
        let mut begin_options = self.begin_gax_options.clone().unwrap_or_default();
        self.amend_gax_options(&mut begin_options);
        self.context
            .begin_explicitly_if_not_started(begin_options, mutation_key)
            .await
    }

    pub(crate) fn is_starting(&self) -> bool {
        self.context.transaction_selector.is_starting()
    }

    pub(crate) fn aborted_info(&self) -> Option<&AbortedInfo> {
        self.context.aborted_info()
    }

    /// Acquires an exclusive mutex lock on the buffered mutations, recovering from lock poisoning via `into_inner()`.
    ///
    /// # Poison Recovery Rationale
    /// `mutations` protects the in-memory buffered mutation list (`Vec<ProtoMutation>`). Standard
    /// library `Vec` collections remain memory-safe and structurally sound in Rust even if a thread
    /// panicked while appending or clearing mutations. Recovering the guard via `into_inner()` prevents
    /// an isolated panic in a caller task from permanently disabling mutation buffering, allowing
    /// subsequent statements, error handlers, or rollback routines to inspect or clear the transaction
    /// state cleanly without crashing.
    fn lock_mutations(&self) -> MutexGuard<'_, Vec<ProtoMutation>> {
        match self.mutations.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    fn commit_request_options(&self) -> Option<crate::model::RequestOptions> {
        let mut options = self.context.amend_request_options(None);
        if self.commit_priority != Priority::Unspecified {
            options
                .get_or_insert_with(crate::model::RequestOptions::default)
                .priority = self.commit_priority.clone();
        }
        options
    }

    fn build_commit_request(
        &self,
        transaction_id: Bytes,
        mutations: Vec<ProtoMutation>,
        precommit_token: Option<MultiplexedSessionPrecommitToken>,
    ) -> CommitRequest {
        create_commit_request(
            self.context.session_name.clone(),
            transaction_id,
            mutations,
            precommit_token,
            self.commit_request_options(),
            self.max_commit_delay,
            self.return_commit_stats,
        )
    }

    /// Disarms the automatic rollback-on-drop guard.
    pub(crate) fn disarm_rollback(&self) {
        self.drop_guard.disarm();
    }

    /// Commits the transaction.
    pub(crate) async fn commit(&self) -> Result<CommitResponse> {
        let result = self.commit_internal().await;
        // Release client-side channel pool affinity lease.
        // Location-aware routing affinity is cleared separately in
        // DatabaseClient::post_route_commit upon commit completion.
        if let Some(affinity) = self.affinity() {
            affinity.release_rw_guard();
        }
        result
    }

    async fn commit_internal(&self) -> Result<CommitResponse> {
        if let Some(info) = self.aborted_info() {
            return Err(info.to_error());
        }
        self.context.transaction_selector.check_failed()?;
        let mutations = take(&mut *self.lock_mutations());
        let mut id = self.context.transaction_selector.get_id_no_wait()?;
        if id.is_none() {
            if self.is_starting() {
                return Err(internal_error(
                    "Commit called while an asynchronous statement is still starting the transaction",
                ));
            }
            let mutation_key = Mutation::select_mutation_key(&mutations);
            if self.begin_explicitly_if_not_started(mutation_key).await? {
                self.context.transaction_selector.check_failed()?;
                id = self.context.transaction_selector.get_id_no_wait()?;
            }
        }
        let transaction_id = id.ok_or_else(|| internal_error("Transaction ID is missing"))?;
        let precommit_token = self.context.precommit_token_tracker.get();

        let request = self.build_commit_request(transaction_id.clone(), mutations, precommit_token);

        let mut gax_options = self.commit_gax_options.clone().unwrap_or_default();
        self.amend_gax_options(&mut gax_options);

        // Disarm rollback immediately before sending the commit RPC.
        // Once commit is dispatched, the transaction must not be rolled back.
        self.disarm_rollback();

        let response = self
            .context
            .client
            .commit(request, gax_options, self.affinity())
            .await
            .inspect_err(|error| self.context.record_if_aborted(error))?;

        let response = if let Some(MultiplexedSessionRetry::PrecommitToken(new_precommit_token)) =
            response.multiplexed_session_retry
        {
            self.context
                .precommit_token_tracker
                .update(Some(*new_precommit_token));
            let retry_commit_req = self.build_commit_request(
                transaction_id,
                Vec::new(), // mutations are never re-sent in retry requests
                self.context.precommit_token_tracker.get(),
            );

            let mut gax_options = self.commit_gax_options.clone().unwrap_or_default();
            self.amend_gax_options(&mut gax_options);

            self.context
                .client
                .commit(retry_commit_req, gax_options, self.affinity())
                .await
                .inspect_err(|error| self.context.record_if_aborted(error))?
        } else {
            response
        };

        Ok(response)
    }

    /// Rolls back the transaction.
    pub(crate) async fn rollback(&self) -> Result<()> {
        let result = self.rollback_internal().await;
        // Release client-side channel pool affinity lease.
        // Location-aware routing affinity is cleared separately in
        // DatabaseClient::post_route_rollback upon rollback completion.
        if let Some(affinity) = self.affinity() {
            affinity.release_rw_guard();
        }
        result
    }

    async fn rollback_internal(&self) -> Result<()> {
        let Some(transaction_id) = self.context.transaction_selector.get_id_no_wait()? else {
            return Ok(());
        };

        // Disarm rollback once a transaction ID has been observed and rollback is dispatched.
        self.disarm_rollback();

        let request = RollbackRequest::default()
            .set_session(self.context.session_name.clone())
            .set_transaction_id(transaction_id);

        let mut gax_options = self
            .drop_guard
            .rollback_gax_options
            .clone()
            .unwrap_or_default();
        amend_gax_options_for_rollback(
            self.context.client.leader_aware_routing_enabled,
            &mut gax_options,
        );

        self.context
            .client
            .rollback(request, gax_options, self.affinity())
            .await?;

        Ok(())
    }

    fn amend_gax_options(&self, options: &mut GaxRequestOptions) {
        amend_gax_options(
            self.context.client.leader_aware_routing_enabled,
            self.deadline,
            options,
        );
    }

    /// Returns a reference to the transaction channel affinity handle, if set.
    #[allow(dead_code)]
    pub(crate) fn affinity(&self) -> Option<&TransactionAffinity> {
        self.context.affinity()
    }
}

pub(crate) const DEFAULT_ROLLBACK_TIMEOUT: Duration = Duration::from_secs(30);

pub(crate) fn amend_gax_options_for_rollback(
    leader_aware_routing_enabled: bool,
    options: &mut GaxRequestOptions,
) {
    if options.idempotent().is_none() {
        options.set_idempotency(true);
    }
    let timeout = (*options.attempt_timeout()).unwrap_or(DEFAULT_ROLLBACK_TIMEOUT);
    let rollback_deadline = Instant::now() + DEFAULT_ROLLBACK_TIMEOUT.max(timeout);
    amend_gax_options(
        leader_aware_routing_enabled,
        Some(rollback_deadline),
        options,
    );
}

pub(crate) fn amend_gax_options(
    leader_aware_routing_enabled: bool,
    deadline: Option<Instant>,
    options: &mut GaxRequestOptions,
) {
    if let Some(deadline) = deadline {
        let remaining = deadline.saturating_duration_since(Instant::now());
        let attempt_timeout = match options.attempt_timeout() {
            Some(custom_timeout) => std::cmp::min(*custom_timeout, remaining),
            None => remaining,
        };
        options.set_attempt_timeout(attempt_timeout);

        let inner_policy = options
            .retry_policy()
            .clone()
            .unwrap_or_else(|| Arc::new(SpannerRetryPolicy::new()));
        let bounded_policy = TransactionBoundedRetryPolicy {
            inner: inner_policy,
            deadline,
        };
        options.set_retry_policy(bounded_policy);
    }
    *options = amend_request_options_for_lar(leader_aware_routing_enabled, take(options));
}

/// A retry policy that wraps another policy and bounds the total execution time
/// by a specific transaction deadline.
///
/// This policy delegates `on_error` to the inner policy but overrides `remaining_time`
/// to ensure that it never exceeds the time left until the transaction deadline.
#[derive(Debug)]
struct TransactionBoundedRetryPolicy {
    inner: Arc<dyn RetryPolicy>,
    deadline: Instant,
}

impl RetryPolicy for TransactionBoundedRetryPolicy {
    fn on_error(&self, state: &RetryState, error: GaxError) -> RetryResult {
        if Instant::now() >= self.deadline {
            return RetryResult::Exhausted(error);
        }
        self.inner.on_error(state, error)
    }

    fn on_throttle(&self, state: &RetryState, error: GaxError) -> ThrottleResult {
        if Instant::now() >= self.deadline {
            return ThrottleResult::Exhausted(error);
        }
        self.inner.on_throttle(state, error)
    }

    fn remaining_time(&self, state: &RetryState) -> Option<Duration> {
        let remaining = self.deadline.saturating_duration_since(Instant::now());
        let attempt_timeout = self
            .inner
            .remaining_time(state)
            .map(|inner| min(remaining, inner))
            .unwrap_or(remaining);
        Some(attempt_timeout)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::{Spanner, SpannerBuilderExt};
    use crate::error::BatchUpdateError;
    use crate::key;
    use crate::key::KeySet;
    use crate::read::ReadRequest;
    use crate::read_only_transaction::tests::{create_session_mock, setup_db_client};
    use crate::result_set::tests::{adapt, string_val};
    use crate::transaction_retry_policy::BasicTransactionRetryPolicy;
    use crate::transaction_retry_policy::is_aborted;
    use crate::transaction_retry_policy::tests::create_aborted_status;
    use gaxi::grpc::tonic;
    use gaxi::grpc::tonic::MetadataMap;
    use gaxi::grpc::tonic::{Code as TonicCode, Response, Status as TonicStatus};
    use google_cloud_gax::options::internal::RequestOptionsExt as _;
    use google_cloud_gax::retry_policy::NeverRetry;
    use google_cloud_gax::retry_result::RetryResult;
    use google_cloud_gax::retry_state::RetryState;
    use google_cloud_test_macros::tokio_test_no_panics;
    use http::{HeaderMap, HeaderValue};
    use mockall::Sequence;
    use prost_types::Timestamp;
    use spanner_grpc_mock::MockSpanner;
    use spanner_grpc_mock::google::rpc::Status as RpcStatus;
    use spanner_grpc_mock::google::spanner::v1;
    use spanner_grpc_mock::google::spanner::v1::commit_request::Transaction as CommitTransaction;
    use spanner_grpc_mock::google::spanner::v1::result_set_stats::RowCount;
    use spanner_grpc_mock::google::spanner::v1::struct_type;
    use spanner_grpc_mock::google::spanner::v1::transaction_options::Mode;
    use spanner_grpc_mock::google::spanner::v1::transaction_selector::Selector as ProtoSelector;
    use spanner_grpc_mock::google::spanner::v1::{
        CommitResponse, ExecuteBatchDmlResponse as ProtoExecuteBatchDmlResponse, ExecuteSqlRequest,
        PartialResultSet, ResultSet as ProtoResultSet, ResultSetMetadata, ResultSetStats,
        StructType, Transaction as ProtoTransaction, TransactionSelector, Type, TypeCode,
    };
    use std::fmt::Debug;
    use std::net::SocketAddr;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::time::Duration as StdDuration;
    use tokio::sync::mpsc::Receiver;
    use tokio::task::JoinHandle;

    impl ReadWriteTransaction {
        /// Returns `true` if this transaction encountered an `Aborted` error from Spanner.
        pub(crate) fn is_aborted(&self) -> bool {
            self.aborted_info().is_some()
        }
    }

    #[test]
    fn auto_traits() {
        static_assertions::assert_impl_all!(ReadWriteTransactionBuilder: Send, Sync, Clone, Debug);
        static_assertions::assert_impl_all!(ReadWriteTransaction: Send, Sync, Debug);
    }

    #[tokio_test_no_panics]
    async fn read_write_transaction_commit_retry_explicit() -> anyhow::Result<()> {
        run_read_write_transaction_commit_retry(BeginTransactionOption::ExplicitBegin).await
    }

    #[tokio_test_no_panics]
    async fn read_write_transaction_commit_retry_inline() -> anyhow::Result<()> {
        run_read_write_transaction_commit_retry(BeginTransactionOption::InlineBegin).await
    }

    async fn run_read_write_transaction_commit_retry(
        begin_transaction_option: BeginTransactionOption,
    ) -> anyhow::Result<()> {
        let mut mock = create_session_mock();
        let remotes = Arc::new(Mutex::new(Vec::new()));

        if begin_transaction_option == BeginTransactionOption::ExplicitBegin {
            let remotes_clone = remotes.clone();
            mock.expect_begin_transaction()
                .once()
                .returning(move |req| {
                    remotes_clone
                        .lock()
                        .unwrap()
                        .push(req.remote_addr().expect("remote_addr should be available"));
                    let req = req.into_inner();
                    assert_eq!(
                        req.session,
                        "projects/p/instances/i/databases/d/sessions/123"
                    );
                    Ok(tonic::Response::new(v1::Transaction {
                        id: vec![0, 0, 7],
                        ..Default::default()
                    }))
                });
        }

        // execute_update returns a precommit token.
        let remotes_clone = remotes.clone();
        mock.expect_execute_sql().once().returning(move |req| {
            remotes_clone
                .lock()
                .unwrap()
                .push(req.remote_addr().expect("remote_addr should be available"));
            let req = req.into_inner();
            assert_eq!(req.sql, "UPDATE Users SET Name = 'Bob' WHERE Id = 1");

            if begin_transaction_option == BeginTransactionOption::InlineBegin {
                let transaction = req
                    .transaction
                    .as_ref()
                    .expect("transaction options required for inline begin");
                let selector = transaction.selector.as_ref().expect("selector required");
                assert!(matches!(
                    selector,
                    v1::transaction_selector::Selector::Begin(_)
                ));
            }

            let mut metadata = v1::ResultSetMetadata {
                row_type: Some(v1::StructType { fields: vec![] }),
                ..Default::default()
            };
            if begin_transaction_option == BeginTransactionOption::InlineBegin {
                metadata.transaction = Some(v1::Transaction {
                    id: vec![0, 0, 7],
                    ..Default::default()
                });
            }

            Ok(tonic::Response::new(v1::ResultSet {
                metadata: Some(metadata),
                stats: Some(v1::ResultSetStats {
                    row_count: Some(v1::result_set_stats::RowCount::RowCountExact(1)),
                    ..Default::default()
                }),
                precommit_token: Some(v1::MultiplexedSessionPrecommitToken {
                    precommit_token: vec![101],
                    seq_num: 1,
                }),
                ..Default::default()
            }))
        });

        // Simulate that commit returns a precommit token in the response.
        // This would normally not happen, but we test it here to verify
        // that the commit is retried.
        let remotes_clone = remotes.clone();
        mock.expect_commit().once().returning(move |req| {
            remotes_clone
                .lock()
                .unwrap()
                .push(req.remote_addr().expect("remote_addr should be available"));
            let req = req.into_inner();
            assert_eq!(
                req.precommit_token,
                Some(v1::MultiplexedSessionPrecommitToken {
                    precommit_token: vec![101],
                    seq_num: 1,
                })
            );
            Ok(tonic::Response::new(v1::CommitResponse {
                commit_timestamp: Some(prost_types::Timestamp {
                    seconds: 1000,
                    nanos: 0,
                }),
                multiplexed_session_retry: Some(
                    v1::commit_response::MultiplexedSessionRetry::PrecommitToken(
                        v1::MultiplexedSessionPrecommitToken {
                            precommit_token: vec![202],
                            seq_num: 2,
                        },
                    ),
                ),
                ..Default::default()
            }))
        });

        // Second commit retry is automatically issued with the new token
        let remotes_clone = remotes.clone();
        mock.expect_commit().once().returning(move |req| {
            remotes_clone
                .lock()
                .unwrap()
                .push(req.remote_addr().expect("remote_addr should be available"));
            let req = req.into_inner();
            assert_eq!(
                req.precommit_token,
                Some(v1::MultiplexedSessionPrecommitToken {
                    precommit_token: vec![202],
                    seq_num: 2,
                })
            );
            assert!(
                req.mutations.is_empty(),
                "Expected mutations to be empty in retried CommitRequest"
            );
            Ok(tonic::Response::new(v1::CommitResponse {
                commit_timestamp: Some(prost_types::Timestamp {
                    seconds: 1001,
                    nanos: 0,
                }),
                ..Default::default()
            }))
        });

        let (db_client, _server) = setup_db_client(mock).await;

        let tx = ReadWriteTransactionBuilder::new(db_client.clone())
            .set_begin_transaction_option(begin_transaction_option)
            .build(None)
            .await
            .expect("Failed to build transaction");

        let count = tx
            .execute_update("UPDATE Users SET Name = 'Bob' WHERE Id = 1")
            .await?;
        assert_eq!(count, 1);

        let timestamp = tx.commit().await?;
        assert_eq!(
            timestamp
                .commit_timestamp
                .as_ref()
                .expect("timestamp should be present")
                .seconds(),
            1001
        );

        // Verify that all RPCs used the same channel (same remote address)
        let remotes = remotes.lock().unwrap();
        let expected_rpcs = if begin_transaction_option == BeginTransactionOption::ExplicitBegin {
            4
        } else {
            3
        };
        assert_eq!(
            remotes.len(),
            expected_rpcs,
            "Expected exactly {} RPCs",
            expected_rpcs
        );
        let first = remotes[0];
        for addr in remotes.iter() {
            assert_eq!(*addr, first, "All RPCs should use the same gRPC channel");
        }

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn read_write_transaction_commit_retry_preserves_options() -> anyhow::Result<()> {
        let mut mock = create_session_mock();

        // execute_update returns a precommit token.
        mock.expect_execute_sql().once().returning(move |req| {
            let req = req.into_inner();
            assert_eq!(req.sql, "UPDATE Users SET Name = 'Bob' WHERE Id = 1");

            let mut metadata = v1::ResultSetMetadata {
                row_type: Some(v1::StructType { fields: vec![] }),
                ..Default::default()
            };
            metadata.transaction = Some(v1::Transaction {
                id: vec![0, 0, 7],
                ..Default::default()
            });

            Ok(tonic::Response::new(v1::ResultSet {
                metadata: Some(metadata),
                stats: Some(v1::ResultSetStats {
                    row_count: Some(v1::result_set_stats::RowCount::RowCountExact(1)),
                    ..Default::default()
                }),
                precommit_token: Some(v1::MultiplexedSessionPrecommitToken {
                    precommit_token: vec![101],
                    seq_num: 1,
                }),
                ..Default::default()
            }))
        });

        // Simulate that commit returns a precommit token in the response.
        // This would normally not happen, but we test it here to verify
        // that the commit is retried and the options are preserved.
        let expected_delay = prost_types::Duration {
            seconds: 0,
            nanos: 200_000_000,
        };

        let expected_delay_clone = expected_delay;
        mock.expect_commit().once().returning(move |req| {
            let req = req.into_inner();
            assert_eq!(
                req.precommit_token,
                Some(v1::MultiplexedSessionPrecommitToken {
                    precommit_token: vec![101],
                    seq_num: 1,
                })
            );
            // Assert original options are present
            assert!(
                req.return_commit_stats,
                "Expected return_commit_stats to be true in first commit"
            );
            assert_eq!(
                req.max_commit_delay.as_ref(),
                Some(&expected_delay_clone),
                "Expected max_commit_delay to be set in first commit"
            );

            Ok(tonic::Response::new(v1::CommitResponse {
                commit_timestamp: Some(prost_types::Timestamp {
                    seconds: 1000,
                    nanos: 0,
                }),
                multiplexed_session_retry: Some(
                    v1::commit_response::MultiplexedSessionRetry::PrecommitToken(
                        v1::MultiplexedSessionPrecommitToken {
                            precommit_token: vec![202],
                            seq_num: 2,
                        },
                    ),
                ),
                ..Default::default()
            }))
        });

        // Second commit retry is automatically issued with the new token and MUST preserve original options
        mock.expect_commit().once().returning(move |req| {
            let req = req.into_inner();
            assert_eq!(
                req.precommit_token,
                Some(v1::MultiplexedSessionPrecommitToken {
                    precommit_token: vec![202],
                    seq_num: 2,
                })
            );
            assert!(
                req.return_commit_stats,
                "Expected return_commit_stats to be preserved in retried commit request"
            );
            assert_eq!(
                req.max_commit_delay.as_ref(),
                Some(&expected_delay),
                "Expected max_commit_delay to be preserved in retried commit request"
            );
            assert!(
                req.mutations.is_empty(),
                "Expected mutations to be empty in retried CommitRequest"
            );

            Ok(tonic::Response::new(v1::CommitResponse {
                commit_timestamp: Some(prost_types::Timestamp {
                    seconds: 1001,
                    nanos: 0,
                }),
                ..Default::default()
            }))
        });

        let (db_client, _server) = setup_db_client(mock).await;

        let tx = ReadWriteTransactionBuilder::new(db_client.clone())
            .set_begin_transaction_option(BeginTransactionOption::InlineBegin)
            .set_return_commit_stats(true)
            .set_max_commit_delay(Duration::from_millis(200))
            .build(None)
            .await
            .expect("Failed to build transaction");

        let count = tx
            .execute_update("UPDATE Users SET Name = 'Bob' WHERE Id = 1")
            .await?;
        assert_eq!(count, 1);

        let timestamp = tx.commit().await?;
        assert_eq!(
            timestamp
                .commit_timestamp
                .as_ref()
                .expect("timestamp should be present")
                .seconds(),
            1001
        );

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn read_write_transaction_commit_carries_commit_priority() -> anyhow::Result<()> {
        let mut mock = create_session_mock();

        mock.expect_execute_sql().once().returning(move |_req| {
            let mut metadata = v1::ResultSetMetadata {
                row_type: Some(v1::StructType { fields: vec![] }),
                ..Default::default()
            };
            metadata.transaction = Some(v1::Transaction {
                id: vec![1, 2, 3],
                ..Default::default()
            });

            Ok(tonic::Response::new(v1::ResultSet {
                metadata: Some(metadata),
                stats: Some(v1::ResultSetStats {
                    row_count: Some(v1::result_set_stats::RowCount::RowCountExact(1)),
                    ..Default::default()
                }),
                ..Default::default()
            }))
        });

        mock.expect_commit().once().returning(|req| {
            let req = req.into_inner();
            let request_options = req
                .request_options
                .expect("Expected request_options in CommitRequest");
            assert_eq!(
                request_options.priority,
                v1::request_options::Priority::Low as i32,
                "Expected priority to be Priority::Low in CommitRequest"
            );

            Ok(tonic::Response::new(v1::CommitResponse {
                commit_timestamp: Some(prost_types::Timestamp {
                    seconds: 123456789,
                    nanos: 0,
                }),
                ..Default::default()
            }))
        });

        let (db_client, _server) = setup_db_client(mock).await;

        let tx = ReadWriteTransactionBuilder::new(db_client.clone())
            .set_begin_transaction_option(BeginTransactionOption::InlineBegin)
            .set_commit_priority(Priority::Low)
            .build(None)
            .await
            .expect("Failed to build transaction");

        let count = tx
            .execute_update("UPDATE Users SET Name = 'Alice' WHERE Id = 1")
            .await?;
        assert_eq!(count, 1);

        let _ = tx.commit().await?;

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn read_write_transaction_execute_update_last_statement() {
        let stmt = Statement::builder("UPDATE Users SET Name = 'Alice' WHERE Id = 1")
            .set_last_statement(true)
            .build();
        run_read_write_transaction_execute_update(BeginTransactionOption::ExplicitBegin, stmt)
            .await;
    }

    #[tokio_test_no_panics]
    async fn read_write_transaction_execute_update_explicit() {
        run_read_write_transaction_execute_update(
            BeginTransactionOption::ExplicitBegin,
            "UPDATE Users SET Name = 'Alice' WHERE Id = 1",
        )
        .await;
    }

    #[tokio_test_no_panics]
    async fn read_write_transaction_execute_update_inline() {
        run_read_write_transaction_execute_update(
            BeginTransactionOption::InlineBegin,
            "UPDATE Users SET Name = 'Alice' WHERE Id = 1",
        )
        .await;
    }

    async fn run_read_write_transaction_execute_update(
        begin_transaction_option: BeginTransactionOption,
        statement: impl Into<Statement>,
    ) {
        let statement = statement.into();
        let expected_last_statement = statement.last_statement;
        let mut mock = create_session_mock();

        if begin_transaction_option == BeginTransactionOption::ExplicitBegin {
            mock.expect_begin_transaction().once().returning(|req| {
                let req = req.into_inner();
                assert_eq!(
                    req.session,
                    "projects/p/instances/i/databases/d/sessions/123"
                );
                Ok(tonic::Response::new(v1::Transaction {
                    id: vec![1, 2, 3],
                    ..Default::default()
                }))
            });
        }

        mock.expect_execute_sql().once().returning(move |req| {
            let req = req.into_inner();
            assert_eq!(req.sql, "UPDATE Users SET Name = 'Alice' WHERE Id = 1");
            assert_eq!(req.seqno, 1);
            assert_eq!(req.last_statement, expected_last_statement);

            if begin_transaction_option == BeginTransactionOption::InlineBegin {
                let transaction = req
                    .transaction
                    .as_ref()
                    .expect("transaction options required for inline begin");
                let selector = transaction.selector.as_ref().expect("selector required");
                assert!(matches!(
                    selector,
                    v1::transaction_selector::Selector::Begin(_)
                ));
            }

            let mut metadata = v1::ResultSetMetadata {
                row_type: Some(v1::StructType { fields: vec![] }),
                ..Default::default()
            };
            if begin_transaction_option == BeginTransactionOption::InlineBegin {
                metadata.transaction = Some(v1::Transaction {
                    id: vec![1, 2, 3],
                    ..Default::default()
                });
            }

            Ok(tonic::Response::new(v1::ResultSet {
                metadata: Some(metadata),
                stats: Some(v1::ResultSetStats {
                    row_count: Some(v1::result_set_stats::RowCount::RowCountExact(1)),
                    ..Default::default()
                }),
                ..Default::default()
            }))
        });

        mock.expect_commit().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(
                req.session,
                "projects/p/instances/i/databases/d/sessions/123"
            );
            assert_eq!(
                req.transaction,
                Some(v1::commit_request::Transaction::TransactionId(vec![
                    1, 2, 3
                ]))
            );
            Ok(tonic::Response::new(v1::CommitResponse {
                commit_timestamp: Some(prost_types::Timestamp {
                    seconds: 123456789,
                    nanos: 0,
                }),
                ..Default::default()
            }))
        });

        let (db_client, _server) = setup_db_client(mock).await;

        let tx = ReadWriteTransactionBuilder::new(db_client.clone())
            .set_begin_transaction_option(begin_transaction_option)
            .build(None)
            .await
            .expect("Failed to build transaction");
        let count = tx
            .execute_update(statement)
            .await
            .expect("Failed to execute update");
        assert_eq!(count, 1);

        let ts = tx.commit().await.expect("Failed to commit");
        assert_eq!(
            ts.commit_timestamp
                .expect("Commit timestamp should be present")
                .seconds(),
            123456789
        );
    }

    #[tokio_test_no_panics]
    async fn read_write_transaction_execute_update_invalid_stats_explicit() -> anyhow::Result<()> {
        run_read_write_transaction_execute_update_invalid_stats(
            BeginTransactionOption::ExplicitBegin,
        )
        .await
    }

    #[tokio_test_no_panics]
    async fn read_write_transaction_execute_update_invalid_stats_inline() -> anyhow::Result<()> {
        run_read_write_transaction_execute_update_invalid_stats(BeginTransactionOption::InlineBegin)
            .await
    }

    async fn run_read_write_transaction_execute_update_invalid_stats(
        begin_transaction_option: BeginTransactionOption,
    ) -> anyhow::Result<()> {
        let mut mock = create_session_mock();

        if begin_transaction_option == BeginTransactionOption::ExplicitBegin {
            mock.expect_begin_transaction().once().returning(|_| {
                Ok(tonic::Response::new(v1::Transaction {
                    id: vec![1, 2, 3],
                    ..Default::default()
                }))
            });
        }

        mock.expect_execute_sql().once().returning(move |req| {
            let req = req.into_inner();
            if begin_transaction_option == BeginTransactionOption::InlineBegin {
                let transaction = req
                    .transaction
                    .as_ref()
                    .expect("transaction options required for inline begin");
                let selector = transaction.selector.as_ref().expect("selector required");
                assert!(matches!(
                    selector,
                    v1::transaction_selector::Selector::Begin(_)
                ));
            }

            let mut metadata = v1::ResultSetMetadata {
                row_type: Some(v1::StructType { fields: vec![] }),
                ..Default::default()
            };
            if begin_transaction_option == BeginTransactionOption::InlineBegin {
                metadata.transaction = Some(v1::Transaction {
                    id: vec![1, 2, 3],
                    ..Default::default()
                });
            }

            Ok(tonic::Response::new(v1::ResultSet {
                metadata: Some(metadata),
                stats: Some(v1::ResultSetStats {
                    row_count: Some(v1::result_set_stats::RowCount::RowCountLowerBound(1)),
                    ..Default::default()
                }),
                ..Default::default()
            }))
        });

        let (db_client, _server) = setup_db_client(mock).await;

        let tx = ReadWriteTransactionBuilder::new(db_client.clone())
            .set_begin_transaction_option(begin_transaction_option)
            .build(None)
            .await
            .expect("Failed to build transaction");

        let result = tx
            .execute_update("UPDATE Users SET Name = 'Alice' WHERE Id = 1")
            .await;

        let err = result.expect_err("Expected an error for invalid row count stats");
        assert!(
            format!("{:?}", err).contains("invalid or missing row count type"),
            "Error did not contain expected message: {:?}",
            err
        );
        Ok(())
    }

    #[tokio_test_no_panics]
    async fn read_write_transaction_rollback_explicit() -> anyhow::Result<()> {
        run_read_write_transaction_rollback(BeginTransactionOption::ExplicitBegin).await
    }

    #[tokio_test_no_panics]
    async fn read_write_transaction_rollback_inline() -> anyhow::Result<()> {
        run_read_write_transaction_rollback(BeginTransactionOption::InlineBegin).await
    }

    async fn run_read_write_transaction_rollback(
        begin_transaction_option: BeginTransactionOption,
    ) -> anyhow::Result<()> {
        let mut mock = create_session_mock();
        let transaction_id = vec![9, 9, 9];

        if begin_transaction_option == BeginTransactionOption::ExplicitBegin {
            let id = transaction_id.clone();
            mock.expect_begin_transaction().once().returning(move |_| {
                Ok(tonic::Response::new(v1::Transaction {
                    id: id.clone(),
                    ..Default::default()
                }))
            });
        } else {
            let id = transaction_id.clone();
            mock.expect_execute_sql().once().returning(move |req| {
                let req = req.into_inner();
                let transaction = req
                    .transaction
                    .as_ref()
                    .expect("transaction options required for inline begin");
                let selector = transaction.selector.as_ref().expect("selector required");
                assert!(matches!(
                    selector,
                    v1::transaction_selector::Selector::Begin(_)
                ));

                Ok(tonic::Response::new(v1::ResultSet {
                    metadata: Some(v1::ResultSetMetadata {
                        transaction: Some(v1::Transaction {
                            id: id.clone(),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    stats: Some(v1::ResultSetStats {
                        row_count: Some(v1::result_set_stats::RowCount::RowCountExact(1)),
                        ..Default::default()
                    }),
                    ..Default::default()
                }))
            });
        }

        let id = transaction_id.clone();
        mock.expect_rollback().once().returning(move |req| {
            let req = req.into_inner();
            assert_eq!(
                req.session,
                "projects/p/instances/i/databases/d/sessions/123"
            );
            assert_eq!(req.transaction_id, id);
            Ok(tonic::Response::new(()))
        });

        let (db_client, _server) = setup_db_client(mock).await;

        let tx = ReadWriteTransactionBuilder::new(db_client.clone())
            .set_begin_transaction_option(begin_transaction_option)
            .build(None)
            .await?;

        if begin_transaction_option == BeginTransactionOption::InlineBegin {
            tx.execute_update("UPDATE Users SET Name = 'Alice' WHERE Id = 1")
                .await
                .expect("Failed to execute update");
        }

        tx.rollback().await?;
        Ok(())
    }

    #[tokio_test_no_panics]
    async fn read_write_transaction_execute_batch_update_last_statements() -> anyhow::Result<()> {
        let batch = BatchDml::builder()
            .add_statement("UPDATE Users SET Name = 'Alice' WHERE Id = 1")
            .add_statement("UPDATE Users SET Name = 'Bob' WHERE Id = 2")
            .set_last_statements(true)
            .build();
        run_read_write_transaction_execute_batch_update(
            BeginTransactionOption::ExplicitBegin,
            batch,
        )
        .await
    }

    #[tokio_test_no_panics]
    async fn read_write_transaction_execute_batch_update_explicit() -> anyhow::Result<()> {
        let batch = BatchDml::builder()
            .add_statement("UPDATE Users SET Name = 'Alice' WHERE Id = 1")
            .add_statement("UPDATE Users SET Name = 'Bob' WHERE Id = 2")
            .build();
        run_read_write_transaction_execute_batch_update(
            BeginTransactionOption::ExplicitBegin,
            batch,
        )
        .await
    }

    #[tokio_test_no_panics]
    async fn read_write_transaction_execute_batch_update_inline() -> anyhow::Result<()> {
        let batch = BatchDml::builder()
            .add_statement("UPDATE Users SET Name = 'Alice' WHERE Id = 1")
            .add_statement("UPDATE Users SET Name = 'Bob' WHERE Id = 2")
            .build();
        run_read_write_transaction_execute_batch_update(BeginTransactionOption::InlineBegin, batch)
            .await
    }

    #[tokio_test_no_panics]
    async fn read_write_transaction_execute_batch_update_vec() -> anyhow::Result<()> {
        let statements = vec![
            "UPDATE Users SET Name = 'Alice' WHERE Id = 1",
            "UPDATE Users SET Name = 'Bob' WHERE Id = 2",
        ];
        run_read_write_transaction_execute_batch_update(
            BeginTransactionOption::InlineBegin,
            statements,
        )
        .await
    }

    #[tokio_test_no_panics]
    async fn read_write_transaction_execute_batch_update_vec_statement() -> anyhow::Result<()> {
        let statement1 = Statement::builder("UPDATE Users SET Name = 'Alice' WHERE Id = 1").build();
        let statement2 = Statement::builder("UPDATE Users SET Name = 'Bob' WHERE Id = 2").build();
        let statements = vec![statement1, statement2];
        run_read_write_transaction_execute_batch_update(
            BeginTransactionOption::InlineBegin,
            statements,
        )
        .await
    }

    async fn run_read_write_transaction_execute_batch_update(
        begin_transaction_option: BeginTransactionOption,
        batch: impl Into<BatchDml>,
    ) -> anyhow::Result<()> {
        let batch = batch.into();
        let expected_last_statements = batch.last_statements;
        let mut mock = create_session_mock();

        if begin_transaction_option == BeginTransactionOption::ExplicitBegin {
            mock.expect_begin_transaction().once().returning(|_| {
                Ok(tonic::Response::new(v1::Transaction {
                    id: vec![4, 5, 6],
                    ..Default::default()
                }))
            });
        }

        mock.expect_execute_batch_dml()
            .once()
            .returning(move |req| {
                let req = req.into_inner();
                assert_eq!(req.last_statements, expected_last_statements);
                assert_eq!(req.statements.len(), 2);
                assert_eq!(
                    req.statements[0].sql,
                    "UPDATE Users SET Name = 'Alice' WHERE Id = 1"
                );
                assert_eq!(
                    req.statements[1].sql,
                    "UPDATE Users SET Name = 'Bob' WHERE Id = 2"
                );

                if begin_transaction_option == BeginTransactionOption::InlineBegin {
                    let selector = req
                        .transaction
                        .expect("missing transaction selector")
                        .selector
                        .expect("missing selector");
                    assert!(matches!(
                        selector,
                        v1::transaction_selector::Selector::Begin(_)
                    ));
                }

                let mut metadata = v1::ResultSetMetadata {
                    ..Default::default()
                };
                if begin_transaction_option == BeginTransactionOption::InlineBegin {
                    metadata.transaction = Some(v1::Transaction {
                        id: vec![4, 5, 6],
                        ..Default::default()
                    });
                }

                Ok(tonic::Response::new(v1::ExecuteBatchDmlResponse {
                    result_sets: vec![
                        v1::ResultSet {
                            metadata: Some(metadata),
                            stats: Some(v1::ResultSetStats {
                                row_count: Some(v1::result_set_stats::RowCount::RowCountExact(1)),
                                ..Default::default()
                            }),
                            ..Default::default()
                        },
                        v1::ResultSet {
                            stats: Some(v1::ResultSetStats {
                                row_count: Some(v1::result_set_stats::RowCount::RowCountExact(1)),
                                ..Default::default()
                            }),
                            ..Default::default()
                        },
                    ],
                    status: Some(spanner_grpc_mock::google::rpc::Status {
                        code: 0,
                        message: "OK".into(),
                        details: vec![],
                    }),
                    ..Default::default()
                }))
            });

        let (db_client, _server) = setup_db_client(mock).await;

        let transaction = ReadWriteTransactionBuilder::new(db_client)
            .set_begin_transaction_option(begin_transaction_option)
            .build(None)
            .await?;

        let counts = transaction.execute_batch_update(batch).await?;

        assert_eq!(counts, vec![1, 1]);
        Ok(())
    }

    #[tokio_test_no_panics]
    async fn read_write_transaction_execute_batch_update_partial_failure_explicit()
    -> anyhow::Result<()> {
        run_read_write_transaction_execute_batch_update_partial_failure(
            BeginTransactionOption::ExplicitBegin,
        )
        .await
    }

    #[tokio_test_no_panics]
    async fn read_write_transaction_execute_batch_update_partial_failure_inline()
    -> anyhow::Result<()> {
        run_read_write_transaction_execute_batch_update_partial_failure(
            BeginTransactionOption::InlineBegin,
        )
        .await
    }

    async fn run_read_write_transaction_execute_batch_update_partial_failure(
        begin_transaction_option: BeginTransactionOption,
    ) -> anyhow::Result<()> {
        let mut mock = create_session_mock();

        if begin_transaction_option == BeginTransactionOption::ExplicitBegin {
            mock.expect_begin_transaction().once().returning(|_| {
                Ok(tonic::Response::new(v1::Transaction {
                    id: vec![7, 8, 9],
                    ..Default::default()
                }))
            });
        }

        mock.expect_execute_batch_dml()
            .once()
            .returning(move |req| {
                let req = req.into_inner();
                if begin_transaction_option == BeginTransactionOption::InlineBegin {
                    let selector = req
                        .transaction
                        .expect("missing transaction selector")
                        .selector
                        .expect("missing selector");
                    assert!(matches!(
                        selector,
                        v1::transaction_selector::Selector::Begin(_)
                    ));
                }

                let mut metadata = v1::ResultSetMetadata {
                    ..Default::default()
                };
                if begin_transaction_option == BeginTransactionOption::InlineBegin {
                    metadata.transaction = Some(v1::Transaction {
                        id: vec![7, 8, 9],
                        ..Default::default()
                    });
                }

                Ok(tonic::Response::new(v1::ExecuteBatchDmlResponse {
                    result_sets: vec![v1::ResultSet {
                        metadata: Some(metadata),
                        stats: Some(v1::ResultSetStats {
                            row_count: Some(v1::result_set_stats::RowCount::RowCountExact(1)),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }],
                    status: Some(spanner_grpc_mock::google::rpc::Status {
                        code: gaxi::grpc::tonic::Code::AlreadyExists as i32,
                        message: "row already exists".into(),
                        details: vec![],
                    }),
                    ..Default::default()
                }))
            });

        let (db_client, _server) = setup_db_client(mock).await;

        let tx = ReadWriteTransactionBuilder::new(db_client)
            .set_begin_transaction_option(begin_transaction_option)
            .build(None)
            .await?;

        let batch = BatchDml::builder()
            .add_statement("UPDATE Users SET Name = 'Alice' WHERE Id = 1")
            .add_statement("INSERT INTO Users (Id) VALUES (2)");

        let res = tx.execute_batch_update(batch.build()).await;

        let err = res.expect_err("expected error");
        use std::error::Error;
        let batch_err = err
            .source()
            .and_then(|e| e.downcast_ref::<BatchUpdateError>())
            .expect("should be BatchUpdateError");
        assert_eq!(batch_err.update_counts, vec![1]);
        assert_eq!(
            batch_err.status.status().expect("status").code,
            (gaxi::grpc::tonic::Code::AlreadyExists as i32).into()
        );
        Ok(())
    }

    #[tokio_test_no_panics]
    async fn read_write_transaction_execute_multiple_updates_explicit() -> anyhow::Result<()> {
        run_read_write_transaction_execute_multiple_updates(BeginTransactionOption::ExplicitBegin)
            .await
    }

    #[tokio_test_no_panics]
    async fn read_write_transaction_execute_multiple_updates_inline() -> anyhow::Result<()> {
        run_read_write_transaction_execute_multiple_updates(BeginTransactionOption::InlineBegin)
            .await
    }

    async fn run_read_write_transaction_execute_multiple_updates(
        begin_transaction_option: BeginTransactionOption,
    ) -> anyhow::Result<()> {
        let mut mock = create_session_mock();

        if begin_transaction_option == BeginTransactionOption::ExplicitBegin {
            mock.expect_begin_transaction().once().returning(|req| {
                let req = req.into_inner();
                assert_eq!(
                    req.session,
                    "projects/p/instances/i/databases/d/sessions/123"
                );
                Ok(tonic::Response::new(v1::Transaction {
                    id: vec![4, 5, 6],
                    ..Default::default()
                }))
            });
        }

        let counter = Arc::new(AtomicI64::new(1));
        mock.expect_execute_sql().times(3).returning(move |req| {
            let req = req.into_inner();
            assert_eq!(req.sql, "UPDATE Users SET Name = 'Alice' WHERE Id = 1");
            let c = counter.fetch_add(1, Ordering::SeqCst);
            assert_eq!(req.seqno, c);

            let mut metadata = v1::ResultSetMetadata {
                ..Default::default()
            };

            if begin_transaction_option == BeginTransactionOption::InlineBegin {
                if c == 1 {
                    let selector = req
                        .transaction
                        .expect("missing transaction selector")
                        .selector
                        .expect("missing selector");
                    assert!(matches!(
                        selector,
                        v1::transaction_selector::Selector::Begin(_)
                    ));
                    metadata.transaction = Some(v1::Transaction {
                        id: vec![4, 5, 6],
                        ..Default::default()
                    });
                } else {
                    let selector = req
                        .transaction
                        .expect("missing transaction selector")
                        .selector
                        .expect("missing selector");
                    match selector {
                        v1::transaction_selector::Selector::Id(id) => {
                            assert_eq!(id, vec![4, 5, 6]);
                        }
                        _ => panic!("Expected Selector::Id"),
                    }
                }
            }

            Ok(tonic::Response::new(v1::ResultSet {
                metadata: Some(metadata),
                stats: Some(v1::ResultSetStats {
                    row_count: Some(v1::result_set_stats::RowCount::RowCountExact(1)),
                    ..Default::default()
                }),
                ..Default::default()
            }))
        });

        let (db_client, _server) = setup_db_client(mock).await;

        let tx = ReadWriteTransactionBuilder::new(db_client.clone())
            .set_begin_transaction_option(begin_transaction_option)
            .build(None)
            .await
            .expect("Failed to build transaction");

        for i in 1..=3 {
            let count = tx
                .execute_update("UPDATE Users SET Name = 'Alice' WHERE Id = 1")
                .await
                .unwrap_or_else(|_| panic!("Failed to execute update {}", i));
            assert_eq!(count, 1);
        }
        Ok(())
    }

    #[tokio_test_no_panics]
    async fn read_write_transaction_execute_query() {
        use crate::statement::Statement;
        let mut mock = create_session_mock();

        mock.expect_begin_transaction().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(
                req.session,
                "projects/p/instances/i/databases/d/sessions/123"
            );
            Ok(tonic::Response::new(v1::Transaction {
                id: vec![7, 8, 9],
                ..Default::default()
            }))
        });

        mock.expect_execute_streaming_sql().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(req.sql, "SELECT 1", "SQL statement mismatch");
            // Queries in read/write transactions should always include a sequence number.
            assert_eq!(req.seqno, 1, "ExecuteSqlRequest seqno mismatch");

            assert_eq!(
                req.transaction,
                Some(v1::TransactionSelector {
                    selector: Some(v1::transaction_selector::Selector::Id(vec![7, 8, 9]))
                })
            );

            let prs = v1::PartialResultSet {
                metadata: Some(v1::ResultSetMetadata {
                    row_type: Some(v1::StructType { fields: vec![] }),
                    ..Default::default()
                }),
                ..Default::default()
            };
            Ok(tonic::Response::from(crate::result_set::tests::adapt([
                Ok(prs),
            ])))
        });

        let (db_client, _server) = setup_db_client(mock).await;

        let tx = ReadWriteTransactionBuilder::new(db_client.clone())
            .set_begin_transaction_option(BeginTransactionOption::ExplicitBegin)
            .build(None)
            .await
            .expect("Failed to build transaction");

        let mut rs = tx
            .execute_query(Statement::builder("SELECT 1").build())
            .await
            .expect("Failed to execute query");

        let result = rs.next().await;
        assert!(result.is_none(), "expected None, got empty stream");
    }

    #[tokio_test_no_panics]
    async fn read_write_transaction_with_options() {
        let mut mock = create_session_mock();

        mock.expect_begin_transaction().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(
                req.session,
                "projects/p/instances/i/databases/d/sessions/123"
            );

            let options = req.options.expect("missing transaction options");
            let mode = options.mode.expect("missing mode");
            match mode {
                v1::transaction_options::Mode::ReadWrite(rw) => {
                    assert_eq!(
                        rw.read_lock_mode,
                        v1::transaction_options::read_write::ReadLockMode::Pessimistic as i32
                    );
                }
                _ => panic!("Expected ReadWrite transaction mode"),
            }
            // Ensure isolation level is passed through
            assert_eq!(
                options.isolation_level,
                v1::transaction_options::IsolationLevel::Serializable as i32
            );

            Ok(tonic::Response::new(v1::Transaction {
                id: vec![9, 9, 9],
                ..Default::default()
            }))
        });

        let (db_client, _server) = setup_db_client(mock).await;

        let _tx = ReadWriteTransactionBuilder::new(db_client.clone())
            .set_isolation_level(IsolationLevel::Serializable)
            .set_read_lock_mode(ReadLockMode::Pessimistic)
            .set_begin_transaction_option(BeginTransactionOption::ExplicitBegin)
            .build(None)
            .await
            .expect("Failed to build transaction");
    }

    #[tokio_test_no_panics]
    async fn read_write_transaction_with_exclude_txn_from_change_streams() {
        let mut mock = create_session_mock();

        mock.expect_begin_transaction().once().returning(|req| {
            let req = req.into_inner();
            let options = req.options.expect("missing transaction options");
            assert!(options.exclude_txn_from_change_streams);

            Ok(tonic::Response::new(v1::Transaction {
                id: vec![9, 9, 9],
                ..Default::default()
            }))
        });

        let (db_client, _server) = setup_db_client(mock).await;

        let _tx = ReadWriteTransactionBuilder::new(db_client.clone())
            .set_exclude_txn_from_change_streams(true)
            .set_begin_transaction_option(BeginTransactionOption::ExplicitBegin)
            .build(None)
            .await
            .expect("Failed to build transaction");
    }

    #[tokio_test_no_panics]
    async fn read_write_transaction_tracks_highest_precommit_token() {
        let mut mock = create_session_mock();

        mock.expect_begin_transaction().once().returning(|_| {
            Ok(tonic::Response::new(v1::Transaction {
                id: vec![4, 2],
                ..Default::default()
            }))
        });

        // 3 sequential updates returning tokens [seq 2, seq 5, seq 3]
        let tokens_iter = vec![2, 5, 3].into_iter();
        let counter_mutex = Mutex::new(tokens_iter);

        mock.expect_execute_sql().times(3).returning(move |_req| {
            let seq = counter_mutex
                .lock()
                .expect("Failed to lock mutex")
                .next()
                .expect("Failed to get next token");
            Ok(tonic::Response::new(v1::ResultSet {
                stats: Some(v1::ResultSetStats {
                    row_count: Some(v1::result_set_stats::RowCount::RowCountExact(1)),
                    ..Default::default()
                }),
                precommit_token: Some(v1::MultiplexedSessionPrecommitToken {
                    precommit_token: vec![seq as u8],
                    seq_num: seq,
                }),
                ..Default::default()
            }))
        });

        // Commit should only use the highest token (seq 5)
        mock.expect_commit().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(
                req.precommit_token,
                Some(v1::MultiplexedSessionPrecommitToken {
                    precommit_token: vec![5],
                    seq_num: 5,
                })
            );
            Ok(tonic::Response::new(v1::CommitResponse {
                commit_timestamp: Some(prost_types::Timestamp {
                    seconds: 12345,
                    nanos: 0,
                }),
                ..Default::default()
            }))
        });

        let (db_client, _server) = setup_db_client(mock).await;
        let tx = ReadWriteTransactionBuilder::new(db_client.clone())
            .set_begin_transaction_option(BeginTransactionOption::ExplicitBegin)
            .build(None)
            .await
            .expect("Failed to build transaction");

        for _ in 0..3 {
            tx.execute_update("UPDATE Y")
                .await
                .expect("Failed to execute update");
        }
        let ts = tx.commit().await.expect("Failed to commit transaction");
        assert_eq!(
            ts.commit_timestamp
                .expect("Commit timestamp should be present")
                .seconds(),
            12345
        );
    }

    #[tokio_test_no_panics]
    async fn read_write_transaction_commit_retry_exactly_once_explicit() -> anyhow::Result<()> {
        run_read_write_transaction_commit_retry_exactly_once(BeginTransactionOption::ExplicitBegin)
            .await
    }

    #[tokio_test_no_panics]
    async fn read_write_transaction_commit_retry_exactly_once_inline() -> anyhow::Result<()> {
        run_read_write_transaction_commit_retry_exactly_once(BeginTransactionOption::InlineBegin)
            .await
    }

    async fn run_read_write_transaction_commit_retry_exactly_once(
        begin_transaction_option: BeginTransactionOption,
    ) -> anyhow::Result<()> {
        let mut mock = create_session_mock();

        let transaction_id = vec![7, 7];

        if begin_transaction_option == BeginTransactionOption::ExplicitBegin {
            let id = transaction_id.clone();
            mock.expect_begin_transaction().once().returning(move |_| {
                Ok(tonic::Response::new(v1::Transaction {
                    id: id.clone(),
                    ..Default::default()
                }))
            });
        } else {
            let id = transaction_id.clone();
            mock.expect_execute_sql().once().returning(move |req| {
                let req = req.into_inner();
                let transaction = req
                    .transaction
                    .as_ref()
                    .expect("transaction options required for inline begin");
                let selector = transaction.selector.as_ref().expect("selector required");
                assert!(matches!(
                    selector,
                    v1::transaction_selector::Selector::Begin(_)
                ));

                Ok(tonic::Response::new(v1::ResultSet {
                    metadata: Some(v1::ResultSetMetadata {
                        transaction: Some(v1::Transaction {
                            id: id.clone(),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    stats: Some(v1::ResultSetStats {
                        row_count: Some(v1::result_set_stats::RowCount::RowCountExact(1)),
                        ..Default::default()
                    }),
                    ..Default::default()
                }))
            });
        }

        let mut seq = mockall::Sequence::new();

        // Initial commit returns a retry token (seq 2)
        mock.expect_commit()
            .once()
            .in_sequence(&mut seq)
            .returning(|_| {
                Ok(tonic::Response::new(v1::CommitResponse {
                    commit_timestamp: Some(prost_types::Timestamp {
                        seconds: 1000,
                        nanos: 0,
                    }),
                    multiplexed_session_retry: Some(
                        v1::commit_response::MultiplexedSessionRetry::PrecommitToken(
                            v1::MultiplexedSessionPrecommitToken {
                                precommit_token: vec![2],
                                seq_num: 2,
                            },
                        ),
                    ),
                    ..Default::default()
                }))
            });

        // Retry commit returns another retry token (seq 3).
        // The library should not retry multiple times.
        mock.expect_commit()
            .once()
            .in_sequence(&mut seq)
            .returning(|req| {
                let req = req.into_inner();
                assert_eq!(
                    req.precommit_token
                        .as_ref()
                        .expect("Missing precommit token in retry req")
                        .seq_num,
                    2
                );

                Ok(tonic::Response::new(v1::CommitResponse {
                    commit_timestamp: Some(prost_types::Timestamp {
                        seconds: 9999,
                        nanos: 0,
                    }),
                    multiplexed_session_retry: Some(
                        v1::commit_response::MultiplexedSessionRetry::PrecommitToken(
                            v1::MultiplexedSessionPrecommitToken {
                                precommit_token: vec![3],
                                seq_num: 3,
                            },
                        ),
                    ),
                    ..Default::default()
                }))
            });

        let (db_client, _server) = setup_db_client(mock).await;
        let tx = ReadWriteTransactionBuilder::new(db_client.clone())
            .set_begin_transaction_option(begin_transaction_option)
            .build(None)
            .await?;

        if begin_transaction_option == BeginTransactionOption::InlineBegin {
            tx.execute_update("UPDATE Users SET Name = 'Alice' WHERE Id = 1")
                .await?;
        }

        let ts = tx.commit().await.expect("Failed to commit transaction");
        assert_eq!(
            ts.commit_timestamp
                .as_ref()
                .expect("timestamp should be present")
                .seconds(),
            9999
        );
        Ok(())
    }

    #[tokio_test_no_panics]
    async fn read_write_transaction_commit_with_max_commit_delay_explicit() -> anyhow::Result<()> {
        run_read_write_transaction_commit_with_max_commit_delay(
            BeginTransactionOption::ExplicitBegin,
        )
        .await
    }

    #[tokio_test_no_panics]
    async fn read_write_transaction_commit_with_max_commit_delay_inline() -> anyhow::Result<()> {
        run_read_write_transaction_commit_with_max_commit_delay(BeginTransactionOption::InlineBegin)
            .await
    }

    async fn run_read_write_transaction_commit_with_max_commit_delay(
        begin_transaction_option: BeginTransactionOption,
    ) -> anyhow::Result<()> {
        let mut mock = create_session_mock();

        if begin_transaction_option == BeginTransactionOption::ExplicitBegin {
            mock.expect_begin_transaction().once().returning(|_| {
                Ok(tonic::Response::new(v1::Transaction {
                    id: vec![1, 2, 3],
                    ..Default::default()
                }))
            });
        } else {
            mock.expect_execute_sql().once().returning(|req| {
                let req = req.into_inner();
                let transaction = req
                    .transaction
                    .as_ref()
                    .expect("transaction options required for inline begin");
                let selector = transaction.selector.as_ref().expect("selector required");
                assert!(matches!(
                    selector,
                    v1::transaction_selector::Selector::Begin(_)
                ));

                Ok(tonic::Response::new(v1::ResultSet {
                    metadata: Some(v1::ResultSetMetadata {
                        transaction: Some(v1::Transaction {
                            id: vec![1, 2, 3],
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    stats: Some(v1::ResultSetStats {
                        row_count: Some(v1::result_set_stats::RowCount::RowCountExact(1)),
                        ..Default::default()
                    }),
                    ..Default::default()
                }))
            });
        }

        mock.expect_commit().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(
                req.max_commit_delay,
                Some(::prost_types::Duration {
                    seconds: 0,
                    nanos: 200_000_000, // 200ms
                })
            );
            Ok(tonic::Response::new(v1::CommitResponse {
                commit_timestamp: Some(prost_types::Timestamp {
                    seconds: 123456789,
                    nanos: 0,
                }),
                ..Default::default()
            }))
        });

        let (db_client, _server) = setup_db_client(mock).await;

        let tx = ReadWriteTransactionBuilder::new(db_client.clone())
            .set_max_commit_delay(Duration::from_millis(200))
            .set_begin_transaction_option(begin_transaction_option)
            .build(None)
            .await
            .expect("Failed to build transaction");

        if begin_transaction_option == BeginTransactionOption::InlineBegin {
            tx.execute_update("UPDATE Users SET Name = 'Alice' WHERE Id = 1")
                .await?;
        }

        let ts = tx.commit().await.expect("Failed to commit");
        assert_eq!(
            ts.commit_timestamp
                .expect("Commit timestamp should be present")
                .seconds(),
            123456789
        );
        Ok(())
    }

    #[tokio_test_no_panics]
    async fn read_write_transaction_execute_update_no_fallback() {
        let mut mock = create_session_mock();

        // 1. Initial DML attempt fails!
        mock.expect_execute_sql().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(req.sql, "UPDATE Users SET Name = 'Alice' WHERE Id = 1");

            let selector = req
                .transaction
                .expect("missing transaction selector")
                .selector
                .expect("missing selector");
            match selector {
                v1::transaction_selector::Selector::Begin(_) => {}
                _ => panic!("Expected Selector::Begin"),
            }

            Err(tonic::Status::new(tonic::Code::Internal, "internal error"))
        });

        let (db_client, _server) = setup_db_client(mock).await;

        let tx = ReadWriteTransactionBuilder::new(db_client.clone())
            .build(None)
            .await
            .expect("Failed to build transaction");

        // First statement fails and propagates the error
        let err = tx
            .execute_update("UPDATE Users SET Name = 'Alice' WHERE Id = 1")
            .await
            .expect_err("Expected update statement to fail");
        assert_eq!(err.status().unwrap().code, Code::Internal);

        // Second statement fails with Aborted because the transaction is marked as failed
        let err2 = tx
            .execute_update("UPDATE Users SET Name = 'Alice' WHERE Id = 2")
            .await
            .expect_err("Expected second statement to fail with Aborted");
        assert_eq!(err2.status().unwrap().code, Code::Aborted);
        assert_eq!(
            err2.status().unwrap().message,
            "Aborted due to failed initial statement"
        );
    }

    #[tokio_test_no_panics]
    async fn read_write_transaction_execute_batch_update_no_fallback() -> anyhow::Result<()> {
        let mut mock = create_session_mock();

        // 1. First Batch DML attempt fails!
        mock.expect_execute_batch_dml().once().returning(|req| {
            let req = req.into_inner();
            let selector = req
                .transaction
                .expect("missing transaction selector")
                .selector
                .expect("missing selector");
            match selector {
                v1::transaction_selector::Selector::Begin(_) => {}
                _ => panic!("Expected Selector::Begin"),
            }

            Err(tonic::Status::new(tonic::Code::Internal, "internal error"))
        });

        let (db_client, _server) = setup_db_client(mock).await;

        let tx = ReadWriteTransactionBuilder::new(db_client)
            .build(None)
            .await?;

        let batch =
            BatchDml::builder().add_statement("UPDATE Users SET Name = 'Alice' WHERE Id = 1");

        // First batch fails and propagates error
        let err = tx
            .execute_batch_update(batch.build())
            .await
            .expect_err("Expected batch update to fail");
        assert_eq!(err.status().unwrap().code, Code::Internal);

        // Second statement fails with Aborted
        let err2 = tx
            .execute_update("UPDATE Users SET Name = 'Alice' WHERE Id = 2")
            .await
            .expect_err("Expected statement to fail with Aborted");
        assert_eq!(err2.status().unwrap().code, Code::Aborted);

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn leader_aware_routing_enabled_by_default() -> anyhow::Result<()> {
        let mut mock = create_session_mock();
        mock.expect_begin_transaction().once().returning(|req| {
            assert_eq!(
                req.metadata()
                    .get("x-goog-spanner-route-to-leader")
                    .expect("header required")
                    .to_str()
                    .unwrap(),
                "true"
            );
            Ok(tonic::Response::new(v1::Transaction {
                id: vec![1, 2, 3],
                ..Default::default()
            }))
        });
        mock.expect_execute_sql().once().returning(|req| {
            assert_eq!(
                req.metadata()
                    .get("x-goog-spanner-route-to-leader")
                    .expect("header required")
                    .to_str()
                    .unwrap(),
                "true"
            );
            Ok(tonic::Response::new(v1::ResultSet {
                stats: Some(v1::ResultSetStats {
                    row_count: Some(v1::result_set_stats::RowCount::RowCountExact(1)),
                    ..Default::default()
                }),
                ..Default::default()
            }))
        });
        mock.expect_commit().once().returning(|req| {
            assert_eq!(
                req.metadata()
                    .get("x-goog-spanner-route-to-leader")
                    .expect("header required")
                    .to_str()
                    .unwrap(),
                "true"
            );
            Ok(tonic::Response::new(v1::CommitResponse {
                commit_timestamp: Some(prost_types::Timestamp {
                    seconds: 1,
                    nanos: 0,
                }),
                ..Default::default()
            }))
        });

        let (db_client, _server) = setup_db_client(mock).await;
        let tx = ReadWriteTransactionBuilder::new(db_client)
            .set_begin_transaction_option(BeginTransactionOption::ExplicitBegin)
            .build(None)
            .await?;
        let count = tx.execute_update("UPDATE Users SET active = true").await?;
        assert_eq!(count, 1);
        let _ = tx.commit().await?;
        Ok(())
    }

    #[tokio_test_no_panics]
    async fn leader_aware_routing_disabled() -> anyhow::Result<()> {
        use crate::client::Spanner;
        use google_cloud_auth::credentials::anonymous::Builder as Anonymous;

        let mut mock = create_session_mock();
        mock.expect_begin_transaction().once().returning(|req| {
            assert!(
                req.metadata()
                    .get("x-goog-spanner-route-to-leader")
                    .is_none()
            );
            Ok(tonic::Response::new(v1::Transaction {
                id: vec![1, 2, 3],
                ..Default::default()
            }))
        });
        mock.expect_execute_sql().once().returning(|req| {
            assert!(
                req.metadata()
                    .get("x-goog-spanner-route-to-leader")
                    .is_none()
            );
            Ok(tonic::Response::new(v1::ResultSet {
                stats: Some(v1::ResultSetStats {
                    row_count: Some(v1::result_set_stats::RowCount::RowCountExact(1)),
                    ..Default::default()
                }),
                ..Default::default()
            }))
        });
        mock.expect_commit().once().returning(|req| {
            assert!(
                req.metadata()
                    .get("x-goog-spanner-route-to-leader")
                    .is_none()
            );
            Ok(tonic::Response::new(v1::CommitResponse {
                commit_timestamp: Some(prost_types::Timestamp {
                    seconds: 1,
                    nanos: 0,
                }),
                ..Default::default()
            }))
        });

        let (address, _server) = spanner_grpc_mock::start("0.0.0.0:0", mock).await.unwrap();
        let spanner = Spanner::builder()
            .with_endpoint(address)
            .with_credentials(Anonymous::new().build())
            .build()
            .await?;
        let db_client = spanner
            .database_client("projects/p/instances/i/databases/d")
            .with_leader_aware_routing(false)
            .build()
            .await?;

        let tx = ReadWriteTransactionBuilder::new(db_client)
            .set_begin_transaction_option(BeginTransactionOption::ExplicitBegin)
            .build(None)
            .await?;
        let count = tx.execute_update("UPDATE Users SET active = true").await?;
        assert_eq!(count, 1);
        let _ = tx.commit().await?;
        Ok(())
    }

    #[tokio_test_no_panics]
    async fn leader_aware_routing_query_in_read_write() -> anyhow::Result<()> {
        let mut mock = create_session_mock();
        mock.expect_begin_transaction().once().returning(|req| {
            assert_eq!(
                req.metadata()
                    .get("x-goog-spanner-route-to-leader")
                    .expect("header required")
                    .to_str()
                    .unwrap(),
                "true"
            );
            Ok(tonic::Response::new(v1::Transaction {
                id: vec![1, 2, 3],
                ..Default::default()
            }))
        });
        mock.expect_execute_streaming_sql().once().returning(|req| {
            assert_eq!(
                req.metadata()
                    .get("x-goog-spanner-route-to-leader")
                    .expect("header required")
                    .to_str()
                    .unwrap(),
                "true"
            );
            let stream = adapt([Ok(v1::PartialResultSet {
                metadata: Some(v1::ResultSetMetadata {
                    row_type: Some(v1::StructType { fields: vec![] }),
                    ..Default::default()
                }),
                ..Default::default()
            })]);
            Ok(tonic::Response::from(stream))
        });

        let (db_client, _server) = setup_db_client(mock).await;
        let tx = ReadWriteTransactionBuilder::new(db_client)
            .set_begin_transaction_option(BeginTransactionOption::ExplicitBegin)
            .build(None)
            .await?;
        let _rs = tx
            .execute_query(Statement::builder("SELECT 1").build())
            .await?;
        Ok(())
    }

    #[tokio_test_no_panics]
    async fn leader_aware_routing_merges_custom_headers() -> anyhow::Result<()> {
        let mut mock = create_session_mock();
        mock.expect_begin_transaction().once().returning(|req| {
            assert_eq!(
                req.metadata()
                    .get("x-goog-spanner-route-to-leader")
                    .expect("header required")
                    .to_str()
                    .unwrap(),
                "true"
            );
            Ok(tonic::Response::new(v1::Transaction {
                id: vec![1, 2, 3],
                ..Default::default()
            }))
        });
        mock.expect_execute_sql().once().returning(|req| {
            assert_eq!(
                req.metadata()
                    .get("x-goog-spanner-route-to-leader")
                    .expect("header required")
                    .to_str()
                    .unwrap(),
                "true"
            );
            assert_eq!(
                req.metadata()
                    .get("x-custom-user-header")
                    .expect("custom header required")
                    .to_str()
                    .unwrap(),
                "custom-value"
            );
            Ok(tonic::Response::new(v1::ResultSet {
                stats: Some(v1::ResultSetStats {
                    row_count: Some(v1::result_set_stats::RowCount::RowCountExact(1)),
                    ..Default::default()
                }),
                ..Default::default()
            }))
        });

        let (db_client, _server) = setup_db_client(mock).await;
        let tx = ReadWriteTransactionBuilder::new(db_client)
            .set_begin_transaction_option(BeginTransactionOption::ExplicitBegin)
            .build(None)
            .await?;

        let mut custom_headers = HeaderMap::new();
        custom_headers.insert(
            "x-custom-user-header",
            HeaderValue::from_static("custom-value"),
        );

        let mut stmt = Statement::builder("UPDATE Users SET active = true").build();
        let opts = stmt.gax_options().clone().insert_extension(custom_headers);
        stmt = stmt.with_gax_options(opts);

        let count = tx.execute_update(stmt).await?;
        assert_eq!(count, 1);
        Ok(())
    }

    #[tokio_test_no_panics]
    async fn leader_aware_routing_implicit_begin_runner_retry() -> anyhow::Result<()> {
        let mut mock = create_session_mock();
        let mut seq = mockall::Sequence::new();

        // 1. Initial execute_sql attempts implicit begin and fails with Internal.
        // It must include the LAR header because it is a modifying operation.
        mock.expect_execute_sql()
            .once()
            .in_sequence(&mut seq)
            .returning(|req| {
                assert_eq!(
                    req.metadata()
                        .get("x-goog-spanner-route-to-leader")
                        .expect("header required on initial execute")
                        .to_str()
                        .unwrap(),
                    "true"
                );
                let req = req.into_inner();
                let transaction = req.transaction.unwrap();
                assert!(matches!(
                    transaction.selector.unwrap(),
                    v1::transaction_selector::Selector::Begin(_)
                ));
                Err(tonic::Status::new(
                    tonic::Code::Internal,
                    "transient internal error",
                ))
            });

        // 2. Commit of the first attempt will fail with AbortedDueToFailedFirstStatement (client-side aborted).
        // The runner retries. Since first statement failed on starting, it forces explicit BeginTransaction.
        // This BeginTransaction must include the LAR header.
        mock.expect_begin_transaction()
            .once()
            .in_sequence(&mut seq)
            .returning(|req| {
                assert_eq!(
                    req.metadata()
                        .get("x-goog-spanner-route-to-leader")
                        .expect("header required on explicit begin fallback")
                        .to_str()
                        .unwrap(),
                    "true"
                );
                Ok(tonic::Response::new(v1::Transaction {
                    id: vec![42],
                    ..Default::default()
                }))
            });

        // 3. Statement execution on the retry attempt must succeed and use the transaction ID.
        // It must also include the LAR header.
        mock.expect_execute_sql()
            .once()
            .in_sequence(&mut seq)
            .returning(|req| {
                assert_eq!(
                    req.metadata()
                        .get("x-goog-spanner-route-to-leader")
                        .expect("header required on retry execute")
                        .to_str()
                        .unwrap(),
                    "true"
                );
                let req = req.into_inner();
                let transaction = req.transaction.unwrap();
                assert!(matches!(
                    transaction.selector.unwrap(),
                    v1::transaction_selector::Selector::Id(id) if id == vec![42]
                ));
                Ok(tonic::Response::new(v1::ResultSet {
                    metadata: Some(v1::ResultSetMetadata {
                        transaction: Some(v1::Transaction {
                            id: vec![42],
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    stats: Some(v1::ResultSetStats {
                        row_count: Some(v1::result_set_stats::RowCount::RowCountExact(1)),
                        ..Default::default()
                    }),
                    ..Default::default()
                }))
            });

        mock.expect_commit().once().returning(|_req| {
            Ok(tonic::Response::new(v1::CommitResponse {
                commit_timestamp: Some(prost_types::Timestamp {
                    seconds: 123,
                    nanos: 0,
                }),
                ..Default::default()
            }))
        });

        let (db_client, _server) = setup_db_client(mock).await;

        let runner = db_client
            .read_write_transaction()
            .with_retry_policy(BasicTransactionRetryPolicy::new().with_max_attempts(2))
            .build();

        let attempt = Arc::new(AtomicU32::new(0));
        let att_clone = attempt.clone();
        let res = runner
            .run(async |tx| {
                let current_attempt = att_clone.fetch_add(1, Ordering::SeqCst) + 1;
                let res = tx.execute_update("UPDATE Users SET active = true").await;
                if current_attempt == 1 {
                    assert!(res.is_err());
                    let err = res.unwrap_err();
                    assert_eq!(err.status().map(|s| s.code), Some(Code::Internal));
                    // Swallow the internal error and let the transaction attempt to commit.
                    // The commit will fail with AbortedDueToFailedFirstStatement because the first statement failed.
                    Ok(())
                } else {
                    assert_eq!(res.unwrap(), 1);
                    Ok(())
                }
            })
            .await?;

        assert!(res.commit_response.commit_timestamp.is_some());
        Ok(())
    }

    #[tokio_test_no_panics]
    async fn read_write_transaction_tag_forwarded_on_runner_retry() -> anyhow::Result<()> {
        let mut mock = create_session_mock();
        let mut seq = mockall::Sequence::new();

        // 1. Initial execute_sql attempts inline begin and fails with Internal.
        // It must include the transaction tag.
        mock.expect_execute_sql()
            .once()
            .in_sequence(&mut seq)
            .returning(|req| {
                let req = req.into_inner();
                assert_eq!(
                    req.request_options
                        .as_ref()
                        .expect("Missing request_options on initial RPC")
                        .transaction_tag,
                    "fallback-test-tag"
                );
                assert!(matches!(
                    req.transaction.unwrap().selector.unwrap(),
                    v1::transaction_selector::Selector::Begin(_)
                ));
                Err(tonic::Status::new(
                    tonic::Code::Internal,
                    "transient internal error",
                ))
            });

        // 2. Commit fails with AbortedDueToFailedFirstStatement, runner retries with explicit BeginTransaction.
        // The BeginTransaction must include the transaction tag.
        mock.expect_begin_transaction()
            .once()
            .in_sequence(&mut seq)
            .returning(|req| {
                let req = req.into_inner();
                assert_eq!(
                    req.request_options
                        .as_ref()
                        .expect("Missing request_options on explicit begin")
                        .transaction_tag,
                    "fallback-test-tag"
                );
                Ok(tonic::Response::new(v1::Transaction {
                    id: vec![7, 7, 7],
                    ..Default::default()
                }))
            });

        // 3. Retried statement execution must succeed and use transaction ID.
        // It must include the transaction tag.
        mock.expect_execute_sql()
            .once()
            .in_sequence(&mut seq)
            .returning(|req| {
                let req = req.into_inner();
                assert_eq!(
                    req.request_options
                        .as_ref()
                        .expect("Missing request_options on retried RPC")
                        .transaction_tag,
                    "fallback-test-tag"
                );
                assert!(matches!(
                    req.transaction.unwrap().selector.unwrap(),
                    v1::transaction_selector::Selector::Id(id) if id == vec![7, 7, 7]
                ));
                Ok(tonic::Response::new(v1::ResultSet {
                    metadata: Some(v1::ResultSetMetadata {
                        transaction: Some(v1::Transaction {
                            id: vec![7, 7, 7],
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    stats: Some(v1::ResultSetStats {
                        row_count: Some(v1::result_set_stats::RowCount::RowCountExact(1)),
                        ..Default::default()
                    }),
                    ..Default::default()
                }))
            });

        mock.expect_commit().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(
                req.request_options
                    .as_ref()
                    .expect("Missing request_options on commit")
                    .transaction_tag,
                "fallback-test-tag"
            );
            Ok(tonic::Response::new(v1::CommitResponse {
                commit_timestamp: Some(prost_types::Timestamp {
                    seconds: 123,
                    nanos: 0,
                }),
                ..Default::default()
            }))
        });

        let (db_client, _server) = setup_db_client(mock).await;
        let runner = db_client
            .read_write_transaction()
            .set_transaction_tag("fallback-test-tag")
            .with_retry_policy(BasicTransactionRetryPolicy::new().with_max_attempts(2))
            .build();

        let attempt = Arc::new(AtomicU32::new(0));
        let att_clone = attempt.clone();
        let res = runner
            .run(async |tx| {
                let current_attempt = att_clone.fetch_add(1, Ordering::SeqCst) + 1;
                let res = tx.execute_update("UPDATE Users SET active = true").await;
                if current_attempt == 1 {
                    assert!(res.is_err());
                    let err = res.unwrap_err();
                    assert_eq!(err.status().map(|s| s.code), Some(Code::Internal));
                    // Swallow the internal error and let it commit to trigger AbortedDueToFailedFirstStatement retry
                    Ok(())
                } else {
                    assert_eq!(res.unwrap(), 1);
                    Ok(())
                }
            })
            .await?;

        assert!(res.commit_response.commit_timestamp.is_some());
        Ok(())
    }

    #[tokio_test_no_panics]
    async fn read_write_transaction_mutation_only_inline_begin_commit() -> anyhow::Result<()> {
        let mut mock = create_session_mock();

        // Since no statement was executed, commit will detect NotStarted and call begin_explicitly
        mock.expect_begin_transaction().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(
                req.session,
                "projects/p/instances/i/databases/d/sessions/123"
            );
            assert!(
                req.mutation_key.is_some(),
                "mutation_key should be populated when starting transaction at commit time"
            );
            let key = req
                .mutation_key
                .as_ref()
                .expect("mutation_key is populated");
            assert!(
                key.operation.is_some(),
                "mutation_key should have an operation"
            );
            Ok(tonic::Response::new(v1::Transaction {
                id: vec![7, 7, 7],
                ..Default::default()
            }))
        });

        mock.expect_commit().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(
                req.session,
                "projects/p/instances/i/databases/d/sessions/123"
            );
            assert_eq!(
                req.transaction,
                Some(v1::commit_request::Transaction::TransactionId(vec![
                    7, 7, 7
                ]))
            );
            assert_eq!(req.mutations.len(), 1);
            Ok(tonic::Response::new(v1::CommitResponse {
                commit_timestamp: Some(prost_types::Timestamp {
                    seconds: 5000,
                    nanos: 0,
                }),
                ..Default::default()
            }))
        });

        let (db_client, _server) = setup_db_client(mock).await;
        let tx = ReadWriteTransactionBuilder::new(db_client)
            .set_begin_transaction_option(BeginTransactionOption::InlineBegin)
            .build(None)
            .await
            .expect("Transaction build should succeed");

        let mutation = Mutation::new_insert_builder("Users")
            .set("UserId")
            .to(1)
            .build();
        tx.buffer([mutation]).expect("Buffer should succeed");

        let response = tx.commit().await.expect("Commit should succeed");
        assert_eq!(
            response
                .commit_timestamp
                .expect("timestamp present")
                .seconds(),
            5000
        );
        Ok(())
    }

    #[tokio_test_no_panics]
    async fn transaction_runner_batch_dml_aborted_retry() -> anyhow::Result<()> {
        use crate::retry_delay::{ProtoRetryInfo, RETRY_INFO_TYPE_URL};
        use prost::Message;

        let mut mock = create_session_mock();
        let mut sequence = mockall::Sequence::new();

        // 1. First attempt: Inline begin, execute_batch_dml returns OK with status Aborted.
        mock.expect_execute_batch_dml()
            .times(1)
            .in_sequence(&mut sequence)
            .returning(|req| {
                let req = req.into_inner();
                assert!(matches!(
                    req.transaction.unwrap().selector.unwrap(),
                    v1::transaction_selector::Selector::Begin(_)
                ));
                let mut retry_bytes = Vec::new();
                ProtoRetryInfo {
                    retry_delay: Some(prost_types::Duration {
                        seconds: 0,
                        nanos: 1,
                    }),
                }
                .encode(&mut retry_bytes)
                .expect("encoding ProtoRetryInfo should succeed");
                Ok(tonic::Response::new(v1::ExecuteBatchDmlResponse {
                    result_sets: vec![],
                    status: Some(spanner_grpc_mock::google::rpc::Status {
                        code: tonic::Code::Aborted as i32,
                        message: "concurrent lock abort".into(),
                        details: vec![prost_types::Any {
                            type_url: RETRY_INFO_TYPE_URL.to_string(),
                            value: retry_bytes,
                        }],
                    }),
                    ..Default::default()
                }))
            });

        // 2. TransactionRunner catches Aborted error and initiates attempt 2.
        mock.expect_execute_batch_dml()
            .times(1)
            .in_sequence(&mut sequence)
            .returning(|req| {
                let req = req.into_inner();
                assert!(matches!(
                    req.transaction.unwrap().selector.unwrap(),
                    v1::transaction_selector::Selector::Begin(_)
                ));
                Ok(tonic::Response::new(v1::ExecuteBatchDmlResponse {
                    result_sets: vec![v1::ResultSet {
                        metadata: Some(v1::ResultSetMetadata {
                            transaction: Some(v1::Transaction {
                                id: vec![9, 9, 9],
                                ..Default::default()
                            }),
                            ..Default::default()
                        }),
                        stats: Some(v1::ResultSetStats {
                            row_count: Some(v1::result_set_stats::RowCount::RowCountExact(1)),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }],
                    status: Some(spanner_grpc_mock::google::rpc::Status {
                        code: 0,
                        message: "OK".into(),
                        details: vec![],
                    }),
                    ..Default::default()
                }))
            });

        mock.expect_commit().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(
                req.transaction,
                Some(v1::commit_request::Transaction::TransactionId(vec![
                    9, 9, 9
                ]))
            );
            Ok(tonic::Response::new(v1::CommitResponse {
                commit_timestamp: Some(prost_types::Timestamp {
                    seconds: 999,
                    nanos: 0,
                }),
                ..Default::default()
            }))
        });

        let (db_client, _server) = setup_db_client(mock).await;

        let runner = db_client
            .read_write_transaction()
            .with_retry_policy(
                BasicTransactionRetryPolicy::new()
                    .with_max_attempts(3)
                    .with_total_timeout(std::time::Duration::from_secs(5)),
            )
            .build();

        runner
            .run(async |tx| {
                let batch = BatchDml::builder()
                    .add_statement("UPDATE Users SET active = true WHERE id = 1");
                tx.execute_batch_update(batch.build()).await?;
                Ok(())
            })
            .await?;

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn read_write_transaction_first_dml_aborted_and_continue_success() -> anyhow::Result<()> {
        let mut mock = create_session_mock();
        let mut sequence = mockall::Sequence::new();

        // 1. First statement (execute_sql) attempts inline begin and is aborted by Spanner
        mock.expect_execute_sql()
            .times(1)
            .in_sequence(&mut sequence)
            .returning(|req| {
                let req = req.into_inner();
                assert!(matches!(
                    req.transaction
                        .as_ref()
                        .expect("transaction options required for inline begin")
                        .selector
                        .as_ref()
                        .expect("selector required"),
                    v1::transaction_selector::Selector::Begin(_)
                ));
                Err(create_aborted_status(StdDuration::from_nanos(1)))
            });

        // 2. On retry (attempt 2), first statement attempts inline begin again (saving 1 RPC) and succeeds
        mock.expect_execute_sql()
            .times(1)
            .in_sequence(&mut sequence)
            .returning(|req| {
                let req = req.into_inner();
                assert!(matches!(
                    req.transaction
                        .as_ref()
                        .expect("transaction options required for inline begin")
                        .selector
                        .as_ref()
                        .expect("selector required"),
                    v1::transaction_selector::Selector::Begin(_)
                ));
                Ok(tonic::Response::new(v1::ResultSet {
                    metadata: Some(v1::ResultSetMetadata {
                        transaction: Some(v1::Transaction {
                            id: vec![9, 9, 9],
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    stats: Some(v1::ResultSetStats {
                        row_count: Some(v1::result_set_stats::RowCount::RowCountExact(1)),
                        ..Default::default()
                    }),
                    ..Default::default()
                }))
            });

        // 3. Second statement executes with transaction ID [9, 9, 9]
        mock.expect_execute_sql()
            .times(1)
            .in_sequence(&mut sequence)
            .returning(|req| {
                let req = req.into_inner();
                assert_eq!(
                    req.transaction
                        .as_ref()
                        .expect("transaction options required")
                        .selector
                        .as_ref()
                        .expect("selector required"),
                    &v1::transaction_selector::Selector::Id(vec![9, 9, 9]),
                    "Expected Selector::Id on second statement"
                );
                Ok(tonic::Response::new(v1::ResultSet {
                    stats: Some(v1::ResultSetStats {
                        row_count: Some(v1::result_set_stats::RowCount::RowCountExact(1)),
                        ..Default::default()
                    }),
                    ..Default::default()
                }))
            });

        // 4. Commit called with the transaction ID returned in step 2
        mock.expect_commit().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(
                req.transaction,
                Some(v1::commit_request::Transaction::TransactionId(vec![
                    9, 9, 9
                ]))
            );
            Ok(tonic::Response::new(v1::CommitResponse {
                commit_timestamp: Some(prost_types::Timestamp {
                    seconds: 999,
                    nanos: 0,
                }),
                ..Default::default()
            }))
        });

        let (database_client, _server) = setup_db_client(mock).await;

        let runner = database_client
            .read_write_transaction()
            .with_retry_policy(
                BasicTransactionRetryPolicy::new()
                    .with_max_attempts(2)
                    .with_total_timeout(StdDuration::from_secs(5)),
            )
            .build();

        let attempt_counter = Arc::new(AtomicU32::new(0));
        let attempt_counter_clone = Arc::clone(&attempt_counter);

        runner
            .run(async |transaction| {
                let current_attempt = attempt_counter_clone.fetch_add(1, Ordering::SeqCst) + 1;
                if current_attempt == 1 {
                    // 1. First statement fails with Aborted.
                    let result1 = transaction
                        .execute_update("UPDATE Users SET active = true WHERE id = 1")
                        .await;
                    assert!(result1.is_err(), "First statement must return error");
                    assert!(
                        is_aborted(&result1.expect_err("first statement failed")),
                        "Error must be Aborted"
                    );

                    // 2. Second statement in the same attempt must fail fast with Aborted
                    // rather than starting an orphaned transaction or hanging.
                    let result2 = transaction
                        .execute_update("UPDATE Users SET active = true WHERE id = 2")
                        .await;
                    assert!(
                        result2.is_err(),
                        "Second statement must fail because the initial statement aborted"
                    );
                    let error2 = result2.expect_err("second statement failed");
                    assert!(
                        is_aborted(&error2),
                        "Second statement error must be Aborted"
                    );
                    assert!(
                        error2.to_string().contains("test transaction aborted"),
                        "Expected abort error message from initial statement, got: {error2}"
                    );

                    return Err(error2);
                }

                // Attempt 2: both statements succeed in the retried transaction.
                let count1 = transaction
                    .execute_update("UPDATE Users SET active = true WHERE id = 1")
                    .await?;
                assert_eq!(count1, 1, "Expected 1 row updated in statement 1");

                let count2 = transaction
                    .execute_update("UPDATE Users SET active = true WHERE id = 2")
                    .await?;
                assert_eq!(count2, 1, "Expected 1 row updated in statement 2");

                Ok(())
            })
            .await?;

        assert_eq!(
            attempt_counter.load(Ordering::SeqCst),
            2,
            "Expected exactly 2 attempts"
        );

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn read_write_transaction_first_batch_dml_aborted_and_continue_success()
    -> anyhow::Result<()> {
        let mut mock = create_session_mock();
        let mut sequence = mockall::Sequence::new();

        // 1. First statement (execute_batch_dml) attempts inline begin and is aborted by Spanner
        mock.expect_execute_batch_dml()
            .times(1)
            .in_sequence(&mut sequence)
            .returning(|req| {
                let req = req.into_inner();
                assert!(matches!(
                    req.transaction
                        .as_ref()
                        .expect("transaction options required for inline begin")
                        .selector
                        .as_ref()
                        .expect("selector required"),
                    v1::transaction_selector::Selector::Begin(_)
                ));
                Err(create_aborted_status(StdDuration::from_nanos(1)))
            });

        // 2. On retry (attempt 2), first statement (execute_batch_dml) attempts inline begin again (saving 1 RPC) and succeeds
        mock.expect_execute_batch_dml()
            .times(1)
            .in_sequence(&mut sequence)
            .returning(|req| {
                let req = req.into_inner();
                assert!(matches!(
                    req.transaction
                        .as_ref()
                        .expect("transaction options required for inline begin")
                        .selector
                        .as_ref()
                        .expect("selector required"),
                    v1::transaction_selector::Selector::Begin(_)
                ));
                Ok(tonic::Response::new(v1::ExecuteBatchDmlResponse {
                    result_sets: vec![v1::ResultSet {
                        metadata: Some(v1::ResultSetMetadata {
                            transaction: Some(v1::Transaction {
                                id: vec![9, 9, 9],
                                ..Default::default()
                            }),
                            ..Default::default()
                        }),
                        stats: Some(v1::ResultSetStats {
                            row_count: Some(v1::result_set_stats::RowCount::RowCountExact(1)),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }],
                    status: Some(RpcStatus {
                        code: 0,
                        message: "OK".into(),
                        details: vec![],
                    }),
                    ..Default::default()
                }))
            });

        // 3. Second statement executes with transaction ID [9, 9, 9]
        mock.expect_execute_sql()
            .times(1)
            .in_sequence(&mut sequence)
            .returning(|req| {
                let req = req.into_inner();
                assert_eq!(
                    req.transaction
                        .as_ref()
                        .expect("transaction options required")
                        .selector
                        .as_ref()
                        .expect("selector required"),
                    &v1::transaction_selector::Selector::Id(vec![9, 9, 9]),
                    "Expected Selector::Id on second statement"
                );
                Ok(tonic::Response::new(v1::ResultSet {
                    stats: Some(v1::ResultSetStats {
                        row_count: Some(v1::result_set_stats::RowCount::RowCountExact(1)),
                        ..Default::default()
                    }),
                    ..Default::default()
                }))
            });

        // 4. Commit called with the transaction ID returned in step 2
        mock.expect_commit().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(
                req.transaction,
                Some(v1::commit_request::Transaction::TransactionId(vec![
                    9, 9, 9
                ]))
            );
            Ok(tonic::Response::new(v1::CommitResponse {
                commit_timestamp: Some(prost_types::Timestamp {
                    seconds: 999,
                    nanos: 0,
                }),
                ..Default::default()
            }))
        });

        let (database_client, _server) = setup_db_client(mock).await;

        let runner = database_client
            .read_write_transaction()
            .with_retry_policy(
                BasicTransactionRetryPolicy::new()
                    .with_max_attempts(2)
                    .with_total_timeout(StdDuration::from_secs(5)),
            )
            .build();

        let attempt_counter = Arc::new(AtomicU32::new(0));
        let attempt_counter_clone = Arc::clone(&attempt_counter);

        runner
            .run(async |transaction| {
                let current_attempt = attempt_counter_clone.fetch_add(1, Ordering::SeqCst) + 1;
                if current_attempt == 1 {
                    // 1. First statement (Batch DML) fails with Aborted.
                    let batch = BatchDml::builder()
                        .add_statement("UPDATE Users SET active = true WHERE id = 1");
                    let result1 = transaction.execute_batch_update(batch.build()).await;
                    assert!(result1.is_err(), "First statement must return error");
                    assert!(
                        is_aborted(&result1.expect_err("first statement failed")),
                        "Error must be Aborted"
                    );

                    // 2. Second statement in the same attempt must fail fast with Aborted
                    // rather than starting an orphaned transaction or hanging.
                    let result2 = transaction
                        .execute_update("UPDATE Users SET active = true WHERE id = 2")
                        .await;
                    assert!(
                        result2.is_err(),
                        "Second statement must fail because the initial statement aborted"
                    );
                    let error2 = result2.expect_err("second statement failed");
                    assert!(
                        is_aborted(&error2),
                        "Second statement error must be Aborted"
                    );
                    assert!(
                        error2.to_string().contains("test transaction aborted"),
                        "Expected abort error message from initial statement, got: {error2}"
                    );

                    return Err(error2);
                }

                // Attempt 2: both statements succeed in the retried transaction.
                let batch = BatchDml::builder()
                    .add_statement("UPDATE Users SET active = true WHERE id = 1");
                let update_counts = transaction.execute_batch_update(batch.build()).await?;
                assert_eq!(
                    update_counts,
                    vec![1],
                    "Expected 1 update count in batch statement"
                );

                let count2 = transaction
                    .execute_update("UPDATE Users SET active = true WHERE id = 2")
                    .await?;
                assert_eq!(count2, 1, "Expected 1 row updated in statement 2");

                Ok(())
            })
            .await?;

        assert_eq!(
            attempt_counter.load(Ordering::SeqCst),
            2,
            "Expected exactly 2 attempts"
        );

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn read_write_transaction_first_query_failed_and_continue_retry() -> anyhow::Result<()> {
        let mut mock = create_session_mock();
        let mut sequence = mockall::Sequence::new();

        // 1. First attempt: execute_streaming_sql fails directly with Internal error
        mock.expect_execute_streaming_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(|_| {
                Err(tonic::Status::new(
                    tonic::Code::Internal,
                    "transient internal error",
                ))
            });

        // 2. Commit fails client-side with Aborted due to failed initial statement.
        // Runner retries with explicit BeginTransaction
        mock.expect_begin_transaction()
            .once()
            .in_sequence(&mut sequence)
            .returning(|_| {
                Ok(tonic::Response::new(v1::Transaction {
                    id: vec![42],
                    ..Default::default()
                }))
            });

        // 3. Second attempt query succeeds
        mock.expect_execute_streaming_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(|_| {
                let rx = adapt([Ok(v1::PartialResultSet {
                    metadata: Some(v1::ResultSetMetadata {
                        row_type: Some(v1::StructType {
                            fields: vec![v1::struct_type::Field {
                                name: "Name".to_string(),
                                r#type: Some(v1::Type {
                                    code: v1::TypeCode::String as i32,
                                    ..Default::default()
                                }),
                            }],
                        }),
                        transaction: Some(v1::Transaction {
                            id: vec![42],
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    values: vec![string_val("alice")],
                    ..Default::default()
                })]);
                Ok(tonic::Response::from(rx))
            });

        mock.expect_commit().once().returning(|_| {
            Ok(tonic::Response::new(v1::CommitResponse {
                commit_timestamp: Some(prost_types::Timestamp {
                    seconds: 1234,
                    nanos: 0,
                }),
                ..Default::default()
            }))
        });

        let (db_client, _server) = setup_db_client(mock).await;
        let runner = db_client
            .read_write_transaction()
            .with_retry_policy(BasicTransactionRetryPolicy::new().with_max_attempts(2))
            .build();

        let attempt = Arc::new(AtomicU32::new(0));
        let attempt_clone = attempt.clone();
        let transaction_result = runner
            .run(async |transaction| {
                let current_attempt = attempt_clone.fetch_add(1, Ordering::SeqCst) + 1;
                let query_result = transaction
                    .execute_query("SELECT Name FROM Users WHERE Id = 1")
                    .await;
                if current_attempt == 1 {
                    assert!(query_result.is_err(), "First attempt query must fail");
                    let err = query_result.expect_err("Expected query error on first attempt");
                    assert_eq!(err.status().map(|s| s.code), Some(Code::Internal));
                    // Swallow error and continue
                    Ok(())
                } else {
                    let mut result_set = query_result.expect("Second attempt query must succeed");
                    let row = result_set
                        .next()
                        .await
                        .expect("Expected row option")
                        .expect("Expected valid row");
                    assert_eq!(row.get::<String>(0)?, "alice");
                    Ok(())
                }
            })
            .await?;

        assert!(
            transaction_result
                .commit_response
                .commit_timestamp
                .is_some(),
            "Expected commit timestamp"
        );
        Ok(())
    }

    #[tokio_test_no_panics]
    async fn read_write_transaction_first_query_failed_propagate_no_retry() -> anyhow::Result<()> {
        let mut mock = create_session_mock();

        // 1. Initial query fails directly
        mock.expect_execute_streaming_sql().once().returning(|_| {
            Err(tonic::Status::new(
                tonic::Code::Internal,
                "transient internal error",
            ))
        });

        let (db_client, _server) = setup_db_client(mock).await;
        let runner = db_client
            .read_write_transaction()
            .with_retry_policy(BasicTransactionRetryPolicy::new().with_max_attempts(2))
            .build();

        let result = runner
            .run(async |transaction| {
                let _ = transaction
                    .execute_query("SELECT Name FROM Users WHERE Id = 1")
                    .await?;
                Ok(())
            })
            .await;

        assert!(result.is_err(), "Transaction must fail without retrying");
        let err = result.expect_err("Expected transaction failure");
        assert_eq!(err.status().map(|s| s.code), Some(Code::Internal));
        Ok(())
    }

    #[tokio_test_no_panics]
    async fn read_write_transaction_query_failed_after_id_received_no_retry() -> anyhow::Result<()>
    {
        let mut mock = create_session_mock();
        let mut sequence = mockall::Sequence::new();

        // 1. Query execution starts and yields a stream
        mock.expect_execute_streaming_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(|_| {
                let rx = adapt([
                    Ok(v1::PartialResultSet {
                        metadata: Some(v1::ResultSetMetadata {
                            row_type: Some(v1::StructType {
                                fields: vec![v1::struct_type::Field {
                                    name: "Name".to_string(),
                                    r#type: Some(v1::Type {
                                        code: v1::TypeCode::String as i32,
                                        ..Default::default()
                                    }),
                                }],
                            }),
                            transaction: Some(v1::Transaction {
                                id: vec![42],
                                ..Default::default()
                            }),
                            ..Default::default()
                        }),
                        values: vec![string_val("alice")],
                        resume_token: b"resume-1".to_vec(),
                        ..Default::default()
                    }),
                    Err(tonic::Status::new(tonic::Code::Internal, "stream broken")),
                ]);
                Ok(tonic::Response::from(rx))
            });

        // 2. Commit fails because the transaction was already started, but we returned an Internal error
        mock.expect_commit()
            .once()
            .in_sequence(&mut sequence)
            .returning(|req| {
                let req = req.into_inner();
                assert_eq!(
                    req.transaction,
                    Some(v1::commit_request::Transaction::TransactionId(vec![42]))
                );
                Err(tonic::Status::new(tonic::Code::Internal, "commit failed"))
            });

        let (db_client, _server) = setup_db_client(mock).await;
        let runner = db_client
            .read_write_transaction()
            .with_retry_policy(BasicTransactionRetryPolicy::new().with_max_attempts(2))
            .build();

        let attempt = Arc::new(AtomicU32::new(0));
        let attempt_clone = attempt.clone();
        let result = runner
            .run(async |transaction| {
                let _attempt = attempt_clone.fetch_add(1, Ordering::SeqCst) + 1;
                let mut result_set = transaction
                    .execute_query("SELECT Name FROM Users WHERE Id = 1")
                    .await?;
                // First next() succeeds
                let row = result_set
                    .next()
                    .await
                    .expect("Expected first row")
                    .expect("First row should be Ok");
                assert_eq!(row.get::<String>(0)?, "alice");
                // Second next() fails
                let next_result = result_set.next().await.expect("Expected stream item");
                assert!(next_result.is_err(), "Second row fetch must fail");
                assert_eq!(
                    next_result
                        .expect_err("Expected stream error")
                        .status()
                        .map(|s| s.code),
                    Some(Code::Internal)
                );
                // Swallow error and return Ok
                Ok(())
            })
            .await;

        assert!(result.is_err(), "Commit must fail with Internal");
        let err = result.expect_err("Expected commit failure");
        assert_eq!(err.status().map(|s| s.code), Some(Code::Internal));
        assert_eq!(
            attempt.load(Ordering::SeqCst),
            1,
            "Should not retry since transaction ID was received"
        );
        Ok(())
    }

    #[tokio_test_no_panics]
    async fn read_write_transaction_batch_dml_failed_after_id_received_no_retry()
    -> anyhow::Result<()> {
        let mut mock = create_session_mock();
        let mut sequence = mockall::Sequence::new();

        // 1. First Batch DML execution returns N succeeded statements and one failed status,
        // and includes the transaction ID in the first result set's metadata.
        mock.expect_execute_batch_dml()
            .once()
            .in_sequence(&mut sequence)
            .returning(|_| {
                Ok(tonic::Response::new(v1::ExecuteBatchDmlResponse {
                    result_sets: vec![v1::ResultSet {
                        metadata: Some(v1::ResultSetMetadata {
                            transaction: Some(v1::Transaction {
                                id: vec![42],
                                ..Default::default()
                            }),
                            ..Default::default()
                        }),
                        stats: Some(v1::ResultSetStats {
                            row_count: Some(v1::result_set_stats::RowCount::RowCountExact(1)),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }],
                    status: Some(spanner_grpc_mock::google::rpc::Status {
                        code: tonic::Code::InvalidArgument as i32,
                        message: "invalid argument on statement 2".into(),
                        details: vec![],
                    }),
                    ..Default::default()
                }))
            });

        // Commit fails because of some other reason
        mock.expect_commit()
            .once()
            .in_sequence(&mut sequence)
            .returning(|req| {
                let req = req.into_inner();
                assert_eq!(
                    req.transaction,
                    Some(v1::commit_request::Transaction::TransactionId(vec![42]))
                );
                Err(tonic::Status::new(tonic::Code::Internal, "commit failed"))
            });

        let (db_client, _server) = setup_db_client(mock).await;
        let runner = db_client
            .read_write_transaction()
            .with_retry_policy(BasicTransactionRetryPolicy::new().with_max_attempts(2))
            .build();

        let attempt = Arc::new(AtomicU32::new(0));
        let attempt_clone = attempt.clone();
        let result = runner
            .run(async |transaction| {
                let _attempt = attempt_clone.fetch_add(1, Ordering::SeqCst) + 1;
                let batch = BatchDml::builder()
                    .add_statement("UPDATE Users SET active = true WHERE id = 1")
                    .add_statement("UPDATE Users SET active = true WHERE id = invalid_val");
                let batch_result = transaction.execute_batch_update(batch.build()).await;
                assert!(batch_result.is_err(), "Batch DML must return error");
                let err = batch_result.expect_err("Expected BatchUpdateError");
                assert!(
                    BatchUpdateError::extract(&err).is_some(),
                    "Expected BatchUpdateError source"
                );
                // Swallow error
                Ok(())
            })
            .await;

        assert!(result.is_err(), "Commit must fail with Internal");
        let err = result.expect_err("Expected commit error");
        assert_eq!(err.status().map(|s| s.code), Some(Code::Internal));
        assert_eq!(
            attempt.load(Ordering::SeqCst),
            1,
            "Should not retry since transaction ID was received"
        );
        Ok(())
    }

    fn parse_grpc_timeout(metadata: &MetadataMap) -> Option<StdDuration> {
        let timeout_header = metadata.get("grpc-timeout")?.to_str().ok()?;
        let numeric_part: String = timeout_header
            .chars()
            .take_while(|c| c.is_ascii_digit())
            .collect();
        let value = numeric_part.parse::<u64>().ok()?;
        let unit = timeout_header.trim_start_matches(&numeric_part);
        let duration = match unit {
            "u" => StdDuration::from_micros(value),
            "m" => StdDuration::from_millis(value),
            "S" => StdDuration::from_secs(value),
            "M" => StdDuration::from_secs(value * 60),
            "H" => StdDuration::from_secs(value * 3600),
            _ => return None,
        };
        Some(duration)
    }

    #[tokio_test_no_panics]
    async fn read_write_transaction_lazy_begin_never_retry() -> anyhow::Result<()> {
        let mut mock = create_session_mock();
        let mut sequence = mockall::Sequence::new();

        // 1. First statement execution uses inline-begin and fails with Unavailable (transient error)
        mock.expect_execute_sql()
            .once()
            .in_sequence(&mut sequence)
            .withf(|req| {
                matches!(
                    req.get_ref()
                        .transaction
                        .as_ref()
                        .and_then(|t| t.selector.as_ref()),
                    Some(v1::transaction_selector::Selector::Begin(_))
                )
            })
            .returning(move |_req| Err(tonic::Status::unavailable("transient error")));

        // 2. Commit of the first attempt will fail client-side with AbortedDueToFailedFirstStatement.
        // Runner retries and calls begin_transaction explicitly, which fails with Unavailable.
        // Since we set NeverRetry for begin_retry_policy, it should not retry the BeginTransaction RPC.
        mock.expect_begin_transaction()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |_req| Err(tonic::Status::unavailable("transient error")));

        let (db_client, _server) = setup_db_client(mock).await;

        let runner = db_client
            .read_write_transaction()
            .set_begin_transaction_option(BeginTransactionOption::InlineBegin)
            .with_begin_retry_policy(NeverRetry)
            .build();

        let res = runner
            .run(async |tx| {
                let mut stmt_opts = crate::RequestOptions::default();
                stmt_opts.set_retry_policy(NeverRetry);
                let stmt = Statement::builder("UPDATE Users SET active = true WHERE id = 1")
                    .build()
                    .with_gax_options(stmt_opts);
                let res = tx.execute_update(stmt).await;
                assert!(res.is_err());
                let err = res.unwrap_err();
                assert_eq!(err.status().map(|s| s.code), Some(Code::Unavailable));
                // Swallow the transient error to trigger commit and retry
                Ok(())
            })
            .await;

        assert!(
            res.is_err(),
            "Should fail because explicit BeginTransaction on retry failed with Unavailable and NeverRetry was configured"
        );
        let err = res.unwrap_err();
        assert_eq!(err.status().map(|s| s.code), Some(Code::Unavailable));

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn read_write_transaction_commit_under_deadline_delegates_to_custom_retry_policy()
    -> anyhow::Result<()> {
        let mut mock = create_session_mock();
        mock.expect_begin_transaction().once().returning(|_| {
            Ok(tonic::Response::new(v1::Transaction {
                id: vec![8, 8, 8],
                ..Default::default()
            }))
        });

        // Commit fails with Unavailable. Since we use NeverRetry, it must fail immediately without retry.
        mock.expect_commit()
            .once()
            .returning(|_| Err(tonic::Status::unavailable("transient error")));

        let (db_client, _server) = setup_db_client(mock).await;

        let runner = db_client
            .read_write_transaction()
            .set_begin_transaction_option(BeginTransactionOption::ExplicitBegin)
            .with_commit_retry_policy(NeverRetry)
            .with_transaction_timeout(StdDuration::from_secs(5))
            .build();

        let res = runner.run(async |_tx| Ok(())).await;

        assert!(
            res.is_err(),
            "Should fail because NeverRetry aborted retries"
        );
        let err = res.unwrap_err();
        assert_eq!(
            err.status().map(|s| s.code),
            Some(Code::Unavailable),
            "Error code should be Unavailable"
        );
        Ok(())
    }

    #[tokio_test_no_panics]
    async fn read_write_transaction_commit_timeout_combination() -> anyhow::Result<()> {
        let mut mock = create_session_mock();
        mock.expect_begin_transaction().once().returning(|_| {
            Ok(tonic::Response::new(v1::Transaction {
                id: vec![8, 8, 8],
                ..Default::default()
            }))
        });

        // Assert that the commit attempt timeout of 2 seconds propagates as the gRPC timeout header metadata (approx 2000m/2000000u).
        mock.expect_commit()
            .once()
            .withf(|req| {
                let duration =
                    parse_grpc_timeout(req.metadata()).expect("valid grpc-timeout header");
                assert_eq!(
                    duration,
                    StdDuration::from_secs(2),
                    "Timeout duration should be exactly 2 seconds"
                );
                true
            })
            .returning(|_| {
                Ok(tonic::Response::new(v1::CommitResponse {
                    commit_timestamp: Some(Timestamp {
                        seconds: 999,
                        nanos: 0,
                    }),
                    ..Default::default()
                }))
            });

        let (db_client, _server) = setup_db_client(mock).await;

        let runner = db_client
            .read_write_transaction()
            .set_begin_transaction_option(BeginTransactionOption::ExplicitBegin)
            .with_commit_attempt_timeout(StdDuration::from_secs(2))
            .with_transaction_timeout(StdDuration::from_secs(10))
            .build();

        let res = runner.run(async |_tx| Ok(())).await?;

        assert!(res.commit_response.commit_timestamp.is_some());
        Ok(())
    }

    #[tokio_test_no_panics]
    async fn read_write_transaction_fallback_begin_under_deadline() -> anyhow::Result<()> {
        let mut mock = create_session_mock();
        let mut sequence = mockall::Sequence::new();

        // 1. Initial Attempt: First statement execution fails with Unavailable (transient error)
        mock.expect_execute_sql()
            .once()
            .in_sequence(&mut sequence)
            .withf(|req| {
                matches!(
                    req.get_ref()
                        .transaction
                        .as_ref()
                        .and_then(|t| t.selector.as_ref()),
                    Some(v1::transaction_selector::Selector::Begin(_))
                )
            })
            .returning(move |_req| Err(tonic::Status::unavailable("transient error")));

        // 2. Commit of the first attempt will fail client-side with AbortedDueToFailedFirstStatement.
        // Runner retries and calls begin_transaction explicitly.
        // The attempt timeout must be based on the remaining transaction deadline (approx 5 seconds).
        mock.expect_begin_transaction()
            .once()
            .in_sequence(&mut sequence)
            .withf(|req| {
                let duration =
                    parse_grpc_timeout(req.metadata()).expect("valid grpc-timeout header");
                assert!(
                    duration >= StdDuration::from_millis(4000)
                        && duration <= StdDuration::from_millis(6000),
                    "Retry begin timeout is wrong: {:?}",
                    duration
                );
                true
            })
            .returning(move |_req| {
                Ok(tonic::Response::new(v1::Transaction {
                    id: vec![42],
                    ..Default::default()
                }))
            });

        // 3. Statement retry succeeds on the started transaction
        mock.expect_execute_sql()
            .once()
            .in_sequence(&mut sequence)
            .withf(|req| {
                matches!(
                    req.get_ref()
                        .transaction
                        .as_ref()
                        .and_then(|t| t.selector.as_ref()),
                    Some(v1::transaction_selector::Selector::Id(id)) if id == &vec![42]
                )
            })
            .returning(move |_req| {
                Ok(tonic::Response::new(v1::ResultSet {
                    metadata: Some(v1::ResultSetMetadata {
                        transaction: Some(v1::Transaction {
                            id: vec![42],
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    stats: Some(v1::ResultSetStats {
                        row_count: Some(v1::result_set_stats::RowCount::RowCountExact(1)),
                        ..Default::default()
                    }),
                    ..Default::default()
                }))
            });

        // 4. Commit succeeds
        mock.expect_commit()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |_req| {
                Ok(tonic::Response::new(v1::CommitResponse {
                    commit_timestamp: Some(Timestamp {
                        seconds: 1234,
                        nanos: 0,
                    }),
                    ..Default::default()
                }))
            });

        let (db_client, _server) = setup_db_client(mock).await;

        let runner = db_client
            .read_write_transaction()
            .set_begin_transaction_option(BeginTransactionOption::InlineBegin)
            .with_retry_policy(BasicTransactionRetryPolicy::new().with_max_attempts(2))
            .with_transaction_timeout(StdDuration::from_secs(5))
            .build();

        let attempt = Arc::new(AtomicU32::new(0));
        let att_clone = attempt.clone();
        let res = runner
            .run(async |tx| {
                let current_attempt = att_clone.fetch_add(1, Ordering::SeqCst) + 1;
                let mut query_opts = crate::RequestOptions::default();
                query_opts.set_retry_policy(NeverRetry);
                let stmt = Statement::builder("UPDATE Users SET active = true WHERE id = 1")
                    .build()
                    .with_gax_options(query_opts);
                let res = tx.execute_update(stmt).await;
                if current_attempt == 1 {
                    assert!(res.is_err());
                    let err = res.unwrap_err();
                    assert_eq!(err.status().map(|s| s.code), Some(Code::Unavailable));
                    // Swallow the transient error and attempt commit to trigger AbortedDueToFailedFirstStatement retry
                    Ok(())
                } else {
                    assert_eq!(res.unwrap(), 1);
                    Ok(())
                }
            })
            .await?;

        assert!(res.commit_response.commit_timestamp.is_some());
        Ok(())
    }

    #[tokio_test_no_panics]
    async fn read_write_transaction_commit_fallback_begin_under_deadline() -> anyhow::Result<()> {
        let mut mock = create_session_mock();
        let mut sequence = mockall::Sequence::new();

        // 1. Transaction was never started (empty runner block), so commit falls back to explicit BeginTransaction.
        // Assert fallback explicit BeginTransaction sets timeout based on remaining transaction deadline (approx 5 seconds).
        mock.expect_begin_transaction()
            .once()
            .in_sequence(&mut sequence)
            .withf(|req| {
                let duration =
                    parse_grpc_timeout(req.metadata()).expect("valid grpc-timeout header");
                assert!(
                    duration >= StdDuration::from_millis(4000)
                        && duration <= StdDuration::from_millis(6000),
                    "Fallback begin timeout inside commit is wrong: {:?}",
                    duration
                );
                true
            })
            .returning(move |_req| {
                Ok(tonic::Response::new(v1::Transaction {
                    id: vec![42],
                    ..Default::default()
                }))
            });

        // 2. Commit succeeds
        mock.expect_commit()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |_req| {
                Ok(tonic::Response::new(v1::CommitResponse {
                    commit_timestamp: Some(Timestamp {
                        seconds: 5678,
                        nanos: 0,
                    }),
                    ..Default::default()
                }))
            });

        let (db_client, _server) = setup_db_client(mock).await;

        let runner = db_client
            .read_write_transaction()
            .set_begin_transaction_option(BeginTransactionOption::InlineBegin)
            .with_transaction_timeout(StdDuration::from_secs(5))
            .build();

        let res = runner.run(async |_tx| Ok(())).await?;

        assert!(res.commit_response.commit_timestamp.is_some());
        Ok(())
    }

    #[test]
    fn test_amend_gax_options() {
        // Case 1: No Deadline, LAR disabled
        let mut options = RequestOptions::default();
        options.set_attempt_timeout(StdDuration::from_secs(4));
        amend_gax_options(false, None, &mut options);
        assert_eq!(*options.attempt_timeout(), Some(StdDuration::from_secs(4)));
        assert!(options.retry_policy().is_none());

        // Case 2: No Deadline, LAR enabled
        let mut options = RequestOptions::default();
        amend_gax_options(true, None, &mut options);
        // Verify LAR extension is added
        let headers = options
            .get_extension::<HeaderMap>()
            .expect("HeaderMap extension missing");
        assert_eq!(
            headers
                .get("x-goog-spanner-route-to-leader")
                .unwrap()
                .to_str()
                .unwrap(),
            "true"
        );

        // Case 3: Deadline present, no custom timeout.
        // Since Instant::now() is called inside amend_gax_options slightly after the test's
        // Instant::now() call, the remaining time will be slightly less than 5 seconds.
        // Therefore, we assert that it falls within a very close range.
        let mut options = RequestOptions::default();
        let deadline = Instant::now() + StdDuration::from_secs(5);
        amend_gax_options(false, Some(deadline), &mut options);
        let timeout = options.attempt_timeout().expect("attempt timeout missing");
        assert!(
            timeout >= StdDuration::from_millis(4500) && timeout <= StdDuration::from_millis(5500)
        );
        assert!(
            options.retry_policy().is_some(),
            "retry policy should be wrapped"
        );

        // Case 4: Deadline present, custom timeout shorter than deadline.
        // Since custom timeout is 2s and remaining deadline is 10s, it does not depend
        // on Time/Instant and must be exactly 2s.
        let mut options = RequestOptions::default();
        options.set_attempt_timeout(StdDuration::from_secs(2));
        let deadline = Instant::now() + StdDuration::from_secs(10);
        amend_gax_options(false, Some(deadline), &mut options);
        assert_eq!(*options.attempt_timeout(), Some(StdDuration::from_secs(2)));

        // Case 5: Deadline present, custom timeout longer than deadline.
        // The remaining deadline (approx 2 seconds) is shorter than custom timeout (10s).
        // Due to slight time passing, remaining will be slightly less than 2 seconds.
        let mut options = RequestOptions::default();
        options.set_attempt_timeout(StdDuration::from_secs(10));
        let deadline = Instant::now() + StdDuration::from_secs(2);
        amend_gax_options(false, Some(deadline), &mut options);
        let timeout = options.attempt_timeout().expect("attempt timeout missing");
        assert!(
            timeout >= StdDuration::from_millis(1500) && timeout <= StdDuration::from_millis(2500)
        );
    }

    #[test]
    fn test_transaction_bounded_retry_policy_throttle_delegation() {
        #[derive(Debug)]
        struct ThrottleTestPolicy;
        impl RetryPolicy for ThrottleTestPolicy {
            fn on_error(&self, _state: &RetryState, error: GaxError) -> RetryResult {
                RetryResult::Continue(error)
            }
            fn on_throttle(&self, _state: &RetryState, error: GaxError) -> ThrottleResult {
                ThrottleResult::Exhausted(error)
            }
        }

        let inner = Arc::new(ThrottleTestPolicy);
        let deadline = Instant::now() + StdDuration::from_secs(10);
        let bounded = TransactionBoundedRetryPolicy { inner, deadline };

        let state = RetryState::new(true);
        let status = Status::default()
            .set_code(Code::Unavailable)
            .set_message("error");
        let error = GaxError::service(status);

        let res = bounded.on_throttle(&state, error);
        assert!(matches!(res, ThrottleResult::Exhausted(_)));
    }

    #[test]
    fn test_transaction_bounded_retry_policy_remaining_time_capping() {
        #[derive(Debug)]
        struct RemainingTimeTestPolicy {
            timeout: Option<StdDuration>,
        }
        impl RetryPolicy for RemainingTimeTestPolicy {
            fn on_error(&self, _state: &RetryState, error: GaxError) -> RetryResult {
                RetryResult::Continue(error)
            }
            fn remaining_time(&self, _state: &RetryState) -> Option<StdDuration> {
                self.timeout
            }
        }

        let state = RetryState::new(true);

        // Case A: Inner policy timeout (3s) is shorter than remaining transaction deadline (approx 10s)
        let inner = Arc::new(RemainingTimeTestPolicy {
            timeout: Some(StdDuration::from_secs(3)),
        });
        let deadline = Instant::now() + StdDuration::from_secs(10);
        let bounded = TransactionBoundedRetryPolicy { inner, deadline };
        let remaining = bounded.remaining_time(&state).expect("remaining time");
        assert!(
            remaining >= StdDuration::from_millis(2500)
                && remaining <= StdDuration::from_millis(3500)
        );

        // Case B: Transaction deadline (approx 2s) is shorter than inner policy timeout (10s)
        let inner = Arc::new(RemainingTimeTestPolicy {
            timeout: Some(StdDuration::from_secs(10)),
        });
        let deadline = Instant::now() + StdDuration::from_secs(2);
        let bounded = TransactionBoundedRetryPolicy { inner, deadline };
        let remaining = bounded.remaining_time(&state).expect("remaining time");
        assert!(
            remaining >= StdDuration::from_millis(1500)
                && remaining <= StdDuration::from_millis(2500)
        );

        // Case C: Inner policy timeout is None (returns transaction remaining approx 10s)
        let inner = Arc::new(RemainingTimeTestPolicy { timeout: None });
        let deadline = Instant::now() + StdDuration::from_secs(10);
        let bounded = TransactionBoundedRetryPolicy { inner, deadline };
        let remaining = bounded.remaining_time(&state).expect("remaining time");
        assert!(
            remaining >= StdDuration::from_millis(9500)
                && remaining <= StdDuration::from_millis(10500)
        );
    }

    #[test]
    fn amend_gax_options_for_rollback_defaults_and_custom() {
        // Case 1: Default timeout and retry policy
        let mut options = RequestOptions::default();
        amend_gax_options_for_rollback(false, &mut options);
        let timeout = options.attempt_timeout().expect("attempt timeout missing");
        assert!(
            timeout >= StdDuration::from_millis(28000)
                && timeout <= StdDuration::from_millis(32000),
            "rollback timeout should default to ~30s, got: {:?}",
            timeout
        );
        assert!(
            options.retry_policy().is_some(),
            "retry policy should be wrapped in TransactionBoundedRetryPolicy"
        );
        assert_eq!(
            options.idempotent(),
            Some(true),
            "rollback should be configured as idempotent"
        );

        // Case 2: Custom attempt timeout
        let mut options = RequestOptions::default();
        let custom_timeout = StdDuration::from_secs(2);
        options.set_attempt_timeout(custom_timeout);
        amend_gax_options_for_rollback(false, &mut options);
        let timeout = options.attempt_timeout().expect("attempt timeout missing");
        assert!(
            timeout >= StdDuration::from_millis(1800) && timeout <= StdDuration::from_millis(2200),
            "rollback timeout should match custom timeout (~2s), got: {:?}",
            timeout
        );

        // Case 3: Leader-aware routing enabled
        let mut options = RequestOptions::default();
        amend_gax_options_for_rollback(true, &mut options);
        let headers = options
            .get_extension::<HeaderMap>()
            .expect("HeaderMap extension missing");
        assert_eq!(
            headers
                .get("x-goog-spanner-route-to-leader")
                .expect("route to leader header")
                .to_str()
                .expect("valid str"),
            "true"
        );

        // Case 4: Explicit non-idempotent configuration preserved
        let mut options = RequestOptions::default();
        options.set_idempotency(false);
        amend_gax_options_for_rollback(false, &mut options);
        assert_eq!(
            options.idempotent(),
            Some(false),
            "explicit non-idempotent setting must be preserved"
        );
    }

    #[test]
    fn transaction_bounded_retry_policy_exhausted_on_deadline() {
        #[derive(Debug)]
        struct ContinualPolicy;
        impl RetryPolicy for ContinualPolicy {
            fn on_error(&self, _state: &RetryState, error: GaxError) -> RetryResult {
                RetryResult::Continue(error)
            }
            fn on_throttle(&self, _state: &RetryState, error: GaxError) -> ThrottleResult {
                ThrottleResult::Continue(error)
            }
        }

        let inner: Arc<dyn RetryPolicy> = Arc::new(ContinualPolicy);
        // Expired deadline (in the past)
        let expired_deadline = Instant::now() - StdDuration::from_millis(100);
        let bounded_expired = TransactionBoundedRetryPolicy {
            inner: Arc::clone(&inner),
            deadline: expired_deadline,
        };

        let state = RetryState::new(true);
        let make_error = || {
            let status = Status::default()
                .set_code(Code::Unavailable)
                .set_message("unavailable");
            GaxError::service(status)
        };

        let error_result = bounded_expired.on_error(&state, make_error());
        assert!(
            matches!(error_result, RetryResult::Exhausted(_)),
            "on_error must return Exhausted when deadline has passed"
        );

        let throttle_result = bounded_expired.on_throttle(&state, make_error());
        assert!(
            matches!(throttle_result, ThrottleResult::Exhausted(_)),
            "on_throttle must return Exhausted when deadline has passed"
        );

        // Future deadline: delegates to inner policy
        let future_deadline = Instant::now() + StdDuration::from_secs(60);
        let bounded_future = TransactionBoundedRetryPolicy {
            inner,
            deadline: future_deadline,
        };

        let error_result_future = bounded_future.on_error(&state, make_error());
        assert!(
            matches!(error_result_future, RetryResult::Continue(_)),
            "on_error must delegate to inner when before deadline"
        );

        let throttle_result_future = bounded_future.on_throttle(&state, make_error());
        assert!(
            matches!(throttle_result_future, ThrottleResult::Continue(_)),
            "on_throttle must delegate to inner when before deadline"
        );
    }

    #[tokio_test_no_panics]
    async fn rollback_under_expired_deadline_uses_graceful_timeout() -> anyhow::Result<()> {
        let mut mock = create_session_mock();
        let transaction_id = vec![1, 2, 3, 4];
        let id_clone = transaction_id.clone();

        mock.expect_begin_transaction()
            .once()
            .returning(move |_request| {
                Ok(tonic::Response::new(v1::Transaction {
                    id: id_clone.clone(),
                    ..Default::default()
                }))
            });

        let id_clone_rollback = transaction_id.clone();
        mock.expect_rollback().once().returning(move |request| {
            let duration = parse_grpc_timeout(request.metadata())
                .expect("valid grpc-timeout header on rollback");
            assert!(
                duration >= StdDuration::from_millis(28000)
                    && duration <= StdDuration::from_millis(32000),
                "rollback timeout should use default graceful timeout (~30s), got: {:?}",
                duration
            );
            let inner_request = request.into_inner();
            assert_eq!(inner_request.transaction_id, id_clone_rollback);
            Ok(tonic::Response::new(()))
        });

        let (database_client, _server) = setup_db_client(mock).await;

        // Build a transaction with a short deadline that will expire before rollback
        let deadline = Instant::now() + StdDuration::from_millis(50);
        let transaction = ReadWriteTransactionBuilder::new(database_client)
            .set_begin_transaction_option(BeginTransactionOption::ExplicitBegin)
            .build(Some(deadline))
            .await?;

        // Sleep until transaction deadline has expired
        tokio::time::sleep(StdDuration::from_millis(60)).await;

        // Executing rollback when deadline has already expired should succeed and use graceful timeout
        transaction.rollback().await?;
        Ok(())
    }

    #[tokio_test_no_panics]
    async fn rollback_under_expired_deadline_respects_custom_timeout() -> anyhow::Result<()> {
        let mut mock = create_session_mock();
        let transaction_id = vec![5, 6, 7, 8];
        let id_clone = transaction_id.clone();

        mock.expect_begin_transaction()
            .once()
            .returning(move |_request| {
                Ok(tonic::Response::new(v1::Transaction {
                    id: id_clone.clone(),
                    ..Default::default()
                }))
            });

        let id_clone_rollback = transaction_id.clone();
        mock.expect_rollback().once().returning(move |request| {
            let duration = parse_grpc_timeout(request.metadata())
                .expect("valid grpc-timeout header on rollback");
            assert!(
                duration >= StdDuration::from_millis(1800)
                    && duration <= StdDuration::from_millis(2200),
                "rollback timeout should match configured custom timeout (~2s), got: {:?}",
                duration
            );
            let inner_request = request.into_inner();
            assert_eq!(inner_request.transaction_id, id_clone_rollback);
            Ok(tonic::Response::new(()))
        });

        let (database_client, _server) = setup_db_client(mock).await;

        let mut rollback_options = RequestOptions::default();
        rollback_options.set_attempt_timeout(StdDuration::from_secs(2));

        let deadline = Instant::now() + StdDuration::from_millis(50);
        let transaction = ReadWriteTransactionBuilder::new(database_client)
            .set_begin_transaction_option(BeginTransactionOption::ExplicitBegin)
            .with_rollback_request_options(Some(rollback_options))
            .build(Some(deadline))
            .await?;

        // Sleep until transaction deadline has expired
        tokio::time::sleep(StdDuration::from_millis(60)).await;

        transaction.rollback().await?;
        Ok(())
    }

    #[tokio_test_no_panics]
    async fn rollback_retries_on_unavailable_with_standard_retry_policy() -> anyhow::Result<()> {
        use google_cloud_gax::exponential_backoff::ExponentialBackoffBuilder;

        let mut mock = create_session_mock();
        let transaction_id = vec![10, 20, 30, 40];
        let transaction_id_clone = transaction_id.clone();

        mock.expect_begin_transaction()
            .once()
            .returning(move |_request| {
                Ok(tonic::Response::new(v1::Transaction {
                    id: transaction_id_clone.clone(),
                    ..Default::default()
                }))
            });

        let rollback_attempts = Arc::new(AtomicU32::new(0));
        let attempts_clone = Arc::clone(&rollback_attempts);
        let transaction_id_for_rollback = transaction_id.clone();
        mock.expect_rollback().times(2).returning(move |request| {
            let inner_request = request.into_inner();
            assert_eq!(
                inner_request.transaction_id, transaction_id_for_rollback,
                "transaction id in rollback request must match"
            );
            let attempt = attempts_clone.fetch_add(1, Ordering::SeqCst);
            if attempt == 0 {
                Err(tonic::Status::unavailable("transient unavailable error"))
            } else {
                Ok(tonic::Response::new(()))
            }
        });

        let (database_client, _server) = setup_db_client(mock).await;

        let mut rollback_options = RequestOptions::default();
        rollback_options.set_backoff_policy(
            ExponentialBackoffBuilder::new()
                .with_initial_delay(StdDuration::from_millis(1))
                .with_maximum_delay(StdDuration::from_millis(2))
                .build()
                .expect("valid backoff policy"),
        );

        let transaction = ReadWriteTransactionBuilder::new(database_client)
            .set_begin_transaction_option(BeginTransactionOption::ExplicitBegin)
            .with_rollback_request_options(Some(rollback_options))
            .build(None)
            .await?;

        transaction.rollback().await?;
        assert_eq!(
            rollback_attempts.load(Ordering::SeqCst),
            2,
            "rollback should have retried after transient unavailable error"
        );
        Ok(())
    }

    #[tokio_test_no_panics]
    async fn read_write_transaction_commit_observes_cache_update() -> anyhow::Result<()> {
        use crate::client::{Spanner, SpannerBuilderExt};
        use crate::omni::InstanceType;
        use crate::routing::key_range_cache::RangeMode;
        use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
        use spanner_grpc_mock::google::spanner::v1::{
            CacheUpdate, CommitResponse, Group, Range, Tablet,
        };
        use spanner_grpc_mock::start;

        let mut mock = create_session_mock();
        mock.expect_fetch_cache_update().returning(|_| {
            let (_sender, receiver) = tokio::sync::mpsc::channel(1);
            Ok(tonic::Response::from(receiver))
        });
        mock.expect_begin_transaction().returning(|_| {
            Ok(tonic::Response::new(v1::Transaction {
                id: vec![1, 2, 3],
                ..Default::default()
            }))
        });
        mock.expect_commit().returning(|_| {
            let cache_update = CacheUpdate {
                database_id: 88,
                group: vec![Group {
                    group_uid: 99,
                    leader_index: 0,
                    tablets: vec![Tablet {
                        server_address: "node-99.spanner.internal:15000".to_string(),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                range: vec![Range {
                    group_uid: 99,
                    start_key: b"rw1".to_vec(),
                    limit_key: b"rw9".to_vec(),
                    ..Default::default()
                }],
                ..Default::default()
            };
            Ok(tonic::Response::new(CommitResponse {
                commit_timestamp: Some(prost_types::Timestamp {
                    seconds: 1000,
                    nanos: 0,
                }),
                cache_update: Some(cache_update),
                ..Default::default()
            }))
        });

        let (address, _server) = start("127.0.0.1:0", mock).await?;
        let client = Spanner::builder()
            .with_endpoint(address)
            .with_instance_type(InstanceType::Omni)
            .with_credentials(Anonymous::new().build())
            .build()
            .await?;

        let db_client = client
            .database_client("db")
            .with_location_aware_routing(true)
            .build()
            .await?;
        let runner = db_client.read_write_transaction().build();
        runner.run(|_transaction| async move { Ok(()) }).await?;

        assert_eq!(db_client.database_id(), Some(88));
        let router = db_client
            .location_router()
            .expect("location router present");
        let found = router
            .key_range_cache()
            .find_range(b"rw5", &[], RangeMode::CoveringSplit);
        assert!(found.is_some(), "range covering 'rw5' should be cached");
        assert_eq!(found.expect("range present").group_uid, 99);

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn read_write_transaction_batch_dml_observes_cache_update() -> anyhow::Result<()> {
        use super::ReadWriteTransaction;
        use crate::client::{Spanner, SpannerBuilderExt};
        use crate::omni::InstanceType;
        use crate::routing::key_range_cache::RangeMode;
        use crate::statement::Statement;
        use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
        use spanner_grpc_mock::google::spanner::v1::result_set_stats::RowCount;
        use spanner_grpc_mock::google::spanner::v1::{
            CacheUpdate, CommitResponse, ExecuteBatchDmlResponse, Group, Range, ResultSet,
            ResultSetStats, Tablet,
        };
        use spanner_grpc_mock::start;

        let mut mock = create_session_mock();
        mock.expect_fetch_cache_update().returning(|_| {
            let (_sender, receiver) = tokio::sync::mpsc::channel(1);
            Ok(tonic::Response::from(receiver))
        });
        mock.expect_begin_transaction().returning(|_| {
            Ok(tonic::Response::new(v1::Transaction {
                id: vec![1, 2, 3],
                ..Default::default()
            }))
        });
        mock.expect_execute_batch_dml().returning(|_| {
            let cache_update = CacheUpdate {
                database_id: 77,
                group: vec![Group {
                    group_uid: 88,
                    leader_index: 0,
                    tablets: vec![Tablet {
                        server_address: "node-88.spanner.internal:15000".to_string(),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                range: vec![Range {
                    group_uid: 88,
                    start_key: b"batch1".to_vec(),
                    limit_key: b"batch9".to_vec(),
                    ..Default::default()
                }],
                ..Default::default()
            };
            Ok(tonic::Response::new(ExecuteBatchDmlResponse {
                result_sets: vec![ResultSet {
                    metadata: Some(v1::ResultSetMetadata {
                        transaction: Some(v1::Transaction {
                            id: vec![1, 2, 3],
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    cache_update: Some(cache_update),
                    stats: Some(ResultSetStats {
                        row_count: Some(RowCount::RowCountExact(1)),
                        ..Default::default()
                    }),
                    ..Default::default()
                }],
                status: Some(spanner_grpc_mock::google::rpc::Status {
                    code: 0,
                    ..Default::default()
                }),
                ..Default::default()
            }))
        });
        mock.expect_commit().returning(|_| {
            Ok(tonic::Response::new(CommitResponse {
                commit_timestamp: Some(prost_types::Timestamp {
                    seconds: 1000,
                    nanos: 0,
                }),
                ..Default::default()
            }))
        });

        let (address, _server) = start("127.0.0.1:0", mock).await?;
        let client = Spanner::builder()
            .with_endpoint(address)
            .with_instance_type(InstanceType::Omni)
            .with_credentials(Anonymous::new().build())
            .build()
            .await?;

        let db_client = client
            .database_client("db")
            .with_location_aware_routing(true)
            .build()
            .await?;
        let runner = db_client.read_write_transaction().build();
        runner
            .run(|transaction: ReadWriteTransaction| async move {
                transaction
                    .execute_batch_update(vec![Statement::from("UPDATE Users SET active = true")])
                    .await?;
                Ok(())
            })
            .await?;

        assert_eq!(db_client.database_id(), Some(77));
        let router = db_client
            .location_router()
            .expect("location router present");
        let found = router
            .key_range_cache()
            .find_range(b"batch5", &[], RangeMode::CoveringSplit);
        assert!(found.is_some(), "range covering 'batch5' should be cached");
        assert_eq!(found.expect("range present").group_uid, 88);

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn read_write_transaction_affinity_preserved_and_shared() -> crate::Result<()> {
        use crate::read_only_transaction::tests::setup_select1_with_transaction_id;
        use crate::result_set::tests::adapt;
        use crate::statement::Statement;
        use gaxi::grpc::tonic::Response;

        let mut mock = create_session_mock();
        mock.expect_execute_streaming_sql().once().returning(|_| {
            Ok(Response::from(adapt([Ok(
                setup_select1_with_transaction_id(vec![1, 2, 3]),
            )])))
        });
        let (db_client, _server) = setup_db_client(mock).await;

        let affinity = Arc::new(TransactionAffinity::new_read_write());
        let builder =
            ReadWriteTransactionBuilder::new(db_client).with_affinity(Arc::clone(&affinity));

        let transaction = builder.build(None).await?;
        assert!(
            transaction
                .affinity()
                .expect("affinity present")
                .is_read_write(),
            "ReadWriteTransaction affinity must be ReadWrite"
        );
        assert_eq!(
            transaction
                .affinity()
                .expect("affinity present")
                .pinned_entry_id(),
            None,
            "Initial pinned entry ID should be None"
        );

        transaction
            .affinity()
            .expect("affinity present")
            .compare_and_set_entry_id(0, 1)
            .expect("pin entry");
        assert_eq!(
            affinity.pinned_entry_id(),
            Some(1),
            "Affinity handle passed to builder must observe the pinned channel ID"
        );

        let result_set = transaction
            .execute_query(Statement::builder("SELECT 1").build())
            .await?;
        assert_eq!(
            result_set.affinity().pinned_entry_id(),
            Some(1),
            "ResultSet generated from ReadWrite transaction must share the same pinned affinity"
        );

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn read_write_transaction_default_affinity_created() -> crate::Result<()> {
        let mock = create_session_mock();
        let (db_client, _server) = setup_db_client(mock).await;

        let builder = ReadWriteTransactionBuilder::new(db_client);
        let transaction = builder.build(None).await?;
        assert!(
            transaction
                .affinity()
                .expect("affinity present")
                .is_read_write(),
            "Default ReadWriteTransaction affinity must be ReadWrite"
        );
        assert_eq!(
            transaction
                .affinity()
                .expect("affinity present")
                .pinned_entry_id(),
            None,
            "Default affinity should start unpinned"
        );

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn read_write_transaction_releases_rw_guard_on_commit_rollback_and_drop()
    -> anyhow::Result<()> {
        use crate::channel_pool::affinity::ChannelTarget;
        use crate::client::Spanner;
        use crate::read_only_transaction::tests::setup_select1_with_transaction_id;
        use crate::result_set::tests::adapt;
        use crate::statement::Statement;
        use gaxi::grpc::tonic::Response;
        use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
        use spanner_grpc_mock::google::spanner::v1 as mock_v1;
        use spanner_grpc_mock::start;

        let mut mock = create_session_mock();
        mock.expect_execute_streaming_sql().returning(|_| {
            Ok(Response::from(adapt([Ok(
                setup_select1_with_transaction_id(vec![1, 2, 3]),
            )])))
        });
        mock.expect_commit()
            .returning(|_| Ok(Response::new(mock_v1::CommitResponse::default())));
        mock.expect_rollback().returning(|_| Ok(Response::new(())));

        let (address, _server) = start("127.0.0.1:0", mock).await?;
        let spanner = Spanner::builder()
            .with_endpoint(address)
            .with_credentials(Anonymous::new().build())
            .build()
            .await?;

        let database_client = spanner
            .database_client("projects/p/instances/i/databases/d")
            .build()
            .await?;

        let entries = spanner.channel_pool().active_entries();

        // 1. Commit releases the active R/W guard on the pinned ChannelEntry
        {
            let transaction = ReadWriteTransactionBuilder::new(database_client.clone())
                .build(None)
                .await?;
            let mut result_set = transaction
                .execute_query(Statement::builder("SELECT 1").build())
                .await?;
            while let Some(row) = result_set.next().await {
                let _ = row?;
            }

            let pinned_id = transaction
                .affinity()
                .expect("affinity present")
                .pinned_entry_id()
                .expect("entry should be pinned after statement execution");
            let pinned_entry = entries
                .iter()
                .find(|entry| entry.id == pinned_id)
                .expect("pinned entry must exist in active entries");

            assert_eq!(
                pinned_entry.active_rw_count(),
                1,
                "pinned entry active_rw_count must be 1 during active transaction"
            );

            transaction.commit().await?;

            assert_eq!(
                pinned_entry.active_rw_count(),
                0,
                "pinned entry active_rw_count must drop to 0 immediately after commit"
            );
        }

        // 2. Rollback releases the active R/W guard on the pinned ChannelEntry
        {
            let transaction = ReadWriteTransactionBuilder::new(database_client.clone())
                .build(None)
                .await?;
            let mut result_set = transaction
                .execute_query(Statement::builder("SELECT 1").build())
                .await?;
            while let Some(row) = result_set.next().await {
                let _ = row?;
            }

            let pinned_id = transaction
                .affinity()
                .expect("affinity present")
                .pinned_entry_id()
                .expect("entry should be pinned after statement execution");
            let pinned_entry = entries
                .iter()
                .find(|entry| entry.id == pinned_id)
                .expect("pinned entry must exist in active entries");

            assert_eq!(
                pinned_entry.active_rw_count(),
                1,
                "pinned entry active_rw_count must be 1 during active transaction"
            );

            transaction.rollback().await?;

            assert_eq!(
                pinned_entry.active_rw_count(),
                0,
                "pinned entry active_rw_count must drop to 0 immediately after rollback"
            );
        }

        // 3. Dropping an uncommitted transaction releases the active R/W guard on the pinned ChannelEntry
        {
            let pinned_entry;
            {
                let transaction = ReadWriteTransactionBuilder::new(database_client.clone())
                    .build(None)
                    .await?;
                let mut result_set = transaction
                    .execute_query(Statement::builder("SELECT 1").build())
                    .await?;
                while let Some(row) = result_set.next().await {
                    let _ = row?;
                }

                let pinned_id = transaction
                    .affinity()
                    .expect("affinity present")
                    .pinned_entry_id()
                    .expect("entry should be pinned after statement execution");
                pinned_entry = entries
                    .iter()
                    .find(|entry| entry.id == pinned_id)
                    .cloned()
                    .expect("pinned entry must exist in active entries");

                assert_eq!(
                    pinned_entry.active_rw_count(),
                    1,
                    "pinned entry active_rw_count must be 1 during active transaction"
                );
            }

            assert_eq!(
                pinned_entry.active_rw_count(),
                0,
                "pinned entry active_rw_count must drop to 0 when uncommitted transaction is dropped"
            );
        }

        // 4. Dropping a transaction before a transaction ID is established also releases the active R/W guard
        {
            let pinned_entry;
            {
                let affinity = Arc::new(TransactionAffinity::new_read_write());
                let pinned_id = entries.first().expect("at least one active entry").id;
                affinity
                    .compare_and_set_entry_id(0, pinned_id)
                    .expect("pin entry");
                let target = ChannelTarget::from(&affinity);
                let lease = spanner.pick_channel_for_target(&target);
                affinity.ensure_rw_guard(&lease);

                pinned_entry = entries
                    .iter()
                    .find(|entry| entry.id == pinned_id)
                    .cloned()
                    .expect("pinned entry must exist");
                assert_eq!(
                    pinned_entry.active_rw_count(),
                    1,
                    "pinned entry active_rw_count must be 1 with active guard"
                );

                let transaction = ReadWriteTransactionBuilder::new(database_client.clone())
                    .with_affinity(affinity)
                    .build(None)
                    .await?;

                // Dropping transaction without ever starting a statement (no transaction ID)
                drop(transaction);
            }

            assert_eq!(
                pinned_entry.active_rw_count(),
                0,
                "pinned entry active_rw_count must drop to 0 when transaction without ID is dropped"
            );
        }

        Ok(())
    }

    async fn setup_db_client_with_dynamic_pool(
        mock: MockSpanner,
        initial_channels: usize,
        max_channels: usize,
    ) -> (DatabaseClient, Spanner, JoinHandle<()>) {
        use crate::channel_pool::DynamicChannelPoolConfig;
        use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
        use spanner_grpc_mock::start;

        let (address, server) = start("127.0.0.1:0", mock)
            .await
            .expect("Failed to start mock server");

        let dynamic_config = DynamicChannelPoolConfig::new()
            .with_initial_channels(initial_channels)
            .with_min_channels(initial_channels)
            .with_max_channels(max_channels);

        let spanner = Spanner::builder()
            .with_endpoint(address)
            .with_credentials(Anonymous::new().build())
            .with_channel_pool(dynamic_config)
            .build()
            .await
            .expect("Failed to build client");

        let database_client = spanner
            .database_client("projects/p/instances/i/databases/d")
            .build()
            .await
            .expect("Failed to create DatabaseClient");

        (database_client, spanner, server)
    }

    fn assert_all_rpcs_use_same_channel(addresses: &[SocketAddr], expected_count: usize) {
        assert_eq!(
            addresses.len(),
            expected_count,
            "Expected {expected_count} RPCs executed"
        );
        let first_address = addresses[0];
        for (index, address) in addresses.iter().enumerate() {
            assert_eq!(
                *address, first_address,
                "RPC at index {index} must use the same channel as first RPC ({first_address})"
            );
        }
    }

    #[tokio_test_no_panics]
    async fn read_write_transaction_affinity_under_dynamic_pool_inline_begin() -> anyhow::Result<()>
    {
        run_read_write_transaction_affinity_under_dynamic_pool(BeginTransactionOption::InlineBegin)
            .await
    }

    #[tokio_test_no_panics]
    async fn read_write_transaction_affinity_under_dynamic_pool_explicit_begin()
    -> anyhow::Result<()> {
        run_read_write_transaction_affinity_under_dynamic_pool(
            BeginTransactionOption::ExplicitBegin,
        )
        .await
    }

    async fn run_read_write_transaction_affinity_under_dynamic_pool(
        begin_transaction_option: BeginTransactionOption,
    ) -> anyhow::Result<()> {
        use crate::batch_dml::BatchDml;
        use crate::statement::Statement;
        use prost_types::Timestamp;

        let mut mock = create_session_mock();
        let remote_addresses = Arc::new(Mutex::new(Vec::new()));

        if begin_transaction_option == BeginTransactionOption::ExplicitBegin {
            let remote_addresses_clone = Arc::clone(&remote_addresses);
            mock.expect_begin_transaction()
                .once()
                .returning(move |request| {
                    remote_addresses_clone.lock().expect("mutex lock").push(
                        request
                            .remote_addr()
                            .expect("remote_addr should be available"),
                    );
                    Ok(Response::new(v1::Transaction {
                        id: vec![1, 2, 3],
                        ..Default::default()
                    }))
                });
        }

        // 1. Query: execute_streaming_sql
        let remote_addresses_clone = Arc::clone(&remote_addresses);
        mock.expect_execute_streaming_sql()
            .once()
            .returning(move |request| {
                remote_addresses_clone.lock().expect("mutex lock").push(
                    request
                        .remote_addr()
                        .expect("remote_addr should be available"),
                );
                let mut metadata = v1::ResultSetMetadata {
                    row_type: Some(v1::StructType { fields: vec![] }),
                    ..Default::default()
                };
                if begin_transaction_option == BeginTransactionOption::InlineBegin {
                    metadata.transaction = Some(v1::Transaction {
                        id: vec![1, 2, 3],
                        ..Default::default()
                    });
                }
                let partial_result_set = v1::PartialResultSet {
                    metadata: Some(metadata),
                    ..Default::default()
                };
                Ok(Response::from(adapt([Ok(partial_result_set)])))
            });

        // 2. DML: execute_sql
        let remote_addresses_clone = Arc::clone(&remote_addresses);
        mock.expect_execute_sql().once().returning(move |request| {
            remote_addresses_clone.lock().expect("mutex lock").push(
                request
                    .remote_addr()
                    .expect("remote_addr should be available"),
            );
            Ok(Response::new(v1::ResultSet {
                metadata: Some(v1::ResultSetMetadata {
                    row_type: Some(v1::StructType { fields: vec![] }),
                    ..Default::default()
                }),
                stats: Some(v1::ResultSetStats {
                    row_count: Some(v1::result_set_stats::RowCount::RowCountExact(1)),
                    ..Default::default()
                }),
                ..Default::default()
            }))
        });

        // 3. Batch DML: execute_batch_dml
        let remote_addresses_clone = Arc::clone(&remote_addresses);
        mock.expect_execute_batch_dml()
            .once()
            .returning(move |request| {
                remote_addresses_clone.lock().expect("mutex lock").push(
                    request
                        .remote_addr()
                        .expect("remote_addr should be available"),
                );
                Ok(Response::new(v1::ExecuteBatchDmlResponse {
                    result_sets: vec![v1::ResultSet {
                        stats: Some(v1::ResultSetStats {
                            row_count: Some(v1::result_set_stats::RowCount::RowCountExact(1)),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }],
                    ..Default::default()
                }))
            });

        // 4. Commit: commit
        let remote_addresses_clone = Arc::clone(&remote_addresses);
        mock.expect_commit().once().returning(move |request| {
            remote_addresses_clone.lock().expect("mutex lock").push(
                request
                    .remote_addr()
                    .expect("remote_addr should be available"),
            );
            Ok(Response::new(v1::CommitResponse {
                commit_timestamp: Some(Timestamp {
                    seconds: 1000,
                    nanos: 0,
                }),
                ..Default::default()
            }))
        });

        let (database_client, _spanner, _server) =
            setup_db_client_with_dynamic_pool(mock, 4, 8).await;

        let transaction = ReadWriteTransactionBuilder::new(database_client)
            .set_begin_transaction_option(begin_transaction_option)
            .build(None)
            .await?;

        let mut result_set = transaction
            .execute_query(Statement::builder("SELECT 1").build())
            .await?;
        let _ = result_set.next().await.transpose()?;

        let count = transaction
            .execute_update("UPDATE Users SET Name = 'Alice' WHERE Id = 1")
            .await?;
        assert_eq!(count, 1, "Expected 1 row updated");

        let batch_result = transaction
            .execute_batch_update(
                BatchDml::builder()
                    .add_statement("UPDATE Users SET Name = 'Bob' WHERE Id = 2")
                    .build(),
            )
            .await?;
        assert_eq!(batch_result.len(), 1, "Expected 1 batch statement executed");

        let commit_result = transaction.commit().await?;
        assert_eq!(
            commit_result
                .commit_timestamp
                .expect("timestamp should be present")
                .seconds(),
            1000,
            "Commit timestamp mismatch"
        );

        let addresses = remote_addresses.lock().expect("mutex lock");
        let expected_rpc_count =
            if begin_transaction_option == BeginTransactionOption::ExplicitBegin {
                5
            } else {
                4
            };
        assert_all_rpcs_use_same_channel(&addresses, expected_rpc_count);

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn read_write_transaction_affinity_preserved_across_dynamic_pool_scale_up()
    -> anyhow::Result<()> {
        use crate::channel_pool::entry::ChannelEntry;
        use crate::statement::Statement;
        use prost_types::Timestamp;

        let mut mock = create_session_mock();
        let remote_addresses = Arc::new(Mutex::new(Vec::new()));

        // Statement 1: Query (Inline begin)
        let remote_addresses_clone = Arc::clone(&remote_addresses);
        mock.expect_execute_streaming_sql()
            .once()
            .returning(move |request| {
                remote_addresses_clone.lock().expect("mutex lock").push(
                    request
                        .remote_addr()
                        .expect("remote_addr should be available"),
                );
                let metadata = v1::ResultSetMetadata {
                    row_type: Some(v1::StructType { fields: vec![] }),
                    transaction: Some(v1::Transaction {
                        id: vec![1, 2, 3],
                        ..Default::default()
                    }),
                    ..Default::default()
                };
                let partial_result_set = v1::PartialResultSet {
                    metadata: Some(metadata),
                    ..Default::default()
                };
                Ok(Response::from(adapt([Ok(partial_result_set)])))
            });

        // Statement 2: Update
        let remote_addresses_clone = Arc::clone(&remote_addresses);
        mock.expect_execute_sql().once().returning(move |request| {
            remote_addresses_clone.lock().expect("mutex lock").push(
                request
                    .remote_addr()
                    .expect("remote_addr should be available"),
            );
            Ok(Response::new(v1::ResultSet {
                metadata: Some(v1::ResultSetMetadata {
                    row_type: Some(v1::StructType { fields: vec![] }),
                    ..Default::default()
                }),
                stats: Some(v1::ResultSetStats {
                    row_count: Some(v1::result_set_stats::RowCount::RowCountExact(1)),
                    ..Default::default()
                }),
                ..Default::default()
            }))
        });

        // Commit
        let remote_addresses_clone = Arc::clone(&remote_addresses);
        mock.expect_commit().once().returning(move |request| {
            remote_addresses_clone.lock().expect("mutex lock").push(
                request
                    .remote_addr()
                    .expect("remote_addr should be available"),
            );
            Ok(Response::new(v1::CommitResponse {
                commit_timestamp: Some(Timestamp {
                    seconds: 1000,
                    nanos: 0,
                }),
                ..Default::default()
            }))
        });

        // Start with 2 channels, pool can scale up to 8
        let (database_client, spanner, _server) =
            setup_db_client_with_dynamic_pool(mock, 2, 8).await;

        let transaction = ReadWriteTransactionBuilder::new(database_client)
            .build(None)
            .await?;

        // Execute Statement 1
        let mut result_set = transaction
            .execute_query(Statement::builder("SELECT 1").build())
            .await?;
        let _ = result_set.next().await.transpose()?;

        let pinned_entry_id = transaction
            .affinity()
            .expect("affinity must be present")
            .pinned_entry_id();
        assert!(
            pinned_entry_id.is_some(),
            "Transaction affinity must be pinned after first statement"
        );

        // Simulate dynamic scale-up mid-transaction: add new channels to active_entries
        {
            let default_channel = spanner
                .channel_pool()
                .default_channel()
                .expect("default channel must exist");
            let mut active_write = spanner
                .channel_pool()
                .inner
                .active_entries
                .write()
                .expect("lock active_entries");
            let current_len = active_write.len();
            let next_entry_id = current_len as u64 + 1;
            // Add 2 new channels, doubling active channels from 2 to 4.
            // Note: `logical_channel_id` is 1-based (1..=max_channels), so the next available
            // logical slots are `current_len + 1` and `current_len + 2`.
            active_write.push(Arc::new(ChannelEntry::new(
                next_entry_id,
                current_len + 1,
                default_channel.clone(),
            )));
            active_write.push(Arc::new(ChannelEntry::new(
                next_entry_id + 1,
                current_len + 2,
                default_channel,
            )));
            assert_eq!(
                active_write.len(),
                4,
                "Pool must now have 4 active channels"
            );
        }

        // Execute Statement 2: Update
        let count = transaction
            .execute_update("UPDATE Users SET Name = 'Alice' WHERE Id = 1")
            .await?;
        assert_eq!(count, 1, "Expected 1 row updated");

        // Commit
        let _ = transaction.commit().await?;

        // Verify that all RPCs used the exact same channel remote address
        let addresses = remote_addresses.lock().expect("mutex lock");
        assert_all_rpcs_use_same_channel(&addresses, 3);

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn read_write_transaction_affinity_preserved_during_channel_draining()
    -> anyhow::Result<()> {
        use crate::channel_pool::entry::ChannelState;
        use crate::channel_pool::scaler::sweep_draining_channels;
        use crate::statement::Statement;
        use prost_types::Timestamp;
        use std::time::Duration;

        let mut mock = create_session_mock();
        let remote_addresses = Arc::new(Mutex::new(Vec::new()));

        let remote_addresses_clone = Arc::clone(&remote_addresses);
        mock.expect_execute_streaming_sql()
            .once()
            .returning(move |request| {
                remote_addresses_clone.lock().expect("mutex lock").push(
                    request
                        .remote_addr()
                        .expect("remote_addr should be available"),
                );
                let metadata = v1::ResultSetMetadata {
                    row_type: Some(v1::StructType { fields: vec![] }),
                    transaction: Some(v1::Transaction {
                        id: vec![1, 2, 3],
                        ..Default::default()
                    }),
                    ..Default::default()
                };
                let partial_result_set = v1::PartialResultSet {
                    metadata: Some(metadata),
                    ..Default::default()
                };
                Ok(Response::from(adapt([Ok(partial_result_set)])))
            });

        let remote_addresses_clone = Arc::clone(&remote_addresses);
        mock.expect_execute_sql().once().returning(move |request| {
            remote_addresses_clone.lock().expect("mutex lock").push(
                request
                    .remote_addr()
                    .expect("remote_addr should be available"),
            );
            Ok(Response::new(v1::ResultSet {
                metadata: Some(v1::ResultSetMetadata {
                    row_type: Some(v1::StructType { fields: vec![] }),
                    ..Default::default()
                }),
                stats: Some(v1::ResultSetStats {
                    row_count: Some(v1::result_set_stats::RowCount::RowCountExact(1)),
                    ..Default::default()
                }),
                ..Default::default()
            }))
        });

        let remote_addresses_clone = Arc::clone(&remote_addresses);
        mock.expect_commit().once().returning(move |request| {
            remote_addresses_clone.lock().expect("mutex lock").push(
                request
                    .remote_addr()
                    .expect("remote_addr should be available"),
            );
            Ok(Response::new(v1::CommitResponse {
                commit_timestamp: Some(Timestamp {
                    seconds: 1000,
                    nanos: 0,
                }),
                ..Default::default()
            }))
        });

        let (database_client, spanner, _server) =
            setup_db_client_with_dynamic_pool(mock, 4, 8).await;

        let transaction = ReadWriteTransactionBuilder::new(database_client)
            .build(None)
            .await?;

        // Statement 1: Query pins the channel
        let mut result_set = transaction
            .execute_query(Statement::builder("SELECT 1").build())
            .await?;
        let _ = result_set.next().await.transpose()?;
        drop(result_set);

        let pinned_entry_id = transaction
            .affinity()
            .expect("affinity must be present")
            .pinned_entry_id()
            .expect("channel must be pinned after query");

        // Locate pinned channel entry and verify active_rw_count is 1
        let pinned_entry = {
            let active_guard = spanner
                .channel_pool()
                .inner
                .active_entries
                .read()
                .expect("lock active_entries");
            active_guard
                .iter()
                .find(|entry| entry.id == pinned_entry_id)
                .map(Arc::clone)
                .expect("pinned entry must exist in active_entries")
        };
        assert_eq!(
            pinned_entry.active_rw_count(),
            1,
            "Pinned entry must have active_rw_count == 1 while transaction is open"
        );

        // Move the pinned channel to Draining (simulating scale-down)
        {
            let mut active_write = spanner
                .channel_pool()
                .inner
                .active_entries
                .write()
                .expect("lock active_entries");
            active_write.retain(|entry| entry.id != pinned_entry_id);
            drop(active_write);

            pinned_entry.set_state(ChannelState::Draining);
            let mut draining_write = spanner
                .channel_pool()
                .inner
                .draining_entries
                .write()
                .expect("lock draining_entries");
            draining_write.push(Arc::clone(&pinned_entry));
        }

        // Run sweep_draining_channels. Because active_rw_count is 1, Channel X must NOT be closed!
        sweep_draining_channels(&spanner.channel_pool().inner, Duration::from_millis(0));
        assert!(
            pinned_entry.is_draining(),
            "Pinned entry must remain in Draining state while R/W transaction is active"
        );
        assert_eq!(
            spanner.channel_pool().draining_channel_count(),
            1,
            "Draining channel count must still be 1"
        );

        // Execute Statement 2: Update while the channel is draining!
        let count = transaction
            .execute_update("UPDATE Users SET Name = 'Alice' WHERE Id = 1")
            .await?;
        assert_eq!(count, 1, "Expected 1 row updated");

        // Commit while the channel is draining!
        let _ = transaction.commit().await?;

        // Verify that all 3 RPCs executed on the same channel
        let addresses = remote_addresses.lock().expect("mutex lock");
        assert_all_rpcs_use_same_channel(&addresses, 3);

        // Now that the transaction is done and dropped, active_rw_count must be 0
        assert_eq!(
            pinned_entry.active_rw_count(),
            0,
            "active_rw_count must drop to 0 after transaction completes"
        );

        // Sweeping now must close the channel and remove it from draining entries!
        sweep_draining_channels(&spanner.channel_pool().inner, Duration::from_millis(0));
        assert!(
            pinned_entry.is_closed(),
            "Draining entry must transition to Closed once transaction completes"
        );
        assert_eq!(
            spanner.channel_pool().draining_channel_count(),
            0,
            "Draining channels must be empty after sweep"
        );

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn read_write_transaction_affinity_preserved_on_rollback() -> anyhow::Result<()> {
        use crate::statement::Statement;

        let mut mock = create_session_mock();
        let remote_addresses = Arc::new(Mutex::new(Vec::new()));

        let remote_addresses_clone = Arc::clone(&remote_addresses);
        mock.expect_execute_streaming_sql()
            .once()
            .returning(move |request| {
                remote_addresses_clone.lock().expect("mutex lock").push(
                    request
                        .remote_addr()
                        .expect("remote_addr should be available"),
                );
                let metadata = v1::ResultSetMetadata {
                    row_type: Some(v1::StructType { fields: vec![] }),
                    transaction: Some(v1::Transaction {
                        id: vec![1, 2, 3],
                        ..Default::default()
                    }),
                    ..Default::default()
                };
                let partial_result_set = v1::PartialResultSet {
                    metadata: Some(metadata),
                    ..Default::default()
                };
                Ok(Response::from(adapt([Ok(partial_result_set)])))
            });

        let remote_addresses_clone = Arc::clone(&remote_addresses);
        mock.expect_rollback().once().returning(move |request| {
            remote_addresses_clone.lock().expect("mutex lock").push(
                request
                    .remote_addr()
                    .expect("remote_addr should be available"),
            );
            let request = request.into_inner();
            assert_eq!(
                request.transaction_id,
                vec![1, 2, 3],
                "Transaction ID mismatch on rollback"
            );
            Ok(Response::new(()))
        });

        let (database_client, _spanner, _server) =
            setup_db_client_with_dynamic_pool(mock, 4, 8).await;

        let transaction = ReadWriteTransactionBuilder::new(database_client)
            .build(None)
            .await?;

        // Statement 1: Query pins the channel
        let mut result_set = transaction
            .execute_query(Statement::builder("SELECT 1").build())
            .await?;
        let _ = result_set.next().await.transpose()?;

        // Explicit rollback
        transaction.rollback().await?;

        let addresses = remote_addresses.lock().expect("mutex lock");
        assert_all_rpcs_use_same_channel(&addresses, 2);

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn read_write_transaction_drop_guard_clears_location_router_affinity_on_drop()
    -> anyhow::Result<()> {
        use crate::omni::InstanceType;
        use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
        use spanner_grpc_mock::google::spanner::v1::Transaction;
        use spanner_grpc_mock::start;
        use tokio::sync::mpsc::channel;
        use tokio::time::timeout;

        let mut mock = create_session_mock();
        mock.expect_fetch_cache_update().returning(|_| {
            let (_sender, receiver) = channel(1);
            Ok(tonic::Response::from(receiver))
        });
        mock.expect_begin_transaction().returning(|_| {
            Ok(tonic::Response::new(Transaction {
                id: vec![42, 42],
                read_timestamp: None,
                ..Default::default()
            }))
        });

        let (rollback_sender, mut rollback_receiver) = channel(1);
        mock.expect_rollback().once().returning(move |request| {
            let _ = rollback_sender.try_send(request.into_inner().transaction_id);
            Ok(Response::new(()))
        });

        let (address, _server) = start("127.0.0.1:0", mock).await?;
        let client = Spanner::builder()
            .with_endpoint(address)
            .with_instance_type(InstanceType::Omni)
            .with_credentials(Anonymous::new().build())
            .build()
            .await?;

        let database_client = client
            .database_client(
                "projects/test-project/instances/test-instance/databases/test-database",
            )
            .with_location_aware_routing(true)
            .build()
            .await?;

        let router = database_client
            .location_router()
            .expect("location router must be present when location-aware routing is enabled");

        let transaction = ReadWriteTransactionBuilder::new(database_client.clone())
            .set_begin_transaction_option(BeginTransactionOption::ExplicitBegin)
            .build(None)
            .await?;

        let transaction_id = vec![42, 42];
        router.record_transaction_affinity(&transaction_id, "node-1.spanner.internal:15000");
        assert_eq!(
            router.get_transaction_affinity(&transaction_id).as_deref(),
            Some("node-1.spanner.internal:15000"),
            "location router affinity must be recorded"
        );
        assert_eq!(
            router.affinity_count(),
            1,
            "location router affinity count should be 1"
        );

        // Dropping transaction without commit or rollback must trigger RAII drop guard.
        drop(transaction);

        let rolled_back_transaction_id =
            timeout(StdDuration::from_secs(2), rollback_receiver.recv())
                .await
                .expect("rollback must complete within timeout")
                .expect("rollback receiver must receive transaction ID");
        assert_eq!(
            rolled_back_transaction_id, transaction_id,
            "drop guard must rollback the active transaction ID"
        );

        let mut affinity_cleared = false;
        for _ in 0..100 {
            tokio::task::yield_now().await;
            if router.get_transaction_affinity(&transaction_id).is_none() {
                affinity_cleared = true;
                break;
            }
        }
        assert!(
            affinity_cleared,
            "ReadWriteTransactionDropGuard must clear location router affinity after rollback completes"
        );
        assert_eq!(
            router.affinity_count(),
            0,
            "location router affinity count must be 0 after drop"
        );

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn read_write_transaction_drop_clears_location_router_affinity_when_disarmed()
    -> anyhow::Result<()> {
        use crate::omni::InstanceType;
        use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
        use spanner_grpc_mock::google::spanner::v1::Transaction;
        use spanner_grpc_mock::start;
        use tokio::sync::mpsc::channel;

        let mut mock = create_session_mock();
        mock.expect_fetch_cache_update().returning(|_| {
            let (_sender, receiver) = channel(1);
            Ok(tonic::Response::from(receiver))
        });
        mock.expect_begin_transaction().returning(|_| {
            Ok(Response::new(Transaction {
                id: vec![53, 53],
                read_timestamp: None,
                ..Default::default()
            }))
        });
        mock.expect_rollback().never();

        let (address, _server) = start("127.0.0.1:0", mock).await?;
        let client = Spanner::builder()
            .with_endpoint(address)
            .with_instance_type(InstanceType::Omni)
            .with_credentials(Anonymous::new().build())
            .build()
            .await?;

        let database_client = client
            .database_client(
                "projects/test-project/instances/test-instance/databases/test-database",
            )
            .with_location_aware_routing(true)
            .build()
            .await?;

        let router = database_client
            .location_router()
            .expect("location router must be present when location-aware routing is enabled");

        let transaction = ReadWriteTransactionBuilder::new(database_client.clone())
            .set_begin_transaction_option(BeginTransactionOption::ExplicitBegin)
            .build(None)
            .await?;

        let transaction_id = vec![53, 53];
        router.record_transaction_affinity(&transaction_id, "node-1.spanner.internal:15000");
        assert_eq!(
            router.get_transaction_affinity(&transaction_id).as_deref(),
            Some("node-1.spanner.internal:15000"),
            "location router affinity must be recorded"
        );
        assert_eq!(
            router.affinity_count(),
            1,
            "location router affinity count should be 1"
        );

        // Disarm rollback (as happens during commit or explicit rollback)
        transaction.disarm_rollback();

        // Dropping transaction must still clear location routing affinity immediately
        drop(transaction);

        assert_eq!(
            router.get_transaction_affinity(&transaction_id),
            None,
            "location router affinity must be cleared on drop even when rollback is disarmed"
        );
        assert_eq!(
            router.affinity_count(),
            0,
            "location router affinity count must be 0 after drop"
        );

        Ok(())
    }

    #[test]
    fn read_write_transaction_drop_guard_outside_tokio_runtime_does_not_panic() {
        use crate::omni::InstanceType;
        use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
        use spanner_grpc_mock::google::spanner::v1::Transaction;
        use spanner_grpc_mock::start;
        use std::thread;
        use tokio::runtime::Builder as RuntimeBuilder;
        use tokio::sync::mpsc::channel;

        let runtime = RuntimeBuilder::new_current_thread()
            .enable_all()
            .build()
            .expect("build runtime");
        let (database_client, transaction) = runtime.block_on(async {
            let mut mock = create_session_mock();
            mock.expect_fetch_cache_update().returning(|_| {
                let (_sender, receiver) = channel(1);
                Ok(tonic::Response::from(receiver))
            });
            mock.expect_begin_transaction().once().returning(|_| {
                Ok(Response::new(Transaction {
                    id: vec![77, 78],
                    ..Default::default()
                }))
            });
            let (address, _server) = start("127.0.0.1:0", mock).await.expect("start mock");
            let client = Spanner::builder()
                .with_endpoint(address)
                .with_instance_type(InstanceType::Omni)
                .with_credentials(Anonymous::new().build())
                .build()
                .await
                .expect("build client");
            let database_client = client
                .database_client("projects/p/instances/i/databases/d")
                .with_location_aware_routing(true)
                .build()
                .await
                .expect("build db client");
            let router = database_client
                .location_router()
                .expect("location router must be present");
            let transaction_id = vec![77, 78];
            router.record_transaction_affinity(&transaction_id, "node-1.spanner.internal:15000");

            let transaction = ReadWriteTransactionBuilder::new(database_client.clone())
                .set_begin_transaction_option(BeginTransactionOption::ExplicitBegin)
                .build(None)
                .await
                .expect("build transaction");
            (database_client, transaction)
        });
        // Shut down the runtime completely.
        drop(runtime);

        // Dropping the transaction after the runtime has shut down must NOT panic,
        // and must reliably clear LocationRouter affinity via AffinityCleanupGuard.
        let join_result = thread::spawn(move || {
            drop(transaction);
        })
        .join();

        assert!(
            join_result.is_ok(),
            "dropping transaction after runtime shutdown must not panic"
        );

        let router = database_client
            .location_router()
            .expect("location router must be present");
        assert_eq!(
            router.get_transaction_affinity(&[77, 78]),
            None,
            "location router affinity must be cleared when transaction is dropped after runtime shutdown"
        );
        assert_eq!(
            router.affinity_count(),
            0,
            "affinity count must be 0 after drop"
        );
    }

    #[test]
    fn read_write_transaction_drop_guard_without_handle_outside_runtime_clears_affinity_safely() {
        use crate::model::TransactionSelector;
        use bytes::Bytes;
        use std::thread;
        use tokio::runtime::Builder as RuntimeBuilder;

        let runtime = RuntimeBuilder::new_current_thread()
            .enable_all()
            .build()
            .expect("build runtime");
        let (database_client, selector, affinity) = runtime.block_on(async {
            let mock = create_session_mock();
            let (database_client, _server) = setup_db_client(mock).await;
            let transaction_id = Bytes::from_static(b"drop-no-handle-test");
            let selector = ReadContextTransactionSelector::Fixed(
                TransactionSelector::new().set_id(transaction_id),
                None,
            );
            let affinity = Arc::new(TransactionAffinity::new_read_write());
            (database_client, selector, affinity)
        });
        drop(runtime);

        // Construct guard with tokio_handle: None on a thread without Tokio runtime.
        let join_result = thread::spawn(move || {
            let guard = ReadWriteTransactionDropGuard::new(
                database_client,
                "test-session".to_string(),
                selector,
                Some(affinity),
                None,
                None,
            );
            drop(guard);
        })
        .join();

        assert!(
            join_result.is_ok(),
            "dropping guard without handle outside Tokio runtime must not panic"
        );
    }

    #[tokio_test_no_panics]
    async fn read_write_transaction_background_rollback_on_drop_handles_rpc_failure()
    -> anyhow::Result<()> {
        use crate::omni::InstanceType;
        use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
        use spanner_grpc_mock::google::spanner::v1::Transaction;
        use spanner_grpc_mock::start;
        use tokio::sync::mpsc::channel;
        use tokio::time::timeout;

        let mut mock = create_session_mock();
        mock.expect_fetch_cache_update().returning(|_| {
            let (_sender, receiver) = channel(1);
            Ok(tonic::Response::from(receiver))
        });
        mock.expect_begin_transaction().once().returning(|_| {
            Ok(Response::new(Transaction {
                id: vec![88, 89],
                read_timestamp: None,
                ..Default::default()
            }))
        });

        let (rollback_sender, mut rollback_receiver) = channel(1);
        mock.expect_rollback().once().returning(move |request| {
            let _ = rollback_sender.try_send(request.into_inner().transaction_id);
            Err(tonic::Status::permission_denied(
                "simulated background rollback permission failure",
            ))
        });

        let (address, _server) = start("127.0.0.1:0", mock).await?;
        let client = Spanner::builder()
            .with_endpoint(address)
            .with_instance_type(InstanceType::Omni)
            .with_credentials(Anonymous::new().build())
            .build()
            .await?;

        let database_client = client
            .database_client(
                "projects/test-project/instances/test-instance/databases/test-database",
            )
            .with_location_aware_routing(true)
            .build()
            .await?;

        let router = database_client
            .location_router()
            .expect("location router must be present when location-aware routing is enabled");

        let transaction = ReadWriteTransactionBuilder::new(database_client.clone())
            .set_begin_transaction_option(BeginTransactionOption::ExplicitBegin)
            .build(None)
            .await?;

        let transaction_id = vec![88, 89];
        router.record_transaction_affinity(&transaction_id, "node-1.spanner.internal:15000");

        // Dropping transaction triggers background rollback which encounters RPC error.
        drop(transaction);

        let rolled_back_transaction_id =
            timeout(StdDuration::from_secs(2), rollback_receiver.recv())
                .await
                .expect("rollback must complete within timeout")
                .expect("rollback receiver must receive transaction ID");
        assert_eq!(
            rolled_back_transaction_id, transaction_id,
            "drop guard must attempt to rollback the active transaction ID"
        );

        let mut affinity_cleared = false;
        for _ in 0..100 {
            tokio::task::yield_now().await;
            if router.get_transaction_affinity(&transaction_id).is_none() {
                affinity_cleared = true;
                break;
            }
        }
        assert!(
            affinity_cleared,
            "location router affinity must be cleared even when rollback RPC fails"
        );
        assert_eq!(
            router.affinity_count(),
            0,
            "location router affinity count must be 0 after failed rollback"
        );

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn read_write_transaction_drop_guard_preserves_custom_rollback_gax_options()
    -> anyhow::Result<()> {
        use crate::omni::InstanceType;
        use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
        use spanner_grpc_mock::google::spanner::v1::Transaction;
        use spanner_grpc_mock::start;
        use std::time::Duration as StdDuration;
        use tokio::sync::mpsc::channel;
        use tokio::time::timeout;

        let mut mock = create_session_mock();
        mock.expect_fetch_cache_update().returning(|_| {
            let (_sender, receiver) = channel(1);
            Ok(tonic::Response::from(receiver))
        });
        mock.expect_begin_transaction().once().returning(|_| {
            Ok(Response::new(Transaction {
                id: vec![91, 92],
                ..Default::default()
            }))
        });

        let (timeout_sender, mut timeout_receiver) = channel(1);
        mock.expect_rollback().once().returning(move |request| {
            let timeout_header = request
                .metadata()
                .get("grpc-timeout")
                .and_then(|header_value| header_value.to_str().ok())
                .map(|header_str| header_str.to_string());
            let _ = timeout_sender.try_send(timeout_header);
            Ok(Response::new(()))
        });

        let (address, _server) = start("127.0.0.1:0", mock).await?;
        let client = Spanner::builder()
            .with_endpoint(address)
            .with_instance_type(InstanceType::Omni)
            .with_credentials(Anonymous::new().build())
            .build()
            .await?;

        let database_client = client
            .database_client("projects/p/instances/i/databases/d")
            .with_location_aware_routing(true)
            .build()
            .await?;

        let custom_timeout = StdDuration::from_secs(12);
        let mut custom_options = RequestOptions::default();
        custom_options.set_attempt_timeout(custom_timeout);

        let transaction = ReadWriteTransactionBuilder::new(database_client)
            .set_begin_transaction_option(BeginTransactionOption::ExplicitBegin)
            .with_rollback_request_options(Some(custom_options))
            .build(None)
            .await?;

        // Dropping transaction triggers background rollback with custom options
        drop(transaction);

        let timeout_header = timeout(StdDuration::from_secs(2), timeout_receiver.recv())
            .await
            .expect("rollback must complete within timeout")
            .expect("timeout receiver must receive header");

        assert!(
            timeout_header.is_some(),
            "grpc-timeout header must be sent on background rollback RPC"
        );
        let header_str = timeout_header.expect("header present");
        assert!(
            header_str == "12000000u" || header_str.ends_with('u') || header_str.ends_with('S'),
            "grpc-timeout header must reflect the 12s attempt timeout: {header_str}"
        );

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn read_write_transaction_commit_clears_location_router_affinity() -> anyhow::Result<()> {
        use crate::omni::InstanceType;
        use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
        use spanner_grpc_mock::google::spanner::v1::{CommitResponse, Transaction};
        use spanner_grpc_mock::start;

        let mut mock = create_session_mock();
        mock.expect_fetch_cache_update().returning(|_| {
            let (_sender, receiver) = tokio::sync::mpsc::channel(1);
            Ok(tonic::Response::from(receiver))
        });
        mock.expect_begin_transaction().returning(|_| {
            Ok(tonic::Response::new(Transaction {
                id: vec![43, 43],
                read_timestamp: None,
                ..Default::default()
            }))
        });
        mock.expect_commit()
            .returning(|_| Ok(tonic::Response::new(CommitResponse::default())));

        let (address, _server) = start("127.0.0.1:0", mock).await?;
        let client = Spanner::builder()
            .with_endpoint(address)
            .with_instance_type(InstanceType::Omni)
            .with_credentials(Anonymous::new().build())
            .build()
            .await?;

        let database_client = client
            .database_client(
                "projects/test-project/instances/test-instance/databases/test-database",
            )
            .with_location_aware_routing(true)
            .build()
            .await?;

        let router = database_client
            .location_router()
            .expect("location router must be present when location-aware routing is enabled");

        let transaction = ReadWriteTransactionBuilder::new(database_client.clone())
            .set_begin_transaction_option(BeginTransactionOption::ExplicitBegin)
            .build(None)
            .await?;

        let transaction_id = vec![43, 43];
        router.record_transaction_affinity(&transaction_id, "node-1.spanner.internal:15000");
        assert_eq!(
            router.get_transaction_affinity(&transaction_id).as_deref(),
            Some("node-1.spanner.internal:15000"),
            "location router affinity must be recorded before commit"
        );
        assert_eq!(
            router.affinity_count(),
            1,
            "location router affinity count must be 1 before commit"
        );

        transaction.commit().await?;

        assert_eq!(
            router.get_transaction_affinity(&transaction_id),
            None,
            "location router affinity must be cleared after commit"
        );
        assert_eq!(
            router.affinity_count(),
            0,
            "location router affinity count must be 0 after commit"
        );

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn read_write_transaction_rollback_clears_location_router_affinity() -> anyhow::Result<()>
    {
        use crate::omni::InstanceType;
        use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
        use spanner_grpc_mock::google::spanner::v1::Transaction;
        use spanner_grpc_mock::start;

        let mut mock = create_session_mock();
        mock.expect_fetch_cache_update().returning(|_| {
            let (_sender, receiver) = tokio::sync::mpsc::channel(1);
            Ok(tonic::Response::from(receiver))
        });
        mock.expect_begin_transaction().returning(|_| {
            Ok(tonic::Response::new(Transaction {
                id: vec![44, 44],
                read_timestamp: None,
                ..Default::default()
            }))
        });
        mock.expect_rollback()
            .returning(|_| Ok(tonic::Response::new(())));

        let (address, _server) = start("127.0.0.1:0", mock).await?;
        let client = Spanner::builder()
            .with_endpoint(address)
            .with_instance_type(InstanceType::Omni)
            .with_credentials(Anonymous::new().build())
            .build()
            .await?;

        let database_client = client
            .database_client(
                "projects/test-project/instances/test-instance/databases/test-database",
            )
            .with_location_aware_routing(true)
            .build()
            .await?;

        let router = database_client
            .location_router()
            .expect("location router must be present when location-aware routing is enabled");

        let transaction = ReadWriteTransactionBuilder::new(database_client.clone())
            .set_begin_transaction_option(BeginTransactionOption::ExplicitBegin)
            .build(None)
            .await?;

        let transaction_id = vec![44, 44];
        router.record_transaction_affinity(&transaction_id, "node-1.spanner.internal:15000");
        assert_eq!(
            router.get_transaction_affinity(&transaction_id).as_deref(),
            Some("node-1.spanner.internal:15000"),
            "location router affinity must be recorded before rollback"
        );
        assert_eq!(
            router.affinity_count(),
            1,
            "location router affinity count must be 1 before rollback"
        );

        transaction.rollback().await?;

        assert_eq!(
            router.get_transaction_affinity(&transaction_id),
            None,
            "location router affinity must be cleared after rollback"
        );
        assert_eq!(
            router.affinity_count(),
            0,
            "location router affinity count must be 0 after rollback"
        );

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn read_write_transaction_drop_guard_clears_location_router_affinity_on_commit_failure()
    -> anyhow::Result<()> {
        use crate::omni::InstanceType;
        use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
        use spanner_grpc_mock::google::spanner::v1::Transaction;
        use spanner_grpc_mock::start;

        let mut mock = create_session_mock();
        mock.expect_fetch_cache_update().returning(|_| {
            let (_sender, receiver) = tokio::sync::mpsc::channel(1);
            Ok(tonic::Response::from(receiver))
        });
        mock.expect_begin_transaction().returning(|_| {
            Ok(tonic::Response::new(Transaction {
                id: vec![45, 45],
                read_timestamp: None,
                ..Default::default()
            }))
        });
        mock.expect_commit()
            .returning(|_| Err(tonic::Status::invalid_argument("simulated commit failure")));
        mock.expect_rollback().never();

        let (address, _server) = start("127.0.0.1:0", mock).await?;
        let client = Spanner::builder()
            .with_endpoint(address)
            .with_instance_type(InstanceType::Omni)
            .with_credentials(Anonymous::new().build())
            .build()
            .await?;

        let database_client = client
            .database_client(
                "projects/test-project/instances/test-instance/databases/test-database",
            )
            .with_location_aware_routing(true)
            .build()
            .await?;

        let router = database_client
            .location_router()
            .expect("location router must be present when location-aware routing is enabled");

        let transaction = ReadWriteTransactionBuilder::new(database_client.clone())
            .set_begin_transaction_option(BeginTransactionOption::ExplicitBegin)
            .build(None)
            .await?;

        let transaction_id = vec![45, 45];
        router.record_transaction_affinity(&transaction_id, "node-1.spanner.internal:15000");
        assert_eq!(
            router.get_transaction_affinity(&transaction_id).as_deref(),
            Some("node-1.spanner.internal:15000"),
            "location router affinity must be recorded before failed commit"
        );
        assert_eq!(
            router.affinity_count(),
            1,
            "location router affinity count must be 1 before failed commit"
        );

        let commit_result = transaction.commit().await;
        assert!(
            commit_result.is_err(),
            "commit must return Err when commit RPC fails"
        );

        // When commit fails, the consumed transaction is dropped,
        // and its LocationRoutingDropGuard must clear the affinity.
        assert_eq!(
            router.get_transaction_affinity(&transaction_id),
            None,
            "location router affinity must be cleared when commit fails"
        );
        assert_eq!(
            router.affinity_count(),
            0,
            "location router affinity count must be 0 after failed commit drop"
        );

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn read_write_transaction_inline_begin_drop_guard_clears_location_router_affinity()
    -> anyhow::Result<()> {
        use crate::omni::InstanceType;
        use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
        use prost_types::Timestamp;
        use spanner_grpc_mock::google::spanner::v1::result_set_stats::RowCount;
        use spanner_grpc_mock::google::spanner::v1::{
            ResultSet, ResultSetMetadata, ResultSetStats, StructType, Transaction,
        };
        use spanner_grpc_mock::start;
        use tokio::sync::mpsc::channel;
        use tokio::time::timeout;

        let mut mock = create_session_mock();
        mock.expect_fetch_cache_update().returning(|_| {
            let (_sender, receiver) = channel(1);
            Ok(tonic::Response::from(receiver))
        });

        mock.expect_execute_sql().once().returning(|_| {
            Ok(tonic::Response::new(ResultSet {
                metadata: Some(ResultSetMetadata {
                    row_type: Some(StructType { fields: vec![] }),
                    transaction: Some(Transaction {
                        id: vec![46, 46],
                        read_timestamp: Some(Timestamp {
                            seconds: 100,
                            nanos: 0,
                        }),
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
                stats: Some(ResultSetStats {
                    row_count: Some(RowCount::RowCountExact(1)),
                    ..Default::default()
                }),
                ..Default::default()
            }))
        });

        let (rollback_sender, mut rollback_receiver) = channel(1);
        mock.expect_rollback().once().returning(move |request| {
            let _ = rollback_sender.try_send(request.into_inner().transaction_id);
            Ok(Response::new(()))
        });

        let (address, _server) = start("127.0.0.1:0", mock).await?;
        let client = Spanner::builder()
            .with_endpoint(address)
            .with_instance_type(InstanceType::Omni)
            .with_credentials(Anonymous::new().build())
            .build()
            .await?;

        let database_client = client
            .database_client(
                "projects/test-project/instances/test-instance/databases/test-database",
            )
            .with_location_aware_routing(true)
            .build()
            .await?;

        let router = database_client
            .location_router()
            .expect("location router must be present when location-aware routing is enabled");

        let transaction = ReadWriteTransactionBuilder::new(database_client.clone())
            .set_begin_transaction_option(BeginTransactionOption::InlineBegin)
            .build(None)
            .await?;

        let update_count = transaction
            .execute_update("UPDATE Users SET Name = 'Alice' WHERE Id = 1")
            .await?;
        assert_eq!(update_count, 1, "update count should be 1");

        let transaction_id = vec![46, 46];
        router.record_transaction_affinity(&transaction_id, "node-1.spanner.internal:15000");
        assert_eq!(
            router.get_transaction_affinity(&transaction_id).as_deref(),
            Some("node-1.spanner.internal:15000"),
            "location router affinity must be recorded"
        );
        assert_eq!(
            router.affinity_count(),
            1,
            "location router affinity count should be 1"
        );

        // Dropping inline transaction after starting must clear the affinity and rollback
        drop(transaction);

        let rolled_back_transaction_id =
            timeout(StdDuration::from_secs(2), rollback_receiver.recv())
                .await
                .expect("rollback must complete within timeout")
                .expect("rollback receiver must receive transaction ID");
        assert_eq!(
            rolled_back_transaction_id, transaction_id,
            "drop guard must rollback the inline begin transaction ID"
        );

        let mut affinity_cleared = false;
        for _ in 0..100 {
            tokio::task::yield_now().await;
            if router.get_transaction_affinity(&transaction_id).is_none() {
                affinity_cleared = true;
                break;
            }
        }
        assert!(
            affinity_cleared,
            "ReadWriteTransactionDropGuard must clear location router affinity after rollback completes"
        );
        assert_eq!(
            router.affinity_count(),
            0,
            "location router affinity count must be 0 after drop"
        );

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn read_write_transaction_unstarted_inline_begin_drop_is_safe() -> anyhow::Result<()> {
        use crate::omni::InstanceType;
        use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
        use spanner_grpc_mock::start;

        let mut mock = create_session_mock();
        mock.expect_fetch_cache_update().returning(|_| {
            let (_sender, receiver) = tokio::sync::mpsc::channel(1);
            Ok(tonic::Response::from(receiver))
        });
        mock.expect_rollback().never();

        let (address, _server) = start("127.0.0.1:0", mock).await?;
        let client = Spanner::builder()
            .with_endpoint(address)
            .with_instance_type(InstanceType::Omni)
            .with_credentials(Anonymous::new().build())
            .build()
            .await?;

        let database_client = client
            .database_client(
                "projects/test-project/instances/test-instance/databases/test-database",
            )
            .with_location_aware_routing(true)
            .build()
            .await?;

        let router = database_client
            .location_router()
            .expect("location router must be present when location-aware routing is enabled");

        let transaction = ReadWriteTransactionBuilder::new(database_client.clone())
            .set_begin_transaction_option(BeginTransactionOption::InlineBegin)
            .build(None)
            .await?;

        // Drop without executing any statements
        drop(transaction);

        assert_eq!(
            router.affinity_count(),
            0,
            "location router affinity count must be 0 after dropping unstarted inline transaction"
        );

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn read_write_transaction_multi_clone_drop_semantics() -> anyhow::Result<()> {
        use crate::omni::InstanceType;
        use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
        use spanner_grpc_mock::google::spanner::v1::Transaction;
        use spanner_grpc_mock::start;
        use tokio::sync::mpsc::channel;
        use tokio::time::timeout;

        let mut mock = create_session_mock();
        mock.expect_fetch_cache_update().returning(|_| {
            let (_sender, receiver) = channel(1);
            Ok(tonic::Response::from(receiver))
        });
        mock.expect_begin_transaction().returning(|_| {
            Ok(tonic::Response::new(Transaction {
                id: vec![50, 50],
                read_timestamp: None,
                ..Default::default()
            }))
        });

        let (rollback_sender, mut rollback_receiver) = channel(1);
        mock.expect_rollback().once().returning(move |request| {
            let _ = rollback_sender.try_send(request.into_inner().transaction_id);
            Ok(Response::new(()))
        });

        let (address, _server) = start("127.0.0.1:0", mock).await?;
        let client = Spanner::builder()
            .with_endpoint(address)
            .with_instance_type(InstanceType::Omni)
            .with_credentials(Anonymous::new().build())
            .build()
            .await?;

        let database_client = client
            .database_client(
                "projects/test-project/instances/test-instance/databases/test-database",
            )
            .with_location_aware_routing(true)
            .build()
            .await?;

        let router = database_client
            .location_router()
            .expect("location router must be present when location-aware routing is enabled");

        let transaction = ReadWriteTransactionBuilder::new(database_client.clone())
            .set_begin_transaction_option(BeginTransactionOption::ExplicitBegin)
            .build(None)
            .await?;

        let transaction_id = vec![50, 50];
        router.record_transaction_affinity(&transaction_id, "node-1.spanner.internal:15000");
        assert_eq!(
            router.get_transaction_affinity(&transaction_id).as_deref(),
            Some("node-1.spanner.internal:15000"),
            "location router affinity must be recorded"
        );
        assert_eq!(
            router.affinity_count(),
            1,
            "location router affinity count should be 1"
        );

        let clone_first = transaction.clone();
        let clone_second = transaction.clone();

        // Dropping the original transaction handle should not clear affinity because clones remain
        drop(transaction);
        assert_eq!(
            router.get_transaction_affinity(&transaction_id).as_deref(),
            Some("node-1.spanner.internal:15000"),
            "location router affinity must remain recorded while clones exist"
        );
        assert_eq!(
            router.affinity_count(),
            1,
            "location router affinity count should still be 1"
        );
        assert!(
            rollback_receiver.try_recv().is_err(),
            "rollback must not be dispatched while clones exist"
        );

        // Dropping the first clone should not clear affinity
        drop(clone_first);
        assert_eq!(
            router.get_transaction_affinity(&transaction_id).as_deref(),
            Some("node-1.spanner.internal:15000"),
            "location router affinity must remain recorded while one clone remains"
        );
        assert_eq!(
            router.affinity_count(),
            1,
            "location router affinity count should still be 1"
        );
        assert!(
            rollback_receiver.try_recv().is_err(),
            "rollback must not be dispatched while one clone remains"
        );

        // Dropping the final clone must invoke drop guard cleanup and clear affinity
        drop(clone_second);

        let rolled_back_transaction_id =
            timeout(StdDuration::from_secs(2), rollback_receiver.recv())
                .await
                .expect("rollback must complete within timeout")
                .expect("rollback receiver must receive transaction ID");
        assert_eq!(
            rolled_back_transaction_id, transaction_id,
            "drop guard on final clone must rollback the active transaction ID"
        );

        let mut affinity_cleared = false;
        for _ in 0..100 {
            tokio::task::yield_now().await;
            if router.get_transaction_affinity(&transaction_id).is_none() {
                affinity_cleared = true;
                break;
            }
        }
        assert!(
            affinity_cleared,
            "drop guard on final clone must clear location router affinity after rollback"
        );
        assert_eq!(
            router.affinity_count(),
            0,
            "location router affinity count must be 0 after all transaction clones dropped"
        );

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn read_write_transaction_commit_precommit_token_retry_preserves_location_router_affinity()
    -> anyhow::Result<()> {
        use crate::omni::InstanceType;
        use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
        use spanner_grpc_mock::google::spanner::v1::commit_response::MultiplexedSessionRetry;
        use spanner_grpc_mock::google::spanner::v1::{
            CommitResponse, MultiplexedSessionPrecommitToken, Transaction,
        };
        use spanner_grpc_mock::start;
        use std::sync::Arc;
        use std::sync::atomic::{AtomicU32, Ordering};

        let mut mock = create_session_mock();
        mock.expect_fetch_cache_update().returning(|_| {
            let (_sender, receiver) = tokio::sync::mpsc::channel(1);
            Ok(tonic::Response::from(receiver))
        });
        mock.expect_begin_transaction().returning(|_| {
            Ok(tonic::Response::new(Transaction {
                id: vec![51, 51],
                read_timestamp: None,
                ..Default::default()
            }))
        });

        let commit_calls = Arc::new(AtomicU32::new(0));
        let commit_calls_clone = Arc::clone(&commit_calls);
        mock.expect_commit().times(2).returning(move |_| {
            let call_number = commit_calls_clone.fetch_add(1, Ordering::SeqCst);
            if call_number == 0 {
                // First commit response returns precommit token requesting retry
                Ok(tonic::Response::new(CommitResponse {
                    multiplexed_session_retry: Some(MultiplexedSessionRetry::PrecommitToken(
                        MultiplexedSessionPrecommitToken {
                            precommit_token: vec![101, 102],
                            seq_num: 1,
                        },
                    )),
                    ..Default::default()
                }))
            } else {
                // Second commit response succeeds without precommit token
                Ok(tonic::Response::new(CommitResponse {
                    commit_timestamp: Some(prost_types::Timestamp {
                        seconds: 1700000000,
                        nanos: 0,
                    }),
                    ..Default::default()
                }))
            }
        });

        let (address, _server) = start("127.0.0.1:0", mock).await?;
        let client = Spanner::builder()
            .with_endpoint(address)
            .with_instance_type(InstanceType::Omni)
            .with_credentials(Anonymous::new().build())
            .build()
            .await?;

        let database_client = client
            .database_client(
                "projects/test-project/instances/test-instance/databases/test-database",
            )
            .with_location_aware_routing(true)
            .build()
            .await?;

        let router = database_client
            .location_router()
            .expect("location router must be present when location-aware routing is enabled");

        let transaction = ReadWriteTransactionBuilder::new(database_client.clone())
            .set_begin_transaction_option(BeginTransactionOption::ExplicitBegin)
            .build(None)
            .await?;

        let transaction_id = vec![51, 51];
        router.record_transaction_affinity(&transaction_id, "node-1.spanner.internal:15000");
        assert_eq!(
            router.get_transaction_affinity(&transaction_id).as_deref(),
            Some("node-1.spanner.internal:15000"),
            "location router affinity must be recorded before commit"
        );
        assert_eq!(
            router.affinity_count(),
            1,
            "location router affinity count should be 1"
        );

        let response = transaction.commit().await?;
        assert_eq!(
            commit_calls.load(Ordering::SeqCst),
            2,
            "commit must have been executed twice due to precommit_token"
        );
        assert!(
            response.commit_timestamp.is_some(),
            "final commit response must contain commit timestamp"
        );
        assert_eq!(
            router.get_transaction_affinity(&transaction_id),
            None,
            "final commit completion must clear location router transaction affinity"
        );
        assert_eq!(
            router.affinity_count(),
            0,
            "location router affinity count must be 0 after successful commit"
        );

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn read_write_transaction_drop_guard_clears_location_router_affinity_when_rollback_rpc_fails()
    -> anyhow::Result<()> {
        use crate::omni::InstanceType;
        use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
        use spanner_grpc_mock::google::spanner::v1::Transaction;
        use spanner_grpc_mock::start;

        let mut mock = create_session_mock();
        mock.expect_fetch_cache_update().returning(|_| {
            let (_sender, receiver) = tokio::sync::mpsc::channel(1);
            Ok(tonic::Response::from(receiver))
        });
        mock.expect_begin_transaction().returning(|_| {
            Ok(tonic::Response::new(Transaction {
                id: vec![52, 52],
                read_timestamp: None,
                ..Default::default()
            }))
        });
        mock.expect_rollback()
            .returning(|_| Err(tonic::Status::internal("simulated rollback failure")));

        let (address, _server) = start("127.0.0.1:0", mock).await?;
        let client = Spanner::builder()
            .with_endpoint(address)
            .with_instance_type(InstanceType::Omni)
            .with_credentials(Anonymous::new().build())
            .build()
            .await?;

        let database_client = client
            .database_client(
                "projects/test-project/instances/test-instance/databases/test-database",
            )
            .with_location_aware_routing(true)
            .build()
            .await?;

        let router = database_client
            .location_router()
            .expect("location router must be present when location-aware routing is enabled");

        let transaction = ReadWriteTransactionBuilder::new(database_client.clone())
            .set_begin_transaction_option(BeginTransactionOption::ExplicitBegin)
            .build(None)
            .await?;

        let transaction_id = vec![52, 52];
        router.record_transaction_affinity(&transaction_id, "node-1.spanner.internal:15000");
        assert_eq!(
            router.get_transaction_affinity(&transaction_id).as_deref(),
            Some("node-1.spanner.internal:15000"),
            "location router affinity must be recorded before rollback"
        );
        assert_eq!(
            router.affinity_count(),
            1,
            "location router affinity count should be 1"
        );

        let rollback_result = transaction.rollback().await;
        assert!(
            rollback_result.is_err(),
            "rollback call must return error when RPC fails"
        );

        // Even though rollback RPC returned an error, the drop guard must have cleared affinity
        assert_eq!(
            router.get_transaction_affinity(&transaction_id),
            None,
            "drop guard must clear location router affinity even when rollback RPC fails"
        );
        assert_eq!(
            router.affinity_count(),
            0,
            "location router affinity count must be 0 after failed rollback"
        );

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn inlined_begin_batch_dml_partial_abort_preserves_retry_info_and_retries_inline_begin()
    -> anyhow::Result<()> {
        use crate::retry_delay::{
            ProtoRetryInfo, RETRY_INFO_TYPE_URL, extract_retry_delay_from_error,
        };
        use prost::Message;

        let mut mock = create_session_mock();
        let mut sequence = mockall::Sequence::new();

        let retry_info = ProtoRetryInfo {
            retry_delay: Some(prost_types::Duration {
                seconds: 0,
                nanos: 1,
            }),
        };
        let mut retry_bytes = Vec::new();
        retry_info
            .encode(&mut retry_bytes)
            .expect("encoding ProtoRetryInfo should succeed");

        // Attempt 1: ExecuteBatchDml with InlineBegin returns statement 1 ok (with tx id [42])
        // and statement 2 Aborted (with RetryInfo).
        mock.expect_execute_batch_dml()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |request| {
                let inner = request.into_inner();
                assert!(
                    matches!(
                        inner.transaction.and_then(|t| t.selector),
                        Some(v1::transaction_selector::Selector::Begin(_))
                    ),
                    "First attempt must use InlineBegin"
                );
                Ok(Response::new(v1::ExecuteBatchDmlResponse {
                    result_sets: vec![v1::ResultSet {
                        metadata: Some(v1::ResultSetMetadata {
                            transaction: Some(v1::Transaction {
                                id: vec![42],
                                ..Default::default()
                            }),
                            ..Default::default()
                        }),
                        stats: Some(v1::ResultSetStats {
                            row_count: Some(v1::result_set_stats::RowCount::RowCountExact(1)),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }],
                    status: Some(RpcStatus {
                        code: tonic::Code::Aborted as i32,
                        message: "transaction aborted on statement 2".into(),
                        details: vec![prost_types::Any {
                            type_url: RETRY_INFO_TYPE_URL.to_string(),
                            value: retry_bytes.clone(),
                        }],
                    }),
                    ..Default::default()
                }))
            });

        // Attempt 2: Because the failure was Aborted, the runner
        // retains InlineBegin (no explicit BeginTransaction RPC) and succeeds!
        mock.expect_execute_batch_dml()
            .once()
            .in_sequence(&mut sequence)
            .returning(|request| {
                let inner = request.into_inner();
                assert!(
                    matches!(
                        inner.transaction.and_then(|t| t.selector),
                        Some(v1::transaction_selector::Selector::Begin(_))
                    ),
                    "Second attempt after Aborted must still use InlineBegin"
                );
                Ok(Response::new(v1::ExecuteBatchDmlResponse {
                    result_sets: vec![
                        v1::ResultSet {
                            metadata: Some(v1::ResultSetMetadata {
                                transaction: Some(v1::Transaction {
                                    id: vec![43],
                                    ..Default::default()
                                }),
                                ..Default::default()
                            }),
                            stats: Some(v1::ResultSetStats {
                                row_count: Some(v1::result_set_stats::RowCount::RowCountExact(1)),
                                ..Default::default()
                            }),
                            ..Default::default()
                        },
                        v1::ResultSet {
                            stats: Some(v1::ResultSetStats {
                                row_count: Some(v1::result_set_stats::RowCount::RowCountExact(1)),
                                ..Default::default()
                            }),
                            ..Default::default()
                        },
                    ],
                    status: Some(RpcStatus {
                        code: tonic::Code::Ok as i32,
                        ..Default::default()
                    }),
                    ..Default::default()
                }))
            });

        mock.expect_commit()
            .once()
            .in_sequence(&mut sequence)
            .returning(|request| {
                let inner = request.into_inner();
                assert_eq!(
                    inner.transaction,
                    Some(v1::commit_request::Transaction::TransactionId(vec![43])),
                    "Commit must use transaction ID [43] from second attempt"
                );
                Ok(Response::new(v1::CommitResponse {
                    commit_timestamp: Some(Timestamp {
                        seconds: 2000,
                        nanos: 0,
                    }),
                    ..Default::default()
                }))
            });

        let (db_client, _server) = setup_db_client(mock).await;
        let runner = db_client
            .read_write_transaction()
            .with_retry_policy(BasicTransactionRetryPolicy::new().with_max_attempts(3))
            .build();

        let attempt = Arc::new(AtomicU32::new(0));
        let attempt_clone = Arc::clone(&attempt);
        let transaction_result = runner
            .run(async move |transaction| {
                let current_attempt = attempt_clone.fetch_add(1, Ordering::SeqCst) + 1;
                let batch = BatchDml::builder()
                    .add_statement("UPDATE Users SET active = true WHERE id = 1")
                    .add_statement("UPDATE Users SET active = true WHERE id = 2")
                    .build();
                let batch_result = transaction.execute_batch_update(batch).await;
                if current_attempt == 1 {
                    let batch_err =
                        batch_result.expect_err("First attempt batch DML must fail with Aborted");
                    assert_eq!(
                        extract_retry_delay_from_error(&batch_err),
                        Some(StdDuration::from_nanos(1)),
                        "BatchUpdateError must preserve RetryInfo delay"
                    );
                    // Even if the application catches the error and tries another statement,
                    // the transaction must fail fast and preserve RetryInfo!
                    let follow_up_err = transaction
                        .execute_update("UPDATE Users SET active = false WHERE id = 3")
                        .await
                        .expect_err("Subsequent statement after abort must fail fast");
                    assert_eq!(
                        extract_retry_delay_from_error(&follow_up_err),
                        Some(StdDuration::from_nanos(1)),
                        "Fast-fail Aborted must preserve RetryInfo delay"
                    );
                    return Err(follow_up_err);
                }
                let counts = batch_result?;
                assert_eq!(counts, vec![1, 1], "Expected 2 updated rows on retry");
                Ok(())
            })
            .await?;

        assert_eq!(attempt.load(Ordering::SeqCst), 2, "Expected 2 attempts");
        assert!(
            transaction_result
                .commit_response
                .commit_timestamp
                .is_some(),
            "Expected commit timestamp"
        );
        Ok(())
    }

    #[tokio_test_no_panics]
    async fn inlined_begin_batch_dml_first_statement_failed_caught_retries_with_explicit_begin()
    -> anyhow::Result<()> {
        let mut mock = create_session_mock();
        let mut sequence = mockall::Sequence::new();

        // Attempt 1: ExecuteBatchDml with InlineBegin fails on statement 1 (empty result_sets, no tx id).
        mock.expect_execute_batch_dml()
            .once()
            .in_sequence(&mut sequence)
            .returning(|request| {
                let inner = request.into_inner();
                assert!(
                    matches!(
                        inner.transaction.and_then(|t| t.selector),
                        Some(v1::transaction_selector::Selector::Begin(_))
                    ),
                    "First attempt must use InlineBegin"
                );
                Ok(Response::new(v1::ExecuteBatchDmlResponse {
                    result_sets: vec![],
                    status: Some(RpcStatus {
                        code: tonic::Code::InvalidArgument as i32,
                        message: "syntax error on statement 1".into(),
                        details: vec![],
                    }),
                    ..Default::default()
                }))
            });

        // Attempt 2: Because statement 1 failed without returning a transaction ID and the closure
        // caught the error and proceeded to commit, the runner retries with an explicit BeginTransaction!
        mock.expect_begin_transaction()
            .once()
            .in_sequence(&mut sequence)
            .returning(|_| {
                Ok(Response::new(v1::Transaction {
                    id: vec![99],
                    ..Default::default()
                }))
            });

        mock.expect_execute_batch_dml()
            .once()
            .in_sequence(&mut sequence)
            .returning(|request| {
                let inner = request.into_inner();
                assert_eq!(
                    inner.transaction.and_then(|t| t.selector),
                    Some(v1::transaction_selector::Selector::Id(vec![99])),
                    "Second attempt must use explicit transaction ID [99]"
                );
                Ok(Response::new(v1::ExecuteBatchDmlResponse {
                    result_sets: vec![],
                    status: Some(RpcStatus {
                        code: tonic::Code::InvalidArgument as i32,
                        message: "syntax error on statement 1".into(),
                        details: vec![],
                    }),
                    ..Default::default()
                }))
            });

        mock.expect_commit()
            .once()
            .in_sequence(&mut sequence)
            .returning(|request| {
                let inner = request.into_inner();
                assert_eq!(
                    inner.transaction,
                    Some(v1::commit_request::Transaction::TransactionId(vec![99])),
                    "Commit must use explicit transaction ID [99]"
                );
                Ok(Response::new(v1::CommitResponse {
                    commit_timestamp: Some(Timestamp {
                        seconds: 3000,
                        nanos: 0,
                    }),
                    ..Default::default()
                }))
            });

        let (db_client, _server) = setup_db_client(mock).await;
        let runner = db_client
            .read_write_transaction()
            .with_retry_policy(BasicTransactionRetryPolicy::new().with_max_attempts(3))
            .build();

        let attempt = Arc::new(AtomicU32::new(0));
        let attempt_clone = Arc::clone(&attempt);
        let transaction_result = runner
            .run(async move |transaction| {
                attempt_clone.fetch_add(1, Ordering::SeqCst);
                let batch = BatchDml::builder().add_statement("INVALID DML").build();
                let batch_result = transaction.execute_batch_update(batch).await;
                assert!(
                    batch_result.is_err(),
                    "Expected batch DML to fail with InvalidArgument"
                );
                // Application catches the error and commits anyway
                Ok(())
            })
            .await?;

        assert_eq!(
            attempt.load(Ordering::SeqCst),
            2,
            "Expected 2 attempts (initial InlineBegin + retry with ExplicitBegin)"
        );
        assert!(
            transaction_result
                .commit_response
                .commit_timestamp
                .is_some(),
            "Expected commit timestamp"
        );
        Ok(())
    }

    #[tokio_test_no_panics]
    async fn inlined_begin_execute_update_leader_cancelled_wakes_follower_and_retries_explicit_begin()
    -> anyhow::Result<()> {
        use google_cloud_gax::exponential_backoff::ExponentialBackoffBuilder;
        use google_cloud_gax::retry_policy::AlwaysRetry;
        use tokio::sync::Notify;
        use tokio::task::{spawn, yield_now};
        use tonic::Status as TonicStatus;

        let mut mock = create_session_mock();
        let mut sequence = mockall::Sequence::new();
        let leader_rpc_received = Arc::new(Notify::new());
        let leader_rpc_received_clone = Arc::clone(&leader_rpc_received);

        // Attempt 1: Leader's ExecuteSql with inline Begin arrives and returns Unavailable
        // so the leader suspends in its retry backoff while holding `StartingGuard`.
        // The backoff timer itself is never waited on because the test immediately aborts
        // the leader task once notified.
        mock.expect_execute_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |request| {
                let inner = request.into_inner();
                assert!(
                    matches!(
                        inner.transaction.and_then(|t| t.selector),
                        Some(v1::transaction_selector::Selector::Begin(_))
                    ),
                    "First attempt leader must use inline Begin"
                );
                leader_rpc_received_clone.notify_one();
                Err(TonicStatus::unavailable(
                    "hold leader in Starting until aborted",
                ))
            });

        // Attempt 2: After the attempt 1 leader is cancelled while in `Starting`,
        // state transitions to FirstStatementFailed, follower wakes up with synthetic
        // Aborted, and the runner retries with ExplicitBegin!
        mock.expect_begin_transaction()
            .once()
            .in_sequence(&mut sequence)
            .returning(|_| {
                Ok(Response::new(v1::Transaction {
                    id: vec![77],
                    ..Default::default()
                }))
            });

        mock.expect_execute_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(|request| {
                let inner = request.into_inner();
                assert_eq!(
                    inner.transaction.and_then(|t| t.selector),
                    Some(v1::transaction_selector::Selector::Id(vec![77])),
                    "Second attempt must use explicit transaction ID [77]"
                );
                Ok(Response::new(v1::ResultSet {
                    stats: Some(v1::ResultSetStats {
                        row_count: Some(v1::result_set_stats::RowCount::RowCountExact(1)),
                        ..Default::default()
                    }),
                    ..Default::default()
                }))
            });

        mock.expect_commit()
            .once()
            .in_sequence(&mut sequence)
            .returning(|request| {
                let inner = request.into_inner();
                assert_eq!(
                    inner.transaction,
                    Some(v1::commit_request::Transaction::TransactionId(vec![77])),
                    "Commit must use explicit transaction ID [77]"
                );
                Ok(Response::new(v1::CommitResponse {
                    commit_timestamp: Some(Timestamp {
                        seconds: 4000,
                        nanos: 0,
                    }),
                    ..Default::default()
                }))
            });

        let (db_client, _server) = setup_db_client(mock).await;
        let runner = db_client
            .read_write_transaction()
            .with_retry_policy(BasicTransactionRetryPolicy::new().with_max_attempts(3))
            .build();

        let attempt = Arc::new(AtomicU32::new(0));
        let attempt_clone = Arc::clone(&attempt);
        let transaction_result = runner
            .run(async move |transaction| {
                let current_attempt = attempt_clone.fetch_add(1, Ordering::SeqCst) + 1;
                if current_attempt == 1 {
                    let leader_statement =
                        Statement::builder("UPDATE Users SET active = true WHERE id = 1")
                            .with_retry_policy(AlwaysRetry)
                            .with_backoff_policy(
                                ExponentialBackoffBuilder::new()
                                    .with_initial_delay(StdDuration::from_secs(60))
                                    .with_maximum_delay(StdDuration::from_secs(60))
                                    .with_scaling(2.0)
                                    .build()
                                    .expect("valid leader backoff"),
                            )
                            .build();

                    let transaction_leader = transaction.clone();
                    let leader_handle = spawn(async move {
                        transaction_leader.execute_update(leader_statement).await
                    });

                    let transaction_follower = transaction.clone();
                    let follower_handle = spawn(async move {
                        transaction_follower
                            .execute_update("UPDATE Users SET active = true WHERE id = 2")
                            .await
                    });

                    // Wait until the mock server has processed the leader's RPC, then yield once
                    // so the leader parks on its backoff sleep timer and the follower parks on `Wait(notified)`.
                    leader_rpc_received.notified().await;
                    yield_now().await;
                    assert!(
                        transaction.is_starting(),
                        "Leader must be in Starting state before cancellation"
                    );

                    // Cancel the leader while it is in `Starting` (immediately cancelling its backoff sleep).
                    leader_handle.abort();
                    let _ = leader_handle.await;

                    // Follower must wake up promptly with synthetic Aborted
                    let follower_outcome =
                        follower_handle.await.expect("follower task must not panic");
                    let follower_err =
                        follower_outcome.expect_err("follower must fail with synthetic Aborted");
                    assert!(
                        is_aborted(&follower_err),
                        "Expected synthetic Aborted error on follower, got: {follower_err}"
                    );
                    return Err(follower_err);
                }

                let updated = transaction
                    .execute_update("UPDATE Users SET active = true WHERE id = 2")
                    .await?;
                assert_eq!(updated, 1, "Expected 1 updated row on retry");
                Ok(())
            })
            .await?;

        assert_eq!(attempt.load(Ordering::SeqCst), 2, "Expected 2 attempts");
        assert!(
            transaction_result
                .commit_response
                .commit_timestamp
                .is_some(),
            "Expected commit timestamp"
        );
        Ok(())
    }

    #[tokio::test]
    async fn read_write_transaction_recovers_from_poisoned_mutations_lock() -> crate::Result<()> {
        use std::mem::take;
        use std::panic::catch_unwind;

        let mock = create_session_mock();
        let (db_client, _server) = setup_db_client(mock).await;
        let transaction = ReadWriteTransactionBuilder::new(db_client)
            .set_begin_transaction_option(BeginTransactionOption::InlineBegin)
            .build(None)
            .await
            .expect("Failed to build transaction");

        // Deliberately poison the mutations mutex.
        let mutations_clone = Arc::clone(&transaction.mutations);
        let _ = catch_unwind(move || {
            let _guard = mutations_clone
                .lock()
                .expect("mutex lock before deliberate panic");
            panic!("deliberate panic to poison mutations mutex");
        });
        assert!(
            transaction.mutations.is_poisoned(),
            "mutations mutex must be poisoned"
        );

        // Verify buffer() recovers from the poisoned lock and successfully buffers mutations.
        use crate::key::KeySet;
        let mutation = Mutation::delete("Users", KeySet::all());
        transaction.buffer(vec![mutation])?;

        // Verify lock_mutations() recovers from the poisoned lock and exposes the mutation.
        let mut guard = transaction.lock_mutations();
        assert_eq!(
            guard.len(),
            1,
            "expected one buffered mutation after poison recovery"
        );
        let drained: Vec<_> = take(&mut *guard);
        assert_eq!(drained.len(), 1, "expected one drained mutation");

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn read_write_transaction_explicit_begin_tracks_precommit_token() -> anyhow::Result<()> {
        use crate::key::KeySet;
        use mockall::Sequence;

        let mut mock = create_session_mock();
        let mut sequence = Sequence::new();

        mock.expect_begin_transaction()
            .once()
            .in_sequence(&mut sequence)
            .returning(|_| {
                Ok(Response::new(v1::Transaction {
                    id: vec![1, 2, 3],
                    precommit_token: Some(v1::MultiplexedSessionPrecommitToken {
                        precommit_token: vec![10, 20, 30],
                        seq_num: 5,
                    }),
                    ..Default::default()
                }))
            });

        mock.expect_execute_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(|_| {
                Ok(Response::new(v1::ResultSet {
                    stats: Some(v1::ResultSetStats {
                        row_count: Some(v1::result_set_stats::RowCount::RowCountExact(1)),
                        ..Default::default()
                    }),
                    precommit_token: Some(v1::MultiplexedSessionPrecommitToken {
                        precommit_token: vec![1, 2, 3],
                        seq_num: 2,
                    }),
                    ..Default::default()
                }))
            });

        mock.expect_commit()
            .once()
            .in_sequence(&mut sequence)
            .returning(|request| {
                let request = request.into_inner();
                assert_eq!(
                    request.precommit_token,
                    Some(v1::MultiplexedSessionPrecommitToken {
                        precommit_token: vec![10, 20, 30],
                        seq_num: 5,
                    }),
                    "CommitRequest must retain the higher seq_num precommit_token from ExplicitBegin"
                );
                Ok(Response::new(v1::CommitResponse {
                    commit_timestamp: Some(Timestamp {
                        seconds: 1000,
                        nanos: 0,
                    }),
                    ..Default::default()
                }))
            });

        let (database_client, _server) = setup_db_client(mock).await;
        let transaction = ReadWriteTransactionBuilder::new(database_client)
            .set_begin_transaction_option(BeginTransactionOption::ExplicitBegin)
            .build(None)
            .await
            .expect("Failed to build transaction");

        transaction
            .execute_update("UPDATE Users SET Name = 'Alice' WHERE Id = 1")
            .await?;
        transaction.buffer([Mutation::delete("Users", KeySet::all())])?;
        let response = transaction.commit().await?;
        assert!(
            response.commit_timestamp.is_some(),
            "Expected commit timestamp in CommitResponse"
        );

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn read_write_transaction_commit_retry_preserves_higher_seq_num_precommit_token()
    -> anyhow::Result<()> {
        use crate::key::KeySet;
        use mockall::Sequence;
        use v1::commit_response::MultiplexedSessionRetry as MockMultiplexedSessionRetry;

        let mut mock = create_session_mock();
        let mut sequence = Sequence::new();

        mock.expect_begin_transaction()
            .once()
            .in_sequence(&mut sequence)
            .returning(|_| {
                Ok(Response::new(v1::Transaction {
                    id: vec![1, 2, 3],
                    precommit_token: Some(v1::MultiplexedSessionPrecommitToken {
                        precommit_token: vec![50, 60],
                        seq_num: 5,
                    }),
                    ..Default::default()
                }))
            });

        mock.expect_commit()
            .once()
            .in_sequence(&mut sequence)
            .returning(|request| {
                let request = request.into_inner();
                assert_eq!(
                    request.precommit_token,
                    Some(v1::MultiplexedSessionPrecommitToken {
                        precommit_token: vec![50, 60],
                        seq_num: 5,
                    }),
                    "First CommitRequest must include the precommit_token with seq_num 5"
                );
                Ok(Response::new(v1::CommitResponse {
                    multiplexed_session_retry: Some(MockMultiplexedSessionRetry::PrecommitToken(
                        v1::MultiplexedSessionPrecommitToken {
                            precommit_token: vec![10, 20],
                            seq_num: 2,
                        },
                    )),
                    ..Default::default()
                }))
            });

        mock.expect_commit()
            .once()
            .in_sequence(&mut sequence)
            .returning(|request| {
                let request = request.into_inner();
                assert!(
                    request.mutations.is_empty(),
                    "Expected mutations to be empty on commit retry"
                );
                assert_eq!(
                    request.precommit_token,
                    Some(v1::MultiplexedSessionPrecommitToken {
                        precommit_token: vec![50, 60],
                        seq_num: 5,
                    }),
                    "Retry CommitRequest must retain the higher seq_num precommit_token (seq_num 5)"
                );
                Ok(Response::new(v1::CommitResponse {
                    commit_timestamp: Some(Timestamp {
                        seconds: 1000,
                        nanos: 0,
                    }),
                    ..Default::default()
                }))
            });

        let (database_client, _server) = setup_db_client(mock).await;
        let transaction = ReadWriteTransactionBuilder::new(database_client)
            .set_begin_transaction_option(BeginTransactionOption::ExplicitBegin)
            .build(None)
            .await
            .expect("Failed to build transaction");

        transaction.buffer([Mutation::delete("Users", KeySet::all())])?;
        let response = transaction.commit().await?;
        assert!(
            response.commit_timestamp.is_some(),
            "Expected commit timestamp in CommitResponse"
        );

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn read_write_transaction_batch_dml_tracks_precommit_token() -> anyhow::Result<()> {
        use mockall::Sequence;

        let mut mock = create_session_mock();
        let mut sequence = Sequence::new();

        mock.expect_execute_batch_dml()
            .once()
            .in_sequence(&mut sequence)
            .returning(|_| {
                Ok(Response::new(v1::ExecuteBatchDmlResponse {
                    result_sets: vec![
                        v1::ResultSet {
                            metadata: Some(v1::ResultSetMetadata {
                                transaction: Some(v1::Transaction {
                                    id: vec![4, 5, 6],
                                    ..Default::default()
                                }),
                                ..Default::default()
                            }),
                            stats: Some(v1::ResultSetStats {
                                row_count: Some(v1::result_set_stats::RowCount::RowCountExact(1)),
                                ..Default::default()
                            }),
                            ..Default::default()
                        },
                        v1::ResultSet {
                            stats: Some(v1::ResultSetStats {
                                row_count: Some(v1::result_set_stats::RowCount::RowCountExact(1)),
                                ..Default::default()
                            }),
                            ..Default::default()
                        },
                    ],
                    status: Some(RpcStatus {
                        code: Code::Ok as i32,
                        message: "OK".into(),
                        details: vec![],
                    }),
                    precommit_token: Some(v1::MultiplexedSessionPrecommitToken {
                        precommit_token: vec![70, 80],
                        seq_num: 3,
                    }),
                }))
            });

        mock.expect_commit()
            .once()
            .in_sequence(&mut sequence)
            .returning(|request| {
                let request = request.into_inner();
                assert_eq!(
                    request.precommit_token,
                    Some(v1::MultiplexedSessionPrecommitToken {
                        precommit_token: vec![70, 80],
                        seq_num: 3,
                    }),
                    "CommitRequest must include the precommit_token returned by ExecuteBatchDml"
                );
                Ok(Response::new(v1::CommitResponse {
                    commit_timestamp: Some(Timestamp {
                        seconds: 1000,
                        nanos: 0,
                    }),
                    ..Default::default()
                }))
            });

        let (database_client, _server) = setup_db_client(mock).await;
        let transaction = ReadWriteTransactionBuilder::new(database_client)
            .set_begin_transaction_option(BeginTransactionOption::InlineBegin)
            .build(None)
            .await
            .expect("Failed to build transaction");

        let batch = BatchDml::builder()
            .add_statement("UPDATE Users SET Name = 'Alice' WHERE Id = 1")
            .add_statement("UPDATE Users SET Name = 'Bob' WHERE Id = 2")
            .build();
        let update_counts = transaction.execute_batch_update(batch).await?;
        assert_eq!(update_counts, vec![1, 1], "Expected 2 updated rows");

        let response = transaction.commit().await?;
        assert!(
            response.commit_timestamp.is_some(),
            "Expected commit timestamp in CommitResponse"
        );

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn read_write_transaction_batch_dml_partial_failure_tracks_precommit_token()
    -> anyhow::Result<()> {
        use mockall::Sequence;

        let mut mock = create_session_mock();
        let mut sequence = Sequence::new();

        mock.expect_execute_batch_dml()
            .once()
            .in_sequence(&mut sequence)
            .returning(|_| {
                Ok(Response::new(v1::ExecuteBatchDmlResponse {
                    result_sets: vec![v1::ResultSet {
                        metadata: Some(v1::ResultSetMetadata {
                            transaction: Some(v1::Transaction {
                                id: vec![7, 8, 9],
                                ..Default::default()
                            }),
                            ..Default::default()
                        }),
                        stats: Some(v1::ResultSetStats {
                            row_count: Some(v1::result_set_stats::RowCount::RowCountExact(1)),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }],
                    status: Some(RpcStatus {
                        code: Code::AlreadyExists as i32,
                        message: "row already exists".into(),
                        details: vec![],
                    }),
                    precommit_token: Some(v1::MultiplexedSessionPrecommitToken {
                        precommit_token: vec![90, 100],
                        seq_num: 4,
                    }),
                }))
            });

        mock.expect_commit()
            .once()
            .in_sequence(&mut sequence)
            .returning(|request| {
                let request = request.into_inner();
                assert_eq!(
                    request.precommit_token,
                    Some(v1::MultiplexedSessionPrecommitToken {
                        precommit_token: vec![90, 100],
                        seq_num: 4,
                    }),
                    "CommitRequest must include the precommit_token from a partially failed ExecuteBatchDmlResponse"
                );
                Ok(Response::new(v1::CommitResponse {
                    commit_timestamp: Some(Timestamp {
                        seconds: 1000,
                        nanos: 0,
                    }),
                    ..Default::default()
                }))
            });

        let (database_client, _server) = setup_db_client(mock).await;
        let transaction = ReadWriteTransactionBuilder::new(database_client)
            .set_begin_transaction_option(BeginTransactionOption::InlineBegin)
            .build(None)
            .await
            .expect("Failed to build transaction");

        let batch = BatchDml::builder()
            .add_statement("UPDATE Users SET Name = 'Alice' WHERE Id = 1")
            .add_statement("INSERT INTO Users (Id) VALUES (2)")
            .build();
        let batch_result = transaction.execute_batch_update(batch).await;
        assert!(
            batch_result.is_err(),
            "Expected partial failure error from execute_batch_update"
        );

        let response = transaction.commit().await?;
        assert!(
            response.commit_timestamp.is_some(),
            "Expected commit timestamp in CommitResponse"
        );

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn read_write_transaction_mutation_only_inline_begin_tracks_precommit_token()
    -> anyhow::Result<()> {
        use crate::key::KeySet;
        use mockall::Sequence;

        let mut mock = create_session_mock();
        let mut sequence = Sequence::new();

        mock.expect_begin_transaction()
            .once()
            .in_sequence(&mut sequence)
            .returning(|request| {
                let request = request.into_inner();
                assert!(
                    request.mutation_key.is_some(),
                    "Expected mutation_key on BeginTransactionRequest for mutation-only inline begin"
                );
                Ok(Response::new(v1::Transaction {
                    id: vec![11, 22],
                    precommit_token: Some(v1::MultiplexedSessionPrecommitToken {
                        precommit_token: vec![33, 44],
                        seq_num: 2,
                    }),
                    ..Default::default()
                }))
            });

        mock.expect_commit()
            .once()
            .in_sequence(&mut sequence)
            .returning(|request| {
                let request = request.into_inner();
                assert_eq!(
                    request.precommit_token,
                    Some(v1::MultiplexedSessionPrecommitToken {
                        precommit_token: vec![33, 44],
                        seq_num: 2,
                    }),
                    "CommitRequest must include the precommit_token returned by fallback BeginTransaction"
                );
                Ok(Response::new(v1::CommitResponse {
                    commit_timestamp: Some(Timestamp {
                        seconds: 1000,
                        nanos: 0,
                    }),
                    ..Default::default()
                }))
            });

        let (database_client, _server) = setup_db_client(mock).await;
        let transaction = ReadWriteTransactionBuilder::new(database_client)
            .set_begin_transaction_option(BeginTransactionOption::InlineBegin)
            .build(None)
            .await
            .expect("Failed to build transaction");

        transaction.buffer([Mutation::delete("Users", KeySet::all())])?;
        let response = transaction.commit().await?;
        assert!(
            response.commit_timestamp.is_some(),
            "Expected commit timestamp in CommitResponse"
        );

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn read_write_transaction_execute_query_tracks_highest_precommit_token()
    -> anyhow::Result<()> {
        use mockall::Sequence;

        let mut mock = create_session_mock();
        let mut sequence = Sequence::new();

        mock.expect_execute_streaming_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(|_| {
                let first_chunk = v1::PartialResultSet {
                    metadata: Some(v1::ResultSetMetadata {
                        row_type: Some(v1::StructType {
                            fields: vec![v1::struct_type::Field {
                                name: "Id".to_string(),
                                r#type: Some(v1::Type {
                                    code: v1::TypeCode::Int64 as i32,
                                    ..Default::default()
                                }),
                            }],
                        }),
                        transaction: Some(v1::Transaction {
                            id: vec![21, 22],
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    values: vec![string_val("1")],
                    precommit_token: Some(v1::MultiplexedSessionPrecommitToken {
                        precommit_token: vec![40, 41],
                        seq_num: 4,
                    }),
                    ..Default::default()
                };
                let second_chunk = v1::PartialResultSet {
                    values: vec![string_val("2")],
                    precommit_token: Some(v1::MultiplexedSessionPrecommitToken {
                        precommit_token: vec![20, 21],
                        seq_num: 2,
                    }),
                    last: true,
                    ..Default::default()
                };
                Ok(Response::from(adapt([Ok(first_chunk), Ok(second_chunk)])))
            });

        mock.expect_commit()
            .once()
            .in_sequence(&mut sequence)
            .returning(|request| {
                let request = request.into_inner();
                assert_eq!(
                    request.precommit_token,
                    Some(v1::MultiplexedSessionPrecommitToken {
                        precommit_token: vec![40, 41],
                        seq_num: 4,
                    }),
                    "CommitRequest must include the highest seq_num precommit_token from PartialResultSet chunks"
                );
                Ok(Response::new(v1::CommitResponse {
                    commit_timestamp: Some(Timestamp {
                        seconds: 1000,
                        nanos: 0,
                    }),
                    ..Default::default()
                }))
            });

        let (database_client, _server) = setup_db_client(mock).await;
        let transaction = ReadWriteTransactionBuilder::new(database_client)
            .set_begin_transaction_option(BeginTransactionOption::InlineBegin)
            .build(None)
            .await
            .expect("Failed to build transaction");

        let mut result_set = transaction.execute_query("SELECT Id FROM Users").await?;
        while let Some(row) = result_set.next().await {
            row?;
        }

        let response = transaction.commit().await?;
        assert!(
            response.commit_timestamp.is_some(),
            "Expected commit timestamp in CommitResponse"
        );

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn read_write_transaction_execute_read_tracks_highest_precommit_token()
    -> anyhow::Result<()> {
        use crate::key::KeySet;
        use crate::read::ReadRequest;
        use mockall::Sequence;

        let mut mock = create_session_mock();
        let mut sequence = Sequence::new();

        mock.expect_streaming_read()
            .once()
            .in_sequence(&mut sequence)
            .returning(|_| {
                let first_chunk = v1::PartialResultSet {
                    metadata: Some(v1::ResultSetMetadata {
                        row_type: Some(v1::StructType {
                            fields: vec![v1::struct_type::Field {
                                name: "Id".to_string(),
                                r#type: Some(v1::Type {
                                    code: v1::TypeCode::Int64 as i32,
                                    ..Default::default()
                                }),
                            }],
                        }),
                        transaction: Some(v1::Transaction {
                            id: vec![31, 32],
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    values: vec![string_val("1")],
                    precommit_token: Some(v1::MultiplexedSessionPrecommitToken {
                        precommit_token: vec![20, 21],
                        seq_num: 2,
                    }),
                    ..Default::default()
                };
                let second_chunk = v1::PartialResultSet {
                    values: vec![string_val("2")],
                    precommit_token: Some(v1::MultiplexedSessionPrecommitToken {
                        precommit_token: vec![60, 61],
                        seq_num: 6,
                    }),
                    last: true,
                    ..Default::default()
                };
                Ok(Response::from(adapt([Ok(first_chunk), Ok(second_chunk)])))
            });

        mock.expect_commit()
            .once()
            .in_sequence(&mut sequence)
            .returning(|request| {
                let request = request.into_inner();
                assert_eq!(
                    request.precommit_token,
                    Some(v1::MultiplexedSessionPrecommitToken {
                        precommit_token: vec![60, 61],
                        seq_num: 6,
                    }),
                    "CommitRequest must include the highest seq_num precommit_token from StreamingRead chunks"
                );
                Ok(Response::new(v1::CommitResponse {
                    commit_timestamp: Some(Timestamp {
                        seconds: 1000,
                        nanos: 0,
                    }),
                    ..Default::default()
                }))
            });

        let (database_client, _server) = setup_db_client(mock).await;
        let transaction = ReadWriteTransactionBuilder::new(database_client)
            .set_begin_transaction_option(BeginTransactionOption::InlineBegin)
            .build(None)
            .await
            .expect("Failed to build transaction");

        let read_request = ReadRequest::builder("Users", vec!["Id"])
            .with_keys(KeySet::all())
            .build();
        let mut result_set = transaction.execute_read(read_request).await?;
        while let Some(row) = result_set.next().await {
            row?;
        }

        let response = transaction.commit().await?;
        assert!(
            response.commit_timestamp.is_some(),
            "Expected commit timestamp in CommitResponse"
        );

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn transaction_runner_first_statement_failed_explicit_begin_fallback_tracks_precommit_token()
    -> anyhow::Result<()> {
        use mockall::Sequence;

        let mut mock = create_session_mock();
        let mut sequence = Sequence::new();

        mock.expect_execute_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(|_| Err(tonic::Status::invalid_argument("invalid SQL")));

        mock.expect_begin_transaction()
            .once()
            .in_sequence(&mut sequence)
            .returning(|_| {
                Ok(Response::new(v1::Transaction {
                    id: vec![88, 99],
                    precommit_token: Some(v1::MultiplexedSessionPrecommitToken {
                        precommit_token: vec![77, 88],
                        seq_num: 7,
                    }),
                    ..Default::default()
                }))
            });

        mock.expect_execute_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(|_| Err(tonic::Status::invalid_argument("invalid SQL")));

        mock.expect_commit()
            .once()
            .in_sequence(&mut sequence)
            .returning(|request| {
                let request = request.into_inner();
                assert_eq!(
                    request.precommit_token,
                    Some(v1::MultiplexedSessionPrecommitToken {
                        precommit_token: vec![77, 88],
                        seq_num: 7,
                    }),
                    "CommitRequest must include the precommit_token returned by ExplicitBegin fallback"
                );
                Ok(Response::new(v1::CommitResponse {
                    commit_timestamp: Some(Timestamp {
                        seconds: 1000,
                        nanos: 0,
                    }),
                    ..Default::default()
                }))
            });

        let (database_client, _server) = setup_db_client(mock).await;
        let runner = database_client.read_write_transaction().build();

        let result = runner
            .run(async move |transaction| {
                let _ = transaction.execute_update("INVALID SQL").await;
                Ok(())
            })
            .await?;

        assert!(
            result.commit_response.commit_timestamp.is_some(),
            "Expected commit timestamp in CommitResponse"
        );

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn read_write_transaction_is_aborted() -> Result<()> {
        let mut mock = create_session_mock();
        mock.expect_execute_sql().once().returning(|_| {
            Err(TonicStatus::new(
                TonicCode::Aborted,
                "Transaction was aborted",
            ))
        });

        let (db_client, _server) = setup_db_client(mock).await;
        let transaction = ReadWriteTransactionBuilder::new(db_client)
            .build(None)
            .await?;

        assert!(
            !transaction.is_aborted(),
            "transaction should not be aborted initially"
        );

        let result = transaction
            .execute_update("UPDATE Users SET active = true")
            .await;
        assert!(result.is_err(), "statement execution should fail");

        assert!(
            transaction.is_aborted(),
            "transaction should be marked as aborted after receiving Aborted error"
        );
        assert!(
            transaction.aborted_info().is_some(),
            "aborted_info should be recorded"
        );

        Ok(())
    }

    fn create_query_partial_result_set(
        transaction_id: Option<Vec<u8>>,
        names: &[&str],
        is_last: bool,
    ) -> PartialResultSet {
        let metadata = Some(ResultSetMetadata {
            row_type: Some(StructType {
                fields: vec![struct_type::Field {
                    name: "Name".to_string(),
                    r#type: Some(Type {
                        code: TypeCode::String as i32,
                        ..Default::default()
                    }),
                }],
            }),
            transaction: transaction_id.map(|id| ProtoTransaction {
                id,
                ..Default::default()
            }),
            ..Default::default()
        });
        let values = names.iter().map(|name| string_val(name)).collect();
        PartialResultSet {
            metadata,
            values,
            last: is_last,
            resume_token: vec![1],
            ..Default::default()
        }
    }

    fn single_row_streaming_response(
        transaction_id: Option<Vec<u8>>,
        name: &str,
    ) -> Response<Receiver<tonic::Result<PartialResultSet>>> {
        let receiver = adapt([Ok(create_query_partial_result_set(
            transaction_id,
            &[name],
            true,
        ))]);
        Response::from(receiver)
    }

    fn halfway_aborted_streaming_response(
        transaction_id: Option<Vec<u8>>,
        name: &str,
    ) -> Response<Receiver<tonic::Result<PartialResultSet>>> {
        let receiver = adapt([
            Ok(create_query_partial_result_set(
                transaction_id,
                &[name],
                false,
            )),
            Err(create_aborted_status(StdDuration::from_nanos(1))),
        ]);
        Response::from(receiver)
    }

    fn dml_response(transaction_id: Option<Vec<u8>>) -> Response<ProtoResultSet> {
        Response::new(ProtoResultSet {
            metadata: transaction_id.map(|id| ResultSetMetadata {
                transaction: Some(ProtoTransaction {
                    id,
                    ..Default::default()
                }),
                ..Default::default()
            }),
            stats: Some(ResultSetStats {
                row_count: Some(RowCount::RowCountExact(1)),
                ..Default::default()
            }),
            ..Default::default()
        })
    }

    fn expect_commit(
        mock: &mut MockSpanner,
        sequence: &mut Sequence,
        expected_transaction_id: Vec<u8>,
    ) {
        mock.expect_commit()
            .once()
            .in_sequence(sequence)
            .returning(move |request| {
                let inner = request.into_inner();
                assert_eq!(
                    inner.transaction,
                    Some(CommitTransaction::TransactionId(
                        expected_transaction_id.clone()
                    )),
                    "Commit must use expected transaction ID"
                );
                Ok(Response::new(CommitResponse {
                    commit_timestamp: Some(Timestamp {
                        seconds: 1000,
                        nanos: 0,
                    }),
                    ..Default::default()
                }))
            });
    }

    fn expect_begin_transaction(
        mock: &mut MockSpanner,
        sequence: &mut Sequence,
        expected_previous_transaction_id: Option<Vec<u8>>,
        returned_transaction_id: Vec<u8>,
    ) {
        mock.expect_begin_transaction()
            .once()
            .in_sequence(sequence)
            .returning(move |request| {
                let inner = request.into_inner();
                let previous_transaction_id = inner
                    .options
                    .and_then(|options| options.mode)
                    .and_then(|mode| match mode {
                        Mode::ReadWrite(read_write) => {
                            if read_write
                                .multiplexed_session_previous_transaction_id
                                .is_empty()
                            {
                                None
                            } else {
                                Some(read_write.multiplexed_session_previous_transaction_id)
                            }
                        }
                        _ => None,
                    });
                assert_eq!(
                    previous_transaction_id, expected_previous_transaction_id,
                    "BeginTransaction previous_transaction_id mismatch"
                );
                Ok(Response::new(ProtoTransaction {
                    id: returned_transaction_id.clone(),
                    ..Default::default()
                }))
            });
    }

    fn assert_inline_begin_selector(
        selector: Option<&TransactionSelector>,
        expected_previous_transaction_id: Option<&[u8]>,
    ) {
        let selector = selector.and_then(|transaction| transaction.selector.as_ref());
        match selector {
            Some(ProtoSelector::Begin(options)) => match &options.mode {
                Some(Mode::ReadWrite(read_write)) => {
                    let actual_previous_id = (!read_write
                        .multiplexed_session_previous_transaction_id
                        .is_empty())
                    .then_some(
                        read_write
                            .multiplexed_session_previous_transaction_id
                            .as_slice(),
                    );
                    assert_eq!(
                        actual_previous_id, expected_previous_transaction_id,
                        "previous_transaction_id must match expected"
                    );
                }
                _ => panic!("Expected ReadWrite mode in Begin options"),
            },
            _ => panic!("Request must use inline begin, got {selector:?}"),
        }
    }

    fn assert_inline_begin(
        request: &ExecuteSqlRequest,
        expected_previous_transaction_id: Option<&[u8]>,
    ) {
        assert_inline_begin_selector(
            request.transaction.as_ref(),
            expected_previous_transaction_id,
        );
    }

    fn assert_transaction_id(request: &ExecuteSqlRequest, expected_id: &[u8]) {
        let selector = request
            .transaction
            .as_ref()
            .and_then(|transaction| transaction.selector.as_ref());
        match selector {
            Some(ProtoSelector::Id(id)) => {
                assert_eq!(
                    id.as_slice(),
                    expected_id,
                    "Request must use expected transaction ID"
                );
            }
            _ => panic!("Expected Selector::Id with expected transaction ID, got {selector:?}"),
        }
    }

    #[tokio_test_no_panics]
    async fn read_write_transaction_query_aborted_halfway_inline_begin_retries() -> Result<()> {
        let mut mock = create_session_mock();
        let mut sequence = Sequence::new();

        // 1. Attempt 1: First statement (query) uses inline begin.
        // Delivers first row with transaction ID [42], then stream item returns Aborted status.
        mock.expect_execute_streaming_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(|request| {
                assert_inline_begin(&request.into_inner(), None);
                Ok(halfway_aborted_streaming_response(Some(vec![42]), "alice"))
            });

        // 2. Attempt 2: Runner retries with inline begin and previous_transaction_id = [42].
        mock.expect_execute_streaming_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(|request| {
                assert_inline_begin(&request.into_inner(), Some(&[42]));
                Ok(single_row_streaming_response(Some(vec![43]), "bob"))
            });

        expect_commit(&mut mock, &mut sequence, vec![43]);

        let (database_client, _server) = setup_db_client(mock).await;
        let runner = database_client
            .read_write_transaction()
            .with_retry_policy(BasicTransactionRetryPolicy::new().with_max_attempts(3))
            .build();

        let attempt_counter = Arc::new(AtomicU32::new(0));
        let attempt_counter_clone = Arc::clone(&attempt_counter);
        let transaction_result = runner
            .run(async move |transaction| {
                let current_attempt = attempt_counter_clone.fetch_add(1, Ordering::SeqCst) + 1;
                let mut result_set = transaction
                    .execute_query("SELECT Name FROM Users WHERE Id = 1")
                    .await?;
                let mut names = Vec::new();
                while let Some(row) = result_set.next().await.transpose()? {
                    let name: String = row.get("Name")?;
                    names.push(name);
                }
                if current_attempt == 2 {
                    assert_eq!(names, vec!["bob"], "Attempt 2 should consume 'bob'");
                }
                Ok(())
            })
            .await?;

        assert_eq!(
            attempt_counter.load(Ordering::SeqCst),
            2,
            "Expected 2 attempts"
        );
        assert!(
            transaction_result
                .commit_response
                .commit_timestamp
                .is_some(),
            "Expected commit timestamp"
        );
        Ok(())
    }

    #[tokio_test_no_panics]
    async fn read_write_transaction_query_aborted_at_start_inline_begin_retries() -> Result<()> {
        let mut mock = create_session_mock();
        let mut sequence = Sequence::new();

        // 1. Attempt 1: First statement (query) uses inline begin and fails immediately at RPC start.
        mock.expect_execute_streaming_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(|request| {
                assert_inline_begin(&request.into_inner(), None);
                Err(create_aborted_status(StdDuration::from_nanos(1)))
            });

        // 2. Attempt 2: Because no transaction ID was produced, runner retries with inline begin
        // and no previous transaction ID.
        mock.expect_execute_streaming_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(|request| {
                assert_inline_begin(&request.into_inner(), None);
                Ok(single_row_streaming_response(Some(vec![43]), "alice"))
            });

        expect_commit(&mut mock, &mut sequence, vec![43]);

        let (database_client, _server) = setup_db_client(mock).await;
        let runner = database_client
            .read_write_transaction()
            .with_retry_policy(BasicTransactionRetryPolicy::new().with_max_attempts(3))
            .build();

        let attempt_counter = Arc::new(AtomicU32::new(0));
        let attempt_counter_clone = Arc::clone(&attempt_counter);
        let transaction_result = runner
            .run(async move |transaction| {
                let _attempt = attempt_counter_clone.fetch_add(1, Ordering::SeqCst) + 1;
                let mut result_set = transaction
                    .execute_query("SELECT Name FROM Users WHERE Id = 1")
                    .await?;
                while let Some(_row) = result_set.next().await.transpose()? {}
                Ok(())
            })
            .await?;

        assert_eq!(
            attempt_counter.load(Ordering::SeqCst),
            2,
            "Expected 2 attempts"
        );
        assert!(
            transaction_result
                .commit_response
                .commit_timestamp
                .is_some(),
            "Expected commit timestamp"
        );
        Ok(())
    }

    #[tokio_test_no_panics]
    async fn read_write_transaction_query_aborted_at_start_in_middle_of_transaction() -> Result<()>
    {
        let mut mock = create_session_mock();
        let mut sequence = Sequence::new();

        // Attempt 1:
        // Statement 1: DML succeeds and inlines begin (transaction ID [10]).
        mock.expect_execute_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(|_| Ok(dml_response(Some(vec![10]))));

        // Statement 2: Query fails immediately at RPC start with Aborted.
        mock.expect_execute_streaming_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(|request| {
                assert_transaction_id(&request.into_inner(), &[10]);
                Err(create_aborted_status(StdDuration::from_nanos(1)))
            });

        // Attempt 2:
        // Statement 1: DML retries with inline begin and previous_transaction_id = [10].
        mock.expect_execute_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(|_| Ok(dml_response(Some(vec![11]))));

        // Statement 2: Query succeeds.
        mock.expect_execute_streaming_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(|request| {
                assert_transaction_id(&request.into_inner(), &[11]);
                Ok(single_row_streaming_response(None, "alice"))
            });

        expect_commit(&mut mock, &mut sequence, vec![11]);

        let (database_client, _server) = setup_db_client(mock).await;
        let runner = database_client
            .read_write_transaction()
            .with_retry_policy(BasicTransactionRetryPolicy::new().with_max_attempts(3))
            .build();

        let attempt_counter = Arc::new(AtomicU32::new(0));
        let attempt_counter_clone = Arc::clone(&attempt_counter);
        let transaction_result = runner
            .run(async move |transaction| {
                let _attempt = attempt_counter_clone.fetch_add(1, Ordering::SeqCst) + 1;
                transaction
                    .execute_update("UPDATE Users SET active = true WHERE id = 1")
                    .await?;
                let mut result_set = transaction
                    .execute_query("SELECT Name FROM Users WHERE Id = 1")
                    .await?;
                while let Some(_row) = result_set.next().await.transpose()? {}
                Ok(())
            })
            .await?;

        assert_eq!(
            attempt_counter.load(Ordering::SeqCst),
            2,
            "Expected 2 attempts"
        );
        assert!(
            transaction_result
                .commit_response
                .commit_timestamp
                .is_some(),
            "Expected commit timestamp"
        );
        Ok(())
    }

    #[tokio_test_no_panics]
    async fn read_write_transaction_query_aborted_halfway_in_middle_of_transaction() -> Result<()> {
        let mut mock = create_session_mock();
        let mut sequence = Sequence::new();

        // Attempt 1:
        // Statement 1: DML succeeds and inlines begin (transaction ID [10]).
        mock.expect_execute_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(|_| Ok(dml_response(Some(vec![10]))));

        // Statement 2: Query starts, yields chunk 1, then chunk 2 aborts.
        mock.expect_execute_streaming_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(|request| {
                assert_transaction_id(&request.into_inner(), &[10]);
                Ok(halfway_aborted_streaming_response(None, "alice"))
            });

        // Attempt 2:
        // Statement 1 succeeds (ID [11]).
        mock.expect_execute_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(|_| Ok(dml_response(Some(vec![11]))));

        // Statement 2 succeeds.
        mock.expect_execute_streaming_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(|request| {
                assert_transaction_id(&request.into_inner(), &[11]);
                Ok(single_row_streaming_response(None, "alice"))
            });

        expect_commit(&mut mock, &mut sequence, vec![11]);

        let (database_client, _server) = setup_db_client(mock).await;
        let runner = database_client
            .read_write_transaction()
            .with_retry_policy(BasicTransactionRetryPolicy::new().with_max_attempts(3))
            .build();

        let attempt_counter = Arc::new(AtomicU32::new(0));
        let attempt_counter_clone = Arc::clone(&attempt_counter);
        let transaction_result = runner
            .run(async move |transaction| {
                let _attempt = attempt_counter_clone.fetch_add(1, Ordering::SeqCst) + 1;
                transaction
                    .execute_update("UPDATE Users SET active = true WHERE id = 1")
                    .await?;
                let mut result_set = transaction
                    .execute_query("SELECT Name FROM Users WHERE Id = 1")
                    .await?;
                while let Some(_row) = result_set.next().await.transpose()? {}
                Ok(())
            })
            .await?;

        assert_eq!(
            attempt_counter.load(Ordering::SeqCst),
            2,
            "Expected 2 attempts"
        );
        assert!(
            transaction_result
                .commit_response
                .commit_timestamp
                .is_some(),
            "Expected commit timestamp"
        );
        Ok(())
    }

    #[tokio_test_no_panics]
    async fn read_write_transaction_query_aborted_at_start_explicit_begin() -> Result<()> {
        let mut mock = create_session_mock();
        let mut sequence = Sequence::new();

        // Attempt 1: Explicit BeginTransaction RPC.
        expect_begin_transaction(&mut mock, &mut sequence, None, vec![20]);

        // Query fails immediately at RPC start with Aborted.
        mock.expect_execute_streaming_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(|request| {
                assert_transaction_id(&request.into_inner(), &[20]);
                Err(create_aborted_status(StdDuration::from_nanos(1)))
            });

        // Attempt 2: Explicit BeginTransaction with previous_transaction_id = [20].
        expect_begin_transaction(&mut mock, &mut sequence, Some(vec![20]), vec![21]);

        // Query succeeds.
        mock.expect_execute_streaming_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(|request| {
                assert_transaction_id(&request.into_inner(), &[21]);
                Ok(single_row_streaming_response(None, "alice"))
            });

        expect_commit(&mut mock, &mut sequence, vec![21]);

        let (database_client, _server) = setup_db_client(mock).await;
        let runner = database_client
            .read_write_transaction()
            .set_begin_transaction_option(BeginTransactionOption::ExplicitBegin)
            .with_retry_policy(BasicTransactionRetryPolicy::new().with_max_attempts(3))
            .build();

        let attempt_counter = Arc::new(AtomicU32::new(0));
        let attempt_counter_clone = Arc::clone(&attempt_counter);
        let transaction_result = runner
            .run(async move |transaction| {
                let _attempt = attempt_counter_clone.fetch_add(1, Ordering::SeqCst) + 1;
                let mut result_set = transaction
                    .execute_query("SELECT Name FROM Users WHERE Id = 1")
                    .await?;
                while let Some(_row) = result_set.next().await.transpose()? {}
                Ok(())
            })
            .await?;

        assert_eq!(
            attempt_counter.load(Ordering::SeqCst),
            2,
            "Expected 2 attempts"
        );
        assert!(
            transaction_result
                .commit_response
                .commit_timestamp
                .is_some(),
            "Expected commit timestamp"
        );
        Ok(())
    }

    #[tokio_test_no_panics]
    async fn read_write_transaction_query_aborted_halfway_explicit_begin() -> Result<()> {
        let mut mock = create_session_mock();
        let mut sequence = Sequence::new();

        // Attempt 1: Explicit BeginTransaction RPC.
        expect_begin_transaction(&mut mock, &mut sequence, None, vec![30]);

        // Query yields row 1, then chunk 2 aborts.
        mock.expect_execute_streaming_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(|request| {
                assert_transaction_id(&request.into_inner(), &[30]);
                Ok(halfway_aborted_streaming_response(None, "alice"))
            });

        // Attempt 2: Explicit BeginTransaction with previous_transaction_id = [30].
        expect_begin_transaction(&mut mock, &mut sequence, Some(vec![30]), vec![31]);

        // Query succeeds.
        mock.expect_execute_streaming_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(|request| {
                assert_transaction_id(&request.into_inner(), &[31]);
                Ok(single_row_streaming_response(None, "alice"))
            });

        expect_commit(&mut mock, &mut sequence, vec![31]);

        let (database_client, _server) = setup_db_client(mock).await;
        let runner = database_client
            .read_write_transaction()
            .set_begin_transaction_option(BeginTransactionOption::ExplicitBegin)
            .with_retry_policy(BasicTransactionRetryPolicy::new().with_max_attempts(3))
            .build();

        let attempt_counter = Arc::new(AtomicU32::new(0));
        let attempt_counter_clone = Arc::clone(&attempt_counter);
        let transaction_result = runner
            .run(async move |transaction| {
                let _attempt = attempt_counter_clone.fetch_add(1, Ordering::SeqCst) + 1;
                let mut result_set = transaction
                    .execute_query("SELECT Name FROM Users WHERE Id = 1")
                    .await?;
                while let Some(_row) = result_set.next().await.transpose()? {}
                Ok(())
            })
            .await?;

        assert_eq!(
            attempt_counter.load(Ordering::SeqCst),
            2,
            "Expected 2 attempts"
        );
        assert!(
            transaction_result
                .commit_response
                .commit_timestamp
                .is_some(),
            "Expected commit timestamp"
        );
        Ok(())
    }

    #[tokio_test_no_panics]
    async fn read_write_transaction_query_aborted_on_retry_after_initial_statement_failed()
    -> Result<()> {
        let mut mock = create_session_mock();
        let mut sequence = Sequence::new();

        // Attempt 1: First statement (query) fails with InvalidArgument (table not found).
        mock.expect_execute_streaming_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(|_| {
                Err(TonicStatus::new(
                    TonicCode::InvalidArgument,
                    "Table not found",
                ))
            });

        // Attempt 2: Because initial statement failed, runner switches to explicit begin.
        expect_begin_transaction(&mut mock, &mut sequence, None, vec![50]);

        // Query on attempt 2 fails with Aborted!
        mock.expect_execute_streaming_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(|request| {
                assert_transaction_id(&request.into_inner(), &[50]);
                Err(create_aborted_status(StdDuration::from_nanos(1)))
            });

        // Attempt 3: Explicit begin with previous_transaction_id = [50].
        expect_begin_transaction(&mut mock, &mut sequence, Some(vec![50]), vec![51]);

        // Query succeeds on attempt 3.
        mock.expect_execute_streaming_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(|request| {
                assert_transaction_id(&request.into_inner(), &[51]);
                Ok(single_row_streaming_response(None, "alice"))
            });

        expect_commit(&mut mock, &mut sequence, vec![51]);

        let (database_client, _server) = setup_db_client(mock).await;
        let runner = database_client
            .read_write_transaction()
            .with_retry_policy(BasicTransactionRetryPolicy::new().with_max_attempts(4))
            .build();

        let attempt_counter = Arc::new(AtomicU32::new(0));
        let attempt_counter_clone = Arc::clone(&attempt_counter);
        let transaction_result = runner
            .run(async move |transaction| {
                let current_attempt = attempt_counter_clone.fetch_add(1, Ordering::SeqCst) + 1;
                let query_result = transaction
                    .execute_query("SELECT Name FROM Users WHERE Id = 1")
                    .await;
                if current_attempt == 1 {
                    // Application catches the InvalidArgument error and tries an update,
                    // which fails fast with synthetic Aborted.
                    assert!(query_result.is_err(), "Attempt 1 query must fail");
                    let follow_up_result = transaction
                        .execute_update("UPDATE Users SET active = true WHERE id = 1")
                        .await;
                    let follow_up_error = follow_up_result
                        .expect_err("Follow-up statement after first statement failure must fail");
                    return Err(follow_up_error);
                }
                let mut result_set = query_result?;
                while let Some(_row) = result_set.next().await.transpose()? {}
                Ok(())
            })
            .await?;

        assert_eq!(
            attempt_counter.load(Ordering::SeqCst),
            3,
            "Expected 3 attempts"
        );
        assert!(
            transaction_result
                .commit_response
                .commit_timestamp
                .is_some(),
            "Expected commit timestamp"
        );
        Ok(())
    }

    #[tokio_test_no_panics]
    async fn read_write_transaction_query_aborted_swallowed_subsequent_statement_fails_fast()
    -> Result<()> {
        let mut mock = create_session_mock();
        let mut sequence = Sequence::new();

        // Attempt 1: Query stream yields row 1 with ID [60], then chunk 2 aborts.
        mock.expect_execute_streaming_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(|_| Ok(halfway_aborted_streaming_response(Some(vec![60]), "alice")));

        // Attempt 2: Both query and update succeed.
        mock.expect_execute_streaming_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(|_| Ok(single_row_streaming_response(Some(vec![61]), "alice")));

        mock.expect_execute_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(|_| Ok(dml_response(None)));

        expect_commit(&mut mock, &mut sequence, vec![61]);

        let (database_client, _server) = setup_db_client(mock).await;
        let runner = database_client
            .read_write_transaction()
            .with_retry_policy(BasicTransactionRetryPolicy::new().with_max_attempts(3))
            .build();

        let attempt_counter = Arc::new(AtomicU32::new(0));
        let attempt_counter_clone = Arc::clone(&attempt_counter);
        let transaction_result = runner
            .run(async move |transaction| {
                let current_attempt = attempt_counter_clone.fetch_add(1, Ordering::SeqCst) + 1;
                let mut result_set = transaction
                    .execute_query("SELECT Name FROM Users WHERE Id = 1")
                    .await?;
                if current_attempt == 1 {
                    // Consume first row
                    let row = result_set.next().await.expect("expected first row")?;
                    let name: String = row.get("Name")?;
                    assert_eq!(name, "alice", "expected name to be alice");
                    // Next row fails with Aborted; closure swallows it!
                    let stream_error = result_set.next().await.expect("expected stream error item");
                    let _error = stream_error.expect_err("Expected abort in stream");

                    // Stream must be fused: subsequent poll of next() after abort must return None!
                    let fused_next = result_set.next().await;
                    assert!(
                        fused_next.is_none(),
                        "ResultSet must return None after stream error, got: {fused_next:?}"
                    );

                    // Subsequent statement must fail fast with Aborted!
                    let update_result = transaction
                        .execute_update("UPDATE Users SET active = true WHERE id = 2")
                        .await;
                    let update_error = update_result.expect_err(
                        "Subsequent statement after query abort must fail with Aborted",
                    );
                    assert!(
                        is_aborted(&update_error),
                        "Expected update error to be Aborted, got: {update_error}"
                    );
                    return Err(update_error);
                }

                while let Some(_row) = result_set.next().await.transpose()? {}
                transaction
                    .execute_update("UPDATE Users SET active = true WHERE id = 2")
                    .await?;
                Ok(())
            })
            .await?;

        assert_eq!(
            attempt_counter.load(Ordering::SeqCst),
            2,
            "Expected 2 attempts"
        );
        assert!(
            transaction_result
                .commit_response
                .commit_timestamp
                .is_some(),
            "Expected commit timestamp"
        );
        Ok(())
    }

    #[tokio_test_no_panics]
    async fn read_write_transaction_query_aborted_preserves_retry_delay() -> Result<()> {
        let mut mock = create_session_mock();
        mock.expect_execute_streaming_sql().once().returning(|_| {
            let receiver = adapt([
                Ok(create_query_partial_result_set(
                    Some(vec![70]),
                    &["alice"],
                    false,
                )),
                Err(create_aborted_status(StdDuration::from_millis(42))),
            ]);
            Ok(Response::from(receiver))
        });

        let (database_client, _server) = setup_db_client(mock).await;
        let transaction = ReadWriteTransactionBuilder::new(database_client)
            .build(None)
            .await?;

        let mut result_set = transaction
            .execute_query("SELECT Name FROM Users WHERE Id = 1")
            .await?;
        let first_row = result_set.next().await.expect("first row must exist")?;
        let name: String = first_row.get("Name")?;
        assert_eq!(name, "alice", "expected name to be alice");

        let stream_error_item = result_set.next().await.expect("stream error must exist");
        let stream_error = stream_error_item.expect_err("stream item should be Err");
        assert!(is_aborted(&stream_error), "stream error must be Aborted");

        assert!(
            transaction.is_aborted(),
            "transaction must be recorded as aborted"
        );
        let aborted_info = transaction
            .aborted_info()
            .expect("aborted_info must be recorded");
        assert_eq!(
            aborted_info.retry_delay,
            Some(StdDuration::from_millis(42)),
            "aborted_info must record retry delay of 42ms"
        );

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn read_write_transaction_read_aborted_halfway_retries_and_succeeds() -> Result<()> {
        let mut mock = create_session_mock();
        let mut sequence = Sequence::new();

        // Attempt 1: streaming_read starts transaction, yields 1 row, then aborts
        mock.expect_streaming_read()
            .once()
            .in_sequence(&mut sequence)
            .returning(|_| {
                let receiver = adapt([
                    Ok(create_query_partial_result_set(
                        Some(vec![80]),
                        &["alice"],
                        false,
                    )),
                    Err(create_aborted_status(StdDuration::from_nanos(1))),
                ]);
                Ok(Response::from(receiver))
            });

        // Attempt 2: streaming_read starts transaction with previous_transaction_id, yields row and completes
        mock.expect_streaming_read()
            .once()
            .in_sequence(&mut sequence)
            .returning(|request| {
                let request = request.into_inner();
                let transaction = request
                    .transaction
                    .expect("transaction selector must be present");
                match transaction.selector {
                    Some(ProtoSelector::Begin(begin)) => {
                        let read_write = begin.mode.expect("read-write options must be present");
                        match read_write {
                            Mode::ReadWrite(read_write) => {
                                assert_eq!(
                                    read_write.multiplexed_session_previous_transaction_id,
                                    vec![80],
                                    "retry attempt must send previous_transaction_id"
                                );
                            }
                            _ => panic!("expected ReadWrite mode"),
                        }
                    }
                    _ => panic!("expected Begin selector on attempt 2"),
                }
                let receiver = adapt([Ok(create_query_partial_result_set(
                    Some(vec![81]),
                    &["alice"],
                    false,
                ))]);
                Ok(Response::from(receiver))
            });

        expect_commit(&mut mock, &mut sequence, vec![81]);

        let (database_client, _server) = setup_db_client(mock).await;
        let runner = database_client
            .read_write_transaction()
            .with_retry_policy(BasicTransactionRetryPolicy::new().with_max_attempts(3))
            .build();

        let attempt_counter = Arc::new(AtomicU32::new(0));
        let attempt_counter_clone = Arc::clone(&attempt_counter);
        let transaction_result = runner
            .run(async move |transaction| {
                attempt_counter_clone.fetch_add(1, Ordering::SeqCst);
                let read_request = ReadRequest::builder("Users", vec!["Name"])
                    .with_keys(key!["1"])
                    .build();
                let mut result_set = transaction.execute_read(read_request).await?;
                let mut names = Vec::new();
                while let Some(row) = result_set.next().await.transpose()? {
                    let name: String = row.get("Name")?;
                    names.push(name);
                }
                Ok(names)
            })
            .await?;

        assert_eq!(
            attempt_counter.load(Ordering::SeqCst),
            2,
            "Expected 2 attempts"
        );
        assert_eq!(
            transaction_result.result,
            vec!["alice"],
            "expected result to match alice"
        );
        Ok(())
    }

    #[tokio_test_no_panics]
    async fn read_write_transaction_execute_batch_update_aborts_records_aborted_state() -> Result<()>
    {
        let mut mock = create_session_mock();
        mock.expect_execute_batch_dml()
            .once()
            .returning(|_| Err(create_aborted_status(StdDuration::from_nanos(1))));

        let (database_client, _server) = setup_db_client(mock).await;
        let transaction = ReadWriteTransactionBuilder::new(database_client)
            .build(None)
            .await?;

        let batch = BatchDml::builder()
            .add_statement(
                Statement::builder("UPDATE Users SET active = true WHERE id = 1").build(),
            )
            .build();
        let result = transaction.execute_batch_update(batch).await;
        let error = result.expect_err("expected execute_batch_update to fail with Aborted");
        assert!(is_aborted(&error), "error must be Aborted: {error:?}");
        assert!(
            transaction.is_aborted(),
            "transaction must record abort state on batch update failure"
        );
        assert!(
            transaction.aborted_info().is_some(),
            "transaction aborted_info must be populated"
        );
        Ok(())
    }

    #[tokio_test_no_panics]
    async fn read_write_transaction_emulator_internal_error_records_aborted_state() -> Result<()> {
        let mut mock = create_session_mock();
        mock.expect_execute_sql().once().returning(|_| {
            Err(TonicStatus::new(
                TonicCode::Internal,
                "Schema generation error: was not registered with the Action Manager",
            ))
        });

        let (mut database_client, _server) = setup_db_client(mock).await;
        database_client.spanner.is_emulator = true;
        let transaction = ReadWriteTransactionBuilder::new(database_client)
            .build(None)
            .await?;

        let result = transaction
            .execute_update("UPDATE Users SET active = true WHERE id = 1")
            .await;
        let error = result.expect_err("expected emulator schema generation error");
        assert_eq!(
            error.status().map(|status| status.code),
            Some(Code::Internal),
            "expected Internal gRPC error code"
        );
        assert!(
            transaction.is_aborted(),
            "transaction must record emulator internal error as aborted"
        );
        assert!(
            transaction.aborted_info().is_some(),
            "transaction aborted_info must be populated for emulator error"
        );
        Ok(())
    }

    #[tokio_test_no_panics]
    async fn read_write_transaction_mutations_only_commit_begin_abort_records_aborted_state()
    -> Result<()> {
        let mut mock = create_session_mock();
        mock.expect_begin_transaction()
            .once()
            .returning(|_| Err(create_aborted_status(StdDuration::from_nanos(1))));

        let (database_client, _server) = setup_db_client(mock).await;
        let transaction = ReadWriteTransactionBuilder::new(database_client)
            .build(None)
            .await?;

        transaction.buffer(vec![Mutation::delete("Users", KeySet::all())])?;
        let result = transaction.commit().await;
        let error = result.expect_err("expected commit to fail when begin aborts");
        assert!(is_aborted(&error), "error must be Aborted: {error:?}");
        assert!(
            transaction.is_aborted(),
            "transaction must record abort state on begin transaction abort"
        );
        assert!(
            transaction.aborted_info().is_some(),
            "transaction aborted_info must be populated"
        );
        Ok(())
    }

    #[tokio_test_no_panics]
    async fn read_write_transaction_commit_abort_records_aborted_state() -> Result<()> {
        let mut mock = create_session_mock();
        mock.expect_execute_sql().once().returning(|_| {
            Ok(Response::new(ProtoResultSet {
                metadata: Some(ResultSetMetadata {
                    transaction: Some(ProtoTransaction {
                        id: vec![77],
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
                stats: Some(ResultSetStats {
                    row_count: Some(RowCount::RowCountExact(1)),
                    ..Default::default()
                }),
                ..Default::default()
            }))
        });
        mock.expect_commit()
            .once()
            .returning(|_| Err(create_aborted_status(StdDuration::from_millis(25))));

        let (database_client, _server) = setup_db_client(mock).await;
        let transaction = ReadWriteTransactionBuilder::new(database_client)
            .build(None)
            .await?;

        let _ = transaction
            .execute_update("UPDATE Users SET active = true WHERE id = 1")
            .await?;

        let commit_result = transaction.commit().await;
        let error = commit_result.expect_err("expected commit to fail with Aborted");
        assert!(is_aborted(&error), "error must be Aborted: {error:?}");
        assert!(
            transaction.is_aborted(),
            "transaction handle must record abort state on commit failure"
        );
        let aborted_info = transaction
            .aborted_info()
            .expect("transaction aborted_info must be populated");
        assert_eq!(
            aborted_info.retry_delay,
            Some(StdDuration::from_millis(25)),
            "aborted_info must record retry delay from commit abort"
        );
        Ok(())
    }

    #[tokio_test_no_panics]
    async fn read_write_transaction_fails_fast_when_transaction_is_aborted() -> Result<()> {
        let mock = create_session_mock();
        let (database_client, _server) = setup_db_client(mock).await;
        let transaction = ReadWriteTransactionBuilder::new(database_client)
            .build(None)
            .await?;

        let aborted_error = Error::service(Status::default().set_code(Code::Aborted));
        transaction.context.record_if_aborted(&aborted_error);

        let update_result = transaction
            .execute_update("UPDATE Users SET active = true WHERE id = 1")
            .await;
        let update_error =
            update_result.expect_err("execute_update must fail fast when transaction aborted");
        assert!(
            is_aborted(&update_error),
            "execute_update error must be Aborted: {update_error:?}"
        );

        let query_result = transaction.execute_query("SELECT 1").await;
        let query_error =
            query_result.expect_err("execute_query must fail fast when transaction aborted");
        assert!(
            is_aborted(&query_error),
            "execute_query error must be Aborted: {query_error:?}"
        );

        let read_request = ReadRequest::builder("Users", vec!["id"])
            .with_keys(KeySet::all())
            .build();
        let read_result = transaction.execute_read(read_request).await;
        let read_error =
            read_result.expect_err("execute_read must fail fast when transaction aborted");
        assert!(
            is_aborted(&read_error),
            "execute_read error must be Aborted: {read_error:?}"
        );

        let batch = BatchDml::builder()
            .add_statement(
                Statement::builder("UPDATE Users SET active = true WHERE id = 1").build(),
            )
            .build();
        let batch_result = transaction.execute_batch_update(batch).await;
        let batch_error =
            batch_result.expect_err("execute_batch_update must fail fast when transaction aborted");
        assert!(
            is_aborted(&batch_error),
            "execute_batch_update error must be Aborted: {batch_error:?}"
        );

        let commit_result = transaction.commit().await;
        let commit_error =
            commit_result.expect_err("commit must fail fast when transaction aborted");
        assert!(
            is_aborted(&commit_error),
            "commit error must be Aborted: {commit_error:?}"
        );

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn read_write_transaction_batch_dml_partial_success_aborted_inline_begin_retries()
    -> Result<()> {
        let mut mock = create_session_mock();
        let mut sequence = Sequence::new();

        // 1. Attempt 1: First statement is Batch DML with inline begin.
        // Returns 1 successful result set with transaction ID [71], but status is Aborted.
        mock.expect_execute_batch_dml()
            .once()
            .in_sequence(&mut sequence)
            .returning(|request| {
                let inner = request.into_inner();
                let selector = inner
                    .transaction
                    .and_then(|transaction| transaction.selector);
                assert!(
                    matches!(selector, Some(ProtoSelector::Begin(_))),
                    "Attempt 1 must use inline Begin"
                );
                Ok(Response::new(ProtoExecuteBatchDmlResponse {
                    result_sets: vec![ProtoResultSet {
                        metadata: Some(ResultSetMetadata {
                            transaction: Some(ProtoTransaction {
                                id: vec![71],
                                ..Default::default()
                            }),
                            ..Default::default()
                        }),
                        stats: Some(ResultSetStats {
                            row_count: Some(RowCount::RowCountExact(1)),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }],
                    status: Some(RpcStatus {
                        code: TonicCode::Aborted as i32,
                        message: "Transaction was aborted".to_string(),
                        details: vec![],
                    }),
                    precommit_token: None,
                }))
            });

        // 2. Attempt 2: Runner retries with inline begin and previous_transaction_id = [71].
        mock.expect_execute_batch_dml()
            .once()
            .in_sequence(&mut sequence)
            .returning(|request| {
                let inner = request.into_inner();
                assert_inline_begin_selector(inner.transaction.as_ref(), Some(&[71]));
                Ok(Response::new(ProtoExecuteBatchDmlResponse {
                    result_sets: vec![ProtoResultSet {
                        metadata: Some(ResultSetMetadata {
                            transaction: Some(ProtoTransaction {
                                id: vec![72],
                                ..Default::default()
                            }),
                            ..Default::default()
                        }),
                        stats: Some(ResultSetStats {
                            row_count: Some(RowCount::RowCountExact(1)),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }],
                    status: Some(RpcStatus {
                        code: TonicCode::Ok as i32,
                        message: String::new(),
                        details: vec![],
                    }),
                    precommit_token: None,
                }))
            });

        expect_commit(&mut mock, &mut sequence, vec![72]);

        let (database_client, _server) = setup_db_client(mock).await;
        let runner = database_client
            .read_write_transaction()
            .with_retry_policy(BasicTransactionRetryPolicy::new().with_max_attempts(3))
            .build();

        let attempt_counter = Arc::new(AtomicU32::new(0));
        let attempt_counter_clone = Arc::clone(&attempt_counter);
        let transaction_result = runner
            .run(async move |transaction| {
                let current_attempt = attempt_counter_clone.fetch_add(1, Ordering::SeqCst) + 1;
                let batch = BatchDml::builder()
                    .add_statement(
                        Statement::builder("UPDATE Users SET active = true WHERE id = 1").build(),
                    )
                    .add_statement(
                        Statement::builder("UPDATE Users SET active = true WHERE id = 2").build(),
                    )
                    .build();
                let counts = transaction.execute_batch_update(batch).await?;
                if current_attempt == 2 {
                    assert_eq!(
                        counts,
                        vec![1],
                        "Attempt 2 should succeed with update count 1"
                    );
                }
                Ok(())
            })
            .await?;

        assert_eq!(
            attempt_counter.load(Ordering::SeqCst),
            2,
            "Expected 2 attempts"
        );
        assert!(
            transaction_result
                .commit_response
                .commit_timestamp
                .is_some(),
            "Expected commit timestamp"
        );
        Ok(())
    }

    #[tokio_test_no_panics]
    async fn read_write_transaction_inline_begin_aborted_swallowed_commit_fails_fast_with_server_error()
    -> Result<()> {
        let mut mock = create_session_mock();
        let mut sequence = Sequence::new();

        // 1. Attempt 1: First statement is execute_update with inline begin.
        // It fails with Aborted: "server transaction abort".
        mock.expect_execute_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(|request| {
                let inner = request.into_inner();
                let selector = inner
                    .transaction
                    .and_then(|transaction| transaction.selector);
                assert!(
                    matches!(selector, Some(ProtoSelector::Begin(_))),
                    "Attempt 1 must use inline Begin"
                );
                Err(TonicStatus::aborted("server transaction abort"))
            });

        // Note: Commit RPC is NOT called on Attempt 1 because commit_internal detects the recorded
        // aborted status and fails fast without issuing a network Commit RPC.

        // 2. Attempt 2: Because Attempt 1 aborted (rather than a non-aborted failure),
        // runner retries with InlineBegin!
        mock.expect_execute_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(|request| {
                let inner = request.into_inner();
                let selector = inner
                    .transaction
                    .and_then(|transaction| transaction.selector);
                assert!(
                    matches!(selector, Some(ProtoSelector::Begin(_))),
                    "Attempt 2 must retry with inline Begin"
                );
                Ok(Response::new(ProtoResultSet {
                    metadata: Some(ResultSetMetadata {
                        transaction: Some(ProtoTransaction {
                            id: vec![88],
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    stats: Some(ResultSetStats {
                        row_count: Some(RowCount::RowCountExact(1)),
                        ..Default::default()
                    }),
                    ..Default::default()
                }))
            });

        expect_commit(&mut mock, &mut sequence, vec![88]);

        let (database_client, _server) = setup_db_client(mock).await;
        let runner = database_client
            .read_write_transaction()
            .with_retry_policy(BasicTransactionRetryPolicy::new().with_max_attempts(3))
            .build();

        let attempt_counter = Arc::new(AtomicU32::new(0));
        let attempt_counter_clone = Arc::clone(&attempt_counter);
        let transaction_result = runner
            .run(async move |transaction| {
                let current_attempt = attempt_counter_clone.fetch_add(1, Ordering::SeqCst) + 1;
                if current_attempt == 1 {
                    // Execute statement with inline begin, which aborts on the server.
                    let result = transaction
                        .execute_update("UPDATE Users SET active = true WHERE id = 1")
                        .await;
                    assert!(result.is_err(), "Expected first attempt statement to fail");
                    // Application catches the error and swallows it, returning Ok(()).
                    // This forces commit() to execute on the aborted transaction.
                    return Ok(());
                }

                let count = transaction
                    .execute_update("UPDATE Users SET active = true WHERE id = 1")
                    .await?;
                assert_eq!(count, 1, "Attempt 2 should succeed with update count 1");
                Ok(())
            })
            .await?;

        assert_eq!(
            attempt_counter.load(Ordering::SeqCst),
            2,
            "Expected 2 attempts"
        );
        assert!(
            transaction_result
                .commit_response
                .commit_timestamp
                .is_some(),
            "Expected commit timestamp"
        );
        Ok(())
    }

    #[tokio_test_no_panics]
    async fn read_write_transaction_inline_begin_aborted_commit_returns_original_error_message()
    -> Result<()> {
        let mut mock = create_session_mock();
        mock.expect_execute_sql()
            .once()
            .returning(|_| Err(TonicStatus::aborted("exact original abort reason")));

        let (database_client, _server) = setup_db_client(mock).await;
        let transaction = ReadWriteTransactionBuilder::new(database_client)
            .build(None)
            .await?;

        let statement_result = transaction
            .execute_update("UPDATE Users SET active = true WHERE id = 1")
            .await;
        assert!(statement_result.is_err(), "Expected statement to fail");

        let commit_result = transaction.commit().await;
        let commit_error = commit_result.expect_err("Expected commit to fail fast");
        assert!(is_aborted(&commit_error), "Error must be Aborted");
        assert!(
            commit_error
                .to_string()
                .contains("exact original abort reason"),
            "Commit error must preserve original abort message, got: {commit_error}"
        );
        assert!(
            !commit_error
                .to_string()
                .contains("Aborted due to failed initial statement"),
            "Commit error must not be synthetic generic message, got: {commit_error}"
        );
        Ok(())
    }

    #[tokio_test_no_panics]
    async fn read_write_transaction_concurrent_statements_leader_aborted_preserves_error_details()
    -> Result<()> {
        let mut mock = create_session_mock();

        // The leader's inline begin RPC fails with Aborted and a custom message
        mock.expect_execute_sql().once().returning(|_| {
            Err(TonicStatus::aborted(
                "concurrent contention on user key range",
            ))
        });

        let (database_client, _server) = setup_db_client(mock).await;
        let transaction = ReadWriteTransactionBuilder::new(database_client)
            .build(None)
            .await?;

        let statement1_future =
            transaction.execute_update("UPDATE Users SET active = true WHERE id = 1");
        let statement2_future =
            transaction.execute_update("UPDATE Users SET active = true WHERE id = 2");

        let (result1, result2) = tokio::join!(statement1_future, statement2_future);

        let error1 = result1.expect_err("statement 1 must fail with abort");
        let error2 = result2.expect_err("statement 2 must fail with abort");

        assert!(is_aborted(&error1), "statement 1 error must be Aborted");
        assert!(is_aborted(&error2), "statement 2 error must be Aborted");

        assert!(
            error1
                .to_string()
                .contains("concurrent contention on user key range"),
            "statement 1 must preserve real server error, got: {error1}"
        );
        assert!(
            error2
                .to_string()
                .contains("concurrent contention on user key range"),
            "statement 2 must preserve real server error, got: {error2}"
        );

        assert!(
            !error1
                .to_string()
                .contains("Aborted due to failed initial statement"),
            "statement 1 must not contain synthetic message, got: {error1}"
        );
        assert!(
            !error2
                .to_string()
                .contains("Aborted due to failed initial statement"),
            "statement 2 must not contain synthetic message, got: {error2}"
        );

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn read_write_transaction_completed_result_set_returns_none_after_subsequent_abort()
    -> Result<()> {
        let mut mock = create_session_mock();
        let mut sequence = Sequence::new();

        // Query completes cleanly with a single row (seen_last == true)
        mock.expect_execute_streaming_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(|_| Ok(single_row_streaming_response(Some(vec![80]), "alice")));

        // Subsequent update fails with Aborted
        mock.expect_execute_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(|_| Err(TonicStatus::aborted("lock conflict on user update")));

        let (database_client, _server) = setup_db_client(mock).await;
        let transaction = ReadWriteTransactionBuilder::new(database_client)
            .build(None)
            .await?;

        let mut result_set = transaction
            .execute_query("SELECT Name FROM Users WHERE Id = 1")
            .await?;

        // Consume the only row
        let row = result_set
            .next()
            .await
            .expect("stream should yield an item")?;
        let name: String = row.get("Name")?;
        assert_eq!(name, "alice", "expected name alice");

        // The update fails with Aborted
        let update_error = transaction
            .execute_update("UPDATE Users SET active = true WHERE id = 1")
            .await
            .expect_err("update should fail with Aborted");
        assert!(is_aborted(&update_error), "error should be Aborted");

        // Calling next() on the already-completed result_set must return None, NOT an error!
        let next_item = result_set.next().await;
        assert!(
            next_item.is_none(),
            "Completed ResultSet must return None even after a subsequent abort in the transaction, got: {next_item:?}"
        );

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn read_write_transaction_concurrent_follower_woken_by_partial_inline_begin_batch_dml_abort_fails_fast()
    -> Result<()> {
        let mut mock = create_session_mock();
        let mut sequence = Sequence::new();

        // Attempt 1: Leader runs ExecuteBatchDml with InlineBegin.
        // Statement 0 succeeds (returning transaction ID [85]), statement 1 aborts.
        mock.expect_execute_batch_dml()
            .once()
            .in_sequence(&mut sequence)
            .returning(|request| {
                let inner = request.into_inner();
                assert_inline_begin_selector(inner.transaction.as_ref(), None);
                Ok(Response::new(ProtoExecuteBatchDmlResponse {
                    result_sets: vec![ProtoResultSet {
                        metadata: Some(ResultSetMetadata {
                            transaction: Some(ProtoTransaction {
                                id: vec![85],
                                ..Default::default()
                            }),
                            ..Default::default()
                        }),
                        stats: Some(ResultSetStats {
                            row_count: Some(RowCount::RowCountExact(1)),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }],
                    status: Some(RpcStatus {
                        code: TonicCode::Aborted as i32,
                        message: "concurrent lock abort in batch statement 1".to_string(),
                        details: vec![],
                    }),
                    ..Default::default()
                }))
            });

        // Notice: NO execute_sql call on Attempt 1 for the follower, because the follower
        // must fail fast client-side when woken by the partial batch DML abort!

        // Attempt 2: Both statements succeed. InlineBegin carries previous_transaction_id = [85].
        mock.expect_execute_batch_dml()
            .once()
            .in_sequence(&mut sequence)
            .returning(|request| {
                let inner = request.into_inner();
                assert_inline_begin_selector(inner.transaction.as_ref(), Some(&[85]));
                Ok(Response::new(ProtoExecuteBatchDmlResponse {
                    result_sets: vec![
                        ProtoResultSet {
                            metadata: Some(ResultSetMetadata {
                                transaction: Some(ProtoTransaction {
                                    id: vec![86],
                                    ..Default::default()
                                }),
                                ..Default::default()
                            }),
                            stats: Some(ResultSetStats {
                                row_count: Some(RowCount::RowCountExact(1)),
                                ..Default::default()
                            }),
                            ..Default::default()
                        },
                        ProtoResultSet {
                            stats: Some(ResultSetStats {
                                row_count: Some(RowCount::RowCountExact(1)),
                                ..Default::default()
                            }),
                            ..Default::default()
                        },
                    ],
                    status: Some(RpcStatus {
                        code: TonicCode::Ok as i32,
                        ..Default::default()
                    }),
                    ..Default::default()
                }))
            });

        mock.expect_execute_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(|request| {
                let inner = request.into_inner();
                assert_eq!(
                    inner.transaction,
                    Some(TransactionSelector {
                        selector: Some(ProtoSelector::Id(vec![86])),
                    }),
                    "Attempt 2 follower must use established transaction ID [86]"
                );
                Ok(dml_response(None))
            });

        expect_commit(&mut mock, &mut sequence, vec![86]);

        let (database_client, _server) = setup_db_client(mock).await;
        let runner = database_client
            .read_write_transaction()
            .with_retry_policy(BasicTransactionRetryPolicy::new().with_max_attempts(3))
            .build();

        let attempts = Arc::new(AtomicU32::new(0));
        let attempts_clone = Arc::clone(&attempts);
        let transaction_result = runner
            .run(async move |transaction| {
                let current_attempt = attempts_clone.fetch_add(1, Ordering::SeqCst) + 1;
                let batch = BatchDml::builder()
                    .add_statement("UPDATE Users SET active = true WHERE id = 1")
                    .add_statement("UPDATE Users SET active = true WHERE id = 2")
                    .build();

                if current_attempt == 1 {
                    let (batch_result, follower_result) = tokio::join!(
                        transaction.execute_batch_update(batch),
                        transaction.execute_update("UPDATE Users SET active = false WHERE id = 3"),
                    );
                    let batch_error =
                        batch_result.expect_err("batch update must fail with Aborted");
                    let follower_error =
                        follower_result.expect_err("follower update must fail fast with Aborted");

                    assert!(is_aborted(&batch_error), "batch error must be Aborted");
                    assert!(
                        is_aborted(&follower_error),
                        "follower error must be Aborted"
                    );
                    return Err(follower_error);
                }

                let batch_counts = transaction.execute_batch_update(batch).await?;
                assert_eq!(batch_counts, vec![1, 1], "expected 2 updated rows in batch");
                let follower_count = transaction
                    .execute_update("UPDATE Users SET active = false WHERE id = 3")
                    .await?;
                assert_eq!(follower_count, 1, "expected 1 updated row in follower");
                Ok(())
            })
            .await?;

        assert_eq!(
            attempts.load(Ordering::SeqCst),
            2,
            "runner must have executed 2 attempts"
        );
        assert!(
            transaction_result
                .commit_response
                .commit_timestamp
                .is_some(),
            "commit must have timestamp"
        );
        Ok(())
    }

    #[tokio_test_no_panics]
    async fn read_write_transaction_inline_begin_first_statement_failed_retains_previous_transaction_id()
    -> anyhow::Result<()> {
        let mut mock = create_session_mock();
        let mut sequence = Sequence::new();

        // Attempt 1:
        // Statement 1 inline begins transaction [10].
        mock.expect_execute_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(|request| {
                let inner = request.into_inner();
                assert_inline_begin(&inner, None);
                Ok(dml_response(Some(vec![10])))
            });

        // Statement 2 aborts on Spanner.
        mock.expect_execute_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(|request| {
                let inner = request.into_inner();
                assert_transaction_id(&inner, &[10]);
                Err(TonicStatus::aborted("concurrent lock abort in statement 2"))
            });

        // Attempt 2:
        // Statement 1 inline begins with previous_transaction_id = [10], but fails with InvalidArgument (FirstStatementFailed).
        mock.expect_execute_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(|request| {
                let inner = request.into_inner();
                assert_inline_begin(&inner, Some(&[10]));
                Err(TonicStatus::invalid_argument("Column does not exist"))
            });

        // Notice: NO execute_sql on Spanner for Statement 2 on Attempt 2, because Statement 2 fails fast client-side!

        // Attempt 3:
        // Because Attempt 2 failed with FirstStatementFailed, Attempt 3 forces ExplicitBegin.
        // Crucial assertion: BeginTransaction MUST retain previous_transaction_id = [10] from Attempt 1!
        expect_begin_transaction(&mut mock, &mut sequence, Some(vec![10]), vec![12]);

        mock.expect_execute_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(|request| {
                let inner = request.into_inner();
                assert_transaction_id(&inner, &[12]);
                Ok(dml_response(None))
            });

        mock.expect_execute_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(|request| {
                let inner = request.into_inner();
                assert_transaction_id(&inner, &[12]);
                Ok(dml_response(None))
            });

        expect_commit(&mut mock, &mut sequence, vec![12]);

        let (database_client, _server) = setup_db_client(mock).await;
        let runner = database_client
            .read_write_transaction()
            .with_retry_policy(BasicTransactionRetryPolicy::new().with_max_attempts(4))
            .build();

        let mut attempts = 0;
        let transaction_result = runner
            .run(async |transaction| {
                attempts += 1;
                if attempts == 1 {
                    let first_count = transaction
                        .execute_update("UPDATE Users SET active = true WHERE id = 1")
                        .await?;
                    assert_eq!(first_count, 1);

                    // Statement 2 aborts on Spanner
                    let second_error = transaction
                        .execute_update("UPDATE Users SET active = true WHERE id = 2")
                        .await
                        .expect_err("statement 2 must abort on attempt 1");
                    return Err(second_error);
                }

                if attempts == 2 {
                    // Statement 1 fails with InvalidArgument
                    let first_result = transaction
                        .execute_update("UPDATE Users SET non_existing = 1 WHERE id = 1")
                        .await;
                    assert!(first_result.is_err(), "first statement must fail");

                    // Statement 2 encounters FirstStatementFailed and returns Aborted
                    let second_error = transaction
                        .execute_update("UPDATE Users SET active = true WHERE id = 2")
                        .await
                        .expect_err("second statement must fail fast with synthetic aborted");
                    return Err(second_error);
                }

                let first_count = transaction
                    .execute_update("UPDATE Users SET active = true WHERE id = 1")
                    .await?;
                let second_count = transaction
                    .execute_update("UPDATE Users SET active = true WHERE id = 2")
                    .await?;
                Ok(first_count + second_count)
            })
            .await?;

        assert_eq!(attempts, 3, "runner must have executed 3 attempts");
        assert_eq!(transaction_result.result, 2, "update count should be 2");
        Ok(())
    }
}
