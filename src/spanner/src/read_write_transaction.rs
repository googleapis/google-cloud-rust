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

use crate::BatchDml;
use crate::Error;
use crate::RequestOptions;
use crate::client::{Mutation, amend_request_options_for_lar};
use crate::database_client::DatabaseClient;
use crate::error::internal_error;
use crate::model::CommitRequest;
use crate::model::CommitResponse;
use crate::model::ExecuteBatchDmlRequest;
use crate::model::ExecuteBatchDmlResponse;
use crate::model::Mutation as ProtoMutation;
use crate::model::ResultSet as ProtoResultSet;
use crate::model::RollbackRequest;
use crate::model::TransactionOptions;
use crate::model::TransactionSelector;
use crate::model::execute_batch_dml_request::Statement as ExecuteBatchDmlStatement;
use crate::model::request_options::Priority;
use crate::model::result_set_stats::RowCount;
use crate::model::transaction_options::IsolationLevel;
use crate::model::transaction_options::Mode;
use crate::model::transaction_options::ReadWrite;
use crate::model::transaction_options::read_write::ReadLockMode;
use crate::model::transaction_selector::Selector;
use crate::precommit::PrecommitTokenTracker;
use crate::read_only_transaction::{
    BeginTransactionOption, ReadContext, ReadContextTransactionSelector, TransactionState,
};
use crate::result_set::ResultSet;
use crate::statement::Statement;
use crate::transaction_retry_policy::is_aborted;
use crate::write_only_transaction::create_commit_request;
use google_cloud_gax::error::Error as GaxError;
use google_cloud_gax::error::rpc::{Code, Status};
use google_cloud_gax::options::RequestOptions as GaxRequestOptions;
use google_cloud_gax::retry_policy::{Aip194Strict, RetryPolicy};
use google_cloud_gax::retry_result::RetryResult;
use google_cloud_gax::retry_state::RetryState;
use google_cloud_gax::throttle_result::ThrottleResult;
use std::cmp::min;
use std::mem::take;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration as StdDuration;
use tokio::time::Instant;
use wkt::Duration;

/// A builder for [ReadWriteTransaction].
#[derive(Clone, Debug)]
pub(crate) struct ReadWriteTransactionBuilder {
    client: DatabaseClient,
    options: TransactionOptions,
    transaction_tag: Option<String>,
    max_commit_delay: Option<Duration>,
    pub(crate) session_name: String,
    return_commit_stats: bool,
    commit_priority: Priority,
    begin_transaction_option: BeginTransactionOption,
    begin_gax_options: Option<crate::RequestOptions>,
    commit_gax_options: Option<crate::RequestOptions>,
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
        }
    }

    pub(crate) fn with_isolation_level(mut self, isolation_level: IsolationLevel) -> Self {
        self.options = self.options.set_isolation_level(isolation_level);
        self
    }

    pub(crate) fn with_read_lock_mode(mut self, read_lock_mode: ReadLockMode) -> Self {
        if let Some(Mode::ReadWrite(rw)) = self.options.mode.take() {
            self.options = self
                .options
                .set_read_write(rw.set_read_lock_mode(read_lock_mode));
        }
        self
    }

    pub(crate) fn with_previous_transaction_id(mut self, id: Option<bytes::Bytes>) -> Self {
        if let Some(id) = id {
            if let Some(Mode::ReadWrite(rw)) = self.options.mode.take() {
                self.options = self
                    .options
                    .set_read_write(rw.set_multiplexed_session_previous_transaction_id(id));
            }
        }
        self
    }

    pub(crate) fn with_transaction_tag(mut self, tag: impl Into<String>) -> Self {
        self.transaction_tag = Some(tag.into());
        self
    }

    pub(crate) fn with_commit_priority(mut self, priority: Priority) -> Self {
        self.commit_priority = priority;
        self
    }

    pub(crate) fn with_max_commit_delay(mut self, delay: Duration) -> Self {
        self.max_commit_delay = Some(delay);
        self
    }

    pub(crate) fn with_exclude_txn_from_change_streams(mut self, exclude: bool) -> Self {
        self.options = self.options.set_exclude_txn_from_change_streams(exclude);
        self
    }

    pub(crate) fn with_return_commit_stats(mut self, return_stats: bool) -> Self {
        self.return_commit_stats = return_stats;
        self
    }

    pub fn with_begin_transaction_option(mut self, option: BeginTransactionOption) -> Self {
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

    async fn begin(
        &self,
        session_name: String,
        channel_hint: usize,
        request_options: crate::RequestOptions,
    ) -> crate::Result<ReadContextTransactionSelector> {
        let response = crate::read_only_transaction::execute_begin_transaction(
            &self.client,
            session_name,
            self.options.clone(),
            self.transaction_tag.clone(),
            channel_hint,
            request_options,
            None,
        )
        .await?;

        Ok(ReadContextTransactionSelector::Fixed(
            TransactionSelector::default().set_id(response.id),
            None,
        ))
    }

    pub(crate) async fn build(
        &self,
        deadline: Option<Instant>,
    ) -> crate::Result<ReadWriteTransaction> {
        let session_name = self.session_name.clone();
        let channel_hint = self.client.spanner.next_channel_hint();
        let transaction_selector = match self.begin_transaction_option {
            BeginTransactionOption::ExplicitBegin => {
                let mut options = self.begin_gax_options.clone().unwrap_or_default();
                amend_gax_options(
                    self.client.leader_aware_routing_enabled,
                    deadline,
                    &mut options,
                );

                self.begin(session_name.clone(), channel_hint, options)
                    .await?
            }
            BeginTransactionOption::InlineBegin => ReadContextTransactionSelector::Lazy(Arc::new(
                Mutex::new(TransactionState::NotStarted(self.options.clone())),
            )),
        };

        Ok(ReadWriteTransaction {
            context: ReadContext {
                session_name,
                client: self.client.clone(),
                transaction_selector,
                precommit_token_tracker: PrecommitTokenTracker::new(),
                transaction_tag: self.transaction_tag.clone(),
                channel_hint,
                begin_transaction_request_options: None,
            },
            seqno: Arc::new(AtomicI64::new(1)),
            max_commit_delay: self.max_commit_delay,
            return_commit_stats: self.return_commit_stats,
            deadline,
            commit_priority: self.commit_priority.clone(),
            mutations: Arc::new(Mutex::new(Vec::new())),
            begin_gax_options: self.begin_gax_options.clone(),
            commit_gax_options: self.commit_gax_options.clone(),
        })
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
/// If Spanner encounters an error during inline transaction initialization (such as a missing table),
/// it returns an `Ok(ExecuteBatchDmlResponse)` containing the error status but with empty `result_sets`.
/// This implementation evaluates that payload so fallback handlers can recover.
impl CheckServiceError for ExecuteBatchDmlResponse {
    fn check_service_error(&self) -> Option<Error> {
        if self.result_sets.is_empty() {
            if let Some(status) = &self.status {
                if status.code != Code::Ok as i32 {
                    let rpc_status = Status::default()
                        .set_code(status.code)
                        .set_message(status.message.clone());
                    return Some(Error::service(rpc_status));
                }
            }
        }
        None
    }
}

/// A scope-bound guard that manages the state of a lazy transaction start attempt.
///
/// If the first statement in a transaction is executed using an inline `BeginTransaction` option,
/// the transaction selector is transitioned to the `Starting` state.
/// If that initial statement execution fails, or if the transaction ID is not successfully returned,
/// we must reset the starting state back to `NotStarted` and unlock any concurrent threads waiting
/// for this transaction to start.
///
/// This struct implements the RAII pattern:
/// - It is initialized with `active = true` when the statement is starting the transaction.
/// - If the transaction successfully starts and yields a valid ID, the guard is `disarm()`ed.
/// - If the scope exits early due to an error (e.g., aborted error, protocol error, etc.), the guard
///   is dropped, and its `Drop` implementation automatically calls `maybe_reset_starting()` to
///   restore the selector state and notify waiters.
struct LazyTransactionStartGuard {
    selector: ReadContextTransactionSelector,
    active: bool,
}

impl LazyTransactionStartGuard {
    fn new(selector: ReadContextTransactionSelector, active: bool) -> Self {
        Self { selector, active }
    }

    fn disarm(&mut self) {
        self.active = false;
    }
}

impl Drop for LazyTransactionStartGuard {
    fn drop(&mut self) {
        if self.active {
            self.selector.maybe_reset_starting();
        }
    }
}

/// Helper macro to execute a DML or BatchDML RPC with retry logic if the
/// request included a BeginTransaction option.
macro_rules! execute_with_retry {
    ($self:expr, $request:ident, $gax_options:expr, $rpc_method:ident, $extract_id:expr) => {{
        let is_starting = matches!(
            $request
                .transaction
                .as_ref()
                .and_then(|t| t.selector.as_ref()),
            Some(Selector::Begin(_))
        );

        let mut guard =
            LazyTransactionStartGuard::new($self.context.transaction_selector.clone(), is_starting);

        let response_result = $self
            .context
            .client
            .spanner
            .$rpc_method(
                $request.clone(),
                $gax_options.clone(),
                $self.context.channel_hint,
            )
            .await;

        let service_error = response_result
            .as_ref()
            .ok()
            .and_then(|res| res.check_service_error());
        let err_ref = response_result.as_ref().err().or(service_error.as_ref());

        let response = match err_ref {
            None => {
                let response = response_result?;
                if is_starting {
                    let id = $extract_id(&response).ok_or_else(|| {
                        internal_error("Transaction ID was not returned by Spanner")
                    })?;
                    $self.context.transaction_selector.update(id, None)?;
                    guard.disarm();
                }
                response
            }
            Some(error) => {
                if !is_starting {
                    response_result?
                } else if is_aborted(error) {
                    response_result?
                } else {
                    $self.begin_explicitly_if_not_started(true, None).await?;

                    $request.transaction =
                        Some($self.context.transaction_selector.selector().await?);

                    let res = $self
                        .context
                        .client
                        .spanner
                        .$rpc_method($request.clone(), $gax_options, $self.context.channel_hint)
                        .await?;

                    guard.disarm();
                    res
                }
            }
        };

        response
    }};
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
}

impl ReadWriteTransaction {
    /// Buffers one or more mutations to be applied when the transaction commits.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::{Mutation, Spanner};
    /// # async fn sample(spanner: Spanner) -> Result<(), google_cloud_spanner::Error> {
    /// let db_client = spanner.database_client("projects/p/instances/i/databases/d").build().await?;
    /// let runner = db_client.read_write_transaction().build().await?;
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
        let mut guard = self
            .mutations
            .lock()
            .map_err(|_| crate::error::internal_error("mutations mutex poisoned"))?;
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
        self.context.execute_query(stmt).await
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
        let statement = statement.into();
        let mut gax_options = statement.gax_options().clone();
        self.amend_gax_options(&mut gax_options);
        let seqno = self.seqno.fetch_add(1, Ordering::SeqCst);
        let mut request = statement
            .into_request()
            .set_session(self.context.session_name.clone())
            .set_transaction(self.context.transaction_selector.selector().await?)
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
            }
        );

        self.context
            .precommit_token_tracker
            .update(response.precommit_token);

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
    /// # use google_cloud_spanner::client::{Spanner, Statement};
    /// # use google_cloud_spanner::batch_dml::BatchDml;
    /// # async fn build(spanner: Spanner) -> Result<(), google_cloud_spanner::Error> {
    /// let db_client = spanner.database_client("projects/p/instances/i/databases/d").build().await?;
    /// let runner = db_client.read_write_transaction().build().await?;
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
    /// # use google_cloud_spanner::client::{Spanner, Statement};
    /// # use google_cloud_spanner::batch_dml::BatchDml;
    /// # use google_cloud_spanner::BatchUpdateError;
    /// # async fn build(spanner: Spanner) -> Result<(), google_cloud_spanner::Error> {
    /// # let db_client = spanner.database_client("projects/p/instances/i/databases/d").build().await?;
    /// # let runner = db_client.read_write_transaction().build().await?;
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
        let mut batch = batch.into();
        self.amend_gax_options(&mut batch.gax_options);
        let seqno = self.seqno.fetch_add(1, Ordering::SeqCst);

        let statements: Vec<ExecuteBatchDmlStatement> = batch
            .statements
            .into_iter()
            .map(|stmt: crate::statement::Statement| stmt.into_batch_statement())
            .collect();

        let mut request = ExecuteBatchDmlRequest::default()
            .set_session(self.context.session_name.clone())
            .set_transaction(self.context.transaction_selector.selector().await?)
            .set_seqno(seqno)
            .set_statements(statements)
            .set_or_clear_request_options(
                self.context.amend_request_options(batch.request_options),
            );

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
            }
        );

        self.context
            .precommit_token_tracker
            .update(response.precommit_token.clone());
        crate::batch_dml::process_response(response)
    }

    pub(crate) async fn begin_explicitly_if_not_started(
        &self,
        is_stream_fallback: bool,
        mutation_key: Option<crate::model::Mutation>,
    ) -> crate::Result<bool> {
        let mut begin_options = self.begin_gax_options.clone().unwrap_or_default();
        self.amend_gax_options(&mut begin_options);
        self.context
            .begin_explicitly_if_not_started(begin_options, is_stream_fallback, mutation_key)
            .await
    }

    pub(crate) fn is_starting(&self) -> crate::Result<bool> {
        self.context.transaction_selector.is_starting()
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
        transaction_id: bytes::Bytes,
        mutations: Vec<ProtoMutation>,
        precommit_token: Option<crate::model::MultiplexedSessionPrecommitToken>,
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

    /// Commits the transaction.
    pub(crate) async fn commit(self) -> crate::Result<CommitResponse> {
        let mutations = take(&mut *self.mutations.lock().unwrap());
        let mut id = self.context.transaction_selector.get_id_no_wait()?;
        if id.is_none() {
            if self.is_starting()? {
                return Err(crate::error::internal_error(
                    "Commit called while an asynchronous statement is still starting the transaction",
                ));
            }
            let mutation_key = Mutation::select_mutation_key(&mutations);
            if self
                .begin_explicitly_if_not_started(false, mutation_key)
                .await?
            {
                id = self.context.transaction_selector.get_id_no_wait()?;
            }
        }
        let transaction_id = id.ok_or_else(|| internal_error("Transaction ID is missing"))?;
        let precommit_token = self.context.precommit_token_tracker.get();

        let request = self.build_commit_request(transaction_id.clone(), mutations, precommit_token);

        let mut gax_options = self.commit_gax_options.clone().unwrap_or_default();
        self.amend_gax_options(&mut gax_options);

        let response = self
            .context
            .client
            .spanner
            .commit(request, gax_options, self.context.channel_hint)
            .await?;

        let response =
            if let Some(new_precommit_token) = response.precommit_token().map(|b| (*b).clone()) {
                let retry_commit_req = self.build_commit_request(
                    transaction_id,
                    Vec::new(), // mutations are never re-sent in retry requests
                    Some(*new_precommit_token),
                );

                let mut gax_options = self.commit_gax_options.clone().unwrap_or_default();
                self.amend_gax_options(&mut gax_options);

                self.context
                    .client
                    .spanner
                    .commit(retry_commit_req, gax_options, self.context.channel_hint)
                    .await?
            } else {
                response
            };

        Ok(response)
    }

    /// Rolls back the transaction.
    pub(crate) async fn rollback(self) -> crate::Result<()> {
        let Some(transaction_id) = self.context.transaction_selector.get_id_no_wait()? else {
            return Ok(());
        };

        let request = RollbackRequest::default()
            .set_session(self.context.session_name.clone())
            .set_transaction_id(transaction_id);

        let mut gax_options = RequestOptions::default();
        self.amend_gax_options(&mut gax_options);

        self.context
            .client
            .spanner
            .rollback(request, gax_options, self.context.channel_hint)
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
            .unwrap_or_else(|| Arc::new(Aip194Strict));
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
        self.inner.on_error(state, error)
    }

    fn on_throttle(&self, state: &RetryState, error: GaxError) -> ThrottleResult {
        self.inner.on_throttle(state, error)
    }

    fn remaining_time(&self, state: &RetryState) -> Option<StdDuration> {
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
    use crate::BatchUpdateError;
    use crate::read_only_transaction::tests::{create_session_mock, setup_db_client};
    use crate::result_set::tests::adapt;
    use crate::transaction_retry_policy::BasicTransactionRetryPolicy;
    use gaxi::grpc::tonic;
    use gaxi::grpc::tonic::MetadataMap;
    use google_cloud_gax::options::internal::RequestOptionsExt as _;
    use google_cloud_gax::retry_policy::NeverRetry;
    use google_cloud_gax::retry_result::RetryResult;
    use google_cloud_gax::retry_state::RetryState;
    use google_cloud_test_macros::tokio_test_no_panics;
    use http::HeaderMap;
    use prost_types::Timestamp;
    use spanner_grpc_mock::google::spanner::v1;
    use std::fmt::Debug;
    use std::sync::Mutex;
    use std::time::Duration as StdDuration;

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
            .with_begin_transaction_option(begin_transaction_option)
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
            .with_begin_transaction_option(BeginTransactionOption::InlineBegin)
            .with_return_commit_stats(true)
            .with_max_commit_delay(Duration::new(0, 200_000_000).expect("valid duration"))
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
            .with_begin_transaction_option(BeginTransactionOption::InlineBegin)
            .with_commit_priority(Priority::Low)
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
    async fn read_write_transaction_execute_update_explicit() {
        run_read_write_transaction_execute_update(BeginTransactionOption::ExplicitBegin).await;
    }

    #[tokio_test_no_panics]
    async fn read_write_transaction_execute_update_inline() {
        run_read_write_transaction_execute_update(BeginTransactionOption::InlineBegin).await;
    }

    async fn run_read_write_transaction_execute_update(
        begin_transaction_option: BeginTransactionOption,
    ) {
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
            .with_begin_transaction_option(begin_transaction_option)
            .build(None)
            .await
            .expect("Failed to build transaction");
        let count = tx
            .execute_update("UPDATE Users SET Name = 'Alice' WHERE Id = 1")
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
            .with_begin_transaction_option(begin_transaction_option)
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
            .with_begin_transaction_option(begin_transaction_option)
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
            .with_begin_transaction_option(begin_transaction_option)
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
            .with_begin_transaction_option(begin_transaction_option)
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
            .with_begin_transaction_option(begin_transaction_option)
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
        use crate::client::Statement;
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
            assert_eq!(req.sql, "SELECT 1");
            // Queries do not need to include a sequence number.
            assert_eq!(req.seqno, 0);

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
            .with_begin_transaction_option(BeginTransactionOption::ExplicitBegin)
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
            .with_isolation_level(IsolationLevel::Serializable)
            .with_read_lock_mode(ReadLockMode::Pessimistic)
            .with_begin_transaction_option(BeginTransactionOption::ExplicitBegin)
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
            .with_exclude_txn_from_change_streams(true)
            .with_begin_transaction_option(BeginTransactionOption::ExplicitBegin)
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
            .with_begin_transaction_option(BeginTransactionOption::ExplicitBegin)
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
            .with_begin_transaction_option(begin_transaction_option)
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
            .with_max_commit_delay(Duration::new(0, 200_000_000).unwrap())
            .with_begin_transaction_option(begin_transaction_option)
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
    async fn read_write_transaction_execute_update_fallback() {
        let mut mock = create_session_mock();

        // 1. First DML attempt fails!
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

        // 2. Client falls back to explicit BeginTransaction!
        mock.expect_begin_transaction().once().returning(|_| {
            Ok(tonic::Response::new(v1::Transaction {
                id: vec![7, 8, 9],
                ..Default::default()
            }))
        });

        // 3. Client retries DML with new ID!
        mock.expect_execute_sql().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(req.sql, "UPDATE Users SET Name = 'Alice' WHERE Id = 1");

            let selector = req
                .transaction
                .expect("missing transaction selector")
                .selector
                .expect("missing selector");
            match selector {
                v1::transaction_selector::Selector::Id(id) => {
                    assert_eq!(id, vec![7, 8, 9]);
                }
                _ => panic!("Expected Selector::Id"),
            }

            Ok(tonic::Response::new(v1::ResultSet {
                stats: Some(v1::ResultSetStats {
                    row_count: Some(v1::result_set_stats::RowCount::RowCountExact(1)),
                    ..Default::default()
                }),
                ..Default::default()
            }))
        });

        let (db_client, _server) = setup_db_client(mock).await;

        let tx = ReadWriteTransactionBuilder::new(db_client.clone())
            .build(None)
            .await
            .expect("Failed to build transaction");

        let count = tx
            .execute_update("UPDATE Users SET Name = 'Alice' WHERE Id = 1")
            .await
            .expect("Failed to execute update after fallback");
        assert_eq!(count, 1);
    }

    #[tokio_test_no_panics]
    async fn read_write_transaction_execute_batch_update_fallback() -> anyhow::Result<()> {
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

        // 2. Client falls back to explicit BeginTransaction!
        mock.expect_begin_transaction().once().returning(|_| {
            Ok(tonic::Response::new(v1::Transaction {
                id: vec![4, 5, 6],
                ..Default::default()
            }))
        });

        // 3. Client retries Batch DML with new ID!
        mock.expect_execute_batch_dml().once().returning(|req| {
            let req = req.into_inner();
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

            Ok(tonic::Response::new(v1::ExecuteBatchDmlResponse {
                result_sets: vec![v1::ResultSet {
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

        let (db_client, _server) = setup_db_client(mock).await;

        let tx = ReadWriteTransactionBuilder::new(db_client)
            .build(None)
            .await?;

        let batch =
            BatchDml::builder().add_statement("UPDATE Users SET Name = 'Alice' WHERE Id = 1");

        let counts = tx.execute_batch_update(batch.build()).await?;

        assert_eq!(counts, vec![1]);

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
            .with_begin_transaction_option(BeginTransactionOption::ExplicitBegin)
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
            .with_begin_transaction_option(BeginTransactionOption::ExplicitBegin)
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
            .with_begin_transaction_option(BeginTransactionOption::ExplicitBegin)
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
            .with_begin_transaction_option(BeginTransactionOption::ExplicitBegin)
            .build(None)
            .await?;

        let mut custom_headers = http::HeaderMap::new();
        custom_headers.insert(
            "x-custom-user-header",
            http::HeaderValue::from_static("custom-value"),
        );

        let mut stmt = Statement::builder("UPDATE Users SET active = true").build();
        let opts = stmt.gax_options().clone().insert_extension(custom_headers);
        stmt = stmt.with_gax_options(opts);

        let count = tx.execute_update(stmt).await?;
        assert_eq!(count, 1);
        Ok(())
    }

    #[tokio_test_no_panics]
    async fn leader_aware_routing_implicit_begin_fallback() -> anyhow::Result<()> {
        let mut mock = create_session_mock();

        // 1. Initial execute_sql attempts implicit begin and transiently fails.
        // It must include the LAR header because it is a modifying operation.
        mock.expect_execute_sql().once().returning(|req| {
            assert_eq!(
                req.metadata()
                    .get("x-goog-spanner-route-to-leader")
                    .expect("header required on initial execute")
                    .to_str()
                    .unwrap(),
                "true"
            );
            Err(tonic::Status::new(tonic::Code::Internal, "internal error"))
        });

        // 2. Client fallback mechanism invokes begin_explicitly_if_not_started.
        // This should also include the LAR header.
        mock.expect_begin_transaction().once().returning(|req| {
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

        // 3. Retried execute_sql with fixed ID.
        mock.expect_execute_sql().once().returning(|req| {
            assert_eq!(
                req.metadata()
                    .get("x-goog-spanner-route-to-leader")
                    .expect("header required on retried execute")
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

        let (db_client, _server) = setup_db_client(mock).await;
        // Construct transaction using implicit begin (explicit_begin = false)
        let tx = ReadWriteTransactionBuilder::new(db_client)
            .with_begin_transaction_option(BeginTransactionOption::InlineBegin)
            .build(None)
            .await?;

        let count = tx.execute_update("UPDATE Users SET active = true").await?;
        assert_eq!(count, 1);
        Ok(())
    }

    #[tokio_test_no_panics]
    async fn read_write_transaction_fallback_forwards_transaction_tag() -> anyhow::Result<()> {
        let mut mock = create_session_mock();

        // 1. Initial execute_sql attempts inline begin and transiently fails.
        // The initial request includes the transaction tag.
        mock.expect_execute_sql().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(
                req.request_options
                    .as_ref()
                    .expect("Missing request_options on initial RPC")
                    .transaction_tag,
                "fallback-test-tag"
            );
            Err(tonic::Status::new(tonic::Code::Internal, "internal error"))
        });

        // 2. Client fallback mechanism invokes explicit begin.
        // This should include the transaction tag.
        mock.expect_begin_transaction().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(
                req.request_options
                    .as_ref()
                    .expect("Missing request_options on explicit begin fallback")
                    .transaction_tag,
                "fallback-test-tag"
            );
            Ok(tonic::Response::new(v1::Transaction {
                id: vec![7, 7, 7],
                ..Default::default()
            }))
        });

        // 3. Retried execute_sql with the explicit transaction ID.
        mock.expect_execute_sql().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(
                req.request_options
                    .as_ref()
                    .expect("Missing request_options on retried RPC")
                    .transaction_tag,
                "fallback-test-tag"
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
            .with_begin_transaction_option(BeginTransactionOption::InlineBegin)
            .with_transaction_tag("fallback-test-tag")
            .build(None)
            .await?;

        let count = tx.execute_update("UPDATE Users SET active = true").await?;
        assert_eq!(count, 1);
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
            .with_begin_transaction_option(BeginTransactionOption::InlineBegin)
            .build(None)
            .await
            .expect("Transaction build should succeed");

        let mutation = Mutation::new_insert_builder("Users")
            .set("UserId")
            .to(&1)
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
                Ok(tonic::Response::new(v1::ExecuteBatchDmlResponse {
                    result_sets: vec![],
                    status: Some(spanner_grpc_mock::google::rpc::Status {
                        code: tonic::Code::Aborted as i32,
                        message: "concurrent lock abort".into(),
                        details: vec![],
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
            .build()
            .await?;

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
                    req.transaction.unwrap().selector.unwrap(),
                    v1::transaction_selector::Selector::Begin(_)
                ));
                Err(tonic::Status::new(
                    tonic::Code::Aborted,
                    "concurrent lock abort",
                ))
            });

        // 2. Second statement (execute_sql) sees NotStarted and attempts inline begin again
        mock.expect_execute_sql()
            .times(1)
            .in_sequence(&mut sequence)
            .returning(|req| {
                let req = req.into_inner();
                assert!(matches!(
                    req.transaction.unwrap().selector.unwrap(),
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

        // 3. Commit called with the transaction ID returned in step 2
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
                    .with_max_attempts(1)
                    .with_total_timeout(std::time::Duration::from_secs(5)),
            )
            .build()
            .await?;

        runner
            .run(async |tx| {
                // 1. First statement fails with Aborted. We catch it and continue.
                let res = tx
                    .execute_update("UPDATE Users SET active = true WHERE id = 1")
                    .await;
                assert!(res.is_err(), "First statement must return error");
                assert!(is_aborted(&res.unwrap_err()), "Error must be Aborted");

                // 2. Second statement continues. Without the fix, this would block/deadlock forever.
                let count = tx
                    .execute_update("UPDATE Users SET active = true WHERE id = 2")
                    .await?;
                assert_eq!(count, 1);
                Ok(())
            })
            .await?;

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
                    req.transaction.unwrap().selector.unwrap(),
                    v1::transaction_selector::Selector::Begin(_)
                ));
                Err(tonic::Status::new(
                    tonic::Code::Aborted,
                    "concurrent lock abort",
                ))
            });

        // 2. Second statement (execute_sql) sees NotStarted and attempts inline begin again
        mock.expect_execute_sql()
            .times(1)
            .in_sequence(&mut sequence)
            .returning(|req| {
                let req = req.into_inner();
                assert!(matches!(
                    req.transaction.unwrap().selector.unwrap(),
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

        // 3. Commit called with the transaction ID returned in step 2
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
                    .with_max_attempts(1)
                    .with_total_timeout(std::time::Duration::from_secs(5)),
            )
            .build()
            .await?;

        runner
            .run(async |tx| {
                // 1. First statement (Batch DML) fails with Aborted. We catch it and continue.
                let batch = BatchDml::builder()
                    .add_statement("UPDATE Users SET active = true WHERE id = 1");
                let res = tx.execute_batch_update(batch.build()).await;
                assert!(res.is_err(), "First statement must return error");
                assert!(is_aborted(&res.unwrap_err()), "Error must be Aborted");

                // 2. Second statement continues. Without the fix, this would block/deadlock forever.
                let count = tx
                    .execute_update("UPDATE Users SET active = true WHERE id = 2")
                    .await?;
                assert_eq!(count, 1);
                Ok(())
            })
            .await?;

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
    async fn read_write_transaction_lazy_begin_fallback_never_retry() -> anyhow::Result<()> {
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

        // 2. Fallback explicit BeginTransaction is executed exactly once and fails (because we configure NeverRetry)
        mock.expect_begin_transaction()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |_req| Err(tonic::Status::unavailable("transient error")));

        let (db_client, _server) = setup_db_client(mock).await;

        let runner = db_client
            .read_write_transaction()
            .with_begin_transaction_option(BeginTransactionOption::InlineBegin)
            .with_begin_retry_policy(NeverRetry)
            .build()
            .await?;

        let res = runner
            .run(async |tx| {
                let mut stmt_opts = crate::RequestOptions::default();
                stmt_opts.set_retry_policy(NeverRetry);
                let stmt = Statement::builder("UPDATE Users SET active = true WHERE id = 1")
                    .build()
                    .with_gax_options(stmt_opts);
                let _count = tx.execute_update(stmt).await?;
                Ok(())
            })
            .await;

        assert!(
            res.is_err(),
            "Should fail immediately because NeverRetry aborted retries of explicit begin"
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
            .with_begin_transaction_option(BeginTransactionOption::ExplicitBegin)
            .with_commit_retry_policy(NeverRetry)
            .with_transaction_timeout(StdDuration::from_secs(5))
            .build()
            .await?;

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
            .with_begin_transaction_option(BeginTransactionOption::ExplicitBegin)
            .with_commit_attempt_timeout(StdDuration::from_secs(2))
            .with_transaction_timeout(StdDuration::from_secs(10))
            .build()
            .await?;

        let res = runner.run(async |_tx| Ok(())).await?;

        assert!(res.commit_response.commit_timestamp.is_some());
        Ok(())
    }

    #[tokio_test_no_panics]
    async fn read_write_transaction_fallback_begin_under_deadline() -> anyhow::Result<()> {
        let mut mock = create_session_mock();
        let mut sequence = mockall::Sequence::new();

        // 1. First statement execution fails with Unavailable (transient error)
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

        // 2. Fallback explicit BeginTransaction is executed and sets attempt timeout based on remaining transaction deadline (approx 5 seconds).
        mock.expect_begin_transaction()
            .once()
            .in_sequence(&mut sequence)
            .withf(|req| {
                let duration =
                    parse_grpc_timeout(req.metadata()).expect("valid grpc-timeout header");
                assert!(
                    duration >= StdDuration::from_millis(4000)
                        && duration <= StdDuration::from_millis(6000),
                    "Fallback begin timeout is wrong: {:?}",
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

        // 3. Statement retry succeeds
        mock.expect_execute_sql()
            .once()
            .in_sequence(&mut sequence)
            .withf(|req| {
                matches!(
                    req.get_ref()
                        .transaction
                        .as_ref()
                        .and_then(|t| t.selector.as_ref()),
                    Some(v1::transaction_selector::Selector::Id(_))
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
            .with_begin_transaction_option(BeginTransactionOption::InlineBegin)
            .with_transaction_timeout(StdDuration::from_secs(5))
            .build()
            .await?;

        let res = runner
            .run(async |tx| {
                let mut query_opts = crate::RequestOptions::default();
                query_opts.set_retry_policy(NeverRetry);
                let stmt = Statement::builder("UPDATE Users SET active = true WHERE id = 1")
                    .build()
                    .with_gax_options(query_opts);
                let count = tx.execute_update(stmt).await?;
                assert_eq!(count, 1);
                Ok(())
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
            .with_begin_transaction_option(BeginTransactionOption::InlineBegin)
            .with_transaction_timeout(StdDuration::from_secs(5))
            .build()
            .await?;

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
}
