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

use crate::channel_pool::TransactionAffinity;
use crate::client::amend_request_options_for_lar;
use crate::database_client::DatabaseClient;
use crate::error::SpannerInternalError;
use crate::google::spanner::v1::PartialResultSet;
use crate::google::spanner::v1::result_set_stats::RowCount::RowCountLowerBound;
use crate::model::transaction_options::PartitionedDml;
use crate::model::{
    BeginTransactionRequest, ExecuteSqlRequest, TransactionOptions, TransactionSelector,
    transaction_selector,
};
use crate::request_id_interceptor::update_request_id_attempt;
use crate::retry_policy::SpannerRetryPolicy;
use crate::server_streaming::stream::PartialResultSetStream;
use crate::statement::Statement;
use crate::transaction_retry_policy::{
    BasicTransactionRetryPolicy, TransactionRetryPolicy, default_retry_backoff,
    extract_retry_delay, is_aborted, is_internal_emulator_error,
};
use bytes::Bytes;
use gaxi::prost::FromProto;
use google_cloud_gax::backoff_policy::BackoffPolicy;
use google_cloud_gax::error::rpc::Code;
use google_cloud_gax::exponential_backoff::{ExponentialBackoff, ExponentialBackoffBuilder};
use google_cloud_gax::options::RequestOptions as GaxRequestOptions;
use google_cloud_gax::retry_result::RetryResult;
use google_cloud_gax::retry_state::RetryState;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::sleep;

/// A builder for [PartitionedDmlTransaction].
///
/// # Example
/// ```
/// # use google_cloud_spanner::client::Spanner;
/// # use google_cloud_spanner::statement::Statement;
/// # async fn build_transaction(spanner: Spanner) -> Result<(), google_cloud_spanner::Error> {
///     let db_client = spanner.database_client("projects/p/instances/i/databases/d").build().await?;
///     let transaction = db_client.partitioned_dml_transaction().build().await?;
///     let statement = Statement::builder("UPDATE users SET active = true WHERE TRUE").build();
///     let modified_rows = transaction.execute_update(statement).await?;
/// #   Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct PartitionedDmlTransactionBuilder {
    client: DatabaseClient,
    retry_policy: Box<dyn TransactionRetryPolicy>,
    exclude_txn_from_change_streams: bool,
}

impl PartitionedDmlTransactionBuilder {
    pub(crate) fn new(client: DatabaseClient) -> Self {
        Self {
            client,
            retry_policy: Box::new(BasicTransactionRetryPolicy::default()),
            exclude_txn_from_change_streams: false,
        }
    }

    /// Sets whether to exclude the transaction from change streams.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::Spanner;
    /// # async fn build_transaction(spanner: Spanner) -> Result<(), google_cloud_spanner::Error> {
    ///     let db_client = spanner.database_client("projects/p/instances/i/databases/d").build().await?;
    ///     let transaction = db_client
    ///         .partitioned_dml_transaction()
    ///         .with_exclude_txn_from_change_streams(true)
    ///         .build()
    ///         .await?;
    /// #   Ok(())
    /// # }
    /// ```
    ///
    /// When set to `true`, it prevents modifications from this transaction from being tracked in change streams.
    /// Note that this only affects change streams that have been created with the DDL option `allow_txn_exclusion = true`.
    /// If `allow_txn_exclusion` is not set or set to `false` for a change stream, updates made within this transaction
    /// are recorded in that change stream regardless of this setting.
    ///
    /// When set to `false` or not specified, modifications from this transaction are recorded in all change streams
    /// tracking columns modified by this transaction.
    pub fn with_exclude_txn_from_change_streams(mut self, exclude: bool) -> Self {
        self.exclude_txn_from_change_streams = exclude;
        self
    }

    /// Sets the transaction retry policy for handling transaction aborts (`Code::Aborted`)
    /// and transient startup errors (`Code::Unavailable`) occurring prior to receiving a resume token.
    ///
    /// # Example
    /// ```
    /// # use std::time::Duration;
    /// # use google_cloud_spanner::client::Spanner;
    /// # use google_cloud_spanner::transaction::BasicTransactionRetryPolicy;
    /// # async fn build_transaction(spanner: Spanner) -> Result<(), google_cloud_spanner::Error> {
    ///     let db_client = spanner.database_client("projects/p/instances/i/databases/d").build().await?;
    ///     
    ///     let retry_policy = BasicTransactionRetryPolicy::new()
    ///         .with_max_attempts(5)
    ///         .with_total_timeout(Duration::from_secs(60));
    ///
    ///     let transaction = db_client
    ///         .partitioned_dml_transaction()
    ///         .with_retry_policy(retry_policy)
    ///         .build()
    ///         .await?;
    /// #   Ok(())
    /// # }
    /// ```
    ///
    /// The client will retry the entire transaction if it is aborted by Spanner, or if a transient
    /// network error (`Code::Unavailable`) occurs before any resume token is received.
    /// This policy can be used to customize whether a transaction should be retried
    /// or not. The default is to retry indefinitely until the transaction succeeds.
    ///
    /// Once the stream has produced at least one resume token, subsequent transient stream
    /// errors (`Code::Unavailable`) are handled separately at the RPC level by resuming the
    /// stream; stream-level retry and backoff policies can be configured on the [Statement]
    /// passed to [PartitionedDmlTransaction::execute_update].
    pub fn with_retry_policy<P: TransactionRetryPolicy + 'static>(mut self, policy: P) -> Self {
        self.retry_policy = Box::new(policy);
        self
    }

    /// Builds the [PartitionedDmlTransaction].
    pub async fn build(self) -> crate::Result<PartitionedDmlTransaction> {
        Ok(PartitionedDmlTransaction {
            client: self.client,
            retry_policy: self.retry_policy,
            exclude_txn_from_change_streams: self.exclude_txn_from_change_streams,
        })
    }
}

/// A Partitioned DML transaction.
///
/// Partitioned DML transactions are used to execute a single DML statement that may modify a large
/// number of rows. The execution of the statement will automatically be partitioned into smaller
/// transactions by Spanner, which may execute in parallel.
///
/// A Partitioned DML transaction cannot be committed or rolled back.
///
/// See also: <https://docs.cloud.google.com/spanner/docs/dml-partitioned>
#[derive(Debug)]
pub struct PartitionedDmlTransaction {
    client: DatabaseClient,
    retry_policy: Box<dyn TransactionRetryPolicy>,
    exclude_txn_from_change_streams: bool,
}

impl PartitionedDmlTransaction {
    /// Executes a Partitioned DML statement.
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
    /// # Return
    ///
    /// The number of rows that was at least modified by the statement. Note that the actual number
    /// of rows that was modified may be higher than this number if the statement was retried or
    /// split into multiple transactions by Spanner, and some of these (sub)transactions were
    /// executed multiple times.
    ///
    /// See also: <https://docs.cloud.google.com/spanner/docs/dml-partitioned>
    pub async fn execute_update<T: Into<Statement>>(self, statement: T) -> crate::Result<i64> {
        let statement = statement.into();
        let mut gax_options = statement.gax_options().clone();
        self.amend_gax_options(&mut gax_options);

        let session_name = self.client.session_name();
        let transaction_options = TransactionOptions::default()
            .set_partitioned_dml(PartitionedDml::default())
            .set_exclude_txn_from_change_streams(self.exclude_txn_from_change_streams);
        let begin_request = BeginTransactionRequest {
            session: session_name.clone(),
            options: Some(transaction_options),
            ..Default::default()
        };
        let base_request = statement.into_request();
        let retry_policy = self.retry_policy;
        let client = self.client;
        let is_emulator = client.is_emulator();

        let start_time = Instant::now();
        let mut attempts: u32 = 0;
        let backoff = default_retry_backoff();

        loop {
            attempts += 1;
            let affinity = TransactionAffinity::new_read_write();
            let transaction = match client
                .begin_transaction(begin_request.clone(), gax_options.clone(), &affinity)
                .await
            {
                Ok(transaction) => transaction,
                Err(error) => {
                    affinity.release_rw_guard();
                    if !should_retry_transaction(&error, false, is_emulator) {
                        return Err(error);
                    }
                    backoff_transaction_retry(
                        error,
                        &*retry_policy,
                        attempts,
                        start_time,
                        &backoff,
                    )
                    .await?;
                    continue;
                }
            };

            let execute_request = base_request
                .clone()
                .set_session(session_name.clone())
                .set_transaction(TransactionSelector {
                    selector: Some(transaction_selector::Selector::Id(transaction.id.clone())),
                    ..Default::default()
                });

            let mut runner = PartitionedDmlStreamRunner::new(
                client.clone(),
                execute_request,
                gax_options.clone(),
                affinity,
            );
            let result = runner.run().await;
            let has_progress = runner.has_received_resume_token_or_stats();
            runner.release_rw_guard();

            match result {
                Ok(rows) => return Ok(rows),
                Err(error) => {
                    if !should_retry_transaction(&error, has_progress, is_emulator) {
                        return Err(error);
                    }
                    backoff_transaction_retry(
                        error,
                        &*retry_policy,
                        attempts,
                        start_time,
                        &backoff,
                    )
                    .await?;
                }
            }
        }
    }

    fn amend_gax_options(&self, options: &mut GaxRequestOptions) {
        *options = amend_request_options_for_lar(
            self.client.leader_aware_routing_enabled,
            options.clone(),
        );
    }
}

/// Applies default RPC retry and backoff policies for resuming the `ExecuteStreamingSql`
/// stream on transient errors (e.g. `Unavailable`).
///
/// Under default settings, transient stream errors (`Code::Unavailable`) are retried indefinitely
/// with exponential backoff until success. Unlike single-use or interactive queries, Partitioned DML
/// operations represent large-scale, long-running batch updates where capping stream retries could
/// prematurely terminate a multi-hour operation. Callers can customize this behavior by setting
/// an attempt limit or custom retry policy on the [Statement] passed to
/// [PartitionedDmlTransaction::execute_update].
///
/// This does not control transaction-level abort retries (`Code::Aborted`), which are
/// managed by [`TransactionRetryPolicy`].
fn apply_stream_rpc_defaults(mut gax_options: GaxRequestOptions) -> GaxRequestOptions {
    if gax_options.retry_policy().is_none() {
        gax_options.set_retry_policy(SpannerRetryPolicy::new());
    }
    if gax_options.backoff_policy().is_none() {
        gax_options.set_backoff_policy(default_stream_backoff_policy());
    }
    gax_options
}

/// Default exponential backoff policy for retrying transient stream RPC errors (`Unavailable`).
fn default_stream_backoff_policy() -> Arc<dyn BackoffPolicy> {
    Arc::new(ExponentialBackoffBuilder::default().clamp())
}

struct PartitionedDmlStreamRunner {
    client: DatabaseClient,
    execute_request: ExecuteSqlRequest,
    gax_options: GaxRequestOptions,
    affinity: TransactionAffinity,
    last_resume_token: Bytes,
    total_lower_bound: Option<i64>,
    retry_count: usize,
}

impl PartitionedDmlStreamRunner {
    fn new(
        client: DatabaseClient,
        execute_request: ExecuteSqlRequest,
        gax_options: GaxRequestOptions,
        affinity: TransactionAffinity,
    ) -> Self {
        Self {
            client,
            execute_request,
            gax_options: apply_stream_rpc_defaults(gax_options),
            affinity,
            last_resume_token: Bytes::new(),
            total_lower_bound: None,
            retry_count: 0,
        }
    }

    fn release_rw_guard(&self) {
        self.affinity.release_rw_guard();
    }

    async fn run(&mut self) -> crate::Result<i64> {
        let mut stream = self.start_stream().await?;
        while let Some(message_result) = stream.next_message().await {
            match message_result {
                Ok(partial_result_set) => self.process_partial_result_set(partial_result_set),
                Err(stream_error) => {
                    // Only transient stream errors with a valid resume token are retried here;
                    // permanent errors or transaction aborts are propagated immediately.
                    stream = self.maybe_resume_stream(stream_error).await?;
                }
            }
        }
        self.finish()
    }

    async fn start_stream(&mut self) -> crate::Result<PartialResultSetStream> {
        let builder = self.client.execute_streaming_sql(
            self.execute_request.clone(),
            self.gax_options.clone(),
            &self.affinity,
        );
        self.gax_options = builder.options().clone();
        builder.send().await
    }

    fn process_partial_result_set(&mut self, mut partial_result_set: PartialResultSet) {
        if !partial_result_set.resume_token.is_empty() {
            self.last_resume_token = partial_result_set.resume_token;
        }
        let cache_update = partial_result_set
            .cache_update
            .take()
            .and_then(|update| update.cnv().ok());
        self.client.observe_cache_update(cache_update);
        if let Some(RowCountLowerBound(row_count)) =
            partial_result_set.stats.and_then(|stats| stats.row_count)
        {
            self.total_lower_bound = Some(self.total_lower_bound.unwrap_or(0) + row_count);
        }
    }

    /// Attempts to resume the stream if the error is transient and a resume token was previously
    /// received. Permanent errors, missing resume tokens, or abort-like errors are returned immediately.
    async fn maybe_resume_stream(
        &mut self,
        mut current_error: crate::Error,
    ) -> crate::Result<PartialResultSetStream> {
        if self.is_abort_like(&current_error) || self.last_resume_token.is_empty() {
            return Err(current_error);
        }

        loop {
            self.prepare_next_retry(current_error).await?;
            match self.reconnect_stream().await {
                Ok(stream) => return Ok(stream),
                Err(reconnect_error) if self.is_abort_like(&reconnect_error) => {
                    return Err(reconnect_error);
                }
                Err(reconnect_error) => current_error = reconnect_error,
            }
        }
    }

    async fn prepare_next_retry(&mut self, error: crate::Error) -> crate::Result<()> {
        let attempt_count = 1 + self.retry_count as u32;
        // Resuming with a valid resume token is idempotent; AIP-194 / Spanner retry policies
        // require idempotency = true to allow retrying transient errors like Unavailable.
        let state = RetryState::default()
            .set_idempotent(true)
            .set_attempt_count(attempt_count);
        let retry_policy = self
            .gax_options
            .retry_policy()
            .as_ref()
            .expect("retry policy is initialized by apply_stream_rpc_defaults");

        match retry_policy.on_error(&state, error) {
            RetryResult::Continue(retryable_error) => {
                self.retry_count += 1;
                update_request_id_attempt(&mut self.gax_options, (self.retry_count + 1) as u32);
                let delay = extract_retry_delay(&retryable_error)
                    .or_else(|| {
                        self.gax_options
                            .backoff_policy()
                            .as_ref()
                            .map(|policy| policy.on_failure(&state))
                    })
                    .unwrap_or(Duration::ZERO);
                if !delay.is_zero() {
                    sleep(delay).await;
                }
                Ok(())
            }
            RetryResult::Permanent(terminal_error) | RetryResult::Exhausted(terminal_error) => {
                Err(terminal_error)
            }
        }
    }

    async fn reconnect_stream(&self) -> crate::Result<PartialResultSetStream> {
        let mut resume_request = self.execute_request.clone();
        resume_request.resume_token = self.last_resume_token.clone();

        self.client
            .execute_streaming_sql(resume_request, self.gax_options.clone(), &self.affinity)
            .send()
            .await
    }

    fn is_abort_like(&self, error: &crate::Error) -> bool {
        is_aborted(error) || (self.client.is_emulator() && is_internal_emulator_error(error))
    }

    fn finish(&self) -> crate::Result<i64> {
        self.total_lower_bound.ok_or_else(|| {
            crate::Error::deser(SpannerInternalError::new(
                "ExecuteStreamingSql completed successfully but no row_count_lower_bound was returned",
            ))
        })
    }

    fn has_received_resume_token_or_stats(&self) -> bool {
        !self.last_resume_token.is_empty() || self.total_lower_bound.is_some()
    }
}

fn is_unavailable_or_io(error: &crate::Error) -> bool {
    error
        .status()
        .is_some_and(|status| status.code == Code::Unavailable)
        || error.is_transport()
        || error.is_io()
}

fn should_retry_transaction(error: &crate::Error, has_progress: bool, is_emulator: bool) -> bool {
    is_aborted(error)
        || (is_emulator && is_internal_emulator_error(error))
        || (!has_progress && is_unavailable_or_io(error))
}

async fn backoff_transaction_retry(
    error: crate::Error,
    policy: &dyn TransactionRetryPolicy,
    attempts: u32,
    start_time: Instant,
    backoff: &ExponentialBackoff,
) -> crate::Result<()> {
    let error = match policy.on_abort(error, attempts, start_time.elapsed()) {
        RetryResult::Continue(error) => error,
        RetryResult::Exhausted(error) | RetryResult::Permanent(error) => {
            return Err(error);
        }
    };
    let sleep_duration = extract_retry_delay(&error)
        .unwrap_or_else(|| backoff.on_failure(&RetryState::new(true).set_attempt_count(attempts)));
    if !sleep_duration.is_zero() {
        sleep(sleep_duration).await;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::read_only_transaction::tests::{create_session_mock, setup_db_client};
    use crate::result_set::tests::adapt;
    use crate::transaction_retry_policy::tests::create_aborted_status;
    use gaxi::grpc::tonic;
    use google_cloud_test_macros::tokio_test_no_panics;
    use spanner_grpc_mock::google::spanner::v1;
    use std::fmt::Debug;
    use std::sync::Mutex;
    use std::time::Instant;
    use tokio::sync::mpsc;

    #[test]
    fn auto_traits() {
        static_assertions::assert_impl_all!(PartitionedDmlTransactionBuilder: Debug, Send, Sync);
        static_assertions::assert_impl_all!(PartitionedDmlTransaction: Debug, Send, Sync);
    }

    #[tokio_test_no_panics]
    async fn execute_update_success() {
        let mut mock = create_session_mock();

        mock.expect_begin_transaction().once().returning(|request| {
            let request = request.into_inner();
            assert_eq!(
                request.session, "projects/p/instances/i/databases/d/sessions/123",
                "unexpected session name"
            );
            Ok(tonic::Response::new(v1::Transaction {
                id: vec![0, 1, 2],
                ..Default::default()
            }))
        });

        mock.expect_execute_streaming_sql()
            .once()
            .returning(|request| {
                let request = request.into_inner();
                assert_eq!(
                    request.sql, "UPDATE Users SET active = true",
                    "sql statement must match"
                );

                let stream = adapt([Ok(v1::PartialResultSet {
                    stats: Some(v1::ResultSetStats {
                        row_count: Some(v1::result_set_stats::RowCount::RowCountLowerBound(500)),
                        ..Default::default()
                    }),
                    ..Default::default()
                })]);
                Ok(tonic::Response::from(stream))
            });

        let (db_client, _server) = setup_db_client(mock).await;
        let transaction = db_client
            .partitioned_dml_transaction()
            .build()
            .await
            .expect("build transaction should succeed");
        let statement = Statement::builder("UPDATE Users SET active = true").build();
        let row_count: i64 = transaction
            .execute_update(statement)
            .await
            .expect("execute_update should succeed");
        assert_eq!(row_count, 500, "modified row count must match lower bound");
    }

    #[tokio_test_no_panics]
    async fn execute_update_with_exclude_txn_from_change_streams() {
        let mut mock = create_session_mock();

        mock.expect_begin_transaction().once().returning(|request| {
            let request = request.into_inner();
            let options = request.options.expect("missing transaction options");
            assert!(
                options.exclude_txn_from_change_streams,
                "exclude_txn_from_change_streams must be true"
            );

            Ok(tonic::Response::new(v1::Transaction {
                id: vec![0, 1, 2],
                ..Default::default()
            }))
        });

        mock.expect_execute_streaming_sql()
            .once()
            .returning(|_request| {
                let stream = adapt([Ok(v1::PartialResultSet {
                    stats: Some(v1::ResultSetStats {
                        row_count: Some(v1::result_set_stats::RowCount::RowCountLowerBound(500)),
                        ..Default::default()
                    }),
                    ..Default::default()
                })]);
                Ok(tonic::Response::from(stream))
            });

        let (db_client, _server) = setup_db_client(mock).await;
        let transaction = db_client
            .partitioned_dml_transaction()
            .with_exclude_txn_from_change_streams(true)
            .build()
            .await
            .expect("build transaction should succeed");
        let statement = Statement::builder("UPDATE Users SET active = true").build();
        let row_count: i64 = transaction
            .execute_update(statement)
            .await
            .expect("execute_update should succeed");
        assert_eq!(row_count, 500, "modified row count must match lower bound");
    }

    #[tokio_test_no_panics]
    async fn execute_update_with_aborted_retry() {
        let mut mock = create_session_mock();

        mock.expect_begin_transaction()
            .times(2)
            .returning(|_request| {
                Ok(tonic::Response::new(v1::Transaction {
                    id: vec![0, 1, 2],
                    ..Default::default()
                }))
            });

        let mut sequence = mockall::Sequence::new();
        mock.expect_execute_streaming_sql()
            .times(1)
            .in_sequence(&mut sequence)
            .returning(move |_request| {
                // Return an error stream on first try
                let stream = adapt([Err(create_aborted_status(Duration::from_nanos(1)))]);
                Ok(tonic::Response::from(stream))
            });
        mock.expect_execute_streaming_sql()
            .times(1)
            .in_sequence(&mut sequence)
            .returning(move |_request| {
                let stream = adapt([Ok(v1::PartialResultSet {
                    stats: Some(v1::ResultSetStats {
                        row_count: Some(v1::result_set_stats::RowCount::RowCountLowerBound(100)),
                        ..Default::default()
                    }),
                    ..Default::default()
                })]);
                Ok(tonic::Response::from(stream))
            });

        let (db_client, _server) = setup_db_client(mock).await;
        let transaction = db_client
            .partitioned_dml_transaction()
            .build()
            .await
            .expect("build transaction should succeed");
        let row_count: i64 = transaction
            .execute_update(Statement::builder("UPDATE Users SET active = true").build())
            .await
            .expect("execute_update should succeed");
        assert_eq!(
            row_count, 100,
            "modified row count must match lower bound after aborted retry"
        );
    }

    #[tokio_test_no_panics]
    async fn builder_with_retry_settings() {
        let mock = create_session_mock();
        let (db_client, _server) = setup_db_client(mock).await;

        let policy = BasicTransactionRetryPolicy::new()
            .with_max_attempts(10)
            .with_total_timeout(Duration::from_secs(42));

        let _transaction = db_client
            .partitioned_dml_transaction()
            .with_retry_policy(policy)
            .build()
            .await
            .expect("build transaction should succeed");
    }

    #[tokio_test_no_panics]
    async fn execute_update_missing_lower_bound() {
        let mut mock = create_session_mock();

        mock.expect_begin_transaction()
            .once()
            .returning(|_request| {
                Ok(tonic::Response::new(v1::Transaction {
                    id: vec![0, 1, 2],
                    ..Default::default()
                }))
            });

        mock.expect_execute_streaming_sql()
            .once()
            .returning(|_request| {
                let stream = adapt([Ok(v1::PartialResultSet {
                    stats: Some(v1::ResultSetStats {
                        // Provide a RowCountExact instead of RowCountLowerBound
                        row_count: Some(v1::result_set_stats::RowCount::RowCountExact(100)),
                        ..Default::default()
                    }),
                    ..Default::default()
                })]);
                Ok(tonic::Response::from(stream))
            });

        let (db_client, _server) = setup_db_client(mock).await;
        let transaction = db_client
            .partitioned_dml_transaction()
            .build()
            .await
            .expect("build transaction should succeed");

        let statement = Statement::builder("UPDATE Users SET active = true").build();
        let result = transaction.execute_update(statement).await;

        assert!(
            result.is_err(),
            "execute_update must fail when row_count_lower_bound is missing"
        );
        let error = result.expect_err("result must be error");
        assert!(
            error.is_deserialization(),
            "error must be deserialization error"
        );
        assert!(
            error
                .to_string()
                .contains("no row_count_lower_bound was returned"),
            "error message must describe missing row_count_lower_bound"
        );
    }

    #[tokio_test_no_panics]
    async fn leader_aware_routing_enabled_by_default() {
        let mut mock = create_session_mock();
        mock.expect_begin_transaction().once().returning(|request| {
            assert_eq!(
                request
                    .metadata()
                    .get("x-goog-spanner-route-to-leader")
                    .expect("header required")
                    .to_str()
                    .expect("route-to-leader header must be valid ASCII string"),
                "true",
                "route-to-leader header on begin_transaction must be true"
            );
            Ok(tonic::Response::new(v1::Transaction {
                id: vec![0, 1, 2],
                ..Default::default()
            }))
        });

        mock.expect_execute_streaming_sql()
            .once()
            .returning(|request| {
                assert_eq!(
                    request
                        .metadata()
                        .get("x-goog-spanner-route-to-leader")
                        .expect("header required")
                        .to_str()
                        .expect("route-to-leader header must be valid ASCII string"),
                    "true",
                    "route-to-leader header on execute_streaming_sql must be true"
                );
                let stream = adapt([Ok(v1::PartialResultSet {
                    stats: Some(v1::ResultSetStats {
                        row_count: Some(v1::result_set_stats::RowCount::RowCountLowerBound(500)),
                        ..Default::default()
                    }),
                    ..Default::default()
                })]);
                Ok(tonic::Response::from(stream))
            });

        let (db_client, _server) = setup_db_client(mock).await;
        let transaction = db_client
            .partitioned_dml_transaction()
            .build()
            .await
            .expect("build transaction should succeed");
        let statement = Statement::builder("UPDATE Users SET active = true").build();
        let row_count: i64 = transaction
            .execute_update(statement)
            .await
            .expect("execute_update should succeed");
        assert_eq!(row_count, 500, "modified row count must match lower bound");
    }

    #[tokio_test_no_panics]
    async fn partitioned_dml_transaction_observes_cache_update() -> anyhow::Result<()> {
        use crate::client::{Spanner, SpannerBuilderExt};
        use crate::omni::InstanceType;
        use crate::routing::key_range_cache::RangeMode;
        use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
        use spanner_grpc_mock::MockSpanner;
        use spanner_grpc_mock::google::spanner::v1::{CacheUpdate, Group, Range, Tablet};
        use spanner_grpc_mock::start;

        let mut mock = MockSpanner::new();
        mock.expect_fetch_cache_update().returning(|_| {
            let (_sender, receiver) = mpsc::channel(1);
            Ok(tonic::Response::from(receiver))
        });
        mock.expect_create_session().returning(|_| {
            Ok(tonic::Response::new(v1::Session {
                name: "projects/p/instances/i/databases/d/sessions/123".to_string(),
                multiplexed: true,
                ..Default::default()
            }))
        });
        mock.expect_begin_transaction().returning(|_| {
            let cache_update = CacheUpdate {
                database_id: 111,
                group: vec![Group {
                    group_uid: 222,
                    leader_index: 0,
                    tablets: vec![Tablet {
                        server_address: "node-222.spanner.internal:15000".to_string(),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                range: vec![Range {
                    group_uid: 222,
                    start_key: b"pd1".to_vec(),
                    limit_key: b"pd9".to_vec(),
                    ..Default::default()
                }],
                ..Default::default()
            };
            Ok(tonic::Response::new(v1::Transaction {
                id: vec![0, 1, 2],
                cache_update: Some(cache_update),
                ..Default::default()
            }))
        });
        mock.expect_execute_streaming_sql().returning(|_| {
            let stream = adapt([Ok(v1::PartialResultSet {
                stats: Some(v1::ResultSetStats {
                    row_count: Some(v1::result_set_stats::RowCount::RowCountLowerBound(500)),
                    ..Default::default()
                }),
                ..Default::default()
            })]);
            Ok(tonic::Response::from(stream))
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
        let transaction = db_client.partitioned_dml_transaction().build().await?;
        let statement = Statement::builder("UPDATE Users SET active = true").build();
        let rows = transaction.execute_update(statement).await?;
        assert_eq!(rows, 500, "modified row count must match lower bound");

        assert_eq!(
            db_client.database_id(),
            Some(111),
            "database id must be updated from cache update"
        );
        let router = db_client
            .location_router()
            .expect("location router present");
        let found = router
            .key_range_cache()
            .find_range(b"pd5", &[], RangeMode::CoveringSplit);
        assert!(found.is_some(), "range covering 'pd5' should be cached");
        assert_eq!(
            found.expect("range present").group_uid,
            222,
            "group uid must match cached update"
        );

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn execute_update_resumes_stream_on_unavailable() {
        let mut mock = create_session_mock();

        mock.expect_begin_transaction().once().returning(|request| {
            let request = request.into_inner();
            assert_eq!(
                request.session, "projects/p/instances/i/databases/d/sessions/123",
                "unexpected session name"
            );
            Ok(tonic::Response::new(v1::Transaction {
                id: vec![0, 1, 2],
                ..Default::default()
            }))
        });

        let mut sequence = mockall::Sequence::new();
        mock.expect_execute_streaming_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(|request| {
                let request = request.into_inner();
                assert_eq!(
                    request.sql, "UPDATE Users SET active = true",
                    "unexpected sql query"
                );
                assert!(
                    request.resume_token.is_empty(),
                    "first request must not contain resume token"
                );

                let stream = adapt([
                    Ok(v1::PartialResultSet {
                        resume_token: b"token-1".to_vec(),
                        ..Default::default()
                    }),
                    Err(tonic::Status::unavailable("connection reset")),
                ]);
                Ok(tonic::Response::from(stream))
            });

        mock.expect_execute_streaming_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(|request| {
                let request = request.into_inner();
                assert_eq!(
                    request.resume_token,
                    b"token-1".to_vec(),
                    "resumed request must contain previous resume token"
                );

                let stream = adapt([Ok(v1::PartialResultSet {
                    stats: Some(v1::ResultSetStats {
                        row_count: Some(v1::result_set_stats::RowCount::RowCountLowerBound(750)),
                        ..Default::default()
                    }),
                    ..Default::default()
                })]);
                Ok(tonic::Response::from(stream))
            });

        let (db_client, _server) = setup_db_client(mock).await;
        let transaction = db_client
            .partitioned_dml_transaction()
            .build()
            .await
            .expect("build transaction should succeed");
        let statement = Statement::builder("UPDATE Users SET active = true")
            .with_backoff_policy(
                ExponentialBackoffBuilder::new()
                    .with_initial_delay(Duration::from_millis(1))
                    .clamp(),
            )
            .build();
        let rows = transaction
            .execute_update(statement)
            .await
            .expect("execute_update should resume and succeed");
        assert_eq!(rows, 750, "expected total lower bound of 750");
    }

    #[tokio_test_no_panics]
    async fn execute_update_accumulates_lower_bounds_across_resumed_stream() {
        let mut mock = create_session_mock();

        mock.expect_begin_transaction().once().returning(|request| {
            let request = request.into_inner();
            assert_eq!(
                request.session, "projects/p/instances/i/databases/d/sessions/123",
                "unexpected session name"
            );
            Ok(tonic::Response::new(v1::Transaction {
                id: vec![0, 1, 2],
                ..Default::default()
            }))
        });

        let mut sequence = mockall::Sequence::new();
        mock.expect_execute_streaming_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(|request| {
                let request = request.into_inner();
                assert!(
                    request.resume_token.is_empty(),
                    "initial request must not have a resume token"
                );

                let stream = adapt([
                    Ok(v1::PartialResultSet {
                        resume_token: b"token-1".to_vec(),
                        stats: Some(v1::ResultSetStats {
                            row_count: Some(v1::result_set_stats::RowCount::RowCountLowerBound(
                                300,
                            )),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    Err(tonic::Status::unavailable("temporary drop")),
                ]);
                Ok(tonic::Response::from(stream))
            });

        mock.expect_execute_streaming_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(|request| {
                let request = request.into_inner();
                assert_eq!(
                    request.resume_token,
                    b"token-1".to_vec(),
                    "resumed request must contain token-1"
                );

                let stream = adapt([Ok(v1::PartialResultSet {
                    resume_token: b"token-2".to_vec(),
                    stats: Some(v1::ResultSetStats {
                        row_count: Some(v1::result_set_stats::RowCount::RowCountLowerBound(200)),
                        ..Default::default()
                    }),
                    ..Default::default()
                })]);
                Ok(tonic::Response::from(stream))
            });

        let (db_client, _server) = setup_db_client(mock).await;
        let transaction = db_client
            .partitioned_dml_transaction()
            .build()
            .await
            .expect("build transaction should succeed");
        let statement = Statement::builder("UPDATE Users SET active = true")
            .with_backoff_policy(
                ExponentialBackoffBuilder::new()
                    .with_initial_delay(Duration::from_millis(1))
                    .clamp(),
            )
            .build();
        let rows = transaction
            .execute_update(statement)
            .await
            .expect("execute_update should succeed");
        assert_eq!(
            rows, 500,
            "expected accumulated lower bound of 300 + 200 = 500"
        );
    }

    #[tokio_test_no_panics]
    async fn execute_update_aborted_during_resumable_stream_restarts_transaction() {
        let mut mock = create_session_mock();

        let mut begin_sequence = mockall::Sequence::new();
        mock.expect_begin_transaction()
            .once()
            .in_sequence(&mut begin_sequence)
            .returning(|_request| {
                Ok(tonic::Response::new(v1::Transaction {
                    id: vec![1, 1, 1],
                    ..Default::default()
                }))
            });
        mock.expect_begin_transaction()
            .once()
            .in_sequence(&mut begin_sequence)
            .returning(|_request| {
                Ok(tonic::Response::new(v1::Transaction {
                    id: vec![2, 2, 2],
                    ..Default::default()
                }))
            });

        let mut stream_sequence = mockall::Sequence::new();
        mock.expect_execute_streaming_sql()
            .once()
            .in_sequence(&mut stream_sequence)
            .returning(|request| {
                let request = request.into_inner();
                let transaction_id = request
                    .transaction
                    .and_then(|selector| match selector.selector {
                        Some(v1::transaction_selector::Selector::Id(id)) => Some(id),
                        _ => None,
                    })
                    .expect("transaction selector id present");
                assert_eq!(
                    transaction_id,
                    vec![1, 1, 1],
                    "expected first transaction ID"
                );

                let stream = adapt([
                    Ok(v1::PartialResultSet {
                        resume_token: b"token-1".to_vec(),
                        stats: Some(v1::ResultSetStats {
                            row_count: Some(v1::result_set_stats::RowCount::RowCountLowerBound(
                                300,
                            )),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    Err(create_aborted_status(Duration::from_nanos(1))),
                ]);
                Ok(tonic::Response::from(stream))
            });

        mock.expect_execute_streaming_sql()
            .once()
            .in_sequence(&mut stream_sequence)
            .returning(|request| {
                let request = request.into_inner();
                let transaction_id = request
                    .transaction
                    .and_then(|selector| match selector.selector {
                        Some(v1::transaction_selector::Selector::Id(id)) => Some(id),
                        _ => None,
                    })
                    .expect("transaction selector id present");
                assert_eq!(
                    transaction_id,
                    vec![2, 2, 2],
                    "expected second transaction ID"
                );
                assert!(
                    request.resume_token.is_empty(),
                    "retried transaction must start with empty resume token"
                );

                let stream = adapt([Ok(v1::PartialResultSet {
                    stats: Some(v1::ResultSetStats {
                        row_count: Some(v1::result_set_stats::RowCount::RowCountLowerBound(100)),
                        ..Default::default()
                    }),
                    ..Default::default()
                })]);
                Ok(tonic::Response::from(stream))
            });

        let (db_client, _server) = setup_db_client(mock).await;
        let transaction = db_client
            .partitioned_dml_transaction()
            .build()
            .await
            .expect("build transaction should succeed");
        let statement = Statement::builder("UPDATE Users SET active = true").build();
        let rows = transaction
            .execute_update(statement)
            .await
            .expect("execute_update should succeed after aborted retry");
        assert_eq!(
            rows, 100,
            "expected lower bound of 100 from new transaction"
        );
    }

    #[tokio_test_no_panics]
    async fn execute_update_stream_error_without_resume_token_restarts_transaction() {
        let mut mock = create_session_mock();

        let mut begin_sequence = mockall::Sequence::new();
        mock.expect_begin_transaction()
            .once()
            .in_sequence(&mut begin_sequence)
            .returning(|_request| {
                Ok(tonic::Response::new(v1::Transaction {
                    id: vec![1, 1, 1],
                    ..Default::default()
                }))
            });
        mock.expect_begin_transaction()
            .once()
            .in_sequence(&mut begin_sequence)
            .returning(|_request| {
                Ok(tonic::Response::new(v1::Transaction {
                    id: vec![2, 2, 2],
                    ..Default::default()
                }))
            });

        let mut stream_sequence = mockall::Sequence::new();
        mock.expect_execute_streaming_sql()
            .once()
            .in_sequence(&mut stream_sequence)
            .returning(|request| {
                let request = request.into_inner();
                let transaction_id = request
                    .transaction
                    .and_then(|selector| match selector.selector {
                        Some(v1::transaction_selector::Selector::Id(id)) => Some(id),
                        _ => None,
                    })
                    .expect("transaction selector id present");
                assert_eq!(
                    transaction_id,
                    vec![1, 1, 1],
                    "expected first transaction ID"
                );
                let stream = adapt([Err(tonic::Status::unavailable("connection reset"))]);
                Ok(tonic::Response::from(stream))
            });

        mock.expect_execute_streaming_sql()
            .once()
            .in_sequence(&mut stream_sequence)
            .returning(|request| {
                let request = request.into_inner();
                let transaction_id = request
                    .transaction
                    .and_then(|selector| match selector.selector {
                        Some(v1::transaction_selector::Selector::Id(id)) => Some(id),
                        _ => None,
                    })
                    .expect("transaction selector id present");
                assert_eq!(
                    transaction_id,
                    vec![2, 2, 2],
                    "expected second transaction ID"
                );
                let stream = adapt([Ok(v1::PartialResultSet {
                    stats: Some(v1::ResultSetStats {
                        row_count: Some(v1::result_set_stats::RowCount::RowCountLowerBound(42)),
                        ..Default::default()
                    }),
                    ..Default::default()
                })]);
                Ok(tonic::Response::from(stream))
            });

        let (db_client, _server) = setup_db_client(mock).await;
        let transaction = db_client
            .partitioned_dml_transaction()
            .build()
            .await
            .expect("build transaction should succeed");
        let statement = Statement::builder("UPDATE Users SET active = true").build();
        let rows = transaction
            .execute_update(statement)
            .await
            .expect("execute_update should succeed on retried transaction");
        assert_eq!(
            rows, 42,
            "expected lower bound of 42 from restarted transaction"
        );
    }

    #[tokio_test_no_panics]
    async fn execute_update_stream_error_without_resume_token_exhausts_retry_policy() {
        let mut mock = create_session_mock();

        mock.expect_begin_transaction()
            .once()
            .returning(|_request| {
                Ok(tonic::Response::new(v1::Transaction {
                    id: vec![1, 1, 1],
                    ..Default::default()
                }))
            });

        mock.expect_execute_streaming_sql()
            .once()
            .returning(|_request| {
                let stream = adapt([Err(tonic::Status::unavailable("connection reset"))]);
                Ok(tonic::Response::from(stream))
            });

        let (db_client, _server) = setup_db_client(mock).await;
        let transaction = db_client
            .partitioned_dml_transaction()
            .with_retry_policy(BasicTransactionRetryPolicy::new().with_max_attempts(1))
            .build()
            .await
            .expect("build transaction should succeed");
        let statement = Statement::builder("UPDATE Users SET active = true").build();
        let result = transaction.execute_update(statement).await;

        assert!(
            result.is_err(),
            "stream error without resume token must fail when retry policy max attempts is 1"
        );
        let error = result.expect_err("result must be error");
        assert!(
            error.to_string().contains("connection reset"),
            "expected connection reset error message, got: {error}"
        );
    }

    #[derive(Debug, PartialEq, Eq)]
    struct ParsedRequestId {
        prefix_without_attempt: String,
        request_id: u64,
        attempt: u32,
    }

    fn parse_request_id(request_id_str: &str) -> ParsedRequestId {
        let parts: Vec<&str> = request_id_str.split('.').collect();
        assert_eq!(
            parts.len(),
            6,
            "expected 6 parts in request id, got: {request_id_str}"
        );
        ParsedRequestId {
            prefix_without_attempt: parts[..5].join("."),
            request_id: parts[4]
                .parse()
                .expect("request id part must be a valid integer"),
            attempt: parts[5]
                .parse()
                .expect("attempt suffix must be a valid integer"),
        }
    }

    #[tokio_test_no_panics]
    async fn execute_update_request_id_resumption_bumps_attempt_and_keeps_prefix() {
        let mut mock = create_session_mock();
        let recorded_request_ids = Arc::new(Mutex::new(Vec::new()));

        let recorded_clone = Arc::clone(&recorded_request_ids);
        mock.expect_begin_transaction()
            .once()
            .returning(move |request| {
                let header_value = request
                    .metadata()
                    .get("x-goog-spanner-request-id")
                    .expect("x-goog-spanner-request-id present on begin_transaction")
                    .to_str()
                    .expect("header is valid ascii")
                    .to_string();
                recorded_clone
                    .lock()
                    .expect("lock acquired")
                    .push(("begin_transaction", header_value));
                Ok(tonic::Response::new(v1::Transaction {
                    id: vec![1, 2, 3],
                    ..Default::default()
                }))
            });

        let mut sequence = mockall::Sequence::new();

        let recorded_clone = Arc::clone(&recorded_request_ids);
        mock.expect_execute_streaming_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |request| {
                let header_value = request
                    .metadata()
                    .get("x-goog-spanner-request-id")
                    .expect("x-goog-spanner-request-id present on initial execute_streaming_sql")
                    .to_str()
                    .expect("header is valid ascii")
                    .to_string();
                recorded_clone
                    .lock()
                    .expect("lock acquired")
                    .push(("execute_streaming_sql_1", header_value));

                let stream = adapt([
                    Ok(v1::PartialResultSet {
                        resume_token: b"token-1".to_vec(),
                        ..Default::default()
                    }),
                    Err(tonic::Status::unavailable("connection dropped")),
                ]);
                Ok(tonic::Response::from(stream))
            });

        let recorded_clone = Arc::clone(&recorded_request_ids);
        mock.expect_execute_streaming_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |request| {
                let header_value = request
                    .metadata()
                    .get("x-goog-spanner-request-id")
                    .expect(
                        "x-goog-spanner-request-id present on first resumed execute_streaming_sql",
                    )
                    .to_str()
                    .expect("header is valid ascii")
                    .to_string();
                recorded_clone
                    .lock()
                    .expect("lock acquired")
                    .push(("execute_streaming_sql_2", header_value));

                let stream = adapt([
                    Ok(v1::PartialResultSet {
                        resume_token: b"token-2".to_vec(),
                        ..Default::default()
                    }),
                    Err(tonic::Status::unavailable("temporary reset")),
                ]);
                Ok(tonic::Response::from(stream))
            });

        let recorded_clone = Arc::clone(&recorded_request_ids);
        mock.expect_execute_streaming_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |request| {
                let header_value = request
                    .metadata()
                    .get("x-goog-spanner-request-id")
                    .expect(
                        "x-goog-spanner-request-id present on second resumed execute_streaming_sql",
                    )
                    .to_str()
                    .expect("header is valid ascii")
                    .to_string();
                recorded_clone
                    .lock()
                    .expect("lock acquired")
                    .push(("execute_streaming_sql_3", header_value));

                let stream = adapt([Ok(v1::PartialResultSet {
                    stats: Some(v1::ResultSetStats {
                        row_count: Some(v1::result_set_stats::RowCount::RowCountLowerBound(300)),
                        ..Default::default()
                    }),
                    ..Default::default()
                })]);
                Ok(tonic::Response::from(stream))
            });

        let (db_client, _server) = setup_db_client(mock).await;
        let transaction = db_client
            .partitioned_dml_transaction()
            .build()
            .await
            .expect("build transaction should succeed");
        let statement = Statement::builder("UPDATE Users SET active = true")
            .with_backoff_policy(
                ExponentialBackoffBuilder::new()
                    .with_initial_delay(Duration::from_millis(1))
                    .clamp(),
            )
            .build();
        let rows = transaction
            .execute_update(statement)
            .await
            .expect("execute_update should succeed");
        assert_eq!(rows, 300, "expected lower bound of 300");

        let entries = recorded_request_ids.lock().expect("lock acquired");
        assert_eq!(entries.len(), 4, "expected 4 RPC calls");

        let begin_id = parse_request_id(&entries[0].1);
        let stream_initial = parse_request_id(&entries[1].1);
        let stream_resume_1 = parse_request_id(&entries[2].1);
        let stream_resume_2 = parse_request_id(&entries[3].1);

        assert_eq!(
            begin_id.attempt, 1,
            "begin_transaction must start at attempt 1"
        );
        assert_eq!(
            stream_initial.attempt, 1,
            "initial streaming SQL must start at attempt 1"
        );
        assert!(
            stream_initial.request_id > begin_id.request_id,
            "subsequent RPC must increment request_id counter"
        );

        assert_eq!(
            stream_resume_1.prefix_without_attempt, stream_initial.prefix_without_attempt,
            "first stream resumption must preserve RequestId prefix"
        );
        assert_eq!(
            stream_resume_1.request_id, stream_initial.request_id,
            "first stream resumption must preserve request_id"
        );
        assert_eq!(
            stream_resume_1.attempt, 2,
            "first stream resumption must bump attempt suffix to 2"
        );

        assert_eq!(
            stream_resume_2.prefix_without_attempt, stream_initial.prefix_without_attempt,
            "second stream resumption must preserve RequestId prefix"
        );
        assert_eq!(
            stream_resume_2.request_id, stream_initial.request_id,
            "second stream resumption must preserve request_id"
        );
        assert_eq!(
            stream_resume_2.attempt, 3,
            "second stream resumption must bump attempt suffix to 3"
        );
    }

    #[tokio_test_no_panics]
    async fn execute_update_request_id_unavailable_then_aborted_resets_attempt() {
        let mut mock = create_session_mock();
        let recorded_request_ids = Arc::new(Mutex::new(Vec::new()));

        let mut begin_sequence = mockall::Sequence::new();

        let recorded_clone = Arc::clone(&recorded_request_ids);
        mock.expect_begin_transaction()
            .once()
            .in_sequence(&mut begin_sequence)
            .returning(move |request| {
                let header_value = request
                    .metadata()
                    .get("x-goog-spanner-request-id")
                    .expect("header present")
                    .to_str()
                    .expect("valid ascii")
                    .to_string();
                recorded_clone
                    .lock()
                    .expect("lock acquired")
                    .push(("begin_transaction_1", header_value));
                Ok(tonic::Response::new(v1::Transaction {
                    id: vec![1, 1, 1],
                    ..Default::default()
                }))
            });

        let recorded_clone = Arc::clone(&recorded_request_ids);
        mock.expect_begin_transaction()
            .once()
            .in_sequence(&mut begin_sequence)
            .returning(move |request| {
                let header_value = request
                    .metadata()
                    .get("x-goog-spanner-request-id")
                    .expect("header present")
                    .to_str()
                    .expect("valid ascii")
                    .to_string();
                recorded_clone
                    .lock()
                    .expect("lock acquired")
                    .push(("begin_transaction_2", header_value));
                Ok(tonic::Response::new(v1::Transaction {
                    id: vec![2, 2, 2],
                    ..Default::default()
                }))
            });

        let mut stream_sequence = mockall::Sequence::new();

        // 1. Initial attempt: sends token-1 then UNAVAILABLE
        let recorded_clone = Arc::clone(&recorded_request_ids);
        mock.expect_execute_streaming_sql()
            .once()
            .in_sequence(&mut stream_sequence)
            .returning(move |request| {
                let header_value = request
                    .metadata()
                    .get("x-goog-spanner-request-id")
                    .expect("header present")
                    .to_str()
                    .expect("valid ascii")
                    .to_string();
                recorded_clone
                    .lock()
                    .expect("lock acquired")
                    .push(("execute_streaming_sql_1_initial", header_value));

                let stream = adapt([
                    Ok(v1::PartialResultSet {
                        resume_token: b"token-1".to_vec(),
                        ..Default::default()
                    }),
                    Err(tonic::Status::unavailable("connection reset")),
                ]);
                Ok(tonic::Response::from(stream))
            });

        // 2. Resumed stream on same transaction: receives ABORTED
        let recorded_clone = Arc::clone(&recorded_request_ids);
        mock.expect_execute_streaming_sql()
            .once()
            .in_sequence(&mut stream_sequence)
            .returning(move |request| {
                let header_value = request
                    .metadata()
                    .get("x-goog-spanner-request-id")
                    .expect("header present")
                    .to_str()
                    .expect("valid ascii")
                    .to_string();
                recorded_clone
                    .lock()
                    .expect("lock acquired")
                    .push(("execute_streaming_sql_1_resumed", header_value));

                let stream = adapt([Err(create_aborted_status(Duration::from_nanos(1)))]);
                Ok(tonic::Response::from(stream))
            });

        // 3. New transaction after aborted: initial stream sends token-2 then UNAVAILABLE
        let recorded_clone = Arc::clone(&recorded_request_ids);
        mock.expect_execute_streaming_sql()
            .once()
            .in_sequence(&mut stream_sequence)
            .returning(move |request| {
                let header_value = request
                    .metadata()
                    .get("x-goog-spanner-request-id")
                    .expect("header present")
                    .to_str()
                    .expect("valid ascii")
                    .to_string();
                recorded_clone
                    .lock()
                    .expect("lock acquired")
                    .push(("execute_streaming_sql_2_initial", header_value));

                let stream = adapt([
                    Ok(v1::PartialResultSet {
                        resume_token: b"token-2".to_vec(),
                        ..Default::default()
                    }),
                    Err(tonic::Status::unavailable("network hiccup")),
                ]);
                Ok(tonic::Response::from(stream))
            });

        // 4. Resumed stream on new transaction: completes successfully
        let recorded_clone = Arc::clone(&recorded_request_ids);
        mock.expect_execute_streaming_sql()
            .once()
            .in_sequence(&mut stream_sequence)
            .returning(move |request| {
                let header_value = request
                    .metadata()
                    .get("x-goog-spanner-request-id")
                    .expect("header present")
                    .to_str()
                    .expect("valid ascii")
                    .to_string();
                recorded_clone
                    .lock()
                    .expect("lock acquired")
                    .push(("execute_streaming_sql_2_resumed", header_value));

                let stream = adapt([Ok(v1::PartialResultSet {
                    stats: Some(v1::ResultSetStats {
                        row_count: Some(v1::result_set_stats::RowCount::RowCountLowerBound(500)),
                        ..Default::default()
                    }),
                    ..Default::default()
                })]);
                Ok(tonic::Response::from(stream))
            });

        let (db_client, _server) = setup_db_client(mock).await;
        let transaction = db_client
            .partitioned_dml_transaction()
            .build()
            .await
            .expect("build transaction should succeed");
        let statement = Statement::builder("UPDATE Users SET active = true")
            .with_backoff_policy(
                ExponentialBackoffBuilder::new()
                    .with_initial_delay(Duration::from_millis(1))
                    .clamp(),
            )
            .build();
        let rows = transaction
            .execute_update(statement)
            .await
            .expect("execute_update should succeed after aborted retry");
        assert_eq!(rows, 500, "expected lower bound of 500");

        let entries = recorded_request_ids.lock().expect("lock acquired");
        assert_eq!(entries.len(), 6, "expected 6 RPC calls in total");

        let begin_1 = parse_request_id(&entries[0].1);
        let stream_1_initial = parse_request_id(&entries[1].1);
        let stream_1_resumed = parse_request_id(&entries[2].1);
        let begin_2 = parse_request_id(&entries[3].1);
        let stream_2_initial = parse_request_id(&entries[4].1);
        let stream_2_resumed = parse_request_id(&entries[5].1);

        assert_eq!(
            begin_1.attempt, 1,
            "first begin_transaction must start with attempt 1"
        );
        assert_eq!(
            stream_1_initial.attempt, 1,
            "first transaction stream must start with attempt 1"
        );
        assert!(
            stream_1_initial.request_id > begin_1.request_id,
            "first stream must increment request_id after begin_transaction"
        );
        assert_eq!(
            stream_1_resumed.prefix_without_attempt, stream_1_initial.prefix_without_attempt,
            "resumed stream must maintain prefix"
        );
        assert_eq!(
            stream_1_resumed.attempt, 2,
            "resumed stream must bump attempt to 2"
        );

        // Transaction 2: new transaction after Aborted restarts with fresh RequestId and attempt=1
        assert_eq!(
            begin_2.attempt, 1,
            "retried begin_transaction must start at attempt 1"
        );
        assert!(
            begin_2.request_id > stream_1_initial.request_id,
            "retried begin_transaction must have newer request_id"
        );

        assert_eq!(
            stream_2_initial.attempt, 1,
            "stream in retried transaction must reset attempt to 1"
        );
        assert!(
            stream_2_initial.request_id > begin_2.request_id,
            "stream in retried transaction must have newer request_id"
        );

        // Resumed stream on Transaction 2: bumps to attempt=2 and preserves Transaction 2 prefix
        assert_eq!(
            stream_2_resumed.prefix_without_attempt, stream_2_initial.prefix_without_attempt,
            "second transaction resumed stream must preserve second transaction prefix"
        );
        assert_eq!(
            stream_2_resumed.attempt, 2,
            "second transaction resumed stream must bump attempt to 2"
        );
    }

    #[tokio_test_no_panics]
    async fn execute_update_request_id_aborted_then_aborted_then_success() {
        let mut mock = create_session_mock();
        let recorded_request_ids = Arc::new(Mutex::new(Vec::new()));

        let mut begin_sequence = mockall::Sequence::new();
        for _ in 0..3 {
            let recorded_clone = Arc::clone(&recorded_request_ids);
            mock.expect_begin_transaction()
                .once()
                .in_sequence(&mut begin_sequence)
                .returning(move |request| {
                    let header_value = request
                        .metadata()
                        .get("x-goog-spanner-request-id")
                        .expect("header present")
                        .to_str()
                        .expect("valid ascii")
                        .to_string();
                    recorded_clone
                        .lock()
                        .expect("lock acquired")
                        .push(("begin_transaction", header_value));
                    Ok(tonic::Response::new(v1::Transaction {
                        id: vec![0, 1, 2],
                        ..Default::default()
                    }))
                });
        }

        let mut stream_sequence = mockall::Sequence::new();

        // Attempt 1: ABORTED
        let recorded_clone = Arc::clone(&recorded_request_ids);
        mock.expect_execute_streaming_sql()
            .once()
            .in_sequence(&mut stream_sequence)
            .returning(move |request| {
                let header_value = request
                    .metadata()
                    .get("x-goog-spanner-request-id")
                    .expect("header present")
                    .to_str()
                    .expect("valid ascii")
                    .to_string();
                recorded_clone
                    .lock()
                    .expect("lock acquired")
                    .push(("execute_streaming_sql_attempt_1", header_value));

                let stream = adapt([Err(create_aborted_status(Duration::from_nanos(1)))]);
                Ok(tonic::Response::from(stream))
            });

        // Attempt 2: ABORTED
        let recorded_clone = Arc::clone(&recorded_request_ids);
        mock.expect_execute_streaming_sql()
            .once()
            .in_sequence(&mut stream_sequence)
            .returning(move |request| {
                let header_value = request
                    .metadata()
                    .get("x-goog-spanner-request-id")
                    .expect("header present")
                    .to_str()
                    .expect("valid ascii")
                    .to_string();
                recorded_clone
                    .lock()
                    .expect("lock acquired")
                    .push(("execute_streaming_sql_attempt_2", header_value));

                let stream = adapt([Err(create_aborted_status(Duration::from_nanos(1)))]);
                Ok(tonic::Response::from(stream))
            });

        // Attempt 3: SUCCESS
        let recorded_clone = Arc::clone(&recorded_request_ids);
        mock.expect_execute_streaming_sql()
            .once()
            .in_sequence(&mut stream_sequence)
            .returning(move |request| {
                let header_value = request
                    .metadata()
                    .get("x-goog-spanner-request-id")
                    .expect("header present")
                    .to_str()
                    .expect("valid ascii")
                    .to_string();
                recorded_clone
                    .lock()
                    .expect("lock acquired")
                    .push(("execute_streaming_sql_attempt_3", header_value));

                let stream = adapt([Ok(v1::PartialResultSet {
                    stats: Some(v1::ResultSetStats {
                        row_count: Some(v1::result_set_stats::RowCount::RowCountLowerBound(42)),
                        ..Default::default()
                    }),
                    ..Default::default()
                })]);
                Ok(tonic::Response::from(stream))
            });

        let (db_client, _server) = setup_db_client(mock).await;
        let transaction = db_client
            .partitioned_dml_transaction()
            .build()
            .await
            .expect("build transaction should succeed");
        let statement = Statement::builder("UPDATE Users SET active = true").build();
        let rows = transaction
            .execute_update(statement)
            .await
            .expect("execute_update should succeed on third attempt");
        assert_eq!(rows, 42, "expected lower bound of 42");

        let entries = recorded_request_ids.lock().expect("lock acquired");
        assert_eq!(entries.len(), 6, "expected 6 RPC calls (3 pairs)");

        let mut previous_request_id = 0;
        for (i, entry) in entries.iter().enumerate() {
            let parsed = parse_request_id(&entry.1);
            assert_eq!(
                parsed.attempt, 1,
                "all distinct RPC attempts after Aborted must have attempt=1 (failed at entry {i})"
            );
            assert!(
                parsed.request_id > previous_request_id,
                "request_id must strictly increase across distinct RPCs (entry {i})"
            );
            previous_request_id = parsed.request_id;
        }
    }

    #[tokio_test_no_panics]
    async fn execute_update_request_id_unavailable_without_token_allocates_fresh_request_id() {
        let mut mock = create_session_mock();
        let recorded_request_ids = Arc::new(Mutex::new(Vec::new()));

        let mut begin_sequence = mockall::Sequence::new();
        let recorded_clone = Arc::clone(&recorded_request_ids);
        mock.expect_begin_transaction()
            .once()
            .in_sequence(&mut begin_sequence)
            .returning(move |request| {
                let header_value = request
                    .metadata()
                    .get("x-goog-spanner-request-id")
                    .expect("header present")
                    .to_str()
                    .expect("valid ascii")
                    .to_string();
                recorded_clone
                    .lock()
                    .expect("lock acquired")
                    .push(("begin_transaction_1", header_value));
                Ok(tonic::Response::new(v1::Transaction {
                    id: vec![1, 1, 1],
                    ..Default::default()
                }))
            });

        let recorded_clone = Arc::clone(&recorded_request_ids);
        mock.expect_begin_transaction()
            .once()
            .in_sequence(&mut begin_sequence)
            .returning(move |request| {
                let header_value = request
                    .metadata()
                    .get("x-goog-spanner-request-id")
                    .expect("header present")
                    .to_str()
                    .expect("valid ascii")
                    .to_string();
                recorded_clone
                    .lock()
                    .expect("lock acquired")
                    .push(("begin_transaction_2", header_value));
                Ok(tonic::Response::new(v1::Transaction {
                    id: vec![2, 2, 2],
                    ..Default::default()
                }))
            });

        let mut stream_sequence = mockall::Sequence::new();
        let recorded_clone = Arc::clone(&recorded_request_ids);
        mock.expect_execute_streaming_sql()
            .once()
            .in_sequence(&mut stream_sequence)
            .returning(move |request| {
                let header_value = request
                    .metadata()
                    .get("x-goog-spanner-request-id")
                    .expect("header present")
                    .to_str()
                    .expect("valid ascii")
                    .to_string();
                recorded_clone
                    .lock()
                    .expect("lock acquired")
                    .push(("execute_streaming_sql_1", header_value));

                let stream = adapt([Err(tonic::Status::unavailable("connection reset"))]);
                Ok(tonic::Response::from(stream))
            });

        let recorded_clone = Arc::clone(&recorded_request_ids);
        mock.expect_execute_streaming_sql()
            .once()
            .in_sequence(&mut stream_sequence)
            .returning(move |request| {
                let header_value = request
                    .metadata()
                    .get("x-goog-spanner-request-id")
                    .expect("header present")
                    .to_str()
                    .expect("valid ascii")
                    .to_string();
                recorded_clone
                    .lock()
                    .expect("lock acquired")
                    .push(("execute_streaming_sql_2", header_value));

                let stream = adapt([Ok(v1::PartialResultSet {
                    stats: Some(v1::ResultSetStats {
                        row_count: Some(v1::result_set_stats::RowCount::RowCountLowerBound(42)),
                        ..Default::default()
                    }),
                    ..Default::default()
                })]);
                Ok(tonic::Response::from(stream))
            });

        let (db_client, _server) = setup_db_client(mock).await;
        let transaction = db_client
            .partitioned_dml_transaction()
            .build()
            .await
            .expect("build transaction should succeed");
        let statement = Statement::builder("UPDATE Users SET active = true").build();
        let rows = transaction
            .execute_update(statement)
            .await
            .expect("execute_update should succeed on retried transaction");
        assert_eq!(rows, 42, "expected lower bound of 42");

        let entries = recorded_request_ids.lock().expect("lock acquired");
        assert_eq!(entries.len(), 4, "expected 4 RPC calls (2 pairs)");

        let mut previous_request_id = 0;
        for (i, entry) in entries.iter().enumerate() {
            let parsed = parse_request_id(&entry.1);
            assert_eq!(
                parsed.attempt, 1,
                "all distinct RPC attempts after transaction restart must have attempt=1 (failed at entry {i})"
            );
            assert!(
                parsed.request_id > previous_request_id,
                "request_id must strictly increase across distinct RPCs (entry {i})"
            );
            previous_request_id = parsed.request_id;
        }
    }

    fn create_unavailable_status(retry_delay: Duration) -> tonic::Status {
        use crate::retry_delay::{ProtoRetryInfo, RETRY_INFO_TYPE_URL};
        use prost::Message;

        let retry_info = ProtoRetryInfo {
            retry_delay: Some(prost_types::Duration {
                seconds: retry_delay.as_secs() as i64,
                nanos: retry_delay.subsec_nanos() as i32,
            }),
        };

        let mut retry_buffer = vec![];
        retry_info
            .encode(&mut retry_buffer)
            .expect("encoding retry_info should succeed");

        let status = spanner_grpc_mock::google::rpc::Status {
            code: tonic::Code::Unavailable as i32,
            message: "test stream unavailable with retry info".to_string(),
            details: vec![prost_types::Any {
                type_url: RETRY_INFO_TYPE_URL.to_string(),
                value: retry_buffer,
            }],
        };

        let mut status_buffer = vec![];
        status
            .encode(&mut status_buffer)
            .expect("encoding mock status should succeed");

        tonic::Status::with_details(
            tonic::Code::Unavailable,
            "test stream unavailable with retry info",
            status_buffer.into(),
        )
    }

    #[tokio_test_no_panics]
    async fn execute_update_permanent_error_after_resume_token_fails_immediately() {
        let mut mock = create_session_mock();

        mock.expect_begin_transaction()
            .once()
            .returning(|_request| {
                Ok(tonic::Response::new(v1::Transaction {
                    id: vec![0, 1, 2],
                    ..Default::default()
                }))
            });

        mock.expect_execute_streaming_sql()
            .once()
            .returning(|_request| {
                let stream = adapt([
                    Ok(v1::PartialResultSet {
                        resume_token: b"token-1".to_vec(),
                        ..Default::default()
                    }),
                    Err(tonic::Status::permission_denied("insufficient privileges")),
                ]);
                Ok(tonic::Response::from(stream))
            });

        let (db_client, _server) = setup_db_client(mock).await;
        let transaction = db_client
            .partitioned_dml_transaction()
            .build()
            .await
            .expect("build transaction should succeed");
        let statement = Statement::builder("UPDATE Users SET active = true").build();
        let result = transaction.execute_update(statement).await;

        assert!(
            result.is_err(),
            "permanent error after resume token must fail immediately"
        );
        let error = result.expect_err("result must be error");
        assert!(
            error.to_string().contains("insufficient privileges"),
            "expected permission denied error message, got: {error}"
        );
    }

    #[tokio_test_no_panics]
    async fn execute_update_reconnect_transient_error_recovers() {
        let mut mock = create_session_mock();

        mock.expect_begin_transaction()
            .once()
            .returning(|_request| {
                Ok(tonic::Response::new(v1::Transaction {
                    id: vec![0, 1, 2],
                    ..Default::default()
                }))
            });

        let mut sequence = mockall::Sequence::new();
        // 1. Initial stream: sends token-1, then UNAVAILABLE
        mock.expect_execute_streaming_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(|request| {
                let request = request.into_inner();
                assert!(
                    request.resume_token.is_empty(),
                    "initial stream must not have resume token"
                );
                let stream = adapt([
                    Ok(v1::PartialResultSet {
                        resume_token: b"token-1".to_vec(),
                        stats: Some(v1::ResultSetStats {
                            row_count: Some(v1::result_set_stats::RowCount::RowCountLowerBound(10)),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    Err(tonic::Status::unavailable("stream drop 1")),
                ]);
                Ok(tonic::Response::from(stream))
            });

        // 2. First reconnect: fails immediately with UNAVAILABLE
        mock.expect_execute_streaming_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(|request| {
                let request = request.into_inner();
                assert_eq!(
                    request.resume_token,
                    b"token-1".to_vec(),
                    "reconnect request must have token-1"
                );
                let stream = adapt([Err(tonic::Status::unavailable("reconnect drop 2"))]);
                Ok(tonic::Response::from(stream))
            });

        // 3. Second reconnect: succeeds with token-2 and stats
        mock.expect_execute_streaming_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(|request| {
                let request = request.into_inner();
                assert_eq!(
                    request.resume_token,
                    b"token-1".to_vec(),
                    "second reconnect request must still use last valid token-1"
                );
                let stream = adapt([Ok(v1::PartialResultSet {
                    resume_token: b"token-2".to_vec(),
                    stats: Some(v1::ResultSetStats {
                        row_count: Some(v1::result_set_stats::RowCount::RowCountLowerBound(32)),
                        ..Default::default()
                    }),
                    ..Default::default()
                })]);
                Ok(tonic::Response::from(stream))
            });

        let (db_client, _server) = setup_db_client(mock).await;
        let transaction = db_client
            .partitioned_dml_transaction()
            .build()
            .await
            .expect("build transaction should succeed");
        let statement = Statement::builder("UPDATE Users SET active = true")
            .with_backoff_policy(
                ExponentialBackoffBuilder::new()
                    .with_initial_delay(Duration::from_millis(1))
                    .clamp(),
            )
            .build();
        let rows = transaction
            .execute_update(statement)
            .await
            .expect("execute_update should succeed after second reconnect");
        assert_eq!(rows, 42, "expected accumulated lower bound of 10 + 32 = 42");
    }

    #[tokio_test_no_panics]
    async fn execute_update_stream_retry_policy_exhausted() {
        use google_cloud_gax::retry_policy::RetryPolicyExt;

        let mut mock = create_session_mock();

        mock.expect_begin_transaction()
            .once()
            .returning(|_request| {
                Ok(tonic::Response::new(v1::Transaction {
                    id: vec![0, 1, 2],
                    ..Default::default()
                }))
            });

        let mut sequence = mockall::Sequence::new();
        // Attempt 1: yields token-1, then UNAVAILABLE
        mock.expect_execute_streaming_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(|_request| {
                let stream = adapt([
                    Ok(v1::PartialResultSet {
                        resume_token: b"token-1".to_vec(),
                        ..Default::default()
                    }),
                    Err(tonic::Status::unavailable("stream failure 1")),
                ]);
                Ok(tonic::Response::from(stream))
            });

        // Attempt 2: fails with UNAVAILABLE (attempt count reaches limit of 2)
        mock.expect_execute_streaming_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(|_request| {
                let stream = adapt([Err(tonic::Status::unavailable("stream failure 2"))]);
                Ok(tonic::Response::from(stream))
            });

        let (db_client, _server) = setup_db_client(mock).await;
        let transaction = db_client
            .partitioned_dml_transaction()
            .build()
            .await
            .expect("build transaction should succeed");
        // Limit stream attempts to 2
        let statement = Statement::builder("UPDATE Users SET active = true")
            .with_retry_policy(SpannerRetryPolicy::new().with_attempt_limit(2))
            .with_backoff_policy(
                ExponentialBackoffBuilder::new()
                    .with_initial_delay(Duration::from_millis(1))
                    .clamp(),
            )
            .build();
        let result = transaction.execute_update(statement).await;

        assert!(
            result.is_err(),
            "stream retry policy must be exhausted after 2 attempts"
        );
        let error = result.expect_err("result must be error");
        assert!(
            error.to_string().contains("stream failure 2"),
            "expected final failure error, got: {error}"
        );
    }

    #[tokio_test_no_panics]
    async fn execute_update_resumption_respects_server_retry_info() {
        let mut mock = create_session_mock();

        mock.expect_begin_transaction()
            .once()
            .returning(|_request| {
                Ok(tonic::Response::new(v1::Transaction {
                    id: vec![0, 1, 2],
                    ..Default::default()
                }))
            });

        let mut sequence = mockall::Sequence::new();
        mock.expect_execute_streaming_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(|_request| {
                let stream = adapt([
                    Ok(v1::PartialResultSet {
                        resume_token: b"token-1".to_vec(),
                        ..Default::default()
                    }),
                    Err(create_unavailable_status(Duration::from_millis(5))),
                ]);
                Ok(tonic::Response::from(stream))
            });

        mock.expect_execute_streaming_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(|_request| {
                let stream = adapt([Ok(v1::PartialResultSet {
                    stats: Some(v1::ResultSetStats {
                        row_count: Some(v1::result_set_stats::RowCount::RowCountLowerBound(99)),
                        ..Default::default()
                    }),
                    ..Default::default()
                })]);
                Ok(tonic::Response::from(stream))
            });

        let (db_client, _server) = setup_db_client(mock).await;
        let transaction = db_client
            .partitioned_dml_transaction()
            .build()
            .await
            .expect("build transaction should succeed");
        let statement = Statement::builder("UPDATE Users SET active = true").build();
        let start_time = Instant::now();
        let rows = transaction
            .execute_update(statement)
            .await
            .expect("execute_update should succeed");
        let elapsed = start_time.elapsed();
        assert_eq!(rows, 99, "expected lower bound of 99");
        assert!(
            elapsed >= Duration::from_millis(5),
            "expected execution to sleep for at least the 5ms RetryInfo delay, elapsed was {elapsed:?}"
        );
    }

    #[tokio_test_no_panics]
    async fn execute_update_begin_transaction_permanent_error_fails_immediately() {
        let mut mock = create_session_mock();

        mock.expect_begin_transaction()
            .once()
            .returning(|_request| Err(tonic::Status::permission_denied("insufficient privileges")));

        mock.expect_execute_streaming_sql().never();

        let (db_client, _server) = setup_db_client(mock).await;
        let transaction = db_client
            .partitioned_dml_transaction()
            .build()
            .await
            .expect("build transaction should succeed");
        let statement = Statement::builder("UPDATE Users SET active = true").build();
        let result = transaction.execute_update(statement).await;

        assert!(
            result.is_err(),
            "execute_update must fail immediately on permanent error in begin_transaction"
        );
        let error = result.expect_err("result must be error");
        assert_eq!(
            error.status().map(|status| status.code),
            Some(Code::PermissionDenied),
            "expected PermissionDenied status, got: {error}"
        );
    }

    #[tokio_test_no_panics]
    async fn execute_update_begin_transaction_unavailable_retries_and_succeeds() {
        let mut mock = create_session_mock();

        let mut begin_sequence = mockall::Sequence::new();
        mock.expect_begin_transaction()
            .once()
            .in_sequence(&mut begin_sequence)
            .returning(|_request| Err(tonic::Status::unavailable("connection reset")));
        mock.expect_begin_transaction()
            .once()
            .in_sequence(&mut begin_sequence)
            .returning(|_request| {
                Ok(tonic::Response::new(v1::Transaction {
                    id: vec![0, 1, 2],
                    ..Default::default()
                }))
            });

        mock.expect_execute_streaming_sql()
            .once()
            .returning(|_request| {
                let stream = adapt([Ok(v1::PartialResultSet {
                    stats: Some(v1::ResultSetStats {
                        row_count: Some(v1::result_set_stats::RowCount::RowCountLowerBound(42)),
                        ..Default::default()
                    }),
                    ..Default::default()
                })]);
                Ok(tonic::Response::from(stream))
            });

        let (db_client, _server) = setup_db_client(mock).await;
        let transaction = db_client
            .partitioned_dml_transaction()
            .with_retry_policy(BasicTransactionRetryPolicy::new().with_max_attempts(3))
            .build()
            .await
            .expect("build transaction should succeed");
        let statement = Statement::builder("UPDATE Users SET active = true").build();
        let row_count = transaction
            .execute_update(statement)
            .await
            .expect("execute_update should succeed after begin_transaction retry");
        assert_eq!(
            row_count, 42,
            "modified row count must match lower bound after begin_transaction retries"
        );
    }

    #[tokio_test_no_panics]
    async fn execute_update_begin_transaction_retry_policy_exhausted() {
        let mut mock = create_session_mock();

        mock.expect_begin_transaction()
            .times(2)
            .returning(|_request| Err(create_aborted_status(Duration::from_millis(1))));

        mock.expect_execute_streaming_sql().never();

        let (db_client, _server) = setup_db_client(mock).await;
        let transaction = db_client
            .partitioned_dml_transaction()
            .with_retry_policy(BasicTransactionRetryPolicy::new().with_max_attempts(2))
            .build()
            .await
            .expect("build transaction should succeed");
        let statement = Statement::builder("UPDATE Users SET active = true").build();
        let result = transaction.execute_update(statement).await;

        assert!(
            result.is_err(),
            "execute_update must fail when begin_transaction retries exhaust retry policy"
        );
        let error = result.expect_err("result must be error");
        assert_eq!(
            error.status().map(|status| status.code),
            Some(Code::Aborted),
            "expected Aborted status, got: {error}"
        );
    }

    #[tokio_test_no_panics]
    async fn execute_update_reconnect_permanent_error_fails_immediately() {
        let mut mock = create_session_mock();

        mock.expect_begin_transaction()
            .once()
            .returning(|_request| {
                Ok(tonic::Response::new(v1::Transaction {
                    id: vec![0, 1, 2],
                    ..Default::default()
                }))
            });

        let mut sequence = mockall::Sequence::new();
        // Initial stream produces a resume token, then drops
        mock.expect_execute_streaming_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(|_request| {
                let stream = adapt([
                    Ok(v1::PartialResultSet {
                        resume_token: b"token-1".to_vec(),
                        ..Default::default()
                    }),
                    Err(tonic::Status::unavailable("stream dropped")),
                ]);
                Ok(tonic::Response::from(stream))
            });

        // Reconnect RPC fails with a permanent error (PermissionDenied)
        mock.expect_execute_streaming_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(|_request| Err(tonic::Status::permission_denied("permission revoked")));

        let (db_client, _server) = setup_db_client(mock).await;
        let transaction = db_client
            .partitioned_dml_transaction()
            .build()
            .await
            .expect("build transaction should succeed");
        let statement = Statement::builder("UPDATE Users SET active = true").build();
        let result = transaction.execute_update(statement).await;

        assert!(
            result.is_err(),
            "execute_update must fail immediately when stream reconnect encounters permanent error"
        );
        let error = result.expect_err("result must be error");
        assert_eq!(
            error.status().map(|status| status.code),
            Some(Code::PermissionDenied),
            "expected PermissionDenied status from reconnect failure, got: {error}"
        );
    }

    #[tokio_test_no_panics]
    async fn execute_update_reconnect_aborted_restarts_transaction() {
        let mut mock = create_session_mock();

        let mut begin_sequence = mockall::Sequence::new();
        // First begin_transaction returns transaction 1
        mock.expect_begin_transaction()
            .once()
            .in_sequence(&mut begin_sequence)
            .returning(|_request| {
                Ok(tonic::Response::new(v1::Transaction {
                    id: vec![1],
                    ..Default::default()
                }))
            });

        // Second begin_transaction returns transaction 2 after transaction restart
        mock.expect_begin_transaction()
            .once()
            .in_sequence(&mut begin_sequence)
            .returning(|_request| {
                Ok(tonic::Response::new(v1::Transaction {
                    id: vec![2],
                    ..Default::default()
                }))
            });

        let mut stream_sequence = mockall::Sequence::new();
        // 1. Initial stream produces a resume token, then drops with transient Unavailable
        mock.expect_execute_streaming_sql()
            .once()
            .in_sequence(&mut stream_sequence)
            .returning(|_request| {
                let stream = adapt([
                    Ok(v1::PartialResultSet {
                        resume_token: b"token-1".to_vec(),
                        ..Default::default()
                    }),
                    Err(tonic::Status::unavailable("stream dropped")),
                ]);
                Ok(tonic::Response::from(stream))
            });

        // 2. Reconnect stream RPC returns Aborted, forcing transaction restart
        mock.expect_execute_streaming_sql()
            .once()
            .in_sequence(&mut stream_sequence)
            .returning(|_request| Err(create_aborted_status(Duration::from_nanos(1))));

        // 3. Restarted transaction executes streaming SQL from scratch and succeeds
        mock.expect_execute_streaming_sql()
            .once()
            .in_sequence(&mut stream_sequence)
            .returning(|request| {
                let request = request.into_inner();
                assert_eq!(
                    request.transaction,
                    Some(v1::TransactionSelector {
                        selector: Some(v1::transaction_selector::Selector::Id(vec![2])),
                    }),
                    "restarted transaction must use new transaction ID"
                );
                assert!(
                    request.resume_token.is_empty(),
                    "restarted transaction must start without resume token"
                );
                let stream = adapt([Ok(v1::PartialResultSet {
                    stats: Some(v1::ResultSetStats {
                        row_count: Some(v1::result_set_stats::RowCount::RowCountLowerBound(100)),
                        ..Default::default()
                    }),
                    ..Default::default()
                })]);
                Ok(tonic::Response::from(stream))
            });

        let (db_client, _server) = setup_db_client(mock).await;
        let transaction = db_client
            .partitioned_dml_transaction()
            .build()
            .await
            .expect("build transaction should succeed");
        let statement = Statement::builder("UPDATE Users SET active = true").build();
        let rows_affected = transaction
            .execute_update(statement)
            .await
            .expect("execute_update should succeed after restarting aborted transaction");

        assert_eq!(
            rows_affected, 100,
            "rows affected should match restarted transaction result"
        );
    }

    #[tokio_test_no_panics]
    async fn execute_update_start_stream_rpc_error_retries_and_succeeds() {
        let mut mock = create_session_mock();

        let mut begin_sequence = mockall::Sequence::new();
        // First begin_transaction returns transaction 1
        mock.expect_begin_transaction()
            .once()
            .in_sequence(&mut begin_sequence)
            .returning(|_request| {
                Ok(tonic::Response::new(v1::Transaction {
                    id: vec![1],
                    ..Default::default()
                }))
            });

        // Second begin_transaction returns transaction 2 (restarted)
        mock.expect_begin_transaction()
            .once()
            .in_sequence(&mut begin_sequence)
            .returning(|_request| {
                Ok(tonic::Response::new(v1::Transaction {
                    id: vec![2],
                    ..Default::default()
                }))
            });

        let mut stream_sequence = mockall::Sequence::new();
        // 1. Initial start_stream RPC fails immediately with Unavailable
        mock.expect_execute_streaming_sql()
            .once()
            .in_sequence(&mut stream_sequence)
            .returning(|_request| Err(tonic::Status::unavailable("connection refused")));

        // 2. Restarted transaction's start_stream succeeds
        mock.expect_execute_streaming_sql()
            .once()
            .in_sequence(&mut stream_sequence)
            .returning(|request| {
                let request = request.into_inner();
                assert_eq!(
                    request.transaction,
                    Some(v1::TransactionSelector {
                        selector: Some(v1::transaction_selector::Selector::Id(vec![2])),
                    }),
                    "restarted transaction must use new transaction ID"
                );
                assert!(
                    request.resume_token.is_empty(),
                    "restarted transaction must start without resume token"
                );
                let stream = adapt([Ok(v1::PartialResultSet {
                    stats: Some(v1::ResultSetStats {
                        row_count: Some(v1::result_set_stats::RowCount::RowCountLowerBound(50)),
                        ..Default::default()
                    }),
                    ..Default::default()
                })]);
                Ok(tonic::Response::from(stream))
            });

        let (db_client, _server) = setup_db_client(mock).await;
        let transaction = db_client
            .partitioned_dml_transaction()
            .build()
            .await
            .expect("build transaction should succeed");
        let statement = Statement::builder("UPDATE Users SET active = true").build();
        let rows_affected = transaction
            .execute_update(statement)
            .await
            .expect("execute_update should succeed after restarting transaction following initial RPC failure");

        assert_eq!(
            rows_affected, 50,
            "rows affected should match restarted transaction result"
        );
    }
}
