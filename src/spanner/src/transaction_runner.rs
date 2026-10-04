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
use crate::database_client::DatabaseClient;
use crate::model::CommitResponse;
use crate::model::commit_response::CommitStats;
use crate::model::request_options::Priority;
use crate::model::transaction_options::IsolationLevel;
use crate::model::transaction_options::read_write::ReadLockMode;
use crate::read_only_transaction::{BeginTransactionOption, ReadContextTransactionSelector};
use crate::read_write_transaction::{ReadWriteTransaction, ReadWriteTransactionBuilder};
use crate::transaction_retry_policy::{
    BasicTransactionRetryPolicy, TransactionRetryPolicy, backoff_if_aborted, is_aborted,
};
use google_cloud_gax::backoff_policy::BackoffPolicyArg;
use google_cloud_gax::retry_policy::RetryPolicyArg;
use std::sync::Arc;

use std::time::Duration;
use tokio::time::Instant;
use wkt::Timestamp;

/// A builder for a [TransactionRunner] for a read/write transaction.
///
/// # Example
/// ```
/// # use google_cloud_spanner::client::Spanner;
/// # use google_cloud_spanner::statement::Statement;
/// # async fn run(client: Spanner) -> Result<(), google_cloud_spanner::Error> {
/// let db_client = client.database_client("projects/p/instances/i/databases/d").build().await?;
/// let runner = db_client.read_write_transaction().build();
///
/// let result = runner.run(async |transaction| {
///     let statement = Statement::builder("UPDATE MyTable SET MyColumn = 'MyValue' WHERE Id = 1").build();
///     transaction.execute_update(statement).await?;
///     Ok(42)
/// }).await?;
/// # Ok(())
/// # }
/// ```
///
/// Spanner can abort any read/write transaction at any time. A [TransactionRunner]
/// automatically retries aborted transactions according to the configured retry policy.
#[derive(Debug)]
pub struct TransactionRunnerBuilder {
    builder: ReadWriteTransactionBuilder,
    retry_policy: Box<dyn TransactionRetryPolicy>,
    timeout: Option<Duration>,
    begin_gax_options: Option<crate::RequestOptions>,
    commit_gax_options: Option<crate::RequestOptions>,
}

impl TransactionRunnerBuilder {
    pub(crate) fn new(client: DatabaseClient) -> Self {
        Self {
            builder: ReadWriteTransactionBuilder::new(client),
            retry_policy: Box::new(BasicTransactionRetryPolicy::default()),
            timeout: None,
            begin_gax_options: None,
            commit_gax_options: None,
        }
    }

    /// Sets the timeout for the entire transaction.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::Spanner;
    /// # use std::time::Duration;
    /// # async fn run(client: Spanner) -> Result<(), google_cloud_spanner::Error> {
    /// # let db_client = client.database_client("projects/p/instances/i/databases/d").build().await?;
    /// let runner = db_client.read_write_transaction()
    ///     .with_transaction_timeout(Duration::from_secs(5))
    ///     .build();
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// This timeout applies to the total time spent executing the transaction, including
    /// all statements and automatic retries. Each individual RPC within the transaction
    /// is automatically assigned a deadline derived from the remaining time of this
    /// overall timeout.
    pub fn with_transaction_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Sets the per-attempt timeout for the BeginTransaction RPC.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::Spanner;
    /// # use std::time::Duration;
    /// # async fn sample(spanner: Spanner) -> Result<(), google_cloud_spanner::Error> {
    /// let db_client = spanner.database_client("projects/p/instances/i/databases/d").build().await?;
    /// let runner = db_client.read_write_transaction()
    ///     .with_begin_attempt_timeout(Duration::from_secs(5))
    ///     .build();
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// Note: This timeout is only used if the transaction uses the `ExplicitBegin` transaction option.
    pub fn with_begin_attempt_timeout(mut self, timeout: Duration) -> Self {
        self.begin_gax_options
            .get_or_insert_with(crate::RequestOptions::default)
            .set_attempt_timeout(timeout);
        self
    }

    /// Sets the retry policy for the BeginTransaction RPC.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::Spanner;
    /// # use google_cloud_gax::retry_policy::NeverRetry;
    /// # async fn sample(spanner: Spanner) -> Result<(), google_cloud_spanner::Error> {
    /// let db_client = spanner.database_client("projects/p/instances/i/databases/d").build().await?;
    /// let runner = db_client.read_write_transaction()
    ///     .with_begin_retry_policy(NeverRetry)
    ///     .build();
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// Note: This policy is only used if the transaction uses the `ExplicitBegin` transaction option.
    pub fn with_begin_retry_policy(mut self, policy: impl Into<RetryPolicyArg>) -> Self {
        self.begin_gax_options
            .get_or_insert_with(crate::RequestOptions::default)
            .set_retry_policy(policy);
        self
    }

    /// Sets the backoff policy for the BeginTransaction RPC.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::Spanner;
    /// # use google_cloud_gax::exponential_backoff::ExponentialBackoff;
    /// # async fn sample(spanner: Spanner) -> Result<(), google_cloud_spanner::Error> {
    /// let db_client = spanner.database_client("projects/p/instances/i/databases/d").build().await?;
    /// let runner = db_client.read_write_transaction()
    ///     .with_begin_backoff_policy(ExponentialBackoff::default())
    ///     .build();
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// Note: This policy is only used if the transaction uses the `ExplicitBegin` transaction option.
    pub fn with_begin_backoff_policy(mut self, policy: impl Into<BackoffPolicyArg>) -> Self {
        self.begin_gax_options
            .get_or_insert_with(crate::RequestOptions::default)
            .set_backoff_policy(policy);
        self
    }

    /// Sets the per-attempt timeout for the Commit RPC.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::Spanner;
    /// # use std::time::Duration;
    /// # async fn sample(spanner: Spanner) -> Result<(), google_cloud_spanner::Error> {
    /// let db_client = spanner.database_client("projects/p/instances/i/databases/d").build().await?;
    /// let runner = db_client.read_write_transaction()
    ///     .with_commit_attempt_timeout(Duration::from_secs(5))
    ///     .build();
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_commit_attempt_timeout(mut self, timeout: Duration) -> Self {
        self.commit_gax_options
            .get_or_insert_with(crate::RequestOptions::default)
            .set_attempt_timeout(timeout);
        self
    }

    /// Sets the retry policy for the Commit RPC.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::Spanner;
    /// # use google_cloud_gax::retry_policy::NeverRetry;
    /// # async fn sample(spanner: Spanner) -> Result<(), google_cloud_spanner::Error> {
    /// let db_client = spanner.database_client("projects/p/instances/i/databases/d").build().await?;
    /// let runner = db_client.read_write_transaction()
    ///     .with_commit_retry_policy(NeverRetry)
    ///     .build();
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_commit_retry_policy(mut self, policy: impl Into<RetryPolicyArg>) -> Self {
        self.commit_gax_options
            .get_or_insert_with(crate::RequestOptions::default)
            .set_retry_policy(policy);
        self
    }

    /// Sets the backoff policy for the Commit RPC.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::Spanner;
    /// # use google_cloud_gax::exponential_backoff::ExponentialBackoff;
    /// # async fn sample(spanner: Spanner) -> Result<(), google_cloud_spanner::Error> {
    /// let db_client = spanner.database_client("projects/p/instances/i/databases/d").build().await?;
    /// let runner = db_client.read_write_transaction()
    ///     .with_commit_backoff_policy(ExponentialBackoff::default())
    ///     .build();
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_commit_backoff_policy(mut self, policy: impl Into<BackoffPolicyArg>) -> Self {
        self.commit_gax_options
            .get_or_insert_with(crate::RequestOptions::default)
            .set_backoff_policy(policy);
        self
    }

    /// Sets the isolation level for the transaction.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::Spanner;
    /// # use google_cloud_spanner::model::transaction_options::IsolationLevel;
    /// # async fn run(client: Spanner) -> Result<(), google_cloud_spanner::Error> {
    /// let db_client = client.database_client("projects/p/instances/i/databases/d").build().await?;
    /// let runner = db_client
    ///     .read_write_transaction()
    ///     .set_isolation_level(IsolationLevel::Serializable)
    ///     .build();
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// See also: <https://docs.cloud.google.com/spanner/docs/isolation-levels>
    pub fn set_isolation_level(mut self, isolation_level: IsolationLevel) -> Self {
        self.builder = self.builder.set_isolation_level(isolation_level);
        self
    }

    /// Sets the read lock mode for the transaction.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::Spanner;
    /// # use google_cloud_spanner::model::transaction_options::read_write::ReadLockMode;
    /// # async fn run(client: Spanner) -> Result<(), google_cloud_spanner::Error> {
    /// let db_client = client.database_client("projects/p/instances/i/databases/d").build().await?;
    /// let runner = db_client
    ///     .read_write_transaction()
    ///     .set_read_lock_mode(ReadLockMode::Pessimistic)
    ///     .build();
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// See also: <https://docs.cloud.google.com/spanner/docs/concurrency-control>
    pub fn set_read_lock_mode(mut self, read_lock_mode: ReadLockMode) -> Self {
        self.builder = self.builder.set_read_lock_mode(read_lock_mode);
        self
    }

    /// Sets the transaction tag for the transaction.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::Spanner;
    /// # async fn build_tx(spanner: Spanner) -> Result<(), google_cloud_spanner::Error> {
    /// let db_client = spanner.database_client("projects/p/instances/i/databases/d").build().await?;
    /// let runner = db_client.read_write_transaction()
    ///     .set_transaction_tag("my-tag")
    ///     .build();
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// The tag is applied to all statements executed within the transaction.
    ///
    /// See also: [Troubleshooting with tags](https://docs.cloud.google.com/spanner/docs/introspection/troubleshooting-with-tags)
    pub fn set_transaction_tag(mut self, tag: impl Into<String>) -> Self {
        self.builder = self.builder.set_transaction_tag(tag);
        self
    }

    /// Sets the option for how to start a transaction.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::Spanner;
    /// # use google_cloud_spanner::transaction::BeginTransactionOption;
    /// # async fn run(client: Spanner) -> Result<(), google_cloud_spanner::Error> {
    /// let db_client = client.database_client("projects/p/instances/i/databases/d").build().await?;
    /// let runner = db_client
    ///     .read_write_transaction()
    ///     .with_begin_transaction_option(BeginTransactionOption::ExplicitBegin)
    ///     .build();
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// By default, the Spanner client will inline the `BeginTransaction` call with the first query
    /// or DML statement in the transaction. This reduces the number of round-trips to Spanner that
    /// are needed for a transaction. Setting this option to `ExplicitBegin` can be beneficial for
    /// specific transaction shapes:
    ///
    /// 1. When the transaction executes multiple parallel queries at the start of the transaction.
    ///    Only one query can include a `BeginTransaction` option, and all other queries must wait for
    ///    the first query to return the first result before they can proceed to execute. A
    ///    `BeginTransaction` RPC will quickly return a transaction ID and allow all queries to start
    ///    execution in parallel once the transaction ID has been returned.
    /// 2. When the first statement in the transaction could fail. If the statement fails, then it
    ///    will not start a transaction and returns the error to the transaction closure. Subsequent
    ///    statements or commit in that attempt fail fast, and the transaction runner re-executes
    ///    the closure using an explicit `BeginTransaction` RPC.
    ///
    /// Default is `BeginTransactionOption::InlineBegin`.
    pub fn with_begin_transaction_option(mut self, option: BeginTransactionOption) -> Self {
        self.builder = self.builder.with_begin_transaction_option(option);
        self
    }

    /// Sets the RPC priority to use for the commit of this transaction.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::Spanner;
    /// # use google_cloud_spanner::model::request_options::Priority;
    /// # async fn run(client: Spanner) -> Result<(), google_cloud_spanner::Error> {
    /// let db_client = client.database_client("projects/p/instances/i/databases/d").build().await?;
    /// let runner = db_client
    ///     .read_write_transaction()
    ///     .set_commit_priority(Priority::Low)
    ///     .build();
    /// # Ok(())
    /// # }
    /// ```
    pub fn set_commit_priority(mut self, priority: Priority) -> Self {
        self.builder = self.builder.set_commit_priority(priority);
        self
    }

    /// Sets the maximum commit delay for the transaction.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::Spanner;
    /// # use std::time::Duration;
    /// # async fn run(client: Spanner) -> Result<(), google_cloud_spanner::Error> {
    /// let db_client = client.database_client("projects/p/instances/i/databases/d").build().await?;
    /// let runner = db_client
    ///     .read_write_transaction()
    ///     .set_max_commit_delay(Duration::from_millis(200))
    ///     .build();
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// This option allows you to specify the maximum amount of time Spanner can
    /// adjust the commit timestamp of the transaction to allow for commit batching.
    /// Increasing this value can increase throughput at the expense of latency.
    /// The value must be between 0 and 500 milliseconds. If not set, or set to 0,
    /// Spanner does not delay the commit.
    pub fn set_max_commit_delay(mut self, delay: Duration) -> Self {
        self.builder = self.builder.set_max_commit_delay(delay);
        self
    }

    /// Sets whether to exclude the transaction from change streams.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::Spanner;
    /// # async fn build_tx(spanner: Spanner) -> Result<(), google_cloud_spanner::Error> {
    /// let db_client = spanner.database_client("projects/p/instances/i/databases/d").build().await?;
    /// let runner = db_client.read_write_transaction()
    ///     .set_exclude_txn_from_change_streams(true)
    ///     .build();
    /// # Ok(())
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
    pub fn set_exclude_txn_from_change_streams(mut self, exclude: bool) -> Self {
        self.builder = self.builder.set_exclude_txn_from_change_streams(exclude);
        self
    }

    /// Sets whether to return commit stats for the transaction.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::Spanner;
    /// # use google_cloud_spanner::statement::Statement;
    /// # async fn run_tx(client: Spanner) -> Result<(), google_cloud_spanner::Error> {
    /// # let db_client = client.database_client("projects/p/instances/i/databases/d").build().await?;
    /// let runner = db_client.read_write_transaction()
    ///     .set_return_commit_stats(true)
    ///     .build();
    ///
    /// let result = runner.run(async |transaction| {
    ///     let statement = Statement::builder("UPDATE MyTable SET MyColumn = 'MyValue' WHERE Id = 1").build();
    ///     transaction.execute_update(statement).await?;
    ///     Ok(42)
    /// }).await?;
    ///
    /// if let Some(stats) = result.commit_response.commit_stats {
    ///     println!("Mutation count: {}", stats.mutation_count);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// See also: <https://docs.cloud.google.com/spanner/docs/commit-statistics>
    pub fn set_return_commit_stats(mut self, return_stats: bool) -> Self {
        self.builder = self.builder.set_return_commit_stats(return_stats);
        self
    }

    /// Sets the retry policy for the transaction.
    ///
    /// # Example
    /// ```
    /// # use std::time::Duration;
    /// # use google_cloud_spanner::client::Spanner;
    /// # use google_cloud_spanner::transaction::BasicTransactionRetryPolicy;
    /// # async fn run(client: Spanner) -> Result<(), google_cloud_spanner::Error> {
    /// let db_client = client.database_client("projects/p/instances/i/databases/d").build().await?;
    ///
    /// let retry_policy = BasicTransactionRetryPolicy::new()
    ///     .with_max_attempts(5)
    ///     .with_total_timeout(Duration::from_secs(60));
    ///
    /// let runner = db_client
    ///     .read_write_transaction()
    ///     .with_retry_policy(retry_policy)
    ///     .build();
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_retry_policy<P: TransactionRetryPolicy + 'static>(mut self, policy: P) -> Self {
        self.retry_policy = Box::new(policy);
        self
    }

    /// Builds a [TransactionRunner] for a read/write transaction.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::Spanner;
    /// # use google_cloud_spanner::statement::Statement;
    /// # async fn run(client: Spanner) -> Result<(), google_cloud_spanner::Error> {
    /// let db_client = client.database_client("projects/p/instances/i/databases/d").build().await?;
    /// let runner = db_client.read_write_transaction().build();
    ///
    /// let result = runner.run(async |transaction| {
    ///     let statement = Statement::builder("UPDATE MyTable SET MyColumn = 'MyValue' WHERE Id = 1").build();
    ///     transaction.execute_update(statement).await?;
    ///     Ok(42)
    /// }).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn build(self) -> TransactionRunner {
        TransactionRunner {
            builder: self
                .builder
                .with_begin_transaction_request_options(self.begin_gax_options)
                .with_commit_request_options(self.commit_gax_options),
            retry_policy: self.retry_policy,
            timeout: self.timeout,
        }
    }
}

/// Result of a read/write transaction executed by a [TransactionRunner].
///
/// Contains both the application-defined return value of the closure and the
/// [`CommitResponse`] returned by the Spanner service upon successful commit.
///
/// # Example
/// ```
/// # use google_cloud_spanner::client::Spanner;
/// # use google_cloud_spanner::statement::Statement;
/// # async fn example(client: Spanner) -> Result<(), google_cloud_spanner::Error> {
/// let db_client = client.database_client("projects/p/instances/i/databases/d").build().await?;
/// let runner = db_client.read_write_transaction().build();
///
/// let tx_result = runner
///     .run(async |transaction| {
///         let statement = Statement::builder("UPDATE Users SET Active = true WHERE Id = 1").build();
///         let updated_rows = transaction.execute_update(statement).await?;
///         Ok(updated_rows)
///     })
///     .await?;
///
/// println!("Updated {} rows", tx_result.result);
/// if let Some(timestamp) = tx_result.commit_timestamp() {
///     println!("Committed at: {timestamp:?}");
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub struct TransactionResult<T> {
    /// The result returned by the closure executed within the transaction.
    pub result: T,
    /// The response from the commit RPC.
    pub commit_response: CommitResponse,
}

impl<T> TransactionResult<T> {
    /// Creates a new `TransactionResult` with the given result and commit response.
    pub fn new(result: T, commit_response: CommitResponse) -> Self {
        Self {
            result,
            commit_response,
        }
    }

    /// Consumes the `TransactionResult`, returning the closure result.
    pub fn into_inner(self) -> T {
        self.result
    }

    /// Consumes the `TransactionResult`, returning both the closure result and commit response.
    pub fn into_parts(self) -> (T, CommitResponse) {
        (self.result, self.commit_response)
    }

    /// Returns the Cloud Spanner timestamp at which the transaction committed, if available.
    pub fn commit_timestamp(&self) -> Option<Timestamp> {
        self.commit_response.commit_timestamp
    }

    /// Returns the statistics about the commit, if requested and returned by Spanner.
    pub fn commit_stats(&self) -> Option<&CommitStats> {
        self.commit_response.commit_stats.as_ref()
    }
}

/// A runner for read/write transactions. Aborted transactions are automatically retried.
#[derive(Debug)]
pub struct TransactionRunner {
    builder: ReadWriteTransactionBuilder,
    retry_policy: Box<dyn TransactionRetryPolicy>,
    timeout: Option<Duration>,
}

impl TransactionRunner {
    /// Runs the provided closure within the context of a read/write transaction.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::Spanner;
    /// # use google_cloud_spanner::statement::Statement;
    /// # async fn run_transaction(spanner: Spanner) -> Result<(), google_cloud_spanner::Error> {
    /// let database_client = spanner
    ///     .database_client("projects/p/instances/i/databases/d")
    ///     .build()
    ///     .await?;
    /// let runner = database_client.read_write_transaction().build();
    ///
    /// let transaction_result = runner
    ///     .run(async |transaction| {
    ///         let statement = Statement::builder(
    ///             "UPDATE MyTable SET MyColumn = 'MyValue' WHERE Id = 1",
    ///         )
    ///         .build();
    ///         let updated_rows = transaction.execute_update(statement).await?;
    ///         Ok(updated_rows)
    ///     })
    ///     .await?;
    ///
    /// println!("Updated {} rows", transaction_result.result);
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// Returns a `TransactionResult` containing both the user-defined result value
    /// and the [`CommitResponse`][crate::model::CommitResponse] on success.
    ///
    /// The transaction is automatically committed if the closure returns `Ok`.
    /// If the closure returns `Err`, the transaction is rolled back and the error is propagated.
    ///
    /// If the transaction is aborted by Spanner due to concurrency contention, the closure
    /// is automatically retried according to the configured `TransactionRetryPolicy`.
    pub async fn run<T, F>(mut self, mut work: F) -> crate::Result<TransactionResult<T>>
    where
        F: std::ops::AsyncFnMut(ReadWriteTransaction) -> crate::Result<T>,
    {
        let start_time = Instant::now();
        let mut attempts: u32 = 0;
        let backoff = crate::transaction_retry_policy::default_retry_backoff();
        let deadline = self.timeout.map(|t| start_time + t);

        let mut force_explicit_begin = false;
        loop {
            attempts += 1;

            // Create a fresh affinity handle for each attempt. Each retry begins a new
            // Spanner transaction (with lock priority carried in the request payload via
            // `multiplexed_session_previous_transaction_id`, independent of the gRPC channel).
            // A per-attempt affinity keeps all RPCs within the attempt pinned to the same
            // channel, while allowing retries to avoid channels that began draining or
            // became degraded during the previous attempt.
            let channel_pool_affinity = Arc::new(TransactionAffinity::new_read_write());
            let mut current_tx_id = None;
            let attempt_result = async {
                let mut builder = self
                    .builder
                    .clone()
                    .with_affinity(Arc::clone(&channel_pool_affinity));
                if force_explicit_begin {
                    builder = builder
                        .with_begin_transaction_option(BeginTransactionOption::ExplicitBegin);
                }
                let transaction = builder.build(deadline).await.map_err(|e| (e, None))?;
                let selector = transaction.context.transaction_selector.clone();

                let result = match work(transaction.clone()).await {
                    Ok(res) => res,
                    Err(e) => {
                        // We call `get_id_no_wait` here to retrieve the transaction ID without waiting.
                        // We do not require the transaction ID to be unconditionally available here;
                        // we only wish to capture it if the transaction successfully started prior to
                        // failing, so it can be used as the previous transaction ID if the transaction
                        // was aborted.
                        let id = selector.get_id_no_wait().ok().flatten();
                        // Rollback if the closure failed and it was not an Aborted error.
                        // `rollback()` automatically clears location-aware routing affinity in its post-route hook.
                        // Aborted transactions are not rolled back, so we explicitly clear location-aware
                        // routing affinity here before entering retry backoff.
                        // Note: Channel pool affinity (`channel_pool_affinity`) is retained across retries
                        // to keep subsequent attempts pinned to the same physical gRPC connection.
                        if !is_aborted(&e) {
                            let _ = transaction.rollback().await;
                        } else {
                            self.builder
                                .client
                                .clear_transaction_affinity_routing(id.as_deref());
                        }
                        current_tx_id = id;
                        return Err((e, Some(selector)));
                    }
                };

                // `commit()` consumes `transaction`. If the commit RPC fails with an Aborted error,
                // we still need access to the transaction ID so we can provide it as `previous_transaction_id`
                // on retry. Cloning only `transaction_selector` preserves access to the internal state efficiently.
                let commit_result = transaction.commit().await;
                current_tx_id = selector.get_id_no_wait().ok().flatten();
                let commit_response = match commit_result {
                    Ok(r) => r,
                    Err(e) => return Err((e, Some(selector))),
                };
                Ok::<TransactionResult<T>, (crate::Error, Option<ReadContextTransactionSelector>)>(
                    TransactionResult {
                        result,
                        commit_response,
                    },
                )
            }
            .await;

            // Every exit path inside the `attempt_result` async block (`?` and `return`)
            // returns only from that inner block, so execution always reaches this point
            // before sleeping in `backoff_if_aborted` or returning from `run` (even when
            // `work` fails with `Aborted` and skips `rollback()`, or if the user closure
            // retains a cloned `ReadWriteTransaction`). If the `run` future itself is
            // cancelled/dropped mid-attempt, dropping `channel_pool_affinity` releases the guard via RAII.
            channel_pool_affinity.release_rw_guard();

            match attempt_result {
                Ok(res) => return Ok(res),
                Err((e, selector_opt)) => {
                    if is_aborted(&e) {
                        self.builder = self.builder.set_previous_transaction_id(current_tx_id);
                        if selector_opt
                            .as_ref()
                            .is_some_and(|s| s.is_first_statement_failed())
                        {
                            // When an inlined-begin statement fails with a non-retryable error, Spanner never
                            // started a transaction. If the closure handled or ignored that error and continued,
                            // subsequent operations or commit failed fast with a synthetic Aborted error.
                            // Switching to an explicit BeginTransaction RPC on retry ensures an active
                            // transaction ID exists before the closure runs, allowing subsequent statements
                            // to proceed inside a real transaction.
                            force_explicit_begin = true;
                        }
                    }

                    backoff_if_aborted(
                        e,
                        attempts,
                        start_time.elapsed(),
                        self.retry_policy.as_ref(),
                        &backoff,
                        self.builder.client.is_emulator(),
                    )
                    .await?;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Error;
    use crate::batch_dml::BatchDml;
    use crate::channel_pool::entry::{ChannelEntry, ChannelState};
    use crate::channel_pool::scaler::sweep_draining_channels;
    use crate::key::KeySet;
    use crate::mutation::Mutation;
    use crate::read::ReadRequest;
    use crate::read_only_transaction::tests::{create_session_mock, setup_db_client};
    use crate::result_set::tests::adapt;
    use crate::statement::Statement;
    use crate::transaction_retry_policy::tests::create_aborted_status;
    use gaxi::grpc::tonic;
    use gaxi::grpc::tonic::{Code, Response, Status};
    use google_cloud_gax::error::rpc::{Code as GaxCode, Status as GaxStatus};
    use google_cloud_gax::exponential_backoff::{ExponentialBackoff, ExponentialBackoffBuilder};
    use google_cloud_gax::retry_policy::NeverRetry;
    use google_cloud_test_macros::tokio_test_no_panics;
    use mockall::Sequence;
    use prost_types::Timestamp;
    use prost_types::Value as ProtoValue;
    use prost_types::value::Kind;
    use spanner_grpc_mock::MockSpanner;
    use spanner_grpc_mock::google::rpc::Status as RpcStatus;
    use spanner_grpc_mock::google::spanner::v1;
    use spanner_grpc_mock::google::spanner::v1::CommitResponse;
    use spanner_grpc_mock::google::spanner::v1::commit_request::Transaction as CommitTransaction;
    use spanner_grpc_mock::google::spanner::v1::commit_response::CommitStats;
    use spanner_grpc_mock::google::spanner::v1::mutation::Operation;
    use spanner_grpc_mock::google::spanner::v1::transaction_options::Mode;
    use spanner_grpc_mock::google::spanner::v1::transaction_selector::Selector as ProtoSelector;
    use std::fmt::Debug;
    use std::net::SocketAddr;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
    use std::sync::mpsc::channel as std_channel;
    use std::time::Duration as StdDuration;
    use tokio::spawn;
    use tokio::sync::Notify;
    use tokio::sync::mpsc::channel;
    use tokio::sync::oneshot::channel as oneshot_channel;
    use tokio::task::{JoinHandle, yield_now};

    fn expect_begin_transaction(mock: &mut MockSpanner, times: usize, transaction_id: Vec<u8>) {
        mock.expect_begin_transaction()
            .times(times)
            .returning(move |req| {
                let req = req.into_inner();
                assert_eq!(
                    req.session,
                    "projects/p/instances/i/databases/d/sessions/123"
                );
                Ok(tonic::Response::new(v1::Transaction {
                    id: transaction_id.clone(),
                    ..Default::default()
                }))
            });
    }

    async fn execute_test_runner(
        mock: MockSpanner,
        begin_transaction_option: BeginTransactionOption,
    ) -> Result<i64, crate::Error> {
        let (db_client, server) = setup_db_client(mock).await;
        let runner = TransactionRunnerBuilder::new(db_client)
            .with_begin_transaction_option(begin_transaction_option)
            .build();
        tokio::select! {
            res = runner.run(async |tx| {
                let count = tx.execute_update("UPDATE Users SET active = true").await?;
                Ok(count)
            }) => res.map(|r| r.result),
            err = server => panic!("Mock server panicked or terminated unexpectedly: {:?}", err),
        }
    }

    fn commit_response() -> Result<tonic::Response<v1::CommitResponse>, tonic::Status> {
        Ok(tonic::Response::new(v1::CommitResponse {
            commit_timestamp: Some(prost_types::Timestamp {
                seconds: 123456789,
                nanos: 0,
            }),
            ..Default::default()
        }))
    }

    fn row_count_exact_response(
        count: i64,
    ) -> Result<tonic::Response<v1::ResultSet>, tonic::Status> {
        Ok(tonic::Response::new(v1::ResultSet {
            stats: Some(v1::ResultSetStats {
                row_count: Some(v1::result_set_stats::RowCount::RowCountExact(count)),
                ..Default::default()
            }),
            ..Default::default()
        }))
    }

    #[test]
    fn auto_traits() {
        static_assertions::assert_impl_all!(TransactionRunnerBuilder: Debug, Send, Sync);
        static_assertions::assert_impl_all!(TransactionRunner: Debug, Send, Sync);
        static_assertions::assert_impl_all!(
            TransactionResult<String>: Clone,
            Debug,
            PartialEq,
            Send,
            Sync
        );
    }

    #[test]
    fn transaction_result_constructor_and_derives() {
        use crate::model::CommitResponse as ModelCommitResponse;
        use wkt::Timestamp;

        let commit_timestamp = Timestamp::clamp(1_234_567_890, 123);
        let commit_response = ModelCommitResponse::default().set_commit_timestamp(commit_timestamp);
        let transaction_result = TransactionResult::new(42_i64, commit_response.clone());
        assert_eq!(
            transaction_result.result, 42_i64,
            "expected result field to match input"
        );
        assert_eq!(
            transaction_result.commit_response, commit_response,
            "expected commit_response field to match input"
        );
        assert_eq!(
            transaction_result.commit_timestamp(),
            Some(commit_timestamp),
            "expected commit_timestamp to match commit_response"
        );
        assert_eq!(
            transaction_result.clone(),
            transaction_result,
            "expected clone to equal original"
        );
        let debug_representation = format!("{transaction_result:?}");
        assert!(
            debug_representation.contains("42"),
            "debug representation should contain result value"
        );

        let (unwrapped_result, unwrapped_response) = transaction_result.clone().into_parts();
        assert_eq!(
            unwrapped_result, 42_i64,
            "expected into_parts result to match"
        );
        assert_eq!(
            unwrapped_response, commit_response,
            "expected into_parts response to match"
        );
        assert_eq!(
            transaction_result.into_inner(),
            42_i64,
            "expected into_inner to return result value"
        );
    }

    #[tokio_test_no_panics]
    async fn execute_run_success_explicit() {
        run_success(BeginTransactionOption::ExplicitBegin).await;
    }

    #[tokio_test_no_panics]
    async fn execute_run_success_inline() {
        run_success(BeginTransactionOption::InlineBegin).await;
    }

    async fn run_success(begin_transaction_option: BeginTransactionOption) {
        let mut mock = create_session_mock();

        if begin_transaction_option == BeginTransactionOption::ExplicitBegin {
            expect_begin_transaction(&mut mock, 1, vec![1, 2, 3]);
        }

        mock.expect_execute_sql().once().returning(move |req| {
            let req = req.into_inner();
            assert_eq!(req.sql, "UPDATE Users SET active = true");
            assert_eq!(req.seqno, 1);

            if begin_transaction_option == BeginTransactionOption::InlineBegin {
                let transaction = req
                    .transaction
                    .as_ref()
                    .expect("transaction options required for inline begin");
                let selector = transaction.selector.as_ref().expect("selector required");
                assert!(
                    matches!(selector, v1::transaction_selector::Selector::Begin(_)),
                    "Expected Selector::Begin on attempt 1"
                );
            }

            let mut metadata = v1::ResultSetMetadata {
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
                req.transaction,
                Some(v1::commit_request::Transaction::TransactionId(vec![
                    1, 2, 3
                ]))
            );
            commit_response()
        });

        let res = execute_test_runner(mock, begin_transaction_option)
            .await
            .unwrap();
        assert_eq!(res, 1);
    }

    #[tokio_test_no_panics]
    async fn execute_run_success_with_commit_stats_explicit() {
        run_success_with_commit_stats(BeginTransactionOption::ExplicitBegin).await;
    }

    #[tokio_test_no_panics]
    async fn execute_run_success_with_commit_stats_inline() {
        run_success_with_commit_stats(BeginTransactionOption::InlineBegin).await;
    }

    async fn run_success_with_commit_stats(begin_transaction_option: BeginTransactionOption) {
        let mut mock = create_session_mock();

        if begin_transaction_option == BeginTransactionOption::ExplicitBegin {
            expect_begin_transaction(&mut mock, 1, vec![1, 2, 3]);
        }

        mock.expect_execute_sql().once().returning(move |req| {
            let req = req.into_inner();
            assert_eq!(req.sql, "UPDATE Users SET active = true");

            if begin_transaction_option == BeginTransactionOption::InlineBegin {
                let transaction = req
                    .transaction
                    .as_ref()
                    .expect("transaction options required for inline begin");
                let selector = transaction.selector.as_ref().expect("selector required");
                assert!(
                    matches!(selector, v1::transaction_selector::Selector::Begin(_)),
                    "Expected Selector::Begin on attempt 1"
                );
            }

            let mut metadata = v1::ResultSetMetadata {
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
            assert!(req.return_commit_stats);
            Ok(tonic::Response::new(CommitResponse {
                commit_timestamp: Some(prost_types::Timestamp {
                    seconds: 123456789,
                    nanos: 0,
                }),
                commit_stats: Some(CommitStats { mutation_count: 5 }),
                ..Default::default()
            }))
        });

        let (db_client, _server) = setup_db_client(mock).await;
        let runner = TransactionRunnerBuilder::new(db_client)
            .set_return_commit_stats(true)
            .with_begin_transaction_option(begin_transaction_option)
            .build();

        let res = runner
            .run(async |tx| {
                let count = tx.execute_update("UPDATE Users SET active = true").await?;
                Ok(count)
            })
            .await
            .unwrap();

        assert_eq!(res.result, 1);
        assert!(res.commit_response.commit_stats.is_some());
        assert_eq!(
            res.commit_response
                .commit_stats
                .expect("Commit stats should be present")
                .mutation_count,
            5
        );
    }

    #[tokio_test_no_panics]
    async fn execute_run_with_aborted_retry_explicit() -> anyhow::Result<()> {
        run_with_aborted_retry(BeginTransactionOption::ExplicitBegin).await
    }

    #[tokio_test_no_panics]
    async fn execute_run_with_aborted_retry_inline() -> anyhow::Result<()> {
        run_with_aborted_retry(BeginTransactionOption::InlineBegin).await
    }

    async fn run_with_aborted_retry(
        begin_transaction_option: BeginTransactionOption,
    ) -> anyhow::Result<()> {
        let mut mock = create_session_mock();
        let mut seq = mockall::Sequence::new();

        if begin_transaction_option == BeginTransactionOption::ExplicitBegin {
            mock.expect_begin_transaction()
                .once()
                .in_sequence(&mut seq)
                .returning(move |req| {
                    let req = req.into_inner();
                    assert_eq!(
                        req.session,
                        "projects/p/instances/i/databases/d/sessions/123"
                    );
                    Ok(tonic::Response::new(v1::Transaction {
                        id: vec![9, 9, 9],
                        ..Default::default()
                    }))
                });
        }

        if begin_transaction_option == BeginTransactionOption::InlineBegin {
            // Attempt 1: execute_sql fails with Aborted
            mock.expect_execute_sql()
                .once()
                .in_sequence(&mut seq)
                .returning(move |req| {
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

                    Err(create_aborted_status(std::time::Duration::from_nanos(1)))
                });
        } else {
            mock.expect_execute_sql()
                .once()
                .in_sequence(&mut seq)
                .returning(move |_req| {
                    Err(create_aborted_status(std::time::Duration::from_nanos(1)))
                });
        }

        if begin_transaction_option == BeginTransactionOption::ExplicitBegin {
            mock.expect_begin_transaction()
                .once()
                .in_sequence(&mut seq)
                .returning(move |req| {
                    let req = req.into_inner();
                    assert_eq!(req.session, "projects/p/instances/i/databases/d/sessions/123");

                    let options = req.options.as_ref().expect("options required on retry");
                    let read_write = options.mode.as_ref().expect("mode required on retry");
                    match read_write {
                        Mode::ReadWrite(rw) => {
                            assert_eq!(rw.multiplexed_session_previous_transaction_id, vec![9, 9, 9], "previous_transaction_id should be set to the ID of the aborted transaction");
                        }
                        _ => panic!("Expected ReadWrite mode"),
                    }

                    Ok(tonic::Response::new(v1::Transaction {
                        id: vec![8, 8, 8],
                        ..Default::default()
                    }))
                });
        }

        // Attempt 2 (retry of closure)
        mock.expect_execute_sql()
            .once()
            .in_sequence(&mut seq)
            .returning(move |req| {
                if begin_transaction_option == BeginTransactionOption::InlineBegin {
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

                    let options = match selector {
                        v1::transaction_selector::Selector::Begin(o) => o,
                        _ => panic!("Expected Begin"),
                    };
                    let read_write = options.mode.as_ref().expect("mode required");
                    match read_write {
                        Mode::ReadWrite(rw) => {
                            assert!(rw.multiplexed_session_previous_transaction_id.is_empty());
                        }
                        _ => panic!("Expected ReadWrite"),
                    }
                }

                let mut metadata = v1::ResultSetMetadata {
                    ..Default::default()
                };
                if begin_transaction_option == BeginTransactionOption::InlineBegin {
                    metadata.transaction = Some(v1::Transaction {
                        id: vec![8, 8, 8],
                        ..Default::default()
                    });
                }

                Ok(tonic::Response::new(v1::ResultSet {
                    metadata: Some(metadata),
                    stats: Some(v1::ResultSetStats {
                        row_count: Some(v1::result_set_stats::RowCount::RowCountExact(5)),
                        ..Default::default()
                    }),
                    ..Default::default()
                }))
            });

        mock.expect_commit()
            .once()
            .returning(|_req| commit_response());

        let res = execute_test_runner(mock, begin_transaction_option)
            .await
            .expect("runner should succeed");
        assert_eq!(res, 5);
        Ok(())
    }

    #[tokio_test_no_panics]
    async fn execute_run_query_stream_with_aborted_retry_explicit() -> anyhow::Result<()> {
        run_query_stream_with_aborted_retry(BeginTransactionOption::ExplicitBegin).await
    }

    #[tokio_test_no_panics]
    async fn execute_run_query_stream_with_aborted_retry_inline() -> anyhow::Result<()> {
        run_query_stream_with_aborted_retry(BeginTransactionOption::InlineBegin).await
    }

    async fn run_query_stream_with_aborted_retry(
        begin_transaction_option: BeginTransactionOption,
    ) -> anyhow::Result<()> {
        let mut mock = create_session_mock();
        let mut seq = mockall::Sequence::new();

        let tx_id_1 = vec![9, 9, 9];
        let tx_id_2 = vec![8, 8, 8];

        let tx_id_1_c1 = tx_id_1.clone();
        if begin_transaction_option == BeginTransactionOption::ExplicitBegin {
            mock.expect_begin_transaction()
                .once()
                .in_sequence(&mut seq)
                .returning(move |_| {
                    Ok(tonic::Response::new(v1::Transaction {
                        id: tx_id_1_c1.clone(),
                        ..Default::default()
                    }))
                });
        }

        let tx_id_1_c2 = tx_id_1.clone();
        mock.expect_execute_streaming_sql()
            .once()
            .in_sequence(&mut seq)
            .returning(move |req| {
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

                let mut rs = v1::PartialResultSet {
                    metadata: Some(v1::ResultSetMetadata {
                        row_type: Some(v1::StructType {
                            fields: vec![Default::default()],
                        }),
                        ..Default::default()
                    }),
                    values: vec![ProtoValue {
                        kind: Some(prost_types::value::Kind::StringValue("1".to_string())),
                    }],
                    resume_token: b"token1".to_vec(),
                    ..Default::default()
                };

                if begin_transaction_option == BeginTransactionOption::InlineBegin {
                    rs.metadata.as_mut().unwrap().transaction = Some(v1::Transaction {
                        id: tx_id_1_c2.clone(),
                        ..Default::default()
                    });
                }

                let (tx, rx) = channel(2);
                tx.try_send(Ok(rs)).expect("channel send should succeed");
                tx.try_send(Err(create_aborted_status(StdDuration::from_nanos(1))))
                    .expect("channel send should succeed");
                Ok(tonic::Response::from(rx))
            });

        let tx_id_2_c1 = tx_id_2.clone();
        if begin_transaction_option == BeginTransactionOption::ExplicitBegin {
            mock.expect_begin_transaction()
                .once()
                .in_sequence(&mut seq)
                .returning(move |req| {
                    let req = req.into_inner();
                    let options = req.options.as_ref().expect("options required on retry");
                    let read_write = options.mode.as_ref().expect("mode required on retry");
                    match read_write {
                        Mode::ReadWrite(rw) => {
                            assert_eq!(
                                rw.multiplexed_session_previous_transaction_id,
                                vec![9, 9, 9]
                            );
                        }
                        _ => panic!("Expected ReadWrite mode"),
                    }

                    Ok(tonic::Response::new(v1::Transaction {
                        id: tx_id_2_c1.clone(),
                        ..Default::default()
                    }))
                });
        }

        let tx_id_2_c2 = tx_id_2.clone();
        mock.expect_execute_streaming_sql()
            .once()
            .in_sequence(&mut seq)
            .returning(move |req| {
                if begin_transaction_option == BeginTransactionOption::InlineBegin {
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

                    let options = match selector {
                        v1::transaction_selector::Selector::Begin(o) => o,
                        _ => panic!("Expected Begin"),
                    };
                    let read_write = options.mode.as_ref().expect("mode required");
                    match read_write {
                        Mode::ReadWrite(rw) => {
                            assert_eq!(
                                rw.multiplexed_session_previous_transaction_id,
                                vec![9, 9, 9]
                            );
                        }
                        _ => panic!("Expected ReadWrite"),
                    }
                }

                let mut rs = v1::PartialResultSet {
                    metadata: Some(v1::ResultSetMetadata {
                        row_type: Some(v1::StructType {
                            fields: vec![Default::default()],
                        }),
                        ..Default::default()
                    }),
                    values: vec![ProtoValue {
                        kind: Some(prost_types::value::Kind::StringValue("1".to_string())),
                    }],
                    last: true,
                    ..Default::default()
                };

                if begin_transaction_option == BeginTransactionOption::InlineBegin {
                    rs.metadata.as_mut().unwrap().transaction = Some(v1::Transaction {
                        id: tx_id_2_c2.clone(),
                        ..Default::default()
                    });
                }

                let (tx, rx) = channel(2);
                tx.try_send(Ok(rs)).expect("channel send should succeed");
                Ok(tonic::Response::from(rx))
            });

        mock.expect_commit()
            .once()
            .returning(|_req| commit_response());

        let (db_client, _server) = setup_db_client(mock).await;
        let runner = TransactionRunnerBuilder::new(db_client)
            .with_begin_transaction_option(begin_transaction_option)
            .build();

        let mut attempt_counter = 0;
        let res = runner
            .run(async |tx| {
                attempt_counter += 1;
                let mut rs = tx.execute_query("SELECT 1").await?;
                let mut last_val = None;
                while let Some(row_res) = rs.next().await {
                    let row = row_res?;
                    last_val = Some(
                        row.raw_values()[0]
                            .as_str()
                            .expect("raw value should be string")
                            .to_string(),
                    );
                }
                Ok(last_val.expect("at least one row should have been returned"))
            })
            .await?;

        assert_eq!(res.result, "1");
        assert_eq!(attempt_counter, 2);
        Ok(())
    }

    #[tokio_test_no_panics]
    async fn execute_run_with_non_aborted_error_explicit() {
        run_with_non_aborted_error(BeginTransactionOption::ExplicitBegin).await;
    }

    #[tokio_test_no_panics]
    async fn execute_run_with_non_aborted_error_inline() {
        run_with_non_aborted_error(BeginTransactionOption::InlineBegin).await;
    }

    async fn run_with_non_aborted_error(begin_transaction_option: BeginTransactionOption) {
        let mut mock = create_session_mock();

        if begin_transaction_option == BeginTransactionOption::ExplicitBegin {
            expect_begin_transaction(&mut mock, 1, vec![9, 9, 9]);
        }

        // Let execute_sql return an error to trigger a rollback.
        mock.expect_execute_sql().once().returning(move |_req| {
            Err(tonic::Status::new(
                tonic::Code::PermissionDenied,
                "permission denied",
            ))
        });

        if begin_transaction_option == BeginTransactionOption::InlineBegin {
            expect_begin_transaction(&mut mock, 1, vec![9, 9, 9]);
            mock.expect_execute_sql().once().returning(move |_req| {
                Err(tonic::Status::new(
                    tonic::Code::PermissionDenied,
                    "permission denied",
                ))
            });
        }

        // Must explicitly trigger rollback
        mock.expect_rollback()
            .once()
            .returning(|_req| Ok(tonic::Response::new(())));

        let res = execute_test_runner(mock, begin_transaction_option).await;

        assert!(res.is_err());
        let err = res.unwrap_err();
        if let Some(status) = err.status() {
            assert_eq!(status.code, GaxCode::PermissionDenied);
        } else {
            panic!("Expected GRPC error");
        }
    }

    #[tokio_test_no_panics]
    async fn execute_run_with_non_aborted_error_and_rollback_fails_explicit() {
        run_with_non_aborted_error_and_rollback_fails(BeginTransactionOption::ExplicitBegin).await;
    }

    #[tokio_test_no_panics]
    async fn execute_run_with_non_aborted_error_and_rollback_fails_inline() {
        run_with_non_aborted_error_and_rollback_fails(BeginTransactionOption::InlineBegin).await;
    }

    async fn run_with_non_aborted_error_and_rollback_fails(
        begin_transaction_option: BeginTransactionOption,
    ) {
        let mut mock = create_session_mock();

        if begin_transaction_option == BeginTransactionOption::ExplicitBegin {
            expect_begin_transaction(&mut mock, 1, vec![9, 9, 9]);
        }

        // Let execute_sql return an error to trigger a rollback.
        mock.expect_execute_sql().once().returning(move |_req| {
            Err(tonic::Status::new(
                tonic::Code::PermissionDenied,
                "permission denied",
            ))
        });

        if begin_transaction_option == BeginTransactionOption::InlineBegin {
            expect_begin_transaction(&mut mock, 1, vec![9, 9, 9]);
            mock.expect_execute_sql().once().returning(move |_req| {
                Err(tonic::Status::new(
                    tonic::Code::PermissionDenied,
                    "permission denied",
                ))
            });
        }

        // Force the rollback itself to fail as well
        mock.expect_rollback()
            .once()
            .returning(|_req| Err(tonic::Status::new(tonic::Code::Internal, "rollback failed")));

        let res = execute_test_runner(mock, begin_transaction_option).await;

        // Verify the user unequivocally receives the PRIMARY original error
        assert!(res.is_err());
        let err = res.unwrap_err();
        if let Some(status) = err.status() {
            assert_eq!(status.code, GaxCode::PermissionDenied);
        } else {
            panic!("Expected GRPC error");
        }
    }

    #[tokio_test_no_panics]
    async fn execute_run_commit_aborted_retry_explicit() {
        run_commit_aborted_retry(BeginTransactionOption::ExplicitBegin).await;
    }

    #[tokio_test_no_panics]
    async fn execute_run_commit_aborted_retry_inline() {
        run_commit_aborted_retry(BeginTransactionOption::InlineBegin).await;
    }

    async fn run_commit_aborted_retry(begin_transaction_option: BeginTransactionOption) {
        let mut mock = create_session_mock();

        if begin_transaction_option == BeginTransactionOption::ExplicitBegin {
            expect_begin_transaction(&mut mock, 2, vec![9, 9, 9]);
        }

        let mut attempt = 0;
        mock.expect_execute_sql().times(2).returning(move |req| {
            if begin_transaction_option == BeginTransactionOption::InlineBegin {
                let req = req.into_inner();
                let transaction = req
                    .transaction
                    .as_ref()
                    .expect("transaction options required for inline begin");
                let selector = transaction.selector.as_ref().expect("selector required");
                assert!(
                    matches!(selector, v1::transaction_selector::Selector::Begin(_)),
                    "Expected Selector::Begin on attempt 1"
                );

                attempt += 1;
                if attempt == 2 {
                    let options = match selector {
                        v1::transaction_selector::Selector::Begin(o) => o,
                        _ => panic!("Expected Begin"),
                    };
                    let read_write = options.mode.as_ref().expect("mode required");
                    match read_write {
                        Mode::ReadWrite(rw) => {
                            assert_eq!(
                                rw.multiplexed_session_previous_transaction_id,
                                vec![9, 9, 9]
                            );
                        }
                        _ => panic!("Expected ReadWrite"),
                    }
                }

                let mut metadata = v1::ResultSetMetadata {
                    ..Default::default()
                };
                metadata.transaction = Some(v1::Transaction {
                    id: vec![9, 9, 9],
                    ..Default::default()
                });

                return Ok(tonic::Response::new(v1::ResultSet {
                    metadata: Some(metadata),
                    stats: Some(v1::ResultSetStats {
                        row_count: Some(v1::result_set_stats::RowCount::RowCountExact(5)),
                        ..Default::default()
                    }),
                    ..Default::default()
                }));
            }
            row_count_exact_response(5)
        });

        let mut commit_attempt = 0;
        mock.expect_commit().times(2).returning(move |_req| {
            commit_attempt += 1;
            if commit_attempt == 1 {
                Err(create_aborted_status(std::time::Duration::from_nanos(1)))
            } else {
                commit_response()
            }
        });

        let res = execute_test_runner(mock, begin_transaction_option)
            .await
            .unwrap();
        assert_eq!(res, 5);
    }

    #[tokio_test_no_panics]
    async fn execute_run_begin_transaction_fails_explicit() {
        run_begin_transaction_fails(BeginTransactionOption::ExplicitBegin).await;
    }

    #[tokio_test_no_panics]
    async fn execute_run_begin_transaction_fails_inline() {
        run_begin_transaction_fails(BeginTransactionOption::InlineBegin).await;
    }

    async fn run_begin_transaction_fails(begin_transaction_option: BeginTransactionOption) {
        let mut mock = create_session_mock();
        let mut seq = mockall::Sequence::new();

        if begin_transaction_option == BeginTransactionOption::ExplicitBegin {
            mock.expect_begin_transaction()
                .once()
                .returning(|_req| Err(tonic::Status::new(tonic::Code::Internal, "internal error")));
        } else {
            mock.expect_execute_sql()
                .once()
                .in_sequence(&mut seq)
                .returning(move |req| {
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

                    Err(tonic::Status::new(tonic::Code::Internal, "internal error"))
                });

            mock.expect_begin_transaction()
                .once()
                .in_sequence(&mut seq)
                .returning(|_req| Err(tonic::Status::new(tonic::Code::Internal, "internal error")));
        }

        let res = execute_test_runner(mock, begin_transaction_option).await;

        assert!(res.is_err());
        let err = res.unwrap_err();
        if let Some(status) = err.status() {
            assert_eq!(status.code, GaxCode::Internal);
        } else {
            panic!("Expected GRPC error");
        }
    }

    #[tokio_test_no_panics]
    async fn builder_options() {
        use crate::transaction_retry_policy::BasicTransactionRetryPolicy;

        let mock = create_session_mock();
        let (db_client, _server) = setup_db_client(mock).await;

        let retry_policy = BasicTransactionRetryPolicy::new()
            .with_max_attempts(1)
            .with_total_timeout(std::time::Duration::from_secs(10));

        // Validate builder chaining safely accepts and compiles options dynamically
        let _runner = TransactionRunnerBuilder::new(db_client)
            .set_isolation_level(IsolationLevel::Serializable)
            .set_read_lock_mode(ReadLockMode::Pessimistic)
            .with_retry_policy(retry_policy)
            .build();
    }

    #[tokio_test_no_panics]
    async fn execute_run_batch_dml_aborted_retry_explicit() {
        run_batch_dml_aborted_retry(BeginTransactionOption::ExplicitBegin).await;
    }

    #[tokio_test_no_panics]
    async fn execute_run_batch_dml_aborted_retry_inline() {
        run_batch_dml_aborted_retry(BeginTransactionOption::InlineBegin).await;
    }

    async fn run_batch_dml_aborted_retry(begin_transaction_option: BeginTransactionOption) {
        use crate::batch_dml::BatchDml;
        use crate::retry_delay::{ProtoRetryInfo, RETRY_INFO_TYPE_URL};
        use crate::statement::Statement;
        use gaxi::grpc::tonic::Code;
        use prost::Message;
        use prost_types::{Any, Duration as ProtoDuration};
        use spanner_grpc_mock::google::rpc::Status;
        use spanner_grpc_mock::google::spanner::v1::result_set_stats::RowCount;

        let mut mock = create_session_mock();

        if begin_transaction_option == BeginTransactionOption::ExplicitBegin {
            expect_begin_transaction(&mut mock, 2, vec![9, 9, 9]);
        }

        let mut seq = mockall::Sequence::new();
        mock.expect_execute_batch_dml()
            .once()
            .in_sequence(&mut seq)
            .returning(move |req| {
                if begin_transaction_option == BeginTransactionOption::InlineBegin {
                    let req = req.into_inner();
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

                // Return a successful response but with an embedded aborted status.
                let mut retry_bytes = Vec::new();
                ProtoRetryInfo {
                    retry_delay: Some(ProtoDuration {
                        seconds: 0,
                        nanos: 1,
                    }),
                }
                .encode(&mut retry_bytes)
                .expect("encoding ProtoRetryInfo should succeed");
                let status = Status {
                    code: Code::Aborted as i32,
                    message: "transaction aborted".to_string(),
                    details: vec![Any {
                        type_url: RETRY_INFO_TYPE_URL.to_string(),
                        value: retry_bytes,
                    }],
                };

                let mut metadata = v1::ResultSetMetadata {
                    ..Default::default()
                };
                if begin_transaction_option == BeginTransactionOption::InlineBegin {
                    metadata.transaction = Some(v1::Transaction {
                        id: vec![9, 9, 9],
                        ..Default::default()
                    });
                }

                Ok(tonic::Response::new(v1::ExecuteBatchDmlResponse {
                    result_sets: vec![v1::ResultSet {
                        metadata: Some(metadata),
                        stats: Some(v1::ResultSetStats {
                            row_count: Some(RowCount::RowCountExact(1)),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }],
                    status: Some(status),
                    ..Default::default()
                }))
            });
        mock.expect_execute_batch_dml()
            .once()
            .in_sequence(&mut seq)
            .returning(move |req| {
                if begin_transaction_option == BeginTransactionOption::InlineBegin {
                    let req = req.into_inner();
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
                        id: vec![9, 9, 9],
                        ..Default::default()
                    });
                }

                // Return success after the retry.
                Ok(tonic::Response::new(v1::ExecuteBatchDmlResponse {
                    result_sets: vec![v1::ResultSet {
                        metadata: Some(metadata),
                        stats: Some(v1::ResultSetStats {
                            row_count: Some(RowCount::RowCountExact(5)),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }],
                    ..Default::default()
                }))
            });

        mock.expect_commit()
            .once()
            .returning(move |_| commit_response());

        let (db_client, _) = setup_db_client(mock).await;
        let runner = TransactionRunnerBuilder::new(db_client)
            .with_begin_transaction_option(begin_transaction_option)
            .build();

        let mut attempt_counter = 0;

        // TransactionRunner retries the closure on transaction aborts
        let res = runner
            .run(async |tx| {
                attempt_counter += 1;
                let stmt = Statement::builder("UPDATE t SET c = 1").build();
                let batch = BatchDml::builder().add_statement(stmt).build();
                let counts = tx.execute_batch_update(batch).await?;
                Ok(counts)
            })
            .await
            .expect("transaction failed");

        assert_eq!(res.result, vec![5]);
        assert_eq!(attempt_counter, 2);
    }

    #[tokio_test_no_panics]
    async fn execute_run_with_transaction_tag_explicit() -> anyhow::Result<()> {
        run_with_transaction_tag(BeginTransactionOption::ExplicitBegin).await
    }

    #[tokio_test_no_panics]
    async fn execute_run_with_transaction_tag_inline() -> anyhow::Result<()> {
        run_with_transaction_tag(BeginTransactionOption::InlineBegin).await
    }

    async fn run_with_transaction_tag(
        begin_transaction_option: BeginTransactionOption,
    ) -> anyhow::Result<()> {
        let mut mock = create_session_mock();

        if begin_transaction_option == BeginTransactionOption::ExplicitBegin {
            mock.expect_begin_transaction().once().returning(|req| {
                let req = req.into_inner();
                // Check if the transaction tag is correctly propagated.
                assert_eq!(
                    req.request_options
                        .expect("Missing request_options")
                        .transaction_tag,
                    "my-test-tag"
                );

                Ok(tonic::Response::new(v1::Transaction {
                    id: vec![9, 9, 9],
                    ..Default::default()
                }))
            });
        }

        mock.expect_execute_sql().once().returning(move |req| {
            let req = req.into_inner();
            assert_eq!(
                req.request_options
                    .expect("Missing request_options")
                    .transaction_tag,
                "my-test-tag"
            );

            if begin_transaction_option == BeginTransactionOption::InlineBegin {
                let transaction = req
                    .transaction
                    .as_ref()
                    .expect("transaction options required for inline begin");
                let selector = transaction.selector.as_ref().expect("selector required");
                assert!(
                    matches!(selector, v1::transaction_selector::Selector::Begin(_)),
                    "Expected Selector::Begin on attempt 1"
                );
            }

            let mut metadata = v1::ResultSetMetadata {
                ..Default::default()
            };
            if begin_transaction_option == BeginTransactionOption::InlineBegin {
                metadata.transaction = Some(v1::Transaction {
                    id: vec![9, 9, 9],
                    ..Default::default()
                });
            }

            Ok(tonic::Response::new(v1::ResultSet {
                metadata: Some(metadata),
                stats: Some(v1::ResultSetStats {
                    row_count: Some(v1::result_set_stats::RowCount::RowCountExact(5)),
                    ..Default::default()
                }),
                ..Default::default()
            }))
        });

        mock.expect_commit().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(
                req.request_options
                    .expect("Missing request_options")
                    .transaction_tag,
                "my-test-tag"
            );
            commit_response()
        });

        let (db_client, _server) = setup_db_client(mock).await;

        let runner = TransactionRunnerBuilder::new(db_client)
            .with_begin_transaction_option(begin_transaction_option)
            .set_transaction_tag("my-test-tag")
            .build();

        let res = runner
            .run(async |tx| {
                let count = tx.execute_update("UPDATE Users SET active = true").await?;
                Ok(count)
            })
            .await?;

        assert_eq!(res.result, 5);

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn execute_run_with_exclude_txn_from_change_streams_explicit() -> anyhow::Result<()> {
        run_with_exclude_txn_from_change_streams(BeginTransactionOption::ExplicitBegin).await
    }

    #[tokio_test_no_panics]
    async fn execute_run_with_exclude_txn_from_change_streams_inline() -> anyhow::Result<()> {
        run_with_exclude_txn_from_change_streams(BeginTransactionOption::InlineBegin).await
    }

    async fn run_with_exclude_txn_from_change_streams(
        begin_transaction_option: BeginTransactionOption,
    ) -> anyhow::Result<()> {
        let mut mock = create_session_mock();

        if begin_transaction_option == BeginTransactionOption::ExplicitBegin {
            mock.expect_begin_transaction().once().returning(|req| {
                let req = req.into_inner();
                let options = req.options.expect("Missing transaction options");
                assert!(options.exclude_txn_from_change_streams);

                Ok(tonic::Response::new(v1::Transaction {
                    id: vec![9, 9, 9],
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
                assert!(
                    matches!(selector, v1::transaction_selector::Selector::Begin(_)),
                    "Expected Selector::Begin on attempt 1"
                );
            }

            let mut metadata = v1::ResultSetMetadata {
                ..Default::default()
            };
            if begin_transaction_option == BeginTransactionOption::InlineBegin {
                metadata.transaction = Some(v1::Transaction {
                    id: vec![9, 9, 9],
                    ..Default::default()
                });
            }

            Ok(tonic::Response::new(v1::ResultSet {
                metadata: Some(metadata),
                stats: Some(v1::ResultSetStats {
                    row_count: Some(v1::result_set_stats::RowCount::RowCountExact(5)),
                    ..Default::default()
                }),
                ..Default::default()
            }))
        });

        mock.expect_commit()
            .once()
            .returning(|_req| commit_response());

        let (db_client, _server) = setup_db_client(mock).await;

        let runner = TransactionRunnerBuilder::new(db_client)
            .set_exclude_txn_from_change_streams(true)
            .with_begin_transaction_option(begin_transaction_option)
            .build();

        let res = runner
            .run(async |tx| {
                let count = tx.execute_update("UPDATE Users SET active = true").await?;
                Ok(count)
            })
            .await?;

        assert_eq!(res.result, 5);

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn execute_run_with_max_commit_delay_explicit() -> anyhow::Result<()> {
        run_with_max_commit_delay(BeginTransactionOption::ExplicitBegin).await
    }

    #[tokio_test_no_panics]
    async fn execute_run_with_max_commit_delay_inline() -> anyhow::Result<()> {
        run_with_max_commit_delay(BeginTransactionOption::InlineBegin).await
    }

    async fn run_with_max_commit_delay(
        begin_transaction_option: BeginTransactionOption,
    ) -> anyhow::Result<()> {
        let mut mock = create_session_mock();

        if begin_transaction_option == BeginTransactionOption::ExplicitBegin {
            expect_begin_transaction(&mut mock, 1, vec![1, 2, 3]);
        }

        mock.expect_execute_sql().once().returning(move |req| {
            let req = req.into_inner();
            if begin_transaction_option == BeginTransactionOption::InlineBegin {
                let transaction = req
                    .transaction
                    .as_ref()
                    .expect("transaction options required for inline begin");
                let selector = transaction.selector.as_ref().expect("selector required");
                assert!(
                    matches!(selector, v1::transaction_selector::Selector::Begin(_)),
                    "Expected Selector::Begin on attempt 1"
                );
            }

            let mut metadata = v1::ResultSetMetadata {
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
                req.max_commit_delay,
                Some(::prost_types::Duration {
                    seconds: 0,
                    nanos: 200_000_000, // 200ms
                })
            );
            commit_response()
        });

        let (db_client, _server) = setup_db_client(mock).await;
        let runner = TransactionRunnerBuilder::new(db_client)
            .set_max_commit_delay(Duration::from_millis(200))
            .with_begin_transaction_option(begin_transaction_option)
            .build();

        let res = runner
            .run(async |tx| {
                let count = tx.execute_update("UPDATE Users SET active = true").await?;
                Ok(count)
            })
            .await?;
        assert_eq!(res.result, 1);
        Ok(())
    }

    #[tokio_test_no_panics]
    async fn execute_run_empty_closure_inline() {
        let mut mock = create_session_mock();
        expect_begin_transaction(&mut mock, 1, vec![1, 2, 3]);
        mock.expect_commit().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(
                req.transaction,
                Some(v1::commit_request::Transaction::TransactionId(vec![
                    1, 2, 3
                ]))
            );
            commit_response()
        });

        let (db_client, _server) = setup_db_client(mock).await;

        let runner = TransactionRunnerBuilder::new(db_client)
            .with_begin_transaction_option(BeginTransactionOption::InlineBegin)
            .build();

        let res = runner.run(async |_tx| Ok(42)).await.unwrap();
        assert_eq!(res.result, 42);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn execute_run_async_statement_still_starting() {
        let (tx_rpc, rx_rpc) = std_channel();
        let (tx_started, rx_started) = oneshot_channel();
        let tx_started_mutex = Mutex::new(Some(tx_started));

        let mut mock = create_session_mock();

        mock.expect_execute_sql().once().returning(move |_req| {
            if let Some(tx) = tx_started_mutex.lock().unwrap().take() {
                let _ = tx.send(());
            }
            rx_rpc.recv().unwrap();
            row_count_exact_response(1)
        });

        let (db_client, _server) = setup_db_client(mock).await;

        let runner = TransactionRunnerBuilder::new(db_client)
            .with_begin_transaction_option(BeginTransactionOption::InlineBegin)
            .build();

        let mut rx_started_opt = Some(rx_started);
        let res = runner
            .run(async |tx| {
                spawn(async move {
                    let _ = tx.execute_update("UPDATE Users SET active = true").await;
                });
                if let Some(rx) = rx_started_opt.take() {
                    rx.await.unwrap();
                }
                Ok(42)
            })
            .await;

        tx_rpc.send(()).unwrap();

        assert!(res.is_err());
        assert!(
            format!("{:?}", res.unwrap_err())
                .contains("asynchronous statement is still starting the transaction")
        );
    }

    #[tokio_test_no_panics]
    async fn execute_run_with_mutations_happy_flow() {
        let mut mock = create_session_mock();

        mock.expect_execute_sql().once().returning(move |req| {
            let req = req.into_inner();
            assert_eq!(req.sql, "UPDATE Users SET active = true");
            let transaction = req
                .transaction
                .as_ref()
                .expect("transaction options required for inline begin");
            let selector = transaction.selector.as_ref().expect("selector required");
            assert!(matches!(selector, ProtoSelector::Begin(_)));

            Ok(tonic::Response::new(v1::ResultSet {
                metadata: Some(v1::ResultSetMetadata {
                    transaction: Some(v1::Transaction {
                        id: vec![1, 1, 1],
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
                req.transaction,
                Some(CommitTransaction::TransactionId(vec![1, 1, 1]))
            );
            assert_eq!(req.mutations.len(), 1);
            commit_response()
        });

        let (db_client, _server) = setup_db_client(mock).await;
        let runner = TransactionRunnerBuilder::new(db_client)
            .with_begin_transaction_option(BeginTransactionOption::InlineBegin)
            .build();

        let res = runner
            .run(async |tx| {
                let count = tx.execute_update("UPDATE Users SET active = true").await?;
                let mutation = Mutation::new_insert_builder("Audits")
                    .set("AuditId")
                    .to(1)
                    .build();
                tx.buffer([mutation])?;
                Ok(count)
            })
            .await
            .expect("Transaction runner failed");

        assert_eq!(res.result, 1);
    }

    #[tokio_test_no_panics]
    async fn execute_run_with_mutations_aborted_retry() {
        let mut mock = create_session_mock();
        let mut sequence = mockall::Sequence::new();

        // Initial attempt: statement succeeds, returns tx id [10, 20, 30]
        mock.expect_execute_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |_req| {
                Ok(tonic::Response::new(v1::ResultSet {
                    metadata: Some(v1::ResultSetMetadata {
                        transaction: Some(v1::Transaction {
                            id: vec![10, 20, 30],
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

        // Initial commit fails with Aborted
        mock.expect_commit()
            .once()
            .in_sequence(&mut sequence)
            .returning(|req| {
                let req = req.into_inner();
                assert_eq!(req.mutations.len(), 1);
                // Verify initial attempt added mutation for UserId 100
                let write = req.mutations[0]
                    .operation
                    .as_ref()
                    .expect("Operation required");
                match write {
                    Operation::Insert(w) => {
                        assert_eq!(
                            w.values[0].values[0].kind,
                            Some(Kind::StringValue("100".to_string()))
                        );
                    }
                    _ => panic!("Expected insert mutation"),
                }
                Err(create_aborted_status(StdDuration::from_nanos(1)))
            });

        // Retry attempt: execute_sql sends inline BeginTransaction with previous_transaction_id
        mock.expect_execute_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |req| {
                let req = req.into_inner();
                let transaction = req
                    .transaction
                    .as_ref()
                    .expect("transaction options required for inline begin");
                let selector = transaction.selector.as_ref().expect("selector required");
                let options = match selector {
                    ProtoSelector::Begin(o) => o,
                    _ => panic!("Expected Begin"),
                };
                let read_write = options.mode.as_ref().expect("mode required");
                match read_write {
                    Mode::ReadWrite(rw) => {
                        assert_eq!(
                            rw.multiplexed_session_previous_transaction_id,
                            vec![10, 20, 30]
                        );
                    }
                    _ => panic!("Expected ReadWrite"),
                }

                Ok(tonic::Response::new(v1::ResultSet {
                    metadata: Some(v1::ResultSetMetadata {
                        transaction: Some(v1::Transaction {
                            id: vec![99, 99, 99],
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

        // Second commit succeeds with the new mutation
        mock.expect_commit()
            .once()
            .in_sequence(&mut sequence)
            .returning(|req| {
                let req = req.into_inner();
                assert_eq!(
                    req.transaction,
                    Some(CommitTransaction::TransactionId(vec![99, 99, 99]))
                );
                assert_eq!(req.mutations.len(), 1);
                // Verify retry attempt added mutation for UserId 200
                let write = req.mutations[0]
                    .operation
                    .as_ref()
                    .expect("Operation required");
                match write {
                    Operation::Insert(w) => {
                        assert_eq!(
                            w.values[0].values[0].kind,
                            Some(Kind::StringValue("200".to_string()))
                        );
                    }
                    _ => panic!("Expected insert mutation"),
                }
                commit_response()
            });

        let (db_client, _server) = setup_db_client(mock).await;
        let runner = TransactionRunnerBuilder::new(db_client)
            .with_begin_transaction_option(BeginTransactionOption::InlineBegin)
            .build();

        let mut attempt = 0;
        let res = runner
            .run(async |tx| {
                attempt += 1;
                let count = tx.execute_update("UPDATE Users SET active = true").await?;
                let mutation_value = if attempt == 1 { 100 } else { 200 };
                let mutation = Mutation::new_insert_builder("Users")
                    .set("UserId")
                    .to(mutation_value)
                    .build();
                tx.buffer([mutation])?;
                Ok(count)
            })
            .await
            .expect("Transaction runner failed");

        assert_eq!(res.result, 1);
    }

    #[tokio_test_no_panics]
    async fn execute_run_mutation_only_explicit_begin_fallback() {
        let mut mock = create_session_mock();

        // Since the user closure executes no statements, commit() calls explicit BeginTransaction
        mock.expect_begin_transaction().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(
                req.session,
                "projects/p/instances/i/databases/d/sessions/123"
            );
            Ok(tonic::Response::new(v1::Transaction {
                id: vec![77, 88, 99],
                ..Default::default()
            }))
        });

        mock.expect_commit().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(
                req.transaction,
                Some(CommitTransaction::TransactionId(vec![77, 88, 99]))
            );
            assert_eq!(req.mutations.len(), 2);
            commit_response()
        });

        let (db_client, _server) = setup_db_client(mock).await;
        let runner = TransactionRunnerBuilder::new(db_client)
            .with_begin_transaction_option(BeginTransactionOption::InlineBegin)
            .build();

        let res = runner
            .run(async |tx| {
                let m1 = Mutation::new_insert_builder("Orders")
                    .set("OrderId")
                    .to(1)
                    .build();
                let m2 = Mutation::new_insert_builder("Orders")
                    .set("OrderId")
                    .to(2)
                    .build();
                tx.buffer([m1, m2])?;
                Ok(())
            })
            .await
            .expect("Transaction runner failed");

        assert_eq!(
            res.commit_response
                .commit_timestamp
                .expect("Timestamp required")
                .seconds(),
            123456789
        );
    }

    #[tokio_test_no_panics]
    async fn read_write_transaction_builder_sets_gax_options() -> anyhow::Result<()> {
        let mock = create_session_mock();
        let (db_client, _server) = setup_db_client(mock).await;

        let runner = TransactionRunnerBuilder::new(db_client)
            .with_begin_attempt_timeout(StdDuration::from_secs(5))
            .with_begin_retry_policy(NeverRetry)
            .with_begin_backoff_policy(ExponentialBackoff::default())
            .with_commit_attempt_timeout(StdDuration::from_secs(10))
            .with_commit_retry_policy(NeverRetry)
            .with_commit_backoff_policy(ExponentialBackoff::default());

        let begin_gax = runner
            .begin_gax_options
            .as_ref()
            .expect("begin_gax_options missing");
        assert_eq!(
            *begin_gax.attempt_timeout(),
            Some(StdDuration::from_secs(5))
        );
        assert!(begin_gax.retry_policy().is_some());
        assert!(begin_gax.backoff_policy().is_some());

        let commit_gax = runner
            .commit_gax_options
            .as_ref()
            .expect("commit_gax_options missing");
        assert_eq!(
            *commit_gax.attempt_timeout(),
            Some(StdDuration::from_secs(10))
        );
        assert!(commit_gax.retry_policy().is_some());
        assert!(commit_gax.backoff_policy().is_some());

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn transaction_runner_creates_fresh_affinity_for_each_aborted_retry() -> anyhow::Result<()>
    {
        let mut mock = create_session_mock();
        mock.expect_begin_transaction().once().returning(|_| {
            Ok(Response::new(v1::Transaction {
                id: vec![77, 88, 99],
                ..Default::default()
            }))
        });
        mock.expect_commit().once().returning(|_| commit_response());

        let (db_client, _server) = setup_db_client(mock).await;

        let runner = db_client.read_write_transaction().build();
        let attempt_count = Arc::new(AtomicU32::new(0));
        let captured_affinity_ids = Arc::new(Mutex::new(Vec::new()));

        let attempt_count_clone = Arc::clone(&attempt_count);
        let captured_affinity_ids_clone = Arc::clone(&captured_affinity_ids);

        let result = runner
            .run(|transaction: ReadWriteTransaction| {
                let attempts = Arc::clone(&attempt_count_clone);
                let captured_ids = Arc::clone(&captured_affinity_ids_clone);
                async move {
                    let current = attempts.fetch_add(1, Ordering::Relaxed);
                    if current == 0 {
                        // First attempt: simulate pinning affinity to channel ID 17, then aborting
                        transaction
                            .affinity()
                            .expect("affinity present")
                            .set_entry_id(17);
                        captured_ids.lock().expect("mutex lock").push(
                            transaction
                                .affinity()
                                .expect("affinity present")
                                .pinned_entry_id(),
                        );

                        let aborted_status = GaxStatus::default()
                            .set_code(GaxCode::Aborted)
                            .set_message("Transaction aborted");
                        Err(Error::service(aborted_status))
                    } else {
                        // Second attempt: verify that the new attempt starts with an unpinned affinity handle
                        captured_ids.lock().expect("mutex lock").push(
                            transaction
                                .affinity()
                                .expect("affinity present")
                                .pinned_entry_id(),
                        );
                        Ok(100)
                    }
                }
            })
            .await;

        assert!(result.is_ok(), "Transaction runner should succeed on retry");
        assert_eq!(
            attempt_count.load(Ordering::Relaxed),
            2,
            "Runner should have executed 2 attempts"
        );
        let captured_ids = captured_affinity_ids.lock().expect("mutex lock");
        assert_eq!(
            *captured_ids,
            vec![Some(17), None],
            "Retried attempt must start with a fresh, unpinned channel affinity handle"
        );

        Ok(())
    }

    async fn setup_db_client_with_dynamic_pool(
        mock: MockSpanner,
        initial_channels: usize,
        max_channels: usize,
    ) -> (DatabaseClient, JoinHandle<()>) {
        use crate::channel_pool::DynamicChannelPoolConfig;
        use crate::client::{Spanner, SpannerBuilderExt};
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

        (database_client, server)
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
                "RPC at index {index} must use the same channel ({first_address})"
            );
        }
    }

    #[tokio_test_no_panics]
    async fn transaction_runner_aborted_retry_routes_statements_within_each_attempt_to_same_channel()
    -> anyhow::Result<()> {
        let mut mock = create_session_mock();
        let remote_addresses = Arc::new(Mutex::new(Vec::new()));

        // Attempt 1: Statement 1 (ExecuteStreamingSql) succeeds
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

        // Attempt 1: Statement 2 (ExecuteSql) fails with Aborted
        let remote_addresses_clone = Arc::clone(&remote_addresses);
        mock.expect_execute_sql().once().returning(move |request| {
            remote_addresses_clone.lock().expect("mutex lock").push(
                request
                    .remote_addr()
                    .expect("remote_addr should be available"),
            );
            Err(create_aborted_status(StdDuration::from_nanos(1)))
        });

        // Attempt 2: Statement 1 (ExecuteStreamingSql) succeeds
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
                        id: vec![4, 5, 6],
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

        // Attempt 2: Statement 2 (ExecuteSql) succeeds
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

        // Attempt 2: Commit succeeds
        let remote_addresses_clone = Arc::clone(&remote_addresses);
        mock.expect_commit().once().returning(move |request| {
            remote_addresses_clone.lock().expect("mutex lock").push(
                request
                    .remote_addr()
                    .expect("remote_addr should be available"),
            );
            Ok(Response::new(CommitResponse {
                commit_timestamp: Some(Timestamp {
                    seconds: 2000,
                    nanos: 0,
                }),
                ..Default::default()
            }))
        });

        let (database_client, _server) = setup_db_client_with_dynamic_pool(mock, 4, 8).await;

        let runner = database_client.read_write_transaction().build();
        let result = runner
            .run(|transaction: ReadWriteTransaction| async move {
                let mut result_set = transaction
                    .execute_query(Statement::builder("SELECT 1").build())
                    .await?;
                let _ = result_set.next().await.transpose()?;
                let count = transaction
                    .execute_update("UPDATE Users SET Name = 'Alice' WHERE Id = 1")
                    .await?;
                Ok(count)
            })
            .await?;

        assert_eq!(result.result, 1, "Expected update count of 1 on retry");

        let addresses = remote_addresses.lock().expect("mutex lock");
        assert_eq!(
            addresses.len(),
            5,
            "Expected 5 total RPCs across 2 attempts"
        );
        assert_all_rpcs_use_same_channel(&addresses[0..2], 2);
        assert_all_rpcs_use_same_channel(&addresses[2..5], 3);

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn transaction_runner_statement_transparent_retry_uses_same_channel() -> anyhow::Result<()>
    {
        let mut mock = create_session_mock();
        let remote_addresses = Arc::new(Mutex::new(Vec::new()));
        let closure_invocations = Arc::new(AtomicUsize::new(0));

        // Statement 1: ExecuteStreamingSql succeeds
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

        // Statement 2: ExecuteSql attempt 1 fails with UNAVAILABLE
        let remote_addresses_clone = Arc::clone(&remote_addresses);
        mock.expect_execute_sql().once().returning(move |request| {
            remote_addresses_clone.lock().expect("mutex lock").push(
                request
                    .remote_addr()
                    .expect("remote_addr should be available"),
            );
            Err(Status::new(Code::Unavailable, "Transient network failure"))
        });

        // Statement 2: ExecuteSql attempt 2 (transparent retry by GAX) succeeds
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

        // Commit succeeds
        let remote_addresses_clone = Arc::clone(&remote_addresses);
        mock.expect_commit().once().returning(move |request| {
            remote_addresses_clone.lock().expect("mutex lock").push(
                request
                    .remote_addr()
                    .expect("remote_addr should be available"),
            );
            Ok(Response::new(CommitResponse {
                commit_timestamp: Some(Timestamp {
                    seconds: 2000,
                    nanos: 0,
                }),
                ..Default::default()
            }))
        });

        let (database_client, _server) = setup_db_client_with_dynamic_pool(mock, 4, 8).await;

        let runner = database_client.read_write_transaction().build();
        let closure_invocations_clone = Arc::clone(&closure_invocations);
        let result = runner
            .run(|transaction: ReadWriteTransaction| {
                let closure_invocations_clone = Arc::clone(&closure_invocations_clone);
                async move {
                    closure_invocations_clone.fetch_add(1, Ordering::SeqCst);
                    let mut result_set = transaction
                        .execute_query(Statement::builder("SELECT 1").build())
                        .await?;
                    let _ = result_set.next().await.transpose()?;
                    let update_statement =
                        Statement::builder("UPDATE Users SET Name = 'Alice' WHERE Id = 1")
                            .with_backoff_policy(
                                ExponentialBackoffBuilder::new()
                                    .with_initial_delay(StdDuration::from_nanos(1))
                                    .clamp(),
                            )
                            .build();
                    let count = transaction.execute_update(update_statement).await?;
                    Ok(count)
                }
            })
            .await?;

        assert_eq!(
            result.result, 1,
            "Expected update count of 1 on successful retry"
        );
        assert_eq!(
            closure_invocations.load(Ordering::SeqCst),
            1,
            "Application closure should execute exactly once because UNAVAILABLE is retried transparently by GAX"
        );

        let addresses = remote_addresses.lock().expect("mutex lock");
        assert_all_rpcs_use_same_channel(&addresses, 4);

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn transaction_runner_query_stream_transparent_retry_uses_same_channel()
    -> anyhow::Result<()> {
        let mut mock = create_session_mock();
        let remote_addresses = Arc::new(Mutex::new(Vec::new()));
        let closure_invocations = Arc::new(AtomicUsize::new(0));

        // Statement 1: ExecuteStreamingSql attempt 1 yields UNAVAILABLE
        let remote_addresses_clone = Arc::clone(&remote_addresses);
        mock.expect_execute_streaming_sql()
            .once()
            .returning(move |request| {
                remote_addresses_clone.lock().expect("mutex lock").push(
                    request
                        .remote_addr()
                        .expect("remote_addr should be available"),
                );
                Ok(Response::from(adapt([Err(Status::new(
                    Code::Unavailable,
                    "Transient stream disconnect",
                ))])))
            });

        // Statement 1: ExecuteStreamingSql attempt 2 (stream restart) succeeds
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

        // Statement 2: ExecuteSql succeeds
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

        // Commit succeeds
        let remote_addresses_clone = Arc::clone(&remote_addresses);
        mock.expect_commit().once().returning(move |request| {
            remote_addresses_clone.lock().expect("mutex lock").push(
                request
                    .remote_addr()
                    .expect("remote_addr should be available"),
            );
            Ok(Response::new(CommitResponse {
                commit_timestamp: Some(Timestamp {
                    seconds: 2000,
                    nanos: 0,
                }),
                ..Default::default()
            }))
        });

        let (database_client, _server) = setup_db_client_with_dynamic_pool(mock, 4, 8).await;

        let runner = database_client.read_write_transaction().build();
        let closure_invocations_clone = Arc::clone(&closure_invocations);
        let result = runner
            .run(|transaction: ReadWriteTransaction| {
                let closure_invocations_clone = Arc::clone(&closure_invocations_clone);
                async move {
                    closure_invocations_clone.fetch_add(1, Ordering::SeqCst);
                    let query_statement = Statement::builder("SELECT 1")
                        .with_backoff_policy(
                            ExponentialBackoffBuilder::new()
                                .with_initial_delay(StdDuration::from_nanos(1))
                                .clamp(),
                        )
                        .build();
                    let mut result_set = transaction.execute_query(query_statement).await?;
                    let _ = result_set.next().await.transpose()?;
                    let count = transaction
                        .execute_update("UPDATE Users SET Name = 'Alice' WHERE Id = 1")
                        .await?;
                    Ok(count)
                }
            })
            .await?;

        assert_eq!(result.result, 1, "Expected update count of 1");
        assert_eq!(
            closure_invocations.load(Ordering::SeqCst),
            1,
            "Application closure should execute exactly once because query stream resumed transparently"
        );

        let addresses = remote_addresses.lock().expect("mutex lock");
        assert_all_rpcs_use_same_channel(&addresses, 4);

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn transaction_runner_commit_aborted_retry_routes_statements_within_each_attempt_to_same_channel()
    -> anyhow::Result<()> {
        let mut mock = create_session_mock();
        let remote_addresses = Arc::new(Mutex::new(Vec::new()));
        let closure_invocations = Arc::new(AtomicUsize::new(0));

        // Attempt 1: Statement 1 (ExecuteStreamingSql) succeeds
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

        // Attempt 1: Statement 2 (ExecuteSql) succeeds
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

        // Attempt 1: Commit fails with Aborted
        let remote_addresses_clone = Arc::clone(&remote_addresses);
        mock.expect_commit().once().returning(move |request| {
            remote_addresses_clone.lock().expect("mutex lock").push(
                request
                    .remote_addr()
                    .expect("remote_addr should be available"),
            );
            Err(Status::new(
                Code::Aborted,
                "Transaction was aborted during commit",
            ))
        });

        // Attempt 2: Statement 1 (ExecuteStreamingSql) succeeds
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
                        id: vec![4, 5, 6],
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

        // Attempt 2: Statement 2 (ExecuteSql) succeeds
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

        // Attempt 2: Commit succeeds
        let remote_addresses_clone = Arc::clone(&remote_addresses);
        mock.expect_commit().once().returning(move |request| {
            remote_addresses_clone.lock().expect("mutex lock").push(
                request
                    .remote_addr()
                    .expect("remote_addr should be available"),
            );
            Ok(Response::new(CommitResponse {
                commit_timestamp: Some(Timestamp {
                    seconds: 2000,
                    nanos: 0,
                }),
                ..Default::default()
            }))
        });

        let (database_client, _server) = setup_db_client_with_dynamic_pool(mock, 4, 8).await;

        let runner = database_client.read_write_transaction().build();
        let closure_invocations_clone = Arc::clone(&closure_invocations);
        let result = runner
            .run(|transaction: ReadWriteTransaction| {
                let closure_invocations_clone = Arc::clone(&closure_invocations_clone);
                async move {
                    closure_invocations_clone.fetch_add(1, Ordering::SeqCst);
                    let mut result_set = transaction
                        .execute_query(Statement::builder("SELECT 1").build())
                        .await?;
                    let _ = result_set.next().await.transpose()?;
                    let count = transaction
                        .execute_update("UPDATE Users SET Name = 'Alice' WHERE Id = 1")
                        .await?;
                    Ok(count)
                }
            })
            .await?;

        assert_eq!(result.result, 1, "Expected update count of 1 on retry");
        assert_eq!(
            closure_invocations.load(Ordering::SeqCst),
            2,
            "Expected closure to be invoked twice due to commit abort"
        );

        let addresses = remote_addresses.lock().expect("mutex lock");
        assert_eq!(
            addresses.len(),
            6,
            "Expected 6 total RPCs across 2 attempts"
        );
        assert_all_rpcs_use_same_channel(&addresses[0..3], 3);
        assert_all_rpcs_use_same_channel(&addresses[3..6], 3);

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn transaction_runner_multiple_aborted_retries_route_statements_within_each_attempt_to_same_channel()
    -> anyhow::Result<()> {
        let mut mock = create_session_mock();
        let remote_addresses = Arc::new(Mutex::new(Vec::new()));
        let closure_invocations = Arc::new(AtomicUsize::new(0));

        // Attempt 1: Statement 1 (ExecuteStreamingSql) succeeds
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
                        id: vec![1, 1, 1],
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

        // Attempt 1: Statement 2 (ExecuteSql) fails with Aborted
        let remote_addresses_clone = Arc::clone(&remote_addresses);
        mock.expect_execute_sql().once().returning(move |request| {
            remote_addresses_clone.lock().expect("mutex lock").push(
                request
                    .remote_addr()
                    .expect("remote_addr should be available"),
            );
            Err(Status::new(
                Code::Aborted,
                "Transaction aborted on attempt 1",
            ))
        });

        // Attempt 2: Statement 1 (ExecuteStreamingSql) succeeds
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
                        id: vec![2, 2, 2],
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

        // Attempt 2: Statement 2 (ExecuteSql) succeeds
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

        // Attempt 2: Commit fails with Aborted
        let remote_addresses_clone = Arc::clone(&remote_addresses);
        mock.expect_commit().once().returning(move |request| {
            remote_addresses_clone.lock().expect("mutex lock").push(
                request
                    .remote_addr()
                    .expect("remote_addr should be available"),
            );
            Err(Status::new(
                Code::Aborted,
                "Transaction aborted on attempt 2 commit",
            ))
        });

        // Attempt 3: Statement 1 (ExecuteStreamingSql) succeeds
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
                        id: vec![3, 3, 3],
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

        // Attempt 3: Statement 2 (ExecuteSql) succeeds
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

        // Attempt 3: Commit succeeds
        let remote_addresses_clone = Arc::clone(&remote_addresses);
        mock.expect_commit().once().returning(move |request| {
            remote_addresses_clone.lock().expect("mutex lock").push(
                request
                    .remote_addr()
                    .expect("remote_addr should be available"),
            );
            Ok(Response::new(CommitResponse {
                commit_timestamp: Some(Timestamp {
                    seconds: 3000,
                    nanos: 0,
                }),
                ..Default::default()
            }))
        });

        let (database_client, _server) = setup_db_client_with_dynamic_pool(mock, 4, 8).await;

        let runner = database_client.read_write_transaction().build();
        let closure_invocations_clone = Arc::clone(&closure_invocations);
        let result = runner
            .run(|transaction: ReadWriteTransaction| {
                let closure_invocations_clone = Arc::clone(&closure_invocations_clone);
                async move {
                    closure_invocations_clone.fetch_add(1, Ordering::SeqCst);
                    let mut result_set = transaction
                        .execute_query(Statement::builder("SELECT 1").build())
                        .await?;
                    let _ = result_set.next().await.transpose()?;
                    let count = transaction
                        .execute_update("UPDATE Users SET Name = 'Alice' WHERE Id = 1")
                        .await?;
                    Ok(count)
                }
            })
            .await?;

        assert_eq!(result.result, 1, "Expected update count of 1 on retry");
        assert_eq!(
            closure_invocations.load(Ordering::SeqCst),
            3,
            "Expected closure to be invoked 3 times across multiple aborted attempts"
        );

        let addresses = remote_addresses.lock().expect("mutex lock");
        assert_eq!(
            addresses.len(),
            8,
            "Expected 8 total RPCs across 3 attempts"
        );
        assert_all_rpcs_use_same_channel(&addresses[0..2], 2);
        assert_all_rpcs_use_same_channel(&addresses[2..5], 3);
        assert_all_rpcs_use_same_channel(&addresses[5..8], 3);

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn transaction_runner_rollback_on_user_error_uses_same_channel() -> anyhow::Result<()> {
        use crate::error::internal_error;

        let mut mock = create_session_mock();
        let remote_addresses = Arc::new(Mutex::new(Vec::new()));

        // Statement: ExecuteSql succeeds with inline begin
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

        // Rollback is triggered on non-aborted user error and must use the same channel
        let remote_addresses_clone = Arc::clone(&remote_addresses);
        mock.expect_rollback().once().returning(move |request| {
            remote_addresses_clone.lock().expect("mutex lock").push(
                request
                    .remote_addr()
                    .expect("remote_addr should be available"),
            );
            Ok(Response::new(()))
        });

        let (database_client, _server) = setup_db_client_with_dynamic_pool(mock, 4, 8).await;
        let pool_client = database_client.clone();

        let runner = database_client.read_write_transaction().build();
        let result = runner
            .run(|transaction: ReadWriteTransaction| async move {
                let _count = transaction
                    .execute_update("UPDATE Users SET Name = 'Alice' WHERE Id = 1")
                    .await?;
                Err::<(), Error>(internal_error("Application-level validation error"))
            })
            .await;

        assert!(
            result.is_err(),
            "Expected runner to return the user-facing application error"
        );

        let addresses = remote_addresses.lock().expect("mutex lock");
        assert_all_rpcs_use_same_channel(&addresses, 2);

        let total_active_rw: u32 = pool_client
            .spanner
            .channel_pool()
            .inner
            .active_entries
            .read()
            .expect("lock active_entries")
            .iter()
            .map(|entry| entry.active_rw_count())
            .sum();
        assert_eq!(
            total_active_rw, 0,
            "Non-aborted error and rollback must release the active RW guard on the channel"
        );

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn transaction_runner_explicit_begin_aborted_retry_routes_statements_within_each_attempt_to_same_channel()
    -> anyhow::Result<()> {
        let mut mock = create_session_mock();
        let remote_addresses = Arc::new(Mutex::new(Vec::new()));
        let closure_invocations = Arc::new(AtomicUsize::new(0));

        // Attempt 1: BeginTransaction succeeds
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

        // Attempt 1: ExecuteSql succeeds
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

        // Attempt 1: Commit fails with Aborted
        let remote_addresses_clone = Arc::clone(&remote_addresses);
        mock.expect_commit().once().returning(move |request| {
            remote_addresses_clone.lock().expect("mutex lock").push(
                request
                    .remote_addr()
                    .expect("remote_addr should be available"),
            );
            Err(Status::new(
                Code::Aborted,
                "Transaction was aborted during explicit begin commit",
            ))
        });

        // Attempt 2: BeginTransaction succeeds
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
                    id: vec![4, 5, 6],
                    ..Default::default()
                }))
            });

        // Attempt 2: ExecuteSql succeeds
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

        // Attempt 2: Commit succeeds
        let remote_addresses_clone = Arc::clone(&remote_addresses);
        mock.expect_commit().once().returning(move |request| {
            remote_addresses_clone.lock().expect("mutex lock").push(
                request
                    .remote_addr()
                    .expect("remote_addr should be available"),
            );
            Ok(Response::new(CommitResponse {
                commit_timestamp: Some(Timestamp {
                    seconds: 2000,
                    nanos: 0,
                }),
                ..Default::default()
            }))
        });

        let (database_client, _server) = setup_db_client_with_dynamic_pool(mock, 4, 8).await;

        let runner = database_client
            .read_write_transaction()
            .with_begin_transaction_option(BeginTransactionOption::ExplicitBegin)
            .build();
        let closure_invocations_clone = Arc::clone(&closure_invocations);
        let result = runner
            .run(|transaction: ReadWriteTransaction| {
                let closure_invocations_clone = Arc::clone(&closure_invocations_clone);
                async move {
                    closure_invocations_clone.fetch_add(1, Ordering::SeqCst);
                    let count = transaction
                        .execute_update("UPDATE Users SET Name = 'Alice' WHERE Id = 1")
                        .await?;
                    Ok(count)
                }
            })
            .await?;

        assert_eq!(result.result, 1, "Expected update count of 1 on retry");
        assert_eq!(
            closure_invocations.load(Ordering::SeqCst),
            2,
            "Expected closure to be invoked twice due to aborted retry with explicit begin"
        );

        let addresses = remote_addresses.lock().expect("mutex lock");
        assert_eq!(
            addresses.len(),
            6,
            "Expected 6 total RPCs across 2 attempts"
        );
        assert_all_rpcs_use_same_channel(&addresses[0..3], 3);
        assert_all_rpcs_use_same_channel(&addresses[3..6], 3);

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn transaction_runner_releases_rw_guard_before_aborted_backoff() -> anyhow::Result<()> {
        use crate::transaction_retry_policy::TransactionRetryPolicy;
        use google_cloud_gax::retry_result::RetryResult;

        #[derive(Debug)]
        struct GuardCheckRetryPolicy {
            pinned_entry: Arc<Mutex<Option<Arc<ChannelEntry>>>>,
            active_rw_counts_during_backoff: Arc<Mutex<Vec<u32>>>,
        }

        impl TransactionRetryPolicy for GuardCheckRetryPolicy {
            fn on_abort(&self, error: Error, attempts: u32, _elapsed: StdDuration) -> RetryResult {
                if let Some(entry) = self
                    .pinned_entry
                    .lock()
                    .expect("lock pinned_entry")
                    .as_ref()
                {
                    self.active_rw_counts_during_backoff
                        .lock()
                        .expect("lock active_rw_counts_during_backoff")
                        .push(entry.active_rw_count());
                }
                if attempts < 3 {
                    RetryResult::Continue(error)
                } else {
                    RetryResult::Exhausted(error)
                }
            }
        }

        let mut mock = create_session_mock();
        let mut sequence = Sequence::new();

        // Attempt 1: Statement 1 (ExecuteStreamingSql) succeeds
        mock.expect_execute_streaming_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(|_| {
                let metadata = v1::ResultSetMetadata {
                    row_type: Some(v1::StructType { fields: vec![] }),
                    transaction: Some(v1::Transaction {
                        id: vec![11, 22, 33],
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

        // Attempt 1: Statement 2 (ExecuteSql) fails with Aborted (skips rollback)
        mock.expect_execute_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(|_| Err(Status::new(Code::Aborted, "Aborted during work")));

        // Attempt 2: Statement 1 (ExecuteStreamingSql) succeeds
        mock.expect_execute_streaming_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(|_| {
                let metadata = v1::ResultSetMetadata {
                    row_type: Some(v1::StructType { fields: vec![] }),
                    transaction: Some(v1::Transaction {
                        id: vec![44, 55, 66],
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

        // Attempt 2: Statement 2 (ExecuteSql) succeeds
        mock.expect_execute_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(|_| {
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

        // Attempt 2: Commit succeeds
        mock.expect_commit()
            .once()
            .in_sequence(&mut sequence)
            .returning(|_| {
                Ok(Response::new(CommitResponse {
                    commit_timestamp: Some(Timestamp {
                        seconds: 2000,
                        nanos: 0,
                    }),
                    ..Default::default()
                }))
            });

        let (database_client, _server) = setup_db_client_with_dynamic_pool(mock, 4, 8).await;

        let pinned_entry_slot: Arc<Mutex<Option<Arc<ChannelEntry>>>> = Arc::new(Mutex::new(None));
        let active_rw_counts_during_backoff = Arc::new(Mutex::new(Vec::new()));
        // Retain cloned ReadWriteTransaction handles outside the closure to verify that
        // `affinity.release_rw_guard()` releases the channel guard even when external
        // `Arc<TransactionAffinity>` references remain alive.
        let retained_transactions: Arc<Mutex<Vec<ReadWriteTransaction>>> =
            Arc::new(Mutex::new(Vec::new()));

        let policy = GuardCheckRetryPolicy {
            pinned_entry: Arc::clone(&pinned_entry_slot),
            active_rw_counts_during_backoff: Arc::clone(&active_rw_counts_during_backoff),
        };

        let runner = database_client
            .read_write_transaction()
            .with_retry_policy(policy)
            .build();

        let pinned_entry_slot_clone = Arc::clone(&pinned_entry_slot);
        let retained_transactions_clone = Arc::clone(&retained_transactions);
        let result = runner
            .run(|transaction: ReadWriteTransaction| {
                let pinned_entry_slot_clone = Arc::clone(&pinned_entry_slot_clone);
                let retained_transactions_clone = Arc::clone(&retained_transactions_clone);
                async move {
                    retained_transactions_clone
                        .lock()
                        .expect("lock retained_transactions")
                        .push(transaction.clone());

                    let mut result_set = transaction
                        .execute_query(Statement::builder("SELECT 1").build())
                        .await?;
                    let _ = result_set.next().await.transpose()?;
                    drop(result_set);

                    let entry_id = transaction
                        .affinity()
                        .expect("affinity must be present")
                        .pinned_entry_id()
                        .expect("channel must be pinned after first statement");
                    let entry = {
                        let active_guard = transaction
                            .context
                            .client
                            .spanner
                            .channel_pool()
                            .inner
                            .active_entries
                            .read()
                            .expect("lock active_entries");
                        active_guard
                            .iter()
                            .find(|candidate| candidate.id == entry_id)
                            .map(Arc::clone)
                            .expect("pinned entry must be in active_entries")
                    };
                    assert_eq!(
                        entry.active_rw_count(),
                        1,
                        "Pinned channel entry must have active_rw_count == 1 while attempt is active"
                    );
                    *pinned_entry_slot_clone.lock().expect("lock pinned_entry") = Some(entry);

                    let count = transaction
                        .execute_update("UPDATE Users SET Name = 'Alice' WHERE Id = 1")
                        .await?;
                    Ok(count)
                }
            })
            .await?;

        assert_eq!(result.result, 1, "Expected update count of 1 on retry");
        assert_eq!(
            retained_transactions
                .lock()
                .expect("lock retained_transactions")
                .len(),
            2,
            "Both attempts should have retained a cloned ReadWriteTransaction"
        );

        let counts = active_rw_counts_during_backoff
            .lock()
            .expect("lock active_rw_counts_during_backoff");
        assert_eq!(
            *counts,
            vec![0],
            "active_rw_count on the pinned channel must be decremented to 0 before backoff_if_aborted evaluates retry policy and sleeps, even when a cloned ReadWriteTransaction is retained"
        );

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn transaction_runner_aborted_retry_bypasses_draining_channel_and_allows_scale_in()
    -> anyhow::Result<()> {
        let mut mock = create_session_mock();
        let mut sequence = Sequence::new();
        let remote_addresses = Arc::new(Mutex::new(Vec::new()));

        // Attempt 1: Statement 1 (ExecuteStreamingSql) pins initial active channel
        let remote_addresses_clone = Arc::clone(&remote_addresses);
        mock.expect_execute_streaming_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |request| {
                remote_addresses_clone.lock().expect("mutex lock").push(
                    request
                        .remote_addr()
                        .expect("remote_addr should be available"),
                );
                let metadata = v1::ResultSetMetadata {
                    row_type: Some(v1::StructType { fields: vec![] }),
                    transaction: Some(v1::Transaction {
                        id: vec![10, 20, 30],
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

        // Attempt 1: Statement 2 (ExecuteSql) runs on draining channel and fails with Aborted
        let remote_addresses_clone = Arc::clone(&remote_addresses);
        mock.expect_execute_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |request| {
                remote_addresses_clone.lock().expect("mutex lock").push(
                    request
                        .remote_addr()
                        .expect("remote_addr should be available"),
                );
                Err(Status::new(
                    Code::Aborted,
                    "Transaction aborted on draining channel",
                ))
            });

        // Attempt 2: Statement 1 (ExecuteStreamingSql) must select a fresh active channel
        let remote_addresses_clone = Arc::clone(&remote_addresses);
        mock.expect_execute_streaming_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |request| {
                remote_addresses_clone.lock().expect("mutex lock").push(
                    request
                        .remote_addr()
                        .expect("remote_addr should be available"),
                );
                let metadata = v1::ResultSetMetadata {
                    row_type: Some(v1::StructType { fields: vec![] }),
                    transaction: Some(v1::Transaction {
                        id: vec![40, 50, 60],
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

        // Attempt 2: Statement 2 (ExecuteSql) succeeds on the new active channel
        let remote_addresses_clone = Arc::clone(&remote_addresses);
        mock.expect_execute_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |request| {
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

        // Attempt 2: Commit succeeds on the new active channel
        let remote_addresses_clone = Arc::clone(&remote_addresses);
        mock.expect_commit()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |request| {
                remote_addresses_clone.lock().expect("mutex lock").push(
                    request
                        .remote_addr()
                        .expect("remote_addr should be available"),
                );
                Ok(Response::new(CommitResponse {
                    commit_timestamp: Some(Timestamp {
                        seconds: 2000,
                        nanos: 0,
                    }),
                    ..Default::default()
                }))
            });

        let (database_client, _server) = setup_db_client_with_dynamic_pool(mock, 4, 8).await;
        let pool_client = database_client.clone();

        let runner = database_client.read_write_transaction().build();
        let attempt_counter = Arc::new(AtomicUsize::new(0));
        let drained_entry_slot: Arc<Mutex<Option<Arc<ChannelEntry>>>> = Arc::new(Mutex::new(None));
        let attempt_2_entry_id_slot: Arc<Mutex<Option<u64>>> = Arc::new(Mutex::new(None));

        let attempt_counter_clone = Arc::clone(&attempt_counter);
        let drained_entry_slot_clone = Arc::clone(&drained_entry_slot);
        let attempt_2_entry_id_slot_clone = Arc::clone(&attempt_2_entry_id_slot);

        let result = runner
            .run(|transaction: ReadWriteTransaction| {
                let attempt_counter_clone = Arc::clone(&attempt_counter_clone);
                let drained_entry_slot_clone = Arc::clone(&drained_entry_slot_clone);
                let attempt_2_entry_id_slot_clone = Arc::clone(&attempt_2_entry_id_slot_clone);
                async move {
                    let current_attempt = attempt_counter_clone.fetch_add(1, Ordering::SeqCst);

                    let mut result_set = transaction
                        .execute_query(Statement::builder("SELECT 1").build())
                        .await?;
                    let _ = result_set.next().await.transpose()?;
                    drop(result_set);

                    let pinned_entry_id = transaction
                        .affinity()
                        .expect("affinity must be present")
                        .pinned_entry_id()
                        .expect("channel must be pinned after first statement");

                    let pool = transaction.context.client.spanner.channel_pool();
                    if current_attempt == 0 {
                        // Locate the pinned channel entry from Attempt 1 and transition it to Draining
                        let pinned_entry = {
                            let mut active_write =
                                pool.inner.active_entries.write().expect("lock active_entries");
                            let entry = active_write
                                .iter()
                                .find(|candidate| candidate.id == pinned_entry_id)
                                .map(Arc::clone)
                                .expect("pinned entry must exist in active_entries");
                            assert_eq!(
                                entry.active_rw_count(),
                                1,
                                "Attempt 1 channel must have active_rw_count == 1 while active"
                            );
                            active_write.retain(|candidate| candidate.id != pinned_entry_id);
                            entry
                        };

                        pinned_entry.set_state(ChannelState::Draining);
                        pool.inner
                            .draining_entries
                            .write()
                            .expect("lock draining_entries")
                            .push(Arc::clone(&pinned_entry));
                        *drained_entry_slot_clone.lock().expect("lock drained_entry") =
                            Some(pinned_entry);
                    } else {
                        *attempt_2_entry_id_slot_clone
                            .lock()
                            .expect("lock attempt_2_entry_id") = Some(pinned_entry_id);
                        let drained_entry = drained_entry_slot_clone
                            .lock()
                            .expect("lock drained_entry")
                            .as_ref()
                            .map(Arc::clone)
                            .expect("drained_entry must be recorded from attempt 1");
                        assert_eq!(
                            drained_entry.active_rw_count(),
                            0,
                            "Draining channel from attempt 1 must have active_rw_count == 0 during attempt 2"
                        );
                    }

                    let count = transaction
                        .execute_update("UPDATE Users SET Name = 'Alice' WHERE Id = 1")
                        .await?;
                    Ok(count)
                }
            })
            .await?;

        assert_eq!(result.result, 1, "Expected update count of 1 on retry");

        let drained_entry = drained_entry_slot
            .lock()
            .expect("lock drained_entry")
            .as_ref()
            .map(Arc::clone)
            .expect("drained_entry must be populated");
        let attempt_2_entry_id = attempt_2_entry_id_slot
            .lock()
            .expect("lock attempt_2_entry_id")
            .expect("attempt 2 must have pinned an entry");

        assert_ne!(
            attempt_2_entry_id, drained_entry.id,
            "Attempt 2 must not pin onto the draining channel from attempt 1"
        );

        let addresses = remote_addresses.lock().expect("mutex lock");
        assert_eq!(
            addresses.len(),
            5,
            "Expected 5 total RPCs across 2 attempts"
        );
        // Within Attempt 1, Statement 2 stayed on the draining channel (hard stickiness within attempt)
        assert_all_rpcs_use_same_channel(&addresses[0..2], 2);
        // Within Attempt 2, all 3 RPCs used the newly selected active channel
        assert_all_rpcs_use_same_channel(&addresses[2..5], 3);
        assert_ne!(
            addresses[0], addresses[2],
            "Attempt 2 must route to a different physical channel than the draining channel from attempt 1"
        );

        // Sweeping draining channels must now immediately close the drained channel
        sweep_draining_channels(
            &pool_client.spanner.channel_pool().inner,
            StdDuration::from_millis(0),
        );
        assert!(
            drained_entry.is_closed(),
            "Drained channel from attempt 1 must transition to Closed on sweep"
        );
        assert_eq!(
            pool_client.spanner.channel_pool().draining_channel_count(),
            0,
            "Draining channel pool must be empty after sweep"
        );

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn transaction_runner_future_drop_releases_rw_guard_via_raii() -> anyhow::Result<()> {
        use std::future::pending;
        use tokio::sync::Notify;

        let mut mock = create_session_mock();

        // Statement 1 (ExecuteStreamingSql) succeeds and pins a channel
        mock.expect_execute_streaming_sql().once().returning(|_| {
            let metadata = v1::ResultSetMetadata {
                row_type: Some(v1::StructType { fields: vec![] }),
                transaction: Some(v1::Transaction {
                    id: vec![21, 31, 41],
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

        let (database_client, _server) = setup_db_client_with_dynamic_pool(mock, 4, 8).await;
        let runner = database_client.read_write_transaction().build();

        let pinned_entry_slot: Arc<Mutex<Option<Arc<ChannelEntry>>>> = Arc::new(Mutex::new(None));
        let channel_pinned_notify = Arc::new(Notify::new());

        let pinned_entry_slot_clone = Arc::clone(&pinned_entry_slot);
        let channel_pinned_notify_clone = Arc::clone(&channel_pinned_notify);

        tokio::select! {
            result = runner.run(|transaction: ReadWriteTransaction| {
                let pinned_entry_slot_clone = Arc::clone(&pinned_entry_slot_clone);
                let channel_pinned_notify_clone = Arc::clone(&channel_pinned_notify_clone);
                async move {
                    let mut result_set = transaction
                        .execute_query(Statement::builder("SELECT 1").build())
                        .await?;
                    let _ = result_set.next().await.transpose()?;
                    drop(result_set);

                    let entry_id = transaction
                        .affinity()
                        .expect("affinity must be present")
                        .pinned_entry_id()
                        .expect("channel must be pinned after first statement");
                    let entry = {
                        let active_guard = transaction
                            .context
                            .client
                            .spanner
                            .channel_pool()
                            .inner
                            .active_entries
                            .read()
                            .expect("lock active_entries");
                        active_guard
                            .iter()
                            .find(|candidate| candidate.id == entry_id)
                            .map(Arc::clone)
                            .expect("pinned entry must be in active_entries")
                    };
                    assert_eq!(
                        entry.active_rw_count(),
                        1,
                        "Pinned channel entry must have active_rw_count == 1 while future is suspended mid-attempt"
                    );
                    *pinned_entry_slot_clone.lock().expect("lock pinned_entry") = Some(entry);
                    channel_pinned_notify_clone.notify_one();

                    pending::<Result<i64, Error>>().await
                }
            }) => {
                panic!("runner.run future should have been cancelled before completing: {result:?}");
            }
            _ = channel_pinned_notify.notified() => {
                // Exiting select! drops the in-flight `runner.run(...)` future mid-attempt
            }
        }

        let pinned_entry = pinned_entry_slot
            .lock()
            .expect("lock pinned_entry")
            .as_ref()
            .map(Arc::clone)
            .expect("pinned_entry must have been captured before cancellation");
        assert_eq!(
            pinned_entry.active_rw_count(),
            0,
            "Cancelling and dropping the TransactionRunner::run future mid-attempt must release the RW guard via RAII"
        );

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn transaction_runner_aborted_attempt_clears_location_router_affinity()
    -> anyhow::Result<()> {
        use crate::client::{Spanner, SpannerBuilderExt};
        use crate::omni::InstanceType;
        use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
        use spanner_grpc_mock::google::spanner::v1::result_set_stats::RowCount;
        use spanner_grpc_mock::google::spanner::v1::{
            ResultSet, ResultSetMetadata, ResultSetStats, StructType, Transaction,
        };
        use spanner_grpc_mock::start;

        let mut mock = create_session_mock();
        mock.expect_fetch_cache_update().returning(|_| {
            let (_sender, receiver) = tokio::sync::mpsc::channel(1);
            Ok(tonic::Response::from(receiver))
        });

        let aborted_transaction_id = vec![1, 2, 3];
        let retry_transaction_id = vec![4, 5, 6];

        // Attempt 1: BeginTransaction returns aborted_transaction_id
        let first_transaction_id = aborted_transaction_id.clone();
        mock.expect_begin_transaction().once().returning(move |_| {
            Ok(Response::new(Transaction {
                id: first_transaction_id.clone(),
                ..Default::default()
            }))
        });

        // Attempt 1: ExecuteSql fails with Aborted
        mock.expect_execute_sql().once().returning(|_| {
            Err(Status::new(
                Code::Aborted,
                "Transaction was aborted by the server",
            ))
        });

        // Attempt 2: BeginTransaction returns retry_transaction_id
        let second_transaction_id = retry_transaction_id.clone();
        mock.expect_begin_transaction().once().returning(move |_| {
            Ok(Response::new(Transaction {
                id: second_transaction_id.clone(),
                ..Default::default()
            }))
        });

        // Attempt 2: ExecuteSql succeeds
        mock.expect_execute_sql().once().returning(|_| {
            Ok(Response::new(ResultSet {
                metadata: Some(ResultSetMetadata {
                    row_type: Some(StructType { fields: vec![] }),
                    ..Default::default()
                }),
                stats: Some(ResultSetStats {
                    row_count: Some(RowCount::RowCountExact(1)),
                    ..Default::default()
                }),
                ..Default::default()
            }))
        });

        // Attempt 2: Commit succeeds
        mock.expect_commit().once().returning(|_| {
            Ok(Response::new(CommitResponse {
                commit_timestamp: Some(Timestamp {
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

        let database_client = client
            .database_client("projects/p/instances/i/databases/d")
            .with_location_aware_routing(true)
            .build()
            .await?;

        let router = database_client
            .location_router()
            .expect("location router must be present when location-aware routing is enabled");

        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_clone = Arc::clone(&attempts);
        let router_clone = Arc::clone(router);
        let aborted_transaction_id_clone = aborted_transaction_id.clone();

        let runner = database_client
            .read_write_transaction()
            .with_begin_transaction_option(BeginTransactionOption::ExplicitBegin)
            .build();

        let result = runner
            .run(move |transaction: ReadWriteTransaction| {
                let attempts_clone = Arc::clone(&attempts_clone);
                let router_clone = Arc::clone(&router_clone);
                let aborted_transaction_id = aborted_transaction_id_clone.clone();

                async move {
                    let current_attempt = attempts_clone.fetch_add(1, Ordering::SeqCst);
                    if current_attempt == 0 {
                        // On attempt 1, record affinity
                        router_clone.record_transaction_affinity(
                            &aborted_transaction_id,
                            "node-1.spanner.internal:15000",
                        );
                        assert_eq!(
                            router_clone
                                .get_transaction_affinity(&aborted_transaction_id)
                                .as_deref(),
                            Some("node-1.spanner.internal:15000"),
                            "affinity must be recorded on attempt 1"
                        );
                    } else if current_attempt == 1 {
                        // On attempt 2 (retry), verify that the aborted transaction ID from attempt 1 has been cleared
                        assert!(
                            router_clone.get_transaction_affinity(&aborted_transaction_id).is_none(),
                            "aborted transaction affinity must be removed before retry attempt executes"
                        );
                    }
                    let count = transaction
                        .execute_update("UPDATE Users SET Name = 'Alice' WHERE Id = 1")
                        .await?;
                    Ok(count)
                }
            })
            .await?;

        assert_eq!(result.result, 1, "Expected update count of 1");
        assert_eq!(
            attempts.load(Ordering::SeqCst),
            2,
            "Expected 2 attempts due to retry"
        );
        assert!(
            router
                .get_transaction_affinity(&aborted_transaction_id)
                .is_none(),
            "aborted transaction affinity must be cleared"
        );
        assert_eq!(
            router.affinity_count(),
            0,
            "all transaction affinities must be cleared after runner completes"
        );

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn transaction_runner_commit_aborted_attempt_clears_location_router_affinity()
    -> anyhow::Result<()> {
        use crate::client::{Spanner, SpannerBuilderExt};
        use crate::omni::InstanceType;
        use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
        use spanner_grpc_mock::google::spanner::v1::result_set_stats::RowCount;
        use spanner_grpc_mock::google::spanner::v1::{
            ResultSet, ResultSetMetadata, ResultSetStats, StructType, Transaction,
        };
        use spanner_grpc_mock::start;

        let mut mock = create_session_mock();
        mock.expect_fetch_cache_update().returning(|_| {
            let (_sender, receiver) = tokio::sync::mpsc::channel(1);
            Ok(tonic::Response::from(receiver))
        });

        let aborted_transaction_id = vec![10, 11];
        let retry_transaction_id = vec![20, 21];

        // Attempt 1: BeginTransaction returns aborted_transaction_id
        let first_transaction_id = aborted_transaction_id.clone();
        mock.expect_begin_transaction().once().returning(move |_| {
            Ok(Response::new(Transaction {
                id: first_transaction_id.clone(),
                ..Default::default()
            }))
        });

        // Attempt 1: ExecuteSql succeeds
        mock.expect_execute_sql().once().returning(|_| {
            Ok(Response::new(ResultSet {
                metadata: Some(ResultSetMetadata {
                    row_type: Some(StructType { fields: vec![] }),
                    ..Default::default()
                }),
                stats: Some(ResultSetStats {
                    row_count: Some(RowCount::RowCountExact(1)),
                    ..Default::default()
                }),
                ..Default::default()
            }))
        });

        // Attempt 1: Commit fails with Aborted
        mock.expect_commit().once().returning(|_| {
            Err(Status::new(
                Code::Aborted,
                "Commit was aborted by the server",
            ))
        });

        // Attempt 2: BeginTransaction returns retry_transaction_id
        let second_transaction_id = retry_transaction_id.clone();
        mock.expect_begin_transaction().once().returning(move |_| {
            Ok(Response::new(Transaction {
                id: second_transaction_id.clone(),
                ..Default::default()
            }))
        });

        // Attempt 2: ExecuteSql succeeds
        mock.expect_execute_sql().once().returning(|_| {
            Ok(Response::new(ResultSet {
                metadata: Some(ResultSetMetadata {
                    row_type: Some(StructType { fields: vec![] }),
                    ..Default::default()
                }),
                stats: Some(ResultSetStats {
                    row_count: Some(RowCount::RowCountExact(1)),
                    ..Default::default()
                }),
                ..Default::default()
            }))
        });

        // Attempt 2: Commit succeeds
        mock.expect_commit().once().returning(|_| {
            Ok(Response::new(CommitResponse {
                commit_timestamp: Some(Timestamp {
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

        let database_client = client
            .database_client("projects/p/instances/i/databases/d")
            .with_location_aware_routing(true)
            .build()
            .await?;

        let router = database_client
            .location_router()
            .expect("location router must be present when location-aware routing is enabled");

        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_clone = Arc::clone(&attempts);
        let router_clone = Arc::clone(router);
        let aborted_transaction_id_clone = aborted_transaction_id.clone();

        let runner = database_client
            .read_write_transaction()
            .with_begin_transaction_option(BeginTransactionOption::ExplicitBegin)
            .build();

        let result = runner
            .run(move |transaction: ReadWriteTransaction| {
                let attempts_clone = Arc::clone(&attempts_clone);
                let router_clone = Arc::clone(&router_clone);
                let aborted_transaction_id = aborted_transaction_id_clone.clone();

                async move {
                    let current_attempt = attempts_clone.fetch_add(1, Ordering::SeqCst);
                    if current_attempt == 0 {
                        // On attempt 1, record an affinity for the transaction
                        router_clone.record_transaction_affinity(
                            &aborted_transaction_id,
                            "node-1.spanner.internal:15000",
                        );
                        assert_eq!(
                            router_clone
                                .get_transaction_affinity(&aborted_transaction_id)
                                .as_deref(),
                            Some("node-1.spanner.internal:15000"),
                            "affinity must be recorded on attempt 1"
                        );
                    } else if current_attempt == 1 {
                        // On attempt 2 (retry), verify that the aborted transaction affinity from attempt 1 has been cleared
                        assert!(
                            router_clone
                                .get_transaction_affinity(&aborted_transaction_id)
                                .is_none(),
                            "aborted commit transaction affinity must be removed before retry attempt executes"
                        );
                    }
                    let count = transaction
                        .execute_update("UPDATE Users SET Name = 'Alice' WHERE Id = 1")
                        .await?;
                    Ok(count)
                }
            })
            .await?;

        assert_eq!(result.result, 1, "Expected update count of 1");
        assert_eq!(
            attempts.load(Ordering::SeqCst),
            2,
            "Expected 2 attempts due to commit abort retry"
        );
        assert!(
            router
                .get_transaction_affinity(&aborted_transaction_id)
                .is_none(),
            "aborted transaction affinity must be cleared"
        );
        assert_eq!(
            router.affinity_count(),
            0,
            "all transaction affinities must be cleared after runner completes"
        );

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn transaction_runner_non_aborted_error_clears_location_router_affinity()
    -> anyhow::Result<()> {
        use crate::client::{Spanner, SpannerBuilderExt};
        use crate::error::internal_error;
        use crate::omni::InstanceType;
        use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
        use spanner_grpc_mock::google::spanner::v1::Transaction;
        use spanner_grpc_mock::start;

        let mut mock = create_session_mock();
        mock.expect_fetch_cache_update().returning(|_| {
            let (_sender, receiver) = tokio::sync::mpsc::channel(1);
            Ok(tonic::Response::from(receiver))
        });

        let transaction_id = vec![30, 31];

        // BeginTransaction returns transaction_id
        let id_for_begin = transaction_id.clone();
        mock.expect_begin_transaction().once().returning(move |_| {
            Ok(Response::new(Transaction {
                id: id_for_begin.clone(),
                ..Default::default()
            }))
        });

        // Rollback is invoked when closure fails with non-aborted error
        mock.expect_rollback()
            .once()
            .returning(|_| Ok(Response::new(())));

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

        let router = database_client
            .location_router()
            .expect("location router must be present when location-aware routing is enabled");

        let router_clone = Arc::clone(router);
        let transaction_id_clone = transaction_id.clone();

        let runner = database_client
            .read_write_transaction()
            .with_begin_transaction_option(BeginTransactionOption::ExplicitBegin)
            .build();

        let run_result = runner
            .run(move |_transaction: ReadWriteTransaction| {
                let router_clone = Arc::clone(&router_clone);
                let transaction_id = transaction_id_clone.clone();

                async move {
                    router_clone.record_transaction_affinity(
                        &transaction_id,
                        "node-1.spanner.internal:15000",
                    );
                    assert_eq!(
                        router_clone
                            .get_transaction_affinity(&transaction_id)
                            .as_deref(),
                        Some("node-1.spanner.internal:15000"),
                        "affinity must be recorded in closure"
                    );
                    Err::<(), _>(internal_error("user-initiated closure failure"))
                }
            })
            .await;

        assert!(
            run_result.is_err(),
            "runner must return Err when user closure fails with non-aborted error"
        );
        assert!(
            router.get_transaction_affinity(&transaction_id).is_none(),
            "transaction affinity must be cleared when closure fails with non-aborted error"
        );
        assert_eq!(
            router.affinity_count(),
            0,
            "all transaction affinities must be cleared after failed runner"
        );

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn inlined_begin_query_stream_fails_with_invalid_argument_uncaught() -> anyhow::Result<()>
    {
        let mut mock = create_session_mock();
        let mut sequence = Sequence::new();

        // Initial streaming query with inline begin fails with InvalidArgument.
        // It must NOT trigger begin_transaction, restart_stream, commit, or rollback.
        mock.expect_begin_transaction().never();
        mock.expect_commit().never();
        mock.expect_rollback().never();

        mock.expect_execute_streaming_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |request| {
                let request = request.into_inner();
                let transaction = request
                    .transaction
                    .as_ref()
                    .expect("transaction options required for inline begin");
                let selector = transaction.selector.as_ref().expect("selector required");
                assert!(
                    matches!(selector, v1::transaction_selector::Selector::Begin(_)),
                    "Expected Selector::Begin on attempt 1"
                );

                let (sender, receiver) = channel(2);
                sender
                    .try_send(Err(Status::new(
                        Code::InvalidArgument,
                        "Table not found: non_existing_table",
                    )))
                    .expect("channel send should succeed");
                Ok(Response::from(receiver))
            });

        let (database_client, _server) = setup_db_client(mock).await;
        let runner = TransactionRunnerBuilder::new(database_client)
            .with_begin_transaction_option(BeginTransactionOption::InlineBegin)
            .build();

        let result = runner
            .run::<(), _>(async |transaction| {
                // execute_query eagerly awaits stream initialization to fetch metadata.
                // The stream yields InvalidArgument immediately, so execute_query returns Err directly.
                let _result_set = transaction
                    .execute_query("SELECT * FROM non_existing_table")
                    .await?;
                panic!("execute_query eagerly initializes the stream chunk and must return Err directly");
            })
            .await;

        assert!(
            result.is_err(),
            "Expected error from invalid argument query"
        );
        let error = result.expect_err("Expected error");
        let status = error.status().expect("Expected gRPC status");
        assert_eq!(
            status.code,
            GaxCode::InvalidArgument,
            "Expected InvalidArgument error code"
        );

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn inlined_begin_query_stream_fails_with_invalid_argument_and_continues()
    -> anyhow::Result<()> {
        let mut mock = create_session_mock();
        let mut sequence = Sequence::new();

        let transaction_id = vec![1, 2, 3, 4];
        let transaction_id_for_begin = transaction_id.clone();
        let transaction_id_for_query = transaction_id.clone();
        let transaction_id_for_update = transaction_id.clone();
        let transaction_id_for_commit = transaction_id.clone();

        // Attempt 1:
        // 1. Initial streaming query with inline begin fails with InvalidArgument.
        mock.expect_execute_streaming_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |request| {
                let request = request.into_inner();
                let transaction = request
                    .transaction
                    .as_ref()
                    .expect("transaction options required for inline begin");
                let selector = transaction.selector.as_ref().expect("selector required");
                assert!(
                    matches!(selector, v1::transaction_selector::Selector::Begin(_)),
                    "Expected Selector::Begin on attempt 1"
                );

                let (sender, receiver) = channel(2);
                sender
                    .try_send(Err(Status::new(
                        Code::InvalidArgument,
                        "Table not found: non_existing_table",
                    )))
                    .expect("channel send should succeed");
                Ok(Response::from(receiver))
            });

        // Attempt 2:
        // The closure caught the error and called execute_update, which detected
        // FirstStatementFailed and returned Aborted ("Aborted due to failed initial statement").
        // TransactionRunner retries with force_explicit_begin = true.
        // 2. Explicit BeginTransaction RPC runs first.
        mock.expect_begin_transaction()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |_request| {
                Ok(Response::new(v1::Transaction {
                    id: transaction_id_for_begin.clone(),
                    ..Default::default()
                }))
            });

        // 3. Statement 1 is re-executed inside the new transaction and fails again with InvalidArgument.
        mock.expect_execute_streaming_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |request| {
                let request = request.into_inner();
                let transaction = request
                    .transaction
                    .as_ref()
                    .expect("transaction selector required");
                assert_eq!(
                    transaction.selector,
                    Some(v1::transaction_selector::Selector::Id(
                        transaction_id_for_query.clone()
                    )),
                    "Expected Selector::Id on retry query"
                );

                let (sender, receiver) = channel(2);
                sender
                    .try_send(Err(Status::new(
                        Code::InvalidArgument,
                        "Table not found: non_existing_table",
                    )))
                    .expect("channel send should succeed");
                Ok(Response::from(receiver))
            });

        // 4. Statement 2 (execute_update) runs within the explicitly started transaction and succeeds.
        mock.expect_execute_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |request| {
                let request = request.into_inner();
                let transaction = request
                    .transaction
                    .as_ref()
                    .expect("transaction selector required");
                assert_eq!(
                    transaction.selector,
                    Some(v1::transaction_selector::Selector::Id(
                        transaction_id_for_update.clone()
                    )),
                    "Expected Selector::Id on retry update"
                );
                row_count_exact_response(1)
            });

        // 5. Commit runs and succeeds.
        mock.expect_commit()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |request| {
                let request = request.into_inner();
                assert_eq!(
                    request.transaction,
                    Some(CommitTransaction::TransactionId(
                        transaction_id_for_commit.clone()
                    )),
                    "Expected commit with transaction ID on retry"
                );
                commit_response()
            });

        let (database_client, _server) = setup_db_client(mock).await;
        let runner = TransactionRunnerBuilder::new(database_client)
            .with_begin_transaction_option(BeginTransactionOption::InlineBegin)
            .build();

        let mut attempt_counter = 0;
        let result = runner
            .run(async |transaction| {
                attempt_counter += 1;
                // Note that the ? operator is intentionally NOT used here so that the closure
                // does not return early on error. Instead, we manually inspect the result
                // so the transaction can test continuing with subsequent statements.
                let query_result = transaction
                    .execute_query("SELECT * FROM non_existing_table")
                    .await;
                assert!(
                    query_result.is_err(),
                    "Expected query to fail with InvalidArgument on attempt {}",
                    attempt_counter
                );

                let updated_count = transaction
                    .execute_update("UPDATE Users SET active = true WHERE id = 1")
                    .await?;
                Ok(updated_count)
            })
            .await?;

        assert_eq!(
            attempt_counter, 2,
            "Expected exactly 2 attempts due to retry after failed initial statement"
        );
        assert_eq!(result.result, 1, "Expected 1 row updated on success");

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn inlined_begin_query_stream_fails_with_aborted() -> anyhow::Result<()> {
        let mut mock = create_session_mock();
        let mut sequence = Sequence::new();

        mock.expect_begin_transaction().never();
        mock.expect_rollback().never();

        let transaction_id = vec![5, 6, 7, 8];
        let transaction_id_clone = transaction_id.clone();

        // Attempt 1:
        // Initial streaming query with inline begin fails with Aborted.
        // It must NOT silently begin an explicit transaction; it must propagate Aborted to TransactionRunner.
        mock.expect_execute_streaming_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |request| {
                let request = request.into_inner();
                let transaction = request
                    .transaction
                    .as_ref()
                    .expect("transaction options required for inline begin on attempt 1");
                let selector = transaction.selector.as_ref().expect("selector required");
                assert!(
                    matches!(selector, v1::transaction_selector::Selector::Begin(_)),
                    "Expected inline begin option on attempt 1"
                );

                let (sender, receiver) = channel(2);
                sender
                    .try_send(Err(create_aborted_status(StdDuration::from_nanos(1))))
                    .expect("channel send should succeed");
                Ok(Response::from(receiver))
            });

        // Attempt 2:
        // Query succeeds on retry, also using inline begin since no transaction ID was established on attempt 1.
        mock.expect_execute_streaming_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |request| {
                let request = request.into_inner();
                let transaction = request
                    .transaction
                    .as_ref()
                    .expect("transaction options required for inline begin on attempt 2");
                let selector = transaction.selector.as_ref().expect("selector required");
                assert!(
                    matches!(selector, v1::transaction_selector::Selector::Begin(_)),
                    "Expected inline begin option on attempt 2"
                );

                let partial_result_set = v1::PartialResultSet {
                    metadata: Some(v1::ResultSetMetadata {
                        row_type: Some(v1::StructType {
                            fields: vec![Default::default()],
                        }),
                        transaction: Some(v1::Transaction {
                            id: transaction_id_clone.clone(),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    values: vec![ProtoValue {
                        kind: Some(Kind::StringValue("1".to_string())),
                    }],
                    last: true,
                    ..Default::default()
                };

                let (sender, receiver) = channel(2);
                sender
                    .try_send(Ok(partial_result_set))
                    .expect("channel send should succeed");
                Ok(Response::from(receiver))
            });

        mock.expect_commit()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |request| {
                let request = request.into_inner();
                assert_eq!(
                    request.transaction,
                    Some(CommitTransaction::TransactionId(transaction_id.clone())),
                    "Expected commit with transaction ID on retry"
                );
                commit_response()
            });

        let (database_client, _server) = setup_db_client(mock).await;
        let runner = TransactionRunnerBuilder::new(database_client)
            .with_begin_transaction_option(BeginTransactionOption::InlineBegin)
            .build();

        let mut attempt_counter = 0;
        runner
            .run(async |transaction| {
                attempt_counter += 1;
                let mut result_set = transaction.execute_query("SELECT 1").await?;
                let row = result_set.next().await.expect("Expected row")?;
                assert_eq!(
                    row.raw_values()[0].0,
                    Kind::StringValue("1".to_string()).into(),
                    "Expected query value 1"
                );
                Ok(())
            })
            .await?;

        assert_eq!(
            attempt_counter, 2,
            "Expected closure to be retried once on Aborted"
        );

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn inlined_begin_streaming_read_fails_with_invalid_argument_uncaught()
    -> anyhow::Result<()> {
        let mut mock = create_session_mock();
        let mut sequence = Sequence::new();

        // Initial streaming read fails with InvalidArgument.
        // It must NOT trigger begin_transaction, restart_stream, commit, or rollback.
        mock.expect_begin_transaction().never();
        mock.expect_commit().never();
        mock.expect_rollback().never();

        mock.expect_streaming_read()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |request| {
                let request = request.into_inner();
                let transaction = request
                    .transaction
                    .as_ref()
                    .expect("transaction options required for inline begin");
                let selector = transaction.selector.as_ref().expect("selector required");
                assert!(
                    matches!(selector, v1::transaction_selector::Selector::Begin(_)),
                    "Expected Selector::Begin on attempt 1"
                );

                let (sender, receiver) = channel(2);
                sender
                    .try_send(Err(Status::new(
                        Code::InvalidArgument,
                        "Table not found: non_existing_table",
                    )))
                    .expect("channel send should succeed");
                Ok(Response::from(receiver))
            });

        let (database_client, _server) = setup_db_client(mock).await;
        let runner = TransactionRunnerBuilder::new(database_client)
            .with_begin_transaction_option(BeginTransactionOption::InlineBegin)
            .build();

        let result = runner
            .run::<(), _>(async |transaction| {
                let request = ReadRequest::builder("non_existing_table", vec!["id"])
                    .with_keys(KeySet::all())
                    .build();
                // execute_read eagerly awaits stream initialization to fetch metadata.
                // The stream yields InvalidArgument immediately, so execute_read returns Err directly.
                let _result_set = transaction.execute_read(request).await?;
                panic!(
                    "execute_read eagerly initializes the stream chunk and must return Err directly"
                );
            })
            .await;

        assert!(result.is_err(), "Expected error from invalid argument read");
        let error = result.expect_err("Expected error");
        let status = error.status().expect("Expected gRPC status");
        assert_eq!(
            status.code,
            GaxCode::InvalidArgument,
            "Expected InvalidArgument error code"
        );

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn inlined_begin_query_stream_chunk2_fails_allows_commit() -> anyhow::Result<()> {
        let mut mock = create_session_mock();
        let mut sequence = Sequence::new();

        mock.expect_begin_transaction().never();
        mock.expect_rollback().never();

        let transaction_id = vec![11, 12, 13, 14];
        let transaction_id_for_query = transaction_id.clone();
        let transaction_id_for_commit = transaction_id.clone();

        mock.expect_execute_streaming_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |request| {
                let request = request.into_inner();
                let transaction = request
                    .transaction
                    .as_ref()
                    .expect("transaction options required for inline begin");
                let selector = transaction.selector.as_ref().expect("selector required");
                assert!(
                    matches!(selector, v1::transaction_selector::Selector::Begin(_)),
                    "Expected inline begin option"
                );

                let partial_result_set = v1::PartialResultSet {
                    metadata: Some(v1::ResultSetMetadata {
                        row_type: Some(v1::StructType {
                            fields: vec![Default::default()],
                        }),
                        transaction: Some(v1::Transaction {
                            id: transaction_id_for_query.clone(),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    values: vec![ProtoValue {
                        kind: Some(Kind::StringValue("row1".to_string())),
                    }],
                    resume_token: b"token1".to_vec(),
                    ..Default::default()
                };

                let (sender, receiver) = channel(2);
                sender
                    .try_send(Ok(partial_result_set))
                    .expect("channel send should succeed for chunk 1");
                sender
                    .try_send(Err(Status::new(
                        Code::DataLoss,
                        "Stream data loss on second chunk",
                    )))
                    .expect("channel send should succeed for chunk 2");
                Ok(Response::from(receiver))
            });

        mock.expect_commit()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |request| {
                let request = request.into_inner();
                assert_eq!(
                    request.transaction,
                    Some(CommitTransaction::TransactionId(
                        transaction_id_for_commit.clone()
                    )),
                    "Expected commit with transaction ID on retry"
                );
                commit_response()
            });

        let (database_client, _server) = setup_db_client(mock).await;
        let runner = TransactionRunnerBuilder::new(database_client)
            .with_begin_transaction_option(BeginTransactionOption::InlineBegin)
            .build();

        let mut attempt_counter = 0;
        let result = runner
            .run(async |transaction| {
                attempt_counter += 1;
                let mut result_set = transaction
                    .execute_query("SELECT * FROM broken_table")
                    .await?;

                let first_row = result_set.next().await.expect("expected first row")?;
                assert_eq!(
                    first_row.raw_values()[0].0,
                    Kind::StringValue("row1".to_string()).into(),
                    "Expected row1 from first chunk"
                );

                // Note that the ? operator is intentionally NOT used on the second row read
                // so that the stream error is caught and handled rather than aborting early.
                let second_row_result = result_set.next().await;
                assert!(
                    second_row_result.is_some(),
                    "Expected stream error on second chunk"
                );
                let stream_error = second_row_result
                    .expect("second row option")
                    .expect_err("expected error on second chunk");
                let status = stream_error.status().expect("expected gRPC status");
                assert_eq!(
                    status.code,
                    GaxCode::DataLoss,
                    "Expected DataLoss error code on second chunk"
                );

                // Because chunk 1 already established the transaction ID, the transaction
                // is active and can commit without being retried or calling BeginTransaction.
                Ok(42)
            })
            .await?;

        assert_eq!(
            attempt_counter, 1,
            "Expected exactly 1 attempt without retries since transaction ID was already established"
        );
        assert_eq!(result.result, 42, "Expected successful closure result");

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn inlined_begin_query_stream_fails_and_retry_explicit_begin_fails() -> anyhow::Result<()>
    {
        let mut mock = create_session_mock();
        let mut sequence = Sequence::new();

        mock.expect_commit().never();
        mock.expect_rollback().never();

        // Attempt 1:
        // Initial streaming query with inline begin fails with InvalidArgument.
        mock.expect_execute_streaming_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |request| {
                let request = request.into_inner();
                let transaction = request
                    .transaction
                    .as_ref()
                    .expect("transaction options required for inline begin");
                let selector = transaction.selector.as_ref().expect("selector required");
                assert!(
                    matches!(selector, v1::transaction_selector::Selector::Begin(_)),
                    "Expected Selector::Begin on attempt 1"
                );

                let (sender, receiver) = channel(2);
                sender
                    .try_send(Err(Status::new(
                        Code::InvalidArgument,
                        "Table not found: non_existing_table",
                    )))
                    .expect("channel send should succeed");
                Ok(Response::from(receiver))
            });

        // Attempt 2:
        // Closure caught the error and called execute_update, which detected
        // FirstStatementFailed and returned Aborted ("Aborted due to failed initial statement").
        // TransactionRunner retries with force_explicit_begin = true.
        // Explicit BeginTransaction RPC fails with Internal.
        mock.expect_begin_transaction()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |_request| {
                Err(Status::new(
                    Code::Internal,
                    "Begin transaction failed due to an internal error",
                ))
            });

        let (database_client, _server) = setup_db_client(mock).await;
        let runner = TransactionRunnerBuilder::new(database_client)
            .with_begin_transaction_option(BeginTransactionOption::InlineBegin)
            .build();

        let mut attempt_counter = 0;
        let result = runner
            .run::<(), _>(async |transaction| {
                attempt_counter += 1;
                // Note that the ? operator is intentionally NOT used here so that the closure
                // does not return early on error. Instead, we manually inspect the result.
                let query_result = transaction
                    .execute_query("SELECT * FROM non_existing_table")
                    .await;
                assert!(
                    query_result.is_err(),
                    "Expected query to fail with InvalidArgument on attempt {}",
                    attempt_counter
                );

                let _updated_count = transaction
                    .execute_update("UPDATE Users SET active = true WHERE id = 1")
                    .await?;
                Ok(())
            })
            .await;

        assert_eq!(
            attempt_counter, 1,
            "Closure should not be entered on attempt 2 because explicit BeginTransaction failed"
        );
        assert!(
            result.is_err(),
            "Expected runner to return error when explicit BeginTransaction fails"
        );
        let error = result.expect_err("Expected error");
        let status = error.status().expect("Expected gRPC status");
        assert_eq!(
            status.code,
            GaxCode::Internal,
            "Expected Internal error code from failed BeginTransaction"
        );

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn inlined_begin_streaming_read_fails_with_invalid_argument_and_continues()
    -> anyhow::Result<()> {
        let mut mock = create_session_mock();
        let mut sequence = Sequence::new();

        let transaction_id = vec![21, 22, 23, 24];
        let transaction_id_for_begin = transaction_id.clone();
        let transaction_id_for_read = transaction_id.clone();
        let transaction_id_for_update = transaction_id.clone();
        let transaction_id_for_commit = transaction_id.clone();

        // Attempt 1:
        // Initial streaming read with inline begin fails with InvalidArgument.
        mock.expect_streaming_read()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |request| {
                let request = request.into_inner();
                let transaction = request
                    .transaction
                    .as_ref()
                    .expect("transaction options required for inline begin");
                let selector = transaction.selector.as_ref().expect("selector required");
                assert!(
                    matches!(selector, v1::transaction_selector::Selector::Begin(_)),
                    "Expected Selector::Begin on attempt 1"
                );

                let (sender, receiver) = channel(2);
                sender
                    .try_send(Err(Status::new(
                        Code::InvalidArgument,
                        "Table not found: non_existing_table",
                    )))
                    .expect("channel send should succeed");
                Ok(Response::from(receiver))
            });

        // Attempt 2:
        // Closure caught the error and called execute_update, which detected
        // FirstStatementFailed and returned Aborted.
        // TransactionRunner retries with force_explicit_begin = true.
        // 1. Explicit BeginTransaction RPC runs first.
        mock.expect_begin_transaction()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |_request| {
                Ok(Response::new(v1::Transaction {
                    id: transaction_id_for_begin.clone(),
                    ..Default::default()
                }))
            });

        // 2. Statement 1 (read) is re-executed inside the new transaction and fails again with InvalidArgument.
        mock.expect_streaming_read()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |request| {
                let request = request.into_inner();
                let transaction = request
                    .transaction
                    .as_ref()
                    .expect("transaction selector required");
                assert_eq!(
                    transaction.selector,
                    Some(v1::transaction_selector::Selector::Id(
                        transaction_id_for_read.clone()
                    )),
                    "Expected Selector::Id on retry read"
                );

                let (sender, receiver) = channel(2);
                sender
                    .try_send(Err(Status::new(
                        Code::InvalidArgument,
                        "Table not found: non_existing_table",
                    )))
                    .expect("channel send should succeed");
                Ok(Response::from(receiver))
            });

        // 3. Statement 2 (execute_update) runs within the explicitly started transaction and succeeds.
        mock.expect_execute_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |request| {
                let request = request.into_inner();
                let transaction = request
                    .transaction
                    .as_ref()
                    .expect("transaction selector required");
                assert_eq!(
                    transaction.selector,
                    Some(v1::transaction_selector::Selector::Id(
                        transaction_id_for_update.clone()
                    )),
                    "Expected Selector::Id on retry update"
                );
                row_count_exact_response(1)
            });

        // 4. Commit runs and succeeds.
        mock.expect_commit()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |request| {
                let request = request.into_inner();
                assert_eq!(
                    request.transaction,
                    Some(CommitTransaction::TransactionId(
                        transaction_id_for_commit.clone()
                    )),
                    "Expected commit with transaction ID on retry"
                );
                commit_response()
            });

        let (database_client, _server) = setup_db_client(mock).await;
        let runner = TransactionRunnerBuilder::new(database_client)
            .with_begin_transaction_option(BeginTransactionOption::InlineBegin)
            .build();

        let mut attempt_counter = 0;
        let result = runner
            .run(async |transaction| {
                attempt_counter += 1;
                let request = ReadRequest::builder("non_existing_table", vec!["id"])
                    .with_keys(KeySet::all())
                    .build();
                // Note that the ? operator is intentionally NOT used here so that the closure
                // does not return early on error. Instead, we manually inspect the result.
                let read_result = transaction.execute_read(request).await;
                assert!(
                    read_result.is_err(),
                    "Expected read to fail with InvalidArgument on attempt {}",
                    attempt_counter
                );

                let updated_count = transaction
                    .execute_update("UPDATE Users SET active = true WHERE id = 1")
                    .await?;
                Ok(updated_count)
            })
            .await?;

        assert_eq!(
            attempt_counter, 2,
            "Expected exactly 2 attempts due to retry after failed initial read statement"
        );
        assert_eq!(result.result, 1, "Expected 1 row updated on success");

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn inlined_begin_query_stream_succeeds_and_subsequent_statement_fails_uncaught_rolls_back()
    -> anyhow::Result<()> {
        let mut mock = create_session_mock();
        let mut sequence = Sequence::new();

        mock.expect_begin_transaction().never();
        mock.expect_commit().never();

        let transaction_id = vec![31, 32, 33, 34];
        let transaction_id_for_query = transaction_id.clone();
        let transaction_id_for_rollback = transaction_id.clone();

        // 1. Initial streaming query with inline begin succeeds and returns transaction ID.
        mock.expect_execute_streaming_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |request| {
                let request = request.into_inner();
                let transaction = request
                    .transaction
                    .as_ref()
                    .expect("transaction options required for inline begin");
                let selector = transaction.selector.as_ref().expect("selector required");
                assert!(
                    matches!(selector, v1::transaction_selector::Selector::Begin(_)),
                    "Expected Selector::Begin on attempt 1"
                );

                let partial_result_set = v1::PartialResultSet {
                    metadata: Some(v1::ResultSetMetadata {
                        row_type: Some(v1::StructType {
                            fields: vec![Default::default()],
                        }),
                        transaction: Some(v1::Transaction {
                            id: transaction_id_for_query.clone(),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    values: vec![ProtoValue {
                        kind: Some(Kind::StringValue("1".to_string())),
                    }],
                    last: true,
                    ..Default::default()
                };

                let (sender, receiver) = channel(2);
                sender
                    .try_send(Ok(partial_result_set))
                    .expect("channel send should succeed");
                Ok(Response::from(receiver))
            });

        // 2. Statement 2 (execute_update) fails with InvalidArgument (uncaught).
        mock.expect_execute_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |_request| {
                Err(Status::new(
                    Code::InvalidArgument,
                    "Invalid update statement",
                ))
            });

        // 3. Rollback must be dispatched with the transaction ID established by statement 1.
        mock.expect_rollback()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |request| {
                let request = request.into_inner();
                assert_eq!(
                    request.transaction_id, transaction_id_for_rollback,
                    "Expected rollback to use transaction ID established by statement 1"
                );
                Ok(Response::new(()))
            });

        let (database_client, _server) = setup_db_client(mock).await;
        let runner = TransactionRunnerBuilder::new(database_client)
            .with_begin_transaction_option(BeginTransactionOption::InlineBegin)
            .build();

        let result = runner
            .run::<(), _>(async |transaction| {
                let mut result_set = transaction.execute_query("SELECT 1").await?;
                let _row = result_set.next().await.expect("Expected row")?;

                // Statement 2 fails uncaught (propagated via ?)
                let _count = transaction
                    .execute_update("UPDATE non_existing_table SET col = 1")
                    .await?;
                Ok(())
            })
            .await;

        assert!(
            result.is_err(),
            "Expected runner to return error from uncaught failure"
        );
        let error = result.expect_err("Expected error");
        let status = error.status().expect("Expected gRPC status");
        assert_eq!(
            status.code,
            GaxCode::InvalidArgument,
            "Expected InvalidArgument error code"
        );

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn inlined_begin_query_stream_fails_and_buffer_mutations_commits_on_retry()
    -> anyhow::Result<()> {
        let mut mock = create_session_mock();
        let mut sequence = Sequence::new();

        let transaction_id = vec![41, 42, 43, 44];
        let transaction_id_for_begin = transaction_id.clone();
        let transaction_id_for_query = transaction_id.clone();
        let transaction_id_for_commit = transaction_id.clone();

        // Attempt 1:
        // Initial streaming query with inline begin fails with InvalidArgument.
        mock.expect_execute_streaming_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |request| {
                let request = request.into_inner();
                let transaction = request
                    .transaction
                    .as_ref()
                    .expect("transaction options required for inline begin");
                let selector = transaction.selector.as_ref().expect("selector required");
                assert!(
                    matches!(selector, v1::transaction_selector::Selector::Begin(_)),
                    "Expected Selector::Begin on attempt 1"
                );

                let (sender, receiver) = channel(2);
                sender
                    .try_send(Err(Status::new(
                        Code::InvalidArgument,
                        "Table not found: non_existing_table",
                    )))
                    .expect("channel send should succeed");
                Ok(Response::from(receiver))
            });

        // Attempt 2:
        // Closure caught the error, buffered a mutation, and returned Ok(()).
        // commit() detects FirstStatementFailed via check_failed() and returns Aborted.
        // TransactionRunner retries with force_explicit_begin = true.
        // 1. Explicit BeginTransaction RPC runs first.
        mock.expect_begin_transaction()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |_request| {
                Ok(Response::new(v1::Transaction {
                    id: transaction_id_for_begin.clone(),
                    ..Default::default()
                }))
            });

        // 2. Query is re-executed inside the new transaction and fails again with InvalidArgument.
        mock.expect_execute_streaming_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |request| {
                let request = request.into_inner();
                let transaction = request
                    .transaction
                    .as_ref()
                    .expect("transaction selector required");
                assert_eq!(
                    transaction.selector,
                    Some(v1::transaction_selector::Selector::Id(
                        transaction_id_for_query.clone()
                    )),
                    "Expected Selector::Id on retry query"
                );

                let (sender, receiver) = channel(2);
                sender
                    .try_send(Err(Status::new(
                        Code::InvalidArgument,
                        "Table not found: non_existing_table",
                    )))
                    .expect("channel send should succeed");
                Ok(Response::from(receiver))
            });

        // 3. Commit runs with the buffered mutation within the explicit transaction and succeeds.
        mock.expect_commit()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |request| {
                let request = request.into_inner();
                assert_eq!(
                    request.transaction,
                    Some(CommitTransaction::TransactionId(
                        transaction_id_for_commit.clone()
                    )),
                    "Expected commit with transaction ID on retry"
                );
                assert_eq!(request.mutations.len(), 1, "Expected 1 buffered mutation");
                commit_response()
            });

        let (database_client, _server) = setup_db_client(mock).await;
        let runner = TransactionRunnerBuilder::new(database_client)
            .with_begin_transaction_option(BeginTransactionOption::InlineBegin)
            .build();

        let mut attempt_counter = 0;
        let result = runner
            .run(async |transaction| {
                attempt_counter += 1;
                // Note that the ? operator is intentionally NOT used here so that the closure
                // does not return early on error. Instead, we manually inspect the result.
                let query_result = transaction
                    .execute_query("SELECT * FROM non_existing_table")
                    .await;
                assert!(
                    query_result.is_err(),
                    "Expected query to fail with InvalidArgument on attempt {}",
                    attempt_counter
                );

                // Buffer a mutation without executing any other SQL statements.
                // commit() must detect FirstStatementFailed and abort rather than failing with missing transaction ID.
                let mutation = Mutation::new_insert_builder("Users")
                    .set("id")
                    .to(1)
                    .build();
                transaction.buffer([mutation])?;
                Ok(())
            })
            .await?;

        assert_eq!(
            attempt_counter, 2,
            "Expected exactly 2 attempts due to retry after failed initial statement caught before commit"
        );
        assert_eq!(result.result, (), "Expected successful closure result");

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn inlined_begin_query_send_error_retries_with_explicit_begin() -> anyhow::Result<()> {
        let mut mock = create_session_mock();
        let mut sequence = Sequence::new();

        let transaction_id = vec![51, 52, 53, 54];
        let transaction_id_for_begin = transaction_id.clone();
        let transaction_id_for_query = transaction_id.clone();
        let transaction_id_for_update = transaction_id.clone();
        let transaction_id_for_commit = transaction_id.clone();

        // Attempt 1:
        // ExecuteStreamingSql returns Err directly from send().await (transport failure).
        mock.expect_execute_streaming_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |request| {
                let request = request.into_inner();
                let transaction = request
                    .transaction
                    .as_ref()
                    .expect("transaction options required for inline begin");
                let selector = transaction.selector.as_ref().expect("selector required");
                assert!(
                    matches!(selector, v1::transaction_selector::Selector::Begin(_)),
                    "Expected Selector::Begin on attempt 1"
                );

                Err(Status::new(
                    Code::InvalidArgument,
                    "Table not found on direct send",
                ))
            });

        // Attempt 2:
        // 1. Explicit BeginTransaction RPC runs first.
        mock.expect_begin_transaction()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |_request| {
                Ok(Response::new(v1::Transaction {
                    id: transaction_id_for_begin.clone(),
                    ..Default::default()
                }))
            });

        // 2. Query fails again on send().
        mock.expect_execute_streaming_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |request| {
                let request = request.into_inner();
                let transaction = request
                    .transaction
                    .as_ref()
                    .expect("transaction selector required");
                assert_eq!(
                    transaction.selector,
                    Some(v1::transaction_selector::Selector::Id(
                        transaction_id_for_query.clone()
                    )),
                    "Expected Selector::Id on retry query"
                );

                Err(Status::new(
                    Code::InvalidArgument,
                    "Table not found on direct send",
                ))
            });

        // 3. Statement 2 (execute_update) runs within the explicit transaction and succeeds.
        mock.expect_execute_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |request| {
                let request = request.into_inner();
                let transaction = request
                    .transaction
                    .as_ref()
                    .expect("transaction selector required");
                assert_eq!(
                    transaction.selector,
                    Some(v1::transaction_selector::Selector::Id(
                        transaction_id_for_update.clone()
                    )),
                    "Expected Selector::Id on retry update"
                );
                row_count_exact_response(1)
            });

        // 4. Commit runs and succeeds.
        mock.expect_commit()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |request| {
                let request = request.into_inner();
                assert_eq!(
                    request.transaction,
                    Some(CommitTransaction::TransactionId(
                        transaction_id_for_commit.clone()
                    )),
                    "Expected commit with transaction ID on retry"
                );
                commit_response()
            });

        let (database_client, _server) = setup_db_client(mock).await;
        let runner = TransactionRunnerBuilder::new(database_client)
            .with_begin_transaction_option(BeginTransactionOption::InlineBegin)
            .build();

        let mut attempt_counter = 0;
        let result = runner
            .run(async |transaction| {
                attempt_counter += 1;
                let query_result = transaction
                    .execute_query("SELECT * FROM non_existing_table")
                    .await;
                assert!(
                    query_result.is_err(),
                    "Expected query to fail on attempt {}",
                    attempt_counter
                );

                let updated_count = transaction
                    .execute_update("UPDATE Users SET active = true WHERE id = 1")
                    .await?;
                Ok(updated_count)
            })
            .await?;

        assert_eq!(
            attempt_counter, 2,
            "Expected 2 attempts due to retry after direct send error"
        );
        assert_eq!(result.result, 1, "Expected 1 row updated on success");

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn inlined_begin_query_stream_empty_error_fails_and_retries_with_explicit_begin()
    -> anyhow::Result<()> {
        let mut mock = create_session_mock();
        let mut sequence = Sequence::new();

        let transaction_id = vec![61, 62, 63, 64];
        let transaction_id_for_begin = transaction_id.clone();
        let transaction_id_for_query = transaction_id.clone();
        let transaction_id_for_update = transaction_id.clone();
        let transaction_id_for_commit = transaction_id.clone();

        // Attempt 1:
        // ExecuteStreamingSql opens stream but drops channel immediately without sending any chunk.
        mock.expect_execute_streaming_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |request| {
                let request = request.into_inner();
                let transaction = request
                    .transaction
                    .as_ref()
                    .expect("transaction options required for inline begin");
                let selector = transaction.selector.as_ref().expect("selector required");
                assert!(
                    matches!(selector, v1::transaction_selector::Selector::Begin(_)),
                    "Expected Selector::Begin on attempt 1"
                );

                let (sender, receiver) = channel(1);
                drop(sender);
                Ok(Response::from(receiver))
            });

        // Attempt 2:
        // 1. Explicit BeginTransaction RPC runs first.
        mock.expect_begin_transaction()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |_request| {
                Ok(Response::new(v1::Transaction {
                    id: transaction_id_for_begin.clone(),
                    ..Default::default()
                }))
            });

        // 2. Query succeeds on retry.
        mock.expect_execute_streaming_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |request| {
                let request = request.into_inner();
                let transaction = request
                    .transaction
                    .as_ref()
                    .expect("transaction selector required");
                assert_eq!(
                    transaction.selector,
                    Some(v1::transaction_selector::Selector::Id(
                        transaction_id_for_query.clone()
                    )),
                    "Expected Selector::Id on retry query"
                );

                let partial_result_set = v1::PartialResultSet {
                    metadata: Some(v1::ResultSetMetadata {
                        row_type: Some(v1::StructType {
                            fields: vec![Default::default()],
                        }),
                        ..Default::default()
                    }),
                    values: vec![ProtoValue {
                        kind: Some(Kind::StringValue("1".to_string())),
                    }],
                    last: true,
                    ..Default::default()
                };

                let (sender, receiver) = channel(2);
                sender
                    .try_send(Ok(partial_result_set))
                    .expect("channel send should succeed");
                Ok(Response::from(receiver))
            });

        // 3. Statement 2 (execute_update) runs within the explicit transaction and succeeds.
        mock.expect_execute_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |request| {
                let request = request.into_inner();
                let transaction = request
                    .transaction
                    .as_ref()
                    .expect("transaction selector required");
                assert_eq!(
                    transaction.selector,
                    Some(v1::transaction_selector::Selector::Id(
                        transaction_id_for_update.clone()
                    )),
                    "Expected Selector::Id on retry update"
                );
                row_count_exact_response(1)
            });

        // 4. Commit runs and succeeds.
        mock.expect_commit()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |request| {
                let request = request.into_inner();
                assert_eq!(
                    request.transaction,
                    Some(CommitTransaction::TransactionId(
                        transaction_id_for_commit.clone()
                    )),
                    "Expected commit with transaction ID on retry"
                );
                commit_response()
            });

        let (database_client, _server) = setup_db_client(mock).await;
        let runner = TransactionRunnerBuilder::new(database_client)
            .with_begin_transaction_option(BeginTransactionOption::InlineBegin)
            .build();

        let mut attempt_counter = 0;
        let result = runner
            .run(async |transaction| {
                attempt_counter += 1;
                if attempt_counter == 1 {
                    let query_result = transaction.execute_query("SELECT 1").await;
                    assert!(
                        query_result.is_err(),
                        "Expected query to fail on attempt 1 due to empty stream"
                    );
                } else {
                    let mut result_set = transaction.execute_query("SELECT 1").await?;
                    let row = result_set.next().await.expect("Expected row")?;
                    assert_eq!(
                        row.raw_values()[0].0,
                        Kind::StringValue("1".to_string()).into(),
                        "Expected query value 1"
                    );
                }

                let updated_count = transaction
                    .execute_update("UPDATE Users SET active = true WHERE id = 1")
                    .await?;
                Ok(updated_count)
            })
            .await?;

        assert_eq!(
            attempt_counter, 2,
            "Expected 2 attempts due to retry after empty stream error"
        );
        assert_eq!(result.result, 1, "Expected 1 row updated on success");

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn inlined_begin_query_transient_retry_connection_failure_retries_with_explicit_begin()
    -> anyhow::Result<()> {
        let mut mock = create_session_mock();
        let mut sequence = Sequence::new();

        let transaction_id = vec![61, 62, 63, 64];
        let transaction_id_for_begin = transaction_id.clone();
        let transaction_id_for_query = transaction_id.clone();
        let transaction_id_for_update = transaction_id.clone();
        let transaction_id_for_commit = transaction_id.clone();

        // Attempt 1:
        // 1. Initial query stream with inline begin fails immediately with UNAVAILABLE.
        mock.expect_execute_streaming_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |request| {
                let request = request.into_inner();
                let transaction = request
                    .transaction
                    .as_ref()
                    .expect("transaction options required for inline begin");
                let selector = transaction.selector.as_ref().expect("selector required");
                assert!(
                    matches!(selector, v1::transaction_selector::Selector::Begin(_)),
                    "Expected Selector::Begin on attempt 1"
                );

                let (sender, receiver) = channel(1);
                sender
                    .try_send(Err(Status::new(
                        Code::Unavailable,
                        "Transient initial stream failure",
                    )))
                    .expect("channel send should succeed");
                Ok(Response::from(receiver))
            });

        // 2. ResultSet attempts to restart stream, but the reconnection fails with a non-retryable error.
        mock.expect_execute_streaming_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |_request| {
                Err(Status::new(
                    Code::InvalidArgument,
                    "Invalid argument during restart",
                ))
            });

        // Attempt 2:
        // 3. Fallback to explicit BeginTransaction RPC.
        mock.expect_begin_transaction()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |_request| {
                Ok(Response::new(v1::Transaction {
                    id: transaction_id_for_begin.clone(),
                    ..Default::default()
                }))
            });

        // 4. Query re-executes on attempt 2 using the explicit transaction ID.
        mock.expect_execute_streaming_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |request| {
                let request = request.into_inner();
                let transaction = request
                    .transaction
                    .as_ref()
                    .expect("transaction selector required");
                assert_eq!(
                    transaction.selector,
                    Some(v1::transaction_selector::Selector::Id(
                        transaction_id_for_query.clone()
                    )),
                    "Expected Selector::Id on retry query"
                );

                let partial_result_set = v1::PartialResultSet {
                    metadata: Some(v1::ResultSetMetadata {
                        row_type: Some(v1::StructType {
                            fields: vec![Default::default()],
                        }),
                        ..Default::default()
                    }),
                    values: vec![ProtoValue {
                        kind: Some(Kind::StringValue("1".to_string())),
                    }],
                    last: true,
                    ..Default::default()
                };

                let (sender, receiver) = channel(2);
                sender
                    .try_send(Ok(partial_result_set))
                    .expect("channel send should succeed");
                Ok(Response::from(receiver))
            });

        // 5. Update executes within the transaction.
        mock.expect_execute_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |request| {
                let request = request.into_inner();
                let transaction = request
                    .transaction
                    .as_ref()
                    .expect("transaction selector required");
                assert_eq!(
                    transaction.selector,
                    Some(v1::transaction_selector::Selector::Id(
                        transaction_id_for_update.clone()
                    )),
                    "Expected Selector::Id on retry update"
                );
                row_count_exact_response(1)
            });

        // 6. Commit succeeds.
        mock.expect_commit()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |request| {
                let request = request.into_inner();
                assert_eq!(
                    request.transaction,
                    Some(CommitTransaction::TransactionId(
                        transaction_id_for_commit.clone()
                    )),
                    "Expected commit with transaction ID on retry"
                );
                commit_response()
            });

        let (db_client, _server) = setup_db_client(mock).await;
        let runner = TransactionRunnerBuilder::new(db_client)
            .with_begin_transaction_option(BeginTransactionOption::InlineBegin)
            .build();

        let mut attempt_counter = 0;
        let result = runner
            .run(async |transaction| {
                attempt_counter += 1;
                if attempt_counter == 1 {
                    let statement = Statement::builder("SELECT 1")
                        .with_backoff_policy(
                            ExponentialBackoffBuilder::new()
                                .with_initial_delay(StdDuration::from_nanos(1))
                                .clamp(),
                        )
                        .build();
                    let query_result = transaction.execute_query(statement).await;
                    assert!(
                        query_result.is_err(),
                        "Expected query to fail after restart_stream reconnection error on attempt 1"
                    );
                } else {
                    let mut result_set = transaction.execute_query("SELECT 1").await?;
                    let row = result_set.next().await.expect("Expected row")?;
                    assert_eq!(
                        row.raw_values()[0].0,
                        Kind::StringValue("1".to_string()).into(),
                        "Expected query value 1"
                    );
                }

                let updated_count = transaction
                    .execute_update("UPDATE Users SET active = true WHERE id = 1")
                    .await?;
                Ok(updated_count)
            })
            .await?;

        assert_eq!(
            attempt_counter, 2,
            "Expected 2 attempts due to retry after restart_stream failure"
        );
        assert_eq!(result.result, 1, "Expected 1 row updated on success");

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn inlined_begin_execute_update_missing_transaction_id_retries_with_explicit_begin()
    -> anyhow::Result<()> {
        let mut mock = create_session_mock();
        let mut sequence = Sequence::new();

        let transaction_id = vec![81, 82, 83, 84];
        let transaction_id_for_begin = transaction_id.clone();
        let transaction_id_for_update = transaction_id.clone();
        let transaction_id_for_query = transaction_id.clone();
        let transaction_id_for_commit = transaction_id.clone();

        // Attempt 1:
        // 1. Initial unary update with inline begin succeeds at gRPC layer,
        //    but Spanner returns a ResultSet without a transaction entity.
        mock.expect_execute_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |request| {
                let request = request.into_inner();
                let transaction = request
                    .transaction
                    .as_ref()
                    .expect("transaction options required for inline begin on attempt 1");
                let selector = transaction.selector.as_ref().expect("selector required");
                assert!(
                    matches!(selector, v1::transaction_selector::Selector::Begin(_)),
                    "Expected inline begin option on attempt 1"
                );
                row_count_exact_response(1)
            });

        // Attempt 2:
        // 2. Explicit BeginTransaction RPC called after failed initial statement.
        mock.expect_begin_transaction()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |_request| {
                Ok(Response::new(v1::Transaction {
                    id: transaction_id_for_begin.clone(),
                    ..Default::default()
                }))
            });

        // 3. Update re-executed within explicit transaction on attempt 2.
        mock.expect_execute_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |request| {
                let request = request.into_inner();
                let transaction = request
                    .transaction
                    .as_ref()
                    .expect("transaction selector required on attempt 2 update");
                assert_eq!(
                    transaction.selector,
                    Some(v1::transaction_selector::Selector::Id(
                        transaction_id_for_update.clone()
                    )),
                    "Expected update to use established transaction ID on attempt 2"
                );
                row_count_exact_response(1)
            });

        // 4. Subsequent query within attempt 2 succeeds.
        mock.expect_execute_streaming_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |request| {
                let request = request.into_inner();
                let transaction = request
                    .transaction
                    .as_ref()
                    .expect("transaction selector required on attempt 2 query");
                assert_eq!(
                    transaction.selector,
                    Some(v1::transaction_selector::Selector::Id(
                        transaction_id_for_query.clone()
                    )),
                    "Expected query to use established transaction ID on attempt 2"
                );

                let partial_result_set = v1::PartialResultSet {
                    metadata: Some(v1::ResultSetMetadata {
                        row_type: Some(v1::StructType {
                            fields: vec![Default::default()],
                        }),
                        ..Default::default()
                    }),
                    values: vec![ProtoValue {
                        kind: Some(Kind::StringValue("1".to_string())),
                    }],
                    last: true,
                    ..Default::default()
                };

                let (sender, receiver) = channel(2);
                sender
                    .try_send(Ok(partial_result_set))
                    .expect("channel send should succeed");
                Ok(Response::from(receiver))
            });

        // 5. Commit succeeds on attempt 2.
        mock.expect_commit()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |request| {
                let request = request.into_inner();
                assert_eq!(
                    request.transaction,
                    Some(CommitTransaction::TransactionId(
                        transaction_id_for_commit.clone()
                    )),
                    "Expected commit to use established transaction ID on attempt 2"
                );
                commit_response()
            });

        let (database_client, _server) = setup_db_client(mock).await;
        let runner = TransactionRunnerBuilder::new(database_client)
            .with_begin_transaction_option(BeginTransactionOption::InlineBegin)
            .build();

        let mut attempt_counter = 0;
        let result = runner
            .run(async |transaction| {
                attempt_counter += 1;
                if attempt_counter == 1 {
                    let update_result = transaction
                        .execute_update("UPDATE Users SET active = true WHERE id = 1")
                        .await;
                    assert!(
                        update_result.is_err(),
                        "Expected update to fail due to missing transaction ID on attempt 1"
                    );
                    let error_message = update_result
                        .expect_err("update failed on attempt 1")
                        .to_string();
                    assert!(
                        error_message.contains("Transaction ID was not returned by Spanner"),
                        "Expected missing transaction ID error message, got: {error_message}"
                    );
                    // Application catches error and continues with a subsequent query.
                    // The subsequent statement detects FirstStatementFailed and returns Aborted.
                    let query_result = transaction.execute_query("SELECT 1").await;
                    assert!(
                        query_result.is_err(),
                        "Expected subsequent query on attempt 1 to fail fast"
                    );
                    let query_error =
                        query_result.expect_err("subsequent query failed on attempt 1");
                    assert!(
                        query_error
                            .to_string()
                            .contains("Aborted due to failed initial statement"),
                        "Expected aborted due to failed initial statement, got: {query_error}"
                    );
                    return Err(query_error);
                }

                let updated_count = transaction
                    .execute_update("UPDATE Users SET active = true WHERE id = 1")
                    .await?;
                let mut result_set = transaction.execute_query("SELECT 1").await?;
                let row = result_set.next().await.expect("Expected row")?;
                assert_eq!(
                    row.raw_values()[0].0,
                    Kind::StringValue("1".to_string()).into(),
                    "Expected query value 1"
                );

                Ok(updated_count)
            })
            .await?;

        assert_eq!(
            attempt_counter, 2,
            "Expected 2 attempts due to retry after missing transaction ID"
        );
        assert_eq!(result.result, 1, "Expected 1 row updated on success");

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn inlined_begin_query_transient_stream_restart_recovers_within_retry_loop()
    -> anyhow::Result<()> {
        let mut mock = create_session_mock();
        let mut sequence = Sequence::new();

        mock.expect_begin_transaction().never();
        mock.expect_rollback().never();

        let transaction_id = vec![91, 92, 93, 94];
        let transaction_id_for_query = transaction_id.clone();
        let transaction_id_for_commit = transaction_id.clone();

        // 1. Initial streaming query with inline begin fails on reading initial chunk with Unavailable.
        mock.expect_execute_streaming_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |request| {
                let request = request.into_inner();
                let transaction = request
                    .transaction
                    .as_ref()
                    .expect("transaction options required for inline begin");
                let selector = transaction.selector.as_ref().expect("selector required");
                assert!(
                    matches!(selector, v1::transaction_selector::Selector::Begin(_)),
                    "Expected inline begin option on attempt 1"
                );

                let (sender, receiver) = channel(1);
                sender
                    .try_send(Err(Status::new(
                        Code::Unavailable,
                        "Transient initial stream chunk failure",
                    )))
                    .expect("channel send should succeed");
                Ok(Response::from(receiver))
            });

        // 2. restart_stream() attempt 1 fails on send() with Unavailable.
        mock.expect_execute_streaming_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |request| {
                let request = request.into_inner();
                let transaction = request
                    .transaction
                    .as_ref()
                    .expect("transaction options required for restarted stream");
                let selector = transaction.selector.as_ref().expect("selector required");
                assert!(
                    matches!(selector, v1::transaction_selector::Selector::Begin(_)),
                    "Expected inline begin option on restarted stream attempt 1"
                );
                Err(Status::new(
                    Code::Unavailable,
                    "Connection refused during restart attempt 1",
                ))
            });

        // 3. restart_stream() attempt 2 on loop succeeds and yields metadata with transaction ID and row.
        mock.expect_execute_streaming_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |request| {
                let request = request.into_inner();
                let transaction = request
                    .transaction
                    .as_ref()
                    .expect("transaction options required for restarted stream attempt 2");
                let selector = transaction.selector.as_ref().expect("selector required");
                assert!(
                    matches!(selector, v1::transaction_selector::Selector::Begin(_)),
                    "Expected inline begin option on restarted stream attempt 2"
                );

                let partial_result_set = v1::PartialResultSet {
                    metadata: Some(v1::ResultSetMetadata {
                        row_type: Some(v1::StructType {
                            fields: vec![Default::default()],
                        }),
                        transaction: Some(v1::Transaction {
                            id: transaction_id_for_query.clone(),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    values: vec![ProtoValue {
                        kind: Some(Kind::StringValue("1".to_string())),
                    }],
                    last: true,
                    ..Default::default()
                };

                let (sender, receiver) = channel(2);
                sender
                    .try_send(Ok(partial_result_set))
                    .expect("channel send should succeed");
                Ok(Response::from(receiver))
            });

        // 4. Commit succeeds using the transaction ID established by the restarted stream.
        mock.expect_commit()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |request| {
                let request = request.into_inner();
                assert_eq!(
                    request.transaction,
                    Some(CommitTransaction::TransactionId(
                        transaction_id_for_commit.clone()
                    )),
                    "Expected commit to use established transaction ID"
                );
                commit_response()
            });

        let (database_client, _server) = setup_db_client(mock).await;
        let runner = TransactionRunnerBuilder::new(database_client)
            .with_begin_transaction_option(BeginTransactionOption::InlineBegin)
            .build();

        let mut attempt_counter = 0;
        let result = runner
            .run(async |transaction| {
                attempt_counter += 1;
                let statement = Statement::builder("SELECT 1")
                    .with_backoff_policy(
                        ExponentialBackoffBuilder::new()
                            .with_initial_delay(StdDuration::from_nanos(1))
                            .clamp(),
                    )
                    .build();
                let mut result_set = transaction.execute_query(statement).await?;
                let row = result_set.next().await.expect("Expected row")?;
                assert_eq!(
                    row.raw_values()[0].0,
                    Kind::StringValue("1".to_string()).into(),
                    "Expected query value 1"
                );
                Ok(())
            })
            .await?;

        assert_eq!(
            attempt_counter, 1,
            "Expected exactly 1 runner attempt because stream restart recovered within handle_stream_error"
        );
        assert_eq!(result.result, (), "Expected successful execution");

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn inlined_begin_concurrent_operations_leader_fails_with_invalid_argument_retries_explicit()
    -> anyhow::Result<()> {
        let mut mock = create_session_mock();
        let mut sequence = Sequence::new();

        let transaction_id = vec![111, 112, 113, 114];
        let transaction_id_for_begin = transaction_id.clone();
        let transaction_id_for_query = transaction_id.clone();
        let transaction_id_for_update = transaction_id.clone();
        let transaction_id_for_commit = transaction_id.clone();

        let leader_started_notify = Arc::new(Notify::new());
        let leader_started_notify_clone = Arc::clone(&leader_started_notify);

        // Attempt 1:
        // 1. Task 1 (leader) starts streaming query with inline begin.
        //    It signals when the RPC is initiated, and then fails with InvalidArgument.
        mock.expect_execute_streaming_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |request| {
                let request = request.into_inner();
                let transaction = request
                    .transaction
                    .as_ref()
                    .expect("transaction options required for inline begin on attempt 1");
                let selector = transaction.selector.as_ref().expect("selector required");
                assert!(
                    matches!(selector, v1::transaction_selector::Selector::Begin(_)),
                    "Expected inline begin option for leader query"
                );

                leader_started_notify_clone.notify_one();

                let (sender, receiver) = channel(2);
                sender
                    .try_send(Err(Status::new(
                        Code::InvalidArgument,
                        "Table not found: invalid_table",
                    )))
                    .expect("channel send should succeed");
                Ok(Response::from(receiver))
            });

        // Attempt 2:
        // 2. Explicit begin transaction after leader failure.
        mock.expect_begin_transaction()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |_request| {
                Ok(Response::new(v1::Transaction {
                    id: transaction_id_for_begin.clone(),
                    ..Default::default()
                }))
            });

        // 3. Query on attempt 2 succeeds using explicit transaction ID.
        mock.expect_execute_streaming_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |request| {
                let request = request.into_inner();
                let transaction = request
                    .transaction
                    .as_ref()
                    .expect("transaction selector required on attempt 2 query");
                assert_eq!(
                    transaction.selector,
                    Some(v1::transaction_selector::Selector::Id(
                        transaction_id_for_query.clone()
                    )),
                    "Expected Selector::Id on retry query"
                );

                let partial_result_set = v1::PartialResultSet {
                    metadata: Some(v1::ResultSetMetadata {
                        row_type: Some(v1::StructType {
                            fields: vec![Default::default()],
                        }),
                        ..Default::default()
                    }),
                    values: vec![ProtoValue {
                        kind: Some(Kind::StringValue("1".to_string())),
                    }],
                    last: true,
                    ..Default::default()
                };

                let (sender, receiver) = channel(2);
                sender
                    .try_send(Ok(partial_result_set))
                    .expect("channel send should succeed");
                Ok(Response::from(receiver))
            });

        // 4. Update on attempt 2 succeeds using explicit transaction ID.
        mock.expect_execute_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |request| {
                let request = request.into_inner();
                let transaction = request
                    .transaction
                    .as_ref()
                    .expect("transaction selector required on attempt 2 update");
                assert_eq!(
                    transaction.selector,
                    Some(v1::transaction_selector::Selector::Id(
                        transaction_id_for_update.clone()
                    )),
                    "Expected Selector::Id on retry update"
                );
                row_count_exact_response(1)
            });

        // 5. Commit succeeds on attempt 2.
        mock.expect_commit()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |request| {
                let request = request.into_inner();
                assert_eq!(
                    request.transaction,
                    Some(CommitTransaction::TransactionId(
                        transaction_id_for_commit.clone()
                    )),
                    "Expected commit with transaction ID on retry"
                );
                commit_response()
            });

        let (database_client, _server) = setup_db_client(mock).await;
        let runner = TransactionRunnerBuilder::new(database_client)
            .with_begin_transaction_option(BeginTransactionOption::InlineBegin)
            .build();

        let mut attempt_counter = 0;
        let result = runner
            .run(async |transaction| {
                attempt_counter += 1;
                if attempt_counter == 1 {
                    let transaction_clone = transaction.clone();
                    let leader_started_clone = Arc::clone(&leader_started_notify);

                    // Task 1: Leader executing query with inline begin
                    let leader_handle = spawn(async move {
                        transaction_clone
                            .execute_query("SELECT * FROM invalid_table")
                            .await
                    });

                    // Wait until the leader has dispatched its RPC and placed selector in Starting state
                    leader_started_clone.notified().await;

                    // Task 2: Follower attempting to execute update while leader is in Starting state
                    let follower_result = transaction
                        .execute_update("UPDATE Users SET active = true WHERE id = 1")
                        .await;

                    let leader_result = leader_handle.await.expect("task join should succeed");

                    assert!(
                        leader_result.is_err(),
                        "Expected leader query to fail with InvalidArgument"
                    );
                    let leader_error = leader_result.expect_err("leader query failed").to_string();
                    assert!(
                        leader_error.contains("Table not found: invalid_table"),
                        "Expected leader error message: {leader_error}"
                    );

                    assert!(
                        follower_result.is_err(),
                        "Expected follower to receive error when awakened by failed leader"
                    );
                    let follower_error = follower_result.expect_err("follower update failed");
                    assert!(
                        follower_error
                            .to_string()
                            .contains("Aborted due to failed initial statement"),
                        "Expected follower to receive synthetic Aborted, got: {follower_error}"
                    );

                    return Err(follower_error);
                }

                // Attempt 2: Both query and update succeed
                let mut result_set = transaction.execute_query("SELECT 1").await?;
                let row = result_set.next().await.expect("Expected row")?;
                assert_eq!(
                    row.raw_values()[0].0,
                    Kind::StringValue("1".to_string()).into(),
                    "Expected query value 1"
                );

                let updated_count = transaction
                    .execute_update("UPDATE Users SET active = true WHERE id = 1")
                    .await?;
                Ok(updated_count)
            })
            .await?;

        assert_eq!(
            attempt_counter, 2,
            "Expected 2 attempts due to retry after concurrent leader failure"
        );
        assert_eq!(result.result, 1, "Expected 1 row updated on success");

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn inlined_begin_query_dml_then_return_fails_with_invalid_argument_retries_explicit()
    -> anyhow::Result<()> {
        let mut mock = create_session_mock();
        let mut sequence = Sequence::new();

        let transaction_id = vec![121, 122, 123, 124];
        let transaction_id_for_begin = transaction_id.clone();
        let transaction_id_for_dml = transaction_id.clone();
        let transaction_id_for_commit = transaction_id.clone();

        // Attempt 1:
        // 1. Initial DML with THEN RETURN executed via execute_query fails with InvalidArgument.
        mock.expect_execute_streaming_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |request| {
                let request = request.into_inner();
                let transaction = request
                    .transaction
                    .as_ref()
                    .expect("transaction options required for inline begin on attempt 1");
                let selector = transaction.selector.as_ref().expect("selector required");
                assert!(
                    matches!(selector, v1::transaction_selector::Selector::Begin(_)),
                    "Expected inline begin option for initial DML statement"
                );

                let (sender, receiver) = channel(2);
                sender
                    .try_send(Err(Status::new(
                        Code::InvalidArgument,
                        "Column 'non_existing' does not exist in table 'Users'",
                    )))
                    .expect("channel send should succeed");
                Ok(Response::from(receiver))
            });

        // Attempt 2:
        // 2. Fallback to explicit BeginTransaction RPC.
        mock.expect_begin_transaction()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |_request| {
                Ok(Response::new(v1::Transaction {
                    id: transaction_id_for_begin.clone(),
                    ..Default::default()
                }))
            });

        // 3. DML with THEN RETURN re-executes on attempt 2 using explicit transaction ID and succeeds.
        mock.expect_execute_streaming_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |request| {
                let request = request.into_inner();
                let transaction = request
                    .transaction
                    .as_ref()
                    .expect("transaction selector required on attempt 2");
                assert_eq!(
                    transaction.selector,
                    Some(v1::transaction_selector::Selector::Id(
                        transaction_id_for_dml.clone()
                    )),
                    "Expected attempt 2 to use explicit transaction ID"
                );

                let partial_result_set = v1::PartialResultSet {
                    metadata: Some(v1::ResultSetMetadata {
                        row_type: Some(v1::StructType {
                            fields: vec![Default::default()],
                        }),
                        ..Default::default()
                    }),
                    values: vec![ProtoValue {
                        kind: Some(Kind::StringValue("42".to_string())),
                    }],
                    last: true,
                    ..Default::default()
                };

                let (sender, receiver) = channel(2);
                sender
                    .try_send(Ok(partial_result_set))
                    .expect("channel send should succeed");
                Ok(Response::from(receiver))
            });

        // 4. Commit succeeds on attempt 2.
        mock.expect_commit()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |request| {
                let request = request.into_inner();
                assert_eq!(
                    request.transaction,
                    Some(CommitTransaction::TransactionId(
                        transaction_id_for_commit.clone()
                    )),
                    "Expected commit to use established transaction ID on attempt 2"
                );
                commit_response()
            });

        let (database_client, _server) = setup_db_client(mock).await;
        let runner = TransactionRunnerBuilder::new(database_client)
            .with_begin_transaction_option(BeginTransactionOption::InlineBegin)
            .build();

        let mut attempt_counter = 0;
        let result = runner
            .run(async |transaction| {
                attempt_counter += 1;
                if attempt_counter == 1 {
                    let dml_result = transaction
                        .execute_query(
                            "INSERT INTO Users (name, non_existing) VALUES ('Alice', 1) THEN RETURN id",
                        )
                        .await;
                    assert!(
                        dml_result.is_err(),
                        "Expected initial DML with THEN RETURN to fail on attempt 1"
                    );
                    let dml_error = dml_result
                        .expect_err("initial DML failed")
                        .to_string();
                    assert!(
                        dml_error.contains("Column 'non_existing' does not exist"),
                        "Expected column error message: {dml_error}"
                    );

                    // Continuing after error causes subsequent statement to detect FirstStatementFailed and return Aborted
                    let subsequent_query = transaction.execute_query("SELECT 1").await;
                    assert!(
                        subsequent_query.is_err(),
                        "Expected subsequent query on attempt 1 to fail fast"
                    );
                    let subsequent_error = subsequent_query
                        .expect_err("subsequent query failed");
                    assert!(
                        subsequent_error.to_string().contains("Aborted due to failed initial statement"),
                        "Expected aborted due to failed initial statement, got: {subsequent_error}"
                    );
                    return Err(subsequent_error);
                }

                // Attempt 2 succeeds
                let mut result_set = transaction
                    .execute_query(
                        "INSERT INTO Users (name) VALUES ('Alice') THEN RETURN id",
                    )
                    .await?;
                let row = result_set.next().await.expect("Expected returned row")?;
                assert_eq!(
                    row.raw_values()[0].0,
                    Kind::StringValue("42".to_string()).into(),
                    "Expected returned id 42"
                );
                Ok(())
            })
            .await?;

        assert_eq!(
            attempt_counter, 2,
            "Expected 2 attempts due to retry after failed initial DML with THEN RETURN"
        );
        assert_eq!(
            result.result,
            (),
            "Expected successful execution on attempt 2"
        );

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn inlined_begin_concurrent_operations_leader_aborts_retries_with_inline_begin()
    -> anyhow::Result<()> {
        let mut mock = create_session_mock();
        let mut sequence = Sequence::new();

        mock.expect_begin_transaction().never();
        mock.expect_rollback().never();

        let transaction_id = vec![201, 202, 203, 204];
        let transaction_id_for_query = transaction_id.clone();
        let transaction_id_for_update = transaction_id.clone();
        let transaction_id_for_commit = transaction_id.clone();

        let leader_started_notify = Arc::new(Notify::new());
        let leader_started_notify_clone = Arc::clone(&leader_started_notify);

        // Attempt 1:
        // 1. Task 1 (leader) starts streaming query with inline begin.
        //    It signals when the RPC is initiated, and then fails with Aborted.
        mock.expect_execute_streaming_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |request| {
                let request = request.into_inner();
                let transaction = request
                    .transaction
                    .as_ref()
                    .expect("transaction options required for inline begin on attempt 1");
                let selector = transaction.selector.as_ref().expect("selector required");
                assert!(
                    matches!(selector, v1::transaction_selector::Selector::Begin(_)),
                    "Expected inline begin option for leader query on attempt 1"
                );

                leader_started_notify_clone.notify_one();

                let (sender, receiver) = channel(2);
                sender
                    .try_send(Err(create_aborted_status(StdDuration::from_nanos(1))))
                    .expect("channel send should succeed");
                Ok(Response::from(receiver))
            });

        // Attempt 2:
        // 2. Query on attempt 2 succeeds using inline begin (BeginTransactionOption::InlineBegin retained).
        //    Crucially, NO explicit BeginTransaction RPC is executed!
        mock.expect_execute_streaming_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |request| {
                let request = request.into_inner();
                let transaction = request
                    .transaction
                    .as_ref()
                    .expect("transaction selector required on attempt 2 query");
                let selector = transaction.selector.as_ref().expect("selector required");
                assert!(
                    matches!(selector, v1::transaction_selector::Selector::Begin(_)),
                    "Expected inline begin option on attempt 2 query"
                );

                let partial_result_set = v1::PartialResultSet {
                    metadata: Some(v1::ResultSetMetadata {
                        row_type: Some(v1::StructType {
                            fields: vec![Default::default()],
                        }),
                        transaction: Some(v1::Transaction {
                            id: transaction_id_for_query.clone(),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    values: vec![ProtoValue {
                        kind: Some(Kind::StringValue("1".to_string())),
                    }],
                    last: true,
                    ..Default::default()
                };

                let (sender, receiver) = channel(2);
                sender
                    .try_send(Ok(partial_result_set))
                    .expect("channel send should succeed");
                Ok(Response::from(receiver))
            });

        // 3. Update on attempt 2 succeeds using established transaction ID.
        mock.expect_execute_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |request| {
                let request = request.into_inner();
                let transaction = request
                    .transaction
                    .as_ref()
                    .expect("transaction selector required on attempt 2 update");
                assert_eq!(
                    transaction.selector,
                    Some(v1::transaction_selector::Selector::Id(
                        transaction_id_for_update.clone()
                    )),
                    "Expected update to use established transaction ID"
                );
                row_count_exact_response(1)
            });

        // 4. Commit succeeds on attempt 2.
        mock.expect_commit()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |request| {
                let request = request.into_inner();
                assert_eq!(
                    request.transaction,
                    Some(CommitTransaction::TransactionId(
                        transaction_id_for_commit.clone()
                    )),
                    "Expected commit to use established transaction ID"
                );
                commit_response()
            });

        let (database_client, _server) = setup_db_client(mock).await;
        let runner = TransactionRunnerBuilder::new(database_client)
            .with_begin_transaction_option(BeginTransactionOption::InlineBegin)
            .build();

        let mut attempt_counter = 0;
        let result = runner
            .run(async |transaction| {
                attempt_counter += 1;
                if attempt_counter == 1 {
                    let transaction_clone = transaction.clone();
                    let leader_started_clone = Arc::clone(&leader_started_notify);

                    // Task 1: Leader executing query with inline begin that aborts
                    let leader_handle =
                        spawn(async move { transaction_clone.execute_query("SELECT 1").await });

                    // Wait until the leader has dispatched its RPC and placed selector in Starting state
                    leader_started_clone.notified().await;

                    // Task 2: Follower attempting to execute update while leader is in Starting state
                    let follower_result = transaction
                        .execute_update("UPDATE Users SET active = true WHERE id = 1")
                        .await;

                    let leader_result = leader_handle.await.expect("task join should succeed");

                    assert!(
                        leader_result.is_err(),
                        "Expected leader query to fail with Aborted"
                    );
                    let leader_error = leader_result.expect_err("leader query failed").to_string();
                    assert!(
                        leader_error.contains("test transaction aborted"),
                        "Expected leader error message: {leader_error}"
                    );

                    assert!(
                        follower_result.is_err(),
                        "Expected follower to receive error when awakened by aborted leader"
                    );
                    let follower_error = follower_result.expect_err("follower update failed");
                    assert!(
                        follower_error
                            .to_string()
                            .contains("Aborted due to failed initial statement"),
                        "Expected follower to receive synthetic Aborted, got: {follower_error}"
                    );

                    return Err(follower_error);
                }

                let mut result_set = transaction.execute_query("SELECT 1").await?;
                let row = result_set.next().await.expect("Expected row")?;
                assert_eq!(
                    row.raw_values()[0].0,
                    Kind::StringValue("1".to_string()).into(),
                    "Expected query value 1"
                );

                let updated_count = transaction
                    .execute_update("UPDATE Users SET active = true WHERE id = 1")
                    .await?;
                Ok(updated_count)
            })
            .await?;

        assert_eq!(
            attempt_counter, 2,
            "Expected 2 runner attempts because attempt 1 aborted and retried"
        );
        assert_eq!(result.result, 1, "Expected 1 row updated on success");

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn inlined_begin_query_leader_cancelled_wakes_follower_safely() -> anyhow::Result<()> {
        let mut mock = create_session_mock();
        let mut sequence = Sequence::new();

        let transaction_id = vec![211, 212, 213, 214];
        let transaction_id_for_begin = transaction_id.clone();
        let transaction_id_for_query = transaction_id.clone();
        let transaction_id_for_commit = transaction_id.clone();

        let leader_started_notify = Arc::new(Notify::new());
        let leader_started_notify_clone = Arc::clone(&leader_started_notify);

        let held_sender = Arc::new(Mutex::new(None));
        let held_sender_clone = Arc::clone(&held_sender);

        // Attempt 1:
        // 1. Leader streaming query is invoked with inline begin.
        //    Mock holds response indefinitely; leader will be cancelled via task abort.
        mock.expect_execute_streaming_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |request| {
                let request = request.into_inner();
                let transaction = request
                    .transaction
                    .as_ref()
                    .expect("transaction options required for inline begin on attempt 1");
                let selector = transaction.selector.as_ref().expect("selector required");
                assert!(
                    matches!(selector, v1::transaction_selector::Selector::Begin(_)),
                    "Expected inline begin option for leader query on attempt 1"
                );

                leader_started_notify_clone.notify_one();

                let (sender, receiver) = channel(2);
                *held_sender_clone.lock().expect("mutex lock") = Some(sender);
                Ok(Response::from(receiver))
            });

        // Attempt 2:
        // 2. Explicit begin transaction after leader was cancelled on attempt 1.
        mock.expect_begin_transaction()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |_request| {
                Ok(Response::new(v1::Transaction {
                    id: transaction_id_for_begin.clone(),
                    ..Default::default()
                }))
            });

        // 3. Query on attempt 2 succeeds using explicit transaction ID.
        mock.expect_execute_streaming_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |request| {
                let request = request.into_inner();
                let transaction = request
                    .transaction
                    .as_ref()
                    .expect("transaction selector required on attempt 2 query");
                assert_eq!(
                    transaction.selector,
                    Some(v1::transaction_selector::Selector::Id(
                        transaction_id_for_query.clone()
                    )),
                    "Expected query to use explicit transaction ID"
                );

                let partial_result_set = v1::PartialResultSet {
                    metadata: Some(v1::ResultSetMetadata {
                        row_type: Some(v1::StructType {
                            fields: vec![Default::default()],
                        }),
                        ..Default::default()
                    }),
                    values: vec![ProtoValue {
                        kind: Some(Kind::StringValue("1".to_string())),
                    }],
                    last: true,
                    ..Default::default()
                };

                let (sender, receiver) = channel(2);
                sender
                    .try_send(Ok(partial_result_set))
                    .expect("channel send should succeed");
                Ok(Response::from(receiver))
            });

        // 4. Commit succeeds on attempt 2.
        mock.expect_commit()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |request| {
                let request = request.into_inner();
                assert_eq!(
                    request.transaction,
                    Some(CommitTransaction::TransactionId(
                        transaction_id_for_commit.clone()
                    )),
                    "Expected commit to use explicit transaction ID"
                );
                commit_response()
            });

        let (database_client, _server) = setup_db_client(mock).await;
        let runner = TransactionRunnerBuilder::new(database_client)
            .with_begin_transaction_option(BeginTransactionOption::InlineBegin)
            .build();

        let mut attempt_counter = 0;
        let result = runner
            .run(async |transaction| {
                attempt_counter += 1;
                if attempt_counter == 1 {
                    let transaction_clone = transaction.clone();
                    let leader_started_clone = Arc::clone(&leader_started_notify);

                    // Task 1: Leader query with inline begin suspended waiting for stream metadata
                    let leader_handle =
                        spawn(async move { transaction_clone.execute_query("SELECT 1").await });

                    // Wait until leader has initiated RPC and placed selector in Starting state
                    leader_started_clone.notified().await;

                    // Task 2: Follower attempting update while leader is starting
                    let transaction_follower = transaction.clone();
                    let follower_handle = spawn(async move {
                        transaction_follower
                            .execute_update("UPDATE Users SET active = true WHERE id = 1")
                            .await
                    });
                    yield_now().await;

                    // Cancel the leader task while it is suspended in Starting state
                    leader_handle.abort();
                    let _ = leader_handle.await;

                    let follower_result = follower_handle.await.expect("task join should succeed");

                    // Follower must not hang! It must be awakened by the cancellation guard and fail fast with Aborted.
                    assert!(
                        follower_result.is_err(),
                        "Expected follower to receive error when awakened after leader cancellation"
                    );
                    let follower_error = follower_result.expect_err("follower update failed");
                    assert!(
                        follower_error
                            .to_string()
                            .contains("Aborted due to failed initial statement"),
                        "Expected follower to receive synthetic Aborted, got: {follower_error}"
                    );

                    return Err(follower_error);
                }

                let mut result_set = transaction.execute_query("SELECT 1").await?;
                let row = result_set.next().await.expect("Expected row")?;
                assert_eq!(
                    row.raw_values()[0].0,
                    Kind::StringValue("1".to_string()).into(),
                    "Expected query value 1"
                );
                Ok(())
            })
            .await?;

        assert_eq!(
            attempt_counter, 2,
            "Expected 2 attempts because leader cancellation on attempt 1 retried with explicit begin"
        );
        assert_eq!(
            result.result,
            (),
            "Expected successful execution on attempt 2"
        );

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn inlined_begin_query_stream_missing_transaction_id_retries_with_explicit_begin()
    -> anyhow::Result<()> {
        let mut mock = create_session_mock();
        let mut sequence = Sequence::new();

        let transaction_id = vec![81, 82, 83];
        let transaction_id_for_begin = transaction_id.clone();
        let transaction_id_for_query = transaction_id.clone();
        let transaction_id_for_commit = transaction_id.clone();

        // 1. Initial query stream with inline begin returns metadata with transaction = None.
        mock.expect_execute_streaming_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |request| {
                let request = request.into_inner();
                let transaction = request
                    .transaction
                    .as_ref()
                    .expect("transaction options required for inline begin");
                let selector = transaction.selector.as_ref().expect("selector required");
                assert!(
                    matches!(selector, v1::transaction_selector::Selector::Begin(_)),
                    "Expected Selector::Begin on attempt 1"
                );

                let partial_result_set = v1::PartialResultSet {
                    metadata: Some(v1::ResultSetMetadata {
                        row_type: Some(v1::StructType {
                            fields: vec![Default::default()],
                        }),
                        transaction: None, // Missing transaction ID!
                        ..Default::default()
                    }),
                    values: vec![ProtoValue {
                        kind: Some(Kind::StringValue("1".to_string())),
                    }],
                    last: true,
                    ..Default::default()
                };

                let (sender, receiver) = channel(1);
                sender
                    .try_send(Ok(partial_result_set))
                    .expect("channel send should succeed");
                Ok(Response::from(receiver))
            });

        // 2. Runner catches failure, transitions to explicit begin, and calls BeginTransaction.
        mock.expect_begin_transaction()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |_request| {
                Ok(Response::new(v1::Transaction {
                    id: transaction_id_for_begin.clone(),
                    ..Default::default()
                }))
            });

        // 3. Query re-executes on attempt 2 using the explicit transaction ID.
        mock.expect_execute_streaming_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |request| {
                let request = request.into_inner();
                let transaction = request
                    .transaction
                    .as_ref()
                    .expect("transaction selector required");
                assert_eq!(
                    transaction.selector,
                    Some(v1::transaction_selector::Selector::Id(
                        transaction_id_for_query.clone()
                    )),
                    "Expected Selector::Id on attempt 2"
                );

                let partial_result_set = v1::PartialResultSet {
                    metadata: Some(v1::ResultSetMetadata {
                        row_type: Some(v1::StructType {
                            fields: vec![Default::default()],
                        }),
                        ..Default::default()
                    }),
                    values: vec![ProtoValue {
                        kind: Some(Kind::StringValue("1".to_string())),
                    }],
                    last: true,
                    ..Default::default()
                };

                let (sender, receiver) = channel(1);
                sender
                    .try_send(Ok(partial_result_set))
                    .expect("channel send should succeed");
                Ok(Response::from(receiver))
            });

        // 4. Commit succeeds on attempt 2.
        mock.expect_commit()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |request| {
                let request = request.into_inner();
                assert_eq!(
                    request.transaction,
                    Some(CommitTransaction::TransactionId(
                        transaction_id_for_commit.clone()
                    )),
                    "Expected commit with transaction ID on attempt 2"
                );
                commit_response()
            });

        let (database_client, _server) = setup_db_client(mock).await;
        let runner = TransactionRunnerBuilder::new(database_client)
            .with_begin_transaction_option(BeginTransactionOption::InlineBegin)
            .build();

        let mut attempt_counter = 0;
        let result = runner
            .run(async |transaction| {
                attempt_counter += 1;
                if attempt_counter == 1 {
                    let query_result = transaction.execute_query("SELECT 1").await;
                    assert!(
                        query_result.is_err(),
                        "Expected query to fail due to missing transaction ID on attempt 1"
                    );
                    let query_error = query_result
                        .expect_err("query failed on attempt 1")
                        .to_string();
                    assert!(
                        query_error.contains("failed to return a transaction ID"),
                        "Expected missing transaction ID error message, got: {query_error}"
                    );

                    let follower_result = transaction
                        .execute_update("UPDATE Users SET active = true WHERE id = 1")
                        .await;
                    assert!(
                        follower_result.is_err(),
                        "Expected follower update to fail on attempt 1"
                    );
                    let follower_error = follower_result.expect_err("follower update failed");
                    assert!(
                        follower_error
                            .to_string()
                            .contains("Aborted due to failed initial statement"),
                        "Expected synthetic abort error message, got: {follower_error}"
                    );

                    return Err(follower_error);
                }

                let mut result_set = transaction.execute_query("SELECT 1").await?;
                let row = result_set.next().await.expect("Expected row")?;
                assert_eq!(
                    row.raw_values()[0].0,
                    Kind::StringValue("1".to_string()).into(),
                    "Expected query value 1"
                );
                Ok(())
            })
            .await?;

        assert_eq!(
            attempt_counter, 2,
            "Expected 2 attempts because streaming query missing transaction ID triggered retry with explicit begin"
        );
        assert_eq!(
            result.result,
            (),
            "Expected successful execution on attempt 2"
        );

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn inlined_begin_query_stream_malformed_chunk_retries_with_explicit_begin()
    -> anyhow::Result<()> {
        let mut mock = create_session_mock();
        let mut sequence = Sequence::new();

        let transaction_id = vec![84, 85, 86];
        let transaction_id_for_begin = transaction_id.clone();
        let transaction_id_for_query = transaction_id.clone();
        let transaction_id_for_commit = transaction_id.clone();

        // 1. Initial query stream with inline begin returns chunk without metadata (client decode failure).
        mock.expect_execute_streaming_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |request| {
                let request = request.into_inner();
                let transaction = request
                    .transaction
                    .as_ref()
                    .expect("transaction options required for inline begin");
                let selector = transaction.selector.as_ref().expect("selector required");
                assert!(
                    matches!(selector, v1::transaction_selector::Selector::Begin(_)),
                    "Expected Selector::Begin on attempt 1"
                );

                let (sender, receiver) = channel(1);
                sender
                    .try_send(Ok(v1::PartialResultSet::default()))
                    .expect("channel send should succeed");
                Ok(Response::from(receiver))
            });

        // 2. Runner retries with explicit BeginTransaction.
        mock.expect_begin_transaction()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |_request| {
                Ok(Response::new(v1::Transaction {
                    id: transaction_id_for_begin.clone(),
                    ..Default::default()
                }))
            });

        // 3. Query re-executes on attempt 2 using explicit transaction ID.
        mock.expect_execute_streaming_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |request| {
                let request = request.into_inner();
                let transaction = request
                    .transaction
                    .as_ref()
                    .expect("transaction selector required");
                assert_eq!(
                    transaction.selector,
                    Some(v1::transaction_selector::Selector::Id(
                        transaction_id_for_query.clone()
                    )),
                    "Expected Selector::Id on attempt 2"
                );

                let partial_result_set = v1::PartialResultSet {
                    metadata: Some(v1::ResultSetMetadata {
                        row_type: Some(v1::StructType {
                            fields: vec![Default::default()],
                        }),
                        ..Default::default()
                    }),
                    values: vec![ProtoValue {
                        kind: Some(Kind::StringValue("1".to_string())),
                    }],
                    last: true,
                    ..Default::default()
                };

                let (sender, receiver) = channel(1);
                sender
                    .try_send(Ok(partial_result_set))
                    .expect("channel send should succeed");
                Ok(Response::from(receiver))
            });

        // 4. Commit succeeds on attempt 2.
        mock.expect_commit()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |request| {
                let request = request.into_inner();
                assert_eq!(
                    request.transaction,
                    Some(CommitTransaction::TransactionId(
                        transaction_id_for_commit.clone()
                    )),
                    "Expected commit with transaction ID on attempt 2"
                );
                commit_response()
            });

        let (database_client, _server) = setup_db_client(mock).await;
        let runner = TransactionRunnerBuilder::new(database_client)
            .with_begin_transaction_option(BeginTransactionOption::InlineBegin)
            .build();

        let mut attempt_counter = 0;
        let result = runner
            .run(async |transaction| {
                attempt_counter += 1;
                if attempt_counter == 1 {
                    let query_result = transaction.execute_query("SELECT 1").await;
                    assert!(
                        query_result.is_err(),
                        "Expected query to fail due to missing metadata on attempt 1"
                    );
                    let query_error = query_result
                        .expect_err("query failed on attempt 1")
                        .to_string();
                    assert!(
                        query_error.contains("First PartialResultSet did not contain metadata"),
                        "Expected missing metadata error message, got: {query_error}"
                    );

                    let follower_result = transaction
                        .execute_update("UPDATE Users SET active = true WHERE id = 1")
                        .await;
                    assert!(
                        follower_result.is_err(),
                        "Expected follower update to fail on attempt 1"
                    );
                    let follower_error = follower_result.expect_err("follower update failed");
                    assert!(
                        follower_error
                            .to_string()
                            .contains("Aborted due to failed initial statement"),
                        "Expected synthetic abort error message, got: {follower_error}"
                    );

                    return Err(follower_error);
                }

                let mut result_set = transaction.execute_query("SELECT 1").await?;
                let row = result_set.next().await.expect("Expected row")?;
                assert_eq!(
                    row.raw_values()[0].0,
                    Kind::StringValue("1".to_string()).into(),
                    "Expected query value 1"
                );
                Ok(())
            })
            .await?;

        assert_eq!(
            attempt_counter, 2,
            "Expected 2 attempts because malformed first chunk triggered retry with explicit begin"
        );
        assert_eq!(
            result.result,
            (),
            "Expected successful execution on attempt 2"
        );

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn inlined_begin_query_unavailable_then_restart_aborts_retries_with_inline_begin()
    -> anyhow::Result<()> {
        let mut mock = create_session_mock();
        let mut sequence = Sequence::new();

        let transaction_id = vec![87, 88, 89];
        let transaction_id_for_query = transaction_id.clone();
        let transaction_id_for_commit = transaction_id.clone();

        // 1. Initial query stream receives UNAVAILABLE.
        mock.expect_execute_streaming_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |request| {
                let request = request.into_inner();
                let transaction = request
                    .transaction
                    .as_ref()
                    .expect("transaction options required for inline begin");
                let selector = transaction.selector.as_ref().expect("selector required");
                assert!(
                    matches!(selector, v1::transaction_selector::Selector::Begin(_)),
                    "Expected Selector::Begin on attempt 1 initial request"
                );

                let (sender, receiver) = channel(1);
                sender
                    .try_send(Err(Status::new(Code::Unavailable, "Connection dropped")))
                    .expect("channel send should succeed");
                Ok(Response::from(receiver))
            });

        // 2. Reconnection inside ResultSet (restart_stream) carries Selector::Begin and returns Aborted.
        mock.expect_execute_streaming_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |request| {
                let request = request.into_inner();
                let transaction = request
                    .transaction
                    .as_ref()
                    .expect("transaction options required for restarted stream");
                let selector = transaction.selector.as_ref().expect("selector required");
                assert!(
                    matches!(selector, v1::transaction_selector::Selector::Begin(_)),
                    "Expected Selector::Begin on attempt 1 restarted request"
                );

                Err(create_aborted_status(StdDuration::from_nanos(1)))
            });

        // 3. Attempt 2 retries with InlineBegin (saving 1 RPC) because initial statement was aborted!
        mock.expect_execute_streaming_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |request| {
                let request = request.into_inner();
                let transaction = request
                    .transaction
                    .as_ref()
                    .expect("transaction options required for inline begin retry");
                let selector = transaction.selector.as_ref().expect("selector required");
                assert!(
                    matches!(selector, v1::transaction_selector::Selector::Begin(_)),
                    "Expected Selector::Begin on attempt 2 retry"
                );

                let partial_result_set = v1::PartialResultSet {
                    metadata: Some(v1::ResultSetMetadata {
                        row_type: Some(v1::StructType {
                            fields: vec![Default::default()],
                        }),
                        transaction: Some(v1::Transaction {
                            id: transaction_id_for_query.clone(),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    values: vec![ProtoValue {
                        kind: Some(Kind::StringValue("1".to_string())),
                    }],
                    last: true,
                    ..Default::default()
                };

                let (sender, receiver) = channel(1);
                sender
                    .try_send(Ok(partial_result_set))
                    .expect("channel send should succeed");
                Ok(Response::from(receiver))
            });

        // 4. Commit succeeds on attempt 2.
        mock.expect_commit()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |request| {
                let request = request.into_inner();
                assert_eq!(
                    request.transaction,
                    Some(CommitTransaction::TransactionId(
                        transaction_id_for_commit.clone()
                    )),
                    "Expected commit with transaction ID on attempt 2"
                );
                commit_response()
            });

        let (database_client, _server) = setup_db_client(mock).await;
        let runner = TransactionRunnerBuilder::new(database_client)
            .with_begin_transaction_option(BeginTransactionOption::InlineBegin)
            .build();

        let mut attempt_counter = 0;
        let result = runner
            .run(async |transaction| {
                attempt_counter += 1;
                if attempt_counter == 1 {
                    let statement = Statement::builder("SELECT 1")
                        .with_backoff_policy(
                            ExponentialBackoffBuilder::new()
                                .with_initial_delay(StdDuration::from_nanos(1))
                                .clamp(),
                        )
                        .build();
                    let query_result = transaction.execute_query(statement).await;
                    assert!(
                        query_result.is_err(),
                        "Expected query to fail with Aborted on attempt 1"
                    );
                    let query_error = query_result.expect_err("query failed on attempt 1");
                    assert!(
                        query_error
                            .status()
                            .is_some_and(|status| status.code == GaxCode::Aborted),
                        "Expected query error to be Aborted, got: {query_error}"
                    );

                    let follower_result = transaction
                        .execute_update("UPDATE Users SET active = true WHERE id = 1")
                        .await;
                    assert!(
                        follower_result.is_err(),
                        "Expected follower update to fail on attempt 1"
                    );
                    let follower_error = follower_result.expect_err("follower update failed");
                    assert!(
                        follower_error
                            .to_string()
                            .contains("Aborted due to failed initial statement"),
                        "Expected synthetic abort error message, got: {follower_error}"
                    );

                    return Err(follower_error);
                }

                let mut result_set = transaction.execute_query("SELECT 1").await?;
                let row = result_set.next().await.expect("Expected row")?;
                assert_eq!(
                    row.raw_values()[0].0,
                    Kind::StringValue("1".to_string()).into(),
                    "Expected query value 1"
                );
                Ok(())
            })
            .await?;

        assert_eq!(
            attempt_counter, 2,
            "Expected 2 attempts because aborted restart retried with inline begin"
        );
        assert_eq!(
            result.result,
            (),
            "Expected successful execution on attempt 2"
        );

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn inlined_begin_query_retry_exhaustion_retries_with_explicit_begin() -> anyhow::Result<()>
    {
        let mut mock = create_session_mock();
        let mut sequence = Sequence::new();

        let transaction_id = vec![91, 92, 93];
        let transaction_id_for_begin = transaction_id.clone();
        let transaction_id_for_query = transaction_id.clone();
        let transaction_id_for_commit = transaction_id.clone();

        // 1. Initial query stream with inline begin returns UNAVAILABLE.
        // With NeverRetry configured on statement, retries are immediately exhausted.
        mock.expect_execute_streaming_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |request| {
                let request = request.into_inner();
                let transaction = request
                    .transaction
                    .as_ref()
                    .expect("transaction options required for inline begin");
                let selector = transaction.selector.as_ref().expect("selector required");
                assert!(
                    matches!(selector, v1::transaction_selector::Selector::Begin(_)),
                    "Expected Selector::Begin on attempt 1"
                );

                let (sender, receiver) = channel(1);
                sender
                    .try_send(Err(Status::new(Code::Unavailable, "Server unavailable")))
                    .expect("channel send should succeed");
                Ok(Response::from(receiver))
            });

        // 2. Retry exhaustion transitions state to FirstStatementFailed.
        // Runner retries with explicit BeginTransaction.
        mock.expect_begin_transaction()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |_request| {
                Ok(Response::new(v1::Transaction {
                    id: transaction_id_for_begin.clone(),
                    ..Default::default()
                }))
            });

        // 3. Query re-executes on attempt 2 using explicit transaction ID.
        mock.expect_execute_streaming_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |request| {
                let request = request.into_inner();
                let transaction = request
                    .transaction
                    .as_ref()
                    .expect("transaction selector required");
                assert_eq!(
                    transaction.selector,
                    Some(v1::transaction_selector::Selector::Id(
                        transaction_id_for_query.clone()
                    )),
                    "Expected Selector::Id on attempt 2"
                );

                let partial_result_set = v1::PartialResultSet {
                    metadata: Some(v1::ResultSetMetadata {
                        row_type: Some(v1::StructType {
                            fields: vec![Default::default()],
                        }),
                        ..Default::default()
                    }),
                    values: vec![ProtoValue {
                        kind: Some(Kind::StringValue("1".to_string())),
                    }],
                    last: true,
                    ..Default::default()
                };

                let (sender, receiver) = channel(1);
                sender
                    .try_send(Ok(partial_result_set))
                    .expect("channel send should succeed");
                Ok(Response::from(receiver))
            });

        // 4. Commit succeeds on attempt 2.
        mock.expect_commit()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |request| {
                let request = request.into_inner();
                assert_eq!(
                    request.transaction,
                    Some(CommitTransaction::TransactionId(
                        transaction_id_for_commit.clone()
                    )),
                    "Expected commit with transaction ID on attempt 2"
                );
                commit_response()
            });

        let (database_client, _server) = setup_db_client(mock).await;
        let runner = TransactionRunnerBuilder::new(database_client)
            .with_begin_transaction_option(BeginTransactionOption::InlineBegin)
            .build();

        let mut attempt_counter = 0;
        let result = runner
            .run(async |transaction| {
                attempt_counter += 1;
                if attempt_counter == 1 {
                    let statement = Statement::builder("SELECT 1")
                        .with_retry_policy(NeverRetry)
                        .build();
                    let query_result = transaction.execute_query(statement).await;
                    assert!(
                        query_result.is_err(),
                        "Expected query to fail after retry exhaustion on attempt 1"
                    );
                    let query_error = query_result.expect_err("query failed on attempt 1");
                    assert_eq!(
                        query_error.status().map(|status| status.code),
                        Some(GaxCode::Unavailable),
                        "Expected Unavailable error code"
                    );

                    let follower_result = transaction
                        .execute_update("UPDATE Users SET active = true WHERE id = 1")
                        .await;
                    assert!(
                        follower_result.is_err(),
                        "Expected follower update to fail on attempt 1"
                    );
                    let follower_error = follower_result.expect_err("follower update failed");
                    assert!(
                        follower_error
                            .to_string()
                            .contains("Aborted due to failed initial statement"),
                        "Expected synthetic abort error message, got: {follower_error}"
                    );

                    return Err(follower_error);
                }

                let mut result_set = transaction.execute_query("SELECT 1").await?;
                let row = result_set.next().await.expect("Expected row")?;
                assert_eq!(
                    row.raw_values()[0].0,
                    Kind::StringValue("1".to_string()).into(),
                    "Expected query value 1"
                );
                Ok(())
            })
            .await?;

        assert_eq!(
            attempt_counter, 2,
            "Expected 2 attempts because retry exhaustion triggered explicit begin"
        );
        assert_eq!(
            result.result,
            (),
            "Expected successful execution on attempt 2"
        );

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn inlined_begin_batch_dml_missing_transaction_id_retries_with_explicit_begin()
    -> anyhow::Result<()> {
        let mut mock = create_session_mock();
        let mut sequence = Sequence::new();

        let transaction_id = vec![94, 95, 96];
        let transaction_id_for_begin = transaction_id.clone();
        let transaction_id_for_batch = transaction_id.clone();
        let transaction_id_for_commit = transaction_id.clone();

        // 1. Initial Batch DML with inline begin returns response with empty result_sets (no transaction ID).
        mock.expect_execute_batch_dml()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |request| {
                let request = request.into_inner();
                let transaction = request
                    .transaction
                    .as_ref()
                    .expect("transaction options required for inline begin");
                let selector = transaction.selector.as_ref().expect("selector required");
                assert!(
                    matches!(selector, v1::transaction_selector::Selector::Begin(_)),
                    "Expected Selector::Begin on attempt 1 batch DML"
                );

                Ok(tonic::Response::new(v1::ExecuteBatchDmlResponse {
                    result_sets: vec![], // No result set, thus no transaction ID
                    status: Some(RpcStatus {
                        code: 0,
                        message: "OK".into(),
                        details: vec![],
                    }),
                    ..Default::default()
                }))
            });

        // 2. Runner catches failure, transitions to explicit begin, and calls BeginTransaction.
        mock.expect_begin_transaction()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |_request| {
                Ok(Response::new(v1::Transaction {
                    id: transaction_id_for_begin.clone(),
                    ..Default::default()
                }))
            });

        // 3. Batch DML re-executes on attempt 2 using explicit transaction ID.
        mock.expect_execute_batch_dml()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |request| {
                let request = request.into_inner();
                let transaction = request
                    .transaction
                    .as_ref()
                    .expect("transaction selector required");
                assert_eq!(
                    transaction.selector,
                    Some(v1::transaction_selector::Selector::Id(
                        transaction_id_for_batch.clone()
                    )),
                    "Expected Selector::Id on attempt 2 batch DML"
                );

                Ok(tonic::Response::new(v1::ExecuteBatchDmlResponse {
                    result_sets: vec![v1::ResultSet {
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

        // 4. Commit succeeds on attempt 2.
        mock.expect_commit()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |request| {
                let request = request.into_inner();
                assert_eq!(
                    request.transaction,
                    Some(CommitTransaction::TransactionId(
                        transaction_id_for_commit.clone()
                    )),
                    "Expected commit with transaction ID on attempt 2"
                );
                commit_response()
            });

        let (database_client, _server) = setup_db_client(mock).await;
        let runner = TransactionRunnerBuilder::new(database_client)
            .with_begin_transaction_option(BeginTransactionOption::InlineBegin)
            .build();

        let mut attempt_counter = 0;
        let result = runner
            .run(async |transaction| {
                attempt_counter += 1;
                if attempt_counter == 1 {
                    let batch = BatchDml::builder()
                        .add_statement("UPDATE Users SET active = true WHERE id = 1");
                    let batch_result = transaction.execute_batch_update(batch.build()).await;
                    assert!(
                        batch_result.is_err(),
                        "Expected batch DML to fail due to missing transaction ID on attempt 1"
                    );
                    let batch_error = batch_result
                        .expect_err("batch update failed on attempt 1")
                        .to_string();
                    assert!(
                        batch_error.contains("failed to return a transaction ID")
                            || batch_error.contains("Transaction ID was not returned"),
                        "Expected missing transaction ID error message, got: {batch_error}"
                    );

                    let follower_result = transaction
                        .execute_update("UPDATE Users SET active = true WHERE id = 2")
                        .await;
                    assert!(
                        follower_result.is_err(),
                        "Expected follower update to fail on attempt 1"
                    );
                    let follower_error = follower_result.expect_err("follower update failed");
                    assert!(
                        follower_error
                            .to_string()
                            .contains("Aborted due to failed initial statement"),
                        "Expected synthetic abort error message, got: {follower_error}"
                    );

                    return Err(follower_error);
                }

                let batch = BatchDml::builder()
                    .add_statement("UPDATE Users SET active = true WHERE id = 1");
                let update_counts = transaction.execute_batch_update(batch.build()).await?;
                assert_eq!(
                    update_counts,
                    vec![1],
                    "Expected 1 update count on attempt 2"
                );
                Ok(())
            })
            .await?;

        assert_eq!(
            attempt_counter, 2,
            "Expected 2 attempts because batch DML missing transaction ID triggered retry with explicit begin"
        );
        assert_eq!(
            result.result,
            (),
            "Expected successful execution on attempt 2"
        );

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn inlined_begin_streaming_read_restart_failure_retries_with_explicit_begin()
    -> anyhow::Result<()> {
        let mut mock = create_session_mock();
        let mut sequence = Sequence::new();

        let transaction_id = vec![97, 98, 99];
        let transaction_id_for_begin = transaction_id.clone();
        let transaction_id_for_read = transaction_id.clone();
        let transaction_id_for_commit = transaction_id.clone();

        // 1. Initial streaming read with inline begin returns UNAVAILABLE.
        mock.expect_streaming_read()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |request| {
                let request = request.into_inner();
                let transaction = request
                    .transaction
                    .as_ref()
                    .expect("transaction options required for inline begin");
                let selector = transaction.selector.as_ref().expect("selector required");
                assert!(
                    matches!(selector, v1::transaction_selector::Selector::Begin(_)),
                    "Expected Selector::Begin on attempt 1 streaming read"
                );

                let (sender, receiver) = channel(1);
                sender
                    .try_send(Err(Status::new(
                        Code::Unavailable,
                        "Transient read failure",
                    )))
                    .expect("channel send should succeed");
                Ok(Response::from(receiver))
            });

        // 2. Reconnection inside ResultSet (restart_stream) fails with fatal error (InvalidArgument).
        mock.expect_streaming_read()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |_request| {
                Err(Status::new(
                    Code::InvalidArgument,
                    "Invalid argument during read restart",
                ))
            });

        // 3. Runner retries with explicit BeginTransaction.
        mock.expect_begin_transaction()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |_request| {
                Ok(Response::new(v1::Transaction {
                    id: transaction_id_for_begin.clone(),
                    ..Default::default()
                }))
            });

        // 4. Streaming read re-executes on attempt 2 using explicit transaction ID.
        mock.expect_streaming_read()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |request| {
                let request = request.into_inner();
                let transaction = request
                    .transaction
                    .as_ref()
                    .expect("transaction selector required");
                assert_eq!(
                    transaction.selector,
                    Some(v1::transaction_selector::Selector::Id(
                        transaction_id_for_read.clone()
                    )),
                    "Expected Selector::Id on attempt 2 streaming read"
                );

                let partial_result_set = v1::PartialResultSet {
                    metadata: Some(v1::ResultSetMetadata {
                        row_type: Some(v1::StructType {
                            fields: vec![Default::default()],
                        }),
                        ..Default::default()
                    }),
                    values: vec![ProtoValue {
                        kind: Some(Kind::StringValue("1".to_string())),
                    }],
                    last: true,
                    ..Default::default()
                };

                let (sender, receiver) = channel(1);
                sender
                    .try_send(Ok(partial_result_set))
                    .expect("channel send should succeed");
                Ok(Response::from(receiver))
            });

        // 5. Commit succeeds on attempt 2.
        mock.expect_commit()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |request| {
                let request = request.into_inner();
                assert_eq!(
                    request.transaction,
                    Some(CommitTransaction::TransactionId(
                        transaction_id_for_commit.clone()
                    )),
                    "Expected commit with transaction ID on attempt 2"
                );
                commit_response()
            });

        let (database_client, _server) = setup_db_client(mock).await;
        let runner = TransactionRunnerBuilder::new(database_client)
            .with_begin_transaction_option(BeginTransactionOption::InlineBegin)
            .build();

        let mut attempt_counter = 0;
        let result = runner
            .run(async |transaction| {
                attempt_counter += 1;
                if attempt_counter == 1 {
                    let read_request = ReadRequest::builder("Users", vec!["id"])
                        .with_keys(KeySet::all())
                        .with_backoff_policy(
                            ExponentialBackoffBuilder::new()
                                .with_initial_delay(StdDuration::from_nanos(1))
                                .clamp(),
                        )
                        .build();
                    let read_result = transaction.execute_read(read_request).await;
                    assert!(
                        read_result.is_err(),
                        "Expected streaming read to fail on attempt 1"
                    );
                    let read_error = read_result
                        .expect_err("read failed on attempt 1")
                        .to_string();
                    assert!(
                        read_error.contains("Invalid argument during read restart"),
                        "Expected restart error message, got: {read_error}"
                    );

                    let follower_result = transaction
                        .execute_update("UPDATE Users SET active = true WHERE id = 1")
                        .await;
                    assert!(
                        follower_result.is_err(),
                        "Expected follower update to fail on attempt 1"
                    );
                    let follower_error = follower_result.expect_err("follower update failed");
                    assert!(
                        follower_error
                            .to_string()
                            .contains("Aborted due to failed initial statement"),
                        "Expected synthetic abort error message, got: {follower_error}"
                    );

                    return Err(follower_error);
                }

                let read_request = ReadRequest::builder("Users", vec!["id"])
                    .with_keys(KeySet::all())
                    .build();
                let mut result_set = transaction.execute_read(read_request).await?;
                let row = result_set.next().await.expect("Expected row")?;
                assert_eq!(
                    row.raw_values()[0].0,
                    Kind::StringValue("1".to_string()).into(),
                    "Expected read value 1"
                );
                Ok(())
            })
            .await?;

        assert_eq!(
            attempt_counter, 2,
            "Expected 2 attempts because streaming read restart failure triggered retry with explicit begin"
        );
        assert_eq!(
            result.result,
            (),
            "Expected successful execution on attempt 2"
        );

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn sticky_explicit_begin_persists_across_subsequent_aborts() -> crate::Result<()> {
        let mut mock = create_session_mock();
        let mut sequence = Sequence::new();

        let transaction_id_attempt2 = vec![2, 2, 2];
        let transaction_id_attempt2_query = transaction_id_attempt2.clone();
        let transaction_id_attempt2_commit = transaction_id_attempt2.clone();

        let transaction_id_attempt3 = vec![3, 3, 3];
        let transaction_id_attempt3_query = transaction_id_attempt3.clone();
        let transaction_id_attempt3_commit = transaction_id_attempt3.clone();

        // 1. Attempt 1: First statement (execute_sql) fails with InvalidArgument.
        mock.expect_execute_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(|request| {
                let request = request.into_inner();
                let transaction = request
                    .transaction
                    .as_ref()
                    .expect("transaction options required for inline begin");
                let selector = transaction.selector.as_ref().expect("selector required");
                assert!(
                    matches!(selector, v1::transaction_selector::Selector::Begin(_)),
                    "Expected Selector::Begin on attempt 1 execute_sql"
                );
                Err(Status::new(Code::InvalidArgument, "table not found"))
            });

        // 2. Attempt 2: Explicit BeginTransaction RPC due to first statement failure in attempt 1.
        mock.expect_begin_transaction()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |_request| {
                Ok(Response::new(v1::Transaction {
                    id: transaction_id_attempt2.clone(),
                    ..Default::default()
                }))
            });

        // 3. Attempt 2: execute_sql uses explicit transaction ID [2, 2, 2].
        mock.expect_execute_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |request| {
                let request = request.into_inner();
                let transaction = request
                    .transaction
                    .as_ref()
                    .expect("transaction selector required");
                assert_eq!(
                    transaction.selector,
                    Some(v1::transaction_selector::Selector::Id(
                        transaction_id_attempt2_query.clone()
                    )),
                    "Expected Selector::Id on attempt 2"
                );
                Ok(Response::new(v1::ResultSet {
                    stats: Some(v1::ResultSetStats {
                        row_count: Some(v1::result_set_stats::RowCount::RowCountExact(1)),
                        ..Default::default()
                    }),
                    ..Default::default()
                }))
            });

        // 4. Attempt 2: Commit aborts with concurrent transaction conflict.
        mock.expect_commit()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |request| {
                let request = request.into_inner();
                assert_eq!(
                    request.transaction,
                    Some(CommitTransaction::TransactionId(
                        transaction_id_attempt2_commit.clone()
                    )),
                    "Expected commit with transaction ID on attempt 2"
                );
                Err(create_aborted_status(StdDuration::from_nanos(1)))
            });

        // 5. Attempt 3: MUST still use explicit BeginTransaction (force_explicit_begin sticky).
        mock.expect_begin_transaction()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |_request| {
                Ok(Response::new(v1::Transaction {
                    id: transaction_id_attempt3.clone(),
                    ..Default::default()
                }))
            });

        // 6. Attempt 3: execute_sql uses explicit transaction ID [3, 3, 3].
        mock.expect_execute_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |request| {
                let request = request.into_inner();
                let transaction = request
                    .transaction
                    .as_ref()
                    .expect("transaction selector required");
                assert_eq!(
                    transaction.selector,
                    Some(v1::transaction_selector::Selector::Id(
                        transaction_id_attempt3_query.clone()
                    )),
                    "Expected Selector::Id on attempt 3"
                );
                Ok(Response::new(v1::ResultSet {
                    stats: Some(v1::ResultSetStats {
                        row_count: Some(v1::result_set_stats::RowCount::RowCountExact(1)),
                        ..Default::default()
                    }),
                    ..Default::default()
                }))
            });

        // 7. Attempt 3: Commit succeeds.
        mock.expect_commit()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |request| {
                let request = request.into_inner();
                assert_eq!(
                    request.transaction,
                    Some(CommitTransaction::TransactionId(
                        transaction_id_attempt3_commit.clone()
                    )),
                    "Expected commit with transaction ID on attempt 3"
                );
                commit_response()
            });

        let (database_client, _server) = setup_db_client(mock).await;
        let runner = TransactionRunnerBuilder::new(database_client)
            .with_begin_transaction_option(BeginTransactionOption::InlineBegin)
            .with_retry_policy(
                BasicTransactionRetryPolicy::new()
                    .with_max_attempts(5)
                    .with_total_timeout(StdDuration::from_secs(5)),
            )
            .build();

        let mut attempt_counter = 0;
        let result = runner
            .run(async |transaction| {
                attempt_counter += 1;
                if attempt_counter == 1 {
                    let update_result = transaction
                        .execute_update("UPDATE NonExistentTable SET active = true WHERE id = 1")
                        .await;
                    assert!(
                        update_result.is_err(),
                        "Expected error on attempt 1 due to non-existent table"
                    );

                    let follower_result = transaction
                        .execute_update("UPDATE Users SET active = true WHERE id = 2")
                        .await;
                    assert!(
                        follower_result.is_err(),
                        "Expected follower statement to fail fast with aborted"
                    );
                    let follower_error = follower_result.expect_err("follower statement must fail");
                    assert!(
                        is_aborted(&follower_error),
                        "Expected aborted error, got: {follower_error}"
                    );
                    return Err(follower_error);
                }

                let count = transaction
                    .execute_update("UPDATE Users SET active = true WHERE id = 1")
                    .await?;
                assert_eq!(
                    count, 1,
                    "Expected 1 row updated on attempt {attempt_counter}"
                );
                Ok(())
            })
            .await?;

        assert_eq!(
            attempt_counter, 3,
            "Expected exactly 3 attempts: attempt 1 failed initial statement, attempt 2 explicit begin aborted on commit, attempt 3 explicit begin succeeded"
        );
        assert_eq!(
            result.result,
            (),
            "Expected successful execution on attempt 3"
        );

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn inlined_begin_execute_update_empty_transaction_id_retries_with_explicit_begin()
    -> crate::Result<()> {
        let mut mock = create_session_mock();
        let mut sequence = Sequence::new();

        let transaction_id_for_begin = vec![1, 2, 3];
        let transaction_id_for_update = transaction_id_for_begin.clone();
        let transaction_id_for_commit = transaction_id_for_begin.clone();

        // 1. Attempt 1: First statement (execute_sql) returns metadata with empty transaction ID (vec![]).
        mock.expect_execute_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(|request| {
                let request = request.into_inner();
                let transaction = request
                    .transaction
                    .as_ref()
                    .expect("transaction options required for inline begin");
                let selector = transaction.selector.as_ref().expect("selector required");
                assert!(
                    matches!(selector, v1::transaction_selector::Selector::Begin(_)),
                    "Expected Selector::Begin on attempt 1 execute_sql"
                );
                Ok(Response::new(v1::ResultSet {
                    metadata: Some(v1::ResultSetMetadata {
                        transaction: Some(v1::Transaction {
                            id: vec![], // Empty transaction ID
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

        // 2. Attempt 2: Explicit BeginTransaction RPC because attempt 1 had an empty transaction ID.
        mock.expect_begin_transaction()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |_request| {
                Ok(Response::new(v1::Transaction {
                    id: transaction_id_for_begin.clone(),
                    ..Default::default()
                }))
            });

        // 3. Attempt 2: execute_sql uses explicit transaction ID [1, 2, 3].
        mock.expect_execute_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |request| {
                let request = request.into_inner();
                let transaction = request
                    .transaction
                    .as_ref()
                    .expect("transaction selector required");
                assert_eq!(
                    transaction.selector,
                    Some(v1::transaction_selector::Selector::Id(
                        transaction_id_for_update.clone()
                    )),
                    "Expected Selector::Id on attempt 2"
                );
                Ok(Response::new(v1::ResultSet {
                    stats: Some(v1::ResultSetStats {
                        row_count: Some(v1::result_set_stats::RowCount::RowCountExact(1)),
                        ..Default::default()
                    }),
                    ..Default::default()
                }))
            });

        // 4. Attempt 2: Commit succeeds.
        mock.expect_commit()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |request| {
                let request = request.into_inner();
                assert_eq!(
                    request.transaction,
                    Some(CommitTransaction::TransactionId(
                        transaction_id_for_commit.clone()
                    )),
                    "Expected commit with transaction ID on attempt 2"
                );
                commit_response()
            });

        let (database_client, _server) = setup_db_client(mock).await;
        let runner = TransactionRunnerBuilder::new(database_client)
            .with_begin_transaction_option(BeginTransactionOption::InlineBegin)
            .build();

        let mut attempt_counter = 0;
        let result = runner
            .run(async |transaction| {
                attempt_counter += 1;
                if attempt_counter == 1 {
                    let update_result = transaction
                        .execute_update("UPDATE Users SET active = true WHERE id = 1")
                        .await;
                    assert!(
                        update_result.is_err(),
                        "Expected error on attempt 1 because transaction ID was empty"
                    );
                    let err = update_result.expect_err("update must fail when tx id is empty");
                    assert!(
                        err.to_string()
                            .contains("Transaction ID was not returned by Spanner"),
                        "Expected missing transaction ID error message, got: {err}"
                    );

                    let follower_result = transaction
                        .execute_update("UPDATE Users SET active = true WHERE id = 2")
                        .await;
                    assert!(
                        follower_result.is_err(),
                        "Expected follower statement to fail fast with aborted"
                    );
                    let follower_error = follower_result.expect_err("follower statement must fail");
                    assert!(
                        is_aborted(&follower_error),
                        "Expected aborted error, got: {follower_error}"
                    );
                    return Err(follower_error);
                }

                let count = transaction
                    .execute_update("UPDATE Users SET active = true WHERE id = 1")
                    .await?;
                assert_eq!(count, 1, "Expected 1 row updated on attempt 2");
                Ok(())
            })
            .await?;

        assert_eq!(
            attempt_counter, 2,
            "Expected 2 attempts because empty transaction ID triggered retry with explicit begin"
        );
        assert_eq!(
            result.result,
            (),
            "Expected successful execution on attempt 2"
        );

        Ok(())
    }
}
