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

use crate::channel_pool::{ChannelTarget, TransactionAffinity};
use crate::database_client::DatabaseClient;
use crate::error::{SpannerInternalError, aborted_due_to_failed_initial_statement, internal_error};
use crate::model::transaction_options::{Mode, ReadOnly};
use crate::model::transaction_selector::Selector;
use crate::model::{
    BeginTransactionRequest, Mutation, Transaction, TransactionOptions, TransactionSelector,
};
use crate::precommit::PrecommitTokenTracker;
use crate::result_set::{ResultSet, ResultSetParams, StreamOperation};
use crate::retry_delay::extract_retry_delay_from_error;
use crate::statement::Statement;
use crate::timestamp_bound::TimestampBound;
use crate::transaction_retry_policy::is_aborted_or_emulator_error;
use crate::{Error, Result};
use google_cloud_gax::backoff_policy::BackoffPolicyArg;
use google_cloud_gax::error::rpc::Status;
use google_cloud_gax::options::RequestOptions as GaxRequestOptions;
use google_cloud_gax::options::internal::RequestOptionsExt as _;
use google_cloud_gax::retry_policy::RetryPolicyArg;
use http::HeaderMap;
use std::error::Error as _;
use std::mem::replace;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError};
use std::time::Duration;
use tokio::sync::Notify;
use tokio::sync::futures::OwnedNotified;

/// A builder for [SingleUseReadOnlyTransaction].
///
/// # Example
/// ```
/// # use google_cloud_spanner::client::Spanner;
/// # use google_cloud_spanner::transaction::TimestampBound;
/// # async fn build_tx(spanner: Spanner) -> Result<(), google_cloud_spanner::Error> {
/// let db_client = spanner.database_client("projects/p/instances/i/databases/d").build().await?;
/// let read_only_tx = db_client.single_use()
///     .set_timestamp_bound(TimestampBound::strong())
///     .build();
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct SingleUseReadOnlyTransactionBuilder {
    client: DatabaseClient,
    timestamp_bound: Option<TimestampBound>,
}

impl SingleUseReadOnlyTransactionBuilder {
    pub(crate) fn new(client: DatabaseClient) -> Self {
        Self {
            client,
            timestamp_bound: None,
        }
    }

    /// Sets the timestamp bound for the read-only transaction.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::Spanner;
    /// # use google_cloud_spanner::transaction::TimestampBound;
    /// # async fn set_bound(spanner: Spanner) -> Result<(), google_cloud_spanner::Error> {
    /// let db_client = spanner.database_client("projects/p/instances/i/databases/d").build().await?;
    /// let builder = db_client.single_use().set_timestamp_bound(TimestampBound::strong());
    /// # Ok(())
    /// # }
    /// ```
    /// When reading data in Spanner in a read-only transaction, you can set a timestamp bound,
    /// which tells Spanner how to choose a timestamp at which to read the data.
    ///
    /// See <https://docs.cloud.google.com/spanner/docs/timestamp-bounds> for more information.
    pub fn set_timestamp_bound(mut self, bound: TimestampBound) -> Self {
        self.timestamp_bound = Some(bound);
        self
    }

    /// Builds the [SingleUseReadOnlyTransaction].
    ///
    /// # Example
    ///
    /// ```
    /// # use google_cloud_spanner::client::Spanner;
    /// # async fn build(spanner: Spanner) -> Result<(), google_cloud_spanner::Error> {
    /// let db_client = spanner.database_client("projects/p/instances/i/databases/d").build().await?;
    /// let tx = db_client.single_use().build();
    /// # Ok(())
    /// # }
    /// ```
    pub fn build(self) -> SingleUseReadOnlyTransaction {
        let read_only = match self.timestamp_bound {
            Some(bound) => ReadOnly::default().set_timestamp_bound(bound.0),
            None => ReadOnly::default().set_strong(true),
        };
        let transaction_selector = crate::model::TransactionSelector::default()
            .set_single_use(TransactionOptions::default().set_read_only(read_only));

        let session_name = self.client.session_name();
        SingleUseReadOnlyTransaction {
            context: ReadContext {
                session_name,
                client: self.client,
                transaction_selector: ReadContextTransactionSelector::Fixed(
                    transaction_selector,
                    None,
                ),
                precommit_token_tracker: PrecommitTokenTracker::new_noop(),
                transaction_tag: None,
                begin_transaction_request_options: None,
                affinity: None,
                aborted_state: AbortedState::new_noop(),
            },
        }
    }
}

/// A single-use read-only transaction. A single-use read-only transaction is the most
/// efficient way to execute a single query or read operation.
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
/// let rs = tx.execute_query(stmt).await?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct SingleUseReadOnlyTransaction {
    context: ReadContext,
}

impl SingleUseReadOnlyTransaction {
    /// Executes a query.
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
    /// while let Some(row) = rs.next().await {
    ///     let _row = row?;
    ///     // process row
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn execute_query<T: Into<Statement>>(
        &self,
        statement: T,
    ) -> crate::Result<ResultSet> {
        self.context.execute_query(statement, None).await
    }

    /// Reads rows from the database using key lookups and scans, as a simple key/value style alternative to execute_query.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::Spanner;
    /// # use google_cloud_spanner::key::KeySet;
    /// # use google_cloud_spanner::read::ReadRequest;
    /// # use google_cloud_spanner::key;
    /// # async fn run(spanner: Spanner) -> Result<(), google_cloud_spanner::Error> {
    /// let db_client = spanner.database_client("projects/p/instances/i/databases/d").build().await?;
    /// let transaction = db_client.single_use().build();
    ///
    /// // Read using the primary key
    /// let read_by_pk = ReadRequest::builder("Users", vec!["Id", "Name"]).with_keys(KeySet::all()).build();
    /// let mut result_set = transaction.execute_read(read_by_pk).await?;
    /// while let Some(row) = result_set.next().await {
    ///     let _row = row?;
    ///     // process row
    /// }
    ///
    /// // Read using a secondary index
    /// let read_by_index = ReadRequest::builder("Users", vec!["Id", "Name"])
    ///     .with_index("UsersByIndex", key![1_i64]).build();
    /// let mut result_set = transaction.execute_read(read_by_index).await?;
    /// while let Some(row) = result_set.next().await {
    ///     let _row = row?;
    ///     // process row
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn execute_read<T: Into<crate::read::ReadRequest>>(
        &self,
        read: T,
    ) -> crate::Result<ResultSet> {
        self.context.execute_read(read).await
    }
}

/// Options for how to start a transaction.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum BeginTransactionOption {
    /// The transaction will be started inlined with the first statement.
    /// This reduces the number of round-trips to Spanner by one.
    ///
    /// If the first statement in the transaction fails, the transaction will not
    /// be started and will be marked as permanently failed. Any subsequent statements
    /// executed on that transaction will also fail. To allow a transaction to continue
    /// executing subsequent statements after an initial statement failure, use
    /// `BeginTransactionOption::ExplicitBegin`.
    #[default]
    InlineBegin,
    /// The transaction will be started explicitly using a `BeginTransaction` RPC before
    /// any statement is executed.
    ///
    /// Use this option when:
    /// 1. Multiple queries are executed concurrently at the start of the transaction.
    /// 2. The first statement in the transaction might fail and the application wants to
    ///    catch the error and continue executing subsequent statements in the same transaction
    ///    at the same snapshot read timestamp.
    ExplicitBegin,
}

/// A builder for [MultiUseReadOnlyTransaction].
///
/// # Example
/// ```
/// # use google_cloud_spanner::client::Spanner;
/// # use google_cloud_spanner::transaction::TimestampBound;
/// # async fn build_tx(spanner: Spanner) -> Result<(), google_cloud_spanner::Error> {
/// let db_client = spanner.database_client("projects/p/instances/i/databases/d").build().await?;
/// let read_only_tx = db_client.read_only_transaction()
///     .set_timestamp_bound(TimestampBound::strong())
///     .build()
///     .await?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct MultiUseReadOnlyTransactionBuilder {
    client: DatabaseClient,
    timestamp_bound: Option<TimestampBound>,
    begin_transaction_option: BeginTransactionOption,
    begin_gax_options: Option<crate::RequestOptions>,
    affinity: Option<Arc<TransactionAffinity>>,
}

impl MultiUseReadOnlyTransactionBuilder {
    pub(crate) fn new(client: DatabaseClient) -> Self {
        Self {
            client,
            timestamp_bound: None,
            begin_transaction_option: BeginTransactionOption::InlineBegin,
            begin_gax_options: None,
            affinity: None,
        }
    }

    /// Sets the option for how to start a transaction.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::Spanner;
    /// # use google_cloud_spanner::transaction::BeginTransactionOption;
    /// # use google_cloud_spanner::statement::Statement;
    /// # async fn set_begin_option(spanner: Spanner) -> Result<(), google_cloud_spanner::Error> {
    /// let db_client = spanner.database_client("projects/p/instances/i/databases/d").build().await?;
    /// let transaction = db_client.read_only_transaction().set_begin_transaction_option(BeginTransactionOption::ExplicitBegin).build().await?;
    /// let statement = Statement::builder("SELECT * FROM users").build();
    /// let result_set = transaction.execute_query(statement).await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// By default, the Spanner client will inline the `BeginTransaction` call with the first query
    /// in the transaction (`BeginTransactionOption::InlineBegin`). This reduces the number of round-trips
    /// to Spanner that are needed for a transaction. Setting this option to `ExplicitBegin` can be
    /// beneficial for specific transaction shapes:
    ///
    /// 1. When the transaction executes multiple parallel queries at the start of the transaction.
    ///    Only one query can include a `BeginTransaction` option, and all other queries must wait for
    ///    the first query to return the first result before they can proceed to execute. An explicit
    ///    `BeginTransaction` RPC will quickly return a transaction ID and allow all queries to start
    ///    execution in parallel once the transaction ID has been returned.
    /// 2. When the first query in the transaction could fail, and the application wants to catch the error
    ///    and continue executing subsequent queries in the same read-only transaction. With `InlineBegin`,
    ///    if the initial statement fails, the transaction is marked as permanently failed and will not
    ///    start; all subsequent queries on that transaction will also fail without starting a transaction
    ///    that excludes the first statement. Using `ExplicitBegin` establishes the snapshot transaction
    ///    upfront before any statements are executed, allowing subsequent statements to continue executing
    ///    at the same snapshot read timestamp even if an earlier statement fails.
    ///
    /// # Example: Retrying an Initial Query with ExplicitBegin
    ///
    /// Applications that want the latency benefit of `InlineBegin` for the
    /// initial query, but want to fall back to `ExplicitBegin` if the first
    /// statement fails, can implement a retry loop:
    ///
    /// ```
    /// # use google_cloud_spanner::client::DatabaseClient;
    /// # use google_cloud_spanner::result::ResultSet;
    /// # use google_cloud_spanner::statement::Statement;
    /// # use google_cloud_spanner::transaction::{
    /// #     BeginTransactionOption, MultiUseReadOnlyTransaction,
    /// # };
    /// # async fn execute_query_with_retry(
    /// #     database_client: &DatabaseClient,
    /// #     statement: &Statement,
    /// # ) -> Result<(MultiUseReadOnlyTransaction, ResultSet), google_cloud_spanner::Error> {
    /// // Note: For read-only transactions that execute queries in parallel at the start,
    /// // the recommendation is to use ExplicitBegin directly, rather than a retry loop.
    /// let mut last_error = None;
    /// for option in [
    ///     BeginTransactionOption::InlineBegin,
    ///     BeginTransactionOption::ExplicitBegin,
    /// ] {
    ///     let transaction = database_client
    ///         .read_only_transaction()
    ///         .set_begin_transaction_option(option)
    ///         .build()
    ///         .await?;
    ///
    ///     match transaction.execute_query(statement.clone()).await {
    ///         Ok(result_set) => return Ok((transaction, result_set)),
    ///         Err(error) => last_error = Some(error),
    ///     }
    /// }
    ///
    /// Err(last_error.expect("at least one attempt failed"))
    /// # }
    /// ```
    ///
    /// Default is `BeginTransactionOption::InlineBegin`.
    pub fn set_begin_transaction_option(mut self, option: BeginTransactionOption) -> Self {
        self.begin_transaction_option = option;
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
    /// let transaction = db_client.read_only_transaction()
    ///     .with_begin_attempt_timeout(Duration::from_secs(10))
    ///     .build()
    ///     .await?;
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
    /// let transaction = db_client.read_only_transaction()
    ///     .with_begin_retry_policy(NeverRetry)
    ///     .build()
    ///     .await?;
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
    /// let transaction = db_client.read_only_transaction()
    ///     .with_begin_backoff_policy(ExponentialBackoff::default())
    ///     .build()
    ///     .await?;
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

    /// Sets the timestamp bound for the read-only transaction.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::Spanner;
    /// # use google_cloud_spanner::transaction::TimestampBound;
    /// # async fn set_bound(spanner: Spanner) -> Result<(), google_cloud_spanner::Error> {
    /// let db_client = spanner.database_client("projects/p/instances/i/databases/d").build().await?;
    /// let builder = db_client.read_only_transaction().set_timestamp_bound(TimestampBound::strong());
    /// # Ok(())
    /// # }
    /// ```
    pub fn set_timestamp_bound(mut self, bound: TimestampBound) -> Self {
        self.timestamp_bound = Some(bound);
        self
    }

    /// Builds the [MultiUseReadOnlyTransaction] and starts the transaction
    /// by calling the `BeginTransaction` RPC.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::Spanner;
    /// # async fn build(spanner: Spanner) -> Result<(), google_cloud_spanner::Error> {
    /// let db_client = spanner.database_client("projects/p/instances/i/databases/d").build().await?;
    /// let tx = db_client.read_only_transaction().build().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn build(self) -> crate::Result<MultiUseReadOnlyTransaction> {
        let read_only = ReadOnly::default().set_return_read_timestamp(true);
        let read_only = match self.timestamp_bound {
            Some(bound) => read_only.set_timestamp_bound(bound.0),
            None => read_only.set_strong(true),
        };
        let options = TransactionOptions::default().set_read_only(read_only);

        let session_name = self.client.session_name();
        let affinity = Some(
            self.affinity
                .unwrap_or_else(|| Arc::new(TransactionAffinity::new_read_only())),
        );
        let selector = match self.begin_transaction_option {
            BeginTransactionOption::ExplicitBegin => {
                let response = execute_begin_transaction(
                    &self.client,
                    session_name.clone(),
                    options,
                    None,
                    &affinity,
                    self.begin_gax_options.clone().unwrap_or_default(),
                    None,
                )
                .await?;

                let transaction_selector =
                    crate::model::TransactionSelector::default().set_id(response.id);

                ReadContextTransactionSelector::Fixed(transaction_selector, response.read_timestamp)
            }
            BeginTransactionOption::InlineBegin => ReadContextTransactionSelector::Lazy(Arc::new(
                Mutex::new(TransactionState::NotStarted(options)),
            )),
        };

        Ok(MultiUseReadOnlyTransaction {
            context: ReadContext {
                session_name,
                client: self.client,
                transaction_selector: selector,
                precommit_token_tracker: PrecommitTokenTracker::new_noop(),
                transaction_tag: None,
                begin_transaction_request_options: self.begin_gax_options,
                affinity,
                aborted_state: AbortedState::new_noop(),
            },
        })
    }

    #[allow(dead_code)]
    pub(crate) fn with_affinity(mut self, affinity: Arc<TransactionAffinity>) -> Self {
        self.affinity = Some(affinity);
        self
    }
}

/// A multi-use read-only transaction. This transaction can be used for multiple read queries
/// ensuring consistency across all queries.
///
/// # Example
/// ```
/// # use google_cloud_spanner::client::Spanner;
/// # use google_cloud_spanner::statement::Statement;
/// # async fn run(spanner: Spanner) -> Result<(), google_cloud_spanner::Error> {
/// let db_client = spanner.database_client("projects/p/instances/i/databases/d").build().await?;
/// let tx = db_client.read_only_transaction().build().await?;
/// let stmt1 = Statement::builder("SELECT * FROM users WHERE id = @id")
///     .add_param("id", &42)
///     .build();
/// let mut rs1 = tx.execute_query(stmt1).await?;
///
/// let stmt2 = Statement::builder("SELECT * FROM other_table")
///     .build();
/// let mut rs2 = tx.execute_query(stmt2).await?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct MultiUseReadOnlyTransaction {
    pub(crate) context: ReadContext,
}

impl MultiUseReadOnlyTransaction {
    /// Returns the read timestamp chosen for the transaction.
    pub fn read_timestamp(&self) -> Option<wkt::Timestamp> {
        self.context.transaction_selector.read_timestamp()
    }

    /// Executes a query using this transaction.
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
    /// while let Some(row) = rs.next().await {
    ///     let _row = row?;
    ///     // process row
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Note
    /// If this transaction was configured with `BeginTransactionOption::InlineBegin` (the default)
    /// and this is the first statement, an execution failure will prevent the transaction from starting and
    /// mark it as permanently failed. All subsequent statements on this transaction will also fail. To allow a
    /// read-only transaction to continue executing subsequent statements after an initial statement failure,
    /// configure `BeginTransactionOption::ExplicitBegin` when building the transaction.
    pub async fn execute_query<T: Into<Statement>>(
        &self,
        statement: T,
    ) -> crate::Result<ResultSet> {
        self.context.execute_query(statement, None).await
    }

    /// Reads rows from the database using key lookups and scans, as a simple key/value style alternative to execute_query.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::Spanner;
    /// # use google_cloud_spanner::key::KeySet;
    /// # use google_cloud_spanner::read::ReadRequest;
    /// # use google_cloud_spanner::key;
    /// # async fn run(spanner: Spanner) -> Result<(), google_cloud_spanner::Error> {
    /// let db_client = spanner.database_client("projects/p/instances/i/databases/d").build().await?;
    /// let transaction = db_client.read_only_transaction().build().await?;
    ///
    /// // Read using the primary key
    /// let read_by_pk = ReadRequest::builder("Users", vec!["Id", "Name"]).with_keys(KeySet::all()).build();
    /// let mut result_set = transaction.execute_read(read_by_pk).await?;
    /// while let Some(row) = result_set.next().await {
    ///     let _row = row?;
    ///     // process row
    /// }
    ///
    /// // Read using a secondary index
    /// let read_by_index = ReadRequest::builder("Users", vec!["Id", "Name"])
    ///     .with_index("UsersByIndex", key![1_i64]).build();
    /// let mut result_set = transaction.execute_read(read_by_index).await?;
    /// while let Some(row) = result_set.next().await {
    ///     let _row = row?;
    ///     // process row
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Note
    /// If this transaction was configured with `BeginTransactionOption::InlineBegin` (the default)
    /// and this is the first statement, an execution failure will prevent the transaction from starting and
    /// mark it as permanently failed. All subsequent statements on this transaction will also fail. To allow a
    /// read-only transaction to continue executing subsequent statements after an initial statement failure,
    /// configure `BeginTransactionOption::ExplicitBegin` when building the transaction.
    pub async fn execute_read<T: Into<crate::read::ReadRequest>>(
        &self,
        read: T,
    ) -> crate::Result<ResultSet> {
        self.context.execute_read(read).await
    }

    /// Returns a reference to the transaction channel affinity handle, if set.
    #[allow(dead_code)]
    pub(crate) fn affinity(&self) -> Option<&TransactionAffinity> {
        self.context.affinity()
    }
}

/// Executes an explicit `BeginTransaction` RPC on Spanner.
pub(crate) async fn execute_begin_transaction<'a>(
    client: &DatabaseClient,
    session_name: String,
    options: TransactionOptions,
    transaction_tag: Option<String>,
    channel_target: impl Into<ChannelTarget<'a>>,
    request_options: GaxRequestOptions,
    mutation_key: Option<Mutation>,
) -> Result<Transaction> {
    let mut request = BeginTransactionRequest::default()
        .set_session(session_name)
        .set_options(options)
        .set_or_clear_mutation_key(mutation_key);
    if let Some(tag) = transaction_tag {
        request = request
            .set_request_options(crate::model::RequestOptions::default().set_transaction_tag(tag));
    }

    let response = client
        .begin_transaction(request, request_options, channel_target)
        .await?;
    if response.id.is_empty() {
        return Err(internal_error("Transaction ID was not returned by Spanner"));
    }
    Ok(response)
}

#[derive(Clone, Debug)]
pub(crate) enum ReadContextTransactionSelector {
    Fixed(crate::model::TransactionSelector, Option<wkt::Timestamp>),
    Lazy(Arc<Mutex<TransactionState>>),
}

#[derive(Clone, Debug)]
pub(crate) enum TransactionState {
    NotStarted(crate::model::TransactionOptions),
    Starting(crate::model::TransactionOptions, Arc<Notify>),
    Started(crate::model::TransactionSelector, Option<wkt::Timestamp>),
    Failed(TransactionStartFailure),
    FirstStatementFailed,
}

enum SelectorStatus {
    Ready(crate::model::TransactionSelector),
    Wait(OwnedNotified),
}

/// Represents the failure reason when a transaction fails to start.
///
/// Unlike [crate::Error], [TransactionStartFailure] implements [Clone] so it can be stored
/// in the shared [TransactionState::Failed] state and cleanly broadcast to any concurrent
/// followers or subsequent operations without loss of gRPC status, message, or AIP-193 error details.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum TransactionStartFailure {
    /// A gRPC service error returned by Spanner (e.g. `NotFound`, `PermissionDenied`).
    /// Preserves the exact status code, message, and all AIP-193 error details.
    Service(Status),
    /// An internal or client-side failure (e.g. Spanner failed to return a transaction ID).
    Internal(String),
}

impl TransactionStartFailure {
    pub(crate) fn from_error(err: &crate::Error) -> Self {
        if let Some(status) = err.status() {
            return Self::Service(status.clone());
        }

        if let Some(SpannerInternalError::UnexpectedData(message)) = err
            .source()
            .and_then(|source| source.downcast_ref::<SpannerInternalError>())
        {
            return Self::Internal(message.clone());
        }

        Self::Internal(err.to_string())
    }

    pub(crate) fn to_error(&self) -> crate::Error {
        match self {
            Self::Service(status) => {
                let error_description = if status.message.is_empty() {
                    format!("status code {}", status.code)
                } else {
                    status.message.clone()
                };
                let status = status.clone().set_message(format!(
                    "Transaction cannot be used: initial statement failed with: {error_description}"
                ));
                crate::Error::service(status)
            }
            Self::Internal(message) => internal_error(format!(
                "Transaction cannot be used: initial statement failed with: {message}"
            )),
        }
    }
}

impl ReadContextTransactionSelector {
    /// Acquires an exclusive mutex lock on the inner lazy transaction state, recovering from lock poisoning via `into_inner()`.
    ///
    /// Returns `None` if this is a `Fixed` transaction selector.
    ///
    /// # Poison Recovery Rationale
    /// `lazy` protects the transaction lifecycle state (`TransactionState`). State transitions are
    /// atomic enum assignments; the underlying data is always memory-safe and structurally sound
    /// in Rust even if an earlier thread panicked. Recovering the guard via `into_inner()` ensures
    /// that an isolated panic in an application query, streaming task, or fallback routine does not
    /// permanently poison the selector lock and cascade into fatal panics across concurrent waiters,
    /// subsequent statements, commit attempts, or cleanup routines.
    fn lock_state(&self) -> Option<MutexGuard<'_, TransactionState>> {
        match self {
            Self::Lazy(lazy) => Some(lazy.lock().unwrap_or_else(PoisonError::into_inner)),
            Self::Fixed(_, _) => None,
        }
    }

    /// Returns whether this is a lazy transaction selector.
    pub(crate) fn is_lazy(&self) -> bool {
        matches!(self, Self::Lazy(_))
    }

    /// Returns whether the transaction has successfully started.
    pub(crate) fn is_started(&self) -> bool {
        if let Self::Fixed(selector, _) = self {
            return matches!(selector.selector, Some(Selector::Id(_)));
        }
        if let Some(guard) = self.lock_state() {
            return matches!(&*guard, TransactionState::Started(_, _));
        }
        false
    }

    pub(crate) async fn selector(&self) -> crate::Result<crate::model::TransactionSelector> {
        match self {
            Self::Fixed(selector, _) => Ok(selector.clone()),
            Self::Lazy(_) => loop {
                match self.poll_selector_status()? {
                    SelectorStatus::Ready(selector) => return Ok(selector),
                    SelectorStatus::Wait(notified) => notified.await,
                }
            },
        }
    }

    /// Inspects the current lazy selector state returning whether it is ready,
    /// failed, or needs to wait for the transaction to start.
    fn poll_selector_status(&self) -> crate::Result<SelectorStatus> {
        let Some(mut guard) = self.lock_state() else {
            unreachable!("poll_selector_status called on non-Lazy selector");
        };

        // Fast path: Transaction is already started.
        if let TransactionState::Started(selector, _) = &*guard {
            return Ok(SelectorStatus::Ready(selector.clone()));
        }

        // If the transaction has not started, extract options and transition the state to Starting.
        let pending_options = if let TransactionState::NotStarted(options) = &*guard {
            Some(options.clone())
        } else {
            // The state is either Starting or Failed. Concurrent threads will yield None here
            // and fall through to either wait for the leader or fail immediately.
            None
        };
        if let Some(options) = pending_options {
            // This thread becomes the "leader" and will start the transaction.
            let notify = Arc::new(Notify::new());
            *guard = TransactionState::Starting(options.clone(), Arc::clone(&notify));
            return Ok(SelectorStatus::Ready(
                crate::model::TransactionSelector::default().set_begin(options),
            ));
        }

        // Handle other states: yield error or wait.
        match &*guard {
            // Note: Failed will be reached if either:
            // 1. The transaction was configured with explicit BeginTransaction and the BeginTransaction RPC failed.
            // 2. Or the initial statement with inlined begin failed, marking the transaction as failed.
            TransactionState::Failed(failure) => Err(failure.to_error()),
            // FirstStatementFailed is reached if the initial statement fails in a Read-Write transaction using inline-begin.
            // When in this state, the transaction must first call an explicit BeginTransaction RPC.
            // Any subsequent statements or commit attempts should fail fast.
            // Returning Aborted signals the TransactionRunner to retry the transaction with an explicit BeginTransaction RPC.
            TransactionState::FirstStatementFailed => {
                Err(aborted_due_to_failed_initial_statement())
            }
            // Transaction is starting. Register the Notified future while holding the lock to prevent lost wakeups.
            TransactionState::Starting(_, notify) => {
                Ok(SelectorStatus::Wait(Arc::clone(notify).notified_owned()))
            }
            TransactionState::Started(_, _) | TransactionState::NotStarted(_) => unreachable!(),
        }
    }
}

pub(crate) struct ExplicitBeginParams {
    pub(crate) client: DatabaseClient,
    pub(crate) session_name: String,
    pub(crate) transaction_tag: Option<String>,
    pub(crate) request_options: GaxRequestOptions,
    pub(crate) precommit_token_tracker: PrecommitTokenTracker,
    pub(crate) mutation_key: Option<Mutation>,
    pub(crate) affinity: Option<Arc<TransactionAffinity>>,
}

impl ReadContextTransactionSelector {
    /// Explicitly begins a transaction if the transaction selector is a `Lazy`
    /// selector and the transaction has not yet been started.
    pub(crate) async fn begin_explicitly(&self, params: ExplicitBeginParams) -> crate::Result<()> {
        if !self.is_lazy() {
            return Ok(());
        }

        enum FallbackAction {
            Begin(crate::model::TransactionOptions, Option<Arc<Notify>>),
            Wait(OwnedNotified),
            Done(crate::Result<()>),
        }

        // We loop until a definitive outcome is reached:
        // - If `NotStarted`, this thread becomes the leader, transitions to `Starting`,
        //   and breaks out of the loop to execute the BeginTransaction RPC.
        // - If `Starting` and this thread is a follower, it waits for the leader's `Notify`
        //   and loops back to re-evaluate the state upon waking up. Re-checking is essential:
        //   1. If the leader succeeded, the state is `Started` and we return `Ok(())`.
        //   2. If the leader failed, the state is `Failed` (or `FirstStatementFailed`)
        //      and we return the leader's error rather than Ok(()).
        //   3. If the leader was cancelled before completing, `on_cancelled_starting` reset the
        //      state to `NotStarted`, allowing this follower to elect itself as the new leader
        //      and begin the transaction.
        // - If already in a terminal state (`Started` or `Failed`), return the result immediately.
        let (options, notify_opt) = loop {
            let action = {
                let Some(mut guard) = self.lock_state() else {
                    return Ok(());
                };
                match &*guard {
                    TransactionState::NotStarted(options) => {
                        // The transaction has not started yet. This thread becomes the "leader"
                        // and transitions the state to Starting before performing the BeginTransaction RPC.
                        let options = options.clone();
                        let notify = Arc::new(Notify::new());
                        *guard = TransactionState::Starting(options.clone(), Arc::clone(&notify));
                        FallbackAction::Begin(options, Some(notify))
                    }
                    TransactionState::Starting(_options, notify) => {
                        // The transaction is already in the process of starting. This thread is a follower
                        // and must wait for the leader.
                        FallbackAction::Wait(Arc::clone(notify).notified_owned())
                    }
                    TransactionState::Started(_, _) => FallbackAction::Done(Ok(())),
                    TransactionState::Failed(failure) => {
                        FallbackAction::Done(Err(failure.to_error()))
                    }
                    TransactionState::FirstStatementFailed => {
                        FallbackAction::Done(Err(aborted_due_to_failed_initial_statement()))
                    }
                }
            };

            match action {
                FallbackAction::Done(result) => return result,
                FallbackAction::Wait(notified) => notified.await,
                FallbackAction::Begin(opts, notif) => break (opts, notif),
            }
        };

        // Only the leader thread will reach this point to perform the explicit begin.
        // Waiters are blocked in `poll_selector_status` waiting for the result,
        // and already completed states return early above.
        let mut start_guard = LazyTransactionStartGuard::new(Some(self.clone()));

        let response = match execute_begin_transaction(
            &params.client,
            params.session_name,
            options,
            params.transaction_tag,
            &params.affinity,
            params.request_options,
            params.mutation_key,
        )
        .await
        {
            Ok(r) => r,
            Err(e) => {
                if let Some(mut guard) = self.lock_state() {
                    *guard = TransactionState::Failed(TransactionStartFailure::from_error(&e));
                    drop(guard);
                }
                // Release the lock and notify all the waiting queries that
                // the transaction has failed.
                if let Some(notify) = notify_opt {
                    notify.notify_waiters();
                }

                start_guard.disarm();
                return Err(e);
            }
        };

        params
            .precommit_token_tracker
            .update(response.precommit_token);
        self.update(response.id, response.read_timestamp)?;
        start_guard.disarm();

        Ok(())
    }

    /// Updates the transaction state to `Started` with the given transaction ID and optional
    /// read timestamp.
    ///
    /// This method is called when a transaction has successfully been initiated (either via
    /// an inline begin in a query or an explicit `BeginTransaction` RPC).
    ///
    /// If the previous state was `Starting`, it will notify all concurrent threads that were
    /// waiting for the transaction to start.
    pub(crate) fn update(
        &self,
        id: bytes::Bytes,
        timestamp: Option<wkt::Timestamp>,
    ) -> crate::Result<()> {
        let Some(mut guard) = self.lock_state() else {
            return Ok(());
        };

        if matches!(
            &*guard,
            TransactionState::NotStarted(_) | TransactionState::Starting(_, _)
        ) {
            // Atomically transition the state to Started and extract the previous state.
            // We do this to take ownership of the Notify handle (if it was Starting)
            // so we can notify waiters after dropping the lock.
            let previous_state = replace(
                &mut *guard,
                TransactionState::Started(TransactionSelector::default().set_id(id), timestamp),
            );
            drop(guard);

            // Notify all queries that are waiting for the transaction.
            if let TransactionState::Starting(_, notify) = previous_state {
                notify.notify_waiters();
            }
            Ok(())
        } else if let TransactionState::Started(existing_selector, _) = &*guard {
            // Spanner returns the transaction ID on all statements executed within that transaction
            // when using multiplexed sessions.If the transaction has already started with the same ID,
            // this is expected behavior and can be ignored.
            if existing_selector.id() == Some(&id) {
                Ok(())
            } else {
                Err(crate::error::internal_error(
                    "got a transaction id for an already Started or Failed transaction",
                ))
            }
        } else {
            // This should never happen.
            Err(crate::error::internal_error(
                "got a transaction id for an already Started or Failed transaction",
            ))
        }
    }

    /// Returns the transaction ID if it is already available, without waiting.
    ///
    /// This method inspects the selector and returns the transaction ID if the
    /// transaction has already started. It returns `None` if the transaction
    /// has not yet started or is in a state without an ID.
    pub(crate) fn get_id_no_wait(&self) -> crate::Result<Option<bytes::Bytes>> {
        match self {
            Self::Fixed(selector, _) => {
                if let Some(Selector::Id(id)) = &selector.selector {
                    return Ok(Some(id.clone()));
                }
            }
            Self::Lazy(_) => {
                if let Some(guard) = self.lock_state()
                    && let TransactionState::Started(selector, _) = &*guard
                    && let Some(Selector::Id(id)) = &selector.selector
                {
                    return Ok(Some(id.clone()));
                }
            }
        }
        Ok(None)
    }

    /// Returns whether the transaction selector is currently in the `Starting` state.
    pub(crate) fn is_starting(&self) -> bool {
        let Some(guard) = self.lock_state() else {
            return false;
        };
        matches!(&*guard, TransactionState::Starting(_, _))
    }

    /// Handles the cancellation of an operation that was starting the transaction.
    ///
    /// If the transaction was in the `Starting` state, transitions the state appropriately
    /// and wakes any waiting concurrent operations:
    /// - For read-write transactions: transitions to `FirstStatementFailed`, so concurrent
    ///   operations fail fast with `Aborted` and the runner retries with an explicit `BeginTransaction`.
    /// - For read-only transactions: resets to `NotStarted`, allowing another query to attempt
    ///   the inlined begin.
    pub(crate) fn on_cancelled_starting(&self) {
        let Some(mut guard) = self.lock_state() else {
            return;
        };

        if let TransactionState::Starting(options, notify) = &*guard {
            let notify = Arc::clone(notify);
            let is_read_write = matches!(&options.mode, Some(Mode::ReadWrite(_)));
            if is_read_write {
                *guard = TransactionState::FirstStatementFailed;
            } else {
                *guard = TransactionState::NotStarted(options.clone());
            }
            drop(guard);
            notify.notify_waiters();
        }
    }

    pub(crate) fn is_first_statement_failed(&self) -> bool {
        let Some(guard) = self.lock_state() else {
            return false;
        };
        matches!(&*guard, TransactionState::FirstStatementFailed)
    }

    /// Returns the selector to use when restarting a streaming SQL query or streaming read.
    ///
    /// This method is called exclusively by [`ResultSet::restart_stream`] when retrying
    /// after a transient stream failure. Unary RPCs (such as DML `ExecuteSql` or `ExecuteBatchDml`)
    /// do not use this method because GAX automatically retries the cloned unary request.
    ///
    /// Unlike [`selector()`](Self::selector), which returns `Wait(notified)` when the transaction
    /// is in the `Starting` state (which would cause a stream leader to self-deadlock waiting on itself),
    /// this method inspects the state synchronously:
    /// - If the transaction has already started (`Started`), returns the established selector.
    /// - If the transaction is in the process of starting (`Starting`), returns a selector
    ///   with the inlined begin options so the restart request can re-attempt the inlined begin.
    /// - If the transaction reached a failure state (`Failed` or `FirstStatementFailed`),
    ///   returns the corresponding error immediately.
    /// - A stream restart should never occur when the selector is `NotStarted`, as a stream
    ///   must have already been initiated to require a restart.
    pub(crate) fn selector_for_restart(&self) -> crate::Result<crate::model::TransactionSelector> {
        match self {
            Self::Fixed(selector, _) => Ok(selector.clone()),
            Self::Lazy(lazy) => {
                let guard = lazy.lock().unwrap_or_else(PoisonError::into_inner);
                match &*guard {
                    TransactionState::Started(selector, _) => Ok(selector.clone()),
                    TransactionState::Starting(options, _) => {
                        Ok(crate::model::TransactionSelector::default().set_begin(options.clone()))
                    }
                    TransactionState::NotStarted(_) => Err(internal_error(
                        "Cannot restart stream on a NotStarted transaction",
                    )),
                    TransactionState::Failed(failure) => Err(failure.to_error()),
                    TransactionState::FirstStatementFailed => {
                        Err(aborted_due_to_failed_initial_statement())
                    }
                }
            }
        }
    }

    /// Transitions the transaction state to a failed state and wakes any concurrent
    /// operations parked waiting for an inlined begin to finish.
    ///
    /// # Rationale
    /// When an initial statement in a transaction includes an inlined `BeginTransaction`
    /// option and fails before receiving a transaction ID:
    /// - **Read-Write Transactions**: We must not automatically begin a transaction or
    ///   restart the stream. The error is returned directly to the application closure.
    ///   If the error is `Aborted` (or emulator error), the state transitions to `Failed`.
    ///   Parked followers wake up and fail fast with the leader's error without creating orphaned
    ///   transactions, while the runner retry loop retries with an inlined begin.
    ///   If non-retryable (e.g., `InvalidArgument`), the state transitions to
    ///   `FirstStatementFailed` and wakes any concurrent waiters. The runner will retry
    ///   with an explicit `BeginTransaction` RPC.
    /// - **Read-Only Transactions**: The state transitions to `Failed` and wakes any waiting
    ///   concurrent operations. The transaction fails fast; no fallback `BeginTransaction` RPC
    ///   is sent, and subsequent operations on the transaction will also fail. Applications
    ///   requiring resilience to initial statement failures should configure
    ///   `BeginTransactionOption::ExplicitBegin`.
    /// - **Already Started / Non-Lazy**: If the transaction is already established or is not
    ///   in the `Starting` state, this is a no-op.
    pub(crate) fn set_failed(&self, error: &Error, is_emulator: bool) {
        let Some(mut guard) = self.lock_state() else {
            return;
        };
        if let TransactionState::Starting(options, notify) = &*guard {
            let notify = Arc::clone(notify);
            let is_read_write = matches!(&options.mode, Some(Mode::ReadWrite(_)));
            // If the initial statement in a ReadWrite transaction failed with a non-aborted error
            // (e.g. InvalidArgument or syntax error), Spanner never started a transaction.
            // Marking `FirstStatementFailed` ensures that if the application catches this error
            // and attempts subsequent statements, they fail fast with a synthetic Aborted error,
            // signaling TransactionRunner to retry with an explicit BeginTransaction RPC so that
            // subsequent operations can execute within an active transaction.
            //
            // If the initial statement failed with a genuine Aborted error (concurrency conflict),
            // retrying with InlineBegin is still preferred to preserve single-hop efficiency, so
            // we transition to `Failed` and let the runner perform a normal backoff retry.
            if is_read_write && !is_aborted_or_emulator_error(error, is_emulator) {
                *guard = TransactionState::FirstStatementFailed;
            } else {
                tracing::debug!(
                    "Initial statement in read-only transaction failed; marking transaction as permanently failed. \
                    All subsequent statements on this transaction will fail. \
                    Hint: If your application needs to handle errors on the first statement and continue \
                    using the read-only transaction, configure `BeginTransactionOption::ExplicitBegin`. \
                    Error: {error:?}"
                );
                *guard = TransactionState::Failed(TransactionStartFailure::from_error(error));
            }
            drop(guard);
            notify.notify_waiters();
        }
    }

    pub(crate) fn check_failed(&self) -> crate::Result<()> {
        let Some(guard) = self.lock_state() else {
            return Ok(());
        };
        match &*guard {
            TransactionState::Failed(failure) => Err(failure.to_error()),
            TransactionState::FirstStatementFailed => {
                Err(aborted_due_to_failed_initial_statement())
            }
            _ => Ok(()),
        }
    }

    /// Returns the read timestamp of the transaction, if available.
    ///
    /// For `Fixed` transactions, this returns the timestamp stored in the variant.
    /// For `Lazy` transactions, this returns the timestamp once the transaction has successfully started
    /// and yielded a timestamp. Returns `None` if the transaction has not started or did not yield a timestamp.
    pub(crate) fn read_timestamp(&self) -> Option<wkt::Timestamp> {
        match self {
            Self::Fixed(_, timestamp) => *timestamp,
            Self::Lazy(_) => match self.lock_state().as_deref() {
                Some(TransactionState::Started(_, timestamp)) => *timestamp,
                _ => None,
            },
        }
    }
}

/// A guard that manages the `Starting` state of a transaction during an operation.
///
/// When an operation initiates an inlined `BeginTransaction`, it transitions the
/// transaction state to `Starting`. If that operation is cancelled (e.g. timeout,
/// cancellation of future before completion) without disarming this guard,
/// `on_cancelled_starting()` is called to safely transition the state and wake any
/// waiting concurrent operations.
pub(crate) struct LazyTransactionStartGuard {
    selector: Option<ReadContextTransactionSelector>,
}

impl LazyTransactionStartGuard {
    pub(crate) fn new(selector: Option<ReadContextTransactionSelector>) -> Self {
        Self { selector }
    }

    pub(crate) fn disarm(&mut self) {
        self.selector = None;
    }
}

impl Drop for LazyTransactionStartGuard {
    fn drop(&mut self) {
        if let Some(selector) = self.selector.take() {
            selector.on_cancelled_starting();
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct AbortedInfo {
    pub(crate) status: Status,
    pub(crate) retry_delay: Option<Duration>,
}

impl AbortedInfo {
    pub(crate) fn from_error(error: &Error) -> Option<Self> {
        let status = error.status()?.clone();
        let retry_delay = extract_retry_delay_from_error(error);
        Some(Self {
            status,
            retry_delay,
        })
    }

    pub(crate) fn to_error(&self) -> Error {
        Error::service(self.status.clone())
    }
}

#[derive(Clone, Debug)]
pub(crate) enum AbortedState {
    NoOp,
    Track(Arc<OnceLock<AbortedInfo>>),
}

impl AbortedState {
    pub(crate) fn new() -> Self {
        Self::Track(Arc::new(OnceLock::new()))
    }

    pub(crate) fn new_noop() -> Self {
        Self::NoOp
    }

    pub(crate) fn record(&self, error: &Error, is_emulator: bool) {
        if let Self::Track(info_lock) = self {
            if info_lock.get().is_some() || !is_aborted_or_emulator_error(error, is_emulator) {
                return;
            }
            if let Some(info) = AbortedInfo::from_error(error) {
                let _ = info_lock.set(info);
            }
        }
    }

    pub(crate) fn aborted_info(&self) -> Option<&AbortedInfo> {
        match self {
            Self::Track(info_lock) => info_lock.get(),
            Self::NoOp => None,
        }
    }
}

impl Default for AbortedState {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ReadContext {
    pub(crate) session_name: String,
    pub(crate) client: DatabaseClient,
    pub(crate) transaction_selector: ReadContextTransactionSelector,
    pub(crate) precommit_token_tracker: PrecommitTokenTracker,
    pub(crate) transaction_tag: Option<String>,
    pub(crate) begin_transaction_request_options: Option<GaxRequestOptions>,
    pub(crate) affinity: Option<Arc<TransactionAffinity>>,
    pub(crate) aborted_state: AbortedState,
}

impl ReadContext {
    /// Amends the given request options with the transaction tag if present.
    ///
    /// This method returns the `RequestOptions` that should be used for the request.
    /// If no `transaction_tag` has been set, the given `RequestOptions` is returned unchanged.
    /// If a `transaction_tag` has been set, the given `RequestOptions` is modified to include the tag
    /// (or a new `RequestOptions` is created if `None` was passed in).
    pub(crate) fn amend_request_options(
        &self,
        mut options: Option<crate::model::RequestOptions>,
    ) -> Option<crate::model::RequestOptions> {
        if let Some(tag) = &self.transaction_tag {
            options
                .get_or_insert_with(crate::model::RequestOptions::default)
                .transaction_tag = tag.clone();
        }
        options
    }

    /// Attempts to execute an explicit `begin_transaction` RPC if the current transaction
    /// selector is still in the `Lazy(NotStarted)` state (for example, when committing a
    /// transaction without preceding read or query statements).
    pub(crate) async fn begin_explicitly_if_not_started(
        &self,
        fallback_options: GaxRequestOptions,
        mutation_key: Option<Mutation>,
    ) -> Result<bool> {
        if !self.transaction_selector.is_lazy() || self.transaction_selector.is_started() {
            return Ok(false);
        }

        let options = merge_request_options(
            fallback_options,
            self.begin_transaction_request_options.as_ref(),
        );

        self.transaction_selector
            .begin_explicitly(ExplicitBeginParams {
                client: self.client.clone(),
                session_name: self.session_name.clone(),
                transaction_tag: self.transaction_tag.clone(),
                request_options: options,
                precommit_token_tracker: self.precommit_token_tracker.clone(),
                mutation_key,
                affinity: self.affinity.as_ref().map(Arc::clone),
            })
            .await
            .inspect_err(|error| self.record_if_aborted(error))?;
        Ok(true)
    }

    /// Returns a reference to the transaction channel affinity handle, if set.
    #[allow(dead_code)]
    pub(crate) fn affinity(&self) -> Option<&TransactionAffinity> {
        self.affinity.as_deref()
    }

    pub(crate) async fn selector(&self) -> Result<crate::model::TransactionSelector> {
        self.transaction_selector.selector().await
    }

    pub(crate) fn record_if_aborted(&self, error: &Error) {
        self.aborted_state.record(error, self.client.is_emulator());
    }

    pub(crate) fn aborted_info(&self) -> Option<&AbortedInfo> {
        self.aborted_state.aborted_info()
    }
}

/// Merges the configured fields from a `source` `RequestOptions` into a `destination` `RequestOptions`.
/// Configured options in `source` will override those in `destination`.
pub(crate) fn merge_request_options(
    mut destination: GaxRequestOptions,
    source: Option<&GaxRequestOptions>,
) -> GaxRequestOptions {
    let Some(source) = source else {
        return destination;
    };

    if let Some(timeout) = source.attempt_timeout() {
        destination.set_attempt_timeout(*timeout);
    }
    if let Some(retry) = source.retry_policy() {
        destination.set_retry_policy(retry.clone());
    }
    if let Some(backoff) = source.backoff_policy() {
        destination.set_backoff_policy(backoff.clone());
    }
    if let Some(source_headers) = source.get_extension::<HeaderMap>() {
        let destination_headers = destination.get_extension_or_default_mut::<HeaderMap>();
        for (name, value) in source_headers.iter() {
            destination_headers.insert(name.clone(), value.clone());
        }
    }
    destination
}

/// Helper macro to execute a streaming SQL or streaming read RPC with lazy transaction start tracking.
macro_rules! execute_stream_with_retry {
    ($self:expr, $request:ident, $gax_options:ident, $operation_variant:path, $method_name:expr) => {{
        if let Some(info) = $self.aborted_info() {
            return Err(info.to_error());
        }
        let is_starting = matches!(
            $request
                .transaction
                .as_ref()
                .and_then(|t| t.selector.as_ref()),
            Some(crate::model::transaction_selector::Selector::Begin(_))
        );
        // RAII cancellation guard: If this future is dropped/cancelled at any .await point
        // before completion, the guard's Drop handler executes `on_cancelled_starting()`.
        // This wakes any concurrent followers parked waiting for this inlined begin to finish
        // and safely transitions the state (resetting to NotStarted for read-only, or marking
        // FirstStatementFailed for read-write), preventing permanent follower deadlocks.
        //
        // Because `Drop` serves as the abnormal exit / cancellation trigger, any path that
        // runs to normal completion (whether Ok or a handled Err) must explicitly call
        // `start_guard.disarm()` so the Drop handler does not treat deliberate completion
        // as an unexpected cancellation.
        let mut start_guard =
            LazyTransactionStartGuard::new(is_starting.then(|| $self.transaction_selector.clone()));
        let result = Box::pin(ResultSet::create(ResultSetParams {
            transaction_selector: Some($self.transaction_selector.clone()),
            precommit_token_tracker: $self.precommit_token_tracker.clone(),
            client: $self.client.clone(),
            operation: $operation_variant($request),
            gax_options: $gax_options,
            method_name: $method_name,
            affinity: $self.affinity.as_ref().map(Arc::clone),
            aborted_state: $self.aborted_state.clone(),
        }))
        .await;

        if is_starting {
            if let Err(ref e) = result {
                $self.record_if_aborted(e);
                $self
                    .transaction_selector
                    .set_failed(e, $self.client.is_emulator());
            }
        }
        start_guard.disarm();
        result
    }};
}

impl ReadContext {
    pub(crate) async fn execute_query<T: Into<Statement>>(
        &self,
        statement: T,
        seqno: Option<i64>,
    ) -> Result<ResultSet> {
        if let Some(info) = self.aborted_info() {
            return Err(info.to_error());
        }
        let statement = statement.into();
        let gax_options = statement.gax_options().clone();
        let selector = self.selector().await?;
        let mut request = statement
            .into_request()
            .set_session(self.session_name.clone())
            .set_transaction(selector)
            .set_seqno(seqno.unwrap_or(0));
        request.request_options = self.amend_request_options(request.request_options);

        execute_stream_with_retry!(
            self,
            request,
            gax_options,
            StreamOperation::Query,
            "ExecuteStreamingSql"
        )
    }

    pub(crate) async fn execute_read<T: Into<crate::read::ReadRequest>>(
        &self,
        read: T,
    ) -> Result<ResultSet> {
        if let Some(info) = self.aborted_info() {
            return Err(info.to_error());
        }
        let read = read.into();
        let gax_options = read.gax_options.clone();
        let selector = self.selector().await?;
        let mut request = read
            .into_request()
            .set_session(self.session_name.clone())
            .set_transaction(selector);
        request.request_options = self.amend_request_options(request.request_options);

        execute_stream_with_retry!(
            self,
            request,
            gax_options,
            StreamOperation::Read,
            "StreamingRead"
        )
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::RequestOptions;
    use crate::key::KeySet;
    use crate::model::ExecuteSqlRequest;
    use crate::model::transaction_selector::Selector as ModelSelector;
    use crate::read::ReadRequest;
    use crate::request_id_interceptor::REQUEST_ID_HEADER;
    use crate::result_set::tests::adapt;
    use crate::result_set::tests::string_val;
    use crate::retry_policy::SpannerRetryPolicy;
    use crate::statement::Statement;
    use crate::value::Value;
    use gaxi::grpc::tonic::{self, Code, Response, Status};
    use google_cloud_gax::backoff_policy::BackoffPolicy;
    use google_cloud_gax::error::rpc::Code as GaxCode;
    use google_cloud_gax::error::rpc::Status as GaxStatus;
    use google_cloud_gax::error::rpc::StatusDetails;
    use google_cloud_gax::exponential_backoff::{ExponentialBackoff, ExponentialBackoffBuilder};
    use google_cloud_gax::retry_policy::{NeverRetry, RetryPolicyExt};
    use google_cloud_gax::retry_state::RetryState;
    use google_cloud_rpc::model::RetryInfo;
    use google_cloud_test_macros::tokio_test_no_panics;
    use http::{HeaderMap, HeaderName, HeaderValue};
    use mock_v1::transaction_selector::Selector;
    use mockall::Sequence;
    use prost_types::Timestamp;
    use spanner_grpc_mock::MockSpanner;
    use spanner_grpc_mock::google::spanner::v1 as mock_v1;
    use std::sync::mpsc::channel as std_channel;
    use std::sync::{Arc, Mutex as StdMutex};
    use std::time::Duration;
    use tokio::spawn;
    use tokio::sync::oneshot::channel as oneshot_channel;
    use tokio::sync::{Barrier, Mutex, Notify, mpsc};
    use tokio::task::yield_now;
    use wkt::Duration as WktDuration;

    mockall::mock! {
        #[derive(Debug)]
        BackoffPolicy {}
        impl BackoffPolicy for BackoffPolicy {
            fn on_failure(&self, state: &RetryState) -> Duration;
        }
    }

    #[test]
    fn auto_traits() {
        use std::fmt::Debug;
        static_assertions::assert_impl_all!(SingleUseReadOnlyTransactionBuilder: Debug, Send, Sync);
        static_assertions::assert_impl_all!(SingleUseReadOnlyTransaction: Debug, Send, Sync);
        static_assertions::assert_impl_all!(MultiUseReadOnlyTransactionBuilder: Debug, Send, Sync);
        static_assertions::assert_impl_all!(MultiUseReadOnlyTransaction: Debug, Send, Sync);
        static_assertions::assert_impl_all!(ReadContext: Debug, Send, Sync);
        static_assertions::assert_impl_all!(
            BeginTransactionOption: Send,
            Sync,
            Clone,
            Copy,
            Debug,
            Default,
            PartialEq,
            Eq
        );
    }

    pub(crate) fn create_session_mock() -> MockSpanner {
        let mut mock = MockSpanner::new();
        mock.expect_create_session().once().returning(|_| {
            Ok(Response::new(mock_v1::Session {
                name: "projects/p/instances/i/databases/d/sessions/123".to_string(),
                ..Default::default()
            }))
        });
        mock
    }

    pub(crate) fn setup_select1() -> spanner_grpc_mock::google::spanner::v1::PartialResultSet {
        spanner_grpc_mock::google::spanner::v1::PartialResultSet {
            metadata: Some(spanner_grpc_mock::google::spanner::v1::ResultSetMetadata {
                row_type: Some(spanner_grpc_mock::google::spanner::v1::StructType {
                    fields: vec![Default::default()],
                }),
                ..Default::default()
            }),
            values: vec![prost_types::Value {
                kind: Some(prost_types::value::Kind::StringValue("1".to_string())),
            }],
            last: true,
            ..Default::default()
        }
    }

    pub(crate) fn setup_select1_with_transaction_id(
        transaction_id: Vec<u8>,
    ) -> spanner_grpc_mock::google::spanner::v1::PartialResultSet {
        let mut result_set = setup_select1();
        result_set
            .metadata
            .as_mut()
            .expect("metadata present")
            .transaction = Some(spanner_grpc_mock::google::spanner::v1::Transaction {
            id: transaction_id,
            read_timestamp: Some(Timestamp {
                seconds: 1234567890,
                nanos: 0,
            }),
            ..Default::default()
        });
        result_set
    }

    pub(crate) async fn setup_db_client(
        mock: spanner_grpc_mock::MockSpanner,
    ) -> (DatabaseClient, tokio::task::JoinHandle<()>) {
        use crate::client::Spanner;
        use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
        let (address, server) = spanner_grpc_mock::start("0.0.0.0:0", mock)
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
            .expect("Failed to create DatabaseClient");

        (db_client, server)
    }

    #[tokio_test_no_panics]
    async fn single_use_builder() {
        let mock = create_session_mock();

        let (db_client, _server) = setup_db_client(mock).await;

        let tx = db_client.single_use().build();
        let selector = tx
            .context
            .transaction_selector
            .selector()
            .await
            .expect("Failed to get selector");
        let ro = selector
            .single_use()
            .expect("Expected SingleUse selector")
            .read_only()
            .expect("Expected ReadOnly mode");
        assert_eq!(
            ro.timestamp_bound,
            Some(crate::model::transaction_options::read_only::TimestampBound::Strong(true))
        );

        let tx2 = db_client
            .single_use()
            .set_timestamp_bound(crate::timestamp_bound::TimestampBound::max_staleness(
                std::time::Duration::from_secs(10),
            ))
            .build();
        let selector = tx2
            .context
            .transaction_selector
            .selector()
            .await
            .expect("Failed to get selector");
        let ro2 = selector
            .single_use()
            .expect("Expected SingleUse selector")
            .read_only()
            .expect("Expected ReadOnly mode");
        assert_eq!(
            ro2.timestamp_bound,
            Some(
                crate::model::transaction_options::read_only::TimestampBound::MaxStaleness(
                    Box::new(wkt::Duration::new(10, 0).expect("failed to create Duration"))
                )
            )
        );
    }

    #[tokio_test_no_panics]
    async fn execute_single_query() {
        use super::super::result_set::tests::string_val;
        use crate::statement::Statement;
        use crate::value::Value;

        let mut mock = create_session_mock();

        mock.expect_execute_streaming_sql().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(
                req.session,
                "projects/p/instances/i/databases/d/sessions/123"
            );
            assert_eq!(req.sql, "SELECT 1");

            Ok(gaxi::grpc::tonic::Response::from(adapt([Ok(
                setup_select1(),
            )])))
        });

        let (db_client, _server) = setup_db_client(mock).await;

        let tx = db_client.single_use().build();
        let mut rs = tx
            .execute_query(Statement::builder("SELECT 1").build())
            .await
            .expect("Failed to execute query");

        let row = rs.next().await.expect("has row").expect("has valid row");
        assert_eq!(row.raw_values(), [Value(string_val("1"))]);
        let result = rs.next().await;
        assert!(result.is_none(), "expected None, got {result:?}");
    }

    #[tokio_test_no_panics]
    async fn execute_multi_query() {
        use super::super::result_set::tests::string_val;
        use crate::statement::Statement;
        use crate::value::Value;
        use spanner_grpc_mock::google::spanner::v1 as mock_v1;

        let mut mock = create_session_mock();

        mock.expect_begin_transaction().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(
                req.session,
                "projects/p/instances/i/databases/d/sessions/123"
            );
            Ok(tonic::Response::new(mock_v1::Transaction {
                id: vec![1, 2, 3],
                // prost_types::Timestamp fields need to be explicitly set because default is 0 for both
                read_timestamp: Some(prost_types::Timestamp {
                    seconds: 123456789,
                    nanos: 0,
                }),
                ..Default::default()
            }))
        });

        mock.expect_execute_streaming_sql()
            .times(2)
            .returning(|req| {
                let req = req.into_inner();
                assert_eq!(
                    req.session,
                    "projects/p/instances/i/databases/d/sessions/123"
                );
                assert_eq!(
                    req.transaction
                        .expect("transaction should be present")
                        .selector
                        .expect("selector should be present"),
                    mock_v1::transaction_selector::Selector::Id(vec![1, 2, 3])
                );

                Ok(gaxi::grpc::tonic::Response::from(adapt([Ok(
                    setup_select1(),
                )])))
            });

        let (db_client, _server) = setup_db_client(mock).await;

        let tx = db_client
            .read_only_transaction()
            .set_begin_transaction_option(BeginTransactionOption::ExplicitBegin)
            .build()
            .await
            .expect("Failed to start tx");
        assert_eq!(
            tx.read_timestamp()
                .expect("expected read timestamp")
                .seconds(),
            123456789
        );

        for _ in 0..2 {
            let mut rs = tx
                .execute_query(Statement::builder("SELECT 1").build())
                .await
                .expect("Failed to execute query");

            let row = rs.next().await.expect("has row").expect("has valid row");
            assert_eq!(row.raw_values(), [Value(string_val("1"))]);

            let result = rs.next().await;
            assert!(result.is_none(), "expected None, got {result:?}");
        }
    }

    #[tokio_test_no_panics]
    async fn execute_multi_query_inline_begin() -> anyhow::Result<()> {
        use super::super::result_set::tests::string_val;
        use crate::statement::Statement;
        use crate::value::Value;
        use spanner_grpc_mock::google::spanner::v1 as mock_v1;

        let mut mock = create_session_mock();

        // No explicit begin_transaction should be called.
        mock.expect_begin_transaction().never();

        let mut seq = mockall::Sequence::new();

        mock.expect_execute_streaming_sql()
            .times(1)
            .in_sequence(&mut seq)
            .returning(move |req| {
                let req = req.into_inner();
                assert_eq!(
                    req.session,
                    "projects/p/instances/i/databases/d/sessions/123"
                );

                // First call: Should have Selector::Begin
                match req.transaction.unwrap().selector.unwrap() {
                    mock_v1::transaction_selector::Selector::Begin(_) => {}
                    _ => panic!("Expected Selector::Begin"),
                }
                let mut rs = setup_select1();
                rs.metadata.as_mut().unwrap().transaction = Some(mock_v1::Transaction {
                    id: vec![4, 5, 6],
                    read_timestamp: Some(prost_types::Timestamp {
                        seconds: 987654321,
                        nanos: 0,
                    }),
                    ..Default::default()
                });
                Ok(gaxi::grpc::tonic::Response::from(adapt([Ok(rs)])))
            });

        mock.expect_execute_streaming_sql()
            .times(1)
            .in_sequence(&mut seq)
            .returning(move |req| {
                let req = req.into_inner();
                // Second call: Should have Selector::Id using the ID returned in the first call
                match req.transaction.unwrap().selector.unwrap() {
                    mock_v1::transaction_selector::Selector::Id(id) => {
                        assert_eq!(id, vec![4, 5, 6]);
                    }
                    _ => panic!("Expected Selector::Id"),
                }
                Ok(gaxi::grpc::tonic::Response::from(adapt([Ok(
                    setup_select1(),
                )])))
            });

        let (db_client, _server) = setup_db_client(mock).await;

        let tx = db_client
            .read_only_transaction()
            .set_begin_transaction_option(BeginTransactionOption::InlineBegin)
            .build()
            .await?;

        // The read timestamp is not available until the first query is executed.
        assert!(tx.read_timestamp().is_none());

        for i in 0..2 {
            let mut rs = tx
                .execute_query(Statement::builder("SELECT 1").build())
                .await?;

            let row = rs.next().await.expect("Expected a row")?;
            assert_eq!(row.raw_values(), [Value(string_val("1"))]);

            let result = rs.next().await;
            assert!(result.is_none(), "Expected None, got {result:?}");

            if i == 0 {
                // Read timestamp becomes available.
                assert_eq!(
                    tx.read_timestamp()
                        .expect("Expected read timestamp")
                        .seconds(),
                    987654321
                );
            }
        }

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn execute_single_read() {
        use super::super::result_set::tests::string_val;
        use crate::key::KeySet;
        use crate::read::ReadRequest;
        use crate::value::Value;

        let mut mock = create_session_mock();

        mock.expect_streaming_read().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(
                req.session,
                "projects/p/instances/i/databases/d/sessions/123"
            );
            assert_eq!(req.table, "Users");
            assert_eq!(req.columns, vec!["Id".to_string(), "Name".to_string()]);

            Ok(gaxi::grpc::tonic::Response::from(adapt([Ok(
                setup_select1(),
            )])))
        });

        let (db_client, _server) = setup_db_client(mock).await;

        let tx = db_client.single_use().build();
        let read = ReadRequest::builder("Users", vec!["Id", "Name"])
            .with_keys(KeySet::all())
            .build();
        let mut rs = tx.execute_read(read).await.expect("Failed to execute read");

        let row = rs.next().await.expect("has row").expect("has valid row");
        assert_eq!(row.raw_values(), [Value(string_val("1"))]);
        let result = rs.next().await;
        assert!(result.is_none(), "expected None, got {result:?}");
    }

    #[tokio_test_no_panics]
    async fn execute_multi_read() -> anyhow::Result<()> {
        use super::super::result_set::tests::string_val;
        use crate::key::KeySet;
        use crate::read::ReadRequest;
        use crate::value::Value;
        use spanner_grpc_mock::google::spanner::v1 as mock_v1;

        let mut mock = create_session_mock();

        // No explicit begin_transaction should be called.
        mock.expect_begin_transaction().never();

        let mut seq = mockall::Sequence::new();

        mock.expect_streaming_read()
            .times(1)
            .in_sequence(&mut seq)
            .returning(move |req| {
                let req = req.into_inner();
                assert_eq!(
                    req.session,
                    "projects/p/instances/i/databases/d/sessions/123"
                );

                // First call: Should have Selector::Begin
                match req.transaction.unwrap().selector.unwrap() {
                    mock_v1::transaction_selector::Selector::Begin(_) => {}
                    _ => panic!("Expected Selector::Begin"),
                }
                let mut rs = setup_select1();
                rs.metadata.as_mut().unwrap().transaction = Some(mock_v1::Transaction {
                    id: vec![4, 5, 6],
                    read_timestamp: Some(prost_types::Timestamp {
                        seconds: 987654321,
                        nanos: 0,
                    }),
                    ..Default::default()
                });
                Ok(gaxi::grpc::tonic::Response::from(adapt([Ok(rs)])))
            });

        mock.expect_streaming_read()
            .times(1)
            .in_sequence(&mut seq)
            .returning(move |req| {
                let req = req.into_inner();
                // Second call: Should have Selector::Id using the ID returned in the first call
                match req.transaction.unwrap().selector.unwrap() {
                    mock_v1::transaction_selector::Selector::Id(id) => {
                        assert_eq!(id, vec![4, 5, 6]);
                    }
                    _ => panic!("Expected Selector::Id"),
                }
                Ok(gaxi::grpc::tonic::Response::from(adapt([Ok(
                    setup_select1(),
                )])))
            });

        let (db_client, _server) = setup_db_client(mock).await;

        let tx = db_client
            .read_only_transaction()
            .set_begin_transaction_option(BeginTransactionOption::InlineBegin)
            .build()
            .await?;

        // The read timestamp is not available until the first query is executed.
        assert!(tx.read_timestamp().is_none());

        for i in 0..2 {
            let read = ReadRequest::builder("Users", vec!["Id", "Name"])
                .with_keys(KeySet::all())
                .build();
            let mut rs = tx.execute_read(read).await?;

            let row = rs.next().await.expect("Expected a row")?;
            assert_eq!(row.raw_values(), [Value(string_val("1"))]);

            let result = rs.next().await;
            assert!(result.is_none(), "Expected None, got {result:?}");

            if i == 0 {
                // Read timestamp becomes available.
                assert_eq!(
                    tx.read_timestamp()
                        .expect("Expected read timestamp")
                        .seconds(),
                    987654321
                );
            }
        }

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn inline_begin_query_failure_fails_fast() -> anyhow::Result<()> {
        use gaxi::grpc::tonic::Status;

        let mut mock = create_session_mock();

        // 1. Initial query fails
        mock.expect_execute_streaming_sql()
            .times(1)
            .returning(|_| Err(Status::internal("Internal error on first statement")));

        // 2. BeginTransaction must NEVER be called as a fallback
        mock.expect_begin_transaction().never();

        let (db_client, _server) = setup_db_client(mock).await;
        let tx = db_client
            .read_only_transaction()
            .set_begin_transaction_option(BeginTransactionOption::InlineBegin)
            .build()
            .await?;

        // First query fails fast with the initial error.
        let first_result = tx
            .execute_query(Statement::builder("SELECT 1").build())
            .await;
        assert!(
            first_result.is_err(),
            "Initial query must fail when execute_streaming_sql returns an error"
        );
        let error_message = first_result.expect_err("initial query failed").to_string();
        assert!(
            error_message.contains("Internal error on first statement"),
            "Error message should propagate: {error_message}"
        );

        // Subsequent query on the same transaction must also fail immediately without any RPCs.
        let second_result = tx
            .execute_query(Statement::builder("SELECT 2").build())
            .await;
        assert!(
            second_result.is_err(),
            "Subsequent query must fail fast on a failed read-only transaction"
        );
        let second_error = second_result.expect_err("subsequent query failed");
        let status = second_error
            .status()
            .expect("Service status error must be preserved on subsequent query failure");
        assert_eq!(
            status.code,
            GaxCode::Internal,
            "Status code must match original failure"
        );
        assert_eq!(
            status.message,
            "Transaction cannot be used: initial statement failed with: Internal error on first statement",
            "Subsequent query error message must explicitly explain transaction cannot be used"
        );

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn inline_begin_read_failure_fails_fast() -> anyhow::Result<()> {
        use crate::key::KeySet;
        use crate::read::ReadRequest;
        use gaxi::grpc::tonic::Status;

        let mut mock = create_session_mock();

        // 1. Initial read fails
        mock.expect_streaming_read()
            .times(1)
            .returning(|_| Err(Status::internal("Internal error on first read")));

        // 2. BeginTransaction must NEVER be called as a fallback
        mock.expect_begin_transaction().never();

        let (db_client, _server) = setup_db_client(mock).await;
        let tx = db_client
            .read_only_transaction()
            .set_begin_transaction_option(BeginTransactionOption::InlineBegin)
            .build()
            .await?;

        let read = ReadRequest::builder("Users", vec!["Id", "Name"])
            .with_keys(KeySet::all())
            .build();
        let first_result = tx.execute_read(read).await;
        assert!(
            first_result.is_err(),
            "Initial read must fail when streaming_read returns an error"
        );
        let error_message = first_result.expect_err("initial read failed").to_string();
        assert!(
            error_message.contains("Internal error on first read"),
            "Error message should propagate: {error_message}"
        );

        // Subsequent query on the same transaction must fail fast without any RPCs.
        let second_result = tx
            .execute_query(Statement::builder("SELECT 1").build())
            .await;
        assert!(
            second_result.is_err(),
            "Subsequent query must fail fast on a failed read-only transaction"
        );
        let second_error = second_result.expect_err("subsequent query failed");
        let status = second_error
            .status()
            .expect("Service status error must be preserved on subsequent query failure");
        assert_eq!(
            status.code,
            GaxCode::Internal,
            "Status code must match original failure"
        );
        assert_eq!(
            status.message,
            "Transaction cannot be used: initial statement failed with: Internal error on first read",
            "Subsequent query error message must explicitly explain transaction cannot be used"
        );

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn inlined_begin_query_stream_fails_with_invalid_argument() -> anyhow::Result<()> {
        let mut mock = create_session_mock();

        // 1. Initial query stream with inline begin fails with InvalidArgument.
        mock.expect_execute_streaming_sql()
            .times(1)
            .returning(|request| {
                let request = request.into_inner();
                match request
                    .transaction
                    .expect("transaction selector must be present")
                    .selector
                    .expect("selector kind must be present")
                {
                    Selector::Begin(_) => {}
                    _ => panic!("Expected Selector::Begin for first query"),
                }
                Ok(Response::from(adapt([Err(Status::invalid_argument(
                    "Table not found: invalid_table",
                ))])))
            });

        // 2. BeginTransaction must NEVER be called as a fallback.
        mock.expect_begin_transaction().never();

        let (database_client, _server) = setup_db_client(mock).await;
        let transaction = database_client
            .read_only_transaction()
            .set_begin_transaction_option(BeginTransactionOption::InlineBegin)
            .build()
            .await?;

        let first_query_result = transaction
            .execute_query(Statement::builder("SELECT * FROM invalid_table").build())
            .await;
        assert!(
            first_query_result.is_err(),
            "Expected first query to fail with InvalidArgument"
        );
        let error_message = first_query_result
            .expect_err("first query failed")
            .to_string();
        assert!(
            error_message.contains("Table not found: invalid_table"),
            "Expected error message to propagate: {error_message}"
        );

        // Transaction is permanently marked as Failed; subsequent query fails fast without RPC.
        let second_query_result = transaction
            .execute_query(Statement::builder("SELECT 1").build())
            .await;
        assert!(
            second_query_result.is_err(),
            "Expected second query to fail fast on a failed read-only transaction"
        );
        let second_error = second_query_result.expect_err("second query failed");
        let status = second_error
            .status()
            .expect("Service status error must be preserved on subsequent query failure");
        assert_eq!(
            status.code,
            GaxCode::InvalidArgument,
            "Status code must match original failure"
        );
        assert_eq!(
            status.message,
            "Transaction cannot be used: initial statement failed with: Table not found: invalid_table",
            "Subsequent query error message must explicitly explain transaction cannot be used"
        );

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn inlined_begin_query_stream_chunk_failure_fails_fast() -> anyhow::Result<()> {
        let mut mock = create_session_mock();

        // 1. Initial streaming query with inline begin fails on initial chunk with Unavailable.
        mock.expect_execute_streaming_sql()
            .times(1)
            .returning(|request| {
                let request = request.into_inner();
                match request
                    .transaction
                    .expect("transaction selector must be present")
                    .selector
                    .expect("selector kind must be present")
                {
                    Selector::Begin(_) => {}
                    _ => panic!("Expected Selector::Begin for first query"),
                }
                Ok(Response::from(adapt([Err(Status::unavailable(
                    "Transient initial stream failure",
                ))])))
            });

        // 2. BeginTransaction must NEVER be called as a fallback.
        mock.expect_begin_transaction().never();

        let (database_client, _server) = setup_db_client(mock).await;
        let transaction = database_client
            .read_only_transaction()
            .set_begin_transaction_option(BeginTransactionOption::InlineBegin)
            .build()
            .await?;

        let statement = Statement::builder("SELECT 1")
            .with_retry_policy(NeverRetry)
            .build();
        let first_query_result = transaction.execute_query(statement).await;
        assert!(
            first_query_result.is_err(),
            "Expected initial query to fail fast on stream initialization error"
        );
        let error_message = first_query_result
            .expect_err("first query failed")
            .to_string();
        assert!(
            error_message.contains("Transient initial stream failure"),
            "Expected transient error message to propagate: {error_message}"
        );

        // Subsequent query on the same transaction fails fast without any RPCs.
        let second_query_result = transaction
            .execute_query(Statement::builder("SELECT 1").build())
            .await;
        assert!(
            second_query_result.is_err(),
            "Subsequent query must fail fast on a failed read-only transaction"
        );
        let second_error = second_query_result.expect_err("second query failed");
        let status = second_error
            .status()
            .expect("Service status error must be preserved on subsequent query failure");
        assert_eq!(
            status.code,
            GaxCode::Unavailable,
            "Status code must match original failure"
        );
        assert_eq!(
            status.message,
            "Transaction cannot be used: initial statement failed with: Transient initial stream failure",
            "Subsequent query error message must explicitly explain transaction cannot be used"
        );

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn inlined_begin_streaming_read_fails_with_invalid_argument() -> anyhow::Result<()> {
        let mut mock = create_session_mock();

        // 1. Initial streaming read with inline begin fails with InvalidArgument.
        mock.expect_streaming_read().times(1).returning(|request| {
            let request = request.into_inner();
            match request
                .transaction
                .expect("transaction selector must be present")
                .selector
                .expect("selector kind must be present")
            {
                Selector::Begin(_) => {}
                _ => panic!("Expected Selector::Begin for first read"),
            }
            Ok(Response::from(adapt([Err(Status::invalid_argument(
                "Table not found: invalid_table",
            ))])))
        });

        // 2. BeginTransaction must NEVER be called as a fallback.
        mock.expect_begin_transaction().never();

        let (database_client, _server) = setup_db_client(mock).await;
        let transaction = database_client
            .read_only_transaction()
            .set_begin_transaction_option(BeginTransactionOption::InlineBegin)
            .build()
            .await?;

        let read_request = ReadRequest::builder("invalid_table", vec!["Id"])
            .with_keys(KeySet::all())
            .build();
        let first_read_result = transaction.execute_read(read_request).await;
        assert!(
            first_read_result.is_err(),
            "Expected first read to fail with InvalidArgument"
        );
        let error_message = first_read_result
            .expect_err("first read failed")
            .to_string();
        assert!(
            error_message.contains("Table not found: invalid_table"),
            "Expected error message to propagate: {error_message}"
        );

        // Subsequent query on the same transaction fails fast without any RPCs.
        let second_query_result = transaction
            .execute_query(Statement::builder("SELECT 1").build())
            .await;
        assert!(
            second_query_result.is_err(),
            "Subsequent query must fail fast on a failed read-only transaction"
        );
        let second_error = second_query_result.expect_err("second query failed");
        let status = second_error
            .status()
            .expect("Service status error must be preserved on subsequent query failure");
        assert_eq!(
            status.code,
            GaxCode::InvalidArgument,
            "Status code must match original failure"
        );
        assert_eq!(
            status.message,
            "Transaction cannot be used: initial statement failed with: Table not found: invalid_table",
            "Subsequent query error message must explicitly explain transaction cannot be used"
        );

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn explicit_begin_allows_subsequent_statements_after_first_fails() -> anyhow::Result<()> {
        let mut mock = create_session_mock();
        let mut sequence = Sequence::new();

        // 1. Explicit begin transaction succeeds at build() time.
        mock.expect_begin_transaction()
            .times(1)
            .in_sequence(&mut sequence)
            .returning(|request| {
                let request = request.into_inner();
                assert_eq!(
                    request.session, "projects/p/instances/i/databases/d/sessions/123",
                    "Session name matches expected session"
                );
                Ok(Response::new(mock_v1::Transaction {
                    id: vec![7, 8, 9],
                    read_timestamp: Some(Timestamp {
                        seconds: 123456789,
                        nanos: 0,
                    }),
                    ..Default::default()
                }))
            });

        // 2. First query with established transaction ID fails with InvalidArgument (e.g. invalid table).
        mock.expect_execute_streaming_sql()
            .times(1)
            .in_sequence(&mut sequence)
            .returning(|request| {
                let request = request.into_inner();
                match request
                    .transaction
                    .expect("transaction selector must be present")
                    .selector
                    .expect("selector kind must be present")
                {
                    Selector::Id(transaction_id) => {
                        assert_eq!(
                            transaction_id,
                            vec![7, 8, 9],
                            "Expected first query to use established transaction ID"
                        );
                    }
                    _ => panic!("Expected Selector::Id on query with explicit begin"),
                }
                Ok(Response::from(adapt([Err(Status::invalid_argument(
                    "Table not found: invalid_table",
                ))])))
            });

        // 3. Second query with same established transaction ID succeeds.
        mock.expect_execute_streaming_sql()
            .times(1)
            .in_sequence(&mut sequence)
            .returning(|request| {
                let request = request.into_inner();
                match request
                    .transaction
                    .expect("transaction selector must be present")
                    .selector
                    .expect("selector kind must be present")
                {
                    Selector::Id(transaction_id) => {
                        assert_eq!(
                            transaction_id,
                            vec![7, 8, 9],
                            "Expected second query to use established transaction ID"
                        );
                    }
                    _ => panic!("Expected Selector::Id on second query"),
                }
                Ok(Response::from(adapt([Ok(setup_select1())])))
            });

        let (database_client, _server) = setup_db_client(mock).await;
        let transaction = database_client
            .read_only_transaction()
            .set_begin_transaction_option(BeginTransactionOption::ExplicitBegin)
            .build()
            .await?;

        // First query fails with InvalidArgument.
        let first_query_result = transaction
            .execute_query(Statement::builder("SELECT * FROM invalid_table").build())
            .await;
        assert!(
            first_query_result.is_err(),
            "Expected first query to fail with InvalidArgument"
        );

        // Because ExplicitBegin was used, the transaction remains valid with its established snapshot.
        let mut second_result_set = transaction
            .execute_query(Statement::builder("SELECT 1").build())
            .await?;
        let row = second_result_set
            .next()
            .await
            .expect("Expected row result from second query")?;
        assert_eq!(
            row.raw_values(),
            [Value(string_val("1"))],
            "Second query successfully executed within established transaction"
        );
        assert_eq!(
            transaction
                .read_timestamp()
                .expect("Expected read timestamp to be set")
                .seconds(),
            123456789,
            "Read timestamp should match explicit begin timestamp"
        );

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn inlined_begin_subsequent_statement_succeeds_after_second_statement_fails()
    -> anyhow::Result<()> {
        let mut mock = create_session_mock();
        let mut sequence = Sequence::new();

        // 1. Initial query with inline begin succeeds, returning metadata with transaction ID.
        mock.expect_execute_streaming_sql()
            .times(1)
            .in_sequence(&mut sequence)
            .returning(|request| {
                let request = request.into_inner();
                match request
                    .transaction
                    .expect("transaction selector must be present")
                    .selector
                    .expect("selector kind must be present")
                {
                    Selector::Begin(_) => {}
                    _ => panic!("Expected Selector::Begin for first query"),
                }
                let mut result_set_partial = setup_select1();
                result_set_partial
                    .metadata
                    .as_mut()
                    .expect("metadata should be present")
                    .transaction = Some(mock_v1::Transaction {
                    id: vec![7, 8, 9],
                    read_timestamp: Some(Timestamp {
                        seconds: 123456789,
                        nanos: 0,
                    }),
                    ..Default::default()
                });
                Ok(Response::from(adapt([Ok(result_set_partial)])))
            });

        // 2. Second query using established transaction ID fails with InvalidArgument (e.g. invalid table).
        mock.expect_execute_streaming_sql()
            .times(1)
            .in_sequence(&mut sequence)
            .returning(|request| {
                let request = request.into_inner();
                match request
                    .transaction
                    .expect("transaction selector must be present")
                    .selector
                    .expect("selector kind must be present")
                {
                    Selector::Id(transaction_id) => {
                        assert_eq!(
                            transaction_id,
                            vec![7, 8, 9],
                            "Expected second query to use established transaction ID"
                        );
                    }
                    _ => panic!("Expected Selector::Id on second query"),
                }
                Ok(Response::from(adapt([Err(Status::invalid_argument(
                    "Table not found: invalid_table",
                ))])))
            });

        // 3. Third query using established transaction ID succeeds.
        mock.expect_execute_streaming_sql()
            .times(1)
            .in_sequence(&mut sequence)
            .returning(|request| {
                let request = request.into_inner();
                match request
                    .transaction
                    .expect("transaction selector must be present")
                    .selector
                    .expect("selector kind must be present")
                {
                    Selector::Id(transaction_id) => {
                        assert_eq!(
                            transaction_id,
                            vec![7, 8, 9],
                            "Expected third query to use established transaction ID"
                        );
                    }
                    _ => panic!("Expected Selector::Id on third query"),
                }
                Ok(Response::from(adapt([Ok(setup_select1())])))
            });

        let (database_client, _server) = setup_db_client(mock).await;
        let transaction = database_client
            .read_only_transaction()
            .set_begin_transaction_option(BeginTransactionOption::InlineBegin)
            .build()
            .await?;

        // 1. First query succeeds and establishes the transaction.
        let mut first_result_set = transaction
            .execute_query(Statement::builder("SELECT 1").build())
            .await?;
        let first_row = first_result_set.next().await.expect("row expected")?;
        assert_eq!(
            first_row.raw_values(),
            [Value(string_val("1"))],
            "First query result matches expected value"
        );
        assert_eq!(
            transaction
                .read_timestamp()
                .expect("Expected read timestamp to be set")
                .seconds(),
            123456789,
            "Read timestamp matches inlined begin timestamp"
        );

        // 2. Second query fails with InvalidArgument.
        let second_result = transaction
            .execute_query(Statement::builder("SELECT * FROM invalid_table").build())
            .await;
        assert!(
            second_result.is_err(),
            "Expected second query to fail with InvalidArgument"
        );

        // 3. Third query continues to execute successfully on the existing transaction snapshot.
        let mut third_result_set = transaction
            .execute_query(Statement::builder("SELECT 1").build())
            .await?;
        let third_row = third_result_set
            .next()
            .await
            .expect("row expected from third query")?;
        assert_eq!(
            third_row.raw_values(),
            [Value(string_val("1"))],
            "Third query result matches expected value"
        );

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn inlined_begin_streaming_read_chunk_failure_fails_fast() -> anyhow::Result<()> {
        let mut mock = create_session_mock();

        // 1. Initial streaming read with inline begin fails on chunk with Unavailable.
        mock.expect_streaming_read().times(1).returning(|request| {
            let request = request.into_inner();
            match request
                .transaction
                .expect("transaction selector must be present")
                .selector
                .expect("selector kind must be present")
            {
                Selector::Begin(_) => {}
                _ => panic!("Expected Selector::Begin for first read"),
            }
            Ok(Response::from(adapt([Err(Status::unavailable(
                "Transient initial read chunk failure",
            ))])))
        });

        // 2. BeginTransaction must NEVER be called as a fallback.
        mock.expect_begin_transaction().never();

        let (database_client, _server) = setup_db_client(mock).await;
        let transaction = database_client
            .read_only_transaction()
            .set_begin_transaction_option(BeginTransactionOption::InlineBegin)
            .build()
            .await?;

        let read_request = ReadRequest::builder("Users", vec!["Id", "Name"])
            .with_keys(KeySet::all())
            .with_retry_policy(NeverRetry)
            .build();
        let first_read_result = transaction.execute_read(read_request).await;
        assert!(
            first_read_result.is_err(),
            "Expected initial read to fail fast on stream initialization chunk error"
        );
        let error_message = first_read_result
            .expect_err("first read failed")
            .to_string();
        assert!(
            error_message.contains("Transient initial read chunk failure"),
            "Expected transient read error message to propagate: {error_message}"
        );

        // Subsequent query on the same transaction fails fast without any RPCs.
        let second_query_result = transaction
            .execute_query(Statement::builder("SELECT 1").build())
            .await;
        assert!(
            second_query_result.is_err(),
            "Subsequent query must fail fast on a failed read-only transaction"
        );
        let second_error = second_query_result.expect_err("second query failed");
        let status = second_error
            .status()
            .expect("Service status error must be preserved on subsequent query failure");
        assert_eq!(
            status.code,
            GaxCode::Unavailable,
            "Status code must match original failure"
        );
        assert_eq!(
            status.message,
            "Transaction cannot be used: initial statement failed with: Transient initial read chunk failure",
            "Subsequent query error message must explicitly explain transaction cannot be used"
        );

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn single_use_query_send_error_returns_immediately() -> anyhow::Result<()> {
        use crate::statement::Statement;
        use gaxi::grpc::tonic::Status;

        let mut mock = create_session_mock();

        mock.expect_execute_streaming_sql()
            .times(1)
            .returning(|_| Err(Status::internal("Internal error single use query")));

        mock.expect_begin_transaction().never();

        let (db_client, _server) = setup_db_client(mock).await;
        // single_use creates a Fixed selector
        let tx = db_client.single_use().build();

        let rs_result = tx
            .execute_query(Statement::builder("SELECT 1").build())
            .await;

        assert!(rs_result.is_err());
        let err_str = rs_result.unwrap_err().to_string();
        assert!(err_str.contains("Internal error single use query"));

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn inline_begin_already_started_query_send_error_returns_immediately()
    -> anyhow::Result<()> {
        use crate::statement::Statement;
        use gaxi::grpc::tonic::Status;
        use spanner_grpc_mock::google::spanner::v1 as mock_v1;

        let mut mock = create_session_mock();
        let mut seq = mockall::Sequence::new();

        mock.expect_begin_transaction().never();

        // 1. First query executes successfully and implicitly starts the transaction.
        mock.expect_execute_streaming_sql()
            .times(1)
            .in_sequence(&mut seq)
            .returning(move |_req| {
                let mut rs = setup_select1();
                rs.metadata.as_mut().unwrap().transaction = Some(mock_v1::Transaction {
                    id: vec![4, 5, 6],
                    read_timestamp: None,
                    ..Default::default()
                });
                Ok(gaxi::grpc::tonic::Response::from(adapt([Ok(rs)])))
            });

        // 2. Second query fails immediately upon send()
        mock.expect_execute_streaming_sql()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|_| Err(Status::internal("Internal error second query")));

        let (db_client, _server) = setup_db_client(mock).await;

        let tx = db_client
            .read_only_transaction()
            .set_begin_transaction_option(BeginTransactionOption::InlineBegin)
            .build()
            .await?;

        // Run first query (starts tx)
        let mut rs = tx
            .execute_query(Statement::builder("SELECT 1").build())
            .await?;
        let _ = rs.next().await.expect("has row")?;

        // Run second query (fails)
        let rs_result = tx
            .execute_query(Statement::builder("SELECT 2").build())
            .await;

        assert!(rs_result.is_err());
        let err_str = rs_result.unwrap_err().to_string();
        assert!(err_str.contains("Internal error second query"));

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn execute_concurrent_queries_inline_begin() -> anyhow::Result<()> {
        let mut mock = create_session_mock();
        mock.expect_begin_transaction().never();

        let mut seq = mockall::Sequence::new();
        let (tx_sender, rx_receiver) = mpsc::channel(1);
        let rx_receiver = Arc::new(Mutex::new(Some(rx_receiver)));

        let task1_ready = Arc::new(Notify::new());
        let task1_ready_clone = Arc::clone(&task1_ready);
        let tasks_started = Arc::new(Barrier::new(3));

        // 1. First query: should include Selector::Begin
        mock.expect_execute_streaming_sql()
            .times(1)
            .in_sequence(&mut seq)
            .returning(move |req| {
                task1_ready_clone.notify_one();
                let req = req.into_inner();
                match req
                    .transaction
                    .expect("transaction selector must be present")
                    .selector
                    .expect("selector kind must be present")
                {
                    Selector::Begin(_) => {}
                    _ => panic!("Expected Selector::Begin for first query"),
                }
                let rx = rx_receiver
                    .try_lock()
                    .expect("mutex poisoned")
                    .take()
                    .expect("rx receiver must be present");
                Ok(Response::from(rx))
            });

        // 2. The other queries: should include populated Selector::Id
        mock.expect_execute_streaming_sql()
            .times(2)
            .in_sequence(&mut seq)
            .returning(move |req| {
                let req = req.into_inner();
                match req
                    .transaction
                    .expect("transaction selector must be present")
                    .selector
                    .expect("selector kind must be present")
                {
                    Selector::Id(id) => {
                        assert_eq!(id, vec![4, 5, 6]);
                    }
                    _ => panic!("Expected Selector::Id for other queries"),
                }

                let (tx, rx) = mpsc::channel(1);
                tx.try_send(Ok(setup_select1()))
                    .expect("send should succeed");
                Ok(Response::from(rx))
            });

        let (db_client, _server) = setup_db_client(mock).await;
        let tx = db_client
            .read_only_transaction()
            .set_begin_transaction_option(BeginTransactionOption::InlineBegin)
            .build()
            .await?;
        let tx = Arc::new(tx);

        // Spawn 3 concurrent queries.
        // Task 1 launches first and executes the first query.
        let tx1 = Arc::clone(&tx);
        let handle1 = tokio::spawn(async move {
            let mut rs = tx1
                .execute_query(Statement::builder("SELECT 1").build())
                .await?;
            // Read the first result to get the transaction ID.
            let _ = rs.next().await;
            Ok::<_, crate::Error>(rs)
        });

        // Wait for Task 1 to reach the mock server.
        task1_ready.notified().await;

        let tx2 = Arc::clone(&tx);
        let tasks_started2 = Arc::clone(&tasks_started);
        let handle2 = tokio::spawn(async move {
            tasks_started2.wait().await;
            tx2.execute_query(Statement::builder("SELECT 1").build())
                .await
        });

        let tx3 = Arc::clone(&tx);
        let tasks_started3 = Arc::clone(&tasks_started);
        let handle3 = tokio::spawn(async move {
            tasks_started3.wait().await;
            tx3.execute_query(Statement::builder("SELECT 1").build())
                .await
        });

        // Ensure both Tasks 2 and 3 have reached the barrier before proceeding.
        tasks_started.wait().await;

        // Flush the scheduler on this single-threaded executor.
        // This guarantees that Tasks 2 & 3 run until they both hit the internal
        // selector Notify latch and become suspended.
        tokio::task::yield_now().await;

        // Provide the first result (including the transaction ID) to Task 1.
        // This transitions the selector to 'Started' and unblocks Tasks 2 and 3.
        let mut rs = setup_select1();
        rs.metadata
            .as_mut()
            .expect("metadata should be present")
            .transaction = Some(mock_v1::Transaction {
            id: vec![4, 5, 6],
            read_timestamp: Some(prost_types::Timestamp {
                seconds: 987654321,
                nanos: 0,
            }),
            ..Default::default()
        });
        tx_sender.send(Ok(rs)).await.expect("channel broken");
        drop(tx_sender);

        // Collect all results
        let mut rs1 = handle1.await??;
        let mut rs2 = handle2.await??;
        let mut rs3 = handle3.await??;

        // Verify the query results
        assert!(rs1.next().await.is_none());

        let row2 = rs2.next().await.expect("Expected a row")?;
        assert_eq!(row2.raw_values(), [Value(string_val("1"))]);
        assert!(rs2.next().await.is_none());

        let row3 = rs3.next().await.expect("Expected a row")?;
        assert_eq!(row3.raw_values(), [Value(string_val("1"))]);
        assert!(rs3.next().await.is_none());

        // Verify that the read timestamp was populated
        assert_eq!(
            tx.read_timestamp()
                .expect("read timestamp should be populated")
                .seconds(),
            987654321
        );

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn execute_concurrent_queries_inline_begin_failed_cascade() -> anyhow::Result<()> {
        let mut mock = create_session_mock();
        let mut seq = mockall::Sequence::new();

        let (tx_sender, rx_receiver) = mpsc::channel(1);
        let rx_receiver = Arc::new(Mutex::new(Some(rx_receiver)));

        let task1_ready = Arc::new(Notify::new());
        let task1_ready_clone = Arc::clone(&task1_ready);
        let tasks_started = Arc::new(Barrier::new(3));

        // 1. Return a stream connected to tx_sender.
        // We will use tx_sender later in the test to inject a failed first chunk.
        mock.expect_execute_streaming_sql()
            .times(1)
            .in_sequence(&mut seq)
            .returning(move |_req| {
                task1_ready_clone.notify_one();
                let rx = rx_receiver
                    .try_lock()
                    .expect("mutex poisoned")
                    .take()
                    .expect("receiver should be present");
                Ok(tonic::Response::from(rx))
            });

        // 2. BeginTransaction must NEVER be called as a fallback.
        mock.expect_begin_transaction().never();

        // The other queries will never be executed.
        mock.expect_execute_streaming_sql().times(0).returning(|_| {
            panic!("Other queries should not launch after failure to start the transaction")
        });

        let (db_client, _server) = setup_db_client(mock).await;
        let tx = db_client
            .read_only_transaction()
            .set_begin_transaction_option(BeginTransactionOption::InlineBegin)
            .build()
            .await?;
        let tx = Arc::new(tx);

        // Spawn 3 concurrent queries.
        let tx1 = Arc::clone(&tx);
        let handle1 = tokio::spawn(async move {
            let mut rs = tx1
                .execute_query(Statement::builder("SELECT 1").build())
                .await?;
            rs.next().await.ok_or_else(|| {
                crate::error::internal_error("stream exhausted (this should never happen)")
            })??;
            Ok::<_, crate::Error>(rs)
        });

        // Wait for Task 1 to reach the mock and transition the selector to Starting.
        task1_ready.notified().await;

        let tx2 = Arc::clone(&tx);
        let tasks_started2 = Arc::clone(&tasks_started);
        let handle2 = tokio::spawn(async move {
            tasks_started2.wait().await;
            tx2.execute_query(Statement::builder("SELECT 1").build())
                .await
        });

        let tx3 = Arc::clone(&tx);
        let tasks_started3 = Arc::clone(&tasks_started);
        let handle3 = tokio::spawn(async move {
            tasks_started3.wait().await;
            tx3.execute_query(Statement::builder("SELECT 1").build())
                .await
        });

        // Ensure both Tasks 2 and 3 have reached the barrier before proceeding.
        tasks_started.wait().await;

        // Flush the scheduler on this single-threaded executor.
        // This guarantees that Tasks 2 & 3 run until they both hit the internal
        // selector Notify latch and become suspended.
        tokio::task::yield_now().await;

        // Push error to channel failing first query stream!
        tx_sender
            .send(Err(gaxi::grpc::tonic::Status::internal(
                "Mocked boot failed",
            )))
            .await
            .expect("channel broken");
        drop(tx_sender);

        // Collect all results - all should fail with identical cached error!
        let err1 = handle1
            .await?
            .expect_err("task 1 should have failed")
            .to_string();
        let err2 = handle2
            .await?
            .expect_err("task 2 should have failed")
            .to_string();
        let err3 = handle3
            .await?
            .expect_err("task 3 should have failed")
            .to_string();

        assert!(err1.contains("Mocked boot failed"), "err1: {}", err1);
        assert!(
            err2.contains(
                "Transaction cannot be used: initial statement failed with: Mocked boot failed"
            ),
            "err2: {}",
            err2
        );
        assert!(
            err3.contains(
                "Transaction cannot be used: initial statement failed with: Mocked boot failed"
            ),
            "err3: {}",
            err3
        );

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn execute_concurrent_queries_inline_begin_stream_restart_deadlock_prevention()
    -> crate::Result<()> {
        let mut mock = create_session_mock();
        mock.expect_begin_transaction().never();

        let mut seq = mockall::Sequence::new();

        let (tx_sender, rx_receiver) = mpsc::channel(1);
        let rx_receiver = Arc::new(Mutex::new(Some(rx_receiver)));

        let task1_ready = Arc::new(Notify::new());
        let task1_ready_clone = Arc::clone(&task1_ready);
        let tasks_started = Arc::new(Barrier::new(3));

        // 1. Task 1 initial query: Return a stream connected to tx_sender for error injection.
        mock.expect_execute_streaming_sql()
            .times(1)
            .in_sequence(&mut seq)
            .returning(move |req| {
                let req = req.into_inner();
                // Return a stream connected to tx_sender.
                // We will use tx_sender later in the test to inject a transient error.
                task1_ready_clone.notify_one();
                match req
                    .transaction
                    .expect("transaction should be present")
                    .selector
                    .expect("selector should be present")
                {
                    Selector::Begin(_) => {}
                    _ => panic!("Expected Selector::Begin for first query"),
                }
                let rx = rx_receiver
                    .try_lock()
                    .expect("mutex poisoned")
                    .take()
                    .expect("receiver should be present");
                Ok(Response::from(rx))
            });

        // 2. Task 1 restart query: should include Selector::Begin, since
        // it failed with a transient error.
        mock.expect_execute_streaming_sql()
            .times(1)
            .in_sequence(&mut seq)
            .returning(move |req| {
                let req = req.into_inner();
                match req
                    .transaction
                    .expect("transaction should be present")
                    .selector
                    .expect("selector should be present")
                {
                    Selector::Begin(_) => {
                        let mut rs = setup_select1();
                        rs.metadata
                            .as_mut()
                            .expect("metadata should be present")
                            .transaction = Some(mock_v1::Transaction {
                            id: vec![4, 5, 6],
                            ..Default::default()
                        });
                        let (tx, rx) = mpsc::channel(1);
                        tx.try_send(Ok(rs)).expect("send should succeed");
                        Ok(Response::from(rx))
                    }
                    _ => panic!("Expected Selector::Begin for stream restart query"),
                }
            });

        // 3. Tasks 2 & 3: should include populated Selector::Id
        mock.expect_execute_streaming_sql()
            .times(2)
            .in_sequence(&mut seq)
            .returning(move |req| {
                let req = req.into_inner();
                match req
                    .transaction
                    .expect("transaction should be present")
                    .selector
                    .expect("selector should be present")
                {
                    Selector::Id(id) => {
                        assert_eq!(id, vec![4, 5, 6]);
                        let (tx, rx) = mpsc::channel(1);
                        tx.try_send(Ok(setup_select1()))
                            .expect("send should succeed");
                        Ok(Response::from(rx))
                    }
                    _ => panic!("Expected Selector::Id for concurrent queries"),
                }
            });

        let (db_client, _server) = setup_db_client(mock).await;
        let tx = db_client
            .read_only_transaction()
            .set_begin_transaction_option(BeginTransactionOption::InlineBegin)
            .build()
            .await?;
        let tx = Arc::new(tx);

        let handle1_tx = Arc::clone(&tx);
        let handle1 = tokio::spawn(async move {
            let statement = Statement::builder("SELECT 1")
                .with_backoff_policy(
                    ExponentialBackoffBuilder::new()
                        .with_initial_delay(Duration::from_nanos(1))
                        .clamp(),
                )
                .build();
            let mut rs = handle1_tx.execute_query(statement).await?;
            let _ = rs.next().await.ok_or_else(|| {
                crate::error::internal_error("stream exhausted (this should never happen)")
            })??;
            Ok::<_, crate::Error>(rs)
        });

        // Wait for Task 1 to reach the mock and transition the selector to Starting.
        task1_ready.notified().await;

        let handle2_tx = Arc::clone(&tx);
        let tasks_started2 = Arc::clone(&tasks_started);
        let handle2 = tokio::spawn(async move {
            tasks_started2.wait().await;
            let mut rs = handle2_tx
                .execute_query(Statement::builder("SELECT 1").build())
                .await?;
            let _ = rs.next().await.ok_or_else(|| {
                crate::error::internal_error("stream exhausted (this should never happen)")
            })??;
            Ok::<_, crate::Error>(rs)
        });

        let handle3_tx = Arc::clone(&tx);
        let tasks_started3 = Arc::clone(&tasks_started);
        let handle3 = tokio::spawn(async move {
            tasks_started3.wait().await;
            let mut rs = handle3_tx
                .execute_query(Statement::builder("SELECT 1").build())
                .await?;
            let _ = rs.next().await.ok_or_else(|| {
                crate::error::internal_error("stream exhausted (this should never happen)")
            })??;
            Ok::<_, crate::Error>(rs)
        });

        // Ensure both Tasks 2 and 3 have reached the barrier before proceeding.
        tasks_started.wait().await;

        // Flush the scheduler on this single-threaded executor.
        // This guarantees that Tasks 2 & 3 run until they both hit the internal
        // selector Notify latch and become suspended.
        tokio::task::yield_now().await;

        let grpc_status = Status::new(gaxi::grpc::tonic::Code::Unavailable, "transient error");
        tx_sender.send(Err(grpc_status)).await.expect("send failed");
        drop(tx_sender);

        // Collect and verify all results.
        // handle.await returns Result<Result<ResultSet, Error>, JoinError>.
        // The first ? handles the potential JoinError (panic in the task),
        // and the second ? handles the Spanner error.
        let mut rs1 = handle1.await.expect("Task 1 panicked")?;
        let mut rs2 = handle2.await.expect("Task 2 panicked")?;
        let mut rs3 = handle3.await.expect("Task 3 panicked")?;

        // Verify that all results have been exhausted.
        // (The tasks themselves already successfully read the first row).
        assert!(rs1.next().await.is_none(), "Stream 1 should be exhausted");
        assert!(rs2.next().await.is_none(), "Stream 2 should be exhausted");
        assert!(rs3.next().await.is_none(), "Stream 3 should be exhausted");

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn execute_concurrent_queries_late_arrival_failure() -> anyhow::Result<()> {
        let mut mock = create_session_mock();
        let mut seq = mockall::Sequence::new();

        // 1. Initial query fails.
        mock.expect_execute_streaming_sql()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|req| {
                let req = req.into_inner();
                match req
                    .transaction
                    .expect("transaction should be present")
                    .selector
                    .expect("selector should be present")
                {
                    Selector::Begin(_) => {}
                    _ => panic!("Expected Selector::Begin for first query"),
                }
                Err(Status::internal("Initial inline-begin failed"))
            });

        // 2. BeginTransaction must NEVER be called as a fallback.
        mock.expect_begin_transaction().never();

        // Any further attempts would panic because we haven't mocked them.
        mock.expect_execute_streaming_sql().never();

        let (db_client, _server) = setup_db_client(mock).await;
        let tx = db_client
            .read_only_transaction()
            .set_begin_transaction_option(BeginTransactionOption::InlineBegin)
            .build()
            .await?;

        // First query: triggers the failure and transitions the state to Failed.
        let err1 = tx
            .execute_query(Statement::builder("SELECT 1").build())
            .await
            .expect_err("First query should fail");
        assert!(err1.to_string().contains("Initial inline-begin failed"));

        // Second query: starts AFTER the failure is already cached.
        // It should immediately return the same error without invoking the mock server.
        let err2 = tx
            .execute_query(Statement::builder("SELECT 1").build())
            .await
            .expect_err("Late query should fail immediately");
        assert!(
            err2.to_string().contains(
                "Transaction cannot be used: initial statement failed with: Initial inline-begin failed"
            ),
            "err2: {err2}"
        );

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn execute_concurrent_reads_inline_begin() -> anyhow::Result<()> {
        use crate::key::KeySet;
        use crate::read::ReadRequest;
        let mut mock = create_session_mock();
        mock.expect_begin_transaction().never();

        let mut seq = mockall::Sequence::new();
        let (tx_sender, rx_receiver) = mpsc::channel(1);
        let rx_receiver = Arc::new(Mutex::new(Some(rx_receiver)));

        let task1_ready = Arc::new(Notify::new());
        let task1_ready_clone = Arc::clone(&task1_ready);
        let tasks_started = Arc::new(Barrier::new(3));

        // 1. First read: should include Selector::Begin
        mock.expect_streaming_read()
            .times(1)
            .in_sequence(&mut seq)
            .returning(move |req| {
                task1_ready_clone.notify_one();
                let req = req.into_inner();
                match req
                    .transaction
                    .expect("transaction should be present")
                    .selector
                    .expect("selector should be present")
                {
                    mock_v1::transaction_selector::Selector::Begin(_) => {}
                    _ => panic!("Expected Selector::Begin for first read"),
                }

                let rx = rx_receiver
                    .try_lock()
                    .expect("mutex poisoned")
                    .take()
                    .expect("receiver should be present");
                Ok(Response::from(rx))
            });

        // 2. The other reads: should include populated Selector::Id
        mock.expect_streaming_read()
            .times(2)
            .in_sequence(&mut seq)
            .returning(move |req| {
                let req = req.into_inner();
                match req
                    .transaction
                    .expect("transaction should be present")
                    .selector
                    .expect("selector should be present")
                {
                    mock_v1::transaction_selector::Selector::Id(id) => {
                        assert_eq!(id, vec![4, 5, 6]);
                    }
                    _ => panic!("Expected Selector::Id for other reads"),
                }

                let (tx, rx) = mpsc::channel(1);
                tx.try_send(Ok(setup_select1()))
                    .expect("send should succeed");
                Ok(Response::from(rx))
            });

        let (db_client, _server) = setup_db_client(mock).await;
        let tx = db_client
            .read_only_transaction()
            .set_begin_transaction_option(BeginTransactionOption::InlineBegin)
            .build()
            .await?;
        let tx = Arc::new(tx);

        let read_req = ReadRequest::builder("Table", vec!["Col"])
            .with_keys(KeySet::all())
            .build();

        // Spawn 3 concurrent reads.
        let tx1 = Arc::clone(&tx);
        let read1 = read_req.clone();
        let handle1 = tokio::spawn(async move {
            let mut rs = tx1.execute_read(read1).await?;
            let _ = rs.next().await;
            Ok::<_, crate::Error>(rs)
        });

        task1_ready.notified().await;

        let tx2 = Arc::clone(&tx);
        let read2 = read_req.clone();
        let tasks_started2 = Arc::clone(&tasks_started);
        let handle2 = tokio::spawn(async move {
            tasks_started2.wait().await;
            let mut rs = tx2.execute_read(read2).await?;
            let _ = rs.next().await;
            Ok::<_, crate::Error>(rs)
        });

        let tx3 = Arc::clone(&tx);
        let read3 = read_req.clone();
        let tasks_started3 = Arc::clone(&tasks_started);
        let handle3 = tokio::spawn(async move {
            tasks_started3.wait().await;
            let mut rs = tx3.execute_read(read3).await?;
            let _ = rs.next().await;
            Ok::<_, crate::Error>(rs)
        });

        tasks_started.wait().await;
        tokio::task::yield_now().await;

        // Provide the transaction ID.
        let mut rs = setup_select1();
        rs.metadata
            .as_mut()
            .expect("metadata should be present")
            .transaction = Some(mock_v1::Transaction {
            id: vec![4, 5, 6],
            ..Default::default()
        });
        tx_sender.send(Ok(rs)).await.expect("send failed");
        drop(tx_sender);

        let mut rs1 = handle1.await.expect("Task 1 panicked")?;
        let mut rs2 = handle2.await.expect("Task 2 panicked")?;
        let mut rs3 = handle3.await.expect("Task 3 panicked")?;

        assert!(rs1.next().await.is_none());
        assert!(rs2.next().await.is_none());
        assert!(rs3.next().await.is_none());

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn execute_inline_begin_idempotent_update() -> anyhow::Result<()> {
        let (db_client, _server) = setup_db_client(create_session_mock()).await;
        // Access internal state for unit testing.
        let tx = db_client
            .read_only_transaction()
            .set_begin_transaction_option(BeginTransactionOption::InlineBegin)
            .build()
            .await?;

        let id1 = bytes::Bytes::from_static(b"tx1");
        let id2 = bytes::Bytes::from_static(b"tx2");

        // 1. Initial update.
        tx.context.transaction_selector.update(id1.clone(), None)?;
        assert_eq!(
            tx.context
                .transaction_selector
                .selector()
                .await?
                .id()
                .expect("ID should be present"),
            &id1
        );

        // 2. Redundant update with same ID should succeed, as Spanner returns the
        // transaction ID on all statements executed within that transaction when
        // using multiplexed sessions.
        tx.context.transaction_selector.update(id1.clone(), None)?;

        // 3. Update with DIFFERENT ID after already Started should fail.
        let err2 = tx
            .context
            .transaction_selector
            .update(id2, None)
            .expect_err("Update after Started should fail");
        assert!(err2.to_string().contains("already Started or Failed"));

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn execute_inline_begin_with_transient_failure() -> anyhow::Result<()> {
        let mut mock = create_session_mock();
        let mut sequence = Sequence::new();

        // 1. First attempt fails transiently.
        mock.expect_execute_streaming_sql()
            .times(1)
            .in_sequence(&mut sequence)
            .returning(|_| Err(Status::new(Code::Unavailable, "Transient 1")));

        // 2. The retry of the query statement retains the inline-begin selector and succeeds.
        mock.expect_execute_streaming_sql()
            .times(1)
            .in_sequence(&mut sequence)
            .returning(|request| {
                let request = request.into_inner();
                let selector = request
                    .transaction
                    .expect("transaction selector must be present")
                    .selector
                    .expect("selector kind must be present");
                assert!(
                    matches!(selector, Selector::Begin(_)),
                    "Expected Selector::Begin for retried query with inline begin"
                );
                let mut result_set_partial = setup_select1();
                result_set_partial
                    .metadata
                    .as_mut()
                    .expect("metadata should be present")
                    .transaction = Some(mock_v1::Transaction {
                    id: vec![1, 2, 3],
                    ..Default::default()
                });
                let (tx, rx) = mpsc::channel(1);
                tx.try_send(Ok(result_set_partial))
                    .expect("send should succeed");
                Ok(Response::from(rx))
            });

        let (db_client, _server) = setup_db_client(mock).await;
        let transaction = db_client
            .read_only_transaction()
            .set_begin_transaction_option(BeginTransactionOption::InlineBegin)
            .build()
            .await?;

        let mut mock_backoff = MockBackoffPolicy::new();
        mock_backoff
            .expect_on_failure()
            .times(1)
            .returning(|_| Duration::from_nanos(1));

        let mut result_set = transaction
            .execute_query(
                Statement::builder("SELECT 1")
                    .with_backoff_policy(mock_backoff)
                    .build(),
            )
            .await?;
        assert!(
            result_set.next().await.is_some(),
            "expected first row to be present"
        );
        assert!(
            result_set.next().await.is_none(),
            "stream should be exhausted"
        );

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn execute_inline_begin_fails_fast_when_retries_exhaust() -> anyhow::Result<()> {
        let mut mock = create_session_mock();
        let mut sequence = Sequence::new();

        // 1. Initial attempt fails transiently.
        mock.expect_execute_streaming_sql()
            .times(1)
            .in_sequence(&mut sequence)
            .returning(|_| Err(Status::new(Code::Unavailable, "Transient 1")));

        // 2. Retry attempt fails transiently, exhausting the retry limit (2 attempts).
        mock.expect_execute_streaming_sql()
            .times(1)
            .in_sequence(&mut sequence)
            .returning(|_| Err(Status::new(Code::Unavailable, "Transient 2")));

        // 3. BeginTransaction must NEVER be called as a fallback.
        mock.expect_begin_transaction().never();

        let (db_client, _server) = setup_db_client(mock).await;
        let transaction = db_client
            .read_only_transaction()
            .set_begin_transaction_option(BeginTransactionOption::InlineBegin)
            .build()
            .await?;

        let mut mock_backoff = MockBackoffPolicy::new();
        mock_backoff
            .expect_on_failure()
            .times(1)
            .returning(|_| Duration::from_nanos(1));

        let first_result = transaction
            .execute_query(
                Statement::builder("SELECT 1")
                    .with_backoff_policy(mock_backoff)
                    .with_retry_policy(SpannerRetryPolicy::new().with_attempt_limit(2))
                    .build(),
            )
            .await;
        assert!(
            first_result.is_err(),
            "Expected first query to fail with Unavailable after retries exhaust"
        );
        let first_error = first_result.expect_err("first query should fail");
        assert_eq!(
            first_error.status().expect("status").code,
            GaxCode::Unavailable,
            "Expected Unavailable status code on first query failure"
        );

        // Subsequent query fails fast without any RPCs.
        let second_result = transaction
            .execute_query(Statement::builder("SELECT 1").build())
            .await;
        assert!(
            second_result.is_err(),
            "Expected second query to fail fast on a failed read-only transaction"
        );
        let second_error = second_result.expect_err("second query should fail fast");
        assert_eq!(
            second_error.status().expect("status").code,
            GaxCode::Unavailable,
            "Subsequent query error status code should match initial error"
        );

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn leader_aware_routing_query_in_read_only() -> anyhow::Result<()> {
        let mut mock = create_session_mock();
        mock.expect_execute_streaming_sql().once().returning(|req| {
            assert!(
                req.metadata()
                    .get("x-goog-spanner-route-to-leader")
                    .is_none()
            );
            let stream = adapt([Ok(mock_v1::PartialResultSet {
                metadata: Some(mock_v1::ResultSetMetadata {
                    row_type: Some(mock_v1::StructType { fields: vec![] }),
                    ..Default::default()
                }),
                ..Default::default()
            })]);
            Ok(tonic::Response::from(stream))
        });

        let (db_client, _server) = setup_db_client(mock).await;
        let tx = db_client.single_use().build();
        let _rs = tx
            .execute_query(Statement::builder("SELECT 1").build())
            .await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn execute_concurrent_begin_explicitly_redundancy_prevention() -> anyhow::Result<()> {
        let (tx_rpc, rx_rpc) = std_channel();
        let (tx_started, rx_started) = oneshot_channel();
        let tx_started_mutex = StdMutex::new(Some(tx_started));

        let mut mock = create_session_mock();
        let mut seq = mockall::Sequence::new();

        // Task 1 (leader) fires the initial query inline.
        mock.expect_execute_streaming_sql()
            .once()
            .in_sequence(&mut seq)
            .returning(move |_req| {
                if let Some(tx) = tx_started_mutex.lock().expect("mutex poisoned").take() {
                    let _ = tx.send(());
                }
                rx_rpc.recv().expect("channel broken");
                let (tx, rx) = mpsc::channel(1);
                let metadata = mock_v1::ResultSetMetadata {
                    transaction: Some(mock_v1::Transaction {
                        id: vec![42],
                        ..Default::default()
                    }),
                    ..Default::default()
                };
                let prs = mock_v1::PartialResultSet {
                    metadata: Some(metadata),
                    ..Default::default()
                };
                tx.try_send(Ok(prs)).expect("send should succeed");
                Ok(tonic::Response::new(rx))
            });

        // Task 2 (follower) arrives while Task 1 is in flight, suspends until Task 1 completes,
        // and then successfully fires its query using the newly extracted transaction ID (vec![42]).
        mock.expect_execute_streaming_sql()
            .once()
            .in_sequence(&mut seq)
            .returning(move |req| {
                let req = req.into_inner();
                assert_eq!(
                    req.transaction,
                    Some(mock_v1::TransactionSelector {
                        selector: Some(mock_v1::transaction_selector::Selector::Id(vec![42])),
                    })
                );
                let (tx, rx) = mpsc::channel(1);
                let metadata = mock_v1::ResultSetMetadata {
                    row_type: Some(mock_v1::StructType { fields: vec![] }),
                    ..Default::default()
                };
                let prs = mock_v1::PartialResultSet {
                    metadata: Some(metadata),
                    ..Default::default()
                };
                tx.try_send(Ok(prs)).expect("send should succeed");
                Ok(tonic::Response::new(rx))
            });

        let (db_client, _server) = setup_db_client(mock).await;

        let tx = Arc::new(
            db_client
                .read_only_transaction()
                .set_begin_transaction_option(BeginTransactionOption::InlineBegin)
                .build()
                .await?,
        );

        let tx_leader = Arc::clone(&tx);
        let handle_leader = tokio::spawn(async move {
            let mut rs = tx_leader
                .execute_query(Statement::builder("SELECT 1").build())
                .await?;
            let _ = rs.next().await;
            Ok::<_, crate::Error>(())
        });

        rx_started.await.expect("oneshot broken");

        // Now the state is Starting and the leader is blocked inside execute_streaming_sql.
        // Task 2 executes a concurrent query, which must wait for the leader rather than firing a redundant RPC.
        let tx_follower = Arc::clone(&tx);
        let handle_follower = tokio::spawn(async move {
            let mut rs = tx_follower
                .execute_query(Statement::builder("SELECT 2").build())
                .await?;
            let _ = rs.next().await;
            Ok::<_, crate::Error>(())
        });

        // Unblock the leader
        tx_rpc.send(()).expect("send failed");

        handle_leader.await.expect("Task 1 panicked")?;
        handle_follower.await.expect("Task 2 panicked")?;

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn execute_multi_query_redundant_transaction_id_explicit() -> anyhow::Result<()> {
        run_execute_multi_query_redundant_transaction_id(BeginTransactionOption::ExplicitBegin)
            .await
    }

    #[tokio_test_no_panics]
    async fn execute_multi_query_redundant_transaction_id_inline() -> anyhow::Result<()> {
        run_execute_multi_query_redundant_transaction_id(BeginTransactionOption::InlineBegin).await
    }

    async fn run_execute_multi_query_redundant_transaction_id(
        option: BeginTransactionOption,
    ) -> anyhow::Result<()> {
        let mut mock = create_session_mock();
        let mut sequence = mockall::Sequence::new();

        if option == BeginTransactionOption::ExplicitBegin {
            mock.expect_begin_transaction()
                .once()
                .in_sequence(&mut sequence)
                .returning(|req| {
                    let req = req.into_inner();
                    assert_eq!(
                        req.session,
                        "projects/p/instances/i/databases/d/sessions/123"
                    );
                    Ok(tonic::Response::new(mock_v1::Transaction {
                        id: vec![4, 5, 6],
                        read_timestamp: Some(prost_types::Timestamp {
                            seconds: 123456789,
                            nanos: 0,
                        }),
                        ..Default::default()
                    }))
                });

            mock.expect_execute_streaming_sql()
                .times(2)
                .returning(|req| {
                    let req = req.into_inner();
                    assert_eq!(
                        req.transaction
                            .expect("transaction should be present")
                            .selector
                            .expect("selector should be present"),
                        mock_v1::transaction_selector::Selector::Id(vec![4, 5, 6])
                    );

                    let mut result_set_partial = setup_select1();
                    result_set_partial
                        .metadata
                        .as_mut()
                        .expect("metadata should be present")
                        .transaction = Some(mock_v1::Transaction {
                        id: vec![4, 5, 6],
                        read_timestamp: Some(prost_types::Timestamp {
                            seconds: 123456789,
                            nanos: 0,
                        }),
                        ..Default::default()
                    });
                    Ok(gaxi::grpc::tonic::Response::from(adapt([Ok(
                        result_set_partial,
                    )])))
                });
        } else {
            mock.expect_begin_transaction().never();

            mock.expect_execute_streaming_sql()
                .times(1)
                .in_sequence(&mut sequence)
                .returning(|req| {
                    let req = req.into_inner();
                    assert_eq!(
                        req.session,
                        "projects/p/instances/i/databases/d/sessions/123"
                    );

                    match req
                        .transaction
                        .expect("transaction should be present")
                        .selector
                        .expect("selector should be present")
                    {
                        mock_v1::transaction_selector::Selector::Begin(_) => {}
                        _ => panic!("Expected Selector::Begin"),
                    }
                    let mut result_set_partial = setup_select1();
                    result_set_partial
                        .metadata
                        .as_mut()
                        .expect("metadata should be present")
                        .transaction = Some(mock_v1::Transaction {
                        id: vec![4, 5, 6],
                        read_timestamp: Some(prost_types::Timestamp {
                            seconds: 987654321,
                            nanos: 0,
                        }),
                        ..Default::default()
                    });
                    Ok(gaxi::grpc::tonic::Response::from(adapt([Ok(
                        result_set_partial,
                    )])))
                });

            mock.expect_execute_streaming_sql()
                .times(1)
                .in_sequence(&mut sequence)
                .returning(|req| {
                    let req = req.into_inner();
                    match req
                        .transaction
                        .expect("transaction should be present")
                        .selector
                        .expect("selector should be present")
                    {
                        mock_v1::transaction_selector::Selector::Id(id) => {
                            assert_eq!(id, vec![4, 5, 6]);
                        }
                        _ => panic!("Expected Selector::Id"),
                    }
                    let mut result_set_partial = setup_select1();
                    result_set_partial
                        .metadata
                        .as_mut()
                        .expect("metadata should be present")
                        .transaction = Some(mock_v1::Transaction {
                        id: vec![4, 5, 6],
                        read_timestamp: Some(prost_types::Timestamp {
                            seconds: 987654321,
                            nanos: 0,
                        }),
                        ..Default::default()
                    });
                    Ok(gaxi::grpc::tonic::Response::from(adapt([Ok(
                        result_set_partial,
                    )])))
                });
        }

        let (db_client, _server) = setup_db_client(mock).await;

        let transaction = db_client
            .read_only_transaction()
            .set_begin_transaction_option(option)
            .build()
            .await
            .expect("Failed to start transaction");

        for _ in 0..2 {
            let mut result_set = transaction
                .execute_query(Statement::builder("SELECT 1").build())
                .await
                .expect("Failed to execute query");

            let row = result_set
                .next()
                .await
                .expect("has row")
                .expect("has valid row");
            assert_eq!(row.raw_values(), [Value(string_val("1"))]);

            let next_result = result_set.next().await;
            assert!(next_result.is_none(), "expected None, got {next_result:?}");
        }

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn read_only_transaction_begin_with_never_retry() -> anyhow::Result<()> {
        let mut mock = MockSpanner::new();
        let mut sequence = mockall::Sequence::new();

        mock.expect_begin_transaction()
            .once()
            .in_sequence(&mut sequence)
            .returning(|_| Err(tonic::Status::unavailable("transient error")));

        mock.expect_create_session().returning(|_| {
            Ok(Response::new(mock_v1::Session {
                name: "session".to_string(),
                multiplexed: true,
                ..Default::default()
            }))
        });

        let (db_client, _server) = setup_db_client(mock).await;

        let res = db_client
            .read_only_transaction()
            .set_begin_transaction_option(BeginTransactionOption::ExplicitBegin)
            .with_begin_retry_policy(NeverRetry)
            .build()
            .await;

        assert!(res.is_err(), "should fail immediately without retry");
        let err = res.unwrap_err();
        assert_eq!(err.status().expect("status").code, GaxCode::Unavailable);

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn read_only_transaction_begin_with_attempt_timeout() -> anyhow::Result<()> {
        let mut mock = MockSpanner::new();
        let mut sequence = mockall::Sequence::new();

        mock.expect_begin_transaction()
            .once()
            .in_sequence(&mut sequence)
            .withf(|req| {
                let timeout_header = req.metadata().get("grpc-timeout");
                assert!(
                    timeout_header.is_some(),
                    "grpc-timeout header should be present"
                );
                let val = timeout_header.unwrap().to_str().unwrap();
                assert!(
                    val.contains("5000") || val.contains("5"),
                    "timeout header value '{}' should represent 5 seconds",
                    val
                );
                true
            })
            .returning(|_| {
                Ok(Response::new(mock_v1::Transaction {
                    id: vec![42],
                    ..Default::default()
                }))
            });

        mock.expect_create_session().returning(|_| {
            Ok(Response::new(mock_v1::Session {
                name: "session".to_string(),
                multiplexed: true,
                ..Default::default()
            }))
        });

        let (db_client, _server) = setup_db_client(mock).await;

        let _transaction = db_client
            .read_only_transaction()
            .set_begin_transaction_option(BeginTransactionOption::ExplicitBegin)
            .with_begin_attempt_timeout(std::time::Duration::from_secs(5))
            .build()
            .await?;

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn read_only_transaction_builder_sets_gax_options() -> anyhow::Result<()> {
        let mut mock = MockSpanner::new();
        mock.expect_create_session().returning(|_| {
            Ok(Response::new(mock_v1::Session {
                name: "session".to_string(),
                multiplexed: true,
                ..Default::default()
            }))
        });
        let (db_client, _server) = setup_db_client(mock).await;

        let builder = db_client
            .read_only_transaction()
            .with_begin_attempt_timeout(Duration::from_secs(5))
            .with_begin_retry_policy(NeverRetry)
            .with_begin_backoff_policy(ExponentialBackoff::default());

        let gax = builder
            .begin_gax_options
            .as_ref()
            .expect("begin_gax_options missing");
        assert_eq!(*gax.attempt_timeout(), Some(Duration::from_secs(5)));
        assert!(gax.retry_policy().is_some());
        assert!(gax.backoff_policy().is_some());

        Ok(())
    }

    #[test]
    fn test_merge_request_options() {
        // Case 1: Destination has values, source is empty (Destination preserved)
        let mut dest = crate::RequestOptions::default();
        dest.set_attempt_timeout(Duration::from_secs(2));
        dest.set_retry_policy(NeverRetry);

        // Source is None (Destination preserved)
        let merged = merge_request_options(dest, None);

        assert_eq!(*merged.attempt_timeout(), Some(Duration::from_secs(2)));
        assert!(merged.retry_policy().is_some());

        // Case 2: Source has overriding values, destination is empty (Source overrides)
        let dest = crate::RequestOptions::default();

        let mut source = crate::RequestOptions::default();
        source.set_attempt_timeout(Duration::from_secs(5));
        source.set_retry_policy(NeverRetry);

        let merged = merge_request_options(dest, Some(&source));

        assert_eq!(*merged.attempt_timeout(), Some(Duration::from_secs(5)));
        assert!(merged.retry_policy().is_some());

        // Case 3: Both have distinct custom headers (Headers must merge/combine)
        let mut dest = crate::RequestOptions::default();
        let mut dest_headers = HeaderMap::new();
        dest_headers.insert(
            HeaderName::from_static("x-goog-spanner-route-to-leader"),
            HeaderValue::from_static("true"),
        );
        dest = dest.insert_extension(dest_headers);

        let mut source = crate::RequestOptions::default();
        let mut src_headers = HeaderMap::new();
        src_headers.insert(
            HeaderName::from_static("x-custom-header"),
            HeaderValue::from_static("custom-value"),
        );
        source = source.insert_extension(src_headers);

        let merged = merge_request_options(dest, Some(&source));
        let merged_headers = merged
            .get_extension::<HeaderMap>()
            .expect("HeaderMap missing");

        assert_eq!(
            merged_headers
                .get("x-goog-spanner-route-to-leader")
                .expect("route to leader header should be present")
                .to_str()
                .expect("header value should be valid string"),
            "true"
        );
        assert_eq!(
            merged_headers
                .get("x-custom-header")
                .expect("custom header should be present")
                .to_str()
                .expect("header value should be valid string"),
            "custom-value"
        );
        assert_eq!(
            merged_headers
                .get_all("x-goog-spanner-route-to-leader")
                .iter()
                .count(),
            1,
            "route to leader header should only appear once"
        );
        assert_eq!(
            merged_headers.get_all("x-custom-header").iter().count(),
            1,
            "custom header should only appear once"
        );

        // Case 4: Overwriting existing header replaces value without duplicating key
        let mut dest = crate::RequestOptions::default();
        let mut dest_headers = HeaderMap::new();
        dest_headers.insert(
            HeaderName::from_static("x-custom-header"),
            HeaderValue::from_static("original-value"),
        );
        dest = dest.insert_extension(dest_headers);

        let mut source = crate::RequestOptions::default();
        let mut src_headers = HeaderMap::new();
        src_headers.insert(
            HeaderName::from_static("x-custom-header"),
            HeaderValue::from_static("overridden-value"),
        );
        source = source.insert_extension(src_headers);

        let merged = merge_request_options(dest, Some(&source));
        let merged_headers = merged
            .get_extension::<HeaderMap>()
            .expect("HeaderMap missing");

        assert_eq!(
            merged_headers.get_all("x-custom-header").iter().count(),
            1,
            "Overwritten header must only be present once"
        );
        assert_eq!(
            merged_headers
                .get("x-custom-header")
                .expect("custom header present")
                .to_str()
                .expect("valid ascii"),
            "overridden-value"
        );

        // Case 5: Source options is completely untouched (isolation)
        let source_headers = source
            .get_extension::<HeaderMap>()
            .expect("source HeaderMap present");
        assert_eq!(
            source_headers
                .get("x-custom-header")
                .expect("source custom header present")
                .to_str()
                .expect("valid ascii"),
            "overridden-value"
        );
        assert_eq!(
            source_headers.len(),
            1,
            "source HeaderMap must remain isolated and unmodified"
        );

        // Case 6: Pure merge_request_options preserves all headers intact, including request ID header
        let mut destination = RequestOptions::default();
        let mut destination_headers = HeaderMap::new();
        destination_headers.insert(
            REQUEST_ID_HEADER.clone(),
            HeaderValue::from_static("12345.1"),
        );
        destination_headers.insert(
            HeaderName::from_static("x-goog-spanner-route-to-leader"),
            HeaderValue::from_static("true"),
        );
        destination = destination.insert_extension(destination_headers);

        let merged = merge_request_options(destination, None);
        let merged_headers = merged
            .get_extension::<HeaderMap>()
            .expect("merged HeaderMap should be present");
        assert_eq!(
            merged_headers
                .get(&REQUEST_ID_HEADER)
                .expect("REQUEST_ID_HEADER should be preserved by pure merge_request_options")
                .to_str()
                .expect("valid ascii"),
            "12345.1"
        );
        assert_eq!(
            merged_headers
                .get("x-goog-spanner-route-to-leader")
                .expect("route to leader header should be preserved")
                .to_str()
                .expect("valid ascii"),
            "true"
        );
    }

    #[test]
    fn test_transaction_selector_check_failed_propagates_original_error() {
        let options = TransactionOptions {
            mode: Some(Mode::ReadWrite(Default::default())),
            ..Default::default()
        };
        let selector = ReadContextTransactionSelector::Lazy(Arc::new(StdMutex::new(
            TransactionState::Starting(options, Arc::new(Notify::new())),
        )));

        let initial_err = crate::error::internal_error("initial statement error");
        selector.set_failed(&initial_err, false);

        assert!(selector.is_first_statement_failed());

        let res = selector.check_failed();
        assert!(res.is_err(), "selector.check_failed must return an error");
        let err = res.expect_err("res must be err");
        assert!(
            err.to_string()
                .contains("Aborted due to failed initial statement"),
            "error message must contain expected text"
        );
    }

    #[tokio_test_no_panics]
    async fn test_subsequent_query_failure_does_not_lock_mutex() -> anyhow::Result<()> {
        let mut mock = create_session_mock();

        // The query fails with a transient error
        mock.expect_execute_streaming_sql()
            .once()
            .returning(|_| Err(tonic::Status::new(tonic::Code::Internal, "query failed")));

        let (db_client, _server) = setup_db_client(mock).await;

        // Construct a lazy read-only transaction context
        let mut transaction_selector = crate::model::TransactionSelector::default();
        transaction_selector.selector = Some(crate::model::transaction_selector::Selector::Id(
            bytes::Bytes::copy_from_slice(&[1, 2, 3]),
        ));

        let state = Arc::new(StdMutex::new(TransactionState::Started(
            transaction_selector.clone(),
            None,
        )));
        let selector = ReadContextTransactionSelector::Lazy(state.clone());

        let context = ReadContext {
            session_name: "projects/p/instances/i/databases/d/sessions/123".to_string(),
            client: db_client,
            transaction_selector: selector,
            precommit_token_tracker: crate::read_only_transaction::PrecommitTokenTracker::new(),
            transaction_tag: None,
            begin_transaction_request_options: None,
            affinity: None,
            aborted_state: AbortedState::new_noop(),
        };

        // Poison the mutex
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = state.lock().expect("mutex should lock");
            panic!("poisoning the mutex intentionally");
        }));

        // Execute a subsequent query (request uses Selector::Id, meaning it's a subsequent query)
        let statement = Statement::builder("SELECT 1").build();
        let request = statement
            .into_request()
            .set_session(context.session_name.clone())
            .set_transaction(transaction_selector);

        let gax_options = RequestOptions::default();

        async fn run_macro(
            context: &ReadContext,
            request: ExecuteSqlRequest,
            gax_options: RequestOptions,
        ) -> crate::Result<ResultSet> {
            execute_stream_with_retry!(
                context,
                request,
                gax_options,
                StreamOperation::Query,
                "ExecuteStreamingSql"
            )
        }

        let res = run_macro(&context, request, gax_options).await;

        // Assert that we get the query failed error rather than a panic from the poisoned mutex
        assert!(
            res.is_err(),
            "expected query to fail rather than panic on poisoned mutex"
        );
        let err = res.expect_err("expected query to fail");
        assert_eq!(err.status().map(|s| s.code), Some(GaxCode::Internal));
        assert_eq!(
            err.status().map(|s| s.message.as_str()),
            Some("query failed")
        );

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn concurrent_queries_shared_statement_with_custom_headers_isolation()
    -> anyhow::Result<()> {
        let mut mock = create_session_mock();

        mock.expect_execute_streaming_sql()
            .times(10)
            .returning(|req| {
                let metadata = req.metadata();
                // 1. Verify custom header is present, has the expected value, and is present only ONCE
                let custom_val = metadata
                    .get("x-custom-header")
                    .expect("x-custom-header must be present");
                assert_eq!(
                    custom_val.to_str().expect("valid ascii"),
                    "custom-shared-value"
                );
                assert_eq!(
                    metadata.get_all("x-custom-header").iter().count(),
                    1,
                    "x-custom-header must only be present once"
                );

                // 2. Verify request ID header is present and is present only ONCE
                assert!(
                    metadata.get("x-goog-spanner-request-id").is_some(),
                    "request ID must be present"
                );
                assert_eq!(
                    metadata.get_all("x-goog-spanner-request-id").iter().count(),
                    1,
                    "request ID header must only be present once"
                );

                Ok(gaxi::grpc::tonic::Response::from(adapt([Ok(
                    setup_select1(),
                )])))
            });

        let (db_client, _server) = setup_db_client(mock).await;

        let mut custom_headers = HeaderMap::new();
        custom_headers.insert(
            HeaderName::from_static("x-custom-header"),
            HeaderValue::from_static("custom-shared-value"),
        );

        let shared_stmt = Statement::builder("SELECT 1")
            .build()
            .with_gax_options(crate::RequestOptions::default().insert_extension(custom_headers));

        let mut handles = Vec::new();
        for _ in 0..10 {
            let client = db_client.clone();
            let stmt = shared_stmt.clone();
            handles.push(tokio::spawn(async move {
                let mut rs = client
                    .single_use()
                    .build()
                    .execute_query(stmt)
                    .await
                    .expect("Failed to execute query");
                let row = rs.next().await.expect("has row").expect("has valid row");
                assert_eq!(row.raw_values(), [Value(string_val("1"))]);
            }));
        }

        for handle in handles {
            handle.await.expect("task panicked");
        }

        // Verify the original shared statement's gax_options remained intact and isolated
        let stmt_headers = shared_stmt
            .gax_options()
            .get_extension::<HeaderMap>()
            .expect("shared statement HeaderMap intact");
        assert_eq!(
            stmt_headers
                .get("x-custom-header")
                .expect("custom header present")
                .to_str()
                .expect("valid ascii"),
            "custom-shared-value"
        );
        assert_eq!(
            stmt_headers.get_all("x-custom-header").iter().count(),
            1,
            "Shared statement custom header must only be present once"
        );
        assert!(
            stmt_headers.get("x-goog-spanner-request-id").is_none(),
            "Shared statement must NOT have been contaminated with request ID"
        );

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn multi_use_read_only_transaction_affinity_preserved() -> crate::Result<()> {
        use crate::statement::Statement;
        use gaxi::grpc::tonic::Response;

        let mut mock = create_session_mock();
        mock.expect_execute_streaming_sql().once().returning(|_| {
            Ok(Response::from(adapt([Ok(
                setup_select1_with_transaction_id(vec![1, 2, 3]),
            )])))
        });
        let (db_client, _server) = setup_db_client(mock).await;

        let affinity = Arc::new(TransactionAffinity::new_read_only());
        let builder = db_client
            .read_only_transaction()
            .with_affinity(Arc::clone(&affinity));

        let transaction = builder.build().await?;
        assert!(
            transaction
                .affinity()
                .expect("affinity present")
                .is_read_only(),
            "MultiUseReadOnlyTransaction affinity must be ReadOnly"
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
            "ResultSet generated from MultiUse transaction must share the same pinned affinity"
        );

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn multi_use_read_only_transaction_default_affinity_created() -> crate::Result<()> {
        let mock = create_session_mock();
        let (db_client, _server) = setup_db_client(mock).await;

        let builder = db_client.read_only_transaction();
        let transaction = builder.build().await?;
        assert!(
            transaction
                .affinity()
                .expect("affinity present")
                .is_read_only(),
            "Default MultiUseReadOnlyTransaction affinity must be ReadOnly"
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
    async fn single_use_read_only_transaction_affinity_present() -> crate::Result<()> {
        use crate::statement::Statement;
        use gaxi::grpc::tonic::Response;

        let mut mock = create_session_mock();
        mock.expect_execute_streaming_sql()
            .once()
            .returning(|_| Ok(Response::from(adapt([Ok(setup_select1())]))));

        let (db_client, _server) = setup_db_client(mock).await;

        let transaction = db_client.single_use().build();
        let result_set = transaction
            .execute_query(Statement::builder("SELECT 1").build())
            .await?;

        let affinity = result_set.affinity();
        assert!(
            affinity.is_read_only(),
            "SingleUse query ResultSet affinity must be ReadOnly"
        );
        assert_eq!(
            affinity.pinned_entry_id(),
            None,
            "Initial pinned entry ID should be None"
        );

        affinity
            .compare_and_set_entry_id(0, 505)
            .expect("pin entry");
        assert_eq!(
            result_set.affinity().pinned_entry_id(),
            Some(505),
            "Pinned channel ID must be retained on the ResultSet for stream resumption"
        );

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn multi_use_read_only_transaction_does_not_record_location_router_affinity() -> Result<()>
    {
        use crate::client::Spanner;
        use crate::client::SpannerBuilderExt;
        use crate::omni::InstanceType;
        use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
        use spanner_grpc_mock::start;
        use tokio::sync::mpsc::channel as mpsc_channel;

        let mut mock = create_session_mock();
        let transaction_id = vec![1, 2, 3];
        let transaction_id_clone = transaction_id.clone();

        mock.expect_fetch_cache_update().returning(|_| {
            let (_sender, receiver) = mpsc_channel(1);
            Ok(tonic::Response::from(receiver))
        });

        mock.expect_execute_streaming_sql()
            .times(2)
            .returning(move |request| {
                let request = request.into_inner();
                let result_set = if request
                    .transaction
                    .as_ref()
                    .and_then(|transaction| transaction.selector.as_ref())
                    .is_some_and(|selector| matches!(selector, Selector::Begin(_)))
                {
                    setup_select1_with_transaction_id(transaction_id_clone.clone())
                } else {
                    setup_select1()
                };
                let (sender, receiver) = mpsc_channel(1);
                sender
                    .try_send(Ok(result_set))
                    .expect("send should succeed");
                Ok(Response::from(receiver))
            });

        let (address, _server) = start("127.0.0.1:0", mock)
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
            .database_client(
                "projects/test-project/instances/test-instance/databases/test-database",
            )
            .with_location_aware_routing(true)
            .build()
            .await
            .expect("Failed to create DatabaseClient");

        let router = database_client
            .location_router()
            .expect("location router must be present for Omni");

        let transaction = database_client
            .read_only_transaction()
            .set_begin_transaction_option(BeginTransactionOption::InlineBegin)
            .build()
            .await
            .expect("Failed to build read only transaction");

        // Execute first query (inline begin)
        let mut result_set = transaction
            .execute_query(Statement::builder("SELECT 1").build())
            .await
            .expect("first query should succeed");
        let _ = result_set.next().await;

        // Execute second query (dispatched with Selector::Id)
        let mut second_result_set = transaction
            .execute_query(Statement::builder("SELECT 2").build())
            .await
            .expect("second query should succeed");
        let _ = second_result_set.next().await;

        assert_eq!(
            router.affinity_count(),
            0,
            "MultiUseReadOnlyTransaction must not record affinity in LocationRouter"
        );
        assert_eq!(
            router.get_transaction_affinity(&transaction_id),
            None,
            "Transaction ID must not have recorded affinity in LocationRouter"
        );

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn inlined_begin_query_metadata_missing_transaction_fails_subsequent_statement_receives_error()
    -> anyhow::Result<()> {
        use crate::statement::Statement;
        use gaxi::grpc::tonic::Response;

        let mut mock = create_session_mock();
        let mut sequence = Sequence::new();

        // 1. Initial query stream returns metadata without transaction entity.
        mock.expect_execute_streaming_sql()
            .times(1)
            .in_sequence(&mut sequence)
            .returning(|_| {
                Ok(Response::from(adapt([Ok(setup_select1())]))) // setup_select1 has metadata but no transaction ID
            });

        let (database_client, _server) = setup_db_client(mock).await;
        let transaction = database_client
            .read_only_transaction()
            .set_begin_transaction_option(BeginTransactionOption::InlineBegin)
            .build()
            .await?;

        // First statement fails because metadata is missing transaction ID.
        let first_result = transaction
            .execute_query(Statement::builder("SELECT 1").build())
            .await;
        assert!(
            first_result.is_err(),
            "Expected first query to fail with missing transaction ID error"
        );
        let first_error = first_result.expect_err("first query failed").to_string();
        assert!(
            first_error.contains("failed to return a transaction ID"),
            "Expected missing transaction ID error: {first_error}"
        );

        // Subsequent statement on the same read-only transaction must fail with the start failure,
        // and must NOT return Aborted (read-only transactions are never aborted).
        let second_result = transaction
            .execute_query(Statement::builder("SELECT 2").build())
            .await;
        assert!(
            second_result.is_err(),
            "Expected subsequent query on failed read-only transaction to fail"
        );
        let second_error = second_result.expect_err("second query failed").to_string();
        assert_eq!(
            second_error,
            "cannot deserialize the response unexpected data received from Spanner: Transaction cannot be used: initial statement failed with: Spanner failed to return a transaction ID for a query that included a BeginTransaction option",
            "Expected exact cleanly wrapped error message on subsequent query without duplicate prefixes"
        );
        assert!(
            !second_error.contains("Aborted"),
            "Read-only transactions should never return Aborted error: {second_error}"
        );

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn inlined_begin_query_stream_empty_fails_fast() -> anyhow::Result<()> {
        let mut mock = create_session_mock();

        // 1. Initial query stream returns an empty stream (no metadata, no chunks).
        mock.expect_execute_streaming_sql()
            .times(1)
            .returning(|request| {
                let request = request.into_inner();
                let transaction = request
                    .transaction
                    .as_ref()
                    .expect("transaction options required for inline begin");
                let selector = transaction.selector.as_ref().expect("selector required");
                assert!(
                    matches!(selector, Selector::Begin(_)),
                    "Expected Selector::Begin on initial query"
                );
                Ok(Response::from(adapt([])))
            });

        // 2. BeginTransaction must NEVER be called as a fallback.
        mock.expect_begin_transaction().never();

        let (database_client, _server) = setup_db_client(mock).await;
        let transaction = database_client
            .read_only_transaction()
            .set_begin_transaction_option(BeginTransactionOption::InlineBegin)
            .build()
            .await?;

        // First query fails fast because the stream ended without metadata.
        let first_result = transaction
            .execute_query(Statement::builder("SELECT 1").build())
            .await;
        assert!(
            first_result.is_err(),
            "Initial query must fail when stream ends without metadata"
        );

        // Subsequent query on the same transaction fails fast without any RPCs.
        let second_result = transaction
            .execute_query(Statement::builder("SELECT 2").build())
            .await;
        assert!(
            second_result.is_err(),
            "Subsequent query must fail fast on a failed read-only transaction"
        );
        let second_error = second_result.expect_err("second query failed");
        assert!(
            second_error.to_string().contains(
                "Transaction cannot be used: initial statement failed with: Query stream ended without metadata or error"
            ),
            "Expected formatted failure message, got: {second_error}"
        );

        Ok(())
    }

    #[test]
    fn selector_for_restart_state_machine_arms() {
        let options = TransactionOptions::default();
        let notify = Arc::new(Notify::new());

        // 1. NotStarted returns Internal error
        let selector = ReadContextTransactionSelector::Lazy(Arc::new(StdMutex::new(
            TransactionState::NotStarted(options.clone()),
        )));
        let result = selector.selector_for_restart();
        assert!(result.is_err(), "NotStarted must return error");
        let error = result.expect_err("NotStarted returns error");
        assert!(
            error
                .to_string()
                .contains("Cannot restart stream on a NotStarted transaction"),
            "Expected cannot restart message, got: {error}"
        );

        // 2. Starting returns Begin selector
        let selector = ReadContextTransactionSelector::Lazy(Arc::new(StdMutex::new(
            TransactionState::Starting(options.clone(), notify),
        )));
        let result = selector.selector_for_restart();
        assert!(result.is_ok(), "Starting must return Begin selector");
        assert!(
            matches!(
                result.expect("selector is ok").selector,
                Some(ModelSelector::Begin(_))
            ),
            "Expected Begin selector"
        );

        // 3. Started returns Id selector
        let started_selector = TransactionSelector::default().set_id(vec![1, 2, 3]);
        let selector = ReadContextTransactionSelector::Lazy(Arc::new(StdMutex::new(
            TransactionState::Started(started_selector.clone(), None),
        )));
        let result = selector.selector_for_restart();
        assert!(result.is_ok(), "Started must return Id selector");
        assert_eq!(
            result.expect("selector is ok"),
            started_selector,
            "Expected Id selector"
        );

        // 4. Failed returns failure.to_error() (preserving service error or wrapping internal error)
        let status = GaxStatus::default()
            .set_code(GaxCode::InvalidArgument)
            .set_message("Original failure");
        let selector = ReadContextTransactionSelector::Lazy(Arc::new(StdMutex::new(
            TransactionState::Failed(TransactionStartFailure::Service(status)),
        )));
        let result = selector.selector_for_restart();
        assert!(result.is_err(), "Failed must return error");
        let error = result.expect_err("Failed returns error");
        assert_eq!(
            error.status().map(|status| status.code),
            Some(GaxCode::InvalidArgument),
            "Expected InvalidArgument status code"
        );
        assert_eq!(
            error.status().map(|status| status.message.as_str()),
            Some("Transaction cannot be used: initial statement failed with: Original failure"),
            "Expected formatted error message preserving service error"
        );

        let selector = ReadContextTransactionSelector::Lazy(Arc::new(StdMutex::new(
            TransactionState::Failed(TransactionStartFailure::Internal(
                "Missing transaction ID".to_string(),
            )),
        )));
        let result = selector.selector_for_restart();
        assert!(result.is_err(), "Failed must return error");
        let error = result.expect_err("Failed returns error");
        assert!(
            error
                .to_string()
                .contains("Transaction cannot be used: initial statement failed with:")
                && error.to_string().contains("Missing transaction ID"),
            "Expected start failure message, got: {error}"
        );

        // 5. FirstStatementFailed returns synthetic Aborted
        let selector = ReadContextTransactionSelector::Lazy(Arc::new(StdMutex::new(
            TransactionState::FirstStatementFailed,
        )));
        let result = selector.selector_for_restart();
        assert!(result.is_err(), "FirstStatementFailed must return error");
        let error = result.expect_err("FirstStatementFailed returns error");
        assert!(
            error
                .to_string()
                .contains("Aborted due to failed initial statement"),
            "Expected aborted error, got: {error}"
        );

        // 6. Fixed returns the fixed selector
        let fixed_selector = TransactionSelector::default().set_id(vec![4, 5, 6]);
        let selector = ReadContextTransactionSelector::Fixed(fixed_selector.clone(), None);
        let result = selector.selector_for_restart();
        assert!(result.is_ok(), "Fixed must return fixed selector");
        assert_eq!(
            result.expect("selector is ok"),
            fixed_selector,
            "Expected fixed selector"
        );
    }

    #[test]
    fn transaction_start_failure_preserves_error_details() {
        use google_cloud_rpc::model::RetryInfo;
        use wkt::Duration;

        let retry_info = RetryInfo::default().set_retry_delay(Duration::clamp(5, 0));
        let status = GaxStatus::default()
            .set_code(GaxCode::ResourceExhausted)
            .set_message("Quota exceeded")
            .set_details(vec![StatusDetails::RetryInfo(retry_info)]);

        let failure = TransactionStartFailure::Service(status);
        let error = failure.to_error();

        let error_status = error.status().expect("status should be preserved");
        assert_eq!(
            error_status.code,
            GaxCode::ResourceExhausted,
            "status code must match"
        );
        assert_eq!(
            error_status.message,
            "Transaction cannot be used: initial statement failed with: Quota exceeded",
            "status message must match"
        );
        assert_eq!(
            error_status.details.len(),
            1,
            "status details must be preserved"
        );
        match &error_status.details[0] {
            StatusDetails::RetryInfo(info) => {
                assert_eq!(
                    info.retry_delay,
                    Some(Duration::clamp(5, 0)),
                    "retry_delay in details must match"
                );
            }
            other => panic!("Expected RetryInfo details, got: {other:?}"),
        }
    }

    #[test]
    fn on_cancelled_starting_state_machine_transitions() {
        use crate::generated::gapic_dataplane::model::TransactionOptions;
        use crate::generated::gapic_dataplane::model::transaction_options::Mode;

        let rw_options =
            TransactionOptions::default().set_mode(Mode::ReadWrite(Default::default()));

        // 1. ReadWrite mode: Starting -> FirstStatementFailed (simulating drop without disarm)
        let notify = Arc::new(Notify::new());
        let selector = ReadContextTransactionSelector::Lazy(Arc::new(StdMutex::new(
            TransactionState::Starting(rw_options.clone(), notify),
        )));
        selector.on_cancelled_starting();
        assert!(
            selector.is_first_statement_failed(),
            "on_cancelled_starting in ReadWrite mode must transition state to FirstStatementFailed"
        );

        // 2. ReadOnly mode: Starting -> NotStarted (resets so next attempt can restart)
        let ro_options = TransactionOptions::default().set_mode(Mode::ReadOnly(Default::default()));
        let notify = Arc::new(Notify::new());
        let selector = ReadContextTransactionSelector::Lazy(Arc::new(StdMutex::new(
            TransactionState::Starting(ro_options.clone(), notify),
        )));
        selector.on_cancelled_starting();
        assert!(
            matches!(
                selector.poll_selector_status(),
                Ok(SelectorStatus::Wait(_)) | Err(_)
            ) || selector.get_id_no_wait().is_ok(),
            "on_cancelled_starting in ReadOnly mode must reset to NotStarted"
        );

        // 3. LazyTransactionStartGuard drop without disarm -> transitions to FirstStatementFailed
        let notify = Arc::new(Notify::new());
        let selector = ReadContextTransactionSelector::Lazy(Arc::new(StdMutex::new(
            TransactionState::Starting(rw_options.clone(), notify),
        )));
        {
            let _guard = LazyTransactionStartGuard::new(Some(selector.clone()));
            // guard dropped here without disarm()
        }
        assert!(
            selector.is_first_statement_failed(),
            "Guard dropped without disarm must transition state to FirstStatementFailed"
        );

        // 4. LazyTransactionStartGuard with disarm() -> does not trigger cancellation
        let notify = Arc::new(Notify::new());
        let selector = ReadContextTransactionSelector::Lazy(Arc::new(StdMutex::new(
            TransactionState::Starting(rw_options.clone(), notify),
        )));
        {
            let mut guard = LazyTransactionStartGuard::new(Some(selector.clone()));
            guard.disarm();
        }
        assert!(
            !selector.is_first_statement_failed(),
            "Disarmed guard must not transition state on drop"
        );
    }

    #[tokio_test_no_panics]
    async fn begin_explicitly_cancellation_safely_wakes_waiters() -> crate::Result<()> {
        use google_cloud_gax::exponential_backoff::ExponentialBackoffBuilder;
        use google_cloud_gax::retry_policy::AlwaysRetry;
        use tokio::sync::Notify;

        let mut mock = create_session_mock();
        let mut sequence = Sequence::new();
        let leader_rpc_received = Arc::new(Notify::new());
        let leader_rpc_received_clone = Arc::clone(&leader_rpc_received);

        // 1. Leader's BeginTransaction RPC arrives and returns Unavailable so the leader
        //    suspends in its retry backoff while holding `StartingGuard` (state == `Starting`).
        //    The backoff timer itself is never waited on because the test immediately aborts
        //    the leader task once notified.
        mock.expect_begin_transaction()
            .once()
            .in_sequence(&mut sequence)
            .returning(move |_| {
                leader_rpc_received_clone.notify_one();
                Err(Status::unavailable("hold leader in Starting until aborted"))
            });

        // 2. After the leader is cancelled in `Starting`, the woken follower becomes the
        //    new leader and succeeds on its BeginTransaction RPC.
        mock.expect_begin_transaction()
            .once()
            .in_sequence(&mut sequence)
            .returning(|_| {
                Ok(Response::new(mock_v1::Transaction {
                    id: vec![1, 2, 3],
                    read_timestamp: Some(Timestamp {
                        seconds: 100,
                        nanos: 0,
                    }),
                    ..Default::default()
                }))
            });

        let (db_client, _server) = setup_db_client(mock).await;
        let transaction = db_client
            .read_only_transaction()
            .set_timestamp_bound(TimestampBound::strong())
            .build()
            .await?;

        let selector = transaction.context.transaction_selector.clone();

        let mut leader_options = GaxRequestOptions::default();
        leader_options.set_retry_policy(AlwaysRetry);
        leader_options.set_backoff_policy(
            ExponentialBackoffBuilder::new()
                .with_initial_delay(Duration::from_secs(60))
                .with_maximum_delay(Duration::from_secs(60))
                .with_scaling(2.0)
                .build()
                .expect("valid backoff"),
        );

        let context_leader = transaction.context.clone();
        let leader_handle = spawn(async move {
            context_leader
                .begin_explicitly_if_not_started(leader_options, None)
                .await
        });

        let context_follower = transaction.context.clone();
        let follower_handle = spawn(async move {
            context_follower
                .begin_explicitly_if_not_started(GaxRequestOptions::default(), None)
                .await
        });

        // Wait until the mock server has processed the leader's RPC, then yield once
        // so the leader parks on its backoff sleep timer and the follower parks on `Wait(notified)`.
        leader_rpc_received.notified().await;
        yield_now().await;
        assert!(
            selector.is_starting(),
            "Leader must be in Starting state before cancellation"
        );

        // Cancel the leader task while in `Starting` (immediately cancelling its backoff sleep).
        leader_handle.abort();
        let _ = leader_handle.await;

        // The follower MUST NOT hang; it should be woken up by on_cancelled_starting(),
        // loop around in begin_explicitly, see NotStarted, and execute BeginTransaction!
        let follower_inner = follower_handle.await.expect("follower task must not panic");
        assert!(
            follower_inner.is_ok(),
            "Follower begin_explicitly call should succeed as new leader after reset"
        );
        assert_eq!(
            selector.get_id_no_wait()?,
            Some(bytes::Bytes::from_static(&[1, 2, 3])),
            "Follower should have established transaction ID [1, 2, 3]"
        );

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn begin_explicitly_empty_transaction_id_fails_and_propagates_to_followers()
    -> crate::Result<()> {
        let mut mock = create_session_mock();
        mock.expect_begin_transaction().once().returning(|_| {
            Ok(Response::new(mock_v1::Transaction {
                id: vec![],
                ..Default::default()
            }))
        });

        let (db_client, _server) = setup_db_client(mock).await;
        let transaction = db_client
            .read_only_transaction()
            .set_timestamp_bound(TimestampBound::strong())
            .build()
            .await?;

        let result = transaction
            .context
            .begin_explicitly_if_not_started(GaxRequestOptions::default(), None)
            .await;
        assert!(
            result.is_err(),
            "begin_explicitly must fail when Spanner returns an empty transaction ID"
        );
        let error = result.expect_err("expected empty ID error");
        assert!(
            error
                .to_string()
                .contains("Transaction ID was not returned by Spanner"),
            "Expected missing transaction ID error, got: {error}"
        );

        // Subsequent call to begin_explicitly in Failed state must also return Err, not Ok(())
        let second_result = transaction
            .context
            .begin_explicitly_if_not_started(GaxRequestOptions::default(), None)
            .await;
        assert!(
            second_result.is_err(),
            "Subsequent begin_explicitly on Failed transaction must return Err"
        );

        // Call to begin_explicitly in FirstStatementFailed state must return synthetic Aborted
        if let ReadContextTransactionSelector::Lazy(ref state) =
            transaction.context.transaction_selector
        {
            let mut guard = state.lock().expect("lock state");
            *guard = TransactionState::FirstStatementFailed;
        }
        let third_result = transaction
            .context
            .begin_explicitly_if_not_started(GaxRequestOptions::default(), None)
            .await;
        assert!(
            third_result.is_err(),
            "begin_explicitly in FirstStatementFailed state must return Err"
        );
        let third_error = third_result.expect_err("expected synthetic aborted error");
        assert!(
            third_error
                .to_string()
                .contains("Aborted due to failed initial statement"),
            "Expected synthetic aborted error, got: {third_error}"
        );

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn streaming_query_empty_transaction_id_fails_and_wakes_waiters() -> crate::Result<()> {
        let mut mock = create_session_mock();
        mock.expect_execute_streaming_sql().once().returning(|_| {
            Ok(Response::from(adapt([Ok(
                setup_select1_with_transaction_id(vec![]),
            )])))
        });
        let (db_client, _server) = setup_db_client(mock).await;
        let transaction = db_client
            .read_only_transaction()
            .set_timestamp_bound(TimestampBound::strong())
            .build()
            .await?;

        let query = Statement::builder("SELECT 1").build();
        let result = transaction.execute_query(query).await;
        assert!(
            result.is_err(),
            "execute_query must fail when Spanner returns an empty transaction ID"
        );
        let error = result.expect_err("expected error");
        assert!(
            error
                .to_string()
                .contains("Spanner failed to return a transaction ID"),
            "Expected missing transaction ID error message, got: {error}"
        );

        Ok(())
    }

    #[test]
    fn transaction_selector_recovers_from_poisoned_lock_on_start() -> crate::Result<()> {
        use bytes::Bytes;
        use std::panic::catch_unwind;

        let inner_state = Arc::new(StdMutex::new(TransactionState::NotStarted(
            TransactionOptions::default().set_read_only(ReadOnly::default()),
        )));
        let selector = ReadContextTransactionSelector::Lazy(Arc::clone(&inner_state));

        // Deliberately poison the mutex.
        let inner_state_clone = Arc::clone(&inner_state);
        let _ = catch_unwind(move || {
            let _guard = inner_state_clone
                .lock()
                .expect("lock must succeed before poison");
            panic!("deliberate panic to poison mutex");
        });
        assert!(
            inner_state.is_poisoned(),
            "transaction state mutex must be poisoned"
        );

        // Verify query methods recover under poison.
        assert!(
            selector.is_lazy(),
            "selector must be identified as lazy even if poisoned"
        );
        assert!(
            !selector.is_started(),
            "selector must not be started initially"
        );
        assert!(
            !selector.is_starting(),
            "selector must not be starting initially"
        );

        // Verify poll_selector_status successfully transitions under poison.
        let status = selector.poll_selector_status()?;
        match status {
            SelectorStatus::Ready(transaction_selector) => {
                assert!(
                    transaction_selector.begin().is_some(),
                    "expected begin options in ready status"
                );
            }
            SelectorStatus::Wait(_) => {
                panic!("first poller should become leader, not waiter");
            }
        }
        assert!(
            selector.is_starting(),
            "selector must be starting after poll_selector_status"
        );

        // Verify update succeeds and transitions to Started under poison.
        let timestamp = wkt::Timestamp::clamp(12345, 6789);
        selector.update(Bytes::from_static(b"test-tx-id"), Some(timestamp))?;

        assert!(
            selector.is_started(),
            "selector must be started after update"
        );
        assert_eq!(
            selector.get_id_no_wait()?,
            Some(Bytes::from_static(b"test-tx-id")),
            "expected transaction ID"
        );
        assert_eq!(
            selector.read_timestamp(),
            Some(timestamp),
            "expected read timestamp"
        );

        Ok(())
    }

    #[test]
    fn transaction_selector_recovers_from_poisoned_lock_on_failure() -> crate::Result<()> {
        use std::panic::catch_unwind;

        let notify = Arc::new(Notify::new());
        let options = TransactionOptions::default().set_read_only(ReadOnly::default());
        let inner_state = Arc::new(StdMutex::new(TransactionState::Starting(
            options,
            Arc::clone(&notify),
        )));
        let selector = ReadContextTransactionSelector::Lazy(Arc::clone(&inner_state));

        // Deliberately poison the mutex.
        let inner_state_clone = Arc::clone(&inner_state);
        let _ = catch_unwind(move || {
            let _guard = inner_state_clone
                .lock()
                .expect("lock must succeed before poison");
            panic!("deliberate panic to poison mutex");
        });
        assert!(
            inner_state.is_poisoned(),
            "transaction state mutex must be poisoned"
        );

        // For read-only, set_failed transitions directly to Failed (fail-fast).
        let test_error = internal_error("transient stream error");
        // set_failed marks the selector as Failed despite lock poisoning.
        selector.set_failed(&test_error, false);

        let check_result = selector.check_failed();
        assert!(
            check_result.is_err(),
            "check_failed must return error after set_failed on poisoned lock"
        );

        let restart_result = selector.selector_for_restart();
        assert!(
            restart_result.is_err(),
            "selector_for_restart must return error for Failed transaction on poisoned lock"
        );

        Ok(())
    }

    #[test]
    fn transaction_selector_on_cancelled_starting_recovers_from_poisoned_lock() -> crate::Result<()>
    {
        use std::panic::catch_unwind;

        // Test read-write cancellation transitions to FirstStatementFailed.
        {
            use crate::model::transaction_options::ReadWrite;
            let notify = Arc::new(Notify::new());
            let read_write_options =
                TransactionOptions::default().set_read_write(ReadWrite::default());
            let inner_state = Arc::new(StdMutex::new(TransactionState::Starting(
                read_write_options,
                Arc::clone(&notify),
            )));
            let selector = ReadContextTransactionSelector::Lazy(Arc::clone(&inner_state));

            let inner_state_clone = Arc::clone(&inner_state);
            let _ = catch_unwind(move || {
                let _guard = inner_state_clone
                    .lock()
                    .expect("lock must succeed before poison");
                panic!("deliberate panic to poison mutex");
            });
            assert!(
                inner_state.is_poisoned(),
                "read-write transaction mutex must be poisoned"
            );

            selector.on_cancelled_starting();
            assert!(
                selector.is_first_statement_failed(),
                "read-write selector must transition to FirstStatementFailed on cancellation"
            );
        }

        // Test read-only cancellation transitions back to NotStarted.
        {
            let notify = Arc::new(Notify::new());
            let read_only_options =
                TransactionOptions::default().set_read_only(ReadOnly::default());
            let inner_state = Arc::new(StdMutex::new(TransactionState::Starting(
                read_only_options,
                Arc::clone(&notify),
            )));
            let selector = ReadContextTransactionSelector::Lazy(Arc::clone(&inner_state));

            let inner_state_clone = Arc::clone(&inner_state);
            let _ = catch_unwind(move || {
                let _guard = inner_state_clone
                    .lock()
                    .expect("lock must succeed before poison");
                panic!("deliberate panic to poison mutex");
            });
            assert!(
                inner_state.is_poisoned(),
                "read-only transaction mutex must be poisoned"
            );

            selector.on_cancelled_starting();
            assert!(
                !selector.is_starting(),
                "read-only selector must reset from Starting back to NotStarted on cancellation"
            );
        }

        Ok(())
    }

    #[test]
    fn aborted_state_record_and_get() {
        let state = AbortedState::default();
        assert!(
            state.aborted_info().is_none(),
            "initially aborted_info must be None"
        );

        let retry_info = RetryInfo::default().set_retry_delay(WktDuration::clamp(5, 42_000_000));
        let status = GaxStatus::default()
            .set_code(GaxCode::Aborted)
            .set_message("transaction was aborted")
            .set_details(vec![StatusDetails::RetryInfo(retry_info)]);
        let error = Error::service(status);

        state.record(&error, false);
        let info = state
            .aborted_info()
            .expect("aborted_info must be present after recording abort");
        assert_eq!(
            info.status.code,
            GaxCode::Aborted,
            "status code must be Aborted"
        );
        assert_eq!(
            info.status.message, "transaction was aborted",
            "status message must match"
        );
        assert_eq!(
            info.retry_delay,
            Some(Duration::new(5, 42_000_000)),
            "retry delay must be extracted from RetryInfo"
        );

        let reconstructed = info.to_error();
        assert_eq!(
            reconstructed.status().map(|status| status.code),
            Some(GaxCode::Aborted),
            "reconstructed error must have Aborted code"
        );
        assert_eq!(
            reconstructed.status().map(|status| status.message.as_str()),
            Some("transaction was aborted"),
            "reconstructed error must preserve message"
        );
    }

    #[test]
    fn aborted_state_first_abort_wins() {
        let state = AbortedState::default();

        let status1 = GaxStatus::default()
            .set_code(GaxCode::Aborted)
            .set_message("first abort");
        let error1 = Error::service(status1);

        let status2 = GaxStatus::default()
            .set_code(GaxCode::Aborted)
            .set_message("second abort");
        let error2 = Error::service(status2);

        state.record(&error1, false);
        state.record(&error2, false);

        let info = state
            .aborted_info()
            .expect("aborted_info must be present after recording abort");
        assert_eq!(
            info.status.message, "first abort",
            "first abort message must be preserved"
        );
    }

    #[test]
    fn aborted_state_ignores_non_aborted_errors() {
        let state = AbortedState::default();

        let status = GaxStatus::default()
            .set_code(GaxCode::NotFound)
            .set_message("not found");
        let error = Error::service(status);

        state.record(&error, false);
        assert!(
            state.aborted_info().is_none(),
            "non-aborted error must not be recorded"
        );
    }

    #[test]
    fn aborted_state_emulator_internal_error_handling() {
        let state = AbortedState::default();

        let status = GaxStatus::default()
            .set_code(GaxCode::Internal)
            .set_message("Schema generation error: was not registered with the Action Manager");
        let error = Error::service(status);

        // When not in emulator mode, Internal error is NOT treated as aborted.
        state.record(&error, false);
        assert!(
            state.aborted_info().is_none(),
            "emulator internal error must not be recorded when is_emulator is false"
        );

        // When in emulator mode, it IS treated as aborted.
        state.record(&error, true);
        let info = state
            .aborted_info()
            .expect("emulator internal error must be recorded when is_emulator is true");
        assert_eq!(
            info.status.code,
            GaxCode::Internal,
            "recorded status must be Internal"
        );
    }

    #[test]
    fn aborted_state_noop_ignores_all_errors() {
        let state = AbortedState::new_noop();
        let status = GaxStatus::default().set_code(GaxCode::Aborted);
        let error = Error::service(status);
        state.record(&error, false);
        state.record(&error, true);
        assert!(
            state.aborted_info().is_none(),
            "noop state must ignore all errors"
        );
    }
}
