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

use crate::database_client::DatabaseClient;
use crate::error::internal_error;
use crate::model::TransactionOptions;
use crate::model::TransactionSelector;
use crate::model::transaction_options::ReadOnly;
use crate::precommit::PrecommitTokenTracker;
use crate::result_set::{ResultSet, ResultSetParams, StreamOperation};
use crate::statement::Statement;
use crate::timestamp_bound::TimestampBound;
use crate::transaction_retry_policy::is_aborted;
use google_cloud_gax::backoff_policy::BackoffPolicyArg;
use google_cloud_gax::options::internal::RequestOptionsExt as _;
use google_cloud_gax::retry_policy::RetryPolicyArg;
use std::mem::replace;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Notify;

/// A builder for [SingleUseReadOnlyTransaction].
///
/// # Example
/// ```
/// # use google_cloud_spanner::client::Spanner;
/// # use google_cloud_spanner::TimestampBound;
/// # async fn build_tx(spanner: Spanner) -> Result<(), google_cloud_spanner::Error> {
/// let db_client = spanner.database_client("projects/p/instances/i/databases/d").build().await?;
/// let read_only_tx = db_client.single_use()
///     .with_timestamp_bound(TimestampBound::strong())
///     .build();
/// # Ok(())
/// # }
/// ```
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
    /// # use google_cloud_spanner::TimestampBound;
    /// # async fn set_bound(spanner: Spanner) -> Result<(), google_cloud_spanner::Error> {
    /// let db_client = spanner.database_client("projects/p/instances/i/databases/d").build().await?;
    /// let builder = db_client.single_use().with_timestamp_bound(TimestampBound::strong());
    /// # Ok(())
    /// # }
    /// ```
    /// When reading data in Spanner in a read-only transaction, you can set a timestamp bound,
    /// which tells Spanner how to choose a timestamp at which to read the data.
    ///
    /// See <https://docs.cloud.google.com/spanner/docs/timestamp-bounds> for more information.
    pub fn with_timestamp_bound(mut self, bound: TimestampBound) -> Self {
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
            Some(b) => ReadOnly::default().set_timestamp_bound(b.0),
            None => ReadOnly::default().set_strong(true),
        };
        let transaction_selector = crate::model::TransactionSelector::default()
            .set_single_use(TransactionOptions::default().set_read_only(read_only));

        let session_name = self.client.session_name();
        let channel_hint = self.client.spanner.next_channel_hint();
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
                channel_hint,
                begin_transaction_request_options: None,
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
/// # use google_cloud_spanner::Statement;
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
    /// # use google_cloud_spanner::Statement;
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
        self.context.execute_query(statement).await
    }

    /// Reads rows from the database using key lookups and scans, as a simple key/value style alternative to execute_query.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::Spanner;
    /// # use google_cloud_spanner::{KeySet, ReadRequest};
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
    #[default]
    InlineBegin,
    /// The transaction will be started explicitly using a `BeginTransaction` RPC.
    ExplicitBegin,
}

/// A builder for [MultiUseReadOnlyTransaction].
///
/// # Example
/// ```
/// # use google_cloud_spanner::client::Spanner;
/// # use google_cloud_spanner::TimestampBound;
/// # async fn build_tx(spanner: Spanner) -> Result<(), google_cloud_spanner::Error> {
/// let db_client = spanner.database_client("projects/p/instances/i/databases/d").build().await?;
/// let read_only_tx = db_client.read_only_transaction()
///     .with_timestamp_bound(TimestampBound::strong())
///     .build()
///     .await?;
/// # Ok(())
/// # }
/// ```
pub struct MultiUseReadOnlyTransactionBuilder {
    client: DatabaseClient,
    timestamp_bound: Option<TimestampBound>,
    begin_transaction_option: BeginTransactionOption,
    begin_gax_options: Option<crate::RequestOptions>,
}

impl MultiUseReadOnlyTransactionBuilder {
    pub(crate) fn new(client: DatabaseClient) -> Self {
        Self {
            client,
            timestamp_bound: None,
            begin_transaction_option: BeginTransactionOption::InlineBegin,
            begin_gax_options: None,
        }
    }

    /// Sets the option for how to start a transaction.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::Spanner;
    /// # use google_cloud_spanner::transaction::BeginTransactionOption;
    /// # use google_cloud_spanner::Statement;
    /// # async fn set_begin_option(spanner: Spanner) -> Result<(), google_cloud_spanner::Error> {
    /// let db_client = spanner.database_client("projects/p/instances/i/databases/d").build().await?;
    /// let transaction = db_client.read_only_transaction().with_begin_transaction_option(BeginTransactionOption::ExplicitBegin).build().await?;
    /// let statement = Statement::builder("SELECT * FROM users").build();
    /// let result_set = transaction.execute_query(statement).await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// By default, the Spanner client will inline the `BeginTransaction` call with the first query
    /// in the transaction. This reduces the number of round-trips to Spanner that are needed for a
    /// transaction. Setting this option to `ExplicitBegin` can be beneficial for specific transaction
    /// shapes:
    ///
    /// 1. When the transaction executes multiple parallel queries at the start of the transaction.
    ///    Only one query can include a `BeginTransaction` option, and all other queries must wait for
    ///    the first query to return the first result before they can proceed to execute. A
    ///    `BeginTransaction` RPC will quickly return a transaction ID and allow all queries to start
    ///    execution in parallel once the transaction ID has been returned.
    /// 2. When the first query in the transaction could fail. If the query fails, then it will also
    ///    not start a transaction and return a transaction ID. The transaction will then fall back to
    ///    executing a `BeginTransaction` RPC and retry the first query.
    ///
    /// Default is `BeginTransactionOption::InlineBegin`.
    pub fn with_begin_transaction_option(mut self, option: BeginTransactionOption) -> Self {
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
    /// # use google_cloud_spanner::TimestampBound;
    /// # async fn set_bound(spanner: Spanner) -> Result<(), google_cloud_spanner::Error> {
    /// let db_client = spanner.database_client("projects/p/instances/i/databases/d").build().await?;
    /// let builder = db_client.read_only_transaction().with_timestamp_bound(TimestampBound::strong());
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_timestamp_bound(mut self, bound: TimestampBound) -> Self {
        self.timestamp_bound = Some(bound);
        self
    }

    async fn begin(
        &self,
        session_name: String,
        options: TransactionOptions,
        channel_hint: usize,
        request_options: crate::RequestOptions,
    ) -> crate::Result<ReadContextTransactionSelector> {
        let response = execute_begin_transaction(
            &self.client,
            session_name,
            options,
            None,
            channel_hint,
            request_options,
            None,
        )
        .await?;

        let transaction_selector = crate::model::TransactionSelector::default().set_id(response.id);

        Ok(ReadContextTransactionSelector::Fixed(
            transaction_selector,
            response.read_timestamp,
        ))
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
        let read_only = match self.timestamp_bound.as_ref() {
            Some(b) => read_only.set_timestamp_bound(b.0.clone()),
            None => read_only.set_strong(true),
        };
        let options = TransactionOptions::default().set_read_only(read_only);

        let session_name = self.client.session_name();
        let channel_hint = self.client.spanner.next_channel_hint();
        let selector = match self.begin_transaction_option {
            BeginTransactionOption::ExplicitBegin => {
                self.begin(
                    session_name.clone(),
                    options,
                    channel_hint,
                    self.begin_gax_options.clone().unwrap_or_default(),
                )
                .await?
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
                channel_hint,
                begin_transaction_request_options: self.begin_gax_options.clone(),
            },
        })
    }
}

/// A multi-use read-only transaction. This transaction can be used for multiple read queries
/// ensuring consistency across all queries.
///
/// # Example
/// ```
/// # use google_cloud_spanner::client::Spanner;
/// # use google_cloud_spanner::Statement;
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
    /// # use google_cloud_spanner::Statement;
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
    pub async fn execute_query<T: Into<Statement>>(
        &self,
        statement: T,
    ) -> crate::Result<ResultSet> {
        self.context.execute_query(statement).await
    }

    /// Reads rows from the database using key lookups and scans, as a simple key/value style alternative to execute_query.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::Spanner;
    /// # use google_cloud_spanner::{KeySet, ReadRequest};
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
    pub async fn execute_read<T: Into<crate::read::ReadRequest>>(
        &self,
        read: T,
    ) -> crate::Result<ResultSet> {
        self.context.execute_read(read).await
    }
}

/// Executes an explicit `BeginTransaction` RPC on Spanner.
pub(crate) async fn execute_begin_transaction(
    client: &crate::database_client::DatabaseClient,
    session_name: String,
    options: crate::model::TransactionOptions,
    transaction_tag: Option<String>,
    channel_hint: usize,
    request_options: crate::RequestOptions,
    mutation_key: Option<crate::model::Mutation>,
) -> crate::Result<crate::model::Transaction> {
    let mut request = crate::model::BeginTransactionRequest::default()
        .set_session(session_name)
        .set_options(options)
        .set_or_clear_mutation_key(mutation_key);
    if let Some(tag) = transaction_tag {
        request = request
            .set_request_options(crate::model::RequestOptions::default().set_transaction_tag(tag));
    }

    client
        .spanner
        .begin_transaction(request, request_options, channel_hint)
        .await
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
    Failed(Arc<crate::Error>),
}

enum SelectorStatus {
    Ready(crate::model::TransactionSelector),
    Wait(std::sync::Arc<tokio::sync::Notify>),
}

impl ReadContextTransactionSelector {
    pub(crate) async fn selector(&self) -> crate::Result<crate::model::TransactionSelector> {
        match self {
            Self::Fixed(selector, _) => Ok(selector.clone()),
            Self::Lazy(_) => loop {
                match self.poll_selector_status()? {
                    SelectorStatus::Ready(selector) => return Ok(selector),
                    SelectorStatus::Wait(notify) => notify.notified().await,
                }
            },
        }
    }

    /// Inspects the current lazy selector state returning whether it is ready,
    /// failed, or needs to wait for the transaction to start.
    fn poll_selector_status(&self) -> crate::Result<SelectorStatus> {
        let Self::Lazy(lazy) = self else {
            unreachable!("poll_selector_status called on non-Lazy selector");
        };
        let mut guard = lazy.lock().expect("transaction state mutex poisoned");

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
            // Note: Failed will only be reached if the following happens:
            // 1. The first query fails and the transaction falls back to an explicit BeginTransaction RPC.
            // 2. The BeginTransaction RPC fails. This is the error that will be returned to all the waiting queries.
            TransactionState::Failed(err) => {
                let error = if let Some(status) = err.status() {
                    crate::Error::service(status.clone())
                } else {
                    crate::error::internal_error(format!("Transaction failed to start: {}", err))
                };
                Err(error)
            }
            // Transaction is starting. Wait until a transaction ID is returned.
            TransactionState::Starting(_, notify) => Ok(SelectorStatus::Wait(Arc::clone(notify))),
            TransactionState::Started(_, _) | TransactionState::NotStarted(_) => unreachable!(),
        }
    }
}

pub(crate) struct ExplicitBeginParams {
    pub(crate) client: crate::database_client::DatabaseClient,
    pub(crate) session_name: String,
    pub(crate) transaction_tag: Option<String>,
    pub(crate) channel_hint: usize,
    pub(crate) request_options: crate::RequestOptions,
    pub(crate) is_stream_fallback: bool,
    pub(crate) precommit_token_tracker: crate::precommit::PrecommitTokenTracker,
    pub(crate) mutation_key: Option<crate::model::Mutation>,
}

impl ReadContextTransactionSelector {
    /// Explicitly begins a transaction if the transaction selector is a `Lazy`
    /// selector and the transaction has not yet been started. This is used by
    /// the client to force the start of a transaction if the first statement
    /// failed.
    pub(crate) async fn begin_explicitly(&self, params: ExplicitBeginParams) -> crate::Result<()> {
        let Self::Lazy(lazy) = self else {
            return Ok(());
        };

        enum FallbackAction {
            Begin(
                crate::model::TransactionOptions,
                Option<Arc<tokio::sync::Notify>>,
            ),
            Wait(Arc<tokio::sync::Notify>),
            None,
        }

        let action = {
            let mut guard = lazy
                .lock()
                .map_err(|_| internal_error("transaction state mutex poisoned"))?;
            match &*guard {
                TransactionState::NotStarted(options) => {
                    // The transaction has not started yet. This thread becomes the "leader"
                    // and transitions the state to Starting before performing the BeginTransaction RPC.
                    let options = options.clone();
                    let notify = Arc::new(tokio::sync::Notify::new());
                    *guard = TransactionState::Starting(options.clone(), Arc::clone(&notify));
                    FallbackAction::Begin(options, Some(notify))
                }
                TransactionState::Starting(options, notify) => {
                    // The transaction is already in the process of starting. If this call originated from
                    // an explicit begin request (`is_stream_fallback = false`), this thread is a follower
                    // and must wait for the leader. If this call originated from a stream resume fallback
                    // (`is_stream_fallback = true`), this thread is the stream leader whose initial query failed,
                    // and it must proceed with an explicit BeginTransaction RPC.
                    if !params.is_stream_fallback {
                        FallbackAction::Wait(Arc::clone(notify))
                    } else {
                        FallbackAction::Begin(options.clone(), Some(Arc::clone(notify)))
                    }
                }
                TransactionState::Started(_, _) | TransactionState::Failed(_) => {
                    // The transaction has already reached a terminal state (Started or Failed).
                    // No further action is needed in this explicit begin attempt.
                    FallbackAction::None
                }
            }
        };

        let (options, notify_opt) = match action {
            FallbackAction::None => return Ok(()),
            FallbackAction::Wait(notify) => {
                notify.notified().await;
                return Ok(());
            }
            FallbackAction::Begin(opts, notif) => (opts, notif),
        };

        // Only the leader thread will reach this point to perform the explicit begin.
        // Waiters are blocked in `poll_selector_status` waiting for the result,
        // and already completed states return early above.
        let response = match execute_begin_transaction(
            &params.client,
            params.session_name,
            options,
            params.transaction_tag,
            params.channel_hint,
            params.request_options,
            params.mutation_key,
        )
        .await
        {
            Ok(r) => r,
            Err(e) => {
                let mut guard = lazy.lock().expect("transaction state mutex poisoned");
                let error = Arc::new(e);
                *guard = TransactionState::Failed(Arc::clone(&error));
                // Release the lock and notify all the waiting queries that
                // the transaction has failed.
                drop(guard);
                if let Some(notify) = notify_opt {
                    notify.notify_waiters();
                }

                let return_error = if let Some(status) = error.status() {
                    crate::Error::service(status.clone())
                } else {
                    crate::error::internal_error(format!("Transaction failed to start: {}", error))
                };
                return Err(return_error);
            }
        };

        self.update(response.id, response.read_timestamp)?;
        params
            .precommit_token_tracker
            .update(response.precommit_token);

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
        let Self::Lazy(lazy) = self else {
            return Ok(());
        };
        let mut guard = lazy.lock().expect("transaction state mutex poisoned");

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
        use crate::model::transaction_selector::Selector;
        match self {
            Self::Fixed(selector, _) => {
                if let Some(Selector::Id(id)) = &selector.selector {
                    return Ok(Some(id.clone()));
                }
            }
            Self::Lazy(lazy) => {
                let guard = lazy
                    .lock()
                    .map_err(|_| internal_error("transaction state mutex poisoned"))?;
                if let TransactionState::Started(selector, _) = &*guard {
                    if let Some(Selector::Id(id)) = &selector.selector {
                        return Ok(Some(id.clone()));
                    }
                }
            }
        }
        Ok(None)
    }

    /// Returns whether the transaction selector is currently in the `Starting` state.
    pub(crate) fn is_starting(&self) -> crate::Result<bool> {
        match self {
            Self::Lazy(lazy) => {
                let guard = lazy
                    .lock()
                    .map_err(|_| internal_error("transaction state mutex poisoned"))?;
                Ok(matches!(&*guard, TransactionState::Starting(_, _)))
            }
            _ => Ok(false),
        }
    }

    /// Resets the selector state from `Starting` back to `NotStarted`.
    ///
    /// This is used during stream resume fallbacks when the first query stream
    /// fails before yielding a transaction ID. It unlocks any parked waiters
    /// allowing them (or the retry attempt) to include the begin option again.
    /// Only one of the waiters will win that 'race' and include a new
    /// BeginTransaction option. All the others will continue to wait.
    pub(crate) fn maybe_reset_starting(&self) {
        let Self::Lazy(lazy) = self else {
            return;
        };

        let mut guard = lazy.lock().expect("transaction state mutex poisoned");
        if let TransactionState::Starting(options, notify) = &*guard {
            let options = options.clone();
            let notify = Arc::clone(notify);
            *guard = TransactionState::NotStarted(options);
            drop(guard);
            notify.notify_waiters();
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
            Self::Lazy(lazy) => {
                let guard = lazy.lock().expect("transaction state mutex poisoned");
                if let TransactionState::Started(_, timestamp) = &*guard {
                    *timestamp
                } else {
                    None
                }
            }
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ReadContext {
    pub(crate) session_name: String,
    pub(crate) client: DatabaseClient,
    pub(crate) transaction_selector: ReadContextTransactionSelector,
    pub(crate) precommit_token_tracker: PrecommitTokenTracker,
    pub(crate) transaction_tag: Option<String>,
    pub(crate) channel_hint: usize,
    pub(crate) begin_transaction_request_options: Option<crate::RequestOptions>,
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
    /// selector is still in the `Lazy(NotStarted)` state. This is used as a
    /// fallback mechanism when an initial implicit begin attempt failed.
    pub(crate) async fn begin_explicitly_if_not_started(
        &self,
        fallback_options: crate::RequestOptions,
        is_stream_fallback: bool,
        mutation_key: Option<crate::model::Mutation>,
    ) -> crate::Result<bool> {
        let ReadContextTransactionSelector::Lazy(lazy) = &self.transaction_selector else {
            return Ok(false);
        };
        let is_started = matches!(&*lazy.lock().unwrap(), TransactionState::Started(_, _));
        if is_started {
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
                channel_hint: self.channel_hint,
                request_options: options,
                is_stream_fallback,
                precommit_token_tracker: self.precommit_token_tracker.clone(),
                mutation_key,
            })
            .await?;
        Ok(true)
    }
}

/// Merges the configured fields from a `source` `RequestOptions` into a `destination` `RequestOptions`.
/// Configured options in `source` will override those in `destination`.
fn merge_request_options(
    mut destination: crate::RequestOptions,
    source: Option<&crate::RequestOptions>,
) -> crate::RequestOptions {
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
    if let Some(src_headers) = source.get_extension::<http::HeaderMap>() {
        let mut dest_headers = destination
            .get_extension::<http::HeaderMap>()
            .cloned()
            .unwrap_or_default();
        for (name, value) in src_headers.iter() {
            dest_headers.insert(name.clone(), value.clone());
        }
        destination = destination.insert_extension(dest_headers);
    }
    destination
}

/// Helper macro to execute a streaming SQL or streaming read RPC with retry logic.
macro_rules! execute_stream_with_retry {
    ($self:expr, $request:ident, $gax_options:ident, $rpc_method:ident, $operation_variant:path) => {{
        let stream = match $self
            .client
            .spanner
            .$rpc_method($request.clone(), $gax_options.clone(), $self.channel_hint)
            .send()
            .await
        {
            Ok(s) => s,
            Err(e) => {
                if is_aborted(&e) {
                    return Err(e);
                }
                if $self
                    .begin_explicitly_if_not_started($gax_options.clone(), true, None)
                    .await?
                {
                    $request.transaction = Some($self.transaction_selector.selector().await?);
                    $self
                        .client
                        .spanner
                        .$rpc_method($request.clone(), $gax_options.clone(), $self.channel_hint)
                        .send()
                        .await?
                } else {
                    return Err(e);
                }
            }
        };

        ResultSet::create(ResultSetParams {
            stream,
            transaction_selector: Some($self.transaction_selector.clone()),
            precommit_token_tracker: $self.precommit_token_tracker.clone(),
            client: $self.client.clone(),
            session_name: $self.session_name.clone(),
            transaction_tag: $self.transaction_tag.clone(),
            operation: $operation_variant($request),
            channel_hint: $self.channel_hint,
            gax_options: $gax_options,
        })
        .await
    }};
}

impl ReadContext {
    pub(crate) async fn execute_query<T: Into<Statement>>(
        &self,
        statement: T,
    ) -> crate::Result<ResultSet> {
        let statement = statement.into();
        let gax_options = statement.gax_options().clone();
        let mut request = statement
            .into_request()
            .set_session(self.session_name.clone())
            .set_transaction(self.transaction_selector.selector().await?);
        request.request_options = self.amend_request_options(request.request_options);

        execute_stream_with_retry!(
            self,
            request,
            gax_options,
            execute_streaming_sql,
            StreamOperation::Query
        )
    }

    pub(crate) async fn execute_read<T: Into<crate::read::ReadRequest>>(
        &self,
        read: T,
    ) -> crate::Result<ResultSet> {
        let read = read.into();
        let gax_options = read.gax_options.clone();
        let mut request = read
            .into_request()
            .set_session(self.session_name.clone())
            .set_transaction(self.transaction_selector.selector().await?);
        request.request_options = self.amend_request_options(request.request_options);

        execute_stream_with_retry!(
            self,
            request,
            gax_options,
            streaming_read,
            StreamOperation::Read
        )
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::Statement;
    use crate::result_set::tests::adapt;
    use crate::result_set::tests::string_val;
    use crate::value::Value;
    use gaxi::grpc::tonic::{self, Code, Response, Status};
    use google_cloud_gax::error::rpc::Code as GaxCode;
    use google_cloud_gax::exponential_backoff::ExponentialBackoff;
    use google_cloud_gax::retry_policy::NeverRetry;
    use google_cloud_test_macros::tokio_test_no_panics;
    use http::{HeaderMap, HeaderName, HeaderValue};
    use mock_v1::transaction_selector::Selector;
    use spanner_grpc_mock::MockSpanner;
    use spanner_grpc_mock::google::spanner::v1 as mock_v1;
    use std::sync::mpsc::channel as std_channel;
    use std::sync::{Arc, Mutex as StdMutex};
    use tokio::sync::oneshot::channel as oneshot_channel;
    use tokio::sync::{Barrier, Mutex, Notify, mpsc};

    #[test]
    fn auto_traits() {
        static_assertions::assert_impl_all!(SingleUseReadOnlyTransactionBuilder: Send, Sync);
        static_assertions::assert_impl_all!(SingleUseReadOnlyTransaction: Send, Sync, std::fmt::Debug);
        static_assertions::assert_impl_all!(MultiUseReadOnlyTransactionBuilder: Send, Sync);
        static_assertions::assert_impl_all!(MultiUseReadOnlyTransaction: Send, Sync, std::fmt::Debug);
        static_assertions::assert_impl_all!(ReadContext: Send, Sync, std::fmt::Debug);
    }

    pub(crate) fn create_session_mock() -> spanner_grpc_mock::MockSpanner {
        let mut mock = spanner_grpc_mock::MockSpanner::new();
        mock.expect_create_session().once().returning(|_| {
            Ok(Response::new(mock_v1::Session {
                name: "projects/p/instances/i/databases/d/sessions/123".to_string(),
                ..Default::default()
            }))
        });
        mock
    }

    fn setup_select1() -> spanner_grpc_mock::google::spanner::v1::PartialResultSet {
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
            .with_timestamp_bound(crate::timestamp_bound::TimestampBound::max_staleness(
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
        use crate::Statement;
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
        use crate::Statement;
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
            .with_begin_transaction_option(BeginTransactionOption::ExplicitBegin)
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
        use crate::Statement;
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
            .with_begin_transaction_option(BeginTransactionOption::InlineBegin)
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
        use crate::value::Value;
        use crate::{KeySet, ReadRequest};

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
        use crate::value::Value;
        use crate::{KeySet, ReadRequest};
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
            .with_begin_transaction_option(BeginTransactionOption::InlineBegin)
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
    async fn inline_begin_failure_retry_success() -> anyhow::Result<()> {
        use crate::value::Value;
        use gaxi::grpc::tonic::Status;
        use tonic::Response;

        let mut mock = create_session_mock();
        let mut seq = mockall::Sequence::new();

        // 1. Initial query fails
        mock.expect_execute_streaming_sql()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|_| Err(Status::internal("Internal error")));

        // 2. Explicit begin transaction succeeds
        mock.expect_begin_transaction()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|req| {
                let req = req.into_inner();
                assert_eq!(
                    req.session,
                    "projects/p/instances/i/databases/d/sessions/123"
                );
                // Return a transaction with ID
                Ok(Response::new(mock_v1::Transaction {
                    id: vec![7, 8, 9],
                    read_timestamp: Some(prost_types::Timestamp {
                        seconds: 123456789,
                        nanos: 0,
                    }),
                    ..Default::default()
                }))
            });

        // 3. Retry of the query succeeds
        mock.expect_execute_streaming_sql()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|req| {
                let req = req.into_inner();
                // Ensure it uses the new transaction ID
                match req.transaction.unwrap().selector.unwrap() {
                    mock_v1::transaction_selector::Selector::Id(id) => {
                        assert_eq!(id, vec![7, 8, 9]);
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
            .with_begin_transaction_option(BeginTransactionOption::InlineBegin)
            .build()
            .await?;

        let mut rs = tx
            .execute_query(Statement::builder("SELECT 1").build())
            .await?;

        let row = rs
            .next()
            .await
            .ok_or_else(|| anyhow::anyhow!("Expected a row but stream cleanly exhausted"))??;
        assert_eq!(
            row.raw_values(),
            [Value(string_val("1"))],
            "The parsed row value safely matched the underlying stream chunk"
        );

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn inline_begin_failure_retry_failure() -> anyhow::Result<()> {
        use gaxi::grpc::tonic::Status;
        use tonic::Response;

        let mut mock = create_session_mock();
        let mut seq = mockall::Sequence::new();

        // 1. Initial query fails
        mock.expect_execute_streaming_sql()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|_| Err(Status::internal("Internal error first")));

        // 2. Explicit begin transaction succeeds
        mock.expect_begin_transaction()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|_| {
                Ok(Response::new(mock_v1::Transaction {
                    id: vec![7, 8, 9],
                    read_timestamp: Some(prost_types::Timestamp {
                        seconds: 123456789,
                        nanos: 0,
                    }),
                    ..Default::default()
                }))
            });

        // 3. Retry of the query fails again
        mock.expect_execute_streaming_sql()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|_| Err(Status::internal("Internal error second")));

        let (db_client, _server) = setup_db_client(mock).await;
        let tx = db_client
            .read_only_transaction()
            .with_begin_transaction_option(BeginTransactionOption::InlineBegin)
            .build()
            .await?;

        let rs_result = tx
            .execute_query(Statement::builder("SELECT 1").build())
            .await;

        assert!(
            rs_result.is_err(),
            "The failed execution bubbled upwards securely"
        );
        let err_str = rs_result.unwrap_err().to_string();
        assert!(
            err_str.contains("Internal error second"),
            "Secondary error message accurately propagates: {}",
            err_str
        );

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn inline_begin_failure_fallback_rpc_fails() -> anyhow::Result<()> {
        use gaxi::grpc::tonic::Status;

        let mut mock = create_session_mock();
        let mut seq = mockall::Sequence::new();

        // 1. Initial query fails
        mock.expect_execute_streaming_sql()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|_| Err(Status::internal("Internal error query")));

        // 2. Explicit begin transaction fails
        mock.expect_begin_transaction()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|_| Err(Status::internal("Internal error begin tx")));

        let (db_client, _server) = setup_db_client(mock).await;
        let tx = db_client
            .read_only_transaction()
            .with_begin_transaction_option(BeginTransactionOption::InlineBegin)
            .build()
            .await?;

        let rs_result = tx
            .execute_query(Statement::builder("SELECT 1").build())
            .await;

        assert!(
            rs_result.is_err(),
            "The explicitly errored fallback boot securely propagated outwards"
        );
        let err_str = rs_result.unwrap_err().to_string();
        assert!(
            err_str.contains("Internal error begin tx"),
            "Natively propagated specific BeginTx bounds: {}",
            err_str
        );

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn inline_begin_read_failure_retry_success() -> anyhow::Result<()> {
        use crate::value::Value;
        use crate::{KeySet, ReadRequest};
        use gaxi::grpc::tonic::Status;
        use tonic::Response;

        let mut mock = create_session_mock();
        let mut seq = mockall::Sequence::new();

        // 1. Initial read fails
        mock.expect_streaming_read()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|_| Err(Status::internal("Internal error")));

        // 2. Explicit begin transaction succeeds
        mock.expect_begin_transaction()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|_| {
                Ok(Response::new(mock_v1::Transaction {
                    id: vec![7, 8, 9],
                    read_timestamp: None,
                    ..Default::default()
                }))
            });

        // 3. Retry of the read succeeds
        mock.expect_streaming_read()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|req| {
                let req = req.into_inner();
                // Ensure it uses the new transaction ID
                match req.transaction.unwrap().selector.unwrap() {
                    mock_v1::transaction_selector::Selector::Id(id) => {
                        assert_eq!(id, vec![7, 8, 9]);
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
            .with_begin_transaction_option(BeginTransactionOption::InlineBegin)
            .build()
            .await?;

        let read = ReadRequest::builder("Users", vec!["Id", "Name"])
            .with_keys(KeySet::all())
            .build();
        let mut rs = tx.execute_read(read).await?;

        let row = rs
            .next()
            .await
            .ok_or_else(|| anyhow::anyhow!("Expected a row uniquely returned"))??;
        assert_eq!(
            row.raw_values(),
            [Value(string_val("1"))],
            "The macro correctly unpacked read arrays seamlessly"
        );

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn single_use_query_send_error_returns_immediately() -> anyhow::Result<()> {
        use crate::Statement;
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
        use crate::Statement;
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
            .with_begin_transaction_option(BeginTransactionOption::InlineBegin)
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
                match req.transaction.unwrap().selector.unwrap() {
                    Selector::Begin(_) => {}
                    _ => panic!("Expected Selector::Begin for first query"),
                }
                let rx = rx_receiver
                    .try_lock()
                    .expect("mutex poisoned")
                    .take()
                    .unwrap();
                Ok(Response::from(rx))
            });

        // 2. The other queries: should include populated Selector::Id
        mock.expect_execute_streaming_sql()
            .times(2)
            .in_sequence(&mut seq)
            .returning(move |req| {
                let req = req.into_inner();
                match req.transaction.unwrap().selector.unwrap() {
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
            .with_begin_transaction_option(BeginTransactionOption::InlineBegin)
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

        // 2. Fallback BeginTransaction RPC fails
        mock.expect_begin_transaction()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|_| {
                Err(gaxi::grpc::tonic::Status::internal(
                    "Fallback BeginTransaction failed",
                ))
            });

        // The other queries will never be executed.
        mock.expect_execute_streaming_sql().times(0).returning(|_| {
            panic!("Other queries should not launch after failure to start the transaction")
        });

        let (db_client, _server) = setup_db_client(mock).await;
        let tx = db_client
            .read_only_transaction()
            .with_begin_transaction_option(BeginTransactionOption::InlineBegin)
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

        assert!(
            err1.contains("Fallback BeginTransaction failed"),
            "err1: {}",
            err1
        );
        assert!(
            err2.contains("Fallback BeginTransaction failed"),
            "err2: {}",
            err2
        );
        assert!(
            err3.contains("Fallback BeginTransaction failed"),
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
            .with_begin_transaction_option(BeginTransactionOption::InlineBegin)
            .build()
            .await?;
        let tx = Arc::new(tx);

        let handle1_tx = Arc::clone(&tx);
        let handle1 = tokio::spawn(async move {
            let mut rs = handle1_tx
                .execute_query(Statement::builder("SELECT 1").build())
                .await?;
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

        // 2. Fallback BeginTransaction RPC also fails.
        mock.expect_begin_transaction()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|_| Err(Status::internal("Fallback BeginTransaction failed")));

        // Any further attempts would panic because we haven't mocked them.
        mock.expect_execute_streaming_sql().never();

        let (db_client, _server) = setup_db_client(mock).await;
        let tx = db_client
            .read_only_transaction()
            .with_begin_transaction_option(BeginTransactionOption::InlineBegin)
            .build()
            .await?;

        // First query: triggers the failure and transitions the state to Failed.
        let err1 = tx
            .execute_query(Statement::builder("SELECT 1").build())
            .await
            .expect_err("First query should fail");
        assert!(
            err1.to_string()
                .contains("Fallback BeginTransaction failed")
        );

        // Second query: starts AFTER the failure is already cached.
        // It should immediately return the same error without invoking the mock server.
        let err2 = tx
            .execute_query(Statement::builder("SELECT 1").build())
            .await
            .expect_err("Late query should fail immediately");
        assert!(
            err2.to_string()
                .contains("Fallback BeginTransaction failed")
        );

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn execute_concurrent_reads_inline_begin() -> anyhow::Result<()> {
        use crate::{KeySet, ReadRequest};
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
            .with_begin_transaction_option(BeginTransactionOption::InlineBegin)
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
            .with_begin_transaction_option(BeginTransactionOption::InlineBegin)
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
        let mut seq = mockall::Sequence::new();

        // 1. First attempt fails transiently.
        mock.expect_execute_streaming_sql()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|_| Err(Status::new(Code::Unavailable, "Transient 1")));

        // 2. Fallback BeginTransaction succeeds.
        mock.expect_begin_transaction()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|_| {
                Ok(Response::new(mock_v1::Transaction {
                    id: vec![7, 8, 9],
                    ..Default::default()
                }))
            });

        // 3. The manual retry of the query (which happens after explicit begin fallback).
        mock.expect_execute_streaming_sql()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|_| {
                let (tx, rx) = mpsc::channel(1);
                tx.try_send(Ok(setup_select1()))
                    .expect("send should succeed");
                Ok(Response::from(rx))
            });

        let (db_client, _server) = setup_db_client(mock).await;
        let tx = db_client
            .read_only_transaction()
            .with_begin_transaction_option(BeginTransactionOption::InlineBegin)
            .build()
            .await?;

        let mut rs = tx
            .execute_query(Statement::builder("SELECT 1").build())
            .await?;
        assert!(rs.next().await.is_some());
        assert!(rs.next().await.is_none());

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
                .with_begin_transaction_option(BeginTransactionOption::InlineBegin)
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
            .with_begin_transaction_option(option)
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
            .with_begin_transaction_option(BeginTransactionOption::ExplicitBegin)
            .with_begin_retry_policy(NeverRetry)
            .build()
            .await;

        assert!(res.is_err(), "should fail immediately without retry");
        let err = res.unwrap_err();
        assert_eq!(err.status().expect("status").code, GaxCode::Unavailable);

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn read_only_transaction_lazy_begin_fallback_never_retry() -> anyhow::Result<()> {
        let mut mock = MockSpanner::new();
        let mut sequence = mockall::Sequence::new();

        // 1. First query execution fails with Unavailable (transient error)
        mock.expect_execute_streaming_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(|_| Err(tonic::Status::unavailable("transient error")));

        // 2. Fallback explicit BeginTransaction is executed exactly once and fails
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

        let transaction = db_client
            .read_only_transaction()
            .with_begin_transaction_option(BeginTransactionOption::InlineBegin)
            .with_begin_retry_policy(NeverRetry)
            .build()
            .await?;

        let stmt = Statement::builder("SELECT 1").build();
        let res = transaction.execute_query(stmt).await;

        assert!(
            res.is_err(),
            "should fail immediately during fallback without retrying the fallback RPC"
        );
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
            .with_begin_transaction_option(BeginTransactionOption::ExplicitBegin)
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

    #[tokio_test_no_panics]
    async fn read_only_transaction_lazy_begin_fallback_uses_statement_options_when_unconfigured()
    -> anyhow::Result<()> {
        let mut mock = MockSpanner::new();
        let mut sequence = mockall::Sequence::new();

        // 1. First query execution fails with Unavailable (transient error)
        mock.expect_execute_streaming_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(|_| Err(tonic::Status::unavailable("transient error")));

        // 2. Fallback explicit BeginTransaction is executed. Since the transaction itself has no
        // custom options, it must inherit the statement options, which set attempt_timeout to 5 seconds.
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

        // 3. Query is retried with the successfully obtained transaction ID, succeeding this time
        mock.expect_execute_streaming_sql()
            .once()
            .in_sequence(&mut sequence)
            .withf(|req| {
                matches!(
                    req.get_ref()
                        .transaction
                        .as_ref()
                        .and_then(|t| t.selector.as_ref()),
                    Some(mock_v1::transaction_selector::Selector::Id(id)) if id == &vec![42]
                )
            })
            .returning(|_| {
                let mut result_set_partial = setup_select1();
                result_set_partial
                    .metadata
                    .as_mut()
                    .expect("metadata should be present")
                    .transaction = Some(mock_v1::Transaction {
                    id: vec![42],
                    ..Default::default()
                });
                Ok(gaxi::grpc::tonic::Response::from(adapt([Ok(
                    result_set_partial,
                )])))
            });

        mock.expect_create_session().returning(|_| {
            Ok(Response::new(mock_v1::Session {
                name: "session".to_string(),
                multiplexed: true,
                ..Default::default()
            }))
        });

        let (db_client, _server) = setup_db_client(mock).await;

        let transaction = db_client
            .read_only_transaction()
            .with_begin_transaction_option(BeginTransactionOption::InlineBegin)
            .build()
            .await?;

        let mut stmt_opts = crate::RequestOptions::default();
        stmt_opts.set_attempt_timeout(Duration::from_secs(5));
        let stmt = Statement::builder("SELECT 1")
            .build()
            .with_gax_options(stmt_opts);

        let mut rs = transaction.execute_query(stmt).await?;
        let row = rs.next().await.expect("has row")?;
        assert_eq!(row.raw_values(), [Value(string_val("1"))]);

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn read_only_transaction_lazy_begin_fallback_merges_custom_options() -> anyhow::Result<()>
    {
        let mut mock = MockSpanner::new();
        let mut sequence = mockall::Sequence::new();

        // 1. First query execution fails with Unavailable (transient error)
        mock.expect_execute_streaming_sql()
            .once()
            .in_sequence(&mut sequence)
            .returning(|_| Err(tonic::Status::unavailable("transient error")));

        // 2. Fallback explicit BeginTransaction must have BOTH:
        // - attempt_timeout of 5 seconds (inherited from statement's options)
        // - retry_policy of NeverRetry (inherited from transaction's begin options)
        // If it did not merge correctly, the timeout header would be missing, or it would retry.
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
            .returning(|_| Err(tonic::Status::unavailable("transient error")));

        mock.expect_create_session().returning(|_| {
            Ok(Response::new(mock_v1::Session {
                name: "session".to_string(),
                multiplexed: true,
                ..Default::default()
            }))
        });

        let (db_client, _server) = setup_db_client(mock).await;

        let transaction = db_client
            .read_only_transaction()
            .with_begin_transaction_option(BeginTransactionOption::InlineBegin)
            .with_begin_retry_policy(NeverRetry)
            .build()
            .await?;

        let mut stmt_opts = crate::RequestOptions::default();
        stmt_opts.set_attempt_timeout(Duration::from_secs(5));
        let stmt = Statement::builder("SELECT 1")
            .build()
            .with_gax_options(stmt_opts);

        let res = transaction.execute_query(stmt).await;

        assert!(
            res.is_err(),
            "should fail immediately because of NeverRetry"
        );
        let err = res.unwrap_err();
        assert_eq!(err.status().expect("status").code, GaxCode::Unavailable);

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
                .unwrap()
                .to_str()
                .unwrap(),
            "true"
        );
        assert_eq!(
            merged_headers
                .get("x-custom-header")
                .unwrap()
                .to_str()
                .unwrap(),
            "custom-value"
        );
    }
}
