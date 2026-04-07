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
use crate::model::TransactionOptions;
use crate::model::transaction_options::ReadOnly;
use crate::precommit::PrecommitTokenTracker;
use crate::result_set::{ResultSet, StreamOperation};
use crate::statement::Statement;
use crate::timestamp_bound::TimestampBound;
use crate::transaction_retry_policy::is_aborted;
use std::sync::{Arc, Mutex};
use tokio::sync::Notify;

/// A builder for [SingleUseReadOnlyTransaction].
///
/// # Example
/// ```
/// # use google_cloud_spanner::client::Spanner;
/// # use google_cloud_spanner::client::TimestampBound;
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
    /// # use google_cloud_spanner::client::TimestampBound;
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

        SingleUseReadOnlyTransaction {
            context: ReadContext {
                client: self.client,
                transaction_selector: ReadContextTransactionSelector::Fixed(
                    transaction_selector,
                    None,
                ),
                precommit_token_tracker: PrecommitTokenTracker::new_noop(),
                transaction_tag: None,
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
/// # use google_cloud_spanner::client::Statement;
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
    /// # use google_cloud_spanner::client::Statement;
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
    /// # use google_cloud_spanner::client::{Spanner, ReadRequest, KeySet};
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
/// # use google_cloud_spanner::client::TimestampBound;
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
}

impl MultiUseReadOnlyTransactionBuilder {
    pub(crate) fn new(client: DatabaseClient) -> Self {
        Self {
            client,
            timestamp_bound: None,
            begin_transaction_option: BeginTransactionOption::InlineBegin,
        }
    }

    /// Sets the option for how to start a transaction.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::{Spanner, BeginTransactionOption};
    /// # use google_cloud_spanner::client::Statement;
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
    /// transaction. Setting this option to `ExplicitBegin` can be beneficial for specific transaction shapes:
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

    /// Sets the timestamp bound for the read-only transaction.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::Spanner;
    /// # use google_cloud_spanner::client::TimestampBound;
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
        options: TransactionOptions,
    ) -> crate::Result<ReadContextTransactionSelector> {
        let response =
            execute_begin_transaction(&self.client, options, /* transaction_tag= */ None).await?;

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

        let selector = match self.begin_transaction_option {
            BeginTransactionOption::ExplicitBegin => self.begin(options).await?,
            BeginTransactionOption::InlineBegin => ReadContextTransactionSelector::Lazy(Arc::new(
                Mutex::new(TransactionState::NotStarted(options)),
            )),
        };

        Ok(MultiUseReadOnlyTransaction {
            context: ReadContext {
                client: self.client,
                transaction_selector: selector,
                precommit_token_tracker: PrecommitTokenTracker::new_noop(),
                transaction_tag: None,
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
/// # use google_cloud_spanner::client::Statement;
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
    /// # use google_cloud_spanner::client::Statement;
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
    /// # use google_cloud_spanner::client::{Spanner, ReadRequest, KeySet};
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
    options: crate::model::TransactionOptions,
    transaction_tag: Option<String>,
) -> crate::Result<crate::model::Transaction> {
    let request = crate::model::BeginTransactionRequest::default()
        .set_session(client.session.name.clone())
        .set_options(options)
        .set_or_clear_request_options(
            transaction_tag
                .map(|tag| crate::model::RequestOptions::default().set_transaction_tag(tag)),
        );

    // TODO(#4972): make request options configurable
    client
        .spanner
        .begin_transaction(request, crate::RequestOptions::default())
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

        // If the transaction has not started, extract options and proceed to transition.
        let pending_options = if let TransactionState::NotStarted(options) = &*guard {
            Some(options.clone())
        } else {
            None
        };
        if let Some(options) = pending_options {
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

    /// Explicitly begins a transaction if the transaction selector is a `Lazy`
    /// selector and the transaction has not yet been started. This is used by
    /// the client to force the start of a transaction if the first statement
    /// failed.
    pub(crate) async fn begin_explicitly(
        &self,
        client: &crate::database_client::DatabaseClient,
    ) -> crate::Result<()> {
        let Self::Lazy(lazy) = self else {
            return Ok(());
        };

        let (options, notify_opt) = {
            let guard = lazy.lock().expect("transaction state mutex poisoned");
            match &*guard {
                // This should never happen in the current implementation.
                TransactionState::NotStarted(_) => {
                    return Err(crate::error::internal_error(
                        "explicit begin with NotStarted state is currently unsupported",
                    ));
                }
                TransactionState::Starting(options, notify) => {
                    (options.clone(), Some(Arc::clone(notify)))
                }
                TransactionState::Started(_, _) | TransactionState::Failed(_) => return Ok(()),
            }
        };

        let response =
            match execute_begin_transaction(client, options, /* transaction_tag= */ None).await {
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
                        crate::error::internal_error(format!(
                            "Transaction failed to start: {}",
                            error
                        ))
                    };
                    return Err(return_error);
                }
            };

        self.update(response.id, response.read_timestamp)?;

        Ok(())
    }

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
            let previous_state = std::mem::replace(
                &mut *guard,
                TransactionState::Started(
                    crate::model::TransactionSelector::default().set_id(id),
                    timestamp,
                ),
            );
            drop(guard);

            // Notify all queries that are waiting for the transaction.
            if let TransactionState::Starting(_, notify) = previous_state {
                notify.notify_waiters();
            }
            Ok(())
        } else {
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
    pub(crate) fn get_id_no_wait(&self) -> Option<bytes::Bytes> {
        use crate::generated::gapic_dataplane::model::transaction_selector::Selector;
        match self {
            Self::Fixed(selector, _) => {
                if let Some(Selector::Id(id)) = &selector.selector {
                    return Some(id.clone());
                }
            }
            Self::Lazy(lazy) => {
                let guard = lazy.lock().expect("transaction state mutex poisoned");
                if let TransactionState::Started(selector, _) = &*guard {
                    if let Some(Selector::Id(id)) = &selector.selector {
                        return Some(id.clone());
                    }
                }
            }
        }
        None
    }

    /// Resets the selector state from `Starting` back to `NotStarted`.
    ///
    /// This is used during stream resume fallbacks when the first query stream
    /// fails before yielding a transaction ID. It unlocks any parked waiters
    /// allowing them (or the retry attempt) to include the begin option again.
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
    pub(crate) client: DatabaseClient,
    pub(crate) transaction_selector: ReadContextTransactionSelector,
    pub(crate) precommit_token_tracker: PrecommitTokenTracker,
    pub(crate) transaction_tag: Option<String>,
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
    async fn begin_explicitly_if_not_started(&self) -> crate::Result<bool> {
        let ReadContextTransactionSelector::Lazy(lazy) = &self.transaction_selector else {
            return Ok(false);
        };
        let is_started = matches!(&*lazy.lock().unwrap(), TransactionState::Started(_, _));
        if is_started {
            return Ok(false);
        }

        self.transaction_selector
            .begin_explicitly(&self.client)
            .await?;
        Ok(true)
    }
}

/// Helper macro to execute a streaming SQL or streaming read RPC with retry logic.
macro_rules! execute_stream_with_retry {
    ($self:expr, $request:ident, $rpc_method:ident, $operation_variant:path) => {{
        let stream = match $self
            .client
            .spanner
            // TODO(#4972): make request options configurable
            .$rpc_method($request.clone(), crate::RequestOptions::default())
            .send()
            .await
        {
            Ok(s) => s,
            Err(e) => {
                if is_aborted(&e) {
                    return Err(e);
                }
                if $self.begin_explicitly_if_not_started().await? {
                    $request.transaction = Some($self.transaction_selector.selector().await?);
                    $self
                        .client
                        .spanner
                        // TODO(#4972): make request options configurable
                        .$rpc_method($request.clone(), crate::RequestOptions::default())
                        .send()
                        .await?
                } else {
                    return Err(e);
                }
            }
        };

        Ok(ResultSet::new(
            stream,
            Some($self.transaction_selector.clone()),
            $self.precommit_token_tracker.clone(),
            $self.client.clone(),
            $operation_variant($request),
        ))
    }};
}

impl ReadContext {
    pub(crate) async fn execute_query<T: Into<Statement>>(
        &self,
        statement: T,
    ) -> crate::Result<ResultSet> {
        let mut request = statement
            .into()
            .into_request()
            .set_session(self.client.session.name.clone())
            .set_transaction(self.transaction_selector.selector().await?);
        request.request_options = self.amend_request_options(request.request_options);

        execute_stream_with_retry!(self, request, execute_streaming_sql, StreamOperation::Query)
    }

    pub(crate) async fn execute_read<T: Into<crate::read::ReadRequest>>(
        &self,
        read: T,
    ) -> crate::Result<ResultSet> {
        let mut request = read
            .into()
            .into_request()
            .set_session(self.client.session.name.clone())
            .set_transaction(self.transaction_selector.selector().await?);
        request.request_options = self.amend_request_options(request.request_options);

        execute_stream_with_retry!(self, request, streaming_read, StreamOperation::Read)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::client::Statement;
    use crate::result_set::tests::string_val;
    use crate::value::Value;
    use gaxi::grpc::tonic::{self, Code, Response, Status};
    use mock_v1::transaction_selector::Selector;
    use spanner_grpc_mock::google::spanner::v1 as mock_v1;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::task::{Context, Poll};
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

    #[tokio::test]
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

    #[tokio::test]
    async fn execute_single_query() {
        use super::super::result_set::tests::string_val;
        use crate::client::Statement;
        use crate::value::Value;

        let mut mock = create_session_mock();

        mock.expect_execute_streaming_sql().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(
                req.session,
                "projects/p/instances/i/databases/d/sessions/123"
            );
            assert_eq!(req.sql, "SELECT 1");

            Ok(tonic::Response::new(Box::pin(tokio_stream::iter(vec![
                Ok(setup_select1()),
            ]))))
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

    #[tokio::test]
    async fn execute_multi_query() {
        use super::super::result_set::tests::string_val;
        use crate::client::Statement;
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

                Ok(tonic::Response::new(Box::pin(tokio_stream::iter(vec![
                    Ok(setup_select1()),
                ]))))
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

    #[tokio::test]
    async fn execute_multi_query_inline_begin() -> anyhow::Result<()> {
        use super::super::result_set::tests::string_val;
        use crate::client::Statement;
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
                Ok(tonic::Response::new(Box::pin(tokio_stream::iter(vec![
                    Ok(rs),
                ]))))
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
                Ok(tonic::Response::new(Box::pin(tokio_stream::iter(vec![
                    Ok(setup_select1()),
                ]))))
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

    #[tokio::test]
    async fn execute_single_read() {
        use super::super::result_set::tests::string_val;
        use crate::client::{KeySet, ReadRequest};
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

            Ok(tonic::Response::new(Box::pin(tokio_stream::iter(vec![
                Ok(setup_select1()),
            ]))))
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

    #[tokio::test]
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
                Ok(Response::new(Box::pin(tokio_stream::iter(vec![Ok(
                    setup_select1(),
                )]))))
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

    #[tokio::test]
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

    #[tokio::test]
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

    #[tokio::test]
    async fn inline_begin_read_failure_retry_success() -> anyhow::Result<()> {
        use crate::client::{KeySet, ReadRequest};
        use crate::value::Value;
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
                Ok(Response::new(Box::pin(tokio_stream::iter(vec![Ok(
                    setup_select1(),
                )]))))
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

    #[tokio::test]
    async fn single_use_query_send_error_returns_immediately() -> anyhow::Result<()> {
        use crate::client::Statement;
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

    #[tokio::test]
    async fn inline_begin_already_started_query_send_error_returns_immediately()
    -> anyhow::Result<()> {
        use crate::client::Statement;
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
                Ok(tonic::Response::new(Box::pin(tokio_stream::iter(vec![
                    Ok(rs),
                ]))))
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

    /// A wrapper that implements `tokio_stream::Stream` for a `mpsc::Receiver`.
    /// Useful in mock setups to yield controlled streaming test responses.
    struct ReceiverStream<T>(mpsc::Receiver<T>);
    impl<T> tokio_stream::Stream for ReceiverStream<T> {
        type Item = T;
        fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<T>> {
            self.0.poll_recv(cx)
        }
    }

    #[tokio::test]
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
                Ok(Response::new(Box::pin(ReceiverStream(rx))))
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

                Ok(Response::new(Box::pin(tokio_stream::iter(vec![Ok(
                    setup_select1(),
                )]))))
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
        rs.metadata.as_mut().unwrap().transaction = Some(mock_v1::Transaction {
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
        assert_eq!(tx.read_timestamp().unwrap().seconds(), 987654321);

        Ok(())
    }

    #[tokio::test]
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
                    .unwrap();
                Ok(tonic::Response::new(Box::pin(ReceiverStream(rx))))
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
        let err1 = handle1.await?.unwrap_err().to_string();
        let err2 = handle2.await?.unwrap_err().to_string();
        let err3 = handle3.await?.unwrap_err().to_string();

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

    #[tokio::test]
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
                match req.transaction.unwrap().selector.unwrap() {
                    Selector::Begin(_) => {}
                    _ => panic!("Expected Selector::Begin for first query"),
                }
                let rx = rx_receiver
                    .try_lock()
                    .expect("mutex poisoned")
                    .take()
                    .unwrap();
                Ok(Response::new(Box::pin(ReceiverStream(rx))))
            });

        // 2. Task 1 restart query: should include Selector::Begin, since
        // it failed with a transient error.
        mock.expect_execute_streaming_sql()
            .times(1)
            .in_sequence(&mut seq)
            .returning(move |req| {
                let req = req.into_inner();
                match req.transaction.unwrap().selector.unwrap() {
                    Selector::Begin(_) => {
                        let mut rs = setup_select1();
                        rs.metadata.as_mut().unwrap().transaction = Some(mock_v1::Transaction {
                            id: vec![4, 5, 6],
                            ..Default::default()
                        });
                        Ok(Response::new(Box::pin(tokio_stream::iter(vec![Ok(rs)]))))
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
                match req.transaction.unwrap().selector.unwrap() {
                    Selector::Id(id) => {
                        assert_eq!(id, vec![4, 5, 6]);
                        Ok(Response::new(Box::pin(tokio_stream::iter(vec![Ok(
                            setup_select1(),
                        )]))))
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

    #[tokio::test]
    async fn execute_concurrent_queries_late_arrival_failure() -> anyhow::Result<()> {
        let mut mock = create_session_mock();
        let mut seq = mockall::Sequence::new();

        // 1. Initial query fails.
        mock.expect_execute_streaming_sql()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|req| {
                let req = req.into_inner();
                match req.transaction.unwrap().selector.unwrap() {
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

    #[tokio::test]
    async fn execute_concurrent_reads_inline_begin() -> anyhow::Result<()> {
        use crate::client::{KeySet, ReadRequest};
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
                match req.transaction.unwrap().selector.unwrap() {
                    mock_v1::transaction_selector::Selector::Begin(_) => {}
                    _ => panic!("Expected Selector::Begin for first read"),
                }

                let rx = rx_receiver
                    .try_lock()
                    .expect("mutex poisoned")
                    .take()
                    .unwrap();
                Ok(Response::new(Box::pin(ReceiverStream(rx))))
            });

        // 2. The other reads: should include populated Selector::Id
        mock.expect_streaming_read()
            .times(2)
            .in_sequence(&mut seq)
            .returning(move |req| {
                let req = req.into_inner();
                match req.transaction.unwrap().selector.unwrap() {
                    mock_v1::transaction_selector::Selector::Id(id) => {
                        assert_eq!(id, vec![4, 5, 6]);
                    }
                    _ => panic!("Expected Selector::Id for other reads"),
                }

                Ok(Response::new(Box::pin(tokio_stream::iter(vec![Ok(
                    setup_select1(),
                )]))))
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
        rs.metadata.as_mut().unwrap().transaction = Some(mock_v1::Transaction {
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

    #[tokio::test]
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
                .unwrap(),
            &id1
        );

        // 2. Redundant update with same ID should result in an error.
        // The implementation explicitly prevents redundant updates to ensure state consistency.
        let err1 = tx
            .context
            .transaction_selector
            .update(id1.clone(), None)
            .expect_err("Redundant update should fail");
        assert!(err1.to_string().contains("already Started or Failed"));

        // 3. Update with DIFFERENT ID after already Started should also fail.
        let err2 = tx
            .context
            .transaction_selector
            .update(id2, None)
            .expect_err("Update after Started should fail");
        assert!(err2.to_string().contains("already Started or Failed"));

        Ok(())
    }

    #[tokio::test]
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
                Ok(Response::new(Box::pin(tokio_stream::iter(vec![Ok(
                    setup_select1(),
                )]))))
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
}
