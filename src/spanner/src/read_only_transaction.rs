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
use std::sync::{Arc, Mutex};

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
    explicit_begin: bool,
}

impl MultiUseReadOnlyTransactionBuilder {
    pub(crate) fn new(client: DatabaseClient) -> Self {
        Self {
            client,
            timestamp_bound: None,
            explicit_begin: false,
        }
    }

    /// Sets whether the transaction should be explicitly started using a `BeginTransaction` RPC.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::Spanner;
    /// # use google_cloud_spanner::client::Statement;
    /// # async fn set_explicit_begin(spanner: Spanner) -> Result<(), google_cloud_spanner::Error> {
    /// let db_client = spanner.database_client("projects/p/instances/i/databases/d").build().await?;
    /// let transaction = db_client.read_only_transaction().with_explicit_begin_transaction(true).build().await?;
    /// let statement = Statement::builder("SELECT * FROM users").build();
    /// let result_set = transaction.execute_query(statement).await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// By default, the Spanner client will inline the `BeginTransaction` call with the first query
    /// in the transaction. This reduces the number of round-trips to Spanner that are needed for a
    /// transaction. Setting this option to `true` can be beneficial for specific transaction shapes:
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
    /// Default is `false` (inline begin).
    pub fn with_explicit_begin_transaction(mut self, explicit: bool) -> Self {
        self.explicit_begin = explicit;
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
        session_name: String,
        options: TransactionOptions,
    ) -> crate::Result<ReadContextTransactionSelector> {
        let response = execute_begin_transaction(&self.client, session_name, options).await?;

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
        let selector = if self.explicit_begin {
            self.begin(session_name.clone(), options).await?
        } else {
            ReadContextTransactionSelector::Lazy(Arc::new(Mutex::new(
                TransactionState::NotStarted(options),
            )))
        };

        Ok(MultiUseReadOnlyTransaction {
            context: ReadContext {
                session_name,
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
async fn execute_begin_transaction(
    client: &crate::database_client::DatabaseClient,
    session_name: String,
    options: crate::model::TransactionOptions,
) -> crate::Result<crate::model::Transaction> {
    let request = crate::model::BeginTransactionRequest::default()
        .set_session(session_name)
        .set_options(options);

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
    Started(crate::model::TransactionSelector, Option<wkt::Timestamp>),
}

impl TransactionState {
    fn selector(&self) -> crate::model::TransactionSelector {
        match self {
            Self::Started(selector, _) => selector.clone(),
            Self::NotStarted(options) => {
                crate::model::TransactionSelector::default().set_begin(options.clone())
            }
        }
    }
}

impl ReadContextTransactionSelector {
    pub(crate) fn selector(&self) -> crate::model::TransactionSelector {
        match self {
            Self::Fixed(selector, _) => selector.clone(),
            Self::Lazy(lazy) => lazy
                .lock()
                .expect("transaction state mutex poisoned")
                .selector(),
        }
    }

    /// Explicitly begins a transaction if the transaction selector is a `Lazy`
    /// selector and the transaction has not yet been started. This is used by
    /// the client to force the start of a transaction if the first statement
    /// failed.
    pub(crate) async fn begin_explicitly(
        &self,
        client: &crate::database_client::DatabaseClient,
        session_name: String,
    ) -> crate::Result<()> {
        let Self::Lazy(lazy) = self else {
            return Ok(());
        };

        let options = {
            let guard = lazy.lock().expect("transaction state mutex poisoned");
            let TransactionState::NotStarted(options) = &*guard else {
                return Ok(());
            };
            options.clone()
        };

        let response = execute_begin_transaction(client, session_name, options).await?;
        self.update(response.id, response.read_timestamp);

        Ok(())
    }

    pub(crate) fn update(&self, id: bytes::Bytes, timestamp: Option<wkt::Timestamp>) {
        if let Self::Lazy(lazy) = self {
            let mut guard = lazy.lock().expect("transaction state mutex poisoned");
            if matches!(&*guard, TransactionState::NotStarted(_)) {
                *guard = TransactionState::Started(
                    crate::model::TransactionSelector::default().set_id(id),
                    timestamp,
                );
            }
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
    pub(crate) session_name: String,
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
            .begin_explicitly(&self.client, self.session_name.clone())
            .await?;
        Ok(true)
    }
}

/// Helper macro to execute a streaming SQL or streaming read RPC with retry logic.
macro_rules! execute_stream_with_retry {
    ($self:expr, $request:ident, $gax_options:ident, $rpc_method:ident, $operation_variant:path) => {{
        let stream = match $self
            .client
            .spanner
            .$rpc_method($request.clone(), $gax_options.clone())
            .send()
            .await
        {
            Ok(s) => s,
            Err(e) => {
                if $self.begin_explicitly_if_not_started().await? {
                    $request.transaction = Some($self.transaction_selector.selector());
                    $self
                        .client
                        .spanner
                        .$rpc_method($request.clone(), $gax_options.clone())
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
            $self.session_name.clone(),
            $operation_variant($request),
            $gax_options,
        ))
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
            .set_transaction(self.transaction_selector.selector());
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
            .set_transaction(self.transaction_selector.selector());
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
    use crate::result_set::tests::{adapt, string_val};
    use spanner_grpc_mock::google::spanner::v1 as mock_v1;

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
            Ok(gaxi::grpc::tonic::Response::new(
                spanner_grpc_mock::google::spanner::v1::Session {
                    name: "projects/p/instances/i/databases/d/sessions/123".to_string(),
                    ..Default::default()
                },
            ))
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
        let selector = tx.context.transaction_selector.selector();
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
        let selector = tx2.context.transaction_selector.selector();
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
            Ok(gaxi::grpc::tonic::Response::new(mock_v1::Transaction {
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
            .with_explicit_begin_transaction(true)
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
            .with_explicit_begin_transaction(false)
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

    #[tokio::test]
    async fn execute_multi_read() -> anyhow::Result<()> {
        use super::super::result_set::tests::string_val;
        use crate::client::{KeySet, ReadRequest};
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
            .with_explicit_begin_transaction(false)
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

    #[tokio::test]
    async fn inline_begin_failure_retry_success() -> anyhow::Result<()> {
        use crate::value::Value;
        use gaxi::grpc::tonic::Response;
        use gaxi::grpc::tonic::Status;

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
            .with_explicit_begin_transaction(false)
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
        use gaxi::grpc::tonic::Response;
        use gaxi::grpc::tonic::Status;

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
            .with_explicit_begin_transaction(false)
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
            .with_explicit_begin_transaction(false)
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
        use gaxi::grpc::tonic::Response;
        use gaxi::grpc::tonic::Status;

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
            .with_explicit_begin_transaction(false)
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
            .with_explicit_begin_transaction(false)
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
}
