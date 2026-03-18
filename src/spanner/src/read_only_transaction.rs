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
use crate::result_set::ResultSet;
use crate::statement::Statement;
use crate::timestamp_bound::TimestampBound;

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
            context: ReadContext::new(self.client, transaction_selector),
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
    /// # use google_cloud_spanner::client::{Spanner, Read, KeySet};
    /// # use google_cloud_spanner::key;
    /// # async fn run(spanner: Spanner) -> Result<(), google_cloud_spanner::Error> {
    /// let db_client = spanner.database_client("projects/p/instances/i/databases/d").build().await?;
    /// let transaction = db_client.single_use().build();
    ///
    /// // Read using the primary key
    /// let read_by_pk = Read::new("Users", vec!["Id", "Name"]).with_keys(KeySet::all());
    /// let mut result_set = transaction.read(read_by_pk).await?;
    /// while let Some(row) = result_set.next().await {
    ///     let _row = row?;
    ///     // process row
    /// }
    ///
    /// // Read using a secondary index
    /// let read_by_index = Read::new("Users", vec!["Id", "Name"])
    ///     .with_index("UsersByIndex", key![1_i64]);
    /// let mut result_set = transaction.read(read_by_index).await?;
    /// while let Some(row) = result_set.next().await {
    ///     let _row = row?;
    ///     // process row
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn read<T: Into<crate::read::ReadRequest>>(
        &self,
        read: T,
    ) -> crate::Result<ResultSet> {
        self.context.read(read).await
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
}

impl MultiUseReadOnlyTransactionBuilder {
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
    /// let builder = db_client.read_only_transaction().with_timestamp_bound(TimestampBound::strong());
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_timestamp_bound(mut self, bound: TimestampBound) -> Self {
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
            Some(b) => read_only.set_timestamp_bound(b.0),
            None => read_only.set_strong(true),
        };
        let request = crate::model::BeginTransactionRequest::default()
            .set_session(self.client.session.name.clone())
            .set_options(TransactionOptions::default().set_read_only(read_only));

        // TODO(#4972): make request options configurable
        let response = self
            .client
            .spanner
            .begin_transaction(request, crate::RequestOptions::default())
            .await?;

        let transaction_selector = crate::model::TransactionSelector::default().set_id(response.id);
        Ok(MultiUseReadOnlyTransaction {
            context: ReadContext::new(self.client, transaction_selector),
            read_timestamp: response.read_timestamp,
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
    context: ReadContext,
    pub(crate) read_timestamp: Option<wkt::Timestamp>,
}

impl MultiUseReadOnlyTransaction {
    /// Returns the read timestamp chosen for the transaction.
    pub fn read_timestamp(&self) -> Option<wkt::Timestamp> {
        self.read_timestamp
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
    /// # use google_cloud_spanner::client::{Spanner, Read, KeySet};
    /// # use google_cloud_spanner::key;
    /// # async fn run(spanner: Spanner) -> Result<(), google_cloud_spanner::Error> {
    /// let db_client = spanner.database_client("projects/p/instances/i/databases/d").build().await?;
    /// let transaction = db_client.read_only_transaction().build().await?;
    ///
    /// // Read using the primary key
    /// let read_by_pk = Read::new("Users", vec!["Id", "Name"]).with_keys(KeySet::all());
    /// let mut result_set = transaction.read(read_by_pk).await?;
    /// while let Some(row) = result_set.next().await {
    ///     let _row = row?;
    ///     // process row
    /// }
    ///
    /// // Read using a secondary index
    /// let read_by_index = Read::new("Users", vec!["Id", "Name"])
    ///     .with_index("UsersByIndex", key![1_i64]);
    /// let mut result_set = transaction.read(read_by_index).await?;
    /// while let Some(row) = result_set.next().await {
    ///     let _row = row?;
    ///     // process row
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn read<T: Into<crate::read::ReadRequest>>(
        &self,
        read: T,
    ) -> crate::Result<ResultSet> {
        self.context.read(read).await
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ReadContext {
    pub(crate) client: DatabaseClient,
    pub(crate) transaction_selector: crate::model::TransactionSelector,
}

impl ReadContext {
    pub(crate) fn new(
        client: DatabaseClient,
        transaction_selector: crate::model::TransactionSelector,
    ) -> Self {
        Self {
            client,
            transaction_selector,
        }
    }

    pub(crate) async fn execute_query<T: Into<Statement>>(
        &self,
        statement: T,
    ) -> crate::Result<ResultSet> {
        let statement = statement.into();

        let mut request = crate::model::ExecuteSqlRequest::default()
            .set_session(self.client.session.name.clone())
            .set_transaction(self.transaction_selector.clone());
        request.params = statement.get_params();
        request.param_types = statement.get_param_types();
        request = request.set_sql(statement.sql);

        let stream = self
            .client
            .spanner
            // TODO(#4972): make request options configurable
            .execute_streaming_sql(request, crate::RequestOptions::default())
            .send()
            .await?;

        Ok(ResultSet::new(stream))
    }

    pub(crate) async fn read<T: Into<crate::read::ReadRequest>>(
        &self,
        read: T,
    ) -> crate::Result<ResultSet> {
        let read = read.into();

        let mut request = crate::model::ReadRequest::default()
            .set_session(self.client.session.name.clone())
            .set_transaction(self.transaction_selector.clone())
            .set_table(read.table)
            .set_columns(read.columns)
            .set_key_set(read.keys.into_proto());

        if let Some(index) = read.index {
            request = request.set_index(index);
        }

        if let Some(limit) = read.limit {
            request = request.set_limit(limit);
        }

        let stream = self
            .client
            .spanner
            // TODO(#4972): make request options configurable
            .streaming_read(request, crate::RequestOptions::default())
            .send()
            .await?;

        Ok(ResultSet::new(stream))
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

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
        let ro = tx
            .context
            .transaction_selector
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
        let ro2 = tx2
            .context
            .transaction_selector
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

            Ok(gaxi::grpc::tonic::Response::new(Box::pin(
                tokio_stream::iter(vec![Ok(setup_select1())]),
            )))
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

                Ok(gaxi::grpc::tonic::Response::new(Box::pin(
                    tokio_stream::iter(vec![Ok(setup_select1())]),
                )))
            });

        let (db_client, _server) = setup_db_client(mock).await;

        let tx = db_client
            .read_only_transaction()
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
    async fn execute_single_read() {
        use super::super::result_set::tests::string_val;
        use crate::client::{KeySet, Read};
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

            Ok(gaxi::grpc::tonic::Response::new(Box::pin(
                tokio_stream::iter(vec![Ok(setup_select1())]),
            )))
        });

        let (db_client, _server) = setup_db_client(mock).await;

        let tx = db_client.single_use().build();
        let read = Read::new("Users", vec!["Id", "Name"]).with_keys(KeySet::all());
        let mut rs = tx.read(read).await.expect("Failed to execute read");

        let row = rs.next().await.expect("has row").expect("has valid row");
        assert_eq!(row.raw_values(), [Value(string_val("1"))]);
        let result = rs.next().await;
        assert!(result.is_none(), "expected None, got {result:?}");
    }
}
